// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.ClusterLogReader;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentity;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowImageLoader;
import com.openexchange.cluster.shadow.ShadowProducer;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.shadow.ShadowWriterThread;
import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The reader thread {@code shadow-reader-R<slot>}: bootstrap once, then follow the local
 * recording L that {@code ClusterBackup} fills, apply it through {@link ClusterLogReader} and {@link ReplayContainer}
 * into the engine, and hand triggers to {@link ShadowTriggerPolicy}.
 *
 * <h2>Bootstrap (first match wins)</h2>
 * <p>Every branch first waits for L's descriptor ({@code REPLICA_BOOTSTRAP_WAIT_MS}, detail
 * {@code bootstrap: waiting for log recording}), then one scan with genesis unknown ({@code verifyAllComplete}):</p>
 * <pre>
 * geneses  two or more geneses among COMPLETE/VERIFIED copies    refuse (two-geneses text), exit 1
 * shadow   newest usable local shadow S with S.P_s >= L.start     BOOTSTRAP_ADOPT check, load, restore, matches, the
 *                                                                 engine's post-restore witness, requireProducible; a
 *                                                                 CORRUPT or refused candidate falls to the next;
 *                                                                 restore_path LOCAL_SHADOW
 * genesis  no usable local shadow and L.start == 0                genesis(); the writer is armed after the ADOPT frame
 * pruned   no usable local shadow and L.start > 0                 refuse with the runbook text, exit 1
 * refused  usable shadows, all refused or below L.start           refuse as pruned, with usable/refused counts
 * </pre>
 * <p>One {@code restore} or {@code genesis} per process; a failure after {@code restore} halts.</p>
 *
 * <p>After L is found the bootstrap lists the recordings on the cluster log and snapshot streams that
 * are neither L nor referenced by the backup RecordingLog (CRITICAL, {@code shadow_replica_orphan_recordings});
 * housekeeping purges them through {@link RetrievedSetTrim}.</p>
 *
 * <h2>Follow</h2>
 * <p>A replay of L is opened only when L's {@code RecordingPos} counter is alive for L (found by recording id, so a
 * recycled counter id never reads alive) or L's stop position is beyond what the reader consumed; otherwise
 * the loop idles {@code REPLICA_ATTACH_BACKOFF_MS} with readiness detail {@code awaiting live recording}. After a
 * backup reset the old replay ends at L's stop; the next one starts at {@code reader.consumedPosition()} through
 * {@link ClusterLogReader#successor}, so a gap stops {@code START_MISMATCH} (loud). L holds committed bytes only.</p>
 *
 * <p>The first attach waits for this process's backup to update its RecordingLog, then the newest
 * retrieved member service snapshot must hold the local shadows' genesis and agree with the RecordingLog's term
 * entry; otherwise {@code FATAL replica: cluster genesis <g2> != local shadows <g1>; wipe BASE_DIR} before any frame
 * is applied. A dead counter with {@code stop > consumed} reads {@code catching up (backup reset)}.</p>
 *
 * <h2>Alignment trust</h2>
 * <p>A GENESIS start is VERIFIED; a LOCAL_SHADOW start is UNVERIFIED ({@code /ready} 503 {@code unverified
 * continuation}, no STANDBY or LOCAL shadow) until the first ALIGNED image compares byte-equal with the member
 * snapshot the backup retrieved at the same position. Every ALIGNED is compared ({@link AlignmentTrust}); a
 * same-genesis byte difference holds, a copy of another genesis is a history fence.</p>
 *
 * <h2>Fence</h2>
 * <p>Any throwable out of a poll latches the fence, aborts the in-flight write and calls the host's fatal hook
 * ({@code FATAL replica: apply fenced at <pos>: <reason>}); nothing is serialized after it because the trigger's
 * serialize runs only after a poll returned normally. The fence carries a {@link FenceFamily} ({@link ReplicaFence}):
 * a same-history byte mismatch HOLDS (this thread stops applying and only ticks liveness until the process is
 * stopped), a history fence (another genesis, an applied-past member snapshot, a cluster identity refusal) purges own
 * shadows above the last EQUAL position or the restore point, whichever is higher, through the housekeeping thread,
 * awaited, then exits 1; every other fence exits 1.</p>
 */
public final class ReplicaFollower implements Runnable, AutoCloseable {

    /** The host's fatal hook: {@code lifecycle.fatal(1, reason)}; and the mismatch hold notice. */
    @FunctionalInterface
    public interface Halt {
        void fatal(String reason);

        /**
         * The replica entered the oracle mismatch hold: the process must stay up (endpoint, metrics, housekeeping)
         * until it is stopped. The default does nothing beyond what the caller logged.
         */
        default void hold(final String reason) {
        }
    }

    static final String REPLAY_CHANNEL = "aeron:ipc?linger=0|sparse=true";
    /** The reason prefix {@code ShadowCatalog} gives copies of several geneses on a bootstrap catalog. */
    static final String AMBIGUOUS_GENESIS = "ambiguous genesis on a bootstrap catalog";
    private static final long STATUS_PERIOD_NS = TimeUnit.MILLISECONDS.toNanos(100);

    private final ReplicaConfig cfg;
    private final ReplicaEngine binding;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final AeronArchive.Context writerArchiveContext;
    private final BackupEvents events;
    private final ReplicaReadiness readiness;
    private final ShadowCounters shadowCounters;
    private final ShadowVerdicts verdicts;
    private final NanoClock clock;
    private final IdleStrategy idle;
    private final Halt halt;
    private final ReplicaTermIndex termIndex = new ReplicaTermIndex();
    private final Frames frames = new Frames();
    private final long attachBackoffNs;
    /** The backup RecordingLog, looked at once per pass, parsed again only when the file changed. */
    private final RecordingLogView logView;

    private volatile boolean running = true;
    /** The fence latch and what each family does after it. */
    private final ReplicaFence fence;
    private volatile long knownGenesis;
    private volatile long logRecordingId = -1;
    private volatile long logStartPosition = -1;
    private volatile boolean bootstrapDone;
    private volatile long applied = -1;
    private volatile long recorded = -1;
    private volatile boolean liveAlive;
    private volatile long reattachTotal;
    private volatile long attachAttemptsTotal;
    private volatile long attachFailuresTotal;
    private volatile long restoreAttempts;
    private volatile long restoredPosition = -1;
    private volatile long shadowsRefused;
    private volatile long appliedClusterTime = -1;
    private volatile long firstProducerSeq = -1;
    /** The genesis of the local usable shadows at bootstrap (the geneses rule leaves at most one), 0 when there are none. */
    private volatile long localShadowGenesis;
    /**
     * The genesis compare against the member snapshot the live
     * member's backup response named; paced, never latched for good.
     */
    private final GenesisGuard genesisGuard;
    /** The local copy of the named snapshot and the candidates never read again. */
    private final NamedCopyResolver copies;
    /** The orphan recordings found at bootstrap and not purged yet (housekeeping removes them). */
    private final java.util.concurrent.ConcurrentSkipListSet<Long> orphans =
            new java.util.concurrent.ConcurrentSkipListSet<>();

    private volatile AlignmentTrust trust;
    private ReplayContainer container;
    private ShadowTriggerPolicy policy;
    private ShadowWriter writer;
    private ShadowWriterThread writerThread;
    private AeronArchive writerArchive;
    private ShadowImageBuffer imageBuffer;
    private ClusterLogReader reader;
    private boolean firstPollOfReader;
    private Subscription subscription;
    private Image image;
    private long replaySessionId = Aeron.NULL_VALUE;
    private long nextAttemptNs;
    private long nextStatusNs;
    /** Refused attaches, one line per distinct reason plus a periodic count. */
    private final AttachRefusalLog attachRefusals;
    private long replayCount;
    private Thread thread;

    /**
     * @param archive              the follower's own client of the local archive (not thread-safe: this thread only)
     * @param writerArchiveContext a context the writer thread's own client connects with
     */
    public ReplicaFollower(final ReplicaConfig cfg, final ReplicaEngine binding, final Aeron aeron,
                           final AeronArchive archive, final AeronArchive.Context writerArchiveContext,
                           final BackupEvents events, final ReplicaReadiness readiness,
                           final ShadowCounters shadowCounters, final ShadowVerdicts verdicts, final NanoClock clock,
                           final IdleStrategy idle, final Halt halt) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.archive = Objects.requireNonNull(archive, "archive");
        this.writerArchiveContext = Objects.requireNonNull(writerArchiveContext, "writerArchiveContext");
        this.events = Objects.requireNonNull(events, "events");
        this.readiness = Objects.requireNonNull(readiness, "readiness");
        this.shadowCounters = Objects.requireNonNull(shadowCounters, "shadowCounters");
        this.verdicts = Objects.requireNonNull(verdicts, "verdicts");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.idle = Objects.requireNonNull(idle, "idle");
        this.halt = Objects.requireNonNull(halt, "halt");
        this.attachBackoffNs = TimeUnit.MILLISECONDS.toNanos(cfg.attachBackoffMs);
        this.attachRefusals = new AttachRefusalLog(System.err, clock);
        this.fence = new ReplicaFence(halt, new ReplicaFence.Effects() {
            @Override
            public void fenced(final String reason) {
                readiness.fenced(reason);
            }

            @Override
            public void refused(final String reason) {
                readiness.refused(reason);
            }

            @Override
            public void abortWrite() {
                final ShadowWriter w = writer;
                if (w != null) {
                    w.requestAbort();
                }
            }
        }, new MismatchHold(cfg.mismatchHoldMs, clock, halt),
                new MismatchDump(cfg.mismatchDir(), cfg.mismatchDumpMaxMb,
                        org.agrona.concurrent.SystemEpochClock.INSTANCE, MismatchDump.FILES),
                cfg.fencePurgeWaitMs, cfg.off);
        this.logView = new RecordingLogView(cfg.clusterDir());
        this.copies = new NamedCopyResolver(new NamedCopyResolver.Source() {
            @Override
            public long retrievedCopy(final GenesisGuard.Named n) {
                return events.retrievedCopy(n.position(), n.leadershipTermId());
            }

            @Override
            public List<Long> candidates() {
                return ClusterIdentityCheck.candidates(archive, cfg.snapshotStreamIds());
            }

            @Override
            public ClusterIdentityCheck.Markers markers(final long recordingId) {
                return ClusterIdentityCheck.markers(aeron, archive, clock, recordingId,
                        clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cfg.loadTimeoutMs));
            }

            @Override
            public long genesisOf(final long recordingId) {
                return ClusterIdentityCheck.genesisOf(aeron, archive, clock, recordingId,
                        clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cfg.loadTimeoutMs), binding, cfg.clusterId);
            }
        });
        this.genesisGuard = new GenesisGuard(new GenesisGuard.Source() {
            @Override
            public GenesisGuard.Named newestNamed() {
                return events.newestNamed();
            }

            @Override
            public long localCopy(final GenesisGuard.Named n) {
                return copies.resolve(n);
            }

            @Override
            public long genesisOf(final long recordingId) {
                return copies.genesisOf(recordingId); // never cached when it fails; a refusal is final
            }

            @Override
            public String termContradiction(final long recordingId) {
                return ClusterIdentityCheck.termContradictionOf(logView.get(), recordingId);
            }

            @Override
            public String refusalOf(final GenesisGuard.Named n) {
                return copies.refusalOf(n);
            }
        }, clock, AlignmentTrust.CHECK_PACE_NS, cfg.off.contains(ReplicaNegativeControl.GENESIS_CHECK_OFF));
        if (binding.engineId() != cfg.engineId) {
            throw new IllegalArgumentException("engine binding id " + binding.engineId() + " != config engineId "
                    + cfg.engineId);
        }
    }

    /** Starts {@code shadow-reader-R<slot>}. */
    public void start() {
        if (thread != null) {
            throw new IllegalStateException("follower already started");
        }
        thread = new Thread(this, "shadow-reader-R" + cfg.slot);
        thread.setDaemon(true);
        thread.start();
    }

    // ------------------------------------------------------------------------------------------------ run

    @Override
    public void run() {
        try {
            if (!bootstrap()) {
                return;
            }
            follow();
        } catch (final Throwable t) {
            // the thread ends here; the fence has already reached the host's fatal hook (exit 1)
            fence("FATAL replica: apply fenced at " + (container == null ? -1 : container.logPosition()) + ": "
                    + t);
        } finally {
            closeReplay();
        }
        holdLoop();
    }

    /**
     * The mismatch hold: nothing is applied or written; this thread only keeps its liveness tick so {@code /health}
     * reads the process as alive (it is: it serves its evidence) until the process is stopped.
     */
    private void holdLoop() {
        if (!fence.holding()) {
            return;
        }
        System.err.println("[REPLICA] follower held at " + applied + ": " + fence.reason());
        while (running) {
            readiness.followerTick();
            sleepQuietly(100);
        }
    }

    // ------------------------------------------------------------------------------------------------ bootstrap

    private record Recording(long recordingId, long startPosition, long stopPosition) {
    }

    private boolean bootstrap() {
        container = new ReplayContainer(binding.engine(), binding.appVersion(), binding.validator(),
                (logPosition, termId, timestamp, flags) -> policy != null
                        && policy.onSnapshotAction(logPosition, termId, timestamp, flags));

        // every branch waits for L's descriptor first
        readiness.bootstrapping("bootstrap: waiting for log recording");
        final Recording l = awaitLogRecording();
        if (l == null) {
            return false; // stopped while waiting, or refused (the refusal is already recorded)
        }
        logRecordingId = l.recordingId();
        logStartPosition = l.startPosition();
        System.out.println("[REPLICA] live log recording L=" + l.recordingId() + " start=" + l.startPosition()
                + " stop=" + l.stopPosition());

        readiness.bootstrapping("bootstrap: scanning local shadows");
        final long loadTimeoutNs = TimeUnit.MILLISECONDS.toNanos(cfg.loadTimeoutMs);
        final ShadowCatalog bootCatalog = new ShadowCatalog(aeron, archive, cfg.engineId, cfg.clusterId, 0, verdicts,
                ShadowCatalog.DEFAULT_LIST_BATCH, clock, loadTimeoutNs, shadowCounters);
        final ShadowCatalog.Scan scan = bootCatalog.verifyAllComplete();
        if (!scan.known() && scan.errors() > 0) {
            return refuse("FATAL replica: the local shadow scan had " + scan.errors() + " archive errors; refusing"
                    + " to choose a bootstrap path on a partial view (restart the replica)");
        }

        // Two geneses. A bootstrap catalog (genesis unknown) already turns COMPLETE copies of two or more geneses into
        // FOREIGN "ambiguous genesis on a bootstrap catalog" (ShadowCatalog step 3), so usable copies alone never
        // show two geneses: those FOREIGN copies count here too (the rule is unreachable otherwise).
        final Set<Long> geneses = new HashSet<>();
        for (final ShadowEntry e : scan.all()) {
            if (e.header() != null && (e.state().usable() || (e.state() == ShadowEntry.State.FOREIGN
                    && e.reason() != null && e.reason().startsWith(AMBIGUOUS_GENESIS)))) {
                geneses.add(e.header().genesisId());
            }
        }
        if (geneses.size() >= 2) {
            return refuse("FATAL replica: replica archive holds shadows of " + geneses.size() + " geneses "
                    + geneses + "; wipe BASE_DIR or pin the genesis");
        }
        localShadowGenesis = geneses.isEmpty() ? 0 : geneses.iterator().next();
        findOrphans(l.recordingId());

        // restore from a local shadow
        final List<LogicalShadow> candidates = new ArrayList<>(scan.usable());
        candidates.sort(Comparator.comparingLong(LogicalShadow::logPosition).reversed());
        final ShadowImageLoader loader = new ShadowImageLoader(aeron, archive, verdicts, clock, loadTimeoutNs,
                shadowCounters);
        int usable = 0;
        int refusedCount = 0;
        for (final LogicalShadow s : candidates) {
            usable++;
            if (s.conflict()) {
                refusedCount++;
                System.out.println("[REPLICA] bootstrap: shadow at " + s.logPosition() + " is CONFLICT (operator"
                        + " state), skipped");
                continue;
            }
            if (s.logPosition() < l.startPosition()) {
                refusedCount++;
                System.out.println("[REPLICA] bootstrap: shadow at " + s.logPosition() + " is below L.start="
                        + l.startPosition() + " (the log it needs is gone), skipped");
                continue;
            }
            final Boolean restored = tryRestore(s, loader);
            if (restored == null) {
                return false; // halted
            }
            if (restored) {
                shadowsRefused = refusedCount;
                return armAfterBootstrap(scan, ReplicaReadiness.RestorePath.LOCAL_SHADOW);
            }
            refusedCount++;
        }
        shadowsRefused = refusedCount;

        // genesis, or refuse: the log is pruned, or every usable shadow was refused
        if (usable == 0) {
            if (l.startPosition() == 0 || cfg.off.contains(ReplicaNegativeControl.GENESIS_PRECONDITION_OFF)) {
                if (l.startPosition() != 0) {
                    System.err.println("[REPLICA] NC GENESIS_PRECONDITION_OFF: genesis on a pruned log: start="
                            + l.startPosition());
                }
                restoreAttempts++;
                container.genesis();
                restoredPosition = 0;
                return armAfterBootstrap(scan, ReplicaReadiness.RestorePath.GENESIS);
            }
            return refuse(runbook(l.startPosition(), ""));
        }
        if (l.startPosition() == 0) {
            return refuse("FATAL replica: no usable local shadow (shadows usable=" + usable + " refused="
                    + refusedCount + ") and genesis is never taken over refused shadows. Runbook: inspect the"
                    + " refusals above; wipe BASE_DIR to start from genesis while a member still holds the log"
                    + " from 0.");
        }
        return refuse(runbook(l.startPosition(), " shadows usable=" + usable + " refused=" + refusedCount));
    }

    private static String runbook(final long start, final String counts) {
        return "FATAL replica: no local shadow and the cluster log starts at " + start + " > 0." + counts
                + " Runbook: (1) copy a VERIFIED shadow of this genesis into BASE_DIR/archive from a member;"
                + " (2) or wipe BASE_DIR and start the replica while a member still holds the"
                + " log from 0.";
    }

    /**
     * One candidate's copies, verified first. True when restored, false when every copy was refused before any
     * restore, null when halted (a failure after restore).
     */
    private Boolean tryRestore(final LogicalShadow s, final ShadowImageLoader loader) {
        final List<ShadowEntry> copies = new ArrayList<>(s.copies());
        copies.sort(Comparator.comparingInt(e -> e.state() == ShadowEntry.State.VERIFIED ? 0 : 1));
        for (final ShadowEntry e : copies) {
            if (!e.state().usable() || e.header() == null) {
                continue;
            }
            final ShadowHeader h = e.header();
            final ReplicaTermIndex seeded = new ReplicaTermIndex();
            seeded.seed(h.leadershipTermId(), h.termBaseLogPosition());
            final ShadowIdentity.Outcome outcome = ShadowIdentity.check(h,
                    seeded.local(cfg.engineId, cfg.clusterId, 0));
            if (!ShadowIdentity.accepted(outcome, ShadowIdentity.Policy.BOOTSTRAP_ADOPT)) {
                System.out.println("[REPLICA] bootstrap: rec=" + e.recordingId() + " P=" + h.logPosition()
                        + " refused by identity: " + outcome);
                continue;
            }
            if (h.payloadLength() <= 0 || h.payloadLength() > ShadowHeader.MAX_PAYLOAD_LENGTH) {
                System.out.println("[REPLICA] bootstrap: rec=" + e.recordingId() + " payloadLength "
                        + h.payloadLength() + " out of range, skipped");
                continue;
            }
            final UnsafeBuffer into = new UnsafeBuffer(ByteBuffer.allocateDirect((int)h.payloadLength()));
            final ShadowImageLoader.LoadResult loaded = loader.load(e, into);
            if (!loaded.ok()) {
                System.out.println("[REPLICA] bootstrap: rec=" + e.recordingId() + " P=" + h.logPosition()
                        + " not loaded: " + loaded.kind() + " " + loaded.reason());
                continue;
            }
            if (restoreAttempts > 0 && !cfg.off.contains(ReplicaNegativeControl.TWO_RESTORES_ALLOWED)) {
                halt("FATAL replica: two restore attempts in one process");
                return null;
            }
            restoreAttempts++;
            final ShadowImageLoader.LoadedShadow ls = loaded.loaded();
            try {
                container.restore(ls.payload(), ls.length(), h.logPosition(), ls.cursor());
            } catch (final RuntimeException failed) {
                if (cfg.off.contains(ReplicaNegativeControl.TWO_RESTORES_ALLOWED)) {
                    System.err.println("[REPLICA] NC TWO_RESTORES_ALLOWED: two restore attempts in one process"
                            + " after " + failed);
                    container = new ReplayContainer(binding.engine(), binding.appVersion(), binding.validator(),
                            (logPosition, termId, timestamp, flags) -> policy != null
                                    && policy.onSnapshotAction(logPosition, termId, timestamp, flags));
                    continue;
                }
                halt("FATAL replica: restore of the shadow at " + h.logPosition() + " (rec=" + e.recordingId()
                        + ") failed: " + failed);
                return null;
            }
            if (!cfg.off.contains(ReplicaNegativeControl.MATCHES_OFF)
                    && !ShadowIdentity.matches(h, binding.identity(), shadowCounters)) {
                halt("FATAL replica: IDENTITY_CROSS_CHECK_FAILED at " + h.logPosition() + ": "
                        + ShadowIdentity.mismatch(h, binding.identity()));
                return null;
            }
            final String witness = binding.postRestoreWitness(h);
            if (witness != null) {
                halt("FATAL replica: " + witness);
                return null;
            }
            try {
                ShadowIdentity.requireProducible(binding.identity().genesisId());
            } catch (final IllegalStateException unproducible) {
                halt("FATAL replica: " + unproducible.getMessage());
                return null;
            }
            termIndex.seed(h.leadershipTermId(), h.termBaseLogPosition());
            knownGenesis = h.genesisId();
            restoredPosition = h.logPosition();
            System.out.println("[REPLICA] restore_path=LOCAL_SHADOW from rec=" + e.recordingId() + " P_s="
                    + h.logPosition() + " kind=" + h.kindEnum() + " producer=" + e.producer() + " genesisId="
                    + h.genesisId());
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    private Recording awaitLogRecording() {
        final long deadline = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cfg.bootstrapWaitMs);
        idle.reset();
        while (running) {
            logView.beginPass();
            readiness.followerTick();
            final long l = currentLogRecordingId();
            if (l >= 0) {
                final Recording r = describe(l);
                if (r != null) {
                    return r;
                }
            }
            if (clock.nanoTime() - deadline >= 0) {
                refuse("FATAL replica: no live log recording within REPLICA_BOOTSTRAP_WAIT_MS=" + cfg.bootstrapWaitMs
                        + " (bootstrap: waiting for log recording; backup queries=" + events.queries()
                        + " responses=" + events.responses() + " authRejected=" + events.authRejected()
                        + " lastFailure=" + events.lastFailure() + ")");
                return null;
            }
            idle.idle(0);
            sleepQuietly(10);
        }
        return null;
    }

    /** L: the backup's last report, else the backup RecordingLog's last term (read side, as AH:148). */
    long currentLogRecordingId() {
        final long reported = events.liveRecordingId();
        if (reported >= 0) {
            return reported;
        }
        final RecordingLog log = logView.get(); // the pass's one parse
        return log == null ? -1 : log.findLastTermRecordingId();
    }

    private Recording describe(final long recordingId) {
        final long[] d = {-1, -1};
        final int found = archive.listRecording(recordingId,
                (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, startPosition, stopPosition,
                 initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                 strippedChannel, originalChannel, sourceIdentity) -> {
                    d[0] = startPosition;
                    d[1] = stopPosition;
                });
        return found == 0 ? null : new Recording(recordingId, d[0], d[1]);
    }

    private boolean armAfterBootstrap(final ShadowCatalog.Scan scan, final ReplicaReadiness.RestorePath path) {
        // the writer resumes this producer's sequence; frames at or below P_s re-seen are NOT_MONOTONE
        long maxSeq = 0;
        long maxPos = -1;
        for (final ShadowEntry e : scan.all()) {
            final ShadowHeader h = e.header();
            if (h != null && h.producerKind() == ProducerKind.REPLICA.code() && h.producerId() == cfg.slot) {
                maxSeq = Math.max(maxSeq, h.producerSeq());
                if (e.state().usable()) {
                    // the NOT_MONOTONE floor counts usable copies only; a CORRUPT or CONFLICT copy at
                    // P_c must not stop the replica writing P_c again after restoring below it
                    maxPos = Math.max(maxPos, h.logPosition());
                }
            } else if (h == null && e.alias() != null && e.alias().producerKind() == ProducerKind.REPLICA.code()
                    && e.alias().producerId() == cfg.slot) {
                // a crashed write without a readable header (TOO_SHORT): its alias carries the seq
                maxSeq = Math.max(maxSeq, e.alias().producerSeq());
            }
        }
        firstProducerSeq = maxSeq + 1;
        final long bufferBytes = (long)cfg.shadowBufferMb * 1024 * 1024;
        imageBuffer = new ShadowImageBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(
                (int)Math.min(Integer.MAX_VALUE, bufferBytes + ShadowHeader.LENGTH))));
        policy = new ShadowTriggerPolicy(imageBuffer, cfg.engineId, () -> container.cursor(), binding.identity(),
                dst -> container.serialize(dst), clock, ReplicaContexts.idle(cfg),
                TimeUnit.MILLISECONDS.toNanos(cfg.alignedWaitMs), cfg.standbyFallbackMs, cfg.shadowBufferMb,
                new ShadowTriggerPolicy.Fence() {
                    @Override
                    public void fence(final String reason) {
                        ReplicaFollower.this.fence(reason);
                    }

                    @Override
                    public void fence(final FenceFamily family, final String reason) {
                        ReplicaFollower.this.fence(family, reason);
                    }
                }, cfg.faults, cfg.off);
        policy.startAt(container.cursor());
        policy.fencedProbe(() -> fence.reason() != null); // a fence on any thread stops every hand-off
        // only a full replay from genesis is trusted at once; a restore is UNVERIFIED until the first
        // byte-equal ALIGNED compare with a member snapshot
        final Set<Integer> snapshotStreams = cfg.snapshotStreamIds();
        trust = new AlignmentTrust(ClusterIdentityCheck.retrieved(aeron, archive, clock, logView,
                TimeUnit.MILLISECONDS.toNanos(cfg.loadTimeoutMs), events::newestReportedServiceSnapshot,
                () -> ClusterIdentityCheck.retrievalProgress(aeron.countersReader(), snapshotStreams), binding,
                cfg.clusterId), clock, cfg.verifyWaitMs, cfg.engineId, cfg.clusterId, cfg.off);
        trust.start(path == ReplicaReadiness.RestorePath.GENESIS, container.logPosition());
        trust.backupCycle(cfg.backupQueryIntervalMs + cfg.backupCooldownMs); // the no-usable-named-copy CRITICAL threshold
        // while the newest named snapshot has no usable copy the stall clock runs
        trust.namedCopy(new AlignmentTrust.NamedCopy() {
            @Override
            public boolean unusable() {
                return genesisGuard.namedCopyUnusable();
            }

            @Override
            public long completedCompares() {
                return genesisGuard.completedComparesTotal();
            }
        });
        policy.alignmentTrust(trust);
        readiness.verified(trust.ready(), trust.detail());
        writerArchive = AeronArchive.connect(writerArchiveContext.clone().aeron(aeron).ownsAeronClient(false));
        writer = new ShadowWriter(aeron, writerArchive,
                new ShadowProducer(cfg.clusterId, ProducerKind.REPLICA, cfg.slot), binding.selfCheck(),
                cfg.shadowTermLength, clock, TimeUnit.MILLISECONDS.toNanos(cfg.writeConnectTimeoutMs),
                TimeUnit.MILLISECONDS.toNanos(cfg.writeOfferTimeoutMs),
                TimeUnit.MILLISECONDS.toNanos(cfg.writeRecordTimeoutMs), maxSeq + 1, maxPos, shadowCounters);
        writerThread = new ShadowWriterThread(writer, policy, this::onWriterError, shadowCounters);
        writerThread.fenceLatch(() -> fence.reason() != null); // read by the writer thread before each write starts
        policy.attachWriter(ShadowTriggerPolicy.port(writerThread));
        readiness.restored(path);
        bootstrapDone = true;
        applied = container.logPosition();
        appliedClusterTime = container.clusterTime();
        System.out.println("[REPLICA] bootstrap done: restore_path=" + path + " from=" + container.logPosition()
                + " producer=R" + cfg.slot + " nextSeq=" + (maxSeq + 1) + " lastWritten=" + maxPos
                + " shadowsRefused=" + shadowsRefused);
        return true;
    }

    private void onWriterError(final Throwable t) {
        System.err.println("[REPLICA] shadow writer thread error: " + t);
        if (t instanceof Error) {
            fence("FATAL replica: shadow writer thread failed: " + t);
        }
    }

    private boolean refuse(final String text) {
        System.err.println(text);
        readiness.refused(text);
        halt.fatal(text);
        return false;
    }

    private void halt(final String text) {
        fence.fence(FenceFamily.FAULT, text, false, -1, null);
    }

    // ------------------------------------------------------------------------------------------------ follow

    private void follow() {
        final long first = container.logPosition();
        System.out.println("[REPLICA] following L=" + logRecordingId + " from " + first);
        idle.reset();
        while (running && fence.reason() == null) {
            logView.beginPass();
            readiness.followerTick();
            int work = 0;
            if (image == null) {
                work += tryAttach();
            } else if (!checkCluster()) {
                // every named snapshot is compared, paced; a refusal fences, an unreadable copy is retried
                if (fence.reason() != null) {
                    return;
                }
            }
            if (image != null) {
                final int fragments;
                try {
                    fragments = reader.poll(image, Long.MAX_VALUE);
                } catch (final RuntimeException | Error failed) {
                    if (cfg.off.contains(ReplicaNegativeControl.FENCE_OFF)) {
                        System.err.println("[REPLICA] NC FENCE_OFF: apply failure swallowed at "
                                + container.logPosition() + ": " + failed);
                        final long resumeAt = reader.consumedPosition();
                        closeReplay();
                        reader = new ClusterLogReader(frames, cfg.fragmentLimit, resumeAt);
                        firstPollOfReader = false;
                        continue;
                    }
                    fence("FATAL replica: apply fenced at " + container.logPosition() + ": " + failed);
                    return;
                }
                if (fragments > 0) {
                    work += fragments;
                    if (firstPollOfReader) {
                        firstPollOfReader = false;
                        if (replayCount > 1) {
                            reattachTotal++; // a successful successor poll, not a replay request
                        }
                    }
                    final LogCursor at = container.cursor();
                    applied = at.logPosition();
                    appliedClusterTime = at.clusterTime();
                    armGenesisIfAdopted();
                    policy.observeFrames(at);
                    if (policy.hasPending()) {
                        policy.producePending();
                        if (fence.reason() != null) {
                            return;
                        }
                    }
                    policy.pollHeld(applied);
                    if (fence.reason() != null) {
                        return;
                    }
                } else if (image.isEndOfStream() || image.isClosed()) {
                    closeReplay();
                    nextAttemptNs = clock.nanoTime() + attachBackoffNs;
                }
            }
            policy.idleTick();
            if (policy.hasPending()) {
                policy.producePending();
            }
            policy.pollHeld(applied);
            attachRefusals.tick();
            status();
            idle.idle(work);
        }
    }

    /** Open a replay only when it can carry bytes. */
    private int tryAttach() {
        final long now = clock.nanoTime();
        if (now - nextAttemptNs < 0) {
            return 0;
        }
        nextAttemptNs = now + attachBackoffNs;
        if (!checkCluster()) {
            return 0; // no frame is applied while the named copy is unreadable or refused
        }
        final long l = currentLogRecordingId();
        if (l < 0) {
            readiness.live(false, "awaiting live recording");
            return 0;
        }
        if (l != logRecordingId) {
            // the backup started a NEW recording (its recording log lost L): positions cannot be joined
            fence("FATAL replica: the backup's live log recording changed from L=" + logRecordingId + " to " + l
                    + "; the replica cannot join two recordings (wipe BASE_DIR)");
            return 0;
        }
        final long from = reader == null ? container.logPosition() : reader.consumedPosition();
        final boolean alive = liveCounterId(l) != Aeron.NULL_VALUE;
        if (!alive) {
            final long stop = archive.getStopPosition(l);
            if (stop <= from) {
                readiness.live(false, "awaiting live recording");
                return 0;
            }
        }
        attachAttemptsTotal++;
        final long sessionId;
        try {
            sessionId = archive.startReplay(l, from, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, cfg.replayStreamId);
        } catch (final RuntimeException refusedReplay) {
            attachFailuresTotal++;
            final String text = String.valueOf(refusedReplay);
            attachRefusals.refused(l, from, text);
            readiness.live(false, "replay refused: " + text);
            return 0;
        }
        replaySessionId = sessionId;
        subscription = aeron.addSubscription(ChannelUri.addSessionId(REPLAY_CHANNEL, (int)sessionId),
                cfg.replayStreamId);
        final long deadline = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cfg.loadTimeoutMs);
        while ((image = subscription.imageBySessionId((int)sessionId)) == null) {
            readiness.followerTick();
            if (!running || clock.nanoTime() - deadline >= 0) {
                attachFailuresTotal++;
                System.err.println("[REPLICA] replay image of L=" + l + " from " + from + " not available within"
                        + " SHADOW_LOAD_TIMEOUT_MS=" + cfg.loadTimeoutMs);
                closeReplay();
                return 0;
            }
            sleepQuietly(1);
        }
        replayCount++;
        reader = reader == null ? new ClusterLogReader(frames, cfg.fragmentLimit, from)
                : ClusterLogReader.successor(reader, frames, cfg.fragmentLimit);
        firstPollOfReader = true;
        attachRefusals.attached();
        return 1;
    }

    /**
     * The readiness detail while L's counter is not alive. A stopped L with bytes beyond what the reader
     * consumed is a catch-up replay after a backup reset (not ready by design: replaying, not following).
     */
    static String notAliveDetail(final long stopPosition, final long consumedPosition) {
        return stopPosition >= 0 && stopPosition > consumedPosition ? "catching up (backup reset)"
                : "awaiting live recording";
    }

    /** The orphan recordings found at bootstrap that housekeeping has not purged yet. */
    public java.util.Collection<Long> orphanRecordings() {
        return java.util.Collections.unmodifiableCollection(orphans);
    }

    /** Housekeeping found an orphan above L; true when it is new. */
    boolean addOrphan(final long recordingId) {
        return orphans.add(recordingId);
    }

    /** Housekeeping: {@code recordingId} is purged (or no longer an orphan). */
    void orphanGone(final long recordingId) {
        orphans.remove(recordingId);
    }

    /** At bootstrap, the recordings a {@code cluster/} wipe left behind (housekeeping purges them). */
    private void findOrphans(final long liveLog) {
        final List<Long> found = ClusterIdentityCheck.orphans(archive, cfg.clusterDir(), liveLog,
                cfg.clusterStreams(), false);
        if (!found.isEmpty()) {
            orphans.addAll(found);
            System.err.println("[REPLICA] CRITICAL: " + found.size() + " orphan recording(s) " + found + " on the"
                    + " cluster log and snapshot streams are neither L=" + liveLog + " nor referenced by the backup"
                    + " RecordingLog (a cluster/ wipe left them); housekeeping purges them");
        }
    }

    /**
     * Before the first attach and on every follower pass after it
     * (paced inside {@link GenesisGuard}): the member service snapshot the newest backup response NAMED must hold the
     * genesis of the local shadows; a contradiction refuses (exit 1: {@code FATAL replica: cluster genesis <g2> != local
     * shadows <g1>; wipe BASE_DIR}). The attach to the local L tail no longer waits for this process's
     * first backup event (nothing is compared before a snapshot is named, and an UNVERIFIED replica writes nothing).
     *
     * @return true when the attach may proceed (passed, or nothing to compare yet); false while the named copy does
     *         not decode, or after a refusal
     */
    private boolean checkCluster() {
        final GenesisGuard.Outcome o = genesisGuard.check(localShadowGenesis);
        switch (o) {
            case REFUSED:
                refuseCluster(genesisGuard.refusal());
                return false;
            case UNREADABLE:
                readiness.live(false, genesisGuard.detail());
                return false;
            default:
                return true;
        }
    }

    /**
     * A cluster identity refusal after bootstrap: a history fence (the loop stops, read as refused, own shadows above
     * the last EQUAL position or the restore point purged, awaited, then exit 1). The same family and purge as
     * another genesis seen at the alignment compare, whichever thread sees the new cluster first.
     */
    private void refuseCluster(final String text) {
        final AlignmentTrust t = trust;
        fence.fence(FenceFamily.HISTORY, text, true,
                ReplicaFence.historyPurgeBound(t == null ? -1 : t.verifiedAt(), restoredPosition), null);
    }

    /** The counter recording L right now, by recording id (a recycled counter id never matches), or {@code NULL_VALUE}. */
    private int liveCounterId(final long l) {
        final CountersReader counters = aeron.countersReader();
        final int id = RecordingPos.findCounterIdByRecording(counters, l);
        if (id == CountersReader.NULL_COUNTER_ID || !RecordingPos.isActive(counters, id, l)) {
            return Aeron.NULL_VALUE;
        }
        return id;
    }

    private void status() {
        final long now = clock.nanoTime();
        if (now - nextStatusNs < 0) {
            return;
        }
        nextStatusNs = now + STATUS_PERIOD_NS;
        final long l = logRecordingId;
        final int counterId = liveCounterId(l);
        final boolean alive = counterId != Aeron.NULL_VALUE;
        long rec;
        if (alive) {
            rec = aeron.countersReader().getCounterValue(counterId);
        } else {
            try {
                rec = archive.getStopPosition(l);
            } catch (final RuntimeException unreadable) {
                rec = -1;
            }
        }
        recorded = rec;
        liveAlive = alive;
        if (!genesisGuard.comparedOnce()) {
            final String d = genesisGuard.detail();
            readiness.live(false, d == null ? "genesis check: pending" : d);
        } else {
            readiness.live(alive, alive ? null : notAliveDetail(rec, applied));
        }
        readiness.authRejected(events.authRejectedSinceResponse());
        readiness.positions(applied, rec);
        readiness.appliedClusterTime(appliedClusterTime);
        readiness.diskFull(policy.diskFull());
        final AlignmentTrust t = trust;
        if (t != null) {
            readiness.verified(t.ready(), t.detail()); // an ambiguity reads not ready
        }
    }

    private void armGenesisIfAdopted() {
        if (knownGenesis == 0) {
            final long g = binding.identity().genesisId();
            if (g > 0) {
                ShadowIdentity.requireProducible(g);
                knownGenesis = g;
                System.out.println("[REPLICA] genesis adopted at " + container.logPosition() + ": genesisId=" + g
                        + "; triggers now produce shadows");
            }
        }
    }

    private void closeReplay() {
        image = null;
        CloseHelper.quietClose(subscription);
        subscription = null;
        if (replaySessionId != Aeron.NULL_VALUE) {
            try {
                archive.stopReplay(replaySessionId);
            } catch (final RuntimeException alreadyEnded) {
                // the replay ended with the recording; nothing to stop
            }
            replaySessionId = Aeron.NULL_VALUE;
        }
    }

    // ------------------------------------------------------------------------------------------------ fence

    /** Latch a FAULT fence (any thread): abort the in-flight write, readiness 503, the host's fatal hook. */
    void fence(final String reason) {
        fence(FenceFamily.FAULT, reason);
    }

    /** Latch a fence of {@code family} ({@link ReplicaFence}). */
    void fence(final FenceFamily family, final String reason) {
        final AlignmentTrust t = trust;
        fence.fence(family, reason, false,
                ReplicaFence.historyPurgeBound(t == null ? -1 : t.verifiedAt(), restoredPosition),
                family == FenceFamily.MISMATCH && t != null ? t.mismatchEvidence() : null);
    }

    /** The housekeeping thread's suspect purge, armed by the process after the thread is built. */
    void suspectPurge(final ReplicaFence.SuspectPurge port) {
        fence.suspectPurge(port);
    }

    /** Housekeeping pass: the mismatch hold's own bound. */
    void holdTick() {
        fence.tickHold();
    }

    // ------------------------------------------------------------------------------------------------ close

    /** Stops the reader thread, then the writer thread (aborting an in-flight write), then the clients. */
    @Override
    public void close() {
        running = false;
        final Thread t = thread;
        joinQuietly(t, 2_000);
        // a reader blocked on an ALIGNED wait is released by the writer's abort (the slot frees)
        final ShadowWriterThread wt = writerThread;
        if (wt != null) {
            wt.close(cfg.writeRecordTimeoutMs);
        }
        joinQuietly(t, cfg.loadTimeoutMs + 5_000);
        if (t != null && t.isAlive()) {
            System.err.println("[REPLICA] reader thread did not stop within " + (cfg.loadTimeoutMs + 7_000) + " ms");
        }
        CloseHelper.quietClose(writerArchive);
    }

    private static void joinQuietly(final Thread t, final long ms) {
        if (t == null || t == Thread.currentThread()) {
            return;
        }
        try {
            t.join(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------------------------------------ queries

    public boolean bootstrapDone() {
        return bootstrapDone;
    }

    /**
     * A real genesis compare passed and no newer named member snapshot is unchecked (housekeeping waits for it).
     */
    public boolean clusterChecked() {
        return genesisGuard.checked();
    }

    /** Paced cluster identity evaluations. */
    public long clusterCheckEvaluationsTotal() {
        return genesisGuard.evaluationsTotal();
    }

    /** The alignment trust, null before bootstrap completed. */
    public AlignmentTrust trust() {
        return trust;
    }

    /** The genesis the engine holds (0 before ADOPT on a genesis start); housekeeping waits for it. */
    public long knownGenesis() {
        return knownGenesis;
    }

    public long logRecordingId() {
        return logRecordingId;
    }

    public long logStartPositionAtBootstrap() {
        return logStartPosition;
    }

    public String fenced() {
        return fence.reason();
    }

    /** The latched fence's family, null when not fenced. */
    public FenceFamily fenceFamily() {
        return fence.family();
    }

    /** {@code shadow_oracle_mismatch_latched}: in the oracle mismatch hold. */
    public boolean holding() {
        return fence.holding();
    }

    /** {@code shadow_oracle_hold_ms}: -1 when not holding. */
    public long heldMs() {
        return fence.heldMs();
    }

    /** {@code shadow_oracle_dump_incomplete}. */
    public boolean dumpIncomplete() {
        return fence.dumpIncomplete();
    }

    /** The position of the latched byte mismatch (the retrieved set housekeeping pins), -1 when none. */
    public long mismatchPosition() {
        final AlignmentTrust t = trust;
        return t == null || fence.family() != FenceFamily.MISMATCH ? -1 : t.mismatchAt();
    }

    public long appliedPosition() {
        return applied;
    }

    public long recordedPosition() {
        return recorded;
    }

    public long appliedClusterTime() {
        return appliedClusterTime;
    }

    public boolean liveRecordingAlive() {
        return liveAlive;
    }

    public long reattachTotal() {
        return reattachTotal;
    }

    /** {@code replica_attach_attempts_total}: replays requested. */
    public long attachAttemptsTotal() {
        return attachAttemptsTotal;
    }

    public long attachFailuresTotal() {
        return attachFailuresTotal;
    }

    /**
     * The producer seq this process's writer starts at, -1 before bootstrap. Every copy of this producer with a lower
     * seq predates this process (a TOO_SHORT or NO_TRAILER one is a crashed write nobody owns).
     */
    public long firstProducerSeq() {
        return firstProducerSeq;
    }

    public long restoreAttempts() {
        return restoreAttempts;
    }

    /** The position the engine was restored at (LOCAL_SHADOW) or 0 (GENESIS), -1 before. */
    public long restoredPosition() {
        return restoredPosition;
    }

    public long shadowsRefusedAtBootstrap() {
        return shadowsRefused;
    }

    public ReplicaTermIndex termIndex() {
        return termIndex;
    }

    /** Null before bootstrap completed. */
    public ShadowTriggerPolicy policy() {
        return policy;
    }

    public Thread thread() {
        return thread;
    }

    // ------------------------------------------------------------------------------------------------ frames

    /**
     * The container plus two observers: the test fault seam before each frame, the term index after each accepted
     * term event. Everything else is the container's.
     */
    private final class Frames implements ClusterLogListener {
        @Override
        public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                     final DirectBuffer buffer, final int offset, final int length) {
            cfg.faults.beforeFrame(FrameKind.SESSION_MESSAGE, logPosition);
            container.onSessionMessage(logPosition, clusterSessionId, timestamp, buffer, offset, length);
        }

        @Override
        public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
            cfg.faults.beforeFrame(FrameKind.TIMER_EVENT, logPosition);
            container.onTimerEvent(logPosition, correlationId, timestamp);
        }

        @Override
        public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                  final long timestamp, final int responseStreamId, final String responseChannel,
                                  final byte[] encodedPrincipal) {
            cfg.faults.beforeFrame(FrameKind.SESSION_OPEN, logPosition);
            if (cfg.off.contains(ReplicaNegativeControl.SKIP_SESSION_OPEN)) {
                System.err.println("[REPLICA] NC SKIP_SESSION_OPEN: session open at " + logPosition + " dropped");
                return;
            }
            container.onSessionOpen(leadershipTermId, logPosition, clusterSessionId, timestamp, responseStreamId,
                    responseChannel, encodedPrincipal);
        }

        @Override
        public void onSessionClose(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                   final long timestamp, final CloseReason closeReason) {
            cfg.faults.beforeFrame(FrameKind.SESSION_CLOSE, logPosition);
            container.onSessionClose(leadershipTermId, logPosition, clusterSessionId, timestamp, closeReason);
        }

        @Override
        public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                       final ClusterAction action, final int flags) {
            cfg.faults.beforeFrame(FrameKind.CLUSTER_ACTION, logPosition);
            return container.onClusterAction(leadershipTermId, logPosition, timestamp, action, flags);
        }

        @Override
        public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition,
                                             final long timestamp, final long termBaseLogPosition,
                                             final int leaderMemberId, final int logSessionId,
                                             final TimeUnit timeUnit, final int appVersion) {
            cfg.faults.beforeFrame(FrameKind.NEW_LEADERSHIP_TERM, logPosition);
            container.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, termBaseLogPosition,
                    leaderMemberId, logSessionId, timeUnit, appVersion);
            termIndex.observe(leadershipTermId, termBaseLogPosition);
        }

        @Override
        public void onSkipped(final FrameKind kind, final long logPosition, final long timestamp) {
            container.onSkipped(kind, logPosition, timestamp);
        }
    }
}
