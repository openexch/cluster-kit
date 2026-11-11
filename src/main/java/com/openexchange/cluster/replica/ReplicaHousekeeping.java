// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ProducerKind;
import com.openexchange.cluster.shadow.ShadowAlias;
import com.openexchange.cluster.shadow.ShadowCatalog;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowFloor;
import com.openexchange.cluster.shadow.ShadowHousekeeping;
import com.openexchange.cluster.shadow.ShadowRetention;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NanoClock;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The replica's one housekeeping thread {@code shadow-housekeeping-R<slot>}: shadow retention
 * (the archive's only shadow purger), the log purge of L below the floor, and the retrieved member set trim. It never
 * touches the engine and owns its own archive client.
 *
 * <ul>
 *   <li>{@link ShadowCatalog} with the genesis known after bootstrap (waits for it: a genesis start knows it only
 *       after the ADOPT frame), {@link ShadowRetention} as the purge owner, {@link ShadowHousekeeping} with a no-op refresh.</li>
 *   <li>{@link ShadowFloor} with P_cm := the newest VERIFIED own shadow, a STUB newest image and no
 *       skip-safe candidate, so with {@code previous-verified} the floor is the previous verified shadow's P_s.
 *       Guards: L known, a term entry in the backup RecordingLog, {@code scan.known()}, bootstrap done
 *       and not fenced. The purge point is {@code min(floor, newest retrieved member set)}, segment aligned; with no
 *       retrieved set nothing is purged (as {@code ArchiveHousekeeping.purgeBelow}, AH:177-181). Only the KEPT sets'
 *       recordings are checked to resolve (the trimmed older sets are gone by design), so the replica does not
 *       call {@code ArchiveHousekeeping.purgeBelow}, whose safety net would report every trimmed set as CRITICAL.</li>
 *   <li>{@link RetrievedSetTrim}, and the orphan purge through it.</li>
 * </ul>
 *
 * <p>Fences. While the replica HOLDS an oracle mismatch this thread keeps running: the log purge of L stays frozen
 * (the fence gate, so an offline replay from the previous VERIFIED shadow to the mismatch position stays possible),
 * the retrieved set at the mismatch position is pinned (never trimmed, not counted toward K), retention, the trim of
 * other sets and the orphan purge continue, and each pass ticks the hold's own bound. A history fence hands this
 * thread a one-slot suspect purge ({@link #request}): own shadows above the last EQUAL position (or the restore point,
 * whichever is higher; the ones at or below it are kept) are purged through
 * {@link ShadowRetention#purgeAbove} (counted {@code shadow_poisoned_purged_total}) while the fencing thread waits.</p>
 */
public final class ReplicaHousekeeping implements Runnable, AutoCloseable, ReplicaFence.SuspectPurge {

    private static final long PACE_MS = 200;

    private final ReplicaConfig cfg;
    private final Aeron aeron;
    private final AeronArchive.Context archiveContext;
    private final ReplicaFollower follower;
    private final ReplicaReadiness readiness;
    private final ShadowCounters shadowCounters;
    private final ShadowVerdicts verdicts;
    private final ShadowHousekeeping.Counters counters = new ShadowHousekeeping.Counters();
    private final NanoClock clock;
    private final ReplicaFollower.Halt halt;
    private final RetrievedSetTrim trim;

    private volatile boolean running = true;
    private AeronArchive archive;
    private ShadowCatalog catalog;
    private ShadowFloor floor;
    private ShadowRetention retention;
    private ShadowHousekeeping housekeeping;
    private Thread thread;
    private String lastRefusal;

    private volatile int retrievedSets;
    private volatile long retrievedBytes;
    private volatile long logPurgePosition = -1;
    private volatile long logPurgesTotal;
    private volatile long errorsTotal;
    private volatile long newestVerifiedPosition = -1;
    private volatile long crashedWritesPurgedTotal;
    private long lastPurgeSet = -1;
    private final java.util.Set<Long> sweptRecordings = new java.util.HashSet<>();
    private final java.util.Set<Long> aboveLSeen = new java.util.HashSet<>();
    private volatile LongSupplier backupState = () -> -1;
    private final AtomicReference<SuspectRequest> suspectRequest = new AtomicReference<>();
    private volatile long poisonedPurgedTotal;
    private long pinnedLogged = -1;

    public ReplicaHousekeeping(final ReplicaConfig cfg, final Aeron aeron, final AeronArchive.Context archiveContext,
                               final ReplicaFollower follower, final ReplicaReadiness readiness,
                               final ShadowCounters shadowCounters, final ShadowVerdicts verdicts,
                               final NanoClock clock, final ReplicaFollower.Halt halt) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.archiveContext = Objects.requireNonNull(archiveContext, "archiveContext");
        this.follower = Objects.requireNonNull(follower, "follower");
        this.readiness = Objects.requireNonNull(readiness, "readiness");
        this.shadowCounters = Objects.requireNonNull(shadowCounters, "shadowCounters");
        this.verdicts = Objects.requireNonNull(verdicts, "verdicts");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.halt = Objects.requireNonNull(halt, "halt");
        this.trim = new RetrievedSetTrim(cfg.retrievedSetsKeep);
    }

    public void start() {
        if (thread != null) {
            throw new IllegalStateException("housekeeping already started");
        }
        thread = new Thread(this, "shadow-housekeeping-R" + cfg.slot);
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void run() {
        while (running) {
            readiness.housekeepingTick();
            follower.holdTick();
            try {
                serveSuspectPurge();
                pass();
            } catch (final RuntimeException failed) {
                errorsTotal++;
                System.err.println("[REPLICA-HK] housekeeping pass failed (archive client reconnected next pass): "
                        + failed);
                CloseHelper.quietClose(retention);
                retention = null;
                housekeeping = null;
                CloseHelper.quietClose(archive);
                archive = null;
            } catch (final Error fatal) {
                halt.fatal("FATAL replica: housekeeping thread failed: " + fatal);
                return;
            }
            sleep(PACE_MS);
        }
    }

    /** One pass; package-visible for tests that drive it without the thread. */
    void pass() {
        if (archive == null) {
            archive = AeronArchive.connect(archiveContext.clone().aeron(aeron).ownsAeronClient(false));
        }
        if (housekeeping == null && !build()) {
            return;
        }
        housekeeping.poll();
        final ShadowCatalog.Scan scan = housekeeping.scan();
        sweepCrashedWrites(scan);
        newestVerifiedPosition = newestVerified(scan);
        final String gate = purgeGate(follower.bootstrapDone(), follower.clusterChecked(), follower.fenced());
        if (gate != null) {
            refusedOnce("[REPLICA-HK] the log purge and the orphan purge wait: " + gate);
            if (follower.holding()) {
                // the hold freezes the log purge only: orphans are not an input of the disagreement
                findOrphansAboveL();
                purgeOrphans();
            }
        } else {
            purgeLog(scan);
            findOrphansAboveL();
            purgeOrphans();
        }
        final RetrievedSetTrim.Pass t = trim.apply(cfg.clusterDir(), archive,
                !cfg.off.contains(ReplicaNegativeControl.SET_TRIM_OFF), pinned());
        retrievedSets = t.setsPresent();
        retrievedBytes = t.bytesPresent();
    }

    private boolean build() {
        final long genesis = follower.knownGenesis();
        if (genesis <= 0) {
            refusedOnce("[REPLICA-HK] no genesis known yet (bootstrap, or a genesis start before the ADOPT frame): retention and"
                    + " the log purge wait");
            return false;
        }
        final long timeoutNs = TimeUnit.MILLISECONDS.toNanos(cfg.loadTimeoutMs);
        catalog = new ShadowCatalog(aeron, archive, cfg.engineId, cfg.clusterId, genesis, verdicts,
                ShadowCatalog.DEFAULT_LIST_BATCH, clock, timeoutNs, shadowCounters);
        final ShadowFloor[] holder = new ShadowFloor[1];
        final LongSupplier newestVerified = () -> newestVerified(holder[0] == null ? null : holder[0].scan());
        floor = new ShadowFloor(newestVerified, () -> ShadowFloor.NewestImage.STUB, header -> false,
                cfg.floorMargin, this::logStart, counters);
        holder[0] = floor;
        try {
            retention = new ShadowRetention(catalog, floor, cfg.retainN, counters);
        } catch (final IllegalStateException secondOwner) {
            halt.fatal("FATAL replica: " + secondOwner.getMessage());
            running = false;
            return false;
        }
        housekeeping = new ShadowHousekeeping(catalog, retention, floor, clock, cfg.scanPeriodMs, cfg.deepReadPaceMs,
                () -> { }, counters);
        System.out.println("[REPLICA-HK] armed: archive=" + catalog.archiveId() + " genesisId=" + genesis
                + " retainN=" + cfg.retainN + " margin=" + cfg.floorMargin.text() + " retrievedSetsKeep="
                + cfg.retrievedSetsKeep + " scanPeriodMs=" + cfg.scanPeriodMs);
        return true;
    }

    /**
     * A {@code kill -9} in the middle of a write leaves a TOO_SHORT or NO_TRAILER recording of this
     * replica's producer that no writer will clean up (the writer purges only its own aborted writes). The
     * retention owner purges such copies when their producer seq is below the seq this process's writer started at
     * (so it predates this process and no write of this process can be in flight on it), on a scan that can answer.
     * Never another producer's (evidence for its operator), never a usable copy.
     */
    private void sweepCrashedWrites(final ShadowCatalog.Scan scan) {
        final long firstSeq = follower.firstProducerSeq();
        if (scan == null || !scan.known() || firstSeq <= 0 || retention == null) {
            return;
        }
        for (final ShadowEntry e : scan.all()) {
            if (e.state() != ShadowEntry.State.TOO_SHORT && e.state() != ShadowEntry.State.NO_TRAILER) {
                continue;
            }
            final ShadowAlias a = e.alias();
            if (sweptRecordings.contains(e.recordingId()) || a == null
                    || a.producerKind() != ProducerKind.REPLICA.code() || a.producerId() != cfg.slot) {
                continue;
            }
            final long seq = e.header() != null ? e.header().producerSeq() : a.producerSeq();
            if (seq >= firstSeq) {
                continue;
            }
            final ShadowCatalog.PurgeResult r = retention.purge(e.recordingId(), false);
            if (r.purged()) {
                sweptRecordings.add(e.recordingId()); // the cached scan may list it until the next re-scan
                crashedWritesPurgedTotal++;
                System.out.println("[REPLICA-HK] purged a crashed write of R" + cfg.slot + " from before this"
                        + " process: rec=" + e.recordingId() + " seq=" + seq + " state=" + e.state() + " P="
                        + a.logPosition());
            } else {
                refusedOnce("[REPLICA-HK] crashed write rec=" + e.recordingId() + " of R" + cfg.slot
                        + " not purged: " + r.detail() + " (code " + r.code() + ", retried next pass)");
            }
        }
    }

    /** The retrieved set an oracle mismatch disagreed with: never trimmed while the replica holds it. */
    private Set<Long> pinned() {
        final long p = follower.mismatchPosition();
        if (p < 0 || cfg.off.contains(ReplicaNegativeControl.MISMATCH_PIN_OFF)) {
            return Set.of();
        }
        if (pinnedLogged != p) {
            pinnedLogged = p;
            System.err.println("[REPLICA-HK] CRITICAL: the retrieved member set at " + p + " is pinned (the oracle"
                    + " disagreed with it); the log purge is frozen at " + logPurgePosition);
        }
        return Set.of(p);
    }

    /** The bootstrap's orphans, once bootstrap is done and the follower is not fenced (or holds a mismatch). */
    private void purgeOrphans() {
        final java.util.Collection<Long> candidates = follower.orphanRecordings();
        if (candidates.isEmpty() || !follower.holding() && purgeGate(follower.bootstrapDone(),
                follower.clusterChecked(), follower.fenced()) != null) {
            return;
        }
        for (final long id : trim.purgeOrphans(cfg.clusterDir(), archive, follower.logRecordingId(), candidates,
                cfg.clusterStreams())) {
            follower.orphanGone(id);
        }
    }

    /**
     * Null when the log purge and the orphan purge may run, else why not: bootstrap done, the cluster check passed,
     * not fenced.
     */
    static String purgeGate(final boolean bootstrapDone, final boolean clusterChecked, final String fenced) {
        if (!bootstrapDone) {
            return "bootstrap not done";
        }
        if (fenced != null) {
            return "fenced";
        }
        if (!clusterChecked) {
            return "the cluster check has not passed";
        }
        return null;
    }

    /**
     * Recordings above L on the cluster streams that the RecordingLog does not reference and that
     * are stopped (sets a failed backup cycle retrieved and never recorded) become orphans after two consecutive
     * passes that both saw them with the backup BACKING_UP: an in-flight retrieval is either still recording or
     * referenced once its cycle reaches BACKING_UP. The purge re-checks each id first.
     */
    private void findOrphansAboveL() {
        final long l = follower.logRecordingId();
        if (l < 0) {
            return;
        }
        if (backupState.getAsLong() != ReplicaReadiness.BACKING_UP) {
            aboveLSeen.clear();
            return;
        }
        final java.util.List<Long> now = ClusterIdentityCheck.orphans(archive, cfg.clusterDir(), l,
                cfg.clusterStreams(), true);
        for (final long id : now) {
            if (aboveLSeen.contains(id) && follower.addOrphan(id)) {
                System.err.println("[REPLICA-HK] CRITICAL: orphan recording rec=" + id + " above L=" + l + " on a"
                        + " cluster stream, stopped and not referenced by the backup RecordingLog in two passes with the"
                        + " backup BACKING_UP (a failed backup cycle left it); purged");
            }
        }
        aboveLSeen.clear();
        aboveLSeen.addAll(now);
    }

    // ------------------------------------------------------------------------------------------------ suspect purge

    /** One history fence's request; completed by this thread. */
    private static final class SuspectRequest implements ReplicaFence.Pending {
        final long above;
        final String reason;
        final CountDownLatch done = new CountDownLatch(1);
        volatile int suspects = -1;
        volatile int purged = -1;

        SuspectRequest(final long above, final String reason) {
            this.above = above;
            this.reason = reason;
        }

        @Override
        public int await(final long timeoutMs) throws InterruptedException {
            return done.await(timeoutMs, TimeUnit.MILLISECONDS) ? purged : -1;
        }

        @Override
        public int suspects() {
            return suspects;
        }

        void complete(final int n) {
            purged = n;
            done.countDown();
        }
    }

    /** A history fence asks for the suspect purge; served at the start of this thread's next pass. */
    @Override
    public ReplicaFence.Pending request(final long above, final String reason) {
        final SuspectRequest r = new SuspectRequest(above, reason);
        suspectRequest.set(r);
        return r;
    }

    private void serveSuspectPurge() {
        final SuspectRequest r = suspectRequest.getAndSet(null);
        if (r == null) {
            return;
        }
        try {
            if (archive == null) {
                archive = AeronArchive.connect(archiveContext.clone().aeron(aeron).ownsAeronClient(false));
            }
            if (retention == null && !build()) {
                System.err.println("[REPLICA-HK] history fence: no genesis known, no shadow can exist: nothing to"
                        + " purge above " + r.above);
                r.suspects = 0;
                r.complete(0);
                return;
            }
            final List<com.openexchange.cluster.shadow.ShadowCatalog.PurgeResult> results = retention.purgeAbove(
                    r.above, ProducerKind.REPLICA.code(), cfg.slot, r.reason, n -> r.suspects = n);
            final StringBuilder line = new StringBuilder();
            int purged = 0;
            for (final com.openexchange.cluster.shadow.ShadowCatalog.PurgeResult result : results) {
                if (result.purged()) {
                    purged++;
                    poisonedPurgedTotal++;
                    final ShadowEntry e = result.target();
                    final com.openexchange.cluster.shadow.ShadowHeader h = e == null ? null : e.header();
                    line.append(line.length() == 0 ? "" : " ")
                            .append(h != null ? h.logPosition() : e == null || e.alias() == null ? -1
                                    : e.alias().logPosition())
                            .append(':').append(h != null ? h.kindEnum() : "UNREADABLE_HEADER");
                }
            }
            System.err.println("[REPLICA-HK] CRITICAL: poisoned shadows purged: [" + line + "] (above " + r.above
                    + ", the last EQUAL position or the restore point; kept at or below " + r.above + "; "
                    + (results.size() - purged) + " refused)");
            r.complete(purged);
        } catch (final RuntimeException failed) {
            System.err.println("[REPLICA-HK] CRITICAL: history fence: the suspect purge failed: " + failed);
            throw failed; // never completed: the fencing thread times out with the wipe line
        }
    }

    /** {@code shadow_poisoned_purged_total}: shadows the history fence's suspect purge removed. */
    public long poisonedPurgedTotal() {
        return poisonedPurgedTotal;
    }

    /** The backup state counter ({@code ClusterBackup.State} code), read by the above-L orphan rule. */
    public void backupState(final LongSupplier state) {
        this.backupState = Objects.requireNonNull(state, "state");
    }

    private static long newestVerified(final ShadowCatalog.Scan scan) {
        long newest = -1;
        if (scan != null) {
            for (final LogicalShadow v : scan.verified()) {
                newest = Math.max(newest, v.logPosition());
            }
        }
        return newest;
    }

    private long logStart() {
        final long l = follower.logRecordingId();
        if (l < 0 || archive == null) {
            return -1;
        }
        final long[] d = {-1};
        archive.listRecording(l, (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, startPosition,
                stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                strippedChannel, originalChannel, sourceIdentity) -> d[0] = startPosition);
        return d[0];
    }

    /** The log purge, guarded; never above the floor. */
    private void purgeLog(final ShadowCatalog.Scan scan) {
        // the floor requests a pass when a VERIFIED shadow appears; the purge point is also bounded by the newest
        // retrieved member set, which the backup fetches at its NEXT query, so a pass also runs when a newer set
        // arrives (otherwise L would reach the floor one member snapshot late)
        final TreeMap<Long, List<RecordingLog.Entry>> setsNow = RetrievedSetTrim.sets(cfg.clusterDir());
        final long newestSetNow = setsNow.isEmpty() ? -1 : setsNow.firstKey();
        if (!floor.purgeRequested() && newestSetNow == lastPurgeSet) {
            return;
        }
        final long l = follower.logRecordingId();
        if (l < 0 || scan == null || !scan.known() || purgeGate(follower.bootstrapDone(), follower.clusterChecked(),
                follower.fenced()) != null) {
            return;
        }
        if (!hasTermEntry(cfg.clusterDir(), l)) {
            refusedOnce("[REPLICA-HK] the backup RecordingLog has no term entry for L=" + l + " yet: no log purge");
            return;
        }
        long target = floor.getAsLong(); // consumes the request
        lastPurgeSet = newestSetNow;
        if (cfg.off.contains(ReplicaNegativeControl.FLOOR_MARGIN_IGNORED)) {
            target = newestVerifiedPosition;
            System.err.println("[REPLICA-HK] NC FLOOR_MARGIN_IGNORED: purging to the newest verified shadow " + target);
        }
        if (target <= 0) {
            return;
        }
        final TreeMap<Long, List<RecordingLog.Entry>> sets = setsNow;
        if (sets.isEmpty()) {
            refusedOnce("[REPLICA-HK] no retrieved member snapshot set yet: the log of L is not purged (AH:177-181)");
            return;
        }
        final long newestSet = sets.firstKey();
        for (final RecordingLog.Entry e : sets.firstEntry().getValue()) {
            if (archive.listRecording(e.recordingId, (a, b, c, x, f, g, h, i, j, k, m, n, o, p, q, r) -> { }) == 0) {
                System.err.println("[REPLICA-HK] CRITICAL: the newest retrieved member set at " + newestSet
                        + " references recording " + e.recordingId + " (serviceId=" + e.serviceId
                        + ") that is NOT in the archive: the oracle input is incomplete");
            }
        }
        final long purgePoint = Math.min(target, newestSet);
        final long[] d = new long[3];
        if (archive.listRecording(l, (controlSessionId, correlationId, id, startTimestamp, stopTimestamp,
                startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            d[0] = startPosition;
            d[1] = segmentFileLength;
            d[2] = termBufferLength;
        }) == 0) {
            refusedOnce("[REPLICA-HK] L=" + l + " is not in the archive: no log purge");
            return;
        }
        final long start = d[0];
        final long newStart = AeronArchive.segmentFileBasePosition(start, purgePoint, (int)d[2], (int)d[1]);
        if (newStart > start) {
            archive.purgeSegments(l, newStart);
            logPurgesTotal++;
            System.out.println("[REPLICA-HK] purged L=" + l + " segments " + start + " -> " + newStart + " (floor "
                    + target + ", newest retrieved set " + newestSet + ")");
        }
        logPurgePosition = Math.max(start, newStart);
    }

    static boolean hasTermEntry(final File clusterDir, final long recordingId) {
        if (!new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            return false;
        }
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            return log.findLastTermRecordingId() == recordingId;
        }
    }

    private void refusedOnce(final String line) {
        if (!line.equals(lastRefusal)) {
            lastRefusal = line;
            System.out.println(line);
        }
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        final Thread t = thread;
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(10_000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        CloseHelper.quietClose(retention);
        CloseHelper.quietClose(archive);
    }

    // ------------------------------------------------------------------------------------------------ queries

    public ShadowHousekeeping.Counters counters() {
        return counters;
    }

    /** Null before the genesis is known. */
    public ShadowHousekeeping housekeeping() {
        return housekeeping;
    }

    public int retrievedSets() {
        return retrievedSets;
    }

    public long retrievedBytes() {
        return retrievedBytes;
    }

    public long retrievedSetsPurgedTotal() {
        return trim.setsPurgedTotal();
    }

    /** {@code shadow_log_purge_position}: L's start after the last purge pass, -1 before. */
    public long logPurgePosition() {
        return logPurgePosition;
    }

    public long logPurgesTotal() {
        return logPurgesTotal;
    }

    /** {@code shadow_replica_crashed_writes_purged_total}. */
    public long crashedWritesPurgedTotal() {
        return crashedWritesPurgedTotal;
    }

    public long errorsTotal() {
        return errorsTotal;
    }

    public Thread thread() {
        return thread;
    }
}
