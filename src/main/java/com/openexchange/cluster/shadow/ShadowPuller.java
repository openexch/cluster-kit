// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.DeepResult;
import com.openexchange.cluster.shadow.ShadowCatalog.PurgeResult;
import com.openexchange.cluster.shadow.ShadowEntry.State;
import com.openexchange.cluster.shadow.ShadowPullSettings.Endpoint;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ReplicationParams;
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NanoClock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.function.LongSupplier;

/**
 * The shadow puller: copies shadows another archive holds into this node's archive, verified, through the archive
 * control channel only (no log entry, no coordination message, no remote replay, never another process's endpoint,
 * disk or env). One class for the member and the replica.
 *
 * <h2>Discovery</h2>
 * <p>Every {@code SHADOW_PULL_PERIOD_MS} each source of the ordered allowlist {@code SHADOW_PULL_SOURCES} is listed
 * over its archive control channel ({@code listRecordingsForUri(alias=oxs.v1.<engine>., 4301)}, a remote client per
 * source, connected asynchronously and reconnected after a failure). A descriptor is a candidate iff its alias parses;
 * it is stopped (never ACTIVE); its length holds a header and a trailer; engine and cluster are the host's; its genesis
 * is the host's known genesis when there is one; its kind is in {@code SHADOW_PULL_KINDS} (default ALIGNED); its FULL
 * alias (position included) is not held by a local copy in a usable state (evidence states and partial copies never
 * hide a source); it is not in this process's content-refused set; its position is above the newest VERIFIED local
 * position (steady state) or accepted by the {@link Target} (a startup pull). One exception in steady state: an ALIGNED
 * at a position this node holds usable ALIGNED copies of, all from ONE producer, is a candidate when its producer is
 * another one (the second witness the two-producer anchor needs; once two producers are held there, nothing more is
 * pulled at that position). Newest position first, then source order. The alias is a locator: the local header
 * decides after the pull.</p>
 *
 * <h2>Pull and completion</h2>
 * <p>At most {@code SHADOW_PULL_MAX_INFLIGHT} replications ({@code replicate} with {@link ReplicationParams}: the
 * replication channel from {@code SHADOW_PULL_REPLICATION_ENDPOINT}, the source's response channel from
 * {@code SHADOW_PULL_RESPONSE_ENDPOINT}, each only when set). A copy is COMPLETE when the archive signalled SYNC for the
 * replication, or when it ended with the local length equal to the source length recorded at discovery; then the local
 * catalog classifies it and the deep read verifies it (shared {@link ShadowVerdicts}). REPLICATE_END without either is
 * INCOMPLETE (the source crashed, its driver went away, its replay was stopped): the partial copy is purged and the
 * candidate retried. Only a COMPLETE copy can be refused for its content.</p>
 *
 * <h2>Refusal classes (never relabelled into each other)</h2>
 * <pre>
 * archive     an error response: GENERIC with the source's text (Aeron answers every source-side refusal so), or a
 *             destination code (MAX_RECORDINGS, REPLICATION_CONNECTION_FAILURE, STORAGE_SPACE): purge, retry
 * INCOMPLETE  the copy ended short of the source without an error: purge, retry
 * TIMEOUT     the replication outlived SHADOW_PULL_TIMEOUT_MS (or a startup pull's deadline): stop, purge, retry
 * source      the source did not answer its listing: skipped this pass, reconnected
 * content     a COMPLETE copy that is not COMPLETE locally or whose deep read is CORRUPT: purged, the SOURCE
 *             recording refused for the life of the process (never re-pulled, never pinned), CRITICAL
 * conflict    a COMPLETE copy that disagrees with a local copy at its position: kept (an operator resolves it), CRITICAL
 * </pre>
 * <p>Retries wait {@code SHADOW_PULL_RETRY_BACKOFF_MS}, doubling to 8x per candidate, reset on success. The puller
 * purges only recordings it created in this process; an archive refusal of that purge is retried next pass and never
 * blocks discovery.</p>
 *
 * <h2>The leader never pulls</h2>
 * <p>While {@code leaderNow} is true a pass lists nothing and starts nothing ({@code shadow_pull_skipped_leader_total});
 * every in-flight replication is stopped and its partial copy purged ({@code shadow_pull_aborted_leader_total}). The
 * aborted candidate is pulled again after the role changed (its alias is not held by a usable local copy).</p>
 *
 * <h2>Nobody pulls from the leader</h2>
 * <p>A member passes {@link #deferredSource} the current leader's archive control endpoint: a discovery pass skips
 * that source ({@code shadow_pull_source_skipped_leader_total}) and lists it again once the leader changed.</p>
 *
 * <p>Threads: one owner thread per instance ({@link #start} or the caller of {@link #pullOnce}); the counters are
 * atomic for the host's exporter. Never opens a {@code RecordingLog}, never launches a driver, never schedules a cluster
 * timer.</p>
 */
public final class ShadowPuller implements AutoCloseable {

    /** The response stream of the listing clients (distinct from the archive clients' 20). */
    public static final int LISTING_RESPONSE_STREAM_ID = 4304;
    /** The cap of the backoff doubling. */
    public static final int BACKOFF_MAX_FACTOR = 8;

    /** A startup pull's choice: a rank per candidate alias, lower first, negative = not a target. */
    @FunctionalInterface
    public interface Target {
        int rank(ShadowAlias alias);
    }

    /** One pullable source recording. */
    public record Candidate(Endpoint source, int sourceOrder, long srcRecordingId, ShadowAlias alias, long length) {
        String key() {
            return source.text() + "#" + srcRecordingId;
        }

        @Override
        public String toString() {
            return alias.format() + " rec=" + srcRecordingId + " from " + source.text() + " (" + length + " B)";
        }
    }

    /** What {@link #pullOnce} found. */
    public record Report(Kind kind, int sources, int candidates, int refused, String last, Set<Long> geneses,
                         ShadowEntry pulled, Endpoint pulledFrom) {

        public enum Kind {
            /** A copy was pulled and VERIFIED; {@link #pulled} is the local copy. */
            VERIFIED,
            /** Nothing pulled before the deadline. */
            NONE,
            /** No source configured. */
            DISABLED,
            /** The candidates carry two or more geneses (genesis unknown): nothing pulled. */
            GENESES
        }

        public Report {
            geneses = Set.copyOf(geneses);
        }

        /** {@code tried <n> sources, <c> candidates, refused <r>, last: <reason>} or {@code disabled}. */
        public String text() {
            return kind == Kind.DISABLED ? "disabled"
                    : "tried " + sources + " sources, " + candidates + " candidates, refused " + refused + ", last: "
                    + last;
        }
    }

    /** The puller counters, exported by the host. */
    public enum Counter {
        PULL_TOTAL("shadow_pull_total"),
        PULL_BYTES_TOTAL("shadow_pull_bytes_total"),
        RETRIES_TOTAL("shadow_pull_retries_total"),
        STARTED_TOTAL("shadow_pull_started_total"),
        SOURCE_UNREACHABLE_TOTAL("shadow_pull_source_unreachable_total"),
        SOURCE_RECONNECTS_TOTAL("shadow_pull_source_reconnects_total"),
        SKIPPED_LEADER_TOTAL("shadow_pull_skipped_leader_total"),
        SOURCE_SKIPPED_LEADER_TOTAL("shadow_pull_source_skipped_leader_total"),
        ABORTED_LEADER_TOTAL("shadow_pull_aborted_leader_total"),
        INFLIGHT("shadow_pull_inflight"),
        PURGE_PENDING("shadow_pull_purge_pending"),
        PASSES_TOTAL("shadow_pull_passes_total"),
        ERRORS_TOTAL("shadow_pull_errors_total");

        private final String metricName;

        Counter(final String metricName) {
            this.metricName = metricName;
        }

        public String metricName() {
            return metricName;
        }
    }

    /** Atomic counters: written on the owner thread, read by the exporter. */
    public static final class Counters {
        private final AtomicLongArray values = new AtomicLongArray(Counter.values().length);
        private final Map<String, Long> refused = new ConcurrentHashMap<>();

        public long get(final Counter c) {
            return values.get(c.ordinal());
        }

        /** {@code shadow_pull_refused_total{reason}}. */
        public long refused(final String reason) {
            return refused.getOrDefault(reason, 0L);
        }

        void increment(final Counter c) {
            values.incrementAndGet(c.ordinal());
        }

        void add(final Counter c, final long v) {
            values.addAndGet(c.ordinal(), v);
        }

        void set(final Counter c, final long v) {
            values.set(c.ordinal(), v);
        }

        void incrementRefused(final String reason) {
            refused.merge(reason, 1L, Long::sum);
        }

        /** {@code reason -> count} of {@code shadow_pull_refused_total}, sorted. */
        public Map<String, Long> refusedByReason() {
            return new TreeMap<>(refused);
        }

        public Map<String, Long> snapshot() {
            final Map<String, Long> out = new TreeMap<>();
            for (final Counter c : Counter.values()) {
                out.put(c.metricName(), get(c));
            }
            refused.forEach((r, n) -> out.put("shadow_pull_refused_total{reason=" + r + "}", n));
            return out;
        }
    }

    /** One replication this process started. */
    private static final class Inflight {
        final Candidate candidate;
        final long replicationId;
        final long startedNs;
        final long deadlineNs;
        final List<String> signals = new ArrayList<>();
        long dst = -1;
        boolean sync;
        boolean ended;
        int errorCode = -1;
        String error;
        boolean verifying;

        Inflight(final Candidate candidate, final long replicationId, final long startedNs, final long deadlineNs) {
            this.candidate = candidate;
            this.replicationId = replicationId;
            this.startedNs = startedNs;
            this.deadlineNs = deadlineNs;
        }
    }

    private static final class Backoff {
        int failures;
        long nextNs;
    }

    private static final class Remote {
        AeronArchive.AsyncConnect connecting;
        AeronArchive client;
        boolean everConnected;
    }

    private final Aeron aeron;
    private final AeronArchive.Context localContext;
    private final int engineId;
    private final int clusterId;
    private final LongSupplier knownGenesis;
    private final BooleanSupplier leaderNow;
    private final String responseHost;
    private final ShadowPullSettings settings;
    private final ShadowVerdicts verdicts;
    private final ShadowCounters shadowCounters;
    private final NanoClock clock;
    private final long replayTimeoutNs;
    private final String threadName;
    private final Counters counters = new Counters();
    private final Set<NegativeControl> off;
    private final String prefix;

    private final Map<Long, Inflight> inflight = new LinkedHashMap<>();
    private final Map<Long, List<Object[]>> early = new HashMap<>();
    private final Map<String, Remote> remotes = new HashMap<>();
    private final Set<String> contentRefused = new HashSet<>();
    private final Set<String> conflictKept = new HashSet<>();
    private final Map<String, Backoff> backoff = new HashMap<>();
    private final Set<Long> pulledThisProcess = new HashSet<>();
    private final Set<Long> purgePending = new TreeSet<>();

    private AeronArchive control;
    private AeronArchive catalogClient;
    private ShadowCatalog catalog;
    private long catalogGenesis = -1;
    private long nextDiscoveryNs;
    private boolean wasLeader;
    private volatile boolean running = true;
    private Thread thread;

    // the stats of the pullOnce in progress (or of the steady state)
    private Target target;
    private final Set<String> seenCandidates = new HashSet<>();
    private final Set<Long> seenGeneses = new TreeSet<>();
    private int refusedThisCall;
    private String lastReason = "no candidate";
    private ShadowEntry lastPulled;
    private Endpoint lastPulledFrom;
    private volatile String lastRefusal;
    private volatile Supplier<String> deferredSource = () -> null;
    private String deferredLogged;

    /**
     * @param aeron           the host's Aeron client (driver loss reaches the host's error handler)
     * @param localContext    a context of this node's local archive client (cloned; the puller connects its own
     *                        clients with it)
     * @param knownGenesis    the host's genesis, 0 while unknown (a replica bootstrap): candidates are then not
     *                        filtered by genesis
     * @param leaderNow       the service's role: true while LEADER (a replica passes {@code () -> false})
     * @param responseHost    this node's host, for the listing clients' response channel when
     *                        {@code SHADOW_PULL_RESPONSE_ENDPOINT} is empty ({@code <host>:0})
     * @param replayTimeoutNs the bound of one local structural or deep read
     */
    public ShadowPuller(final Aeron aeron, final AeronArchive.Context localContext, final int engineId,
                        final int clusterId, final LongSupplier knownGenesis, final BooleanSupplier leaderNow,
                        final String responseHost, final ShadowPullSettings settings, final ShadowVerdicts verdicts,
                        final ShadowCounters shadowCounters, final NanoClock clock, final long replayTimeoutNs,
                        final String threadName) {
        this(aeron, localContext, engineId, clusterId, knownGenesis, leaderNow, responseHost, settings, verdicts,
                shadowCounters, clock, replayTimeoutNs, threadName, EnumSet.noneOf(NegativeControl.class));
    }

    ShadowPuller(final Aeron aeron, final AeronArchive.Context localContext, final int engineId, final int clusterId,
                 final LongSupplier knownGenesis, final BooleanSupplier leaderNow, final String responseHost,
                 final ShadowPullSettings settings, final ShadowVerdicts verdicts, final ShadowCounters shadowCounters,
                 final NanoClock clock, final long replayTimeoutNs, final String threadName,
                 final Set<NegativeControl> off) {
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.localContext = Objects.requireNonNull(localContext, "localContext");
        this.engineId = engineId;
        if (clusterId < 0) {
            throw new IllegalArgumentException("clusterId must be >= 0: " + clusterId);
        }
        this.clusterId = clusterId;
        this.knownGenesis = Objects.requireNonNull(knownGenesis, "knownGenesis");
        this.leaderNow = Objects.requireNonNull(leaderNow, "leaderNow");
        this.responseHost = Objects.requireNonNull(responseHost, "responseHost");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.verdicts = Objects.requireNonNull(verdicts, "verdicts");
        this.shadowCounters = Objects.requireNonNull(shadowCounters, "shadowCounters");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (replayTimeoutNs <= 0) {
            throw new IllegalArgumentException("replayTimeoutNs must be > 0: " + replayTimeoutNs);
        }
        this.replayTimeoutNs = replayTimeoutNs;
        this.threadName = Objects.requireNonNull(threadName, "threadName");
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
        this.prefix = ShadowAlias.prefix(engineId);
        for (final NegativeControl c : this.off) {
            System.err.println("[SHADOW-PULL] NC " + c + ": a pull rule is switched off (test only)");
        }
    }

    public Counters counters() {
        return counters;
    }

    public ShadowPullSettings settings() {
        return settings;
    }

    /**
     * The source the puller must not list or pull from this pass: a member passes the current leader's archive
     * control endpoint, spelled as its members table spells it (a peer replicating from the leader's archive opens a
     * replay there, disk and network on the commit path), or null while the leader is unknown or is not a source.
     * Compared to each source's {@code host:port} text once per discovery pass; a source skipped this way is counted
     * ({@code shadow_pull_source_skipped_leader_total}) and listed again once the supplier names another endpoint.
     * Default: nothing deferred (the replica). Set before {@link #start}.
     */
    public ShadowPuller deferredSource(final Supplier<String> leaderArchiveEndpoint) {
        this.deferredSource = Objects.requireNonNull(leaderArchiveEndpoint, "leaderArchiveEndpoint");
        return this;
    }

    /** The source recordings refused for their content in this process ({@code host:port#recordingId}). */
    public Set<String> contentRefused() {
        return Set.copyOf(contentRefused);
    }

    /** The source recordings whose COMPLETE copy was kept as a CONFLICT ({@code host:port#recordingId}). */
    public Set<String> conflictKept() {
        return Set.copyOf(conflictKept);
    }

    /** The last refusal as logged ({@code <reason>: <text>}), null before the first. */
    public String lastRefusal() {
        return lastRefusal;
    }

    /** The local recordings this process created by a pull (purged ones included). */
    public Set<Long> pulledThisProcess() {
        return Set.copyOf(pulledThisProcess);
    }

    // ------------------------------------------------------------------------------------------------ lifecycle

    /** Starts {@code <threadName>}: a pass every 10th of the period (polls signals), discovery every period. */
    public void start() {
        if (thread != null) {
            throw new IllegalStateException("puller already started");
        }
        if (!settings.enabled()) {
            System.err.println("[SHADOW-PULL] CRITICAL: " + ShadowPullSettings.DISABLED_LINE);
            return;
        }
        thread = new Thread(this::loop, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        final long sleepMs = Math.max(1, Math.min(100, settings.periodMs() / 10));
        while (running) {
            try {
                pass(null);
            } catch (final RuntimeException failed) {
                counters.increment(Counter.ERRORS_TOTAL);
                System.err.println("[SHADOW-PULL] pass failed: " + failed);
                closeClients();
            }
            sleep(sleepMs);
        }
        stopOwned(); // the owner thread aborts its own in-flight pulls: nothing else touches them
    }

    /**
     * Stops the thread, stops every in-flight replication and purges its partial copy, closes the clients. With a
     * thread, the thread does that itself when it leaves its loop; a thread that does not stop within
     * {@code period + 5 s} is left to do it (logged), never raced from the caller.
     */
    @Override
    public void close() {
        running = false;
        final Thread t = thread;
        if (t == null || t == Thread.currentThread()) {
            stopOwned();
            return;
        }
        try {
            t.join(settings.periodMs() + 5_000);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            System.err.println("[SHADOW-PULL] " + threadName + " did not stop within " + (settings.periodMs() + 5_000)
                    + " ms; it aborts its in-flight pulls when its pass returns");
        }
    }

    private void stopOwned() {
        try {
            abortAll("closing");
        } catch (final RuntimeException ignored) {
            // the archive may be gone already
        }
        closeClients();
    }

    private void closeClients() {
        for (final Remote r : remotes.values()) {
            CloseHelper.quietCloseAll(r.connecting, r.client);
        }
        remotes.clear();
        CloseHelper.quietCloseAll(control, catalogClient);
        control = null;
        catalogClient = null;
        catalog = null;
        catalogGenesis = -1;
    }

    // ------------------------------------------------------------------------------------------------ one-shot pull

    /**
     * A startup pull on the caller's thread: discovery at once, then every period, until one candidate the
     * {@code target} accepts was pulled and VERIFIED, or {@code deadlineNs}. An in-flight replication is stopped and
     * purged at the deadline whatever {@code SHADOW_PULL_TIMEOUT_MS} says (the deadline is the one bound).
     */
    public Report pullOnce(final Target target, final long deadlineNs) {
        return pullOnce(target, deadlineNs, () -> { });
    }

    /** {@link #pullOnce(Target, long)} with {@code eachPass} run after every pass (the host's liveness tick). */
    public Report pullOnce(final Target target, final long deadlineNs, final Runnable eachPass) {
        return pullOnce(target, deadlineNs, eachPass, () -> true);
    }

    /**
     * {@link #pullOnce(Target, long, Runnable)} that also ends as soon as {@code hostRunning} reads false (the host
     * is stopping): checked before every pass, so a stop waits for at most the pass in progress, never for the
     * deadline. An in-flight replication is then stopped and its partial copy purged as at the deadline, and the
     * report is NONE with the reason STOPPED.
     */
    public Report pullOnce(final Target target, final long deadlineNs, final Runnable eachPass,
                           final BooleanSupplier hostRunning) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(eachPass, "eachPass");
        Objects.requireNonNull(hostRunning, "hostRunning");
        if (!settings.enabled()) {
            return new Report(Report.Kind.DISABLED, 0, 0, 0, "disabled", Set.of(), null, null);
        }
        this.target = target;
        seenCandidates.clear();
        seenGeneses.clear();
        refusedThisCall = 0;
        lastReason = "no candidate";
        lastPulled = null;
        lastPulledFrom = null;
        nextDiscoveryNs = clock.nanoTime();
        try {
            boolean stopped = false;
            while (running && clock.nanoTime() - deadlineNs < 0) {
                if (!hostRunning.getAsBoolean()) {
                    stopped = true;
                    break;
                }
                try {
                    pass(deadlineNs);
                } catch (final RuntimeException failed) {
                    counters.increment(Counter.ERRORS_TOTAL);
                    lastReason = "ERROR";
                    System.err.println("[SHADOW-PULL] startup pass failed: " + failed);
                    closeClients();
                }
                if (lastPulled != null) {
                    return report(Report.Kind.VERIFIED);
                }
                if (knownGenesis.getAsLong() <= 0 && seenGeneses.size() >= 2) {
                    abortAll("two geneses among the candidates");
                    return report(Report.Kind.GENESES);
                }
                eachPass.run();
                sleep(1);
            }
            if (!inflight.isEmpty()) {
                for (final Inflight f : new ArrayList<>(inflight.values())) {
                    if (stopped) {
                        System.err.println("[SHADOW-PULL] startup pull stopped by the host: stopping " + f.candidate);
                    } else {
                        counters.incrementRefused("TIMEOUT");
                        System.err.println("[SHADOW-PULL] startup pull deadline reached: stopping " + f.candidate);
                    }
                    stopAndPurge(f);
                }
                inflight.clear();
                counters.set(Counter.INFLIGHT, 0);
                lastReason = stopped ? "STOPPED" : "TIMEOUT";
            } else if (stopped && lastPulled == null) {
                lastReason = "STOPPED";
            }
            return report(lastPulled != null ? Report.Kind.VERIFIED : Report.Kind.NONE);
        } finally {
            this.target = null;
        }
    }

    private Report report(final Report.Kind kind) {
        return new Report(kind, settings.sources().size(), seenCandidates.size(), refusedThisCall, lastReason,
                seenGeneses, lastPulled, lastPulledFrom);
    }

    // ------------------------------------------------------------------------------------------------ the pass

    /**
     * One pass: the leader gate, signals and errors, in-flight outcomes, pending purges, then discovery when due.
     *
     * @param startupDeadlineNs non-null inside {@link #pullOnce}: the bound of every replication it starts
     */
    void pass(final Long startupDeadlineNs) {
        counters.increment(Counter.PASSES_TOTAL);
        ensureLocal();
        final boolean leader = !off.contains(NegativeControl.LEADER_GATE_OFF) && leaderNow.getAsBoolean();
        if (leader) {
            counters.increment(Counter.SKIPPED_LEADER_TOTAL);
            if (!wasLeader) {
                System.out.println("[SHADOW-PULL] this node is LEADER: no listing, no pull while it leads");
            }
            wasLeader = true;
            // the local control session is drained in every pass, so a long leader term never backs up its responses
            pollSignals();
            if (off.contains(NegativeControl.LEADER_FINISHES_INFLIGHT)) {
                outcomes();
            } else if (!inflight.isEmpty()) {
                for (final Inflight f : new ArrayList<>(inflight.values())) {
                    System.err.println("[SHADOW-PULL] became LEADER during a pull: aborting " + f.candidate);
                    stopAndPurge(f);
                    counters.increment(Counter.ABORTED_LEADER_TOTAL);
                }
                inflight.clear();
                counters.set(Counter.INFLIGHT, 0);
            }
            retryPurges();
            return;
        }
        if (wasLeader) {
            wasLeader = false;
            nextDiscoveryNs = clock.nanoTime();
            System.out.println("[SHADOW-PULL] no longer LEADER: pulling again");
        }
        pollSignals();
        outcomes();
        retryPurges();
        final long now = clock.nanoTime();
        if (inflight.size() < settings.maxInflight() && now - nextDiscoveryNs >= 0) {
            nextDiscoveryNs = now + TimeUnit.MILLISECONDS.toNanos(settings.periodMs());
            final List<Candidate> candidates = discover();
            for (final Candidate c : candidates) {
                if (inflight.size() >= settings.maxInflight()) {
                    break;
                }
                start(c, startupDeadlineNs);
            }
        }
    }

    private void ensureLocal() {
        if (control == null) {
            control = AeronArchive.connect(localContext.clone()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .recordingSignalConsumer((controlSessionId, correlationId, recordingId, subscriptionId, position,
                                              signal) -> onSignal(correlationId, recordingId, position, signal))
                    .errorHandler(this::onControlError));
        }
        final long g = Math.max(0, knownGenesis.getAsLong());
        if (catalogClient == null) {
            catalogClient = AeronArchive.connect(localContext.clone().aeron(aeron).ownsAeronClient(false));
        }
        if (catalog == null || catalogGenesis != g) {
            catalog = new ShadowCatalog(aeron, catalogClient, engineId, clusterId, g, verdicts,
                    ShadowCatalog.DEFAULT_LIST_BATCH, clock, replayTimeoutNs, shadowCounters);
            catalogGenesis = g;
        }
    }

    private void pollSignals() {
        for (int i = 0; i < 1_000 && control.pollForRecordingSignals() > 0; i++) {
            // dispatched to onSignal
        }
        // an error answer is dispatched by pollForErrorResponse's poller too; pollForRecordingSignals routes errors to
        // the error handler above
    }

    private void onSignal(final long replicationId, final long recordingId, final long position,
                          final RecordingSignal signal) {
        final Inflight f = inflight.get(replicationId);
        if (f == null) {
            early.computeIfAbsent(replicationId, k -> new ArrayList<>()).add(new Object[] {recordingId, position,
                signal});
            return;
        }
        apply(f, recordingId, position, signal);
    }

    private void apply(final Inflight f, final long recordingId, final long position, final RecordingSignal signal) {
        f.signals.add(signal + "@" + position);
        if (signal == RecordingSignal.REPLICATE && recordingId >= 0) {
            f.dst = recordingId;
            pulledThisProcess.add(recordingId);
        } else if (signal == RecordingSignal.SYNC) {
            f.sync = true;
        } else if (signal == RecordingSignal.REPLICATE_END) {
            f.ended = true;
        }
    }

    private void onControlError(final Throwable t) {
        if (t instanceof ArchiveException ae) {
            final Inflight f = inflight.get(ae.correlationId());
            if (f != null) {
                if (f.error == null) {
                    f.error = ae.getMessage();
                    f.errorCode = ae.errorCode();
                }
                f.signals.add("ERROR(" + ae.errorCode() + ")");
                return;
            }
        }
        System.err.println("[SHADOW-PULL] local archive error: " + t);
    }

    // ------------------------------------------------------------------------------------------------ discovery

    private List<Candidate> discover() {
        final ShadowCatalog.Scan local = catalog.scan();
        final Set<ShadowAlias> heldUsable = new HashSet<>();
        final Set<ShadowAlias> heldAny = new HashSet<>();
        final Set<String> heldSeq = new HashSet<>();
        final Map<Long, Set<String>> alignedProducers = new HashMap<>();
        for (final ShadowEntry e : local.all()) {
            if (e.alias() == null) {
                continue;
            }
            heldAny.add(e.alias());
            heldSeq.add(producerOf(e.alias()) + "." + e.alias().producerSeq());
            if (e.state().usable()) {
                heldUsable.add(e.alias());
                if (e.alias().kind() == ShadowKind.ALIGNED.code()) {
                    alignedProducers.computeIfAbsent(e.alias().logPosition(), k -> new HashSet<>())
                            .add(producerOf(e.alias()));
                }
            }
        }
        final long newestVerified = local.verified().isEmpty() ? -1 : local.verified().get(0).logPosition();
        final long genesis = knownGenesis.getAsLong();
        final Set<String> busy = new HashSet<>();
        for (final Inflight f : inflight.values()) {
            busy.add(f.candidate.key());
        }
        final long now = clock.nanoTime();
        final List<Candidate> out = new ArrayList<>();
        final List<Endpoint> sources = settings.sources();
        final String deferred = off.contains(NegativeControl.LEADER_SOURCE_SKIP_OFF) ? null : deferredSource.get();
        if (!Objects.equals(deferred, deferredLogged)) {
            deferredLogged = deferred;
            if (deferred != null && sources.stream().anyMatch(e -> e.text().equals(deferred))) {
                System.out.println("[SHADOW-PULL] source " + deferred + " is the current leader's archive: not listed"
                        + " while it leads");
            }
        }
        for (int i = 0; i < sources.size(); i++) {
            final Endpoint src = sources.get(i);
            if (deferred != null && deferred.equals(src.text())) {
                counters.increment(Counter.SOURCE_SKIPPED_LEADER_TOTAL);
                continue;
            }
            final List<long[]> descriptors = new ArrayList<>();
            final List<String> channels = new ArrayList<>();
            if (!list(src, descriptors, channels)) {
                continue;
            }
            for (int d = 0; d < descriptors.size(); d++) {
                final long[] desc = descriptors.get(d);
                final ShadowAlias alias = ShadowAlias.tryParse(ShadowAlias.aliasOfChannel(channels.get(d)));
                if (alias == null) {
                    continue;
                }
                final long start = desc[1];
                final long stop = desc[2];
                if (stop == AeronArchive.NULL_POSITION && !off.contains(NegativeControl.PULL_ACTIVE_ALLOWED)) {
                    continue;
                }
                final long length = stop == AeronArchive.NULL_POSITION ? -1 : stop - start;
                if (stop != AeronArchive.NULL_POSITION
                        && length < ShadowHeader.FRAME_LENGTH + ShadowTrailer.FRAME_LENGTH) {
                    continue;
                }
                if (alias.engineId() != engineId || alias.clusterId() != clusterId) {
                    continue;
                }
                final ShadowKind kind = ShadowKind.of(alias.kind());
                if (kind == null || (!settings.kinds().contains(kind)
                        && !off.contains(NegativeControl.PULL_KINDS_ANY))) {
                    continue;
                }
                if (genesis > 0 && alias.genesisId() != genesis) {
                    continue;
                }
                if (target != null && target.rank(alias) < 0) {
                    continue;
                }
                if (target != null && genesis <= 0) {
                    seenGeneses.add(alias.genesisId());
                }
                final Candidate c = new Candidate(src, i, desc[0], alias, length);
                if (held(alias, heldUsable, heldAny, heldSeq)) {
                    continue;
                }
                if (contentRefused.contains(c.key()) || conflictKept.contains(c.key()) || busy.contains(c.key())) {
                    continue;
                }
                if (target == null && alias.logPosition() <= newestVerified
                        && !secondWitness(alias, alignedProducers)) {
                    continue;
                }
                if (target != null) {
                    seenCandidates.add(c.key());
                }
                final Backoff b = backoff.get(c.key());
                if (b != null && now - b.nextNs < 0) {
                    continue;
                }
                out.add(c);
            }
        }
        final Comparator<Candidate> newestFirst = Comparator.comparingLong((Candidate c) -> c.alias().logPosition())
                .reversed().thenComparingInt(Candidate::sourceOrder);
        out.sort(target == null ? newestFirst
                : Comparator.comparingInt((Candidate c) -> target.rank(c.alias())).thenComparing(newestFirst));
        return out;
    }

    /** A producer slot as the alias names it ({@code <producer kind>.<producer id>}). */
    private static String producerOf(final ShadowAlias alias) {
        return alias.producerKind() + "." + alias.producerId();
    }

    /**
     * An ALIGNED at a position held by usable ALIGNED copies of exactly one producer, written by another producer:
     * the second witness of that position (pulled even at or below the newest VERIFIED position).
     */
    private boolean secondWitness(final ShadowAlias alias, final Map<Long, Set<String>> alignedProducers) {
        if (off.contains(NegativeControl.WITNESS_PULL_OFF) || alias.kind() != ShadowKind.ALIGNED.code()) {
            return false;
        }
        final Set<String> held = alignedProducers.get(alias.logPosition());
        return held != null && held.size() == 1 && !held.contains(producerOf(alias));
    }

    private boolean held(final ShadowAlias alias, final Set<ShadowAlias> heldUsable, final Set<ShadowAlias> heldAny,
                         final Set<String> heldSeq) {
        if (off.contains(NegativeControl.DEDUPE_BY_SEQ)) {
            return heldSeq.contains(producerOf(alias) + "." + alias.producerSeq());
        }
        if (off.contains(NegativeControl.DEDUPE_AGAINST_EVIDENCE)) {
            return heldAny.contains(alias);
        }
        return heldUsable.contains(alias);
    }

    /** Lists one source; false (counted, the client closed for a reconnect) when it did not answer. */
    private boolean list(final Endpoint src, final List<long[]> descriptors, final List<String> channels) {
        final Remote r = remotes.computeIfAbsent(src.text(), k -> new Remote());
        try {
            if (r.client == null) {
                if (r.connecting == null) {
                    if (r.everConnected) {
                        counters.increment(Counter.SOURCE_RECONNECTS_TOTAL);
                    }
                    r.connecting = AeronArchive.asyncConnect(remoteContext(src));
                }
                final long deadline = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.periodMs());
                while ((r.client = r.connecting.poll()) == null) {
                    if (clock.nanoTime() - deadline >= 0) {
                        return false; // still connecting: tried again next pass
                    }
                    sleep(1);
                }
                r.connecting = null;
                r.everConnected = true;
            }
            long from = 0;
            while (true) {
                final int before = descriptors.size();
                final int n = r.client.listRecordingsForUri(from, ShadowCatalog.DEFAULT_LIST_BATCH, prefix,
                        ShadowAlias.STREAM_ID,
                        (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                         stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId,
                         streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                            descriptors.add(new long[] {recordingId, startPosition, stopPosition});
                            channels.add(originalChannel);
                        });
                if (n < ShadowCatalog.DEFAULT_LIST_BATCH || descriptors.size() == before) {
                    return true;
                }
                from = descriptors.get(descriptors.size() - 1)[0] + 1;
            }
        } catch (final RuntimeException unreachable) {
            counters.increment(Counter.SOURCE_UNREACHABLE_TOTAL);
            System.err.println("[SHADOW-PULL] source " + src.text() + " did not answer its listing: " + unreachable);
            CloseHelper.quietCloseAll(r.connecting, r.client);
            r.connecting = null;
            r.client = null;
            return false;
        }
    }

    private AeronArchive.Context remoteContext(final Endpoint src) {
        final String response = settings.responseEndpoint() != null ? settings.responseEndpoint().udpChannel()
                : "aeron:udp?endpoint=" + responseHost + ":0";
        return new AeronArchive.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .controlRequestChannel(src.udpChannel())
                .controlRequestStreamId(AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT)
                .controlResponseChannel(response)
                .controlResponseStreamId(LISTING_RESPONSE_STREAM_ID)
                .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(settings.periodMs()));
    }

    // ------------------------------------------------------------------------------------------------ start

    private void start(final Candidate c, final Long startupDeadlineNs) {
        final ReplicationParams params = new ReplicationParams();
        if (settings.replicationEndpoint() != null) {
            params.replicationChannel(settings.replicationEndpoint().udpChannel());
        }
        if (settings.responseEndpoint() != null) {
            params.srcResponseChannel(settings.responseEndpoint().udpChannel());
        }
        final Backoff b = backoff.get(c.key());
        final long now = clock.nanoTime();
        final long id;
        try {
            id = control.replicate(c.srcRecordingId(), AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT,
                    c.source().udpChannel(), params);
        } catch (final ArchiveException refused) {
            if (b != null) {
                counters.increment(Counter.RETRIES_TOTAL);
            }
            archiveRefusal(c, refused.errorCode(), refused.getMessage());
            return;
        }
        counters.increment(Counter.STARTED_TOTAL);
        if (b != null) {
            counters.increment(Counter.RETRIES_TOTAL);
        }
        final long deadline = startupDeadlineNs != null ? startupDeadlineNs
                : now + TimeUnit.MILLISECONDS.toNanos(settings.timeoutMs());
        final Inflight f = new Inflight(c, id, now, deadline);
        inflight.put(id, f);
        final List<Object[]> buffered = early.remove(id);
        if (buffered != null) {
            for (final Object[] s : buffered) {
                apply(f, (Long)s[0], (Long)s[1], (RecordingSignal)s[2]);
            }
        }
        early.clear();
        counters.set(Counter.INFLIGHT, inflight.size());
        System.out.println("[SHADOW-PULL] pulling " + c + " (replication " + id + ")");
    }

    // ------------------------------------------------------------------------------------------------ outcomes

    private void outcomes() {
        final long now = clock.nanoTime();
        for (final Inflight f : new ArrayList<>(inflight.values())) {
            if (f.verifying || (f.ended && f.error == null)) {
                if (!f.verifying && !f.sync && f.dst >= 0 && localLength(f.dst) < 0 && now - f.deadlineNs < 0) {
                    continue; // REPLICATE_END arrives before the destination's stop: judge the length once it is set
                }
                if (!f.verifying && !f.sync && !lengthsEqual(f)
                        && !off.contains(NegativeControl.INCOMPLETE_AS_CONTENT)) {
                    done(f);
                    final long ended = localLength(f.dst);
                    purgeOwn(f.dst);
                    retryLater(f.candidate, "INCOMPLETE", "the copy ended at " + ended + " of "
                            + f.candidate.length() + " B without SYNC and without an error (signals " + f.signals
                            + ")");
                    continue;
                }
                f.verifying = true;
                verify(f);
            } else if (f.ended) {
                done(f);
                purgeOwn(f.dst);
                archiveRefusal(f.candidate, f.errorCode, f.error);
            } else if (now - f.deadlineNs >= 0) {
                done(f);
                stopAndPurge(f);
                retryLater(f.candidate, "TIMEOUT", "the replication outlived its bound (signals " + f.signals + ")");
            }
        }
    }

    private void done(final Inflight f) {
        inflight.remove(f.replicationId);
        counters.set(Counter.INFLIGHT, inflight.size());
    }

    private boolean lengthsEqual(final Inflight f) {
        return f.dst >= 0 && completeCopy(false, localLength(f.dst), f.candidate.length());
    }

    /**
     * The completion rule: a copy is COMPLETE when the archive signalled SYNC for its replication, or when the local
     * copy is stopped ({@code localLength >= 0}) with the length the source descriptor had at discovery. An unknown
     * source length (-1) never completes by length.
     */
    static boolean completeCopy(final boolean sync, final long localLength, final long sourceLength) {
        return sync || (localLength >= 0 && sourceLength >= 0 && localLength == sourceLength);
    }

    private long localLength(final long dst) {
        if (dst < 0) {
            return -1;
        }
        final long[] len = {-1};
        try {
            catalogClient.listRecording(dst, (controlSessionId, correlationId, recordingId, startTimestamp,
                                              stopTimestamp, startPosition, stopPosition, initialTermId,
                                              segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                                              strippedChannel, originalChannel, sourceIdentity) ->
                    len[0] = stopPosition == AeronArchive.NULL_POSITION ? -1 : stopPosition - startPosition);
        } catch (final RuntimeException unreadable) {
            return -1;
        }
        return len[0];
    }

    /** A COMPLETE copy (SYNC or equal lengths): local classification, then the deep read. */
    private void verify(final Inflight f) {
        final ShadowCatalog.Scan s = catalog.scan();
        final ShadowEntry e = s.entry(f.dst);
        if (e == null) {
            done(f);
            retryLater(f.candidate, "INCOMPLETE", "the copy rec=" + f.dst + " is not listed locally");
            return;
        }
        if (e.state() == State.SCAN_ERROR) {
            return; // unknown, not corrupt: the next pass reads it again
        }
        if (e.state() == State.CONFLICT) {
            final DeepResult d = catalog.verifyDeep(e);
            if (d.kind() == DeepResult.Kind.SCAN_ERROR) {
                return;
            }
            done(f);
            if (d.kind() == DeepResult.Kind.CORRUPT) {
                content(f, "CORRUPT", d.reason());
                return;
            }
            conflictKept.add(f.candidate.key());
            System.err.println("[SHADOW-PULL] CRITICAL: the copy of " + f.candidate + " (local rec=" + f.dst
                    + ") disagrees with a local copy at P=" + f.candidate.alias().logPosition() + ": CONFLICT, kept;"
                    + " the position never anchors and an operator purges one side");
            return;
        }
        if (!e.state().usable()) {
            done(f);
            content(f, e.state().name(), e.reason());
            return;
        }
        final DeepResult d = catalog.verifyDeep(e);
        switch (d.kind()) {
            case VERIFIED -> {
                done(f);
                counters.increment(Counter.PULL_TOTAL);
                counters.add(Counter.PULL_BYTES_TOTAL, Math.max(0, e.stopPosition() - e.startPosition()));
                backoff.remove(f.candidate.key());
                lastPulled = e.with(State.VERIFIED, null);
                lastPulledFrom = f.candidate.source();
                lastReason = "VERIFIED";
                System.out.println("[SHADOW-PULL] pulled " + f.candidate + " -> local rec=" + f.dst + " VERIFIED in "
                        + TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - f.startedNs) + " ms (signals " + f.signals
                        + ")");
            }
            case CORRUPT -> {
                done(f);
                content(f, "CORRUPT", d.reason());
            }
            case NOT_VERIFIABLE -> {
                done(f);
                content(f, String.valueOf(d.reason()), "not verifiable");
            }
            default -> {
                // SCAN_ERROR: unknown, read again next pass
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ refusals

    private void archiveRefusal(final Candidate c, final int code, final String text) {
        if (off.contains(NegativeControl.PULL_RETRY_AS_CONTENT)) {
            refuseForContent(c, "CORRUPT", "archive refusal (code " + code + "): " + text);
            return;
        }
        retryLater(c, archiveReason(code), "archive refusal code " + code + ": " + text);
    }

    static String archiveReason(final int code) {
        return switch (code) {
            case ArchiveException.GENERIC -> "GENERIC";
            case ArchiveException.MAX_RECORDINGS -> "MAX_RECORDINGS";
            case ArchiveException.REPLICATION_CONNECTION_FAILURE -> "REPLICATION_CONNECTION_FAILURE";
            case ArchiveException.STORAGE_SPACE -> "STORAGE_SPACE";
            default -> code < 0 ? "GENERIC" : ArchiveException.errorCodeAsString(code);
        };
    }

    private void retryLater(final Candidate c, final String reason, final String detail) {
        counters.incrementRefused(reason);
        refusedThisCall++;
        lastReason = reason;
        final Backoff b = backoff.computeIfAbsent(c.key(), k -> new Backoff());
        b.failures++;
        final long factor = Math.min(BACKOFF_MAX_FACTOR, 1L << Math.min(30, b.failures - 1));
        b.nextNs = clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.retryBackoffMs() * factor);
        nextDiscoveryNs = Math.min(nextDiscoveryNs, b.nextNs);
        lastRefusal = reason + ": " + detail;
        System.err.println("[SHADOW-PULL] REFUSED(" + reason + ") " + c + ": " + detail + "; retry in "
                + settings.retryBackoffMs() * factor + " ms");
    }

    private void content(final Inflight f, final String reason, final String detail) {
        purgeOwn(f.dst);
        refuseForContent(f.candidate, reason, detail);
    }

    private void refuseForContent(final Candidate c, final String reason, final String detail) {
        if (off.contains(NegativeControl.PULL_CONTENT_AS_RETRY)) {
            retryLater(c, reason, detail);
            return;
        }
        counters.incrementRefused(reason);
        refusedThisCall++;
        lastReason = reason;
        contentRefused.add(c.key());
        lastRefusal = reason + ": " + detail;
        System.err.println("[SHADOW-PULL] CRITICAL: refused for content (" + reason + "): source " + c.source().text()
                + " recording " + c.srcRecordingId() + " " + c.alias().format() + ": " + detail
                + "; never pulled again in this process (purge it on the producer)");
    }

    // ------------------------------------------------------------------------------------------------ purges

    private void stopAndPurge(final Inflight f) {
        try {
            control.stopReplication(f.replicationId);
        } catch (final RuntimeException alreadyDone) {
            // the session ended on its own
        }
        purgeOwn(f.dst);
    }

    private void abortAll(final String why) {
        if (control == null) {
            return;
        }
        for (final Inflight f : new ArrayList<>(inflight.values())) {
            System.err.println("[SHADOW-PULL] " + why + ": stopping " + f.candidate);
            stopAndPurge(f);
        }
        inflight.clear();
        counters.set(Counter.INFLIGHT, 0);
    }

    /** Purges a local recording this process created by a pull; a refusal is retried next pass. */
    private void purgeOwn(final long dst) {
        if (dst < 0 || !pulledThisProcess.contains(dst)) {
            return;
        }
        final PurgeResult r = catalog.purge(dst, true);
        if (r.purged() || r.code() == ShadowCatalog.PURGE_CODE_NOT_FOUND) {
            purgePending.remove(dst);
        } else {
            purgePending.add(dst);
            System.err.println("[SHADOW-PULL] purge of the pulled copy rec=" + dst + " refused code=" + r.code() + ": "
                    + r.detail() + "; retried next pass");
        }
        counters.set(Counter.PURGE_PENDING, purgePending.size());
    }

    private void retryPurges() {
        for (final long dst : new ArrayList<>(purgePending)) {
            purgeOwn(dst);
        }
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
