// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.CncFileDescriptor;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ClientTimeoutException;
import io.aeron.exceptions.ConductorServiceTimeoutException;
import io.aeron.exceptions.DriverTimeoutException;
import org.agrona.concurrent.AgentTerminationException;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Process lifecycle of an engine node (match and core run the same code): how the node stops and which exit
 * status the process manager sees.
 *
 * <p>Rules:
 * <ul>
 *   <li>A fatal fault (any other FATAL Aeron error with an external driver; an agent that
 *       terminated unexpectedly; a startup failure such as no CnC within the driver wait) exits with a non-zero
 *       status, never 0: the components are closed on the main thread, then the process halts with the fault's
 *       status; a daemon backstop halts with the same status after the fatal grace if the close hangs.</li>
 *   <li>SIGTERM (any JVM shutdown) stops the node cleanly: the JVM shutdown hook wakes the main thread, which
 *       closes the components, and waits at most the shutdown timeout for that close; the JVM then exits with the
 *       signal's status (143), whether or not the close finished.</li>
 *   <li>A cluster-requested termination (the consensus module / service container termination hook with no fault
 *       recorded) exits 0.</li>
 *   <li>The first stop cause wins, except that a fault upgrades a cluster termination (Aeron runs the termination
 *       hook before it reports the unexpected agent termination behind it). Nothing upgrades a signal stop.</li>
 *   <li>Driver loss with an external driver (an Aeron client reports {@code DriverTimeoutException},
 *       {@code ClientTimeoutException} or {@code ConductorServiceTimeoutException}) halts at once, on the thread
 *       that reported it, with no component close: 1, or 143 when a signal stop is already in progress. That
 *       client closes itself as soon as its error handler returns and unmaps the CnC (aeron 1.53
 *       {@code ClientConductor.onClose} lines 190-206, after a 16 ms idle sleep), while agents of the same process
 *       still write counters in it: an archive whose own client closed decrements its recording-session counter
 *       in the unmapped page ({@code ArchiveConductor.closeRecordingSession} line 1362) and the JVM dies with
 *       SIGSEGV (139, seen on all three ME members). Closing the components cannot win that race and has nothing
 *       durable to write without a driver; the restart recovers exactly as from SIGKILL. A client call made from
 *       another thread can see the loss first: Aeron then throws the timeout to that caller, marks the client
 *       terminating and its conductor later stops with an unexpected {@code AgentTerminationException} instead of
 *       reporting the loss. Such a caller hands the timeout to the client's error handler
 *       ({@link SnapshotLogPruner} does), and an unexpected agent termination after a driver loss joins the halt
 *       on its own thread instead of letting its client close. That conductor's termination can also reach the
 *       error handler before the timeout does; a client conductor's bare termination is itself the loss
 *       ({@link #isClientTimeoutTermination}) and halts the same way.</li>
 *   <li>No leader ever known: the consensus module's election stays in a no-leader state for the
 *       quorum wait after its launch; {@link QuorumWatchdog} records a fatal fault, exit 1.</li>
 *   <li>No rejoin after a leader was known: one election stays open for the rejoin wait and is in a no-leader
 *       state (or INIT) while a quorum of members is reachable; {@link RejoinWatchdog} logs one CRITICAL line and
 *       records a fatal fault, exit 1. The same for a leader elect that stays in LEADER_LOG_REPLICATION or
 *       LEADER_READY for the leader elect wait while a quorum of members is reachable. During a genuine quorum loss
 *       it does not trip.</li>
 *   <li>No catch-up possible: catch-up attempts keep failing and this member's log position is below the leader's
 *       log start; {@link CatchupWatchdog} logs one CRITICAL line naming both positions and records a fatal fault,
 *       exit 1.</li>
 * </ul>
 *
 * <p>This replaces Agrona's {@code ShutdownSignalBarrier} in the engines. That barrier's static JVM hook waits,
 * without a bound, for {@code close()} on every barrier it signalled, and the engines never called it: SIGTERM and
 * an uncaught startup exception both hung the JVM (agrona 2.6.0 {@code ShutdownSignalBarrier.java:96-100, 244-279}),
 * and a fault that let the main thread return ended the JVM with status 0 before the halt backstop ran.
 *
 * <p>All bounds are process settings (environment first, then system property, then the default), never
 * replicated state; a malformed or non-positive value refuses the start.
 */
public final class EngineLifecycle {
    /** Bound on the clean close after SIGTERM: below the console's 5 s cluster-node StopTimeout. */
    public static final String SHUTDOWN_TIMEOUT_ENV = "ENGINE_SHUTDOWN_TIMEOUT_MS";
    public static final String SHUTDOWN_TIMEOUT_PROP = "engine.shutdown.timeout.ms";
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 4_000;
    /** After a fatal fault: how long the close may take before the backstop halts with the fault's status. */
    public static final String FATAL_EXIT_GRACE_ENV = "ENGINE_FATAL_EXIT_GRACE_MS";
    public static final String FATAL_EXIT_GRACE_PROP = "engine.fatal.exit.grace.ms";
    public static final long DEFAULT_FATAL_EXIT_GRACE_MS = 5_000;
    /** External driver mode: how long the start waits for the driver's cnc.dat before it exits 1. */
    public static final String DRIVER_WAIT_ENV = "TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS";
    public static final String DRIVER_WAIT_PROP = "transport.external.driver.timeout.ms";
    public static final long DEFAULT_DRIVER_WAIT_MS = 30_000;
    /**
     * How long the consensus module may stay without any known leader, from its launch,
     * before the node exits 1 ({@link QuorumWatchdog}). Formation needs a quorum of members in canvass within this
     * wait of each other's launch: size it against the supervisor's restart backoff.
     */
    public static final String QUORUM_WAIT_ENV = "ENGINE_QUORUM_WAIT_MS";
    public static final String QUORUM_WAIT_PROP = "engine.quorum.wait.ms";
    public static final long DEFAULT_QUORUM_WAIT_MS = 120_000;
    /**
     * How long one election may stay open, once this process has known a leader, before a member still without a
     * leader exits 1 ({@link RejoinWatchdog}). Size it above the cluster's election timeouts and below the time the
     * cluster may run one member short.
     */
    public static final String REJOIN_WAIT_ENV = "ENGINE_REJOIN_WAIT_MS";
    public static final String REJOIN_WAIT_PROP = "engine.rejoin.wait.ms";
    public static final long DEFAULT_REJOIN_WAIT_MS = 60_000;
    /**
     * How long one observation of a reachable quorum keeps counting as evidence for the {@link RejoinWatchdog}
     * (1000..600000). The watchdog trips only while such evidence holds; a genuine quorum loss never trips it.
     * Set it no shorter than the media driver's publication connection timeout (a peer that died reads reachable
     * for that long); {@link #REJOIN_WAIT_ENV} must exceed it ({@link #rejoinSettingsRefusal}).
     */
    public static final String REJOIN_PEER_WINDOW_ENV = "ENGINE_REJOIN_PEER_WINDOW_MS";
    public static final String REJOIN_PEER_WINDOW_PROP = "engine.rejoin.peer.window.ms";
    public static final long DEFAULT_REJOIN_PEER_WINDOW_MS = 10_000;
    public static final long MIN_REJOIN_PEER_WINDOW_MS = 1_000;
    public static final long MAX_REJOIN_PEER_WINDOW_MS = 600_000;
    /**
     * How long a leader elect may stay in LEADER_LOG_REPLICATION or LEADER_READY, counted only while a quorum of
     * members is reachable, before it exits 1 ({@link RejoinWatchdog}) (10000..3600000). Aeron leaves those states
     * only once a quorum of followers reports its append position, without a timeout: a leader elect whose voter
     * vanished, or whose followers cannot hear it, waits there for ever. Size it above the time followers need to
     * replicate the longest log a new leader may have to send them; it must exceed {@link #REJOIN_PEER_WINDOW_ENV}
     * ({@link #leaderElectSettingsRefusal}).
     */
    public static final String LEADER_ELECT_WAIT_ENV = "ENGINE_LEADER_ELECT_WAIT_MS";
    public static final String LEADER_ELECT_WAIT_PROP = "engine.leader.elect.wait.ms";
    public static final long DEFAULT_LEADER_ELECT_WAIT_MS = 300_000;
    public static final long MIN_LEADER_ELECT_WAIT_MS = 10_000;
    public static final long MAX_LEADER_ELECT_WAIT_MS = 3_600_000;
    /**
     * How many catch-up attempts in a row may fail before the {@link CatchupWatchdog} reads the leader's log start
     * (1..1000): a member whose own log position is below it can never catch up and exits 1.
     */
    public static final String CATCHUP_REFUSALS_ENV = "ENGINE_CATCHUP_REFUSALS";
    public static final String CATCHUP_REFUSALS_PROP = "engine.catchup.refusals";
    public static final long DEFAULT_CATCHUP_REFUSALS = 3;
    public static final long MIN_CATCHUP_REFUSALS = 1;
    public static final long MAX_CATCHUP_REFUSALS = 1_000;
    /**
     * The time bound next to the count (1000..3600000): a member that has been failing its catch-up for this long
     * reads the leader's log start even when fewer attempts were counted.
     */
    public static final String CATCHUP_REFUSAL_WAIT_ENV = "ENGINE_CATCHUP_REFUSAL_WAIT_MS";
    public static final String CATCHUP_REFUSAL_WAIT_PROP = "engine.catchup.refusal.wait.ms";
    public static final long DEFAULT_CATCHUP_REFUSAL_WAIT_MS = 30_000;
    public static final long MIN_CATCHUP_REFUSAL_WAIT_MS = 1_000;
    public static final long MAX_CATCHUP_REFUSAL_WAIT_MS = 3_600_000;

    /** Status of a fatal fault (driver death, unexpected agent termination, startup failure). */
    public static final int EXIT_FATAL = 1;
    /** Status of a cluster-requested termination. */
    public static final int EXIT_CLUSTER_TERMINATED = 0;
    /** Status the JVM gives a SIGTERM stop (128 + 15); used when driver loss halts a signal stop in progress. */
    public static final int EXIT_SIGNAL = 143;
    /**
     * Status of a decided restart (a replica re-bootstrap): not a fault, the supervisor restarts the process
     * ({@code RestartForceExitStatus=3}).
     */
    public static final int EXIT_RESTART = 3;
    /**
     * Status of a node-local refusal a restart cannot fix: this build cannot run what the cluster recorded (a member
     * version outside the window, a behaviour the jar lacks, a jar below the recorded wire ceiling) or a removed
     * setting is present. The units never restart it ({@code RestartPreventExitStatus=78}); the recovery is a
     * compatible build or the setting removed.
     */
    public static final int EXIT_CONFIG = 78;

    /** Why the node is stopping. */
    public enum Cause { NONE, SIGNAL, CLUSTER_TERMINATED, FATAL, RESTART }

    private record Stop(Cause cause, int status, String reason) { }

    private static final Stop RUNNING = new Stop(Cause.NONE, -1, "");

    private final String name;
    private final Consumer<String> log;
    private final IntConsumer halter;
    private final long shutdownTimeoutMs;
    private final long fatalExitGraceMs;
    private final long quorumWaitMs;
    private final long rejoinWaitMs;
    private final long rejoinPeerWindowMs;
    private final long leaderElectWaitMs;
    private final long catchupRefusals;
    private final long catchupRefusalWaitMs;
    private final AtomicReference<Stop> stop = new AtomicReference<>(RUNNING);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private final AtomicBoolean halted = new AtomicBoolean();
    /** Set by {@link #driverLost}: from then on no thread may close an Aeron client (see {@link #onError}). */
    private volatile boolean driverLossSeen;
    /** Released only when the halter returns (tests): until then no thread that lost the halt may return. */
    private final CountDownLatch haltReturned = new CountDownLatch(1);
    /** The quorum and rejoin watchdogs' stops, run by {@link #awaitStop} before the components close. */
    private final java.util.List<java.util.function.LongConsumer> watchdogs =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    static final long WATCHDOG_JOIN_MS = 2_000;
    /** Set when a component close threw during the stop, so a signal stop is not reported clean. */
    private volatile String closeFailure;

    EngineLifecycle(final String name, final Consumer<String> log, final IntConsumer halter,
                    final long shutdownTimeoutMs, final long fatalExitGraceMs) {
        this(name, log, halter, shutdownTimeoutMs, fatalExitGraceMs, DEFAULT_QUORUM_WAIT_MS);
    }

    EngineLifecycle(final String name, final Consumer<String> log, final IntConsumer halter,
                    final long shutdownTimeoutMs, final long fatalExitGraceMs, final long quorumWaitMs) {
        this(name, log, halter, shutdownTimeoutMs, fatalExitGraceMs, quorumWaitMs, DEFAULT_REJOIN_WAIT_MS);
    }

    EngineLifecycle(final String name, final Consumer<String> log, final IntConsumer halter,
                    final long shutdownTimeoutMs, final long fatalExitGraceMs, final long quorumWaitMs,
                    final long rejoinWaitMs) {
        this(name, log, halter, shutdownTimeoutMs, fatalExitGraceMs, quorumWaitMs, rejoinWaitMs,
                DEFAULT_REJOIN_PEER_WINDOW_MS, DEFAULT_CATCHUP_REFUSALS, DEFAULT_CATCHUP_REFUSAL_WAIT_MS);
    }

    EngineLifecycle(final String name, final Consumer<String> log, final IntConsumer halter,
                    final long shutdownTimeoutMs, final long fatalExitGraceMs, final long quorumWaitMs,
                    final long rejoinWaitMs, final long rejoinPeerWindowMs, final long catchupRefusals,
                    final long catchupRefusalWaitMs) {
        this(name, log, halter, shutdownTimeoutMs, fatalExitGraceMs, quorumWaitMs, rejoinWaitMs, rejoinPeerWindowMs,
                DEFAULT_LEADER_ELECT_WAIT_MS, catchupRefusals, catchupRefusalWaitMs);
    }

    EngineLifecycle(final String name, final Consumer<String> log, final IntConsumer halter,
                    final long shutdownTimeoutMs, final long fatalExitGraceMs, final long quorumWaitMs,
                    final long rejoinWaitMs, final long rejoinPeerWindowMs, final long leaderElectWaitMs,
                    final long catchupRefusals, final long catchupRefusalWaitMs) {
        if (shutdownTimeoutMs <= 0 || fatalExitGraceMs <= 0 || quorumWaitMs <= 0 || rejoinWaitMs <= 0
                || rejoinPeerWindowMs <= 0 || leaderElectWaitMs <= 0 || catchupRefusals <= 0
                || catchupRefusalWaitMs <= 0) {
            throw new IllegalArgumentException("lifecycle bounds must be positive");
        }
        this.rejoinPeerWindowMs = rejoinPeerWindowMs;
        this.leaderElectWaitMs = leaderElectWaitMs;
        this.catchupRefusals = catchupRefusals;
        this.catchupRefusalWaitMs = catchupRefusalWaitMs;
        this.name = name;
        this.log = log;
        this.halter = halter;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.fatalExitGraceMs = fatalExitGraceMs;
        this.quorumWaitMs = quorumWaitMs;
        this.rejoinWaitMs = rejoinWaitMs;
    }

    /**
     * The production lifecycle: bounds from the settings, {@link Runtime#halt(int)} as the exit, and a JVM shutdown
     * hook registered for the signal stop. Call first in the node's bootstrap, before any thread starts.
     */
    public static EngineLifecycle install(final String name, final Consumer<String> log) {
        // Every setting is parsed here, in every driver mode: a malformed or out-of-range one refuses the start.
        final long shutdownMs = shutdownTimeoutMs();
        final long graceMs = fatalExitGraceMs();
        driverWaitMs();
        final long quorumMs = quorumWaitMs();
        final long rejoinMs = rejoinWaitMs();
        final long rejoinWindowMs = rejoinPeerWindowMs();
        final String rejoinRefusal = rejoinSettingsRefusal(rejoinMs, rejoinWindowMs);
        if (rejoinRefusal != null) {
            throw new IllegalArgumentException(rejoinRefusal);
        }
        final long leaderElectMs = leaderElectWaitMs();
        final String leaderElectRefusal = leaderElectSettingsRefusal(leaderElectMs, rejoinWindowMs);
        if (leaderElectRefusal != null) {
            throw new IllegalArgumentException(leaderElectRefusal);
        }
        // the retention floor's settings are read again where the pruner is wired, inside the service's start, where
        // a refusal would be too late to stop the boot: check them here
        SnapshotLogPruner.checkSettings();
        // the rejoin watchdog's probe reads its term length and stream id when the watchdog is armed, after the
        // cluster starts
        PeerReachability.probeTermLength();
        PeerReachability.probeStreamId();
        final EngineLifecycle lifecycle = new EngineLifecycle(name, log, status -> Runtime.getRuntime().halt(status),
                shutdownMs, graceMs, quorumMs, rejoinMs, rejoinWindowMs, leaderElectMs, catchupRefusals(),
                catchupRefusalWaitMs());
        Runtime.getRuntime().addShutdownHook(new Thread(lifecycle::onJvmShutdown, name + "-shutdown"));
        return lifecycle;
    }

    public static long shutdownTimeoutMs() {
        return positiveSetting(SHUTDOWN_TIMEOUT_ENV, SHUTDOWN_TIMEOUT_PROP, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    public static long fatalExitGraceMs() {
        return positiveSetting(FATAL_EXIT_GRACE_ENV, FATAL_EXIT_GRACE_PROP, DEFAULT_FATAL_EXIT_GRACE_MS);
    }

    public static long driverWaitMs() {
        return positiveSetting(DRIVER_WAIT_ENV, DRIVER_WAIT_PROP, DEFAULT_DRIVER_WAIT_MS);
    }

    public static long quorumWaitMs() {
        return positiveSetting(QUORUM_WAIT_ENV, QUORUM_WAIT_PROP, DEFAULT_QUORUM_WAIT_MS);
    }

    public static long rejoinWaitMs() {
        return positiveSetting(REJOIN_WAIT_ENV, REJOIN_WAIT_PROP, DEFAULT_REJOIN_WAIT_MS);
    }

    public static long rejoinPeerWindowMs() {
        return rangedSetting(REJOIN_PEER_WINDOW_ENV, REJOIN_PEER_WINDOW_PROP, DEFAULT_REJOIN_PEER_WINDOW_MS,
                MIN_REJOIN_PEER_WINDOW_MS, MAX_REJOIN_PEER_WINDOW_MS);
    }

    /**
     * Why this pair of rejoin settings refuses the boot, or null when it is fine. The rejoin wait has to exceed the
     * peer window: a peer that just died still reads reachable for the driver's publication connection timeout,
     * which the window is set to cover, and a wait no longer than that would restart the lone survivor of a quorum
     * loss once.
     */
    public static String rejoinSettingsRefusal(final long rejoinWaitMs, final long rejoinPeerWindowMs) {
        if (rejoinWaitMs <= rejoinPeerWindowMs) {
            return REJOIN_WAIT_ENV + " (" + rejoinWaitMs + " ms) must exceed " + REJOIN_PEER_WINDOW_ENV + " ("
                    + rejoinPeerWindowMs + " ms): a peer that just died still counts as reachable for the media"
                    + " driver's publication connection timeout, which the window has to cover";
        }
        return null;
    }

    public static long leaderElectWaitMs() {
        return rangedSetting(LEADER_ELECT_WAIT_ENV, LEADER_ELECT_WAIT_PROP, DEFAULT_LEADER_ELECT_WAIT_MS,
                MIN_LEADER_ELECT_WAIT_MS, MAX_LEADER_ELECT_WAIT_MS);
    }

    /**
     * Why this leader elect wait refuses the boot next to this peer window, or null when it is fine: the same rule
     * as {@link #rejoinSettingsRefusal}, for the same reason (a peer that just died still reads reachable for the
     * window).
     */
    public static String leaderElectSettingsRefusal(final long leaderElectWaitMs, final long rejoinPeerWindowMs) {
        if (leaderElectWaitMs <= rejoinPeerWindowMs) {
            return LEADER_ELECT_WAIT_ENV + " (" + leaderElectWaitMs + " ms) must exceed " + REJOIN_PEER_WINDOW_ENV
                    + " (" + rejoinPeerWindowMs + " ms): a peer that just died still counts as reachable for the media"
                    + " driver's publication connection timeout, which the window has to cover";
        }
        return null;
    }

    public static long catchupRefusals() {
        return rangedSetting(CATCHUP_REFUSALS_ENV, CATCHUP_REFUSALS_PROP, DEFAULT_CATCHUP_REFUSALS,
                MIN_CATCHUP_REFUSALS, MAX_CATCHUP_REFUSALS);
    }

    public static long catchupRefusalWaitMs() {
        return rangedSetting(CATCHUP_REFUSAL_WAIT_ENV, CATCHUP_REFUSAL_WAIT_PROP, DEFAULT_CATCHUP_REFUSAL_WAIT_MS,
                MIN_CATCHUP_REFUSAL_WAIT_MS, MAX_CATCHUP_REFUSAL_WAIT_MS);
    }

    /** One line through the node's log (the watchdogs' CRITICAL line). */
    void log(final String line) {
        log.accept(name + " " + line);
    }

    /**
     * Call right after {@code ConsensusModule.launch} (or {@code ClusteredMediaDriver.launch})
     * returns, before the service container launches: starts the {@link QuorumWatchdog} clock now, on the context's
     * election-state counter, with this lifecycle's quorum wait, and arms the {@link RejoinWatchdog} (with its
     * {@link PeerReachability} probe) and the {@link CatchupWatchdog} on the same counters. Both driver modes: the
     * watchdogs read counters, and their probes open clients of their own on the context's driver directory.
     *
     * @throws IllegalArgumentException before anything is armed, when the probe's participation stream
     *                                  ({@code ENGINE_REJOIN_PROBE_STREAM_ID}) equals the context's consensus stream
     */
    public QuorumWatchdog watchQuorum(final io.aeron.cluster.ConsensusModule.Context context) {
        // before anything is armed: on the consensus stream the probe would count a restoring peer as reachable
        final int probeStreamId = PeerReachability.probeStreamId();
        final String probeRefusal = PeerReachability.probeStreamRefusal(probeStreamId, context.consensusStreamId());
        if (probeRefusal != null) {
            throw new IllegalArgumentException(probeRefusal);
        }
        log.accept(name + " quorum watchdog armed: exit 1 if no leader is known within " + quorumWaitMs + " ms ("
                + QUORUM_WAIT_ENV + ")");
        final QuorumWatchdog quorum = QuorumWatchdog.of(this, context, quorumWaitMs);
        watchdogs.add(quorum::stop);
        quorum.startDaemon(name);
        watchRejoin(RejoinWatchdog.of(this, context, rejoinWaitMs, leaderElectWaitMs, rejoinPeerWindowMs,
                probeStreamId), "peer probe on participation stream " + probeStreamId + " ("
                + PeerReachability.PROBE_STREAM_ID_ENV + ")");
        watchCatchup(CatchupWatchdog.of(this, context, catchupRefusals, catchupRefusalWaitMs));
        return quorum;
    }

    /**
     * {@link #watchQuorum(io.aeron.cluster.ConsensusModule.Context)} on explicit suppliers (tests, other hosts).
     * Without a consensus context there is no peer probe: the rejoin watchdog runs without the reachability gate and
     * no catch-up watchdog is armed. A host that has its own evidence passes it to the overload below.
     */
    public QuorumWatchdog watchQuorum(final java.util.function.LongSupplier electionState,
                                      final java.util.function.LongSupplier elections) {
        return watchQuorum(electionState, elections, () -> true);
    }

    /** As above, with the host's own answer to "is a quorum of members reachable right now". */
    public QuorumWatchdog watchQuorum(final java.util.function.LongSupplier electionState,
                                      final java.util.function.LongSupplier elections,
                                      final java.util.function.BooleanSupplier quorumReachable) {
        final QuorumWatchdog quorum = new QuorumWatchdog(this, electionState, elections, quorumWaitMs);
        watchdogs.add(quorum::stop);
        quorum.startDaemon(name);
        watchRejoin(new RejoinWatchdog(this, electionState, elections, rejoinWaitMs, leaderElectWaitMs,
                quorumReachable, rejoinPeerWindowMs, null), "reachability from the host's supplier");
        return quorum;
    }

    /** The rejoin watchdog armed by the last {@code watchQuorum} call, or null (tests). */
    private volatile RejoinWatchdog rejoin;

    RejoinWatchdog rejoinWatchdog() {
        return rejoin;
    }

    private void watchRejoin(final RejoinWatchdog w, final String evidence) {
        log.accept(name + " rejoin watchdog armed: exit 1 if, after a leader was known, one election stays open "
                + "without a leader for " + rejoinWaitMs + " ms (" + REJOIN_WAIT_ENV + ") while a quorum of members"
                + " is reachable (evidence window " + rejoinPeerWindowMs + " ms, " + REJOIN_PEER_WINDOW_ENV + "), or a"
                + " leader elect stays in LEADER_LOG_REPLICATION or LEADER_READY for " + leaderElectWaitMs + " ms ("
                + LEADER_ELECT_WAIT_ENV + ") under the same condition; reachability evidence: " + evidence);
        watchdogs.add(w::stop);
        rejoin = w;
        w.startDaemon(name);
    }

    /** The catch-up watchdog armed by the last {@code watchQuorum(context)} call, or null. */
    private volatile CatchupWatchdog catchup;

    CatchupWatchdog catchupWatchdog() {
        return catchup;
    }

    private void watchCatchup(final CatchupWatchdog w) {
        log.accept(name + " catch-up watchdog armed: exit 1 when " + catchupRefusals + " catch-up attempts in a row"
                + " fail (" + CATCHUP_REFUSALS_ENV + ") or the catch-up has failed for " + catchupRefusalWaitMs
                + " ms (" + CATCHUP_REFUSAL_WAIT_ENV + ") and this member's log position is below the leader's log"
                + " start");
        watchdogs.add(w::stop);
        catchup = w;
        w.startDaemon(name);
    }

    static long rangedSetting(final String env, final String prop, final long defaultValue, final long min,
                              final long max) {
        return parseRanged(env, System.getenv(env), prop, System.getProperty(prop), defaultValue, min, max);
    }

    static long parseRanged(final String env, final String envValue, final String prop, final String propValue,
                            final long defaultValue, final long min, final long max) {
        final String raw;
        final String source;
        if (envValue != null && !envValue.isEmpty()) {
            raw = envValue;
            source = env;
        } else if (propValue != null && !propValue.isEmpty()) {
            raw = propValue;
            source = prop;
        } else {
            return defaultValue;
        }
        final long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(source + " must be a number in " + min + ".." + max + ", got '" + raw
                    + "'");
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(source + " must be " + min + ".." + max + ", got '" + raw + "'");
        }
        return value;
    }

    static long positiveSetting(final String env, final String prop, final long defaultValue) {
        return parsePositive(env, System.getenv(env), prop, System.getProperty(prop), defaultValue);
    }

    static long parsePositive(final String env, final String envValue, final String prop, final String propValue,
                              final long defaultValue) {
        final String raw;
        final String source;
        if (envValue != null && !envValue.isEmpty()) {
            raw = envValue;
            source = env;
        } else if (propValue != null && !propValue.isEmpty()) {
            raw = propValue;
            source = prop;
        } else {
            return defaultValue;
        }
        final long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(source + " must be a positive number of milliseconds, got '" + raw + "'");
        }
        if (value <= 0) {
            throw new IllegalArgumentException(source + " must be a positive number of milliseconds, got '" + raw + "'");
        }
        return value;
    }

    public Cause cause() {
        return stop.get().cause();
    }

    public boolean stopRequested() {
        return stopLatch.getCount() == 0;
    }

    /**
     * Main thread: block until a stop cause is recorded. An interrupt has no JVM shutdown behind it, so it is a fault
     * (exit 1), not a signal stop.
     */
    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (final InterruptedException e) {
            fatal(EXIT_FATAL, "main thread interrupted");
            Thread.currentThread().interrupt();
        }
        // The caller closes the components next: no watchdog may read a consensus counter from here on.
        for (final java.util.function.LongConsumer stopWatchdog : watchdogs) {
            stopWatchdog.accept(WATCHDOG_JOIN_MS);
        }
    }

    /** The consensus module's and the service container's termination hook. */
    public void onClusterTermination() {
        record(new Stop(Cause.CLUSTER_TERMINATED, EXIT_CLUSTER_TERMINATED, "cluster termination hook"));
    }

    /**
     * A fatal fault: record it (unless a signal stop is already in progress), wake the main thread and start the
     * backstop that halts with {@code status} after the fatal grace. Returns true when this call recorded the fault.
     */
    public boolean fatal(final int status, final String reason) {
        if (status == 0) {
            throw new IllegalArgumentException("a fatal fault never exits 0");
        }
        final Stop fault = new Stop(Cause.FATAL, status, reason);
        if (!record(fault)) {
            return false;
        }
        log.accept(name + " FATAL (exit " + status + "): " + reason + " - node exiting, backstop in "
                + fatalExitGraceMs + " ms");
        final Thread backstop = new Thread(() -> {
            if (!awaitQuietly(closedLatch, fatalExitGraceMs)) {
                log.accept(name + " fatal close exceeded " + fatalExitGraceMs + " ms - halting with " + status);
            }
            halt(status);
        }, name + "-fatal-exit");
        backstop.setDaemon(true);
        backstop.start();
        return true;
    }

    /**
     * A decided restart: record it (unless a stop is already in progress), wake the main thread, which closes the
     * components and exits with {@code status} ({@link #EXIT_RESTART}); the same backstop as {@link #fatal} bounds a
     * hanging close. Returns true when this call recorded the restart.
     */
    public boolean restart(final int status, final String reason) {
        if (status == 0 || status == EXIT_FATAL || status == EXIT_SIGNAL) {
            throw new IllegalArgumentException("a restart exits with its own status, never 0, 1 or 143: " + status);
        }
        if (!record(new Stop(Cause.RESTART, status, reason))) {
            return false;
        }
        log.accept(name + " RESTART (exit " + status + "): " + reason + " - node exiting for a supervised restart,"
                + " backstop in " + fatalExitGraceMs + " ms");
        final Thread backstop = new Thread(() -> {
            if (!awaitQuietly(closedLatch, fatalExitGraceMs)) {
                log.accept(name + " restart close exceeded " + fatalExitGraceMs + " ms - halting with " + status);
            }
            halt(status);
        }, name + "-restart-exit");
        backstop.setDaemon(true);
        backstop.start();
        return true;
    }

    /**
     * Classify an error the node's Aeron error handler saw. With an external driver, driver loss halts at once
     * ({@link #driverLost}) and any other FATAL Aeron error is a fatal fault (exit 1), as is an agent that
     * terminated unexpectedly; everything else is only logged by the caller. Call it on the thread that reported
     * the error, from the Aeron error handler itself, so the halt precedes that client's close.
     */
    public boolean onError(final Throwable t, final boolean externalDriver) {
        if (externalDriver && (isDriverLoss(t) || isClientTimeoutTermination(t))) {
            return driverLost(t);
        }
        if (externalDriver && t instanceof AeronException ae && ae.category() == AeronException.Category.FATAL) {
            return fatal(EXIT_FATAL, "FATAL Aeron error in EXTERNAL driver mode: " + t);
        }
        if (t instanceof AgentTerminationException ate && !ate.isExpected()) {
            if (driverLossSeen) {
                // A client whose driver is gone stops its conductor with this, then closes itself and unmaps the CnC:
                // keep this thread in the halt the driver loss started instead of letting it close the client.
                haltAfterDriverLoss(t);
                return false;
            }
            return fatal(EXIT_FATAL, "agent terminated unexpectedly: " + t);
        }
        return false;
    }

    /**
     * An Aeron client's conductor stopping because its client timed out on the driver: aeron 1.53
     * {@code ClientConductor.terminateConductor} marks the client terminating on a driver, client or service interval
     * timeout, and the next {@code doWork} throws a bare {@code AgentTerminationException}. It can reach the error
     * handler before the timeout itself does (the timeout is thrown to a client call on another thread, which reports
     * it later), so on its own it already means the driver is lost for this process.
     */
    static boolean isClientTimeoutTermination(final Throwable t) {
        if (!(t instanceof AgentTerminationException ate) || ate.isExpected() || ate.getMessage() != null) {
            return false;
        }
        final StackTraceElement[] trace = ate.getStackTrace();
        return trace.length > 0 && "io.aeron.ClientConductor".equals(trace[0].getClassName())
                && "doWork".equals(trace[0].getMethodName());
    }

    /** The errors after which the reporting Aeron client closes itself and unmaps the CnC. */
    public static boolean isDriverLoss(final Throwable t) {
        return t instanceof DriverTimeoutException
                || t instanceof ClientTimeoutException
                || t instanceof ConductorServiceTimeoutException;
    }

    /**
     * Driver loss (see the class rules): log, then halt on the calling thread before its Aeron client unmaps the
     * CnC. A fault already recorded keeps its status; a decided restart in progress keeps its status too
     * ({@link #EXIT_RESTART}: the supervisor restarts the node either way, and the restart was decided before the loss;
     * the components are not closed, so a host's close-time step, such as the replica's move of {@code cluster/}, does
     * not run and the next start decides again); a signal stop in progress halts with {@link #EXIT_SIGNAL}.
     * Returns true when this call recorded the fault. With a halter that returns (tests) the lifecycle then
     * behaves as after {@link #fatal}: the main thread's finish is a no-op halt.
     */
    boolean driverLost(final Throwable t) {
        final String reason = "media driver lost in EXTERNAL driver mode: " + t;
        final boolean recorded = record(new Stop(Cause.FATAL, EXIT_FATAL, reason));
        driverLossSeen = true;
        final int status = driverLossStatus();
        log.accept(name + " FATAL (exit " + status + "): " + reason + " - halting now, no component close: the "
                + "client unmaps the CnC under running agents (SIGSEGV), and nothing is durable without a driver");
        halt(status);
        return recorded;
    }

    /** The status a driver loss halts with: a recorded fault or restart keeps its own, a signal stop 143. */
    private int driverLossStatus() {
        final Stop s = stop.get();
        return s.cause() == Cause.FATAL || s.cause() == Cause.RESTART ? s.status() : EXIT_SIGNAL;
    }

    /**
     * Another client of the lost driver stopped its conductor after {@link #driverLost} ran: join the same halt on
     * this thread (it returns only once the halter has returned, which in production it never does).
     */
    private void haltAfterDriverLoss(final Throwable t) {
        // This thread can win the halt while the reporting thread still waits to print its own line (both log
        // through the same stream), so say why here too: the operator log must name the driver loss either way.
        final int status = driverLossStatus();
        log.accept(name + " FATAL (exit " + status + "): media driver lost in EXTERNAL driver mode (an Aeron client"
                + " stopped after the loss: " + t + ") - halting now, no component close");
        halt(status);
    }

    /**
     * A failure before or during the launch (no CnC within the driver wait, a launch exception): print it, then
     * finish. Unless a signal stop is in progress this halts with {@link #EXIT_FATAL}.
     */
    public void startupFailed(final Throwable t) {
        final StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        log.accept(name + " failed: " + sw);
        closeFailure = String.valueOf(t);
        fatal(EXIT_FATAL, "start, launch or close failed: " + t);
        finish();
    }

    /**
     * Main thread, after the components are closed: decide the exit. A fault halts with its status, a cluster
     * termination halts with 0; a signal stop returns (the JVM is already exiting with the signal's status).
     */
    public void finish() {
        final Stop s = stop.get();
        // Log before releasing the shutdown hook: once it returns the JVM may exit at any instant.
        final int status = switch (s.cause()) {
            case SIGNAL -> {
                final String failure = closeFailure;
                log.accept(failure == null ? name + " stopped cleanly on " + s.reason()
                        : name + " stopped on SIGTERM, close failed: " + failure);
                yield -1;
            }
            case FATAL -> {
                log.accept(name + " closed after FATAL: " + s.reason() + " - exit " + s.status());
                yield s.status();
            }
            case CLUSTER_TERMINATED -> {
                log.accept(name + " closed after " + s.reason() + " - exit " + s.status());
                yield s.status();
            }
            case RESTART -> {
                log.accept(name + " closed for a restart: " + s.reason() + " - exit " + s.status());
                yield s.status();
            }
            case NONE -> {
                // The main thread got here without a stop cause: the node is not running, so it must not look clean.
                log.accept(name + " main thread finished without a stop cause - exit " + EXIT_FATAL);
                yield EXIT_FATAL;
            }
        };
        System.out.flush();
        System.err.flush();
        closedLatch.countDown();
        if (status >= 0) {
            halt(status);
        }
    }

    /** JVM shutdown hook body (SIGTERM, SIGINT, System.exit): wake the main thread and bound its close. */
    void onJvmShutdown() {
        record(new Stop(Cause.SIGNAL, -1, "JVM shutdown (SIGTERM)"));
        if (!awaitQuietly(closedLatch, shutdownTimeoutMs)) {
            log.accept(name + " clean close exceeded " + shutdownTimeoutMs + " ms (" + SHUTDOWN_TIMEOUT_ENV
                    + ") - exiting without it");
        }
        // A fault recorded before the signal keeps its status: returning would let the JVM exit 143 before the
        // backstop halts with the fault's status.
        final Stop s = stop.get();
        if (s.cause() == Cause.FATAL || s.cause() == Cause.RESTART) {
            halt(s.status());
        }
    }

    /**
     * External driver mode: wait for the driver's cnc.dat for at most {@code timeoutMs}; throws on timeout or when a
     * stop is requested meanwhile.
     */
    public void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
        final File cncFile = new File(aeronDir, CncFileDescriptor.CNC_FILE);
        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!cncFile.exists()) {
            if (stopRequested()) {
                throw new IllegalStateException("stop requested while waiting for the external media driver at "
                        + aeronDir);
            }
            if (System.nanoTime() - deadlineNs > 0) {
                throw new IllegalStateException("No external media driver found at " + aeronDir + " after "
                        + timeoutMs + "ms (" + DRIVER_WAIT_ENV + ")");
            }
            try {
                stopLatch.await(100, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for external media driver at " + aeronDir, e);
            }
        }
    }

    private boolean record(final Stop next) {
        while (true) {
            final Stop current = stop.get();
            final boolean allowed = current.cause() == Cause.NONE
                    || (current.cause() == Cause.CLUSTER_TERMINATED && next.cause() == Cause.FATAL);
            if (!allowed) {
                return false;
            }
            if (stop.compareAndSet(current, next)) {
                stopLatch.countDown();
                return true;
            }
        }
    }

    /**
     * One halt per process: the main thread's and the backstop's are the same decision. A thread that loses the
     * halt waits for the winner's halter instead of returning: the main thread returning from {@code main} while the
     * winner is still on its way to {@link Runtime#halt} ends the JVM with status 0 (the shutdown hook's own halt is
     * then the loser too), so a fatal fault would look like a clean exit.
     */
    private void halt(final int status) {
        if (!halted.compareAndSet(false, true)) {
            boolean interrupted = false;
            while (true) {
                try {
                    haltReturned.await();
                    break;
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        try {
            System.out.flush();
            System.err.flush();
            halter.accept(status);
        } finally {
            haltReturned.countDown();
        }
    }

    private static boolean awaitQuietly(final CountDownLatch latch, final long timeoutMs) {
        try {
            return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return latch.getCount() == 0;
        }
    }
}
