// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Counter;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * RC2 S17 W12 (R-KIT-Q): a bounded, loud exit when the consensus module never learns of a leader. Nothing else
 * bounds "no leader ever": a lone member of a multi-member cluster (its peers halted on a refused state, or never
 * started) stays in canvass and looks merely unready.
 *
 * <p>Rule: the ConsensusModule election-state counter has stayed in {@link #NO_LEADER_STATES} without interruption
 * for the quorum wait, measured from {@link #start}; then {@link EngineLifecycle#fatal} with exit 1. Any
 * {@code LEADER_*} or {@code FOLLOWER_*} state other than FOLLOWER_BALLOT (a leader exists: log replication, replay,
 * catch-up, init, ready) or CLOSED disarms the watchdog for the process lifetime: the replay and the catch-up run
 * inside the election, so they never trip it, and a later leader loss is an outage, not a foreign state start. A
 * value outside every known state (counter not readable yet) stops the clock; the next entry into the set restarts
 * it. {@link #start} is called by the launcher when {@code ConsensusModule.launch} returns, so the DNS wait and the
 * external-driver wait are not counted.
 *
 * <p>INIT is not in the set (RC2 S17 W12 review V1): aeron 1.53.1 allocates the election-state counter at INIT in
 * {@code Context.conclude}, and {@code ConsensusModuleAgent.onStart} stays there (recoverFromSnapshotAndLog) until the
 * service container acknowledges its own onStart, i.e. for the whole service snapshot load. INIT stops the clock
 * like an unreadable counter; the clock starts at the first entry into CANVASS..FOLLOWER_BALLOT. A lone member of a
 * log-only start leaves INIT for CANVASS at once, so it still trips one wait later.
 *
 * <p>The poller stops before the components close: {@link EngineLifecycle#awaitStop} calls {@link #stop} (review V2),
 * which takes the poll lock, so no counter is read once the main thread starts closing the consensus module.
 *
 * <p>The wait is a process setting ({@link EngineLifecycle#QUORUM_WAIT_ENV}), never replicated state.
 */
public final class QuorumWatchdog {
    /** The election states in which no leader is known (aeron 1.53; pinned by QuorumWatchdogTest). */
    public static final Set<ElectionState> NO_LEADER_STATES = Collections.unmodifiableSet(EnumSet.of(
            ElectionState.CANVASS, ElectionState.NOMINATE, ElectionState.CANDIDATE_BALLOT, ElectionState.FOLLOWER_BALLOT));
    /** Value the context supplier reports while the counter is absent or closed. */
    public static final long STATE_UNAVAILABLE = -1;
    static final long POLL_INTERVAL_MS = 1_000;

    private final EngineLifecycle lifecycle;
    private final LongSupplier electionState;
    private final LongSupplier elections;
    private final long waitMs;
    private final long waitNs;
    private volatile boolean started;
    private volatile boolean disarmed;
    private volatile boolean tripped;
    private volatile boolean stopped;
    private volatile Thread poller;
    private boolean inSet;
    private long enteredNs;

    QuorumWatchdog(final EngineLifecycle lifecycle, final LongSupplier electionState, final LongSupplier elections,
                   final long waitMs) {
        if (waitMs <= 0) {
            throw new IllegalArgumentException("quorum wait must be positive");
        }
        this.lifecycle = lifecycle;
        this.electionState = electionState;
        this.elections = elections;
        this.waitMs = waitMs;
        this.waitNs = TimeUnit.MILLISECONDS.toNanos(waitMs);
    }

    public static boolean isNoLeader(final long code) {
        final ElectionState s = stateOf(code);
        return s != null && NO_LEADER_STATES.contains(s);
    }

    public static boolean isLeaderKnown(final long code) {
        final ElectionState s = stateOf(code);
        return s != null && s != ElectionState.INIT && !NO_LEADER_STATES.contains(s);
    }

    private static ElectionState stateOf(final long code) {
        for (final ElectionState s : ElectionState.values()) {
            if (s.code() == code) {
                return s;
            }
        }
        return null;
    }

    /** The launcher's suppliers: the context's election-state and election counters, read in place. */
    static LongSupplier counterValue(final Counter counter, final long closedValue) {
        return () -> counter == null ? STATE_UNAVAILABLE : counter.isClosed() ? closedValue : counter.get();
    }

    static QuorumWatchdog of(final EngineLifecycle lifecycle, final ConsensusModule.Context context,
                             final long waitMs) {
        // A closed election-state counter means the consensus module closed: the node is stopping, disarm.
        return new QuorumWatchdog(lifecycle,
                counterValue(context.electionStateCounter(), ElectionState.CLOSED.code()),
                counterValue(context.electionCounter(), STATE_UNAVAILABLE), waitMs);
    }

    /** Arm the clock: the consensus module has just launched. */
    public synchronized void start(final long nowNs) {
        if (started) {
            return;
        }
        started = true;
        // the clock runs from launch only if the election is already past INIT (it normally is not)
        inSet = isNoLeader(electionState.getAsLong());
        enteredNs = nowNs;
    }

    /**
     * Stop polling for good, under the poll lock (no counter read after this returns), and join the poller for at
     * most {@code joinMs}. Called by {@link EngineLifecycle#awaitStop} before the components close.
     */
    public void stop(final long joinMs) {
        synchronized (this) {
            stopped = true;
        }
        final Thread t = poller;
        if (t != null && t != Thread.currentThread()) {
            t.interrupt();
            try {
                t.join(joinMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean stopped() {
        return stopped;
    }

    boolean pollerAlive() {
        final Thread t = poller;
        return t != null && t.isAlive();
    }

    public boolean disarmed() {
        return disarmed;
    }

    public boolean tripped() {
        return tripped;
    }

    /** One observation; true when this call tripped the watchdog (the lifecycle's fatal fault is recorded). */
    public synchronized boolean poll(final long nowNs) {
        if (!started || disarmed || tripped || stopped) {
            return false;
        }
        final long code = electionState.getAsLong();
        if (isLeaderKnown(code)) {
            disarmed = true;
            return false;
        }
        if (!isNoLeader(code)) {
            inSet = false;
            return false;
        }
        if (!inSet) {
            inSet = true;
            enteredNs = nowNs;
        }
        if (nowNs - enteredNs < waitNs) {
            return false;
        }
        tripped = true;
        final ElectionState s = stateOf(code);
        lifecycle.fatal(EngineLifecycle.EXIT_FATAL, "no cluster quorum observed within " + waitMs
                + " ms since the consensus module launched (election state " + s + ", elections "
                + elections.getAsLong() + ")");
        return true;
    }

    /** Start the clock now and poll once a second on a daemon thread until disarmed, tripped or stopping. */
    QuorumWatchdog startDaemon(final String name) {
        start(System.nanoTime());
        final Thread t = new Thread(() -> {
            while (!disarmed && !tripped && !stopped && !lifecycle.stopRequested()) {
                if (poll(System.nanoTime())) {
                    return;
                }
                try {
                    Thread.sleep(Math.min(POLL_INTERVAL_MS, Math.max(1, waitMs / 4)));
                } catch (final InterruptedException e) {
                    return;
                }
            }
        }, name + "-quorum-watchdog");
        t.setDaemon(true);
        poller = t;
        t.start();
        return this;
    }
}
