// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * A bounded, loud exit when a member that has known a leader cannot rejoin: the counterpart of
 * {@link QuorumWatchdog}, which disarms for the process lifetime once a leader has been seen.
 *
 * <p>Why: a member can stay in election without a bound after a short partition. Aeron's C media driver 1.53.1
 * ({@code aeron_data_packet_dispatcher_elicit_setup_from_source}) returns before it registers the pending setup when
 * the status message that asks a peer for a setup frame fails to send (EPERM from a host firewall, an unreachable
 * route while inbound traffic is already back). Every later heartbeat from that peer is then ignored, the member
 * canvasses for ever and never hears the new leader. Only a restart clears it.
 *
 * <p>Rule: the watchdog is armed by the first observation of a leader-known election state or CLOSED
 * ({@link QuorumWatchdog#isLeaderKnown}). From then on, every continuous stretch spent in a no-leader election state is timed from its first observation open
 * (any state other than CLOSED) until it is observed CLOSED, or until the election counter moves (Aeron counts an
 * election when it starts one, and starts one only after the previous one closed, so a moved counter means the
 * earlier election closed between two polls). The watchdog trips when one election has been open for the rejoin
 * wait and its current state is a no-leader state ({@link QuorumWatchdog#NO_LEADER_STATES}: CANVASS, NOMINATE,
 * CANDIDATE_BALLOT, FOLLOWER_BALLOT) or INIT (an election's opening state, which a running member leaves at once). A
 * member in a leader-known state (catch-up, log replication, replay, ready) is progressing and never trips, however
 * long that takes; if it falls back to canvass after the wait it trips at the next poll. A counter value outside
 * every known state (not readable) never trips and keeps the clock.
 *
 * <p>On a trip it logs one {@code CRITICAL} line naming the election state, the elapsed time and the setting, then
 * records {@link EngineLifecycle#fatal} with {@link EngineLifecycle#EXIT_FATAL} (exit 1): the supervisor restarts the
 * member, which rejoins through its own log and the leader's catch-up.
 *
 * <p>The wait is a process setting ({@link EngineLifecycle#REJOIN_WAIT_ENV}, default
 * {@link EngineLifecycle#DEFAULT_REJOIN_WAIT_MS}), never replicated state. The poller stops before the components
 * close, like the quorum watchdog's ({@link EngineLifecycle#awaitStop}).
 */
public final class RejoinWatchdog {
    private final EngineLifecycle lifecycle;
    private final LongSupplier electionState;
    private final LongSupplier elections;
    private final long waitMs;
    private final long waitNs;
    private volatile boolean leaderKnown;
    private volatile boolean tripped;
    private volatile boolean stopped;
    private volatile Thread poller;
    private boolean open;
    private long openedNs;
    private long openElection;

    RejoinWatchdog(final EngineLifecycle lifecycle, final LongSupplier electionState, final LongSupplier elections,
                   final long waitMs) {
        if (waitMs <= 0) {
            throw new IllegalArgumentException("rejoin wait must be positive");
        }
        this.lifecycle = lifecycle;
        this.electionState = electionState;
        this.elections = elections;
        this.waitMs = waitMs;
        this.waitNs = TimeUnit.MILLISECONDS.toNanos(waitMs);
    }

    static RejoinWatchdog of(final EngineLifecycle lifecycle, final ConsensusModule.Context context,
                             final long waitMs) {
        // A closed election-state counter means the consensus module closed: it reads CLOSED and never trips.
        return new RejoinWatchdog(lifecycle,
                QuorumWatchdog.counterValue(context.electionStateCounter(), ElectionState.CLOSED.code()),
                QuorumWatchdog.counterValue(context.electionCounter(), QuorumWatchdog.STATE_UNAVAILABLE), waitMs);
    }

    /** The states in which an election open for the wait trips the watchdog (once a leader was known). */
    static boolean isRejoinWait(final long code) {
        return code == ElectionState.INIT.code() || QuorumWatchdog.isNoLeader(code);
    }

    public boolean leaderKnown() {
        return leaderKnown;
    }

    public boolean tripped() {
        return tripped;
    }

    public boolean stopped() {
        return stopped;
    }

    boolean pollerAlive() {
        final Thread t = poller;
        return t != null && t.isAlive();
    }

    /** Stop polling for good, under the poll lock, and join the poller for at most {@code joinMs}. */
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

    /** One observation; true when this call tripped the watchdog (the lifecycle's fatal fault is recorded). */
    public synchronized boolean poll(final long nowNs) {
        if (tripped || stopped) {
            return false;
        }
        final long code = electionState.getAsLong();
        if (!leaderKnown) {
            if (!QuorumWatchdog.isLeaderKnown(code)) {
                return false; // never knew a leader: the quorum watchdog's case
            }
            leaderKnown = true;
        }
        if (!isRejoinWait(code)) {
            open = false; // closed, or a state with a leader (catch-up, leader states): the clock restarts
            return false;
        }
        final long election = elections.getAsLong();
        if (!open || election != openElection) {
            // the continuous stretch without a leader starts here: a catch-up that falls back to canvass inside
            // the same election gets the full wait for a normal re-election
            open = true;
            openedNs = nowNs;
            openElection = election;
        }
        final long openNs = nowNs - openedNs;
        if (openNs < waitNs) {
            return false;
        }
        tripped = true;
        final String state = stateName(code);
        final long openMs = TimeUnit.NANOSECONDS.toMillis(openNs);
        lifecycle.log("CRITICAL rejoin watchdog: election " + election + " open for " + openMs
                + " ms after a leader was known, still in election state " + state + " (wait " + waitMs + " ms, "
                + EngineLifecycle.REJOIN_WAIT_ENV + "); exiting " + EngineLifecycle.EXIT_FATAL
                + " for a supervised restart");
        lifecycle.fatal(EngineLifecycle.EXIT_FATAL, "member did not rejoin: election state " + state + " for "
                + openMs + " ms after a leader was known");
        return true;
    }

    private static String stateName(final long code) {
        for (final ElectionState s : ElectionState.values()) {
            if (s.code() == code) {
                return s.name();
            }
        }
        return String.valueOf(code);
    }

    /** Poll every second (at most a quarter of the wait) on a daemon thread until tripped or stopping. */
    RejoinWatchdog startDaemon(final String name) {
        final Thread t = new Thread(() -> {
            while (!tripped && !stopped && !lifecycle.stopRequested()) {
                if (poll(System.nanoTime())) {
                    return;
                }
                try {
                    Thread.sleep(Math.min(QuorumWatchdog.POLL_INTERVAL_MS, Math.max(1, waitMs / 4)));
                } catch (final InterruptedException e) {
                    return;
                }
            }
        }, name + "-rejoin-watchdog");
        t.setDaemon(true);
        poller = t;
        t.start();
        return this;
    }
}
