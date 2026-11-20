// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
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
 * follower in a leader-known state (catch-up, log replication, replay, ready) is progressing and never trips, however
 * long that takes (Aeron times those states out back to canvass); if it falls back to canvass after the wait it trips
 * at the next poll. A leader elect has its own rule, below. A counter value outside
 * every known state (not readable) never trips and keeps the clock.
 *
 * <p>Leader elect: in Aeron 1.53 a leader elect leaves LEADER_LOG_REPLICATION and LEADER_READY only once a quorum of
 * followers reports its append position ({@code Election.leaderLogReplication}, {@code Election.leaderReady}); there
 * is no timeout. A member whose voter vanished after the ballot, or whose followers cannot hear it, stays there for
 * ever. Those two states are leader-known (they arm the watchdog) and are timed on a clock of their own, against the
 * longer leader elect wait ({@link EngineLifecycle#LEADER_ELECT_WAIT_ENV}): one continuous stretch in one of the two
 * states within one election. Any other state (LEADER_REPLAY, LEADER_INIT, CLOSED, a no-leader state), a move from one
 * of the two to the other (it passed LEADER_INIT between two polls) or a moved election counter starts a new stretch;
 * a fall back to a no-leader state is timed against the rejoin wait from there. The same reachability gate applies,
 * with its own evidence for each stretch.
 *
 * <p>Reachability gate: a restart only helps when the peers are there. The watchdog trips only when it has evidence
 * that a quorum of members is reachable ({@link PeerReachability}: this member plus the peers that answer its
 * probes on their participation stream), and the wait is counted over the time that evidence has held: the clock
 * starts at the later of the election's opening and the moment the quorum became reachable, and it runs up to the
 * last positive answer. A gap in the evidence shorter than the reachability window
 * ({@link EngineLifecycle#REJOIN_PEER_WINDOW_ENV}) does not restart the clock, and is counted once the evidence is
 * back; time after the last positive answer is never counted, so the window cannot carry a member whose peers just
 * died over the wait. Once the last evidence is older
 * than the window the clock is cleared. A peer that died still reads reachable until the driver's publication
 * connection timeout passes; the window is to be set no shorter than that timeout, and the wait has to exceed the
 * window ({@link EngineLifecycle#rejoinSettingsRefusal}, checked at boot). During a genuine quorum loss (two of three
 * members gone) the survivor therefore stays up without a bound, not ready, and may serve a recovering peer's
 * restore; one line says so when the wait passes. When the peers return it gets a full wait to finish the election.
 *
 * <p>A peer counts as reachable only once its consensus module has left startup recovery: each member opens its
 * participation subscription when its own module state leaves INIT, from the poller's tick (every poll, whether or
 * not an election is open). Aeron's consensus module subscribes to the consensus stream when it is constructed but
 * cannot vote until its clustered services have loaded their snapshots, which can take minutes; a peer in that
 * restore reads unreachable, so the survivor of a quorum loss stays up while the returning peer restores, possibly
 * from it, and the wait starts when the peer is running. A running peer, including one in the stuck election above,
 * reads reachable (its module state is ACTIVE). A peer on an older kit has no participation subscription and reads
 * unreachable: with mixed versions the watchdog does not trip.
 *
 * <p>On a trip it logs one {@code CRITICAL} line naming the election state, the elapsed time, the time a quorum was
 * reachable and the setting, then records {@link EngineLifecycle#fatal} with {@link EngineLifecycle#EXIT_FATAL}
 * (exit 1): the supervisor restarts the member, which rejoins through its own log and the leader's catch-up.
 *
 * <p>The waits are process settings ({@link EngineLifecycle#REJOIN_WAIT_ENV}, default
 * {@link EngineLifecycle#DEFAULT_REJOIN_WAIT_MS}; {@link EngineLifecycle#LEADER_ELECT_WAIT_ENV}, default
 * {@link EngineLifecycle#DEFAULT_LEADER_ELECT_WAIT_MS}), never replicated state. The poller stops before the components
 * close, like the quorum watchdog's ({@link EngineLifecycle#awaitStop}).
 */
public final class RejoinWatchdog {
    private final EngineLifecycle lifecycle;
    private final LongSupplier electionState;
    private final LongSupplier elections;
    private final long waitMs;
    private final long waitNs;
    private final long leaderElectWaitMs;
    private final long leaderElectWaitNs;
    private final BooleanSupplier quorumReachable;
    private final long windowNs;
    private final AutoCloseable onStop;
    /** Run first in every poll under the poll lock, whatever the election state (the probe's tick), or null. */
    private final Runnable tick;
    private volatile boolean leaderKnown;
    private volatile boolean tripped;
    private volatile boolean stopped;
    private volatile Thread poller;
    private boolean open;
    /** Whether the open stretch is a leader elect's wait (else an election without a leader). */
    private boolean openLeaderElect;
    /** The leader elect state the open stretch is in: a move between the two passed LEADER_INIT, a new stretch. */
    private long openCode;
    private long openedNs;
    private long openElection;
    /** Whether quorum evidence has been seen in this stretch and is not older than the window. */
    private boolean reachable;
    private long reachableSinceNs;
    private long lastEvidenceNs;
    private boolean quorumLossLogged;

    /** No reachability gate: every poll counts as evidence (the rule before the gate; explicit-supplier hosts). */
    RejoinWatchdog(final EngineLifecycle lifecycle, final LongSupplier electionState, final LongSupplier elections,
                   final long waitMs) {
        this(lifecycle, electionState, elections, waitMs, () -> true, EngineLifecycle.DEFAULT_REJOIN_PEER_WINDOW_MS,
                null);
    }

    /** The leader elect wait at its default ({@link EngineLifecycle#DEFAULT_LEADER_ELECT_WAIT_MS}). */
    RejoinWatchdog(final EngineLifecycle lifecycle, final LongSupplier electionState, final LongSupplier elections,
                   final long waitMs, final BooleanSupplier quorumReachable, final long windowMs,
                   final AutoCloseable onStop) {
        this(lifecycle, electionState, elections, waitMs, EngineLifecycle.DEFAULT_LEADER_ELECT_WAIT_MS,
                quorumReachable, windowMs, onStop);
    }

    /**
     * @param leaderElectWaitMs how long a leader elect may stay in LEADER_LOG_REPLICATION or LEADER_READY while a
     *                        quorum of members is reachable
     * @param quorumReachable asked once per poll while a watched stretch is open (an election without a leader, or
     *                        a waiting leader elect), under the poll lock: true when a quorum of members is
     *                        reachable right now
     * @param windowMs        how long one positive answer keeps counting as evidence
     * @param onStop          closed by {@link #stop} under the poll lock (the probe behind the supplier), or null
     */
    RejoinWatchdog(final EngineLifecycle lifecycle, final LongSupplier electionState, final LongSupplier elections,
                   final long waitMs, final long leaderElectWaitMs, final BooleanSupplier quorumReachable,
                   final long windowMs, final AutoCloseable onStop) {
        this(lifecycle, electionState, elections, waitMs, leaderElectWaitMs, quorumReachable, windowMs, onStop, null);
    }

    /**
     * As above, with {@code tick} run at the start of every poll under the poll lock, whether or not an election is
     * open or a leader was ever known: the probe opens this member's participation subscription from it
     * ({@link PeerReachability#tick}).
     */
    RejoinWatchdog(final EngineLifecycle lifecycle, final LongSupplier electionState, final LongSupplier elections,
                   final long waitMs, final long leaderElectWaitMs, final BooleanSupplier quorumReachable,
                   final long windowMs, final AutoCloseable onStop, final Runnable tick) {
        if (waitMs <= 0) {
            throw new IllegalArgumentException("rejoin wait must be positive");
        }
        if (leaderElectWaitMs <= 0) {
            throw new IllegalArgumentException("leader elect wait must be positive");
        }
        if (windowMs <= 0) {
            throw new IllegalArgumentException("rejoin peer window must be positive");
        }
        this.lifecycle = lifecycle;
        this.electionState = electionState;
        this.elections = elections;
        this.waitMs = waitMs;
        this.waitNs = TimeUnit.MILLISECONDS.toNanos(waitMs);
        this.leaderElectWaitMs = leaderElectWaitMs;
        this.leaderElectWaitNs = TimeUnit.MILLISECONDS.toNanos(leaderElectWaitMs);
        this.quorumReachable = java.util.Objects.requireNonNull(quorumReachable, "quorumReachable");
        this.windowNs = TimeUnit.MILLISECONDS.toNanos(windowMs);
        this.onStop = onStop;
        this.tick = tick;
    }

    static RejoinWatchdog of(final EngineLifecycle lifecycle, final ConsensusModule.Context context,
                             final long waitMs, final long leaderElectWaitMs, final long windowMs,
                             final int probeStreamId) {
        final PeerReachability peers = PeerReachability.of(context, lifecycle::log, probeStreamId);
        // A closed election-state counter means the consensus module closed: it reads CLOSED and never trips.
        return new RejoinWatchdog(lifecycle,
                QuorumWatchdog.counterValue(context.electionStateCounter(), ElectionState.CLOSED.code()),
                QuorumWatchdog.counterValue(context.electionCounter(), QuorumWatchdog.STATE_UNAVAILABLE), waitMs,
                leaderElectWaitMs, peers, windowMs, peers, peers::tick);
    }

    /** The states in which an election open for the wait trips the watchdog (once a leader was known). */
    static boolean isRejoinWait(final long code) {
        return code == ElectionState.INIT.code() || QuorumWatchdog.isNoLeader(code);
    }

    /**
     * The leader elect states Aeron leaves only once a quorum of followers reports its append position, with no
     * timeout (1.53 {@code Election.leaderLogReplication}, {@code Election.leaderReady}).
     */
    static boolean isLeaderElectWait(final long code) {
        return code == ElectionState.LEADER_LOG_REPLICATION.code() || code == ElectionState.LEADER_READY.code();
    }

    /** The reachability evidence this watchdog asks (the production {@link PeerReachability} probe, tests). */
    BooleanSupplier quorumReachable() {
        return quorumReachable;
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
            if (onStop != null) {
                try {
                    onStop.close();
                } catch (final Exception e) {
                    lifecycle.log("rejoin watchdog: closing the peer reachability probe failed: " + e);
                }
            }
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
        if (tick != null) {
            try {
                tick.run();
            } catch (final RuntimeException e) {
                lifecycle.log("rejoin watchdog: the peer reachability probe's tick failed: " + e);
            }
        }
        final long code = electionState.getAsLong();
        if (!leaderKnown) {
            if (!QuorumWatchdog.isLeaderKnown(code)) {
                return false; // never knew a leader: the quorum watchdog's case
            }
            leaderKnown = true;
        }
        final boolean leaderElect = isLeaderElectWait(code);
        if (!leaderElect && !isRejoinWait(code)) {
            // closed, or a state with a leader that is progressing (catch-up, replay, LEADER_INIT): the clock restarts
            clear();
            return false;
        }
        if (open && leaderElect != openLeaderElect) {
            clear(); // the other kind of stretch: its own clock and its own evidence
        }
        final long election = elections.getAsLong();
        if (!open || election != openElection || (leaderElect && code != openCode)) {
            // the continuous stretch starts here: a catch-up that falls back to canvass inside the same election gets
            // the full wait for a normal re-election, and a leader elect that reached LEADER_READY through
            // LEADER_INIT gets the full wait again
            open = true;
            openLeaderElect = leaderElect;
            openCode = code;
            openedNs = nowNs;
            openElection = election;
        }
        final long limitNs = leaderElect ? leaderElectWaitNs : waitNs;
        final boolean evidence;
        try {
            evidence = quorumReachable.getAsBoolean();
        } catch (final RuntimeException e) {
            lifecycle.log("rejoin watchdog: the peer reachability check failed: " + e);
            return false;
        }
        if (evidence) {
            if (!reachable) {
                reachable = true;
                reachableSinceNs = nowNs;
            }
            lastEvidenceNs = nowNs;
        } else if (reachable && nowNs - lastEvidenceNs > windowNs) {
            reachable = false; // the last evidence left the window: the peers are gone, the clock is cleared
        }
        final long openNs = nowNs - openedNs;
        if (!reachable) {
            if (openNs >= limitNs && !quorumLossLogged) {
                quorumLossLogged = true;
                if (leaderElect) {
                    lifecycle.log("rejoin watchdog: leader elect in election " + election + ", election state "
                            + stateName(code) + " for " + TimeUnit.NANOSECONDS.toMillis(openNs) + " ms, and no"
                            + " quorum of members is reachable; not exiting, a restart cannot repair a quorum loss."
                            + " The wait (" + EngineLifecycle.LEADER_ELECT_WAIT_ENV + ") starts when a quorum is"
                            + " reachable again");
                } else {
                    lifecycle.log("rejoin watchdog: election " + election + " open for "
                            + TimeUnit.NANOSECONDS.toMillis(openNs) + " ms without a leader, and no quorum of members"
                            + " is reachable; not exiting, a restart cannot repair a quorum loss. The wait ("
                            + EngineLifecycle.REJOIN_WAIT_ENV + ") starts when a quorum is reachable again");
                }
            }
            return false;
        }
        // the wait counts only time with the quorum reachable, up to the last positive answer: peers that return
        // get a full wait to elect, and the window after the last answer is not evidence
        final long stuckNs = lastEvidenceNs - Math.max(openedNs, reachableSinceNs);
        if (stuckNs < limitNs) {
            return false;
        }
        tripped = true;
        final String state = stateName(code);
        final long openMs = TimeUnit.NANOSECONDS.toMillis(openNs);
        final long stuckMs = TimeUnit.NANOSECONDS.toMillis(stuckNs);
        if (leaderElect) {
            lifecycle.log("CRITICAL rejoin watchdog: leader elect in election " + election + " still in election state "
                    + state + " for " + openMs + " ms, no quorum of followers has reported its position, with a"
                    + " quorum of members reachable for " + stuckMs + " ms (wait " + leaderElectWaitMs + " ms, "
                    + EngineLifecycle.LEADER_ELECT_WAIT_ENV + "); exiting " + EngineLifecycle.EXIT_FATAL
                    + " for a supervised restart");
            lifecycle.fatal(EngineLifecycle.EXIT_FATAL, "leader elect did not complete election " + election
                    + ": election state " + state + " for " + openMs + " ms, a quorum of members reachable for "
                    + stuckMs + " ms");
            return true;
        }
        lifecycle.log("CRITICAL rejoin watchdog: election " + election + " open for " + openMs
                + " ms after a leader was known, still in election state " + state + " with a quorum of members"
                + " reachable for " + stuckMs + " ms (wait " + waitMs + " ms, "
                + EngineLifecycle.REJOIN_WAIT_ENV + "); exiting " + EngineLifecycle.EXIT_FATAL
                + " for a supervised restart");
        lifecycle.fatal(EngineLifecycle.EXIT_FATAL, "member did not rejoin: election state " + state + " for "
                + openMs + " ms after a leader was known, a quorum of members reachable for " + stuckMs + " ms");
        return true;
    }

    private void clear() {
        open = false;
        reachable = false;
        quorumLossLogged = false;
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
                    Thread.sleep(Math.min(QuorumWatchdog.POLL_INTERVAL_MS,
                            Math.max(1, Math.min(waitMs, leaderElectWaitMs) / 4)));
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
