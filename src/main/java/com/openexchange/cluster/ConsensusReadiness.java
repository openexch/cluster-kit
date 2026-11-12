// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Counter;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;

/**
 * Read the already-open local consensus context from the service duty cycle.
 * No counter scan, blocking IPC query, file mapping, sleep, or lock is performed here.
 * A bounded untethered subscription observes current-term leader commit messages;
 * local commit equality alone cannot prove that a follower has received the log.
 * Callers suppress this poll during application callback reentry.
 */
public final class ConsensusReadiness implements AutoCloseable {
    public static final long SAMPLE_INTERVAL_NS = 10_000_000;
    private final ConsensusModule.Context context;
    private final NodeReadiness readiness;
    private final ConsensusCommitObserver observer;
    private final java.util.function.LongFunction<LeaderCheckpoint> leaderCheckpoint;
    private long leaderSource = -1;
    private long lastSampleNs;
    private boolean sampled;
    private volatile LeaderCheckpoint leader;

    /**
     * The leader's latest {@code CommitPosition} for a term as the observer decoded it: the term, the image it came
     * on, the position, when it was observed and the leader member id the message names ({@code -1} when the source
     * did not name one).
     */
    public record LeaderCheckpoint(long term, long source, long position, long observedNs, int leaderMemberId) {
        /** A checkpoint without a leader id (the test seams that predate it): {@link #leader()} never returns it. */
        public LeaderCheckpoint(final long term, final long source, final long position, final long observedNs) {
            this(term, source, position, observedNs, -1);
        }
    }

    public ConsensusReadiness(final ConsensusModule.Context context, final NodeReadiness readiness) {
        this.context = context;
        this.readiness = readiness;
        this.observer = new ConsensusCommitObserver(context);
        this.leaderCheckpoint = observer::poll;
    }

    /** Quantitative source seam for deterministic tests; production uses the Aeron observer. */
    public ConsensusReadiness(final ConsensusModule.Context context, final NodeReadiness readiness,
                             final java.util.function.LongFunction<LeaderCheckpoint> leaderCheckpoint) {
        this.context = context; this.readiness = readiness;
        this.observer = null; this.leaderCheckpoint = java.util.Objects.requireNonNull(leaderCheckpoint);
    }

    public void start(final io.aeron.Aeron aeron) { if (observer != null) { observer.start(aeron); } }
    @Override public void close() { leader = null; if (observer != null) { observer.close(); } }

    /**
     * The current term's leader as the leader's own {@code CommitPosition} names it: the checkpoint the last poll
     * accepted on a FOLLOWER with the election CLOSED, for the term the local consensus counters report, carrying a
     * leader member id. Null before the first such poll, after {@code leader-checkpoint-unavailable}, while the term
     * is ambiguous (two members claimed it: the observer withholds the checkpoint), on a LEADER, during an election
     * and while the consensus counters are unavailable or changing. A read of state readiness already computes, never
     * a readiness input. Written and meant to be read on the service thread that polls; volatile for exporters.
     */
    public LeaderCheckpoint leader() { return leader; }

    public void poll(final Cluster cluster, final long nowNs) {
        readiness.tick();
        readiness.roleChanged(cluster.role()); // includes initial FOLLOWER without a callback
        if (sampled && nowNs - lastSampleNs >= 0 && nowNs - lastSampleNs < SAMPLE_INTERVAL_NS) {
            return;
        }
        sampled = true;
        lastSampleNs = nowNs;
        final Counter commit = context.commitPositionCounter();
        final Counter term = context.leadershipTermIdCounter();
        final Counter elections = context.electionCounter();
        final Counter election = context.electionStateCounter();
        final Counter module = context.moduleStateCounter();
        final Counter role = context.clusterNodeRoleCounter();
        final ClusterMarkFile mark = context.clusterMarkFile();
        if (absent(commit) || absent(term) || absent(elections) || absent(election)
                || absent(module) || absent(role) || mark == null || mark.isClosed()) {
            leader = null;
            readiness.unavailable("consensus-unavailable");
            return;
        }
        final long termId = term.get();
        final long electionCount = elections.get();
        final long electionState = election.get();
        final long moduleState = module.get();
        final long consensusRole = role.get();
        final long localCommit = commit.get();
        final long observedNs = System.nanoTime();
        final LeaderCheckpoint remote = leaderCheckpoint.apply(observedNs);
        final long heartbeat = mark.activityTimestampVolatile();
        final long heartbeatAge = context.clusterClock().timeMillis() - heartbeat;
        final long applied = cluster.logPosition();
        if (termId != term.get() || electionCount != elections.get() || electionState != election.get()
                || moduleState != module.get() || consensusRole != role.get()
                || absent(commit) || absent(term) || absent(elections) || absent(election)
                || absent(module) || absent(role) || mark.isClosed()) {
            leader = null;
            readiness.unavailable("consensus-changing");
            return;
        }
        long committed = localCommit;
        long authorityAge = heartbeat <= 0 ? -1 : heartbeatAge;
        LeaderCheckpoint accepted = null;
        if (cluster.role() == Cluster.Role.FOLLOWER && electionState == ElectionState.CLOSED.code()) {
            if (remote == null || remote.term() != termId || remote.source() < 0 || remote.position() < 0
                    || observedNs - remote.observedNs() < 0) {
                leader = null;
                readiness.unavailable("leader-checkpoint-unavailable"); return;
            }
            accepted = remote.leaderMemberId() >= 0 ? remote : null;
            if (leaderSource != remote.source()) { readiness.catchingUp(); leaderSource = remote.source(); }
            committed = Math.max(localCommit, remote.position());
            if (authorityAge >= 0) {
                authorityAge = Math.max(authorityAge, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                        observedNs - remote.observedNs()));
            }
        }
        leader = accepted;
        readiness.observe(commit.registrationId(), termId, electionCount, cluster.role(),
                applied, committed, electionState == ElectionState.CLOSED.code(),
                moduleState == ConsensusModule.State.ACTIVE.code() && consensusRole == cluster.role().code(),
                authorityAge);
    }

    private static boolean absent(final Counter counter) { return counter == null || counter.isClosed(); }
}
