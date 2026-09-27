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

    public record LeaderCheckpoint(long term, long source, long position, long observedNs) { }

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
    @Override public void close() { if (observer != null) { observer.close(); } }

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
            readiness.unavailable("consensus-changing");
            return;
        }
        long committed = localCommit;
        long authorityAge = heartbeat <= 0 ? -1 : heartbeatAge;
        if (cluster.role() == Cluster.Role.FOLLOWER && electionState == ElectionState.CLOSED.code()) {
            if (remote == null || remote.term() != termId || remote.source() < 0 || remote.position() < 0
                    || observedNs - remote.observedNs() < 0) {
                readiness.unavailable("leader-checkpoint-unavailable"); return;
            }
            if (leaderSource != remote.source()) { readiness.catchingUp(); leaderSource = remote.source(); }
            committed = Math.max(localCommit, remote.position());
            if (authorityAge >= 0) {
                authorityAge = Math.max(authorityAge, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                        observedNs - remote.observedNs()));
            }
        }
        readiness.observe(commit.registrationId(), termId, electionCount, cluster.role(),
                applied, committed, electionState == ElectionState.CLOSED.code(),
                moduleState == ConsensusModule.State.ACTIVE.code() && consensusRole == cluster.role().code(),
                authorityAge);
    }

    private static boolean absent(final Counter counter) { return counter == null || counter.isClosed(); }
}
