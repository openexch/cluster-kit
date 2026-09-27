// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Counter;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;

/**
 * Read the already-open local consensus context from the service duty cycle.
 * No counter scan, IPC query, file mapping, sleep, or lock is performed here.
 * Aeron 1.53 invokes background work after log application callbacks return.
 * The local commit counter is a service application bound, not a remote member
 * watermark; rolling restart coordination must also compare cluster members.
 */
public final class ConsensusReadiness {
    public static final long SAMPLE_INTERVAL_NS = 10_000_000;
    private final ConsensusModule.Context context;
    private final NodeReadiness readiness;
    private long lastSampleNs;
    private boolean sampled;

    public ConsensusReadiness(final ConsensusModule.Context context, final NodeReadiness readiness) {
        this.context = context;
        this.readiness = readiness;
    }

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
        final long committed = commit.get();
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
        readiness.observe(commit.registrationId(), termId, electionCount, cluster.role(),
                applied, committed, electionState == ElectionState.CLOSED.code(),
                moduleState == ConsensusModule.State.ACTIVE.code() && consensusRole == cluster.role().code(),
                heartbeat <= 0 ? -1 : heartbeatAge);
    }

    private static boolean absent(final Counter counter) { return counter == null || counter.isClosed(); }
}
