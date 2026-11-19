// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.util.List;

/** The pruner's package-private seams for tests in other packages (no consensus module behind the pruner). */
public final class PrunerSeams {

    private PrunerSeams() {
    }

    /**
     * Makes {@code pruner} a FOLLOWER (member {@code selfId}) of a leader whose archive answers on
     * {@code leaderArchiveEndpoint}: the members query answers from memory, the retention floor is applied.
     *
     * @param recheckOff the negative control: a pass the retention floor limited leaves no re-check pending
     */
    public static SnapshotLogPruner follower(final SnapshotLogPruner pruner, final int selfId,
                                             final String selfArchiveEndpoint, final int leaderId,
                                             final String leaderArchiveEndpoint, final long pollIntervalMs,
                                             final long recheckMaxMs, final boolean recheckOff) {
        final ClusterMemberPositions.Snapshot answer = new ClusterMemberPositions.Snapshot(0, leaderId, selfId,
            List.of(new ClusterMemberPositions.Member(selfId, 0, 0, selfArchiveEndpoint),
                new ClusterMemberPositions.Member(leaderId, 0, 0, leaderArchiveEndpoint)));
        pruner.membersQuery(timeoutMs -> answer, () -> false)
            .retentionFloor(SnapshotLogPruner.FloorMode.APPLY, SnapshotLogPruner.DEFAULT_MEMBER_WAIT_MS,
                SnapshotLogPruner.DEFAULT_MEMBER_WAIT_BYTES)
            .pollIntervalMs(pollIntervalMs)
            .recheckMaxMs(recheckMaxMs);
        pruner.recheckOff = recheckOff;
        return pruner;
    }
}
