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

    /**
     * Wires {@code watermark} into {@code pruner} with a commit position from memory (no consensus module here).
     *
     * @param offBoxOff the negative control: the pass takes the lowest of the other floors only
     */
    public static SnapshotLogPruner offBox(final SnapshotLogPruner pruner, final OffBoxWatermark watermark,
                                           final java.util.function.LongSupplier commitPosition,
                                           final long pollIntervalMs, final long recheckMaxMs,
                                           final boolean offBoxOff) {
        pruner.offBoxWatermark(watermark).commitPosition(commitPosition).pollIntervalMs(pollIntervalMs)
            .recheckMaxMs(recheckMaxMs);
        pruner.offBoxOff = offBoxOff;
        return pruner;
    }

    /**
     * Gives {@code pruner} a retention floor that cannot be worked out and whose member wait has already passed:
     * the retention floor answers "no constraint" from the first pass on.
     */
    public static SnapshotLogPruner retentionFloorGivenUp(final SnapshotLogPruner pruner) {
        return pruner.membersQuery(timeoutMs -> null, () -> true)
            .retentionFloor(SnapshotLogPruner.FloorMode.APPLY, 1, 1);
    }

    /** The command line's purge. {@code offBoxOff} is its negative control: the file is not evaluated. */
    public static ArchiveHousekeeping.Result housekeep(final java.io.File clusterDir,
                                                       final io.aeron.archive.client.AeronArchive archive,
                                                       final long watermark, final OffBoxWatermark offBox,
                                                       final long commitPosition, final boolean offBoxOff) {
        ArchiveHousekeeping.offBoxOffForTest = offBoxOff;
        try {
            return ArchiveHousekeeping.purgeBelow(clusterDir, archive, watermark, offBox, commitPosition);
        } finally {
            ArchiveHousekeeping.offBoxOffForTest = false;
        }
    }

    /** The command line's reading of the member's identity arguments. */
    public static OffBoxWatermark housekeepingOffBox(final java.io.File clusterDir, final String... args) {
        return ArchiveHousekeeping.offBoxFrom(clusterDir, args);
    }

    /** Runs the watermark tool; fails the test on any exit but 0. */
    public static void tool(final String... args) {
        final java.io.ByteArrayOutputStream err = new java.io.ByteArrayOutputStream();
        final int exit = OffBoxWatermarkTool.run(args, new java.io.PrintStream(new java.io.ByteArrayOutputStream()),
            new java.io.PrintStream(err), System::currentTimeMillis);
        if (exit != 0) {
            throw new IllegalStateException("OffBoxWatermarkTool " + String.join(" ", args) + " exited " + exit
                + ": " + err);
        }
    }
}
