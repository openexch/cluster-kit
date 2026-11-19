// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.PrunerSeams;
import com.openexchange.cluster.SnapshotLogPruner;
import io.aeron.archive.client.AeronArchive;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.openexchange.cluster.shadow.FloorRig.SEGMENT;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A follower follows its leader's log start without waiting for its next snapshot. The real
 * {@link SnapshotLogPruner} thread of a follower (a log, a snapshot group at P_cm, no floor of its own) next to a
 * leader archive whose log start is 0: the first pass is limited by the retention floor and purges nothing. Then
 * ONLY the leader's log start moves (no snapshot, no shadow request on the follower): the pruner looks again on its
 * backoff and purges to the new floor. The control leaves no re-check pending and the follower's log stays whole.
 */
public class PrunerRecheckTest {

    private static final long POLL_MS = 100;
    private static final long RECHECK_MAX_MS = 400;
    private static final long LEADER_START = 4L * SEGMENT;

    private static void run(final boolean recheckOff) throws Exception {
        final int leaderPort = PullRig.freeUdpPort();
        try (FloorRig leader = new FloorRig(FloorRig.Service.STUB, leaderPort); FloorRig follower = new FloorRig()) {
            final AtomicBoolean firstPass = new AtomicBoolean(true);
            final SnapshotLogPruner pruner = PrunerSeams.follower(new SnapshotLogPruner(follower.clusterDir,
                    new AeronArchive.Context()
                            .aeronDirectoryName(follower.rig.fixture.aeronDirectoryName())
                            .controlRequestChannel("aeron:ipc")
                            .controlResponseChannel("aeron:ipc"),
                    follower.rig.fixture.aeron().countersReader(), 0, () -> true)
                    .wake(() -> firstPass.getAndSet(false)),
                    1, "localhost:1", 0, "localhost:" + leaderPort, POLL_MS, RECHECK_MAX_MS, recheckOff);
            pruner.start();
            try {
                await(() -> pruner.prunesRun() >= 1, "the first purge pass");
                assertEquals("the leader's log starts at 0: the follower purges nothing", 0, follower.logStart());
                assertEquals(0, pruner.lastAppliedFloor());
                // several backoff periods with nothing moving: no further pass, the re-check only reads the floor
                Thread.sleep(3 * RECHECK_MAX_MS);
                assertEquals(1, pruner.prunesRun());
                assertEquals(0, pruner.wakesByRecheck());

                // only the leader's log start moves
                leader.archive().purgeSegments(leader.logRecordingId, LEADER_START);
                assertEquals(LEADER_START, leader.logStart());
                final long deadline = System.nanoTime() + 20L * RECHECK_MAX_MS * 1_000_000;
                while (follower.logStart() != LEADER_START && System.nanoTime() - deadline < 0) {
                    Thread.sleep(20);
                }
                if (follower.logStart() != LEADER_START) {
                    throw new AssertionError("the follower never purged after only the leader's log start moved: own"
                            + " log start " + follower.logStart() + ", leader's " + LEADER_START + ", passes "
                            + pruner.prunesRun());
                }
                assertEquals(1, pruner.wakesByRecheck());
                assertEquals(LEADER_START, pruner.lastAppliedFloor());
                assertEquals(1, pruner.wakesByShadow());
                assertTrue(String.valueOf(pruner.lastError()), pruner.lastError() == null);
            } finally {
                pruner.stop();
            }
        }
    }

    private static void await(final java.util.function.BooleanSupplier done, final String what)
            throws InterruptedException {
        final long deadline = System.nanoTime() + 20_000_000_000L;
        while (!done.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new IllegalStateException("timed out: " + what);
            }
            Thread.sleep(20);
        }
    }

    @Test
    public void aFollowerPurgesWhenOnlyTheLeadersLogStartMoved() throws Exception {
        run(false);
        ShadowRig.green("pruner re-check: the follower followed the leader's log start to " + LEADER_START);
        nc("recheck off", "the follower never purged after only the leader's log start moved", () -> {
            try {
                run(true);
            } catch (final AssertionError red) {
                throw red;
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
