// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Three members in one process: a member that is down while the others take a snapshot and purge their logs across
 * segment boundaries above its position. Full snapshots (no shadow floor), 64 KiB segments.
 *
 * <p>The retention floor keeps the log that member still needs on every member, so it catches up from the log when it
 * returns, also after the leader changed while it was away. A member that was written off (the wait bound passed) and
 * returns below the leader's log start exits FATAL with one line naming both positions.</p>
 */
public class MemberRetentionClusterTest {

    private File baseDir;
    private ThreeMemberCluster cluster;

    @Before
    public void setUp() throws Exception {
        baseDir = Files.createTempDirectory("kit-retention-").toFile();
        cluster = new ThreeMemberCluster(baseDir);
        cluster.prunerSetup = p -> RetentionTestSetup.fastPoll(p);
    }

    @After
    public void tearDown() {
        cluster.close();
        org.agrona.IoUtil.delete(baseDir, true);
    }

    private void awaitApplied(final long count, final long timeoutMs, final int... ids) {
        ThreeMemberCluster.await("members " + Arrays.toString(ids) + " to apply " + count, timeoutMs, () -> {
            for (final int id : ids) {
                if (cluster.node(id).service.applied != count) {
                    return false;
                }
            }
            return true;
        });
    }

    private static int[] others(final int id) {
        return new int[] {(id + 1) % 3, (id + 2) % 3};
    }

    /** Stops a follower, grows the log by several segments, snapshots, and waits for the survivors' purge passes. */
    private int downAcrossASnapshotAndPurge(final long[] downPosition) {
        cluster.startAll();
        final ThreeMemberCluster.Node leader = cluster.awaitLeader(30_000);
        cluster.send(50, 256);
        awaitApplied(50, 20_000, 0, 1, 2);
        final int down = (leader.id + 1) % 3;
        downPosition[0] = cluster.logExtent(down)[1];
        cluster.stop(down);

        cluster.send(400, 1024);
        final int[] up = others(down);
        awaitApplied(450, 30_000, up);
        final ThreeMemberCluster.Node leaderNow = cluster.awaitLeader(30_000);
        final long[] prunes = {cluster.node(up[0]).pruner.prunesRun(), cluster.node(up[1]).pruner.prunesRun()};
        cluster.snapshot(leaderNow, 20_000);
        ThreeMemberCluster.await("both survivors' purge passes after the snapshot", 30_000,
                () -> cluster.node(up[0]).pruner.prunesRun() > prunes[0]
                        && cluster.node(up[1]).pruner.prunesRun() > prunes[1]);
        return down;
    }

    private String diagnosis(final int down, final long downPosition) {
        final int[] up = others(down);
        return "member " + down + " stopped at " + downPosition
                + "; log start of member " + up[0] + " = " + cluster.logExtent(up[0])[0]
                + ", of member " + up[1] + " = " + cluster.logExtent(up[1])[0]
                + "; member " + down + " errors " + cluster.node(down).errors
                + "; member " + up[0] + " errors " + cluster.node(up[0]).errors
                + "; member " + up[1] + " errors " + cluster.node(up[1]).errors;
    }

    /**
     * The measured failure: the survivors purged to their newest snapshot, the returning member's position is below
     * the leader's log start, every catch-up is refused and the member never applies the log.
     */
    @Test(timeout = 180_000)
    public void aMemberDownAcrossASnapshotAndPurgeCatchesUpFromTheLog() {
        final long[] downPosition = new long[1];
        final int down = downAcrossASnapshotAndPurge(downPosition);
        final int[] up = others(down);
        // read while the member is still down: once it has caught up nothing holds the floor, and the survivors
        // purge on their re-check without waiting for the next snapshot
        for (final int id : up) {
            assertTrue("member " + id + " kept the log the down member needed: " + diagnosis(down, downPosition[0]),
                    cluster.logExtent(id)[0] <= downPosition[0]);
        }
        cluster.start(down);
        try {
            awaitApplied(450, 30_000, down);
        } catch (final AssertionError stranded) {
            throw new AssertionError("the returning member never caught up: " + diagnosis(down, downPosition[0]),
                    stranded);
        }
        System.out.println("GREEN retention floor: " + diagnosis(down, downPosition[0]));

        // once every member has passed the next snapshot the floor releases and the log is reclaimed again
        cluster.send(50, 1024);
        awaitApplied(500, 30_000, 0, 1, 2);
        final ThreeMemberCluster.Node leader = cluster.awaitLeader(30_000);
        final long startBefore = cluster.logExtent(leader.id)[0];
        cluster.snapshot(leader, 20_000);
        ThreeMemberCluster.await("the leader to reclaim the log once every member caught up", 40_000,
                () -> cluster.logExtent(leader.id)[0] > Math.max(startBefore, downPosition[0]));
        assertEquals(500, cluster.node(down).service.applied);
    }

    /** The same with every member's media driver launched on its own (the external driver's shape). */
    @Test(timeout = 180_000)
    public void aMemberDownAcrossASnapshotAndPurgeCatchesUpWithAStandAloneDriver() {
        cluster.standAloneDriver = true;
        final long[] downPosition = new long[1];
        final int down = downAcrossASnapshotAndPurge(downPosition);
        cluster.start(down);
        try {
            awaitApplied(450, 30_000, down);
        } catch (final AssertionError stranded) {
            throw new AssertionError("the returning member never caught up: " + diagnosis(down, downPosition[0]),
                    stranded);
        }
        for (final int id : others(down)) {
            assertTrue("member " + id + " kept the log the down member needed: " + diagnosis(down, downPosition[0]),
                    cluster.logExtent(id)[0] <= downPosition[0]);
        }
    }

    /**
     * The leader changes while the member is away: the old leader stops, the returning member and the remaining
     * follower elect, and the new leader still holds the log the returning member needs, because as a follower it
     * never purged past the old leader's log start. It then holds its log for the old leader too, which it has not
     * heard from in its term.
     */
    @Test(timeout = 240_000)
    public void theFloorSurvivesALeaderChange() {
        final long[] downPosition = new long[1];
        final int down = downAcrossASnapshotAndPurge(downPosition);
        final int oldLeader = cluster.awaitLeader(30_000).id;
        final int follower = 3 - down - oldLeader;
        assertTrue("the follower kept the log the down member needs: " + diagnosis(down, downPosition[0]),
                cluster.logExtent(follower)[0] <= downPosition[0]);
        final long oldLeaderPosition = cluster.logExtent(oldLeader)[1];
        cluster.stop(oldLeader);

        cluster.start(down);
        try {
            awaitApplied(450, 60_000, down);
        } catch (final AssertionError stranded) {
            throw new AssertionError("the returning member never caught up from the new leader: member " + down
                    + " stopped at " + downPosition[0] + ", log start of member " + follower + " = "
                    + cluster.logExtent(follower)[0] + "; errors " + cluster.node(down).errors + " / "
                    + cluster.node(follower).errors, stranded);
        }
        final ThreeMemberCluster.Node newLeader = cluster.awaitLeader(30_000);
        cluster.reconnectClient();
        cluster.send(100, 1024);
        awaitApplied(550, 30_000, down, follower);
        final long[] prunes = {cluster.node(down).pruner.prunesRun(), cluster.node(follower).pruner.prunesRun()};
        cluster.snapshot(newLeader, 20_000);
        ThreeMemberCluster.await("both members' purge passes after the new leader's snapshot", 30_000,
                () -> cluster.node(down).pruner.prunesRun() > prunes[0]
                        && cluster.node(follower).pruner.prunesRun() > prunes[1]);
        assertTrue("the new leader (member " + newLeader.id + ") holds its log for the old leader it has not heard"
                        + " from: log start " + cluster.logExtent(newLeader.id)[0] + ", old leader stopped at "
                        + oldLeaderPosition + ", report " + newLeader.pruner.lastWatermarkReport(),
                cluster.logExtent(newLeader.id)[0] <= oldLeaderPosition);

        cluster.start(oldLeader);
        awaitApplied(550, 60_000, oldLeader);
        System.out.println("GREEN leader change: member " + down + " (down at " + downPosition[0]
                + ") caught up from the new leader " + newLeader.id + ", the old leader " + oldLeader + " (down at "
                + oldLeaderPosition + ") caught up after it; new leader report: "
                + newLeader.pruner.lastWatermarkReport());
    }

    /**
     * A member that stays down past the bound is written off: the leader says STRANDED and purges past it. When that
     * member returns, every catch-up is refused; after the configured number of failed attempts it exits FATAL with
     * one line naming its own log position and the leader's log start.
     */
    @Test(timeout = 240_000)
    public void aMemberWrittenOffAfterTheBoundExitsLoudlyWhenItReturns() {
        // the bound: one millisecond without progress writes a member off
        cluster.prunerSetup = p -> {
            RetentionTestSetup.fastPoll(p);
            p.retentionFloor(SnapshotLogPruner.FloorMode.APPLY, 1, SnapshotLogPruner.DEFAULT_MEMBER_WAIT_BYTES);
        };
        final long[] downPosition = new long[1];
        final int down = downAcrossASnapshotAndPurge(downPosition);
        final ThreeMemberCluster.Node leader = cluster.awaitLeader(30_000);
        final long leaderLogStart = cluster.logExtent(leader.id)[0];
        assertTrue("the leader purged past the written-off member: " + diagnosis(down, downPosition[0]),
                leaderLogStart > downPosition[0]);
        assertTrue(leader.pruner.lastWatermarkReport(),
                leader.pruner.lastWatermarkReport().contains("STRANDED=[" + down + "]"));

        final ThreeMemberCluster.Node returning = cluster.node(down);
        final EngineLifecycle lifecycle = RejoinTestSetup.lifecycle("member" + down, returning, 60_000, 3, 60_000);
        cluster.start(down, lifecycle);
        ThreeMemberCluster.await("the stranded member to exit: " + returning.log, 60_000, lifecycle::stopRequested);

        assertEquals(EngineLifecycle.Cause.FATAL, lifecycle.cause());
        assertTrue(lifecycle.catchupWatchdog().tripped());
        final String critical = returning.log.stream().filter(l -> l.contains("CRITICAL catch-up watchdog"))
                .findFirst().orElseThrow(() -> new AssertionError("no CRITICAL line: " + returning.log));
        assertTrue(critical, critical.contains("3 catch-up attempt(s) in a row failed"));
        assertTrue(critical, critical.contains("this member's log position " + downPosition[0] + " is below the log"
                + " start position " + leaderLogStart + " of the leader (member " + leader.id + ")"));
        assertTrue(returning.log.toString(), returning.logged("FATAL (exit 1): member cannot catch up: its log"
                + " position " + downPosition[0] + " is below the leader's log start position " + leaderLogStart));
        assertEquals("the member never applied the log it could not get", 50, returning.service.applied);
        System.out.println("GREEN loud exit: " + critical);
    }

    /**
     * {@code RETENTION_FLOOR=report}: the floor is logged and not applied, the purge goes to each node's own
     * snapshot as it did before the floor existed.
     */
    @Test(timeout = 180_000)
    public void reportModeLeavesThePurgeAtTheSnapshot() {
        cluster.prunerSetup = p -> {
            RetentionTestSetup.fastPoll(p);
            p.retentionFloor(SnapshotLogPruner.FloorMode.REPORT, SnapshotLogPruner.DEFAULT_MEMBER_WAIT_MS,
                    SnapshotLogPruner.DEFAULT_MEMBER_WAIT_BYTES);
        };
        final long[] downPosition = new long[1];
        final int down = downAcrossASnapshotAndPurge(downPosition);
        for (final int id : others(down)) {
            assertTrue("member " + id + " purged to its snapshot: " + diagnosis(down, downPosition[0]),
                    cluster.logExtent(id)[0] > downPosition[0]);
            assertEquals(Long.MAX_VALUE, cluster.node(id).pruner.lastAppliedFloor());
        }
    }

    /**
     * The engines' wiring next to an external driver: the pruner's archive context carries the container's live
     * Aeron client, and a directory name that is not the driver's. A follower still reads the leader's log start.
     */
    @Test(timeout = 180_000)
    public void aFollowerReadsTheLeadersLogStartWhenItsArchiveContextNamesAnotherDirectory() {
        cluster.standAloneDriver = true;
        cluster.staleArchiveDirectoryName = true;
        final long[] downPosition = new long[1];
        final int down = downAcrossASnapshotAndPurge(downPosition);
        final int leader = cluster.awaitLeader(30_000).id;
        final ThreeMemberCluster.Node follower = cluster.node(3 - down - leader);
        final String report = follower.pruner.lastWatermarkReport();
        assertTrue("the follower (member " + follower.id + ") read the leader's log start: " + report,
                report != null && report.startsWith("follower floor="));
        assertEquals("a known floor is not a health finding", null, follower.pruner.lastError());
        cluster.start(down);
        awaitApplied(450, 30_000, down);
    }

    /**
     * A member whose directories were wiped returns with no log at all while the leader's log starts above zero
     * (the member was written off). It can never catch up from the log: it exits FATAL naming both positions.
     */
    @Test(timeout = 240_000)
    public void aWipedMemberBelowTheLeadersLogStartExitsLoudly() {
        cluster.prunerSetup = p -> {
            RetentionTestSetup.fastPoll(p);
            p.retentionFloor(SnapshotLogPruner.FloorMode.APPLY, 1, SnapshotLogPruner.DEFAULT_MEMBER_WAIT_BYTES);
        };
        final long[] downPosition = new long[1];
        final int down = downAcrossASnapshotAndPurge(downPosition);
        final ThreeMemberCluster.Node leader = cluster.awaitLeader(30_000);
        final long leaderLogStart = cluster.logExtent(leader.id)[0];
        assertTrue("the leader purged past the written-off member: " + diagnosis(down, downPosition[0]),
                leaderLogStart > 0);

        org.agrona.IoUtil.delete(cluster.memberDir(down), false);
        final ThreeMemberCluster.Node returning = cluster.node(down);
        final EngineLifecycle lifecycle = RejoinTestSetup.lifecycle("member" + down, returning, 60_000, 3, 60_000);
        cluster.start(down, lifecycle);
        final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
        while (!lifecycle.stopRequested() && System.nanoTime() < deadline) {
            java.util.concurrent.locks.LockSupport.parkNanos(50_000_000);
        }
        assertTrue("the wiped member never exited: refusals " + lifecycle.catchupWatchdog().refusals() + ", log "
                + returning.log + ", leader errors " + leader.errors, lifecycle.stopRequested());

        assertEquals(EngineLifecycle.Cause.FATAL, lifecycle.cause());
        final String critical = returning.log.stream().filter(l -> l.contains("CRITICAL catch-up watchdog"))
                .findFirst().orElseThrow(() -> new AssertionError("no CRITICAL line: " + returning.log));
        assertTrue(critical, critical.contains("this member's log position 0 is below the log start position "
                + leaderLogStart + " of the leader (member " + leader.id + ")"));
        assertEquals(0, returning.service.applied);
        System.out.println("GREEN wiped member: " + critical);
    }
}
