// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.cluster.ElectionState;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The rejoin watchdog against a real consensus module: a lone survivor of a quorum loss stays up past the rejoin
 * wait (it may be the restore source of a recovering peer), while a member whose election is stuck although a quorum
 * of peers is reachable still exits for a supervised restart.
 */
public class RejoinReachabilityClusterTest {

    /**
     * Above Aeron's publication connection timeout (5 s): a stopped peer's publication still reads connected for up
     * to that long, so a shorter wait can trip on that stale evidence alone. Production keeps the same order through
     * the boot rule (the wait exceeds the evidence window, the window is at least the connection timeout).
     */
    private static final long REJOIN_WAIT_MS = 8_000;
    /** The evidence window: at least the publication connection timeout, below the wait. */
    private static final long PEER_WINDOW_MS = 6_000;

    private File baseDir;
    private ThreeMemberCluster cluster;

    @Before
    public void setUp() throws Exception {
        baseDir = Files.createTempDirectory("kit-rejoin-").toFile();
        cluster = new ThreeMemberCluster(baseDir);
        cluster.wireRetention = false;
    }

    @After
    public void tearDown() {
        cluster.close();
        org.agrona.IoUtil.delete(baseDir, true);
    }

    private EngineLifecycle lifecycle(final ThreeMemberCluster.Node n) {
        return RejoinTestSetup.leaderElectLifecycle("member" + n.id, n, REJOIN_WAIT_MS, PEER_WINDOW_MS,
                EngineLifecycle.DEFAULT_LEADER_ELECT_WAIT_MS);
    }

    /** The production probe behind a member's rejoin watchdog, as {@code watchQuorum(context)} wired it. */
    private static PeerReachability probe(final ThreeMemberCluster.Node n) {
        return (PeerReachability) n.lifecycle.rejoinWatchdog().quorumReachable();
    }

    /** {@code n}'s probe reads {@code reachable} members (itself included) as reachable. */
    private static void awaitReachable(final ThreeMemberCluster.Node n, final int reachable) {
        final PeerReachability probe = probe(n);
        ThreeMemberCluster.await("member " + n.id + " to read " + reachable + " members reachable", 20_000,
                () -> probe.getAsBoolean() && probe.lastReachable() == reachable);
    }

    private ThreeMemberCluster.Node survivorAfterQuorumLoss() {
        for (int i = 0; i < ThreeMemberCluster.MEMBERS; i++) {
            cluster.start(i, lifecycle(cluster.node(i)));
        }
        final ThreeMemberCluster.Node leader = cluster.awaitLeader(30_000);
        final ThreeMemberCluster.Node survivor = cluster.node(0);
        ThreeMemberCluster.await("the survivor to know the leader", 20_000,
                () -> survivor.lifecycle.rejoinWatchdog().leaderKnown());
        // the production wiring on a healthy cluster: every running member opens its participation subscription from
        // its watchdog's tick (no election is open), and every probe reads its live peers reachable
        for (int i = 0; i < ThreeMemberCluster.MEMBERS; i++) {
            final ThreeMemberCluster.Node n = cluster.node(i);
            ThreeMemberCluster.await("member " + i + " to open its participation subscription", 20_000,
                    () -> probe(n).participating());
        }
        for (int i = 0; i < ThreeMemberCluster.MEMBERS; i++) {
            awaitReachable(cluster.node(i), ThreeMemberCluster.MEMBERS);
        }
        // The follower goes first and the leader last, so the quorum is lost in one step and no election runs while
        // the survivor still has a peer. Stopping the leader first starts an election the survivor can win with the
        // other peer's vote; when that peer stops before it acknowledges the new term, the survivor stays a leader
        // elect (LEADER_LOG_REPLICATION or LEADER_READY, which Aeron never leaves without a quorum of followers)
        // instead of reaching the no-leader election this class is about; LeaderElectWaitClusterTest covers that
        // case.
        final int first = leader.id == 1 ? 2 : 1;
        cluster.stop(first);
        cluster.stop(3 - first);
        ThreeMemberCluster.await("the survivor to be in an election without a leader", 20_000,
                () -> QuorumWatchdog.isNoLeader(survivor.electionState()));
        return survivor;
    }

    /** Two of three members gone: nothing a restart of the survivor could fix, so it must not exit. */
    @Test(timeout = 120_000)
    public void aLoneSurvivorOfAQuorumLossStaysUpPastTheRejoinWait() throws Exception {
        final ThreeMemberCluster.Node survivor = survivorAfterQuorumLoss();
        final long deadline = System.nanoTime() + 4 * REJOIN_WAIT_MS * 1_000_000L;
        while (System.nanoTime() < deadline) {
            assertFalse("the survivor exited during a genuine quorum loss: " + survivor.log,
                    survivor.lifecycle.stopRequested());
            Thread.sleep(100);
        }
        assertFalse(survivor.lifecycle.rejoinWatchdog().tripped());
        assertTrue("the survivor is still in an election", survivor.electionState() != 17);
        System.out.println("GREEN quorum loss: the survivor stayed up for " + 4 * REJOIN_WAIT_MS
                + " ms without a leader (rejoin wait " + REJOIN_WAIT_MS + " ms)");
    }

    /**
     * A stopped peer comes back and spends longer than the wait plus the window in its startup restore (its service's
     * {@code onStart} has not returned, so its consensus module is still recovering and cannot vote). Its driver is up
     * and its consensus subscription exists, but it does not count as reachable: the survivor stays up, and once the
     * restore finishes the two elect a leader without a trip.
     */
    @Test(timeout = 150_000)
    public void aPeerStillInItsStartupRestoreDoesNotCountAsReachable() throws Exception {
        final ThreeMemberCluster.Node survivor = survivorAfterQuorumLoss();
        final ThreeMemberCluster.Node restoring = cluster.node(1);
        final java.util.concurrent.CountDownLatch restore = new java.util.concurrent.CountDownLatch(1);
        restoring.startGate = restore;
        try {
            cluster.start(1, lifecycle(restoring));
            final long holdMs = REJOIN_WAIT_MS + PEER_WINDOW_MS + 6_000;
            final long deadline = System.nanoTime() + holdMs * 1_000_000L;
            while (System.nanoTime() < deadline) {
                assertFalse("the survivor exited while its only returning peer was still restoring: " + survivor.log,
                        survivor.lifecycle.stopRequested());
                Thread.sleep(100);
            }
            assertFalse(survivor.lifecycle.rejoinWatchdog().tripped());
            assertEquals("the restoring peer is still in startup recovery", 0,
                    restoring.consensus().moduleStateCounter().get());
            assertFalse("a restoring peer opens no participation subscription", probe(restoring).participating());
            assertFalse(probe(survivor).getAsBoolean());
            assertEquals("the survivor reads only itself", 1, probe(survivor).lastReachable());
        } finally {
            restore.countDown();
        }
        ThreeMemberCluster.await("a leader known on the survivor and the restored peer", 30_000,
                () -> survivor.electionState() == ElectionState.CLOSED.code()
                        && restoring.electionState() == ElectionState.CLOSED.code()
                        && (survivor.isLeader() || restoring.isLeader()));
        // once running, the restored peer participates and the two read each other reachable
        ThreeMemberCluster.await("the restored peer to open its participation subscription", 20_000,
                () -> probe(restoring).participating());
        awaitReachable(survivor, 2);
        awaitReachable(restoring, 2);
        assertFalse("the survivor exited: " + survivor.log, survivor.lifecycle.stopRequested());
        assertFalse("the restored peer exited: " + restoring.log, restoring.lifecycle.stopRequested());
        assertFalse(survivor.lifecycle.rejoinWatchdog().tripped());
        assertFalse(restoring.lifecycle.rejoinWatchdog().tripped());
        System.out.println("GREEN restoring peer: the survivor stayed up through a restore of more than "
                + (REJOIN_WAIT_MS + PEER_WINDOW_MS) + " ms and the two elected a leader");
    }

    /** Both rules with every member's media driver launched on its own (the external driver's shape). */
    @Test(timeout = 180_000)
    public void theGateHoldsWithAStandAloneDriver() throws Exception {
        cluster.standAloneDriver = true;
        final ThreeMemberCluster.Node survivor = survivorAfterQuorumLoss();
        final long deadline = System.nanoTime() + 3 * REJOIN_WAIT_MS * 1_000_000L;
        while (System.nanoTime() < deadline) {
            assertFalse("the survivor exited during a genuine quorum loss: " + survivor.log,
                    survivor.lifecycle.stopRequested());
            Thread.sleep(100);
        }
        assertTrue(survivor.log.toString(), survivor.logged("no quorum of members is reachable"));
        awaitTripWithStandInPeers(survivor);
        assertTrue(survivor.lifecycle.rejoinWatchdog().tripped());
    }

    /**
     * The wedge's shape: both peers answer on the transport (their participation streams have live subscribers), yet
     * the election never completes. Here the peers are stand-in subscriptions that never vote.
     */
    @Test(timeout = 120_000)
    public void aStuckElectionWithReachablePeersStillTrips() throws Exception {
        final ThreeMemberCluster.Node survivor = survivorAfterQuorumLoss();
        awaitTripWithStandInPeers(survivor);
        assertTrue(survivor.lifecycle.rejoinWatchdog().tripped());
        assertEquals(EngineLifecycle.Cause.FATAL, survivor.lifecycle.cause());
        assertTrue(survivor.log.toString(), survivor.logged("CRITICAL rejoin watchdog"));
        System.out.println("GREEN reachable peers, stuck election: "
                + survivor.log.stream().filter(l -> l.contains("CRITICAL")).findFirst().orElse(""));
    }

    /** Live subscribers on both stopped peers' participation streams (running peers' shape); they never vote. */
    private void awaitTripWithStandInPeers(final ThreeMemberCluster.Node survivor) {
        final String dir = new File(baseDir, "stand-in-driver").getAbsolutePath();
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(dir)
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir));
             Subscription peer1 = aeron.addSubscription("aeron:udp?endpoint=localhost:" + cluster.consensusPort(1),
                     PeerReachability.DEFAULT_PROBE_STREAM_ID);
             Subscription peer2 = aeron.addSubscription("aeron:udp?endpoint=localhost:" + cluster.consensusPort(2),
                     PeerReachability.DEFAULT_PROBE_STREAM_ID)) {
            ThreeMemberCluster.await("the rejoin watchdog to trip with both peers reachable", 30_000, () -> {
                peer1.poll((buffer, offset, length, header) -> { }, 64);
                peer2.poll((buffer, offset, length, header) -> { }, 64);
                return survivor.lifecycle.stopRequested();
            });
        }
    }
}
