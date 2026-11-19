// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.cluster.ConsensusModule;
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

    private static final long REJOIN_WAIT_MS = 3_000;

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
        return RejoinTestSetup.lifecycle("member" + n.id, n, REJOIN_WAIT_MS);
    }

    private ThreeMemberCluster.Node survivorAfterQuorumLoss() {
        for (int i = 0; i < ThreeMemberCluster.MEMBERS; i++) {
            cluster.start(i, lifecycle(cluster.node(i)));
        }
        cluster.awaitLeader(30_000);
        final ThreeMemberCluster.Node survivor = cluster.node(0);
        ThreeMemberCluster.await("the survivor to know the leader", 20_000,
                () -> survivor.lifecycle.rejoinWatchdog().leaderKnown());
        cluster.stop(1);
        cluster.stop(2);
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
     * The wedge's shape: both peers answer on the transport (their consensus endpoints have live subscribers), yet
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

    /** Live subscribers on both stopped peers' consensus endpoints; they never vote. */
    private void awaitTripWithStandInPeers(final ThreeMemberCluster.Node survivor) {
        final String dir = new File(baseDir, "stand-in-driver").getAbsolutePath();
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(dir)
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir));
             Subscription peer1 = aeron.addSubscription("aeron:udp?endpoint=localhost:" + cluster.consensusPort(1),
                     ConsensusModule.Configuration.CONSENSUS_STREAM_ID_DEFAULT);
             Subscription peer2 = aeron.addSubscription("aeron:udp?endpoint=localhost:" + cluster.consensusPort(2),
                     ConsensusModule.Configuration.CONSENSUS_STREAM_ID_DEFAULT)) {
            ThreeMemberCluster.await("the rejoin watchdog to trip with both peers reachable", 30_000, () -> {
                peer1.poll((buffer, offset, length, header) -> { }, 64);
                peer2.poll((buffer, offset, length, header) -> { }, 64);
                return survivor.lifecycle.stopRequested();
            });
        }
    }
}
