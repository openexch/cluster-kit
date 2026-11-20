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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A leader elect that never hears from its followers, against a real consensus module. Aeron 1.53 leaves
 * LEADER_LOG_REPLICATION and LEADER_READY only once a quorum of followers reports its append position, without a
 * timeout. The shape: the leader stops, a member becomes candidate, the other votes for it and is then gone before it
 * acknowledges the new term. The candidate sits as leader elect for ever.
 *
 * <p>The voter is held deterministically: its consensus agent stops at the end of the duty cycle in which it voted
 * ({@link ThreeMemberCluster.Node#holdAfterVote}), so the vote leaves and nothing after it does. It is then stopped.
 * While no quorum of members is reachable the leader elect stays up past its wait; once stand-in peers answer on
 * both stopped members' participation streams (reachable on the transport, never acknowledging) it exits within the
 * leader elect wait, not the shorter rejoin wait.</p>
 */
public class LeaderElectWaitClusterTest {

    private static final long REJOIN_WAIT_MS = 3_000;
    /** At or above the media driver's publication connection timeout (Aeron's default, 5 s). */
    private static final long PEER_WINDOW_MS = 6_000;
    private static final long LEADER_ELECT_WAIT_MS = 10_000;

    private File baseDir;
    private ThreeMemberCluster cluster;

    @Before
    public void setUp() throws Exception {
        baseDir = Files.createTempDirectory("kit-leader-elect-").toFile();
        cluster = new ThreeMemberCluster(baseDir);
        cluster.wireRetention = false;
    }

    @After
    public void tearDown() {
        cluster.close();
        org.agrona.IoUtil.delete(baseDir, true);
    }

    private static boolean isLeaderElect(final long code) {
        return code == ElectionState.LEADER_LOG_REPLICATION.code() || code == ElectionState.LEADER_READY.code();
    }

    @Test(timeout = 180_000)
    public void aLeaderElectWhoseVoterVanishedTripsOnceThePeersAreReachable() {
        for (int i = 0; i < ThreeMemberCluster.MEMBERS; i++) {
            final ThreeMemberCluster.Node n = cluster.node(i);
            cluster.start(i, RejoinTestSetup.leaderElectLifecycle("member" + i, n, REJOIN_WAIT_MS, PEER_WINDOW_MS,
                    LEADER_ELECT_WAIT_MS));
        }
        final ThreeMemberCluster.Node leader = cluster.awaitLeader(30_000);
        final ThreeMemberCluster.Node a = cluster.node(leader.id == 0 ? 1 : 0);
        final ThreeMemberCluster.Node b = cluster.node(3 - leader.id - a.id);
        ThreeMemberCluster.await("both followers to know the leader", 20_000,
                () -> a.lifecycle.rejoinWatchdog().leaderKnown() && b.lifecycle.rejoinWatchdog().leaderKnown());
        a.holdAfterVote = true;
        b.holdAfterVote = true;

        // the leader stops; one follower becomes candidate, the other votes for it and is held right after its vote
        cluster.stop(leader.id);
        ThreeMemberCluster.await("one member to vote and the other to become leader elect", 30_000,
                () -> (a.held && isLeaderElect(b.electionState())) || (b.held && isLeaderElect(a.electionState())));
        final ThreeMemberCluster.Node survivor = a.held ? b : a;
        final ThreeMemberCluster.Node voter = a.held ? a : b;
        final long reached = survivor.electionState();
        System.out.println("leader elect: member " + survivor.id + " in election state " + reached + ", member "
                + voter.id + " held after its vote");
        cluster.stop(voter.id);

        // a genuine quorum loss: past the leader elect wait, still up and still leader elect
        ThreeMemberCluster.await("the leader elect to say no quorum is reachable", 60_000, () -> {
            assertFalse("the leader elect exited during a quorum loss: " + survivor.log,
                    survivor.lifecycle.stopRequested());
            return survivor.logged("no quorum of members is reachable");
        });
        assertFalse(survivor.lifecycle.rejoinWatchdog().tripped());
        assertTrue("still leader elect: " + survivor.electionState(), isLeaderElect(survivor.electionState()));
        assertTrue(survivor.log.toString(), survivor.log.stream().anyMatch(l ->
                l.contains("no quorum of members is reachable") && l.contains("ENGINE_LEADER_ELECT_WAIT_MS")));

        // stand-in peers answer on both stopped members' endpoints: the gate opens, the leader elect wait runs
        final long standInNs = awaitTripWithStandInPeers(survivor);
        final long tookMs = TimeUnit.NANOSECONDS.toMillis(standInNs);
        assertTrue(survivor.lifecycle.rejoinWatchdog().tripped());
        assertEquals(EngineLifecycle.Cause.FATAL, survivor.lifecycle.cause());
        final String critical = survivor.log.stream().filter(l -> l.contains("CRITICAL")).findFirst().orElse("");
        assertTrue(survivor.log.toString(), critical.contains("CRITICAL rejoin watchdog: leader elect")
                && (critical.contains("LEADER_LOG_REPLICATION") || critical.contains("LEADER_READY"))
                && critical.contains("ENGINE_LEADER_ELECT_WAIT_MS"));
        assertTrue("tripped " + tookMs + " ms after the peers became reachable, before the leader elect wait",
                tookMs >= LEADER_ELECT_WAIT_MS);
        System.out.println("GREEN leader elect, peers reachable after " + tookMs + " ms: " + critical);
    }

    /**
     * Live subscribers on the stopped members' participation streams; they never acknowledge. Returns the time taken.
     */
    private long awaitTripWithStandInPeers(final ThreeMemberCluster.Node survivor) {
        final String dir = new File(baseDir, "stand-in-driver").getAbsolutePath();
        final int[] peers = new int[2];
        int k = 0;
        for (int i = 0; i < ThreeMemberCluster.MEMBERS; i++) {
            if (i != survivor.id) {
                peers[k++] = i;
            }
        }
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(dir)
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir));
             Subscription peer1 = aeron.addSubscription(
                     "aeron:udp?endpoint=localhost:" + cluster.consensusPort(peers[0]),
                     PeerReachability.DEFAULT_PROBE_STREAM_ID);
             Subscription peer2 = aeron.addSubscription(
                     "aeron:udp?endpoint=localhost:" + cluster.consensusPort(peers[1]),
                     PeerReachability.DEFAULT_PROBE_STREAM_ID)) {
            final long start = System.nanoTime();
            ThreeMemberCluster.await("the leader elect watchdog to trip with both peers reachable",
                    LEADER_ELECT_WAIT_MS + 30_000, () -> {
                        peer1.poll((buffer, offset, length, header) -> { }, 64);
                        peer2.poll((buffer, offset, length, header) -> { }, 64);
                        return survivor.lifecycle.stopRequested();
                    });
            return System.nanoTime() - start;
        }
    }
}
