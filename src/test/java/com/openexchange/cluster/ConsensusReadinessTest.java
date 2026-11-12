// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Counter;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.MillisecondClusterClock;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.Test;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import static org.junit.Assert.*;

public class ConsensusReadinessTest {
    @Test public void realCountersFenceInitialFollowerElectionAndConsensusStop() throws Exception {
        final var dir = Files.createTempDirectory("consensus-readiness-");
        final CountersManager counters = new CountersManager(new UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(8192)),
                new UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(2048)));
        try (ClusterMarkFile mark = new ClusterMarkFile(dir.resolve(ClusterMarkFile.FILENAME).toFile(),
                ClusterComponentType.CONSENSUS_MODULE, ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH,
                System::currentTimeMillis, 0, 4096)) {
            mark.updateActivityTimestamp(System.currentTimeMillis());
            final var context = new ConsensusModule.Context().clusterMarkFile(mark)
                    .clusterClock(new MillisecondClusterClock())
                    .commitPositionCounter(counter(counters, 100))
                    .leadershipTermIdCounter(counter(counters, 7))
                    .electionCounter(counter(counters, 2))
                    .electionStateCounter(counter(counters, ElectionState.CLOSED.code()))
                    .moduleStateCounter(counter(counters, ConsensusModule.State.ACTIVE.code()))
                    .clusterNodeRoleCounter(counter(counters, Cluster.Role.FOLLOWER.code()));
            final long[] applied = {0};
            final Cluster cluster = (Cluster) Proxy.newProxyInstance(Cluster.class.getClassLoader(),
                    new Class<?>[]{Cluster.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "role" -> Cluster.Role.FOLLOWER;
                        case "logPosition" -> applied[0];
                        default -> throw new AssertionError("unexpected blocking/API call " + method);
                    });
            final NodeReadiness r = new NodeReadiness(); r.started();
            final long[] remotePosition = {100};
            final long[] remoteAge = {0};
            final boolean[] remoteKnown = {true};
            final ConsensusReadiness probe = new ConsensusReadiness(context, r, t -> remoteKnown[0]
                    ? new ConsensusReadiness.LeaderCheckpoint(context.leadershipTermIdCounter().get(),
                        999, remotePosition[0], t - remoteAge[0]) : null);
            long now = System.nanoTime(); probe.poll(cluster, now);
            assertFalse(r.ready());
            applied[0] = 100; probe.poll(cluster, now += 20_000_000);
            assertTrue(r.describe(), r.ready()); // no onRoleChange callback
            context.electionStateCounter().set(ElectionState.CANVASS.code());
            probe.poll(cluster, now += 20_000_000);
            assertFalse(r.ready()); assertTrue(r.live());
            context.electionStateCounter().set(ElectionState.CLOSED.code());
            context.leadershipTermIdCounter().set(8); context.commitPositionCounter().set(200);
            remotePosition[0] = 200;
            probe.poll(cluster, now += 20_000_000); assertFalse(r.ready());
            applied[0] = 200; probe.poll(cluster, now += 20_000_000); assertTrue(r.ready());
            remotePosition[0] = 100_000;
            probe.poll(cluster, now += 20_000_000);
            assertFalse("local equality must not hide leader commit lag: " + r.describe(), r.ready());
            applied[0] = 100_000; context.commitPositionCounter().set(100_000);
            probe.poll(cluster, now += 20_000_000); assertTrue(r.ready());
            remoteAge[0] = 6_000_000_000L;
            probe.poll(cluster, now += 20_000_000); assertFalse("stale leader proof", r.ready());
            remoteAge[0] = 0; remoteKnown[0] = false;
            probe.poll(cluster, now += 20_000_000); assertFalse("missing leader proof", r.ready());
            remoteKnown[0] = true;
            mark.updateActivityTimestamp(System.currentTimeMillis() - 6000);
            probe.poll(cluster, now += 20_000_000); assertFalse(r.ready());
            assertTrue(r.describe().contains("stale-consensus"));
            mark.updateActivityTimestamp(System.currentTimeMillis());
            probe.poll(cluster, now += 20_000_000); assertTrue(r.ready());
            context.commitPositionCounter().close();
            probe.poll(cluster, now += 20_000_000); assertFalse(r.ready());
        } finally {
            try (var paths = Files.walk(dir)) {
                for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) { Files.delete(path); }
            }
        }
    }

    /**
     * The current term's leader id from the leader's own commit message: carried into {@code leader()} on a FOLLOWER
     * with the election CLOSED for the current term, null after {@code leader-checkpoint-unavailable}, null for an
     * older term's checkpoint, null while the observer withholds an ambiguous term, null during an election and on a
     * LEADER. Readiness verdicts are those of the row above.
     */
    @Test public void theLeaderIdOfTheCurrentTermComesFromTheCommitWire() throws Exception {
        final var dir = Files.createTempDirectory("consensus-leader-");
        final CountersManager counters = new CountersManager(new UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(8192)),
                new UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(2048)));
        try (ClusterMarkFile mark = new ClusterMarkFile(dir.resolve(ClusterMarkFile.FILENAME).toFile(),
                ClusterComponentType.CONSENSUS_MODULE, ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH,
                System::currentTimeMillis, 0, 4096)) {
            mark.updateActivityTimestamp(System.currentTimeMillis());
            final var context = new ConsensusModule.Context().clusterMarkFile(mark)
                    .clusterClock(new MillisecondClusterClock())
                    .commitPositionCounter(counter(counters, 100))
                    .leadershipTermIdCounter(counter(counters, 7))
                    .electionCounter(counter(counters, 2))
                    .electionStateCounter(counter(counters, ElectionState.CLOSED.code()))
                    .moduleStateCounter(counter(counters, ConsensusModule.State.ACTIVE.code()))
                    .clusterNodeRoleCounter(counter(counters, Cluster.Role.FOLLOWER.code()));
            final Cluster.Role[] role = {Cluster.Role.FOLLOWER};
            final Cluster cluster = (Cluster) Proxy.newProxyInstance(Cluster.class.getClassLoader(),
                    new Class<?>[]{Cluster.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "role" -> role[0];
                        case "logPosition" -> 100L;
                        default -> throw new AssertionError("unexpected blocking/API call " + method);
                    });
            final NodeReadiness r = new NodeReadiness(); r.started();
            final long[] wireTerm = {7};
            final int[] wireLeader = {2};
            final boolean[] known = {true};
            final ConsensusReadiness probe = new ConsensusReadiness(context, r, t -> known[0]
                    ? new ConsensusReadiness.LeaderCheckpoint(wireTerm[0], 999, 100, t, wireLeader[0]) : null);
            long now = System.nanoTime();
            assertNull("nothing polled yet", probe.leader());
            probe.poll(cluster, now);
            probe.poll(cluster, now += 20_000_000);
            assertTrue(r.describe(), r.ready());
            assertEquals(2, probe.leader().leaderMemberId());
            assertEquals(7, probe.leader().term());
            // the observer withholds an ambiguous term (two members claimed it): the poll sees null
            known[0] = false;
            probe.poll(cluster, now += 20_000_000);
            assertFalse(r.ready());
            assertTrue(r.describe(), r.describe().contains("leader-checkpoint-unavailable"));
            assertNull("unavailable: no leader", probe.leader());
            known[0] = true;
            probe.poll(cluster, now += 20_000_000);
            assertEquals(2, probe.leader().leaderMemberId());
            // the local counters moved to term 8 but the wire still carries term 7: never the older term's leader
            context.leadershipTermIdCounter().set(8);
            probe.poll(cluster, now += 20_000_000);
            assertNull("an older term's checkpoint is not the current leader", probe.leader());
            wireTerm[0] = 8; wireLeader[0] = 0;
            probe.poll(cluster, now += 20_000_000);
            assertEquals("re-learned from the wire alone", 0, probe.leader().leaderMemberId());
            // a checkpoint without a leader id (the older seam) never names a leader
            final ConsensusReadiness legacy = new ConsensusReadiness(context, new NodeReadiness(),
                    t -> new ConsensusReadiness.LeaderCheckpoint(8, 999, 100, t));
            legacy.poll(cluster, now);
            assertNull(legacy.leader());
            // an election: no leader
            context.electionStateCounter().set(ElectionState.CANVASS.code());
            probe.poll(cluster, now += 20_000_000);
            assertNull("during an election", probe.leader());
            context.electionStateCounter().set(ElectionState.CLOSED.code());
            probe.poll(cluster, now += 20_000_000);
            assertEquals(0, probe.leader().leaderMemberId());
            // this node leads: no leader from the wire
            role[0] = Cluster.Role.LEADER;
            context.clusterNodeRoleCounter().set(Cluster.Role.LEADER.code());
            probe.poll(cluster, now += 20_000_000);
            assertNull("on a LEADER", probe.leader());
            role[0] = Cluster.Role.FOLLOWER;
            context.clusterNodeRoleCounter().set(Cluster.Role.FOLLOWER.code());
            probe.poll(cluster, now += 20_000_000);
            assertEquals(0, probe.leader().leaderMemberId());
            context.commitPositionCounter().close();
            probe.poll(cluster, now += 20_000_000);
            assertNull("consensus unavailable", probe.leader());
            probe.close();
        } finally {
            try (var paths = Files.walk(dir)) {
                for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) { Files.delete(path); }
            }
        }
    }

    private static Counter counter(CountersManager manager, long value) {
        int id = manager.allocate("readiness test");
        manager.setCounterRegistrationId(id, 100 + id);
        Counter counter = new Counter(manager, id); counter.set(value); return counter;
    }
}
