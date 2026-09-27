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
            final ConsensusReadiness probe = new ConsensusReadiness(context, r);
            long now = System.nanoTime(); probe.poll(cluster, now);
            assertFalse(r.ready());
            applied[0] = 100; probe.poll(cluster, now += 20_000_000);
            assertTrue(r.describe(), r.ready()); // no onRoleChange callback
            context.electionStateCounter().set(ElectionState.CANVASS.code());
            probe.poll(cluster, now += 20_000_000);
            assertFalse(r.ready()); assertTrue(r.live());
            context.electionStateCounter().set(ElectionState.CLOSED.code());
            context.leadershipTermIdCounter().set(8); context.commitPositionCounter().set(200);
            probe.poll(cluster, now += 20_000_000); assertFalse(r.ready());
            applied[0] = 200; probe.poll(cluster, now += 20_000_000); assertTrue(r.ready());
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

    private static Counter counter(CountersManager manager, long value) {
        int id = manager.allocate("readiness test");
        manager.setCounterRegistrationId(id, 100 + id);
        Counter counter = new Counter(manager, id); counter.set(value); return counter;
    }
}
