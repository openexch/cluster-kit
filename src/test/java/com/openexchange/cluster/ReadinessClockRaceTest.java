// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.service.Cluster.Role;
import org.junit.Test;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

/** A writer can publish a newer tick between the reader's clock and tick reads. */
public class ReadinessClockRaceTest {
    private static final class Fixture {
        final AtomicLong time = new AtomicLong(1_000_000_000);
        final AtomicBoolean interleave = new AtomicBoolean();
        final NodeReadiness readiness = new NodeReadiness(30_000, 5_000, this::clock);
        long clock() {
            long captured = time.get();
            if (interleave.getAndSet(false)) {
                time.incrementAndGet();
                readiness.tick(); // another duty cycle publishes after the reader captured time
            }
            return captured;
        }
        Fixture() {
            readiness.started();
            readiness.observe(42, 7, 2, Role.FOLLOWER, 100, 100, true, true, 0);
            interleave.set(true);
        }
    }
    @Test public void concurrentFreshTickDoesNotKillLiveness() {
        Fixture f = new Fixture(); assertTrue("newer tick is not a stale process", f.readiness.live());
    }
    @Test public void concurrentFreshTickDoesNotWithdrawReadiness() {
        Fixture f = new Fixture(); assertTrue("newer tick is not stale evidence", f.readiness.ready());
    }
    @Test public void httpProbeUsesOneCapturedDutyCycleTimestamp() {
        Fixture f = new Fixture(); NodeReadiness.Probe p = f.readiness.probe();
        assertTrue(p.detail(), p.live()); assertTrue(p.detail(), p.ready());
    }
}
