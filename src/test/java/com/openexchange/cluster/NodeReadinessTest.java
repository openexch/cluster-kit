// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.service.Cluster.Role;
import org.junit.Test;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

public class NodeReadinessTest {
    private final AtomicLong now = new AtomicLong(1_000_000_000L);
    private final NodeReadiness r = new NodeReadiness(30_000, 5_000, now::get);
    private void advance(long ms) { now.addAndGet(ms * 1_000_000); }
    private void observe(long applied, long commit) {
        r.tick();
        r.observe(42, 7, 2, Role.FOLLOWER, applied, commit, true, true, 0);
    }
    private void start() { r.started(); observe(100, 100); assertTrue(r.describe(), r.ready()); }

    @Test public void unknownStartFailsClosed() {
        assertFalse(r.live()); assertFalse(r.ready());
        r.started(); assertTrue(r.live()); assertFalse(r.ready());
    }
    @Test public void laggingFollowerMustApplyObservedCheckpoint() {
        r.started(); observe(0, 100); assertFalse(r.ready());
        observe(100, 120); assertTrue(r.describe(), r.ready());
    }
    @Test public void movingCommitDoesNotRequireInstantEquality() {
        r.started(); observe(0, 100);
        for (int i = 1; i <= 1000; i++) {
            advance(10); observe(i * 100, (i + 1) * 100);
            assertTrue(r.describe(), r.ready());
        }
    }
    @Test public void idleHeartbeatAndSameCheckpointStayHealthy() {
        start();
        for (int i = 0; i < 100; i++) {
            advance(1000); observe(100, 100);
            assertTrue(r.live()); assertTrue(r.describe(), r.ready());
        }
    }
    @Test public void stalledApplyCannotReuseAnOldCheckpoint() {
        start(); advance(10); observe(100, 200);
        advance(1001); observe(100, 300);
        assertFalse(r.ready()); assertTrue(r.describe().contains("stale-checkpoint"));
        observe(300, 300); assertTrue(r.ready());
    }
    @Test public void largeLagFailsEvenWithRecentCheckpoint() {
        start(); advance(10); observe(100, 100 + NodeReadiness.MAX_APPLY_LAG_BYTES + 1);
        assertFalse(r.ready()); assertTrue(r.describe().contains("apply-lag"));
    }
    @Test public void electionInvalidatesEvidenceEvenIfServiceRoleIsStillFollower() {
        start(); r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, false, true, 0);
        assertFalse(r.ready()); assertTrue(r.live());
        assertTrue(r.describe().contains("election"));
    }
    @Test public void changedTermSourceOrElectionRequiresNewCheckpoint() {
        for (int dimension = 0; dimension < 3; dimension++) {
            start();
            r.observe(dimension == 0 ? 43 : 42, dimension == 1 ? 8 : 7,
                    dimension == 2 ? 3 : 2, Role.FOLLOWER, 100, 101, true, true, 0);
            assertFalse("old proof crossed a fence", r.ready());
        }
    }
    @Test public void roleChangeDoesNotReuseCatchup() {
        start(); r.roleChanged(Role.LEADER); assertFalse(r.ready());
        r.observe(42, 8, 3, Role.LEADER, 200, 200, true, true, 0); assertTrue(r.ready());
    }
    @Test public void regressedPositionRequiresFreshProof() {
        start(); observe(90, 95); assertFalse(r.ready());
        observe(95, 95); assertTrue(r.ready());
    }
    @Test public void freshServiceTickCannotRefreshStaleObservation() {
        start(); advance(5001); r.tick(); assertTrue(r.live()); assertFalse(r.ready());
        assertTrue(r.describe().contains("stale-observation"));
    }
    @Test public void staleConsensusIsSeparateFromIdleBusinessWork() {
        start(); r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, true, true, 5000);
        assertFalse(r.ready()); assertTrue(r.live());
        assertTrue(r.describe().contains("stale-consensus"));
    }
    @Test public void stoppedDutyCycleFailsReadinessBeforeLiveness() {
        start(); advance(5001); assertFalse(r.ready()); assertTrue(r.live());
        advance(25000); assertFalse(r.live());
    }
    @Test public void missingOrInactiveSourceWithdrawsReadiness() {
        start(); r.unavailable("counter-closed"); assertFalse(r.ready());
        observe(100, 100); assertTrue(r.ready());
        r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, true, false, 0); assertFalse(r.ready());
    }
    @Test public void needsReseedIsStickyAcrossTicksAndObservations() {
        start(); r.needsReseed("missing-log"); observe(100, 100);
        assertFalse(r.ready()); assertTrue(r.describe().contains("needs-reseed:missing-log"));
        r.started(); observe(100, 100); assertFalse(r.ready());
    }
    @Test public void stoppingWithdrawsReadiness() {
        start(); r.stopping(); observe(100, 100); assertFalse(r.ready());
        assertTrue(r.describe().contains("stopping"));
    }
    @Test public void unknownPositionsAndBackwardsClockFailClosed() {
        r.started(); observe(-1, -1); assertFalse(r.ready());
        observe(100, 100); advance(-1); assertFalse(r.ready()); assertFalse(r.live());
    }
    @Test public void probeCarriesTheEvidenceUsedForItsStatus() {
        start(); NodeReadiness.Probe p = r.probe();
        assertTrue(p.ready()); assertTrue(p.live());
        for (String field : new String[]{"role=FOLLOWER", "recovery=ready", "term=7", "source=42",
                "applied=100", "commit=100", "checkpoint=100", "maxLagBytes=65536"}) {
            assertTrue(p.detail(), p.detail().contains(field));
        }
    }
}
