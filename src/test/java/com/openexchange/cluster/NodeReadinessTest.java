// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.service.Cluster.Role;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The readiness rules, stated as tests because their failure mode is silent and
 * expensive: a node that answers "ready" too early does not break itself, it
 * lets the orchestrator restart the next member and take the cluster's quorum
 * with it.
 */
public class NodeReadinessTest {

    private static NodeReadiness live() {
        final NodeReadiness r = new NodeReadiness();
        r.started();
        r.roleChanged(Role.FOLLOWER);
        return r;
    }

    @Test
    public void aFreshNodeIsNeitherLiveNorReady() {
        final NodeReadiness r = new NodeReadiness();
        assertFalse("never ticked, so nothing is known and nothing may be claimed", r.live());
        assertFalse(r.ready());
        assertEquals("starting", r.describe());
    }

    @Test
    public void startedButRoleUnknownIsNotReady() {
        final NodeReadiness r = new NodeReadiness();
        r.started();
        assertTrue("the duty cycle ticked, so the process is alive", r.live());
        assertFalse("a member with no role cannot be counted on", r.ready());
    }

    @Test
    public void followerAndLeaderAreReady() {
        final NodeReadiness r = live();
        assertTrue(r.ready());
        r.roleChanged(Role.LEADER);
        assertTrue(r.ready());
    }

    @Test
    public void anElectionMakesTheNodeNotReadyButStillLive() {
        final NodeReadiness r = live();
        r.roleChanged(Role.CANDIDATE);
        assertFalse("an election is exactly when a rolling restart must pause", r.ready());
        assertTrue("a member in an election is healthy; killing it would prolong the election",
            r.live());
        assertTrue(r.describe().contains("election"));
    }

    @Test
    public void catchingUpIsNotReady() {
        final NodeReadiness r = live();
        r.catchingUp();
        assertFalse("a replaying member serves stale state", r.ready());
        assertTrue(r.live());
        assertEquals("catching up", r.describe());

        r.catchUpComplete();
        assertTrue(r.ready());
    }

    @Test
    public void aWedgedDutyCycleFailsReadinessFirstAndLivenessLater() throws Exception {
        // Short windows so the wedge is observable without sleeping for 30s.
        final NodeReadiness r = new NodeReadiness(200, 40);
        r.started();
        r.roleChanged(Role.LEADER);
        assertTrue(r.ready());

        Thread.sleep(80);
        assertFalse("readiness goes first: stop sending it work", r.ready());
        assertTrue("but it is too early to kill the process", r.live());
        assertTrue(r.describe().contains("duty cycle"));

        Thread.sleep(200);
        assertFalse("now it is wedged, and a restart is the correct answer", r.live());
    }

    @Test
    public void tickingKeepsItReady() throws Exception {
        final NodeReadiness r = new NodeReadiness(200, 40);
        r.started();
        r.roleChanged(Role.LEADER);
        for (int i = 0; i < 5; i++) {
            Thread.sleep(20);
            r.tick();
            assertTrue("an idle but ticking node stays ready: a quiet market is not a wedge",
                r.ready());
        }
    }

    @Test
    public void stoppingWithdrawsReadinessBeforeTheProcessGoesAway() {
        final NodeReadiness r = live();
        r.stopping();
        assertFalse("drain first: let the orchestrator route away before we die", r.ready());
        assertEquals("stopping", r.describe());
    }
}
