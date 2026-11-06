// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.service.Cluster.Role;
import org.junit.Test;
import static org.junit.Assert.assertFalse;

public class ReadinessEvidenceTest {
    @Test public void followerRoleAloneDoesNotProveCatchup() {
        NodeReadiness r = new NodeReadiness();
        r.started();
        r.roleChanged(Role.FOLLOWER);
        assertFalse("no applied/commit/election evidence was supplied", r.ready());
    }

    @Test public void leaderRoleAloneDoesNotProveCatchup() {
        NodeReadiness r = new NodeReadiness();
        r.started();
        r.roleChanged(Role.LEADER);
        assertFalse("leader role is not service application evidence", r.ready());
    }

    @Test public void manualCatchupCannotBypassMissingEvidence() {
        NodeReadiness r = new NodeReadiness();
        r.started();
        r.roleChanged(Role.FOLLOWER);
        r.catchUpComplete();
        assertFalse("a boolean cannot replace current consensus evidence", r.ready());
    }
}
