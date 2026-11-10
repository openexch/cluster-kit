// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Pure: which listed recording is an orphan candidate, on the configured cluster
 * streams (no literals), below L and above L (left by failed backup cycles); and housekeeping's purge gate
 * waits for the cluster check.
 */
public class OrphanAndPurgeGateTest {

    static final long L = 10;
    static final Set<Integer> STREAMS = Set.of(200, 206, 207);

    @Test
    public void orphansFollowTheConfiguredStreamsAndIncludeIdsAboveL() {
        final Set<Long> referenced = Set.of(3L, 12L);
        final List<String> wrong = new java.util.ArrayList<>();
        check(wrong, "below L on the configured log stream", 5, 100, 200, referenced, true);
        check(wrong, "above L, stopped, unreferenced, snapshot stream", 11, 100, 206, referenced, true);
        check(wrong, "above L still recording", 13, -1, 206, referenced, false);
        check(wrong, "referenced above L", 12, 100, 207, referenced, false);
        check(wrong, "referenced below L", 3, 100, 200, referenced, false);
        check(wrong, "L itself", L, 100, 200, referenced, false);
        check(wrong, "a shadow stream", 6, 100, 4300, referenced, false);
        check(wrong, "Aeron's default log stream when it is not configured", 7, 100, 100, referenced, false);
        if (!wrong.isEmpty()) {
            throw new AssertionError("orphan classification wrong for: " + wrong);
        }
        System.out.println("GREEN orphan classification on streams " + STREAMS);
    }

    private static void check(final List<String> wrong, final String what, final long id, final long stop,
                              final int stream, final Set<Long> referenced, final boolean expected) {
        if (ClusterIdentityCheck.isOrphanCandidate(id, L, stop, stream, STREAMS, referenced) != expected) {
            wrong.add(what + " (id " + id + ", expected " + expected + ")");
        }
    }

    @Test
    public void housekeepingWaitsForTheClusterCheck() {
        final String gate = ReplicaHousekeeping.purgeGate(true, false, null);
        if (gate == null) {
            throw new AssertionError("housekeeping purges before the cluster check passed");
        }
        assertNull(ReplicaHousekeeping.purgeGate(true, true, null));
        assertNotNull(ReplicaHousekeeping.purgeGate(false, true, null));
        assertNotNull(ReplicaHousekeeping.purgeGate(true, true, "fenced"));
        assertFalse(gate.isEmpty());
    }
}
