// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** The live oracle's settings: defaults, both ends of each range, and the refusal text one step outside. */
public class ReplicaOracleSettingsTest {

    static void refused(final ReplicaConfig.Builder b, final String text) {
        try {
            b.build();
            fail("accepted: expected '" + text + "'");
        } catch (final IllegalArgumentException expected) {
            assertEquals(text, expected.getMessage());
        }
    }

    @Test
    public void defaults() {
        final ReplicaConfig c = ReplicaContextsTest.base().build();
        assertEquals(0, c.mismatchHoldMs());
        assertEquals(new File("/var/lib/core-replica3", "mismatch"), c.mismatchDir());
        assertEquals(2048, c.mismatchDumpMaxMb());
        assertEquals(30_000, c.fencePurgeWaitMs());
        assertEquals(0, c.memberCadenceMs());
    }

    @Test
    public void mismatchHoldRange() {
        ReplicaContextsTest.base().mismatchHoldMs(0).build();
        ReplicaContextsTest.base().mismatchHoldMs(1).build();
        ReplicaContextsTest.base().mismatchHoldMs(604_800_000L).build();
        refused(ReplicaContextsTest.base().mismatchHoldMs(-1),
                "REPLICA_MISMATCH_HOLD_MS must be 0 (hold until stopped) or 1..604800000 ms: -1");
        refused(ReplicaContextsTest.base().mismatchHoldMs(604_800_001L),
                "REPLICA_MISMATCH_HOLD_MS must be 0 (hold until stopped) or 1..604800000 ms: 604800001");
    }

    @Test
    public void dumpCapRange() {
        ReplicaContextsTest.base().mismatchDumpMaxMb(0).build();
        ReplicaContextsTest.base().mismatchDumpMaxMb(16_384).build();
        refused(ReplicaContextsTest.base().mismatchDumpMaxMb(-1), "REPLICA_MISMATCH_DUMP_MAX_MB must be 0..16384: -1");
        refused(ReplicaContextsTest.base().mismatchDumpMaxMb(16_385),
                "REPLICA_MISMATCH_DUMP_MAX_MB must be 0..16384: 16385");
    }

    @Test
    public void fencePurgeWaitRange() {
        ReplicaContextsTest.base().fencePurgeWaitMs(1_000).build();
        ReplicaContextsTest.base().fencePurgeWaitMs(600_000).build();
        refused(ReplicaContextsTest.base().fencePurgeWaitMs(999),
                "REPLICA_FENCE_PURGE_WAIT_MS must be 1000..600000 ms: 999");
        refused(ReplicaContextsTest.base().fencePurgeWaitMs(600_001),
                "REPLICA_FENCE_PURGE_WAIT_MS must be 1000..600000 ms: 600001");
    }

    @Test
    public void memberCadenceCrossCheck() {
        // default age 30 min covers a 5 min cadence plus the 180 s verify wait
        ReplicaContextsTest.base().memberCadenceMs(300_000).build();
        refused(ReplicaContextsTest.base().memberCadenceMs(-1),
                "REPLICA_MEMBER_CADENCE_MS must be 0 (not checked) or 1..86400000 ms: -1");
        refused(ReplicaContextsTest.base().memberCadenceMs(300_000).readyShadowAgeMs(479_999),
                "REPLICA_READY_SHADOW_AGE_MS 479999 is below the member cadence plus REPLICA_VERIFY_WAIT_MS (480000):"
                        + " a dropped ALIGNED would flap readiness");
        ReplicaContextsTest.base().memberCadenceMs(300_000).readyShadowAgeMs(480_000).build();
        ReplicaContextsTest.base().memberCadenceMs(300_000).readyShadowAgeMs(0).build(); // age input off
    }
}
