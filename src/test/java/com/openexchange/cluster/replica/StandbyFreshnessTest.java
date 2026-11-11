// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowKind;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * STANDBY and LOCAL are only as fresh as the newest named member snapshot: while the backup named one above the last
 * EQUAL position they are not written (counted), ALIGNED is unaffected (it is the compare), and they resume after the
 * next EQUAL at or above it. A genesis match alone (the genesis check) never resumes them.
 */
public class StandbyFreshnessTest {

    final OracleVerdictTest o = new OracleVerdictTest();

    ShadowKind standbyAt(final ShadowTriggerPolicy p, final long position) {
        o.t.cursor = new LogCursor(position, position, 1, 0);
        p.onSnapshotAction(position, 1, position, 1);
        return p.producePending();
    }

    void alignedEqualAt(final ShadowTriggerPolicy p, final long position) {
        o.t.cursor = new LogCursor(position, position, 1, 0);
        o.retrieved.put(position, o.copy(false, OracleVerdictTest.GENESIS));
        p.onSnapshotAction(position, 1, position, 0);
        assertEquals(ShadowKind.ALIGNED, p.producePending());
    }

    void suspensionRow(final Set<ReplicaNegativeControl> off, final boolean genesisCheckAsRule) {
        final AlignmentTrust trust = o.trust(true, 0, off);
        final ShadowTriggerPolicy p = o.policy(trust, off);
        if (genesisCheckAsRule) {
            p.freshnessRule = () -> true; // the genesis check passed for the named snapshot (no byte compare yet)
        }
        assertEquals("nothing named: written", ShadowKind.STANDBY, standbyAt(p, 1_000));
        int recoveries = 0;
        long newest = 0;
        for (final long named : new long[] {4_096, 12_288}) {
            o.reported = named; // the backup names a member snapshot the replica has not compared
            final ShadowKind k = standbyAt(p, named - 1_024);
            if (k != null) {
                throw new AssertionError((genesisCheckAsRule
                        ? "STANDBY resumed on a genesis match before the byte compare at "
                        : "STANDBY written while a named member snapshot was uncompared at ") + named);
            }
            alignedEqualAt(p, named); // ALIGNED is unaffected: it is the compare
            assertEquals(named, trust.verifiedAt());
            assertEquals("resumes after the EQUAL", ShadowKind.STANDBY, standbyAt(p, named + 512));
            recoveries++;
            newest = named;
        }
        assertEquals(2, trust.staleNamedTotal());
        assertEquals("recoveries", 2, recoveries);
        assertTrue(newest > 0);
    }

    @Test
    public void aNamedSnapshotAboveTheLastEqualSuspendsStandbyUntilTheNextEqualTwice() {
        suspensionRow(EnumSet.noneOf(ReplicaNegativeControl.class), false);
        System.out.println("GREEN freshness: suspended twice, resumed twice");
    }

    @Test
    public void controlFreshnessOffWritesStandbyWhileANamedSnapshotIsUncompared() {
        OracleVerdictTest.expectControl("STANDBY_FRESHNESS_OFF", () -> new StandbyFreshnessTest().suspensionRow(
                EnumSet.of(ReplicaNegativeControl.STANDBY_FRESHNESS_OFF), false),
                "STANDBY written while a named member snapshot was uncompared at 4096");
    }

    @Test
    public void controlGenesisCheckAsTheRuleResumesBeforeTheByteCompare() {
        OracleVerdictTest.expectControl("genesis check as the freshness rule (seam)",
                () -> new StandbyFreshnessTest().suspensionRow(EnumSet.noneOf(ReplicaNegativeControl.class), true),
                "STANDBY resumed on a genesis match before the byte compare at 4096");
    }

    @Test
    public void aDroppedAlignedKeepsStandbySuspendedUntilTheNextEqual() {
        final AlignmentTrust trust = o.trust(true, 0, EnumSet.noneOf(ReplicaNegativeControl.class));
        final ShadowTriggerPolicy p = o.policy(trust, EnumSet.noneOf(ReplicaNegativeControl.class));
        o.reported = 4_096;
        o.t.cursor = new LogCursor(4_096, 4_096, 1, 0);
        p.onSnapshotAction(4_096, 1, 4_096, 0);
        p.producePending(); // held, never retrieved
        o.t.now[0] += TimeUnit.MILLISECONDS.toNanos(OracleVerdictTest.WAIT_MS + 1_000);
        p.pollHeld(4_096);
        assertEquals("dropped at the verify wait", 1, p.verifyTimeouts());
        assertNull("still suspended", standbyAt(p, 5_000));
        alignedEqualAt(p, 8_192);
        o.reported = 8_192;
        assertEquals(ShadowKind.STANDBY, standbyAt(p, 9_000));
        // the readiness bound for these parameters: one member cadence plus the verify wait inside the shadow age
        final long cadence = 300_000;
        assertNull(ReplicaConfig.readyShadowAgeBelowCadence(ReplicaConfig.DEFAULT_READY_SHADOW_AGE_MS, cadence,
                ReplicaConfig.DEFAULT_VERIFY_WAIT_MS));
        assertNotNull(ReplicaConfig.readyShadowAgeBelowCadence(400_000, cadence, ReplicaConfig.DEFAULT_VERIFY_WAIT_MS));
    }
}
