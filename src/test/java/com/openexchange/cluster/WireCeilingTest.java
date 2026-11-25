// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import com.openexchange.cluster.WireCeiling.Verdict;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The wire ceiling rule: the gate table (the recorded ceiling decides before the jar range), and the invalid,
 * lowers and jar-below texts.
 */
public class WireCeilingTest {

    /** The gate's signature, so the table can be run against a deliberately broken variant. */
    interface Gate {
        Verdict gate(int headerVersion, long recordedMax, int jarMin, int jarMax);
    }

    /**
     * Runs the gate table and returns the names of the cases that failed, empty when all pass.
     * Jar ranges are {@code [10, 10]} (the jar at the ceiling) and {@code [10, 11]} (the next jar).
     */
    static List<String> gateTableFailures(final Gate g) {
        final List<String> failed = new ArrayList<>();
        check(failed, "nothing recorded, in range", g.gate(10, 0, 10, 10), Verdict.ACCEPT);
        check(failed, "nothing recorded, above the jar", g.gate(11, 0, 10, 10), Verdict.DROP_OUT_OF_RANGE);
        check(failed, "nothing recorded, below the jar", g.gate(9, 0, 10, 10), Verdict.DROP_OUT_OF_RANGE);
        check(failed, "nothing recorded, the next jar accepts its top", g.gate(11, 0, 10, 11), Verdict.ACCEPT);
        check(failed, "at the ceiling", g.gate(10, 10, 10, 11), Verdict.ACCEPT);
        check(failed, "above the ceiling, above the jar", g.gate(11, 10, 10, 10), Verdict.DROP_ABOVE_CEILING);
        check(failed, "step 1 ignores the jar range", g.gate(11, 10, 10, 11), Verdict.DROP_ABOVE_CEILING);
        check(failed, "below the floor under a ceiling", g.gate(9, 10, 10, 11), Verdict.DROP_OUT_OF_RANGE);
        check(failed, "after the raise, the next jar", g.gate(11, 11, 10, 11), Verdict.ACCEPT);
        check(failed, "after the raise, a frame above the next jar", g.gate(12, 11, 10, 11),
                Verdict.DROP_ABOVE_CEILING);
        check(failed, "ceiling above the jar, frame above the jar", g.gate(12, 12, 10, 11),
                Verdict.DROP_OUT_OF_RANGE);
        return failed;
    }

    private static void check(final List<String> failed, final String name, final Verdict actual,
                              final Verdict expected) {
        if (actual != expected) {
            failed.add(name + ": expected " + expected + " but was " + actual);
        }
    }

    @Test
    public void theGateTableHolds() {
        assertEquals(List.of(), gateTableFailures(WireCeiling::gate));
    }

    /**
     * Control: a gate that checks the jar range before the recorded ceiling fails the table by name. The order
     * matters on the earlier jar: a frame above both the ceiling and the jar must be an above-ceiling drop there
     * too, because an engine may treat the two drops differently (match journals one template as INVALID).
     */
    @Test
    public void aGateThatChecksTheJarRangeFirstFailsTheTable() {
        final Gate swapped = (v, recorded, min, max) -> {
            if (v < min || v > max) {
                return Verdict.DROP_OUT_OF_RANGE;
            }
            if (recorded > 0 && v > recorded) {
                return Verdict.DROP_ABOVE_CEILING;
            }
            return Verdict.ACCEPT;
        };
        assertEquals(List.of(
                        "above the ceiling, above the jar: expected DROP_ABOVE_CEILING but was DROP_OUT_OF_RANGE",
                        "after the raise, a frame above the next jar: expected DROP_ABOVE_CEILING but was DROP_OUT_OF_RANGE"),
                gateTableFailures(swapped));
    }

    /** Control: a gate that lets the jar's top stand in for the recorded ceiling fails by name. */
    @Test
    public void aGateThatReadsTheJarInPlaceOfTheRecordedCeilingFailsTheTable() {
        final Gate jarAsCeiling = (v, recorded, min, max) -> WireCeiling.gate(v, max, min, max);
        final List<String> failed = gateTableFailures(jarAsCeiling);
        assertTrue(failed.toString(), failed.contains(
                "step 1 ignores the jar range: expected DROP_ABOVE_CEILING but was ACCEPT"));
    }

    @Test
    public void invalidReasonNamesTheRange() {
        assertNull(WireCeiling.invalidReason(10, 10));
        assertNull(WireCeiling.invalidReason(65_534, 10));
        assertEquals("maxWireVersion=9 must be in [10, 65534]", WireCeiling.invalidReason(9, 10));
        assertEquals("maxWireVersion=0 must be in [10, 65534]", WireCeiling.invalidReason(0, 10));
        assertEquals("maxWireVersion=65535 must be in [10, 65534]", WireCeiling.invalidReason(65_535, 10));
        assertEquals("maxWireVersion=-1 must be in [12, 65534]", WireCeiling.invalidReason(-1, 12));
    }

    @Test
    public void theCeilingOnlyRises() {
        assertTrue(WireCeiling.lowers(10, 11));
        assertFalse(WireCeiling.lowers(11, 11));
        assertFalse(WireCeiling.lowers(12, 11));
    }

    @Test
    public void aJarBelowTheRecordedCeilingIsNamed() {
        assertNull(WireCeiling.jarBelow(0, 10, "money-schema"));
        assertNull(WireCeiling.jarBelow(10, 10, "money-schema"));
        assertNull(WireCeiling.jarBelow(10, 11, "money-schema"));
        assertEquals("recorded maxWireVersion=11 is above this jar's money-schema version 10",
                WireCeiling.jarBelow(11, 10, "money-schema"));
        assertEquals("recorded maxWireVersion=13 is above this jar's order-schema version 12",
                WireCeiling.jarBelow(13, 12, "order-schema"));
    }
}
