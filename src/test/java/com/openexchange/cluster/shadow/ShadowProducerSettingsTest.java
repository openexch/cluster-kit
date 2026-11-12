// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The member producer settings: defaults, every boundary of every row, the refusal texts, the opt-out line. */
public class ShadowProducerSettingsTest {

    static ShadowProducerSettings parse(final Map<String, String> env) {
        return ShadowProducerSettings.parse(env::get);
    }

    static void refused(final Map<String, String> env, final String text) {
        try {
            parse(env);
            fail("accepted " + env + ", expected: " + text);
        } catch (final IllegalArgumentException e) {
            assertEquals(text, e.getMessage());
        }
    }

    @Test
    public void defaults() {
        final ShadowProducerSettings s = parse(Map.of());
        assertEquals(ShadowProducerSettings.defaults(), s);
        assertEquals(ShadowProducerSettings.Mode.DESIGNATED_FOLLOWER, s.mode());
        assertTrue(s.enabled());
        assertEquals("4m", s.termLength());
        assertEquals(512, s.bufferMb());
        assertEquals(512 * 1024 * 1024, s.bufferBytes());
        assertEquals(10_000, s.connectTimeoutMs());
        assertEquals(30_000, s.offerTimeoutMs());
        assertEquals(60_000, s.recordTimeoutMs());
        assertEquals(50, s.windowBudgetPct());
        assertEquals(1_000, s.serializeEstimateMs());
        assertEquals(10_000, s.rateWindowMs());
        assertEquals(List.of(), s.criticalLines());
        assertEquals(s, parse(Map.of("SHADOW_PRODUCER", " ", "SHADOW_PRODUCER_BUFFER_MB", "")));
    }

    @Test
    public void theProducerModeAndItsOptOutLine() {
        final ShadowProducerSettings none = parse(Map.of("SHADOW_PRODUCER", "none"));
        assertFalse(none.enabled());
        assertEquals(List.of("SHADOW_PRODUCER=none: this member never writes an ALIGNED: the two-producer anchor"
                + " depends on other producers"), none.criticalLines());
        assertTrue(parse(Map.of("SHADOW_PRODUCER", "designated-follower")).enabled());
        refused(Map.of("SHADOW_PRODUCER", "both"), "SHADOW_PRODUCER 'both' (expected designated-follower|none)");
        refused(Map.of("SHADOW_PRODUCER", "CADENCE"), "SHADOW_PRODUCER 'CADENCE' (expected designated-follower|none)");
    }

    @Test
    public void everyNumericBoundary() {
        boundary("SHADOW_PRODUCER_BUFFER_MB", 1, 2047, "");
        boundary("SHADOW_WRITE_CONNECT_TIMEOUT_MS", 1, 86_400_000, " ms");
        boundary("SHADOW_WRITE_OFFER_TIMEOUT_MS", 1, 86_400_000, " ms");
        boundary("SHADOW_WRITE_RECORD_TIMEOUT_MS", 1, 86_400_000, " ms");
        boundary("SHADOW_PRODUCER_WINDOW_BUDGET_PCT", 1, 100, "");
        boundary("SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS", 1, 600_000, " ms");
        boundary("SHADOW_PRODUCER_RATE_WINDOW_MS", 1_000, 3_600_000, " ms");
    }

    static void boundary(final String name, final long min, final long max, final String unit) {
        parse(Map.of(name, String.valueOf(min)));
        parse(Map.of(name, String.valueOf(max)));
        refused(Map.of(name, String.valueOf(min - 1)), name + " must be " + min + ".." + max + unit + ": " + (min - 1));
        refused(Map.of(name, String.valueOf(max + 1)), name + " must be " + min + ".." + max + unit + ": " + (max + 1));
        refused(Map.of(name, "12x"), name + " is not an integer: 12x");
    }

    @Test
    public void theShadowTermLengthIsTheWritersRule() {
        assertEquals("64k", parse(Map.of("SHADOW_TERM_LENGTH", "64k")).termLength());
        assertEquals("16m", parse(Map.of("SHADOW_TERM_LENGTH", "16m")).termLength());
        refused(Map.of("SHADOW_TERM_LENGTH", "32k"), "SHADOW_TERM_LENGTH 32k outside [64k, 16m] or not a power of two"
                + " (every replay maps 3 x the recording's term length)");
        refused(Map.of("SHADOW_TERM_LENGTH", "32m"), "SHADOW_TERM_LENGTH 32m outside [64k, 16m] or not a power of two"
                + " (every replay maps 3 x the recording's term length)");
        refused(Map.of("SHADOW_TERM_LENGTH", "3m"), "SHADOW_TERM_LENGTH 3m outside [64k, 16m] or not a power of two"
                + " (every replay maps 3 x the recording's term length)");
        refused(Map.of("SHADOW_TERM_LENGTH", "four"), "SHADOW_TERM_LENGTH four is not a size (expected <n>, <n>k or"
                + " <n>m)");
    }
}
