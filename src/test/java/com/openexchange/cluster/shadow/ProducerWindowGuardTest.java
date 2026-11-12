// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The follower producer's window and liveness legs: the decision at the budget boundary, the samples ring and its
 * horizon, the seeded serialize time, and the liveness bound read from the client's inter-service timeout. Each
 * negative control switches one leg off and the row's own assertion fails with the named text.
 */
public class ProducerWindowGuardTest {

    static final long S = TimeUnit.SECONDS.toNanos(1);
    static final long MIB = 1024 * 1024;

    @Test
    public void theDecisionAtTheBudgetBoundary() {
        // R = 1 MB/s, T = 1 s, W = 1 MiB: 1,000,000 B against 524,288 (50%) and 1,048,576 (100%)
        assertTrue(ProducerWindowGuard.defer(1_000_000, S, MIB, 50));
        assertFalse(ProducerWindowGuard.defer(1_000_000, S, MIB, 100));
        assertFalse("exactly at the budget produces", ProducerWindowGuard.defer(524_288, S, MIB, 50));
        assertTrue("one byte over defers", ProducerWindowGuard.defer(524_289, S, MIB, 50));
        assertTrue(ProducerWindowGuard.livenessBound(3 * S, 5 * S));
        assertFalse("exactly half the liveness timeout produces", ProducerWindowGuard.livenessBound(2_500_000_000L,
                5 * S));
        assertTrue("an unknown liveness timeout refuses", ProducerWindowGuard.livenessBound(1, 0));
        ShadowRig.green("window guard boundary: defer(1 MB/s, 1 s, 1 MiB, 50%)=true, (100%)=false; liveness 3 s of"
                + " 5 s refused");
    }

    /** Ten seconds at 1 MB/s, then the decision with a 1 s seeded estimate against a 1 MiB window at 50%. */
    static ProducerWindowGuard.Decision overBudget(final Set<NegativeControl> off) {
        final ProducerWindowGuard g = new ProducerWindowGuard(MIB, 50, 10_000, 1_000, off);
        long now = 1_000 * S;
        for (int i = 0; i <= 10; i++) {
            g.sample(now + i * S, i * 1_000_000L);
        }
        now += 10 * S;
        assertEquals(1_000_000, g.rateBytesPerSecond(now), 1.0);
        final ProducerWindowGuard.Decision d = g.decide(now, 5 * S);
        if (d != ProducerWindowGuard.Decision.WINDOW_BUDGET) {
            throw new AssertionError("the guard produced over budget: " + d + " at " + g.describe(now));
        }
        return d;
    }

    @Test
    public void overTheBudgetDefersAndTheControlProduces() {
        overBudget(Set.of());
        ShadowRig.green("window guard: R x T over W x 50% -> WINDOW_BUDGET");
        nc("WINDOW_GUARD_OFF", "the guard produced over budget", () -> overBudget(Set.of(NegativeControl.WINDOW_GUARD_OFF)));
    }

    static void aboveLiveness(final Set<NegativeControl> off) {
        // estimate 3 s, embedded client liveness 5 s: 3 s > 2.5 s
        final ProducerWindowGuard g = new ProducerWindowGuard(16 * MIB, 50, 10_000, 3_000, off);
        final ProducerWindowGuard.Decision d = g.decide(S, 5 * S);
        if (d != ProducerWindowGuard.Decision.LIVENESS_BOUND) {
            throw new AssertionError("the guard produced above the liveness bound: " + d + " T_last="
                    + TimeUnit.NANOSECONDS.toMillis(g.lastSerializeNs()) + " ms, liveness 5000 ms");
        }
    }

    @Test
    public void aSerializeAboveHalfTheLivenessTimeoutIsRefused() {
        aboveLiveness(Set.of());
        ShadowRig.green("liveness bound: T_last 3000 ms > 5000 / 2 -> LIVENESS_BOUND");
        nc("LIVENESS_BOUND_OFF", "the guard produced above the liveness bound",
                () -> aboveLiveness(Set.of(NegativeControl.LIVENESS_BOUND_OFF)));
    }

    @Test
    public void oneSampleHasNoRateAndProduces() {
        final ProducerWindowGuard g = new ProducerWindowGuard(MIB, 1, 10_000, 1_000);
        assertEquals(ProducerWindowGuard.Decision.PRODUCE, g.decide(S, 5 * S));
        g.sample(S, 1_000_000_000L);
        assertEquals("one sample is no rate", 0.0, g.rateBytesPerSecond(S), 0.0);
        assertEquals(ProducerWindowGuard.Decision.PRODUCE, g.decide(S, 5 * S));
    }

    @Test
    public void samplesOutsideTheHorizonAndTooCloseTogetherAreIgnored() {
        final ProducerWindowGuard g = new ProducerWindowGuard(MIB, 50, 5_000, 1_000);
        assertTrue(g.sample(0, 0));
        assertFalse("within a second of the last", g.sample(S / 2, 10_000_000));
        assertTrue(g.sample(S, 10_000_000));    // a burst of 10 MB in the first second
        for (int i = 2; i <= 20; i++) {
            assertTrue(g.sample(i * S, 10_000_000 + (i - 1) * 1_000L));
        }
        // horizon 5 s: only 1 kB/s remains; the burst at the start is outside it
        assertEquals(1_000, g.rateBytesPerSecond(20 * S), 1.0);
        // the ring holds 64 samples: 100 more, the rate is still read over the horizon only
        for (int i = 21; i <= 120; i++) {
            g.sample(i * S, 10_000_000 + 19_000 + (i - 20) * 2_000L);
        }
        assertEquals(2_000, g.rateBytesPerSecond(120 * S), 1.0);
        assertEquals("a horizon with no sample has no rate", 0.0, g.rateBytesPerSecond(1_000 * S), 0.0);
    }

    @Test
    public void theMeasuredSerializeReplacesTheSeed() {
        final ProducerWindowGuard g = new ProducerWindowGuard(MIB, 50, 10_000, 600_000);
        assertFalse(g.measured());
        assertEquals(ProducerWindowGuard.Decision.LIVENESS_BOUND, g.decide(S, 5 * S));
        g.serialized(TimeUnit.MILLISECONDS.toNanos(200));
        assertTrue(g.measured());
        assertEquals(200, TimeUnit.NANOSECONDS.toMillis(g.lastSerializeNs()));
        assertEquals(ProducerWindowGuard.Decision.PRODUCE, g.decide(S, 5 * S));
    }

    @Test
    public void constructorRefusesOutOfRangeSettings() {
        refused(() -> new ProducerWindowGuard(0, 50, 10_000, 1_000), "windowBytes must be > 0: 0");
        refused(() -> new ProducerWindowGuard(MIB, 0, 10_000, 1_000),
                "SHADOW_PRODUCER_WINDOW_BUDGET_PCT must be 1..100: 0");
        refused(() -> new ProducerWindowGuard(MIB, 101, 10_000, 1_000),
                "SHADOW_PRODUCER_WINDOW_BUDGET_PCT must be 1..100: 101");
        refused(() -> new ProducerWindowGuard(MIB, 50, 999, 1_000),
                "SHADOW_PRODUCER_RATE_WINDOW_MS must be 1000..3600000 ms: 999");
        refused(() -> new ProducerWindowGuard(MIB, 50, 3_600_001, 1_000),
                "SHADOW_PRODUCER_RATE_WINDOW_MS must be 1000..3600000 ms: 3600001");
        refused(() -> new ProducerWindowGuard(MIB, 50, 10_000, 0),
                "SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS must be 1..600000 ms: 0");
        refused(() -> new ProducerWindowGuard(MIB, 50, 10_000, 600_001),
                "SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS must be 1..600000 ms: 600001");
    }

    static void refused(final Runnable r, final String text) {
        try {
            r.run();
            fail("not refused: " + text);
        } catch (final IllegalArgumentException e) {
            assertEquals(text, e.getMessage());
        }
    }
}
