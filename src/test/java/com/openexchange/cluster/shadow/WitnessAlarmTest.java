// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The floor-side witness alarm: fewer than two producers at the newest ALIGNED position for longer than the window
 * raises one CRITICAL line per window naming the missing kind and holds the gauge; two producers clear it. The
 * negative control switches the alarm off and the row's own assertion fails with the named text.
 */
public class WitnessAlarmTest {

    static final long MIN = TimeUnit.MINUTES.toNanos(1);
    static final long WINDOW = 120 * MIN; // 2 x a 60 min snapshot interval

    static ShadowEntry copy(final ShadowProducer p, final long position, final ShadowKind kind,
                            final ShadowEntry.State state) {
        final byte[] payload = FloorRig.payload(position);
        final ShadowHeader h = ShadowRig.header(p, 1, kind, position, ShadowRig.GENESIS, 0, payload);
        return new ShadowEntry(1, position + p.producerId(), 0, 1000, "", h.alias(), h, null, 0, null, state, null, -1);
    }

    static LogicalShadow group(final long position, final ShadowEntry... copies) {
        return new LogicalShadow(ShadowRig.ENGINE, 0, ShadowRig.GENESIS, position, List.of(copies), false);
    }

    static ShadowCatalog.Scan scan(final LogicalShadow... verifiedNewestFirst) {
        final List<ShadowEntry> all = new ArrayList<>();
        for (final LogicalShadow v : verifiedNewestFirst) {
            all.addAll(v.copies());
        }
        return new ShadowCatalog.Scan(all, List.of(verifiedNewestFirst), List.of(verifiedNewestFirst), List.of(), 0,
                Map.of(), true, 0, null, 0);
    }

    static final ShadowProducer F1 = ShadowRig.F1;
    static final ShadowProducer R3 = ShadowRig.R3;

    /** The replica is dead: three hours of F-only positions. */
    static List<String> replicaDown(final Set<NegativeControl> off) {
        final WitnessAlarm a = new WitnessAlarm(WINDOW, off);
        final List<String> lines = new ArrayList<>();
        long now = 0;
        for (int hour = 1; hour <= 4; hour++) {
            final long p = hour * 1_000_000L;
            final ShadowCatalog.Scan s = scan(group(p, copy(F1, p, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED)));
            for (int m = 0; m < 60; m += 5, now += 5 * MIN) {
                final WitnessAlarm.Result r = a.observe(now, s);
                if (r.criticalLine() != null) {
                    lines.add(r.criticalLine());
                }
            }
        }
        if (lines.isEmpty()) {
            throw new AssertionError("no floor-side line while the anchor had one witness for "
                    + TimeUnit.NANOSECONDS.toMinutes(now) + " min");
        }
        assertEquals(1, a.missing(ProducerKind.REPLICA));
        assertEquals(0, a.missing(ProducerKind.FOLLOWER));
        return lines;
    }

    @Test
    public void oneProducerPastTheWindowRaisesOneLinePerWindowNamingTheMissingKind() {
        final List<String> lines = replicaDown(Set.of());
        // 4 hours observed every 5 min, window 120 min: a line at 120 min, the next at 240 min
        assertEquals(lines.toString(), 1, lines.size());
        assertEquals("no two-producer anchor for 120 min: newest ALIGNED at P=3000000 has producers [F1]; missing"
                + " kind R (replica)", lines.get(0));
        ShadowRig.green("witness alarm: " + lines.get(0));
        nc("WITNESS_ALARM_OFF", "no floor-side line while the anchor had one witness",
                () -> replicaDown(Set.of(NegativeControl.WITNESS_ALARM_OFF)));
    }

    @Test
    public void twoProducersClearAndOnlyVerifiedAlignedCopiesWitness() {
        final WitnessAlarm a = new WitnessAlarm(WINDOW);
        final long p = 5_000_000;
        final ShadowCatalog.Scan rOnly = scan(group(p, copy(R3, p, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED),
                copy(F1, p, ShadowKind.ALIGNED, ShadowEntry.State.COMPLETE),
                copy(new ShadowProducer(0, ProducerKind.FOLLOWER, 2), p, ShadowKind.STANDBY,
                        ShadowEntry.State.VERIFIED)));
        assertNull(a.observe(0, rOnly).criticalLine());
        final WitnessAlarm.Result held = a.observe(WINDOW, rOnly);
        assertTrue(held.holding());
        assertEquals("no two-producer anchor for 120 min: newest ALIGNED at P=5000000 has producers [R3]; missing"
                + " kind F (designated follower)", held.criticalLine());
        assertEquals(1, a.missing(ProducerKind.FOLLOWER));
        assertNull("one line per window", a.observe(WINDOW + MIN, rOnly).criticalLine());
        final ShadowCatalog.Scan both = scan(group(p, copy(R3, p, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED),
                copy(F1, p, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED)));
        final WitnessAlarm.Result cleared = a.observe(WINDOW + 2 * MIN, both);
        assertFalse(cleared.holding());
        assertEquals(Set.of("R3", "F1"), cleared.producers());
        assertEquals(0, a.missing(ProducerKind.FOLLOWER));
        assertEquals(0, a.missing(ProducerKind.REPLICA));
        assertNull("cleared: the window starts again", a.observe(WINDOW + 3 * MIN, rOnly).criticalLine());
    }

    @Test
    public void noAlignedShadowAtAllMissesBothAndAnUnknownScanChangesNothing() {
        final WitnessAlarm a = new WitnessAlarm(WINDOW);
        final ShadowCatalog.Scan empty = scan();
        a.observe(0, empty);
        final WitnessAlarm.Result r = a.observe(WINDOW, empty);
        assertEquals("no two-producer anchor for 120 min: newest ALIGNED at P=-1 has producers []; missing kind R"
                + " (replica), F (designated follower)", r.criticalLine());
        final ShadowCatalog.Scan unknown = new ShadowCatalog.Scan(List.of(), List.of(), List.of(), List.of(), 1,
                Map.of(), true, 0, null, 0);
        assertFalse(unknown.known());
        assertTrue("an unknown scan keeps the alarm", a.observe(WINDOW + MIN, unknown).holding());
        assertEquals(1, a.missing(ProducerKind.REPLICA));
    }

    @Test
    public void theNewestAlignedGroupIsTheNewestWithAVerifiedAlignedCopy() {
        final LogicalShadow standbyOnly = group(9_000, copy(R3, 9_000, ShadowKind.STANDBY, ShadowEntry.State.VERIFIED));
        final LogicalShadow aligned = group(8_000, copy(R3, 8_000, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED));
        assertEquals(8_000, WitnessAlarm.newestAligned(scan(standbyOnly, aligned)).logPosition());
        assertNull(WitnessAlarm.newestAligned(scan(standbyOnly)));
    }
}
