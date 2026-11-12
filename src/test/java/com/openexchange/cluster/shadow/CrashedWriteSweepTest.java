// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowEntry.State;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.openexchange.cluster.shadow.ShadowRig.F1;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * The crashed-write sweep shared by the replica and the member producer, over a real archive: this producer's
 * TOO_SHORT and NO_TRAILER copies from before this process (seq below the writer's first seq) are purged through
 * retention, counted, once; a copy of this process, another producer's crashed copy and every usable copy stay. The
 * negative control sweeps other producers too and the row's own assertion fails with the named text.
 */
public class CrashedWriteSweepTest {

    record Recs(long noTrailer3, long tooShort4, long noTrailer5, long otherTooShort, long usable2) {
    }

    /** F1: COMPLETE seq 2, NO_TRAILER seq 3 (header), TOO_SHORT seq 4 (no header), NO_TRAILER seq 5; R3 TOO_SHORT. */
    static Recs story(final FloorRig r) {
        final long usable = r.shadow(100_000, FloorRig.SAFE_SEQ, FloorRig.payload(100_000), F1, ShadowKind.ALIGNED);
        final byte[] torn = FloorRig.payload(200_000);
        final long nt3 = r.rig.raw(ShadowRig.header(F1, 3, ShadowKind.ALIGNED, 200_000, ShadowRig.GENESIS, 0, torn),
                torn, ShadowRig.Raw.NO_TRAILER).recordingId();
        final long ts4 = r.rig.rawMessages(new ShadowAlias(ShadowRig.ENGINE, 0, ShadowRig.GENESIS, 300_000, 3,
                ProducerKind.FOLLOWER.code(), 1, 4, ShadowKind.ALIGNED.code()).format(), new byte[] {1, 2, 3});
        final long nt5 = r.rig.raw(ShadowRig.header(F1, 5, ShadowKind.ALIGNED, 400_000, ShadowRig.GENESIS, 0, torn),
                torn, ShadowRig.Raw.NO_TRAILER).recordingId();
        final long other = r.rig.rawMessages(new ShadowAlias(ShadowRig.ENGINE, 0, ShadowRig.GENESIS, 300_000, 3,
                ProducerKind.REPLICA.code(), 3, 1, ShadowKind.ALIGNED.code()).format(), new byte[] {4});
        return new Recs(nt3, ts4, nt5, other, usable);
    }

    static void sweep(final Set<NegativeControl> off, final List<String> lines) {
        try (FloorRig r = new FloorRig()) {
            final Recs recs = story(r);
            final ShadowCatalog c = r.rig.catalog();
            c.verifyAllComplete();
            final ShadowCounters counters = new ShadowCounters();
            try (ShadowRetention retention = new ShadowRetention(c, r.floor(), 10, r.hk)) {
                final ShadowCatalog.Scan before = c.scan();
                assertEquals(State.NO_TRAILER, before.entry(recs.noTrailer3()).state());
                assertEquals(State.TOO_SHORT, before.entry(recs.tooShort4()).state());
                assertEquals(State.NO_TRAILER, before.entry(recs.noTrailer5()).state());
                assertEquals(State.TOO_SHORT, before.entry(recs.otherTooShort()).state());
                final CrashedWriteSweep sweep = new CrashedWriteSweep(F1, "[TEST]", lines::add, counters, off);
                assertEquals("not armed: nothing", 0, sweep.sweep(before, 0, retention));
                final int purged = sweep.sweep(before, 5, retention);
                final ShadowCatalog.Scan after = c.scan();
                if (after.entry(recs.otherTooShort()) == null) {
                    throw new AssertionError("a crashed write of another producer was purged: rec="
                            + recs.otherTooShort());
                }
                assertEquals(2, purged);
                assertEquals(2, sweep.purgedTotal());
                assertEquals(2, counters.get(ShadowCounters.Counter.CRASHED_WRITES_PURGED_TOTAL));
                assertNull("F1 seq 3 (before this process) purged", after.entry(recs.noTrailer3()));
                assertNull("F1 seq 4, header-less: the alias carries the seq", after.entry(recs.tooShort4()));
                assertNotNull("F1 seq 5 is this process's: never swept", after.entry(recs.noTrailer5()));
                assertNotNull("a usable copy is never swept", after.entry(recs.usable2()));
                assertEquals("once per recording", 0, sweep.sweep(before, 5, retention));
                assertEquals(0, sweep.sweep(after, 5, retention));
                assertEquals(2, counters.get(ShadowCounters.Counter.CRASHED_WRITES_PURGED_TOTAL));
                assertEquals("an unknown scan answers nothing", 0, sweep.sweep(new ShadowCatalog.Scan(after.all(),
                        after.usable(), after.verified(), after.conflicts(), 1, java.util.Map.of(), true, 0, null, 0),
                        9, retention));
            }
        }
    }

    @Test(timeout = 120_000)
    public void onlyThisProducersCrashedWritesFromBeforeThisProcessAreSwept() {
        final List<String> lines = new ArrayList<>();
        sweep(Set.of(), lines);
        assertEquals(lines.toString(), 2, lines.size());
        ShadowRig.green("crashed-write sweep: " + lines);
        nc("SWEEP_OTHER_PRODUCERS", "a crashed write of another producer was purged",
                () -> sweep(Set.of(NegativeControl.SWEEP_OTHER_PRODUCERS), new ArrayList<>()));
    }
}
