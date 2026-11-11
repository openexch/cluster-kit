// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.PurgeResult;
import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import com.openexchange.cluster.shadow.ShadowEntry.State;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.openexchange.cluster.shadow.FloorRig.P_1;
import static com.openexchange.cluster.shadow.FloorRig.P_2;
import static com.openexchange.cluster.shadow.FloorRig.P_CM;
import static com.openexchange.cluster.shadow.FloorRig.SAFE_SEQ;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The one purge owner. Retention N counts positions; only COMPLETE and VERIFIED copies
 * beyond N and not pinned are purged; CONFLICT, FOREIGN, CORRUPT and the crash shapes are never purged by retention
 * (counted); the floor's anchor and margin are pinned against an operator purge without {@code force}; one owner per
 * archive (C9); and the housekeeping hook paces deep reads, the startup pass included.
 */
public class ShadowRetentionTest {

    static final long P_100 = 100_000;
    static final long P_160 = 160_000;
    static final long P_200 = 200_000;
    static final long P_120 = 120_000;
    static final long P_140 = 140_000;
    static final long P_180 = 180_000;

    record Story(FloorRig rig, ShadowCatalog catalog, ShadowFloor floor, ShadowRetention retention, long anchorRec,
                 long marginRec, List<Long> conflictRecs, long corruptRec, long foreignRec, long noTrailerRec,
                 List<Long> oldUsable) implements AutoCloseable {
        @Override
        public void close() {
            retention.close();
            rig.close();
        }
    }

    /** VERIFIED at 100k, 200k, P_1, P_2, P_cm; CONFLICT at 160k; CORRUPT at 140k; FOREIGN at 180k; NO_TRAILER at 120k. */
    static Story story(final int retainN, final Set<NegativeControl> off) {
        final FloorRig r = new FloorRig();
        final long r100 = r.shadow(P_100, SAFE_SEQ);
        final long r200 = r.shadow(P_200, SAFE_SEQ);
        final long r1 = r.shadow(P_1, SAFE_SEQ);
        final long r2 = r.shadow(P_2, SAFE_SEQ);
        final long rcm = r.shadow(P_CM, SAFE_SEQ);
        final long c1 = r.shadow(P_160, SAFE_SEQ);
        final long c2 = r.conflictAt(P_160, SAFE_SEQ);
        final byte[] body = FloorRig.payload(P_140);
        final ShadowHeader h = new ShadowHeader(ShadowRig.ENGINE, 0, ShadowKind.ALIGNED.code(),
                ShadowRig.R3.kind().code(), ShadowRig.R3.producerId(), 900, ShadowRig.GENESIS, P_140, 3, 0,
                1_000 + P_140, 1, SAFE_SEQ, body.length, ShadowRig.digest(body), ShadowRig.CHUNK, 7);
        final long corrupt = r.rig.raw(h, body, ShadowRig.Raw.flip(10)).recordingId();
        final long foreign = r.shadow(P_180, SAFE_SEQ, FloorRig.payload(P_180), ShadowRig.GENESIS + 1);
        final byte[] torn = FloorRig.payload(P_120);
        final ShadowHeader th = new ShadowHeader(ShadowRig.ENGINE, 0, ShadowKind.ALIGNED.code(),
                ShadowRig.R3.kind().code(), ShadowRig.R3.producerId(), 901, ShadowRig.GENESIS, P_120, 3, 0,
                1_000 + P_120, 1, SAFE_SEQ, torn.length, ShadowRig.digest(torn), ShadowRig.CHUNK, 7);
        final long noTrailer = r.rig.raw(th, torn, ShadowRig.Raw.NO_TRAILER).recordingId();
        final ShadowCatalog c = r.rig.catalog();
        c.verifyAllComplete();
        final ShadowFloor floor = r.floor();
        final ShadowRetention retention = new ShadowRetention(c, floor, retainN, r.hk, off);
        return new Story(r, c, floor, retention, rcm, r2, List.of(c1, c2), corrupt, foreign, noTrailer,
                List.of(r100, r200, r1));
    }

    /** Retention over the story; asserts the evidence states survive. Returns the pass. */
    static ShadowRetention.Pass retentionPass(final Set<NegativeControl> off) {
        try (Story s = story(2, off)) {
            final Scan before = s.catalog().scan();
            assertEquals(State.CORRUPT, before.entry(s.corruptRec()).state());
            assertEquals(State.FOREIGN, before.entry(s.foreignRec()).state());
            assertEquals(State.NO_TRAILER, before.entry(s.noTrailerRec()).state());
            assertEquals(State.CONFLICT, before.entry(s.conflictRecs().get(0)).state());
            final ShadowRetention.Pass pass = s.retention().apply(before);
            final Scan after = s.catalog().scan();
            for (final long rec : s.conflictRecs()) {
                if (after.entry(rec) == null) {
                    fail("CONFLICT at " + P_160 + " resolved by deletion (rec " + rec + ")");
                }
            }
            assertNotNull("CORRUPT is evidence", after.entry(s.corruptRec()));
            assertNotNull("FOREIGN is evidence", after.entry(s.foreignRec()));
            assertNotNull("NO_TRAILER is the writer's", after.entry(s.noTrailerRec()));
            // N = 2 newest usable positions (P_cm, P_2) plus pins (anchor P_cm, margin P_2); older usable copies go
            for (final long rec : s.oldUsable()) {
                assertEquals("old usable copy rec " + rec + " must be purged", null, after.entry(rec));
            }
            assertEquals(List.of(P_CM, P_2), after.usable().stream().map(LogicalShadow::logPosition).toList());
            assertEquals(Set.of(P_CM, P_2), pass.pinnedPositions());
            assertEquals(3, pass.purged().size());
            assertEquals(3, s.rig().hk.get(ShadowHousekeeping.Counter.RETENTION_PURGED_TOTAL));
            assertEquals(2, s.rig().hk.skippedNonUsable(State.CONFLICT));
            assertEquals(1, s.rig().hk.skippedNonUsable(State.CORRUPT));
            assertEquals(1, s.rig().hk.skippedNonUsable(State.FOREIGN));
            assertEquals(1, s.rig().hk.skippedNonUsable(State.NO_TRAILER));
            assertEquals(1, pass.conflicts());
            return pass;
        }
    }

    @Test
    public void retentionNPurgesOnlyUsableCopiesBeyondNAndNeverEvidence() {
        final ShadowRetention.Pass pass = retentionPass(Set.of());
        ShadowRig.green("9.2 retention N=2: purged " + pass.purged().size() + ", skipped " + pass.skippedNonUsable()
                + ", pins " + pass.pinnedPositions());
        nc("retention purges a CONFLICT side", "CONFLICT at " + P_160 + " resolved by deletion",
                () -> retentionPass(Set.of(NegativeControl.RETENTION_PURGES_CONFLICT)));
    }

    @Test
    public void pinsSurviveRetentionAndAnOperatorPurgeWithoutForce() {
        try (Story s = story(1, Set.of())) {
            final Scan scan = s.catalog().scan();
            final ShadowRetention.Pass pass = s.retention().apply(scan);
            // N = 1 keeps P_cm only, but the margin P_2 is pinned
            final Scan after = s.catalog().scan();
            assertNotNull("anchor pinned", after.entry(s.anchorRec()));
            assertNotNull("margin pinned", after.entry(s.marginRec()));
            assertEquals(Set.of(P_CM, P_2), pass.pinnedPositions());
            final PurgeResult anchor = s.retention().purge(s.anchorRec(), false);
            assertEquals(anchor.detail(), ShadowRetention.PURGE_CODE_PINNED, anchor.code());
            assertTrue(anchor.detail(), anchor.detail().startsWith("PURGE_REFUSED(PINNED)"));
            final PurgeResult margin = s.retention().purge(s.marginRec(), false);
            assertEquals(margin.detail(), ShadowRetention.PURGE_CODE_PINNED, margin.code());
            assertEquals(2, s.rig().hk.get(ShadowHousekeeping.Counter.PURGE_REFUSED_PINNED_TOTAL));
            assertNotNull(s.catalog().scan().entry(s.marginRec()));
            // an operator decision: force deletes it (and the next scan recomputes the pins, C4)
            final PurgeResult forced = s.retention().purge(s.marginRec(), true);
            assertTrue(forced.detail(), forced.purged());
            final Scan later = s.catalog().scan();
            final ShadowFloor.Evaluation e = s.floor().evaluate(later);
            assertEquals("margin recomputed from the scan: none below the anchor now", -1, e.marginPosition());
            assertEquals(Set.of(P_CM), s.retention().pins(later, e));
            ShadowRig.green("9.2 pins: anchor and margin refused PINNED, force purges, pins recomputed");
        }
    }

    @Test
    public void unknownScanPurgesNothing() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_100, SAFE_SEQ);
            r.shadow(P_200, SAFE_SEQ);
            final ShadowCatalog c = r.rig.catalog();
            final ShadowFloor floor = r.floor();
            try (ShadowRetention retention = new ShadowRetention(c, floor, 1, r.hk)) {
                final ShadowRetention.Pass pass = retention.apply(c.scan());
                assertFalse("startup pass pending: retention does not run", pass.ran());
                assertEquals(2, c.scan().usable().size());
            }
        }
    }

    @Test
    public void oneOwnerPerArchive() {
        try (FloorRig r = new FloorRig()) {
            final ShadowCatalog c = r.rig.catalog();
            final ShadowFloor floor = r.floor();
            final ShadowRetention first = new ShadowRetention(c, floor, 3, r.hk);
            try {
                new ShadowRetention(r.rig.catalog(), floor, 3, r.hk);
                fail("a second purge owner over one archive must be refused");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("already has a purge owner"));
                assertTrue(e.getMessage(), e.getMessage().contains("stop the second owner"));
            }
            first.close();
            // the clearing path: the first owner stopped, a new one may start
            new ShadowRetention(c, floor, 3, r.hk).close();
            for (final int bad : new int[] {0, 1001}) {
                try {
                    new ShadowRetention(c, floor, bad, r.hk);
                    fail("SHADOW_RETAIN_N=" + bad + " accepted");
                } catch (final IllegalArgumentException e) {
                    assertEquals("SHADOW_RETAIN_N=" + bad + " out of range [1,1000]", e.getMessage());
                }
            }
            ShadowRig.green("C9: one retention per archive; the second refused; close releases");
        }
    }

    /** A manual clock: the housekeeping reads time only from it. */
    static final class ManualClock implements org.agrona.concurrent.NanoClock {
        final AtomicLong now = new AtomicLong(1_000_000_000L);

        @Override
        public long nanoTime() {
            return now.get();
        }

        void advanceMs(final long ms) {
            now.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
        }
    }

    @Test
    public void housekeepingPacesDeepReadsIncludingTheStartupPass() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_2, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ);
            final ShadowCatalog c = r.rig.catalog();
            final ShadowFloor floor = r.floor();
            final ManualClock clock = new ManualClock();
            try (ShadowRetention retention = new ShadowRetention(c, floor, 3, r.hk)) {
                final AtomicLong refreshes = new AtomicLong();
                final ShadowHousekeeping hk = new ShadowHousekeeping(c, retention, floor, clock, 5_000, 2_000,
                        refreshes::incrementAndGet, r.hk);
                hk.poll();
                assertEquals("one deep read per poll at most", 1, r.hk.get(ShadowHousekeeping.Counter.DEEP_READS_TOTAL));
                assertFalse(hk.startupPassDone());
                assertEquals(ShadowFloor.Evaluation.Kind.UNKNOWN, floor.evaluate().kind());
                assertFalse("UNKNOWN never requests a purge", floor.purgeRequested());
                clock.advanceMs(1_999);
                hk.poll();
                assertEquals("paced: 1999 ms < 2000 ms", 1, r.hk.get(ShadowHousekeeping.Counter.DEEP_READS_TOTAL));
                clock.advanceMs(1);
                hk.poll();
                assertEquals(2, r.hk.get(ShadowHousekeeping.Counter.DEEP_READS_TOTAL));
                assertFalse(hk.startupPassDone());
                clock.advanceMs(2_000);
                hk.poll();
                assertEquals(3, r.hk.get(ShadowHousekeeping.Counter.DEEP_READS_TOTAL));
                assertTrue("the last copy verified: the startup pass is done", hk.startupPassDone());
                assertEquals(3, hk.scan().verified().size());
                assertEquals(ShadowFloor.Evaluation.Kind.FLOOR, floor.evaluate().kind());
                assertEquals(P_2, floor.evaluate().floor());
                assertTrue("new VERIFIED shadows request a purge pass", floor.purgeRequested());
                assertEquals(4, refreshes.get());
                assertTrue("pace waits are accounted", r.hk.get(ShadowHousekeeping.Counter.DEEP_READ_PACED_MS) >= 4_000);
                // the purge pass consumes the request; an unchanged next poll does not raise it again (C3)
                assertEquals(P_2, floor.getAsLong());
                assertFalse(floor.purgeRequested());
                clock.advanceMs(5_000);
                hk.poll();
                assertFalse("a scan alone is not a request", floor.purgeRequested());
                assertEquals(3, r.hk.get(ShadowHousekeeping.Counter.DEEP_READS_TOTAL));
                ShadowRig.green("9.3 housekeeping: 3 deep reads paced over 4 polls, startup pass then floor "
                        + floor.lastEvaluation());
            }
        }
    }

    /**
     * Under the two-producer anchor rule the pins are the witnessed anchor (every copy of it), the margin and the
     * newest VERIFIED; a single-producer newest VERIFIED stays pinned as the newest. Control: the anchor pin off,
     * retention purges the only witness pair.
     */
    static void witnessPairPinned(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            final byte[] image = FloorRig.payload(P_CM);
            r.shadow(P_CM, SAFE_SEQ, image, ShadowRig.R3, ShadowKind.ALIGNED);
            r.shadow(P_CM, SAFE_SEQ, image, ShadowRig.F1, ShadowKind.ALIGNED);
            r.shadow(FloorRig.P_A, SAFE_SEQ);
            final ShadowCatalog c = r.rig.catalog();
            c.verifyAllComplete();
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED,
                    ShadowFloor.AnchorRule.TWO_PRODUCERS, Set.of());
            final ShadowRetention retention = new ShadowRetention(c, floor, 1, r.hk,
                    off.isEmpty() ? java.util.EnumSet.noneOf(NegativeControl.class) : java.util.EnumSet.copyOf(off));
            try {
                final ShadowRetention.Pass pass = retention.apply(c.scan());
                final Scan after = c.scan();
                final ShadowCatalog.Answer anchor = after.equalAt(P_CM);
                if (anchor.kind() != ShadowCatalog.Answer.Kind.FOUND || anchor.shadow().producers().size() != 2) {
                    throw new AssertionError("the only witness pair was purged by retention: " + pass.purged());
                }
                assertTrue(pass.pinnedPositions().toString(), pass.pinnedPositions().containsAll(
                        Set.of(P_1, P_CM, FloorRig.P_A)));
                assertEquals("the single-producer newest stays", ShadowCatalog.Answer.Kind.FOUND,
                        after.equalAt(FloorRig.P_A).kind());
                ShadowRig.green("retention under two-producers: pins " + pass.pinnedPositions());
            } finally {
                retention.close();
            }
        }
    }

    @Test
    public void theWitnessPairIsPinnedUnderTwoProducers() {
        witnessPairPinned(Set.of());
        nc("RETENTION_ANCHOR_PIN_OFF", "the only witness pair was purged by retention",
                () -> witnessPairPinned(Set.of(NegativeControl.RETENTION_ANCHOR_PIN_OFF)));
    }
}
