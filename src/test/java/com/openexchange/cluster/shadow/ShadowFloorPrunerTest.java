// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.Answer;
import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import com.openexchange.cluster.shadow.ShadowEntry.State;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.openexchange.cluster.shadow.FloorRig.P_1;
import static com.openexchange.cluster.shadow.FloorRig.P_2;
import static com.openexchange.cluster.shadow.FloorRig.P_A;
import static com.openexchange.cluster.shadow.FloorRig.P_B;
import static com.openexchange.cluster.shadow.FloorRig.P_CM;
import static com.openexchange.cluster.shadow.FloorRig.SAFE_SEQ;
import static com.openexchange.cluster.shadow.FloorRig.UNSAFE_SEQ;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The floor (literal floor + restorable margin) and the kit half of the anchor rule, over a real archive with 64 KiB
 * segments and a member-shaped {@code recording.log}: the floor decides, {@code ArchiveHousekeeping.purgeBelow}
 * really deletes the log below it (what the pruner's purge pass runs), and the catalog then answers the restore
 * queries a restart would ask. Each negative control switches one floor rule off and its own assertion fails with
 * the contract's text (C6).
 */
public class ShadowFloorPrunerTest {

    static ShadowCatalog verifiedCatalog(final FloorRig r) {
        final ShadowCatalog c = r.rig.catalog();
        c.verifyAllComplete();
        return c;
    }

    static ShadowFloor.Evaluation observe(final ShadowFloor floor, final ShadowCatalog c) {
        floor.observe(c.scan());
        return floor.evaluate();
    }

    /** The TAIL base a restart would take: the newest usable shadow below P_cm at or above the log start. */
    static Answer tailBase(final FloorRig r, final ShadowCatalog c) {
        return c.scan().newestBelow(P_CM, r.logStart());
    }

    @Test
    public void noShadowPurgesNothing() {
        try (FloorRig r = new FloorRig()) {
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor();
            assertEquals(ShadowFloor.Evaluation.Kind.NO_COVERING, observe(floor, c).kind());
            r.purge(floor);
            assertEquals("no shadow: the log stays whole", 0, r.logStart());
            assertEquals(P_CM, r.hk.get(ShadowHousekeeping.Counter.FLOOR_LAG_BYTES));
            ShadowRig.green("11.7 no shadow -> floor 0, log start 0");
        }
    }

    // ---------------------------------------------------------------- D2: the precondition (both SNAPSHOT_MODEs)

    /** The off-mode oracle: a stub newest group with no shadow must never be pruned below P_cm. */
    static void assertNotPurgedBelowTheStub(final FloorRig r) {
        final long start = r.logStart();
        if (start > 0) {
            throw new AssertionError("log purged below stub P_cm=" + P_CM + " with no shadow (off mode): log start "
                    + start);
        }
    }

    /** A newest group holding a FULL service image: the floor is P_cm with no shadow at all (today's pruning). */
    @Test
    public void preconditionFullNewestGroupPrunesToPcm() {
        try (FloorRig r = new FloorRig(true)) {
            assertEquals(ShadowFloor.NewestImage.FULL, r.newestImage());
            final ShadowFloor floor = r.floor();
            // no catalog scan at all (an off node before ADOPT): the precondition answers before the table
            final ShadowFloor.Evaluation e = floor.evaluate();
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.FULL_IMAGE, e.kind());
            assertEquals(P_CM, e.floor());
            r.purge(floor);
            final long start = r.logStart();
            assertTrue("the log below P_cm was purged (today's pruning): log start " + start, start > 0);
            assertTrue("never above P_cm: log start " + start, start <= P_CM);
            assertEquals(0, r.hk.get(ShadowHousekeeping.Counter.FLOOR_UNKNOWN));
            ShadowRig.green("D2 FULL newest group -> floor P_cm, log start " + start);
        }
    }

    /** A newest group holding a stub: the 9.1 table applies (no shadow: NO_COVERING, purge nothing). */
    @Test
    public void preconditionStubNewestGroupUsesTheTable() {
        try (FloorRig r = new FloorRig()) {
            assertEquals(ShadowFloor.NewestImage.STUB, r.newestImage());
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor();
            assertEquals(ShadowFloor.Evaluation.Kind.NO_COVERING, observe(floor, c).kind());
            r.purge(floor);
            assertNotPurgedBelowTheStub(r);
            // with a covering shadow and its margin the table's floor applies, not P_cm
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ);
            final ShadowCatalog c2 = verifiedCatalog(r);
            final ShadowFloor.Evaluation e = observe(floor, c2);
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.FLOOR, e.kind());
            assertEquals(P_1, e.floor());
            ShadowRig.green("D2 STUB newest group -> the 9.1 table (" + e + ")");
        }
    }

    /** The kind cannot be read (no archive client, a missing recording, a failed peek): purge nothing. */
    @Test
    public void preconditionUnreadablePurgesNothing() {
        try (FloorRig r = new FloorRig(true)) {
            final ShadowFloor floor = new ShadowFloor(r::latestSnapshotPosition,
                    () -> { throw new IllegalStateException("no archive client yet"); }, FloorRig.skipSafe(),
                    ShadowFloor.Margin.PREVIOUS_VERIFIED, ShadowFloor.AnchorRule.ANY_VERIFIED, r::logStart, r.hk);
            final ShadowFloor.Evaluation e = floor.evaluate();
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.IMAGE_UNKNOWN, e.kind());
            r.purge(floor);
            assertEquals("unreadable: the log stays whole even though the group is a full image", 0, r.logStart());
            assertEquals(1, r.hk.get(ShadowHousekeeping.Counter.FLOOR_UNKNOWN));
            final ShadowFloor unknown = new ShadowFloor(r::latestSnapshotPosition,
                    () -> ShadowFloor.NewestImage.UNKNOWN, FloorRig.skipSafe(), ShadowFloor.Margin.NONE,
                    ShadowFloor.AnchorRule.ANY_VERIFIED, r::logStart, r.hk);
            assertEquals(0, unknown.evaluate().floor());
            ShadowRig.green("D2 unreadable newest group -> 0 (" + e + ")");
        }
    }

    /**
     * A service snapshot recording with the container frames and no service message (truncated or empty
     * payload). The real {@link StubPeek} answers UNKNOWN and the floor purges nothing, even with margin none.
     * Control: the earlier reading (such a recording answered FULL, which {@link NegativeControl#FLOOR_PRECONDITION_OFF}
     * reproduces: every newest group read as a full image) purges the log below P_cm.
     */
    @Test
    public void preconditionEmptyServiceRecordingPurgesNothing() {
        emptyServiceRecording(Set.of());
        nc("empty service recording read as FULL", "an empty service recording restores nothing",
                () -> emptyServiceRecording(Set.of(NegativeControl.FLOOR_PRECONDITION_OFF)));
    }

    static void emptyServiceRecording(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig(FloorRig.Service.EMPTY)) {
            assertEquals(ShadowFloor.NewestImage.UNKNOWN, r.newestImage());
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.NONE, off);
            final ShadowFloor.Evaluation e = floor.evaluate();
            r.purge(floor);
            assertEquals("an empty service recording restores nothing: the log stays whole (" + e + ")", 0,
                    r.logStart());
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.IMAGE_UNKNOWN, e.kind());
            assertEquals(1, r.hk.get(ShadowHousekeeping.Counter.FLOOR_UNKNOWN));
            ShadowRig.green("empty service recording -> UNKNOWN, floor 0 (" + e + ")");
        }
    }

    static void stubNewestGroupNoShadow(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, off);
            floor.observe(verifiedCatalog(r).scan());
            r.purge(floor);
            assertNotPurgedBelowTheStub(r);
        }
    }

    @Test
    public void controlPreconditionOffPrunesAStubNewestGroupBelowPcm() {
        stubNewestGroupNoShadow(Set.of());
        nc("precondition off", "log purged below stub P_cm=" + P_CM + " with no shadow (off mode)",
                () -> stubNewestGroupNoShadow(Set.of(NegativeControl.FLOOR_PRECONDITION_OFF)));
    }

    @Test
    public void onlyBelowPcmPurgesNothingR03Literal() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor();
            final ShadowFloor.Evaluation e = observe(floor, c);
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.NO_COVERING, e.kind());
            r.purge(floor);
            assertEquals("D11: no purge below P_cm without a covering local shadow (no 'purge below P_s')", 0,
                    r.logStart());
            ShadowRig.green("11.7 only P_s < P_cm -> floor 0 (" + e + ")");
        }
    }

    @Test
    public void oneVerifiedAtPcmWithoutPreviousPurgesNothing() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_CM, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor();
            final ShadowFloor.Evaluation e = observe(floor, c);
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.NO_MARGIN, e.kind());
            assertEquals(P_CM, e.anchorPosition());
            r.purge(floor);
            assertEquals(0, r.logStart());
            ShadowRig.green("11.7 one VERIFIED at P_cm, margin on, no previous -> purge nothing");
        }
    }

    /** Two VERIFIED (P_1 < P_cm = P_2): floor P_1; the log from P_1 survives, so a late CONFLICT at P_2 has TAIL. */
    static String twoVerified(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, off);
            final ShadowFloor.Evaluation e = observe(floor, c);
            assertEquals(P_CM, e.anchorPosition());
            r.purge(floor);
            final long start = r.logStart();
            if (start > P_1) {
                fail("log purged up to the anchor P=" + e.anchorPosition() + " (margin ignored): log start " + start);
            }
            assertEquals(e.toString(), P_1, e.floor());
            assertTrue("segments below P_1 must actually go: start " + start, start > 0);
            // a late CONFLICT at the anchor (a second producer, different content)
            r.conflictAt(P_CM, SAFE_SEQ);
            final Scan s = c.scan();
            assertEquals(Answer.Kind.CONFLICT, s.equalAt(P_CM).kind());
            final Answer tail = tailBase(r, c);
            assertEquals("TAIL from P_1 after the late CONFLICT at P_cm", Answer.Kind.FOUND, tail.kind());
            assertEquals(P_1, tail.shadow().logPosition());
            return e + " logStart=" + start;
        }
    }

    @Test
    public void twoVerifiedFloorAtThePreviousAndMarginNone() {
        ShadowRig.green("11.7 two VERIFIED -> " + twoVerified(Set.of()));
        nc("margin ignored", "log purged up to the anchor P=" + P_CM + " (margin ignored)",
                () -> twoVerified(Set.of(NegativeControl.FLOOR_MARGIN_IGNORED)));
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.NONE, Set.of());
            final ShadowFloor.Evaluation e = observe(floor, c);
            assertEquals(e.toString(), P_CM, e.floor());
            r.purge(floor);
            assertEquals(P_CM / FloorRig.SEGMENT * FloorRig.SEGMENT, r.logStart());
            ShadowRig.green("11.7 margin none -> floor P_cm (" + e + ")");
        }
    }

    @Test
    public void coveringAbovePcmNotSkipSafeAndNothingElsePurgesNothing() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_B, UNSAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor();
            final ShadowFloor.Evaluation e = observe(floor, c);
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.NO_COVERING, e.kind());
            assertTrue(e.restorable().isEmpty());
            r.purge(floor);
            assertEquals(0, r.logStart());
            ShadowRig.green("11.7 covering not skip-safe -> floor 0");
        }
    }

    /**
     * VERIFIED at P_B < P_A, both above P_cm, P_B skip-safe and P_A (the newest, the one SKIP tries) not, margin
     * {@code none}, nothing below P_cm. The restore only ever tries {@code newestAtOrAbove(P_cm + 1)} = P_A, abandons
     * SKIP (not skip-safe), and then needs a TAIL base or a whole log. So P_B must not count as restorable and
     * the log must stay; after the purge the restart queries must still find a path.
     */
    static String olderSkipSafeUnderANewerUnsafe(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_B, SAFE_SEQ);
            r.shadow(P_A, UNSAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.NONE, off);
            final ShadowFloor.Evaluation e = observe(floor, c);
            r.purge(floor);
            final Scan s = c.scan();
            final Answer skip = s.newestAtOrAbove(P_CM + 1);
            final boolean skipPath = skip.kind() == Answer.Kind.FOUND
                    && FloorRig.skipSafe().test(skip.shadow().representative().header());
            final boolean tailPath = tailBase(r, c).kind() == Answer.Kind.FOUND;
            final boolean wholeLog = r.logStart() == 0;
            if (!skipPath && !tailPath && !wholeLog) {
                fail("no restore path after the purge: SKIP tries P=" + skip.shadow().logPosition()
                        + " (not skip-safe), no TAIL base above log start " + r.logStart() + " (" + e + ")");
            }
            assertEquals(e.toString(), ShadowFloor.Evaluation.Kind.NO_COVERING, e.kind());
            assertTrue("P_B is not a restore path while P_A is newer: " + e, e.restorable().isEmpty());
            assertEquals(0, r.logStart());
            return e + " logStart=" + r.logStart();
        }
    }

    @Test
    public void olderSkipSafeShadowUnderANewerUnsafeOneIsNotRestorable() {
        ShadowRig.green("older skip-safe under a newer unsafe -> " + olderSkipSafeUnderANewerUnsafe(Set.of()));
        nc("any skip-safe above P_cm", "no restore path after the purge: SKIP tries P=" + P_A
                + " (not skip-safe)", () -> olderSkipSafeUnderANewerUnsafe(Set.of(NegativeControl.FLOOR_SKIP_ANY_ABOVE)));
        // the newer one skip-safe as well: SKIP takes it, the floor may prune to P_cm with margin none
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_B, SAFE_SEQ);
            r.shadow(P_A, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.NONE, Set.of());
            final ShadowFloor.Evaluation e = observe(floor, c);
            assertEquals(e.toString(), P_CM, e.floor());
            assertEquals("only the SKIP target is restorable above P_cm", List.of(P_A),
                    e.restorable().stream().map(LogicalShadow::logPosition).toList());
            ShadowRig.green("newest skip-safe -> floor P_cm (" + e + ")");
        }
    }

    /**
     * B1: VERIFIED at P_a > P_b > P_cm, P_a skip-safe and P_b not, plus P_1 < P_cm: anchor P_a, margin P_1 (not
     * P_b), floor P_1; then a late CONFLICT at P_a and a restart still finds TAIL from P_1.
     */
    static String b1(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_B, UNSAFE_SEQ);
            r.shadow(P_A, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, off);
            final ShadowFloor.Evaluation e = observe(floor, c);
            assertEquals(e.toString(), P_A, e.anchorPosition());
            r.purge(floor);
            r.conflictAt(P_A, SAFE_SEQ);
            final Scan s = c.scan();
            assertEquals(Answer.Kind.CONFLICT, s.newestAtOrAbove(P_CM + 1).kind());
            final Answer tail = tailBase(r, c);
            if (tail.kind() != Answer.Kind.FOUND) {
                fail("margin shadow at " + e.marginPosition() + " is not restorable: TAIL impossible (log start "
                        + r.logStart() + ", " + e + ")");
            }
            assertEquals(P_1, tail.shadow().logPosition());
            assertEquals(e.toString(), P_1, e.marginPosition());
            assertEquals(P_1, e.floor());
            return e + " logStart=" + r.logStart();
        }
    }

    @Test
    public void b1MarginIsTheRestorablePreviousNotTheNonSkipSafeOne() {
        ShadowRig.green("11.7 B1 -> " + b1(Set.of()));
        nc("prev over all VERIFIED", "margin shadow at " + P_B + " is not restorable: TAIL impossible",
                () -> b1(Set.of(NegativeControl.FLOOR_MARGIN_OVER_ALL_VERIFIED)));
    }

    /** EQUAL shadow at P_cm under a non-skip-safe shadow above: anchor P_cm, floor by margin (P_1). */
    static String equalUnderNonSkipSafe(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ);
            r.shadow(P_B, UNSAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, off);
            final ShadowFloor.Evaluation e = observe(floor, c);
            if (e.floor() == 0) {
                fail("floor 0 with a restorable EQUAL shadow at " + P_CM + " (" + e + ")");
            }
            assertEquals(e.toString(), P_CM, e.anchorPosition());
            assertEquals(P_1, e.floor());
            return e.toString();
        }
    }

    @Test
    public void equalShadowUnderANonSkipSafeShadowAnchorsAtPcm() {
        ShadowRig.green("11.7 EQUAL under non-skip-safe -> " + equalUnderNonSkipSafe(Set.of()));
        nc("newest-only covering", "floor 0 with a restorable EQUAL shadow at " + P_CM,
                () -> equalUnderNonSkipSafe(Set.of(NegativeControl.FLOOR_NEWEST_COVERING_ONLY)));
    }

    @Test
    public void unknownPurgesNothing() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ);
            final ShadowCatalog c = r.rig.catalog();
            // startup pass pending: the scan cannot answer
            final ShadowFloor floor = r.floor();
            floor.observe(c.scan());
            assertEquals(ShadowFloor.Evaluation.Kind.UNKNOWN, floor.evaluate().kind());
            r.purge(floor);
            assertEquals(0, r.logStart());
            assertEquals(1, r.hk.get(ShadowHousekeeping.Counter.FLOOR_UNKNOWN));
            // errors > 0 with the startup pass done: still unknown
            final Scan done = verifiedCatalog(r).scan();
            final Scan withErrors = new Scan(done.all(), done.usable(), done.verified(), done.conflicts(), 1,
                    Map.of(State.SCAN_ERROR, 1), true, 0, null, 0);
            assertEquals(ShadowFloor.Evaluation.Kind.UNKNOWN, floor.evaluate(P_CM, withErrors).kind());
            assertEquals(0, floor.evaluate(P_CM, withErrors).floor());
            // before any observe at all
            assertEquals(ShadowFloor.Evaluation.Kind.UNKNOWN, r.floor().evaluate().kind());
            ShadowRig.green("11.7 UNKNOWN (startup pending, errors > 0, no scan) -> 0");
        }
    }

    /** Scan-only COMPLETE must never feed the floor. */
    static String completeDoesNotFeedTheFloor(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            // appears after the startup pass: COMPLETE, not yet deep-read
            r.shadow(P_CM, SAFE_SEQ);
            final Scan s = c.scan();
            assertEquals(State.COMPLETE, s.equalAt(P_CM).shadow().representative().state());
            assertTrue(s.known());
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, off);
            floor.observe(s);
            final ShadowFloor.Evaluation e = floor.evaluate();
            if (e.floor() != 0) {
                fail("floor raised on an unverified shadow at " + P_CM + " (" + e + ")");
            }
            return e.toString();
        }
    }

    @Test
    public void onlyVerifiedFeedsTheFloor() {
        ShadowRig.green("11.7 COMPLETE ignored -> " + completeDoesNotFeedTheFloor(Set.of()));
        nc("scan-only COMPLETE feeds the floor", "floor raised on an unverified shadow at " + P_CM,
                () -> completeDoesNotFeedTheFloor(Set.of(NegativeControl.FLOOR_READS_USABLE)));
    }

    @Test
    public void aLowerFloorThanTheLogStartIsCountedAndChangesNothing() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor none = r.floor(ShadowFloor.Margin.NONE, Set.of());
            none.observe(c.scan());
            r.purge(none);
            final long start = r.logStart();
            assertTrue(start > P_1);
            final ShadowFloor margin = r.floor();
            margin.observe(c.scan());
            r.purge(margin);
            assertEquals("nothing can un-purge", start, r.logStart());
            assertEquals(1, r.hk.get(ShadowHousekeeping.Counter.FLOOR_REGRESSED_TOTAL));
            ShadowRig.green("11.7 floor below the log start: regressed counted, log unchanged");
        }
    }

    /**
     * 11.8 kit half: shadows at P_1 < P_2 < P_cm with the margin on purge nothing (no covering shadow); once P_3 >=
     * P_cm is VERIFIED the floor moves to P_2 and the log below P_2 goes; TAIL from P_2 is still possible.
     */
    static String tailNeedsRetainedLog(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_2, SAFE_SEQ);
            final ShadowCatalog c = verifiedCatalog(r);
            final ShadowFloor floor = r.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, off);
            floor.observe(c.scan());
            r.purge(floor);
            if (r.logStart() > P_1) {
                fail("purged below P_1 before any covering shadow: log start " + r.logStart());
            }
            final long p3 = P_CM;
            final long rec3 = r.shadow(p3, SAFE_SEQ);
            final ShadowEntry e3 = c.scan().entry(rec3);
            assertEquals(ShadowCatalog.DeepResult.Kind.VERIFIED, c.verifyDeep(e3).kind());
            floor.observe(c.scan());
            final ShadowFloor.Evaluation e = floor.evaluate();
            r.purge(floor);
            final long start = r.logStart();
            // a restart that needs TAIL from P_2 (EQUAL at P_3 lost, e.g. CONFLICT): the log must start at or below P_2
            r.conflictAt(p3, SAFE_SEQ);
            final Answer tail = tailBase(r, c);
            if (tail.kind() != Answer.Kind.FOUND || tail.shadow().logPosition() != P_2) {
                fail("tail start " + P_2 + " below log start " + start + ": TAIL impossible (" + e + ")");
            }
            assertEquals(e.toString(), P_2, e.floor());
            assertTrue("the log below P_2 goes: start " + start, start > P_1);
            return e + " logStart=" + start;
        }
    }

    @Test
    public void tailReplayNeedsRetainedLogKitHalf() {
        ShadowRig.green("11.8 kit half -> " + tailNeedsRetainedLog(Set.of()));
        nc("floor from the anchor", "tail start " + P_2 + " below log start",
                () -> tailNeedsRetainedLog(Set.of(NegativeControl.FLOOR_MARGIN_IGNORED)));
    }

    @Test
    public void marginParsesTheSettingValues() {
        assertEquals(ShadowFloor.Margin.PREVIOUS_VERIFIED, ShadowFloor.Margin.parse("previous-verified"));
        assertEquals(ShadowFloor.Margin.NONE, ShadowFloor.Margin.parse("none"));
        for (final String bad : List.of("", "Previous-Verified", "prev")) {
            try {
                ShadowFloor.Margin.parse(bad);
                fail("accepted " + bad);
            } catch (final IllegalArgumentException e) {
                assertEquals("SHADOW_FLOOR_MARGIN '" + bad + "' (expected previous-verified|none)", e.getMessage());
            }
        }
    }
}
