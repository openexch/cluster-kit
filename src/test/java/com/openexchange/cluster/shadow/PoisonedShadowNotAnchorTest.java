// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowFloor.AnchorRule;
import com.openexchange.cluster.shadow.ShadowFloor.Evaluation;
import com.openexchange.cluster.shadow.ShadowFloor.Margin;
import org.junit.Test;

import java.util.Set;

import static com.openexchange.cluster.shadow.FloorRig.P_1;
import static com.openexchange.cluster.shadow.FloorRig.P_CM;
import static com.openexchange.cluster.shadow.FloorRig.SAFE_SEQ;
import static com.openexchange.cluster.shadow.ShadowRig.F1;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The two-producer anchor rule over a real archive with 64 KiB segments ({@link FloorRig}): below P_cm a member purges
 * only behind an ALIGNED shadow whose digest two distinct producers wrote. One producer's shadow (a poisoned one
 * included) restores and is the margin, never the anchor; two copies of one producer are one witness; a STANDBY agreed
 * by two is no anchor; two producers that disagree are a CONFLICT. Each control switches one rule off and its own
 * assertion fails with the named text.
 */
public class PoisonedShadowNotAnchorTest {

    static ShadowFloor.Evaluation evaluate(final FloorRig r, final ShadowFloor floor) {
        final ShadowCatalog c = r.rig.catalog();
        c.verifyAllComplete();
        floor.observe(c.scan());
        return floor.evaluate();
    }

    /** One producer's ALIGNED at P_cm (a margin below it): NO_WITNESS, floor 0, one witness, the log stays whole. */
    static void oneProducer(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ, FloorRig.payload(P_CM), R3, ShadowKind.ALIGNED);
            final ShadowFloor floor = r.floor(Margin.PREVIOUS_VERIFIED, AnchorRule.TWO_PRODUCERS, off);
            final Evaluation e = evaluate(r, floor);
            r.purge(floor);
            if (r.logStart() > 0) {
                throw new AssertionError("a single-producer shadow anchored a purge below P_cm: log start "
                        + r.logStart() + " (" + e + ")");
            }
            assertEquals(e.toString(), Evaluation.Kind.NO_WITNESS, e.kind());
            assertEquals(0, e.floor());
            assertEquals(1, e.witnesses());
            assertTrue("one producer's shadow still restores", e.restorable().stream()
                    .anyMatch(v -> v.logPosition() == P_CM));
            assertEquals(1, r.hk.get(ShadowHousekeeping.Counter.ANCHOR_WITNESSES));
            assertEquals(1, r.hk.get(ShadowHousekeeping.Counter.FLOOR_NO_WITNESS_TOTAL));
            ShadowRig.green("anchor rule: one producer at P_cm -> " + e);
        }
    }

    @Test
    public void oneProducersShadowNeverAnchors() {
        oneProducer(Set.of());
        nc("ANCHOR_RULE_IGNORED", "a single-producer shadow anchored a purge below P_cm",
                () -> oneProducer(Set.of(NegativeControl.ANCHOR_RULE_IGNORED)));
    }

    /** The same bytes from a second producer: the anchor, the floor by the margin, two witnesses. */
    @Test
    public void twoProducersOfOneDigestAnchor() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            final byte[] image = FloorRig.payload(P_CM);
            r.shadow(P_CM, SAFE_SEQ, image, R3, ShadowKind.ALIGNED);
            r.shadow(P_CM, SAFE_SEQ, image, F1, ShadowKind.ALIGNED);
            final ShadowFloor floor = r.floor(Margin.PREVIOUS_VERIFIED, AnchorRule.TWO_PRODUCERS, Set.of());
            final Evaluation e = evaluate(r, floor);
            assertEquals(e.toString(), Evaluation.Kind.FLOOR, e.kind());
            assertEquals(P_CM, e.anchorPosition());
            assertEquals("the margin keeps the log from the previous restorable shadow", P_1, e.floor());
            assertEquals(2, e.witnesses());
            r.purge(floor);
            final long start = r.logStart();
            assertTrue("purged below the margin: " + start, start > 0 && start <= P_1);
            // margin none: P_cm once witnessed
            final Evaluation none = evaluate(r, r.floor(Margin.NONE, AnchorRule.TWO_PRODUCERS, Set.of()));
            assertEquals(none.toString(), P_CM, none.floor());
            ShadowRig.green("anchor rule: R3 + F1 at P_cm -> " + e + "; margin none -> " + none.floor());
        }
    }

    /** Two recordings of one producer are one witness. */
    static void sameProducerTwice(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            final byte[] image = FloorRig.payload(P_CM);
            r.shadow(P_CM, SAFE_SEQ, image, R3, ShadowKind.ALIGNED);
            r.shadow(P_CM, SAFE_SEQ, image, R3, ShadowKind.ALIGNED);
            final Evaluation e = evaluate(r, r.floor(Margin.PREVIOUS_VERIFIED, AnchorRule.TWO_PRODUCERS, off));
            if (e.kind() != Evaluation.Kind.NO_WITNESS) {
                throw new AssertionError("two copies of one producer counted as two witnesses: " + e);
            }
            assertEquals(1, e.witnesses());
            ShadowRig.green("anchor rule: R3 twice at P_cm -> " + e);
        }
    }

    @Test
    public void twoCopiesOfOneProducerAreOneWitness() {
        sameProducerTwice(Set.of());
        nc("WITNESS_BY_RECORDING", "two copies of one producer counted as two witnesses",
                () -> sameProducerTwice(Set.of(NegativeControl.WITNESS_BY_RECORDING)));
    }

    /** A STANDBY agreed by two producers witnesses nothing. */
    static void standbyAgreed(final Set<NegativeControl> off) {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            final byte[] image = FloorRig.payload(P_CM);
            r.shadow(P_CM, SAFE_SEQ, image, R3, ShadowKind.STANDBY);
            r.shadow(P_CM, SAFE_SEQ, image, F1, ShadowKind.STANDBY);
            final Evaluation e = evaluate(r, r.floor(Margin.PREVIOUS_VERIFIED, AnchorRule.TWO_PRODUCERS, off));
            if (e.anchor() != null) {
                throw new AssertionError("a STANDBY shadow anchored a purge: " + e);
            }
            assertEquals(e.toString(), Evaluation.Kind.NO_WITNESS, e.kind());
            ShadowRig.green("anchor rule: STANDBY by R3 + F1 -> " + e);
        }
    }

    @Test
    public void aStandbyAgreedByTwoIsNoAnchor() {
        standbyAgreed(Set.of());
        nc("ANCHOR_ANY_KIND", "a STANDBY shadow anchored a purge",
                () -> standbyAgreed(Set.of(NegativeControl.ANCHOR_ANY_KIND)));
    }

    /**
     * The poisoned shape: the second producer's image differs (a poisoned shadow): CONFLICT, no anchor; with one poisoned
     * single-producer shadow at P_cm alone it restores but never anchors (the first test's row).
     */
    @Test
    public void twoProducersDisagreeingAreAConflictAndNoAnchor() {
        try (FloorRig r = new FloorRig()) {
            r.shadow(P_1, SAFE_SEQ);
            r.shadow(P_CM, SAFE_SEQ, FloorRig.payload(P_CM), R3, ShadowKind.ALIGNED);
            r.shadow(P_CM, SAFE_SEQ, FloorRig.payload(P_CM + 7), F1, ShadowKind.ALIGNED);
            final ShadowFloor floor = r.floor(Margin.PREVIOUS_VERIFIED, AnchorRule.TWO_PRODUCERS, Set.of());
            final Evaluation e = evaluate(r, floor);
            assertEquals(e.toString(), Evaluation.Kind.NO_COVERING, e.kind());
            assertEquals(0, e.floor());
            r.purge(floor);
            assertEquals(0, r.logStart());
            ShadowRig.green("anchor rule: R3 and F1 disagree at P_cm -> " + e);
        }
    }

    /** The rule's parser, refusal and CRITICAL line. */
    @Test
    public void theRuleSetting() {
        assertEquals(AnchorRule.TWO_PRODUCERS, AnchorRule.parse("two-producers"));
        assertEquals(AnchorRule.ANY_VERIFIED, AnchorRule.parse("any-verified"));
        try {
            AnchorRule.parse("both");
            throw new AssertionError("refused");
        } catch (final IllegalArgumentException e) {
            assertEquals("SHADOW_ANCHOR_RULE 'both' (expected two-producers|any-verified)", e.getMessage());
        }
        assertTrue(AnchorRule.ANY_VERIFIED.criticalLine().startsWith("SHADOW_ANCHOR_RULE=any-verified"));
        assertEquals(null, AnchorRule.TWO_PRODUCERS.criticalLine());
    }
}
