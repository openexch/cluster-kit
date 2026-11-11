// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.Answer;
import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import org.junit.Test;

import java.util.Set;

import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Two producers agree by the payload digest (SHA-256), never by CRC32C: the same image written by R3 and F1 is one
 * position group with two witnesses; the same producer twice is one witness; two different self-sealed images with
 * equal CRC32C (the residue) and different digests are a CONFLICT. Control: the writers computing the digest with
 * CRC32C make two different images agree.
 */
public class DualProducerDigestTest {

    static ShadowFloor floor() {
        return new ShadowFloor(() -> 800, () -> ShadowFloor.NewestImage.STUB, h -> true,
                ShadowFloor.Margin.NONE, ShadowFloor.AnchorRule.TWO_PRODUCERS, () -> 0,
                new ShadowHousekeeping.Counters());
    }

    static Scan verified(final ShadowRig rig) {
        final ShadowCatalog c = rig.catalog();
        c.verifyAllComplete();
        return c.scan();
    }

    @Test
    public void oneImageFromTwoProducersIsTwoWitnesses() {
        final byte[] a = ShadowCatalogTest.selfSealed(1);
        try (ShadowRig rig = new ShadowRig()) {
            ShadowCatalogTest.twoProducersAt(rig, 800, a, a, Set.of());
            final Scan s = verified(rig);
            final Answer at = s.equalAt(800);
            assertEquals(Answer.Kind.FOUND, at.kind());
            assertEquals(at.shadow().toString(), 2, at.shadow().producers().size());
            final ShadowFloor f = floor();
            assertEquals(2, f.witnesses(at.shadow()));
            assertEquals(ShadowFloor.Evaluation.Kind.FLOOR, f.evaluate(800, s).kind());
            ShadowRig.green("dual producer: R3 + F1 one digest -> 2 witnesses " + at.shadow());
        }
        try (ShadowRig rig = new ShadowRig()) {
            for (int i = 0; i < 2; i++) {
                final ShadowImageBuffer image = ShadowRig.image(a.length, 1);
                ShadowRig.copy(image.payloadView(), a);
                ShadowRig.writeOk(rig.writer(ShadowRig.R3, 1 + i, -1, ShadowWriter.Faults.NONE, Set.of(),
                        ShadowRig.CLOCK), image, ShadowKind.ALIGNED, ShadowRig.cursor(800));
            }
            final Scan s = verified(rig);
            assertEquals("R3 twice is one witness", 1, floor().witnesses(s.equalAt(800).shadow()));
            assertEquals(ShadowFloor.Evaluation.Kind.NO_WITNESS, floor().evaluate(800, s).kind());
        }
    }

    static void assertConflict(final Scan s) {
        if (s.equalAt(800).kind() != Answer.Kind.CONFLICT) {
            throw new AssertionError("two different images agreed by CRC: " + s.atPosition(800));
        }
    }

    @Test
    public void equalCrcDifferentDigestIsAConflict() {
        final byte[] a = ShadowCatalogTest.selfSealed(1);
        final byte[] b = ShadowCatalogTest.selfSealed(2);
        assertEquals("the CRC32C residue", ShadowRig.crc(a), ShadowRig.crc(b));
        assertTrue(ShadowRig.digest(a) != ShadowRig.digest(b));
        try (ShadowRig rig = new ShadowRig()) {
            assertConflict(ShadowCatalogTest.twoProducersAt(rig, 800, a, b, Set.of()));
        }
        try (ShadowRig rig = new ShadowRig()) {
            nc("AGREE_BY_CRC", "two different images agreed by CRC", () -> assertConflict(
                    ShadowCatalogTest.twoProducersAt(rig, 800, a, b, Set.of(NegativeControl.DIGEST_AS_CRC32C))));
        }
        ShadowRig.green("dual producer: equal CRC32C " + Integer.toHexString(ShadowRig.crc(a))
                + ", different digests -> CONFLICT");
    }
}
