// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowIdentity.LocalIdentity;
import com.openexchange.cluster.shadow.ShadowIdentity.Outcome;
import com.openexchange.cluster.shadow.ShadowIdentity.Policy;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** RCS S3 11.3: identity rows I1-I8 under the three policies, the post-restore cross-check, the boot guard. */
public class ShadowIdentityTest {

    static ShadowHeader header(final int engine, final int cluster, final long genesis, final long term,
                               final long termBase) {
        return new ShadowHeader(engine, cluster, 1, 1, 3, 1, genesis, termBase + 4096, term, termBase, 77, 1, 41, 100,
                0, 8192, 4);
    }

    static final ShadowIdentity.TermBaseLookup TERMS = t -> Map.of(3L, 1000L).getOrDefault(t, -1L);

    @Test
    public void eachRowUnderEachPolicy() {
        final LocalIdentity local = new LocalIdentity(1, 0, 0x5EED, TERMS);
        final Object[][] rows = {
                {header(2, 0, 0x5EED, 3, 1000), Outcome.ENGINE_MISMATCH, false, false, false},
                {header(1, 7, 0x5EED, 3, 1000), Outcome.CLUSTER_MISMATCH, false, false, false},
                {header(1, 0, 0, 3, 1000), Outcome.UNIDENTIFIED, false, false, false},
                {header(1, 0, -5, 3, 1000), Outcome.UNIDENTIFIED, false, false, false},
                {header(1, 0, 0x5EEE, 3, 1000), Outcome.GENESIS_MISMATCH, false, false, false},
                {header(1, 0, 0x5EED, 4, 1000), Outcome.HISTORY_UNVERIFIED, false, true, true},
                {header(1, 0, 0x5EED, 3, 1032), Outcome.HISTORY_MISMATCH, false, false, false},
                {header(1, 0, 0x5EED, 3, 1000), Outcome.OK, true, true, true},
        };
        for (final Object[] row : rows) {
            final ShadowHeader h = (ShadowHeader)row[0];
            assertEquals(row[1], ShadowIdentity.check(h, local));
            assertEquals(row[1] + " STRICT", row[2], ShadowIdentity.admits(h, local, Policy.STRICT));
            assertEquals(row[1] + " ALLOW", row[3], ShadowIdentity.admits(h, local, Policy.ALLOW_UNVERIFIED_HISTORY));
            assertEquals(row[1] + " BOOT", row[4], ShadowIdentity.admits(h, local, Policy.BOOTSTRAP_ADOPT));
        }
        // I5: a host with no genesis yet
        final LocalIdentity bootstrap = new LocalIdentity(1, 0, 0, TERMS);
        final ShadowHeader h = header(1, 0, 0x5EED, 3, 1000);
        assertEquals(Outcome.GENESIS_UNVERIFIED, ShadowIdentity.check(h, bootstrap));
        assertFalse(ShadowIdentity.admits(h, bootstrap, Policy.STRICT));
        assertFalse(ShadowIdentity.admits(h, bootstrap, Policy.ALLOW_UNVERIFIED_HISTORY));
        assertTrue(ShadowIdentity.admits(h, bootstrap, Policy.BOOTSTRAP_ADOPT));
        // a negative LOCAL nonce never falls through to OK (I4 reads `local != 0`)
        assertEquals(Outcome.GENESIS_MISMATCH, ShadowIdentity.check(h, new LocalIdentity(1, 0, -42, TERMS)));
    }

    /** The reference: rows I1-I8 in order, written independently of the implementation. */
    static Outcome reference(final ShadowHeader h, final LocalIdentity l) {
        final Outcome[] firstMatch = {null};
        final java.util.function.BiConsumer<Boolean, Outcome> row = (cond, o) -> {
            if (firstMatch[0] == null && cond) {
                firstMatch[0] = o;
            }
        };
        row.accept(h.engineId() != l.engineId(), Outcome.ENGINE_MISMATCH);
        row.accept(h.clusterId() != l.clusterId(), Outcome.CLUSTER_MISMATCH);
        row.accept(h.genesisId() <= 0, Outcome.UNIDENTIFIED);
        row.accept(l.genesisId() != 0 && h.genesisId() != l.genesisId(), Outcome.GENESIS_MISMATCH);
        row.accept(l.genesisId() == 0, Outcome.GENESIS_UNVERIFIED);
        row.accept(firstMatch[0] == null && l.terms().termBaseOf(h.leadershipTermId()) == -1,
                Outcome.HISTORY_UNVERIFIED);
        row.accept(firstMatch[0] == null && l.terms().termBaseOf(h.leadershipTermId()) != h.termBaseLogPosition(),
                Outcome.HISTORY_MISMATCH);
        row.accept(true, Outcome.OK);
        return firstMatch[0];
    }

    @Test
    public void sixtyFourRandomPairsMatchTheReference() {
        final Random r = new Random(0x1D);
        final int[] seen = new int[Outcome.values().length];
        for (int i = 0; i < 64; i++) {
            // biased towards agreement so that 64 pairs reach the history rows
            final long g = new long[] {-1, 0, 0x5EED, 0x5EED, 0x5EED, 0x5EED}[r.nextInt(6)];
            final LocalIdentity l = new LocalIdentity(1, 0, g, t -> t == 3 ? 1000 : t == 4 ? 2000 : -1);
            final ShadowHeader h = header(r.nextInt(8) == 0 ? 2 : 1, r.nextInt(8) == 0 ? 1 : 0,
                    new long[] {0, -1, 0x5EED, 0x5EED, 0x5EED, 0x5EEE}[r.nextInt(6)], 2 + r.nextInt(3),
                    new long[] {1000, 2000, 1032}[r.nextInt(3)]);
            final Outcome expected = reference(h, l);
            assertEquals("pair " + i + " " + h + " " + l, expected, ShadowIdentity.check(h, l));
            seen[expected.ordinal()]++;
        }
        for (final Outcome o : Outcome.values()) {
            if (o != Outcome.OK && seen[o.ordinal()] == 0) {
                // the table is only meaningful if it exercises the rows
                fail("64 pairs never produced " + o);
            }
        }
    }

    @Test
    public void matchesComparesTheRestoredTriple() {
        final ShadowHeader h = header(1, 0, 0x5EED, 3, 1000);
        assertTrue(ShadowIdentity.matches(h, new ShadowRig.Id(0x5EED, true, 41, 0)));
        assertNull(ShadowIdentity.mismatch(h, new ShadowRig.Id(0x5EED, true, 41, 99)));
        assertTrue(ShadowIdentity.mismatch(h, new ShadowRig.Id(0x5EEE, true, 41, 0)).startsWith("genesisId"));
        assertTrue(ShadowIdentity.mismatch(h, new ShadowRig.Id(0x5EED, false, 41, 0)).startsWith("journalEnabled"));
        assertTrue(ShadowIdentity.mismatch(h, new ShadowRig.Id(0x5EED, true, 40, 0)).startsWith("journalSeq"));
        final ShadowCounters c = new ShadowCounters();
        assertFalse(ShadowIdentity.matches(h, new ShadowRig.Id(0x5EED, true, 40, 0), c));
        assertEquals(1, c.get(ShadowCounters.Counter.IDENTITY_CROSSCHECK_FAILURES_TOTAL));
    }

    @Test
    public void requireProducibleRefusesZeroAndNegative() {
        ShadowIdentity.requireProducible(1);
        for (final long g : new long[] {0, -42, Long.MIN_VALUE}) {
            try {
                ShadowIdentity.requireProducible(g);
                fail("accepted " + g);
            } catch (final IllegalStateException e) {
                assertEquals("shadow producer refused: recorded genesisId <= 0 (" + g + "); see RCS S3 5.5",
                        e.getMessage());
            }
        }
    }
}
