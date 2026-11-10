// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The alias grammar, its refusals (A2), the discovery prefixes, and one archive leg. */
public class ShadowAliasTest {

    static ShadowAlias random(final Random r) {
        final int engine = r.nextInt(3) == 0 ? 2 : (r.nextBoolean() ? 1 : 99);
        return new ShadowAlias(engine, r.nextInt(10) == 0 ? Integer.MAX_VALUE : r.nextInt(1_000),
                r.nextInt(10) == 0 ? Long.MAX_VALUE : 1 + (r.nextLong() & Long.MAX_VALUE) % (Long.MAX_VALUE - 1),
                r.nextInt(10) == 0 ? Long.MAX_VALUE : (r.nextLong() & Long.MAX_VALUE),
                r.nextInt(10) == 0 ? 0 : (r.nextLong() & Long.MAX_VALUE) % 1_000_000_000_000L,
                r.nextBoolean() ? 1 : 2, r.nextInt(10) == 0 ? Integer.MAX_VALUE : r.nextInt(7),
                r.nextInt(10) == 0 ? Long.MAX_VALUE : 1 + r.nextInt(1_000_000), 1 + r.nextInt(4));
    }

    static void assertNoSeparators(final String alias) {
        for (final char ch : new char[] {'|', '=', '?', ':', ' '}) {
            if (alias.indexOf(ch) >= 0) {
                throw new AssertionError("alias contains a URI separator '" + ch + "': " + alias);
            }
        }
    }

    @Test
    public void tenThousandSeededAliasesRoundTripAndStayInsideTheCharacterSet() {
        final Random r = new Random(0x5EED_A11A5L);
        int maximal = 0;
        for (int i = 0; i < 10_000; i++) {
            final ShadowAlias a = random(r);
            final String s = a.format();
            assertEquals(s, a, ShadowAlias.parse(s));
            assertNoSeparators(s);
            assertTrue(s.length() + " > 128: " + s, s.length() <= ShadowAlias.MAX_LENGTH);
            assertTrue(s, ShadowAlias.prefix(a.engineId()).endsWith(s.substring(0, s.indexOf('.', 7) + 1)));
            maximal = Math.max(maximal, s.length());
        }
        // The longest legal alias: every numeric field at its maximum, under the longest engine token (test99).
        final ShadowAlias longest = new ShadowAlias(ShadowHeader.ENGINE_TEST, Integer.MAX_VALUE, Long.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE, 1, Integer.MAX_VALUE, Long.MAX_VALUE, 1);
        assertEquals("the longest legal alias", 125, longest.format().length());
        assertEquals(longest, ShadowAlias.parse(longest.format()));
        final ShadowAlias longestCore = new ShadowAlias(ShadowHeader.ENGINE_CORE, Integer.MAX_VALUE, Long.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE, 1, Integer.MAX_VALUE, Long.MAX_VALUE, 1);
        assertEquals("the longest core alias", 123, longestCore.format().length());
        assertEquals("oxs.v1.core.c0.g24301.p1048576.t3.rR3.s17.k1",
                new ShadowAlias(1, 0, 24301, 1048576, 3, 1, 3, 17, 1).format());
        ShadowRig.green("alias: 10000 seeded round trips, longest seen " + maximal + ", maximal 125");
    }

    @Test
    public void mutationsAreRefusedNamingTheToken() {
        final String good = "oxs.v1.core.c0.g24301.p1048576.t3.rR3.s17.k1";
        final String[][] cases = {
                {"oxs.v1.core.c0.g-24301.p1048576.t3.rR3.s17.k1", "g"},          // sign
                {"oxs.v1.core.c0.p1048576.t3.rR3.s17.k1", "tokens"},             // missing token
                {"oxs.v1.core.c0.g24301.g24301.p1048576.t3.rR3.s17", "p"},       // duplicated token
                {"oxs.v1.core.c0.g24301.t3.p1048576.rR3.s17.k1", "p"},           // swapped p/t
                {"oxs.v1.core.c0.g24301.p10x8576.t3.rR3.s17.k1", "p"},           // non-numeric
                {"oxs.v1.core.c0.g0.p1048576.t3.rR3.s17.k1", "g"},               // genesis 0
                {"oxs.v1.core.c0.g24301.p1048576.t3.rR3.s17.k5", "k"},           // k5
                {"oxs.v1.core.c0.g24301.p1048576.t3.rX1.s17.k1", "r"},           // rX1
                {"oxs.v1.core.c0.g24301.p01048576.t3.rR3.s17.k1", "p"},          // leading zero
                {"oxs.v1.core.c0.g24301.p1048576.t3.rR3.s0.k1", "s"},            // seq 0
                {"oxs.v1.core.c0.g9223372036854775808.p1.t3.rR3.s17.k1", "g"},   // overflow
                {"oxs.v1.core.c2147483648.g1.p1.t3.rR3.s17.k1", "c"},            // int overflow
                {"oxs.v1.bogus.c0.g24301.p1048576.t3.rR3.s17.k1", "engine"},
                {"oxs.v2.core.c0.g24301.p1048576.t3.rR3.s17.k1", "oxs.v1"},
        };
        ShadowAlias.parse(good);
        final List<String> missed = new ArrayList<>();
        for (final String[] c : cases) {
            try {
                ShadowAlias.parse(c[0]);
                missed.add(c[0] + " accepted");
            } catch (final ShadowAlias.InvalidAliasException e) {
                if (!e.token().equals(c[1])) {
                    missed.add(c[0] + " refused on token " + e.token() + ", expected " + c[1] + " (" + e.getMessage()
                            + ")");
                }
            }
        }
        assertTrue(missed.toString(), missed.isEmpty());
        assertEquals(null, ShadowAlias.tryParse("oxs.v1.core"));
    }

    @Test
    public void prefixesSelectByEngineThenByClusterAndGenesis() {
        final Random r = new Random(7);
        for (int i = 0; i < 1_000; i++) {
            final ShadowAlias a = random(r);
            final String param = ShadowAlias.PARAM + a.format();
            assertTrue(param.startsWith(ShadowAlias.prefix(a.engineId())));
            assertTrue(param.startsWith(ShadowAlias.prefix(a.engineId(), a.clusterId(), a.genesisId())));
            final ShadowAlias other = new ShadowAlias(a.engineId(), a.clusterId(), a.genesisId() == 1 ? 2 : 1,
                    a.logPosition(), a.leadershipTermId(), a.producerKind(), a.producerId(), a.producerSeq(), a.kind());
            assertFalse(param, (ShadowAlias.PARAM + other.format()).startsWith(
                    ShadowAlias.prefix(a.engineId(), a.clusterId(), a.genesisId())));
        }
        assertTrue(ShadowAlias.prefix(1).equals("alias=oxs.v1.core."));
        assertEquals("oxs.v1.match.c1.g2.p3.t4.rF5.s6.k4",
                ShadowAlias.aliasOfChannel("aeron:ipc?term-length=64k|alias=oxs.v1.match.c1.g2.p3.t4.rF5.s6.k4"));
    }

    @Test
    public void listingByPrefixUsesSubstringOnTheOriginalChannelAndTheStreamFilter() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowAlias g1a = new ShadowAlias(99, 0, 11, 100, 3, 1, 3, 1, 1);
            final ShadowAlias g1b = new ShadowAlias(99, 0, 11, 200, 3, 1, 3, 2, 1);
            final ShadowAlias g2 = new ShadowAlias(99, 0, 22, 300, 3, 1, 3, 3, 1);
            rig.rawMessages(g1a.format(), new byte[] {1});
            rig.rawMessages(g1b.format(), new byte[] {2});
            rig.rawMessages(g2.format(), new byte[] {3});
            final var other = rig.fixture.record("aeron:ipc?alias=" + g1a.format(), 4999);
            other.offer(new org.agrona.concurrent.UnsafeBuffer(new byte[] {9}), 0, 1);
            other.stopRecording();
            assertEquals(3, count(rig, ShadowAlias.prefix(99)));
            assertEquals(2, count(rig, ShadowAlias.prefix(99, 0, 11)));
            assertEquals(1, count(rig, ShadowAlias.prefix(99, 0, 22)));
            assertEquals(0, count(rig, ShadowAlias.prefix(1)));
        }
    }

    private static int count(final ShadowRig rig, final String prefix) {
        final int[] n = {0};
        rig.archive().listRecordingsForUri(0, 100, prefix, ShadowAlias.STREAM_ID,
                (a, b, c, d, e, f, g, h, i, j, k, l, m, nn, o, p) -> n[0]++);
        return n[0];
    }

    @Test
    public void negativeControlNc1TheCharacterRuleIsWhatKeepsASeparatorOut() {
        final ShadowAlias a = new ShadowAlias(1, 0, 24301, 1048576, 3, 1, 3, 17, 1);
        try {
            a.format("te|st", 'R', true);
            fail("the character rule let a separator through");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("URI separator"));
        }
        nc("alias builder allows |", "alias contains a URI separator",
                () -> assertNoSeparators(a.format("te|st", 'R', false)));
    }
}
