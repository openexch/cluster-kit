// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Kit half of the named-snapshot genesis compare and its pacing: {@link GenesisGuard} with a fake source (what the
 * backup responses named, the local copies, their genesis). The engine-level matrix is the core {@code ReplicaReGenesisMatrixTest}.
 */
public class GenesisGuardTest {

    static final long G1 = 111;
    static final long G2 = 222;

    final long[] now = {1};
    GenesisGuard.Named named;
    final Map<GenesisGuard.Named, Long> copies = new HashMap<>();
    final Map<Long, Long> genesis = new HashMap<>();
    int decodes;
    boolean decodeFails;

    final GenesisGuard.Source source = new GenesisGuard.Source() {
        @Override
        public GenesisGuard.Named newestNamed() {
            return named;
        }

        @Override
        public long localCopy(final GenesisGuard.Named n) {
            return copies.getOrDefault(n, -1L);
        }

        @Override
        public long genesisOf(final long recordingId) {
            decodes++;
            if (decodeFails) {
                throw new IllegalStateException("retrieved snapshot recording " + recordingId + " has no END marker");
            }
            return genesis.get(recordingId);
        }

        @Override
        public String termContradiction(final long recordingId) {
            return null;
        }

        @Override
        public String refusalOf(final GenesisGuard.Named n) {
            return refusal;
        }
    };
    String refusal;

    GenesisGuard guard() {
        return new GenesisGuard(source, () -> now[0], AlignmentTrust.CHECK_PACE_NS, false);
    }

    void advanceMs(final long ms) {
        now[0] += TimeUnit.MILLISECONDS.toNanos(ms);
    }

    static String clusterGenesisText(final long g2, final long g1) {
        return "FATAL replica: cluster genesis " + g2 + " != local shadows " + g1 + "; wipe BASE_DIR";
    }

    /**
     * The compare runs on the NAMED snapshot; nothing named or not retrieved yet is WAITING (attach allowed,
     * unchecked); the first equal compare checks; a later named snapshot of another genesis refuses with the
     * cluster-genesis text (never latched for good), and {@code checked} is false while it is unchecked.
     */
    @Test
    public void theNamedSnapshotIsComparedAndNeverLatchedForGood() {
        final GenesisGuard g = guard();
        assertEquals(GenesisGuard.Outcome.WAITING, g.check(G1));
        assertFalse(g.checked());
        named = new GenesisGuard.Named(4096, 0, 0);
        advanceMs(250);
        assertEquals("named, not retrieved", GenesisGuard.Outcome.WAITING, g.check(G1));
        copies.put(named, 7L);
        genesis.put(7L, G1);
        advanceMs(250);
        assertEquals(GenesisGuard.Outcome.PASSED, g.check(G1));
        assertTrue(g.checked());
        assertTrue(g.comparedOnce());
        named = new GenesisGuard.Named(8192, 0, 0);
        advanceMs(250);
        assertEquals("newer named, not retrieved", GenesisGuard.Outcome.WAITING, g.check(G1));
        assertFalse("a newer named snapshot is unchecked", g.checked());
        assertTrue("readiness keeps the first compare", g.comparedOnce());
        copies.put(named, 9L);
        genesis.put(9L, G2);
        advanceMs(250);
        final GenesisGuard.Outcome o = g.check(G1);
        if (o != GenesisGuard.Outcome.REFUSED || !clusterGenesisText(G2, G1).equals(g.refusal())) {
            throw new AssertionError("a newer named snapshot of genesis " + G2 + " was not refused with the cluster-genesis"
                    + " text: " + o + " " + g.refusal());
        }
        System.out.println("GREEN named snapshot compared, re-checked, refused: " + g.refusal());
    }

    /** A named snapshot taken before the ADOPT (genesis 0) is nothing to compare; it is decoded once. */
    @Test
    public void aPreAdoptNamedSnapshotIsWaitingAndDecodedOnce() {
        final GenesisGuard g = guard();
        named = new GenesisGuard.Named(4096, 0, 0);
        copies.put(named, 7L);
        genesis.put(7L, 0L);
        for (int i = 0; i < 5; i++) {
            advanceMs(250);
            assertEquals(GenesisGuard.Outcome.WAITING, g.check(G1));
        }
        assertEquals(1, decodes);
        assertFalse(g.checked());
    }

    /**
     * The cluster identity re-check is paced like the attach path. A named copy that never decodes, a follower pass
     * every millisecond for 2 s: at most 2 000 / 200 + 1 evaluations (and decodes).
     */
    @Test
    public void theRecheckIsPaced() {
        final GenesisGuard g = guard();
        named = new GenesisGuard.Named(4096, 0, 0);
        copies.put(named, 7L);
        decodeFails = true;
        for (int pass = 0; pass < 2_000; pass++) {
            advanceMs(1);
            assertEquals(GenesisGuard.Outcome.UNREADABLE, g.check(G1));
        }
        final long bound = 2_000 / TimeUnit.NANOSECONDS.toMillis(AlignmentTrust.CHECK_PACE_NS) + 1;
        if (g.evaluationsTotal() > bound || decodes > bound) {
            throw new AssertionError("the cluster identity re-check is not paced: " + g.evaluationsTotal() + " evaluations, "
                    + decodes + " decodes in 2 000 passes over 2 000 ms (bound " + bound + ")");
        }
        System.out.println("GREEN paced: " + g.evaluationsTotal() + " evaluations in 2 000 ms");
    }

    /** A named copy refused by its content says why in the waiting detail, not "not retrieved yet". */
    @Test
    public void aRefusedNamedCopyCarriesItsReasonIntoTheDetail() {
        final GenesisGuard g = guard();
        named = new GenesisGuard.Named(4096, 0, 0);
        refusal = "rec 9: snapshot CRC32C mismatch";
        assertEquals(GenesisGuard.Outcome.WAITING, g.check(G1));
        final String expected = "genesis check: named member snapshot at 4096 refused: rec 9: snapshot CRC32C mismatch";
        assertTrue(g.detail(), g.detail().startsWith(expected));
        assertTrue(g.namedCopyUnusable());
    }
}
