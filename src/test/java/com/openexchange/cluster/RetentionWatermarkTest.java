// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The watermark decides what gets DELETED from a live money ledger, and the one
 * direction that hurts is deleting something a member still needed. So what is
 * pinned here is every way it can be held back, and every way it is allowed to
 * give up on a member.
 */
public class RetentionWatermarkTest {

    private static final long GIB = 1L << 30;
    private static final long SNAPSHOT = 10_000;
    private static final long NOW = 1_000_000;

    // ---- helpers to build a consensus module answer without a cluster ----

    private static ClusterMemberPositions.Member member(final int id, final long position,
                                                        final long lastAppendNs) throws Exception {
        final Constructor<ClusterMemberPositions.Member> c =
            ClusterMemberPositions.Member.class.getDeclaredConstructor(int.class, long.class, long.class);
        c.setAccessible(true);
        return c.newInstance(id, position, lastAppendNs);
    }

    private static ClusterMemberPositions.Snapshot answer(final long currentTimeNs,
                                                          final ClusterMemberPositions.Member... members)
            throws Exception {
        final Constructor<ClusterMemberPositions.Snapshot> c =
            ClusterMemberPositions.Snapshot.class.getDeclaredConstructor(long.class, int.class, List.class);
        c.setAccessible(true);
        final List<ClusterMemberPositions.Member> list = new ArrayList<>();
        for (final ClusterMemberPositions.Member m : members) {
            list.add(m);
        }
        return c.newInstance(currentTimeNs, 0, list);
    }

    /** A member heard from just now. */
    private static long fresh(final long currentTimeNs) {
        return currentTimeNs - TimeUnit.MILLISECONDS.toNanos(50);
    }

    /** A member last heard from long enough ago that its position means nothing. */
    private static long stale(final long currentTimeNs) {
        return currentTimeNs - TimeUnit.MINUTES.toNanos(5);
    }

    // ---- the ceiling ----

    @Test
    public void withEveryoneCaughtUpTheSnapshotIsTheAnswer() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final RetentionWatermark.Result r = w.compute(SNAPSHOT,
            answer(t, member(0, SNAPSHOT, fresh(t)), member(1, SNAPSHOT + 500, fresh(t))),
            -1, -1, NOW);

        assertEquals(SNAPSHOT, r.position);
        assertEquals("snapshot", r.limiter);
        assertTrue(r.stranded.isEmpty());
    }

    /**
     * The carrying constraint, as a test: with no off-box archive and no backup,
     * reclamation still happens. A -1 is a consumer that is not participating,
     * not one waiting at position -1.
     */
    @Test
    public void anAbsentConsumerIsNotAConstraint() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final RetentionWatermark.Result r =
            w.compute(SNAPSHOT, answer(t, member(0, SNAPSHOT, fresh(t))), -1, -1, NOW);

        assertEquals(SNAPSHOT, r.position);
        assertFalse(r.detail.containsKey("s3Verified"));
        assertFalse(r.detail.containsKey("backup"));
    }

    @Test
    public void aParticipatingConsumerHoldsTheWatermarkDown() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final RetentionWatermark.Result r =
            w.compute(SNAPSHOT, answer(t, member(0, SNAPSHOT, fresh(t))), 4_000, -1, NOW);

        assertEquals(4_000, r.position);
        assertEquals("s3Verified", r.limiter);
    }

    @Test
    public void theSlowestMemberWins() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final RetentionWatermark.Result r = w.compute(SNAPSHOT,
            answer(t, member(0, SNAPSHOT, fresh(t)), member(1, 7_000, fresh(t)),
                member(2, 9_000, fresh(t))),
            -1, -1, NOW);

        assertEquals(7_000, r.position);
        assertEquals("member1", r.limiter);
    }

    // ---- trust ----

    /**
     * A member's position does not decay. One that stopped responding keeps
     * advertising the number it reached, and believing it would purge past the
     * member the bound exists to protect.
     */
    @Test
    public void aStalePositionIsNotBelieved() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);

        // It claims to be fully caught up, but nothing has been heard from it.
        final RetentionWatermark.Result r = w.compute(SNAPSHOT,
            answer(t, member(0, SNAPSHOT, fresh(t)), member(1, SNAPSHOT, stale(t))),
            -1, -1, NOW);

        // First sighting: it is tracked from the snapshot, and nothing is reclaimed
        // past it on the strength of a number nobody can confirm.
        assertEquals(SNAPSHOT, r.position);

        // On the next answer it is still silent, and now it holds the watermark
        // at where it was first parked rather than at what it advertises.
        final RetentionWatermark.Result r2 = w.compute(SNAPSHOT + 5_000,
            answer(t, member(0, SNAPSHOT + 5_000, fresh(t)), member(1, SNAPSHOT + 5_000, stale(t))),
            -1, -1, NOW + 1_000);
        assertEquals(SNAPSHOT, r2.position);
        assertEquals("member1", r2.limiter);
    }

    // ---- the bound ----

    @Test
    public void aMemberTooFarBehindIsWrittenOff() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final long snapshot = 8 * GIB;

        final RetentionWatermark.Result r = w.compute(snapshot,
            answer(t, member(0, snapshot, fresh(t)), member(1, 3 * GIB, fresh(t))),
            -1, -1, NOW);

        // 5 GiB behind, past the 2 GiB bound: stop waiting for it and say so.
        assertEquals(snapshot, r.position);
        assertEquals("snapshot", r.limiter);
        assertEquals(List.of(1), r.stranded);
    }

    @Test
    public void aMemberStuckForTooLongIsWrittenOff() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);

        // Behind, but not far behind: only the clock can write this one off.
        w.compute(SNAPSHOT, answer(t, member(1, 9_000, fresh(t))), -1, -1, NOW);

        final RetentionWatermark.Result held =
            w.compute(SNAPSHOT, answer(t, member(1, 9_000, fresh(t))), -1, -1, NOW + 60_000);
        assertEquals(9_000, held.position);

        final long past = NOW + RetentionWatermark.DEFAULT_MAX_LAG_MS + 1;
        final RetentionWatermark.Result r =
            w.compute(SNAPSHOT, answer(t, member(1, 9_000, fresh(t))), -1, -1, past);
        assertEquals(SNAPSHOT, r.position);
        assertEquals(List.of(1), r.stranded);
    }

    /** The bound is for a member that is not catching up, not one that is merely behind. */
    @Test
    public void slowProgressRestartsTheClock() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);

        w.compute(SNAPSHOT, answer(t, member(1, 9_000, fresh(t))), -1, -1, NOW);

        // It moved, just not much, and the clock starts again from there.
        final long almost = NOW + RetentionWatermark.DEFAULT_MAX_LAG_MS - 1;
        w.compute(SNAPSHOT, answer(t, member(1, 9_001, fresh(t))), -1, -1, almost);

        final RetentionWatermark.Result r = w.compute(SNAPSHOT,
            answer(t, member(1, 9_001, fresh(t))), -1, -1, almost + 1_000);
        assertEquals(9_001, r.position);
        assertTrue(r.stranded.isEmpty());
    }

    /** Catching up clears the record, so the next time it falls behind it gets the full bound. */
    @Test
    public void catchingUpForgivesTheMember() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);

        w.compute(SNAPSHOT, answer(t, member(1, 5_000, fresh(t))), -1, -1, NOW);
        w.compute(SNAPSHOT, answer(t, member(1, SNAPSHOT, fresh(t))), -1, -1, NOW + 1_000);

        // Behind again, and the clock is new: it is not written off at the old deadline.
        final long past = NOW + RetentionWatermark.DEFAULT_MAX_LAG_MS + 1;
        final RetentionWatermark.Result r =
            w.compute(SNAPSHOT, answer(t, member(1, 9_500, fresh(t))), -1, -1, past);
        assertEquals(9_500, r.position);
        assertTrue(r.stranded.isEmpty());
    }

    /**
     * A member that is written off and STAYS stuck does not flap back into the
     * watermark, because then reclamation would depend on which cycle the
     * operator happened to read.
     */
    @Test
    public void aStrandedMemberThatMakesNoProgressStaysStranded() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final long snapshot = 8 * GIB;

        w.compute(snapshot, answer(t, member(1, 3 * GIB, fresh(t))), -1, -1, NOW);
        assertEquals(List.of(1), w.strandedMembers());

        final RetentionWatermark.Result r =
            w.compute(snapshot, answer(t, member(1, 3 * GIB, fresh(t))), -1, -1, NOW + 1_000);
        assertEquals(snapshot, r.position);
        assertEquals(List.of(1), r.stranded);
    }

    /**
     * But progress un-strands it, and that is deliberate. Whatever was already
     * purged is gone either way; holding the watermark for a member that is
     * moving again costs disk and protects the only thing still protectable.
     */
    @Test
    public void aStrandedMemberThatMovesIsTakenBack() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final long snapshot = 8 * GIB;

        w.compute(snapshot, answer(t, member(1, 3 * GIB, fresh(t))), -1, -1, NOW);
        assertEquals(List.of(1), w.strandedMembers());

        final RetentionWatermark.Result r =
            w.compute(snapshot, answer(t, member(1, 7 * GIB, fresh(t))), -1, -1, NOW + 1_000);
        assertEquals(7 * GIB, r.position);
        assertEquals("member1", r.limiter);
        assertTrue(r.stranded.isEmpty());
    }

    // ---- no answer at all ----

    /**
     * The consensus module did not answer. That is not permission to purge to the
     * snapshot: members we were already tracking still hold their positions.
     */
    @Test
    public void anUnansweredQueryHoldsAtTheLastKnownPositions() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);

        w.compute(SNAPSHOT, answer(t, member(1, 6_000, fresh(t))), -1, -1, NOW);

        final RetentionWatermark.Result r = w.compute(SNAPSHOT, null, -1, -1, NOW + 1_000);
        assertEquals(6_000, r.position);
        assertEquals("member1", r.limiter);
    }

    /**
     * With nothing known at all it falls back to the snapshot, which is the
     * pre-watermark behaviour and the bound this node's own recovery needs.
     */
    @Test
    public void anUnansweredQueryWithNoHistoryFallsBackToTheSnapshot() {
        final RetentionWatermark w = new RetentionWatermark();
        final RetentionWatermark.Result r = w.compute(SNAPSHOT, null, -1, -1, NOW);
        assertEquals(SNAPSHOT, r.position);
        assertEquals("snapshot", r.limiter);
    }

    /** Every input is reported, so a surprising number can be read rather than guessed at. */
    @Test
    public void theDetailCarriesEveryInput() throws Exception {
        final RetentionWatermark w = new RetentionWatermark();
        final long t = TimeUnit.MINUTES.toNanos(10);
        final RetentionWatermark.Result r = w.compute(SNAPSHOT,
            answer(t, member(0, 9_500, fresh(t))), 9_800, 9_900, NOW);

        assertEquals(Long.valueOf(SNAPSHOT), r.detail.get("snapshot"));
        assertEquals(Long.valueOf(9_800), r.detail.get("s3Verified"));
        assertEquals(Long.valueOf(9_900), r.detail.get("backup"));
        assertEquals(Long.valueOf(9_500), r.detail.get("slowestMember"));
        assertEquals(9_500, r.position);
    }
}
