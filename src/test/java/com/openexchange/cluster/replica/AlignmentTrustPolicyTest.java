// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowKind;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Kit half of the alignment trust: the trigger policy with the byte-equal alignment trust, pure (the fake writer and
 * clock of {@link ShadowTriggerPolicyTest}, a fake set of retrieved member snapshots). UNVERIFIED writes no STANDBY or
 * LOCAL shadow; an ALIGNED image is held until the member snapshot at the same position is retrieved, then written
 * when equal and fenced when not; the wait is bounded; a VERIFIED replica that diverges fences at the next ALIGNED.
 * Control {@code ALIGN_COMPARE_OFF}: the rule before the alignment trust (trusted at once, no compare).
 */
public class AlignmentTrustPolicyTest {

    static final long WAIT_MS = 120_000;
    static final long P = 4096;

    final ShadowTriggerPolicyTest t = new ShadowTriggerPolicyTest();
    final Map<Long, DirectBuffer> retrieved = new HashMap<>();

    long reported = -1;
    /** The bytes the backup's snapshot retrieval recorded so far (-1: unknown). */
    long retrieval = -1;

    final AlignmentTrust.MemberSnapshots snapshots = new AlignmentTrust.MemberSnapshots() {
        @Override
        public long newestReported() {
            return reported;
        }

        @Override
        public long retrievalProgress() {
            return retrieval;
        }

        @Override
        public long[] positions() {
            return retrieved.keySet().stream().mapToLong(Long::longValue).sorted().toArray();
        }

        @Override
        public DirectBuffer payload(final long position) {
            return retrieved.get(position);
        }
    };

    /** What the fake serializer writes: {@code length} enveloped bytes of a zero payload ({@code differ}: one flipped). */
    DirectBuffer memberImage(final int length, final boolean differ) {
        return ShadowTriggerPolicyTest.enveloped(length, differ);
    }

    AlignmentTrust trust(final boolean fullReplay, final Set<ReplicaNegativeControl> off) {
        final AlignmentTrust trust = new AlignmentTrust(snapshots, () -> t.now[0], WAIT_MS, 1, 0,
                off.contains(ReplicaNegativeControl.ALIGN_COMPARE_OFF));
        trust.start(fullReplay, P - 1024);
        return trust;
    }

    ShadowTriggerPolicy policy(final AlignmentTrust trust, final Set<ReplicaNegativeControl> off) {
        t.cursor = new LogCursor(P, 1_000, 1, 0);
        final ShadowTriggerPolicy p = t.policy(off);
        p.alignmentTrust(trust);
        return p;
    }

    static Set<ReplicaNegativeControl> none() {
        return EnumSet.noneOf(ReplicaNegativeControl.class);
    }

    void advanceMs(final long ms) {
        t.now[0] += TimeUnit.MILLISECONDS.toNanos(ms);
    }

    @Test
    public void anUnverifiedReplicaWritesNoStandbyOrLocalShadow() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 1);
        p.producePending();
        p.pollHeld(P);
        if (!t.writer.offered.isEmpty()) {
            throw new AssertionError("a STANDBY shadow was offered while UNVERIFIED (at " + P + ")");
        }
        assertEquals(1, p.untrustedTotal());
        System.out.println("GREEN unverified: no STANDBY shadow, untrusted=" + p.untrustedTotal());
    }

    @Test
    public void anAlignedImageIsHeldUntilTheMemberSnapshotThenWrittenWhenEqual() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        assertTrue(p.onSnapshotAction(P, 1, 1_000, 0));
        p.producePending();
        p.pollHeld(P);
        if (!t.writer.offered.isEmpty()) {
            throw new AssertionError("ALIGNED offered before the member compare (at " + P + ")");
        }
        assertTrue("held", p.holding());
        assertFalse("still unverified", trust.trusted());
        retrieved.put(P, memberImage(t.serializeLength, false));
        advanceMs(500);
        p.pollHeld(P);
        assertEquals("written after the EQUAL compare", 1, t.writer.offered.size());
        assertEquals(ShadowKind.ALIGNED, t.writer.offered.get(0).kind());
        assertTrue("VERIFIED after an EQUAL compare", trust.trusted());
        assertTrue(t.fences.toString(), t.fences.isEmpty());
        // VERIFIED: STANDBY is written again
        t.writer.offered.clear();
        p.onSnapshotAction(P, 1, 1_000, 1);
        assertEquals(ShadowKind.STANDBY, p.producePending());
        System.out.println("GREEN held then EQUAL: written, verified");
    }

    @Test
    public void aDifferentMemberSnapshotFences() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        retrieved.put(P, memberImage(t.serializeLength, true));
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        p.pollHeld(P);
        assertDiffers();
        assertTrue("nothing written", t.writer.offered.isEmpty());
    }

    @Test
    public void aVerifiedReplicaThatDivergesFencesAtTheNextAligned() {
        final AlignmentTrust trust = trust(true, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        retrieved.put(P, memberImage(t.serializeLength, true));
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        p.pollHeld(P);
        assertDiffers();
    }

    void assertDiffers() {
        final String text = "oracle mismatch at " + P + ": first difference at byte";
        if (t.fences.stream().noneMatch(f -> f.contains(text))) {
            throw new AssertionError("divergence not fenced: expected '" + text + "', fences " + t.fences
                    + ", offered " + t.writer.offered.size());
        }
    }

    @Test
    public void theVerifyWaitIsBounded() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        p.pollHeld(P); // the follower polls every pass (the stall clock starts at the first observation)
        advanceMs(WAIT_MS - 1_000);
        p.pollHeld(P);
        assertTrue("not yet: " + t.fences, t.fences.isEmpty());
        advanceMs(2_000);
        p.pollHeld(P);
        if (!t.fences.contains(stallText(P))) {
            throw new AssertionError("the verify wait is not bounded: expected '" + stallText(P) + "', fences "
                    + t.fences);
        }
        // a VERIFIED replica drops that one shadow instead (a backup timing gap is not a divergence)
        final ShadowTriggerPolicyTest t2 = new ShadowTriggerPolicyTest();
        final AlignmentTrust verified = new AlignmentTrust(snapshots, () -> t2.now[0], WAIT_MS, 1, 0, false);
        verified.start(true, 0);
        t2.cursor = new LogCursor(P, 1_000, 1, 0);
        final ShadowTriggerPolicy p2 = t2.policy(none());
        p2.alignmentTrust(verified);
        p2.onSnapshotAction(P, 1, 1_000, 0);
        p2.producePending();
        t2.now[0] += TimeUnit.MILLISECONDS.toNanos(WAIT_MS + 1_000);
        p2.pollHeld(P);
        assertTrue(t2.fences.toString(), t2.fences.isEmpty());
        assertFalse("dropped", p2.holding());
        assertTrue("never written", t2.writer.offered.isEmpty());
    }

    @Test
    public void anUnverifiedReplicaThatPassedAMemberSnapshotWithoutAnActionFences() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        retrieved.put(P + 512, memberImage(t.serializeLength, false));
        p.pollHeld(P + 2048);
        if (t.fences.stream().noneMatch(f -> f.contains("state at " + (P + 512) + " differs from member snapshot"))) {
            throw new AssertionError("a member snapshot position passed without a SNAPSHOT action is not fenced:"
                    + " fences " + t.fences);
        }
    }

    /**
     * A member snapshot a backup response named but a failed backup cycle never recorded (the re-genesis with cluster/
     * kept: the supplier refuses the live-log replay at the old stop) starts the bounded wait too.
     */
    @Test
    public void aReportedButNeverRetrievedSnapshotStartsTheBoundedWait() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        reported = P + 8192;
        p.pollHeld(P);
        assertTrue(t.fences.toString(), t.fences.isEmpty());
        advanceMs(WAIT_MS + 1_000);
        p.pollHeld(P);
        if (!t.fences.contains(stallText(P))) {
            throw new AssertionError("a reported member snapshot the replica never reaches is not fenced: "
                    + t.fences);
        }
    }

    @Test
    public void aNewerRetrievedSnapshotMakesTheHeldOneUnverifiable() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        retrieved.put(P + 8192, memberImage(t.serializeLength, false));
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        p.pollHeld(P);
        assertFalse("dropped", p.holding());
        assertTrue(t.writer.offered.isEmpty());
        assertTrue(t.fences.toString(), t.fences.isEmpty());
        assertEquals(1, trust.unverifiableTotal());
    }

    @Test
    public void negativeControlAlignCompareOff() {
        final Set<ReplicaNegativeControl> off = EnumSet.of(ReplicaNegativeControl.ALIGN_COMPARE_OFF);
        final AlignmentTrust trust = trust(false, off);
        final ShadowTriggerPolicy p = policy(trust, off);
        retrieved.put(P, memberImage(t.serializeLength, true));
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        p.pollHeld(P);
        assertTrue("trusted at once under the control", trust.trusted());
        assertEquals("written without a compare", 1, t.writer.offered.size());
        assertTrue(t.fences.toString(), t.fences.isEmpty());
        System.out.println("GREEN NC ALIGN_COMPARE_OFF (kit): a different member snapshot is written unfenced");
    }
    // ------------------------------------------------------------------------------------------ stall bound

    /** The one stall text (a re-genesis fences through it; the state-differs text is a byte difference). */
    static String stallText(final long applied) {
        return "FATAL replica: unverified continuation at " + applied + " (history mismatch or a stalled backup);"
                + " wipe BASE_DIR";
    }

    /**
     * A lagging restart. The members advanced and snapshotted while the replica was down; its catch-up to the
     * newest member snapshot takes three times REPLICA_VERIFY_WAIT_MS, its applied position advancing all the while.
     * The ALIGNED at an older member snapshot is UNVERIFIABLE (the backup fetched only the newest) and skipped; the
     * ALIGNED at the newest compares EQUAL. Never fenced.
     */
    @Test
    public void aLaggingCatchUpLongerThanTheBoundIsVerifiedAndNeverFenced() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        final long newest = P + 64 * 1024;
        retrieved.put(newest, memberImage(t.serializeLength, false));
        reported = newest;
        long applied = P - 1024;
        boolean olderAligned = false;
        for (int s = 0; s < 3 * WAIT_MS / 1_000; s++) {
            advanceMs(1_000);
            applied += 128;
            if (!olderAligned && applied >= P) {
                olderAligned = true;
                t.cursor = new LogCursor(P, 1_000, 1, 0);
                p.onSnapshotAction(P, 1, 1_000, 0);
                p.producePending();
            }
            p.pollHeld(applied);
            if (!t.fences.isEmpty()) {
                throw new AssertionError("a lagging restart fenced while its applied position advanced (after "
                        + (s + 1) + " s, applied " + applied + ", newest member snapshot " + newest + "): " + t.fences);
            }
        }
        assertEquals("the older ALIGNED is UNVERIFIABLE, skipped", 1, trust.unverifiableTotal());
        assertTrue("nothing written before the compare", t.writer.offered.isEmpty());
        t.cursor = new LogCursor(newest, 2_000, 1, 0);
        p.onSnapshotAction(newest, 1, 2_000, 0);
        p.producePending();
        p.pollHeld(newest);
        assertTrue(t.fences.toString(), t.fences.isEmpty());
        assertTrue("VERIFIED at the newest member snapshot", trust.trusted());
        assertEquals(1, t.writer.offered.size());
        assertEquals(newest, t.writer.offered.get(0).at().logPosition());
        System.out.println("GREEN lagging restart: " + 3 * WAIT_MS + " ms of catch-up, verified at " + newest);
    }

    /** No advance while a newer member snapshot exists: the stall bound fences with the stall text in full. */
    @Test
    public void aStalledCatchUpFencesWithTheStallText() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        reported = P + 64 * 1024;
        p.pollHeld(P);
        for (int s = 1; s < WAIT_MS / 1_000; s++) {
            advanceMs(1_000);
            p.pollHeld(P);
        }
        assertTrue("not yet: " + t.fences, t.fences.isEmpty());
        advanceMs(2_000);
        p.pollHeld(P);
        if (!t.fences.equals(java.util.List.of(stallText(P)))) {
            throw new AssertionError("expected the stall text '" + stallText(P) + "', fences " + t.fences);
        }
    }

    /**
     * The replica reached the newest member snapshot (its ALIGNED is held) and the payload is never retrieved:
     * the clock runs although the applied position keeps advancing past it (live following is no progress toward
     * verification).
     */
    @Test
    public void reachedTheNewestMemberSnapshotWithoutItsPayloadIsAStall() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertTrue("held", p.holding());
        long applied = P;
        long fencedAt = -1;
        for (int s = 0; s < WAIT_MS / 1_000 + 5 && fencedAt < 0; s++) {
            advanceMs(1_000);
            applied += 64;
            p.pollHeld(applied);
            if (!t.fences.isEmpty()) {
                fencedAt = applied;
            }
        }
        if (fencedAt < 0 || !t.fences.equals(java.util.List.of(stallText(fencedAt)))) {
            throw new AssertionError("reached the newest member snapshot, payload never retrieved: expected the"
                    + " stall text at the applied position, fences " + t.fences);
        }
        assertTrue("nothing written", t.writer.offered.isEmpty());
    }

    /** The member snapshots by position and leadership term id. */
    final Map<Long, Map<Long, DirectBuffer>> byTerm = new HashMap<>();

    final AlignmentTrust.MemberSnapshots termed = new AlignmentTrust.MemberSnapshots() {
        @Override
        public long[] positions() {
            return byTerm.keySet().stream().mapToLong(Long::longValue).sorted().toArray();
        }

        /** What a term-blind lookup returns: the LAST entry at the position (the RecordingLog scan order). */
        @Override
        public DirectBuffer payload(final long position) {
            final Map<Long, DirectBuffer> m = byTerm.get(position);
            return m == null ? null : m.get(m.keySet().stream().mapToLong(Long::longValue).max().orElseThrow());
        }

        @Override
        public DirectBuffer payload(final long position, final long leadershipTermId) {
            final Map<Long, DirectBuffer> m = byTerm.get(position);
            return m == null ? null : m.get(leadershipTermId);
        }

        @Override
        public long[] termsAt(final long position) {
            final Map<Long, DirectBuffer> m = byTerm.get(position);
            return m == null ? new long[0] : m.keySet().stream().mapToLong(Long::longValue).sorted().toArray();
        }
    };

    private void twoTermsAtP() {
        byTerm.computeIfAbsent(P, k -> new HashMap<>()).put(1L, memberImage(t.serializeLength, false));
        byTerm.computeIfAbsent(P, k -> new HashMap<>()).put(2L, memberImage(t.serializeLength, true));
    }

    /** Two valid entries at the trigger's position with different terms: not ready, never compared. */
    @Test
    public void twoMemberEntriesAtOnePositionAreAnAmbiguityNotACompare() {
        twoTermsAtP();
        final AlignmentTrust trust = new AlignmentTrust(termed, () -> t.now[0], WAIT_MS, 1, 0, false);
        trust.start(false, P - 1024);
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        advanceMs(500);
        p.pollHeld(P);
        if (!t.fences.isEmpty() || trust.comparesTotal() != 0 || !t.writer.offered.isEmpty()) {
            throw new AssertionError("two member entries at " + P + " with terms [1, 2] were compared: fences "
                    + t.fences + " compares " + trust.comparesTotal() + " offered " + t.writer.offered.size());
        }
        assertFalse("not ready", trust.ready());
        assertTrue(trust.detail(), trust.detail() != null && trust.detail().contains("ambiguous"));
        System.out.println("GREEN ambiguity (UNVERIFIED): " + trust.detail());
    }

    /** The same ambiguity on a VERIFIED replica reads not ready too, and a single entry of another term is no match. */
    @Test
    public void ambiguityOnAVerifiedReplicaReadsNotReady() {
        twoTermsAtP();
        final AlignmentTrust trust = new AlignmentTrust(termed, () -> t.now[0], WAIT_MS, 1, 0, false);
        trust.start(true, 0);
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        p.pollHeld(P);
        if (!t.fences.isEmpty() || trust.comparesTotal() != 0 || trust.ready()) {
            throw new AssertionError("a VERIFIED replica with two member entries at " + P + " (terms [1, 2]):"
                    + " expected not ready and no compare, got ready=" + trust.ready() + " compares "
                    + trust.comparesTotal() + " fences " + t.fences);
        }
        // one entry, another term: no match, so no compare against it (no fence, nothing written)
        byTerm.clear();
        byTerm.computeIfAbsent(P + 512, k -> new HashMap<>()).put(2L, memberImage(t.serializeLength, true));
        t.cursor = new LogCursor(P + 512, 1_100, 1, 0);
        p.onSnapshotAction(P + 512, 1, 1_100, 0);
        p.producePending();
        p.pollHeld(P + 512);
        if (!t.fences.isEmpty() || trust.comparesTotal() != 0) {
            throw new AssertionError("a member entry of term 2 at " + (P + 512) + " was compared with a term 1"
                    + " trigger: fences " + t.fences + " compares " + trust.comparesTotal());
        }
    }
    // ---------------------------------------------------------------------------------------- stall progress

    /**
     * The replica reached the newest member snapshot and its payload is still arriving (the retrieval's recorded
     * bytes grow every second) for three times REPLICA_VERIFY_WAIT_MS: progress, never fenced; it is VERIFIED once the
     * payload is retrieved. Then a retrieval that stops growing is a stall again (the stall bound still holds).
     */
    @Test
    public void aProgressingRetrievalLongerThanTheBoundIsNotAStall() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        reported = P;
        retrieval = 0;
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertTrue("held", p.holding());
        long applied = P;
        for (int s = 0; s < 3 * WAIT_MS / 1_000; s++) {
            advanceMs(1_000);
            applied += 64;
            retrieval += 64 * 1024;
            p.pollHeld(applied);
            if (!t.fences.isEmpty()) {
                throw new AssertionError("a progressing member snapshot retrieval fenced after " + (s + 1)
                        + " s (REPLICA_VERIFY_WAIT_MS=" + WAIT_MS + ", retrieved " + retrieval + " B): " + t.fences);
            }
        }
        retrieved.put(P, memberImage(t.serializeLength, false));
        advanceMs(500);
        p.pollHeld(applied);
        assertTrue(t.fences.toString(), t.fences.isEmpty());
        assertTrue("VERIFIED once the payload is retrieved", trust.trusted());
        assertEquals(1, t.writer.offered.size());
        // a stalled retrieval still fences at the bound
        final ShadowTriggerPolicyTest t2 = new ShadowTriggerPolicyTest();
        final AlignmentTrust stalled = new AlignmentTrust(snapshots, () -> t2.now[0], WAIT_MS, 1, 0, false);
        retrieved.clear();
        stalled.start(false, P - 1024);
        t2.cursor = new LogCursor(P, 1_000, 1, 0);
        final ShadowTriggerPolicy p2 = t2.policy(none());
        p2.alignmentTrust(stalled);
        p2.onSnapshotAction(P, 1, 1_000, 0);
        p2.producePending();
        for (int s = 0; s < WAIT_MS / 1_000 + 2 && t2.fences.isEmpty(); s++) {
            t2.now[0] += TimeUnit.MILLISECONDS.toNanos(1_000);
            p2.pollHeld(P);
        }
        assertEquals("a retrieval that stopped growing is a stall", java.util.List.of(stallText(P)), t2.fences);
        System.out.println("GREEN progressing retrieval: " + 3 * WAIT_MS + " ms > REPLICA_VERIFY_WAIT_MS="
                + WAIT_MS + ", verified at " + trust.verifiedAt());
    }

    /**
     * The "reached" reset keys on the target position. The replica holds its ALIGNED
     * at the member snapshot P (reached, the payload not retrieved) and live-follows; the backup then names a newer
     * member snapshot ABOVE the applied position, the replica advances to it and reaches it: the stall clock restarts
     * there (a boolean "reached" stays true across the change and never restarts it). A newer target reached at once
     * does not restart it ({@link #aWedgedBackupWithACadenceShorterThanTheBoundFences}).
     */
    @Test
    public void aNewerTargetCaughtUpToRestartsTheStallClock() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertTrue("held", p.holding());
        long applied = P;
        p.pollHeld(applied);
        for (int s = 0; s < WAIT_MS / 1_000 - 2; s++) {
            advanceMs(1_000);
            applied += 16;
            p.pollHeld(applied);
        }
        assertTrue("not yet: " + t.fences, t.fences.isEmpty());
        final long next = applied + 4096;
        reported = next; // named by the backup while the replica is below it
        advanceMs(1_000);
        p.pollHeld(applied);
        applied = next; // the replica advances to it: its ALIGNED there is held, reached
        t.cursor = new LogCursor(next, 2_000, 1, 0);
        p.onSnapshotAction(next, 1, 2_000, 0);
        p.producePending();
        assertTrue("the newer ALIGNED is held", p.holding());
        advanceMs(1_000);
        p.pollHeld(applied);
        advanceMs(3_000);
        p.pollHeld(applied);
        if (!t.fences.isEmpty()) {
            throw new AssertionError("reaching the newer target " + next + " did not restart the stall clock"
                    + " (first reached " + P + " " + (WAIT_MS + 2_000) + " ms ago): " + t.fences);
        }
        for (int s = 0; s < WAIT_MS / 1_000 + 2 && t.fences.isEmpty(); s++) {
            advanceMs(1_000);
            p.pollHeld(applied += 16);
        }
        assertEquals("the clock restarted at the new target still bounds it", java.util.List.of(stallText(applied)),
                t.fences);
        System.out.println("GREEN reached reset keyed on the target: fenced only " + WAIT_MS + " ms after " + next);
    }

    /**
     * An ambiguity is cleared when its held image is dropped (a VERIFIED replica's verify wait, a newer
     * ALIGNED superseding it) or its position turns UNVERIFIABLE; it never keeps a replica not ready after the image
     * it was about is gone.
     */
    @Test
    public void anAmbiguityIsClearedWhenItsHeldImageIsDroppedOrUnverifiable() {
        final java.util.List<String> wrong = new java.util.ArrayList<>();
        // (a) dropped at the verify wait
        twoTermsAtP();
        AlignmentTrust trust = new AlignmentTrust(termed, () -> t.now[0], WAIT_MS, 1, 0, false);
        trust.start(true, 0);
        ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertFalse("ambiguous: not ready", trust.ready());
        advanceMs(WAIT_MS + 1_000);
        p.pollHeld(P);
        if (p.holding() || !trust.ready()) {
            wrong.add("dropped at the verify wait: holding=" + p.holding() + " ready=" + trust.ready() + " detail="
                    + trust.detail());
        }
        // (b) the position turns UNVERIFIABLE (its entries gone, a newer member snapshot retrieved)
        trust = new AlignmentTrust(termed, () -> t.now[0], WAIT_MS, 1, 0, false);
        trust.start(true, 0);
        p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertFalse("ambiguous: not ready", trust.ready());
        byTerm.remove(P);
        byTerm.computeIfAbsent(P + 8192, k -> new HashMap<>()).put(1L, memberImage(t.serializeLength, false));
        advanceMs(500);
        p.pollHeld(P);
        if (p.holding() || !trust.ready()) {
            wrong.add("UNVERIFIABLE: holding=" + p.holding() + " ready=" + trust.ready() + " detail="
                    + trust.detail());
        }
        // (c) superseded by a newer ALIGNED (still pending, not ambiguous)
        byTerm.clear();
        twoTermsAtP();
        trust = new AlignmentTrust(termed, () -> t.now[0], WAIT_MS, 1, 0, false);
        trust.start(true, 0);
        p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertFalse("ambiguous: not ready", trust.ready());
        t.cursor = new LogCursor(P + 512, 1_100, 1, 0);
        p.onSnapshotAction(P + 512, 1, 1_100, 0);
        p.producePending();
        if (!trust.ready()) {
            wrong.add("superseded: ready=" + trust.ready() + " detail=" + trust.detail());
        }
        if (!wrong.isEmpty() || !t.fences.isEmpty()) {
            throw new AssertionError("an ambiguity outlived its held image: " + wrong + " fences " + t.fences);
        }
        System.out.println("GREEN ambiguity cleared on drop, UNVERIFIABLE and supersede");
    }

    /**
     * A member snapshot retrieval that restarts from zero every backup cycle (the backup resets after
     * each retrieval) is not progress: only bytes past the previous maximum count. The replica reached the member
     * snapshot P, its payload never lands; the stall bound fences although the retrieval counter keeps moving.
     */
    @Test
    public void aRetrievalThatRestartsFromZeroEachCycleIsNotProgress() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        reported = P;
        retrieval = 0;
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertTrue("held", p.holding());
        long applied = P;
        final long cycleMs = 12_000;
        long fencedAfterMs = -1;
        for (long ms = 1_000; ms <= 3 * WAIT_MS && fencedAfterMs < 0; ms += 1_000) {
            advanceMs(1_000);
            applied += 64;
            // each cycle the retrieval starts again at 0 and climbs to the same 6 x 64 KiB before the reset
            final long inCycle = (ms % cycleMs) / 1_000;
            retrieval = Math.min(inCycle, 6) * 64 * 1024;
            p.pollHeld(applied);
            if (!t.fences.isEmpty()) {
                fencedAfterMs = ms;
            }
        }
        if (fencedAfterMs < 0) {
            throw new AssertionError("a retrieval restarting from zero every cycle reset the stall clock for "
                    + 3 * WAIT_MS + " ms (REPLICA_VERIFY_WAIT_MS=" + WAIT_MS + "): never fenced, retrieval resets "
                    + trust.retrievalResetsTotal());
        }
        assertTrue("fenced with the stall text: " + t.fences, t.fences.get(0).equals(stallText(applied)));
        assertTrue("nothing written", t.writer.offered.isEmpty());
        System.out.println("GREEN restarting retrieval is no progress: fenced " + fencedAfterMs + " ms after the"
                + " hold, retrieval resets " + trust.retrievalResetsTotal());
    }

    /**
     * A wedged backup (no member payload ever lands) under a live-following UNVERIFIED replica, with a
     * member cadence shorter than the bound: every cadence brings a newer ALIGNED the replica reaches at once (it is
     * already there when the action is applied). A target reached at once is no progress toward verification; only a
     * target the replica was below and then advanced to restarts the clock. The bound still fences.
     */
    @Test
    public void aWedgedBackupWithACadenceShorterThanTheBoundFences() {
        final AlignmentTrust trust = trust(false, none());
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        assertTrue("held", p.holding());
        long applied = P;
        reported = P;
        final long cadenceMs = WAIT_MS / 2;
        long fencedAfterMs = -1;
        long newTargets = 0;
        for (long ms = 1_000; ms <= 3 * WAIT_MS && fencedAfterMs < 0; ms += 1_000) {
            advanceMs(1_000);
            applied += 64;
            if (ms % cadenceMs == 0) {
                // the members' next snapshot: its ALIGNED is applied (held, reached at once) and the backup names it
                t.cursor = new LogCursor(applied, 1_000 + ms, 1, 0);
                p.onSnapshotAction(applied, 1, 1_000 + ms, 0);
                p.producePending();
                reported = applied;
                newTargets++;
            }
            p.pollHeld(applied);
            if (!t.fences.isEmpty()) {
                fencedAfterMs = ms;
            }
        }
        if (fencedAfterMs < 0) {
            throw new AssertionError("a wedged backup with a member cadence of " + cadenceMs + " ms < the bound "
                    + WAIT_MS + " ms never fenced in " + 3 * WAIT_MS + " ms (" + newTargets + " new targets reached at"
                    + " once each restarted the clock)");
        }
        assertEquals(java.util.List.of(stallText(applied)), t.fences);
        assertTrue("nothing written", t.writer.offered.isEmpty());
        System.out.println("GREEN wedged backup, cadence " + cadenceMs + " ms < bound " + WAIT_MS + ": fenced "
                + fencedAfterMs + " ms in");
    }

    /** The fake cluster-identity view of the newest named member snapshot. */
    boolean namedUnusable;
    long namedCompares;

    AlignmentTrust.NamedCopy namedCopy() {
        return new AlignmentTrust.NamedCopy() {
            @Override
            public boolean unusable() {
                return namedUnusable;
            }

            @Override
            public long completedCompares() {
                return namedCompares;
            }
        };
    }

    /**
     * The newest named member snapshot lies BELOW the restore point and has no usable copy (the backup
     * reset during SNAPSHOT_RETRIEVE, a partial copy). Before the named-copy rule no stall clock ran there (the target
     * was not above the restore point) and the replica stayed 503 forever; now the stall clock runs and fences with the stall text.
     */
    @Test
    public void anUnusableNamedCopyBelowTheRestorePointRunsTheStallClock() {
        final AlignmentTrust trust = trust(false, none());
        trust.namedCopy(namedCopy());
        final ShadowTriggerPolicy p = policy(trust, none());
        final long applied = P;
        reported = P - 2048; // below the restore point P - 1024
        namedUnusable = true;
        long fencedAfterMs = -1;
        for (long ms = 1_000; ms <= 3 * WAIT_MS && fencedAfterMs < 0; ms += 1_000) {
            advanceMs(1_000);
            p.pollHeld(applied);
            if (!t.fences.isEmpty()) {
                fencedAfterMs = ms;
            }
        }
        if (fencedAfterMs < 0) {
            throw new AssertionError("an unusable named copy below the restore point ran no stall clock: no"
                    + " fence in " + 3 * WAIT_MS + " ms (REPLICA_VERIFY_WAIT_MS=" + WAIT_MS + ")");
        }
        assertEquals(java.util.List.of(stallText(applied)), t.fences);
        assertTrue("fenced at the bound, not before: " + fencedAfterMs, fencedAfterMs >= WAIT_MS);
        assertTrue("nothing written", t.writer.offered.isEmpty());
        System.out.println("GREEN unusable named copy below the restore point: fenced " + fencedAfterMs + " ms in");
    }

    /**
     * While the named copy is unusable, progress is only a newly passed retrieval maximum or a
     * completed compare; the applied position advancing (live following) is not. Retrieval bytes climbing past their
     * maximum keep the clock from firing; once they stop, the bound fences.
     */
    @Test
    public void retrievalPastItsMaximumIsProgressAppliedAdvanceIsNot() {
        final AlignmentTrust trust = trust(false, none());
        trust.namedCopy(namedCopy());
        final ShadowTriggerPolicy p = policy(trust, none());
        long applied = P;
        reported = P - 2048;
        namedUnusable = true;
        retrieval = 0;
        for (long ms = 1_000; ms <= 2 * WAIT_MS; ms += 1_000) {
            advanceMs(1_000);
            applied += 64;
            retrieval += 1024; // bytes arriving past every previous maximum
            p.pollHeld(applied);
        }
        if (!t.fences.isEmpty()) {
            throw new AssertionError("a retrieval passing its maximum did not count as progress: " + t.fences);
        }
        long fencedAfterMs = -1;
        for (long ms = 1_000; ms <= 3 * WAIT_MS && fencedAfterMs < 0; ms += 1_000) {
            advanceMs(1_000);
            applied += 64; // live following is no progress toward verification
            p.pollHeld(applied);
            if (!t.fences.isEmpty()) {
                fencedAfterMs = ms;
            }
        }
        if (fencedAfterMs < 0) {
            throw new AssertionError("an advancing applied position kept an unusable named copy from fencing"
                    + " for " + 3 * WAIT_MS + " ms");
        }
        assertEquals(java.util.List.of(stallText(applied)), t.fences);
        System.out.println("GREEN progress = retrieval maximum; applied advance is not: fenced " + fencedAfterMs
                + " ms after the retrieval stopped");
    }

    /** A completed compare is progress; a usable copy again leaves the stall and retrieval rules. */
    @Test
    public void aCompletedCompareIsProgress() {
        final AlignmentTrust trust = trust(false, none());
        trust.namedCopy(namedCopy());
        final ShadowTriggerPolicy p = policy(trust, none());
        reported = P - 2048;
        namedUnusable = true;
        for (long ms = 1_000; ms <= 3 * WAIT_MS; ms += 1_000) {
            advanceMs(1_000);
            if (ms % (WAIT_MS / 2) == 0) {
                namedCompares++; // a named copy decoded (pre-ADOPT, or passed), then the next one is named
            }
            p.pollHeld(P);
        }
        if (!t.fences.isEmpty()) {
            throw new AssertionError("completed compares were not progress: " + t.fences);
        }
        namedUnusable = false; // a usable copy, below the restore point: no clock (stall-bound rule)
        for (long ms = 1_000; ms <= 3 * WAIT_MS; ms += 1_000) {
            advanceMs(1_000);
            p.pollHeld(P);
        }
        assertTrue("no fence once the named copy is usable: " + t.fences, t.fences.isEmpty());
        System.out.println("GREEN completed compare is progress; a usable copy runs no clock below the restore"
                + " point");
    }

    /**
     * A named snapshot still in retrieval is the healthy path. The no-usable-named-copy message is INFO while the
     * retrieval passes new maxima; CRITICAL (once) only after one full backup cycle with no new maximum and no completed
     * compare; progress re-arms it. The fence bound is unchanged.
     */
    @Test
    public void theNamedCopyMessageIsCriticalOnlyAfterOneBackupCycleWithoutProgress() {
        final long cycleMs = 30_000;
        final AlignmentTrust trust = trust(false, none());
        trust.backupCycle(cycleMs);
        trust.namedCopy(namedCopy());
        final ShadowTriggerPolicy p = policy(trust, none());
        reported = P - 2048;
        namedUnusable = true;
        retrieval = 0;
        for (long ms = 1_000; ms <= 2 * WAIT_MS; ms += 1_000) {
            advanceMs(1_000);
            retrieval += 1024; // a healthy retrieval: past its maximum every pass
            p.pollHeld(P);
        }
        if (trust.namedCopyCriticalTotal() != 0) {
            throw new AssertionError("the no-usable-named-copy CRITICAL was logged " + trust.namedCopyCriticalTotal() + " time(s) while"
                    + " the retrieval passed new maxima");
        }
        for (long ms = 1_000; ms < cycleMs; ms += 1_000) {
            advanceMs(1_000); // the retrieval stops: less than one backup cycle
            p.pollHeld(P);
        }
        assertEquals("no CRITICAL before one full backup cycle without progress", 0, trust.namedCopyCriticalTotal());
        advanceMs(1_000);
        p.pollHeld(P);
        advanceMs(1_000);
        p.pollHeld(P);
        assertEquals("CRITICAL once after one backup cycle without progress", 1, trust.namedCopyCriticalTotal());
        namedCompares++; // a completed compare is progress: re-armed
        for (long ms = 1_000; ms <= cycleMs + 2_000; ms += 1_000) {
            advanceMs(1_000);
            p.pollHeld(P);
        }
        assertEquals("CRITICAL again after another cycle without progress", 2, trust.namedCopyCriticalTotal());
        assertTrue("the fence bound is unchanged: " + t.fences, t.fences.isEmpty());
        System.out.println("GREEN no-usable-named-copy INFO while retrieving, CRITICAL after one backup cycle (" + cycleMs
                + " ms) without progress");
    }
}
