// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowStub;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The live oracle's verdicts, pure (fake member snapshots, the fake writer and clock of
 * {@link ShadowTriggerPolicyTest}). Order on a retrieved copy: the envelope opens (else UNREADABLE: counted, never a
 * mismatch, never a fence), then the genesis (another genesis is a history fence, genesis 0 is ignored), then the
 * bytes (a same-genesis difference is the latched byte mismatch). The applied-past check runs in both trust states on
 * a ring recorded at the SNAPSHOT action, and a ring miss explained by this replica's own bookkeeping never fences.
 * Each rule has a control that switches it off and makes its row fail with the named text.
 */
public class OracleVerdictTest {

    static final long WAIT_MS = 120_000;
    static final long P = 4096;
    static final long GENESIS = 7; // ShadowTriggerPolicyTest.genesis, the replica's
    static final int ENGINE = ShadowHeader.ENGINE_TEST;

    final ShadowTriggerPolicyTest t = new ShadowTriggerPolicyTest();
    final Map<Long, DirectBuffer> retrieved = new TreeMap<>();
    final Map<DirectBuffer, Long> genesisOfCopy = new IdentityHashMap<>();
    long reported = -1;

    final AlignmentTrust.MemberSnapshots snapshots = new AlignmentTrust.MemberSnapshots() {
        @Override
        public long newestReported() {
            return reported;
        }

        @Override
        public long[] positions() {
            return retrieved.keySet().stream().mapToLong(Long::longValue).toArray();
        }

        @Override
        public DirectBuffer payload(final long position) {
            return retrieved.get(position);
        }

        @Override
        public long recordingId(final long position, final long leadershipTermId) {
            return retrieved.containsKey(position) ? 1000 + position : -1;
        }

        @Override
        public long genesisOf(final DirectBuffer copy, final int length) {
            final Long g = genesisOfCopy.get(copy);
            if (g == null) {
                throw new IllegalStateException("not decodable");
            }
            return g;
        }
    };

    static Set<ReplicaNegativeControl> none() {
        return EnumSet.noneOf(ReplicaNegativeControl.class);
    }

    AlignmentTrust trust(final boolean fullReplay, final long from, final Set<ReplicaNegativeControl> off) {
        final AlignmentTrust trust = new AlignmentTrust(snapshots, () -> t.now[0], WAIT_MS, ENGINE, 0, off);
        trust.start(fullReplay, from);
        return trust;
    }

    ShadowTriggerPolicy policy(final AlignmentTrust trust, final Set<ReplicaNegativeControl> off) {
        t.cursor = new LogCursor(P, 1_000, 1, 0);
        final ShadowTriggerPolicy p = t.policy(off);
        p.alignmentTrust(trust);
        return p;
    }

    /** A same-length copy whose envelope opens; {@code differ} flips one payload byte (sealed again). */
    DirectBuffer copy(final boolean differ, final long genesis) {
        final UnsafeBuffer b = ShadowTriggerPolicyTest.enveloped(t.serializeLength, differ);
        genesisOfCopy.put(b, genesis);
        return b;
    }

    /** A copy whose CRC no longer matches (a payload byte flipped after the seal). */
    DirectBuffer torn() {
        final UnsafeBuffer b = ShadowTriggerPolicyTest.enveloped(t.serializeLength, false);
        b.putByte(SnapshotEnvelope.payloadOffset() + 3, (byte)5);
        genesisOfCopy.put(b, GENESIS);
        return b;
    }

    static UnsafeBuffer stub(final long genesis, final long term, final long position) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[ShadowStub.ENVELOPED_LENGTH]);
        new ShadowStub(ENGINE, 0, genesis, term, position, -1, -1).encodeEnveloped(new SnapshotEnvelope(), b);
        return b;
    }

    void advanceMs(final long ms) {
        t.now[0] += TimeUnit.MILLISECONDS.toNanos(ms);
    }

    ShadowTriggerPolicy holdAt(final AlignmentTrust trust, final Set<ReplicaNegativeControl> off) {
        final ShadowTriggerPolicy p = policy(trust, off);
        p.onSnapshotAction(P, 1, 1_000, 0);
        p.producePending();
        p.pollHeld(P);
        return p;
    }

    static void expectControl(final String control, final Runnable row, final String text) {
        try {
            row.run();
        } catch (final AssertionError expected) {
            if (!expected.getMessage().startsWith(text)) {
                throw new AssertionError("NC " + control + " failed with another text: " + expected.getMessage(),
                        expected);
            }
            System.out.println("NC " + control + " -> " + expected.getMessage());
            return;
        }
        fail("NC " + control + ": the row passed with the rule switched off");
    }

    // ------------------------------------------------------------------------------------------------ verdicts

    void sameGenesisDifferentBytes(final Set<ReplicaNegativeControl> off) {
        final AlignmentTrust trust = trust(true, 0, off);
        retrieved.put(P, copy(true, GENESIS));
        final ShadowTriggerPolicy p = holdAt(trust, off);
        if (trust.mismatchTotal() != 1) {
            throw new AssertionError("mismatch not counted: shadow_oracle_mismatch_total=" + trust.mismatchTotal());
        }
        assertEquals(List.of(FenceFamily.MISMATCH), t.families);
        assertTrue(t.fences.toString(), t.fences.get(0).startsWith("oracle mismatch at " + P
                + ": first difference at byte "));
        assertEquals(P, trust.mismatchAt());
        assertTrue("evidence kept", trust.mismatchEvidence() != null);
        assertEquals(1000 + P, trust.mismatchEvidence().memberRecordingId());
        assertEquals(GENESIS, trust.mismatchEvidence().memberGenesis());
        assertTrue("nothing written", t.writer.offered.isEmpty());
        assertTrue(!p.holding());
    }

    @Test
    public void aSealedSameGenesisCopyWithOtherBytesIsALatchedMismatch() {
        sameGenesisDifferentBytes(none());
        System.out.println("GREEN DIFFERENT: " + t.fences.get(0));
    }

    void tornFullCopy(final Set<ReplicaNegativeControl> off) {
        final AlignmentTrust trust = trust(true, 0, off);
        retrieved.put(P, torn());
        final ShadowTriggerPolicy p = holdAt(trust, off);
        advanceMs(500);
        p.pollHeld(P);
        if (!t.fences.isEmpty()) {
            throw new AssertionError("torn copy read as a mismatch: " + t.fences);
        }
        assertEquals(1, trust.unreadableTotal());
        assertEquals(0, trust.mismatchTotal());
        assertTrue("the held image stays PENDING", p.holding());
    }

    @Test
    public void aCopyWithABadCrcIsUnreadableNotAMismatch() {
        tornFullCopy(none());
        System.out.println("GREEN UNREADABLE (full copy): held, no fence");
    }

    @Test
    public void controlEnvelopeCheckOffReadsATornCopyAsAMismatch() {
        expectControl("ENVELOPE_CHECK_OFF", () -> tornFullCopy(EnumSet.of(ReplicaNegativeControl.ENVELOPE_CHECK_OFF)),
                "torn copy read as a mismatch");
    }

    void tornStub(final Set<ReplicaNegativeControl> off) {
        final AlignmentTrust trust = trust(true, 0, off);
        final UnsafeBuffer s = stub(GENESIS, 1, P);
        s.putByte(SnapshotEnvelope.payloadOffset() + 30, (byte)0x7f); // inside the genesis field, CRC now wrong
        retrieved.put(P, s);
        final ShadowTriggerPolicy p = holdAt(trust, off);
        if (!t.fences.isEmpty()) {
            throw new AssertionError("torn stub copy held the replica: " + t.fences);
        }
        assertEquals(1, trust.unreadableTotal());
        assertTrue(p.holding());
    }

    @Test
    public void aTornStubIsUnreadableNotARefusedStub() {
        tornStub(none());
        System.out.println("GREEN UNREADABLE (stub): held, no fence");
    }

    @Test
    public void controlEnvelopeCheckOffReadsATornStubAsAMismatch() {
        expectControl("ENVELOPE_CHECK_OFF (stub)",
                () -> tornStub(EnumSet.of(ReplicaNegativeControl.ENVELOPE_CHECK_OFF)), "torn stub copy held the replica");
    }

    void otherGenesis(final Set<ReplicaNegativeControl> off) {
        final AlignmentTrust trust = trust(true, 0, off);
        retrieved.put(P, copy(true, 99));
        holdAt(trust, off);
        if (!t.families.equals(List.of(FenceFamily.HISTORY))) {
            throw new AssertionError("another genesis read as a same-history mismatch: " + t.families + " "
                    + t.fences);
        }
        assertTrue(t.fences.get(0), t.fences.get(0).contains("differs from member snapshot")
                && t.fences.get(0).contains("holds genesis 99"));
        assertEquals(1, trust.otherGenesisTotal());
        assertEquals(0, trust.mismatchTotal());
    }

    @Test
    public void anOpenedCopyOfAnotherGenesisIsAHistoryFenceNeverAHold() {
        otherGenesis(none());
        System.out.println("GREEN OTHER_GENESIS: " + t.fences.get(0));
    }

    @Test
    public void controlGenesisCheckOffReadsAnotherGenesisAsAMismatch() {
        expectControl("GENESIS_CHECK_OFF", () -> otherGenesis(EnumSet.of(ReplicaNegativeControl.GENESIS_CHECK_OFF)),
                "another genesis read as a same-history mismatch");
    }

    @Test
    public void aCopyTakenBeforeTheAdoptIsIgnored() {
        final AlignmentTrust trust = trust(true, 0, none());
        retrieved.put(P, copy(true, 0));
        final ShadowTriggerPolicy p = holdAt(trust, none());
        advanceMs(500);
        p.pollHeld(P);
        assertTrue(t.fences.toString(), t.fences.isEmpty());
        assertEquals("counted once", 1, trust.preAdoptTotal());
        assertTrue(p.holding());
    }

    @Test
    public void aStubIsComparedByIdentityAfterItOpened() {
        final AlignmentTrust trust = trust(false, P - 1024, none());
        retrieved.put(P, stub(GENESIS, 1, P));
        holdAt(trust, none());
        assertEquals("written after the identity compare", 1, t.writer.offered.size());
        assertEquals(1, trust.stubComparesTotal());
        assertTrue(trust.trusted());
        // another term at the same position: an identity mismatch (not tearing)
        final OracleVerdictTest other = new OracleVerdictTest();
        final AlignmentTrust t2 = other.trust(true, 0, none());
        other.retrieved.put(P, stub(GENESIS, 3, P));
        other.holdAt(t2, none());
        assertEquals(List.of(FenceFamily.MISMATCH), other.t.families);
        assertTrue(other.t.fences.get(0), other.t.fences.get(0).contains("stub genesis=7 term=3"));
        // a stub of another genesis: another history
        final OracleVerdictTest third = new OracleVerdictTest();
        final AlignmentTrust t3 = third.trust(true, 0, none());
        third.retrieved.put(P, stub(99, 1, P));
        third.holdAt(t3, none());
        assertEquals(List.of(FenceFamily.HISTORY), third.t.families);
        assertEquals(1, t3.equalTotal() + t3.otherGenesisTotal());
    }

    @Test
    public void anUnverifiedStallOnATornCopyNamesTheCopy() {
        final AlignmentTrust trust = trust(false, P - 1024, none());
        retrieved.put(P, torn());
        final ShadowTriggerPolicy p = holdAt(trust, none());
        for (int i = 0; i < 130 && t.fences.isEmpty(); i++) {
            advanceMs(1_000);
            p.pollHeld(P);
        }
        assertEquals(List.of(FenceFamily.FAULT), t.families);
        final String suffix = "; the last member copy at " + P + " (rec " + (1000 + P) + ") was unreadable: ";
        if (!t.fences.get(0).startsWith(AlignmentTrust.stallText(P)) || !t.fences.get(0).contains(suffix)) {
            throw new AssertionError("stall text blames the replica for a torn member copy: " + t.fences);
        }
        System.out.println("GREEN stall on a torn copy: " + t.fences.get(0));
    }

    // ------------------------------------------------------------------------------------------------ ring

    void preAdoptActionInRing(final boolean ringAtHeld) {
        final AlignmentTrust trust = trust(true, 0, none());
        trust.ringAtHeldOnly = ringAtHeld;
        t.genesis = 0; // before the ADOPT: the trigger is unidentified, nothing held
        final ShadowTriggerPolicy p = policy(trust, none());
        p.onSnapshotAction(P, 1, 1_000, 0);
        assertEquals(null, p.producePending());
        t.genesis = GENESIS;
        reported = P; // the backup names the members' snapshot at that action, not retrieved yet
        advanceMs(500);
        p.pollHeld(P + 2048);
        if (!t.fences.isEmpty()) {
            throw new AssertionError("a pre-ADOPT member snapshot fenced a genesis-start replica: " + t.fences);
        }
        assertEquals(1, trust.actionsRing());
    }

    @Test
    public void aPreAdoptActionIsInTheRingAndNeverFences() {
        preAdoptActionInRing(false);
        System.out.println("GREEN ring at the action: pre-ADOPT action recorded, no fence");
    }

    @Test
    public void controlRingAtTheHeldImageFencesAGenesisStart() {
        expectControl("ring at the held image (seam)", () -> preAdoptActionInRing(true),
                "a pre-ADOPT member snapshot fenced a genesis-start replica");
    }

    void retrievedSetBelowVerifiedAt(final boolean ignoreVerifiedAt) {
        final long p2 = P + 4096;
        final AlignmentTrust trust = trust(false, P - 1024, none());
        trust.tickIgnoresVerifiedAt = ignoreVerifiedAt;
        retrieved.put(P, copy(false, GENESIS));
        final ShadowTriggerPolicy p = holdAt(trust, none());
        t.cursor = new LogCursor(p2, 2_000, 1, 0);
        retrieved.put(p2, copy(false, GENESIS));
        p.onSnapshotAction(p2, 1, 2_000, 0);
        p.producePending();
        assertEquals(2, t.writer.offered.size());
        assertEquals(p2, trust.verifiedAt());
        assertEquals("the ring drops entries at or below each EQUAL", 0, trust.actionsRing());
        advanceMs(500);
        p.pollHeld(p2 + 2048);
        if (!t.fences.isEmpty()) {
            throw new AssertionError("a retrieved set below verifiedAt fenced a VERIFIED replica: " + t.fences);
        }
    }

    @Test
    public void aRetrievedSetBelowTheLastEqualNeverFences() {
        retrievedSetBelowVerifiedAt(false);
        System.out.println("GREEN retrieved set below verifiedAt: no fence");
    }

    @Test
    public void controlNoLastEqualLowerBoundFencesAVerifiedReplica() {
        expectControl("no verifiedAt lower bound (seam)", () -> retrievedSetBelowVerifiedAt(true),
                "a retrieved set below verifiedAt fenced a VERIFIED replica");
    }

    void verifiedReplicaAppliedPast(final Set<ReplicaNegativeControl> off) {
        final AlignmentTrust trust = trust(true, 0, off);
        final ShadowTriggerPolicy p = policy(trust, off);
        retrieved.put(P + 512, copy(false, GENESIS)); // a member snapshot where this replica applied no action
        advanceMs(500);
        p.pollHeld(P + 2048);
        if (!t.families.equals(List.of(FenceFamily.HISTORY))) {
            throw new AssertionError("a VERIFIED replica applied past a named member snapshot without fencing: "
                    + t.fences);
        }
        assertTrue(t.fences.get(0), t.fences.get(0).contains("applied past " + (P + 512)
                + " without a SNAPSHOT action there"));
    }

    @Test
    public void aVerifiedReplicaThatAppliedPastAMemberSnapshotFences() {
        verifiedReplicaAppliedPast(none());
        System.out.println("GREEN applied-past while VERIFIED: " + t.fences.get(0));
    }

    @Test
    public void controlTickUnverifiedOnlyLetsAVerifiedReplicaFollowAnotherHistory() {
        expectControl("TICK_UNVERIFIED_ONLY",
                () -> verifiedReplicaAppliedPast(EnumSet.of(ReplicaNegativeControl.TICK_UNVERIFIED_ONLY)),
                "a VERIFIED replica applied past a named member snapshot without fencing");
    }
}
