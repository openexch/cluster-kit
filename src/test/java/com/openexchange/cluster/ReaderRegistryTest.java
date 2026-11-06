// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import static com.openexchange.cluster.ReaderRegistry.Gate.CLOSED;
import static com.openexchange.cluster.ReaderRegistry.Gate.OPEN;
import static com.openexchange.cluster.ReaderRegistry.Kind.BRIDGE;
import static com.openexchange.cluster.ReaderRegistry.Kind.CLUSTER_BACKUP;
import static com.openexchange.cluster.ReaderRegistry.Kind.EXEC_PROJECTOR;
import static com.openexchange.cluster.ReaderRegistry.Reject.BACKWARDS;
import static com.openexchange.cluster.ReaderRegistry.Reject.FUTURE_POSITION;
import static com.openexchange.cluster.ReaderRegistry.Reject.NONE;
import static com.openexchange.cluster.ReaderRegistry.Reject.UNKNOWN_READER;
import static com.openexchange.cluster.ReaderRegistry.Reject.WRONG_EPOCH;
import static com.openexchange.cluster.ReaderRegistry.Status.ACTIVE;
import static com.openexchange.cluster.ReaderRegistry.Status.HOLD;
import static com.openexchange.cluster.ReaderRegistry.Status.STRANDED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Retention contract v0.1 (R03), pinned where it can hurt: the purge boundary is a pure function of
 * COMMITTED facts and cluster time, it never moves backwards, it holds for anyone it has not heard
 * from, and it gives a consumer up only by an explicit rule or an explicit decision. Every input here
 * stands for a message that arrived through the replicated log; nothing reads a wall clock.
 */
public class ReaderRegistryTest {

    private static final long EPOCH = 7_001L;
    private static final long GIB = 1L << 30;
    private static final long MIN = 60_000L;
    private static final long T0 = 1_700_000_000_000L;
    private static final int[] MEMBERS = {0, 1, 2};

    private static ReaderRegistry registry() {
        final ReaderRegistry r = new ReaderRegistry(EPOCH, MEMBERS);
        r.configure(2 * GIB, 30 * MIN, 1 * GIB, false);
        return r;
    }

    private static void observeAll(final ReaderRegistry r, final long nowMs, final long... positions) {
        final boolean[] fresh = new boolean[positions.length];
        java.util.Arrays.fill(fresh, true);
        r.observeMembers(nowMs, MEMBERS, positions, fresh);
    }

    // ---- members: cold leader, unobserved, commit-only authority, monotone ----

    @Test
    public void coldLeaderHoldsTheBoundaryAtZero() {
        final ReaderRegistry r = registry();
        final ReaderRegistry.Decision d = r.decide(10_000L, 50_000L, T0);
        assertEquals(0L, d.boundary);
        assertTrue(d.limiter, d.limiter.startsWith("member"));
        assertEquals(OPEN, d.gate);
    }

    @Test
    public void aMemberNeverObservedHoldsEvenWhenTheOthersAreCurrent() {
        final ReaderRegistry r = registry();
        r.observeMembers(T0, new int[] {0, 1}, new long[] {9_000L, 9_500L}, new boolean[] {true, true});
        assertEquals(0L, r.decide(10_000L, 50_000L, T0).boundary);
        observeAll(r, T0 + 1, 9_000L, 9_500L, 8_000L);
        assertEquals(8_000L, r.decide(10_000L, 50_000L, T0 + 1).boundary);
    }

    @Test
    public void theBoundaryNeverMovesBackwards() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 9_000L, 9_500L, 8_000L);
        assertEquals(8_000L, r.decide(10_000L, 50_000L, T0).boundary);
        // An older, lower observation (an old leader's late message) cannot pull it back.
        observeAll(r, T0 + 5, 9_000L, 9_500L, 7_000L);
        assertEquals(8_000L, r.decide(10_000L, 50_000L, T0 + 5).boundary);
    }

    @Test
    public void aStaleMemberObservationHoldsAtItsLastCommittedPosition() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 9_000L, 9_500L, 8_000L);
        r.observeMembers(T0 + 1_000, MEMBERS, new long[] {9_900L, 9_900L, 8_000L}, new boolean[] {true, true, false});
        assertEquals(8_000L, r.decide(20_000L, 50_000L, T0 + 1_000).boundary);
    }

    @Test
    public void ownRecoveryFloorCapsTheBoundary() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 90_000L, 90_000L, 90_000L);
        final ReaderRegistry.Decision d = r.decide(10_000L, 100_000L, T0);
        assertEquals(10_000L, d.boundary);
        assertEquals("floorOwn", d.limiter);
    }

    // ---- readers: unknown, epoch, future, backwards, lease, strand ----

    @Test
    public void anUnregisteredReaderDoesNotParticipate() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        assertEquals(UNKNOWN_READER, r.checkpoint("ghost", EPOCH, 0L, 1_000L, 0L, 60_000L, T0));
        final ReaderRegistry.Decision d = r.decide(60_000L, 60_000L, T0);
        assertEquals(50_000L, d.boundary);
        assertTrue(d.holds.isEmpty());
    }

    @Test
    public void aWrongEpochIsRejectedAndChangesNothing() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        assertEquals(WRONG_EPOCH, r.register("bridge", BRIDGE, EPOCH + 1, 10 * MIN, T0));
        assertEquals(NONE, r.register("bridge", BRIDGE, EPOCH, 10 * MIN, T0));
        assertEquals(NONE, r.checkpoint("bridge", EPOCH, 0L, 20_000L, 100L, 60_000L, T0));
        assertEquals(WRONG_EPOCH, r.checkpoint("bridge", EPOCH + 1, 0L, 40_000L, 200L, 60_000L, T0 + 1));
        assertEquals(20_000L, r.decide(60_000L, 60_000L, T0 + 1).boundary);
        assertEquals(2, r.rejectCount(WRONG_EPOCH)); // the register and the checkpoint above
    }

    @Test
    public void aFutureCheckpointIsRejectedAndTheReaderStaysPut() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        r.register("proj", EXEC_PROJECTOR, EPOCH, 10 * MIN, T0);
        assertEquals(NONE, r.checkpoint("proj", EPOCH, 0L, 20_000L, 0L, 60_000L, T0));
        assertEquals(FUTURE_POSITION, r.checkpoint("proj", EPOCH, 0L, 70_000L, 0L, 60_000L, T0 + 1));
        assertEquals(20_000L, r.decide(60_000L, 60_000L, T0 + 1).boundary);
    }

    @Test
    public void aBackwardsCheckpointIsRejected() {
        final ReaderRegistry r = registry();
        r.register("proj", EXEC_PROJECTOR, EPOCH, 10 * MIN, T0);
        assertEquals(NONE, r.checkpoint("proj", EPOCH, 0L, 20_000L, 0L, 60_000L, T0));
        assertEquals(BACKWARDS, r.checkpoint("proj", EPOCH, 0L, 19_000L, 0L, 60_000L, T0 + 1));
        assertEquals(20_000L, r.reader("proj").position);
    }

    @Test
    public void aRegisteredReaderThatWentQuietHoldsItIsNotIgnored() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        r.register("bridge", BRIDGE, EPOCH, 10 * MIN, T0);
        r.checkpoint("bridge", EPOCH, 0L, 20_000L, 0L, 60_000L, T0);
        ReaderRegistry.Decision d = r.decide(60_000L, 60_000L, T0 + 11 * MIN);
        assertEquals(HOLD, r.reader("bridge").status);
        assertEquals(20_000L, d.boundary);
        assertEquals("bridge", d.limiter);
        assertTrue(d.holds.contains("bridge"));
        // Distinct from never registered: the same quiet with no registration frees the boundary.
        final ReaderRegistry none = registry();
        observeAll(none, T0, 50_000L, 50_000L, 50_000L);
        d = none.decide(60_000L, 60_000L, T0 + 11 * MIN);
        assertEquals(50_000L, d.boundary);
        assertFalse(d.holds.contains("bridge"));
    }

    @Test
    public void aLeadershipChangeCannotShortenAHold() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        r.register("bridge", BRIDGE, EPOCH, 10 * MIN, T0);
        r.checkpoint("bridge", EPOCH, 0L, 20_000L, 0L, 60_000L, T0);
        // HOLD begins at lease expiry (cluster time); a new leader deciding later sees the same clock.
        r.decide(60_000L, 60_000L, T0 + 11 * MIN);
        assertEquals(HOLD, r.reader("bridge").status);
        // Lag is beyond the byte bound but the time bound (30 min of HOLD) has not elapsed: still HOLD.
        r.decide(60_000L, 20_000L + 3 * GIB, T0 + 11 * MIN + 29 * MIN);
        assertEquals(HOLD, r.reader("bridge").status);
        r.decide(60_000L, 20_000L + 3 * GIB, T0 + 11 * MIN + 31 * MIN);
        assertEquals(STRANDED, r.reader("bridge").status);
    }

    @Test
    public void strandingNeedsBothHalvesOfTheBoundOrAnAdmin() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        r.register("backup", CLUSTER_BACKUP, EPOCH, MIN, T0);
        r.checkpoint("backup", EPOCH, 0L, 1_000L, 0L, 60_000L, T0);
        // Time bound reached, byte bound not: still holds.
        r.decide(60_000L, 60_000L, T0 + 2 * MIN + 31 * MIN);
        assertEquals(HOLD, r.reader("backup").status);
        assertEquals(1_000L, r.decide(60_000L, 60_000L, T0 + 2 * MIN + 31 * MIN).boundary);
        // Admin decision strands at once, boundary moves on, reader is reported.
        assertEquals(NONE, r.strandReader("backup", "operator: backup host decommissioned", T0 + 40 * MIN));
        final ReaderRegistry.Decision d = r.decide(60_000L, 60_000L, T0 + 40 * MIN);
        assertEquals(STRANDED, r.reader("backup").status);
        assertEquals(50_000L, d.boundary);
        assertTrue(d.stranded.contains("backup"));
    }

    @Test
    public void unregisterReleasesTheHold() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        r.register("hft", ReaderRegistry.Kind.HFT_GATEWAY, EPOCH, MIN, T0);
        r.checkpoint("hft", EPOCH, 0L, 5_000L, 0L, 60_000L, T0);
        assertEquals(5_000L, r.decide(60_000L, 60_000L, T0).boundary);
        r.unregister("hft");
        assertEquals(50_000L, r.decide(60_000L, 60_000L, T0 + 1).boundary);
    }

    @Test
    public void anOlderRecordingHoldsTheWholeChainThroughTheMapper() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 500_000L, 500_000L, 500_000L);
        r.register("proj", EXEC_PROJECTOR, EPOCH, 10 * MIN, T0);
        // Chain position = recordingId * 100_000 + position (a stand-in for the producer's own mapping).
        r.positionMapper((recordingId, position) -> recordingId * 100_000L + position);
        assertEquals(NONE, r.checkpoint("proj", EPOCH, 1L, 40_000L, 0L, 600_000L, T0));
        assertEquals(140_000L, r.decide(600_000L, 600_000L, T0).boundary);
        // A checkpoint that goes to an older recording is backwards even if its raw position is higher.
        assertEquals(BACKWARDS, r.checkpoint("proj", EPOCH, 0L, 90_000L, 0L, 600_000L, T0 + 1));
    }

    // ---- budget: stop admission, never delete ----

    @Test
    public void aBlownBudgetClosesTheGateAndLeavesTheBoundaryAlone() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        r.register("bridge", BRIDGE, EPOCH, 10 * MIN, T0);
        r.checkpoint("bridge", EPOCH, 0L, 1_000L, 0L, 60_000L, T0);
        final ReaderRegistry.Decision d = r.decide(2 * GIB, 2 * GIB, T0 + 1);
        assertEquals(1_000L, d.boundary);
        assertEquals(CLOSED, d.gate);
        assertTrue(d.retained > GIB);
    }

    // ---- snapshot: state survives, replay converges ----

    @Test
    public void snapshotRoundTripAndReplayGiveTheSameDecision() {
        final ReaderRegistry r = registry();
        observeAll(r, T0, 50_000L, 50_000L, 50_000L);
        r.register("bridge", BRIDGE, EPOCH, 10 * MIN, T0);
        r.checkpoint("bridge", EPOCH, 0L, 20_000L, 7L, 60_000L, T0);
        r.register("proj", EXEC_PROJECTOR, EPOCH, 10 * MIN, T0);
        r.checkpoint("proj", EPOCH, 0L, 30_000L, 9L, 60_000L, T0);
        r.strandMember(2, "test", T0);
        r.decide(60_000L, 60_000L, T0 + 11 * MIN); // bridge -> HOLD, proj -> HOLD

        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int len = r.serialize(buf, 0);
        final ReaderRegistry restored = new ReaderRegistry(EPOCH, MEMBERS);
        assertEquals(len, restored.deserialize(buf, 0));
        final ReaderRegistry.Decision a = r.decide(60_000L, 60_000L, T0 + 12 * MIN);
        final ReaderRegistry.Decision b = restored.decide(60_000L, 60_000L, T0 + 12 * MIN);
        assertEquals(a.boundary, b.boundary);
        assertEquals(a.limiter, b.limiter);
        assertEquals(a.holds, b.holds);
        assertEquals(a.stranded, b.stranded);
        assertEquals(a.gate, b.gate);

        // Restart from an OLDER snapshot (empty registry) and replay the same committed inputs: same answer.
        final ReaderRegistry replayed = new ReaderRegistry(EPOCH, MEMBERS);
        replayed.configure(2 * GIB, 30 * MIN, 1 * GIB, false);
        observeAll(replayed, T0, 50_000L, 50_000L, 50_000L);
        replayed.register("bridge", BRIDGE, EPOCH, 10 * MIN, T0);
        replayed.checkpoint("bridge", EPOCH, 0L, 20_000L, 7L, 60_000L, T0);
        replayed.register("proj", EXEC_PROJECTOR, EPOCH, 10 * MIN, T0);
        replayed.checkpoint("proj", EPOCH, 0L, 30_000L, 9L, 60_000L, T0);
        replayed.strandMember(2, "test", T0);
        replayed.decide(60_000L, 60_000L, T0 + 11 * MIN);
        final ReaderRegistry.Decision c = replayed.decide(60_000L, 60_000L, T0 + 12 * MIN);
        assertEquals(a.boundary, c.boundary);
        assertEquals(a.holds, c.holds);
        // The serialized bytes are a function of state, not of history.
        final ExpandableArrayBuffer buf2 = new ExpandableArrayBuffer();
        final int len2 = replayed.serialize(buf2, 0);
        assertEquals(len, len2);
        for (int i = 0; i < len; i++) {
            assertEquals("byte " + i, buf.getByte(i), buf2.getByte(i));
        }
    }

    @Test
    public void enforceIsReplicatedConfigAndOffByDefault() {
        final ReaderRegistry r = new ReaderRegistry(EPOCH, MEMBERS);
        assertFalse(r.enforce());
        r.configure(GIB, MIN, GIB, true);
        assertTrue(r.enforce());
        assertEquals(ACTIVE, ACTIVE); // placeholder to keep the enum import honest
    }
}
