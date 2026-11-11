// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.shadow.ShadowWriterThread;
import io.aeron.archive.client.ArchiveException;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The trigger policy (pure: a fake writer thread, a fake clock). ALIGNED blocks until idle and fences at the
 * deadline; STANDBY and LOCAL skip when busy; LOCAL fires on cluster time only.
 */
public class ShadowTriggerPolicyTest {

    static final long MINUTE_MS = 60_000;
    static final long FALLBACK_MS = 30 * MINUTE_MS;

    /** A writer thread stand-in: busy until the fake clock reaches {@code idleAtNs}. */
    final class FakeWriter implements ShadowTriggerPolicy.WriterPort {
        long idleAtNs = Long.MIN_VALUE;
        boolean running = true;
        final List<ShadowWriterThread.Request> offered = new ArrayList<>();

        @Override
        public boolean isIdle() {
            return now[0] >= idleAtNs;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean offer(final ShadowWriterThread.Request request) {
            offered.add(request);
            return true;
        }
    }

    final long[] now = {1_000_000_000L};
    final FakeWriter writer = new FakeWriter();
    final List<String> fences = new ArrayList<>();
    /** The family of each fence, in the order of {@link #fences}. */
    final List<FenceFamily> families = new ArrayList<>();
    /** Called after each recorded fence (a test wires the replica's fence latch here), or null. */
    ShadowTriggerPolicy.Fence onFence;
    LogCursor cursor = new LogCursor(4096, 1_000, 1, 0);
    long genesis = 7;
    int serializeCalls;
    int serializeLength = 256;

    /** Each idle step advances the fake clock by 1 ms. */
    final IdleStrategy idle = new IdleStrategy() {
        @Override
        public void idle(final int workCount) {
            idle();
        }

        @Override
        public void idle() {
            now[0] += TimeUnit.MILLISECONDS.toNanos(1);
        }

        @Override
        public void reset() {
        }
    };

    ShadowTriggerPolicy policy(final Set<ReplicaNegativeControl> off) {
        final ShadowImageBuffer image = new ShadowImageBuffer(new UnsafeBuffer(new byte[ShadowHeader.LENGTH + 1024]));
        final ShadowTriggerPolicy p = new ShadowTriggerPolicy(image, ShadowHeader.ENGINE_TEST, () -> cursor,
                new ShadowIdentitySource() {
                    @Override
                    public long genesisId() {
                        return genesis;
                    }

                    @Override
                    public boolean journalEnabled() {
                        return false;
                    }

                    @Override
                    public long journalSeq() {
                        return 0;
                    }

                    @Override
                    public int payloadLayoutHint() {
                        return 0;
                    }
                }, dst -> {
                    serializeCalls++;
                    if (serializeLength > dst.capacity()) {
                        throw new IndexOutOfBoundsException("index=" + serializeLength + " capacity=" + dst.capacity());
                    }
                    // an enveloped image of zero payload bytes, as an engine seals it (the compare opens it first)
                    dst.setMemory(0, serializeLength, (byte)0);
                    return new com.openexchange.cluster.SnapshotEnvelope().seal(dst,
                            serializeLength - com.openexchange.cluster.SnapshotEnvelope.envelopedLength(0));
                }, () -> now[0], idle, TimeUnit.MILLISECONDS.toNanos(1_000), FALLBACK_MS, 16,
                new ShadowTriggerPolicy.Fence() {
                    @Override
                    public void fence(final String reason) {
                        fence(FenceFamily.FAULT, reason);
                    }

                    @Override
                    public void fence(final FenceFamily family, final String reason) {
                        families.add(family);
                        fences.add(reason);
                        if (onFence != null) {
                            onFence.fence(family, reason);
                        }
                    }
                }, ReplicaFaults.NONE, off);
        p.startAt(cursor);
        p.attachWriter(writer);
        return p;
    }

    /**
     * What the fake serializer writes ({@code length} enveloped bytes of a zero payload); {@code differ} flips one
     * payload byte and seals again (a same-length copy whose envelope opens and whose bytes differ).
     */
    static UnsafeBuffer enveloped(final int length, final boolean differ) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[length]);
        final int payload = length - com.openexchange.cluster.SnapshotEnvelope.envelopedLength(0);
        if (differ) {
            b.putByte(com.openexchange.cluster.SnapshotEnvelope.payloadOffset() + payload / 2, (byte)1);
        }
        new com.openexchange.cluster.SnapshotEnvelope().seal(b, payload);
        return b;
    }

    ShadowTriggerPolicy policy() {
        return policy(EnumSet.noneOf(ReplicaNegativeControl.class));
    }

    private void frame(final ShadowTriggerPolicy p, final long position, final long clusterTime) {
        cursor = new LogCursor(position, clusterTime, 1, 0);
        p.observeFrames(cursor);
    }

    // --------------------------------------------------------------------------------------------- ALIGNED

    @Test
    public void alignedBlocksUntilTheWriterIsIdle() {
        final ShadowTriggerPolicy p = policy();
        writer.idleAtNs = now[0] + TimeUnit.MILLISECONDS.toNanos(50);
        assertTrue("the ALIGNED trigger must end the poll", p.onSnapshotAction(8192, 1, 2_000, 0));
        cursor = new LogCursor(8192, 2_000, 1, 0);
        assertEquals(ShadowKind.ALIGNED, p.producePending());
        assertTrue("fenced while the writer freed in time: " + fences, fences.isEmpty());
        assertEquals(1, writer.offered.size());
        assertEquals(8192, writer.offered.get(0).at().logPosition());
        assertTrue("blocked " + p.alignedBlockedMsLast() + " ms", p.alignedBlockedMsLast() >= 50);
    }

    @Test
    public void alignedFencesAtTheDeadline() {
        final ShadowTriggerPolicy p = policy();
        writer.idleAtNs = Long.MAX_VALUE;
        p.onSnapshotAction(8192, 1, 2_000, 0);
        cursor = new LogCursor(8192, 2_000, 1, 0);
        assertNull(p.producePending());
        assertEquals(fences.toString(), 1, fences.size());
        assertTrue(fences.get(0), fences.get(0).startsWith("FATAL replica: ALIGNED shadow at 8192: writer busy for"));
        assertTrue(fences.get(0), fences.get(0).contains("REPLICA_ALIGNED_WAIT_MS=1000"));
        assertTrue("nothing offered after the deadline", writer.offered.isEmpty());
        assertEquals("no serialize while blocked", 0, serializeCalls);
    }

    @Test
    public void standbyNeverDowngradesAPendingAligned() {
        final ShadowTriggerPolicy p = policy();
        p.onSnapshotAction(8192, 1, 2_000, 0);
        p.onSnapshotAction(8256, 1, 2_000, 1);
        assertEquals(ShadowKind.ALIGNED, p.pendingKind());
    }

    // --------------------------------------------------------------------------------------------- busy skips

    @Test
    public void standbyAndLocalSkipWhenBusy() {
        final ShadowTriggerPolicy p = policy();
        writer.idleAtNs = Long.MAX_VALUE;
        p.onSnapshotAction(8192, 1, 2_000, 1);
        final long before = now[0];
        assertNull(p.producePending());
        assertEquals(1, p.skippedBusyTotal(ShadowKind.STANDBY));
        assertEquals("STANDBY blocked the reader", before, now[0]);
        frame(p, 9000, 2_000 + FALLBACK_MS);
        assertEquals(ShadowKind.LOCAL, p.pendingKind());
        assertNull(p.producePending());
        assertEquals(1, p.skippedBusyTotal(ShadowKind.LOCAL));
        assertTrue(writer.offered.isEmpty());
        assertTrue(fences.isEmpty());
        assertEquals(0, serializeCalls);
    }

    // --------------------------------------------------------------------------------------------- LOCAL

    static void assertNoFallbackOnIdleLog(final ShadowTriggerPolicy p, final FakeWriter w) {
        if (p.fallbackTotal() != 0 || !w.offered.isEmpty()) {
            throw new AssertionError("fallback fired with an idle log: fallback_total=" + p.fallbackTotal()
                    + " offered=" + w.offered.size());
        }
    }

    @Test
    public void localFiresOnClusterTimeOnly() {
        final ShadowTriggerPolicy p = policy();
        frame(p, 5000, 1_000 + FALLBACK_MS - 1);
        assertFalse("LOCAL before the cluster-time deadline", p.hasPending());
        frame(p, 6000, 1_000 + FALLBACK_MS);
        assertEquals(ShadowKind.LOCAL, p.pendingKind());
        assertEquals(ShadowKind.LOCAL, p.producePending());
        assertEquals(1, p.fallbackTotal());
        assertEquals(6000, writer.offered.get(0).at().logPosition());
        // an action frame restarts the deadline
        p.onSnapshotAction(7000, 1, 1_000 + 2 * FALLBACK_MS - 5, 1);
        p.producePending();
        frame(p, 7100, 1_000 + 2 * FALLBACK_MS + 10);
        assertFalse("an action frame did not restart the LOCAL deadline", p.hasPending());
    }

    private void idleLogForTenHours(final ShadowTriggerPolicy p) {
        frame(p, 5000, 1_000);
        for (int i = 0; i < 600; i++) {
            now[0] += TimeUnit.MINUTES.toNanos(1);
            p.idleTick();
            p.observeFrames(cursor); // the same last frame: the log is idle
            p.producePending();
        }
        assertNoFallbackOnIdleLog(p, writer);
    }

    @Test
    public void aWallClockJumpFiresNothing() {
        idleLogForTenHours(policy());
    }

    @Test
    public void negativeControlLocalOnWallClock() {
        try {
            idleLogForTenHours(policy(EnumSet.of(ReplicaNegativeControl.LOCAL_ON_WALL_CLOCK)));
            fail("NC LOCAL_ON_WALL_CLOCK: the idle-log assertion passed");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("fallback fired with an idle log"));
            System.out.println("NC LOCAL_ON_WALL_CLOCK -> " + expected.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------- STANDBY

    static void assertStandbyShadow(final ShadowTriggerPolicy p, final FakeWriter w, final long at) {
        for (final ShadowWriterThread.Request r : w.offered) {
            if (r.kind() == ShadowKind.STANDBY && r.at().logPosition() == at) {
                return;
            }
        }
        throw new AssertionError("no STANDBY shadow at " + at + " within the poll (offered " + w.offered.size() + ")");
    }

    private void standbyAction(final ShadowTriggerPolicy p) {
        p.onSnapshotAction(8192, 1, 2_000, 1);
        cursor = new LogCursor(8192, 2_000, 1, 0);
        p.producePending();
        assertStandbyShadow(p, writer, 8192);
    }

    @Test
    public void standbyIsWrittenAtTheActionFrameEnd() {
        standbyAction(policy());
    }

    @Test
    public void negativeControlTriggerIgnoresStandby() {
        try {
            standbyAction(policy(EnumSet.of(ReplicaNegativeControl.TRIGGER_IGNORES_STANDBY)));
            fail("NC TRIGGER_IGNORES_STANDBY: the STANDBY assertion passed");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("no STANDBY shadow at 8192"));
            System.out.println("NC TRIGGER_IGNORES_STANDBY -> " + expected.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------- refusals

    @Test
    public void beforeAdoptNothingIsSerialized() {
        genesis = 0;
        final ShadowTriggerPolicy p = policy();
        p.onSnapshotAction(8192, 1, 2_000, 0);
        assertNull(p.producePending());
        assertEquals(1, p.unidentifiedTotal());
        assertEquals("serialized before ADOPT", 0, serializeCalls);
        assertTrue(fences.isEmpty());
    }

    @Test
    public void anImageLargerThanTheBufferFences() {
        serializeLength = 4096; // the payload view holds 1024
        final ShadowTriggerPolicy p = policy();
        p.onSnapshotAction(8192, 1, 2_000, 0);
        assertNull(p.producePending());
        assertEquals(fences.toString(), 1, fences.size());
        assertTrue(fences.get(0), fences.get(0).startsWith("FATAL replica: image exceeds REPLICA_SHADOW_BUFFER_MB=16"));
        assertTrue(writer.offered.isEmpty());
    }

    /**
     * The production chain: {@code ReplayContainer.serialize} wraps every engine failure in
     * a {@code ReplayStoppedException} (RC:115-118), so the codec's buffer overflow reaches the policy as a cause. The
     * fence must still carry the buffer text (the operator's hint), not the generic serialize text.
     */
    @Test
    public void aWrappedOverflowFencesWithTheBufferText() {
        final ShadowImageBuffer image = new ShadowImageBuffer(new UnsafeBuffer(new byte[ShadowHeader.LENGTH + 1024]));
        final ShadowTriggerPolicy p = new ShadowTriggerPolicy(image, ShadowHeader.ENGINE_TEST, () -> cursor,
                new ShadowIdentitySource() {
                    @Override
                    public long genesisId() {
                        return genesis;
                    }

                    @Override
                    public boolean journalEnabled() {
                        return false;
                    }

                    @Override
                    public long journalSeq() {
                        return 0;
                    }

                    @Override
                    public int payloadLayoutHint() {
                        return 0;
                    }
                }, dst -> {
                    throw new com.openexchange.cluster.log.ReplayStoppedException(
                            com.openexchange.cluster.log.ReplayStoppedException.Reason.ENGINE_FAILED, "serialize",
                            new IndexOutOfBoundsException("index=4096 length=8 capacity=" + dst.capacity()));
                }, () -> now[0], idle, TimeUnit.MILLISECONDS.toNanos(1_000), FALLBACK_MS, 16, fences::add,
                ReplicaFaults.NONE, EnumSet.noneOf(ReplicaNegativeControl.class));
        p.startAt(cursor);
        p.attachWriter(writer);
        p.onSnapshotAction(8192, 1, 2_000, 0);
        assertNull(p.producePending());
        assertEquals(fences.toString(), 1, fences.size());
        assertTrue(fences.get(0), fences.get(0).startsWith("FATAL replica: image exceeds REPLICA_SHADOW_BUFFER_MB=16"));
        assertTrue(fences.get(0), fences.get(0).contains("(payload capacity 1024 B)"));
        assertTrue(writer.offered.isEmpty());
    }

    @Test
    public void aDeadWriterThreadFences() {
        writer.running = false;
        final ShadowTriggerPolicy p = policy();
        p.onSnapshotAction(8192, 1, 2_000, 1);
        p.producePending();
        assertEquals(fences.toString(), 1, fences.size());
        assertTrue(fences.get(0), fences.get(0).contains("shadow writer thread is not running"));
    }

    // --------------------------------------------------------------------------------------------- results

    private static ShadowWriter.WriteResult result(final ShadowWriter.Reason reason, final int code) {
        return new ShadowWriter.WriteResult(reason, "test", null, code, 9, "alias", 100, 1, 1,
                new ShadowWriter.Purge(ShadowWriter.Purge.Kind.NOT_ATTEMPTED, -1, null));
    }

    @Test
    public void writeResultsStampFenceAndMarkDiskFull() {
        final ShadowTriggerPolicy p = policy();
        p.onSnapshotAction(8192, 1, 2_000, 0);
        cursor = new LogCursor(8192, 2_000, 1, 0);
        p.producePending();
        final ShadowWriterThread.Request r = writer.offered.get(0);
        p.onResult(r, result(ShadowWriter.Reason.ARCHIVE_REFUSED, ArchiveException.STORAGE_SPACE));
        assertTrue("STORAGE_SPACE not surfaced: " + p.diskFull(),
                p.diskFull() != null && p.diskFull().startsWith("disk full"));
        assertTrue("STORAGE_SPACE must not fence (readiness carries it)", fences.isEmpty());
        p.onResult(r, result(ShadowWriter.Reason.OK, -1));
        assertNull(p.diskFull());
        assertEquals(8192, p.lastPosition(ShadowKind.ALIGNED));
        assertEquals(1, p.writtenTotal(ShadowKind.ALIGNED));
        assertTrue(p.lastWriteCompletedNs() > 0);
        p.onResult(r, result(ShadowWriter.Reason.SELF_CHECK_FAILED, -1));
        assertEquals(fences.toString(), 1, fences.size());
        assertTrue(fences.get(0), fences.get(0).startsWith("FATAL replica: SELF_CHECK_FAILED at 8192"));
        p.onResult(r, result(ShadowWriter.Reason.SELF_CHECK_MUTATED_BUFFER, -1));
        assertEquals(2, fences.size());
        assertTrue(fences.get(1), fences.get(1).startsWith("FATAL replica: SELF_CHECK_MUTATED_BUFFER at 8192"));
    }
    // ------------------------------------------------------------------------------------------ fence latch

    /**
     * A fence on the same pass as a pending trigger hands nothing to the writer, on every branch. The seams: the
     * writer thread's SELF_CHECK_FAILED result (the policy's own fence) and the follower's fence latch (the probe), each
     * arriving after the trigger and before {@code producePending}; and a held ALIGNED whose compare turns EQUAL after
     * the fence.
     */
    @Test
    public void aFenceOnTheSamePassAsAPendingTriggerHandsNothingToTheWriter() {
        final List<String> leaks = new ArrayList<>();
        for (final String kind : new String[] {"STANDBY", "LOCAL", "ALIGNED"}) {
            for (final boolean external : new boolean[] {false, true}) {
                final ShadowTriggerPolicyTest h = new ShadowTriggerPolicyTest();
                final boolean[] latched = {false};
                final ShadowTriggerPolicy p = h.policy();
                p.fencedProbe(() -> latched[0]);
                // one ALIGNED written first: the request the writer thread's result names
                p.onSnapshotAction(8192, 1, 2_000, 0);
                h.cursor = new LogCursor(8192, 2_000, 1, 0);
                p.producePending();
                final ShadowWriterThread.Request written = h.writer.offered.remove(0);
                switch (kind) {
                    case "STANDBY" -> p.onSnapshotAction(8704, 1, 2_100, 1);
                    case "LOCAL" -> h.frame(p, 8704, 2_000 + FALLBACK_MS);
                    default -> p.onSnapshotAction(8704, 1, 2_100, 0);
                }
                h.cursor = new LogCursor(8704, 2_100, 1, 0);
                assertTrue(kind + " pending", p.hasPending());
                if (external) {
                    latched[0] = true; // the follower latched a fence (an apply failure, the writer thread's error)
                } else {
                    p.onResult(written, result(ShadowWriter.Reason.SELF_CHECK_FAILED, -1));
                }
                final int serializeBefore = h.serializeCalls;
                p.producePending();
                if (!h.writer.offered.isEmpty() || h.serializeCalls != serializeBefore) {
                    leaks.add(kind + (external ? " (follower latch)" : " (writer result)") + ": offered "
                            + h.writer.offered.size() + ", serialize calls " + (h.serializeCalls - serializeBefore));
                }
            }
        }
        // a held ALIGNED whose member snapshot turns EQUAL after the fence
        final ShadowTriggerPolicyTest h = new ShadowTriggerPolicyTest();
        final boolean[] latched = {false};
        final java.util.Map<Long, org.agrona.DirectBuffer> retrieved = new java.util.HashMap<>();
        final AlignmentTrust trust = new AlignmentTrust(new AlignmentTrust.MemberSnapshots() {
            @Override
            public long[] positions() {
                return retrieved.keySet().stream().mapToLong(Long::longValue).toArray();
            }

            @Override
            public org.agrona.DirectBuffer payload(final long position) {
                return retrieved.get(position);
            }
        }, () -> h.now[0], 120_000, 1, 0, false);
        trust.start(true, 0);
        final ShadowTriggerPolicy p = h.policy();
        p.fencedProbe(() -> latched[0]);
        p.alignmentTrust(trust);
        p.onSnapshotAction(4096, 1, 1_000, 0);
        p.producePending();
        assertTrue("held", p.holding());
        latched[0] = true;
        retrieved.put(4096L, enveloped(h.serializeLength, false));
        h.now[0] += TimeUnit.SECONDS.toNanos(1);
        p.pollHeld(4096);
        if (!h.writer.offered.isEmpty()) {
            leaks.add("held ALIGNED compared EQUAL after the fence: offered " + h.writer.offered.size());
        }
        if (!leaks.isEmpty()) {
            throw new AssertionError("after a fence a shadow reached the writer: " + leaks);
        }
        System.out.println("GREEN nothing reaches the writer after a fence (STANDBY, LOCAL, ALIGNED, held)");
    }
}
