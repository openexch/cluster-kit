// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowImageBuffer;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowWriter;
import com.openexchange.cluster.shadow.ShadowWriterThread;
import io.aeron.archive.client.ArchiveException;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Which shadow the replica writes, and when.
 *
 * <pre>
 * ClusterActionRequest(SNAPSHOT, 0)  ALIGNED  the listener returns true (the poll ends at the frame); serialize now; a
 *                                             busy writer BLOCKS the reader until idle or REPLICA_ALIGNED_WAIT_MS,
 *                                             and the deadline fences (a wedged writer never stalls quietly)
 * ClusterActionRequest(SNAPSHOT, 1)  STANDBY  serialize now; busy: skipped, shadow_trigger_skipped_busy_total{STANDBY}
 * no action frame for SHADOW_STANDBY_FALLBACK_MINUTES of CLUSTER time (frame timestamps)
 *                                    LOCAL    serialize at the end of the poll that crossed the deadline; busy: skipped;
 *                                             an idle log never fires; shadow_trigger_fallback_total
 * </pre>
 *
 * <p>The trigger only records what is due: the follower calls {@link #producePending} after {@code poll} returned
 * normally, so no serialize ever runs after a failed apply. P_s is {@code container.cursor()} right then: for ALIGNED and STANDBY the trigger ended the poll at its frame, so the cursor is that frame's end. The
 * identity is frozen in the request at the same instant ({@code Request.of}). Before the engine holds a genesis (a
 * genesis start before the ADOPT frame) a trigger is counted {@code shadow_trigger_unidentified_total} and nothing is serialized.</p>
 *
 * <p>Write results ({@link #onResult}, on the writer thread): OK stamps the kind's last position; SELF_CHECK_FAILED or
 * SELF_CHECK_MUTATED_BUFFER is CRITICAL and fences (a non-round-trippable image would poison every member);
 * ARCHIVE_REFUSED(STORAGE_SPACE) marks the disk full for readiness at once; any other failure is
 * counted by the writer and the next trigger retries.</p>
 *
 * <p>With an {@link AlignmentTrust}: an ALIGNED image is held in the buffer until the member
 * snapshot at the same position compares byte-equal ({@link #pollHeld}); only then is it offered. While the replica
 * is UNVERIFIED (a continuation that is not a full replay from genesis) STANDBY and LOCAL are not written
 * ({@code shadow_trigger_untrusted_total}); while an image is held they skip as busy. The test seam
 * {@code beforeOffer} runs after the compare, right before the hand-off.</p>
 *
 * <p>Clocks: the injected {@link NanoClock} decides the ALIGNED wait deadline and the verify wait only. LOCAL is
 * decided on cluster time from the frames; the {@code LOCAL_ON_WALL_CLOCK} negative control is the one path that reads the clock for it.</p>
 */
public final class ShadowTriggerPolicy implements ShadowTriggerListener, ShadowWriterThread.ShadowWriteListener {

    /** The writer thread as the policy sees it (a fake in the pure tests). */
    public interface WriterPort {
        boolean isIdle();

        boolean isRunning();

        boolean offer(ShadowWriterThread.Request request);
    }

    /** {@code container::serialize}: the engine image at index 0 of {@code dst}, its length returned. */
    @FunctionalInterface
    public interface Serializer {
        int serialize(MutableDirectBuffer dst);
    }

    /** Latches the replica's fence; must be safe from any thread. */
    @FunctionalInterface
    public interface Fence {
        void fence(String reason);
    }

    public static WriterPort port(final ShadowWriterThread thread) {
        Objects.requireNonNull(thread, "thread");
        return new WriterPort() {
            @Override
            public boolean isIdle() {
                return thread.isIdle();
            }

            @Override
            public boolean isRunning() {
                return thread.isRunning();
            }

            @Override
            public boolean offer(final ShadowWriterThread.Request request) {
                return thread.offer(request);
            }
        };
    }

    private static final int KINDS = ShadowKind.values().length;

    private final ShadowImageBuffer image;
    private final int engineId;
    private final Supplier<LogCursor> cursor;
    private final ShadowIdentitySource identity;
    private final Serializer serializer;
    private final NanoClock clock;
    private final IdleStrategy idle;
    private final long alignedWaitNs;
    private final long fallbackMs;
    private final int bufferMb;
    private final Fence fence;
    private final ReplicaFaults faults;
    private final Set<ReplicaNegativeControl> off;

    private volatile WriterPort writer;
    private volatile AlignmentTrust trust;
    private volatile BooleanSupplier fencedProbe = () -> false;
    private volatile boolean fencedFlag;
    private volatile long afterFenceDroppedTotal;
    /** The ALIGNED request whose image sits in {@link #image} until its member compare. */
    private volatile ShadowWriterThread.Request held;
    private long heldSinceNs;
    private String lastCompareFailure;
    private volatile long untrustedTotal;
    private volatile long verifyTimeouts;
    private ShadowKind pending;
    private long pendingAt = -1;
    private long lastActionClusterTime = -1;
    private long lastWallActionMs = -1;

    private final AtomicLongArray triggers = new AtomicLongArray(KINDS);
    private final AtomicLongArray skippedBusy = new AtomicLongArray(KINDS);
    private final AtomicLongArray written = new AtomicLongArray(KINDS);
    private final AtomicLongArray lastPosition = new AtomicLongArray(KINDS);
    private volatile long fallbackTotal;
    private volatile long unidentifiedTotal;
    private volatile long serializeMsLast;
    private volatile long alignedBlockedMsLast;
    private volatile long lastWriteCompletedNs = -1;
    private volatile long lastWrittenPosition = -1;
    private volatile String diskFull;
    private volatile long failedResults;

    /**
     * @param alignedWaitNs {@code REPLICA_ALIGNED_WAIT_MS}
     * @param fallbackMs    {@code SHADOW_STANDBY_FALLBACK_MINUTES} in ms of cluster time, 0 disables LOCAL
     * @param bufferMb      {@code REPLICA_SHADOW_BUFFER_MB}, for the overflow fence text
     */
    public ShadowTriggerPolicy(final ShadowImageBuffer image, final int engineId, final Supplier<LogCursor> cursor,
                               final ShadowIdentitySource identity, final Serializer serializer, final NanoClock clock,
                               final IdleStrategy idle, final long alignedWaitNs, final long fallbackMs,
                               final int bufferMb, final Fence fence, final ReplicaFaults faults,
                               final Set<ReplicaNegativeControl> off) {
        this.image = Objects.requireNonNull(image, "image");
        this.engineId = engineId;
        this.cursor = Objects.requireNonNull(cursor, "cursor");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.idle = Objects.requireNonNull(idle, "idle");
        if (alignedWaitNs <= 0 || fallbackMs < 0) {
            throw new IllegalArgumentException("alignedWaitNs must be > 0 and fallbackMs >= 0");
        }
        this.alignedWaitNs = alignedWaitNs;
        this.fallbackMs = fallbackMs;
        this.bufferMb = bufferMb;
        this.fence = Objects.requireNonNull(fence, "fence");
        this.faults = Objects.requireNonNull(faults, "faults");
        this.off = off.isEmpty() ? EnumSet.noneOf(ReplicaNegativeControl.class) : EnumSet.copyOf(off);
        for (int i = 0; i < KINDS; i++) {
            lastPosition.set(i, -1);
        }
    }

    /**
     * The replica's fence latch (the follower's {@code fenced != null}), read first on every produce
     * branch so nothing reaches the writer after a fence latched on any thread.
     */
    public void fencedProbe(final BooleanSupplier probe) {
        this.fencedProbe = Objects.requireNonNull(probe, "probe");
    }

    /** The writer thread, armed after bootstrap (it needs the startup scan's producer sequence). */
    public void attachWriter(final WriterPort writer) {
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    /**
     * The cluster-time origin of the LOCAL fallback: the restored cursor's time, or the first applied frame's at
     * genesis (then {@code clusterTime} is -1 and the first {@link #observeFrames} sets it).
     */
    public void startAt(final LogCursor from) {
        lastActionClusterTime = from.clusterTime();
        lastWallActionMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime());
    }

    // --------------------------------------------------------------------------------------------- triggers

    @Override
    public boolean onSnapshotAction(final long logPosition, final long leadershipTermId, final long timestamp,
                                    final int flags) {
        final ShadowKind kind = flags == 0 ? ShadowKind.ALIGNED : ShadowKind.STANDBY;
        lastActionClusterTime = timestamp;
        lastWallActionMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime());
        triggers.incrementAndGet(kind.ordinal());
        if (kind == ShadowKind.STANDBY && off.contains(ReplicaNegativeControl.TRIGGER_IGNORES_STANDBY)) {
            return false;
        }
        if (pending == ShadowKind.ALIGNED && kind != ShadowKind.ALIGNED) {
            return true; // an ALIGNED is owed already; never downgrade it
        }
        pending = kind;
        pendingAt = logPosition;
        return true;
    }

    /**
     * After a poll that applied frames: the LOCAL fallback on cluster time. The cursor is the last applied frame.
     */
    public void observeFrames(final LogCursor at) {
        if (at.clusterTime() < 0) {
            return;
        }
        if (lastActionClusterTime < 0) {
            lastActionClusterTime = at.clusterTime();
            return;
        }
        if (off.contains(ReplicaNegativeControl.LOCAL_ON_WALL_CLOCK)) {
            return; // the control decides in idleTick, on the wall clock
        }
        if (fallbackMs > 0 && pending == null && at.clusterTime() - lastActionClusterTime >= fallbackMs) {
            fireLocal(at.logPosition(), at.clusterTime());
        }
    }

    /** Called by the follower on every loop pass, frames or not; fires only under the LOCAL_ON_WALL_CLOCK control. */
    public void idleTick() {
        if (!off.contains(ReplicaNegativeControl.LOCAL_ON_WALL_CLOCK) || fallbackMs <= 0 || pending != null
                || lastWallActionMs < 0) {
            return;
        }
        final long nowMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime());
        if (nowMs - lastWallActionMs >= fallbackMs) {
            lastWallActionMs = nowMs;
            fireLocal(cursor.get().logPosition(), lastActionClusterTime);
        }
    }

    private void fireLocal(final long position, final long clusterTime) {
        triggers.incrementAndGet(ShadowKind.LOCAL.ordinal());
        fallbackTotal++;
        lastActionClusterTime = clusterTime;
        pending = ShadowKind.LOCAL;
        pendingAt = position;
    }

    /**
     * The alignment trust. Without one (the pure policy tests) the policy writes every ALIGNED at once, the rule
     * before the alignment trust.
     */
    public void alignmentTrust(final AlignmentTrust trust) {
        this.trust = Objects.requireNonNull(trust, "trust");
    }

    /**
     * Once per follower loop pass, after any {@link #producePending}: the held ALIGNED image's
     * compare with the retrieved member snapshot (EQUAL: written; DIFFERENT: fence; UNVERIFIABLE: dropped; PENDING or
     * AMBIGUOUS: kept, a VERIFIED replica drops it after {@code REPLICA_VERIFY_WAIT_MS}), and while UNVERIFIED the
     * trust's own checks: another history ({@link AlignmentTrust#tick}) and the stall bound
     * ({@link AlignmentTrust#stall}). After a fence nothing is compared or offered.
     */
    public void pollHeld(final long appliedPosition) {
        final AlignmentTrust t = trust;
        if (t == null || t.compareOff()) {
            return;
        }
        if (fenced()) {
            if (held != null) {
                held = null;
                droppedAfterFence("the held ALIGNED image");
            }
            return;
        }
        if (held == null) {
            final String text = t.tick(appliedPosition);
            if (text != null) {
                fenceNow(text);
                return;
            }
        } else {
            resolveHeld(t, appliedPosition, false);
        }
        if (fenced() || t.trusted()) {
            return;
        }
        // the stall bound (held or not)
        final ShadowWriterThread.Request h = held;
        final String text = t.stall(appliedPosition, h == null ? -1 : h.at().logPosition());
        if (text != null) {
            held = null;
            fenceNow(text);
        }
    }

    /** One compare of the held image; true when the buffer is free again (written, dropped or fenced). */
    private boolean resolveHeld(final AlignmentTrust t, final long appliedPosition, final boolean now) {
        final ShadowWriterThread.Request req = held;
        final long p = req.at().logPosition();
        final AlignmentTrust.Verdict v;
        try {
            v = t.compare(p, req.at().leadershipTermId(), req.identity().genesisId(), image.payloadView(),
                    image.payloadLength(), now);
        } catch (final RuntimeException unreadable) {
            final String text = "the retrieved member snapshot at " + p + " did not read: " + unreadable;
            if (!text.equals(lastCompareFailure)) {
                lastCompareFailure = text;
                System.err.println("[REPLICA] CRITICAL: " + text + " (retried until REPLICA_VERIFY_WAIT_MS)");
            }
            return pendingDeadline(t, req, appliedPosition);
        }
        switch (v) {
            case EQUAL -> {
                held = null;
                if (fenced()) {
                    droppedAfterFence("ALIGNED at " + p + " (compared EQUAL)");
                    return true;
                }
                faults.beforeOffer(image, req.at());
                final WriterPort w = writer;
                if (w == null || !w.offer(req)) {
                    skippedBusy.incrementAndGet(ShadowKind.ALIGNED.ordinal());
                    fenceNow("FATAL replica: ALIGNED shadow at " + p + " refused by an idle writer thread after"
                            + " its member compare");
                }
                return true;
            }
            case DIFFERENT -> {
                held = null;
                fenceNow(AlignmentTrust.differsText(p));
                return true;
            }
            case UNVERIFIABLE -> {
                held = null;
                return true;
            }
            default -> { // PENDING, and AMBIGUOUS (never compared, not ready)
                return pendingDeadline(t, req, appliedPosition);
            }
        }
    }

    /**
     * A held image still waits. UNVERIFIED: the stall clock in {@link #pollHeld} bounds it (false, kept). VERIFIED:
     * dropped after {@code REPLICA_VERIFY_WAIT_MS} since it was held.
     */
    private boolean pendingDeadline(final AlignmentTrust t, final ShadowWriterThread.Request req,
                                    final long appliedPosition) {
        if (!t.trusted()) {
            return false;
        }
        final long nowNs = clock.nanoTime();
        if (nowNs - heldSinceNs < t.verifyWaitNs()) {
            return false;
        }
        final long p = req.at().logPosition();
        held = null;
        t.heldDropped(p);
        verifyTimeouts++;
        System.err.println("[REPLICA] CRITICAL: no member snapshot at " + p + " retrieved within"
                + " REPLICA_VERIFY_WAIT_MS=" + TimeUnit.NANOSECONDS.toMillis(t.verifyWaitNs()) + ": that ALIGNED"
                + " shadow is not written; the next ALIGNED is compared");
        return true;
    }

    /** {@code shadow_trigger_untrusted_total}: STANDBY and LOCAL triggers not written while UNVERIFIED. */
    public long untrustedTotal() {
        return untrustedTotal;
    }

    /** Whether an ALIGNED image waits for its member compare. */
    public boolean holding() {
        return held != null;
    }

    /** {@code shadow_align_verify_timeouts_total}: VERIFIED replicas' ALIGNED images dropped at the verify wait. */
    public long verifyTimeouts() {
        return verifyTimeouts;
    }

    public boolean hasPending() {
        return pending != null;
    }

    public ShadowKind pendingKind() {
        return pending;
    }

    // --------------------------------------------------------------------------------------------- produce

    /**
     * Serialize and hand off what is due. Called by the follower after {@code poll} returned normally, before the
     * next poll.
     *
     * @return the kind handed to the writer, or null (nothing due, skipped, unidentified, or fenced)
     */
    public ShadowKind producePending() {
        final ShadowKind kind = pending;
        if (kind == null) {
            return null;
        }
        pending = null;
        final LogCursor at = cursor.get();
        if (fenced()) {
            // the fence is checked first on every branch (STANDBY, LOCAL and ALIGNED alike)
            droppedAfterFence(kind + " trigger at " + at.logPosition());
            return null;
        }
        final WriterPort w = writer;
        if (w == null || identity.genesisId() <= 0) {
            unidentifiedTotal++;
            System.out.println("[REPLICA] " + kind + " trigger at " + at.logPosition()
                    + " before the engine holds a genesis (before ADOPT): nothing written");
            return null;
        }
        if (!w.isRunning()) {
            fenceNow("FATAL replica: shadow writer thread is not running (" + kind + " at " + at.logPosition()
                    + ")");
            return null;
        }
        final AlignmentTrust t = trust;
        final boolean comparing = t != null && !t.compareOff();
        if (comparing && kind != ShadowKind.ALIGNED) {
            if (held != null) {
                skippedBusy.incrementAndGet(kind.ordinal()); // the buffer holds an ALIGNED image awaiting its compare
                return null;
            }
            if (!t.trusted()) {
                untrustedTotal++;
                System.out.println("[REPLICA] " + kind + " trigger at " + at.logPosition() + " while UNVERIFIED: not"
                        + " written (waiting for the first ALIGNED compare)");
                return null;
            }
        }
        if (comparing && held != null) {
            // a new ALIGNED: one last compare of the held one, else it is superseded
            final long heldAt = held.at().logPosition();
            resolveHeld(t, at.logPosition(), true);
            if (fenced()) {
                return null;
            }
            if (held != null) {
                held = null;
                t.superseded(heldAt);
            }
        }
        if (!w.isIdle()) {
            if (kind != ShadowKind.ALIGNED) {
                skippedBusy.incrementAndGet(kind.ordinal());
                return null;
            }
            final long start = clock.nanoTime();
            final long deadline = start + alignedWaitNs;
            idle.reset();
            while (!w.isIdle()) {
                if (clock.nanoTime() - deadline >= 0) {
                    alignedBlockedMsLast = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - start);
                    fenceNow("FATAL replica: ALIGNED shadow at " + at.logPosition() + ": writer busy for "
                            + alignedBlockedMsLast + " ms (REPLICA_ALIGNED_WAIT_MS="
                            + TimeUnit.NANOSECONDS.toMillis(alignedWaitNs) + ")");
                    return null;
                }
                if (!w.isRunning()) {
                    fenceNow("FATAL replica: shadow writer thread ended while ALIGNED at " + at.logPosition()
                            + " waited");
                    return null;
                }
                idle.idle();
            }
            alignedBlockedMsLast = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - start);
            if (fenced()) {
                droppedAfterFence(kind + " trigger at " + at.logPosition() + " (fenced while it waited)");
                return null;
            }
        }
        // identity frozen at the serialize instant, on this thread (SWT:28-32)
        final ShadowWriterThread.Request request = ShadowWriterThread.Request.of(image, engineId, kind, at, identity);
        final long t0 = clock.nanoTime();
        final int n;
        try {
            n = serializer.serialize(image.payloadView());
        } catch (final RuntimeException | Error failed) {
            // the container wraps every engine failure (ReplayStoppedException, RC:115-118): the overflow is a cause
            final IndexOutOfBoundsException overflow = overflowIn(failed);
            if (overflow != null) {
                fenceNow("FATAL replica: image exceeds REPLICA_SHADOW_BUFFER_MB=" + bufferMb + " (payload capacity "
                        + image.payloadCapacity() + " B) at " + at.logPosition() + ": " + overflow);
            } else {
                fenceNow("FATAL replica: serialize failed at " + at.logPosition() + ": " + failed);
            }
            return null;
        }
        serializeMsLast = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - t0);
        if (n > image.payloadCapacity() || n <= 0) {
            fenceNow("FATAL replica: image " + n + " B exceeds REPLICA_SHADOW_BUFFER_MB=" + bufferMb
                    + " (payload capacity " + image.payloadCapacity() + " B) at " + at.logPosition());
            return null;
        }
        image.setPayloadLength(n);
        if (comparing && kind == ShadowKind.ALIGNED) {
            // written only after the compare with the member snapshot at the same position (pollHeld)
            held = request;
            heldSinceNs = clock.nanoTime();
            lastCompareFailure = null;
            t.held(at.logPosition());
            resolveHeld(t, at.logPosition(), true);
            return kind;
        }
        faults.beforeOffer(image, at);
        if (fenced()) {
            droppedAfterFence(kind + " shadow at " + at.logPosition() + " (fenced during its serialize)");
            return null;
        }
        if (!w.offer(request)) {
            skippedBusy.incrementAndGet(kind.ordinal());
            if (kind == ShadowKind.ALIGNED) {
                fenceNow("FATAL replica: ALIGNED shadow at " + at.logPosition()
                        + " refused by an idle writer thread");
            }
            return null;
        }
        return kind;
    }

    /** This policy fenced, or the replica's fence latch is set. */
    private boolean fenced() {
        return fencedFlag || fencedProbe.getAsBoolean();
    }

    /** A pending trigger or a held image dropped because the replica is fenced; counted, never offered. */
    private void droppedAfterFence(final String what) {
        afterFenceDroppedTotal++;
        System.err.println("[REPLICA] " + what + " not handed to the shadow writer: the replica is fenced");
    }

    /** {@code shadow_trigger_after_fence_total}: triggers and held images dropped after a fence. */
    public long afterFenceDroppedTotal() {
        return afterFenceDroppedTotal;
    }

    private void fenceNow(final String text) {
        fencedFlag = true;
        fence.fence(text);
    }

    /** The buffer overflow in {@code t}'s cause chain (bounded: a cycle cannot loop), or null. */
    static IndexOutOfBoundsException overflowIn(final Throwable t) {
        Throwable c = t;
        for (int depth = 0; c != null && depth < 16; depth++) {
            if (c instanceof IndexOutOfBoundsException overflow) {
                return overflow;
            }
            c = c.getCause();
        }
        return null;
    }

    // --------------------------------------------------------------------------------------------- results

    @Override
    public void onResult(final ShadowWriterThread.Request request, final ShadowWriter.WriteResult result) {
        final ShadowKind kind = request.kind();
        if (result.ok()) {
            written.incrementAndGet(kind.ordinal());
            lastPosition.set(kind.ordinal(), request.at().logPosition());
            lastWrittenPosition = request.at().logPosition();
            lastWriteCompletedNs = clock.nanoTime();
            diskFull = null;
            return;
        }
        failedResults++;
        if (result.reason() == ShadowWriter.Reason.ABORTED
                && ShadowWriterThread.FENCED_BEFORE_WRITE.equals(result.detail())) {
            afterFenceDroppedTotal++;
            System.err.println("[REPLICA] " + kind + " shadow at " + request.at().logPosition() + " not written: the"
                    + " replica fenced before the writer thread started it");
            return;
        }
        switch (result.reason()) {
            case SELF_CHECK_FAILED, SELF_CHECK_MUTATED_BUFFER -> {
                System.err.println("[REPLICA] CRITICAL: " + kind + " shadow at " + request.at().logPosition() + " "
                        + result + ": a non-round-trippable image is never offered");
                fenceNow("FATAL replica: " + result.reason() + " at " + request.at().logPosition() + ": "
                        + result.detail());
            }
            case ARCHIVE_REFUSED -> {
                if (result.archiveCode() == ArchiveException.STORAGE_SPACE) {
                    diskFull = "disk full: ARCHIVE_REFUSED(STORAGE_SPACE) at " + request.at().logPosition();
                    System.err.println("[REPLICA] CRITICAL: " + diskFull);
                } else {
                    System.err.println("[REPLICA] shadow write refused: " + result);
                }
            }
            case UNIDENTIFIED, NOT_MONOTONE -> System.out.println("[REPLICA] " + kind + " shadow at "
                    + request.at().logPosition() + " not written: " + result);
            default -> System.err.println("[REPLICA] " + kind + " shadow at " + request.at().logPosition()
                    + " failed: " + result + " (the next trigger retries)");
        }
    }

    // --------------------------------------------------------------------------------------------- metrics

    public long triggerTotal(final ShadowKind kind) {
        return triggers.get(kind.ordinal());
    }

    public long skippedBusyTotal(final ShadowKind kind) {
        return skippedBusy.get(kind.ordinal());
    }

    public long writtenTotal(final ShadowKind kind) {
        return written.get(kind.ordinal());
    }

    /** -1 before the first OK write of {@code kind}. */
    public long lastPosition(final ShadowKind kind) {
        return lastPosition.get(kind.ordinal());
    }

    public long fallbackTotal() {
        return fallbackTotal;
    }

    public long unidentifiedTotal() {
        return unidentifiedTotal;
    }

    public long serializeMsLast() {
        return serializeMsLast;
    }

    public long alignedBlockedMsLast() {
        return alignedBlockedMsLast;
    }

    /** -1 before the first OK write in this process. */
    public long lastWriteCompletedNs() {
        return lastWriteCompletedNs;
    }

    public long lastWrittenPosition() {
        return lastWrittenPosition;
    }

    public long failedResults() {
        return failedResults;
    }

    /** Non-null while the last write was refused for disk space (cleared by the next OK write). */
    public String diskFull() {
        return diskFull;
    }
}
