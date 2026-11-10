// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.log.LogCursor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * One daemon thread ({@code shadow-writer-<producer>}) with one slot in front of a {@link ShadowWriter}.
 * {@link #offer} never blocks: it returns false while a write is in flight or the slot is taken
 * ({@code shadow_write_skipped_busy_total} +1). The listener is called on the writer thread with the request (and so
 * the host's buffer) and the result. The busy flag has exactly one reset path: the end of a write, after the listener
 * returned. {@link #isIdle} lets a host block its own reader on an ALIGNED trigger (R 2.5); the kit never blocks the
 * caller. {@link #close} aborts the in-flight write (W7 cleanup: no open publication is left) and joins.
 */
public final class ShadowWriterThread implements AutoCloseable {

    /**
     * One write. The identity values are copied when the request is built ({@link #of}), on the apply thread, at the
     * same instant as the serialize: the writer thread never reads the live engine.
     */
    public record Request(ShadowImageBuffer image, int engineId, ShadowKind kind, LogCursor at,
                          ShadowIdentitySource identity) {

        public static Request of(final ShadowImageBuffer image, final int engineId, final ShadowKind kind,
                                 final LogCursor at, final ShadowIdentitySource live) {
            return new Request(image, engineId, kind, at, new Snapshot(live.genesisId(), live.journalEnabled(),
                    live.journalSeq(), live.payloadLayoutHint()));
        }
    }

    /** Identity values frozen at one instant. */
    public record Snapshot(long genesisId, boolean journalEnabled, long journalSeq, int payloadLayoutHint)
            implements ShadowIdentitySource {
    }

    @FunctionalInterface
    public interface ShadowWriteListener {
        void onResult(Request request, ShadowWriter.WriteResult result);
    }

    private final ShadowWriter writer;
    private final ShadowWriteListener listener;
    private final org.agrona.ErrorHandler errorHandler;
    private final ShadowCounters counters;
    private final boolean guardOff;
    private final AtomicBoolean busy = new AtomicBoolean();
    private final AtomicReference<Request> slot = new AtomicReference<>();
    private final Thread thread;
    private volatile boolean running = true;
    /** The host's fence latch, read before each write starts; never set: no latch. */
    private volatile java.util.function.BooleanSupplier fenceLatch = () -> false;

    /**
     * @param errorHandler the host's: receives every {@code Throwable} from a write or the listener;
     *                     an {@code Error} also ends the thread, so the host's halt path sees it
     */
    public ShadowWriterThread(final ShadowWriter writer, final ShadowWriteListener listener,
                              final org.agrona.ErrorHandler errorHandler, final ShadowCounters counters) {
        this(writer, listener, errorHandler, counters, false);
    }

    /** {@code guardOff=true} only for the writer-thread negative control (the thread before the error guard). */
    ShadowWriterThread(final ShadowWriter writer, final ShadowWriteListener listener,
                       final org.agrona.ErrorHandler errorHandler, final ShadowCounters counters,
                       final boolean guardOff) {
        this.writer = Objects.requireNonNull(writer, "writer");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.guardOff = guardOff;
        final ShadowProducer p = writer.producer();
        this.thread = new Thread(this::run, "shadow-writer-" + p.kind().letter() + p.producerId());
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * @return false (and counted) when a write is in flight or the slot is taken, after {@link #close}, or after the
     *         thread ended on an {@code Error}
     */
    public boolean offer(final Request request) {
        Objects.requireNonNull(request, "request");
        if (!running || !busy.compareAndSet(false, true)) {
            counters.increment(ShadowCounters.Counter.WRITE_SKIPPED_BUSY_TOTAL);
            return false;
        }
        slot.set(request);
        LockSupport.unpark(thread);
        if (!running) {
            // the thread ended between the check and the hand-off: take the request back, never accept-and-lose
            if (slot.compareAndSet(request, null)) {
                busy.set(false);
                counters.increment(ShadowCounters.Counter.WRITE_SKIPPED_BUSY_TOTAL);
                return false;
            }
        }
        return true;
    }

    /**
     * The host's fence latch. The thread reads it after it takes a request and before the
     * write starts; a latched fence hands the request back {@code ABORTED(ABORT_REQUESTED)} unwritten, so a fence that
     * lands between the host's own check and its {@link #offer} still stops that write.
     */
    public void fenceLatch(final java.util.function.BooleanSupplier latch) {
        this.fenceLatch = Objects.requireNonNull(latch, "latch");
    }

    /** No write in flight and the slot is free: the host's buffer is back. */
    public boolean isIdle() {
        return !busy.get();
    }

    /** False after {@link #close} or after the thread ended on an {@code Error}. */
    public boolean isRunning() {
        return running && thread.isAlive();
    }

    public Thread thread() {
        return thread;
    }

    private void report(final Throwable t) {
        counters.increment(ShadowCounters.Counter.WRITER_THREAD_ERRORS_TOTAL);
        try {
            errorHandler.onError(t);
        } catch (final Throwable ignore) {
            // the host's handler failed; the counter still moved
        }
    }

    private static ShadowWriter.WriteResult unexpected(final Throwable t) {
        return new ShadowWriter.WriteResult(ShadowWriter.Reason.UNEXPECTED, t.toString(), null, -1, -1, null, 0, 0, 0,
                new ShadowWriter.Purge(ShadowWriter.Purge.Kind.NOT_ATTEMPTED, -1, null));
    }

    /** The request handed back unwritten because the host's fence latched before the write started. */
    static ShadowWriter.WriteResult fencedBeforeWrite() {
        return new ShadowWriter.WriteResult(ShadowWriter.Reason.ABORTED, FENCED_BEFORE_WRITE,
                ShadowWriter.AbortCause.ABORT_REQUESTED, -1, -1, null, 0, 0, 0,
                new ShadowWriter.Purge(ShadowWriter.Purge.Kind.NOT_ATTEMPTED, -1, null));
    }

    /** The detail of {@link #fencedBeforeWrite}. */
    public static final String FENCED_BEFORE_WRITE = "fenced before the write started";

    private void run() {
        if (guardOff) {
            runUnguarded();
            return;
        }
        try {
            while (running) {
                final Request r = slot.getAndSet(null);
                if (r == null) {
                    LockSupport.park(this);
                    continue;
                }
                ShadowWriter.WriteResult result;
                Error fatal = null;
                try {
                    // A fence latched after the host's own check and before this write starts stops it here
                    result = fenceLatch.getAsBoolean() ? fencedBeforeWrite()
                            : writer.write(r.image(), r.engineId(), r.kind(), r.at(), r.identity());
                } catch (final RuntimeException e) {
                    report(e);
                    result = unexpected(e);
                } catch (final Error e) {
                    report(e);
                    result = unexpected(e);
                    fatal = e;
                }
                try {
                    listener.onResult(r, result);
                } catch (final Error e) {
                    report(e);
                    fatal = fatal == null ? e : fatal;
                } catch (final RuntimeException e) {
                    report(e); // a listener bug never ends the thread
                } finally {
                    busy.set(false);
                }
                if (fatal != null) {
                    return; // an Error ends the thread; running is cleared below, the host halts on its handler
                }
            }
        } finally {
            running = false;
            handBackLeft();
        }
    }

    /**
     * Negative control: the thread as it was before the error guard (listener unguarded, an Error ends it with
     * running still true).
     */
    private void runUnguarded() {
        while (running) {
            final Request r = slot.getAndSet(null);
            if (r == null) {
                LockSupport.park(this);
                continue;
            }
            ShadowWriter.WriteResult result = null;
            try {
                result = writer.write(r.image(), r.engineId(), r.kind(), r.at(), r.identity());
            } catch (final RuntimeException e) {
                result = unexpected(e);
            } finally {
                try {
                    if (result != null) {
                        listener.onResult(r, result);
                    }
                } finally {
                    busy.set(false);
                }
            }
        }
    }

    /** Closed (or ended) with a request still in the slot: hand the buffer back, never written. */
    private void handBackLeft() {
        final Request left = slot.getAndSet(null);
        if (left != null) {
            try {
                listener.onResult(left, new ShadowWriter.WriteResult(ShadowWriter.Reason.ABORTED, "writer thread closed",
                        ShadowWriter.AbortCause.ABORT_REQUESTED, -1, -1, null, 0, 0, 0,
                        new ShadowWriter.Purge(ShadowWriter.Purge.Kind.NOT_ATTEMPTED, -1, null)));
            } catch (final Throwable t) {
                report(t);
            } finally {
                busy.set(false);
            }
        }
    }

    /**
     * Aborts the in-flight write ({@code ABORTED(ABORT_REQUESTED)} through the W7 cleanup) and joins up to
     * {@code timeoutMs}. The abort is repeated while it joins, so a write that started just as close was called is
     * aborted too (an abort with no write in flight is a no-op).
     *
     * @return true when the thread ended within the timeout
     */
    public boolean close(final long timeoutMs) {
        running = false;
        LockSupport.unpark(thread);
        final long rounds = Math.max(1, timeoutMs / 10);
        try {
            for (long i = 0; i < rounds && thread.isAlive(); i++) {
                writer.requestAbort();
                LockSupport.unpark(thread);
                thread.join(10);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return !thread.isAlive();
    }

    @Override
    public void close() {
        close(ShadowWriter.DEFAULT_RECORD_TIMEOUT_NS / 1_000_000L);
    }
}
