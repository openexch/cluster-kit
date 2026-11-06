// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.SnapshotImageReader;
import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * A bounded, session-scoped replay of part of a shadow recording (RCS S3 8.1). The replay stream
 * ({@link ShadowAlias#REPLAY_STREAM_ID}) is shared by every replay the process opens, so the subscription carries the
 * replay's session id and only that image is ever polled (review FIX-2). Messages are reassembled
 * ({@link FragmentAssembler}) and read to end of stream with {@link SnapshotImageReader#readFully} behind an idle
 * strategy that throws {@link ReplayTimeoutException} past the deadline (NOTE-4). {@code stopReplay} and the
 * subscription close run in {@code finally}. The replay channel is {@link ShadowAlias#REPLAY_CHANNEL}
 * ({@code linger=0|sparse=true}, 6.6). One owner thread per instance; replays are serial per instance.
 *
 * <p>An {@link ArchiveException} from {@code startReplay} propagates with its code (the catalog maps
 * {@code INVALID_POSITION} on a trailer window to NO_TRAILER). Never launches a driver or reads a clock other than
 * the injected one.</p>
 */
public final class ShadowReplay {

    /** The replay did not reach end of stream before the deadline. */
    public static final class ReplayTimeoutException extends RuntimeException {
        ReplayTimeoutException(final String message) {
            super(message);
        }
    }

    /** The replay image did not start at {@code from}: unknown, retried, never evidence of corruption. */
    public static final class ReplayJoinedLateException extends RuntimeException {
        ReplayJoinedLateException(final String message) {
            super(message);
        }
    }

    /** The replay reached end of stream before {@code from + length}: unknown, retried, never a verdict. */
    public static final class ReplayEndedEarlyException extends RuntimeException {
        ReplayEndedEarlyException(final String message) {
            super(message);
        }
    }

    /** Package seam: sees each replay image when it is acquired (the /dev/shm budget test samples there). */
    interface Observer {
        Observer NONE = (recordingId, from, length) -> { };

        void onReplayOpen(long recordingId, long from, long length);

        /** After {@code startReplay}, before the subscription is added (a test widens the late-join window). */
        default void beforeSubscribe(long replaySessionId) {
        }

        /** NC-21 only: true switches the join-position check off. */
        default boolean joinCheckOff() {
            return false;
        }

        /** NC-22 only (review FIX-1): true switches the end-position check off. */
        default boolean endCheckOff() {
            return false;
        }
    }

    /** What one replay delivered. */
    public record Result(long bytes, int messages, long position) {
    }

    private static final BooleanSupplier NEVER = () -> false;

    private final String channel;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final NanoClock clock;
    private final boolean sessionScoped;
    private final Observer observer;

    public ShadowReplay(final Aeron aeron, final AeronArchive archive, final NanoClock clock) {
        this(aeron, archive, clock, true, Observer.NONE);
    }

    /** {@code sessionScoped=false} only for the NC-18 negative control (8.1 step 2 switched off). */
    ShadowReplay(final Aeron aeron, final AeronArchive archive, final NanoClock clock, final boolean sessionScoped,
                 final Observer observer) {
        this(aeron, archive, clock, sessionScoped, observer, ShadowAlias.REPLAY_CHANNEL);
    }

    /** {@code channel} other than {@link ShadowAlias#REPLAY_CHANNEL} only for the 11.7 (char) control. */
    ShadowReplay(final Aeron aeron, final AeronArchive archive, final NanoClock clock, final boolean sessionScoped,
                 final Observer observer, final String channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.archive = Objects.requireNonNull(archive, "archive");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sessionScoped = sessionScoped;
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    public AeronArchive archive() {
        return archive;
    }

    public NanoClock clock() {
        return clock;
    }

    /** The whole recording {@code [start, stop)}. */
    public Result readAll(final long recordingId, final long startPosition, final long stopPosition,
                          final FragmentHandler handler, final long deadlineNs) {
        return readRange(recordingId, startPosition, stopPosition - startPosition, handler, NEVER, deadlineNs);
    }

    /** {@link #readAll} that ends early once {@code stopEarly} answers true (a deep read that already failed). */
    public Result readAll(final long recordingId, final long startPosition, final long stopPosition,
                          final FragmentHandler handler, final BooleanSupplier stopEarly, final long deadlineNs) {
        return readRange(recordingId, startPosition, stopPosition - startPosition, handler, stopEarly, deadlineNs);
    }

    /** {@link #readRange(long, long, long, FragmentHandler, BooleanSupplier, long)} to end of stream. */
    public Result readRange(final long recordingId, final long from, final long length, final FragmentHandler handler,
                            final long deadlineNs) {
        return readRange(recordingId, from, length, handler, NEVER, deadlineNs);
    }

    /**
     * Replays {@code [from, from + length)} and hands every whole message to {@code handler}. A handler must not
     * throw: Aeron hands a fragment-handler exception to the client's subscriber error handler, which may swallow it;
     * a handler that wants to stop sets the state {@code stopEarly} reads.
     *
     * @param deadlineNs an absolute value of the injected clock
     * @throws ArchiveException        the archive refused the replay (code kept)
     * @throws ReplayTimeoutException  past the deadline
     * @throws IllegalStateException   the image closed before end of stream
     * @throws ReplayJoinedLateException  the image did not start at {@code from}
     * @throws ReplayEndedEarlyException  end of stream before {@code from + length}, or the publication revoked
     */
    public Result readRange(final long recordingId, final long from, final long length, final FragmentHandler handler,
                            final BooleanSupplier stopEarly, final long deadlineNs) {
        final long replaySessionId = archive.startReplay(recordingId, from, length, channel,
                ShadowAlias.REPLAY_STREAM_ID);
        final int sessionId = (int)replaySessionId;
        Subscription subscription = null;
        try {
            observer.beforeSubscribe(replaySessionId);
            subscription = aeron.addSubscription(sessionScoped ? ChannelUri.addSessionId(channel, sessionId) : channel,
                    ShadowAlias.REPLAY_STREAM_ID);
            final IdleStrategy idle = new DeadlineIdle(clock, deadlineNs, recordingId, from, length);
            Image image;
            while ((image = subscription.imageBySessionId(sessionId)) == null) {
                idle.idle(0);
            }
            if (image.joinPosition() != from && !observer.joinCheckOff()) {
                // another subscriber on the replay stream connected the replay publication before this
                // subscription joined, so the image starts late: never read it (a late image would look corrupt)
                throw new ReplayJoinedLateException("replay of recording " + recordingId + " joined at "
                        + image.joinPosition() + ", expected " + from);
            }
            observer.onReplayOpen(recordingId, from, length);
            final int[] messages = {0};
            final long[] bytes = {0};
            final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, len, header) -> {
                messages[0]++;
                bytes[0] += len;
                handler.onFragment(buffer, offset, len, header);
            });
            final SnapshotImageReader.Source source = sessionScoped
                    ? scoped(image, stopEarly) : unscoped(subscription, image);
            final SnapshotImageReader.Result r = SnapshotImageReader.readFully(source, assembler, idle);
            // review FIX-1: a clean end of stream is not a complete replay. The archive closes or revokes the replay
            // publication on every failure path (segment read error, stopReplay/stopAllReplays from any client), so
            // the reader also sees end of stream after a PARTIAL range; that is unknown, never CORRUPT or NO_TRAILER
            if (!stopEarly.getAsBoolean() && sessionScoped && !observer.endCheckOff()) {
                final boolean revoked = image.isPublicationRevoked();
                if (revoked || (length >= 0 && image.position() != from + length)) {
                    throw new ReplayEndedEarlyException("replay of recording " + recordingId + " ended at "
                            + image.position() + ", expected " + (length >= 0 ? from + length : "end of stream")
                            + (revoked ? " (publication revoked)" : ""));
                }
            }
            return new Result(bytes[0], messages[0], r.position());
        } finally {
            try {
                archive.stopReplay(replaySessionId);
            } catch (final RuntimeException ignore) {
                // the session already ended (the archive answers OK for an unknown session); a closed or lost
                // archive client lands here, and the caller already has the primary failure
            }
            CloseHelper.quietClose(subscription);
        }
    }

    private static SnapshotImageReader.Source scoped(final Image image, final BooleanSupplier stopEarly) {
        return new SnapshotImageReader.Source() {
            public int poll(final FragmentHandler handler, final int fragmentLimit) {
                return stopEarly.getAsBoolean() ? 0 : image.poll(handler, fragmentLimit);
            }

            public boolean isEndOfStream() {
                return image.isEndOfStream() || stopEarly.getAsBoolean();
            }

            public boolean isClosed() {
                return image.isClosed();
            }

            public long position() {
                return image.position();
            }
        };
    }

    /** NC-18: polls every image of a bare subscription; ends with the replay's own image. */
    private static SnapshotImageReader.Source unscoped(final Subscription subscription, final Image own) {
        return new SnapshotImageReader.Source() {
            public int poll(final FragmentHandler handler, final int fragmentLimit) {
                return subscription.poll(handler, fragmentLimit);
            }

            public boolean isEndOfStream() {
                return own.isEndOfStream();
            }

            public boolean isClosed() {
                return own.isClosed();
            }

            public long position() {
                return own.position();
            }
        };
    }

    /** Backs off; throws {@link ReplayTimeoutException} once the injected clock passes the deadline. */
    static final class DeadlineIdle implements IdleStrategy {
        private final BackoffIdleStrategy backoff = new BackoffIdleStrategy(10, 20, 1_000, 1_000_000);
        private final NanoClock clock;
        private final long deadlineNs;
        private final long recordingId;
        private final long from;
        private final long length;

        DeadlineIdle(final NanoClock clock, final long deadlineNs, final long recordingId, final long from,
                     final long length) {
            this.clock = clock;
            this.deadlineNs = deadlineNs;
            this.recordingId = recordingId;
            this.from = from;
            this.length = length;
        }

        private void check() {
            if (clock.nanoTime() - deadlineNs > 0) {
                throw new ReplayTimeoutException("REPLAY_TIMEOUT recording " + recordingId + " [" + from + ", +"
                        + length + ")");
            }
        }

        @Override
        public void idle(final int workCount) {
            check();
            backoff.idle(workCount);
        }

        @Override
        public void idle() {
            check();
            backoff.idle();
        }

        @Override
        public void reset() {
            backoff.reset();
        }
    }
}
