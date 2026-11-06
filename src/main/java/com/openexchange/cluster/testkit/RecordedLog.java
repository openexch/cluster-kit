// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.ClusterLogReader;
import com.openexchange.cluster.log.FrameKind;
import io.aeron.Image;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * An immutable capture of what a {@link ClusterLogReader} delivered (payloads copied), or a synthetic log built with
 * {@link #builder()}. Frames are in log order with strictly increasing frame-end positions. MembershipChange frames are
 * never delivered, so they never appear here.
 */
public final class RecordedLog {

    private static final int FRAME_ALIGNMENT = 32;
    private static final int DATA_HEADER = 32;
    /** Empty polls before a capture gives up: 100 000 x 0.1 ms park = about 10 s. */
    private static final int EMPTY_POLL_BUDGET = 100_000;
    private static final long EMPTY_POLL_PARK_NS = 100_000;

    private final long startPosition;
    private final long endPosition;
    private final List<LogFrame> frames;

    private RecordedLog(final long startPosition, final long endPosition, final List<LogFrame> frames) {
        long previous = startPosition;
        for (final LogFrame frame : frames) {
            if (frame.position() <= previous) {
                throw new IllegalArgumentException("frame positions must increase: " + frame + " after " + previous);
            }
            previous = frame.position();
        }
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.frames = Collections.unmodifiableList(new ArrayList<>(frames));
    }

    /** Wraps frames captured elsewhere. */
    public static RecordedLog of(final long startPosition, final List<LogFrame> frames) {
        return new RecordedLog(startPosition, frames.isEmpty() ? startPosition
                : frames.get(frames.size() - 1).position(), frames);
    }

    /**
     * Reads {@code image} from {@code from} (where it must start) up to {@code to} with a fresh reader and keeps every
     * delivered entry. Ends when the image has consumed {@code to}, or at end of stream.
     *
     * @throws com.openexchange.cluster.log.ClusterLogReaderStoppedException if the reader stops
     */
    public static RecordedLog capture(final Image image, final long from, final long to) {
        final Capture capture = new Capture();
        final ClusterLogReader reader = new ClusterLogReader(capture, 64, from);
        int emptyPolls = 0;
        while (reader.consumedPosition() < to) {
            final int fragments = reader.poll(image, to);
            if (fragments > 0) {
                emptyPolls = 0;
                continue;
            }
            if (image.isEndOfStream() || image.isClosed()) {
                break;
            }
            if (++emptyPolls > EMPTY_POLL_BUDGET) {
                throw new IllegalStateException("capture made no progress at " + reader.consumedPosition()
                        + " (target " + to + ")");
            }
            java.util.concurrent.locks.LockSupport.parkNanos(EMPTY_POLL_PARK_NS);
        }
        return new RecordedLog(from, reader.position(), capture.frames);
    }

    public static Builder builder() {
        return new Builder();
    }

    public long startPosition() {
        return startPosition;
    }

    /** End of the last delivered message (the reader's {@code position()} at the end of the capture). */
    public long endPosition() {
        return endPosition;
    }

    public List<LogFrame> frames() {
        return frames;
    }

    public int size() {
        return frames.size();
    }

    /** Frame-end positions in order. */
    public long[] positions() {
        final long[] positions = new long[frames.size()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = frames.get(i).position();
        }
        return positions;
    }

    /**
     * Delivers every frame with {@code afterExclusive < position <= throughInclusive}, in order. A listener returning
     * true from {@code onClusterAction} does not end a synthetic replay (there is no poll to end).
     */
    public int replay(final long afterExclusive, final long throughInclusive, final ClusterLogListener listener) {
        int delivered = 0;
        for (final LogFrame frame : frames) {
            if (frame.position() > afterExclusive && frame.position() <= throughInclusive) {
                frame.deliver(listener);
                delivered++;
            }
        }
        return delivered;
    }

    public int count(final FrameKind kind) {
        int n = 0;
        for (final LogFrame frame : frames) {
            if (frame.kind() == kind) {
                n++;
            }
        }
        return n;
    }

    /** A synthetic log: frame ends computed as an unfragmented Aeron frame of the encoded length would have them. */
    public static final class Builder {
        private final List<LogFrame> frames = new ArrayList<>();
        private long position;
        private long term = -1;

        private Builder() {
        }

        /** The current end position (where the next frame starts). */
        public long position() {
            return position;
        }

        /** A term event whose term base is where it starts, leader 0, log session 1, milliseconds. */
        public Builder term(final long leadershipTermId, final long timestamp, final int appVersion) {
            return term(leadershipTermId, timestamp, position, 0, 1, TimeUnit.MILLISECONDS, appVersion);
        }

        public Builder term(final long leadershipTermId, final long timestamp, final long termBaseLogPosition,
                            final int leaderMemberId, final int logSessionId, final TimeUnit timeUnit,
                            final int appVersion) {
            term = leadershipTermId;
            frames.add(LogFrame.term(leadershipTermId, advance(8 + 48), timestamp, termBaseLogPosition,
                    leaderMemberId, logSessionId, timeUnit, appVersion));
            return this;
        }

        public Builder open(final long clusterSessionId, final long timestamp, final byte[] encodedPrincipal) {
            return open(clusterSessionId, timestamp, 102, "aeron:udp?endpoint=localhost:0", encodedPrincipal);
        }

        public Builder open(final long clusterSessionId, final long timestamp, final int responseStreamId,
                            final String responseChannel, final byte[] encodedPrincipal) {
            final int length = 8 + 36 + 4 + responseChannel.getBytes(StandardCharsets.US_ASCII).length + 4
                    + encodedPrincipal.length;
            frames.add(LogFrame.open(term, advance(length), clusterSessionId, timestamp, responseStreamId,
                    responseChannel, encodedPrincipal));
            return this;
        }

        public Builder close(final long clusterSessionId, final long timestamp, final CloseReason closeReason) {
            frames.add(LogFrame.close(term, advance(8 + 28), clusterSessionId, timestamp, closeReason));
            return this;
        }

        public Builder message(final long clusterSessionId, final long timestamp, final byte[] payload) {
            frames.add(LogFrame.message(advance(32 + payload.length), clusterSessionId, timestamp, payload));
            return this;
        }

        public Builder message(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                               final int offset, final int length) {
            final byte[] payload = new byte[length];
            buffer.getBytes(offset, payload);
            return message(clusterSessionId, timestamp, payload);
        }

        public Builder timer(final long correlationId, final long timestamp) {
            frames.add(LogFrame.timer(advance(8 + 24), correlationId, timestamp));
            return this;
        }

        public Builder action(final ClusterAction action, final long timestamp, final int flags) {
            frames.add(LogFrame.action(term, advance(8 + 32), timestamp, Objects.requireNonNull(action), flags));
            return this;
        }

        public RecordedLog build() {
            return new RecordedLog(0, position, frames);
        }

        private long advance(final int messageLength) {
            position += align(DATA_HEADER + messageLength);
            return position;
        }

        private static int align(final int length) {
            return (length + FRAME_ALIGNMENT - 1) & -FRAME_ALIGNMENT;
        }
    }

    /** Copies every delivered entry. */
    private static final class Capture implements ClusterLogListener {
        private final List<LogFrame> frames = new ArrayList<>();

        @Override
        public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                     final DirectBuffer buffer, final int offset, final int length) {
            final byte[] payload = new byte[length];
            buffer.getBytes(offset, payload);
            frames.add(LogFrame.message(logPosition, clusterSessionId, timestamp, payload));
        }

        @Override
        public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
            frames.add(LogFrame.timer(logPosition, correlationId, timestamp));
        }

        @Override
        public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                  final long timestamp, final int responseStreamId, final String responseChannel,
                                  final byte[] encodedPrincipal) {
            frames.add(LogFrame.open(leadershipTermId, logPosition, clusterSessionId, timestamp, responseStreamId,
                    responseChannel, encodedPrincipal));
        }

        @Override
        public void onSessionClose(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                   final long timestamp, final CloseReason closeReason) {
            frames.add(LogFrame.close(leadershipTermId, logPosition, clusterSessionId, timestamp, closeReason));
        }

        @Override
        public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                       final ClusterAction action, final int flags) {
            frames.add(LogFrame.action(leadershipTermId, logPosition, timestamp, action, flags));
            return false;
        }

        @Override
        public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                             final long termBaseLogPosition, final int leaderMemberId,
                                             final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
            frames.add(LogFrame.term(leadershipTermId, logPosition, timestamp, termBaseLogPosition, leaderMemberId,
                    logSessionId, timeUnit, appVersion));
        }

        /** A capture sits behind the reader, never behind a gate: nothing to record. */
        @Override
        public void onSkipped(final FrameKind kind, final long logPosition, final long timestamp) {
        }
    }
}
