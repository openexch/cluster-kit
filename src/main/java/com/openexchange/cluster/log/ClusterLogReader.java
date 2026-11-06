// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.BufferBuilder;
import io.aeron.Image;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.ClusterActionRequestDecoder;
import io.aeron.cluster.codecs.ClusterTimeUnit;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.MembershipChangeEventDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventDecoder;
import io.aeron.cluster.codecs.SessionCloseEventDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionOpenEventDecoder;
import io.aeron.cluster.codecs.TimerEventDecoder;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.logbuffer.FrameDescriptor.BEGIN_FRAG_FLAG;
import static io.aeron.logbuffer.FrameDescriptor.END_FRAG_FLAG;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

/**
 * Reads a committed Aeron cluster log image the way a member's service container does, without an agent, a role, a
 * session map or a {@code Cluster}: reassembly is copied from {@code BoundedLogAdapter} (A0:BLA:62-108), decoding
 * from its {@code onMessage} (A0:BLA:136-257), contiguity across images from {@code ClusteredServiceAgent}
 * (A0:CSA:829-835, 863-873). Where Aeron would silently ignore something this reader cannot vouch for (an unknown
 * template, a header version other than 17, a short body, an action body whose position is not the frame end, a
 * continuation with nothing captured), it stops instead (fail closed).
 *
 * <p>Hard rules:</p>
 * <ol>
 *   <li>Nothing ever escapes the fragment handler, and the image never advances past a data frame that was not
 *       delivered: a fragment that ends after {@code maxPosition} is ABORTed (Image checks the bound only at the
 *       fragment start, A0:IMG:575-590), and any {@code RuntimeException} or {@code Error} inside the rules or the
 *       listener latches a stop and ABORTs (Image would otherwise publish a position past the failing fragment,
 *       A0:IMG:636-647); {@link #poll} throws {@link ClusterLogReaderStoppedException} afterwards. Padding past the
 *       bound is consumed by Image without a handler call, which is harmless.</li>
 *   <li>Positions are frame ends from the log and time is the frame timestamp; no clock, role, environment or
 *       session reaches the reader.</li>
 *   <li>Consecutive images of one log are joined only through {@link #successor}; a successor whose image does not
 *       start at {@code previous.consumedPosition()} stops {@code START_MISMATCH}.</li>
 * </ol>
 *
 * <p>Single thread. It does not implement {@code ControlledFragmentHandler}: {@link #poll} is the only entry point,
 * so the bound and the stop cannot be bypassed. Zero allocation per frame except SessionOpen (the response channel
 * {@code String} and the principal {@code byte[]}, as A0:BLA:189-191) and reassembly-buffer growth.</p>
 */
public final class ClusterLogReader {

    /** {@code AeronCluster.SESSION_HEADER_LENGTH}: the message header plus the CM's session header. */
    static final int SESSION_HEADER_LENGTH =
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH;
    private static final int OPEN_VAR_DATA_OFFSET =
            MessageHeaderDecoder.ENCODED_LENGTH + SessionOpenEventDecoder.BLOCK_LENGTH;
    private static final int VAR_DATA_LENGTH_FIELD = 4;
    private static final int NO_TEMPLATE = -1;

    private final ClusterLogListener listener;
    private final int fragmentLimit;
    private final long startPosition;
    private final BufferBuilder builder;
    private final long[] counts;
    private final FragmentHandler handler = new FragmentHandler();

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionHeader = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timerEvent = new TimerEventDecoder();
    private final SessionOpenEventDecoder openEvent = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder closeEvent = new SessionCloseEventDecoder();
    private final ClusterActionRequestDecoder actionRequest = new ClusterActionRequestDecoder();
    private final NewLeadershipTermEventDecoder termEvent = new NewLeadershipTermEventDecoder();

    private Image image;
    private long maxPosition;
    private long position;
    private long consumedPosition;
    private boolean midMessage;
    private boolean boundReached;
    private boolean succeeded;
    private long abandonedPartialCount;
    private long unknownTemplateCount;
    private ReaderStop stop = ReaderStop.NONE;

    /**
     * For the FIRST image of a log only: genesis (0) or the replay start after a restore. Every later image of the same
     * log is joined with {@link #successor}.
     *
     * @param startPosition where the first image must start; frame aligned
     */
    public ClusterLogReader(final ClusterLogListener listener, final int fragmentLimit, final long startPosition) {
        this.listener = Objects.requireNonNull(listener, "listener");
        if (fragmentLimit <= 0) {
            throw new IllegalArgumentException("fragmentLimit must be > 0: " + fragmentLimit);
        }
        if (startPosition < 0 || (startPosition & (FRAME_ALIGNMENT - 1)) != 0) {
            throw new IllegalArgumentException("startPosition must be >= 0 and frame aligned: " + startPosition);
        }
        this.fragmentLimit = fragmentLimit;
        this.startPosition = startPosition;
        this.builder = new BufferBuilder();
        this.counts = new long[FrameKind.values().length];
        this.position = startPosition;
        this.consumedPosition = startPosition;
    }

    private ClusterLogReader(final ClusterLogReader previous, final ClusterLogListener listener,
                             final int fragmentLimit) {
        this.listener = Objects.requireNonNull(listener, "listener");
        if (fragmentLimit <= 0) {
            throw new IllegalArgumentException("fragmentLimit must be > 0: " + fragmentLimit);
        }
        this.fragmentLimit = fragmentLimit;
        this.startPosition = previous.consumedPosition;
        // One adapter for the whole log: BLA keeps its builder (and a captured partial) across images (A0:BLA:120-123).
        this.builder = previous.builder;
        this.midMessage = previous.midMessage;
        this.counts = previous.counts.clone();
        this.abandonedPartialCount = previous.abandonedPartialCount;
        this.unknownTemplateCount = previous.unknownTemplateCount;
        this.position = previous.position;
        this.consumedPosition = previous.consumedPosition;
    }

    /**
     * The reader for the next image of the same log: it must start at {@code previous.consumedPosition()} (the position
     * CSA's {@code closeLog} keeps, padding and a captured partial included, A0:CSA:829-835) and it carries the
     * reassembly state and the counters. A carried partial is completed if the next image continues it and abandoned
     * (counted) if the next image starts a new message. {@code previous} must not be polled again.
     *
     * @throws ClusterLogReaderStoppedException with {@code PREVIOUS_STOPPED} if {@code previous} has stopped
     */
    public static ClusterLogReader successor(final ClusterLogReader previous, final ClusterLogListener listener,
                                             final int fragmentLimit) {
        Objects.requireNonNull(previous, "previous");
        if (previous.stopped()) {
            throw new ClusterLogReaderStoppedException(new ReaderStop(ReaderStop.Reason.PREVIOUS_STOPPED,
                    previous.consumedPosition, previous.stop.templateId(),
                    new ClusterLogReaderStoppedException(previous.stop)));
        }
        if (previous.succeeded) {
            throw new IllegalStateException("reader already has a successor");
        }
        previous.succeeded = true;
        return new ClusterLogReader(previous, listener, fragmentLimit);
    }

    /**
     * Polls {@code image} up to {@code maxPosition}; never delivers (or lets the image consume) a data frame that ends
     * after it. A listener returning true from {@code onClusterAction} ends the poll right after that frame.
     *
     * @return the number of fragments the image consumed
     * @throws ClusterLogReaderStoppedException once stopped (including by this poll)
     */
    public int poll(final Image image, final long maxPosition) {
        if (succeeded) {
            throw new IllegalStateException("reader was replaced by its successor");
        }
        if (stopped()) {
            throw new ClusterLogReaderStoppedException(stop);
        }
        Objects.requireNonNull(image, "image");
        if (this.image == null) {
            if (image.position() != startPosition) {
                stop = new ReaderStop(ReaderStop.Reason.START_MISMATCH, image.position(), NO_TEMPLATE, null);
                throw new ClusterLogReaderStoppedException(stop);
            }
            this.image = image;
        } else if (image != this.image) {
            stop = new ReaderStop(ReaderStop.Reason.IMAGE_CHANGED, image.position(), NO_TEMPLATE, null);
            throw new ClusterLogReaderStoppedException(stop);
        }
        boundReached = false;
        this.maxPosition = maxPosition;
        final int fragments = image.boundedControlledPoll(handler, maxPosition, fragmentLimit);
        consumedPosition = image.position();
        if (stopped()) {
            throw new ClusterLogReaderStoppedException(stop);
        }
        return fragments;
    }

    public long startPosition() {
        return startPosition;
    }

    /** End of the last delivered or ignored message; {@link #startPosition()} (or the predecessor's) before any. */
    public long position() {
        return position;
    }

    /** The image position after the last poll: padding and the fragments of a captured partial included. */
    public long consumedPosition() {
        return consumedPosition;
    }

    /** A BEGIN fragment is captured and its END has not been seen. */
    public boolean midMessage() {
        return midMessage;
    }

    /** The last poll ABORTed a fragment ending after {@code maxPosition}. */
    public boolean boundReached() {
        return boundReached;
    }

    public boolean stopped() {
        return stop.reason() != ReaderStop.Reason.NONE;
    }

    public ReaderStop stop() {
        return stop;
    }

    public long count(final FrameKind kind) {
        return counts[kind.ordinal()];
    }

    /** Partial messages dropped where BLA drops them: a captured BEGIN followed by an unfragmented frame or a BEGIN. */
    public long abandonedPartialCount() {
        return abandonedPartialCount;
    }

    /** 0, or 1 once an unknown template stopped the reader. */
    public long unknownTemplateCount() {
        return unknownTemplateCount;
    }

    // ------------------------------------------------------------------------------------------ fragment rules (3.1)

    private final class FragmentHandler implements ControlledFragmentHandler {
        @Override
        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (stopped()) {
                return Action.ABORT;                                                          // F0
            }
            int restoreLimit = -1;
            try {
                final long fragmentEnd = header.position();
                if (fragmentEnd > maxPosition) {
                    boundReached = true;                                                      // F2
                    return Action.ABORT;
                }
                final byte flags = header.flags();
                if ((flags & UNFRAGMENTED) == UNFRAGMENTED) {
                    if (midMessage) {                                                         // F3
                        builder.reset();
                        midMessage = false;
                        abandonedPartialCount++;
                    }
                    return onMessage(buffer, offset, length, fragmentEnd);
                }
                if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG) {
                    if (midMessage) {                                                         // F4
                        abandonedPartialCount++;
                    }
                    builder.reset()
                            .captureHeader(header)
                            .append(buffer, offset, length)
                            .nextTermOffset(BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT));
                    midMessage = true;
                    return Action.CONTINUE;
                }
                if (!midMessage || offset != builder.nextTermOffset()) {
                    return fail(ReaderStop.Reason.ORPHAN_FRAGMENT, fragmentEnd, NO_TEMPLATE, null); // F5, F6
                }
                final int limit = builder.limit();
                restoreLimit = limit;
                builder.append(buffer, offset, length);
                if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG) {                               // F8
                    final long messageEnd = builder.completeHeader(header).position();
                    final Action action = onMessage(builder.buffer(), 0, builder.limit(), messageEnd);
                    if (action == Action.ABORT) {
                        builder.limit(limit);
                    } else {
                        builder.reset();
                        midMessage = false;
                    }
                    return action;
                }
                builder.nextTermOffset(BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT)); // F7
                return Action.CONTINUE;
            } catch (final Throwable failure) {                                               // F9
                if (restoreLimit >= 0) {
                    builder.limit(restoreLimit);
                }
                if (!stopped()) {
                    stop = new ReaderStop(ReaderStop.Reason.LISTENER_FAILED, header.position(), NO_TEMPLATE, failure);
                }
                return Action.ABORT;
            }
        }
    }

    private ControlledFragmentHandler.Action fail(final ReaderStop.Reason reason, final long framePosition,
                                                  final int templateId, final Throwable cause) {
        stop = new ReaderStop(reason, framePosition, templateId, cause);
        return ControlledFragmentHandler.Action.ABORT;
    }

    // ------------------------------------------------------------------------------------------- message rules (3.2)

    private ControlledFragmentHandler.Action onMessage(final DirectBuffer buffer, final int offset, final int length,
                                                       final long p) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return fail(ReaderStop.Reason.DECODE_FAILED, p, NO_TEMPLATE, null);                      // M0
        }
        messageHeader.wrap(buffer, offset);
        final int templateId = messageHeader.templateId();
        if (messageHeader.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return fail(ReaderStop.Reason.FOREIGN_SCHEMA, p, templateId, null);                      // M1
        }
        final int version = messageHeader.version();
        if (version != MessageHeaderDecoder.SCHEMA_VERSION) {
            return fail(ReaderStop.Reason.SCHEMA_VERSION_MISMATCH, p, templateId, null);             // M2
        }
        final int blockLength = messageHeader.blockLength();
        final int body = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final FrameKind kind;
        boolean endPoll = false;
        switch (templateId) {
            case SessionMessageHeaderDecoder.TEMPLATE_ID: {                                          // M4
                if (!blockFits(blockLength, SessionMessageHeaderDecoder.BLOCK_LENGTH, length)) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, null);
                }
                sessionHeader.wrap(buffer, body, blockLength, version);
                listener.onSessionMessage(p, sessionHeader.clusterSessionId(), sessionHeader.timestamp(),
                        buffer, offset + SESSION_HEADER_LENGTH, length - SESSION_HEADER_LENGTH);
                kind = FrameKind.SESSION_MESSAGE;
                break;
            }
            case TimerEventDecoder.TEMPLATE_ID: {                                                    // M5
                if (!blockFits(blockLength, TimerEventDecoder.BLOCK_LENGTH, length)) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, null);
                }
                timerEvent.wrap(buffer, body, blockLength, version);
                listener.onTimerEvent(p, timerEvent.correlationId(), timerEvent.timestamp());
                kind = FrameKind.TIMER_EVENT;
                break;
            }
            case SessionOpenEventDecoder.TEMPLATE_ID: {                                              // M6
                if (!blockFits(blockLength, SessionOpenEventDecoder.BLOCK_LENGTH, length)
                        || !openVarDataFits(buffer, offset, length)) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, null);
                }
                openEvent.wrap(buffer, body, blockLength, version);
                final long leadershipTermId = openEvent.leadershipTermId();
                final long clusterSessionId = openEvent.clusterSessionId();
                final long timestamp = openEvent.timestamp();
                final int responseStreamId = openEvent.responseStreamId();
                final String responseChannel = openEvent.responseChannel();
                final byte[] encodedPrincipal = new byte[openEvent.encodedPrincipalLength()];
                openEvent.getEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);
                listener.onSessionOpen(leadershipTermId, p, clusterSessionId, timestamp, responseStreamId,
                        responseChannel, encodedPrincipal);
                kind = FrameKind.SESSION_OPEN;
                break;
            }
            case SessionCloseEventDecoder.TEMPLATE_ID: {                                             // M7
                if (!blockFits(blockLength, SessionCloseEventDecoder.BLOCK_LENGTH, length)) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, null);
                }
                closeEvent.wrap(buffer, body, blockLength, version);
                final CloseReason closeReason;
                try {
                    closeReason = closeEvent.closeReason();
                } catch (final IllegalArgumentException undecodable) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, undecodable);
                }
                listener.onSessionClose(closeEvent.leadershipTermId(), p, closeEvent.clusterSessionId(),
                        closeEvent.timestamp(), closeReason);
                kind = FrameKind.SESSION_CLOSE;
                break;
            }
            case ClusterActionRequestDecoder.TEMPLATE_ID: {                                          // M8
                if (!blockFits(blockLength, ClusterActionRequestDecoder.BLOCK_LENGTH, length)) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, null);
                }
                actionRequest.wrap(buffer, body, blockLength, version);
                final int rawFlags = actionRequest.flags();
                final int flags = rawFlags != ClusterActionRequestDecoder.flagsNullValue() ? rawFlags : 0;
                if (actionRequest.logPosition() != p) {
                    return fail(ReaderStop.Reason.BODY_POSITION_MISMATCH, p, templateId, null);
                }
                final ClusterAction action;
                try {
                    action = actionRequest.action();
                } catch (final IllegalArgumentException undecodable) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, undecodable);
                }
                if (action == ClusterAction.NULL_VAL) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, null);
                }
                endPoll = listener.onClusterAction(actionRequest.leadershipTermId(), p, actionRequest.timestamp(),
                        action, flags);
                kind = FrameKind.CLUSTER_ACTION;
                break;
            }
            case NewLeadershipTermEventDecoder.TEMPLATE_ID: {                                        // M9
                if (!blockFits(blockLength, NewLeadershipTermEventDecoder.BLOCK_LENGTH, length)) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, null);
                }
                termEvent.wrap(buffer, body, blockLength, version);
                if (termEvent.logPosition() != p) {
                    return fail(ReaderStop.Reason.BODY_POSITION_MISMATCH, p, templateId, null);
                }
                final TimeUnit timeUnit;
                try {
                    timeUnit = map(termEvent.timeUnit());
                } catch (final IllegalArgumentException undecodable) {
                    return fail(ReaderStop.Reason.DECODE_FAILED, p, templateId, undecodable);
                }
                listener.onNewLeadershipTermEvent(termEvent.leadershipTermId(), p, termEvent.timestamp(),
                        termEvent.termBaseLogPosition(), termEvent.leaderMemberId(), termEvent.logSessionId(),
                        timeUnit, termEvent.appVersion());
                kind = FrameKind.NEW_LEADERSHIP_TERM;
                break;
            }
            case MembershipChangeEventDecoder.TEMPLATE_ID:                                           // M10
                kind = FrameKind.MEMBERSHIP_CHANGE; // "Removed Dynamic Join" (A0:BLA:254-256): counted, never delivered
                break;
            default:                                                                                 // M11
                unknownTemplateCount = 1;
                return fail(ReaderStop.Reason.UNKNOWN_TEMPLATE, p, templateId, null);
        }
        position = p;                                                                                // M12
        counts[kind.ordinal()]++;
        return endPoll ? ControlledFragmentHandler.Action.BREAK : ControlledFragmentHandler.Action.CONTINUE;
    }

    /** M3: the version-17 block, exactly, and a frame long enough to hold it. */
    private static boolean blockFits(final int blockLength, final int expected, final int length) {
        return blockLength == expected && length >= MessageHeaderDecoder.ENCODED_LENGTH + blockLength;
    }

    /** M3 for template 21: both var-data length fields and their data inside the frame, before anything allocates. */
    private static boolean openVarDataFits(final DirectBuffer buffer, final int offset, final int length) {
        long limit = OPEN_VAR_DATA_OFFSET;
        for (int field = 0; field < 2; field++) {
            if (limit + VAR_DATA_LENGTH_FIELD > length) {
                return false;
            }
            final long dataLength = buffer.getInt(offset + (int)limit, ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL;
            limit += VAR_DATA_LENGTH_FIELD + dataLength;
            if (limit > length) {
                return false;
            }
        }
        return true;
    }

    /** {@code ClusterClock.map} (package-private in Aeron, A0:ClusterClock:120-136). */
    private static TimeUnit map(final ClusterTimeUnit unit) {
        switch (unit) {
            case NULL_VAL:
            case MILLIS:
                return TimeUnit.MILLISECONDS;
            case MICROS:
                return TimeUnit.MICROSECONDS;
            case NANOS:
                return TimeUnit.NANOSECONDS;
            default:
                throw new IllegalArgumentException("unsupported time unit: " + unit);
        }
    }
}
