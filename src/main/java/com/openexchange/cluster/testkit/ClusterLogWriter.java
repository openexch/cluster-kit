// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.ClusterActionRequestEncoder;
import io.aeron.cluster.codecs.ClusterTimeUnit;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.MembershipChangeEventEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.SessionCloseEventEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionOpenEventEncoder;
import io.aeron.cluster.codecs.TimerEventEncoder;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.BitUtil.align;

/**
 * Writes cluster-schema log frames on an {@link ExclusivePublication} exactly as Aeron's {@code LogPublisher} encodes
 * them (A0:LP:115-310): the CM's 32-byte session header in front of a message payload, {@code offer} for messages and
 * opens, {@code tryClaim}-sized frames for the fixed-block events, and the body {@code logPosition} of actions and term
 * events computed as {@code publication.position() + alignedFragmentLength} (A0:LP:254, 294). Every method returns the
 * frame-end position the publication reported, so tests compare a reader against the writer, not against itself.
 *
 * <p>Negative frames for reader tests: {@link #membershipChange}, {@link #raw}, {@link #actionWithBodyPosition},
 * {@link #actionValue}. Test-only: it owns a publication by design.</p>
 */
public final class ClusterLogWriter {

    private static final long OFFER_TIMEOUT_NS = 10_000_000_000L;

    private final ExclusivePublication publication;
    private final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
    private final SessionMessageHeaderEncoder sessionHeader = new SessionMessageHeaderEncoder();
    private final UnsafeBuffer sessionHeaderBuffer =
            new UnsafeBuffer(new byte[MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH]);
    private final SessionOpenEventEncoder openEvent = new SessionOpenEventEncoder();
    private final SessionCloseEventEncoder closeEvent = new SessionCloseEventEncoder();
    private final TimerEventEncoder timerEvent = new TimerEventEncoder();
    private final ClusterActionRequestEncoder actionRequest = new ClusterActionRequestEncoder();
    private final NewLeadershipTermEventEncoder termEvent = new NewLeadershipTermEventEncoder();
    private final MembershipChangeEventEncoder membershipChange = new MembershipChangeEventEncoder();
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(4096);

    private long leadershipTermId;

    public ClusterLogWriter(final ExclusivePublication publication) {
        this.publication = Objects.requireNonNull(publication, "publication");
        sessionHeader.wrapAndApplyHeader(sessionHeaderBuffer, 0, new MessageHeaderEncoder());
    }

    /** The term id stamped on later message, open, close, timer and action frames (as the CM's current term). */
    public ClusterLogWriter leadershipTermId(final long leadershipTermId) {
        this.leadershipTermId = leadershipTermId;
        return this;
    }

    public long leadershipTermId() {
        return leadershipTermId;
    }

    public ExclusivePublication publication() {
        return publication;
    }

    /** The publication position: the end of the last frame written. */
    public long position() {
        return publication.position();
    }

    /** {@code appendMessage}: session header, then the client payload (A0:LP:115-142). */
    public long message(final long clusterSessionId, final long timestamp, final DirectBuffer payload,
                        final int offset, final int length) {
        sessionHeader.leadershipTermId(leadershipTermId).clusterSessionId(clusterSessionId).timestamp(timestamp);
        final long deadline = System.nanoTime() + OFFER_TIMEOUT_NS;
        while (true) {
            final long result = publication.offer(sessionHeaderBuffer, 0, sessionHeaderBuffer.capacity(),
                    payload, offset, length, null);
            if (result > 0) {
                return result;
            }
            retry(result, deadline);
        }
    }

    public long message(final long clusterSessionId, final long timestamp, final byte[] payload) {
        return message(clusterSessionId, timestamp, new UnsafeBuffer(payload), 0, payload.length);
    }

    /** {@code appendSessionOpen} (A0:LP:144-177). */
    public long sessionOpen(final long clusterSessionId, final long correlationId, final long timestamp,
                            final int responseStreamId, final String responseChannel,
                            final byte[] encodedPrincipal) {
        openEvent.wrapAndApplyHeader(buffer, 0, messageHeader)
                .leadershipTermId(leadershipTermId)
                .clusterSessionId(clusterSessionId)
                .correlationId(correlationId)
                .timestamp(timestamp)
                .responseStreamId(responseStreamId)
                .responseChannel(responseChannel)
                .putEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);
        return offer(buffer, MessageHeaderEncoder.ENCODED_LENGTH + openEvent.encodedLength());
    }

    /** {@code appendSessionClose} (A0:LP:179-211). */
    public long sessionClose(final long clusterSessionId, final long timestamp, final CloseReason closeReason) {
        closeEvent.wrapAndApplyHeader(buffer, 0, messageHeader)
                .leadershipTermId(leadershipTermId)
                .clusterSessionId(clusterSessionId)
                .timestamp(timestamp)
                .closeReason(closeReason);
        return offer(buffer, MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH);
    }

    /** {@code appendTimer} (A0:LP:213-239). */
    public long timer(final long correlationId, final long timestamp) {
        timerEvent.wrapAndApplyHeader(buffer, 0, messageHeader)
                .leadershipTermId(leadershipTermId)
                .correlationId(correlationId)
                .timestamp(timestamp);
        return offer(buffer, MessageHeaderEncoder.ENCODED_LENGTH + TimerEventEncoder.BLOCK_LENGTH);
    }

    /** {@code appendClusterAction}: body position = frame end (A0:LP:241-276). */
    public long action(final ClusterAction action, final long timestamp, final int flags) {
        return writeAction(action.value(), timestamp, flags, -1L, true);
    }

    /** An action whose flags field holds its null value (a pre-version-11 writer); readers map it to 0. */
    public long actionNullFlags(final ClusterAction action, final long timestamp) {
        return writeAction(action.value(), timestamp, ClusterActionRequestEncoder.flagsNullValue(), -1L, true);
    }

    /** Negative: an action whose body {@code logPosition} is {@code bodyPosition} instead of the frame end. */
    public long actionWithBodyPosition(final ClusterAction action, final long timestamp, final int flags,
                                       final long bodyPosition) {
        return writeAction(action.value(), timestamp, flags, bodyPosition, false);
    }

    /** Negative: an action with a raw action value (an undefined one, or {@code Integer.MIN_VALUE} = NULL_VAL). */
    public long actionValue(final int rawAction, final long timestamp, final int flags) {
        return writeAction(rawAction, timestamp, flags, -1L, true);
    }

    /** {@code appendNewLeadershipTermEvent}: body position = frame end (A0:LP:278-319). Sets the writer's term. */
    public long newLeadershipTerm(final long leadershipTermId, final long timestamp, final long termBaseLogPosition,
                                  final int leaderMemberId, final int logSessionId, final TimeUnit timeUnit,
                                  final int appVersion) {
        this.leadershipTermId = leadershipTermId;
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH;
        final long deadline = System.nanoTime() + OFFER_TIMEOUT_NS;
        while (true) {
            final long logPosition = publication.position() + alignedFrame(length);
            termEvent.wrapAndApplyHeader(buffer, 0, messageHeader)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .termBaseLogPosition(termBaseLogPosition)
                    .leaderMemberId(leaderMemberId)
                    .logSessionId(logSessionId)
                    .timeUnit(clusterTimeUnit(timeUnit))
                    .appVersion(appVersion);
            final long result = publication.offer(buffer, 0, length, null);
            if (result > 0) {
                if (result != logPosition) {
                    throw new IllegalStateException("term event landed at " + result + ", body says " + logPosition
                            + " (term rotation between position read and offer)");
                }
                return result;
            }
            retry(result, deadline);
        }
    }

    /** WK-11: a MembershipChangeEvent (template 25), which Aeron 1.53 no longer writes; synthetic only. */
    public long membershipChange(final long timestamp) {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + MembershipChangeEventEncoder.BLOCK_LENGTH;
        membershipChange.wrapAndApplyHeader(buffer, 0, messageHeader)
                .leadershipTermId(leadershipTermId)
                .logPosition(publication.position() + alignedFrame(length))
                .timestamp(timestamp)
                .leaderMemberId(0)
                .clusterSize(1)
                .changeType(io.aeron.cluster.codecs.ChangeType.JOIN)
                .memberId(0)
                .clusterMembers("");
        return offer(buffer, MessageHeaderEncoder.ENCODED_LENGTH + membershipChange.encodedLength());
    }

    /** Negative: a frame with a hand-built 8-byte message header and {@code body} after it. */
    public long raw(final int schemaId, final int templateId, final int version, final int blockLength,
                    final byte[] body) {
        messageHeader.wrap(buffer, 0)
                .blockLength(blockLength)
                .templateId(templateId)
                .schemaId(schemaId)
                .version(version);
        buffer.putBytes(MessageHeaderEncoder.ENCODED_LENGTH, body);
        return offer(buffer, MessageHeaderEncoder.ENCODED_LENGTH + body.length);
    }

    /** Negative: arbitrary frame bytes (for example shorter than a message header). */
    public long raw(final byte[] frame) {
        buffer.putBytes(0, frame);
        return offer(buffer, frame.length);
    }

    /** The bytes {@link #message} puts in the log for this payload: session header, then the payload. */
    public byte[] encodeMessage(final long clusterSessionId, final long timestamp, final byte[] payload) {
        sessionHeader.leadershipTermId(leadershipTermId).clusterSessionId(clusterSessionId).timestamp(timestamp);
        final byte[] bytes = new byte[sessionHeaderBuffer.capacity() + payload.length];
        sessionHeaderBuffer.getBytes(0, bytes, 0, sessionHeaderBuffer.capacity());
        System.arraycopy(payload, 0, bytes, sessionHeaderBuffer.capacity(), payload.length);
        return bytes;
    }

    /**
     * Negative: one data frame with hand-chosen fragment {@code flags} (for example a lone BEGIN) carrying
     * {@code bytes[offset, offset + length)}, written with {@code offerBlock}. Must fit in the current term.
     *
     * @return the frame end position
     */
    public long fragment(final byte flags, final byte[] bytes, final int offset, final int length) {
        final int frameLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        final int blockLength = align(frameLength, FRAME_ALIGNMENT);
        final UnsafeBuffer block = new UnsafeBuffer(new byte[blockLength]);
        final DataHeaderFlyweight header = new DataHeaderFlyweight(block);
        header.frameLength(frameLength)
                .version(DataHeaderFlyweight.CURRENT_VERSION)
                .flags(flags)
                .headerType(DataHeaderFlyweight.HDR_TYPE_DATA);
        // offerBlock rotates first when the term is exactly full: the block then belongs to the next term.
        final boolean rotates = publication.termOffset() >= publication.termBufferLength();
        header.termOffset(rotates ? 0 : publication.termOffset())
                .sessionId(publication.sessionId())
                .streamId(publication.streamId())
                .termId(rotates ? publication.termId() + 1 : publication.termId())
                .reservedValue(0L);
        block.putBytes(DataHeaderFlyweight.HEADER_LENGTH, bytes, offset, length);
        final long deadline = System.nanoTime() + OFFER_TIMEOUT_NS;
        while (true) {
            final long result = publication.offerBlock(block, 0, blockLength);
            if (result > 0) {
                return result;
            }
            retry(result, deadline);
        }
    }

    /** Bytes left in the current term. */
    public int termRemaining() {
        return publication.termBufferLength() - publication.termOffset();
    }

    // --------------------------------------------------------------------------------------------------- private

    private long writeAction(final int rawAction, final long timestamp, final int flags, final long bodyPosition,
                             final boolean bodyIsFrameEnd) {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterActionRequestEncoder.BLOCK_LENGTH;
        final long deadline = System.nanoTime() + OFFER_TIMEOUT_NS;
        while (true) {
            final long logPosition = bodyIsFrameEnd ? publication.position() + alignedFrame(length) : bodyPosition;
            actionRequest.wrapAndApplyHeader(buffer, 0, messageHeader)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .flags(flags);
            // action(ClusterAction) can only write defined values; put the raw int at the field offset.
            buffer.putInt(MessageHeaderEncoder.ENCODED_LENGTH + ClusterActionRequestEncoder.actionEncodingOffset(),
                    rawAction, java.nio.ByteOrder.LITTLE_ENDIAN);
            final long result = publication.offer(buffer, 0, length, null);
            if (result > 0) {
                if (bodyIsFrameEnd && result != logPosition) {
                    throw new IllegalStateException("action landed at " + result + ", body says " + logPosition);
                }
                return result;
            }
            retry(result, deadline);
        }
    }

    private long offer(final DirectBuffer src, final int length) {
        final long deadline = System.nanoTime() + OFFER_TIMEOUT_NS;
        while (true) {
            final long result = publication.offer(src, 0, length, null);
            if (result > 0) {
                return result;
            }
            retry(result, deadline);
        }
    }

    private void retry(final long result, final long deadline) {
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException("log publication refused the frame: " + Publication.errorString(result));
        }
        if (System.nanoTime() > deadline) {
            throw new IllegalStateException("log publication did not accept the frame in time: "
                    + Publication.errorString(result));
        }
        Thread.onSpinWait();
    }

    private static int alignedFrame(final int length) {
        return align(DataHeaderFlyweight.HEADER_LENGTH + length, FRAME_ALIGNMENT);
    }

    private static ClusterTimeUnit clusterTimeUnit(final TimeUnit timeUnit) {
        switch (timeUnit) {
            case MILLISECONDS:
                return ClusterTimeUnit.MILLIS;
            case MICROSECONDS:
                return ClusterTimeUnit.MICROS;
            case NANOSECONDS:
                return ClusterTimeUnit.NANOS;
            default:
                throw new IllegalArgumentException("unsupported time unit: " + timeUnit);
        }
    }
}
