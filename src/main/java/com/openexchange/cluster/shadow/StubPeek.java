// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.SnapshotEnvelope;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerDecoder;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;

/**
 * The service-payload peek of one service snapshot recording (the floor precondition and the bundle stub
 * refusal): replay the
 * recording session-scoped through {@link ShadowReplay}, skip Aeron's container frames (snapshot marker BEGIN, the
 * client sessions, marker END: {@code ClusteredServiceAgent.snapshotState} writes them before
 * {@code service.onTakeSnapshot}), and look at the first service message. A stub is exactly
 * {@link ShadowStub#ENVELOPED_LENGTH} bytes behind the kit envelope with {@link ShadowStub#STUB_TAG} as its first
 * payload long. The read stops at the first service message, so a full image is never read past its first chunk.
 *
 * <p>Read only; never purges. Used by {@code BundleCapture} (refuse a stub-only bundle) and by the member's
 * {@link ShadowFloor} precondition (is the newest group a full image or a stub).</p>
 */
public final class StubPeek {

    /** What the newest snapshot group's service payload is. */
    public enum Kind {
        /** A full service image (no stub tag): the log below it is not needed to restore. */
        FULL,
        /** A {@link ShadowStub}: restoring it needs a local shadow or the log. */
        STUB,
        /**
         * The replay reached the stop position without a service message after the END marker (no END marker, or an
         * empty or truncated service payload): nothing says what this snapshot restores, so fail closed.
         * The floor purges nothing.
         */
        UNKNOWN
    }

    private StubPeek() {
    }

    /**
     * @param recordingId   the service snapshot recording
     * @param startPosition its start position
     * @param stopPosition  its stop position
     * @param deadlineNs    on {@code clock}
     * @return {@link Kind#STUB} when the first service message carries the stub tag inside the envelope;
     *         {@link Kind#UNKNOWN} when there is no service message after the END marker (fail closed); else
     *         {@link Kind#FULL}
     * @throws RuntimeException when the replay fails or times out (the caller decides: unknown, fail closed)
     */
    public static Kind peek(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                            final long recordingId, final long startPosition, final long stopPosition,
                            final long deadlineNs) {
        Objects.requireNonNull(aeron, "aeron");
        Objects.requireNonNull(archive, "archive");
        Objects.requireNonNull(clock, "clock");
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final SnapshotMarkerDecoder marker = new SnapshotMarkerDecoder();
        final boolean[] containerEnded = {false};
        final byte[][] first = {null};
        new ShadowReplay(aeron, archive, clock).readAll(recordingId, startPosition, stopPosition,
                (buffer, offset, length, h) -> {
                    if (first[0] != null) {
                        return;
                    }
                    if (!containerEnded[0]) {
                        if (length >= MessageHeaderDecoder.ENCODED_LENGTH) {
                            header.wrap(buffer, offset);
                            if (header.schemaId() == MessageHeaderDecoder.SCHEMA_ID
                                    && header.templateId() == SnapshotMarkerDecoder.TEMPLATE_ID) {
                                marker.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                        header.blockLength(), header.version());
                                containerEnded[0] = marker.mark() == SnapshotMark.END;
                            }
                        }
                        return;
                    }
                    final int keep = Math.min(length, ShadowStub.ENVELOPED_LENGTH + 1);
                    first[0] = new byte[keep];
                    buffer.getBytes(offset, first[0], 0, keep);
                }, () -> first[0] != null, deadlineNs);

        final byte[] bytes = first[0];
        if (bytes == null) {
            System.out.println("[STUB-PEEK] service snapshot " + recordingId + " [" + startPosition + ", "
                    + stopPosition + ") holds no service message after " + (containerEnded[0]
                    ? "the END marker" : "its container frames (no END marker)") + ": kind UNKNOWN (fail closed)");
            return Kind.UNKNOWN;
        }
        if (bytes.length < SnapshotEnvelope.payloadOffset() + Long.BYTES) {
            return Kind.FULL;
        }
        final UnsafeBuffer b = new UnsafeBuffer(bytes);
        if (!SnapshotEnvelope.isEnveloped(b, bytes.length)
                || ShadowStub.peekTag(b, SnapshotEnvelope.payloadOffset()) != ShadowStub.STUB_TAG) {
            return Kind.FULL;
        }
        if (bytes.length == ShadowStub.ENVELOPED_LENGTH) {
            try {
                new SnapshotEnvelope().open(b, bytes.length);
            } catch (final IllegalStateException torn) {
                System.out.println("[STUB-PEEK] stub-tagged service snapshot " + recordingId
                        + " fails its envelope check (" + torn.getMessage() + "); treated as a stub either way");
            }
        }
        return Kind.STUB;
    }
}
