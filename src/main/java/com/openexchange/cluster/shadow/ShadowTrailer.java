// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * The last message of a shadow recording, {@value #LENGTH}-byte body in one {@value #FRAME_LENGTH}-byte frame.
 * Offered only after every chunk is recorded (7.4 W8-W9), so it also witnesses that the writer saw the archive
 * durable through the last chunk. Aeron pads before a frame that does not fit a term, never after, so the trailer of a
 * complete recording always starts at {@code stopPosition - 96}.
 *
 * <pre>
 *  0 magic "OXSTRAIL" (int64)  8 version 1  12 headerCrc32c  16 payloadLength  24 payloadDigest (int64)
 * 32 chunkCount  36 engineId  40 logPosition  48 producerSeq  56 reserved (4 B)  60 trailerCrc32c over [0, 60)
 * (amendment Q0: the digest is 8 bytes, the fields after it are re-packed inside the same 64 bytes)
 * </pre>
 */
public record ShadowTrailer(int headerCrc32c, long payloadLength, long payloadDigest, int chunkCount, long logPosition,
                            long producerSeq, int engineId) {

    public static final long MAGIC = 0x4F5853545241494CL;
    public static final int VERSION = 1;
    public static final int LENGTH = 64;
    public static final int FRAME_LENGTH = 96;
    static final int CRC_OFFSET = 60;
    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    /** Refusals: T1/T2 mean "not a trailer"; T3-T5 mean "a trailer that disagrees with the header". */
    public enum Reason { NOT_A_TRAILER, TRAILER_VERSION_UNSUPPORTED, TRAILER_MISMATCH }

    public record Problem(Reason reason, String field) {
        @Override
        public String toString() {
            return field == null ? reason.name() : reason + "(" + field + ")";
        }
    }

    public record Decoded(ShadowTrailer trailer, Problem problem) {
        public boolean ok() {
            return problem == null;
        }
    }

    /** The trailer a header implies. */
    public static ShadowTrailer of(final ShadowHeader header, final int headerCrc32c) {
        return new ShadowTrailer(headerCrc32c, header.payloadLength(), header.payloadDigest(),
                (int)header.chunkCount(), header.logPosition(), header.producerSeq(), header.engineId());
    }

    /** All 64 bytes at {@code offset}; returns the trailer CRC. */
    public int encode(final MutableDirectBuffer buffer, final int offset) {
        buffer.putLong(offset, MAGIC, LE);
        buffer.putInt(offset + 8, VERSION, LE);
        buffer.putInt(offset + 12, headerCrc32c, LE);
        buffer.putLong(offset + 16, payloadLength, LE);
        buffer.putLong(offset + 24, payloadDigest, LE);
        buffer.putInt(offset + 32, chunkCount, LE);
        buffer.putInt(offset + 36, engineId, LE);
        buffer.putLong(offset + 40, logPosition, LE);
        buffer.putLong(offset + 48, producerSeq, LE);
        buffer.putInt(offset + 56, 0, LE);
        return sealCrc(buffer, offset);
    }

    /** Recomputes the trailer CRC over {@code [offset, offset + 60)}. */
    public static int sealCrc(final MutableDirectBuffer buffer, final int offset) {
        final int crc = Crc32c.of(buffer, offset, CRC_OFFSET);
        buffer.putInt(offset + CRC_OFFSET, crc, LE);
        return crc;
    }

    /** T1 (length, magic, CRC), T2 (version), T5 (reserved); the header comparison is {@link #mismatch}. */
    public static Decoded decode(final DirectBuffer buffer, final int offset, final int length) {
        if (length != LENGTH || buffer.getLong(offset, LE) != MAGIC
                || Crc32c.of(buffer, offset, CRC_OFFSET) != buffer.getInt(offset + CRC_OFFSET, LE)) {
            return new Decoded(null, new Problem(Reason.NOT_A_TRAILER, null));
        }
        if (buffer.getInt(offset + 8, LE) != VERSION) {
            return new Decoded(null, new Problem(Reason.TRAILER_VERSION_UNSUPPORTED, null));
        }
        if (buffer.getInt(offset + 56, LE) != 0) {
            return new Decoded(null, new Problem(Reason.TRAILER_MISMATCH, "reserved"));
        }
        return new Decoded(new ShadowTrailer(buffer.getInt(offset + 12, LE), buffer.getLong(offset + 16, LE),
                buffer.getLong(offset + 24, LE), buffer.getInt(offset + 32, LE), buffer.getLong(offset + 40, LE),
                buffer.getLong(offset + 48, LE), buffer.getInt(offset + 36, LE)), null);
    }

    /** T3 (copied fields) and T4 (chunk count) against the header; null when they agree. */
    public Problem mismatch(final ShadowHeader header, final int headerCrc32c) {
        if (this.headerCrc32c != headerCrc32c) {
            return t3("headerCrc32c");
        }
        if (payloadLength != header.payloadLength()) {
            return t3("payloadLength");
        }
        if (payloadDigest != header.payloadDigest()) {
            return t3("payloadDigest");
        }
        if (logPosition != header.logPosition()) {
            return t3("logPosition");
        }
        if (producerSeq != header.producerSeq()) {
            return t3("producerSeq");
        }
        if (engineId != header.engineId()) {
            return t3("engineId");
        }
        if (chunkCount != header.chunkCount()) {
            return t3("chunkCount");
        }
        return null;
    }

    private static Problem t3(final String field) {
        return new Problem(Reason.TRAILER_MISMATCH, field);
    }
}
