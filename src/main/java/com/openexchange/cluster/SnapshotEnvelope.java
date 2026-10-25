// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.zip.CRC32C;

/**
 * Integrity envelope around a snapshot payload, shared by both engines (K5 item 5):
 * {@code [magic:int][version:int][payloadLength:int][reserved:int][payload...][crc32c:int]}.
 *
 * <p>The payload is written in place after the 16-byte header, so a 200 MB ledger is not copied; the
 * trailer is computed once over the finished payload, in place. A loader verifies magic, version,
 * length and CRC before decoding a byte and refuses to start on any mismatch: a truncated, foreign or
 * bit-flipped snapshot must never become a ledger. There is deliberately NO legacy pass-through for a
 * payload without the magic (RC1 S2, clean genesis): a bare payload is refused like any other
 * corruption, because "it looked like an old format" is exactly the silent restore path this class
 * exists to remove.</p>
 *
 * <p>Chunked snapshot transport sits outside the envelope: chunks are concatenated raw and the
 * envelope is opened on the reassembled image.</p>
 */
public final class SnapshotEnvelope {

    public static final int MAGIC = 0x4145534E; // "AESN"
    public static final int VERSION = 1;
    public static final int HEADER_LENGTH = 16;
    public static final int TRAILER_LENGTH = 4;

    private final CRC32C crc = new CRC32C();

    /** Where a payload must start so the header fits before it. */
    public static int payloadOffset() {
        return HEADER_LENGTH;
    }

    /** Total envelope length for a payload of {@code payloadLength} bytes. */
    public static int envelopedLength(final int payloadLength) {
        return HEADER_LENGTH + payloadLength + TRAILER_LENGTH;
    }

    /**
     * Seal a payload already written at {@link #payloadOffset()} with {@code payloadLength} bytes.
     *
     * @return the total envelope length to offer (header + payload + trailer)
     */
    public int seal(final MutableDirectBuffer buf, final int payloadLength) {
        if (payloadLength < 0) {
            throw new IllegalArgumentException("payloadLength " + payloadLength);
        }
        buf.putInt(0, MAGIC);
        buf.putInt(4, VERSION);
        buf.putInt(8, payloadLength);
        buf.putInt(12, 0);
        buf.putInt(HEADER_LENGTH + payloadLength, crc32c(buf, HEADER_LENGTH, payloadLength));
        return envelopedLength(payloadLength);
    }

    /** True if {@code buf[0..length)} starts with this envelope's magic. Diagnostic only; {@link #open} decides. */
    public static boolean isEnveloped(final DirectBuffer buf, final int length) {
        return length >= HEADER_LENGTH + TRAILER_LENGTH && buf.getInt(0) == MAGIC;
    }

    /**
     * Verify an enveloped image and return the payload length (payload starts at {@link #payloadOffset()}).
     *
     * @throws IllegalStateException when the magic is missing or on a version, length or CRC mismatch
     */
    public int open(final DirectBuffer buf, final int length) {
        if (!isEnveloped(buf, length)) {
            throw new IllegalStateException("snapshot envelope magic missing (length=" + length
                    + "): refusing a bare or foreign snapshot image");
        }
        final int version = buf.getInt(4);
        if (version != VERSION) {
            throw new IllegalStateException("snapshot envelope version " + version + ", this build reads " + VERSION);
        }
        final int payloadLength = buf.getInt(8);
        if (payloadLength < 0 || envelopedLength(payloadLength) != length) {
            throw new IllegalStateException("snapshot envelope length mismatch: header says " + payloadLength
                    + " payload bytes, image carries " + length + " total");
        }
        final int expected = buf.getInt(HEADER_LENGTH + payloadLength);
        final int actual = crc32c(buf, HEADER_LENGTH, payloadLength);
        if (expected != actual) {
            throw new IllegalStateException(String.format(
                    "snapshot CRC32C mismatch: trailer %08x, computed %08x over %d bytes", expected, actual, payloadLength));
        }
        return payloadLength;
    }

    private int crc32c(final DirectBuffer buf, final int offset, final int length) {
        crc.reset();
        final byte[] array = buf.byteArray();
        if (array != null) {
            crc.update(array, buf.wrapAdjustment() + offset, length);
        } else {
            crc.update(buf.byteBuffer().duplicate().position(offset).limit(offset + length));
        }
        return (int) crc.getValue();
    }
}
