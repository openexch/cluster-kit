// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.DirectBuffer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The shadow payload digest (RCS S3 amendment Q0, 2026-11-01): the first 8 bytes of SHA-256 over the payload bytes as
 * stored, the engine envelope (and its own CRC) included, read big-endian into a {@code long}. It is the identity of a
 * producer image: content agreement, the loader and the deep read compare it. CRC32C must not be used here: an
 * engine image that ends with the CRC32C of what precedes it (every kit {@code SnapshotEnvelope} image) has the same
 * CRC32C for every image of one length (the CRC residue), so a CRC cannot tell two self-consistent states apart. CRC32C
 * stays where it guards bytes against tearing (header and trailer CRCs). JDK {@link MessageDigest}, no dependency.
 */
final class PayloadDigest {

    private static final int SCRATCH = 64 * 1024;

    private final MessageDigest sha;
    private byte[] scratch;

    PayloadDigest() {
        try {
            sha = MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is mandatory in every JDK", e);
        }
    }

    void reset() {
        sha.reset();
    }

    void update(final DirectBuffer buffer, final int offset, final int length) {
        if (length <= 0) {
            return;
        }
        final byte[] array = buffer.byteArray();
        if (array != null) {
            sha.update(array, (int)(buffer.wrapAdjustment() + offset), length);
            return;
        }
        if (buffer.byteBuffer() != null) {
            final java.nio.ByteBuffer bb = buffer.byteBuffer().duplicate();
            final int base = (int)buffer.wrapAdjustment();
            bb.limit(base + offset + length).position(base + offset);
            sha.update(bb);
            return;
        }
        if (scratch == null) {
            scratch = new byte[SCRATCH];
        }
        for (int done = 0; done < length; ) {
            final int n = Math.min(SCRATCH, length - done);
            buffer.getBytes(offset + done, scratch, 0, n);
            sha.update(scratch, 0, n);
            done += n;
        }
    }

    /** The first 8 bytes of the SHA-256, big-endian; the digest is reset afterwards. */
    long value() {
        final byte[] d = sha.digest();
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (d[i] & 0xFF);
        }
        return v;
    }

    static long of(final DirectBuffer buffer, final int offset, final int length) {
        final PayloadDigest d = new PayloadDigest();
        d.update(buffer, offset, length);
        return d.value();
    }
}
