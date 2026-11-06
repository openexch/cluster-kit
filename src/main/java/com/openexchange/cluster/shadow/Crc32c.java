// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.DirectBuffer;

import java.util.zip.CRC32C;

/** CRC32C over any Agrona buffer: heap array, byte buffer, or a raw address (copied through a scratch array). */
final class Crc32c {

    private static final int SCRATCH = 64 * 1024;

    private final CRC32C crc = new CRC32C();
    private byte[] scratch;

    void reset() {
        crc.reset();
    }

    void update(final DirectBuffer buffer, final int offset, final int length) {
        if (length <= 0) {
            return;
        }
        final byte[] array = buffer.byteArray();
        if (array != null) {
            crc.update(array, (int)(buffer.wrapAdjustment() + offset), length);
            return;
        }
        if (buffer.byteBuffer() != null) {
            final java.nio.ByteBuffer bb = buffer.byteBuffer().duplicate();
            final int base = (int)buffer.wrapAdjustment();
            bb.limit(base + offset + length).position(base + offset);
            crc.update(bb);
            return;
        }
        if (scratch == null) {
            scratch = new byte[SCRATCH];
        }
        int done = 0;
        while (done < length) {
            final int n = Math.min(SCRATCH, length - done);
            buffer.getBytes(offset + done, scratch, 0, n);
            crc.update(scratch, 0, n);
            done += n;
        }
    }

    int value() {
        return (int)crc.getValue();
    }

    static int of(final DirectBuffer buffer, final int offset, final int length) {
        final Crc32c c = new Crc32c();
        c.update(buffer, offset, length);
        return c.value();
    }
}
