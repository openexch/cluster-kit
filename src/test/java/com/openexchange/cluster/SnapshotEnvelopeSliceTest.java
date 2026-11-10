// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * The envelope CRC over a VIEW at a non-zero offset (the shadow image buffer hands {@code serialize} the payload
 * view {@code new UnsafeBuffer(buffer, 128, capacity - 128)} of an off-heap buffer). A seal on the view must open from a
 * plain copy of the bytes, and a plain image must open from a view; before this fix the direct-buffer branch hashed
 * from the ByteBuffer's origin, so a seal on such a view stored a CRC of the wrong bytes.
 */
public class SnapshotEnvelopeSliceTest {

    static void roundTrip(final UnsafeBuffer whole, final int offset) {
        final SnapshotEnvelope envelope = new SnapshotEnvelope();
        final UnsafeBuffer view = new UnsafeBuffer(whole, offset, whole.capacity() - offset);
        final int payload = 1_000;
        final Random r = new Random(offset);
        for (int i = 0; i < whole.capacity(); i++) {
            whole.putByte(i, (byte)r.nextInt()); // the bytes in front of the view differ from the payload
        }
        final int length = envelope.seal(view, payload);
        final byte[] copy = new byte[length];
        view.getBytes(0, copy);
        assertEquals("a seal on a view opens from a plain copy", payload, envelope.open(new UnsafeBuffer(copy), length));
        assertEquals("and from the view itself", payload, envelope.open(view, length));
        final UnsafeBuffer other = new UnsafeBuffer(ByteBuffer.allocateDirect(length + 256));
        other.putBytes(256, copy);
        assertEquals("a plain image opens from a view of another buffer", payload,
                envelope.open(new UnsafeBuffer(other, 256, length), length));
    }

    @Test
    public void heapViewAtAnOffset() {
        roundTrip(new UnsafeBuffer(new byte[4096]), 128);
    }

    @Test
    public void directViewAtAnOffset() {
        roundTrip(new UnsafeBuffer(ByteBuffer.allocateDirect(4096)), 128);
    }

    @Test
    public void directViewAtZero() {
        roundTrip(new UnsafeBuffer(ByteBuffer.allocateDirect(4096)), 0);
    }
}
