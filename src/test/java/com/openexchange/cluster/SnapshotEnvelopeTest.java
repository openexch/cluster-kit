// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC1 S2: the one snapshot envelope both engines verify before decoding a byte. */
public class SnapshotEnvelopeTest {

    private static ExpandableArrayBuffer sealed(final int payloadLength, final int[] lengthOut) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        for (int i = 0; i < payloadLength; i++) {
            buf.putByte(SnapshotEnvelope.payloadOffset() + i, (byte) (i * 31 + 7));
        }
        lengthOut[0] = new SnapshotEnvelope().seal(buf, payloadLength);
        return buf;
    }

    @Test
    public void sealThenOpenReturnsThePayloadLength() {
        final int[] len = new int[1];
        final ExpandableArrayBuffer buf = sealed(1_000, len);
        assertEquals(SnapshotEnvelope.envelopedLength(1_000), len[0]);
        assertTrue(SnapshotEnvelope.isEnveloped(buf, len[0]));
        assertEquals(1_000, new SnapshotEnvelope().open(buf, len[0]));
        final int[] empty = new int[1];
        final ExpandableArrayBuffer e = sealed(0, empty);
        assertEquals(0, new SnapshotEnvelope().open(e, empty[0]));
    }

    @Test
    public void aBarePayloadIsRefusedNotPassedThrough() {
        final UnsafeBuffer bare = new UnsafeBuffer(new byte[64]);
        bare.putLong(0, -3L); // looks exactly like a v3 ledger layout tag
        assertFalse(SnapshotEnvelope.isEnveloped(bare, 64));
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SnapshotEnvelope().open(bare, 64));
        assertTrue(e.getMessage(), e.getMessage().contains("magic missing"));
    }

    @Test
    public void aFlippedPayloadBitIsRefused() {
        final int[] len = new int[1];
        final ExpandableArrayBuffer buf = sealed(512, len);
        final int victim = SnapshotEnvelope.payloadOffset() + 200;
        buf.putByte(victim, (byte) (buf.getByte(victim) ^ 0x10));
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SnapshotEnvelope().open(buf, len[0]));
        assertTrue(e.getMessage(), e.getMessage().contains("CRC32C mismatch"));
    }

    @Test
    public void aTruncatedOrPaddedImageIsRefused() {
        final int[] len = new int[1];
        final ExpandableArrayBuffer buf = sealed(512, len);
        assertThrows(IllegalStateException.class, () -> new SnapshotEnvelope().open(buf, len[0] - 1));
        assertThrows(IllegalStateException.class, () -> new SnapshotEnvelope().open(buf, len[0] + 8));
    }

    @Test
    public void aForeignVersionIsRefused() {
        final int[] len = new int[1];
        final ExpandableArrayBuffer buf = sealed(16, len);
        buf.putInt(4, 2);
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SnapshotEnvelope().open(buf, len[0]));
        assertTrue(e.getMessage(), e.getMessage().contains("version 2"));
    }

    @Test
    public void directBuffersAreCheckedInPlaceToo() {
        final UnsafeBuffer direct = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        for (int i = 0; i < 100; i++) {
            direct.putByte(SnapshotEnvelope.payloadOffset() + i, (byte) i);
        }
        final int len = new SnapshotEnvelope().seal(direct, 100);
        assertEquals(100, new SnapshotEnvelope().open(direct, len));
        direct.putByte(SnapshotEnvelope.payloadOffset() + 50, (byte) 0x7F);
        assertThrows(IllegalStateException.class, () -> new SnapshotEnvelope().open(direct, len));
    }
}
