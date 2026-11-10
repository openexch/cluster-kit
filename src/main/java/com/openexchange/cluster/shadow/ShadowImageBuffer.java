// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;

/**
 * One host-owned buffer (off-heap, reused, sized by the host) with a {@value ShadowHeader#LENGTH}-byte header slot in
 * front: the host calls {@code container.serialize(image.payloadView())}, so the image lands behind the
 * slot with no copy, then {@link #setPayloadLength}. Ownership: the writer's from {@code write} entry until the result
 * (or the thread wrapper's listener callback).
 */
public final class ShadowImageBuffer {

    private final MutableDirectBuffer buffer;
    private final UnsafeBuffer payload;
    private int payloadLength;

    public ShadowImageBuffer(final MutableDirectBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        if (buffer.capacity() <= ShadowHeader.LENGTH) {
            throw new IllegalArgumentException("buffer capacity " + buffer.capacity() + " leaves no payload room");
        }
        this.payload = new UnsafeBuffer(buffer, ShadowHeader.LENGTH, buffer.capacity() - ShadowHeader.LENGTH);
    }

    /** The whole buffer: header slot at 0, payload at {@value ShadowHeader#LENGTH}. */
    public MutableDirectBuffer buffer() {
        return buffer;
    }

    /** {@code [128, capacity)}: hand this to {@code serialize}. */
    public UnsafeBuffer payloadView() {
        return payload;
    }

    public int payloadCapacity() {
        return payload.capacity();
    }

    public void setPayloadLength(final int length) {
        if (length < 0 || length > payload.capacity()) {
            throw new IllegalArgumentException("payload length " + length + " outside [0, " + payload.capacity() + "]");
        }
        this.payloadLength = length;
    }

    public int payloadLength() {
        return payloadLength;
    }
}
