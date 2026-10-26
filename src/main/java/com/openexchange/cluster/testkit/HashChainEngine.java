// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ShadowEngine;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A sensitivity probe, engine id 99: every open, close, message and timer folds (kind, id or correlation id,
 * logPosition, timestamp, payload or principal bytes) into a 64-bit chain, so applying any of them twice, out of order,
 * with a different argument, or not at all changes the image. Term events are state-neutral. There is no session table,
 * so a close of an id never opened still folds: this is NOT a model engine and does not satisfy conformance rule R5.
 * A term event folds nothing but, like every {@code on*} entry, ends the pristine state, so a restore after it is
 * refused (conformance R1).
 *
 * <p>Image: magic, count, chain (20 bytes). Events: one rendered line per folded entry, without the chain (so an event
 * stream can match while the state differs; the runner checks both).</p>
 */
public final class HashChainEngine implements ShadowEngine {

    public static final int ENGINE_ID = 99;
    public static final int IMAGE_LENGTH = 4 + 8 + 8;
    private static final int MAGIC = 0x48434531;
    private static final long SEED = 0xCBF29CE484222325L;
    private static final long PRIME = 0x100000001B3L;

    private long chain = SEED;
    private long count;
    private boolean pristine = true;
    private boolean failed;
    private final List<String> events = new ArrayList<>();

    @Override
    public int engineId() {
        return ENGINE_ID;
    }

    @Override
    public void genesis() {
        ensureNotFailed();
        if (!pristine) {
            throw new IllegalStateException("genesis requires a pristine engine");
        }
        pristine = false;
    }

    @Override
    public void restore(final DirectBuffer image, final int length) {
        ensureNotFailed();
        if (!pristine) {
            throw new IllegalStateException("restore requires a pristine engine");
        }
        pristine = false;
        if (length != IMAGE_LENGTH || image.getInt(0, ByteOrder.LITTLE_ENDIAN) != MAGIC) {
            failed = true;
            throw new IllegalStateException("not a hash-chain image: length=" + length);
        }
        count = image.getLong(4, ByteOrder.LITTLE_ENDIAN);
        chain = image.getLong(12, ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public int serialize(final MutableDirectBuffer dst) {
        ensureNotFailed();
        dst.putInt(0, MAGIC, ByteOrder.LITTLE_ENDIAN);
        dst.putLong(4, count, ByteOrder.LITTLE_ENDIAN);
        dst.putLong(12, chain, ByteOrder.LITTLE_ENDIAN);
        return IMAGE_LENGTH;
    }

    @Override
    public void onSessionOpen(final long clusterSessionId, final long logPosition, final long timestamp,
                              final byte[] encodedPrincipal) {
        enter();
        fold(1, clusterSessionId, logPosition, timestamp);
        for (final byte b : encodedPrincipal) {
            mix(b);
        }
        emit("OPEN", clusterSessionId, logPosition, timestamp, encodedPrincipal.length);
    }

    @Override
    public void onSessionClose(final long clusterSessionId, final long logPosition, final long timestamp,
                               final CloseReason closeReason) {
        enter();
        fold(2, clusterSessionId, logPosition, timestamp);
        emit("CLOSE", clusterSessionId, logPosition, timestamp, 0);
    }

    @Override
    public boolean onSessionMessage(final long clusterSessionId, final long logPosition, final long timestamp,
                                    final DirectBuffer buffer, final int offset, final int length) {
        enter();
        fold(3, clusterSessionId, logPosition, timestamp);
        for (int i = 0; i < length; i++) {
            mix(buffer.getByte(offset + i));
        }
        emit("MSG", clusterSessionId, logPosition, timestamp, length);
        return true;
    }

    @Override
    public int onTimerEvent(final long correlationId, final long logPosition, final long timestamp) {
        enter();
        fold(4, correlationId, logPosition, timestamp);
        emit("TIMER", correlationId, logPosition, timestamp, 0);
        return 0;
    }

    @Override
    public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                         final TimeUnit timeUnit) {
        enter();
    }

    /** Makes every later entry throw {@link IllegalStateException} (conformance R7). */
    public void poison() {
        failed = true;
    }

    public boolean failed() {
        return failed;
    }

    public long chain() {
        return chain;
    }

    public long count() {
        return count;
    }

    /** Rendered events so far (a copy). */
    public List<String> events() {
        return List.copyOf(events);
    }

    private void enter() {
        ensureNotFailed();
        pristine = false;
    }

    private void ensureNotFailed() {
        if (failed) {
            throw new IllegalStateException("hash-chain engine failed");
        }
    }

    private void fold(final int kind, final long id, final long logPosition, final long timestamp) {
        count++;
        mixLong(kind);
        mixLong(id);
        mixLong(logPosition);
        mixLong(timestamp);
    }

    private void mixLong(final long value) {
        for (int i = 0; i < 8; i++) {
            mix((byte)(value >>> (i * 8)));
        }
    }

    private void mix(final byte b) {
        chain = (chain ^ (b & 0xFF)) * PRIME;
    }

    private void emit(final String kind, final long id, final long logPosition, final long timestamp,
                      final int bytes) {
        events.add(kind + " id=" + id + " pos=" + logPosition + " ts=" + timestamp + " bytes=" + bytes);
    }
}
