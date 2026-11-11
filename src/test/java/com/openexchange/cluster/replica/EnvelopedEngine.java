// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.log.ShadowEngine;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

/**
 * A test engine's image inside the {@link SnapshotEnvelope}, as every production engine writes its snapshot (the
 * replica's compare opens the envelope before it reads anything). Delegates everything else.
 */
final class EnvelopedEngine implements ShadowEngine {

    final ShadowEngine inner;
    private final SnapshotEnvelope envelope = new SnapshotEnvelope();

    EnvelopedEngine(final ShadowEngine inner) {
        this.inner = inner;
    }

    @Override
    public int engineId() {
        return inner.engineId();
    }

    @Override
    public void genesis() {
        inner.genesis();
    }

    @Override
    public void restore(final DirectBuffer image, final int length) {
        final int payload = envelope.open(image, length);
        inner.restore(new UnsafeBuffer(image, SnapshotEnvelope.payloadOffset(), payload), payload);
    }

    @Override
    public int serialize(final MutableDirectBuffer dst) {
        final UnsafeBuffer view = new UnsafeBuffer(dst, SnapshotEnvelope.payloadOffset(),
                dst.capacity() - SnapshotEnvelope.envelopedLength(0));
        return envelope.seal(dst, inner.serialize(view));
    }

    @Override
    public void onSessionOpen(final long clusterSessionId, final long logPosition, final long timestamp,
                              final byte[] encodedPrincipal) {
        inner.onSessionOpen(clusterSessionId, logPosition, timestamp, encodedPrincipal);
    }

    @Override
    public void onSessionClose(final long clusterSessionId, final long logPosition, final long timestamp,
                               final CloseReason closeReason) {
        inner.onSessionClose(clusterSessionId, logPosition, timestamp, closeReason);
    }

    @Override
    public boolean onSessionMessage(final long clusterSessionId, final long logPosition, final long timestamp,
                                    final DirectBuffer buffer, final int offset, final int length) {
        return inner.onSessionMessage(clusterSessionId, logPosition, timestamp, buffer, offset, length);
    }

    @Override
    public int onTimerEvent(final long correlationId, final long logPosition, final long timestamp) {
        return inner.onTimerEvent(correlationId, logPosition, timestamp);
    }

    @Override
    public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                         final TimeUnit timeUnit) {
        inner.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
    }
}
