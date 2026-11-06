// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.concurrent.TimeUnit;

/**
 * The replicated state machine of one engine, addressed by log entry rather than by Aeron callback: the same apply
 * function a member's service runs, with the frame-end {@code logPosition} of each entry passed explicitly. Members
 * call it from their {@code ClusteredService}; a replayer calls it from {@link ReplayContainer}. Nothing that can
 * schedule, publish, connect or read a clock is reachable from an implementation.
 *
 * <p>The surface is fixed at these nine methods. Replicated identity values a shadow header needs come from a
 * separate source, and log-derived values from {@link ReplayContainer#cursor()}.</p>
 */
public interface ShadowEngine {

    /** 1 ASSETS, 2 MATCH; stable per class. */
    int engineId();

    /** Start from the genesis state; legal once, on a pristine instance. */
    void genesis();

    /** Start from the engine's own enveloped image bytes; legal once, on a pristine instance; throws on refusal. */
    void restore(DirectBuffer image, int length);

    /** Writes the image at index 0 and returns its length: the same bytes as the member's snapshot payload. */
    int serialize(MutableDirectBuffer dst);

    void onSessionOpen(long clusterSessionId, long logPosition, long timestamp, byte[] encodedPrincipal);

    /** An id that is not open is a full no-op (no state change, no event). */
    void onSessionClose(long clusterSessionId, long logPosition, long timestamp, CloseReason closeReason);

    boolean onSessionMessage(long clusterSessionId, long logPosition, long timestamp,
                             DirectBuffer buffer, int offset, int length);

    /** @return an engine-defined timer kind, opaque to the kit */
    int onTimerEvent(long correlationId, long logPosition, long timestamp);

    void onNewLeadershipTermEvent(long leadershipTermId, long logPosition, long timestamp, TimeUnit timeUnit);
}
