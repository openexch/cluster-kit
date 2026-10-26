// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

/**
 * Exactly what Aeron's {@code BoundedLogAdapter} hands {@code ClusteredServiceAgent} (A0:BLA:155-251), argument order
 * kept; no {@code Header}, no {@code ClientSession}. Every {@code logPosition} is the frame end of the message (for a
 * fragmented message, the end of its last fragment), which is the value a member's {@code cluster.logPosition()}
 * holds during the matching service callback.
 *
 * <p>There is no hook for an unknown template: the reader stops there and that is not a policy a caller can flip.</p>
 */
public interface ClusterLogListener {

    /** Template 1. {@code buffer} is valid during the call only; offset/length exclude the 32-byte session header. */
    void onSessionMessage(long logPosition, long clusterSessionId, long timestamp,
                          DirectBuffer buffer, int offset, int length);

    /** Template 20. */
    void onTimerEvent(long logPosition, long correlationId, long timestamp);

    /** Template 21. {@code encodedPrincipal} is a fresh array owned by the callee. */
    void onSessionOpen(long leadershipTermId, long logPosition, long clusterSessionId, long timestamp,
                       int responseStreamId, String responseChannel, byte[] encodedPrincipal);

    /** Template 22. {@code closeReason} may be {@link CloseReason#NULL_VAL}; it is forwarded as decoded. */
    void onSessionClose(long leadershipTermId, long logPosition, long clusterSessionId, long timestamp,
                        CloseReason closeReason);

    /**
     * Template 23. {@code flags} is 0 when the field holds its null value (A0:BLA:225-226).
     *
     * @return true to end the current poll right after this frame (shadow trigger)
     */
    boolean onClusterAction(long leadershipTermId, long logPosition, long timestamp, ClusterAction action, int flags);

    /** Template 24; {@code timeUnit} mapped as {@code ClusterClock.map}. */
    void onNewLeadershipTermEvent(long leadershipTermId, long logPosition, long timestamp, long termBaseLogPosition,
                                  int leaderMemberId, int logSessionId, TimeUnit timeUnit, int appVersion);

    /**
     * A gate in front of this listener dropped the frame ({@link SkipWindow#gate}). Not called by the reader. A member
     * still advances its position and cluster time for such a frame (CSA sets both before the service, whose SKIP gate
     * then drops the engine call), so a {@link ReplayContainer} does the same here. A decorator must forward it; a
     * listener that ignores skips by design overrides it with an explicit no-op. The default refuses, so a decorator
     * that forgets to forward cannot silently stop the cursor.
     */
    default void onSkipped(final FrameKind kind, final long logPosition, final long timestamp) {
        throw new UnsupportedOperationException("onSkipped not handled by " + getClass().getName());
    }
}
