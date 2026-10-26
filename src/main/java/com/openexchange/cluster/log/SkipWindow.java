// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The SKIP rule: an engine already holding the state at {@code skipThrough} must not apply an engine-mutating frame
 * (message, timer, open, close) ending at or before it again. Actions, term events and membership changes always pass:
 * the term id is transport state, and snapshot actions inside the window are suppressed by {@link ReplayContainer}.
 */
public final class SkipWindow implements SkipRule {

    private final long skipThrough;

    public SkipWindow(final long skipThrough) {
        if (skipThrough < 0) {
            throw new IllegalArgumentException("skipThrough must be >= 0: " + skipThrough);
        }
        this.skipThrough = skipThrough;
    }

    public long skipThrough() {
        return skipThrough;
    }

    @Override
    public boolean drops(final FrameKind kind, final long frameEnd) {
        return isEngineMutating(kind) && frameEnd <= skipThrough;
    }

    /** The kinds that reach an engine apply entry (O4). */
    public static boolean isEngineMutating(final FrameKind kind) {
        switch (kind) {
            case SESSION_MESSAGE:
            case TIMER_EVENT:
            case SESSION_OPEN:
            case SESSION_CLOSE:
                return true;
            default:
                return false;
        }
    }

    /**
     * A listener that forwards every frame {@code rule} does not drop and reports each dropped one through
     * {@link ClusterLogListener#onSkipped} (a container keeps its position and time, as CSA does); a dropped action
     * returns false.
     */
    public static ClusterLogListener gate(final SkipRule rule, final ClusterLogListener downstream) {
        Objects.requireNonNull(rule, "rule");
        Objects.requireNonNull(downstream, "downstream");
        return new ClusterLogListener() {
            @Override
            public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                         final DirectBuffer buffer, final int offset, final int length) {
                if (rule.drops(FrameKind.SESSION_MESSAGE, logPosition)) {
                    downstream.onSkipped(FrameKind.SESSION_MESSAGE, logPosition, timestamp);
                } else {
                    downstream.onSessionMessage(logPosition, clusterSessionId, timestamp, buffer, offset, length);
                }
            }

            @Override
            public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
                if (rule.drops(FrameKind.TIMER_EVENT, logPosition)) {
                    downstream.onSkipped(FrameKind.TIMER_EVENT, logPosition, timestamp);
                } else {
                    downstream.onTimerEvent(logPosition, correlationId, timestamp);
                }
            }

            @Override
            public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                      final long timestamp, final int responseStreamId, final String responseChannel,
                                      final byte[] encodedPrincipal) {
                if (rule.drops(FrameKind.SESSION_OPEN, logPosition)) {
                    downstream.onSkipped(FrameKind.SESSION_OPEN, logPosition, timestamp);
                } else {
                    downstream.onSessionOpen(leadershipTermId, logPosition, clusterSessionId, timestamp,
                            responseStreamId, responseChannel, encodedPrincipal);
                }
            }

            @Override
            public void onSessionClose(final long leadershipTermId, final long logPosition,
                                       final long clusterSessionId, final long timestamp,
                                       final CloseReason closeReason) {
                if (rule.drops(FrameKind.SESSION_CLOSE, logPosition)) {
                    downstream.onSkipped(FrameKind.SESSION_CLOSE, logPosition, timestamp);
                } else {
                    downstream.onSessionClose(leadershipTermId, logPosition, clusterSessionId, timestamp, closeReason);
                }
            }

            @Override
            public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                           final ClusterAction action, final int flags) {
                if (rule.drops(FrameKind.CLUSTER_ACTION, logPosition)) {
                    downstream.onSkipped(FrameKind.CLUSTER_ACTION, logPosition, timestamp);
                    return false;
                }
                return downstream.onClusterAction(leadershipTermId, logPosition, timestamp, action, flags);
            }

            @Override
            public void onSkipped(final FrameKind kind, final long logPosition, final long timestamp) {
                downstream.onSkipped(kind, logPosition, timestamp);
            }

            @Override
            public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition,
                                                 final long timestamp, final long termBaseLogPosition,
                                                 final int leaderMemberId, final int logSessionId,
                                                 final TimeUnit timeUnit, final int appVersion) {
                if (rule.drops(FrameKind.NEW_LEADERSHIP_TERM, logPosition)) {
                    downstream.onSkipped(FrameKind.NEW_LEADERSHIP_TERM, logPosition, timestamp);
                } else {
                    downstream.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp,
                            termBaseLogPosition, leaderMemberId, logSessionId, timeUnit, appVersion);
                }
            }
        };
    }
}
