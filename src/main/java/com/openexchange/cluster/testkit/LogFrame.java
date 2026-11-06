// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.FrameKind;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * One delivered log entry, immutable: the kind, the frame-end position and every argument the reader passed. Unused
 * fields are {@code -1} / {@code null}. {@code data} is the message payload (SESSION_MESSAGE) or the encoded principal
 * (SESSION_OPEN); it is copied in and out.
 */
public record LogFrame(FrameKind kind, long position, long leadershipTermId, long clusterSessionId, long timestamp,
                       long correlationId, int responseStreamId, String responseChannel, byte[] data,
                       CloseReason closeReason, ClusterAction action, int flags, long termBaseLogPosition,
                       int leaderMemberId, int logSessionId, TimeUnit timeUnit, int appVersion) {

    public LogFrame {
        Objects.requireNonNull(kind, "kind");
        data = data == null ? null : data.clone();
    }

    public static LogFrame message(final long position, final long clusterSessionId, final long timestamp,
                                   final byte[] payload) {
        return new LogFrame(FrameKind.SESSION_MESSAGE, position, -1, clusterSessionId, timestamp, -1, -1, null,
                Objects.requireNonNull(payload, "payload"), null, null, -1, -1, -1, -1, null, -1);
    }

    public static LogFrame timer(final long position, final long correlationId, final long timestamp) {
        return new LogFrame(FrameKind.TIMER_EVENT, position, -1, -1, timestamp, correlationId, -1, null, null, null,
                null, -1, -1, -1, -1, null, -1);
    }

    public static LogFrame open(final long leadershipTermId, final long position, final long clusterSessionId,
                                final long timestamp, final int responseStreamId, final String responseChannel,
                                final byte[] encodedPrincipal) {
        return new LogFrame(FrameKind.SESSION_OPEN, position, leadershipTermId, clusterSessionId, timestamp, -1,
                responseStreamId, responseChannel, Objects.requireNonNull(encodedPrincipal, "encodedPrincipal"),
                null, null, -1, -1, -1, -1, null, -1);
    }

    public static LogFrame close(final long leadershipTermId, final long position, final long clusterSessionId,
                                 final long timestamp, final CloseReason closeReason) {
        return new LogFrame(FrameKind.SESSION_CLOSE, position, leadershipTermId, clusterSessionId, timestamp, -1, -1,
                null, null, Objects.requireNonNull(closeReason, "closeReason"), null, -1, -1, -1, -1, null, -1);
    }

    public static LogFrame action(final long leadershipTermId, final long position, final long timestamp,
                                  final ClusterAction action, final int flags) {
        return new LogFrame(FrameKind.CLUSTER_ACTION, position, leadershipTermId, -1, timestamp, -1, -1, null, null,
                null, Objects.requireNonNull(action, "action"), flags, -1, -1, -1, null, -1);
    }

    public static LogFrame term(final long leadershipTermId, final long position, final long timestamp,
                                final long termBaseLogPosition, final int leaderMemberId, final int logSessionId,
                                final TimeUnit timeUnit, final int appVersion) {
        return new LogFrame(FrameKind.NEW_LEADERSHIP_TERM, position, leadershipTermId, -1, timestamp, -1, -1, null,
                null, null, null, -1, termBaseLogPosition, leaderMemberId, logSessionId,
                Objects.requireNonNull(timeUnit, "timeUnit"), appVersion);
    }

    @Override
    public byte[] data() {
        return data == null ? null : data.clone();
    }

    /** The same call the reader made for this entry. */
    public boolean deliver(final ClusterLogListener listener) {
        switch (kind) {
            case SESSION_MESSAGE:
                listener.onSessionMessage(position, clusterSessionId, timestamp, new UnsafeBuffer(data()), 0,
                        data.length);
                return false;
            case TIMER_EVENT:
                listener.onTimerEvent(position, correlationId, timestamp);
                return false;
            case SESSION_OPEN:
                listener.onSessionOpen(leadershipTermId, position, clusterSessionId, timestamp, responseStreamId,
                        responseChannel, data());
                return false;
            case SESSION_CLOSE:
                listener.onSessionClose(leadershipTermId, position, clusterSessionId, timestamp, closeReason);
                return false;
            case CLUSTER_ACTION:
                return listener.onClusterAction(leadershipTermId, position, timestamp, action, flags);
            case NEW_LEADERSHIP_TERM:
                listener.onNewLeadershipTermEvent(leadershipTermId, position, timestamp, termBaseLogPosition,
                        leaderMemberId, logSessionId, timeUnit, appVersion);
                return false;
            default:
                throw new IllegalStateException("not a delivered kind: " + kind);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogFrame f)) {
            return false;
        }
        return kind == f.kind && position == f.position && leadershipTermId == f.leadershipTermId
                && clusterSessionId == f.clusterSessionId && timestamp == f.timestamp
                && correlationId == f.correlationId && responseStreamId == f.responseStreamId
                && Objects.equals(responseChannel, f.responseChannel) && Arrays.equals(data, f.data)
                && closeReason == f.closeReason && action == f.action && flags == f.flags
                && termBaseLogPosition == f.termBaseLogPosition && leaderMemberId == f.leaderMemberId
                && logSessionId == f.logSessionId && timeUnit == f.timeUnit && appVersion == f.appVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, position, clusterSessionId, timestamp, correlationId, Arrays.hashCode(data));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder().append(kind).append('@').append(position);
        switch (kind) {
            case SESSION_MESSAGE -> sb.append(" id=").append(clusterSessionId).append(" ts=").append(timestamp)
                    .append(" len=").append(data.length).append(" payload=").append(hexPrefix(data));
            case TIMER_EVENT -> sb.append(" corr=").append(correlationId).append(" ts=").append(timestamp);
            case SESSION_OPEN -> sb.append(" term=").append(leadershipTermId).append(" id=").append(clusterSessionId)
                    .append(" ts=").append(timestamp).append(" stream=").append(responseStreamId)
                    .append(" channel=").append(responseChannel).append(" principal=").append(hexPrefix(data));
            case SESSION_CLOSE -> sb.append(" term=").append(leadershipTermId).append(" id=").append(clusterSessionId)
                    .append(" ts=").append(timestamp).append(" reason=").append(closeReason);
            case CLUSTER_ACTION -> sb.append(" term=").append(leadershipTermId).append(" ts=").append(timestamp)
                    .append(' ').append(action).append(" flags=").append(flags);
            case NEW_LEADERSHIP_TERM -> sb.append(" term=").append(leadershipTermId).append(" ts=").append(timestamp)
                    .append(" base=").append(termBaseLogPosition).append(" leader=").append(leaderMemberId)
                    .append(" logSession=").append(logSessionId).append(' ').append(timeUnit)
                    .append(" app=").append(appVersion);
            default -> { }
        }
        return sb.toString();
    }

    private static String hexPrefix(final byte[] bytes) {
        final int n = Math.min(bytes.length, 16);
        return HexFormat.of().formatHex(bytes, 0, n) + (bytes.length > n ? "..." : "");
    }
}
