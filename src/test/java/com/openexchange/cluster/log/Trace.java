// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import com.openexchange.cluster.testkit.LogFrame;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;

/** Test listener: records every delivered entry as a {@link LogFrame}; can throw at a position or BREAK on actions. */
final class Trace implements ClusterLogListener {

    final List<LogFrame> frames = new ArrayList<>();
    final List<String> skipped = new ArrayList<>();
    LongPredicate throwAt = p -> false;
    boolean breakOnAction;
    RuntimeException thrown;

    private void check(final long position) {
        if (throwAt.test(position)) {
            thrown = new IllegalStateException("listener refuses frame at " + position);
            throw thrown;
        }
    }

    @Override
    public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                 final DirectBuffer buffer, final int offset, final int length) {
        check(logPosition);
        final byte[] payload = new byte[length];
        buffer.getBytes(offset, payload);
        frames.add(LogFrame.message(logPosition, clusterSessionId, timestamp, payload));
    }

    @Override
    public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
        check(logPosition);
        frames.add(LogFrame.timer(logPosition, correlationId, timestamp));
    }

    @Override
    public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                              final long timestamp, final int responseStreamId, final String responseChannel,
                              final byte[] encodedPrincipal) {
        check(logPosition);
        frames.add(LogFrame.open(leadershipTermId, logPosition, clusterSessionId, timestamp, responseStreamId,
                responseChannel, encodedPrincipal));
    }

    @Override
    public void onSessionClose(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                               final long timestamp, final CloseReason closeReason) {
        check(logPosition);
        frames.add(LogFrame.close(leadershipTermId, logPosition, clusterSessionId, timestamp, closeReason));
    }

    @Override
    public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                   final ClusterAction action, final int flags) {
        check(logPosition);
        frames.add(LogFrame.action(leadershipTermId, logPosition, timestamp, action, flags));
        return breakOnAction;
    }

    @Override
    public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                         final long termBaseLogPosition, final int leaderMemberId,
                                         final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
        check(logPosition);
        frames.add(LogFrame.term(leadershipTermId, logPosition, timestamp, termBaseLogPosition, leaderMemberId,
                logSessionId, timeUnit, appVersion));
    }

    @Override
    public void onSkipped(final FrameKind kind, final long logPosition, final long timestamp) {
        skipped.add(kind + "@" + logPosition);
    }

    List<Long> positions() {
        final List<Long> positions = new ArrayList<>();
        for (final LogFrame f : frames) {
            positions.add(f.position());
        }
        return positions;
    }
}
