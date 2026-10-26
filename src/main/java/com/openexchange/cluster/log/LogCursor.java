// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

/**
 * The log-derived values a replayer holds after a frame: its end position, its timestamp (cluster time), and the term
 * id and term base of the last {@code NewLeadershipTermEvent}. {@code -1} ({@code Aeron.NULL_VALUE}) means not seen.
 */
public record LogCursor(long logPosition, long clusterTime, long leadershipTermId, long termBaseLogPosition) {

    /** Before any frame: position 0, everything else unknown. */
    public static final LogCursor GENESIS = new LogCursor(0, -1L, -1L, -1L);

    public LogCursor {
        if (logPosition < 0) {
            throw new IllegalArgumentException("logPosition must be >= 0: " + logPosition);
        }
    }
}
