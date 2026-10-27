// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

/**
 * Per-leadership-term cluster timer ids: {@code id = base + max(0, leadershipTermId) * TERM_STRIDE}.
 *
 * <p>Aeron's consensus module counts every TimerEvent it consumes while no timer with that id is pending and
 * swallows that many later schedules of the id (the count is never pruned and not snapshotted; RC1 S5 rule
 * 16, critic K3). A timer re-armed by the leader only must therefore carry a fresh id per term: a follower's
 * count for term T can never meet the id its service arms when it leads term T' > T. Any id at or above a
 * base (and below the next kind's base) is that kind; the term is recoverable from the id.</p>
 */
public final class TermTimerId {

    public static final long TERM_STRIDE = 1_000_000L;

    private TermTimerId() {
    }

    /** The id of a timer kind {@code base} for {@code leadershipTermId} (a negative term counts as 0). */
    public static long of(final long base, final long leadershipTermId) {
        return base + Math.max(0L, leadershipTermId) * TERM_STRIDE;
    }

    /** Is {@code id} of the kind starting at {@code base} (and below {@code nextBase}, exclusive)? */
    public static boolean isKind(final long id, final long base, final long nextBase) {
        return id >= base && id < nextBase;
    }

    /** Is {@code id} of the kind starting at {@code base}, the highest kind in use? */
    public static boolean isKind(final long id, final long base) {
        return id >= base;
    }

    /** The leadership term an id of kind {@code base} was armed for. */
    public static long termOf(final long id, final long base) {
        return (id - base) / TERM_STRIDE;
    }
}
