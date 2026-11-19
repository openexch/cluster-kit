// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * How far the log can be reclaimed: the lowest position every consumer has
 * already passed.
 *
 * <p>This replaces a boolean. Housekeeping used to ask "is every member
 * healthy?" and then either purge everything below the snapshot or purge
 * nothing. Both answers are wrong. One strands a member that was offline at
 * snapshot time; the other grows without bound, and that branch is what left the
 * core engine with an unbounded log, a full /dev/shm and a dead money path for
 * seventeen hours while every health check stayed green.</p>
 *
 * <p>A watermark has neither failure mode. It becomes conservative on its own
 * when a member falls behind and aggressive again the moment everyone catches
 * up, and it is safe at every point in between because it never passes a
 * consumer that still needs the data.</p>
 *
 * <h2>Absent is not behind</h2>
 *
 * <p>A consumer reports -1 when it does not exist or has stored nothing, and -1
 * does NOT hold the watermark at -1. A consumer that has never stored anything
 * is not one waiting for data, it is one that is not participating. Treating it
 * as a constraint would mean a deployment with no off-box archive could never
 * reclaim anything, which is precisely the shape that would make durability
 * depend on the paid half.</p>
 *
 * <h2>The bound is required, not optional</h2>
 *
 * <p>Without it a single member that is far behind holds the watermark down
 * forever, the log grows, and the disk fills anyway: the outage returns through
 * the mechanism meant to prevent it. Bounding it converts an accidental
 * disk-full into an explicit, visible decision - this member is stranded, stop
 * waiting for it, reseed it.</p>
 *
 * <p>The bound has two halves and only one of them needs a clock. A member more
 * than {@code maxLagBytes} behind is written off regardless of time, and that
 * half survives anything. The time half runs on the local clock and therefore
 * restarts when leadership moves; that only makes it more patient, never more
 * aggressive, and the byte half is the backstop underneath it.</p>
 */
public final class RetentionWatermark {

    /** 2 GiB behind the snapshot: this member is not catching up. */
    public static final long DEFAULT_MAX_LAG_BYTES = 2L << 30;

    /** Half an hour without progress: same conclusion, by the other road. */
    public static final long DEFAULT_MAX_LAG_MS = TimeUnit.MINUTES.toMillis(30);

    /**
     * A member whose last append is older than this is not reporting a current
     * position. Positions do not decay: a dead member keeps advertising the
     * number it reached before it stopped, and believing it is how a purge
     * strands the very member it was bounded to protect.
     */
    public static final long DEFAULT_STALE_APPEND_MS = 5_000;

    /** The answer, and enough of the reasoning to read it without re-deriving it. */
    public static final class Result {
        /** What to purge below. */
        public final long position;
        /** Which consumer is holding it there. */
        public final String limiter;
        /** Every input, so a surprising watermark can be read rather than reverse-engineered. */
        public final Map<String, Long> detail;
        /** Members written off: they cannot log-catch-up and have to be reseeded. */
        public final List<Integer> stranded;

        Result(final long position, final String limiter, final Map<String, Long> detail,
               final List<Integer> stranded) {
            this.position = position;
            this.limiter = limiter;
            this.detail = detail;
            this.stranded = stranded;
        }

        @Override
        public String toString() {
            return "watermark=" + position + " limiter=" + limiter + " detail=" + detail
                + (stranded.isEmpty() ? "" : " STRANDED=" + stranded);
        }
    }

    /**
     * The last position one member was seen at, and since when it has neither moved nor been seen alive and level
     * with the answering member: the start of its wait.
     */
    private static final class Laggard {
        long sinceMs;
        long position;

        Laggard(final long sinceMs, final long position) {
            this.sinceMs = sinceMs;
            this.position = position;
        }
    }

    private final long maxLagBytes;
    private final long maxLagMs;
    private final long staleAppendMs;

    /**
     * The last position each member was seen at, and since when it has been there. A fresh answer updates it; a
     * member that goes silent keeps the entry, so the floor holds at the last position this leader really saw.
     */
    private final Map<Integer, Laggard> laggards = new HashMap<>();
    private final Map<Integer, Boolean> stranded = new HashMap<>();
    private long leadershipTermId = Long.MIN_VALUE;

    public RetentionWatermark() {
        this(DEFAULT_MAX_LAG_BYTES, DEFAULT_MAX_LAG_MS, DEFAULT_STALE_APPEND_MS);
    }

    public RetentionWatermark(final long maxLagBytes, final long maxLagMs,
                              final long staleAppendMs) {
        this.maxLagBytes = maxLagBytes;
        this.maxLagMs = maxLagMs;
        this.staleAppendMs = staleAppendMs;
    }

    /**
     * Compute the watermark for one cluster, parking a member that has never reported at the snapshot (it gets the
     * whole bound from there and holds nothing until the snapshot moves on).
     *
     * @param snapshotPosition the ceiling. Recovery on each node needs everything above its
     *                         newest snapshot, so nothing above it is reclaimable whatever
     *                         the other inputs say.
     * @param positions        what the leader's consensus module answered, or null when it
     *                         did not answer. Null holds the watermark at the last known
     *                         member positions rather than advancing past members nobody
     *                         can currently see.
     * @param s3Verified       position durably stored off-box, or -1 for not participating
     * @param backupPosition   position the backup has replicated, or -1 for not participating
     * @param nowMs            local wall clock, for the time half of the bound
     */
    public Result compute(final long snapshotPosition,
                          final ClusterMemberPositions.Snapshot positions,
                          final long s3Verified, final long backupPosition, final long nowMs) {
        return compute(snapshotPosition, positions, s3Verified, backupPosition, nowMs, snapshotPosition, 0);
    }

    /**
     * Remember what a fresh answer says, between two computations. A member that stops reporting is then held at a
     * position this leader saw moments before it went silent, not at one from the last snapshot.
     *
     * @param leadershipTermId the term the answer belongs to; a new term forgets every remembered position (they
     *                         were another leader's log, or this node's view from an earlier term)
     */
    public void observe(final ClusterMemberPositions.Snapshot positions, final long leadershipTermId,
                        final long nowMs) {
        onTerm(leadershipTermId);
        if (positions == null) {
            return;
        }
        final long head = headOf(positions);
        for (final ClusterMemberPositions.Member member : positions.members) {
            if (member.memberId != positions.memberId && isFresh(member, positions.currentTimeNs)) {
                remember(member, head, nowMs);
            }
        }
    }

    /** The log position of the member that answered (the leader's own), or -1 when the answer does not carry it. */
    private static long headOf(final ClusterMemberPositions.Snapshot positions) {
        final ClusterMemberPositions.Member self = positions.member(positions.memberId);
        return self == null ? -1 : self.logPosition;
    }

    /**
     * Compute the watermark for one cluster on its leader.
     *
     * <p>Every member counts, whether it answers or not. A member heard from recently counts at the position it
     * reports. A member that has gone silent counts at the last position this leader saw it at in this leadership
     * term. A member this leader has never heard from in this term (it was already down when this node was elected)
     * counts at {@code holdPosition}: this node's own log start, which holds everything this node still has. Each
     * of them is subject to the bound: a member that has held the watermark for {@code maxLagMs}, or that is
     * {@code maxLagBytes} behind the snapshot, is written off and reported as stranded. The time runs from the last
     * moment the member moved, or was heard from while level with this leader or past the snapshot: a member that
     * was alive and keeping up through a quiet stretch and then stops gets the whole wait from when it stopped.</p>
     *
     * @param holdPosition     what a member with no known position counts as
     * @param leadershipTermId the current term; a change forgets the positions remembered in the term before
     */
    public Result compute(final long snapshotPosition,
                          final ClusterMemberPositions.Snapshot positions,
                          final long s3Verified, final long backupPosition, final long nowMs,
                          final long holdPosition, final long leadershipTermId) {
        onTerm(leadershipTermId);

        final Map<String, Long> detail = new LinkedHashMap<>();
        detail.put("snapshot", snapshotPosition);

        long position = snapshotPosition;
        String limiter = "snapshot";

        if (s3Verified >= 0) {
            detail.put("s3Verified", s3Verified);
            if (s3Verified < position) {
                position = s3Verified;
                limiter = "s3Verified";
            }
        }
        if (backupPosition >= 0) {
            detail.put("backup", backupPosition);
            if (backupPosition < position) {
                position = backupPosition;
                limiter = "backup";
            }
        }

        long slowest = -1;
        int slowestMember = -1;

        if (positions == null) {
            // No answer. Every member we have been tracking still counts against
            // us at its last known position; the bound decides when to give up.
            for (final Map.Entry<Integer, Laggard> e : laggards.entrySet()) {
                if (e.getValue().position >= snapshotPosition
                    || isStrandedByBound(e.getKey(), e.getValue(), snapshotPosition, nowMs)) {
                    continue;
                }
                if (slowest < 0 || e.getValue().position < slowest) {
                    slowest = e.getValue().position;
                    slowestMember = e.getKey();
                }
            }
        } else {
            final long head = headOf(positions);
            for (final ClusterMemberPositions.Member member : positions.members) {
                if (member.memberId == positions.memberId) {
                    continue; // the member that answered: its own log is what is being purged
                }
                final boolean trusted = isFresh(member, positions.currentTimeNs);
                final Laggard laggard;
                if (trusted) {
                    laggard = remember(member, head, nowMs);
                    if (member.logPosition >= snapshotPosition) {
                        // Caught up. Its clock resets, so a member that falls behind
                        // repeatedly gets the full bound each time instead of
                        // accumulating toward being declared stranded.
                        laggard.sinceMs = nowMs;
                        stranded.remove(member.memberId);
                        continue;
                    }
                } else {
                    // Unreadable is the dangerous case: it may be far behind and we
                    // cannot see how far. Hold at where it was last seen and let the
                    // bound decide when to write it off.
                    Laggard known = laggards.get(member.memberId);
                    if (known == null) {
                        // Never seen it report in this term: start its clock now.
                        known = new Laggard(nowMs, holdPosition);
                        laggards.put(member.memberId, known);
                    }
                    laggard = known;
                    if (laggard.position >= snapshotPosition) {
                        continue;
                    }
                }

                detail.put("member" + member.memberId, laggard.position);
                if (isStrandedByBound(member.memberId, laggard, snapshotPosition, nowMs)) {
                    continue;
                }
                if (slowest < 0 || laggard.position < slowest) {
                    slowest = laggard.position;
                    slowestMember = member.memberId;
                }
            }
        }

        if (slowest >= 0 && slowest < position) {
            position = slowest;
            limiter = "member" + slowestMember;
            detail.put("slowestMember", slowest);
        }

        final List<Integer> strandedMembers = new ArrayList<>();
        for (final Map.Entry<Integer, Boolean> e : stranded.entrySet()) {
            if (Boolean.TRUE.equals(e.getValue())) {
                strandedMembers.add(e.getKey());
            }
        }

        return new Result(position, limiter, detail, strandedMembers);
    }

    private void onTerm(final long termId) {
        if (termId != leadershipTermId) {
            leadershipTermId = termId;
            laggards.clear();
            stranded.clear();
        }
    }

    /**
     * Record a fresh report. Progress, even slow progress, restarts the clock: the bound is for a member that is
     * not catching up, not one that is behind. So does a report from a member that is level with the answering
     * member ({@code head}): on a quiet log nobody's position moves, and a member that is alive and has everything
     * is not waiting for anything. Its wait starts when it stops answering or falls behind. A member that answers
     * but stays behind the head without moving keeps its clock: that one is not catching up.
     *
     * @param head the answering member's own log position, or -1 when not known
     */
    private Laggard remember(final ClusterMemberPositions.Member member, final long head, final long nowMs) {
        Laggard laggard = laggards.get(member.memberId);
        if (laggard == null || laggard.position != member.logPosition) {
            laggard = new Laggard(nowMs, member.logPosition);
            laggards.put(member.memberId, laggard);
            stranded.remove(member.memberId);
        } else if (head >= 0 && member.logPosition >= head) {
            laggard.sinceMs = nowMs;
        }
        return laggard;
    }

    /**
     * A position is only current if the leader has heard from that member recently.
     * Both times come from the same answer, so this never compares two clocks.
     */
    private boolean isFresh(final ClusterMemberPositions.Member member, final long currentTimeNs) {
        if (member.timeOfLastAppendNs <= 0 || member.logPosition < 0) {
            // no position: a new leader resets every member to "none" until it hears an append position from it
            return false;
        }
        final long ageMs = TimeUnit.NANOSECONDS.toMillis(currentTimeNs - member.timeOfLastAppendNs);
        return ageMs >= 0 && ageMs < staleAppendMs;
    }

    /**
     * Has this member held the watermark long enough to be given up on?
     *
     * <p>Sticky until it makes progress: a stranded member that stays stranded must
     * not flap in and out of the watermark, because that would make reclamation
     * depend on which cycle the operator happened to read.</p>
     */
    private boolean isStrandedByBound(final int memberId, final Laggard laggard,
                                      final long snapshotPosition, final long nowMs) {
        if (Boolean.TRUE.equals(stranded.get(memberId))) {
            return true;
        }
        final long lagBytes = snapshotPosition - laggard.position;
        if (lagBytes >= maxLagBytes || nowMs - laggard.sinceMs >= maxLagMs) {
            stranded.put(memberId, Boolean.TRUE);
            return true;
        }
        return false;
    }

    /** Members currently written off, for status and metrics. */
    public List<Integer> strandedMembers() {
        final List<Integer> out = new ArrayList<>();
        for (final Map.Entry<Integer, Boolean> e : stranded.entrySet()) {
            if (Boolean.TRUE.equals(e.getValue())) {
                out.add(e.getKey());
            }
        }
        return out;
    }
}
