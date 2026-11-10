// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.shadow.ShadowStub;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Trust needs a byte-equal alignment. Follower thread only.
 *
 * <p>After any start that is not a full replay from genesis (a restore from a local shadow, a restart that waits for L's descriptor) the
 * replica is UNVERIFIED: {@code /ready} reads 503 {@code unverified continuation} and no STANDBY or LOCAL shadow is
 * written. At every ALIGNED trigger the policy holds the serialized image at P_cm and this class compares it with the
 * member service snapshot the backup retrieved at the same position:</p>
 * <pre>
 * EQUAL         the replica becomes (or stays) VERIFIED; the held ALIGNED shadow is written
 * DIFFERENT     FATAL replica: state at P_cm differs from member snapshot (history mismatch or divergence); wipe BASE_DIR
 * PENDING       not retrieved yet (an UNVERIFIED replica is then bounded by the stall clock below, a VERIFIED
 *               one drops that one shadow after REPLICA_VERIFY_WAIT_MS, CRITICAL)
 * AMBIGUOUS     two valid member entries at P_cm with different leadership terms: never compared, not ready
 * UNVERIFIABLE  the backup retrieved a newer member snapshot and none at P_cm (it only fetches the latest per query):
 *               the held image is dropped, CRITICAL, the next ALIGNED is compared; never fenced
 * </pre>
 * <p>The member payload is looked up by P_cm AND the trigger's leadership term id; a single entry of another term
 * is no match (never compared).</p>
 * <p>The compare runs at EVERY ALIGNED trigger, so a VERIFIED replica that later diverges also fences. A member
 * snapshot in stub mode (RCS_MODE=shadow) carries no state: its genesis, term and P_cm are compared instead (a
 * weaker, identity-only alignment, logged as such).</p>
 *
 * <p>While UNVERIFIED and nothing is held, {@link #tick} also fences when the backup retrieved a member snapshot at a
 * position this replica already applied without an ALIGNED trigger there (its log has no SNAPSHOT action at that
 * position: another history).</p>
 *
 * <p>{@code REPLICA_VERIFY_WAIT_MS} is a STALL bound, not an elapsed-time bound ({@link #stall}). While
 * UNVERIFIED the clock runs only while a member snapshot newer than the restore point exists (named by a backup
 * response, retrieved, or the held ALIGNED) and the replica makes no progress toward verification: its applied position
 * did not advance below that snapshot (a stalled catch-up, including a replay refused with INVALID_POSITION), or it
 * reached that snapshot and the payload is still not retrieved (live following past it is not progress). An advance
 * below the snapshot resets the clock, so a lagging restart whose catch-up takes longer than the bound is never fenced.
 * Expiry: {@code FATAL replica: unverified continuation at <applied> (history mismatch or a stalled backup); wipe
 * BASE_DIR}. A byte mismatch keeps the state-differs text.</p>
 *
 * <p>A member snapshot retrieval whose bytes are still arriving ({@link MemberSnapshots#retrievalProgress}) is
 * progress and restarts the clock; reaching a target restarts it once per target POSITION (a newer target reached at
 * once restarts it too); an ambiguity is cleared when its held image is dropped, superseded or turns
 * UNVERIFIABLE.</p>
 */
public final class AlignmentTrust {

    public enum Verdict { PENDING, EQUAL, DIFFERENT, UNVERIFIABLE, AMBIGUOUS }

    /** What the backup retrieved: its RecordingLog's service snapshots and their payloads in the local archive. */
    public interface MemberSnapshots {
        /** Valid retrieved service snapshot positions, ascending; empty when none. */
        long[] positions();

        /**
         * The reassembled service payload (the bytes a member's {@code onTakeSnapshot} wrote) of the retrieved snapshot
         * at {@code position}, or null while it is not retrieved or still recording. Throws when it cannot be read.
         */
        DirectBuffer payload(long position);

        /**
         * The payload of the retrieved snapshot at {@code position} of leadership term {@code leadershipTermId}
         * ({@code -1}: any), or null as {@link #payload(long)}. Implementations that know the terms override it.
         */
        default DirectBuffer payload(final long position, final long leadershipTermId) {
            return payload(position);
        }

        /**
         * The distinct leadership term ids of the valid entries at {@code position}, ascending; null when the
         * source does not know the terms (then the lookup is by position only).
         */
        default long[] termsAt(final long position) {
            return null;
        }

        /**
         * The newest member service snapshot position a backup response named, retrieved or not (a failed backup
         * cycle never records it); -1 when none.
         */
        default long newestReported() {
            return -1;
        }

        /**
         * A monotone measure of the member snapshot retrieval in progress (the bytes recorded so far by
         * the backup's active snapshot replications), -1 when the source cannot tell. Any change between two passes
         * is progress toward verification and restarts the stall clock.
         */
        default long retrievalProgress() {
            return -1;
        }
    }

    /**
     * The cluster-identity view of the newest member snapshot a backup response named ({@link GenesisGuard}).
     */
    public interface NamedCopy {
        /** True while the newest named member snapshot has no usable local copy (not retrieved, partial, undecodable). */
        boolean unusable();

        /** Genesis decodes of a named copy that completed (a completed compare is progress toward verification). */
        long completedCompares();
    }

    /** The pace of RecordingLog and archive reads while waiting (a poll pace, not a bound). */
    static final long CHECK_PACE_NS = TimeUnit.MILLISECONDS.toNanos(200);
    private static final int TRIGGERS_KEPT = 64;

    private final MemberSnapshots snapshots;
    private final NanoClock clock;
    private final long verifyWaitNs;
    private final int engineId;
    private final int clusterId;
    private final boolean compareOff;
    private final ArrayDeque<Long> alignedTriggers = new ArrayDeque<>();

    private volatile boolean trusted;
    private volatile String detail = "unverified continuation: not started";
    private long fromPosition = -1;
    /** The stall clock (-1 when not running), the last applied position seen, the cached newest member snapshot. */
    private long stallSinceNs = -1;
    private long lastApplied = -1;
    private long newestTarget = -1;
    /** The target position the replica had reached at the previous pass, -1 when it had reached none. */
    private long reachedTarget = -1;
    /**
     * The highest retrieval progress seen for {@link #retrievalTarget}; bytes count as progress only past it, so a
     * retrieval that restarts from zero every backup cycle is not progress.
     */
    private long retrievalHigh = -1;
    private long retrievalTarget = -1;
    /** The newest target the replica was seen BELOW (it must advance to it for its reach to restart the clock). */
    private long targetSeenBelow = -1;
    private volatile long retrievalResetsTotal;
    /** The no-usable-named-copy CRITICAL lines logged (a normal restart logs none). */
    private volatile long namedCopyCriticalTotal;
    /** One backup cycle; the named-copy clock's last progress (entry, new retrieval maximum, completed compare). */
    private long backupCycleNs;
    private long namedProgressNs;
    private boolean namedCriticalLogged;
    private volatile NamedCopy namedCopy;
    /** The unusable-named-copy clock is running (entered), and the completed compares seen at the last pass. */
    private boolean namedClockSeen;
    private long namedComparesSeen;
    private long nextTargetCheckNs;
    private long nextCheckNs;
    /** Non-null while the last lookup found two member entries at one position (not ready). */
    private volatile String ambiguity;
    /** The position {@link #ambiguity} is about. */
    private long ambiguityAt = -1;
    private String lastTermMismatch;

    private volatile long comparesTotal;
    private volatile long equalTotal;
    private volatile long unverifiableTotal;
    private volatile long supersededTotal;
    private volatile long stubComparesTotal;
    private volatile long ambiguousTotal;
    private volatile long compareMsLast = -1;
    private volatile long verifiedAt = -1;

    public AlignmentTrust(final MemberSnapshots snapshots, final NanoClock clock, final long verifyWaitMs,
                          final int engineId, final int clusterId, final boolean compareOff) {
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (verifyWaitMs <= 0) {
            throw new IllegalArgumentException("REPLICA_VERIFY_WAIT_MS must be > 0: " + verifyWaitMs);
        }
        this.verifyWaitNs = TimeUnit.MILLISECONDS.toNanos(verifyWaitMs);
        this.backupCycleNs = verifyWaitNs / 2;
        this.engineId = engineId;
        this.clusterId = clusterId;
        this.compareOff = compareOff;
    }

    /**
     * After bootstrap. {@code fullReplay} is a GENESIS start (every frame applied from position 0). Under the
     * {@code ALIGN_COMPARE_OFF} control every start is trusted.
     */
    public void start(final boolean fullReplay, final long from) {
        fromPosition = from;
        lastApplied = from;
        trusted = fullReplay || compareOff;
        detail = trusted ? null : "unverified continuation: restored at " + from
                + ", waiting for the first ALIGNED compare with a member snapshot";
        if (compareOff) {
            System.err.println("[REPLICA] NC ALIGN_COMPARE_OFF: trusted at once from " + from
                    + "; ALIGNED shadows are written without the member compare");
        }
    }

    /**
     * One backup cycle ({@code REPLICA_BACKUP_QUERY_INTERVAL_MS + REPLICA_BACKUP_COOLDOWN_MS}, from
     * the running settings). The no-usable-named-copy message turns CRITICAL only after one cycle with no new retrieval maximum and no
     * completed compare. Default until set: half of {@code REPLICA_VERIFY_WAIT_MS} (the bound covers two cycles).
     */
    public void backupCycle(final long ms) {
        if (ms <= 0) {
            throw new IllegalArgumentException("backup cycle must be > 0 ms: " + ms);
        }
        this.backupCycleNs = TimeUnit.MILLISECONDS.toNanos(ms);
    }

    /** The named-copy view the stall clock reads (null: none, the stall and retrieval rules only). */
    public void namedCopy(final NamedCopy probe) {
        this.namedCopy = probe;
    }

    public boolean trusted() {
        return trusted;
    }

    /** What readiness reads: VERIFIED and no ambiguity pending. */
    public boolean ready() {
        return trusted && ambiguity == null;
    }

    public boolean compareOff() {
        return compareOff;
    }

    /** Null when {@link #ready}, else the readiness detail. */
    public String detail() {
        final String a = ambiguity;
        return a != null ? a : detail;
    }

    public long verifyWaitNs() {
        return verifyWaitNs;
    }

    /** An ALIGNED trigger at {@code position} (its image is now held). */
    void held(final long position) {
        alignedTriggers.addLast(position);
        while (alignedTriggers.size() > TRIGGERS_KEPT) {
            alignedTriggers.removeFirst();
        }
        nextCheckNs = 0; // the retrieval may already be there (a replica behind its members)
        if (!trusted) {
            detail = "unverified continuation: ALIGNED at " + position + " held, waiting for the member snapshot";
        }
    }

    void superseded(final long position) {
        clearAmbiguity(position);
        supersededTotal++;
        System.err.println("[REPLICA] CRITICAL: the held ALIGNED image at " + position + " was superseded by a newer"
                + " ALIGNED before its member snapshot was retrieved: that shadow is not written");
    }

    /**
     * Compares the held image at {@code position} with the retrieved member snapshot there. Reads at most every
     * {@link #CHECK_PACE_NS} unless {@code now} is set.
     */
    Verdict compare(final long position, final long leadershipTermId, final long genesisId, final DirectBuffer image,
                    final int length, final boolean now) {
        final long t = clock.nanoTime();
        if (!now && t - nextCheckNs < 0) {
            return Verdict.PENDING;
        }
        nextCheckNs = t + CHECK_PACE_NS;
        final long[] positions = snapshots.positions();
        final long newest = positions.length == 0 ? -1 : positions[positions.length - 1];
        if (!contains(positions, position)) {
            return newest > position ? unverifiable(position, newest) : Verdict.PENDING;
        }
        final long[] terms = snapshots.termsAt(position);
        if (terms != null && terms.length > 1) {
            return ambiguous(position, leadershipTermId, terms);
        }
        if (terms != null && terms.length == 1 && leadershipTermId >= 0 && terms[0] != leadershipTermId) {
            final String text = "the member snapshot at " + position + " is of term " + terms[0] + ", the ALIGNED"
                    + " trigger of term " + leadershipTermId + ": no match, never compared";
            if (!text.equals(lastTermMismatch)) {
                lastTermMismatch = text;
                System.err.println("[REPLICA] CRITICAL: " + text);
            }
            return newest > position ? unverifiable(position, newest) : Verdict.PENDING;
        }
        final DirectBuffer member = snapshots.payload(position, leadershipTermId);
        if (member == null) {
            return newest > position ? unverifiable(position, newest) : Verdict.PENDING;
        }
        comparesTotal++;
        final long t0 = clock.nanoTime();
        final String mismatch = mismatch(position, leadershipTermId, genesisId, image, length, member);
        compareMsLast = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - t0);
        if (mismatch != null) {
            System.err.println("[REPLICA] CRITICAL: alignment compare at " + position + ": " + mismatch);
            return Verdict.DIFFERENT;
        }
        equalTotal++;
        verifiedAt = position;
        if (!trusted) {
            System.out.println("[REPLICA] alignment: the state at " + position + " equals the member snapshot ("
                    + length + " B): VERIFIED continuation from " + fromPosition);
        }
        trusted = true;
        detail = null;
        ambiguity = null;
        ambiguityAt = -1;
        stallSinceNs = -1;
        return Verdict.EQUAL;
    }

    private Verdict ambiguous(final long position, final long term, final long[] terms) {
        final String text = "ambiguous member snapshot at " + position + ": " + terms.length + " valid entries of"
                + " terms " + java.util.Arrays.toString(terms) + " (trigger term " + term + "), never compared";
        if (!text.equals(ambiguity)) {
            ambiguousTotal++;
            System.err.println("[REPLICA] CRITICAL: " + text + "; not ready");
        }
        ambiguity = text;
        ambiguityAt = position;
        return Verdict.AMBIGUOUS;
    }

    private Verdict unverifiable(final long position, final long newest) {
        clearAmbiguity(position);
        unverifiableTotal++;
        System.err.println("[REPLICA] CRITICAL: no member snapshot at " + position + " can be retrieved (the backup"
                + " holds a newer one at " + newest + "): that ALIGNED shadow is not written; the next one is compared");
        if (!trusted) {
            detail = "unverified continuation: the member snapshot at " + position + " was not retrieved (newest "
                    + newest + "), waiting for the next ALIGNED";
        }
        return Verdict.UNVERIFIABLE;
    }

    /** The held image at {@code position} was dropped (a VERIFIED replica's verify wait). */
    void heldDropped(final long position) {
        clearAmbiguity(position);
    }

    /**
     * An ambiguity is about one held image; it is cleared when that image is dropped, superseded or
     * its position turns UNVERIFIABLE, so it never keeps the replica not ready after the image is gone.
     */
    private void clearAmbiguity(final long position) {
        if (ambiguity != null && ambiguityAt == position) {
            System.out.println("[REPLICA] ambiguity at " + position + " cleared: its held image is gone");
            ambiguity = null;
            ambiguityAt = -1;
        }
    }

    /** Null when the member payload aligns with the held image, else why not. */
    private String mismatch(final long position, final long term, final long genesisId, final DirectBuffer image,
                            final int length, final DirectBuffer member) {
        final int memberLength = member.capacity();
        if (memberLength == ShadowStub.ENVELOPED_LENGTH && SnapshotEnvelope.isEnveloped(member, memberLength)
                && ShadowStub.peekTag(member, SnapshotEnvelope.payloadOffset()) == ShadowStub.STUB_TAG) {
            stubComparesTotal++;
            final ShadowStub.Decoded d = ShadowStub.openEnveloped(new SnapshotEnvelope(), member, memberLength,
                    engineId, clusterId);
            if (!d.ok()) {
                return "the member snapshot is a refused stub: " + d.refusal();
            }
            final ShadowStub s = d.stub();
            System.out.println("[REPLICA] alignment: the member snapshot at " + position + " is a stub (RCS_MODE=shadow):"
                    + " identity-only alignment (genesis, term, P_cm), no state bytes to compare");
            if (s.genesisId() != genesisId || s.logPosition() != position || s.leadershipTermId() != term) {
                return "stub genesis=" + s.genesisId() + " term=" + s.leadershipTermId() + " P_cm=" + s.logPosition()
                        + " vs replica genesis=" + genesisId + " term=" + term + " P_cm=" + position;
            }
            return null;
        }
        if (memberLength != length) {
            return "replica image " + length + " B, member snapshot " + memberLength + " B";
        }
        final UnsafeBuffer mine = new UnsafeBuffer(image, 0, length);
        if (mine.compareTo(member) != 0) {
            int at = 0;
            while (at < length && mine.getByte(at) == member.getByte(at)) {
                at++;
            }
            return "first difference at byte " + at + " of " + length;
        }
        return null;
    }

    /**
     * Nothing held. UNVERIFIED only: the fence text when the member snapshots prove another history (a position this
     * replica applied past without a SNAPSHOT action there), else null. The stall bound is {@link #stall}.
     */
    String tick(final long appliedPosition) {
        if (trusted) {
            return null;
        }
        final long t = clock.nanoTime();
        if (t - nextCheckNs < 0) {
            return null;
        }
        nextCheckNs = t + CHECK_PACE_NS;
        final long reported = snapshots.newestReported();
        if (reported > fromPosition) {
            if (reported <= appliedPosition && !alignedTriggers.contains(reported)) {
                return differsText(reported) + " (this replica applied past " + reported + " without a SNAPSHOT"
                        + " action there, applied " + appliedPosition + ")";
            }
        }
        for (final long q : snapshots.positions()) {
            if (q <= fromPosition) {
                continue;
            }
            if (q <= appliedPosition && !alignedTriggers.contains(q)) {
                return differsText(q) + " (this replica applied past " + q + " without a SNAPSHOT action there,"
                        + " applied " + appliedPosition + ")";
            }
        }
        return null;
    }

    /**
     * Once per follower pass while UNVERIFIED (held or not): the stall clock. {@code heldPosition} is the held
     * ALIGNED's position, -1 when none. Returns the stall fence text when the clock ran out, else null.
     */
    String stall(final long appliedPosition, final long heldPosition) {
        if (trusted) {
            stallSinceNs = -1;
            return null;
        }
        final long t = clock.nanoTime();
        final boolean paced = t - nextTargetCheckNs >= 0;
        boolean retrieving = false;
        if (paced) {
            nextTargetCheckNs = t + CHECK_PACE_NS;
            long newest = snapshots.newestReported();
            final long[] positions = snapshots.positions();
            if (positions.length > 0) {
                newest = Math.max(newest, positions[positions.length - 1]);
            }
            newestTarget = Math.max(newestTarget, newest);
        }
        final long target = Math.max(newestTarget, heldPosition);
        if (paced) {
            // Bytes of a member snapshot retrieval are progress toward verification only past the highest
            // value seen for this target (a retrieval restarting from zero each cycle is not progress)
            if (target != retrievalTarget) {
                retrievalTarget = target;
                retrievalHigh = -1;
            }
            final long progress = snapshots.retrievalProgress();
            if (progress >= 0) {
                retrieving = retrievalHigh >= 0 && progress > retrievalHigh;
                retrievalHigh = Math.max(retrievalHigh, progress);
            }
        }
        final boolean advanced = appliedPosition > lastApplied;
        if (advanced) {
            lastApplied = appliedPosition;
        }
        final NamedCopy named = namedCopy;
        if (named != null && named.unusable()) {
            return unusableNamedCopy(named, t, appliedPosition, target, retrieving, paced);
        }
        namedClockSeen = false;
        if (target <= fromPosition) {
            stallSinceNs = -1; // no member snapshot newer than the restore point: nothing to verify against yet
            reachedTarget = -1;
            return null;
        }
        final boolean reached = appliedPosition >= target;
        if (!reached) {
            targetSeenBelow = target;
        }
        // Reaching a target restarts the clock once per target position, and only a target the replica
        // was below and advanced to; a newer target reached at once (a live-following replica under a wedged backup)
        // is no progress toward verification
        final boolean caughtUp = reached && reachedTarget != target && targetSeenBelow == target;
        if ((advanced && !reached) || caughtUp || retrieving) {
            stallSinceNs = t; // progress toward the member snapshot, reaching it, or its retrieval moving
            if (retrieving) {
                retrievalResetsTotal++;
            }
        } else if (stallSinceNs < 0) {
            stallSinceNs = t;
        }
        reachedTarget = reached ? target : -1;
        if (paced) {
            detail = reached
                    ? "unverified continuation: reached the member snapshot at " + target + ", waiting for its payload"
                    + (retrieving ? " (retrieval in progress)" : "") + " (applied " + appliedPosition + ")"
                    : "unverified continuation: catching up to the member snapshot at " + target + " (applied "
                    + appliedPosition + ", restored at " + fromPosition + ")";
        }
        final long stalledNs = t - stallSinceNs;
        if (stalledNs < verifyWaitNs) {
            return null;
        }
        System.err.println("[REPLICA] CRITICAL: verification stall: no progress toward verification for "
                + TimeUnit.NANOSECONDS.toMillis(stalledNs) + " ms (REPLICA_VERIFY_WAIT_MS="
                + TimeUnit.NANOSECONDS.toMillis(verifyWaitNs) + "): restored at " + fromPosition + ", applied "
                + appliedPosition + ", newest member snapshot " + target + (reached
                ? " reached, its payload not retrieved and no retrieval bytes arriving"
                : " not reached, the applied position did not advance"));
        return stallText(appliedPosition);
    }

    /**
     * The newest named member snapshot has no usable local copy (not retrieved, a partial copy left by
     * a backup reset during SNAPSHOT_RETRIEVE, or a copy that does not decode). The stall clock runs whatever the named
     * snapshot's position relative to the restore point; progress is only a newly passed retrieval maximum or a
     * completed compare. The applied position advancing is no progress here (the replica may be live-following a log
     * it cannot verify).
     */
    private String unusableNamedCopy(final NamedCopy named, final long t, final long appliedPosition,
                                     final long target, final boolean retrieving, final boolean paced) {
        final long compares = named.completedCompares();
        final boolean compared = namedClockSeen && compares != namedComparesSeen;
        if (!namedClockSeen) {
            namedClockSeen = true;
            namedProgressNs = t;
            namedCriticalLogged = false;
            // A named snapshot still in retrieval is the healthy path (INFO); CRITICAL only without progress
            System.out.println("[REPLICA] the newest member snapshot named by the backup has no usable copy yet"
                    + " (restored at " + fromPosition + ", applied " + appliedPosition + "): the stall clock runs"
                    + " (REPLICA_VERIFY_WAIT_MS=" + TimeUnit.NANOSECONDS.toMillis(verifyWaitNs) + ")");
        }
        namedComparesSeen = compares;
        if (stallSinceNs < 0 || retrieving || compared) {
            stallSinceNs = t;
            if (retrieving) {
                retrievalResetsTotal++;
            }
        }
        if (retrieving || compared) {
            namedProgressNs = t;
            namedCriticalLogged = false;
        } else if (!namedCriticalLogged && t - namedProgressNs >= backupCycleNs) {
            namedCriticalLogged = true;
            namedCopyCriticalTotal++;
            System.err.println("[REPLICA] CRITICAL: the newest member snapshot named by the backup has no usable"
                    + " copy and no progress for " + TimeUnit.NANOSECONDS.toMillis(t - namedProgressNs) + " ms (one"
                    + " backup cycle, REPLICA_BACKUP_QUERY_INTERVAL_MS + REPLICA_BACKUP_COOLDOWN_MS = "
                    + TimeUnit.NANOSECONDS.toMillis(backupCycleNs) + " ms): no new retrieval maximum and no completed"
                    + " compare (restored at " + fromPosition + ", applied " + appliedPosition + "); fenced after"
                    + " REPLICA_VERIFY_WAIT_MS=" + TimeUnit.NANOSECONDS.toMillis(verifyWaitNs) + " without progress");
        }
        reachedTarget = appliedPosition >= target ? target : -1;
        if (paced) {
            detail = "unverified continuation: the member snapshot named by the backup has no usable copy yet"
                    + (retrieving ? " (retrieval in progress)" : "") + " (applied " + appliedPosition
                    + ", restored at " + fromPosition + ")";
        }
        final long stalledNs = t - stallSinceNs;
        if (stalledNs < verifyWaitNs) {
            return null;
        }
        System.err.println("[REPLICA] CRITICAL: verification stall (no usable named copy): no progress toward verification for "
                + TimeUnit.NANOSECONDS.toMillis(stalledNs) + " ms (REPLICA_VERIFY_WAIT_MS="
                + TimeUnit.NANOSECONDS.toMillis(verifyWaitNs) + "): restored at " + fromPosition + ", applied "
                + appliedPosition + ", the newest named member snapshot has no usable copy and no retrieval bytes"
                + " past their maximum arrived");
        return stallText(appliedPosition);
    }

    /** The stall fence text (a re-genesis fences through it). */
    static String stallText(final long appliedPosition) {
        return "FATAL replica: unverified continuation at " + appliedPosition + " (history mismatch or a stalled"
                + " backup); wipe BASE_DIR";
    }

    static String differsText(final long position) {
        return "FATAL replica: state at " + position + " differs from member snapshot (history mismatch or"
                + " divergence); wipe BASE_DIR";
    }

    static boolean contains(final long[] positions, final long p) {
        for (final long q : positions) {
            if (q == p) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------------------------------------ metrics

    public long comparesTotal() {
        return comparesTotal;
    }

    /** The no-usable-named-copy CRITICAL lines logged by this process. */
    public long namedCopyCriticalTotal() {
        return namedCopyCriticalTotal;
    }

    public long equalTotal() {
        return equalTotal;
    }

    public long unverifiableTotal() {
        return unverifiableTotal;
    }

    public long supersededTotal() {
        return supersededTotal;
    }

    public long stubComparesTotal() {
        return stubComparesTotal;
    }

    /** Lookups that found two member entries of different terms at one position. */
    public long ambiguousTotal() {
        return ambiguousTotal;
    }

    /** Stall clock restarts caused by member snapshot retrieval bytes arriving. */
    public long retrievalResetsTotal() {
        return retrievalResetsTotal;
    }

    public long compareMsLast() {
        return compareMsLast;
    }

    /** The position of the last EQUAL compare, -1 before. */
    public long verifiedAt() {
        return verifiedAt;
    }
}
