// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.shadow.ShadowStub;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Trust needs a byte-equal alignment. Follower thread only.
 *
 * <p>After any start that is not a full replay from genesis (a restore from a local shadow, a restart that waits for L's descriptor) the
 * replica is UNVERIFIED: {@code /ready} reads 503 {@code unverified continuation} and no STANDBY or LOCAL shadow is
 * written. At every ALIGNED trigger the policy holds the serialized image at P_cm and this class compares it with the
 * member service snapshot the backup retrieved at the same position:</p>
 * <pre>
 * EQUAL          the replica becomes (or stays) VERIFIED; the held ALIGNED shadow is written
 * DIFFERENT      same genesis, different bytes: the mismatch latches ({@code shadow_oracle_mismatch_total}), the
 *                replica HOLDS with 503 {@code fenced:oracle mismatch at <P>: <detail>} and dumps both images
 * OTHER_GENESIS  the member copy opened and holds another genesis: a history fence (purge, exit 1), never a hold
 * UNREADABLE     the member copy's envelope does not open (torn or incomplete, stub or full): counted, never a
 *                mismatch, never a fence; the held image stays PENDING
 * PENDING        not retrieved yet (an UNVERIFIED replica is then bounded by the stall clock below, a VERIFIED
 *                one drops that one shadow after REPLICA_VERIFY_WAIT_MS, CRITICAL); also a copy taken before the ADOPT
 *                (genesis 0), which is ignored
 * AMBIGUOUS      two valid member entries at P_cm with different leadership terms: never compared, not ready
 * UNVERIFIABLE   the backup retrieved a newer member snapshot and none at P_cm (it only fetches the latest per query):
 *                the held image is dropped, CRITICAL, the next ALIGNED is compared; never fenced
 * </pre>
 * <p>Order on a retrieved copy, one method for stub and full copies: (1) the envelope opens (else UNREADABLE; nothing
 * else is read from it); (2) a stub is compared by identity (genesis first: another genesis is OTHER_GENESIS, genesis 0
 * is ignored); (3) a full copy is compared byte for byte and, only when the bytes differ, its genesis is decoded
 * before the verdict: another genesis is OTHER_GENESIS, genesis 0 is ignored, the same genesis is DIFFERENT. An
 * equal copy holds this replica's genesis by construction (the genesis is part of the compared state), so an equal
 * compare never pays for the decode, which is a scratch restore of the member image.</p>
 * <p>The member payload is looked up by P_cm AND the trigger's leadership term id; a single entry of another term
 * is no match (never compared).</p>
 * <p>The compare runs at EVERY ALIGNED trigger, so a VERIFIED replica that later diverges also fences. A member
 * snapshot in stub mode (SNAPSHOT_MODE=shadow) carries no state: its genesis, term and P_cm are compared instead (a
 * weaker, identity-only alignment, logged as such).</p>
 *
 * <p>While nothing is held, in both trust states, {@link #tick} fences (a history fence) when the backup named or
 * retrieved a member snapshot at a position q this replica already applied without a SNAPSHOT action there:
 * {@code max(restore point, last EQUAL) < q <= applied}, q not in the action ring, and the copy not taken before the
 * ADOPT (genesis 0, when retrieved). Every member service snapshot sits on a flags-0 SNAPSHOT action the replica also
 * applies, so on one history this cannot happen. The action ring is recorded at every flags-0 action
 * ({@link #action}), before any early return of the policy (pre-ADOPT, fenced, compare off, writer not running), has
 * no fixed capacity and drops entries at or below each EQUAL position; a ring miss explained by this replica's own
 * bookkeeping (below the restore point, at or below the last EQUAL) is never a fence.</p>
 *
 * <p>Freshness: a STANDBY or LOCAL shadow needs {@link #fresh}: the newest member snapshot the backup named or
 * retrieved is at or below the last EQUAL position. A newer named snapshot suspends them until the next EQUAL at or
 * above it ({@code shadow_oracle_stale_named_total}); ALIGNED needs only its own compare.</p>
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

    public enum Verdict { PENDING, EQUAL, DIFFERENT, UNVERIFIABLE, AMBIGUOUS, UNREADABLE, OTHER_GENESIS }

    /**
     * What an operator needs to adjudicate a byte mismatch: both images (the replica's is the held buffer, never
     * overwritten after the fence), the member copy's recording and genesis, the first differing byte.
     * {@code memberGenesis} is {@link Long#MIN_VALUE} when it was not decoded.
     */
    public record MismatchEvidence(long position, long leadershipTermId, long replicaGenesis, long memberGenesis,
                                   long memberRecordingId, long newestNamed, DirectBuffer replicaImage,
                                   int replicaLength, DirectBuffer memberImage, int memberLength,
                                   int firstDifference, String detail) {
    }

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

        /** The recording id of the copy {@link #payload(long, long)} reads, -1 when unknown (logs and the dump). */
        default long recordingId(final long position, final long leadershipTermId) {
            return -1;
        }

        /**
         * The genesis a member copy (the reassembled payload, its envelope already opened) holds: a stub's own field,
         * a full image through the engine's scratch decode; 0 for a copy taken before the ADOPT. The one decoder the
         * genesis check shares. Throws when the copy does not decode.
         */
        default long genesisOf(final DirectBuffer copy, final int length) {
            throw new UnsupportedOperationException("this member snapshot source cannot decode a genesis");
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

    private final MemberSnapshots snapshots;
    private final NanoClock clock;
    private final long verifyWaitNs;
    private final int engineId;
    private final int clusterId;
    private final boolean compareOff;
    private final Set<ReplicaNegativeControl> off;
    private final SnapshotEnvelope envelope = new SnapshotEnvelope();
    /**
     * The flags-0 SNAPSHOT action positions above the last EQUAL (and the restore point), recorded at the action. No
     * fixed capacity: a capacity turns bookkeeping into a false history fence. Its size is
     * {@code shadow_align_actions_ring}; it grows only while no EQUAL happens (bounded by the stall clock while
     * UNVERIFIED, visible as verify timeouts while VERIFIED).
     */
    private final TreeSet<Long> actions = new TreeSet<>();
    private volatile int actionsSize;
    /** Test seam: the ring recorded at the held image only (the shape before it was recorded at the action). */
    volatile boolean ringAtHeldOnly;
    /** Test seam: the trusted-state applied-past check ignores the last EQUAL lower bound. */
    volatile boolean tickIgnoresVerifiedAt;

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

    private volatile long unreadableTotal;
    private volatile long otherGenesisTotal;
    private volatile long preAdoptTotal;
    private volatile long staleNamedTotal;
    private volatile long mismatchTotal;
    private volatile long mismatchAt = -1;
    private volatile String mismatchDetail;
    private volatile MismatchEvidence evidence;
    private volatile String otherGenesisText;
    /** Copies already found unreadable (by recording id, else by position): read once, never again. */
    private final Set<Long> unreadableCopies = new HashSet<>();
    /** Pre-ADOPT copies counted (by recording id, else by position). */
    private final Set<Long> preAdoptCopies = new HashSet<>();
    /** The stall text's suffix while the last verdict was UNREADABLE, else null. */
    private String lastUnreadable;
    private String lastStale;

    public AlignmentTrust(final MemberSnapshots snapshots, final NanoClock clock, final long verifyWaitMs,
                          final int engineId, final int clusterId, final boolean compareOff) {
        this(snapshots, clock, verifyWaitMs, engineId, clusterId,
                compareOff ? EnumSet.of(ReplicaNegativeControl.ALIGN_COMPARE_OFF)
                        : EnumSet.noneOf(ReplicaNegativeControl.class));
    }

    /** {@code off}: the negative controls this class reads (ALIGN_COMPARE_OFF, ENVELOPE_CHECK_OFF, GENESIS_CHECK_OFF, TICK_UNVERIFIED_ONLY). */
    public AlignmentTrust(final MemberSnapshots snapshots, final NanoClock clock, final long verifyWaitMs,
                          final int engineId, final int clusterId, final Set<ReplicaNegativeControl> off) {
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (verifyWaitMs <= 0) {
            throw new IllegalArgumentException("REPLICA_VERIFY_WAIT_MS must be > 0: " + verifyWaitMs);
        }
        this.verifyWaitNs = TimeUnit.MILLISECONDS.toNanos(verifyWaitMs);
        this.backupCycleNs = verifyWaitNs / 2;
        this.engineId = engineId;
        this.clusterId = clusterId;
        this.off = off.isEmpty() ? EnumSet.noneOf(ReplicaNegativeControl.class) : EnumSet.copyOf(off);
        this.compareOff = this.off.contains(ReplicaNegativeControl.ALIGN_COMPARE_OFF);
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

    /**
     * A flags-0 SNAPSHOT action at {@code position}, seen by the policy before any of its early returns: the action
     * ring every applied-past check reads.
     */
    void action(final long position) {
        if (!ringAtHeldOnly) {
            record(position);
        }
    }

    private void record(final long position) {
        if (position > verifiedAt) {
            actions.add(position);
            actionsSize = actions.size();
        }
    }

    /** {@code shadow_align_actions_ring}: SNAPSHOT action positions kept above the last EQUAL. */
    public int actionsRing() {
        return actionsSize;
    }

    /** An ALIGNED trigger at {@code position} (its image is now held). */
    void held(final long position) {
        if (ringAtHeldOnly) {
            record(position);
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
        final long rec = snapshots.recordingId(position, leadershipTermId);
        if (rec >= 0 && unreadableCopies.contains(rec)) {
            return Verdict.UNREADABLE; // read once and refused: a stopped recording never changes
        }
        final DirectBuffer member = snapshots.payload(position, leadershipTermId);
        if (member == null) {
            return newest > position ? unverifiable(position, newest) : Verdict.PENDING;
        }
        final int memberLength = member.capacity();
        // (1) the envelope opens, or nothing else is read from the copy: tearing is not divergence
        if (!off.contains(ReplicaNegativeControl.ENVELOPE_CHECK_OFF)) {
            try {
                envelope.open(member, memberLength);
            } catch (final IllegalStateException torn) {
                return unreadable(position, rec, torn.getMessage());
            }
        }
        lastUnreadable = null;
        comparesTotal++;
        final long t0 = clock.nanoTime();
        final Verdict v = judge(position, leadershipTermId, genesisId, image, length, member, memberLength, rec);
        compareMsLast = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - t0);
        if (v != Verdict.EQUAL) {
            return v;
        }
        equalTotal++;
        verifiedAt = position;
        actions.headSet(position, true).clear();
        actionsSize = actions.size();
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

    /** Steps (2) and (3) of the order on a copy whose envelope opened. */
    private Verdict judge(final long position, final long term, final long genesisId, final DirectBuffer image,
                          final int length, final DirectBuffer member, final int memberLength, final long rec) {
        final boolean genesisOff = off.contains(ReplicaNegativeControl.GENESIS_CHECK_OFF);
        if (memberLength == ShadowStub.ENVELOPED_LENGTH && SnapshotEnvelope.isEnveloped(member, memberLength)
                && ShadowStub.peekTag(member, SnapshotEnvelope.payloadOffset()) == ShadowStub.STUB_TAG) {
            stubComparesTotal++;
            final ShadowStub.Decoded d;
            try {
                d = ShadowStub.openEnveloped(new SnapshotEnvelope(), member, memberLength, engineId, clusterId);
            } catch (final IllegalStateException torn) {
                // reached only with the envelope check off: a torn stub read as a refused one
                return different(position, term, genesisId, Long.MIN_VALUE, rec, image, length, member,
                        memberLength, -1, "the member snapshot is a refused stub: " + torn.getMessage());
            }
            if (!d.ok()) {
                return different(position, term, genesisId, Long.MIN_VALUE, rec, image, length, member,
                        memberLength, -1, "the member snapshot is a refused stub: " + d.refusal());
            }
            final ShadowStub s = d.stub();
            if (!genesisOff && s.genesisId() == 0) {
                return preAdopt(position, rec);
            }
            if (!genesisOff && s.genesisId() != genesisId) {
                return otherGenesis(position, rec, s.genesisId(), genesisId);
            }
            System.out.println("[REPLICA] alignment: the member snapshot at " + position + " is a stub (SNAPSHOT_MODE=shadow):"
                    + " identity-only alignment (genesis, term, P_cm), no state bytes to compare");
            if (s.genesisId() != genesisId || s.logPosition() != position || s.leadershipTermId() != term) {
                return different(position, term, genesisId, s.genesisId(), rec, image, length, member, memberLength,
                        -1, "stub genesis=" + s.genesisId() + " term=" + s.leadershipTermId() + " P_cm="
                                + s.logPosition() + " vs replica genesis=" + genesisId + " term=" + term + " P_cm="
                                + position);
            }
            return Verdict.EQUAL;
        }
        final UnsafeBuffer mine = new UnsafeBuffer(image, 0, length);
        final int common = Math.min(length, memberLength);
        int at = 0;
        while (at < common && mine.getByte(at) == member.getByte(at)) {
            at++;
        }
        if (memberLength == length && at == length) {
            return Verdict.EQUAL;
        }
        String text = memberLength != length
                ? "replica image " + length + " B, member snapshot " + memberLength + " B"
                : "first difference at byte " + at + " of " + length;
        long memberGenesis = Long.MIN_VALUE;
        if (!genesisOff) {
            try {
                memberGenesis = snapshots.genesisOf(member, memberLength);
            } catch (final RuntimeException undecodable) {
                text += "; the member copy's genesis did not decode: " + undecodable;
            }
            if (memberGenesis == 0) {
                return preAdopt(position, rec);
            }
            if (memberGenesis != Long.MIN_VALUE && memberGenesis != genesisId) {
                return otherGenesis(position, rec, memberGenesis, genesisId);
            }
        }
        return different(position, term, genesisId, memberGenesis, rec, image, length, member, memberLength, at,
                text);
    }

    /** The copy's envelope did not open: torn or incomplete, never a divergence. */
    private Verdict unreadable(final long position, final long rec, final String why) {
        final long key = rec >= 0 ? rec : -position - 1;
        lastUnreadable = "; the last member copy at " + position + " (rec " + rec + ") was unreadable: " + why;
        if (unreadableCopies.add(key)) {
            unreadableTotal++;
            System.err.println("[REPLICA] CRITICAL: oracle: the retrieved member snapshot at " + position + " (rec "
                    + rec + ") does not open: " + why + "; it is torn or incomplete, not a divergence");
        }
        if (!trusted) {
            detail = "unverified continuation: the member snapshot at " + position + " (rec " + rec + ") does not"
                    + " open, waiting for the next one";
        }
        return Verdict.UNREADABLE;
    }

    /** A copy taken before the ADOPT (genesis 0): ignored, the held image stays PENDING. */
    private Verdict preAdopt(final long position, final long rec) {
        final long key = rec >= 0 ? rec : -position - 1;
        if (preAdoptCopies.add(key)) {
            preAdoptTotal++;
            System.out.println("[REPLICA] oracle: the member snapshot at " + position + " (rec " + rec + ") was taken"
                    + " before the ADOPT (genesis 0): ignored");
        }
        return Verdict.PENDING;
    }

    private Verdict otherGenesis(final long position, final long rec, final long memberGenesis,
                                 final long genesisId) {
        otherGenesisTotal++;
        otherGenesisText = differsText(position) + " (the member copy rec " + rec + " holds genesis " + memberGenesis
                + ", this replica " + genesisId + ": another history)";
        return Verdict.OTHER_GENESIS;
    }

    private Verdict different(final long position, final long term, final long genesisId, final long memberGenesis,
                              final long rec, final DirectBuffer image, final int length, final DirectBuffer member,
                              final int memberLength, final int firstDifference, final String text) {
        mismatchDetail = text;
        evidence = new MismatchEvidence(position, term, genesisId, memberGenesis, rec, newestNamed(),
                new UnsafeBuffer(image, 0, length), length, member, memberLength, firstDifference, text);
        mismatchAt = position;
        mismatchTotal++;
        System.err.println("[REPLICA] CRITICAL: alignment compare at " + position + ": " + text);
        return Verdict.DIFFERENT;
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

    /**
     * Nothing held, both trust states: the history fence text when the member snapshots prove another history (a
     * position this replica applied past without a SNAPSHOT action there), else null. The stall bound is
     * {@link #stall}.
     */
    String tick(final long appliedPosition) {
        final boolean verified = trusted;
        if (verified && off.contains(ReplicaNegativeControl.TICK_UNVERIFIED_ONLY)) {
            return null;
        }
        final long t = clock.nanoTime();
        if (t - nextCheckNs < 0) {
            return null;
        }
        nextCheckNs = t + CHECK_PACE_NS;
        final long lower = verified && !tickIgnoresVerifiedAt ? Math.max(fromPosition, verifiedAt) : fromPosition;
        final long[] retrieved = snapshots.positions();
        final long reported = snapshots.newestReported();
        if (appliedPast(reported, lower, appliedPosition, retrieved)) {
            return appliedPastText(reported, appliedPosition);
        }
        for (final long q : retrieved) {
            if (appliedPast(q, lower, appliedPosition, retrieved)) {
                return appliedPastText(q, appliedPosition);
            }
        }
        return null;
    }

    private boolean appliedPast(final long q, final long lower, final long applied, final long[] retrieved) {
        return q > lower && q <= applied && !actions.contains(q) && !preAdoptCopyAt(q, retrieved);
    }

    /** A retrieved copy at {@code q} taken before the ADOPT (decoded only at a would-be fence, cached). */
    private boolean preAdoptCopyAt(final long q, final long[] retrieved) {
        if (!contains(retrieved, q) || off.contains(ReplicaNegativeControl.GENESIS_CHECK_OFF)) {
            return false; // named only: nothing to decode yet
        }
        if (preAdoptCopies.contains(-q - 1)) {
            return true;
        }
        try {
            final DirectBuffer copy = snapshots.payload(q, -1);
            if (copy == null) {
                return false;
            }
            envelope.open(copy, copy.capacity());
            if (snapshots.genesisOf(copy, copy.capacity()) == 0) {
                preAdopt(q, -1);
                return true;
            }
        } catch (final RuntimeException undecided) {
            return false;
        }
        return false;
    }

    private static String appliedPastText(final long q, final long applied) {
        return differsText(q) + " (this replica applied past " + q + " without a SNAPSHOT action there, applied "
                + applied + ")";
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
        return stallFenceText(appliedPosition);
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
        return stallFenceText(appliedPosition);
    }

    /** The stall fence text (a re-genesis fences through it). */
    static String stallText(final long appliedPosition) {
        return "FATAL replica: unverified continuation at " + appliedPosition + " (history mismatch or a stalled"
                + " backup); wipe BASE_DIR";
    }

    /** The stall text, naming a torn member copy when the last verdict was UNREADABLE (the copy, not the replica). */
    String stallFenceText(final long appliedPosition) {
        final String torn = lastUnreadable;
        return torn == null ? stallText(appliedPosition) : stallText(appliedPosition) + torn;
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

    // ------------------------------------------------------------------------------------------------ freshness

    /** The newest member snapshot position the backup named or retrieved, -1 when none. */
    long newestNamed() {
        long n = snapshots.newestReported();
        final long[] p = snapshots.positions();
        if (p.length > 0) {
            n = Math.max(n, p[p.length - 1]);
        }
        return n;
    }

    /** STANDBY and LOCAL may be written: the newest named member snapshot is at or below the last EQUAL. */
    boolean fresh() {
        return newestNamed() <= verifiedAt;
    }

    /** A STANDBY or LOCAL trigger skipped by the freshness rule. */
    void staleSkipped(final Object kind, final long position, final long newest) {
        staleNamedTotal++;
        final String text = kind + " at " + position + " not written: the backup named a member snapshot at " + newest
                + " above the last EQUAL " + verifiedAt + " (resumes after the next EQUAL)";
        if (!text.equals(lastStale)) {
            lastStale = text;
            System.out.println("[REPLICA] " + text);
        }
    }

    // ------------------------------------------------------------------------------------------------ oracle

    /** The latched byte mismatch's readiness text, {@code oracle mismatch at <P>: <detail>}. */
    String mismatchText() {
        return "oracle mismatch at " + mismatchAt + ": " + mismatchDetail;
    }

    String otherGenesisText() {
        return otherGenesisText;
    }

    /** The latched mismatch's evidence (for the dump), null when none. */
    public MismatchEvidence mismatchEvidence() {
        return evidence;
    }

    /** {@code shadow_oracle_mismatch_position}: the latched mismatch position, -1 when none. */
    public long mismatchAt() {
        return mismatchAt;
    }

    public long mismatchTotal() {
        return mismatchTotal;
    }

    public long unreadableTotal() {
        return unreadableTotal;
    }

    public long otherGenesisTotal() {
        return otherGenesisTotal;
    }

    public long preAdoptTotal() {
        return preAdoptTotal;
    }

    public long staleNamedTotal() {
        return staleNamedTotal;
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
