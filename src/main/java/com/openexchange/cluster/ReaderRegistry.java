// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The retention contract (R03 v0.1) as a replicated state machine: who may hold the purge boundary,
 * from which committed facts, and how a holder is given up on.
 *
 * <p>Everything in here is REPLICATED STATE. Every mutator stands for one message that arrived through
 * the cluster log ({@code ReaderRegister}, {@code ReaderCheckpoint}, {@code ReaderUnregister},
 * {@code StrandReader}, {@code StrandMember}, {@code MemberPositionsObserved}, {@code RetentionConfig})
 * and every {@code nowMs} is the log timestamp of that message (cluster time, ms). Nothing reads a wall
 * clock, nothing reads a node's environment, so three members fed the same log reach the same
 * boundary, and a snapshot of this object is a function of state, not of history.</p>
 *
 * <h2>What it pins</h2>
 * <ul>
 *   <li>The boundary never moves backwards; it starts at 0 (purge nothing).</li>
 *   <li>Authority is commit-only: a member position the leader merely observed means nothing until
 *       the observation is committed as a message; this class only ever sees committed inputs.</li>
 *   <li>Unregistered readers do not participate (a deployment without premium consumers reclaims);
 *       a registered reader that went quiet HOLDs and is reported, it is never ignored.</li>
 *   <li>A member never observed holds at 0 until an observation is committed or an operator strands it.</li>
 *   <li>A reader is stranded only by an operator, or by the rule "lag past the byte bound AND held past
 *       the time bound", both measured in cluster time, so a leadership change can only make the bound
 *       more patient.</li>
 *   <li>A blown byte budget closes the admission gate; it never deletes.</li>
 *   <li>Positions of different sources never meet: a reader reports its own source's
 *       {@code (epoch, recordingId, position)}; a {@link PositionMapper} supplied by the producer turns
 *       that into the producer's single chain position.</li>
 * </ul>
 *
 * <p>Enforcement is a separate, replicated switch ({@link #enforce()}), off by default: R03 reports,
 * R04 acts.</p>
 */
public final class ReaderRegistry {

    public enum Kind { BRIDGE, EXEC_PROJECTOR, ORDER_PROJECTOR, HFT_GATEWAY, ASSETS_PROJECTOR, CLUSTER_BACKUP, SHADOW_REPLICA, MEMBER_OBSERVER }

    public enum Status { ACTIVE, HOLD, STRANDED }

    public enum Reject { NONE, UNKNOWN_READER, WRONG_EPOCH, FUTURE_POSITION, BACKWARDS, ALREADY_STRANDED }

    public enum Gate { OPEN, CLOSED }

    /** The producer's own notion of "how far along the single chain" a (recordingId, position) is. */
    public interface PositionMapper {
        PositionMapper IDENTITY = (recordingId, position) -> position;

        long toChainPosition(long recordingId, long position);
    }

    /** One registered consumer. Public fields for diagnostics and tests; mutated only by this class. */
    public static final class Reader {
        public final String id;
        public Kind kind;
        public long epoch;
        public long recordingId = -1;
        public long position = -1;
        public long chainPosition = -1;
        public long logicalSeq = -1;
        public long lastSeenMs;
        public long leaseMs;
        public Status status = Status.ACTIVE;
        public long holdSinceMs = -1;
        public String strandReason = "";

        Reader(final String id) {
            this.id = id;
        }

        /** What this reader holds the boundary at: its chain position, or 0 before its first checkpoint. */
        long floor() {
            return chainPosition < 0 ? 0L : chainPosition;
        }
    }

    /** One cluster member as last committed by a MemberPositionsObserved message. */
    private static final class Member {
        final int id;
        long position = -1;        // -1 = never observed
        long observedAtMs = -1;
        long lastFreshAtMs = -1;
        boolean stranded;
        String strandReason = "";

        Member(final int id) {
            this.id = id;
        }
    }

    /** The answer and enough of the reasoning to read it. */
    public static final class Decision {
        public final long boundary;
        public final String limiter;
        public final Gate gate;
        public final long retained;
        public final List<String> holds;
        public final List<String> stranded;
        public final Map<String, Long> detail;

        Decision(final long boundary, final String limiter, final Gate gate, final long retained,
                 final List<String> holds, final List<String> stranded, final Map<String, Long> detail) {
            this.boundary = boundary;
            this.limiter = limiter;
            this.gate = gate;
            this.retained = retained;
            this.holds = holds;
            this.stranded = stranded;
            this.detail = detail;
        }

        @Override
        public String toString() {
            return "boundary=" + boundary + " limiter=" + limiter + " gate=" + gate + " retained=" + retained
                    + (holds.isEmpty() ? "" : " HOLD=" + holds) + (stranded.isEmpty() ? "" : " STRANDED=" + stranded)
                    + " detail=" + detail;
        }
    }

    private static final int SNAPSHOT_MAGIC = 0x52445231; // "RDR1"

    private final long epoch;
    private final TreeMap<Integer, Member> members = new TreeMap<>();
    private final TreeMap<String, Reader> readers = new TreeMap<>();
    private PositionMapper mapper = PositionMapper.IDENTITY;

    // Replicated configuration (RetentionConfig message). Defaults: report only.
    private long maxLagBytes = RetentionWatermark.DEFAULT_MAX_LAG_BYTES;
    private long maxLagMs = RetentionWatermark.DEFAULT_MAX_LAG_MS;
    private long budgetBytes = Long.MAX_VALUE;
    private boolean enforce;

    private long previousBoundary;
    private final long[] rejects = new long[Reject.values().length];

    public ReaderRegistry(final long epoch, final int[] memberIds) {
        this.epoch = epoch;
        for (final int id : memberIds) {
            members.put(id, new Member(id));
        }
    }

    // ---- replicated inputs ----

    /** {@code RetentionConfig}: replicated; the only way enforcement turns on. */
    public void configure(final long maxLagBytes, final long maxLagMs, final long budgetBytes, final boolean enforce) {
        this.maxLagBytes = maxLagBytes;
        this.maxLagMs = maxLagMs;
        this.budgetBytes = budgetBytes;
        this.enforce = enforce;
    }

    public boolean enforce() {
        return enforce;
    }

    /** Node code, not state: how the producer maps a reader's (recordingId, position) onto its chain. */
    public void positionMapper(final PositionMapper mapper) {
        this.mapper = mapper == null ? PositionMapper.IDENTITY : mapper;
    }

    /** {@code ReaderRegister}. A re-registration refreshes the lease; a stranded reader stays stranded. */
    public Reject register(final String id, final Kind kind, final long readerEpoch, final long leaseMs, final long nowMs) {
        if (readerEpoch != epoch) {
            return reject(Reject.WRONG_EPOCH);
        }
        Reader r = readers.get(id);
        if (r == null) {
            r = new Reader(id);
            readers.put(id, r);
        } else if (r.status == Status.STRANDED) {
            return reject(Reject.ALREADY_STRANDED);
        }
        r.kind = kind;
        r.epoch = readerEpoch;
        r.leaseMs = leaseMs;
        r.lastSeenMs = nowMs;
        r.status = Status.ACTIVE;
        r.holdSinceMs = -1;
        return Reject.NONE;
    }

    /**
     * {@code ReaderCheckpoint}. {@code head} is the producer's current chain head: a checkpoint beyond it
     * is a reader bug and is refused; so is one that goes backwards along the chain.
     */
    public Reject checkpoint(final String id, final long readerEpoch, final long recordingId, final long position,
                             final long logicalSeq, final long head, final long nowMs) {
        final Reader r = readers.get(id);
        if (r == null) {
            return reject(Reject.UNKNOWN_READER);
        }
        if (readerEpoch != epoch) {
            return reject(Reject.WRONG_EPOCH);
        }
        if (r.status == Status.STRANDED) {
            return reject(Reject.ALREADY_STRANDED);
        }
        final long chain = mapper.toChainPosition(recordingId, position);
        if (chain > head) {
            return reject(Reject.FUTURE_POSITION);
        }
        if (chain < r.chainPosition) {
            return reject(Reject.BACKWARDS);
        }
        r.recordingId = recordingId;
        r.position = position;
        r.chainPosition = chain;
        r.logicalSeq = logicalSeq;
        r.lastSeenMs = nowMs;
        r.status = Status.ACTIVE;
        r.holdSinceMs = -1;
        return Reject.NONE;
    }

    /** {@code ReaderUnregister}: a graceful departure releases the hold. */
    public void unregister(final String id) {
        readers.remove(id);
    }

    /** {@code StrandReader}: an operator's replicated decision; the reader must be reseeded to return. */
    public Reject strandReader(final String id, final String reason, final long nowMs) {
        final Reader r = readers.get(id);
        if (r == null) {
            return reject(Reject.UNKNOWN_READER);
        }
        r.status = Status.STRANDED;
        r.strandReason = reason == null ? "" : reason;
        r.holdSinceMs = r.holdSinceMs < 0 ? nowMs : r.holdSinceMs;
        return Reject.NONE;
    }

    /** {@code StrandMember}: same, for a cluster member that will be reseeded by hand. */
    public void strandMember(final int memberId, final String reason, final long nowMs) {
        final Member m = members.computeIfAbsent(memberId, Member::new);
        m.stranded = true;
        m.strandReason = reason == null ? "" : reason;
    }

    /**
     * {@code MemberPositionsObserved}: what the leader's housekeeping thread saw, committed. A position
     * never decreases (a late message from an old leader cannot pull a member back); a stale observation
     * keeps the member at its last committed position and only refreshes the "seen stale" clock.
     */
    public void observeMembers(final long nowMs, final int[] memberIds, final long[] positions, final boolean[] fresh) {
        for (int i = 0; i < memberIds.length; i++) {
            final Member m = members.computeIfAbsent(memberIds[i], Member::new);
            m.observedAtMs = nowMs;
            if (fresh[i]) {
                m.position = Math.max(m.position, positions[i]);
                m.lastFreshAtMs = nowMs;
            }
        }
    }

    // ---- the decision ----

    /**
     * Decide at a committed point in time. Mutates reader/member status (lease expiry, the strand rule),
     * so it must be called at the same log points on every member.
     *
     * @param floorOwn this node's own recovery floor (snapshot / shadow rule); nothing above it is in play
     * @param head     the producer's current chain head
     * @param nowMs    cluster time of the committed event that triggered the decision
     */
    public Decision decide(final long floorOwn, final long head, final long nowMs) {
        final Map<String, Long> detail = new LinkedHashMap<>();
        detail.put("floorOwn", floorOwn);
        detail.put("head", head);
        detail.put("previous", previousBoundary);

        long raw = floorOwn;
        String limiter = "floorOwn";

        final List<String> holds = new ArrayList<>();
        final List<String> stranded = new ArrayList<>();
        for (final Reader r : readers.values()) {
            if (r.status == Status.ACTIVE && r.lastSeenMs + r.leaseMs < nowMs) {
                r.status = Status.HOLD;
                r.holdSinceMs = nowMs; // the first committed point at which the lease was seen expired
            }
            if (r.status == Status.HOLD) {
                final boolean bytesBlown = head - r.floor() >= maxLagBytes;
                final boolean timeBlown = nowMs - r.holdSinceMs >= maxLagMs;
                if (bytesBlown && timeBlown) {
                    r.status = Status.STRANDED;
                    r.strandReason = "bound: lag>=" + maxLagBytes + "B for >=" + maxLagMs + "ms";
                }
            }
            if (r.status == Status.STRANDED) {
                stranded.add(r.id);
                continue;
            }
            if (r.status == Status.HOLD) {
                holds.add(r.id);
            }
            final long f = r.floor();
            if (f < raw) {
                raw = f;
                limiter = r.id;
            }
        }
        for (final Member m : members.values()) {
            if (m.stranded) {
                stranded.add("member" + m.id);
                continue;
            }
            if (m.position >= 0 && m.lastFreshAtMs >= 0 && head - m.position >= maxLagBytes
                    && nowMs - m.lastFreshAtMs >= maxLagMs) {
                m.stranded = true;
                m.strandReason = "bound: lag>=" + maxLagBytes + "B, no fresh observation for >=" + maxLagMs + "ms";
                stranded.add("member" + m.id);
                continue;
            }
            final long f = m.position < 0 ? 0L : m.position;
            if (f < raw) {
                raw = f;
                limiter = "member" + m.id + (m.position < 0 ? ":unobserved" : "");
            }
        }
        detail.put("raw", raw);

        final long boundary = Math.max(previousBoundary, raw);
        if (boundary == previousBoundary && raw < previousBoundary) {
            limiter = "previous(" + limiter + ")";
        }
        previousBoundary = boundary;
        final long retained = Math.max(0L, head - boundary);
        final Gate gate = retained > budgetBytes ? Gate.CLOSED : Gate.OPEN;
        detail.put("retained", retained);
        return new Decision(boundary, limiter, gate, retained, Collections.unmodifiableList(holds),
                Collections.unmodifiableList(stranded), detail);
    }

    // ---- reads ----

    public Reader reader(final String id) {
        return readers.get(id);
    }

    public int readerCount() {
        return readers.size();
    }

    public long previousBoundary() {
        return previousBoundary;
    }

    public long epoch() {
        return epoch;
    }

    public long rejectCount(final Reject reject) {
        return rejects[reject.ordinal()];
    }

    private Reject reject(final Reject reject) {
        rejects[reject.ordinal()]++;
        return reject;
    }

    // ---- snapshot (canonical: members and readers by id; strings as length + UTF-8) ----

    public int serialize(final MutableDirectBuffer dst, final int offset) {
        int pos = offset;
        dst.putInt(pos, SNAPSHOT_MAGIC); pos += 4;
        dst.putLong(pos, epoch); pos += 8;
        dst.putLong(pos, maxLagBytes); pos += 8;
        dst.putLong(pos, maxLagMs); pos += 8;
        dst.putLong(pos, budgetBytes); pos += 8;
        dst.putByte(pos, (byte) (enforce ? 1 : 0)); pos += 1;
        dst.putLong(pos, previousBoundary); pos += 8;
        dst.putInt(pos, members.size()); pos += 4;
        for (final Member m : members.values()) {
            dst.putInt(pos, m.id); pos += 4;
            dst.putLong(pos, m.position); pos += 8;
            dst.putLong(pos, m.observedAtMs); pos += 8;
            dst.putLong(pos, m.lastFreshAtMs); pos += 8;
            dst.putByte(pos, (byte) (m.stranded ? 1 : 0)); pos += 1;
            pos = putString(dst, pos, m.strandReason);
        }
        dst.putInt(pos, readers.size()); pos += 4;
        for (final Reader r : readers.values()) {
            pos = putString(dst, pos, r.id);
            dst.putByte(pos, (byte) r.kind.ordinal()); pos += 1;
            dst.putLong(pos, r.epoch); pos += 8;
            dst.putLong(pos, r.recordingId); pos += 8;
            dst.putLong(pos, r.position); pos += 8;
            dst.putLong(pos, r.chainPosition); pos += 8;
            dst.putLong(pos, r.logicalSeq); pos += 8;
            dst.putLong(pos, r.lastSeenMs); pos += 8;
            dst.putLong(pos, r.leaseMs); pos += 8;
            dst.putByte(pos, (byte) r.status.ordinal()); pos += 1;
            dst.putLong(pos, r.holdSinceMs); pos += 8;
            pos = putString(dst, pos, r.strandReason);
        }
        return pos - offset;
    }

    /** Restore into an empty registry built for the same epoch. Returns bytes consumed. */
    public int deserialize(final DirectBuffer src, final int offset) {
        int pos = offset;
        final int magic = src.getInt(pos); pos += 4;
        if (magic != SNAPSHOT_MAGIC) {
            throw new IllegalStateException("reader registry snapshot magic mismatch: " + Integer.toHexString(magic));
        }
        final long snapshotEpoch = src.getLong(pos); pos += 8;
        if (snapshotEpoch != epoch) {
            throw new IllegalStateException("reader registry snapshot epoch " + snapshotEpoch + " != " + epoch);
        }
        maxLagBytes = src.getLong(pos); pos += 8;
        maxLagMs = src.getLong(pos); pos += 8;
        budgetBytes = src.getLong(pos); pos += 8;
        enforce = src.getByte(pos) != 0; pos += 1;
        previousBoundary = src.getLong(pos); pos += 8;
        final int memberCount = src.getInt(pos); pos += 4;
        for (int i = 0; i < memberCount; i++) {
            final int id = src.getInt(pos); pos += 4;
            final Member m = members.computeIfAbsent(id, Member::new);
            m.position = src.getLong(pos); pos += 8;
            m.observedAtMs = src.getLong(pos); pos += 8;
            m.lastFreshAtMs = src.getLong(pos); pos += 8;
            m.stranded = src.getByte(pos) != 0; pos += 1;
            final String[] s = new String[1];
            pos = getString(src, pos, s);
            m.strandReason = s[0];
        }
        final int readerCount = src.getInt(pos); pos += 4;
        for (int i = 0; i < readerCount; i++) {
            final String[] s = new String[1];
            pos = getString(src, pos, s);
            final Reader r = new Reader(s[0]);
            r.kind = Kind.values()[src.getByte(pos)]; pos += 1;
            r.epoch = src.getLong(pos); pos += 8;
            r.recordingId = src.getLong(pos); pos += 8;
            r.position = src.getLong(pos); pos += 8;
            r.chainPosition = src.getLong(pos); pos += 8;
            r.logicalSeq = src.getLong(pos); pos += 8;
            r.lastSeenMs = src.getLong(pos); pos += 8;
            r.leaseMs = src.getLong(pos); pos += 8;
            r.status = Status.values()[src.getByte(pos)]; pos += 1;
            r.holdSinceMs = src.getLong(pos); pos += 8;
            pos = getString(src, pos, s);
            r.strandReason = s[0];
            readers.put(r.id, r);
        }
        return pos - offset;
    }

    private static int putString(final MutableDirectBuffer dst, int pos, final String s) {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        dst.putInt(pos, bytes.length); pos += 4;
        dst.putBytes(pos, bytes); pos += bytes.length;
        return pos;
    }

    private static int getString(final DirectBuffer src, int pos, final String[] out) {
        final int len = src.getInt(pos); pos += 4;
        final byte[] bytes = new byte[len];
        src.getBytes(pos, bytes); pos += len;
        out[0] = new String(bytes, StandardCharsets.UTF_8);
        return pos;
    }
}
