// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.NodeReadiness;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The replica's {@code /ready} and {@code /health}.
 *
 * <p>Ready iff all hold, first failing input named in the detail:</p>
 * <ol>
 *   <li>bootstrap done ({@code restore_path} LOCAL_SHADOW or GENESIS), not refused, not fenced;</li>
 *   <li>the follower ticked within {@code REPLICA_READY_STALE_MS};</li>
 *   <li>no disk-full shadow write outstanding;</li>
 *   <li>a live recording L exists and its {@code RecordingPos} counter is alive for L (the bounded replay on the
 *       supplier is alive); otherwise the follower's detail ({@code awaiting live recording}, or {@code auth rejected}
 *       after an AUTHENTICATION_REJECTED since the last response);</li>
 *   <li>{@code recorded - applied <= REPLICA_READY_MAX_LAG_BYTES};</li>
 *   <li>the newest shadow written within {@code REPLICA_READY_SHADOW_AGE_MS} (0 disables; counted from boot while
 *       none was written in this process, so the first window after boot is "not ready until the first shadow");</li>
 *   <li>{@code wallNow - clusterTime(last applied frame) <=
 *       REPLICA_READY_TIME_LAG_MS} (default 3 000, 0 disables), caught up or not. The AE leader arms its 10 ms drain
 *       belt on leadership gain, so a healthy log ticks every ~10 ms of cluster time; a still log is a
 *       frozen or partitioned supplier, which a quiet cluster can no longer be mistaken for. The former idle branch
 *       ({@code time-lag-idle}, ready) is gone; only the {@code TIME_LAG_IDLE_ALLOWED} control brings it back.
 *       Coupling: this input relies on the leader's belt keeping a liveness tick of at most 1 s.</li>
 * </ol>
 *
 * <p>Never an input: the backup state (it leaves BACKING_UP at every query, CBA:1002-1007), wall-clock log progress.
 * {@code /health}: the follower and the housekeeping thread ticked within {@code REPLICA_LIVENESS_STALE_MS}. Node-local
 * health only: nothing here reaches the applier.</p>
 */
public final class ReplicaReadiness {

    /** {@code shadow_replica_restore_path}. */
    public enum RestorePath {
        NONE(0), LOCAL_SHADOW(1), GENESIS(2), REFUSED(4);

        private final int code;

        RestorePath(final int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    /** {@code ClusterBackup.State.BACKING_UP}'s code. */
    static final long BACKING_UP = 5;

    private final long readyStaleNs;
    private final long livenessStaleNs;
    private final long maxLagBytes;
    private final long shadowAgeNs;
    private final long timeLagMs;
    private final NanoClock clock;
    private final EpochClock wall;
    private final LongSupplier backupState;
    private final LongSupplier lastShadowWrittenNs;
    private final Set<ReplicaNegativeControl> off;
    private final long bootNs;

    private volatile RestorePath restorePath = RestorePath.NONE;
    private volatile String bootstrapDetail = "bootstrap: starting";
    private volatile String refused;
    private volatile String fenced;
    private volatile boolean stopping;
    private volatile long followerTickNs;
    private volatile boolean followerTicked;
    private volatile long housekeepingTickNs;
    private volatile boolean housekeepingTicked;
    private volatile boolean liveAlive;
    private volatile String liveDetail = "awaiting live recording";
    private volatile boolean authRejected;
    private volatile long applied = -1;
    private volatile long recorded = -1;
    private volatile long appliedClusterTime = -1;
    private volatile String diskFull;
    private volatile boolean verified;
    private volatile String verifyDetail = "unverified continuation: not started";

    /**
     * @param lastShadowWrittenNs the writer's completion time of the newest OK write in this process, -1 for none
     * @param backupState         the backup state counter, read only by the READINESS_FROM_BACKUP_STATE control and
     *                            the detail
     */
    public ReplicaReadiness(final long readyStaleMs, final long livenessStaleMs, final long maxLagBytes,
                            final long shadowAgeMs, final long timeLagMs, final NanoClock clock, final EpochClock wall,
                            final LongSupplier backupState, final LongSupplier lastShadowWrittenNs,
                            final Set<ReplicaNegativeControl> off) {
        if (readyStaleMs <= 0 || livenessStaleMs < readyStaleMs || maxLagBytes <= 0 || shadowAgeMs < 0
                || timeLagMs < 0) {
            throw new IllegalArgumentException("invalid replica readiness bounds: readyStaleMs=" + readyStaleMs
                    + " livenessStaleMs=" + livenessStaleMs + " maxLagBytes=" + maxLagBytes + " shadowAgeMs="
                    + shadowAgeMs + " timeLagMs=" + timeLagMs);
        }
        this.readyStaleNs = TimeUnit.MILLISECONDS.toNanos(readyStaleMs);
        this.livenessStaleNs = TimeUnit.MILLISECONDS.toNanos(livenessStaleMs);
        this.maxLagBytes = maxLagBytes;
        this.shadowAgeNs = TimeUnit.MILLISECONDS.toNanos(shadowAgeMs);
        this.timeLagMs = timeLagMs;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.wall = Objects.requireNonNull(wall, "wall");
        this.backupState = Objects.requireNonNull(backupState, "backupState");
        this.lastShadowWrittenNs = Objects.requireNonNull(lastShadowWrittenNs, "lastShadowWrittenNs");
        this.off = off.isEmpty() ? EnumSet.noneOf(ReplicaNegativeControl.class) : EnumSet.copyOf(off);
        this.bootNs = clock.nanoTime();
    }

    // ------------------------------------------------------------------------------------------------ inputs

    public void bootstrapping(final String detail) {
        bootstrapDetail = Objects.requireNonNull(detail, "detail");
    }

    public void restored(final RestorePath path) {
        if (path != RestorePath.LOCAL_SHADOW && path != RestorePath.GENESIS) {
            throw new IllegalArgumentException("restored with " + path);
        }
        restorePath = path;
    }

    /** A refused bootstrap: never ready again in this process. */
    public void refused(final String reason) {
        refused = reason == null ? "unspecified" : reason;
        restorePath = RestorePath.REFUSED;
    }

    /** Fenced: 503 {@code fenced:<reason>} until the process exits. */
    public void fenced(final String reason) {
        if (fenced == null) {
            fenced = reason == null ? "unspecified" : reason;
        }
    }

    public void stopping() {
        stopping = true;
    }

    public void followerTick() {
        followerTickNs = clock.nanoTime();
        followerTicked = true;
    }

    public void housekeepingTick() {
        housekeepingTickNs = clock.nanoTime();
        housekeepingTicked = true;
    }

    /** The live recording's state as the follower read it (the counter rule), with its detail when not alive. */
    public void live(final boolean alive, final String detail) {
        liveAlive = alive;
        if (!alive) {
            liveDetail = detail == null ? "awaiting live recording" : detail;
        }
    }

    /** An AUTHENTICATION_REJECTED since the last backup response (the events listener sets and clears). */
    public void authRejected(final boolean rejected) {
        authRejected = rejected;
    }

    public void positions(final long applied, final long recorded) {
        this.applied = applied;
        this.recorded = recorded;
    }

    /** The cluster time (ms) of the last applied frame. */
    public void appliedClusterTime(final long clusterTimeMs) {
        appliedClusterTime = clusterTimeMs;
    }

    /**
     * Whether the continuation is VERIFIED (a full replay from genesis, or a byte-equal ALIGNED
     * compare with a member snapshot passed). Fail closed: false until the follower says otherwise.
     */
    public void verified(final boolean verified, final String detail) {
        this.verifyDetail = verified ? null : (detail == null ? "unverified continuation" : detail);
        this.verified = verified;
    }

    /** Non-null while a shadow write was refused for disk space. */
    public void diskFull(final String detail) {
        diskFull = detail;
    }

    public RestorePath restorePath() {
        return restorePath;
    }

    public String fencedReason() {
        return fenced;
    }

    // ------------------------------------------------------------------------------------------------ probe

    public NodeReadiness.Probe probe() {
        // every writer's timestamp is captured BEFORE our clock read: a newer tick must never read as a negative age
        final boolean ft = followerTicked;
        final long ftNs = followerTickNs;
        final boolean ht = housekeepingTicked;
        final long htNs = housekeepingTickNs;
        final long a = applied;
        final long r = recorded;
        final long act = appliedClusterTime;
        final long written = lastShadowWrittenNs.getAsLong();
        final long now = clock.nanoTime();
        final long timeLag = act < 0 ? -1 : wall.time() - act;
        final long shadowAge = age(now, written < 0 ? bootNs : written);
        final boolean alive = liveAlive;
        final String reason = off.contains(ReplicaNegativeControl.READINESS_FROM_BACKUP_STATE)
                ? backupStateReason() : reason(now, ft, ftNs, a, r, alive, timeLag, shadowAge);
        final boolean live = ft && age(now, ftNs) < livenessStaleNs && ht && age(now, htNs) < livenessStaleNs;
        final boolean ready = reason.equals("ready") || reason.equals("ready time-lag-idle");
        final String detail = "role=REPLICA recovery=" + reason
                + " restore=" + restorePath
                + " applied=" + a
                + " recorded=" + r
                + " lagBytes=" + (a < 0 || r < 0 ? -1 : r - a)
                + " timeLagMs=" + timeLag
                + " shadowAgeMs=" + TimeUnit.NANOSECONDS.toMillis(shadowAge)
                + " liveRecording=" + (alive ? "alive" : "none")
                + " backupState=" + backupState.getAsLong()
                + " followerTickAgeMs=" + (ft ? TimeUnit.NANOSECONDS.toMillis(age(now, ftNs)) : -1)
                + " housekeepingTickAgeMs=" + (ht ? TimeUnit.NANOSECONDS.toMillis(age(now, htNs)) : -1)
                + (verified ? " verify=VERIFIED" : " verify=" + verifyDetail);
        return new NodeReadiness.Probe(live, ready, detail);
    }

    private String reason(final long now, final boolean ft, final long ftNs, final long a, final long r,
                          final boolean alive, final long timeLag, final long shadowAge) {
        final String early = gate();
        if (early != null) {
            return early;
        }
        if (!verified) {
            return "unverified continuation"; // the detail names what the trust waits for
        }
        if (!ft || age(now, ftNs) >= readyStaleNs) {
            return "stale-follower";
        }
        final String disk = diskFull;
        if (disk != null) {
            return disk;
        }
        if (!alive) {
            return authRejected ? "auth rejected" : liveDetail;
        }
        if (a < 0 || r < 0) {
            return "unknown-position";
        }
        if (r - a > maxLagBytes) {
            return "apply-lag";
        }
        if (shadowAgeNs > 0 && shadowAge > shadowAgeNs) {
            return "shadow-age";
        }
        if (timeLagMs > 0 && timeLag > timeLagMs) {
            if (r == a && off.contains(ReplicaNegativeControl.TIME_LAG_IDLE_ALLOWED)) {
                return "ready time-lag-idle"; // NC only: the dropped idle reading
            }
            return "time-lag";
        }
        return "ready";
    }

    /** Stopping, fenced, refused, bootstrap not done: the inputs no other reading may override. */
    private String gate() {
        if (stopping) {
            return "stopping";
        }
        final String f = fenced;
        if (f != null) {
            return "fenced:" + f;
        }
        final String rf = refused;
        if (rf != null) {
            return "refused:" + rf;
        }
        final RestorePath p = restorePath;
        if (p != RestorePath.LOCAL_SHADOW && p != RestorePath.GENESIS) {
            return bootstrapDetail;
        }
        return null;
    }

    /** NC READINESS_FROM_BACKUP_STATE: the backup-state rule, which flaps at every query. */
    private String backupStateReason() {
        final String early = gate();
        if (early != null) {
            return early;
        }
        return backupState.getAsLong() == BACKING_UP ? "ready" : "backup-state";
    }

    private static long age(final long now, final long then) {
        final long elapsed = now - then;
        return elapsed < 0 ? Long.MAX_VALUE : elapsed;
    }
}
