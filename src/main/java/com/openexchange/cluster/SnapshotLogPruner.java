// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusterCounters;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Turns a snapshot into reclaimed disk, on the node that took it.
 *
 * <p>An Aeron snapshot does not truncate anything - it only ADDS recordings.
 * The log below it stays on disk until something purges it, and until now that
 * something was the admin gateway shelling out {@link ArchiveHousekeeping} per
 * node. Take the gateway away and the engine snapshots forever into a disk that
 * never gets smaller, which is the same ending by a slower road: /dev/shm
 * fills, the archive fails writes with "No space left on device", nodes
 * terminate.</p>
 *
 * <h2>Why a thread and not the duty cycle</h2>
 *
 * <p>Purging is an archive control request and a directory of file deletions.
 * Neither belongs on the thread that matches orders. This runs on its own
 * daemon thread with its own archive client, wakes a few times a minute, and
 * does nothing at all unless the snapshot counter moved.</p>
 *
 * <h2>What it purges, and what that costs</h2>
 *
 * <p>Log segments below THIS node's newest snapshot, and never below the retention floor: the lowest position a
 * member of the cluster may still need from this node's log.</p>
 *
 * <h2>The retention floor</h2>
 *
 * <p>A member that is down while the others snapshot and purge comes back at its old position and asks the leader
 * for the log from there. If any member that may lead by then has purged past that position, the member can never
 * catch up. So no member purges past what a member still needs, and the rule is one every member can apply whoever
 * leads:</p>
 *
 * <ul>
 *   <li><b>The leader</b> holds its purge at the lowest position of any member ({@link RetentionWatermark}). It
 *       asks its own consensus module on every wake and remembers what it hears, so a member that goes silent is
 *       held at the position the leader last saw it at.</li>
 *   <li><b>A follower</b> never purges past the leader's log start position, which it reads from the leader's
 *       archive ({@link LogExtent}). That start position is durable state every member can read; it is not held in
 *       any one node's memory.</li>
 *   <li><b>A new leader</b> therefore starts with a log that reaches at least as far back as the old leader's did.
 *       It has not heard from a member that was down before it was elected, so it counts that member at its own log
 *       start: it purges nothing more until the member reports, or the bound below passes.</li>
 * </ul>
 *
 * <h2>The bound: a member that stays down for ever</h2>
 *
 * <p>The floor must not hold the log for ever, or one dead member fills every disk. A member that has held the
 * floor for {@link #MEMBER_WAIT_MS_ENV} (default 30 minutes) since it last moved or was last heard from while level
 * with the leader, or that is
 * {@link #MEMBER_WAIT_BYTES_ENV} (default 2 GiB) behind the newest snapshot, is written off: one STRANDED line on
 * the leader, and the purge proceeds past it. If that member returns below the leader's log start it cannot catch up
 * from the log; its {@link CatchupWatchdog} exits it with a FATAL line naming both positions, and it has to be
 * reseeded. The same bound applies to a floor this node cannot work out (no answer from its consensus module, a
 * follower that cannot read the leader's archive): it holds its log for the wait, then says so and purges to its own
 * snapshot again; while the floor is unknown {@link #lastError()} says so. The time half runs on the local clock and restarts when leadership moves, which only makes it more
 * patient; the byte half does not depend on a clock.</p>
 *
 * <p>{@link #FLOOR_MODE_ENV}{@code =report} computes and logs the floor without applying it (the purge then goes to
 * this node's own snapshot, as before the floor existed).</p>
 *
 * <p>Two guards, both cheap. A node purges nothing unless it reports itself
 * caught up ({@link NodeReadiness#ready()}) - a member still replaying has no
 * business deleting log. And a purge that finds a snapshot recording missing
 * from the archive says so as a CRITICAL, because that node's
 * recover-from-snapshot is already broken.</p>
 */
public final class SnapshotLogPruner {

    /** How often the snapshot counter is read, and the leader's member positions are noted. */
    private static final long POLL_INTERVAL_MS = 5_000;

    /** {@code apply} (default): the purge is bounded by the retention floor. {@code report}: it is only logged. */
    public static final String FLOOR_MODE_ENV = "RETENTION_FLOOR";
    public static final String FLOOR_MODE_PROP = "retention.floor";
    /**
     * How long the floor waits for a member that does not move, or for a floor that cannot be worked out, before
     * the purge proceeds without it (1000..604800000 ms, default 30 minutes).
     */
    public static final String MEMBER_WAIT_MS_ENV = "RETENTION_MEMBER_WAIT_MS";
    public static final String MEMBER_WAIT_MS_PROP = "retention.member.wait.ms";
    public static final long DEFAULT_MEMBER_WAIT_MS = RetentionWatermark.DEFAULT_MAX_LAG_MS;
    public static final long MIN_MEMBER_WAIT_MS = 1_000;
    public static final long MAX_MEMBER_WAIT_MS = TimeUnit.DAYS.toMillis(7);
    /**
     * How far behind the newest snapshot a member may be before the floor stops waiting for it
     * (1048576..1099511627776 bytes, default 2 GiB).
     */
    public static final String MEMBER_WAIT_BYTES_ENV = "RETENTION_MEMBER_WAIT_BYTES";
    public static final String MEMBER_WAIT_BYTES_PROP = "retention.member.wait.bytes";
    public static final long DEFAULT_MEMBER_WAIT_BYTES = RetentionWatermark.DEFAULT_MAX_LAG_BYTES;
    public static final long MIN_MEMBER_WAIT_BYTES = 1L << 20;
    public static final long MAX_MEMBER_WAIT_BYTES = 1L << 40;

    /** What the pruner does with the retention floor. */
    public enum FloorMode { APPLY, REPORT }

    /** How long to wait for the consensus module to answer a members query. */
    private static final long QUERY_TIMEOUT_MS = 1_000;

    private final File clusterDir;
    private final AeronArchive.Context archiveContext;
    private final CountersReader countersReader;
    private final int clusterId;
    private final BooleanSupplier caughtUp;

    /**
     * The lowest position any consumer still needs. Long.MAX_VALUE means "no
     * external constraint", which is all a single member can know today - the
     * seam is here so the retention watermark can be plugged in without
     * reopening the purge path.
     */
    private volatile java.util.function.LongSupplier watermark = () -> Long.MAX_VALUE;

    // The retention floor. Null until reportWatermark() wires it.
    private ClusterMemberPositions positions;
    private java.util.function.BooleanSupplier isLeader;
    private RetentionWatermark retention;
    private volatile String lastWatermarkReport;
    private FloorMode floorMode = FloorMode.APPLY;
    private long memberWaitMs = DEFAULT_MEMBER_WAIT_MS;
    private long memberWaitBytes = DEFAULT_MEMBER_WAIT_BYTES;
    private long probeTimeoutMs = LogExtent.DEFAULT_PROBE_TIMEOUT_MS;
    private long pollIntervalMs = POLL_INTERVAL_MS;
    /** Since when this node could not work out its floor, or -1. */
    private long floorUnknownSinceMs = -1;
    private boolean floorUnknownGivenUp;
    /** Why the floor is unknown right now, for {@link #lastError()}; null while it is known. */
    private String floorProblem;
    private volatile long lastAppliedFloor = Long.MAX_VALUE;

    // A purge pass may also be requested by the shadow housekeeping, which runs as a hook on
    // this thread before every decision. Without them the pruner behaves exactly as before.
    private volatile BooleanSupplier purgeRequested = () -> false;
    private volatile Runnable wakeHook = () -> { };
    private volatile long wakesByShadow;

    private volatile boolean running;
    private Thread thread;
    private volatile long prunesRun;
    private long bytesReclaimed;
    private volatile String lastError;

    /**
     * @param clusterDir     the node's cluster directory, the one holding recording.log
     * @param archiveContext a context already cloned by the caller; this class connects
     *                       it on its own thread and owns the session
     * @param countersReader this node's Aeron counters
     * @param clusterId      which cluster's counters to read
     * @param caughtUp       whether this node is caught up; false means purge nothing
     */
    public SnapshotLogPruner(final File clusterDir, final AeronArchive.Context archiveContext,
                             final CountersReader countersReader, final int clusterId,
                             final BooleanSupplier caughtUp) {
        this.clusterDir = clusterDir;
        this.archiveContext = archiveContext;
        this.countersReader = countersReader;
        this.clusterId = clusterId;
        this.caughtUp = caughtUp;
    }

    /**
     * Bound the purge by what every consumer has already passed, instead of by
     * this node's snapshot alone. Nothing supplies one yet.
     */
    public SnapshotLogPruner watermark(final java.util.function.LongSupplier watermark) {
        this.watermark = watermark;
        return this;
    }

    /**
     * Whether the purge is bounded by exactly {@code supplier} (identity, not equality): the runtime net that
     * a member in shadow mode prunes behind its {@code ShadowFloor} and nothing else.
     */
    public boolean watermarkIs(final java.util.function.LongSupplier supplier) {
        return supplier != null && watermark == supplier;
    }

    /**
     * A purge pass also runs, without a new CM snapshot, when {@code purgeRequested} answers true
     * after the wake hook ran (the shadow housekeeping: a VERIFIED shadow appeared or the floor rose). Read once per
     * wake, never consumed here: the watermark supplier consumes it when the purge pass reads the floor. The
     * caught-up gate still applies. The poll interval is unchanged.
     */
    public SnapshotLogPruner wake(final BooleanSupplier purgeRequested) {
        this.purgeRequested = java.util.Objects.requireNonNull(purgeRequested, "purgeRequested");
        return this;
    }

    /**
     * {@code hook} runs on this thread after every sleep, before the purge decision (the shadow
     * housekeeping: catalog scan, paced deep reads, retention). A hook that throws is reported and that wake's
     * shadow request is ignored; the CM-snapshot path is not affected.
     */
    public SnapshotLogPruner onWake(final Runnable hook) {
        this.wakeHook = java.util.Objects.requireNonNull(hook, "hook");
        return this;
    }

    /**
     * Wire the retention floor (see the class comment): the leader bounds its purge by the lowest member position,
     * a follower by the leader's log start. Both engines call this on every member.
     *
     * <p>The settings are read here, once, and range checked: {@link #FLOOR_MODE_ENV},
     * {@link #MEMBER_WAIT_MS_ENV}, {@link #MEMBER_WAIT_BYTES_ENV} and {@link LogExtent#PROBE_TIMEOUT_ENV}
     * (environment first, then the system property, then the default). A malformed or out-of-range value throws
     * {@link IllegalArgumentException}; {@link EngineLifecycle#install} has already checked the same settings at the
     * top of the bootstrap ({@link #checkSettings}), so a bad value refuses the boot before any thread starts.</p>
     *
     * <p>The name is historic: the floor used to be reported only. {@code RETENTION_FLOOR=report} keeps that.</p>
     *
     * @param positions how to ask this node's consensus module for the members (who leads, each member's position
     *                  and archive endpoint)
     * @param isLeader  whether this node is the leader right now
     */
    public SnapshotLogPruner reportWatermark(final ClusterMemberPositions positions,
                                             final java.util.function.BooleanSupplier isLeader) {
        final FloorMode mode = floorModeSetting();
        final long waitMs = EngineLifecycle.rangedSetting(MEMBER_WAIT_MS_ENV, MEMBER_WAIT_MS_PROP,
            DEFAULT_MEMBER_WAIT_MS, MIN_MEMBER_WAIT_MS, MAX_MEMBER_WAIT_MS);
        final long waitBytes = EngineLifecycle.rangedSetting(MEMBER_WAIT_BYTES_ENV, MEMBER_WAIT_BYTES_PROP,
            DEFAULT_MEMBER_WAIT_BYTES, MIN_MEMBER_WAIT_BYTES, MAX_MEMBER_WAIT_BYTES);
        this.probeTimeoutMs = LogExtent.probeTimeoutMs();
        this.positions = positions;
        this.isLeader = isLeader;
        return retentionFloor(mode, waitMs, waitBytes);
    }

    /**
     * Parse and range check the retention floor's settings without wiring anything: {@link EngineLifecycle#install}
     * calls this first in the bootstrap, so a malformed value refuses the start.
     *
     * @throws IllegalArgumentException naming the setting and its range
     */
    public static void checkSettings() {
        floorModeSetting();
        EngineLifecycle.rangedSetting(MEMBER_WAIT_MS_ENV, MEMBER_WAIT_MS_PROP,
            DEFAULT_MEMBER_WAIT_MS, MIN_MEMBER_WAIT_MS, MAX_MEMBER_WAIT_MS);
        EngineLifecycle.rangedSetting(MEMBER_WAIT_BYTES_ENV, MEMBER_WAIT_BYTES_PROP,
            DEFAULT_MEMBER_WAIT_BYTES, MIN_MEMBER_WAIT_BYTES, MAX_MEMBER_WAIT_BYTES);
        LogExtent.probeTimeoutMs();
    }

    /** The floor's mode and bound, set directly (tests; a host that parses the settings itself). */
    public SnapshotLogPruner retentionFloor(final FloorMode mode, final long memberWaitMs,
                                            final long memberWaitBytes) {
        if (memberWaitMs <= 0 || memberWaitBytes <= 0) {
            throw new IllegalArgumentException("retention floor bounds must be positive");
        }
        this.floorMode = java.util.Objects.requireNonNull(mode, "mode");
        this.memberWaitMs = memberWaitMs;
        this.memberWaitBytes = memberWaitBytes;
        this.retention = new RetentionWatermark(memberWaitBytes, memberWaitMs,
            RetentionWatermark.DEFAULT_STALE_APPEND_MS);
        return this;
    }

    /** How often the pruner wakes (tests shorten it); the default is {@link #pollIntervalSeconds()}. */
    SnapshotLogPruner pollIntervalMs(final long pollIntervalMs) {
        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("poll interval must be positive");
        }
        this.pollIntervalMs = pollIntervalMs;
        return this;
    }

    static FloorMode floorModeSetting() {
        return parseFloorMode(System.getenv(FLOOR_MODE_ENV), System.getProperty(FLOOR_MODE_PROP));
    }

    static FloorMode parseFloorMode(final String envValue, final String propValue) {
        final String raw = envValue != null && !envValue.isEmpty() ? envValue
            : propValue != null && !propValue.isEmpty() ? propValue : null;
        if (raw == null) {
            return FloorMode.APPLY;
        }
        switch (raw.trim()) {
            case "apply":
                return FloorMode.APPLY;
            case "report":
                return FloorMode.REPORT;
            default:
                throw new IllegalArgumentException(FLOOR_MODE_ENV + " must be apply or report, got '" + raw + "'");
        }
    }

    /** Start watching. Idempotent. */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::run, "snapshot-log-pruner");
        thread.setDaemon(true);
        thread.start();
    }

    /** Stop watching and close the archive session. */
    public void stop() {
        running = false;
        final Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
        thread = null;
    }

    private void run() {
        AeronArchive archive = null;
        try {
            // Seed at the CURRENT count: only snapshots taken from here on trigger a
            // purge. Purging on the way up would run against a node that is still
            // recovering, which is the one moment it must not.
            long seenSnapshots = snapshotCount();
            System.out.println("[PRUNE] watching for snapshots (count=" + seenSnapshots
                + ", clusterDir=" + clusterDir + ")");

            while (running) {
                Thread.sleep(pollIntervalMs);

                noteMemberPositions();

                boolean shadowRequested;
                try {
                    wakeHook.run();
                    shadowRequested = purgeRequested.getAsBoolean();
                } catch (final RuntimeException hookFailed) {
                    shadowRequested = false;
                    lastError = "wake hook: " + hookFailed;
                    System.err.println("[PRUNE] CRITICAL: the wake hook failed, no shadow-requested purge this "
                        + "wake: " + hookFailed);
                }

                final long count = snapshotCount();
                final boolean newSnapshot = count > seenSnapshots;
                if (!newSnapshot && !shadowRequested) {
                    continue;
                }
                if (!caughtUp.getAsBoolean()) {
                    // Not an error and not a skip to remember: the next snapshot
                    // will bring us back here, and by then this node is either
                    // caught up or genuinely broken and saying so on /ready.
                    System.out.println("[PRUNE] " + (newSnapshot ? "snapshot " + count : "shadow purge request")
                        + " seen but this node is not caught up - not purging");
                    continue;
                }

                if (archive == null) {
                    archive = AeronArchive.connect(archiveContext);
                }

                // the node's own floor is read exactly once per pass: the shadow floor consumes its request there
                final long ownFloor = watermark.getAsLong();
                final long retentionFloor = retentionFloor(archive);
                lastAppliedFloor = retentionFloor;

                final ArchiveHousekeeping.Result result =
                    ArchiveHousekeeping.purgeBelow(clusterDir, archive, Math.min(ownFloor, retentionFloor));
                seenSnapshots = Math.max(seenSnapshots, count);
                if (!newSnapshot) {
                    wakesByShadow++;
                }
                prunesRun++;
                bytesReclaimed += Math.max(0, result.logBytesReclaimed);
                lastError = result.errors > 0
                    ? result.errors + " snapshot recording(s) missing from the archive"
                    : floorMode == FloorMode.APPLY ? floorProblem : null;
                System.out.println("[PRUNE] after " + (newSnapshot ? "snapshot " + count : "shadow wake")
                    + ": " + result);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final Exception e) {
            if (!running) {
                // stop() interrupted an archive or consensus module call in flight: a stop, not a failure
                return;
            }
            // A pruner that dies must not do it silently: from here the log grows
            // without bound and everything else still looks healthy.
            lastError = String.valueOf(e);
            System.err.println("[PRUNE] CRITICAL: the log pruner stopped - this node will not "
                + "reclaim archive disk until it is restarted: " + e);
            e.printStackTrace();
        } finally {
            CloseHelper.quietClose(archive);
        }
    }

    /**
     * Between purge passes, on the leader: note every member's position while it is still reporting, so a member
     * that dies is held at a position seen moments before, not at one from the last snapshot.
     */
    private void noteMemberPositions() {
        if (retention == null || positions == null) {
            return;
        }
        try {
            if (isLeader.getAsBoolean()) {
                retention.observe(positions.query(QUERY_TIMEOUT_MS), leadershipTermId(),
                    System.currentTimeMillis());
            }
        } catch (final RuntimeException e) {
            // noting is an optimisation: the purge pass asks again and holds when it gets no answer
            lastError = "member positions: " + e;
        }
    }

    /**
     * The retention floor for this purge pass: the position this node must not purge past for the sake of another
     * member, or {@code Long.MAX_VALUE} for no constraint (not wired, report mode, or a floor given up on after the
     * bound). 0 holds the whole log.
     */
    private long retentionFloor(final AeronArchive archive) {
        if (retention == null || positions == null) {
            return Long.MAX_VALUE;
        }
        final long nowMs = System.currentTimeMillis();
        long snapshotPosition = -1;
        long ownStart = -1;
        long floor;
        try {
            snapshotPosition = latestSnapshotPosition();
            if (snapshotPosition < 0) {
                return Long.MAX_VALUE; // no snapshot: nothing is purged anyway
            }
            ownStart = LogExtent.local(archive, clusterDir).startPosition;
            final ClusterMemberPositions.Snapshot answer = positions.query(QUERY_TIMEOUT_MS);
            if (answer == null) {
                floor = floorUnknown("this node's consensus module did not answer the members query",
                    snapshotPosition, ownStart, nowMs);
            } else if (isLeader.getAsBoolean()) {
                floor = leaderFloor(answer, snapshotPosition, ownStart, nowMs);
            } else {
                floor = followerFloor(archive, answer, snapshotPosition, ownStart, nowMs);
            }
        } catch (final Exception e) {
            if (!running) {
                throw e; // stop() interrupted a read in flight; run() treats it as the stop it is
            }
            // An error is not permission to purge: hold, within the bound.
            floor = floorUnknown("the floor could not be computed: " + e, snapshotPosition, Math.max(0, ownStart),
                nowMs);
        }
        if (floorMode == FloorMode.REPORT) {
            System.out.println("[RETENTION] " + FLOOR_MODE_ENV + "=report: the floor " + describe(floor)
                + " is not applied, purging to this node's own snapshot at " + snapshotPosition);
            return Long.MAX_VALUE;
        }
        return floor;
    }

    private static String describe(final long floor) {
        return floor == Long.MAX_VALUE ? "(none)" : String.valueOf(floor);
    }

    /** Leader: the lowest position of any member, each one subject to the bound. */
    private long leaderFloor(final ClusterMemberPositions.Snapshot answer, final long snapshotPosition,
                             final long ownStart, final long nowMs) {
        floorKnown();
        final RetentionWatermark.Result result = retention.compute(
            snapshotPosition, answer, -1, -1, nowMs, ownStart, leadershipTermId());

        lastWatermarkReport = result.toString();
        System.out.println("[RETENTION] leader: purge floor " + result.position
            + " (limiter=" + result.limiter + ", inputs=" + result.detail
            + ", members=" + answer.members.size() + " leader=" + answer.leaderMemberId
            + ", log start " + ownStart + ", snapshot at " + snapshotPosition + ")");

        for (final int member : result.stranded) {
            // Loud by design: this is a decision to stop waiting for a member,
            // and the only remedy is a reseed. An accidental disk-full is what
            // happens when nobody says this out loud.
            System.err.println("[RETENTION] CRITICAL: member " + member + " STRANDED: it has held the retention"
                + " floor past the bound (" + MEMBER_WAIT_MS_ENV + " " + memberWaitMs + " ms, "
                + MEMBER_WAIT_BYTES_ENV + " " + memberWaitBytes + " bytes) and been written off; the purge"
                + " proceeds without it. If it returns below this log's start it cannot catch up from the log and"
                + " must be reseeded.");
        }
        return result.position;
    }

    /** Follower: never past the leader's log start, read from the leader's archive. */
    private long followerFloor(final AeronArchive archive, final ClusterMemberPositions.Snapshot answer,
                               final long snapshotPosition, final long ownStart, final long nowMs) {
        final ClusterMemberPositions.Member leader = answer.member(answer.leaderMemberId);
        final ClusterMemberPositions.Member self = answer.member(answer.memberId);
        if (leader == null || self == null || leader == self
            || leader.archiveEndpoint == null || self.archiveEndpoint == null) {
            return floorUnknown("this follower's consensus module names no leader with an archive endpoint (leader="
                + answer.leaderMemberId + ")", snapshotPosition, ownStart, nowMs);
        }
        final LogExtent extent;
        try {
            final int logStreamId = LogExtent.local(archive, clusterDir).streamId;
            try (AeronArchive remote = AeronArchive.connect(LogExtent.remoteContext(null,
                driverDirectoryName(), leader.archiveEndpoint,
                LogExtent.hostOf(self.archiveEndpoint), probeTimeoutMs,
                t -> System.err.println("[RETENTION] reading the leader's archive: " + t)))) {
                extent = LogExtent.byStream(remote, logStreamId);
            }
        } catch (final RuntimeException e) {
            if (!running) {
                throw e; // stop() interrupted the read: a stop, not an unknown floor
            }
            return floorUnknown("the log start of the leader (member " + leader.memberId + ") could not be read from"
                + " its archive at " + leader.archiveEndpoint + ": " + e, snapshotPosition, ownStart, nowMs);
        }
        if (extent == null) {
            return floorUnknown("the archive of the leader (member " + leader.memberId + ") at "
                + leader.archiveEndpoint + " holds no log recording", snapshotPosition, ownStart, nowMs);
        }
        floorKnown();
        lastWatermarkReport = "follower floor=" + extent.startPosition + " leader=" + leader.memberId;
        System.out.println("[RETENTION] follower: purge floor " + extent.startPosition + " (the log start of the"
            + " leader, member " + leader.memberId + "; own log start " + ownStart + ", snapshot at "
            + snapshotPosition + ")");
        return extent.startPosition;
    }

    /**
     * The media driver's directory for a client of this class's own. The archive context the engines hand over is a
     * clone of the service container's: it carries the container's live Aeron client, and a directory name that is
     * whatever it was when the context was built, which next to an external driver need not be the driver's. The
     * live client's directory is the truth; the name is the fallback for a context without a client.
     */
    private String driverDirectoryName() {
        final io.aeron.Aeron client = archiveContext.aeron();
        return client != null ? client.context().aeronDirectoryName() : archiveContext.aeronDirectoryName();
    }

    private void floorKnown() {
        floorUnknownSinceMs = -1;
        floorUnknownGivenUp = false;
        floorProblem = null;
    }

    /**
     * The floor cannot be worked out. Hold the whole log (0), but not for ever: after the member wait, or once this
     * node holds the byte bound of log below its own snapshot, say so and purge to the snapshot again.
     */
    private long floorUnknown(final String why, final long snapshotPosition, final long ownStart, final long nowMs) {
        if (floorUnknownSinceMs < 0) {
            floorUnknownSinceMs = nowMs;
        }
        final long unknownMs = nowMs - floorUnknownSinceMs;
        final long heldBytes = snapshotPosition - ownStart;
        // on health too: until the floor is known again this node either holds its whole log or purges unbounded
        floorProblem = "retention floor unknown for " + unknownMs + " ms: " + why;
        if (unknownMs >= memberWaitMs || heldBytes >= memberWaitBytes) {
            if (!floorUnknownGivenUp) {
                floorUnknownGivenUp = true;
                System.err.println("[RETENTION] CRITICAL: the retention floor has been unknown for " + unknownMs
                    + " ms with " + heldBytes + " bytes of log held below the snapshot (" + why + "); the bound ("
                    + MEMBER_WAIT_MS_ENV + " " + memberWaitMs + " ms, " + MEMBER_WAIT_BYTES_ENV + " "
                    + memberWaitBytes + " bytes) has passed and this node purges to its own snapshot again. A"
                    + " member below the new log start cannot catch up from this node.");
            }
            lastWatermarkReport = "floor unknown, bound passed: " + why;
            return Long.MAX_VALUE;
        }
        lastWatermarkReport = "floor unknown, holding: " + why;
        System.out.println("[RETENTION] holding the log at its start " + ownStart + ": " + why + " (unknown for "
            + unknownMs + " ms of " + memberWaitMs + " ms, " + heldBytes + " of " + memberWaitBytes + " bytes)");
        return 0;
    }

    /** The consensus module's leadership term id, or -1 when its counter is not published. */
    private long leadershipTermId() {
        final int counterId = ClusterCounters.find(
            countersReader, io.aeron.AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, clusterId);
        return counterId == CountersReader.NULL_COUNTER_ID ? -1 : countersReader.getCounterValue(counterId);
    }

    /** This node's newest valid snapshot position, or -1. */
    private long latestSnapshotPosition() {
        long latest = -1;
        try (io.aeron.cluster.RecordingLog recordingLog =
                 new io.aeron.cluster.RecordingLog(clusterDir, false)) {
            for (final io.aeron.cluster.RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == io.aeron.cluster.RecordingLog.ENTRY_TYPE_SNAPSHOT
                    && entry.isValid && entry.logPosition > latest) {
                    latest = entry.logPosition;
                }
            }
        }
        return latest;
    }

    /** The retention floor the last purge pass applied ({@code Long.MAX_VALUE}: none), for status and tests. */
    public long lastAppliedFloor() {
        return lastAppliedFloor;
    }

    /** The last retention report, for status and tests. */
    public String lastWatermarkReport() {
        return lastWatermarkReport;
    }

    /**
     * The consensus module's snapshot counter, or -1 when it is not published
     * (the module is not up yet). -1 never looks like progress.
     */
    private long snapshotCount() {
        final int counterId = ClusterCounters.find(
            countersReader, ConsensusModule.Configuration.SNAPSHOT_COUNTER_TYPE_ID, clusterId);
        return counterId == CountersReader.NULL_COUNTER_ID ? -1 : countersReader.getCounterValue(counterId);
    }

    /** How many purges have run on this node. */
    public long prunesRun() {
        return prunesRun;
    }

    /** {@code pruner_wakes_by_shadow_total}: purge passes run because a shadow requested one, not a CM snapshot. */
    public long wakesByShadow() {
        return wakesByShadow;
    }

    /** Total log bytes reclaimed on this node since start. */
    public long bytesReclaimed() {
        return bytesReclaimed;
    }

    /** The last thing that went wrong, or null. */
    public String lastError() {
        return lastError;
    }

    /** Seconds between counter reads by default, for tests and documentation. */
    public static long pollIntervalSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(POLL_INTERVAL_MS);
    }
}
