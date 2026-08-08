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
 * <p>Log segments below THIS node's newest snapshot. That is the bound this
 * node's own recovery needs, and it is all a single member can compute: no
 * member can see how far the others have got (the leader can, and that is where
 * the retention watermark belongs - it holds the purge at the slowest consumer
 * instead).</p>
 *
 * <p>So the cost is stated rather than hidden: <b>a member that was offline
 * across a snapshot can no longer catch up from this node's log and has to be
 * reseeded by hand.</b> That is match#35, it is what etcd does with its own
 * compacted WAL, and it is the deliberate trade - a stranded member is one
 * loud, recoverable node, while an unbounded log is the whole cluster at a
 * scheduled time. The stranding is reported; recovery is an operator action, by
 * design.</p>
 *
 * <p>Two guards, both cheap. A node purges nothing unless it reports itself
 * caught up ({@link NodeReadiness#ready()}) - a member still replaying has no
 * business deleting log. And a purge that finds a snapshot recording missing
 * from the archive says so as a CRITICAL, because that node's
 * recover-from-snapshot is already broken.</p>
 */
public final class SnapshotLogPruner {

    /** How often the snapshot counter is read. Nothing else happens on these wakeups. */
    private static final long POLL_INTERVAL_MS = 5_000;

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

    // Retention reporting: computed on the leader after each snapshot, logged, and
    // NOT yet applied to the purge. Null until reportWatermark() wires it.
    private ClusterMemberPositions positions;
    private java.util.function.BooleanSupplier isLeader;
    private RetentionWatermark retention;
    private volatile String lastWatermarkReport;

    private volatile boolean running;
    private Thread thread;
    private long prunesRun;
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
     * Compute the retention watermark on the leader after each snapshot, and REPORT
     * it. It is not applied to the purge yet.
     *
     * <p>That split is deliberate and it is the same one the gateway made. The
     * watermark replaces a boolean whose two branches are "strand a member" and
     * "grow without bound", so it is a real improvement, but it changes what gets
     * DELETED from a live money ledger and the direction that hurts is deleting
     * what a member still needed. Watching the computed numbers under real traffic
     * costs nothing; being wrong costs a reseed of the ledger.</p>
     *
     * @param positions how to ask the consensus module (leader only; see that class)
     * @param isLeader  whether this node is the leader right now
     */
    public SnapshotLogPruner reportWatermark(final ClusterMemberPositions positions,
                                             final java.util.function.BooleanSupplier isLeader) {
        this.positions = positions;
        this.isLeader = isLeader;
        this.retention = new RetentionWatermark();
        return this;
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
                Thread.sleep(POLL_INTERVAL_MS);

                final long count = snapshotCount();
                if (count <= seenSnapshots) {
                    continue;
                }
                if (!caughtUp.getAsBoolean()) {
                    // Not an error and not a skip to remember: the next snapshot
                    // will bring us back here, and by then this node is either
                    // caught up or genuinely broken and saying so on /ready.
                    System.out.println("[PRUNE] snapshot " + count
                        + " seen but this node is not caught up - not purging");
                    continue;
                }

                if (archive == null) {
                    archive = AeronArchive.connect(archiveContext);
                }

                reportRetentionWatermark();

                final ArchiveHousekeeping.Result result =
                    ArchiveHousekeeping.purgeBelow(clusterDir, archive, watermark.getAsLong());
                seenSnapshots = count;
                prunesRun++;
                bytesReclaimed += Math.max(0, result.logBytesReclaimed);
                lastError = result.errors > 0
                    ? result.errors + " snapshot recording(s) missing from the archive" : null;
                System.out.println("[PRUNE] after snapshot " + count + ": " + result);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final Exception e) {
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
     * Ask the leader's consensus module how far every member has got, work out what
     * that would allow, and say so. Nothing acts on the answer yet.
     *
     * <p>Only on the leader: a follower's consensus module does not track anyone
     * else's position, so its answer would be a confident description of nothing.</p>
     */
    private void reportRetentionWatermark() {
        if (retention == null || !isLeader.getAsBoolean()) {
            return;
        }
        try {
            final long snapshotPosition = latestSnapshotPosition();
            if (snapshotPosition < 0) {
                return;
            }
            final ClusterMemberPositions.Snapshot answer = positions.query(QUERY_TIMEOUT_MS);
            final RetentionWatermark.Result result = retention.compute(
                snapshotPosition, answer, -1, -1, System.currentTimeMillis());

            lastWatermarkReport = result.toString();
            System.out.println("[RETENTION] would purge below " + result.position
                + " (limiter=" + result.limiter + ", inputs=" + result.detail
                + (answer == null
                    ? ", CONSENSUS MODULE DID NOT ANSWER - holding at the last known positions"
                    : ", members=" + answer.members.size() + " leader=" + answer.leaderMemberId)
                + ") - not applied yet, purging to the snapshot at " + snapshotPosition);

            for (final int member : result.stranded) {
                // Loud by design: this is a decision to stop waiting for a member,
                // and the only remedy is a reseed. An accidental disk-full is what
                // happens when nobody says this out loud.
                System.err.println("[RETENTION] member " + member + " STRANDED: it has held the "
                    + "retention watermark past the bound and been written off. It cannot "
                    + "log-catch-up and must be reseeded.");
            }
        } catch (final Exception e) {
            // Reporting must never be the reason a purge does not happen.
            System.err.println("[RETENTION] could not compute the watermark: " + e);
        }
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

    /** Total log bytes reclaimed on this node since start. */
    public long bytesReclaimed() {
        return bytesReclaimed;
    }

    /** The last thing that went wrong, or null. */
    public String lastError() {
        return lastError;
    }

    /** Seconds between counter reads, for tests and documentation. */
    public static long pollIntervalSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(POLL_INTERVAL_MS);
    }
}
