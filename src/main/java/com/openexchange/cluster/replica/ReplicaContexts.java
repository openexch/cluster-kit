// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.ClusterPorts;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupEventsListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The replica's Aeron contexts from a {@link ReplicaConfig}. Fixed slot ports, never {@code :0}
 * (network isolation needs them):
 *
 * <pre>
 * p1 ARCHIVE_CONTROL  the replica archive's control endpoint (shadow pulls, the operator)
 * p3 MEMBER_FACING    the consensus response channel members answer a BackupQuery on
 * p4 LOG              the catch-up endpoint: the supplier's live-log replay AND member snapshot replication
 *                     (CBA:722-724), different sessions on one endpoint
 * p5 TRANSFER         the cluster-archive client's control response channel; the replica archive's own
 *                     replicationChannel and archiveClientContext sit on p5 too for replication into this archive
 *                     (different stream id), unused by the replica itself
 * p2 CLIENT_FACING    unused
 * </pre>
 *
 * <p>Builds contexts only; {@link ReplicaProcess} is the one class that launches a driver, an archive or the backup
 * (scanned).</p>
 */
public final class ReplicaContexts {

    /** The replica archive's own replication client: a stream id distinct from the cluster-archive client's (20). */
    public static final int ARCHIVE_CLIENT_RESPONSE_STREAM_ID = 21;
    /** The local control channel at the default {@code REPLICA_DRIVER_TERM_LENGTH}; see {@link #localControlChannel}. */
    public static final String LOCAL_CONTROL_CHANNEL = "aeron:ipc?term-length=" + ReplicaConfig.DEFAULT_DRIVER_TERM_LENGTH;

    private ReplicaContexts() {
    }

    static int port(final ReplicaConfig c, final int memberOrSlot, final int offset) {
        return ClusterPorts.port(memberOrSlot, c.portBase, offset, c.portStride);
    }

    public static String replicaEndpoint(final ReplicaConfig c, final int offset) {
        return c.replicaAddress + ":" + port(c, c.slot, offset);
    }

    /** Every member's {@code host:port(memberId, base, MEMBER_FACING)}, the string the members publish (CFG:342). */
    public static String consensusEndpoints(final ReplicaConfig c) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < c.memberHosts.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(c.memberHosts.get(i)).append(':').append(port(c, i, ClusterPorts.MEMBER_FACING_OFFSET));
        }
        return sb.toString();
    }

    public static String consensusChannel(final ReplicaConfig c) {
        return "aeron:udp?endpoint=" + replicaEndpoint(c, ClusterPorts.MEMBER_FACING_OFFSET);
    }

    public static String catchupEndpoint(final ReplicaConfig c) {
        return replicaEndpoint(c, ClusterPorts.LOG_OFFSET);
    }

    /**
     * Aeron's default ({@code CB:346-347}) with the send buffer (512k), receive buffer and window (4m, 4m) from
     * settings.
     */
    public static String catchupChannel(final ReplicaConfig c) {
        return "aeron:udp?alias=backup|cc=cubic|so-sndbuf=" + c.catchupSndbuf + "|so-rcvbuf=" + c.catchupRcvbuf
                + "|rcv-wnd=" + c.catchupRcvWnd;
    }

    public static String archiveControlChannel(final ReplicaConfig c) {
        return "aeron:udp?endpoint=" + replicaEndpoint(c, ClusterPorts.ARCHIVE_CONTROL_OFFSET);
    }

    public static String transferChannel(final ReplicaConfig c) {
        return "aeron:udp?endpoint=" + replicaEndpoint(c, ClusterPorts.TRANSFER_OFFSET);
    }

    public static IdleStrategy idle(final ReplicaConfig c) {
        return c.busySpin ? new BusySpinIdleStrategy() : new BackoffIdleStrategy(10, 20, 1_000, 1_000_000);
    }

    /**
     * Embedded mode only: the member driver tuning of AC:197-209 plus the image liveness. The directory is
     * checked before it is replaced ({@code dirDeleteOnStart(false)}, MD:479-510): a stale directory (no driver
     * heartbeat within {@code driverTimeoutMs}) is deleted and recreated, a live driver's directory is refused with
     * {@code ActiveDriverException}. {@code AERON_DIR} is honoured in embedded mode, so an operator's
     * {@code TRANSPORT_DRIVER_MODE=embedded} next to an aeronmd's {@code AERON_DIR} must never take over the shared
     * driver (the external-driver negative control).
     */
    public static MediaDriver.Context driver(final ReplicaConfig c) {
        return new MediaDriver.Context()
                .aeronDirectoryName(c.aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(idle(c))
                .dirDeleteOnStart(false)
                .dirDeleteOnShutdown(true)
                .driverTimeoutMs(500)
                .timerIntervalNs(TimeUnit.MICROSECONDS.toNanos(500))
                .clientLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                .publicationUnblockTimeoutNs(TimeUnit.SECONDS.toNanos(10))
                .imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(c.imageLivenessMs))
                .socketSndbufLength(c.socketBufferLength)
                .socketRcvbufLength(c.socketBufferLength)
                .initialWindowLength(c.socketBufferLength)
                // settings with the member driver's values as defaults (16m terms, 8192 MTU)
                .ipcTermBufferLength(c.driverTermLength)
                .publicationTermBufferLength(c.driverTermLength)
                .mtuLength(c.driverMtu);
    }

    /** {@code BASE_DIR/archive}: shadows, the live log L, the retrieved member sets. Never deleted on start. */
    public static Archive.Context archive(final ReplicaConfig c, final ErrorHandler errorHandler) {
        return new Archive.Context()
                .aeronDirectoryName(c.aeronDir)
                .archiveDir(c.archiveDir())
                .deleteArchiveOnStart(false)
                .controlChannel(archiveControlChannel(c))
                .localControlChannel(localControlChannel(c))
                // replication into this archive: fixed p5, never used by the replica itself
                .replicationChannel(transferChannel(c))
                .archiveClientContext(new AeronArchive.Context()
                        .controlResponseChannel(transferChannel(c))
                        .controlResponseStreamId(ARCHIVE_CLIENT_RESPONSE_STREAM_ID))
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .idleStrategySupplier(() -> idle(c))
                .segmentFileLength(c.archiveSegmentLength)
                .maxConcurrentRecordings(10)
                .maxConcurrentReplays(10)
                .errorHandler(Objects.requireNonNull(errorHandler, "errorHandler"));
    }

    /** The local control channel, its term length from {@code REPLICA_DRIVER_TERM_LENGTH}. */
    public static String localControlChannel(final ReplicaConfig c) {
        return "aeron:ipc?term-length=" + c.driverTermLength;
    }

    /** A client of the replica's own archive over IPC (the follower, the writer, housekeeping each take a clone). */
    public static AeronArchive.Context localArchiveClient(final ReplicaConfig c) {
        return new AeronArchive.Context()
                .aeronDirectoryName(c.aeronDir)
                .controlRequestChannel(localControlChannel(c))
                .controlResponseChannel(localControlChannel(c));
    }

    /** The supplier side: the agent fills the request endpoint from the BackupResponse (CBA:578-592). */
    public static AeronArchive.Context clusterArchiveClient(final ReplicaConfig c) {
        return new AeronArchive.Context()
                .aeronDirectoryName(c.aeronDir)
                .controlRequestChannel("aeron:udp?endpoint=0.0.0.0:0")
                .controlResponseChannel(transferChannel(c));
    }

    /**
     * {@code deleteDirOnStart(false)} (L is extended across restarts, CBA:1099-1105), {@code initialReplayStart}
     * BEGINNING, the tuned backup defaults from settings, the REPLICA credential, the events listener.
     */
    public static ClusterBackup.Context backup(final ReplicaConfig c, final ClusterBackupEventsListener events,
                                               final ErrorHandler errorHandler, final Runnable terminationHook) {
        return new ClusterBackup.Context()
                .aeronDirectoryName(c.aeronDir)
                .clusterDir(c.clusterDir())
                .deleteDirOnStart(false)
                .clusterId(c.clusterId)
                .logStreamId(c.logStreamId) // L's stream, also what orphan detection reads
                .archiveContext(localArchiveClient(c))
                .clusterArchiveContext(clusterArchiveClient(c))
                .clusterConsensusEndpoints(consensusEndpoints(c))
                .consensusChannel(consensusChannel(c))
                .catchupEndpoint(catchupEndpoint(c))
                .catchupChannel(catchupChannel(c))
                .sourceType(c.sourceType)
                .initialReplayStart(ClusterBackup.Configuration.ReplayStart.BEGINNING)
                .clusterBackupIntervalNs(TimeUnit.MILLISECONDS.toNanos(c.backupQueryIntervalMs))
                .clusterBackupResponseTimeoutNs(TimeUnit.MILLISECONDS.toNanos(c.backupResponseTimeoutMs))
                .clusterBackupCoolDownIntervalNs(TimeUnit.MILLISECONDS.toNanos(c.backupCooldownMs))
                .clusterBackupProgressTimeoutNs(TimeUnit.MILLISECONDS.toNanos(c.backupProgressTimeoutMs))
                .credentialsSupplier(c.credentials)
                .eventsListener(Objects.requireNonNull(events, "events"))
                .errorHandler(Objects.requireNonNull(errorHandler, "errorHandler"))
                .terminationHook(Objects.requireNonNull(terminationHook, "terminationHook"));
    }

    /**
     * A cool-down or query interval below the members' backup window locks a reset loop out
     * (one {@code -1} request per member per window, CA:46-47, CA:173-189). Settings, not limits: the hazard is
     * a CRITICAL line, never a refusal. Returns the lines (empty when none).
     */
    public static java.util.List<String> windowWarnings(final ReplicaConfig c) {
        final java.util.List<String> lines = new java.util.ArrayList<>();
        // the window plus one second, as the default cool-down: 12 000 with the 11 000 window
        final long minimum = c.memberBackupWindowMs + 1_000;
        if (c.backupCooldownMs < minimum) {
            lines.add("CRITICAL: REPLICA_BACKUP_COOLDOWN_MS=" + c.backupCooldownMs
                    + " below the members' backup window (REPLICA_MEMBER_BACKUP_WINDOW_MS=" + c.memberBackupWindowMs
                    + " + 1000): a backup reset loop re-queries a member inside its window and is AUTHENTICATION_REJECTED"
                    + " (locked out)");
        }
        if (c.backupQueryIntervalMs < minimum) {
            lines.add("CRITICAL: REPLICA_BACKUP_QUERY_INTERVAL_MS=" + c.backupQueryIntervalMs
                    + " below the members' backup window (REPLICA_MEMBER_BACKUP_WINDOW_MS=" + c.memberBackupWindowMs
                    + " + 1000): steady-state queries become ambiguous");
        }
        return lines;
    }
}
