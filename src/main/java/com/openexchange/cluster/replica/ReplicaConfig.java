// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.shadow.ShadowFloor;
import io.aeron.cluster.ClusterBackup;
import io.aeron.security.CredentialsSupplier;

import java.io.File;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Host-agnostic values of one replica process: addresses, slot, ports, dirs, timeouts, the source
 * type and the credentials. The host (core {@code ReplicaSettings}) parses its environment, applies the ranges and
 * refusal texts of its settings and builds this; the kit never reads an environment variable. {@link Builder#build}
 * re-checks only what would make the process structurally wrong (a missing value, a non-positive period, a
 * negative-control seam without its gate) and names the setting in the refusal.
 */
public final class ReplicaConfig {

    /** {@code TRANSPORT_DRIVER_MODE}: production is EXTERNAL. */
    public enum DriverMode { EMBEDDED, EXTERNAL }

    public static final long DEFAULT_MEMBER_BACKUP_WINDOW_MS = 11_000;
    public static final long DEFAULT_BACKUP_COOLDOWN_MS = DEFAULT_MEMBER_BACKUP_WINDOW_MS + 1_000;
    public static final long DEFAULT_BACKUP_RESPONSE_TIMEOUT_MS = 2_000;
    public static final long DEFAULT_BACKUP_QUERY_INTERVAL_MS = 60_000;
    public static final long DEFAULT_BACKUP_PROGRESS_TIMEOUT_MS = 10_000;
    public static final long DEFAULT_IMAGE_LIVENESS_MS = 3_000;
    public static final String DEFAULT_CATCHUP_RCVBUF = "4m";
    public static final String DEFAULT_CATCHUP_RCV_WND = "4m";
    /** Aeron's catch-up channel default ({@code CB:346-347}); a setting. */
    public static final String DEFAULT_CATCHUP_SNDBUF = "512k";
    /** The embedded driver's MTU (the member driver's, AC:197-209); a setting. */
    public static final int DEFAULT_DRIVER_MTU = 8192;
    /** The embedded driver's IPC and publication term buffer length; a setting. */
    public static final int DEFAULT_DRIVER_TERM_LENGTH = 16 * 1024 * 1024;
    public static final int DEFAULT_SHADOW_BUFFER_MB = 512;
    /** H14: a shadow payload plus its header fits an int-sized buffer, so one image buffer holds at most this. */
    public static final int MAX_SHADOW_BUFFER_MB = 2047;
    public static final long DEFAULT_ALIGNED_WAIT_MS = 90_000;
    public static final long DEFAULT_STANDBY_FALLBACK_MS = 30L * 60_000;
    public static final int DEFAULT_RETRIEVED_SETS_KEEP = 2;
    public static final long DEFAULT_READY_MAX_LAG_BYTES = 64L * 1024 * 1024;
    public static final long DEFAULT_READY_SHADOW_AGE_MS = 30L * 60_000;
    public static final long DEFAULT_READY_STALE_MS = 5_000;
    public static final long DEFAULT_LIVENESS_STALE_MS = 30_000;
    /** The leader's drain belt ticks the log every ~10 ms, so 3 s of cluster time is stale. */
    public static final long DEFAULT_READY_TIME_LAG_MS = 3_000;
    public static final long DEFAULT_ATTACH_BACKOFF_MS = 250;
    public static final long MAX_ATTACH_BACKOFF_MS = 5_000;
    public static final long DEFAULT_BOOTSTRAP_WAIT_MS = 120_000;
    /**
     * The stall bound of an UNVERIFIED replica: never shorter than one backup cycle (2 x the query interval + the
     * cool-down); 180 000 passes with the defaults (2 x 60 000 + 12 000).
     */
    public static final long DEFAULT_VERIFY_WAIT_MS = 180_000;
    /** The cluster log stream the backup records L on (the members' CFG sets none: Aeron's). */
    public static final int DEFAULT_LOG_STREAM_ID = io.aeron.cluster.ConsensusModule.Configuration.LOG_STREAM_ID_DEFAULT;
    /** The members' snapshot streams (service 106, consensus module 107; Aeron's defaults). */
    public static final Set<Integer> DEFAULT_SNAPSHOT_STREAM_IDS = Set.of(
            io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_STREAM_ID_DEFAULT,
            io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_STREAM_ID_DEFAULT);
    public static final int DEFAULT_REPLAY_STREAM_ID = 4303;
    public static final int DEFAULT_FRAGMENT_LIMIT = 64;
    public static final String DEFAULT_SHADOW_TERM_LENGTH = "4m";
    public static final long DEFAULT_WRITE_CONNECT_TIMEOUT_MS = 10_000;
    public static final long DEFAULT_WRITE_OFFER_TIMEOUT_MS = 30_000;
    public static final long DEFAULT_WRITE_RECORD_TIMEOUT_MS = 60_000;
    public static final int DEFAULT_RETAIN_N = 3;
    public static final long DEFAULT_DEEP_READ_PACE_MS = 2_000;
    public static final long DEFAULT_SCAN_PERIOD_MS = 5_000;
    public static final long DEFAULT_LOAD_TIMEOUT_MS = 60_000;
    public static final long DEFAULT_EXTERNAL_DRIVER_TIMEOUT_MS = 30_000;
    public static final int DEFAULT_SOCKET_BUFFER = 4 * 1024 * 1024;
    public static final int DEFAULT_ARCHIVE_SEGMENT_LENGTH = 64 * 1024 * 1024;

    final List<String> memberHosts;
    final int portBase;
    final int portStride;
    final int slot;
    final String replicaAddress;
    final File baseDir;
    final int clusterId;
    final int engineId;
    final DriverMode driverMode;
    final String aeronDir;
    final long externalDriverTimeoutMs;
    final boolean busySpin;
    final ClusterBackup.SourceType sourceType;
    final CredentialsSupplier credentials;
    final long memberBackupWindowMs;
    final long backupCooldownMs;
    final long backupResponseTimeoutMs;
    final long backupQueryIntervalMs;
    final long backupProgressTimeoutMs;
    final long imageLivenessMs;
    final String catchupRcvbuf;
    final String catchupRcvWnd;
    final String catchupSndbuf;
    final int driverMtu;
    final int driverTermLength;
    final int socketBufferLength;
    final int archiveSegmentLength;
    final int shadowBufferMb;
    final long alignedWaitMs;
    final long standbyFallbackMs;
    final int retrievedSetsKeep;
    final long readyMaxLagBytes;
    final long readyShadowAgeMs;
    final long readyStaleMs;
    final long livenessStaleMs;
    final long readyTimeLagMs;
    final long attachBackoffMs;
    final long bootstrapWaitMs;
    final long verifyWaitMs;
    final int logStreamId;
    final Set<Integer> snapshotStreamIds;
    final int replayStreamId;
    final int fragmentLimit;
    final String shadowTermLength;
    final long writeConnectTimeoutMs;
    final long writeOfferTimeoutMs;
    final long writeRecordTimeoutMs;
    final int retainN;
    final ShadowFloor.Margin floorMargin;
    final long deepReadPaceMs;
    final long scanPeriodMs;
    final long loadTimeoutMs;
    final int nodePort;
    final ReplicaFaults faults;
    final Set<ReplicaNegativeControl> off;

    private ReplicaConfig(final Builder b) {
        this.memberHosts = List.copyOf(b.memberHosts);
        this.portBase = b.portBase;
        this.portStride = b.portStride;
        this.slot = b.slot;
        this.replicaAddress = b.replicaAddress;
        this.baseDir = b.baseDir;
        this.clusterId = b.clusterId;
        this.engineId = b.engineId;
        this.driverMode = b.driverMode;
        this.aeronDir = b.aeronDir;
        this.externalDriverTimeoutMs = b.externalDriverTimeoutMs;
        this.busySpin = b.busySpin;
        this.sourceType = b.sourceType;
        this.credentials = b.credentials;
        this.memberBackupWindowMs = b.memberBackupWindowMs;
        this.backupCooldownMs = b.backupCooldownMs;
        this.backupResponseTimeoutMs = b.backupResponseTimeoutMs;
        this.backupQueryIntervalMs = b.backupQueryIntervalMs;
        this.backupProgressTimeoutMs = b.backupProgressTimeoutMs;
        this.imageLivenessMs = b.imageLivenessMs;
        this.catchupRcvbuf = b.catchupRcvbuf;
        this.catchupRcvWnd = b.catchupRcvWnd;
        this.catchupSndbuf = b.catchupSndbuf;
        this.driverMtu = b.driverMtu;
        this.driverTermLength = b.driverTermLength;
        this.socketBufferLength = b.socketBufferLength;
        this.archiveSegmentLength = b.archiveSegmentLength;
        this.shadowBufferMb = b.shadowBufferMb;
        this.alignedWaitMs = b.alignedWaitMs;
        this.standbyFallbackMs = b.standbyFallbackMs;
        this.retrievedSetsKeep = b.retrievedSetsKeep;
        this.readyMaxLagBytes = b.readyMaxLagBytes;
        this.readyShadowAgeMs = b.readyShadowAgeMs;
        this.readyStaleMs = b.readyStaleMs;
        this.livenessStaleMs = b.livenessStaleMs;
        this.readyTimeLagMs = b.readyTimeLagMs;
        this.attachBackoffMs = b.attachBackoffMs;
        this.bootstrapWaitMs = b.bootstrapWaitMs;
        this.verifyWaitMs = b.verifyWaitMs;
        this.logStreamId = b.logStreamId;
        this.snapshotStreamIds = Set.copyOf(b.snapshotStreamIds);
        this.replayStreamId = b.replayStreamId;
        this.fragmentLimit = b.fragmentLimit;
        this.shadowTermLength = b.shadowTermLength;
        this.writeConnectTimeoutMs = b.writeConnectTimeoutMs;
        this.writeOfferTimeoutMs = b.writeOfferTimeoutMs;
        this.writeRecordTimeoutMs = b.writeRecordTimeoutMs;
        this.retainN = b.retainN;
        this.floorMargin = b.floorMargin;
        this.deepReadPaceMs = b.deepReadPaceMs;
        this.scanPeriodMs = b.scanPeriodMs;
        this.loadTimeoutMs = b.loadTimeoutMs;
        this.nodePort = b.nodePort;
        this.faults = b.faults;
        this.off = b.off.isEmpty() ? EnumSet.noneOf(ReplicaNegativeControl.class) : EnumSet.copyOf(b.off);
    }

    /**
     * Null when {@code verifyWaitMs} covers one backup cycle, else the refusal text naming the three
     * values. The host's settings layer uses the same text (one rule, one wording).
     */
    public static String verifyWaitShorterThanOneBackupCycle(final long verifyWaitMs, final long queryIntervalMs,
                                                             final long cooldownMs) {
        final long cycle = 2 * queryIntervalMs + cooldownMs;
        if (verifyWaitMs >= cycle) {
            return null;
        }
        return "REPLICA_VERIFY_WAIT_MS=" + verifyWaitMs + " must be >= 2 x REPLICA_BACKUP_QUERY_INTERVAL_MS="
                + queryIntervalMs + " + REPLICA_BACKUP_COOLDOWN_MS=" + cooldownMs + " = " + cycle + " (the stall bound"
                + " cannot be shorter than one backup cycle)";
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> memberHosts() {
        return memberHosts;
    }

    public int slot() {
        return slot;
    }

    public String replicaAddress() {
        return replicaAddress;
    }

    public File baseDir() {
        return baseDir;
    }

    public File archiveDir() {
        return new File(baseDir, "archive");
    }

    public File clusterDir() {
        return new File(baseDir, "cluster");
    }

    public int clusterId() {
        return clusterId;
    }

    public int engineId() {
        return engineId;
    }

    public DriverMode driverMode() {
        return driverMode;
    }

    public String aeronDir() {
        return aeronDir;
    }

    public ClusterBackup.SourceType sourceType() {
        return sourceType;
    }

    public int nodePort() {
        return nodePort;
    }

    public long backupCooldownMs() {
        return backupCooldownMs;
    }

    public long backupQueryIntervalMs() {
        return backupQueryIntervalMs;
    }

    public long memberBackupWindowMs() {
        return memberBackupWindowMs;
    }

    public long readyTimeLagMs() {
        return readyTimeLagMs;
    }

    public String catchupSndbuf() {
        return catchupSndbuf;
    }

    public int driverMtu() {
        return driverMtu;
    }

    public int driverTermLength() {
        return driverTermLength;
    }

    /** {@code REPLICA_VERIFY_WAIT_MS}. */
    public long verifyWaitMs() {
        return verifyWaitMs;
    }

    /** {@code REPLICA_LOG_STREAM_ID}. */
    public int logStreamId() {
        return logStreamId;
    }

    /** {@code REPLICA_SNAPSHOT_STREAM_IDS}. */
    public Set<Integer> snapshotStreamIds() {
        return snapshotStreamIds;
    }

    /** The streams the backup records and replicates: the log stream plus the snapshot streams. */
    public Set<Integer> clusterStreams() {
        final java.util.HashSet<Integer> all = new java.util.HashSet<>(snapshotStreamIds);
        all.add(logStreamId);
        return Set.copyOf(all);
    }

    public Set<ReplicaNegativeControl> negativeControls() {
        return off;
    }

    /** The producer slot is also the port slot: {@code R<slot>}. */
    @Override
    public String toString() {
        return "ReplicaConfig{slot=" + slot + " members=" + memberHosts + " replicaAddress=" + replicaAddress
                + " portBase=" + portBase + " stride=" + portStride + " baseDir=" + baseDir + " driverMode="
                + driverMode + " aeronDir=" + aeronDir + " source=" + sourceType + " cooldownMs=" + backupCooldownMs
                + " queryIntervalMs=" + backupQueryIntervalMs + " responseTimeoutMs=" + backupResponseTimeoutMs
                + " progressTimeoutMs=" + backupProgressTimeoutMs + " bufferMb=" + shadowBufferMb
                + (off.isEmpty() ? "" : " NEGATIVE_CONTROLS=" + off) + "}";
    }

    /** Defaults are the setting defaults; every value the host does not set keeps them. */
    public static final class Builder {
        private List<String> memberHosts = List.of();
        private int portBase = 9300;
        private int portStride;
        private int slot = 3;
        private String replicaAddress;
        private File baseDir;
        private int clusterId;
        private int engineId = -1;
        private DriverMode driverMode = DriverMode.EMBEDDED;
        private String aeronDir;
        private long externalDriverTimeoutMs = DEFAULT_EXTERNAL_DRIVER_TIMEOUT_MS;
        private boolean busySpin;
        private ClusterBackup.SourceType sourceType = ClusterBackup.SourceType.FOLLOWER;
        private CredentialsSupplier credentials;
        private long memberBackupWindowMs = DEFAULT_MEMBER_BACKUP_WINDOW_MS;
        private long backupCooldownMs = DEFAULT_BACKUP_COOLDOWN_MS;
        private long backupResponseTimeoutMs = DEFAULT_BACKUP_RESPONSE_TIMEOUT_MS;
        private long backupQueryIntervalMs = DEFAULT_BACKUP_QUERY_INTERVAL_MS;
        private long backupProgressTimeoutMs = DEFAULT_BACKUP_PROGRESS_TIMEOUT_MS;
        private long imageLivenessMs = DEFAULT_IMAGE_LIVENESS_MS;
        private String catchupRcvbuf = DEFAULT_CATCHUP_RCVBUF;
        private String catchupRcvWnd = DEFAULT_CATCHUP_RCV_WND;
        private String catchupSndbuf = DEFAULT_CATCHUP_SNDBUF;
        private int driverMtu = DEFAULT_DRIVER_MTU;
        private int driverTermLength = DEFAULT_DRIVER_TERM_LENGTH;
        private int socketBufferLength = DEFAULT_SOCKET_BUFFER;
        private int archiveSegmentLength = DEFAULT_ARCHIVE_SEGMENT_LENGTH;
        private int shadowBufferMb = DEFAULT_SHADOW_BUFFER_MB;
        private long alignedWaitMs = DEFAULT_ALIGNED_WAIT_MS;
        private long standbyFallbackMs = DEFAULT_STANDBY_FALLBACK_MS;
        private int retrievedSetsKeep = DEFAULT_RETRIEVED_SETS_KEEP;
        private long readyMaxLagBytes = DEFAULT_READY_MAX_LAG_BYTES;
        private long readyShadowAgeMs = DEFAULT_READY_SHADOW_AGE_MS;
        private long readyStaleMs = DEFAULT_READY_STALE_MS;
        private long livenessStaleMs = DEFAULT_LIVENESS_STALE_MS;
        private long readyTimeLagMs = DEFAULT_READY_TIME_LAG_MS;
        private long attachBackoffMs = DEFAULT_ATTACH_BACKOFF_MS;
        private long bootstrapWaitMs = DEFAULT_BOOTSTRAP_WAIT_MS;
        private long verifyWaitMs = DEFAULT_VERIFY_WAIT_MS;
        private int logStreamId = DEFAULT_LOG_STREAM_ID;
        private Set<Integer> snapshotStreamIds = DEFAULT_SNAPSHOT_STREAM_IDS;
        private int replayStreamId = DEFAULT_REPLAY_STREAM_ID;
        private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
        private String shadowTermLength = DEFAULT_SHADOW_TERM_LENGTH;
        private long writeConnectTimeoutMs = DEFAULT_WRITE_CONNECT_TIMEOUT_MS;
        private long writeOfferTimeoutMs = DEFAULT_WRITE_OFFER_TIMEOUT_MS;
        private long writeRecordTimeoutMs = DEFAULT_WRITE_RECORD_TIMEOUT_MS;
        private int retainN = DEFAULT_RETAIN_N;
        private ShadowFloor.Margin floorMargin = ShadowFloor.Margin.PREVIOUS_VERIFIED;
        private long deepReadPaceMs = DEFAULT_DEEP_READ_PACE_MS;
        private long scanPeriodMs = DEFAULT_SCAN_PERIOD_MS;
        private long loadTimeoutMs = DEFAULT_LOAD_TIMEOUT_MS;
        private int nodePort = -1;
        private ReplicaFaults faults = ReplicaFaults.NONE;
        private Set<ReplicaNegativeControl> off = EnumSet.noneOf(ReplicaNegativeControl.class);

        private Builder() {
        }

        public Builder memberHosts(final List<String> v) {
            this.memberHosts = List.copyOf(v);
            return this;
        }

        public Builder portBase(final int v) {
            this.portBase = v;
            return this;
        }

        public Builder portStride(final int v) {
            this.portStride = v;
            return this;
        }

        public Builder slot(final int v) {
            this.slot = v;
            return this;
        }

        public Builder replicaAddress(final String v) {
            this.replicaAddress = v;
            return this;
        }

        public Builder baseDir(final File v) {
            this.baseDir = v;
            return this;
        }

        public Builder clusterId(final int v) {
            this.clusterId = v;
            return this;
        }

        public Builder engineId(final int v) {
            this.engineId = v;
            return this;
        }

        public Builder driverMode(final DriverMode v) {
            this.driverMode = v;
            return this;
        }

        public Builder aeronDir(final String v) {
            this.aeronDir = v;
            return this;
        }

        public Builder externalDriverTimeoutMs(final long v) {
            this.externalDriverTimeoutMs = v;
            return this;
        }

        public Builder busySpin(final boolean v) {
            this.busySpin = v;
            return this;
        }

        public Builder sourceType(final ClusterBackup.SourceType v) {
            this.sourceType = v;
            return this;
        }

        public Builder credentials(final CredentialsSupplier v) {
            this.credentials = v;
            return this;
        }

        public Builder memberBackupWindowMs(final long v) {
            this.memberBackupWindowMs = v;
            return this;
        }

        public Builder backupCooldownMs(final long v) {
            this.backupCooldownMs = v;
            return this;
        }

        public Builder backupResponseTimeoutMs(final long v) {
            this.backupResponseTimeoutMs = v;
            return this;
        }

        public Builder backupQueryIntervalMs(final long v) {
            this.backupQueryIntervalMs = v;
            return this;
        }

        public Builder backupProgressTimeoutMs(final long v) {
            this.backupProgressTimeoutMs = v;
            return this;
        }

        public Builder imageLivenessMs(final long v) {
            this.imageLivenessMs = v;
            return this;
        }

        public Builder catchupRcvbuf(final String v) {
            this.catchupRcvbuf = v;
            return this;
        }

        public Builder catchupRcvWnd(final String v) {
            this.catchupRcvWnd = v;
            return this;
        }

        /** {@code REPLICA_CATCHUP_SNDBUF}. */
        public Builder catchupSndbuf(final String v) {
            this.catchupSndbuf = v;
            return this;
        }

        /** {@code REPLICA_DRIVER_MTU} (embedded driver only). */
        public Builder driverMtu(final int v) {
            this.driverMtu = v;
            return this;
        }

        /** {@code REPLICA_DRIVER_TERM_LENGTH} (embedded driver IPC and publication terms). */
        public Builder driverTermLength(final int v) {
            this.driverTermLength = v;
            return this;
        }

        public Builder socketBufferLength(final int v) {
            this.socketBufferLength = v;
            return this;
        }

        public Builder archiveSegmentLength(final int v) {
            this.archiveSegmentLength = v;
            return this;
        }

        public Builder shadowBufferMb(final int v) {
            this.shadowBufferMb = v;
            return this;
        }

        public Builder alignedWaitMs(final long v) {
            this.alignedWaitMs = v;
            return this;
        }

        /** {@code SHADOW_STANDBY_FALLBACK_MINUTES} in ms of CLUSTER time; 0 disables LOCAL. */
        public Builder standbyFallbackMs(final long v) {
            this.standbyFallbackMs = v;
            return this;
        }

        public Builder retrievedSetsKeep(final int v) {
            this.retrievedSetsKeep = v;
            return this;
        }

        public Builder readyMaxLagBytes(final long v) {
            this.readyMaxLagBytes = v;
            return this;
        }

        /** 0 disables the shadow-age readiness input. */
        public Builder readyShadowAgeMs(final long v) {
            this.readyShadowAgeMs = v;
            return this;
        }

        public Builder readyStaleMs(final long v) {
            this.readyStaleMs = v;
            return this;
        }

        public Builder livenessStaleMs(final long v) {
            this.livenessStaleMs = v;
            return this;
        }

        /** The time-lag readiness input; 0 disables it. */
        public Builder readyTimeLagMs(final long v) {
            this.readyTimeLagMs = v;
            return this;
        }

        public Builder attachBackoffMs(final long v) {
            this.attachBackoffMs = v;
            return this;
        }

        public Builder bootstrapWaitMs(final long v) {
            this.bootstrapWaitMs = v;
            return this;
        }

        public Builder verifyWaitMs(final long v) {
            this.verifyWaitMs = v;
            return this;
        }

        public Builder logStreamId(final int v) {
            this.logStreamId = v;
            return this;
        }

        public Builder snapshotStreamIds(final Set<Integer> v) {
            this.snapshotStreamIds = Objects.requireNonNull(v, "REPLICA_SNAPSHOT_STREAM_IDS");
            return this;
        }

        public Builder replayStreamId(final int v) {
            this.replayStreamId = v;
            return this;
        }

        public Builder fragmentLimit(final int v) {
            this.fragmentLimit = v;
            return this;
        }

        public Builder shadowTermLength(final String v) {
            this.shadowTermLength = v;
            return this;
        }

        public Builder writeConnectTimeoutMs(final long v) {
            this.writeConnectTimeoutMs = v;
            return this;
        }

        public Builder writeOfferTimeoutMs(final long v) {
            this.writeOfferTimeoutMs = v;
            return this;
        }

        public Builder writeRecordTimeoutMs(final long v) {
            this.writeRecordTimeoutMs = v;
            return this;
        }

        public Builder retainN(final int v) {
            this.retainN = v;
            return this;
        }

        public Builder floorMargin(final ShadowFloor.Margin v) {
            this.floorMargin = v;
            return this;
        }

        public Builder deepReadPaceMs(final long v) {
            this.deepReadPaceMs = v;
            return this;
        }

        public Builder scanPeriodMs(final long v) {
            this.scanPeriodMs = v;
            return this;
        }

        public Builder loadTimeoutMs(final long v) {
            this.loadTimeoutMs = v;
            return this;
        }

        public Builder nodePort(final int v) {
            this.nodePort = v;
            return this;
        }

        /**
         * Test seams. Refused by {@link #build} unless {@value ReplicaFaults#PROPERTY} is
         * {@code true}: a production replica with a fault seam or a negative control is a configuration error.
         */
        public Builder testHooks(final ReplicaFaults faults, final Set<ReplicaNegativeControl> off) {
            this.faults = Objects.requireNonNull(faults, "faults");
            this.off = off.isEmpty() ? EnumSet.noneOf(ReplicaNegativeControl.class) : EnumSet.copyOf(off);
            return this;
        }

        public ReplicaConfig build() {
            if (memberHosts.isEmpty()) {
                throw refused("CLUSTER_ADDRESSES", "is required (one host per member)");
            }
            if (replicaAddress == null || replicaAddress.isBlank()) {
                throw refused("CLUSTER_REPLICA_ADDRESS", "is required");
            }
            Objects.requireNonNull(baseDir, "BASE_DIR");
            Objects.requireNonNull(aeronDir, "AERON_DIR");
            Objects.requireNonNull(driverMode, "TRANSPORT_DRIVER_MODE");
            Objects.requireNonNull(sourceType, "REPLICA_BACKUP_SOURCE");
            Objects.requireNonNull(floorMargin, "SHADOW_FLOOR_MARGIN");
            if (credentials == null) {
                throw refused("CORE_AUTH_CREDENTIAL", "is required (a REPLICA credential)");
            }
            if (engineId <= 0) {
                throw refused("engineId", "must be > 0: " + engineId);
            }
            if (slot < 0 || clusterId < 0 || portBase <= 0 || portStride < 0) {
                throw refused("CLUSTER_REPLICA_SLOT/CLUSTER_PORT_BASE/CLUSTER_PORT_STRIDE",
                        "out of range: slot=" + slot + " base=" + portBase + " stride=" + portStride);
            }
            if (sourceType == ClusterBackup.SourceType.FOLLOWER && memberHosts.size() < 2) {
                // LogSourceValidator(FOLLOWER) excludes the sole member and PublicationGroup spins
                throw refused("REPLICA_BACKUP_SOURCE", "=FOLLOWER needs >= 2 members; use ANY");
            }
            if (nodePort < 1 || nodePort > 65535) {
                throw refused("CORE_NODE_PORT", "out of range [1,65535]: " + nodePort);
            }
            if (shadowBufferMb < 16 || shadowBufferMb > MAX_SHADOW_BUFFER_MB) {
                throw refused("REPLICA_SHADOW_BUFFER_MB", "=" + shadowBufferMb + " out of range [16,"
                        + MAX_SHADOW_BUFFER_MB + "] (H14: one image buffer is int-sized)");
            }
            if (retrievedSetsKeep < 1) {
                throw refused("REPLICA_RETRIEVED_SETS_KEEP", "must be >= 1: " + retrievedSetsKeep);
            }
            if (attachBackoffMs < 1 || attachBackoffMs > MAX_ATTACH_BACKOFF_MS) {
                throw refused("REPLICA_ATTACH_BACKOFF_MS", "out of range [1," + MAX_ATTACH_BACKOFF_MS + "]: "
                        + attachBackoffMs);
            }
            if (livenessStaleMs < readyStaleMs) {
                throw refused("REPLICA_LIVENESS_STALE_MS", "must be >= REPLICA_READY_STALE_MS");
            }
            positive("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS", externalDriverTimeoutMs);
            positive("REPLICA_MEMBER_BACKUP_WINDOW_MS", memberBackupWindowMs);
            positive("REPLICA_BACKUP_COOLDOWN_MS", backupCooldownMs);
            positive("REPLICA_BACKUP_RESPONSE_TIMEOUT_MS", backupResponseTimeoutMs);
            positive("REPLICA_BACKUP_QUERY_INTERVAL_MS", backupQueryIntervalMs);
            positive("REPLICA_BACKUP_PROGRESS_TIMEOUT_MS", backupProgressTimeoutMs);
            positive("REPLICA_IMAGE_LIVENESS_MS", imageLivenessMs);
            positive("REPLICA_ALIGNED_WAIT_MS", alignedWaitMs);
            positive("REPLICA_READY_MAX_LAG_BYTES", readyMaxLagBytes);
            positive("REPLICA_READY_STALE_MS", readyStaleMs);
            positive("REPLICA_BOOTSTRAP_WAIT_MS", bootstrapWaitMs);
            positive("REPLICA_VERIFY_WAIT_MS", verifyWaitMs);
            final String cycle = verifyWaitShorterThanOneBackupCycle(verifyWaitMs, backupQueryIntervalMs,
                    backupCooldownMs);
            if (cycle != null) {
                throw new IllegalArgumentException(cycle);
            }
            positive("REPLICA_LOG_STREAM_ID", logStreamId);
            if (snapshotStreamIds.isEmpty()) {
                throw refused("REPLICA_SNAPSHOT_STREAM_IDS", "must name at least one stream");
            }
            for (final int id : snapshotStreamIds) {
                positive("REPLICA_SNAPSHOT_STREAM_IDS", id);
            }
            if (snapshotStreamIds.contains(logStreamId)) {
                throw refused("REPLICA_SNAPSHOT_STREAM_IDS", "names the log stream " + logStreamId);
            }
            // never one of the replica's own streams (orphan detection would purge from them)
            refuseOwnStream("REPLICA_LOG_STREAM_ID", logStreamId, replayStreamId);
            for (final int id : new java.util.TreeSet<>(snapshotStreamIds)) {
                refuseOwnStream("REPLICA_SNAPSHOT_STREAM_IDS", id, replayStreamId);
            }
            positive("SHADOW_WRITE_CONNECT_TIMEOUT_MS", writeConnectTimeoutMs);
            positive("SHADOW_WRITE_OFFER_TIMEOUT_MS", writeOfferTimeoutMs);
            positive("SHADOW_WRITE_RECORD_TIMEOUT_MS", writeRecordTimeoutMs);
            positive("SHADOW_SCAN_PERIOD_MS", scanPeriodMs);
            positive("SHADOW_LOAD_TIMEOUT_MS", loadTimeoutMs);
            positive("fragmentLimit", fragmentLimit);
            positive("socketBufferLength", socketBufferLength);
            positive("REPLICA_DRIVER_MTU", driverMtu);
            positive("REPLICA_DRIVER_TERM_LENGTH", driverTermLength);
            Objects.requireNonNull(catchupSndbuf, "REPLICA_CATCHUP_SNDBUF");
            positive("archiveSegmentLength", archiveSegmentLength);
            nonNegative("SHADOW_STANDBY_FALLBACK_MINUTES", standbyFallbackMs);
            nonNegative("REPLICA_READY_SHADOW_AGE_MS", readyShadowAgeMs);
            nonNegative("REPLICA_READY_TIME_LAG_MS", readyTimeLagMs);
            nonNegative("SHADOW_DEEP_READ_PACE_MS", deepReadPaceMs);
            if ((faults != ReplicaFaults.NONE || !off.isEmpty()) && !ReplicaFaults.allowed()) {
                throw new IllegalArgumentException("replica test hooks need -D" + ReplicaFaults.PROPERTY + "=true: a"
                        + " production replica with a fault seam or a negative control is a configuration error");
            }
            return new ReplicaConfig(this);
        }

        private static IllegalArgumentException refused(final String setting, final String why) {
            return new IllegalArgumentException(setting + (why.startsWith("=") ? "" : " ") + why);
        }

        /**
         * {@code id} names one of the replica's own streams: the shadow alias stream, the shadow replay stream or
         * the follower's replay stream (its setting's value).
         */
        private static void refuseOwnStream(final String setting, final int id, final int followerReplay) {
            final String own;
            if (id == com.openexchange.cluster.shadow.ShadowAlias.STREAM_ID) {
                own = "shadow alias";
            } else if (id == com.openexchange.cluster.shadow.ShadowAlias.REPLAY_STREAM_ID) {
                own = "shadow replay";
            } else if (id == followerReplay) {
                own = "follower replay";
            } else {
                return;
            }
            throw refused(setting, "names " + id + ", the replica's own " + own + " stream");
        }

        private static void positive(final String setting, final long v) {
            if (v <= 0) {
                throw refused(setting, "must be > 0: " + v);
            }
        }

        private static void nonNegative(final String setting, final long v) {
            if (v < 0) {
                throw refused(setting, "must be >= 0: " + v);
            }
        }
    }
}
