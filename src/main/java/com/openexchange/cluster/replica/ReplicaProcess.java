// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.cluster.NodeEndpoint;
import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowVerdicts;
import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupEventsListener;
import io.aeron.cluster.ClusterBackupMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.status.CountersReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The replica process: launches the backup in embedded or external driver mode, then the
 * follower, the housekeeping thread and the node endpoint, and closes them in order. The one class of
 * {@code cluster.replica} that launches a driver, an archive or the backup (scanned).
 *
 * <pre>
 * embedded (dev, demo, tests)  ClusterBackupMediaDriver.launch(driver, archive, backup)   (CBMD:84-115)
 * external (production)        host.awaitExternalDriver, Archive.launch, ClusterBackup.launch (AC:157-188)
 * close order                  endpoint, follower (reader, then writer: an in-flight write aborts), housekeeping,
 *                              clients, backup, archive, driver
 * </pre>
 *
 * <p>Errors: the one AC:109-124 shape (count, log, {@code host.onError(t, external)}: driver loss halts at once with
 * exit 1). The backup's termination hook (an {@code AgentTerminationException} in the agent, e.g. STORAGE_SPACE,
 * CBA:1056-1064) is fatal unless the process is closing. A fenced follower and a refused bootstrap call
 * {@code host.fatal}. Exit codes are the lifecycle's: 1 fatal, 143 on a signal, never 0 on a fault.</p>
 *
 * <p>An oracle mismatch HOLDS instead ({@code host.hold}): the endpoint, the metrics, the housekeeping thread and the
 * Aeron error handler stay up (driver loss still halts at once), the process exits on the operator's stop (143), on
 * driver loss (1) or when {@code REPLICA_MISMATCH_HOLD_MS} elapses (1). The mismatch dump dir is created and checked
 * writable before anything is launched ({@code REPLICA_MISMATCH_DIR is not writable: <path>}).</p>
 */
public final class ReplicaProcess implements AutoCloseable {

    /** What the process needs from its host; {@link #of(EngineLifecycle)} in production, a recorder in tests. */
    public interface Host {
        /** A fatal fault: exit 1 after the close (the lifecycle's backstop). */
        void fatal(String reason);

        /** An Aeron error the handler saw (AC:109-124); driver loss in EXTERNAL mode halts here. */
        void onError(Throwable t, boolean externalDriver);

        /** External mode: wait for the aeronmd's {@code cnc.dat} or throw. */
        void awaitExternalDriver(String aeronDir, long timeoutMs);

        /**
         * The replica entered the oracle mismatch hold: keep running until stopped. The default only logs; the
         * lifecycle's stop and error paths are unchanged by a hold.
         */
        default void hold(final String reason) {
            System.err.println("[REPLICA] HOLD: " + reason + " (the process stays up until it is stopped)");
        }
    }

    public static Host of(final EngineLifecycle lifecycle) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        return new Host() {
            @Override
            public void fatal(final String reason) {
                lifecycle.fatal(EngineLifecycle.EXIT_FATAL, reason);
            }

            @Override
            public void onError(final Throwable t, final boolean externalDriver) {
                lifecycle.onError(t, externalDriver);
            }

            @Override
            public void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
                lifecycle.awaitExternalDriver(aeronDir, timeoutMs);
            }
        };
    }

    private final ReplicaConfig cfg;
    private final Host host;
    private final AtomicLong aeronErrors = new AtomicLong();
    private final NanoClock clock = SystemNanoClock.INSTANCE;
    private final EpochClock wall = SystemEpochClock.INSTANCE;
    private volatile boolean closing;

    private ClusterBackupMediaDriver embedded;
    private Archive archiveProcess;
    private ClusterBackup backup;
    private Aeron aeron;
    private AeronArchive followerArchive;
    private BackupEvents events;
    private ReplicaReadiness readiness;
    private ReplicaFollower follower;
    private ReplicaHousekeeping housekeeping;
    private ReplicaMetrics metrics;
    private NodeEndpoint endpoint;
    private int backupStateCounterId = CountersReader.NULL_COUNTER_ID;

    private ReplicaProcess(final ReplicaConfig cfg, final Host host) {
        this.cfg = cfg;
        this.host = host;
    }

    /**
     * Production entry: launch, wait for the lifecycle's stop, close. The caller installs the lifecycle and calls
     * {@code startupFailed} on a throw and {@code finish} after the return (the AC:58-67 discipline).
     */
    public static void run(final ReplicaConfig cfg, final ReplicaEngine engine, final EngineLifecycle lifecycle) {
        try (ReplicaProcess ignored = launch(cfg, engine, of(lifecycle), null)) {
            lifecycle.awaitStop();
        }
    }

    /**
     * Launch everything and return the running process (in-JVM tests use this directly).
     *
     * @param testListener an extra backup events listener (tests), or null
     */
    public static ReplicaProcess launch(final ReplicaConfig cfg, final ReplicaEngine engine, final Host host,
                                        final ClusterBackupEventsListener testListener) {
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(engine, "engine");
        Objects.requireNonNull(host, "host");
        final ReplicaProcess p = new ReplicaProcess(cfg, host);
        try {
            p.start(engine, testListener);
            return p;
        } catch (final RuntimeException | Error failed) {
            p.closing = true;
            p.close();
            throw failed;
        }
    }

    private void start(final ReplicaEngine engine, final ClusterBackupEventsListener testListener) {
        System.out.println("[REPLICA] starting " + cfg);
        MismatchDump.requireWritable(cfg.mismatchDir()); // before any Aeron component
        for (final String line : ReplicaContexts.windowWarnings(cfg)) {
            System.err.println("[REPLICA] " + line);
        }
        final boolean external = cfg.driverMode == ReplicaConfig.DriverMode.EXTERNAL;
        final ErrorHandler errorHandler = t -> {
            final long n = aeronErrors.incrementAndGet();
            System.err.println("[REPLICA] Aeron error #" + n + ": " + t);
            host.onError(t, external);
        };
        events = new BackupEvents(clock, testListener);
        final ClusterBackup.Context backupCtx = ReplicaContexts.backup(cfg, events, errorHandler, () -> {
            if (!closing) {
                host.fatal("FATAL replica: the cluster backup agent terminated (" + events.lastFailure() + ")");
            }
        });
        cfg.faults.backupContext(backupCtx); // a test seam (the named-copy rig); NONE in production
        if (external) {
            System.out.println("[REPLICA] Driver mode EXTERNAL: connecting to media driver at " + cfg.aeronDir);
            host.awaitExternalDriver(cfg.aeronDir, cfg.externalDriverTimeoutMs);
            archiveProcess = Archive.launch(ReplicaContexts.archive(cfg, errorHandler));
            backup = ClusterBackup.launch(backupCtx);
        } else {
            System.out.println("[REPLICA] Driver mode EMBEDDED: driver at " + cfg.aeronDir);
            embedded = ClusterBackupMediaDriver.launch(ReplicaContexts.driver(cfg).errorHandler(errorHandler),
                    ReplicaContexts.archive(cfg, errorHandler), backupCtx);
        }
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cfg.aeronDir).errorHandler(errorHandler));
        followerArchive = AeronArchive.connect(ReplicaContexts.localArchiveClient(cfg).aeron(aeron)
                .ownsAeronClient(false));

        final ShadowCounters shadowCounters = new ShadowCounters();
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final ReplicaFollower[] followerRef = new ReplicaFollower[1];
        readiness = new ReplicaReadiness(cfg.readyStaleMs, cfg.livenessStaleMs, cfg.readyMaxLagBytes,
                cfg.readyShadowAgeMs, cfg.readyTimeLagMs, clock, wall, this::backupState, () -> {
                    final ReplicaFollower f = followerRef[0];
                    final ShadowTriggerPolicy policy = f == null ? null : f.policy();
                    return policy == null ? -1 : policy.lastWriteCompletedNs();
                }, cfg.off);
        final ReplicaFollower.Halt halt = new ReplicaFollower.Halt() {
            @Override
            public void fatal(final String reason) {
                host.fatal(reason);
            }

            @Override
            public void hold(final String reason) {
                host.hold(reason);
            }
        };
        follower = new ReplicaFollower(cfg, engine, aeron, followerArchive, ReplicaContexts.localArchiveClient(cfg),
                events, readiness, shadowCounters, verdicts, clock, ReplicaContexts.idle(cfg), halt);
        followerRef[0] = follower;
        housekeeping = new ReplicaHousekeeping(cfg, aeron, ReplicaContexts.localArchiveClient(cfg), follower,
                readiness, shadowCounters, verdicts, clock, host::fatal);
        follower.suspectPurge(housekeeping);
        housekeeping.backupState(this::backupState);
        metrics = new ReplicaMetrics(follower, events, readiness, shadowCounters, this::backupState, clock, wall);
        metrics.housekeeping(housekeeping);
        endpoint = new NodeEndpoint(readiness::probe, metrics::body);
        try {
            endpoint.start(cfg.nodePort);
        } catch (final IOException e) {
            throw new UncheckedIOException("replica node endpoint on port " + cfg.nodePort, e);
        }
        follower.start();
        housekeeping.start();
        System.out.println("[REPLICA] running: producer R" + cfg.slot + ", /health /ready /metrics on "
                + endpoint.port());
    }

    /** {@code shadow_replica_backup_state}: the backup's state counter (CB:82-92), -1 when not found. */
    long backupState() {
        final Aeron a = aeron;
        if (a == null || a.isClosed()) {
            return -1;
        }
        final CountersReader counters = a.countersReader();
        int id = backupStateCounterId;
        if (id == CountersReader.NULL_COUNTER_ID || counters.getCounterState(id) != CountersReader.RECORD_ALLOCATED
                || counters.getCounterTypeId(id) != ClusterBackup.BACKUP_STATE_TYPE_ID) {
            final int[] found = {CountersReader.NULL_COUNTER_ID};
            counters.forEach((counterId, typeId, keyBuffer, label) -> {
                if (typeId == ClusterBackup.BACKUP_STATE_TYPE_ID && found[0] == CountersReader.NULL_COUNTER_ID) {
                    found[0] = counterId;
                }
            });
            id = found[0];
            backupStateCounterId = id;
        }
        return id == CountersReader.NULL_COUNTER_ID ? -1 : counters.getCounterValue(id);
    }

    // ------------------------------------------------------------------------------------------------ queries

    public ReplicaConfig config() {
        return cfg;
    }

    public BackupEvents events() {
        return events;
    }

    public ReplicaReadiness readiness() {
        return readiness;
    }

    public ReplicaFollower follower() {
        return follower;
    }

    public ReplicaHousekeeping housekeeping() {
        return housekeeping;
    }

    public ReplicaMetrics metrics() {
        return metrics;
    }

    public NodeEndpoint endpoint() {
        return endpoint;
    }

    public Aeron aeron() {
        return aeron;
    }

    public long aeronErrors() {
        return aeronErrors.get();
    }

    /** Endpoint, follower (reader then writer), housekeeping, clients, backup, archive, driver. */
    @Override
    public void close() {
        closing = true;
        if (readiness != null) {
            readiness.stopping();
        }
        if (endpoint != null) {
            endpoint.stop();
        }
        CloseHelper.quietClose(follower);
        CloseHelper.quietClose(housekeeping);
        CloseHelper.quietClose(followerArchive);
        CloseHelper.quietClose(aeron);
        if (embedded != null) {
            CloseHelper.quietClose(embedded);
        } else {
            CloseHelper.quietClose(backup);
            CloseHelper.quietClose(archiveProcess);
        }
        System.out.println("[REPLICA] closed");
    }
}
