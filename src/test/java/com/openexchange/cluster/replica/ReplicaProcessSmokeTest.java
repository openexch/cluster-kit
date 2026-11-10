// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowHousekeeping;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import com.openexchange.cluster.testkit.HashChainEngine;
import com.openexchange.cluster.testkit.ShadowRestoreRunner;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.VersionValidator;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.security.Authenticator;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.SessionProxy;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A kit-level smoke run of {@link ReplicaProcess} in EMBEDDED mode against a real one-member
 * cluster (UDP archive control, source ANY), with {@link HashChainEngine} as the engine. It drives the real
 * {@code ClusterBackup} recording, the follower's attach, the SNAPSHOT (flags 0) and STANDBY_SNAPSHOT (flags 1) toggles
 * into ALIGNED and STANDBY shadows, housekeeping's VERIFIED verdicts, and a restart that restores from the newest
 * local shadow. The engine-specific cases are the assets tests'.
 */
public class ReplicaProcessSmokeTest {

    private File dir;
    private ClusteredMediaDriver memberDriver;
    private ClusteredServiceContainer memberContainer;
    private AeronCluster client;
    private ReplicaProcess replica;
    private final List<String> fatals = new CopyOnWriteArrayList<>();
    private int base;

    @Before
    public void setUp() throws IOException {
        dir = Files.createTempDirectory("replica-smoke").toFile();
        base = freeBase();
    }

    @After
    public void tearDown() {
        CloseHelper.quietCloseAll(client, replica, memberContainer, memberDriver);
        IoUtil.delete(dir, true);
    }

    /** A port base whose member-0 and slot-3 ports (stride 100) are free right now. */
    static int freeBase() {
        for (int attempt = 0; attempt < 50; attempt++) {
            final int b = 20_000 + (int)(Math.random() * 20_000) / 10 * 10;
            if (udpFree(b + 1, 6) && udpFree(b + 301, 5)) {
                return b;
            }
        }
        throw new IllegalStateException("no free port base");
    }

    static boolean udpFree(final int from, final int n) {
        for (int p = from; p < from + n; p++) {
            try (DatagramSocket ignored = new DatagramSocket(p)) {
                // free
            } catch (final IOException taken) {
                return false;
            }
        }
        return true;
    }

    static int freeTcp() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    // ------------------------------------------------------------------------------------------------ member

    /**
     * The member: the same {@link HashChainEngine} the replica applies, fed from the service callbacks with
     * {@code cluster.logPosition()} (the frame end), its image written in {@code onTakeSnapshot}, so the replica's
     * alignment compare sees the member snapshot it would see on a real engine.
     */
    static final class MirrorService implements ClusteredService {
        final HashChainEngine engine = new HashChainEngine();
        private Cluster cluster;

        @Override
        public void onStart(final Cluster cluster, final Image snapshotImage) {
            this.cluster = cluster;
            if (snapshotImage != null) {
                throw new IllegalStateException("the smoke member never restarts from a snapshot");
            }
            engine.genesis();
        }

        @Override
        public void onSessionOpen(final ClientSession session, final long timestamp) {
            engine.onSessionOpen(session.id(), cluster.logPosition(), timestamp, session.encodedPrincipal());
        }

        @Override
        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
            engine.onSessionClose(session.id(), cluster.logPosition(), timestamp, closeReason);
        }

        @Override
        public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                     final int offset, final int length, final Header header) {
            engine.onSessionMessage(session.id(), cluster.logPosition(), timestamp, buffer, offset, length);
        }

        @Override
        public void onTimerEvent(final long correlationId, final long timestamp) {
            engine.onTimerEvent(correlationId, cluster.logPosition(), timestamp);
        }

        @Override
        public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
            final UnsafeBuffer image = new UnsafeBuffer(new byte[HashChainEngine.IMAGE_LENGTH]);
            final int n = engine.serialize(image);
            while (snapshotPublication.offer(image, 0, n) < 0) {
                Thread.onSpinWait();
            }
        }

        @Override
        public void onRoleChange(final Cluster.Role newRole) {
        }

        @Override
        public void onTerminate(final Cluster cluster) {
        }
    }

    static final class AcceptAll implements Authenticator {
        @Override
        public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
        }

        @Override
        public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
        }

        @Override
        public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs) {
            sessionProxy.authenticate(new byte[0]);
        }

        @Override
        public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs) {
        }
    }

    private String ep(final int offset) {
        return "localhost:" + (base + offset);
    }

    private void startMember() {
        final String aeronDir = new File(dir, "member-driver").getAbsolutePath();
        final AeronArchive.Context archiveClient = new AeronArchive.Context()
                .controlRequestChannel("aeron:ipc")
                .controlResponseChannel("aeron:ipc")
                .aeronDirectoryName(aeronDir);
        memberDriver = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                        .aeronDirectoryName(aeronDir)
                        .threadingMode(ThreadingMode.SHARED)
                        .sharedIdleStrategy(new BackoffIdleStrategy())
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true),
                new Archive.Context()
                        .aeronDirectoryName(aeronDir)
                        .archiveDir(new File(dir, "member-archive"))
                        .controlChannel("aeron:udp?endpoint=" + ep(1))
                        .localControlChannel("aeron:ipc")
                        .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .recordingEventsEnabled(false)
                        .threadingMode(ArchiveThreadingMode.SHARED),
                new ConsensusModule.Context()
                        .aeronDirectoryName(aeronDir)
                        .clusterMemberId(0)
                        .clusterMembers("0," + ep(2) + "," + ep(3) + "," + ep(4) + "," + ep(5) + "," + ep(1))
                        .clusterDir(new File(dir, "member-cluster"))
                        .archiveContext(archiveClient.clone())
                        .logChannel("aeron:ipc?term-length=256k")
                        .ingressChannel("aeron:udp")
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .serviceCount(1)
                        .idleStrategySupplier(BackoffIdleStrategy::new)
                        .authenticatorSupplier(AcceptAll::new)
                        // An ALIGNED shadow now waits for the backup to retrieve the member snapshot
                        // (a query interval), longer than a 10 s session timeout of the unpolled test client
                        .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(300))
                        .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                        .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                        .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                        .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                        .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                        .errorHandler(Throwable::printStackTrace));
        memberContainer = ClusteredServiceContainer.launch(new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .archiveContext(archiveClient.clone())
                .clusterDir(new File(dir, "member-cluster"))
                .clusteredService(new MirrorService())
                .serviceId(0)
                .idleStrategySupplier(BackoffIdleStrategy::new)
                .errorHandler(Throwable::printStackTrace));
        client = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:udp")
                .ingressEndpoints("0=" + ep(2))
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .egressListener(new EgressListener() {
                    @Override
                    public void onMessage(final long clusterSessionId, final long timestamp,
                                          final DirectBuffer buffer, final int offset, final int length,
                                          final Header header) {
                    }
                })
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
    }

    private void send(final int n) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        for (int i = 0; i < n; i++) {
            final int length = buffer.putStringWithoutLengthAscii(0, "msg-" + i);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (client.offer(buffer, 0, length) < 0) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("ingress offer timed out");
                }
                client.pollEgress();
                Thread.yield();
            }
        }
    }

    private void toggle(final ClusterControl.ToggleState state) {
        final AtomicCounter t = ClusterControl.findControlToggle(client.context().aeron().countersReader(), 0);
        await("control toggle " + state, () -> state.toggle(t), 10_000);
    }

    // ------------------------------------------------------------------------------------------------ replica

    static final class Binding implements ReplicaEngine {
        final HashChainEngine engine = new HashChainEngine();

        @Override
        public int engineId() {
            return HashChainEngine.ENGINE_ID;
        }

        @Override
        public ShadowEngine engine() {
            return engine;
        }

        @Override
        public ShadowIdentitySource identity() {
            return new ShadowIdentitySource() {
                @Override
                public long genesisId() {
                    return 0x5eedL;
                }

                @Override
                public boolean journalEnabled() {
                    return false;
                }

                @Override
                public long journalSeq() {
                    return 0;
                }

                @Override
                public int payloadLayoutHint() {
                    return 0;
                }
            };
        }

        @Override
        public ShadowSelfCheck selfCheck() {
            return (payload, length) -> {
                final HashChainEngine scratch = new HashChainEngine();
                scratch.restore(payload, length);
                final UnsafeBuffer again = new UnsafeBuffer(new byte[HashChainEngine.IMAGE_LENGTH]);
                final int n = scratch.serialize(again);
                if (n != length) {
                    return false;
                }
                for (int i = 0; i < n; i++) {
                    if (again.getByte(i) != payload.getByte(i)) {
                        return false;
                    }
                }
                return true;
            };
        }

        @Override
        public int appVersion() {
            return ShadowRestoreRunner.DEFAULT_APP_VERSION;
        }

        @Override
        public VersionValidator validator() {
            return ShadowRestoreRunner.DEFAULT_VALIDATOR;
        }

        /** The hash-chain engine carries no genesis in its image: every image is of the one test genesis. */
        @Override
        public long imageGenesisId(final DirectBuffer image, final int length) {
            return 0x5eedL;
        }

        @Override
        public String postRestoreWitness(final ShadowHeader header) {
            return null;
        }
    }

    private ReplicaProcess startReplica(final Binding binding) throws IOException {
        final ReplicaConfig cfg = ReplicaConfig.builder()
                .memberHosts(List.of("localhost"))
                .sourceType(ClusterBackup.SourceType.ANY)
                .replicaAddress("localhost")
                .portBase(base)
                .portStride(100)
                .slot(3)
                .baseDir(new File(dir, "replica"))
                .aeronDir(new File(dir, "replica-driver").getAbsolutePath())
                .engineId(HashChainEngine.ENGINE_ID)
                .nodePort(freeTcp())
                .credentials(new CredentialsSupplier() {
                    @Override
                    public byte[] encodedCredentials() {
                        return "replica".getBytes(StandardCharsets.US_ASCII);
                    }

                    @Override
                    public byte[] onChallenge(final byte[] encodedChallenge) {
                        return new byte[0];
                    }
                })
                .backupCooldownMs(2_000)
                .backupQueryIntervalMs(12_000)
                .shadowBufferMb(16)
                .shadowTermLength("64k")
                .scanPeriodMs(200)
                .deepReadPaceMs(0)
                .readyShadowAgeMs(0)
                .standbyFallbackMs(0)
                .archiveSegmentLength(1024 * 1024)
                .build();
        return ReplicaProcess.launch(cfg, binding, new ReplicaProcess.Host() {
            @Override
            public void fatal(final String reason) {
                fatals.add(reason);
                System.err.println("SMOKE host.fatal: " + reason);
            }

            @Override
            public void onError(final Throwable t, final boolean externalDriver) {
            }

            @Override
            public void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
                throw new IllegalStateException("embedded test");
            }
        }, null);
    }

    static void await(final String what, final BooleanSupplier condition, final long timeoutMs) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out after " + timeoutMs + " ms waiting for " + what);
            }
            try {
                Thread.sleep(20);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private static List<Long> verified(final ReplicaProcess r) {
        final ShadowHousekeeping hk = r.housekeeping().housekeeping();
        final List<Long> out = new ArrayList<>();
        if (hk != null && hk.scan() != null) {
            for (final LogicalShadow v : hk.scan().verified()) {
                out.add(v.logPosition());
            }
        }
        return out;
    }

    @Test(timeout = 180_000)
    public void followsWritesAlignedAndStandbyAndRestartsFromTheNewestShadow() throws IOException {
        startMember();
        send(20);
        final Binding first = new Binding();
        replica = startReplica(first);
        await("bootstrap", () -> replica.follower().bootstrapDone() || !fatals.isEmpty(), 60_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        assertEquals(ReplicaReadiness.RestorePath.GENESIS, replica.readiness().restorePath());
        await("applied past the first messages", () -> replica.follower().appliedPosition() > 0
                && first.engine.count() >= 20, 60_000);

        toggle(ClusterControl.ToggleState.SNAPSHOT);
        await("ALIGNED shadow", () -> replica.follower().policy().writtenTotal(ShadowKind.ALIGNED) >= 1
                || !fatals.isEmpty(), 60_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        final long aligned = replica.follower().policy().lastPosition(ShadowKind.ALIGNED);
        send(5);
        toggle(ClusterControl.ToggleState.STANDBY_SNAPSHOT);
        await("STANDBY shadow", () -> replica.follower().policy().writtenTotal(ShadowKind.STANDBY) >= 1
                || !fatals.isEmpty(), 60_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        final long standby = replica.follower().policy().lastPosition(ShadowKind.STANDBY);
        assertTrue("STANDBY " + standby + " after ALIGNED " + aligned, standby > aligned);
        await("both shadows VERIFIED", () -> verified(replica).contains(aligned)
                && verified(replica).contains(standby), 60_000);
        await("ready", () -> replica.readiness().probe().ready(), 30_000);
        assertTrue(replica.metrics().body(), replica.metrics().body().contains("shadow_replica_live_recording_alive 1"));
        assertEquals("no attach storm: " + replica.follower().attachAttemptsTotal(), 1,
                replica.follower().attachAttemptsTotal());
        // A local tail after the newest shadow in EVERY run. send() goes through ingress and toggle()
        // through the out-of-band control counter, so under load the STANDBY toggle can be sequenced before the five
        // messages above (one observed run: STANDBY at 2368 = ALIGNED 2272 + 96, L stop 2848 = 2368 + 5 x 96);
        // only those runs had a tail to replay at the restart. The restart path now always replays a tail.
        final long countAtStandby = first.engine.count();
        send(4);
        await("the tail applied before the restart", () -> first.engine.count() >= countAtStandby + 4, 60_000);
        final long countBefore = first.engine.count();
        final long chainBefore = first.engine.chain();
        final long appliedBefore = replica.follower().appliedPosition();
        assertTrue("a tail after the newest shadow (applied " + appliedBefore + ", STANDBY " + standby + ")",
                appliedBefore > standby);
        replica.close();
        replica = null;

        // restart on the same dirs: restore from the newest local shadow, the tail replays, then live
        final Binding second = new Binding();
        replica = startReplica(second);
        await("bootstrap after restart", () -> replica.follower().bootstrapDone() || !fatals.isEmpty(), 60_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, replica.readiness().restorePath());
        assertEquals("restored from the newest shadow", standby, replica.follower().restoredPosition());
        final long restartedAt = System.nanoTime();
        try {
            await("the tail replayed", () -> replica.follower().appliedPosition() >= appliedBefore, 60_000);
        } catch (final AssertionError timeout) {
            throw new AssertionError(timeout.getMessage() + " (applied " + replica.follower().appliedPosition()
                    + " < " + appliedBefore + "): " + replica.readiness().probe().detail() + " attachAttempts="
                    + replica.follower().attachAttemptsTotal() + " clusterChecked="
                    + replica.follower().clusterChecked() + " queries=" + replica.events().queries() + " responses="
                    + replica.events().responses() + " resets=" + replica.events().resets() + " liveRecording="
                    + replica.events().liveRecordingId() + " lastFailure=" + replica.events().lastFailure(), timeout);
        }
        System.out.println("[SMOKE] the local tail replayed " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
                - restartedAt) + " ms after the restart's bootstrap (backup responses so far "
                + replica.events().responses() + ", live progress " + replica.events().liveRecordingId()
                + ", clusterChecked " + replica.follower().clusterChecked() + ", tail " + (appliedBefore - standby)
                + " bytes)");
        assertEquals("the restored engine plus the tail equals the first run's engine", countBefore,
                second.engine.count());
        assertEquals(chainBefore, second.engine.chain());
        send(3);
        try {
            await("live after restart", () -> second.engine.count() >= countBefore + 3, 60_000);
        } catch (final AssertionError timeout) {
            throw new AssertionError(timeout.getMessage() + ": " + replica.readiness().probe().detail() + " queries="
                    + replica.events().queries() + " responses=" + replica.events().responses() + " resets="
                    + replica.events().resets() + " lastFailure=" + replica.events().lastFailure(), timeout);
        }
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        System.out.println("GREEN replica smoke: ALIGNED at " + aligned + ", STANDBY at " + standby
                + ", restart LOCAL_SHADOW at " + replica.follower().restoredPosition() + ", count "
                + second.engine.count());
    }
}
