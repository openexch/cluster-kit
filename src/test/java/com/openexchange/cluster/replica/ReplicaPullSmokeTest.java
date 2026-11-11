// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowPullSettings;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.security.CredentialsSupplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.openexchange.cluster.replica.ReplicaProcessSmokeTest.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A kit-level run of the replica's pull paths against a real one-member cluster whose log is purged (64 KiB segments):
 * a fresh replica whose log starts above 0 pulls an ALIGNED from another replica's archive and restores from it
 * (PULLED_SHADOW); a replica the member pruned past refuses the live replay below the supplier's start, pulls a
 * newer ALIGNED, asks for the restart (exit 3), and its close moves {@code cluster/} aside after the backup closed; the
 * restart starts a new log recording from the supplier's start and restores from the pulled shadow.
 */
public class ReplicaPullSmokeTest {

    private File dir;
    private ClusteredMediaDriver memberDriver;
    private ClusteredServiceContainer memberContainer;
    private AeronCluster client;
    private AeronArchive memberArchive;
    private ReplicaProcess a;
    private ReplicaProcess b;
    private final List<String> fatals = new CopyOnWriteArrayList<>();
    private final List<String> restarts = new CopyOnWriteArrayList<>();
    private int base;

    @Before
    public void setUp() throws IOException {
        dir = Files.createTempDirectory("replica-pull-smoke").toFile();
        base = freeBase();
    }

    @After
    public void tearDown() {
        CloseHelper.quietCloseAll(client, a, b, memberArchive, memberContainer, memberDriver);
        IoUtil.delete(dir, true);
    }

    static int freeBase() {
        for (int attempt = 0; attempt < 50; attempt++) {
            final int bb = 20_000 + (int)(Math.random() * 20_000) / 10 * 10;
            if (ReplicaProcessSmokeTest.udpFree(bb + 1, 6) && ReplicaProcessSmokeTest.udpFree(bb + 301, 5)
                    && ReplicaProcessSmokeTest.udpFree(bb + 401, 5)) {
                return bb;
            }
        }
        throw new IllegalStateException("no free port base");
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
                        .segmentFileLength(64 * 1024)
                        .recordingEventsEnabled(false)
                        .threadingMode(ArchiveThreadingMode.SHARED),
                new ConsensusModule.Context()
                        .aeronDirectoryName(aeronDir)
                        .clusterMemberId(0)
                        .clusterMembers("0," + ep(2) + "," + ep(3) + "," + ep(4) + "," + ep(5) + "," + ep(1))
                        .clusterDir(new File(dir, "member-cluster"))
                        .archiveContext(archiveClient.clone())
                        .logChannel("aeron:ipc?term-length=64k")
                        .ingressChannel("aeron:udp")
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .serviceCount(1)
                        .idleStrategySupplier(BackoffIdleStrategy::new)
                        .authenticatorSupplier(ReplicaProcessSmokeTest.AcceptAll::new)
                        .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(600))
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
                .clusteredService(new ReplicaProcessSmokeTest.MirrorService())
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
        memberArchive = AeronArchive.connect(archiveClient.clone().aeron(client.context().aeron())
                .ownsAeronClient(false));
    }

    private void send(final int n) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        for (int i = 0; i < n; i++) {
            final int length = buffer.putStringWithoutLengthAscii(0, "message-with-some-bytes-" + i);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (client.offer(buffer, 0, length) < 0) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("ingress offer timed out");
                }
                client.pollEgress();
                Thread.yield();
            }
            if (i % 64 == 0) {
                client.pollEgress();
            }
        }
    }

    private void snapshot() {
        final AtomicCounter t = ClusterControl.findControlToggle(client.context().aeron().countersReader(), 0);
        await("snapshot toggle", () -> ClusterControl.ToggleState.SNAPSHOT.toggle(t), 10_000);
    }

    /** The member's log recording (stream 100). */
    private long memberLog() {
        final long[] rec = {-1};
        memberArchive.listRecordingsForUri(0, 100, "", ConsensusModule.Configuration.LOG_STREAM_ID_DEFAULT,
                (cs, c, id, st, sp, start, stop, it, sfl, tbl, mtu, sid, str, sc, oc, si) -> rec[0] = id);
        return rec[0];
    }

    /**
     * Purges the member's log segments below {@code position} (segment aligned); returns the new start. The member's
     * own pruner may already have purged past it after a snapshot: then the start is kept (the archive refuses a
     * detach below its lower bound).
     */
    private long purgeMemberLogBelow(final long position) {
        final long rec = memberLog();
        final long newStart = position / (64 * 1024) * (64 * 1024);
        if (memberLogStart(rec) < newStart) {
            memberArchive.purgeSegments(rec, newStart);
        }
        final long start = memberLogStart(rec);
        System.out.println("[PULL-SMOKE] member log rec=" + rec + " purged below " + newStart + ": start " + start);
        return start;
    }

    private long memberLogStart(final long rec) {
        final long[] start = {-1};
        memberArchive.listRecording(rec, (cs, c, id, st, sp, s, stop, it, sfl, tbl, mtu, sid, str, sc, oc, si) ->
                start[0] = s);
        return start[0];
    }

    private ReplicaProcess startReplica(final int slot, final ReplicaProcessSmokeTest.Binding binding,
                                        final ShadowPullSettings pull) throws IOException {
        return startReplica(slot, binding, pull, java.util.EnumSet.noneOf(ReplicaNegativeControl.class));
    }

    private ReplicaProcess startReplica(final int slot, final ReplicaProcessSmokeTest.Binding binding,
                                        final ShadowPullSettings pull, final java.util.Set<ReplicaNegativeControl> off)
            throws IOException {
        final ReplicaConfig.Builder builder = ReplicaConfig.builder()
                .memberHosts(List.of("localhost"))
                .sourceType(ClusterBackup.SourceType.ANY)
                .replicaAddress("localhost")
                .portBase(base)
                .portStride(100)
                .slot(slot)
                .baseDir(new File(dir, "replica" + slot))
                .aeronDir(new File(dir, "replica" + slot + "-driver").getAbsolutePath())
                .engineId(com.openexchange.cluster.testkit.HashChainEngine.ENGINE_ID)
                .nodePort(ReplicaProcessSmokeTest.freeTcp())
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
                .pullSettings(pull)
                .rebootstrapWindowMs(600_000);
        if (!off.isEmpty()) {
            builder.testHooks(ReplicaFaults.NONE, off);
        }
        final ReplicaConfig cfg = builder.build();
        // two replica processes in one JVM: distinct archive ids (one purge owner per archive per process)
        System.setProperty("aeron.archive.id", String.valueOf(1_000 + slot));
        try {
            return launch(cfg, binding);
        } finally {
            System.clearProperty("aeron.archive.id");
        }
    }

    private ReplicaProcess launch(final ReplicaConfig cfg, final ReplicaProcessSmokeTest.Binding binding) {
        return ReplicaProcess.launch(cfg, binding, new ReplicaProcess.Host() {
            @Override
            public void fatal(final String reason) {
                fatals.add(reason);
                System.err.println("PULL-SMOKE host.fatal: " + reason);
            }

            @Override
            public void onError(final Throwable t, final boolean externalDriver) {
            }

            @Override
            public void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
                throw new IllegalStateException("embedded test");
            }

            @Override
            public void restart(final int code, final String reason) {
                restarts.add(code + " " + reason);
                System.err.println("PULL-SMOKE host.restart(" + code + "): " + reason);
            }
        }, null);
    }

    private ShadowPullSettings from(final int slot) {
        return new ShadowPullSettings(List.of(new ShadowPullSettings.Endpoint("localhost", base + slot * 100 + 1)),
                500, 60_000, 120_000, 500, 1, java.util.Set.of(ShadowKind.ALIGNED), null, null);
    }

    /** The member's archive lists no shadow: a pull from it sees 0 candidates and runs until its deadline. */
    private ShadowPullSettings fromMember(final long startupWaitMs) {
        return new ShadowPullSettings(List.of(new ShadowPullSettings.Endpoint("localhost", base + 1)),
                500, startupWaitMs, 120_000, 500, 1, java.util.Set.of(ShadowKind.ALIGNED), null, null);
    }

    private static long aligned(final ReplicaProcess r) {
        return r.follower().policy().lastPosition(ShadowKind.ALIGNED);
    }

    @Test(timeout = 400_000)
    public void aPrunedLogIsPulledIntoAndAPrunedPastReplicaRebootstraps() throws IOException {
        startMember();
        send(1_500);
        final ReplicaProcessSmokeTest.Binding bindingA = new ReplicaProcessSmokeTest.Binding();
        a = startReplica(3, bindingA, ShadowPullSettings.disabled());
        await("A bootstrap", () -> a.follower().bootstrapDone() || !fatals.isEmpty(), 60_000);
        assertEquals(ReplicaReadiness.RestorePath.GENESIS, a.readiness().restorePath());
        await("A applied", () -> bindingA.hash.count() >= 1_500, 60_000);
        snapshot();
        await("A ALIGNED", () -> a.follower().policy().writtenTotal(ShadowKind.ALIGNED) >= 1 || !fatals.isEmpty(),
                90_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        final long p1 = aligned(a);
        final long start1 = purgeMemberLogBelow(p1);
        assertTrue("the member log starts above 0: " + start1, start1 > 0 && start1 <= p1);

        // the bootstrap pull: a fresh replica B, its log starts at the member's start > 0, no local shadow: it pulls A's ALIGNED
        final ReplicaProcessSmokeTest.Binding bindingB = new ReplicaProcessSmokeTest.Binding();
        b = startReplica(4, bindingB, from(3));
        await("B bootstrap", () -> b.follower().bootstrapDone() || !fatals.isEmpty(), 120_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        assertEquals(ReplicaReadiness.RestorePath.PULLED_SHADOW, b.readiness().restorePath());
        assertEquals(p1, b.follower().restoredPosition());
        assertTrue(b.metrics().body(), b.metrics().body().contains("shadow_replica_restore_path 3"));
        assertTrue(b.metrics().body(), b.metrics().body().contains("shadow_pull_total 1"));
        send(10);
        await("A and B equal after the tail", () -> bindingA.hash.count() == bindingB.hash.count()
                && bindingA.hash.chain() == bindingB.hash.chain() && bindingA.hash.count() >= 1_510, 60_000);
        System.out.println("[PULL-SMOKE] bootstrap pull: B restored PULLED_SHADOW at " + p1 + " (log start " + start1
                + "), count " + bindingB.hash.count());

        // re-bootstrap: A stops, the member moves on and prunes past A's L.stop, B writes a newer ALIGNED
        final long aStop = a.follower().appliedPosition();
        a.close();
        a = null;
        // a newer ALIGNED on B whose segment lies past A's L.stop (one more round when it shares A's last segment)
        long newer = p1;
        for (int round = 0; round < 4 && newer / (64 * 1024) * (64 * 1024) <= aStop; round++) {
            final long before = newer;
            send(1_500);
            snapshot();
            await("B's newer ALIGNED", () -> aligned(b) > before || !fatals.isEmpty(), 90_000);
            newer = aligned(b);
        }
        final long p2 = newer;
        final long start2 = purgeMemberLogBelow(p2);
        assertTrue("pruned past A's L.stop " + aStop + ": start " + start2, start2 > aStop);
        final ReplicaProcessSmokeTest.Binding bindingA2 = new ReplicaProcessSmokeTest.Binding();
        a = startReplica(3, bindingA2, from(4));
        await("A asks for the restart", () -> !restarts.isEmpty() || !fatals.isEmpty(), 180_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        final String lastFailure = a.events().lastFailure();
        System.out.println("[PULL-SMOKE] the supplier's refusal text: " + lastFailure);
        assertTrue(lastFailure, lastFailure.contains("Live log replay failed")
                && lastFailure.contains("errorCode=INVALID_POSITION"));
        assertTrue(a.events().replayBelowStart() >= 3);
        assertEquals(1, a.follower().rebootstrapTotal());
        assertTrue(restarts.get(0), restarts.get(0).startsWith(EngineLifecycle.EXIT_RESTART
                + " replica re-bootstrap: members pruned past L.stop="));
        assertTrue(a.readiness().probe().detail(), a.readiness().probe().detail()
                .contains("recovery=rebootstrap: restarting"));
        final File cluster = a.config().clusterDir();
        a.close();
        final File moved = a.movedClusterDir();
        assertTrue("moved: " + moved, moved != null && moved.isDirectory());
        assertTrue("moved after the backup closed", a.movedAfterBackupClosed());
        assertFalse("cluster/ is gone", cluster.exists());
        assertTrue(java.util.Arrays.toString(moved.list()), new File(moved, "recording.log").exists());
        a = null;

        // the restart: a new log recording from the supplier's start, restored from the pulled shadow
        final ReplicaProcessSmokeTest.Binding bindingA3 = new ReplicaProcessSmokeTest.Binding();
        a = startReplica(3, bindingA3, from(4));
        await("A bootstrap after the restart", () -> a.follower().bootstrapDone() || !fatals.isEmpty(), 120_000);
        assertTrue("fatal: " + fatals, fatals.isEmpty());
        assertEquals(ReplicaReadiness.RestorePath.LOCAL_SHADOW, a.readiness().restorePath());
        assertEquals(p2, a.follower().restoredPosition());
        assertTrue("a new log recording from the supplier's start: " + a.follower().logStartPositionAtBootstrap(),
                a.follower().logStartPositionAtBootstrap() >= start2);
        System.out.println("[PULL-SMOKE] re-bootstrap: moved " + moved.getName() + " (" + java.util.Arrays.toString(
                moved.list()) + "), restart restored LOCAL_SHADOW at " + p2 + ", L start "
                + a.follower().logStartPositionAtBootstrap() + ", orphans " + a.follower().orphanRecordings());
        send(10);
        await("A follows after the restart", () -> bindingA3.hash.count() == bindingB.hash.count()
                && bindingA3.hash.count() >= 3_020, 60_000);
        System.out.println("GREEN replica pull smoke: bootstrap pull PULLED_SHADOW at " + p1 + "; re-bootstrap exit "
                + EngineLifecycle.EXIT_RESTART + ", moved " + moved.getName() + ", restored at " + p2);
    }

    /** A stop during a pull returns well before the pull deadline (the close does not wait for it). */
    static final long STOP_DURING_PULL_BOUND_MS = 5_000;
    /** The control's pull deadline: long enough to break the bound, short enough to keep the control cheap. */
    static final long CONTROL_STARTUP_WAIT_MS = 15_000;
    static final String STOP_WAITED = "a stop waited for the pull deadline";

    @Test(timeout = 300_000)
    public void aStopDuringAPullEndsThePullAtOnce() throws IOException {
        startMember();
        send(3_000);
        snapshot();
        final long rec = memberLog();
        await("the member log past its first segment", () -> memberArchive.getRecordingPosition(rec)
                >= 2L * 64 * 1024, 60_000);
        final long start = purgeMemberLogBelow(memberArchive.getRecordingPosition(rec));
        assertTrue("the member log starts above 0: " + start, start > 0);

        // a bootstrap pull with 0 candidates at the default deadline: the stop ends it
        stopDuringPull(3, ShadowPullSettings.DEFAULT_STARTUP_WAIT_MS, java.util.EnumSet.noneOf(
                ReplicaNegativeControl.class));
        System.out.println("GREEN replica stop during a pull: close within " + STOP_DURING_PULL_BOUND_MS + " ms");

        // the control: the pull ignores the stop and the close waits for its deadline
        System.setProperty(ReplicaFaults.PROPERTY, "true");
        try {
            stopDuringPull(4, CONTROL_STARTUP_WAIT_MS, java.util.EnumSet.of(
                    ReplicaNegativeControl.STOP_WAITS_FOR_PULL));
        } catch (final AssertionError expected) {
            assertTrue("NC STOP_WAITS_FOR_PULL failed with another text: " + expected.getMessage(),
                    expected.getMessage().startsWith(STOP_WAITED));
            System.out.println("NC STOP_WAITS_FOR_PULL -> " + expected.getMessage());
            return;
        } finally {
            System.clearProperty(ReplicaFaults.PROPERTY);
        }
        org.junit.Assert.fail("NC STOP_WAITS_FOR_PULL: the row passed with the rule switched off");
    }

    /** Starts a replica that pulls from the member (0 candidates), stops it inside the pull, checks the close. */
    private void stopDuringPull(final int slot, final long startupWaitMs, final java.util.Set<ReplicaNegativeControl> off)
            throws IOException {
        fatals.clear();
        final ReplicaProcess r = startReplica(slot, new ReplicaProcessSmokeTest.Binding(), fromMember(startupWaitMs),
                off);
        a = r;
        await("replica " + slot + " pulling", () -> r.follower().lastPuller() != null || !fatals.isEmpty(), 60_000);
        assertTrue("fatal before the stop: " + fatals, fatals.isEmpty());
        await("replica " + slot + " pull passes", () -> r.follower().lastPuller().counters()
                .get(com.openexchange.cluster.shadow.ShadowPuller.Counter.PASSES_TOTAL) >= 2, 30_000);
        final long[] closeMs = {-1};
        final String err = MismatchHoldTest.captureErr(() -> {
            final long t0 = System.nanoTime();
            r.close();
            closeMs[0] = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            try {
                Thread.sleep(1_000); // anything the pull still prints after the close
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        a = null;
        if (closeMs[0] > STOP_DURING_PULL_BOUND_MS) {
            throw new AssertionError(STOP_WAITED + ": close took " + closeMs[0] + " ms > "
                    + STOP_DURING_PULL_BOUND_MS + " ms (pull deadline " + startupWaitMs + " ms)");
        }
        assertFalse(err, err.contains("reader thread did not stop"));
        assertFalse(err, err.contains("startup pass failed"));
        assertTrue("a stop is not a refusal: " + fatals, fatals.isEmpty());
        System.out.println("[PULL-SMOKE] replica " + slot + " stopped during a pull: close " + closeMs[0] + " ms");
    }
}
