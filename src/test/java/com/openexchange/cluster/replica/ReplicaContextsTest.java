// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.security.NullCredentialsSupplier;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Kit half of the replica contexts: fixed slot ports (never {@code :0}), the tuned backup defaults, the window
 * CRITICAL lines, the FOLLOWER-source refusal, the buffer cap, the test-hook gate.
 */
public class ReplicaContextsTest {

    @After
    public void clear() {
        System.clearProperty(ReplicaFaults.PROPERTY);
    }

    static ReplicaConfig.Builder base() {
        return ReplicaConfig.builder()
                .memberHosts(List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"))
                .replicaAddress("10.0.0.9")
                .portBase(9300)
                .portStride(100)
                .slot(3)
                .baseDir(new File("/var/lib/core-replica3"))
                .aeronDir("/dev/shm/aeron-core-replica3-driver")
                .engineId(1)
                .nodePort(9703)
                .credentials(new NullCredentialsSupplier());
    }

    /**
     * External-driver control: the embedded driver on a directory a live driver owns (an operator's
     * {@code TRANSPORT_DRIVER_MODE=embedded} with the aeronmd's {@code AERON_DIR}) is refused by the embedded launch's
     * own {@code ActiveDriverException}, and the live driver keeps its CnC: never a silent take-over of a shared
     * driver directory (the {@code dirDeleteOnStart(true)} context deleted it without a check, MD:488).
     */
    @Test(timeout = 60_000)
    public void theEmbeddedDriverRefusesADirectoryALiveDriverOwns() throws Exception {
        final File dir = java.nio.file.Files.createTempDirectory("replica-embedded-on-live-dir-").toFile();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        try (io.aeron.driver.MediaDriver live = io.aeron.driver.MediaDriver.launch(new io.aeron.driver.MediaDriver.Context()
                .aeronDirectoryName(aeronDir).threadingMode(io.aeron.driver.ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true))) {
            final File cnc = new File(aeronDir, io.aeron.CncFileDescriptor.CNC_FILE);
            final long cncInode = (Long)java.nio.file.Files.getAttribute(cnc.toPath(), "unix:ino");
            final io.aeron.driver.MediaDriver.Context embedded = ReplicaContexts.driver(base().aeronDir(aeronDir)
                    .baseDir(new File(dir, "state")).build());
            try (io.aeron.driver.MediaDriver taken = io.aeron.driver.MediaDriver.launch(embedded)) {
                fail("embedded driver launched on a live driver's directory " + aeronDir + " (it deleted the CnC of"
                        + " the running driver: inode " + cncInode + " -> "
                        + java.nio.file.Files.getAttribute(cnc.toPath(), "unix:ino") + ")");
            } catch (final io.aeron.driver.exceptions.ActiveDriverException refused) {
                assertTrue(refused.getMessage(), refused.getMessage().contains("Active media driver detected"));
                System.out.println("GREEN external-driver control (kit): the embedded launch refuses a live driver's dir: "
                        + refused.getMessage());
            }
            assertEquals("the live driver's CnC is untouched", cncInode,
                    (long)(Long)java.nio.file.Files.getAttribute(cnc.toPath(), "unix:ino"));
        } finally {
            org.agrona.IoUtil.delete(dir, true);
        }
    }

    @Test
    public void portsAreFixedSlotPortsAndDefaultsAreTuned() {
        final ReplicaConfig c = base().build();
        assertEquals("10.0.0.1:9303,10.0.0.2:9403,10.0.0.3:9503", ReplicaContexts.consensusEndpoints(c));
        assertEquals("aeron:udp?endpoint=10.0.0.9:9603", ReplicaContexts.consensusChannel(c));
        assertEquals("10.0.0.9:9604", ReplicaContexts.catchupEndpoint(c));
        assertEquals("aeron:udp?endpoint=10.0.0.9:9601", ReplicaContexts.archiveControlChannel(c));
        assertEquals("aeron:udp?endpoint=10.0.0.9:9605", ReplicaContexts.transferChannel(c));
        assertTrue(ReplicaContexts.catchupChannel(c), ReplicaContexts.catchupChannel(c)
                .contains("so-rcvbuf=4m|rcv-wnd=4m"));
        final ClusterBackup.Context b = ReplicaContexts.backup(c, new BackupEvents(System::nanoTime), t -> { },
                () -> { });
        assertEquals(ClusterBackup.SourceType.FOLLOWER, b.sourceType());
        assertEquals(TimeUnit.SECONDS.toNanos(12), b.clusterBackupCoolDownIntervalNs());
        assertEquals(TimeUnit.SECONDS.toNanos(60), b.clusterBackupIntervalNs());
        assertEquals(TimeUnit.SECONDS.toNanos(2), b.clusterBackupResponseTimeoutNs());
        assertEquals(TimeUnit.SECONDS.toNanos(10), b.clusterBackupProgressTimeoutNs());
        assertFalse("the replica dir is state: never deleted on start", b.deleteDirOnStart());
        assertEquals(ClusterBackup.Configuration.ReplayStart.BEGINNING, b.initialReplayStart());
        assertEquals(new File("/var/lib/core-replica3/cluster"), b.clusterDir());
        final AeronArchive.Context cluster = b.clusterArchiveContext();
        assertEquals("aeron:udp?endpoint=10.0.0.9:9605", cluster.controlResponseChannel());
        for (final String channel : List.of(ReplicaContexts.consensusChannel(c), ReplicaContexts.transferChannel(c),
                ReplicaContexts.archiveControlChannel(c), "aeron:udp?endpoint=" + ReplicaContexts.catchupEndpoint(c))) {
            assertFalse("ephemeral port in " + channel, channel.endsWith(":0"));
        }
        assertTrue("defaults must not warn: " + ReplicaContexts.windowWarnings(c),
                ReplicaContexts.windowWarnings(c).isEmpty());
    }

    /** The driver MTU, its term lengths and the catch-up so-sndbuf are settings with the old defaults. */
    @Test
    public void driverMtuTermLengthAndCatchupSndbufAreSettings() {
        final ReplicaConfig d = base().build();
        assertEquals(8192, ReplicaContexts.driver(d).mtuLength());
        assertEquals(16 * 1024 * 1024, ReplicaContexts.driver(d).ipcTermBufferLength());
        assertEquals(16 * 1024 * 1024, ReplicaContexts.driver(d).publicationTermBufferLength());
        assertTrue(ReplicaContexts.catchupChannel(d), ReplicaContexts.catchupChannel(d).contains("so-sndbuf=512k|"));
        final ReplicaConfig c = base().driverMtu(1408).driverTermLength(4 * 1024 * 1024).catchupSndbuf("2m").build();
        final io.aeron.driver.MediaDriver.Context driver = ReplicaContexts.driver(c);
        if (driver.mtuLength() != 1408 || driver.ipcTermBufferLength() != 4 * 1024 * 1024
                || driver.publicationTermBufferLength() != 4 * 1024 * 1024
                || !ReplicaContexts.catchupChannel(c).contains("so-sndbuf=2m|")) {
            throw new AssertionError("hard-coded driver values ignore the settings: mtu=" + driver.mtuLength()
                    + " ipcTerm=" + driver.ipcTermBufferLength() + " pubTerm=" + driver.publicationTermBufferLength()
                    + " catchup=" + ReplicaContexts.catchupChannel(c));
        }
    }

    /** {@code REPLICA_DRIVER_DIR_ACTIVE_MS}: default, both ends of the range, one step outside, and the context. */
    @Test
    public void driverDirActiveIsASettingWithARange() {
        final ReplicaConfig d = base().build();
        assertEquals(10_000, d.driverDirActiveMs());
        assertEquals(10_000, ReplicaContexts.driver(d).driverTimeoutMs());
        assertEquals(2_000, ReplicaContexts.driver(base().driverDirActiveMs(2_000).build()).driverTimeoutMs());
        assertEquals(600_000, ReplicaContexts.driver(base().driverDirActiveMs(600_000).build()).driverTimeoutMs());
        ReplicaOracleSettingsTest.refused(base().driverDirActiveMs(1_999),
                "REPLICA_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 1999");
        ReplicaOracleSettingsTest.refused(base().driverDirActiveMs(600_001),
                "REPLICA_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 600001");
        assertEquals("REPLICA_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 0",
                ReplicaConfig.driverDirActiveRefusal(0));
    }

    /**
     * The local control channel's term length follows {@code REPLICA_DRIVER_TERM_LENGTH} (no
     * 16m literal), and the backup records L on {@code REPLICA_LOG_STREAM_ID}.
     */
    @Test
    public void theControlChannelTermLengthAndTheLogStreamAreSettings() {
        final ReplicaConfig d = base().build();
        assertEquals("aeron:ipc?term-length=" + ReplicaConfig.DEFAULT_DRIVER_TERM_LENGTH,
                ReplicaContexts.localControlChannel(d));
        assertEquals(100, ReplicaContexts.backup(d, new BackupEvents(() -> 0, null), t -> { }, () -> { })
                .logStreamId());
        assertEquals(java.util.Set.of(100, 106, 107), d.clusterStreams());
        assertEquals(ReplicaConfig.DEFAULT_VERIFY_WAIT_MS, d.verifyWaitMs());
        final ReplicaConfig c = base().driverTermLength(4 * 1024 * 1024).logStreamId(200)
                .snapshotStreamIds(java.util.Set.of(206, 207)).build();
        final String control = ReplicaContexts.localControlChannel(c);
        final String archiveLocal = ReplicaContexts.archive(c, t -> { }).localControlChannel();
        final String client = ReplicaContexts.localArchiveClient(c).controlRequestChannel();
        final int stream = ReplicaContexts.backup(c, new BackupEvents(() -> 0, null), t -> { }, () -> { })
                .logStreamId();
        final String want = "aeron:ipc?term-length=" + 4 * 1024 * 1024;
        if (!want.equals(control) || !want.equals(archiveLocal) || !want.equals(client) || stream != 200
                || !java.util.Set.of(200, 206, 207).equals(c.clusterStreams())) {
            throw new AssertionError("literals ignore the settings: control=" + control + " archiveLocal="
                    + archiveLocal + " client=" + client + " backupLogStream=" + stream + " streams="
                    + c.clusterStreams());
        }
    }

    @Test
    public void belowTheMembersBackupWindowIsCriticalNotRefused() {
        final ReplicaConfig c = base().backupCooldownMs(1_000).backupQueryIntervalMs(11_999).build();
        final List<String> lines = ReplicaContexts.windowWarnings(c);
        assertEquals(lines.toString(), 2, lines.size());
        assertTrue(lines.get(0), lines.get(0).startsWith("CRITICAL: REPLICA_BACKUP_COOLDOWN_MS=1000 below the"
                + " members' backup window"));
        assertTrue(lines.get(1), lines.get(1).startsWith("CRITICAL: REPLICA_BACKUP_QUERY_INTERVAL_MS=11999 below"));
        assertTrue(ReplicaContexts.windowWarnings(base().backupCooldownMs(12_000).backupQueryIntervalMs(12_000)
                .build()).isEmpty());
        assertEquals("the threshold follows REPLICA_MEMBER_BACKUP_WINDOW_MS", 1, ReplicaContexts.windowWarnings(
                base().memberBackupWindowMs(20_000).backupQueryIntervalMs(60_000).build()).size());
    }

    private static void refused(final ReplicaConfig.Builder b, final String text) {
        try {
            b.build();
            fail("accepted, expected refusal: " + text);
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(text));
        }
    }

    @Test
    public void structuralRefusals() {
        refused(base().memberHosts(List.of("10.0.0.1")),
                "REPLICA_BACKUP_SOURCE=FOLLOWER needs >= 2 members; use ANY");
        base().memberHosts(List.of("10.0.0.1")).sourceType(ClusterBackup.SourceType.ANY).build();
        refused(base().shadowBufferMb(2048), "REPLICA_SHADOW_BUFFER_MB=2048 out of range [16,2047]");
        refused(base().shadowBufferMb(15), "REPLICA_SHADOW_BUFFER_MB=15 out of range");
        base().shadowBufferMb(2047).build();
        refused(base().replicaAddress(" "), "CLUSTER_REPLICA_ADDRESS is required");
        refused(base().credentials(null), "CORE_AUTH_CREDENTIAL is required");
        refused(base().attachBackoffMs(5_001), "REPLICA_ATTACH_BACKOFF_MS out of range [1,5000]");
        refused(base().livenessStaleMs(1_000).readyStaleMs(5_000), "REPLICA_LIVENESS_STALE_MS must be >=");
        refused(base().backupCooldownMs(0), "REPLICA_BACKUP_COOLDOWN_MS must be > 0");
        refused(base().nodePort(0), "CORE_NODE_PORT out of range");
    }

    @Test
    public void testHooksNeedTheGate() {
        refused(base().testHooks(ReplicaFaults.NONE, EnumSet.of(ReplicaNegativeControl.FENCE_OFF)),
                "replica test hooks need -D" + ReplicaFaults.PROPERTY + "=true");
        refused(base().testHooks(new ReplicaFaults() { }, EnumSet.noneOf(ReplicaNegativeControl.class)),
                "a production replica with a fault seam");
        System.setProperty(ReplicaFaults.PROPERTY, "true");
        final ReplicaConfig c = base().testHooks(ReplicaFaults.NONE, EnumSet.of(ReplicaNegativeControl.FENCE_OFF))
                .build();
        assertEquals(EnumSet.of(ReplicaNegativeControl.FENCE_OFF), c.negativeControls());
        assertTrue(c.toString(), c.toString().contains("NEGATIVE_CONTROLS=[FENCE_OFF]"));
    }
    /**
     * The cluster stream settings never overlap the replica's own streams (the shadow alias stream,
     * the shadow replay stream and the follower's replay stream, whatever their settings say). One row per reserved id
     * and setting.
     */
    @Test
    public void reservedReplicaStreamIdsAreRefused() {
        final int movedReplay = 5_000;
        final Object[][] rows = {
                {com.openexchange.cluster.shadow.ShadowAlias.STREAM_ID, "shadow alias", base()},
                {com.openexchange.cluster.shadow.ShadowAlias.REPLAY_STREAM_ID, "shadow replay", base()},
                {ReplicaConfig.DEFAULT_REPLAY_STREAM_ID, "follower replay", base()},
                {movedReplay, "follower replay", null},
        };
        final List<String> wrong = new java.util.ArrayList<>();
        for (final Object[] row : rows) {
            final int id = (Integer)row[0];
            final String name = (String)row[1];
            final ReplicaConfig.Builder log = row[2] == null ? base().replayStreamId(movedReplay) : base();
            final ReplicaConfig.Builder snap = row[2] == null ? base().replayStreamId(movedReplay) : base();
            check(wrong, log.logStreamId(id), "REPLICA_LOG_STREAM_ID names " + id + ", the replica's own " + name
                    + " stream");
            check(wrong, snap.snapshotStreamIds(java.util.Set.of(106, id)), "REPLICA_SNAPSHOT_STREAM_IDS names " + id
                    + ", the replica's own " + name + " stream");
        }
        if (!wrong.isEmpty()) {
            throw new AssertionError("reserved replica stream ids not refused: " + wrong);
        }
        // the default follower replay id is free once the replay stream moved
        assertEquals(ReplicaConfig.DEFAULT_REPLAY_STREAM_ID, base().replayStreamId(movedReplay)
                .logStreamId(ReplicaConfig.DEFAULT_REPLAY_STREAM_ID).build().logStreamId());
    }

    private static void check(final List<String> wrong, final ReplicaConfig.Builder b, final String text) {
        try {
            b.build();
            wrong.add("accepted (expected '" + text + "')");
        } catch (final IllegalArgumentException e) {
            if (!e.getMessage().equals(text)) {
                wrong.add("'" + e.getMessage() + "' (expected '" + text + "')");
            }
        }
    }
    /**
     * The stall bound cannot be shorter than one backup cycle. {@code REPLICA_VERIFY_WAIT_MS} below
     * 2 x {@code REPLICA_BACKUP_QUERY_INTERVAL_MS} + {@code REPLICA_BACKUP_COOLDOWN_MS} is refused with a text naming
     * the three values; the default (180 000) passes with the default query interval and cool-down.
     */
    @Test
    public void aVerifyWaitShorterThanOneBackupCycleIsRefused() {
        final List<String> wrong = new java.util.ArrayList<>();
        if (ReplicaConfig.DEFAULT_VERIFY_WAIT_MS != 180_000) {
            wrong.add("default REPLICA_VERIFY_WAIT_MS=" + ReplicaConfig.DEFAULT_VERIFY_WAIT_MS + " (expected 180000)");
        }
        check(wrong, base().verifyWaitMs(131_999), "REPLICA_VERIFY_WAIT_MS=131999 must be >= 2 x"
                + " REPLICA_BACKUP_QUERY_INTERVAL_MS=60000 + REPLICA_BACKUP_COOLDOWN_MS=12000 = 132000 (the stall bound"
                + " cannot be shorter than one backup cycle)");
        check(wrong, base().backupQueryIntervalMs(5_000).backupCooldownMs(2_000).verifyWaitMs(11_999),
                "REPLICA_VERIFY_WAIT_MS=11999 must be >= 2 x REPLICA_BACKUP_QUERY_INTERVAL_MS=5000 +"
                        + " REPLICA_BACKUP_COOLDOWN_MS=2000 = 12000 (the stall bound cannot be shorter than one backup"
                        + " cycle)");
        if (!wrong.isEmpty()) {
            throw new AssertionError("a verify wait shorter than one backup cycle was not refused: " + wrong);
        }
        assertEquals(132_000, base().verifyWaitMs(132_000).build().verifyWaitMs());
        assertEquals(180_000, base().build().verifyWaitMs());
    }
}
