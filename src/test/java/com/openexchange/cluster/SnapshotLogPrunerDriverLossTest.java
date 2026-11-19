// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.CncFileDescriptor;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The media driver dies while the pruner is about to make its leader members query on the engine's shared client.
 * That call holds the client's lock for its whole wait on the dead driver, so it is the one that sees the driver
 * timeout, and Aeron throws the timeout to the caller instead of reporting it to the client's error handler. The
 * node must still take the driver loss path (halt at once, no component close), not the unexpected agent
 * termination the client's conductor raises afterwards.
 *
 * <p>The driver is an external one, a process of its own killed with SIGKILL (a driver closed in this JVM marks the
 * CnC as shut down, which the client's conductor reads on its next keepalive check, before the pruner calls). The
 * client's error handler is the engines' one: {@link EngineLifecycle#onError} with the external driver flag.</p>
 */
public class SnapshotLogPrunerDriverLossTest {

    private static final long CLIENT_DRIVER_TIMEOUT_MS = 2_000;

    @Test(timeout = 60_000)
    public void aDriverLossSeenFirstByThePrunerHaltsAsADriverLoss() throws Exception {
        final File base = Files.createTempDirectory("pruner-driver-loss-").toFile();
        final File clusterDir = new File(base, "cluster");
        IoUtil.ensureDirectoryExists(clusterDir, "cluster");
        final List<String> logs = new CopyOnWriteArrayList<>();
        final List<Integer> halts = new CopyOnWriteArrayList<>();
        final CountDownLatch halted = new CountDownLatch(1);
        final EngineLifecycle lifecycle = new EngineLifecycle("test-node", logs::add, status -> {
            halts.add(status);
            halted.countDown();
        }, 1_000, 1_000); // a short fatal grace: the wrong path halts too, and the assertions below tell them apart

        final File aeronDir = new File(base, "aeron");
        final Path driverLog = base.toPath().resolve("driver.log");
        final Process driver = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Daeron.dir=" + aeronDir.getAbsolutePath(),
                "-Daeron.threading.mode=SHARED",
                "-Daeron.dir.delete.on.start=true",
                "-cp", System.getProperty("java.class.path"), MediaDriver.class.getName())
                .redirectErrorStream(true).redirectOutput(driverLog.toFile()).start();
        Aeron client = null;
        SnapshotLogPruner pruner = null;
        try {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!new File(aeronDir, CncFileDescriptor.CNC_FILE).exists()) {
                assertTrue("the driver did not start: " + (Files.exists(driverLog) ? Files.readString(driverLog) : ""),
                        System.nanoTime() < deadline && driver.isAlive());
                Thread.sleep(20);
            }
            final Aeron engineClient = Aeron.connect(new Aeron.Context()
                    .aeronDirectoryName(aeronDir.getAbsolutePath())
                    .driverTimeoutMs(CLIENT_DRIVER_TIMEOUT_MS)
                    .errorHandler(t -> {
                        logs.add("client error: " + t);
                        lifecycle.onError(t, true);
                    }));
            client = engineClient;
            pruner = new SnapshotLogPruner(clusterDir,
                    new AeronArchive.Context().aeron(engineClient).ownsAeronClient(false),
                    engineClient.countersReader(), 0, () -> true)
                    .reportWatermark(new ClusterMemberPositions(engineClient, "aeron:ipc?term-length=64k", 104, 105),
                            () -> true)
                    .pollIntervalMs(300);

            // kill -9 of the external driver: the client learns it only through the keepalive age. The pruner's first
            // call comes after 300 ms, well inside the client's driver timeout, and holds the client's lock until then.
            driver.destroyForcibly();
            assertTrue(driver.waitFor(10, TimeUnit.SECONDS));
            pruner.start();

            assertTrue("the node halted within the client's driver timeout: " + logs,
                    halted.await(CLIENT_DRIVER_TIMEOUT_MS + 10_000, TimeUnit.MILLISECONDS));
            // let the client's conductor raise its agent termination, which the fix must not turn into a second stop
            Thread.sleep(1_000);

            assertEquals(EngineLifecycle.Cause.FATAL, lifecycle.cause());
            assertTrue("the driver loss path ran (halt at once, no component close): " + logs,
                    logs.stream().anyMatch(l -> l.contains("media driver lost in EXTERNAL driver mode")
                            && l.contains("halting now, no component close")));
            assertFalse("not the unexpected agent termination path, which closes the components: " + logs,
                    logs.stream().anyMatch(l -> l.contains("agent terminated unexpectedly")));
            assertEquals("one halt, with 1: " + logs, List.of(1), halts);
        } finally {
            if (pruner != null) {
                pruner.stop();
            }
            CloseHelper.quietClose(client);
            driver.destroyForcibly();
            IoUtil.delete(base, true);
        }
    }
}
