// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The embedded launch against a live driver with Aeron's default timer interval (1 s): that driver writes its CnC
 * heartbeat once per second, so its heartbeat age at any instant is anywhere in 0..~1000 ms. Every one of
 * {@value #ATTEMPTS} attempts spread over that cycle must be refused with {@code ActiveDriverException}, and the live
 * driver keeps its CnC. The control uses the same context with the old 500 ms bound: a launch that lands in the
 * second half of a heartbeat cycle reads the live directory as stale, deletes it and starts its own driver.
 */
public class EmbeddedDriverDirCheckTest {

    static final int ATTEMPTS = 20;
    static final int CONTROL_ATTEMPTS = 40;
    /** Coprime with the 1 s heartbeat cycle, so the attempts sample all of its phases. */
    static final long SPACING_MS = 153;
    static final long OLD_BOUND_MS = 500;

    /** A live driver as an operator's aeronmd runs it: its own directory, Aeron's default timer interval. */
    private static MediaDriver external(final String aeronDir) {
        return MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
    }

    private static long inode(final File cnc) throws Exception {
        return (Long)Files.getAttribute(cnc.toPath(), "unix:ino");
    }

    /** The live driver's heartbeat age right now, as Aeron's own check reports it. */
    private static long heartbeatAgeMs(final String aeronDir) {
        final AtomicLong age = new AtomicLong(-1);
        CommonContext.isDriverActive(new File(aeronDir), ReplicaConfig.DEFAULT_DRIVER_DIR_ACTIVE_MS, line -> {
            final int at = line.indexOf("(ms): ");
            if (at >= 0) {
                age.set(Long.parseLong(line.substring(at + 6).trim()));
            }
        });
        return age.get();
    }

    private static ReplicaConfig cfg(final File dir, final String aeronDir) {
        return ReplicaContextsTest.base().aeronDir(aeronDir).baseDir(new File(dir, "state")).build();
    }

    @Test(timeout = 60_000)
    public void everyEmbeddedLaunchOnALiveDefaultTimerDriverIsRefused() throws Exception {
        final File dir = Files.createTempDirectory("replica-embedded-dir-check-").toFile();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        try (MediaDriver live = external(aeronDir)) {
            final File cnc = new File(aeronDir, CncFileDescriptor.CNC_FILE);
            final long cncInode = inode(cnc);
            final ReplicaConfig c = cfg(dir, aeronDir);
            long maxAge = -1;
            for (int i = 1; i <= ATTEMPTS; i++) {
                Thread.sleep(SPACING_MS);
                maxAge = Math.max(maxAge, heartbeatAgeMs(aeronDir));
                try (MediaDriver taken = MediaDriver.launch(ReplicaContexts.driver(c))) {
                    fail("attempt " + i + ": the embedded driver took over the live driver's directory " + aeronDir
                            + " (CnC inode " + cncInode + " -> " + inode(cnc) + ", bound "
                            + c.driverDirActiveMs() + " ms)");
                } catch (final ActiveDriverException refused) {
                    assertTrue(refused.getMessage(), refused.getMessage().contains("Active media driver detected"));
                }
                assertEquals("attempt " + i + ": the live driver's CnC is untouched", cncInode, inode(cnc));
            }
            assertTrue("the live driver is still active after the attempts",
                    CommonContext.isDriverActive(new File(aeronDir), ReplicaConfig.DEFAULT_DRIVER_DIR_ACTIVE_MS,
                            s -> { }));
            System.out.println("GREEN embedded dir check: " + ATTEMPTS + "/" + ATTEMPTS + " launches refused on a live"
                    + " default-timer driver (max heartbeat age seen " + maxAge + " ms, bound " + c.driverDirActiveMs()
                    + " ms), CnC inode " + cncInode + " unchanged");
        } finally {
            IoUtil.delete(dir, true);
        }
    }

    @Test(timeout = 60_000)
    public void negativeControlTheOldBoundTakesTheLiveDirectoryOver() throws Exception {
        final File dir = Files.createTempDirectory("replica-embedded-dir-check-nc-").toFile();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        String text = null;
        try (MediaDriver live = external(aeronDir)) {
            final File cnc = new File(aeronDir, CncFileDescriptor.CNC_FILE);
            final long cncInode = inode(cnc);
            final ReplicaConfig c = cfg(dir, aeronDir);
            int refusals = 0;
            for (int i = 1; i <= CONTROL_ATTEMPTS && text == null; i++) {
                Thread.sleep(SPACING_MS);
                final long age = heartbeatAgeMs(aeronDir);
                try (MediaDriver taken = MediaDriver.launch(ReplicaContexts.driver(c).driverTimeoutMs(OLD_BOUND_MS))) {
                    text = "attempt " + i + " (after " + refusals + " refusals, heartbeat age ~" + age + " ms > "
                            + OLD_BOUND_MS + " ms): the embedded driver deleted the live driver's directory and"
                            + " started its own (CnC inode " + cncInode + " -> " + inode(cnc) + ")";
                } catch (final ActiveDriverException refused) {
                    refusals++;
                }
            }
        } finally {
            IoUtil.delete(dir, true);
        }
        if (text == null) {
            fail("negative control: " + CONTROL_ATTEMPTS + " launches with the old " + OLD_BOUND_MS + " ms bound were"
                    + " all refused; the test does not reach the heartbeat window it claims to cover");
        }
        System.out.println("NC DRIVER_DIR_ACTIVE_500MS -> " + text);
    }
}
