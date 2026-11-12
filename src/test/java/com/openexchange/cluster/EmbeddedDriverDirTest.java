// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The kit's embedded driver directory check, the one rule the replica and the member share. Against a live driver
 * with Aeron's default 1 s timer (an operator's aeronmd) every one of {@value #ATTEMPTS} launches spread over the
 * heartbeat cycle is refused with the host's text naming the heartbeat age and the bound, and the live driver's CnC
 * is untouched; a stale directory (its driver gone) is replaced. The negative control uses the old 500 ms bound and
 * observes the takeover.
 */
public class EmbeddedDriverDirTest {

    static final int ATTEMPTS = 20;
    static final int CONTROL_ATTEMPTS = 40;
    /** Coprime with the 1 s heartbeat cycle, so the attempts sample all of its phases. */
    static final long SPACING_MS = 153;
    static final String OTHERS = "another core member's embedded driver or an aeronmd";

    private static MediaDriver external(final String aeronDir) {
        return MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
    }

    /** An embedded launch as a host does it: the guard on the context, the launch through the helper. */
    private static MediaDriver embedded(final String aeronDir, final long activeMs) {
        return EmbeddedDriverDir.launch(aeronDir, activeMs, () -> MediaDriver.launch(EmbeddedDriverDir.guard(
                new MediaDriver.Context(), activeMs).aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnShutdown(true)), OTHERS, "member");
    }

    /** As {@link #embedded} but with a bound below the helper's range: Aeron's own check at that bound. */
    private static MediaDriver embeddedUnchecked(final String aeronDir, final long activeMs) {
        return MediaDriver.launch(EmbeddedDriverDir.guard(new MediaDriver.Context(), EmbeddedDriverDir.MIN_ACTIVE_MS)
                .driverTimeoutMs(activeMs).aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnShutdown(true));
    }

    private static long inode(final File cnc) throws Exception {
        return (Long)Files.getAttribute(cnc.toPath(), "unix:ino");
    }

    @Test(timeout = 60_000)
    public void everyLaunchOnALiveDefaultTimerDriverIsRefusedWithTheHostText() throws Exception {
        final File dir = Files.createTempDirectory("kit-embedded-dir-").toFile();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        try (MediaDriver live = external(aeronDir)) {
            final File cnc = new File(aeronDir, CncFileDescriptor.CNC_FILE);
            final long cncInode = inode(cnc);
            int refusals = 0;
            long maxAge = -1;
            String text = null;
            for (int i = 1; i <= ATTEMPTS; i++) {
                Thread.sleep(SPACING_MS);
                try (MediaDriver taken = embedded(aeronDir, EmbeddedDriverDir.DEFAULT_ACTIVE_MS)) {
                    fail("attempt " + i + ": the member took over a live driver's directory " + aeronDir);
                } catch (final EmbeddedDriverDir.LiveDriverException refused) {
                    refusals++;
                    text = refused.getMessage();
                    maxAge = Math.max(maxAge, refused.live().heartbeatAgeMs());
                    assertTrue(text, text.startsWith("AERON_DIR " + new File(aeronDir).getAbsolutePath()
                            + " has a live media driver (" + OTHERS + "; heartbeat "));
                    assertTrue(text, text.endsWith(" ms old, bound 10000 ms): stop it or point this member at its own"
                            + " directory"));
                    assertTrue("the age is a number: " + text, refused.live().heartbeatAgeMs() >= 0);
                }
                assertEquals("attempt " + i + ": the live driver's CnC is untouched", cncInode, inode(cnc));
            }
            assertEquals(ATTEMPTS, refusals);
            assertTrue(CommonContext.isDriverActive(new File(aeronDir), EmbeddedDriverDir.DEFAULT_ACTIVE_MS, s -> { }));
            System.out.println("GREEN kit embedded dir check: " + refusals + "/" + ATTEMPTS + " refused (max heartbeat"
                    + " age " + maxAge + " ms); last text: " + text);
        } finally {
            IoUtil.delete(dir, true);
        }
    }

    @Test(timeout = 60_000)
    public void negativeControlTheOldBoundTakesTheLiveDirectoryOver() throws Exception {
        final File dir = Files.createTempDirectory("kit-embedded-dir-nc-").toFile();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        String text = null;
        try (MediaDriver live = external(aeronDir)) {
            final File cnc = new File(aeronDir, CncFileDescriptor.CNC_FILE);
            final long cncInode = inode(cnc);
            for (int i = 1; i <= CONTROL_ATTEMPTS && text == null; i++) {
                Thread.sleep(SPACING_MS);
                try (MediaDriver taken = embeddedUnchecked(aeronDir, 500)) {
                    text = "the member took over a live driver's directory at attempt " + i + " (CnC inode "
                            + cncInode + " -> " + inode(cnc) + ")";
                } catch (final io.aeron.driver.exceptions.ActiveDriverException refused) {
                    // inside the first half of the heartbeat cycle
                }
            }
        } finally {
            IoUtil.delete(dir, true);
        }
        if (text == null) {
            fail("negative control: " + CONTROL_ATTEMPTS + " launches at 500 ms were all refused");
        }
        System.out.println("NC DRIVER_DIR_ACTIVE_500MS -> " + text);
    }

    @Test(timeout = 60_000)
    public void aStaleDirectoryIsReplacedAndAnEmptyOneIsNoDriver() throws Exception {
        final File dir = Files.createTempDirectory("kit-embedded-dir-stale-").toFile();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        try {
            assertNull("no directory: no driver", EmbeddedDriverDir.probe(aeronDir, EmbeddedDriverDir.MIN_ACTIVE_MS));
            final long staleInode;
            // a driver that leaves its directory behind (no delete on shutdown): stale once its heartbeat ages out
            try (MediaDriver gone = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDir)
                    .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(false))) {
                staleInode = inode(new File(aeronDir, CncFileDescriptor.CNC_FILE));
                assertNotNull(EmbeddedDriverDir.probe(aeronDir, EmbeddedDriverDir.MIN_ACTIVE_MS));
            }
            assertTrue("the directory stays", new File(aeronDir, CncFileDescriptor.CNC_FILE).exists());
            Thread.sleep(EmbeddedDriverDir.MIN_ACTIVE_MS + 500);
            assertNull("stale after the bound", EmbeddedDriverDir.probe(aeronDir, EmbeddedDriverDir.MIN_ACTIVE_MS));
            try (MediaDriver replaced = embedded(aeronDir, EmbeddedDriverDir.MIN_ACTIVE_MS)) {
                assertNotNull("the stale directory was replaced by a live driver (CnC inode was " + staleInode + ")",
                        EmbeddedDriverDir.probe(aeronDir, EmbeddedDriverDir.MIN_ACTIVE_MS));
            }
        } finally {
            IoUtil.delete(dir, true);
        }
    }

    @Test
    public void theBoundIsASettingWithARange() {
        assertNull(EmbeddedDriverDir.rangeRefusal("CORE_DRIVER_DIR_ACTIVE_MS", 2_000));
        assertNull(EmbeddedDriverDir.rangeRefusal("CORE_DRIVER_DIR_ACTIVE_MS", 600_000));
        assertEquals("CORE_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 1999",
                EmbeddedDriverDir.rangeRefusal("CORE_DRIVER_DIR_ACTIVE_MS", 1_999));
        assertEquals("CORE_DRIVER_DIR_ACTIVE_MS must be 2000..600000 ms: 600001",
                EmbeddedDriverDir.rangeRefusal("CORE_DRIVER_DIR_ACTIVE_MS", 600_001));
        try {
            EmbeddedDriverDir.guard(new MediaDriver.Context(), 500);
            fail("a 500 ms bound accepted");
        } catch (final IllegalArgumentException e) {
            assertEquals("driver directory bound must be 2000..600000 ms: 500", e.getMessage());
        }
        final MediaDriver.Context c = EmbeddedDriverDir.guard(new MediaDriver.Context(), 10_000);
        assertEquals(10_000, c.driverTimeoutMs());
        assertTrue("never deletes blindly", !c.dirDeleteOnStart());
    }
}
