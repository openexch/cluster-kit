// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.testkit.ForbiddenClockProbe;
import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** RCS S3 11.8: the shadow package obeys hard rule 9.1, and the scan can see what it forbids. */
public class ShadowPackageScanTest {

    static final String PKG = "com/openexchange/cluster/shadow/";
    static final String SELF_CHECK = PKG + "ShadowSelfCheck";

    static Path classesOf(final Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (final URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] bytes(final Class<?> type) {
        try (InputStream in = type.getClassLoader().getResourceAsStream(type.getName().replace('.', '/') + ".class")) {
            return in.readAllBytes();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void theShadowPackageHasNoForbiddenReference() {
        final Path main = classesOf(ShadowCatalog.class);
        final NeverSchedulesScan.Report all = NeverSchedulesScan.scanDirectory(main,
                n -> n.startsWith(PKG) && !n.startsWith(SELF_CHECK), NeverSchedulesScan.Rules.shadow());
        assertTrue("scanned " + all.classesScanned(), all.classesScanned() >= 20);
        assertTrue(all.findings().toString(), all.clean());
        final NeverSchedulesScan.Report self = NeverSchedulesScan.scanDirectory(main,
                n -> n.startsWith(SELF_CHECK), NeverSchedulesScan.Rules.shadowSelfCheck());
        assertTrue(self.findings().toString(), self.clean());
        // the one allowed getProperty is exactly that: the full rule sees it once, in ShadowSelfCheck only
        final NeverSchedulesScan.Report strict = NeverSchedulesScan.scanDirectory(main, n -> n.startsWith(PKG),
                NeverSchedulesScan.Rules.shadow());
        assertEquals(strict.findings().toString(), 1, strict.findings().size());
        assertEquals("java/lang/System.getProperty", strict.findings().get(0).reference());
        assertEquals(SELF_CHECK, strict.findings().get(0).className());
        ShadowRig.green("package scan: " + all.classesScanned() + " classes + " + self.classesScanned()
                + " ShadowSelfCheck class(es), 0 findings; the single getProperty is in " + SELF_CHECK);
    }

    @Test
    public void positiveControlsAreFlagged() {
        final List<NeverSchedulesScan.Finding> driver = NeverSchedulesScan.scanClass(
                PKG + "ForbiddenDriverProbe", bytes(ForbiddenDriverProbe.class), NeverSchedulesScan.Rules.shadow());
        assertTrue(driver.toString(), driver.stream()
                .anyMatch(f -> f.reference().equals("io/aeron/archive/ArchivingMediaDriver")));
        final List<NeverSchedulesScan.Finding> log = NeverSchedulesScan.scanClass(PKG + "ForbiddenRecordingLogProbe",
                bytes(ForbiddenRecordingLogProbe.class), NeverSchedulesScan.Rules.shadow());
        assertTrue(log.toString(), log.stream().anyMatch(f -> f.reference().equals("io/aeron/cluster/RecordingLog")));
        final List<NeverSchedulesScan.Finding> clock = NeverSchedulesScan.scanClass(
                "com/openexchange/cluster/testkit/ForbiddenClockProbe", bytes(ForbiddenClockProbe.class),
                NeverSchedulesScan.Rules.shadow());
        assertTrue(clock.toString(), clock.stream()
                .anyMatch(f -> f.reference().equals("java/lang/System.currentTimeMillis")));
        System.out.println("NC SCAN_POSITIVE_CONTROLS -> " + driver.get(0) + " | " + log.get(0) + " | " + clock.get(0));
    }
}
