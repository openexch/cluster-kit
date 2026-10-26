// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.testkit.closure.ClosureCrossRoot;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Review (RCS S2 review-kit DEFECT-2, FIX-3). RED until fixed: the closure must not silently drop an in-prefix class it
 * cannot find, and RF-6 must see every JDK way to schedule, read a clock or read the environment.
 */
public class ReviewKitRedScanSoundnessTest {

    private static Path classesOf(final Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (final URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] classBytes(final Class<?> type) {
        try (InputStream in = type.getClassLoader().getResourceAsStream(type.getName().replace('.', '/')
                + ".class")) {
            return in.readAllBytes();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void anInPrefixReferenceOutsideTheClassRootIsNotDroppedSilently() {
        // The root lives in test-classes and references com/openexchange/cluster/log/LogCursor in classes: the same
        // shape as AssetsLogApplier (assets-cluster) reaching 113 classes in assets-common.
        final String root = ClosureCrossRoot.class.getName().replace('.', '/');
        final NeverSchedulesScan.Report report;
        try {
            report = NeverSchedulesScan.scanClosure(classesOf(ClosureCrossRoot.class), root, "com/openexchange/",
                    NeverSchedulesScan.Rules.rf6());
        } catch (final IllegalStateException refused) {
            return;                                                     // fail closed: acceptable
        }
        assertTrue("closure silently skipped an unresolved in-prefix class: " + report.classes(),
                report.classes().contains("com/openexchange/cluster/log/LogCursor"));
    }

    @Test
    public void rf6SeesJdkSchedulersClocksAndPropertyReads() {
        final List<NeverSchedulesScan.Finding> findings = NeverSchedulesScan.scanClass(
                ForbiddenJdkProbe.class.getName().replace('.', '/'), classBytes(ForbiddenJdkProbe.class),
                NeverSchedulesScan.Rules.rf6());
        final List<String> missing = new ArrayList<>();
        for (final String reference : List.of("java/util/concurrent/ScheduledExecutorService", "java/util/Timer",
                "java/time/Instant", "java/time/Clock", "java/lang/Thread", "java/lang/Long.getLong",
                "java/lang/Integer.getInteger", "java/lang/Boolean.getBoolean", "java/lang/System.getProperties",
                "org/agrona/concurrent/SystemNanoClock", "org/agrona/concurrent/EpochNanoClock")) {
            if (findings.stream().noneMatch(f -> f.reference().startsWith(reference))) {
                missing.add(reference);
            }
        }
        assertEquals("RF-6 blind to", List.of(), missing);
    }
}
