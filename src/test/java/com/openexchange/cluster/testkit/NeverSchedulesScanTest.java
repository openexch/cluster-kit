// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.testkit.closure.ClosureRoot;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** RCS S2 3.7: the scanner reads descriptors, matches exactly, walks a closure with paths, and sees the controls. */
public class NeverSchedulesScanTest {

    private static Path classesOf(final Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (final URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] classBytes(final Class<?> type) {
        final String resource = type.getName().replace('.', '/') + ".class";
        try (InputStream in = type.getClassLoader().getResourceAsStream(resource)) {
            return in.readAllBytes();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<NeverSchedulesScan.Finding> scan(final Class<?> type, final NeverSchedulesScan.Rules rules) {
        return NeverSchedulesScan.scanClass(type.getName().replace('.', '/'), classBytes(type), rules);
    }

    @Test
    public void aFieldDescriptorAloneIsFlagged() {
        final NeverSchedulesScan.ConstantPool pool = NeverSchedulesScan.ConstantPool.read(
                classBytes(ForbiddenFieldProbe.class));
        assertFalse("control: the probe has no class constant for Cluster, only a descriptor",
                pool.classNames().contains("io/aeron/cluster/service/Cluster"));
        final List<NeverSchedulesScan.Finding> findings = scan(ForbiddenFieldProbe.class,
                NeverSchedulesScan.Rules.rf6());
        assertEquals(1, findings.size());
        assertEquals("io/aeron/cluster/service/Cluster", findings.get(0).reference());
    }

    @Test
    public void clockAndScheduleTimerMembersAreFlaggedUnderRf6() {
        assertTrue(scan(ForbiddenClockProbe.class, NeverSchedulesScan.Rules.rf6()).stream()
                .anyMatch(f -> f.reference().equals("java/lang/System.currentTimeMillis")));
        final String csa = "io/aeron/cluster/service/ClusteredServiceAgent";
        final byte[] csaBytes;
        try (InputStream in = NeverSchedulesScanTest.class.getClassLoader().getResourceAsStream(csa + ".class")) {
            csaBytes = in.readAllBytes();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        assertTrue(NeverSchedulesScan.scanClass(csa, csaBytes, NeverSchedulesScan.Rules.rf6()).stream()
                .anyMatch(f -> f.reference().endsWith(".scheduleTimer")));
    }

    @Test
    public void namesThatOnlyStartWithAForbiddenNameAreNotFlagged() {
        assertEquals(List.of(), scan(ExactNameProbe.class, NeverSchedulesScan.Rules.rf6()));
        // The kit half allows java/lang/System only for arraycopy.
        assertTrue(scan(ExactNameProbe.class, NeverSchedulesScan.Rules.kit()).stream()
                .anyMatch(f -> f.reference().equals("java/lang/System.identityHashCode")));
    }

    @Test
    public void typeMatchingIsExact() {
        final String cluster = "io/aeron/cluster/service/Cluster";
        assertTrue(NeverSchedulesScan.matchesType(cluster, cluster));
        assertTrue(NeverSchedulesScan.matchesType("io.aeron.cluster.service.Cluster", cluster));
        assertTrue(NeverSchedulesScan.matchesType("Lio/aeron/cluster/service/Cluster;", cluster));
        assertTrue(NeverSchedulesScan.matchesType("(JLio/aeron/cluster/service/Cluster;)V", cluster));
        assertTrue(NeverSchedulesScan.matchesType("Ljava/util/List<Lio/aeron/cluster/service/Cluster;>;", cluster));
        assertFalse(NeverSchedulesScan.matchesType("io/aeron/cluster/service/ClusterClock", cluster));
        assertFalse(NeverSchedulesScan.matchesType("Lio/aeron/cluster/service/ClusteredService;", cluster));
        assertFalse(NeverSchedulesScan.matchesType("Lio/aeron/cluster/service/Cluster$Role;", cluster));
    }

    @Test
    public void closureScanFollowsReferencesAndReportsThePath() {
        final String root = ClosureRoot.class.getName().replace('.', '/');
        final String pkg = "com/openexchange/cluster/testkit/closure/";
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanClosure(classesOf(ClosureRoot.class), root,
                pkg, NeverSchedulesScan.Rules.rf6());
        assertEquals(List.of(pkg + "ClosureRoot", pkg + "ClosureMiddle", pkg + "ClosureLeaf"), report.classes());
        assertEquals(1, report.findings().size());
        final NeverSchedulesScan.Finding f = report.findings().get(0);
        assertEquals("java/lang/System.nanoTime", f.reference());
        assertEquals(List.of(pkg + "ClosureRoot", pkg + "ClosureMiddle", pkg + "ClosureLeaf"), f.path());
        assertFalse("an unreferenced class in the package is not part of the closure",
                report.classes().contains(pkg + "ClosureUnreached"));
    }

    @Test
    public void closureResolvesAcrossClassRootsInOrder() {
        // Root in test-classes, its in-prefix reference in main classes: with both roots it is followed.
        final Path testClasses = classesOf(com.openexchange.cluster.testkit.closure.ClosureCrossRoot.class);
        final Path mainClasses = classesOf(com.openexchange.cluster.log.LogCursor.class);
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanClosure(List.of(testClasses, mainClasses),
                "com/openexchange/cluster/testkit/closure/ClosureCrossRoot", "com/openexchange/",
                NeverSchedulesScan.Rules.rf6());
        assertTrue(report.classes().contains("com/openexchange/cluster/log/LogCursor"));
        assertTrue(report.clean());
        // With the test root alone it fails closed, naming the class and the path.
        try {
            NeverSchedulesScan.scanClosure(testClasses, "com/openexchange/cluster/testkit/closure/ClosureCrossRoot",
                    "com/openexchange/", NeverSchedulesScan.Rules.rf6());
            org.junit.Assert.fail("unresolved in-prefix class must throw");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("com/openexchange/cluster/log/LogCursor")
                    && expected.getMessage().contains("ClosureCrossRoot -> "));
        }
    }

    @Test
    public void goldenAssertIsOutsideTheKitHalfForAReason() {
        // System.out and a system-property read: why no kit-half class may reach it (ShadowRestoreRunner uses TextDiff).
        final List<NeverSchedulesScan.Finding> findings = scan(GoldenAssert.class, NeverSchedulesScan.Rules.kit());
        assertTrue(findings.toString(), findings.stream().anyMatch(f -> f.reference().equals("java/lang/System.out")));
        assertTrue(findings.toString(), findings.stream()
                .anyMatch(f -> f.reference().equals("java/lang/Boolean.getBoolean")));
        assertEquals(List.of(), scan(TextDiff.class, NeverSchedulesScan.Rules.kit()));
    }

    @Test
    public void agnosticListFlagsAReflectiveAssetsName() {
        assertTrue(scan(ForbiddenAgnosticProbe.class, NeverSchedulesScan.Rules.kit()).stream()
                .anyMatch(f -> f.rule().equals("forbidden package")));
        assertEquals("the agnostic list is the kit half's only", List.of(),
                scan(ForbiddenAgnosticProbe.class, NeverSchedulesScan.Rules.rf6()));
    }
}
