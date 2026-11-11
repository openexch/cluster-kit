// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Kit half of the package scans and the purge call-site rule: {@code cluster.replica} never schedules, holds no
 * {@code Cluster}/{@code ClientSession}/cluster client, reads no clock or environment of its own; only
 * {@code ReplicaProcess} launches a driver, archive or backup; only {@code RetrievedSetTrim} purges a recording
 * ({@code ShadowRetention} is the shadow owner in {@code cluster.shadow}); {@code cluster.shadow} still never
 * references the backup classes. Each rule has a positive control that the scan flags.
 */
public class ReplicaPackageScanTest {

    static final String PKG = "com/openexchange/cluster/replica/";
    static final String FAULTS = PKG + "ReplicaFaults";
    static final String PROCESS = PKG + "ReplicaProcess";
    static final String TRIM = PKG + "RetrievedSetTrim";
    static final String PROBE = PKG + "ForbiddenReplicaProbe";

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

    static boolean is(final String name, final String cls) {
        return name.equals(cls) || name.startsWith(cls + "$");
    }

    static Path main() {
        return classesOf(ReplicaProcess.class);
    }

    @Test
    public void theReplicaPackageNeverSchedulesOrReadsAClock() {
        final NeverSchedulesScan.Report all = NeverSchedulesScan.scanDirectory(main(),
                n -> n.startsWith(PKG) && !is(n, FAULTS), NeverSchedulesScan.Rules.replica());
        assertTrue("scanned " + all.classesScanned(), all.classesScanned() >= 14);
        assertTrue(all.findings().toString(), all.clean());
        final NeverSchedulesScan.Report gate = NeverSchedulesScan.scanDirectory(main(), n -> is(n, FAULTS),
                NeverSchedulesScan.Rules.replicaFaultsGate());
        assertTrue(gate.findings().toString(), gate.clean());
        // the single allowed getProperty is exactly that: the strict rule sees it once, in ReplicaFaults only
        final NeverSchedulesScan.Report strict = NeverSchedulesScan.scanDirectory(main(), n -> n.startsWith(PKG),
                NeverSchedulesScan.Rules.replica());
        assertEquals(strict.findings().toString(), 1, strict.findings().size());
        assertEquals("java/lang/System.getProperty", strict.findings().get(0).reference());
        assertTrue(strict.findings().get(0).className(), is(strict.findings().get(0).className(), FAULTS));
        System.out.println("GREEN replica package scan: " + all.classesScanned() + " classes, 0 findings; the single"
                + " getProperty is in " + FAULTS);
    }

    @Test
    public void onlyReplicaProcessLaunchesADriverArchiveOrBackup() {
        final NeverSchedulesScan.Report others = NeverSchedulesScan.scanDirectory(main(),
                n -> n.startsWith(PKG) && !is(n, PROCESS), NeverSchedulesScan.Rules.replicaDriverLaunch());
        assertTrue(others.findings().toString(), others.clean());
        final NeverSchedulesScan.Report process = NeverSchedulesScan.scanDirectory(main(), n -> is(n, PROCESS),
                NeverSchedulesScan.Rules.replicaDriverLaunch());
        final Set<String> launches = new TreeSet<>();
        process.findings().forEach(f -> launches.add(f.reference()));
        assertEquals("ReplicaProcess launches exactly the two modes' components", Set.of(
                "io/aeron/archive/Archive.launch",
                "io/aeron/cluster/ClusterBackup.launch",
                "io/aeron/cluster/ClusterBackupMediaDriver.launch"), launches);
    }

    static NeverSchedulesScan.Rules purgeRules() {
        return new NeverSchedulesScan.Rules(Set.of(), List.of(), Set.of(),
                Map.of("io/aeron/archive/client/AeronArchive", Set.of("purgeRecording"),
                        "com/openexchange/cluster/shadow/ShadowCatalog", Set.of("purge")),
                Map.of(), Map.of());
    }

    static Set<String> checkPurgeSites(final List<NeverSchedulesScan.Finding> findings) {
        final Set<String> callers = new TreeSet<>();
        for (final NeverSchedulesScan.Finding f : findings) {
            if (!is(f.className(), TRIM)) {
                throw new AssertionError("purge call site in replica: " + f.className() + " (" + f.reference() + ")");
            }
            callers.add(f.className() + " -> " + f.reference());
        }
        return callers;
    }

    @Test
    public void onlyTheSetTrimPurgesInTheReplicaPackage() {
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanDirectory(main(), n -> n.startsWith(PKG),
                purgeRules());
        final Set<String> callers = checkPurgeSites(report.findings());
        assertEquals(callers.toString(), Set.of(TRIM + " -> io/aeron/archive/client/AeronArchive.purgeRecording"),
                callers);
    }

    @Test
    public void theShadowPackageNeverReferencesTheBackup() {
        final NeverSchedulesScan.Rules backupOnly = new NeverSchedulesScan.Rules(
                Set.of("io/aeron/cluster/ClusterBackup", "io/aeron/cluster/ClusterBackupEventsListener",
                        "io/aeron/cluster/ClusterBackupMediaDriver"),
                List.of("com/openexchange/cluster/replica/"), Set.of(), Map.of(), Map.of(), Map.of());
        final NeverSchedulesScan.Report shadow = NeverSchedulesScan.scanDirectory(main(),
                n -> n.startsWith("com/openexchange/cluster/shadow/"), backupOnly);
        assertTrue("scanned " + shadow.classesScanned(), shadow.classesScanned() >= 20);
        assertTrue(shadow.findings().toString(), shadow.clean());
    }

    // ------------------------------------------------------------------------------------------- positive controls

    @Test
    public void positiveControlsAreFlagged() {
        final byte[] probe = bytes(ForbiddenReplicaProbe.class);
        final List<NeverSchedulesScan.Finding> rf = NeverSchedulesScan.scanClass(PROBE, probe,
                NeverSchedulesScan.Rules.replica());
        final Set<String> refs = new TreeSet<>();
        rf.forEach(f -> refs.add(f.reference()));
        assertTrue(refs.toString(), refs.contains("io/aeron/cluster/service/Cluster"));
        assertTrue(refs.toString(), refs.contains("io/aeron/cluster/service/Cluster.scheduleTimer"));
        assertTrue(refs.toString(), refs.contains("java/lang/System.nanoTime"));
        final List<NeverSchedulesScan.Finding> launch = NeverSchedulesScan.scanClass(PROBE, probe,
                NeverSchedulesScan.Rules.replicaDriverLaunch());
        final Set<String> launches = new TreeSet<>();
        launch.forEach(f -> launches.add(f.reference()));
        assertEquals(Set.of("io/aeron/driver/MediaDriver.launch", "io/aeron/cluster/ClusterBackup.launch"), launches);
        final List<NeverSchedulesScan.Finding> shadowRule = NeverSchedulesScan.scanClass(PROBE, probe,
                NeverSchedulesScan.Rules.shadow());
        assertTrue(shadowRule.toString(), shadowRule.stream()
                .anyMatch(f -> f.reference().equals("io/aeron/cluster/ClusterBackup")));
        System.out.println("NC REPLICA_SCAN_POSITIVE_CONTROLS -> " + refs + " | " + launches + " | "
                + shadowRule.get(0));
    }

    @Test
    public void aPurgeProbeInTheReplicaPackageIsFlagged() {
        final List<NeverSchedulesScan.Finding> withProbe = new java.util.ArrayList<>(NeverSchedulesScan.scanDirectory(
                main(), n -> n.startsWith(PKG), purgeRules()).findings());
        withProbe.addAll(NeverSchedulesScan.scanClass(PROBE, bytes(ForbiddenReplicaProbe.class), purgeRules()));
        try {
            checkPurgeSites(withProbe);
            fail("NC replica purge probe: not flagged");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("purge call site in replica: " + PROBE));
            System.out.println("NC REPLICA_PURGE_PROBE -> " + expected.getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------- the live oracle

    /** The mismatch dump writes files only: it never references the shadow package or the archive client. */
    @Test
    public void theMismatchDumpNeverTouchesTheCatalogOrTheArchive() {
        final NeverSchedulesScan.Rules archiveOrShadow = new NeverSchedulesScan.Rules(Set.of(),
                List.of("com/openexchange/cluster/shadow/", "io/aeron/archive/"), Set.of(), Map.of(), Map.of(),
                Map.of());
        final NeverSchedulesScan.Report dump = NeverSchedulesScan.scanDirectory(main(),
                n -> is(n, PKG + "MismatchDump"), archiveOrShadow);
        assertTrue("scanned " + dump.classesScanned(), dump.classesScanned() >= 1);
        assertTrue(dump.findings().toString(), dump.clean());
        assertTrue("the dump is not in the shadow package",
                MismatchDump.class.getPackageName().endsWith(".replica"));
    }

    /** The action ring is recorded at the action and has no fixed capacity (a capacity is a false history fence). */
    @Test
    public void theActionRingHasNoFixedCapacity() throws NoSuchFieldException {
        for (final java.lang.reflect.Field f : AlignmentTrust.class.getDeclaredFields()) {
            if (f.getName().toUpperCase(java.util.Locale.ROOT).contains("KEPT")
                    || f.getName().toUpperCase(java.util.Locale.ROOT).contains("CAPACITY")) {
                throw new AssertionError("action ring has a fixed capacity: " + f);
            }
        }
        assertEquals(java.util.TreeSet.class, AlignmentTrust.class.getDeclaredField("actions").getType());
    }
}
