// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 9.2 / 11.15 (review F6, D-S4-6): one purge owner per archive, held structurally in the kit. Every main class
 * under {@code com/openexchange/cluster/} is scanned for calls to {@code AeronArchive.purgeRecording} and
 * {@code ShadowCatalog.purge}; the only callers allowed are {@link ShadowRetention} (the owner),
 * {@link ShadowCatalog} itself (its {@code purge}, the archive call at the end), {@link ShadowWriter} (its own aborted
 * recording, by id) and {@code StagingArchive} (bundle staging, never a shadow), plus the replica's
 * {@code replica.RetrievedSetTrim} (the retrieved member snapshot set trim, allow-listed by class name; it purges
 * member CM/service snapshot recordings, never a shadow). Anything else in {@code cluster}, {@code cluster.shadow} or
 * {@code cluster.replica} fails.
 */
public class ShadowPurgeCallSiteTest {

    static final String PKG = "com/openexchange/cluster/";
    static final Set<String> ALLOWED = Set.of(
            PKG + "shadow/ShadowRetention",
            PKG + "shadow/ShadowCatalog",
            PKG + "shadow/ShadowWriter",
            PKG + "StagingArchive",
            PKG + "replica/RetrievedSetTrim");

    static NeverSchedulesScan.Rules purgeRules() {
        return new NeverSchedulesScan.Rules(Set.of(), List.of(), Set.of(),
                Map.of("io/aeron/archive/client/AeronArchive", Set.of("purgeRecording"),
                        "com/openexchange/cluster/shadow/ShadowCatalog", Set.of("purge")),
                Map.of(), Map.of());
    }

    static boolean allowed(final String className) {
        for (final String a : ALLOWED) {
            if (className.equals(a) || className.startsWith(a + "$")) {
                return true;
            }
        }
        return false;
    }

    /** Fails naming the first caller outside the allow list; returns the allowed callers found. */
    static Set<String> check(final List<NeverSchedulesScan.Finding> findings) {
        final Set<String> callers = new TreeSet<>();
        for (final NeverSchedulesScan.Finding f : findings) {
            if (!allowed(f.className())) {
                throw new AssertionError("purge call site outside ShadowRetention: " + f.className() + " ("
                        + f.reference() + ")");
            }
            callers.add(f.className() + " -> " + f.reference());
        }
        return callers;
    }

    static List<NeverSchedulesScan.Finding> mainFindings() {
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanDirectory(
                ShadowPackageScanTest.classesOf(ShadowCatalog.class), n -> n.startsWith(PKG), purgeRules());
        assertTrue("scanned " + report.classesScanned(), report.classesScanned() >= 60);
        return report.findings();
    }

    @Test
    public void onlyTheOwnerTheCatalogTheWriterAndStagingPurge() {
        final Set<String> callers = check(mainFindings());
        assertTrue(callers.toString(), callers.contains(PKG + "shadow/ShadowRetention -> "
                + "com/openexchange/cluster/shadow/ShadowCatalog.purge"));
        assertTrue(callers.toString(), callers.contains(PKG + "shadow/ShadowCatalog -> "
                + "io/aeron/archive/client/AeronArchive.purgeRecording"));
        assertTrue(callers.toString(), callers.contains(PKG + "shadow/ShadowWriter -> "
                + "io/aeron/archive/client/AeronArchive.purgeRecording"));
        assertTrue(callers.toString(), callers.contains(PKG + "StagingArchive -> "
                + "io/aeron/archive/client/AeronArchive.purgeRecording"));
        assertTrue(callers.toString(), callers.contains(PKG + "replica/RetrievedSetTrim -> "
                + "io/aeron/archive/client/AeronArchive.purgeRecording"));
        assertEquals(callers.toString(), 5, callers.size());
        ShadowRig.green("purge call sites: " + callers);
    }

    @Test
    public void aProbeInTheShadowPackageIsFlagged() {
        final List<NeverSchedulesScan.Finding> withProbe = new ArrayList<>(mainFindings());
        withProbe.addAll(NeverSchedulesScan.scanClass(PKG + "shadow/ForbiddenPurgeProbe",
                ShadowPackageScanTest.bytes(ForbiddenPurgeProbe.class), purgeRules()));
        ShadowRig.nc("S4-11.15 kit purge probe", "purge call site outside ShadowRetention: " + PKG
                + "shadow/ForbiddenPurgeProbe", () -> check(withProbe));
    }
}
