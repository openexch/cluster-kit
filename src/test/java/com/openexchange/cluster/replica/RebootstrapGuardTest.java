// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.shadow.LogicalShadow;
import com.openexchange.cluster.shadow.ShadowEntry;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowKind;
import com.openexchange.cluster.shadow.ShadowPuller;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The loop guard ages out of its window, refuses past the bound, and the move-aside keeps the newest dirs only. The
 * detector's pure rules: which local shadow qualifies for a re-bootstrap, the not-taken text, the refusal prefix.
 */
public class RebootstrapGuardTest {

    @Test
    public void theLoopGuardRefusesPastItsBoundAndAgesOut() throws Exception {
        final File base = Files.createTempDirectory("rebootstrap-guard-").toFile();
        try {
            final long window = 3_600_000;
            long now = 1_000_000_000L;
            for (int i = 0; i < 3; i++) {
                assertNull("re-bootstrap " + (i + 1) + " allowed", RebootstrapGuard.check(base, now, 3, window));
                RebootstrapGuard.record(base, now, window);
                now += 60_000;
            }
            assertEquals("re-bootstrap loop: 4 in 3600000 ms; members prune faster than this replica catches up",
                    RebootstrapGuard.check(base, now, 3, window));
            // the window ages the first one out
            final long later = 1_000_000_000L + window + 1;
            assertNull(RebootstrapGuard.check(base, later, 3, window));
            RebootstrapGuard.record(base, later, window);
            assertEquals(3, RebootstrapGuard.recent(base, later, window).size());
            Files.writeString(new File(base, RebootstrapGuard.FILE).toPath(), "garbage\n");
            try {
                RebootstrapGuard.check(base, later, 3, window);
                throw new AssertionError("a garbled marker was read");
            } catch (final IllegalStateException loud) {
                assertTrue(loud.getMessage(), loud.getMessage().contains("remove the file to reset the loop guard"));
            }
        } finally {
            IoUtil.delete(base, true);
        }
    }

    @Test
    public void theMoveKeepsTheNewestDirsAndNeverDeletesCluster() throws Exception {
        final File base = Files.createTempDirectory("rebootstrap-move-").toFile();
        try {
            final File cluster = new File(base, "cluster");
            for (int i = 0; i < 4; i++) {
                assertTrue(cluster.mkdirs());
                Files.writeString(new File(cluster, "cluster-mark.dat").toPath(), "mark " + i);
                final String dir = RebootstrapGuard.dirName(1_700_000_000_000L + i * 1_000L);
                assertTrue(dir, dir.startsWith("cluster.rebootstrap-2023"));
                assertTrue(RebootstrapGuard.moveAside(cluster, dir));
                assertFalse(cluster.exists());
                assertTrue(new File(new File(base, dir), "cluster-mark.dat").exists());
                RebootstrapGuard.pruneMoved(base, 2);
            }
            final String[] left = base.list((d, n) -> n.startsWith(RebootstrapGuard.MOVED_PREFIX));
            java.util.Arrays.sort(left);
            assertEquals(List.of(RebootstrapGuard.dirName(1_700_000_002_000L),
                    RebootstrapGuard.dirName(1_700_000_003_000L)), List.of(left));
            assertFalse("no cluster/ to move", RebootstrapGuard.moveAside(cluster, "cluster.rebootstrap-x"));
        } finally {
            IoUtil.delete(base, true);
        }
    }

    private static final long GENESIS = 7_000_001L;

    private static LogicalShadow shadow(final long genesis, final long p, final ShadowKind kind,
                                        final ShadowEntry.State state, final boolean conflict) {
        final ShadowHeader h = new ShadowHeader(ShadowHeader.ENGINE_CORE, 0, kind.code(), 1, 3, 1, genesis, p, 0, 0,
                1_000, 0, 0, 64, 0, 64, 0);
        final ShadowEntry e = new ShadowEntry(1, p, 0, 256, "aeron:ipc", null, h, null, 0, null, state, null, -1);
        return new LogicalShadow(ShadowHeader.ENGINE_CORE, 0, genesis, p, List.of(e), conflict);
    }

    @Test
    public void onlyAVerifiedLocalShadowOfThisGenesisAboveLStopQualifies() {
        final long lStop = 65_536;
        final Set<ShadowKind> aligned = Set.of(ShadowKind.ALIGNED);
        final LogicalShadow at = shadow(GENESIS, lStop, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED, false);
        final LogicalShadow below = shadow(GENESIS, lStop - 4096, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED,
                false);
        assertNull("a shadow at L.stop never qualifies", ReplicaFollower.newestQualifying(List.of(at, below), GENESIS,
                lStop, aligned));
        final LogicalShadow above = shadow(GENESIS, lStop + 4096, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED,
                false);
        final LogicalShadow newer = shadow(GENESIS, lStop + 8192, ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED,
                false);
        assertEquals(lStop + 8192, ReplicaFollower.newestQualifying(List.of(above, newer, at), GENESIS, lStop,
                aligned).logPosition());
        assertNull("another genesis", ReplicaFollower.newestQualifying(List.of(shadow(GENESIS + 1, lStop + 4096,
                ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED, false)), GENESIS, lStop, aligned));
        assertNull("COMPLETE is not VERIFIED", ReplicaFollower.newestQualifying(List.of(shadow(GENESIS, lStop + 4096,
                ShadowKind.ALIGNED, ShadowEntry.State.COMPLETE, false)), GENESIS, lStop, aligned));
        assertNull("conflict", ReplicaFollower.newestQualifying(List.of(shadow(GENESIS, lStop + 4096,
                ShadowKind.ALIGNED, ShadowEntry.State.VERIFIED, true)), GENESIS, lStop, aligned));
        final List<LogicalShadow> standby = List.of(shadow(GENESIS, lStop + 4096, ShadowKind.STANDBY,
                ShadowEntry.State.VERIFIED, false));
        assertNull("a kind the pull settings do not accept", ReplicaFollower.newestQualifying(standby, GENESIS, lStop,
                aligned));
        assertEquals(lStop + 4096, ReplicaFollower.newestQualifying(standby, GENESIS, lStop,
                Set.of(ShadowKind.ALIGNED, ShadowKind.STANDBY)).logPosition());
    }

    @Test
    public void theNotTakenTextAndTheRefusalPrefixArePlain() {
        final ShadowPuller.Report none = new ShadowPuller.Report(ShadowPuller.Report.Kind.NONE, 3, 0, 0, "none",
                Set.of(), null, null);
        assertEquals("re-bootstrap not taken: no source lists a shadow of kind ALIGNED above L.stop=65536",
                ReplicaFollower.notTakenText(65_536, Set.of(ShadowKind.ALIGNED), none));
        final ShadowPuller.Report refused = new ShadowPuller.Report(ShadowPuller.Report.Kind.NONE, 3, 2, 2, "CORRUPT",
                Set.of(), null, null);
        assertEquals("re-bootstrap not taken: 2 candidates of kind ALIGNED above L.stop=65536, none pulled and"
                + " VERIFIED",
                ReplicaFollower.notTakenText(65_536, Set.of(ShadowKind.ALIGNED), refused));
        final String loop = ReplicaFollower.rebootstrapRefusal("re-bootstrap loop: 4 in 3600000 ms; members prune"
                + " faster than this replica catches up", ReplicaFollower.RUNBOOK_LOOP);
        assertTrue(loop, loop.startsWith("FATAL replica: re-bootstrap refused: re-bootstrap loop: 4 in 3600000 ms;"));
        assertTrue(loop, loop.contains(". Runbook: "));
        assertFalse("not the bootstrap runbook", loop.contains("no local shadow and the cluster log starts at"));
        final String sources = ReplicaFollower.rebootstrapRefusal("re-bootstrap needs SHADOW_PULL_SOURCES",
                ReplicaFollower.RUNBOOK_NO_SOURCES);
        assertTrue(sources, sources.startsWith("FATAL replica: re-bootstrap refused: re-bootstrap needs"
                + " SHADOW_PULL_SOURCES. Runbook: set SHADOW_PULL_SOURCES"));
    }
}
