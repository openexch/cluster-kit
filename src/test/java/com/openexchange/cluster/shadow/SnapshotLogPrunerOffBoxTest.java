// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.cluster.PrunerSeams;
import com.openexchange.cluster.SnapshotLogPruner;
import io.aeron.archive.client.AeronArchive;
import org.junit.Test;

import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static com.openexchange.cluster.shadow.FloorRig.LOG_BYTES;
import static com.openexchange.cluster.shadow.FloorRig.P_CM;
import static com.openexchange.cluster.shadow.FloorRig.SEGMENT;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The real {@link SnapshotLogPruner} thread over a real archive with 64 KiB segments, a log of {@link FloorRig#LOG_BYTES}
 * bytes and a snapshot at {@link FloorRig#P_CM}, bounded by an off-box watermark written with the tool. The log start
 * is read from the archive. A purge pass is requested through the pruner's wake seam (there is no consensus module
 * here to take a snapshot); what moves the log start after a held pass is only the watermark file.
 */
public class SnapshotLogPrunerOffBoxTest {

    private static final long POLL_MS = 100;
    private static final long RECHECK_MAX_MS = 400;
    private static final long GENESIS = 17;

    /** A pruner over {@code rig} with an on-demand purge request, started. */
    private static final class Member implements AutoCloseable {
        final FloorRig rig;
        final SnapshotLogPruner pruner;
        final AtomicBoolean requestPass = new AtomicBoolean();
        final String dir;

        Member(final FloorRig rig, final OffBoxWatermark.Mode mode, final boolean offBoxOff,
               final boolean retentionGivenUp) {
            this.rig = rig;
            this.dir = rig.clusterDir.getAbsolutePath();
            final SnapshotLogPruner p = new SnapshotLogPruner(rig.clusterDir,
                    new AeronArchive.Context()
                            .aeronDirectoryName(rig.rig.fixture.aeronDirectoryName())
                            .controlRequestChannel("aeron:ipc")
                            .controlResponseChannel("aeron:ipc"),
                    rig.rig.fixture.aeron().countersReader(), 0, () -> true)
                    .wake(() -> requestPass.getAndSet(false));
            if (retentionGivenUp) {
                PrunerSeams.retentionFloorGivenUp(p);
            }
            pruner = PrunerSeams.offBox(p, new OffBoxWatermark(rig.clusterDir, "core", 0, () -> GENESIS, mode,
                    OffBoxWatermark.DEFAULT_ALARM_BYTES, OffBoxWatermark.DEFAULT_STALE_MS), () -> LOG_BYTES, POLL_MS,
                    RECHECK_MAX_MS, offBoxOff);
            pruner.start();
        }

        void set(final long position) {
            PrunerSeams.tool("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0", "--genesis-id",
                    String.valueOf(GENESIS), "--log-position-bytes", String.valueOf(position), "--source", "test");
        }

        /** Requests one purge pass and waits until it has run. */
        void pass() throws InterruptedException {
            final long before = pruner.prunesRun();
            requestPass.set(true);
            await(() -> pruner.prunesRun() > before, "a purge pass");
        }

        void assertNotPast(final long x) {
            final long start = rig.logStart();
            if (start > x) {
                throw new AssertionError("purged past the off-box position: log start " + start + " is above " + x);
            }
        }

        @Override
        public void close() {
            pruner.stop();
        }
    }

    private static long base(final long position) {
        return position - position % SEGMENT;
    }

    private static void await(final BooleanSupplier done, final String what) throws InterruptedException {
        final long deadline = System.nanoTime() + 20_000_000_000L;
        while (!done.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(20);
        }
    }

    private static void story(final boolean offBoxOff) throws Exception {
        try (FloorRig rig = new FloorRig(); Member m = new Member(rig, OffBoxWatermark.Mode.REQUIRED, offBoxOff,
                false)) {
            // no file yet: a lost or never-written watermark holds everything
            m.pass();
            m.assertNotPast(0);
            assertTrue(String.valueOf(m.pruner.lastError()), m.pruner.lastError().contains("off-box watermark ABSENT"));

            // X = 300000: the purge goes to the segment below X, never past it
            final long x1 = 300_000;
            m.set(x1);
            await(() -> rig.logStart() == base(x1), "the purge behind the first watermark");
            m.assertNotPast(x1);
            assertNull(m.pruner.lastError());
            m.pass();
            m.assertNotPast(x1);

            // X rises a moment after a held pass: the re-check purges with no new pass requested
            final long passes = m.pruner.prunesRun();
            final long rechecks = m.pruner.wakesByRecheck();
            Thread.sleep(3 * RECHECK_MAX_MS);
            assertEquals("nothing moved: the re-check only reads", passes, m.pruner.prunesRun());
            final long x2 = 600_000;
            m.set(x2);
            await(() -> rig.logStart() == base(x2), "the purge behind the risen watermark");
            assertTrue("a re-check ran the pass", m.pruner.wakesByRecheck() > rechecks);
            m.assertNotPast(x2);

            // the file is deleted mid-run: the next pass holds where it is and says so
            Files.delete(OffBoxWatermark.file(rig.clusterDir));
            m.pass();
            assertEquals("the log start did not move", base(x2), rig.logStart());
            assertTrue(String.valueOf(m.pruner.lastError()), m.pruner.lastError().contains("off-box watermark ABSENT"));
            assertEquals(OffBoxWatermark.State.ABSENT, m.pruner.offBoxWatermark().state());

            // a watermark above the commit position holds as well
            m.set(LOG_BYTES + 1);
            m.pass();
            assertEquals(base(x2), rig.logStart());
            assertEquals(OffBoxWatermark.State.AHEAD, m.pruner.offBoxWatermark().state());

            // corrected down to the snapshot position: the purge reaches this node's own snapshot, the error clears
            PrunerSeams.tool("set", "--cluster-dir", m.dir, "--engine", "core", "--cluster-id", "0", "--genesis-id",
                    String.valueOf(GENESIS), "--log-position-bytes", String.valueOf(P_CM), "--source", "test",
                    "--allow-lower");
            await(() -> rig.logStart() == base(P_CM), "the purge after the watermark came back");
            await(() -> m.pruner.lastError() == null, "the error to clear");
            assertEquals(P_CM, m.pruner.offBoxWatermark().positionBytes());
        }
    }

    @Test
    public void thePurgeNeverPassesTheWatermarkAndFollowsItWhenItRises() throws Exception {
        story(false);
        ShadowRig.green("off-box: held with no file, followed X, held on a deleted file and an AHEAD one");
        nc("off-box bound off", "purged past the off-box position", () -> {
            try {
                story(true);
            } catch (final AssertionError red) {
                throw red;
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Test
    public void reportSaysTheFloorAndDoesNotHold() throws Exception {
        try (FloorRig rig = new FloorRig(); Member m = new Member(rig, OffBoxWatermark.Mode.REPORT, false, false)) {
            m.set(300_000);
            m.pass();
            assertEquals("report does not hold: the purge goes to this node's snapshot", base(P_CM), rig.logStart());
            assertEquals(OffBoxWatermark.State.VALID, m.pruner.offBoxWatermark().state());
            assertEquals(300_000, m.pruner.offBoxWatermark().positionBytes());
            assertEquals("what required would hold", P_CM - 300_000, m.pruner.offBoxWatermark().heldBytes());
            assertNull(m.pruner.lastError());
        }
    }

    /** The retention floor gives up on a member after its wait; that give-up must not release the off-box hold. */
    @Test
    public void theMemberWaitDoesNotReleaseTheOffBoxHold() throws Exception {
        try (FloorRig rig = new FloorRig(); Member m = new Member(rig, OffBoxWatermark.Mode.REQUIRED, false, true)) {
            final long x = 300_000;
            m.set(x);
            m.pass();
            m.pass();
            assertEquals("the retention floor has given up", Long.MAX_VALUE, m.pruner.lastAppliedFloor());
            assertTrue(m.pruner.lastWatermarkReport(), m.pruner.lastWatermarkReport().contains("bound passed"));
            assertEquals("the off-box floor still holds", base(x), rig.logStart());
            m.assertNotPast(x);
        }
    }
}
