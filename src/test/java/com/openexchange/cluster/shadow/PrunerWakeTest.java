// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.SnapshotLogPruner;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static com.openexchange.cluster.shadow.FloorRig.P_1;
import static com.openexchange.cluster.shadow.FloorRig.P_2;
import static com.openexchange.cluster.shadow.FloorRig.P_CM;
import static com.openexchange.cluster.shadow.FloorRig.SAFE_SEQ;
import static com.openexchange.cluster.shadow.FloorRig.SEGMENT;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RCS S4 11.15 (MF-8, C2, C3, C4): the real {@link SnapshotLogPruner} thread over a real archive, with no consensus
 * module (its snapshot counter never moves), the {@link ShadowHousekeeping} hook on {@code onWake} and the floor's
 * request on {@code wake}. A VERIFIED shadow appearing without a CM snapshot triggers one purge pass within two polls;
 * the request is consumed by the pass (two wakes in one run); a shadow VERIFIED between a restore's scan and the first
 * wake is counted on the first wake. The poll interval is the production 5 s (unchanged by S4).
 */
public class PrunerWakeTest {

    static final long POLL_MS = TimeUnit.SECONDS.toMillis(SnapshotLogPruner.pollIntervalSeconds());

    /** One pruner over a rig, with the housekeeping hook and (unless {@code wakeOff}) the wake seam. */
    static final class Run implements AutoCloseable {
        final FloorRig rig;
        final ShadowCatalog catalog;
        final ShadowFloor floor;
        final ShadowRetention retention;
        final ShadowHousekeeping housekeeping;
        final SnapshotLogPruner pruner;

        Run(final FloorRig rig, final boolean wakeOff) {
            this.rig = rig;
            catalog = rig.rig.catalog();
            floor = rig.floor();
            retention = new ShadowRetention(catalog, floor, 10, rig.hk);
            housekeeping = new ShadowHousekeeping(catalog, retention, floor, SystemNanoClock.INSTANCE, 1_000, 500,
                    () -> { }, rig.hk);
            pruner = new SnapshotLogPruner(rig.clusterDir, new AeronArchive.Context()
                    .aeronDirectoryName(rig.rig.fixture.aeronDirectoryName())
                    .controlRequestChannel("aeron:ipc")
                    .controlResponseChannel("aeron:ipc"),
                    rig.rig.fixture.aeron().countersReader(), 0, () -> true)
                    .watermark(floor)
                    .onWake(housekeeping);
            if (!wakeOff) {
                pruner.wake(floor::purgeRequested);
            }
            assertTrue(pruner.watermarkIs(floor));
            assertFalse(pruner.watermarkIs(() -> 0L));
            pruner.start();
        }

        @Override
        public void close() {
            pruner.stop();
            awaitPrunerThreadGone();
            retention.close();
            rig.close();
        }
    }

    static void awaitPrunerThreadGone() {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && "snapshot-log-pruner".equals(t.getName()))) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("the pruner thread did not stop");
            }
            sleep(50);
        }
    }

    static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static boolean await(final BooleanSupplier condition, final long timeoutMs) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            sleep(100);
        }
        return condition.getAsBoolean();
    }

    /** Waits until the hook finished its startup pass and the pruner went quiet (no pass for a whole poll). */
    static long settle(final Run run) {
        assertTrue("startup pass", await(run.housekeeping::startupPassDone, 3 * POLL_MS));
        long passes = run.pruner.prunesRun();
        while (true) {
            sleep(POLL_MS + 500);
            final long now = run.pruner.prunesRun();
            if (now == passes) {
                return now;
            }
            passes = now;
        }
    }

    /** MF-8: a new VERIFIED shadow (no CM snapshot) is followed by one purge pass within two polls. */
    static String wakeOnNewVerified(final boolean wakeOff, final long windowMs) {
        final FloorRig rig = new FloorRig();
        rig.shadow(P_1, SAFE_SEQ);
        try (Run run = new Run(rig, wakeOff)) {
            final long before = settle(run);
            assertEquals("nothing to purge yet (no covering shadow)", 0, rig.logStart());
            rig.shadow(P_CM, SAFE_SEQ);
            final long written = System.nanoTime();
            if (!await(() -> run.pruner.prunesRun() > before, windowMs)) {
                fail("no purge within " + TimeUnit.MILLISECONDS.toSeconds(windowMs)
                        + " s after a new VERIFIED shadow (MF-8)");
            }
            final long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - written);
            assertTrue("one purge pass within two polls, took " + tookMs + " ms", tookMs <= 2 * POLL_MS + 1_000);
            assertEquals(before + 1, run.pruner.prunesRun());
            assertTrue(run.pruner.wakesByShadow() >= 1);
            assertEquals(P_1 / SEGMENT * SEGMENT, rig.logStart());
            assertEquals(P_1, run.floor.lastApplied());
            return "purge pass after " + tookMs + " ms, log start " + rig.logStart();
        }
    }

    @Test
    public void aNewVerifiedShadowWakesThePrunerWithoutACmSnapshot() {
        ShadowRig.green("11.15 wake: " + wakeOnNewVerified(false, 2 * POLL_MS + 1_000));
        nc("S4-11.15 wake seam off", "no purge within 15 s after a new VERIFIED shadow (MF-8)",
                () -> wakeOnNewVerified(true, 15_000));
    }

    @Test
    public void theRequestIsConsumedTwoWakesInOneRun() {
        final FloorRig rig = new FloorRig();
        rig.shadow(P_1, SAFE_SEQ);
        try (Run run = new Run(rig, false)) {
            final long base = settle(run);
            final long wakesBase = run.pruner.wakesByShadow();
            rig.shadow(P_CM, SAFE_SEQ);
            assertTrue(await(() -> run.pruner.prunesRun() == base + 1, 2 * POLL_MS + 1_000));
            final long firstStart = rig.logStart();
            assertEquals(P_1 / SEGMENT * SEGMENT, firstStart);
            sleep(2 * POLL_MS + 500);
            assertEquals("the request was consumed: no second pass without a new shadow", base + 1,
                    run.pruner.prunesRun());
            assertFalse(run.floor.purgeRequested());
            // second wake in the same run: P_2 VERIFIED raises the margin, so the floor rises to P_2
            rig.shadow(P_2, SAFE_SEQ);
            assertTrue(await(() -> run.pruner.prunesRun() == base + 2, 2 * POLL_MS + 1_000));
            assertEquals(P_2 / SEGMENT * SEGMENT, rig.logStart());
            sleep(POLL_MS + 500);
            assertEquals(base + 2, run.pruner.prunesRun());
            assertEquals(wakesBase + 2, run.pruner.wakesByShadow());
            ShadowRig.green("11.15 two wakes in one run: log start " + firstStart + " -> " + rig.logStart()
                    + ", passes " + run.pruner.prunesRun() + ", by shadow " + run.pruner.wakesByShadow());
        }
    }

    @Test
    public void aShadowVerifiedBetweenARestoreScanAndTheFirstWakeCountsOnTheFirstWake() {
        final FloorRig rig = new FloorRig();
        rig.shadow(P_1, SAFE_SEQ);
        rig.shadow(P_CM, SAFE_SEQ);
        // onStart's restore scan: both COMPLETE, none VERIFIED yet
        final ShadowCatalog restore = rig.rig.catalog();
        final ShadowCatalog.Scan atRestore = restore.scan();
        assertTrue(atRestore.verified().isEmpty());
        // the restore verified its candidates after that scan (process-lifetime verdicts are shared)
        for (final ShadowEntry e : atRestore.all()) {
            assertEquals(ShadowCatalog.DeepResult.Kind.VERIFIED, restore.verifyDeep(e).kind());
        }
        try (Run run = new Run(rig, false)) {
            assertTrue("first wake must run the purge the late VERIFIED shadows allow",
                    await(() -> run.pruner.prunesRun() >= 1, POLL_MS + 2_000));
            assertEquals(1, run.pruner.prunesRun());
            assertEquals(1, run.pruner.wakesByShadow());
            assertEquals("no deep read needed: the verdicts were already there", 0,
                    rig.hk.get(ShadowHousekeeping.Counter.DEEP_READS_TOTAL));
            assertEquals(P_1 / SEGMENT * SEGMENT, rig.logStart());
            ShadowRig.green("11.15 C2: VERIFIED between the restore scan and the first wake purged on the first wake");
        }
    }

    @Test
    public void aFloorWithoutAScanNeverRequests() {
        // the floor never requests while UNKNOWN: a fresh floor with no scan does not ask for a pass
        try (FloorRig rig = new FloorRig()) {
            final ShadowFloor floor = rig.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, Set.of());
            assertFalse(floor.purgeRequested());
            assertEquals(0, floor.getAsLong());
        }
    }
}
