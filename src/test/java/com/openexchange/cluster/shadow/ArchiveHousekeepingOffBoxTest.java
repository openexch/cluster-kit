// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.cluster.PrunerSeams;
import org.junit.Test;

import static com.openexchange.cluster.shadow.FloorRig.LOG_BYTES;
import static com.openexchange.cluster.shadow.FloorRig.P_CM;
import static com.openexchange.cluster.shadow.FloorRig.SEGMENT;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;

/**
 * The housekeeping command's purge obeys the member's off-box watermark file like the engine's own pruner: there is
 * no purge path around it. A real archive with 64 KiB segments and a snapshot at {@link FloorRig#P_CM}.
 */
public class ArchiveHousekeepingOffBoxTest {

    private static final long X = 300_000;

    private static OffBoxWatermark required(final FloorRig rig, final long genesis) {
        return new OffBoxWatermark(rig.clusterDir, "core", 0, () -> genesis, OffBoxWatermark.Mode.REQUIRED,
                OffBoxWatermark.DEFAULT_ALARM_BYTES, OffBoxWatermark.DEFAULT_STALE_MS);
    }

    private static void set(final FloorRig rig, final long position) {
        PrunerSeams.tool("set", "--cluster-dir", rig.clusterDir.getAbsolutePath(), "--engine", "core",
                "--cluster-id", "0", "--genesis-id", "17", "--log-position-bytes", String.valueOf(position),
                "--source", "test");
    }

    private static void run(final boolean offBoxOff) {
        try (FloorRig rig = new FloorRig()) {
            set(rig, X);
            PrunerSeams.housekeep(rig.clusterDir, rig.archive(), Long.MAX_VALUE, required(rig, 17), LOG_BYTES,
                    offBoxOff);
            final long start = rig.logStart();
            if (start > X) {
                throw new AssertionError("the command purged past the off-box position: log start " + start
                        + " is above " + X);
            }
            assertEquals(X - X % SEGMENT, start);
        }
    }

    @Test
    public void theCommandNeverPurgesPastTheWatermark() {
        run(false);
        nc("command without the evaluation", "the command purged past the off-box position", () -> run(true));
    }

    @Test
    public void theCommandHoldsOnEveryDoubt() {
        try (FloorRig rig = new FloorRig()) {
            // no file
            PrunerSeams.housekeep(rig.clusterDir, rig.archive(), Long.MAX_VALUE, required(rig, 17), LOG_BYTES, false);
            assertEquals("no file: nothing purged", 0, rig.logStart());
            // a file for another genesis
            set(rig, X);
            PrunerSeams.housekeep(rig.clusterDir, rig.archive(), Long.MAX_VALUE, required(rig, 18), LOG_BYTES, false);
            assertEquals("foreign: nothing purged", 0, rig.logStart());
            // the commit position cannot be read
            PrunerSeams.housekeep(rig.clusterDir, rig.archive(), Long.MAX_VALUE, required(rig, 17), -1, false);
            assertEquals("no commit position: nothing purged", 0, rig.logStart());
            // the command was not told which member this is: the genesis is unknown
            final OffBoxWatermark unknown = PrunerSeams.housekeepingOffBox(rig.clusterDir,
                    rig.clusterDir.getAbsolutePath(), "/aeron");
            assertEquals(OffBoxWatermark.State.UNBOUND, OffBoxWatermark.evaluate(OffBoxWatermark.Mode.REQUIRED,
                    OffBoxWatermark.read(rig.clusterDir), "core", 0, 0, LOG_BYTES).state);
            assertEquals(0, OffBoxWatermark.evaluate(OffBoxWatermark.Mode.REQUIRED,
                    OffBoxWatermark.read(rig.clusterDir), "core", 0, 0, LOG_BYTES).floor);
            // told, it matches the file
            final OffBoxWatermark told = PrunerSeams.housekeepingOffBox(rig.clusterDir,
                    rig.clusterDir.getAbsolutePath(), "/aeron", "--offbox-engine=core", "--offbox-cluster-id=0",
                    "--offbox-genesis-id=17");
            // this JVM has no RETENTION_OFFBOX, so both are in mode off here; the identity is what is under test
            assertEquals(OffBoxWatermark.Mode.OFF, unknown.mode());
            assertEquals(OffBoxWatermark.Mode.OFF, told.mode());
            // the caller's own watermark and the off-box one: the lower wins, in both orders
            PrunerSeams.housekeep(rig.clusterDir, rig.archive(), 100_000, required(rig, 17), LOG_BYTES, false);
            assertEquals(100_000 - 100_000 % SEGMENT, rig.logStart());
            PrunerSeams.housekeep(rig.clusterDir, rig.archive(), P_CM, required(rig, 17), LOG_BYTES, false);
            assertEquals(X - X % SEGMENT, rig.logStart());
        }
    }
}
