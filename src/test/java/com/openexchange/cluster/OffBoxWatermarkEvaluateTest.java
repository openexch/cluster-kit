// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import com.openexchange.cluster.OffBoxWatermark.Evaluation;
import com.openexchange.cluster.OffBoxWatermark.Mode;
import com.openexchange.cluster.OffBoxWatermark.State;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Every row of the evaluation table in each mode, the release arithmetic, and the lines of a purge pass. The member
 * here is engine core, cluster 0, genesis 17, with its commit position at {@link #COMMIT}.
 */
public class OffBoxWatermarkEvaluateTest {

    private static final long COMMIT = 1_000_000;
    private static final long GENESIS = 17;
    private File clusterDir;

    @Before
    public void setUp() throws Exception {
        clusterDir = Files.createTempDirectory("offbox-eval-").toFile();
    }

    @After
    public void tearDown() {
        OffBoxWatermark.absentIsNoConstraintForTest = false;
        OffBoxWatermark.aheadCheckOffForTest = false;
        org.agrona.IoUtil.delete(clusterDir, true);
    }

    private Evaluation evaluate(final Mode mode, final long genesis, final long commit) {
        return OffBoxWatermark.evaluate(mode, OffBoxWatermark.read(clusterDir), "core", 0, genesis, commit);
    }

    private void write(final String engine, final int clusterId, final long genesis, final long position,
                       final long released) throws Exception {
        OffBoxWatermark.write(clusterDir, new OffBoxWatermark.Record(engine, clusterId, genesis, position, released,
            released < 0 ? "" : "operator said so", 1, 1_000, "archiver"));
    }

    private int tool(final String... args) {
        return OffBoxWatermarkTool.run(args, new PrintStream(new ByteArrayOutputStream()),
            new PrintStream(new ByteArrayOutputStream()), () -> 2_000);
    }

    private static void assertHolds(final String what, final Evaluation e, final State state) {
        assertEquals(what, state, e.state);
        if (e.floor != 0) {
            throw new AssertionError(what + ": " + state + " must hold everything (floor 0), got floor "
                + OffBoxWatermark.describe(e.floor));
        }
    }

    @Test
    public void offModeNeverConstrains() throws Exception {
        Evaluation e = evaluate(Mode.OFF, GENESIS, COMMIT);
        assertEquals(State.OFF, e.state);
        assertEquals(OffBoxWatermark.NO_CONSTRAINT, e.floor);
        assertEquals(OffBoxWatermark.NO_CONSTRAINT, e.appliedFloor());
        assertFalse(e.holdsAsError());

        write("core", 0, GENESIS, 4096, -1); // the file appears after boot
        e = evaluate(Mode.OFF, GENESIS, COMMIT);
        assertEquals(State.IGNORED, e.state);
        assertEquals(OffBoxWatermark.NO_CONSTRAINT, e.appliedFloor());

        Files.writeString(OffBoxWatermark.file(clusterDir), "garbage"); // even an unreadable one is only ignored
        assertEquals(State.IGNORED, evaluate(Mode.OFF, GENESIS, COMMIT).state);
    }

    /** required and report compute the same table; only required applies it. */
    @Test
    public void everyRowUnderRequiredAndReport() throws Exception {
        for (final Mode mode : new Mode[] {Mode.REQUIRED, Mode.REPORT}) {
            Files.deleteIfExists(OffBoxWatermark.file(clusterDir));
            assertAbsentHolds(evaluate(mode, GENESIS, COMMIT));

            Files.createDirectories(OffBoxWatermark.file(clusterDir).getParent());
            Files.writeString(OffBoxWatermark.file(clusterDir), "format=offbox-watermark/1\n");
            assertHolds(mode + " invalid", evaluate(mode, GENESIS, COMMIT), State.INVALID);

            write("core", 0, GENESIS, 4096, -1);
            assertHolds(mode + " no recorded genesis", evaluate(mode, 0, COMMIT), State.UNBOUND);
            assertHolds(mode + " other genesis", evaluate(mode, GENESIS + 1, COMMIT), State.FOREIGN);
            write("match", 0, GENESIS, 4096, -1);
            assertHolds(mode + " other engine", evaluate(mode, GENESIS, COMMIT), State.FOREIGN);
            write("core", 1, GENESIS, 4096, -1);
            assertHolds(mode + " other cluster id", evaluate(mode, GENESIS, COMMIT), State.FOREIGN);

            write("core", 0, GENESIS, COMMIT + 1, -1);
            assertAheadHolds(evaluate(mode, GENESIS, COMMIT));
            write("core", 0, GENESIS, 4096, -1);
            assertHolds(mode + " commit position unreadable", evaluate(mode, GENESIS, -1), State.AHEAD);

            write("core", 0, GENESIS, COMMIT, -1); // at the commit position is not above it
            Evaluation e = evaluate(mode, GENESIS, COMMIT);
            assertEquals(State.VALID, e.state);
            assertEquals(COMMIT, e.floor);
            assertNull(e.detail);

            write("core", 0, GENESIS, 4096, -1);
            e = evaluate(mode, GENESIS, COMMIT);
            assertEquals(State.VALID, e.state);
            assertEquals(4096, e.floor);
            assertEquals(mode == Mode.REQUIRED ? 4096 : OffBoxWatermark.NO_CONSTRAINT, e.appliedFloor());
            assertFalse(e.holdsAsError());
        }
        // only required turns a hold into the pruner's error
        Files.deleteIfExists(OffBoxWatermark.file(clusterDir));
        assertTrue(evaluate(Mode.REQUIRED, GENESIS, COMMIT).holdsAsError());
        assertFalse(evaluate(Mode.REPORT, GENESIS, COMMIT).holdsAsError());
        assertEquals(0, evaluate(Mode.REQUIRED, GENESIS, COMMIT).appliedFloor());
        assertEquals(OffBoxWatermark.NO_CONSTRAINT, evaluate(Mode.REPORT, GENESIS, COMMIT).appliedFloor());
    }

    private static void assertAbsentHolds(final Evaluation e) {
        assertEquals(State.ABSENT, e.state);
        if (e.floor != 0) {
            throw new AssertionError("ABSENT under " + e.mode.text() + " was read as no constraint: floor "
                + OffBoxWatermark.describe(e.floor) + ", a lost file must hold everything");
        }
    }

    private static void assertAheadHolds(final Evaluation e) {
        if (e.state != State.AHEAD || e.floor != 0) {
            throw new AssertionError("a watermark above the commit position returned a floor: state " + e.state
                + " floor " + OffBoxWatermark.describe(e.floor));
        }
    }

    @Test
    public void negativeControlAbsentReadAsNoConstraint() throws Exception {
        OffBoxWatermark.absentIsNoConstraintForTest = true;
        try {
            assertAbsentHolds(evaluate(Mode.REQUIRED, GENESIS, COMMIT));
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("ABSENT under required"));
            return;
        }
        fail("negative control did not fail: the assertion cannot see the ABSENT rule");
    }

    @Test
    public void negativeControlAheadCheckOff() throws Exception {
        write("core", 0, GENESIS, COMMIT + 1, -1);
        OffBoxWatermark.aheadCheckOffForTest = true;
        try {
            assertAheadHolds(evaluate(Mode.REQUIRED, GENESIS, COMMIT));
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(),
                expected.getMessage().contains("a watermark above the commit position returned a floor"));
            return;
        }
        fail("negative control did not fail: the assertion cannot see the AHEAD rule");
    }

    /** release raises the floor to max(X, P); a set at or above P clears it; twice, with nothing remembered. */
    @Test
    public void releaseArithmeticAndItsResetTwice() {
        final String dir = clusterDir.getAbsolutePath();
        assertEquals(0, tool("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0", "--genesis-id",
            "17", "--log-position-bytes", "1000", "--source", "archiver"));
        long x = 1000;
        for (int round = 1; round <= 2; round++) {
            final long p = x + 5000;
            assertEquals(x, evaluate(Mode.REQUIRED, GENESIS, COMMIT).floor);
            assertEquals(0, tool("release", "--cluster-dir", dir, "--genesis-id", "17",
                "--up-to-log-position-bytes", String.valueOf(p), "--reason", "round " + round));
            Evaluation e = evaluate(Mode.REQUIRED, GENESIS, COMMIT);
            assertEquals(State.VALID, e.state);
            assertEquals("round " + round + ": the floor is max(X, P)", p, e.floor);
            assertEquals(p, e.record.releasedUpToBytes);
            assertEquals("round " + round, e.record.releaseReason);

            // a set below P keeps the release
            assertEquals(0, tool("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
                "--genesis-id", "17", "--log-position-bytes", String.valueOf(x + 100), "--source", "archiver"));
            e = evaluate(Mode.REQUIRED, GENESIS, COMMIT);
            assertEquals(p, e.floor);
            assertEquals(x + 100, e.record.logPositionBytes);

            // a set at P clears it
            assertEquals(0, tool("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
                "--genesis-id", "17", "--log-position-bytes", String.valueOf(p), "--source", "archiver"));
            e = evaluate(Mode.REQUIRED, GENESIS, COMMIT);
            assertEquals(p, e.floor);
            assertEquals("round " + round + ": the release is cleared", OffBoxWatermark.NO_RELEASE,
                e.record.releasedUpToBytes);
            assertEquals("", e.record.releaseReason);
            x = p;
        }
    }

    @Test
    public void theLinesAndGaugesOfAPass() throws Exception {
        final List<String> out = new ArrayList<>();
        final List<String> err = new ArrayList<>();
        final OffBoxWatermark required = new OffBoxWatermark(clusterDir, "core", 0, () -> GENESIS, Mode.REQUIRED,
            10_000, 60_000);

        // absent: held everything below the other floor, CRITICAL, alarm
        Evaluation e = required.evaluate(COMMIT, 5_000);
        required.reportPass(e, 500_000, 5_000, out::add, err::add);
        assertEquals("[RETENTION] off-box: floor 0 state ABSENT position -1 seq -1 age -1 source -", out.get(0));
        assertTrue(err.toString(), err.get(0).contains("CRITICAL: off-box watermark ABSENT"));
        assertTrue(err.toString(), err.stream().anyMatch(l -> l.contains("500000 bytes of log are held")));
        assertEquals(State.ABSENT.code(), OffBoxWatermark.Gauge.STATE.read(required));
        assertEquals(2, OffBoxWatermark.Gauge.MODE.read(required));
        assertEquals(-1, OffBoxWatermark.Gauge.POSITION_BYTES.read(required));
        assertEquals(500_000, OffBoxWatermark.Gauge.HELD_BYTES.read(required));
        assertEquals(-1, OffBoxWatermark.Gauge.AGE_MS.read(required));

        // valid and fresh, little held: one line, nothing critical
        out.clear();
        err.clear();
        write("core", 0, GENESIS, 495_000, -1);
        e = required.evaluate(COMMIT, 5_000);
        required.reportPass(e, 500_000, 5_000, out::add, err::add);
        assertEquals("[RETENTION] off-box: floor 495000 state VALID position 495000 seq 1 age 4000 source archiver",
            out.get(0));
        assertTrue(err.toString(), err.isEmpty());
        assertEquals(0, OffBoxWatermark.Gauge.STATE.read(required));
        assertEquals(495_000, OffBoxWatermark.Gauge.POSITION_BYTES.read(required));
        assertEquals(5_000, OffBoxWatermark.Gauge.HELD_BYTES.read(required));
        assertEquals(4_000, OffBoxWatermark.Gauge.AGE_MS.read(required));
        assertEquals(-1, OffBoxWatermark.Gauge.RELEASED_BYTES.read(required));

        // stale: the age alarm, and the floor does not change
        e = required.evaluate(COMMIT, 100_000);
        required.reportPass(e, 500_000, 100_000, out::add, err::add);
        assertEquals(495_000, e.floor);
        assertTrue(err.toString(), err.stream().anyMatch(l -> l.contains("was written 99000 ms ago")));

        // released above the durable position: the operator's line on every pass
        err.clear();
        write("core", 0, GENESIS, 495_000, 499_000);
        e = required.evaluate(COMMIT, 5_000);
        required.reportPass(e, 500_000, 5_000, out::add, err::add);
        assertEquals("[RETENTION] CRITICAL: off-box hold released by operator up to 499000 (durable 495000): log in"
            + " [495000, 499000) may be purged without an off-box copy; reason: operator said so", err.get(0));
        assertEquals(499_000, OffBoxWatermark.Gauge.RELEASED_BYTES.read(required));
        assertEquals(1_000, OffBoxWatermark.Gauge.HELD_BYTES.read(required));

        // report: the same floor, said and not applied
        out.clear();
        final OffBoxWatermark report = new OffBoxWatermark(clusterDir, "core", 0, () -> GENESIS, Mode.REPORT, 10_000,
            60_000);
        e = report.evaluate(COMMIT, 5_000);
        report.reportPass(e, 500_000, 5_000, out::add, err::add);
        assertTrue(out.toString(), out.contains(
            "[RETENTION] RETENTION_OFFBOX=report: the off-box floor 499000 (VALID) is not applied"));
        assertEquals(OffBoxWatermark.NO_CONSTRAINT, e.appliedFloor());

        // off with no file: silent, as before the watermark existed
        out.clear();
        err.clear();
        Files.delete(OffBoxWatermark.file(clusterDir));
        final OffBoxWatermark off = new OffBoxWatermark(clusterDir, "core", 0, () -> GENESIS, Mode.OFF, 10_000, 60_000);
        off.reportPass(off.evaluate(COMMIT, 5_000), 500_000, 5_000, out::add, err::add);
        assertTrue(out.isEmpty() && err.isEmpty());
        assertEquals(State.OFF.code(), OffBoxWatermark.Gauge.STATE.read(off));
        assertEquals(State.OFF.code(), OffBoxWatermark.Gauge.STATE.read(null));
    }

    @Test
    public void settingsAreParsedAndRefused() throws Exception {
        assertEquals(Mode.OFF, OffBoxWatermark.parseMode(null, null));
        assertEquals(Mode.OFF, OffBoxWatermark.parseMode("", ""));
        assertEquals(Mode.REQUIRED, OffBoxWatermark.parseMode("required", "report"));
        assertEquals(Mode.REPORT, OffBoxWatermark.parseMode(null, "report"));
        try {
            OffBoxWatermark.parseMode("on", null);
            fail("an unknown mode must refuse");
        } catch (final IllegalArgumentException refused) {
            assertEquals("RETENTION_OFFBOX must be off, report or required, got 'on'", refused.getMessage());
        }
        // off with a file present refuses the start; with no file, or in another mode, it does not
        assertNull(OffBoxWatermark.offWithFileRefusal(Mode.OFF, clusterDir));
        write("core", 0, GENESIS, 4096, -1);
        assertEquals("RETENTION_OFFBOX=off but " + OffBoxWatermark.file(clusterDir) + " exists: set"
            + " RETENTION_OFFBOX=required, or remove the file with OffBoxWatermarkTool clear",
            OffBoxWatermark.offWithFileRefusal(Mode.OFF, clusterDir));
        assertNull(OffBoxWatermark.offWithFileRefusal(Mode.REQUIRED, clusterDir));
        assertNull(OffBoxWatermark.offWithFileRefusal(Mode.REPORT, clusterDir));
        try {
            OffBoxWatermark.checkBoot(clusterDir); // this JVM has no RETENTION_OFFBOX: off
            fail("off with a file present must refuse the start");
        } catch (final IllegalArgumentException refused) {
            assertTrue(refused.getMessage(), refused.getMessage().startsWith("RETENTION_OFFBOX=off but "));
        }
        final String key = OffBoxWatermark.ALARM_BYTES_PROP;
        System.setProperty(key, "12");
        try {
            SnapshotLogPruner.checkSettings();
            fail("an alarm threshold out of range must refuse the start");
        } catch (final IllegalArgumentException refused) {
            assertTrue(refused.getMessage(), refused.getMessage().contains(key));
        } finally {
            System.clearProperty(key);
        }
    }
}
