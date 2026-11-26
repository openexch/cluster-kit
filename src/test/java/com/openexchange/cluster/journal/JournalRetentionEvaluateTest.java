// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.cluster.journal.JournalRetention.Mode;
import com.openexchange.cluster.journal.JournalRetention.Report;
import com.openexchange.cluster.journal.JournalRetention.State;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Every row of the evaluation table in each mode, for a journal with an epoch (order) and one without (money), on an
 * in-memory archive: one active recording holding journalSeq 1..160 in ten segments of sixteen records. Each hold
 * purges nothing, says so, and is left by the next pass once its cause is gone. The member is engine core, cluster
 * 0, genesis 17.
 */
public class JournalRetentionEvaluateTest {

    private static final long GENESIS = JournalRig.GENESIS;
    private static final int SEG = FakeJournalArchive.SEG;

    private File clusterDir;
    private FakeJournalArchive archive;
    private final List<String> out = new ArrayList<>();
    private final List<String> err = new ArrayList<>();
    private long now = 1_764_000_000_000L;

    @Before
    public void setUp() throws Exception {
        clusterDir = Files.createTempDirectory("journal-evaluate-").toFile();
        archive = new FakeJournalArchive().add(1, true, GENESIS, 1, 160);
    }

    @After
    public void tearDown() {
        JournalRetention.Seams.reset();
        org.agrona.IoUtil.delete(clusterDir, true);
    }

    private JournalRetention retention(final JournalProfile profile, final Mode mode, final String... required) {
        return new JournalRetention(profile, clusterDir, "core", 0, mode, Set.of(required), 1L << 30, 3_600_000,
            SystemNanoClock.INSTANCE, out::add, err::add);
    }

    private Report pass(final JournalProfile profile, final Mode mode, final long genesis, final String... required) {
        out.clear();
        err.clear();
        return retention(profile, mode, required).pass(archive, genesis, JournalRetention.OffBox.NONE, now,
            System.nanoTime() + 10_000_000_000L);
    }

    private void set(final JournalProfile profile, final String reader, final long applied, final String... more) {
        final String[] base = {"set", "--cluster-dir", clusterDir.getAbsolutePath(), "--engine", "core",
            "--cluster-id", "0", "--genesis-id", String.valueOf(GENESIS), "--journal", profile.journal(), "--reader",
            reader, "--applied-journal-seq", String.valueOf(applied), "--source", "test"};
        final String[] args = new String[base.length + more.length];
        System.arraycopy(base, 0, args, 0, base.length);
        System.arraycopy(more, 0, args, base.length, more.length);
        tool(args);
    }

    private void tool(final String... args) {
        final ByteArrayOutputStream e = new ByteArrayOutputStream();
        final int exit = JournalReaderTool.run(args, new PrintStream(new ByteArrayOutputStream()), new PrintStream(e),
            () -> now);
        assertEquals(e.toString(), 0, exit);
    }

    private void write(final JournalProfile profile, final String fileName, final JournalReaders.Record record)
        throws Exception {
        final java.nio.file.Path dir = JournalReaders.directory(clusterDir, profile.journal());
        Files.createDirectories(dir);
        Files.write(dir.resolve(fileName), record.encode());
    }

    private void assertHolds(final String what, final Report report, final State state) {
        assertEquals(what, state, report.evaluation.state);
        assertEquals(what + ": no bound", -1, report.evaluation.safeSeq);
        if (!archive.purges.isEmpty()) {
            throw new AssertionError(what + ": " + state + " must purge nothing, purged " + archive.purges);
        }
        assertEquals(what + ": nothing moved", 0, archive.start(1));
    }

    private boolean critical(final String text) {
        return err.stream().anyMatch(l -> l.startsWith("[JOURNAL-RETENTION] CRITICAL: ") && l.contains(text));
    }

    private static final JournalProfile[] PROFILES = {JournalRig.ORDER, JournalRig.MONEY};
    private static final Mode[] ACTIVE_MODES = {Mode.REPORT, Mode.READERS};

    private void fresh() throws Exception {
        org.agrona.IoUtil.delete(new File(clusterDir, "retention"), true);
        archive = new FakeJournalArchive().add(1, true, GENESIS, 1, 160);
    }

    @Test
    public void offNeverPurgesAndIgnoresReaderFiles() throws Exception {
        for (final JournalProfile p : PROFILES) {
            fresh();
            Report r = pass(p, Mode.OFF, GENESIS);
            assertEquals(State.OFF, r.evaluation.state);
            assertEquals("retained bytes are shown under off", 160L * 256, r.retainedBytes);
            assertEquals(1, r.recordings);
            assertNull("off is not a problem", r.problem);
            set(p, "reader", 160);
            write(p, "broken", new JournalReaders.Record("match", 9, 99, "x", "y", 1, -1, "", 1, 1, "t"));
            r = pass(p, Mode.OFF, GENESIS);
            assertEquals("reader files under off are legal and ignored", State.OFF, r.evaluation.state);
            assertTrue(archive.purges.isEmpty());
            assertTrue("off prints no alarm", err.isEmpty());
            assertEquals("off reads no record", 0, archive.reads);
        }
    }

    @Test
    public void noJournalIsNotAHoldAndNotAnAlarm() throws Exception {
        for (final JournalProfile p : PROFILES) {
            for (final Mode mode : ACTIVE_MODES) {
                fresh();
                archive = new FakeJournalArchive().addOtherStream(7, 40);
                Report r = pass(p, mode, GENESIS);
                assertEquals(State.NO_JOURNAL, r.evaluation.state);
                assertTrue("no alarm while the journal is dark: " + err, err.isEmpty());
                assertNull(r.problem);
                assertEquals(0, r.retainedBytes);
                // an empty recording (a writer that connected and wrote nothing) is still no journal
                archive.add(2, true, GENESIS, 1, 0);
                r = pass(p, mode, GENESIS);
                assertEquals(State.NO_JOURNAL, r.evaluation.state);
                assertEquals(0, r.recordings);
            }
        }
        // the order journal of another genesis only: nothing of this genesis to purge, the foreign one is counted
        fresh();
        archive = new FakeJournalArchive().add(1, false, 99, 1, 160);
        set(JournalRig.ORDER, "reader", 160);
        final Report r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertEquals(State.NO_JOURNAL, r.evaluation.state);
        assertEquals(1, r.recordingsForeign);
        assertTrue(archive.purges.isEmpty());
    }

    @Test
    public void everyHoldPurgesNothingAndIsLeftWhenItsCauseIsGone() throws Exception {
        for (final JournalProfile p : PROFILES) {
            for (final Mode mode : ACTIVE_MODES) {
                final String what = p.journal() + "/" + mode.text();

                // UNBOUND: the member has no recorded genesis yet
                fresh();
                set(p, "reader", 100);
                assertHolds(what + " unbound", pass(p, mode, 0), State.UNBOUND);
                assertEquals("an unbound member reads no record", 0, archive.reads);

                // NO_READERS: hold everything, CRITICAL each pass
                fresh();
                Report r = pass(p, mode, GENESIS);
                assertHolds(what + " no readers", r, State.NO_READERS);
                assertTrue(what + ": " + err, critical("no reader is registered for the " + p.journal() + " journal"));
                assertEquals("the tail is still shown", 160, r.tailSeq);
                assertEquals(what, mode == Mode.READERS, r.problem != null);
                r = pass(p, mode, GENESIS);
                assertTrue(what + ": CRITICAL on every pass", critical("no reader is registered"));

                // MISSING: a required reader without a file holds although another reader is registered
                fresh();
                set(p, "other", 100);
                r = pass(p, mode, GENESIS, "needed", "other");
                assertHolds(what + " missing", r, State.MISSING);
                assertEquals("needed", r.evaluation.subject);
                assertTrue(what + ": " + err, critical("the required reader needed of the " + p.journal()
                    + " journal has no file"));

                // INVALID: a corrupt file
                fresh();
                set(p, "reader", 100);
                Files.write(JournalReaders.file(clusterDir, p.journal(), "reader"),
                    "garbage\n".getBytes(StandardCharsets.US_ASCII));
                r = pass(p, mode, GENESIS);
                assertHolds(what + " invalid", r, State.INVALID);
                assertEquals("reader", r.evaluation.subject);
                assertTrue(what + ": " + err, critical("reader file INVALID"));
                set(p, "reader", 100); // the next set replaces it
                assertEquals(what + ": recovered", State.VALID, pass(p, mode, GENESIS).evaluation.state);

                // FOREIGN: each field that names another member, journal or reader
                final String other = p == JournalRig.ORDER ? "money" : "order";
                final JournalReaders.Record[] foreign = {
                    new JournalReaders.Record("match", 0, GENESIS, p.journal(), "reader", 100, -1, "", 1, now, "t"),
                    new JournalReaders.Record("core", 1, GENESIS, p.journal(), "reader", 100, -1, "", 1, now, "t"),
                    new JournalReaders.Record("core", 0, GENESIS + 1, p.journal(), "reader", 100, -1, "", 1, now, "t"),
                    new JournalReaders.Record("core", 0, GENESIS, other, "reader", 100, -1, "", 1, now, "t"),
                    new JournalReaders.Record("core", 0, GENESIS, p.journal(), "someone-else", 100, -1, "", 1, now, "t"),
                };
                for (final JournalReaders.Record f : foreign) {
                    fresh();
                    write(p, "reader", f);
                    r = pass(p, mode, GENESIS);
                    assertHolds(what + " foreign " + f, r, State.FOREIGN);
                    assertTrue(what + ": " + err, critical("reader file FOREIGN"));
                }

                // TAIL_UNKNOWN: the member's own tail cannot be read
                fresh();
                set(p, "reader", 100);
                archive.tailUnreadable = true;
                r = pass(p, mode, GENESIS);
                assertHolds(what + " tail unknown", r, State.TAIL_UNKNOWN);
                assertEquals(-1, r.tailSeq);
                archive.tailUnreadable = false;
                assertEquals(what + ": recovered", State.VALID, pass(p, mode, GENESIS).evaluation.state);

                // AHEAD: a reader above the member's own tail (wrong unit, or the member is behind)
                fresh();
                set(p, "reader", 161);
                r = pass(p, mode, GENESIS);
                assertHolds(what + " ahead", r, State.AHEAD);
                assertEquals("reader", r.evaluation.subject);
                // a release above the tail is ahead too
                fresh();
                set(p, "reader", 10);
                tool("release", "--cluster-dir", clusterDir.getAbsolutePath(), "--genesis-id",
                    String.valueOf(GENESIS), "--journal", p.journal(), "--reader", "reader", "--up-to-journal-seq",
                    "5000", "--reason", "typo");
                assertHolds(what + " ahead by release", pass(p, mode, GENESIS), State.AHEAD);
            }
        }
    }

    @Test
    public void validIsTheMinimumOverTheReadersOfAppliedOrReleased() throws Exception {
        for (final JournalProfile p : PROFILES) {
            fresh();
            set(p, "fast", 150);
            set(p, "slow", 40);
            set(p, "dead", 10);
            tool("release", "--cluster-dir", clusterDir.getAbsolutePath(), "--genesis-id", String.valueOf(GENESIS),
                "--journal", p.journal(), "--reader", "dead", "--up-to-journal-seq", "120", "--reason", "retired");
            final JournalAudit.Snapshot before = archive.snapshot();
            // report: the same bound, nothing purged, the would-purge line
            Report r = pass(p, Mode.REPORT, GENESIS);
            assertEquals(State.VALID, r.evaluation.state);
            assertEquals("min(150, 40, max(10, 120))", 40, r.evaluation.safeSeq);
            assertTrue(archive.purges.isEmpty());
            assertTrue(out.toString(), out.stream().anyMatch(l -> l.contains("report: would purge " + p.journal()
                + " journal recording 1: start 0 -> " + 2L * SEG)));
            assertNull(r.problem);
            assertTrue("a release in force is said on every pass: " + err, critical("hold of reader dead of the "
                + p.journal() + " journal released by operator up to journalSeq 120 (applied 10)"));
            assertEquals(3, r.readers.size());
            // readers: the greatest segment base whose first record is at most safeSeq + 1 (33 at base 2)
            r = pass(p, Mode.READERS, GENESIS);
            assertEquals(40, r.evaluation.safeSeq);
            assertEquals(List.of("segments 1 " + 2L * SEG), archive.purges);
            assertEquals(2L * SEG, r.purgedBytes);
            assertEquals(160L * 256 - 2L * SEG, r.retainedBytes);
            final JournalAudit.Truth truth = new JournalAudit.Truth().cursor("fast", 150).cursor("slow", 40)
                .cursor("dead", 120);
            assertEquals(32, JournalAudit.check(before, archive.snapshot(), truth));
            JournalAudit.checkKept(before, archive.snapshot(), truth, 40);
            // nothing is remembered: the same pass again does nothing, a moved cursor moves the start
            archive.purges.clear();
            assertTrue(pass(p, Mode.READERS, GENESIS).purges == 0 && archive.purges.isEmpty());
            set(p, "slow", 81);
            r = pass(p, Mode.READERS, GENESIS);
            assertEquals(81, r.evaluation.safeSeq);
            assertEquals("base 5 starts at journalSeq 81 <= safeSeq + 1", 5L * SEG, archive.start(1));
        }
    }

    /** C5: a hold that is repaired is followed by a start position that advances. */
    @Test
    public void aRepairedHoldPurgesOnTheNextPass() throws Exception {
        // FOREIGN (a cursor of another genesis), removed and set again
        write(JournalRig.ORDER, "reader", new JournalReaders.Record("core", 0, GENESIS + 1, "order", "reader", 100, -1,
            "", 1, now, "t"));
        assertHolds("foreign", pass(JournalRig.ORDER, Mode.READERS, GENESIS), State.FOREIGN);
        // the control of the identity check: with it off the foreign file's cursor purges this member's journal
        JournalRetention.Seams.foreignCheckOff = true;
        try {
            JournalAudit.nc("foreign reader file", "purged on a reader file of another genesis", () -> {
                final Report x;
                try {
                    x = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
                } catch (final Exception e) {
                    throw new IllegalStateException(e);
                }
                if (x.purges > 0) {
                    throw new AssertionError("purged on a reader file of another genesis: " + x);
                }
            });
        } finally {
            JournalRetention.Seams.reset();
            archive = new FakeJournalArchive().add(1, true, GENESIS, 1, 160);
        }
        tool("remove", "--cluster-dir", clusterDir.getAbsolutePath(), "--genesis-id", String.valueOf(GENESIS + 1),
            "--journal", "order", "--reader", "reader", "--reason", "old genesis");
        assertHolds("removed: the last reader is gone", pass(JournalRig.ORDER, Mode.READERS, GENESIS),
            State.NO_READERS);
        set(JournalRig.ORDER, "reader", 100);
        assertEquals(State.VALID, pass(JournalRig.ORDER, Mode.READERS, GENESIS).evaluation.state);
        assertEquals(6L * SEG, archive.start(1));

        // AHEAD (the member is behind its reader), cleared when the member catches up
        fresh();
        set(JournalRig.ORDER, "reader", 200);
        assertHolds("ahead", pass(JournalRig.ORDER, Mode.READERS, GENESIS), State.AHEAD);
        archive.append(1, 161, 320);
        final Report r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertEquals(State.VALID, r.evaluation.state);
        assertEquals(200, r.evaluation.safeSeq);
        assertEquals(320, r.tailSeq);
        assertEquals("base 12 starts at journalSeq 193", 12L * SEG, archive.start(1));
    }

    /** The pure function: the table's order, and the off-box rows. */
    @Test
    public void theTableInItsOrder() {
        final List<JournalReaders.Entry> none = List.of();
        final JournalRetention.Tail tail = new JournalRetention.TailKnown(160);
        final JournalRetention.OffBox noOffBox = JournalRetention.OffBox.NONE;
        assertEquals(State.OFF, JournalRetention.evaluate(Mode.OFF, "order", "core", 0, 0, none, Set.of("x"), false,
            new JournalRetention.TailUnknown("x"), noOffBox).state);
        assertEquals("no journal comes before unbound", State.NO_JOURNAL, JournalRetention.evaluate(Mode.READERS,
            "order", "core", 0, 0, none, Set.of(), false, tail, noOffBox).state);
        assertEquals("unbound comes before the readers", State.UNBOUND, JournalRetention.evaluate(Mode.READERS,
            "order", "core", 0, 0, none, Set.of(), true, tail, noOffBox).state);
        final JournalReaders.Entry ok = new JournalReaders.Entry("r", JournalReaders.Read.of(
            new JournalReaders.Record("core", 0, GENESIS, "order", "r", 100, -1, "", 1, 1, "t")));
        final JournalReaders.Entry bad = new JournalReaders.Entry("b", JournalReaders.Read.invalid("no"));
        assertEquals("an invalid file comes before an unreadable tail", State.INVALID, JournalRetention.evaluate(
            Mode.READERS, "order", "core", 0, GENESIS, List.of(bad, ok), Set.of(), true,
            new JournalRetention.TailUnknown("x"), noOffBox).state);
        assertEquals(State.TAIL_UNKNOWN, JournalRetention.evaluate(Mode.READERS, "order", "core", 0, GENESIS,
            List.of(ok), Set.of(), true, new JournalRetention.TailUnknown("x"), noOffBox).state);
        final JournalRetention.Evaluation valid = JournalRetention.evaluate(Mode.READERS, "order", "core", 0, GENESIS,
            List.of(ok), Set.of("r"), true, tail, noOffBox);
        assertEquals(State.VALID, valid.state);
        assertEquals(100, valid.safeSeq);
        assertEquals(OffBoxWatermark.NO_CONSTRAINT, valid.logFloor);
        assertTrue(valid.purges());
        assertFalse("report never purges", JournalRetention.evaluate(Mode.REPORT, "order", "core", 0, GENESIS,
            List.of(ok), Set.of(), true, tail, noOffBox).purges());
        assertEquals("a reader at the tail is not ahead", State.VALID, JournalRetention.evaluate(Mode.READERS, "order",
            "core", 0, GENESIS, List.of(ok), Set.of(), true, new JournalRetention.TailKnown(100), noOffBox).state);
        assertEquals(State.AHEAD, JournalRetention.evaluate(Mode.READERS, "order", "core", 0, GENESIS, List.of(ok),
            Set.of(), true, new JournalRetention.TailKnown(99), noOffBox).state);
    }

    // ------------------------------------------------------------------ negative controls (T6)

    /** The audit a control must fail: one pass in mode readers, held against what the test knows to be true. */
    private void auditedPass(final JournalProfile profile, final JournalAudit.Truth truth,
                             final long largestReaderBound) {
        final JournalAudit.Snapshot before = archive.snapshot();
        final long tailBefore = before.tail(GENESIS, profile.hasEpoch());
        pass(profile, Mode.READERS, GENESIS);
        final long purged = JournalAudit.check(before, archive.snapshot(), truth);
        JournalAudit.checkBoundWithinTail(purged, largestReaderBound, tailBefore);
    }

    @Test
    public void controlNoReadersMappedToNoConstraint() throws Exception {
        for (final JournalProfile p : PROFILES) {
            fresh();
            final JournalAudit.Truth truth = p.hasEpoch() ? new JournalAudit.Truth() : new JournalAudit.Truth().noEpoch();
            auditedPass(p, truth, -1); // the rule on: nothing purged, the audit passes
            assertTrue(archive.purges.isEmpty());
            JournalRetention.Seams.noReadersIsNoConstraint = true;
            try {
                JournalAudit.nc("T6a " + p.journal(), "purged with no registered reader",
                    () -> auditedPass(p, truth, -1));
            } finally {
                JournalRetention.Seams.reset();
            }
        }
    }

    @Test
    public void controlAheadCheckOff() throws Exception {
        for (final JournalProfile p : PROFILES) {
            fresh();
            // a cursor in the wrong unit (a byte position): far above anything the member has written
            set(p, "reader", 4_000_000);
            final JournalAudit.Truth truth = (p.hasEpoch() ? new JournalAudit.Truth() : new JournalAudit.Truth().noEpoch())
                .cursor("reader", 4_000_000);
            auditedPass(p, truth, 4_000_000);
            assertTrue(archive.purges.isEmpty());
            JournalRetention.Seams.aheadCheckOff = true;
            try {
                JournalAudit.nc("T6b " + p.journal(), "purged records above the member's tail",
                    () -> auditedPass(p, truth, 4_000_000));
            } finally {
                JournalRetention.Seams.reset();
            }
        }
    }

    @Test
    public void controlMinimumReplacedByMaximum() throws Exception {
        for (final JournalProfile p : PROFILES) {
            fresh();
            set(p, "fast", 150);
            set(p, "slow", 40);
            final JournalAudit.Truth truth = (p.hasEpoch() ? new JournalAudit.Truth() : new JournalAudit.Truth().noEpoch())
                .cursor("fast", 150).cursor("slow", 40);
            auditedPass(p, truth, 150);
            assertEquals(2L * SEG, archive.start(1));
            fresh();
            set(p, "fast", 150);
            set(p, "slow", 40);
            JournalRetention.Seams.minIsMax = true;
            try {
                JournalAudit.nc("T6c " + p.journal(), "purged a record above reader slow's cursor",
                    () -> auditedPass(p, truth, 150));
            } finally {
                JournalRetention.Seams.reset();
            }
        }
    }

    // ------------------------------------------------------------------ alarms and the pass budget

    @Test
    public void retainedBytesAndAStaleReaderAreAlarmsNotLimits() throws Exception {
        set(JournalRig.ORDER, "reader", 40);
        JournalRetention r = new JournalRetention(JournalRig.ORDER, clusterDir, "core", 0, Mode.READERS, Set.of(),
            8L * SEG - 1, 1_000, SystemNanoClock.INSTANCE, out::add, err::add);
        now += 5_000; // the reader file is five seconds old, the stale bound is one
        final Report report = r.pass(archive, GENESIS, JournalRetention.OffBox.NONE, now,
            System.nanoTime() + 10_000_000_000L);
        assertEquals("the bound is the reader's, whatever its file's age", 40, report.evaluation.safeSeq);
        assertEquals(2L * SEG, archive.start(1));
        assertEquals(5_000, report.oldestReaderAgeMs);
        assertTrue(err.toString(), critical("this member holds " + 8L * SEG + " bytes of the order journal (above"
            + " JOURNAL_RETENTION_ALARM_BYTES " + (8L * SEG - 1) + ")"));
        assertTrue(err.toString(), critical("a reader file of the order journal was written 5000 ms ago (above"
            + " JOURNAL_RETENTION_STALE_MS 1000)"));
    }

    @Test
    public void aSequenceThatDoesNotRiseIsAnAnomalyAndTheRecordingIsNotPurged() throws Exception {
        set(JournalRig.ORDER, "reader", 100);
        // the record at the recording's start carries a sequence above the records at its higher segment bases
        archive.overwrite(1, 0, 500);
        final Report r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertEquals(State.VALID, r.evaluation.state);
        assertEquals(List.of(1L), r.anomalies);
        assertTrue(archive.purges.isEmpty());
        assertTrue(err.toString(), critical("order journal recording 1 ANOMALY: journalSeq"));
        assertTrue(r.problem, r.problem.contains("ANOMALY"));
        // nothing is stored: the same archive without the fault purges on the next pass
        archive.overwrite(1, 0, 1);
        assertTrue(pass(JournalRig.ORDER, Mode.READERS, GENESIS).anomalies.isEmpty());
        assertEquals(6L * SEG, archive.start(1));
    }

    @Test
    public void anUnreadableBoundaryDoesNotQualifyALowerOneDoesAndTheRecordingIsCountedAsAnAnomaly() throws Exception {
        set(JournalRig.ORDER, "reader", 100);
        archive.unreadable.add(6L * SEG);
        archive.unreadable.add(5L * SEG);
        Report r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertEquals(State.VALID, r.evaluation.state);
        assertEquals("a journal that is VALID and cannot be cut as far as its reader allows is counted", List.of(1L),
            r.anomalies);
        assertTrue("a lower boundary was taken: " + archive.purges, archive.start(1) > 0 && archive.start(1) < 5L * SEG);
        assertTrue(err.toString(), critical("order journal recording 1 ANOMALY: the record at segment base"));
        assertTrue(err.toString(), err.stream().anyMatch(l -> l.contains("is unreadable (schema id 9")));
        assertTrue(r.problem, r.problem.contains("ANOMALY"));
        // nothing is stored: once the boundaries read, the next pass has no anomaly and cuts to the reader's bound
        archive.unreadable.clear();
        r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertTrue(r.anomalies.isEmpty());
        assertNull(r.problem);
        assertEquals(6L * SEG, archive.start(1));
    }

    @Test
    public void anUnreadableRecordAtTheStartLeavesTheRecordingAloneAndIsCountedAsAnAnomaly() throws Exception {
        set(JournalRig.ORDER, "reader", 100);
        archive.unreadable.add(0L);
        for (final Mode mode : ACTIVE_MODES) {
            final Report r = pass(JournalRig.ORDER, mode, GENESIS);
            assertEquals(State.VALID, r.evaluation.state);
            assertEquals(mode.text(), List.of(1L), r.anomalies);
            assertTrue(archive.purges.isEmpty());
            assertEquals(0, archive.start(1));
            assertTrue(err.toString(), critical("order journal recording 1 ANOMALY: the record at its start 0 is"
                + " unreadable"));
            assertEquals(mode.text(), mode == Mode.READERS, r.problem != null);
        }
        archive.unreadable.clear();
        assertTrue(pass(JournalRig.ORDER, Mode.READERS, GENESIS).anomalies.isEmpty());
        assertEquals(6L * SEG, archive.start(1));
    }

    @Test
    public void aRefusedPurgeIsCountedAndTheNextPassTriesAgain() throws Exception {
        set(JournalRig.ORDER, "reader", 100);
        archive.refuseForReplay = 10; // a reader's replay sits below the target for longer than the retries
        Report r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertEquals(1, r.purgeFailures);
        assertEquals(0, r.purgedBytes);
        assertEquals(0, archive.start(1));
        assertEquals("the hold of a refused purge is the archive's, not a reader's or an input's", State.VALID,
            r.evaluation.state);
        assertNull(r.problem);
        assertTrue(out.toString(), out.stream().anyMatch(l -> l.contains("the archive refused to purge")));
        archive.refuseForReplay = 0;
        r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertEquals(0, r.purgeFailures);
        assertEquals(6L * SEG, archive.start(1));
    }

    @Test
    public void aPassPastItsBudgetStopsAndTheNextOneContinues() throws Exception {
        archive = new FakeJournalArchive().add(1, false, GENESIS, 1, 64).add(2, false, GENESIS, 1, 80)
            .add(3, true, GENESIS, 1, 160);
        set(JournalRig.ORDER, "reader", 100);
        out.clear();
        final Report late = retention(JournalRig.ORDER, Mode.READERS).pass(archive, GENESIS,
            JournalRetention.OffBox.NONE, now, System.nanoTime() - 1);
        assertTrue(late.budgetExceeded);
        assertTrue(archive.purges.isEmpty());
        final Report r = pass(JournalRig.ORDER, Mode.READERS, GENESIS);
        assertFalse(r.budgetExceeded);
        assertEquals(List.of("recording 1", "recording 2", "segments 3 " + 6L * SEG), archive.purges);
        assertEquals(1, r.recordings);
    }
    /**
     * A stopped recording that would go whole has its start read first. A read that ran into the pass deadline says
     * nothing about the start: the recording is not deleted, the pass ends as over budget and nothing is called an
     * ANOMALY; the next pass reads again and deletes. An unreadable start inside the budget is an ANOMALY.
     */
    @Test
    public void aStartReadThatRunsIntoThePassDeadlineIsNotAnAnomaly() throws Exception {
        archive = new FakeJournalArchive().add(1, false, GENESIS, 1, 64).add(3, true, GENESIS, 1, 160);
        set(JournalRig.ORDER, "reader", 100);
        final long[] clock = {1_000};
        final long deadline = 2_000;
        final boolean[] timeOut = {true};
        final JournalRetention.Archive slow = new JournalRetention.Archive() {
            @Override
            public List<JournalRetention.Recording> list() {
                return archive.list();
            }

            @Override
            public JournalRetention.Peek first(final JournalRetention.Recording r, final long position, final long d) {
                if (r.id() == 1 && timeOut[0]) {
                    clock[0] = d; // the replay waited until the deadline it was given
                    return JournalRetention.Peek.unreadable("ReplayTimeoutException: recording 1");
                }
                return archive.first(r, position, d);
            }

            @Override
            public JournalRetention.Peek last(final JournalRetention.Recording r, final long d) {
                return archive.last(r, d);
            }

            @Override
            public void purgeSegments(final long recordingId, final long newStartPosition) {
                archive.purgeSegments(recordingId, newStartPosition);
            }

            @Override
            public void purgeRecording(final long recordingId) {
                archive.purgeRecording(recordingId);
            }
        };
        final JournalRetention retention = new JournalRetention(JournalRig.ORDER, clusterDir, "core", 0, Mode.READERS,
            Set.of(), 1L << 30, 3_600_000, () -> clock[0], out::add, err::add);
        out.clear();
        err.clear();
        Report r = retention.pass(slow, GENESIS, JournalRetention.OffBox.NONE, now, deadline);
        assertTrue("the pass ended at its budget", r.budgetExceeded);
        assertTrue("nothing deleted: " + archive.purges, archive.purges.isEmpty());
        assertTrue("a timed out read is not an anomaly: " + r.anomalies, r.anomalies.isEmpty());
        assertNull(r.problem);
        assertFalse(err.toString(), err.stream().anyMatch(l -> l.contains("ANOMALY")));
        assertTrue(out.toString(), out.stream().anyMatch(l -> l.contains("the pass budget ended while its start was"
            + " read") && l.contains("Not purged this pass")));
        // the same unreadable start inside the budget is what it was: an anomaly, still nothing deleted
        final JournalRetention.Archive unreadable = new JournalRetention.Archive() {
            @Override
            public List<JournalRetention.Recording> list() {
                return archive.list();
            }

            @Override
            public JournalRetention.Peek first(final JournalRetention.Recording rec, final long position, final long d) {
                return rec.id() == 1 ? JournalRetention.Peek.unreadable("schema id 9") : archive.first(rec, position, d);
            }

            @Override
            public JournalRetention.Peek last(final JournalRetention.Recording rec, final long d) {
                return archive.last(rec, d);
            }

            @Override
            public void purgeSegments(final long recordingId, final long newStartPosition) {
                archive.purgeSegments(recordingId, newStartPosition);
            }

            @Override
            public void purgeRecording(final long recordingId) {
                archive.purgeRecording(recordingId);
            }
        };
        clock[0] = 1_000;
        r = retention.pass(unreadable, GENESIS, JournalRetention.OffBox.NONE, now, deadline);
        assertFalse("recording 1 is not deleted: " + archive.purges, archive.purges.contains("recording 1"));
        assertEquals(List.of(1L), r.anomalies);
        assertFalse(r.budgetExceeded);
        assertTrue(err.toString(), err.stream().anyMatch(l -> l.contains("recording 1 ANOMALY: the record at its"
            + " start 0 is unreadable")));
        // the next pass reads the start and deletes
        timeOut[0] = false;
        clock[0] = 1_000;
        r = retention.pass(slow, GENESIS, JournalRetention.OffBox.NONE, now, deadline);
        assertFalse(r.budgetExceeded);
        assertTrue(archive.purges.toString(), archive.purges.contains("recording 1"));
    }
}
