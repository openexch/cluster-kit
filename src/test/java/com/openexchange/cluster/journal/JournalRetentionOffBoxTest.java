// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.cluster.journal.JournalRetention.Mode;
import com.openexchange.cluster.journal.JournalRetention.OffBox;
import com.openexchange.cluster.journal.JournalRetention.Report;
import com.openexchange.cluster.journal.JournalRetention.State;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The off-box bound (T9): under {@code RETENTION_OFFBOX=required} no record whose log position is above the off-box
 * floor is purged, and any watermark state but VALID holds the journal; {@code report} works the floor out and does
 * not apply it; {@code off} is no bound. The archive is in memory: an active recording of journalSeq 1..160 and a
 * stopped one of 1..64, each record's log position rising with its sequence.
 */
public class JournalRetentionOffBoxTest {

    private static final long GENESIS = JournalRig.GENESIS;
    private static final int SEG = FakeJournalArchive.SEG;
    private static final long COMMIT = JournalRig.logPositionOf(160);

    private File clusterDir;
    private FakeJournalArchive archive;
    private final List<String> out = new ArrayList<>();
    private final List<String> err = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        clusterDir = Files.createTempDirectory("journal-offbox-").toFile();
        archive = new FakeJournalArchive().add(1, false, GENESIS, 1, 64).add(2, true, GENESIS, 1, 160);
        final int exit = JournalReaderTool.run(new String[] {"set", "--cluster-dir", clusterDir.getAbsolutePath(),
            "--engine", "core", "--cluster-id", "0", "--genesis-id", String.valueOf(GENESIS), "--journal", "order",
            "--reader", "reader", "--applied-journal-seq", "100", "--source", "test"},
            new PrintStream(new ByteArrayOutputStream()), new PrintStream(new ByteArrayOutputStream()), () -> 1L);
        assertEquals(0, exit);
    }

    @After
    public void tearDown() {
        JournalRetention.Seams.reset();
        org.agrona.IoUtil.delete(clusterDir, true);
    }

    private void watermark(final long logPosition, final long genesis) throws Exception {
        OffBoxWatermark.write(clusterDir, new OffBoxWatermark.Record("core", 0, genesis, logPosition, -1, "", 1, 1,
            "archiver"));
    }

    private OffBox offBox(final OffBoxWatermark.Mode mode) {
        return new OffBox(mode, OffBoxWatermark.evaluate(mode, OffBoxWatermark.read(clusterDir), "core", 0, GENESIS,
            COMMIT));
    }

    private Report pass(final OffBoxWatermark.Mode mode) {
        out.clear();
        err.clear();
        return new JournalRetention(JournalRig.ORDER, clusterDir, "core", 0, Mode.READERS, Set.of(), 1L << 30,
            3_600_000, SystemNanoClock.INSTANCE, out::add, err::add).pass(archive, GENESIS, offBox(mode), 1L,
            System.nanoTime() + 10_000_000_000L);
    }

    private JournalAudit.Truth truth(final long floor) {
        return new JournalAudit.Truth().cursor("reader", 100).logFloor(floor);
    }

    @Test
    public void requiredPurgesNoRecordAboveTheFloor() throws Exception {
        // the off-box copy ends inside journalSeq 60's entry: 1..60 are off-box, the reader has applied 1..100
        final long floor = JournalRig.logPositionOf(60);
        watermark(floor, GENESIS);
        final JournalAudit.Snapshot before = archive.snapshot();
        final Report report = pass(OffBoxWatermark.Mode.REQUIRED);
        assertEquals(State.VALID, report.evaluation.state);
        assertEquals(100, report.evaluation.safeSeq);
        assertEquals(floor, report.evaluation.logFloor);
        JournalAudit.check(before, archive.snapshot(), truth(floor));
        // the active recording: base 3 starts at journalSeq 49 (off-box), base 4 at 65 (not yet)
        assertEquals(3L * SEG, archive.start(2));
        // the stopped recording ends at journalSeq 64, above the floor: not deleted whole, cut to its last segment
        assertTrue(archive.exists(1));
        assertEquals(3L * SEG, archive.start(1));
        // the floor rises past it: it goes whole, and the active one is cut by the reader's cursor again
        watermark(JournalRig.logPositionOf(160), GENESIS);
        pass(OffBoxWatermark.Mode.REQUIRED);
        assertFalse(archive.exists(1));
        assertEquals(6L * SEG, archive.start(2));
    }

    @Test
    public void anyWatermarkStateButValidHoldsUnderRequired() throws Exception {
        // ABSENT
        Report report = pass(OffBoxWatermark.Mode.REQUIRED);
        assertEquals(State.OFFBOX_HOLD, report.evaluation.state);
        assertTrue(report.evaluation.detail, report.evaluation.detail.contains("ABSENT"));
        assertTrue(archive.purges.isEmpty());
        assertTrue(report.problem, report.problem.contains("OFFBOX_HOLD"));
        // FOREIGN (another genesis)
        watermark(JournalRig.logPositionOf(160), GENESIS + 1);
        report = pass(OffBoxWatermark.Mode.REQUIRED);
        assertEquals(State.OFFBOX_HOLD, report.evaluation.state);
        assertTrue(report.evaluation.detail, report.evaluation.detail.contains("FOREIGN"));
        // AHEAD (above this member's commit position)
        watermark(COMMIT + 1, GENESIS);
        report = pass(OffBoxWatermark.Mode.REQUIRED);
        assertEquals(State.OFFBOX_HOLD, report.evaluation.state);
        assertTrue(report.evaluation.detail, report.evaluation.detail.contains("AHEAD"));
        // INVALID
        Files.write(OffBoxWatermark.file(clusterDir), new byte[] {'x', '\n'});
        report = pass(OffBoxWatermark.Mode.REQUIRED);
        assertEquals(State.OFFBOX_HOLD, report.evaluation.state);
        assertTrue(archive.purges.isEmpty());
        // not wired at all under required: a hold, never "no bound"
        assertEquals(State.OFFBOX_HOLD, new JournalRetention(JournalRig.ORDER, clusterDir, "core", 0, Mode.READERS,
            Set.of(), 1L << 30, 3_600_000, SystemNanoClock.INSTANCE, out::add, err::add).pass(archive, GENESIS,
            new OffBox(OffBoxWatermark.Mode.REQUIRED, null), 1L, System.nanoTime() + 10_000_000_000L)
            .evaluation.state);
        assertTrue(archive.purges.isEmpty());
        // a correct watermark: the hold is left and the start advances
        watermark(JournalRig.logPositionOf(60), GENESIS);
        assertEquals(State.VALID, pass(OffBoxWatermark.Mode.REQUIRED).evaluation.state);
        assertEquals(3L * SEG, archive.start(2));
    }

    @Test
    public void reportAndOffDoNotApplyTheFloor() throws Exception {
        watermark(JournalRig.logPositionOf(60), GENESIS);
        Report report = pass(OffBoxWatermark.Mode.REPORT);
        assertEquals(State.VALID, report.evaluation.state);
        assertEquals(OffBoxWatermark.NO_CONSTRAINT, report.evaluation.logFloor);
        assertEquals("only the reader bounds the purge", 6L * SEG, archive.start(2));
        assertFalse(archive.exists(1));

        archive = new FakeJournalArchive().add(1, false, GENESIS, 1, 64).add(2, true, GENESIS, 1, 160);
        Files.delete(OffBoxWatermark.file(clusterDir));
        report = pass(OffBoxWatermark.Mode.OFF);
        assertEquals(State.VALID, report.evaluation.state);
        assertEquals(6L * SEG, archive.start(2));
        assertEquals(State.VALID, new JournalRetention(JournalRig.ORDER, clusterDir, "core", 0, Mode.READERS,
            Set.of(), 1L << 30, 3_600_000, SystemNanoClock.INSTANCE, out::add, err::add).pass(archive, GENESIS,
            OffBox.NONE, 1L, System.nanoTime() + 10_000_000_000L).evaluation.state);
    }

    /** T9's control: with the bound off, records the off-box copy does not hold are purged. */
    @Test
    public void controlOffBoxBoundOff() throws Exception {
        final long floor = JournalRig.logPositionOf(60);
        watermark(floor, GENESIS);
        final Runnable audited = () -> {
            final JournalAudit.Snapshot before = archive.snapshot();
            pass(OffBoxWatermark.Mode.REQUIRED);
            JournalAudit.check(before, archive.snapshot(), truth(floor));
        };
        JournalRetention.Seams.offBoxBoundOff = true;
        try {
            JournalAudit.nc("T9", "purged past the off-box position", audited);
        } finally {
            JournalRetention.Seams.reset();
        }
    }

    /** A record without a log position (an older schema version) cannot be held against the floor: it holds. */
    @Test
    public void aRecordWithoutALogPositionDoesNotQualifyUnderRequired() throws Exception {
        watermark(JournalRig.logPositionOf(160), GENESIS);
        final JournalRetention.Archive noLogPosition = new JournalRetention.Archive() {
            public List<JournalRetention.Recording> list() {
                return archive.list();
            }

            private JournalRetention.Peek strip(final JournalRetention.Peek p) {
                return !p.readable() ? p : JournalRetention.Peek.of(new JournalProfile.Key(p.key().journalSeq(),
                    p.key().epoch(), JournalProfile.NO_LOG_POSITION, 1, 1));
            }

            public JournalRetention.Peek first(final JournalRetention.Recording r, final long position,
                                               final long deadlineNs) {
                return strip(archive.first(r, position, deadlineNs));
            }

            public JournalRetention.Peek last(final JournalRetention.Recording r, final long deadlineNs) {
                return strip(archive.last(r, deadlineNs));
            }

            public void purgeSegments(final long recordingId, final long newStartPosition) {
                archive.purgeSegments(recordingId, newStartPosition);
            }

            public void purgeRecording(final long recordingId) {
                archive.purgeRecording(recordingId);
            }
        };
        final Report report = new JournalRetention(JournalRig.ORDER, clusterDir, "core", 0, Mode.READERS, Set.of(),
            1L << 30, 3_600_000, SystemNanoClock.INSTANCE, out::add, err::add).pass(noLogPosition, GENESIS,
            offBox(OffBoxWatermark.Mode.REQUIRED), 1L, System.nanoTime() + 10_000_000_000L);
        assertEquals(State.VALID, report.evaluation.state);
        assertTrue("nothing qualifies without a log position: " + archive.purges, archive.purges.isEmpty());
    }
}
