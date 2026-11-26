// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.journal.JournalRetention.Mode;
import com.openexchange.cluster.journal.JournalRetention.Report;
import com.openexchange.cluster.journal.JournalRetention.State;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The retention on a real archive with 64 KiB segments, for a journal with an epoch and fixed records (order) and
 * one without an epoch and with three record lengths (money): the one-record reads at segment bases, the bisection,
 * the two-segment clamp, a wrong-schema frame at a boundary, a recording of another epoch, a purge refused under a
 * reader's replay, and that every record above the bound still replays.
 */
public class JournalRetentionArchiveTest {

    private static final int SEG = JournalRig.SEG;
    private JournalRig rig;

    @Before
    public void setUp() {
        rig = new JournalRig();
    }

    @After
    public void tearDown() {
        JournalRetention.Seams.reset();
        rig.close();
    }

    private static JournalAudit.Truth truth(final JournalProfile p) {
        return p.hasEpoch() ? new JournalAudit.Truth() : new JournalAudit.Truth().noEpoch();
    }

    /** The greatest segment base of {@code before}'s recording whose first record has a sequence at most {@code max}. */
    private static long greatestBase(final JournalAudit.Recording recording, final long max, final long upTo) {
        long best = recording.start();
        for (final JournalAudit.Rec rec : recording.records()) {
            if (rec.position() % SEG == 0 && rec.position() <= upTo && rec.seq() <= max
                && rec.position() > recording.start()) {
                best = rec.position();
            }
        }
        return best;
    }

    @Test
    public void theFirstRecordAtEverySegmentBaseIsReadExactly() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final JournalRig.Writer w = rig.writer(p).append(1, p.hasEpoch() ? 1_700 : 4_000);
            final JournalAudit.Snapshot all = rig.snapshot(p);
            final JournalAudit.Recording recording = all.recordings.get(w.recordingId);
            final JournalRetention.AeronPort port = rig.port(p);
            final JournalRetention.Recording listed = port.list().stream().filter(r -> r.id() == w.recordingId)
                .findFirst().orElseThrow();
            assertTrue(listed.active());
            assertEquals(SEG, listed.segmentLength());
            int bases = 0;
            for (final JournalAudit.Rec rec : recording.records()) {
                if (rec.position() % SEG != 0) {
                    continue;
                }
                bases++;
                final JournalRetention.Peek peek = port.first(listed, rec.position(), System.nanoTime() + JournalRig.TIMEOUT_NS);
                assertTrue(p.journal() + " base " + rec.position() + ": " + peek.unreadable(), peek.readable());
                assertEquals(p.journal() + " base " + rec.position(), rec.seq(), peek.key().journalSeq());
                assertEquals(rec.logPosition(), peek.key().logPosition());
                assertEquals(p.hasEpoch() ? JournalRig.GENESIS : 0, peek.key().epoch());
            }
            assertTrue(p.journal() + ": the recording crosses at least four segments, has " + bases, bases >= 5);
            final JournalRetention.Peek last = port.last(listed, System.nanoTime() + JournalRig.TIMEOUT_NS);
            assertTrue(last.unreadable(), last.readable());
            assertEquals(recording.lastSeq(), last.key().journalSeq());
            assertEquals(JournalRig.logPositionOf(recording.lastSeq()), last.key().logPosition());
            // a position inside a frame is not a record; a position below the start or at the end is none either
            assertFalse(port.first(listed, SEG + 32, System.nanoTime() + JournalRig.TIMEOUT_NS).readable());
            assertFalse(port.first(listed, listed.end(), System.nanoTime() + JournalRig.TIMEOUT_NS).readable());
            JournalRig.green(p.journal() + ": the first record at " + bases + " segment bases, and the last record");
        }
    }

    @Test
    public void bisectionPicksTheGreatestQualifyingBoundaryAndEveryRecordAboveTheBoundReplays() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final long total = p.hasEpoch() ? 2_600 : 6_000;
            final JournalRig.Writer w = rig.writer(p).append(1, total);
            for (final long cursor : new long[] {0, 1, total / 3, total / 2 + 7, total}) {
                rig.set(p, "reader", cursor);
                final JournalAudit.Snapshot before = rig.snapshot(p);
                final JournalAudit.Recording recording = before.recordings.get(w.recordingId);
                final long headBase = recording.end() & -(long) SEG;
                final long expected = greatestBase(recording, cursor + 1, headBase - 2L * SEG);
                final Report report = rig.pass(p);
                assertEquals(State.VALID, report.evaluation.state);
                assertEquals(cursor, report.evaluation.safeSeq);
                assertEquals(total, report.tailSeq);
                final JournalAudit.Snapshot after = rig.snapshot(p);
                final JournalAudit.Truth truth = truth(p).cursor("reader", cursor);
                JournalAudit.check(before, after, truth);
                JournalAudit.checkKept(before, after, truth, cursor);
                assertEquals(p.journal() + " cursor " + cursor + ": the start is the greatest qualifying base", expected,
                    rig.start(w.recordingId));
                assertEquals(expected - recording.start(), report.purgedBytes);
                assertEquals(recording.end() - expected, report.retainedBytes);
                assertEquals(0, report.purgeFailures);
            }
            // the reader has applied everything: two whole segments stay below the head
            final long start = rig.start(w.recordingId);
            final long head = w.position();
            assertEquals(p.journal() + ": two whole segments below the head segment", (head & -(long) SEG) - 2L * SEG,
                start);
            JournalRig.green(p.journal() + ": start " + start + " of head " + head + " with every record applied");
        }
    }

    /** T7 (b): with the clamp off the recording is cut up to its head segment. */
    @Test
    public void controlClampOff() {
        final JournalProfile p = JournalRig.ORDER;
        rig.writer(p).append(1, 2_000);
        rig.set(p, "reader", 2_000);
        final Runnable audited = () -> {
            final JournalAudit.Snapshot before = rig.snapshot(p);
            rig.pass(p);
            JournalAudit.check(before, rig.snapshot(p), truth(p).cursor("reader", 2_000));
        };
        JournalRetention.Seams.clampOff = true;
        try {
            JournalAudit.nc("T7b", "fewer than two segments below the head", audited);
        } finally {
            JournalRetention.Seams.reset();
        }
    }

    @Test
    public void aRecordingOfAnotherEpochIsNeverTouched() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer old = rig.writer(p).append(1, 1_500, 99).stop(); // a previous genesis in this archive
        final JournalRig.Writer live = rig.writer(p).append(1, 1_500);
        rig.set(p, "reader", 1_500);
        final Runnable audited = () -> {
            final JournalAudit.Snapshot before = rig.snapshot(p);
            final Report report = rig.pass(p);
            JournalAudit.check(before, rig.snapshot(p), truth(p).cursor("reader", 1_500));
            assertEquals(1, report.recordingsForeign);
            assertEquals("the tail is this genesis' only", 1_500, report.tailSeq);
        };
        audited.run();
        assertEquals("the foreign recording keeps its start", 0, rig.start(old.recordingId));
        assertTrue("this genesis' recording was cut", rig.start(live.recordingId) > 0);
        // T7 (a): with the epoch check off the foreign recording is this genesis' and is deleted
        JournalRetention.Seams.epochCheckOff = true;
        try {
            JournalAudit.nc("T7a", "purged a recording of genesis 99", audited);
        } finally {
            JournalRetention.Seams.reset();
        }
    }

    /**
     * One recording that holds records of two epochs (a process run that wrote before its engine was bound to this
     * genesis, or anything else that went wrong): its last record is this genesis', so it is counted as own, and its
     * first record is not. The retention reads the start before it cuts or deletes: such a recording is an ANOMALY,
     * active or stopped, and nothing of it goes.
     */
    @Test
    public void aRecordingThatStartsInAnotherEpochIsNeitherCutNorDeleted() {
        final JournalProfile p = JournalRig.ORDER;
        // stopped: 300 records of epoch 99, then 1..1500 of this genesis; the live recording covers its tail
        final JournalRig.Writer mixed = rig.writer(p).append(1, 300, 99).append(1, 1_500).stop();
        final JournalRig.Writer live = rig.writer(p).append(1_200, 3_000);
        rig.set(p, "reader", 2_500);
        final Runnable audited = () -> {
            final JournalAudit.Snapshot before = rig.snapshot(p);
            final Report report = rig.pass(p);
            JournalAudit.check(before, rig.snapshot(p), truth(p).cursor("reader", 2_500));
            assertEquals("counted as own by its last record", 0, report.recordingsForeign);
            assertEquals(java.util.List.of(mixed.recordingId), report.anomalies);
            assertTrue(String.valueOf(report.problem), report.problem.contains("ANOMALY"));
        };
        audited.run();
        assertEquals("the stopped recording of two epochs keeps its start", 0, rig.start(mixed.recordingId));
        assertTrue(rig.err(), rig.err().contains("journal recording " + mixed.recordingId + " ANOMALY: the record at"
            + " its start 0 carries epoch 99 (journalSeq 1), this member's genesis is " + JournalRig.GENESIS
            + ": a recording of two epochs"));
        assertTrue("the recording of this genesis alone is cut as before", rig.start(live.recordingId) > 0);
        // the control: with the epoch check off the start is not read and the records of epoch 99 are deleted
        JournalRetention.Seams.epochCheckOff = true;
        try {
            JournalAudit.nc("MJR-K2 stopped", "purged a recording of genesis 99", audited);
        } finally {
            JournalRetention.Seams.reset();
        }
        live.stop();
    }

    /** The same for the active recording: a boundary of this genesis does not excuse a start of another epoch. */
    @Test
    public void anActiveRecordingThatStartsInAnotherEpochIsNotCut() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer mixed = rig.writer(p).append(1, 300, 99).append(1, 1_500);
        rig.set(p, "reader", 1_500);
        final Runnable audited = () -> {
            final JournalAudit.Snapshot before = rig.snapshot(p);
            final Report report = rig.pass(p);
            JournalAudit.check(before, rig.snapshot(p), truth(p).cursor("reader", 1_500));
            assertEquals(java.util.List.of(mixed.recordingId), report.anomalies);
        };
        audited.run();
        assertEquals(0, rig.start(mixed.recordingId));
        assertTrue(rig.err(), rig.err().contains("a recording of two epochs"));
        JournalRetention.Seams.epochCheckOff = true;
        try {
            JournalAudit.nc("MJR-K2 active", "purged a recording of genesis 99", audited);
        } finally {
            JournalRetention.Seams.reset();
        }
        mixed.stop();
    }

    /**
     * Epoch 0 at a recording's start is "written before the member had a genesis" (the core's order journal stamps 0
     * until a CoreConfig is recorded), not another genesis: the recording is cut at a boundary of this genesis that
     * every reader has applied, and it goes whole when it is covered. For a profile of an engine that never writes
     * before its genesis ({@code strictStartEpoch}, the matching engine) the same recording is an ANOMALY.
     */
    @Test
    public void epochZeroAtTheStartIsNoGenesisYetUnlessTheProfileIsStrict() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalProfile strict = p.strictStartEpoch();
        assertTrue(strict.startEpochStrict());
        assertFalse(p.startEpochStrict());
        // one run: 300 records before the genesis was recorded, then this genesis' (the sequence goes on)
        final JournalRig.Writer stopped = rig.writer(p).append(1, 300, 0).append(301, 1_500).stop();
        final JournalRig.Writer live = rig.writer(p).append(1, 300, 0).append(301, 3_000);
        rig.set(p, "reader", 2_500);

        // strict: neither the stopped recording (it would go whole) nor the active one is touched
        JournalAudit.Snapshot before = rig.snapshot(p);
        Report report = rig.pass(rig.retention(strict, JournalRetention.Mode.READERS), JournalRetention.OffBox.NONE);
        JournalAudit.check(before, rig.snapshot(p), truth(p).cursor("reader", 2_500));
        assertEquals(java.util.List.of(stopped.recordingId, live.recordingId), report.anomalies);
        assertEquals(0, rig.start(stopped.recordingId));
        assertEquals(0, rig.start(live.recordingId));
        assertTrue(rig.err(), rig.err().contains("journal recording " + live.recordingId + " ANOMALY: the record at"
            + " its start 0 carries epoch 0 (journalSeq 1), this member's genesis is " + JournalRig.GENESIS));
        // and the strict audit sees the lenient rule: the default profile's pass deletes epoch 0 records
        JournalAudit.nc("MJR-K2 strict", "purged a recording of genesis 0", () -> {
            final JournalAudit.Snapshot b = rig.snapshot(p);
            rig.pass(p);
            JournalAudit.check(b, rig.snapshot(p), truth(p).cursor("reader", 2_500));
        });

        // the default profile (what the pass above did): cut and deleted by sequence, nothing above the cursor
        assertFalse("the covered recording went whole", rig.port(p).list().stream()
            .anyMatch(r -> r.id() == stopped.recordingId));
        assertTrue("the active recording is cut", rig.start(live.recordingId) > 0);
        before = rig.snapshot(p);
        live.append(3_001, 4_000);
        rig.set(p, "reader", 3_600);
        report = rig.pass(p);
        final JournalAudit.Truth lenient = truth(p).cursor("reader", 3_600).epochZeroIsNoGenesis();
        JournalAudit.check(before, rig.snapshot(p), lenient);
        JournalAudit.checkKept(before, rig.snapshot(p), lenient, 3_600);
        assertTrue(report.anomalies.isEmpty());
        // a boundary must still be this genesis': a base whose first record is of epoch 0 never qualifies, so an
        // active recording whose every candidate base lies in its epoch 0 prefix is not cut
        final JournalRig.Writer prefix = rig.writer(p).append(1, 1_500, 0).append(1_501, 1_600);
        before = rig.snapshot(p);
        report = rig.pass(p);
        assertEquals("no base of epoch 0 qualifies", 0, rig.start(prefix.recordingId));
        assertTrue(report.anomalies.isEmpty());
        JournalAudit.check(before, rig.snapshot(p), lenient);
        prefix.stop();
        live.stop();
    }

    @Test
    public void aWrongSchemaFrameAtABoundaryDoesNotQualify() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer w = rig.writer(p);
        final int per = JournalRig.ORDER_PER_SEGMENT;
        // segments 0..2, then the first record of segment 3 carries another schema id, then segments 3..7
        w.append(1, 3L * per);
        w.one(3L * per + 1, JournalRig.GENESIS, JournalRig.logPositionOf(3L * per + 1), 9);
        w.append(3L * per + 2, 8L * per);
        // the reader's cursor makes base 3 the greatest base by sequence; its frame is not a record of the journal
        rig.set(p, "reader", 3L * per + 10);
        final JournalAudit.Snapshot before = rig.snapshot(p);
        final Report report = rig.pass(p);
        assertEquals(State.VALID, report.evaluation.state);
        assertEquals("the base below the wrong-schema frame is taken", 2L * SEG, rig.start(w.recordingId));
        assertTrue(rig.err(), rig.err().contains("CRITICAL: order journal recording " + w.recordingId + " ANOMALY: the"
            + " record at segment base " + 3L * SEG + " is unreadable (schema id 9, the journal's is 5): that base does"
            + " not qualify"));
        assertEquals("counted, so a journal that cannot be cut as far as its reader allows is seen",
            List.of(w.recordingId), report.anomalies);
        JournalAudit.check(before, rig.snapshot(p), truth(p).cursor("reader", 3L * per + 10));
    }

    @Test
    public void reportModePurgesNothingOnARealArchive() {
        final JournalProfile p = JournalRig.MONEY;
        final JournalRig.Writer w = rig.writer(p).append(1, 4_000);
        rig.set(p, "money-projector", 4_000);
        final Report report = rig.pass(rig.retention(p, Mode.REPORT), JournalRetention.OffBox.NONE);
        assertEquals(State.VALID, report.evaluation.state);
        assertEquals(0, report.purgedBytes);
        assertEquals(0, rig.start(w.recordingId));
        assertTrue(rig.out(), rig.out().contains("report: would purge money journal recording " + w.recordingId
            + ": start 0 -> "));
    }

    /** T14 on the kit's archive: a purge under a reader's replay is refused, counted, and done on a later pass. */
    @Test
    public void aPurgeUnderAReadersReplayIsRefusedCountedAndDoneLater() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer w = rig.writer(p).append(1, 2_600);
        rig.set(p, "reader", 2_600);
        // a reader that started at the recording's start and never polls: the archive's replay rests in segment 1
        final String channel = "aeron:ipc?term-length=64k";
        final Subscription slow = rig.aeron.addSubscription(channel, 4_911);
        final long session = rig.archive.startReplay(w.recordingId, 0, AeronArchive.NULL_LENGTH, channel, 4_911);
        final long deadline = System.nanoTime() + JournalRig.TIMEOUT_NS;
        while (slow.imageCount() == 0) {
            JournalRig.await(deadline, "the reader's replay to connect");
        }
        final JournalAudit.Snapshot before = rig.snapshot(p);
        Report report = rig.pass(p);
        assertEquals(State.VALID, report.evaluation.state);
        assertEquals("refused by the archive, counted", 1, report.purgeFailures);
        assertEquals(0, report.purgedBytes);
        assertEquals("nothing changed", 0, rig.start(w.recordingId));
        assertTrue(rig.out(), rig.out().contains("the archive refused to purge order journal recording "
            + w.recordingId));
        assertTrue(rig.out(), rig.out().contains("replay in progress"));
        // the reader moves on (here: it ends), the next pass purges
        rig.archive.stopReplay(session);
        slow.close();
        final long until = System.nanoTime() + JournalRig.TIMEOUT_NS;
        do {
            JournalRig.await(until, "a pass that purges after the replay ended");
            report = rig.pass(p);
        } while (report.purgedBytes == 0);
        assertEquals(0, report.purgeFailures);
        assertTrue(rig.start(w.recordingId) > 0);
        JournalAudit.check(before, rig.snapshot(p), truth(p).cursor("reader", 2_600));
    }

    @Test
    public void recordingsOfOtherStreamsAreNeverListedOrTouched() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer money = rig.writer(JournalRig.MONEY).append(1, 4_000).stop();
        final JournalRig.Writer order = rig.writer(p).append(1, 2_000);
        rig.set(p, "reader", 2_000);
        final List<JournalRetention.Recording> listed = rig.port(p).list();
        assertEquals(1, listed.size());
        assertEquals(order.recordingId, listed.get(0).id());
        rig.pass(p);
        assertTrue(rig.start(order.recordingId) > 0);
        assertEquals("the other stream's recording is whole", 0, rig.start(money.recordingId));
        assertEquals(1, rig.port(JournalRig.MONEY).list().size());
    }
}
