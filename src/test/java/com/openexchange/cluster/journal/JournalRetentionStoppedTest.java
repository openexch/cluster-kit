// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.journal.JournalRetention.Report;
import com.openexchange.cluster.journal.JournalRetention.State;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Stopped recordings (the recordings earlier process runs left behind) on a real archive, for both journal shapes:
 * a whole delete only with tail cover, the shape where the newest recording ends below an older one, a cut that
 * keeps the last segment, and the member's tail the same after every pass as before it.
 */
public class JournalRetentionStoppedTest {

    private static final int SEG = JournalRig.SEG;
    private static final JournalProfile[] PROFILES = {JournalRig.ORDER, JournalRig.MONEY};
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

    /** Records per run that cross several segments for either shape. */
    private static long scale(final JournalProfile p) {
        return p.hasEpoch() ? 1 : 2;
    }

    private long tail(final JournalProfile p) {
        final JournalRetention.Tail tail = rig.retention(p, JournalRetention.Mode.READERS).tail(rig.port(p),
            JournalRig.GENESIS, System.nanoTime() + JournalRig.TIMEOUT_NS);
        assertTrue(String.valueOf(tail), tail instanceof JournalRetention.TailKnown);
        return ((JournalRetention.TailKnown) tail).seq();
    }

    private boolean exists(final JournalProfile p, final long recordingId) {
        return rig.port(p).list().stream().anyMatch(r -> r.id() == recordingId);
    }

    /** One audited pass: the audit, and the tail as the retention's own tail reader answers it. */
    private Report audited(final JournalProfile p, final long cursor) {
        final JournalAudit.Snapshot before = rig.snapshot(p);
        final long tailBefore = tail(p);
        final Report report = rig.pass(p);
        final JournalAudit.Snapshot after = rig.snapshot(p);
        final JournalAudit.Truth truth = truth(p).cursor("reader", cursor);
        JournalAudit.check(before, after, truth);
        JournalAudit.checkKept(before, after, truth, cursor);
        final long tailAfter = tail(p);
        if (tailAfter < tailBefore) {
            throw new AssertionError("journal tail fell from " + tailBefore + " to " + tailAfter);
        }
        assertEquals("the tail the pass reports is the tail before it", tailBefore, report.tailSeq);
        return report;
    }

    /** The normal case seconds after a restart: the old recording is covered by the new one and goes whole. */
    @Test
    public void aStoppedRecordingBelowTheBoundGoesWholeOnceTheNewOnePassedIt() {
        for (final JournalProfile p : PROFILES) {
            final long n = scale(p);
            final JournalRig.Writer old = rig.writer(p).append(1, 1_500 * n).stop();
            final JournalRig.Writer live = rig.writer(p).append(1_200 * n, 1_400 * n);
            rig.set(p, "reader", 1_500 * n);
            // the new recording has not passed the old one: the old one carries the tail and stays (its last
            // segment at least), although every record of it is applied
            Report report = audited(p, 1_500 * n);
            assertEquals(State.VALID, report.evaluation.state);
            assertTrue(p.journal() + ": the tail carrier is not deleted", exists(p, old.recordingId));
            assertEquals(p.journal() + ": it is cut to its last segment", (old.position() - 1) & -(long) SEG,
                rig.start(old.recordingId));
            assertEquals(2, report.recordings);
            // the new recording passes it: the old one goes whole
            live.append(1_400 * n + 1, 3_000 * n);
            rig.set(p, "reader", 2_500 * n);
            report = audited(p, 2_500 * n);
            assertFalse(p.journal() + ": deleted whole", exists(p, old.recordingId));
            assertEquals(1, report.recordings);
            assertEquals(3_000 * n, report.tailSeq);
            assertTrue(rig.out(), rig.out().contains(p.journal() + " journal recording " + old.recordingId
                + ": stopped, last journalSeq " + 1_500 * n + ", whole recording"));
            live.stop();
        }
    }

    /**
     * The shape where the newest recording ends below an older one (a run that crashed while it replayed): the older
     * one carries the tail and is kept; the shorter one is covered by it and goes.
     */
    @Test
    public void theOlderRecordingThatCarriesTheTailIsKept() {
        for (final JournalProfile p : PROFILES) {
            final long n = scale(p);
            final JournalRig.Writer older = rig.writer(p).append(1, 1_500 * n).stop();
            final JournalRig.Writer newer = rig.writer(p).append(1_000 * n, 1_300 * n).stop();
            rig.set(p, "reader", 1_500 * n);
            final Runnable pass = () -> audited(p, 1_500 * n);
            if (p == JournalRig.MONEY) {
                // T8's control: without tail cover the carrier is deleted and the tail reader's answer falls
                JournalRetention.Seams.tailCoverOff = true;
                try {
                    JournalAudit.nc("T8 " + p.journal(), "journal tail fell from " + 1_500 * n + " to ", pass);
                } finally {
                    JournalRetention.Seams.reset();
                }
                continue;
            }
            pass.run();
            assertTrue("the older recording carries the tail and stays", exists(p, older.recordingId));
            assertFalse("the newer, shorter one is covered and goes", exists(p, newer.recordingId));
            assertEquals(1_500 * n, tail(p));
            // again, with the rule off, on what is left: the carrier alone
            JournalRetention.Seams.tailCoverOff = true;
            try {
                JournalAudit.nc("T8 " + p.journal(), "journal tail fell from " + 1_500 * n + " to ", pass);
            } finally {
                JournalRetention.Seams.reset();
            }
        }
    }

    @Test
    public void aStoppedRecordingAboveTheBoundIsCutAndKeepsItsLastSegment() {
        for (final JournalProfile p : PROFILES) {
            final long n = scale(p);
            final JournalRig.Writer old = rig.writer(p).append(1, 1_500 * n).stop();
            final JournalRig.Writer live = rig.writer(p).append(1_400 * n, 1_600 * n);
            rig.set(p, "reader", 700 * n);
            final JournalAudit.Snapshot before = rig.snapshot(p);
            final Report report = audited(p, 700 * n);
            assertTrue(exists(p, old.recordingId));
            long expected = 0;
            for (final JournalAudit.Rec rec : before.recordings.get(old.recordingId).records()) {
                if (rec.position() % SEG == 0 && rec.seq() <= 700 * n + 1) {
                    expected = rec.position();
                }
            }
            assertTrue(expected > 0);
            assertEquals(p.journal() + ": cut to the greatest base at or below the bound", expected,
                rig.start(old.recordingId));
            assertEquals("the active recording is too short to cut", 0, rig.start(live.recordingId));
            assertEquals(expected, report.purgedBytes);
            // the reader catches up past the old recording: it goes whole, the active one covers it
            rig.set(p, "reader", 1_550 * n);
            audited(p, 1_550 * n);
            assertFalse(exists(p, old.recordingId));
            live.stop();
        }
    }

    /** Several restarts: every pass leaves the tail carrier and the active recording, never more. */
    @Test
    public void afterRestartsTheStoppedRecordingsGoAndTwoAtMostStay() {
        final JournalProfile p = JournalRig.ORDER;
        long head = 0;
        JournalRig.Writer live = null;
        for (int run = 0; run < 5; run++) {
            if (live != null) {
                live.stop();
            }
            // a restart replays from a snapshot below the old head and records again from there
            final long from = Math.max(1, head - 300);
            live = rig.writer(p).append(from, head + 700);
            head += 700;
            rig.set(p, "reader", head - 50);
            final Report report = audited(p, head - 50);
            assertTrue("run " + run + ": " + report.recordings + " recordings with bytes", report.recordings <= 2);
            assertEquals(head, report.tailSeq);
        }
        JournalRig.green("five runs: recordings with bytes stay at or below two");
    }
}
