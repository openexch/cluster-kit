// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.journal.JournalRetention.Peek;
import com.openexchange.cluster.journal.JournalRetention.Recording;
import com.openexchange.cluster.shadow.ShadowReplay;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The last record of a recording as {@link JournalRetention.AeronPort#last} reads it, on a real archive, against two
 * independent answers: the record a replay of the whole recording gives, and the record the earlier method gave
 * (a replay of the last {@code w} bytes for every aligned {@code w}, then the last term). The earlier method is kept
 * here as {@link #probing}: it asks the archive for starts inside a frame, each of which the archive raises into its
 * error handler. The method under test must give the same record in every case and raise nothing.
 */
public class RecordingTailTest {

    private static final int TERM = JournalRig.SEG;
    private JournalRig rig;

    @Before
    public void setUp() {
        rig = new JournalRig();
    }

    @After
    public void tearDown() {
        rig.close();
    }

    private static long deadline() {
        return System.nanoTime() + JournalRig.TIMEOUT_NS;
    }

    // ------------------------------------------------------------------ the two other answers

    /** The earlier method, unchanged: windows of every aligned length below the end, then the last term. */
    private Peek probing(final JournalProfile profile, final Recording r) {
        final ShadowReplay replay = new ShadowReplay(rig.aeron, rig.archive, SystemNanoClock.INSTANCE);
        final long deadlineNs = deadline();
        final long stop = r.end();
        try {
            final int max = profile.maxFrameLength();
            final int steps = max / FrameDescriptor.FRAME_ALIGNMENT;
            for (int i = 0; i < steps; i++) {
                final int w = i == 0 ? max : i * FrameDescriptor.FRAME_ALIGNMENT;
                final long from = stop - w;
                if (from < r.start()) {
                    continue;
                }
                final Peek[] found = {null};
                final int[] messages = {0};
                final int[] frameLength = {0};
                try {
                    replay.readRange(r.id(), from, w, (buffer, offset, length, header) -> {
                        messages[0]++;
                        frameLength[0] = header.frameLength();
                        try {
                            found[0] = Peek.of(profile.key(buffer, offset, length, header.frameLength()));
                        } catch (final JournalProfile.NotARecord notARecord) {
                            found[0] = Peek.unreadable(notARecord.getMessage());
                        }
                    }, deadlineNs);
                } catch (final ArchiveException notAFrame) {
                    if (notAFrame.errorCode() == ArchiveException.INVALID_POSITION) {
                        continue;
                    }
                    throw notAFrame;
                }
                if (messages[0] == 1 && found[0] != null
                    && BitUtil.align(frameLength[0], FrameDescriptor.FRAME_ALIGNMENT) == w) {
                    return found[0];
                }
            }
            final long termStart = Math.max(r.start(), (stop - 1) & -(long) r.termLength());
            final Peek[] found = {null};
            replay.readRange(r.id(), termStart, stop - termStart, (buffer, offset, length, header) -> {
                try {
                    found[0] = Peek.of(profile.key(buffer, offset, length, header.frameLength()));
                } catch (final JournalProfile.NotARecord notARecord) {
                    found[0] = Peek.unreadable(notARecord.getMessage());
                }
            }, deadlineNs);
            return found[0] != null ? found[0]
                : Peek.unreadable("no journal message in the last term of recording " + r.id());
        } catch (final ArchiveException | ShadowReplay.ReplayTimeoutException
                       | ShadowReplay.ReplayJoinedLateException | ShadowReplay.ReplayEndedEarlyException
                       | IllegalStateException failed) {
            return Peek.unreadable(String.valueOf(failed));
        }
    }

    /** One message of a recording as a replay of the whole recording delivered it. */
    private record Message(long start, long end, Peek peek) {
    }

    /** Every message of {@code [r.start, r.end)}, in order. */
    private List<Message> scan(final JournalProfile profile, final Recording r) {
        final List<Message> out = new ArrayList<>();
        new ShadowReplay(rig.aeron, rig.archive, SystemNanoClock.INSTANCE).readAll(r.id(), r.start(), r.end(),
            (buffer, offset, length, header) -> {
                Peek peek;
                try {
                    peek = Peek.of(profile.key(buffer, offset, length, header.frameLength()));
                } catch (final JournalProfile.NotARecord notARecord) {
                    peek = Peek.unreadable(notARecord.getMessage());
                }
                final int aligned = BitUtil.align(header.frameLength(), FrameDescriptor.FRAME_ALIGNMENT);
                out.add(new Message(header.position() - aligned, header.position(), peek));
            }, deadline());
        return out;
    }

    /** The definition: the last message that lies in {@code [max(start, term base of end - 1), end)}. */
    private static Peek defined(final List<Message> all, final Recording r) {
        final long floor = Math.max(r.start(), (r.end() - 1) & -(long) r.termLength());
        Peek last = null;
        for (final Message m : all) {
            if (m.start() >= floor && m.end() <= r.end()) {
                last = m.peek();
            }
        }
        return last != null ? last : Peek.unreadable("no journal message in the last term of recording " + r.id());
    }

    /** Every position of {@code [r.start, r.end]} where a frame ends: message ends, and term ends after padding. */
    private static TreeSet<Long> frameEnds(final List<Message> all, final Recording r) {
        final TreeSet<Long> ends = new TreeSet<>();
        for (final Message m : all) {
            ends.add(m.end());
            if (m.start() % TERM == 0 && m.start() > r.start()) {
                ends.add(m.start()); // the end of the term before it: the end of its last record or of its padding
            }
        }
        return ends;
    }

    private Recording listed(final JournalRetention.AeronPort port, final long recordingId) {
        return port.list().stream().filter(r -> r.id() == recordingId).findFirst().orElseThrow();
    }

    private static Recording upTo(final Recording r, final long end) {
        return new Recording(r.id(), r.start(), end, r.active(), r.termLength(), r.segmentLength());
    }

    private static void same(final String what, final Peek expected, final Peek actual) {
        assertEquals(what, expected, actual);
    }

    private void noArchiveError(final String what) {
        assertTrue(what + ": the archive raised " + rig.archiveErrors, rig.archiveErrors.isEmpty());
    }

    // ------------------------------------------------------------------ the cases

    /** An empty recording has no last record, and no replay is asked for; a pass over it reads nothing. */
    @Test
    public void anEmptyRecordingHasNoLastRecordAndNoReplay() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer w = rig.writer(p);
        final JournalRetention.AeronPort port = rig.port(p);
        final Recording r = listed(port, w.recordingId);
        assertEquals(0, r.bytes());
        final Peek last = port.last(r, deadline());
        assertFalse(last.readable());
        assertEquals("no journal message in the last term of recording " + r.id(), last.unreadable());
        assertEquals("no replay", 0, port.tail().firstLooks() + port.tail().resumedReads());
        assertEquals(JournalRetention.State.NO_JOURNAL, rig.retention(p, JournalRetention.Mode.READERS).pass(port,
            JournalRig.GENESIS, JournalRetention.OffBox.NONE, System.currentTimeMillis(), deadline())
            .evaluation.state);
        noArchiveError("an empty recording");
    }

    /** One record, for the fixed-length journal and the journal of three record lengths. */
    @Test
    public void oneRecord() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final JournalRig.Writer w = rig.writer(p).append(1, 1);
            final JournalRetention.AeronPort port = rig.port(p);
            final Recording r = listed(port, w.recordingId);
            final Peek last = port.last(r, deadline());
            assertTrue(last.unreadable(), last.readable());
            assertEquals(1, last.key().journalSeq());
            same(p.journal() + ": the definition", defined(scan(p, r), r), last);
            noArchiveError(p.journal() + " one record");
            same(p.journal() + ": the earlier method", probing(p, r), last);
            rig.archiveErrors.clear();
        }
    }

    /**
     * Every end a recording can have, near every term end and at steps in between: the end exactly on a term base
     * after a full term (fixed 256-byte records fill a 64 KiB term exactly), the end on a term base after padding
     * (three record lengths do not), one record into a term, and every frame end around them. A port that has never
     * seen the recording, the definition and the earlier method agree on each.
     */
    @Test
    public void everyEndNearATermEndReadsTheRecordTheEarlierMethodRead() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final JournalRig.Writer w = rig.writer(p).append(1, p.hasEpoch() ? 3L * JournalRig.ORDER_PER_SEGMENT + 9
                : 2_100);
            final JournalRetention.AeronPort lister = rig.port(p);
            final Recording whole = listed(lister, w.recordingId);
            final List<Message> all = scan(p, whole);
            final List<Long> ends = new ArrayList<>(frameEnds(all, whole));
            assertTrue(p.journal() + ": at least three term ends", whole.end() > 3L * TERM);
            int cases = 0;
            int termEnds = 0;
            int afterPadding = 0;
            for (int i = 0; i < ends.size(); i++) {
                final long end = ends.get(i);
                final long toTermEnd = (TERM - (end % TERM)) % TERM;
                final boolean near = toTermEnd <= 4 * 256 || end % TERM <= 4 * 256 || i < 4 || i % 97 == 0
                    || i == ends.size() - 1;
                if (!near) {
                    continue;
                }
                final Recording r = upTo(whole, end);
                final Peek expected = defined(all, r);
                assertTrue(p.journal() + " end " + end + ": " + expected.unreadable(), expected.readable());
                final JournalRetention.AeronPort fresh = rig.port(p);
                same(p.journal() + " end " + end + ": a first look", expected, fresh.last(r, deadline()));
                noArchiveError(p.journal() + " end " + end);
                same(p.journal() + " end " + end + ": the earlier method", expected, probing(p, r));
                rig.archiveErrors.clear();
                cases++;
                if (end % TERM == 0) {
                    termEnds++;
                    final long lastMessageEnd = all.stream().filter(m -> m.end() <= end).mapToLong(Message::end)
                        .max().orElseThrow();
                    if (lastMessageEnd < end) {
                        afterPadding++;
                    }
                }
            }
            assertTrue(p.journal() + ": ends on a term base were read, " + termEnds, termEnds >= 3);
            if (p.hasEpoch()) {
                assertEquals("fixed records fill a term exactly: no padding", 0, afterPadding);
            } else {
                assertTrue("the three record lengths leave padding at a term end, " + afterPadding, afterPadding >= 1);
            }
            JournalRig.green(p.journal() + ": " + cases + " ends (" + termEnds + " on a term base, " + afterPadding
                + " after padding) read the same record three ways");
        }
    }

    /**
     * One port follows a recording as it grows: by one frame, by many, across a term end, onto a term end, onto
     * padding, by more than a term (a long pause), and without moving. Each look is the definition's record, and the
     * archive raises nothing. The same walk through the earlier method raises refusals: the control that shows the
     * counter of this test sees them.
     */
    @Test
    public void onePortFollowingAGrowingRecordingReadsTheDefinedRecordAndRaisesNothing() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final JournalRig.Writer w = rig.writer(p).append(1, p.hasEpoch() ? 5L * JournalRig.ORDER_PER_SEGMENT + 40
                : 3_600);
            final Recording whole = listed(rig.port(p), w.recordingId);
            final List<Message> all = scan(p, whole);
            final List<Long> ends = new ArrayList<>(frameEnds(all, whole));
            for (int seed = 0; seed < 3; seed++) {
            final JournalRetention.AeronPort port = rig.port(p);
            final Random random = new Random(20261127L + p.streamId() + seed);
            final List<Long> walk = new ArrayList<>();
            int i = 0;
            while (i < ends.size()) {
                walk.add(ends.get(i));
                final long end = ends.get(i);
                final int roll = random.nextInt(100);
                final int step;
                if (roll < 45) {
                    step = 1;
                } else if (roll < 80) {
                    step = 1 + random.nextInt(12);
                } else if (roll < 93) {
                    walk.add(end); // the end has not moved
                    step = 1;
                } else if (roll < 98) {
                    // onto the next term end exactly (after a full term, or after padding)
                    final long nextTermEnd = (end / TERM + 1) * TERM;
                    final int at = ends.indexOf(nextTermEnd);
                    step = at > i ? at - i : 1;
                } else {
                    step = 300 + random.nextInt(400); // more than one term between two looks
                }
                i += step;
            }
            walk.add(whole.end());
            long before = port.tail().firstLooks();
            int unchangedLooks = 0;
            long previous = -1;
            for (final long end : walk) {
                final Recording r = upTo(whole, end);
                same(p.journal() + " end " + end + " after " + previous, defined(all, r), port.last(r, deadline()));
                if (end == previous) {
                    unchangedLooks++;
                }
                previous = end;
            }
            noArchiveError(p.journal() + ": " + walk.size() + " looks by one port");
            assertTrue(p.journal() + ": looks that only read what was recorded since, "
                + port.tail().resumedReads(), port.tail().resumedReads() > walk.size() / 4);
            assertTrue(p.journal() + ": looks into a later term read from its base, " + (port.tail().firstLooks()
                - before), port.tail().firstLooks() - before >= 3);
            assertTrue(p.journal() + ": looks at an end that had not moved", unchangedLooks > 0);
            assertEquals(p.journal() + ": an end that has not moved is not replayed", unchangedLooks,
                port.tail().unchanged());
            JournalRig.green(p.journal() + ": " + walk.size() + " looks by one port (" + port.tail().firstLooks()
                + " from a term base or the start, " + port.tail().resumedReads() + " resumed, "
                + port.tail().unchanged() + " unchanged), no archive error");
            }
        }
        // the control: the earlier method on the journal of three record lengths asks for starts inside a frame
        final JournalProfile p = JournalRig.MONEY;
        final Recording whole = listed(rig.port(p), rig.port(p).list().get(0).id());
        final List<Message> all = scan(p, whole);
        int refused = 0;
        for (final long end : new ArrayList<>(frameEnds(all, whole)).subList(10, 40)) {
            probing(p, upTo(whole, end));
            refused += rig.archiveErrors.size();
            rig.archiveErrors.clear();
        }
        assertTrue("the earlier method raises refusals this test would see, " + refused, refused > 30);
    }

    /**
     * The end moves onto padding and the port resumes there: a replay that starts at a padding frame is a replay
     * from a frame start, and its record is the one before the padding.
     */
    @Test
    public void aLookThatResumesOntoPaddingAtATermEnd() {
        final JournalProfile p = JournalRig.MONEY;
        final JournalRig.Writer w = rig.writer(p).append(1, 1_500);
        final JournalRetention.AeronPort port = rig.port(p);
        final Recording whole = listed(port, w.recordingId);
        final List<Message> all = scan(p, whole);
        long padded = -1;
        long lastBefore = -1;
        for (long base = TERM; base < whole.end(); base += TERM) {
            final long termEnd = base;
            final long last = all.stream().filter(m -> m.end() <= termEnd).mapToLong(Message::end).max().orElseThrow();
            if (last < termEnd) {
                padded = termEnd;
                lastBefore = last;
                break;
            }
        }
        assertTrue("a term of this recording ends in padding", padded > 0);
        final Recording beforePadding = upTo(whole, lastBefore);
        final Peek record = port.last(beforePadding, deadline());
        same("the record before the padding", defined(all, beforePadding), record);
        assertEquals(lastBefore, port.tail().resumePosition(whole.id()));
        final long resumed = port.tail().resumedReads();
        final Recording onPadding = upTo(whole, padded);
        same("the end on the term base: the same record, read through the padding", record,
            port.last(onPadding, deadline()));
        assertEquals("the padding was replayed from its own start", resumed + 1, port.tail().resumedReads());
        same("the definition", defined(all, onPadding), record);
        noArchiveError("a replay that starts at a padding frame");
        same("the earlier method", probing(p, onPadding), record);
        rig.archiveErrors.clear();
        // the next record is the first of the next term: read from that term's base
        final long next = all.stream().filter(m -> m.start() == onPadding.end()).findFirst().orElseThrow().end();
        final Recording intoNext = upTo(whole, next);
        final long firstLooks = port.tail().firstLooks();
        same("the first record of the next term", defined(all, intoNext), port.last(intoNext, deadline()));
        assertEquals(firstLooks + 1, port.tail().firstLooks());
        noArchiveError("into the next term");
    }

    /**
     * A long pause: more than one term was recorded between two looks. The second look reads the last term only,
     * from its base, and gives the record the earlier method gave.
     */
    @Test
    public void moreThanOneTermBetweenTwoLooks() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final JournalRig.Writer w = rig.writer(p).append(1, p.hasEpoch() ? 40 : 90);
            final JournalRetention.AeronPort port = rig.port(p);
            final Recording early = listed(port, w.recordingId);
            assertTrue(early.end() < TERM);
            assertEquals(p.hasEpoch() ? 40 : 90, port.last(early, deadline()).key().journalSeq());
            final long total = p.hasEpoch() ? 4L * JournalRig.ORDER_PER_SEGMENT + 17 : 2_600;
            w.append((p.hasEpoch() ? 40 : 90) + 1, total);
            final Recording late = listed(port, w.recordingId);
            assertTrue(p.journal() + ": more than three terms later", late.end() - early.end() > 3L * TERM);
            final long firstLooks = port.tail().firstLooks();
            final long resumed = port.tail().resumedReads();
            final Peek last = port.last(late, deadline());
            assertEquals(total, last.key().journalSeq());
            assertEquals(p.journal() + ": read from the base of the last term", firstLooks + 1,
                port.tail().firstLooks());
            assertEquals(p.journal() + ": not resumed across the terms in between", resumed,
                port.tail().resumedReads());
            assertTrue(p.journal() + ": the next look resumes inside the last term",
                port.tail().resumePosition(late.id()) > ((late.end() - 1) & -(long) TERM));
            same(p.journal() + ": the definition", defined(scan(p, late), late), last);
            noArchiveError(p.journal() + " after a long pause");
            same(p.journal() + ": the earlier method", probing(p, late), last);
            rig.archiveErrors.clear();
        }
    }

    /** A stopped recording: read once, then answered from what the port holds while its end is the same. */
    @Test
    public void aStoppedRecordingIsReadOnceAndAnsweredTheSameAfterwards() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final JournalRig.Writer w = rig.writer(p).append(1, 700).stop();
            final JournalRetention.AeronPort port = rig.port(p);
            final Recording r = listed(port, w.recordingId);
            assertFalse(r.active());
            final Peek first = port.last(r, deadline());
            assertEquals(700, first.key().journalSeq());
            same(p.journal() + ": the definition", defined(scan(p, r), r), first);
            final long replays = port.tail().firstLooks() + port.tail().resumedReads();
            for (int i = 0; i < 3; i++) {
                same(p.journal() + ": look " + i, first, port.last(listed(port, w.recordingId), deadline()));
            }
            assertEquals(p.journal() + ": no further replay", replays, port.tail().firstLooks()
                + port.tail().resumedReads());
            same(p.journal() + ": a port after a restart", first, rig.port(p).last(r, deadline()));
            noArchiveError(p.journal() + " stopped");
            same(p.journal() + ": the earlier method", probing(p, r), first);
            rig.archiveErrors.clear();
        }
    }

    /**
     * An active recording that grows while it is read: each look is at the end the listing gave, a port kept across
     * the looks and a port made for the look (a restart) read the same record, and it is the record at that end.
     */
    @Test
    public void anActiveRecordingGrowingDuringTheReads() throws Exception {
        final JournalProfile p = JournalRig.MONEY;
        final JournalRig.Writer w = rig.writer(p).append(1, 50);
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final long[] written = {50};
        final Thread writer = new Thread(() -> {
            try {
                long seq = 51;
                while (!stop.get()) {
                    w.one(seq, JournalRig.GENESIS, JournalRig.logPositionOf(seq), p.schemaId());
                    written[0] = seq++;
                    if (seq % 4 == 0) {
                        Thread.sleep(1);
                    }
                }
            } catch (final Throwable t) {
                writerFailure.set(t);
            }
        }, "journal-writer");
        writer.start();
        final JournalRetention.AeronPort kept = rig.port(p);
        final List<Recording> looks = new ArrayList<>();
        final List<Peek> answers = new ArrayList<>();
        long lastSeq = 0;
        int moved = 0;
        try {
            final long until = System.nanoTime() + 3_000_000_000L;
            while (System.nanoTime() < until) {
                final Recording r = listed(kept, w.recordingId);
                assertTrue(r.active());
                final Peek a = kept.last(r, deadline());
                assertTrue(a.unreadable(), a.readable());
                same("a port made for this look", a, rig.port(p).last(r, deadline()));
                assertTrue("the tail never falls: " + a.key().journalSeq() + " after " + lastSeq,
                    a.key().journalSeq() >= lastSeq);
                if (a.key().journalSeq() > lastSeq) {
                    moved++;
                }
                lastSeq = a.key().journalSeq();
                looks.add(r);
                answers.add(a);
            }
        } finally {
            stop.set(true);
            writer.join(10_000);
        }
        assertEquals(null, writerFailure.get());
        w.sync();
        noArchiveError(looks.size() + " looks at a growing recording");
        assertTrue("the recording grew between looks, " + moved + " of " + looks.size(), moved > 20);
        assertTrue("looks that resumed inside a term, " + kept.tail().resumedReads(), kept.tail().resumedReads() > 20);
        assertTrue("it crossed term ends while it was read", looks.get(looks.size() - 1).end() > 3L * TERM);
        final Recording whole = listed(kept, w.recordingId);
        final List<Message> all = scan(p, whole);
        for (int i = 0; i < looks.size(); i++) {
            same("look " + i + " at end " + looks.get(i).end(), defined(all, looks.get(i)), answers.get(i));
        }
        same("the end", defined(all, whole), kept.last(whole, deadline()));
        assertEquals(written[0], kept.last(whole, deadline()).key().journalSeq());
        // the earlier method at a sample of the same ends
        for (int i = 0; i < looks.size(); i += Math.max(1, looks.size() / 25)) {
            same("the earlier method at end " + looks.get(i).end(), probing(p, looks.get(i)), answers.get(i));
        }
        JournalRig.green("money: " + looks.size() + " looks at a recording growing to " + whole.end() + " ("
            + kept.tail().resumedReads() + " resumed, " + kept.tail().firstLooks() + " from a term base, "
            + kept.tail().unchanged() + " unchanged)");
    }

    /**
     * The recording is purged past the position the port holds, and its end has moved on: the port reads from the
     * new range's floor. A purge that leaves the remembered record in place changes nothing.
     */
    @Test
    public void aRecordingPurgedPastTheRememberedPosition() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer w = rig.writer(p).append(1, 6L * JournalRig.ORDER_PER_SEGMENT + 30);
        final JournalRetention.AeronPort port = rig.port(p);
        final Recording whole = listed(port, w.recordingId);
        final List<Message> all = scan(p, whole);
        // the port saw the recording when its end was in the second term
        final Recording early = upTo(whole, TERM + 5L * 256);
        same("the early look", defined(all, early), port.last(early, deadline()));
        assertEquals(TERM + 5L * 256, port.tail().resumePosition(whole.id()));
        rig.archive.purgeSegments(w.recordingId, 3L * TERM);
        final Recording purged = listed(port, w.recordingId);
        assertEquals(3L * TERM, purged.start());
        final long firstLooks = port.tail().firstLooks();
        final Peek after = port.last(purged, deadline());
        assertEquals(6L * JournalRig.ORDER_PER_SEGMENT + 30, after.key().journalSeq());
        assertEquals("read from the floor of the new range", firstLooks + 1, port.tail().firstLooks());
        noArchiveError("after a purge past the remembered position");
        // an end in the first term left after the purge, by the same port: its floor is the new start
        final Recording justAfterStart = upTo(purged, 3L * TERM + 256);
        same("one record above the new start", defined(all, justAfterStart), port.last(justAfterStart, deadline()));
        same("the earlier method", probing(p, justAfterStart), port.last(justAfterStart, deadline()));
        rig.archiveErrors.clear();
        // a purge below the remembered record: the port resumes
        w.append(6L * JournalRig.ORDER_PER_SEGMENT + 31, 6L * JournalRig.ORDER_PER_SEGMENT + 40);
        final JournalRetention.AeronPort second = rig.port(p);
        final Recording head = listed(second, w.recordingId);
        second.last(head, deadline());
        rig.archive.purgeSegments(w.recordingId, 4L * TERM);
        w.append(6L * JournalRig.ORDER_PER_SEGMENT + 41, 6L * JournalRig.ORDER_PER_SEGMENT + 44);
        final long resumed = second.tail().resumedReads();
        assertEquals(6L * JournalRig.ORDER_PER_SEGMENT + 44, second.last(listed(second, w.recordingId), deadline())
            .key().journalSeq());
        assertEquals(resumed + 1, second.tail().resumedReads());
        noArchiveError("after a purge below the remembered position");
    }

    /** The last message is not a record of the journal: the same words as the earlier method, and no refusal. */
    @Test
    public void aLastMessageThatIsNotARecord() {
        final JournalProfile p = JournalRig.ORDER;
        final JournalRig.Writer w = rig.writer(p).append(1, 20);
        final JournalRetention.AeronPort port = rig.port(p);
        assertEquals(20, port.last(listed(port, w.recordingId), deadline()).key().journalSeq());
        w.one(21, JournalRig.GENESIS, JournalRig.logPositionOf(21), 9).sync();
        final Recording r = listed(port, w.recordingId);
        final Peek last = port.last(r, deadline());
        assertFalse(last.readable());
        assertEquals("schema id 9, the journal's is 5", last.unreadable());
        same("a port after a restart", last, rig.port(p).last(r, deadline()));
        noArchiveError("a last message of another schema");
        same("the earlier method", probing(p, r), last);
        rig.archiveErrors.clear();
        // and the tail is unknown, so nothing is purged
        final JournalRetention.Tail tail = rig.retention(p, JournalRetention.Mode.READERS).tail(port,
            JournalRig.GENESIS, deadline());
        assertTrue(String.valueOf(tail), tail instanceof JournalRetention.TailUnknown);
    }

    /**
     * A decoder that does not count a message (it answers null): the tail is the last message before it that
     * counts, on a first look and on a resumed one, as a read of the whole last term gives it.
     */
    @Test
    public void aMessageTheDecoderDoesNotCountIsPassedOver() {
        final JournalProfile p = JournalRig.ORDER;
        final ShadowReplay replay = new ShadowReplay(rig.aeron, rig.archive, SystemNanoClock.INSTANCE);
        final RecordingTail.Decoder<Long> counted = (buffer, offset, length, header) ->
            (buffer.getShort(offset + 4, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF) == p.schemaId()
                ? Long.valueOf(buffer.getLong(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN)) : null;
        final JournalRig.Writer w = rig.writer(p).append(1, 20);
        final RecordingTail<Long> kept = new RecordingTail<>(counted);
        Recording r = listed(rig.port(p), w.recordingId);
        assertEquals(Long.valueOf(20), kept.last(replay, r.id(), r.start(), r.end(), r.termLength(), deadline()));
        // two messages of another schema follow
        w.one(21, JournalRig.GENESIS, JournalRig.logPositionOf(21), 9);
        w.one(22, JournalRig.GENESIS, JournalRig.logPositionOf(22), 9).sync();
        r = listed(rig.port(p), w.recordingId);
        assertEquals("resumed over the two", Long.valueOf(20), kept.last(replay, r.id(), r.start(), r.end(),
            r.termLength(), deadline()));
        assertEquals("the next look resumes after them", r.end(), kept.resumePosition(r.id()));
        assertEquals("a first look", Long.valueOf(20), new RecordingTail<>(counted).last(replay, r.id(), r.start(),
            r.end(), r.termLength(), deadline()));
        w.append(23, 23);
        r = listed(rig.port(p), w.recordingId);
        assertEquals(Long.valueOf(23), kept.last(replay, r.id(), r.start(), r.end(), r.termLength(), deadline()));
        // a term whose only messages do not count has no tail, whatever was counted in the term before
        final long perTerm = JournalRig.ORDER_PER_SEGMENT;
        w.append(24, perTerm);
        w.one(perTerm + 1, JournalRig.GENESIS, JournalRig.logPositionOf(perTerm + 1), 9).sync();
        r = listed(rig.port(p), w.recordingId);
        assertEquals("one message into the second term", TERM + 256, r.end());
        assertEquals("the kept object", null, kept.last(replay, r.id(), r.start(), r.end(), r.termLength(),
            deadline()));
        assertEquals("a first look", null, new RecordingTail<>(counted).last(replay, r.id(), r.start(), r.end(),
            r.termLength(), deadline()));
        w.append(perTerm + 2, perTerm + 2);
        r = listed(rig.port(p), w.recordingId);
        assertEquals(Long.valueOf(perTerm + 2), kept.last(replay, r.id(), r.start(), r.end(), r.termLength(),
            deadline()));
        noArchiveError("a decoder that passes messages over");
    }

    /**
     * A resumed read that runs out of time: the look is unreadable (the tail is then unknown and nothing is
     * purged), nothing the port holds changes, and the next look resumes from the same place and reads the record.
     */
    @Test
    public void aResumedReadThatTimesOutChangesNothingAndTheNextLookIsRight() {
        final JournalProfile p = JournalRig.MONEY;
        final JournalRig.Writer w = rig.writer(p).append(1, 40);
        final JournalRetention.AeronPort port = rig.port(p);
        final Recording first = listed(port, w.recordingId);
        assertEquals(40, port.last(first, deadline()).key().journalSeq());
        final long resume = port.tail().resumePosition(first.id());
        assertEquals(first.end(), resume);
        w.append(41, 75);
        final Recording grown = listed(port, w.recordingId);
        // a deadline that has passed: the replay is started and gives up while it waits for its image
        final Peek timedOut = port.last(grown, System.nanoTime() - 1);
        assertFalse(timedOut.readable());
        assertTrue(timedOut.unreadable(), timedOut.unreadable().contains("REPLAY_TIMEOUT"));
        assertEquals("what the port holds is what it held", resume, port.tail().resumePosition(first.id()));
        final JournalRetention.Tail tail = rig.retention(p, JournalRetention.Mode.READERS).tail(port,
            JournalRig.GENESIS, System.nanoTime() - 1);
        assertTrue("an unreadable tail is unknown: " + tail, tail instanceof JournalRetention.TailUnknown);
        // the look at the old end is still answered, and the next look at the new end resumes
        assertEquals(40, port.last(first, deadline()).key().journalSeq());
        final long resumed = port.tail().resumedReads();
        final long firstLooks = port.tail().firstLooks();
        final Peek after = port.last(grown, deadline());
        assertEquals(75, after.key().journalSeq());
        same("the definition", defined(scan(p, grown), grown), after);
        assertEquals("resumed, from the same place", resumed + 1, port.tail().resumedReads());
        assertEquals(firstLooks, port.tail().firstLooks());
        noArchiveError("a timed out replay");
    }

    /**
     * The one case in which a remembered position is not a frame start: the recording was cut back and recorded on
     * with other bytes. The archive refuses the resumed replay once; the port forgets the recording and reads from
     * the floor in the same call, and the record is the one a read of the whole recording gives. No later look is
     * refused.
     */
    @Test
    public void aRememberedPositionTheArchiveRefusesIsForgottenAndReadFromTheFloor() {
        final JournalProfile p = JournalRig.MONEY;
        // records 6..10 are frames of 160, 96, 96, 160, 96 bytes
        final JournalRig.Writer w = rig.writer(p).append(1, 10);
        final JournalRetention.AeronPort port = rig.port(p);
        final Recording before = listed(port, w.recordingId);
        final List<Message> old = scan(p, before);
        assertEquals(10, port.last(before, deadline()).key().journalSeq());
        final long remembered = port.tail().resumePosition(before.id());
        assertEquals(before.end(), remembered);
        final long cut = old.get(4).end(); // after record 5
        w.truncateAndExtend(cut);
        // five frames of 160 bytes: the old end (cut + 608) is now inside the fourth of them
        for (int i = 0; i < 5; i++) {
            w.one(30 + 3L * i, JournalRig.GENESIS, JournalRig.logPositionOf(30 + 3L * i), p.schemaId());
        }
        w.sync();
        final Recording after = listed(port, w.recordingId);
        final List<Message> now = scan(p, after);
        assertTrue("the recording is longer than it was", after.end() > before.end());
        assertTrue("the remembered position is inside a frame now", now.stream().anyMatch(m -> m.start() < remembered
            && remembered < m.end()));
        noArchiveError("before the look");
        final long firstLooks = port.tail().firstLooks();
        final Peek last = port.last(after, deadline());
        assertTrue(last.unreadable(), last.readable());
        assertEquals(42, last.key().journalSeq());
        same("the definition", defined(now, after), last);
        assertEquals("read from the floor in the same call", firstLooks + 1, port.tail().firstLooks());
        assertEquals("the archive refused the remembered position once: " + rig.archiveErrors, 1,
            rig.archiveErrors.size());
        assertTrue(rig.archiveErrors.get(0), rig.archiveErrors.get(0).contains("does not point to a valid frame"));
        rig.archiveErrors.clear();
        w.one(45, JournalRig.GENESIS, JournalRig.logPositionOf(45), p.schemaId()).sync();
        assertEquals(45, port.last(listed(port, w.recordingId), deadline()).key().journalSeq());
        assertEquals(42, port.last(after, deadline()).key().journalSeq());
        noArchiveError("after the port forgot the recording");
    }

    /**
     * A recording that starts in the middle of a term (its publication began at a position that is no term base):
     * the floor of the first term is the recording's start, a frame start, not the term base below it.
     */
    @Test
    public void aRecordingThatStartsInTheMiddleOfATerm() {
        for (final JournalProfile p : new JournalProfile[] {JournalRig.ORDER, JournalRig.MONEY}) {
            final JournalRig.Writer w = rig.lateWriter(p, 7_264);
            w.append(1, 8);
            final JournalRetention.AeronPort port = rig.port(p);
            Recording r = listed(port, w.recordingId);
            assertEquals(p.journal() + ": the start is inside a term", 7_264, r.start());
            Peek last = port.last(r, deadline());
            assertEquals(p.journal(), 8, last.key().journalSeq());
            same(p.journal() + ": the definition", defined(scan(p, r), r), last);
            noArchiveError(p.journal() + ": a first look from a start inside a term");
            same(p.journal() + ": the earlier method", probing(p, r), last);
            rig.archiveErrors.clear();
            // on through the first term end and beyond, by the same port and by a new one
            final long total = p.hasEpoch() ? 2L * JournalRig.ORDER_PER_SEGMENT + 3 : 1_300;
            for (long to = 9; last.key().journalSeq() < total; to = Math.min(total, to + 97)) {
                w.append(last.key().journalSeq() + 1, to);
                r = listed(port, w.recordingId);
                last = port.last(r, deadline());
                assertEquals(p.journal(), to, last.key().journalSeq());
                same(p.journal() + " end " + r.end() + ": a new port", last, rig.port(p).last(r, deadline()));
            }
            assertTrue(r.end() > 2L * TERM);
            final List<Message> all = scan(p, r);
            // every end inside the first term: the floor is the start
            int inFirstTerm = 0;
            for (final long end : frameEnds(all, r)) {
                if (end > TERM) {
                    break;
                }
                final Recording upTo = upTo(r, end);
                same(p.journal() + " end " + end, defined(all, upTo), rig.port(p).last(upTo, deadline()));
                inFirstTerm++;
            }
            assertTrue(inFirstTerm > 20);
            noArchiveError(p.journal() + ": a recording that starts inside a term");
        }
    }

    /**
     * A recording the port saw is deleted and the port forgets it; passes over a journal of several recordings by
     * one port, while it grows, report the tail and raise nothing.
     */
    @Test
    public void passesByOnePortOverAGrowingJournalRaiseNothing() {
        final JournalProfile p = JournalRig.MONEY;
        final JournalRig.Writer old = rig.writer(p).append(1, 900).stop();
        final JournalRig.Writer live = rig.writer(p).append(901, 1_000);
        final JournalRetention.AeronPort port = rig.port(p);
        final JournalRetention retention = rig.retention(p, JournalRetention.Mode.READERS);
        rig.set(p, "reader", 0);
        long next = 1_001;
        for (int pass = 0; pass < 30; pass++) {
            final long to = next + (pass % 7 == 3 ? 900 : pass % 5 == 0 ? 0 : 23) - 1;
            if (to >= next) {
                live.append(next, to);
                next = to + 1;
            }
            final JournalRetention.Report report = retention.pass(port, JournalRig.GENESIS,
                JournalRetention.OffBox.NONE, System.currentTimeMillis(), deadline());
            assertEquals(JournalRetention.State.VALID, report.evaluation.state);
            assertEquals("pass " + pass, next - 1, report.tailSeq);
        }
        noArchiveError("30 passes by one port");
        assertTrue(port.tail().resumePosition(old.recordingId) > 0);
        rig.archive.purgeRecording(old.recordingId);
        port.list();
        assertEquals("a deleted recording is forgotten", -1, port.tail().resumePosition(old.recordingId));
        assertEquals(next - 1, retention.pass(port, JournalRig.GENESIS, JournalRetention.OffBox.NONE,
            System.currentTimeMillis(), deadline()).tailSeq);
        noArchiveError("after a recording was deleted");
    }
}
