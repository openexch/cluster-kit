// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowEntry.State;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.openexchange.cluster.shadow.ShadowRig.CHUNK;
import static com.openexchange.cluster.shadow.ShadowRig.GENESIS;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.TERM;
import static com.openexchange.cluster.shadow.ShadowRig.cursor;
import static com.openexchange.cluster.shadow.ShadowRig.header;
import static com.openexchange.cluster.shadow.ShadowRig.image;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static com.openexchange.cluster.shadow.ShadowRig.payload;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The reading half: the trailer rule against padding, session scoping, deadlines and cleanup. */
public class ShadowReplayReadTest {

    static final int MAX_FRAME_PAYLOAD = ShadowRig.MTU - 32;

    static long frames(final long length) {
        long total = 0;
        long left = length;
        while (left > 0) {
            final long n = Math.min(left, MAX_FRAME_PAYLOAD);
            total += (32 + n + 31) & ~31L;
            left -= n;
        }
        return total;
    }

    /**
     * Aeron's framing of one shadow (A0:EXP:632-660, 1005-1020): a message (all its fragments) that does not fit the
     * rest of the term is preceded by padding to the term end; padding is never written after a frame. Returns
     * {stop, padding before the trailer}.
     */
    static long[] layout(final int payloadLength) {
        final List<Long> messages = new ArrayList<>();
        messages.add((long)ShadowHeader.FRAME_LENGTH);
        for (int off = 0; off < payloadLength; off += CHUNK) {
            messages.add(frames(Math.min(CHUNK, payloadLength - off)));
        }
        messages.add((long)ShadowTrailer.FRAME_LENGTH);
        long p = 0;
        long padBeforeTrailer = 0;
        for (int i = 0; i < messages.size(); i++) {
            final long m = messages.get(i);
            final long room = TERM - p % TERM;
            if (m > room) {
                p += room;
                if (i == messages.size() - 1) {
                    padBeforeTrailer = room;
                }
            }
            p += m;
        }
        return new long[] {p, padBeforeTrailer};
    }

    static List<byte[]> window(final ShadowReplay replay, final long id, final long from, final int length) {
        final List<byte[]> out = new ArrayList<>();
        replay.readRange(id, from, length, (b, o, l, h) -> {
            final byte[] m = new byte[l];
            b.getBytes(o, m);
            out.add(m);
        }, ShadowRig.CLOCK.nanoTime() + ShadowRig.TIMEOUT_NS);
        return out;
    }

    @Test
    public void theTrailerIsTheLast96BytesEvenAfterPadding() {
        int base = -1;
        for (int len = 40_000; len < 400_000 && base < 0; len += 32) {
            if (layout(len)[1] > 0) {
                base = len;
            }
        }
        assertTrue("a padding case exists", base > 0);
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            final ShadowReplay replay = new ShadowReplay(rig.fixture.aeron(), rig.archive(), ShadowRig.CLOCK);
            int padded = 0;
            int equationFalse = 0;
            final List<String> seen = new ArrayList<>();
            for (int k = -4; k <= 4; k++) {
                final int len = base + 32 * k;
                final ShadowWriter.WriteResult r = ShadowRig.writeOk(w, image(len, 50 + k), ShadowKind.CADENCE,
                        cursor(10_000L + 64 * (k + 4)));
                final long stop = r.bytes();
                final long[] expected = layout(len);
                assertEquals("Aeron framed shadow " + len + " as the model says (padding before, never after)",
                        expected[0], stop);
                if (expected[1] > 0) {
                    padded++;
                    assertEquals("the trailer starts the next term", 0, (stop - ShadowTrailer.FRAME_LENGTH) % TERM);
                }
                final List<byte[]> t = window(replay, r.recordingId(), stop - ShadowTrailer.FRAME_LENGTH,
                        ShadowTrailer.FRAME_LENGTH);
                assertEquals(1, t.size());
                assertTrue(ShadowTrailer.decode(new UnsafeBuffer(t.get(0)), 0, t.get(0).length).ok());
                final List<byte[]> h = window(replay, r.recordingId(), 0, ShadowHeader.FRAME_LENGTH);
                assertEquals(1, h.size());
                assertEquals(ShadowHeader.LENGTH, h.get(0).length);
                if (stop != ShadowHeader.FRAME_LENGTH + len + ShadowTrailer.FRAME_LENGTH) {
                    equationFalse++;
                }
                seen.add(len + "->" + stop + (expected[1] > 0 ? " (" + expected[1] + " B padding before the trailer)" : ""));
            }
            assertTrue("the loop found a trailer right after padding: " + seen, padded > 0);
            assertTrue("(char) the R 2.5 length equation is false for at least one shadow", equationFalse > 0);
            ShadowRig.green("trailer after padding: " + padded + " padded of 9, equation false for " + equationFalse
                    + ": " + seen);
        }
    }

    static void assertNotComplete(final ShadowEntry e) {
        if (e.state() == State.COMPLETE || e.state() == State.VERIFIED) {
            throw new AssertionError("incomplete recording classified COMPLETE: " + e);
        }
    }

    @Test
    public void negativeControlNc3TheLengthEquationCallsACrashShapeComplete() {
        try (ShadowRig rig = new ShadowRig()) {
            // header + every chunk written, the trailer missing; one 4,128-byte chunk = three full 1,408-byte frames,
            // so stop - start = 160 + 4,128 + 96 exactly: the R 2.5 equation's "complete"
            final byte[] p = payload(image(3 * MAX_FRAME_PAYLOAD, 3));
            final ShadowRig.RawShadow crash = rig.raw(header(R3, 1, ShadowKind.ALIGNED, 100, GENESIS, 0, p), p,
                    ShadowRig.Raw.NO_TRAILER);
            assertEquals(ShadowHeader.FRAME_LENGTH + p.length + ShadowTrailer.FRAME_LENGTH, crash.stop());
            final ShadowEntry e = rig.catalog().scan().entry(crash.recordingId());
            assertNotComplete(e);
            nc("trailer rule replaced by the length equation", "incomplete recording classified COMPLETE",
                    () -> assertNotComplete(rig.catalog(100,
                            Set.of(NegativeControl.TRAILER_RULE_REPLACED_BY_LENGTH_EQUATION)).scan()
                            .entry(crash.recordingId())));
        }
    }

    static void assertOneTrailer(final List<byte[]> messages) {
        if (messages.size() >= 2) {
            throw new AssertionError("trailer window delivered " + messages.size() + " messages from more than one"
                    + " replay");
        }
        if (messages.size() != 1 || messages.get(0).length != ShadowTrailer.LENGTH) {
            throw new AssertionError("trailer window delivered no single trailer: " + messages.size() + " message(s)");
        }
    }

    /** A (386 KB) loader read paced by a slow handler, and a trailer window at the same time. */
    static void concurrent(final ShadowRig rig, final boolean scoped, final List<byte[]> trailerOut,
                           final byte[][] payloadOut, final Set<Integer> bareSessions) throws Exception {
        final ShadowWriter w = rig.writer(R3, 1, -1, ShadowWriter.Faults.NONE, Set.of(), ShadowRig.CLOCK);
        final int big = 386 * 1024;
        final ShadowWriter.WriteResult a = ShadowRig.writeOk(w, image(big, 1), ShadowKind.ALIGNED, cursor(100));
        final ShadowWriter.WriteResult b = ShadowRig.writeOk(w, image(CHUNK, 2), ShadowKind.ALIGNED, cursor(200));
        final CountDownLatch aStreaming = new CountDownLatch(1);
        final AtomicBoolean done = new AtomicBoolean();
        // (char) only: a bare subscription on the shared replay stream, draining whatever arrives
        final Subscription bare = bareSessions == null ? null
                : rig.fixture.aeron().addSubscription("aeron:ipc", ShadowAlias.REPLAY_STREAM_ID);
        final Thread drain = new Thread(() -> {
            while (bare != null && !done.get()) {
                if (bare.poll((buf, o, l, h) -> bareSessions.add(h.sessionId()), 64) == 0) {
                    Thread.yield();
                }
            }
        });
        drain.start();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread reader = new Thread(() -> {
            try {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final AtomicInteger n = new AtomicInteger();
                new ShadowReplay(rig.fixture.aeron(), rig.archive(), ShadowRig.CLOCK).readAll(a.recordingId(), 0,
                        a.bytes(), (buf, o, l, h) -> {
                            final int i = n.getAndIncrement();
                            if (i > 0 && l != ShadowTrailer.LENGTH) {
                                final byte[] m = new byte[l];
                                buf.getBytes(o, m);
                                out.writeBytes(m);
                            }
                            if (i == 3) {
                                aStreaming.countDown();
                            }
                            java.util.concurrent.locks.LockSupport.parkNanos(5_000_000L); // a slow loader
                        }, ShadowRig.CLOCK.nanoTime() + ShadowRig.TIMEOUT_NS);
                payloadOut[0] = out.toByteArray();
            } catch (final Throwable t) {
                failure.set(t);
            }
        });
        reader.start();
        try {
            // A streams, or (char run) A was refused at its join; either way B's window runs while A is live or done
            final long until = System.nanoTime() + 10_000_000_000L;
            while (aStreaming.getCount() > 0 && failure.get() == null && System.nanoTime() < until) {
                Thread.sleep(1);
            }
            assertTrue("A neither streamed nor failed", aStreaming.getCount() == 0 || failure.get() != null);
            final ShadowReplay trailerReplay = new ShadowReplay(rig.fixture.aeron(), rig.archive(), ShadowRig.CLOCK,
                    scoped, ShadowReplay.Observer.NONE);
            trailerOut.addAll(window(trailerReplay, b.recordingId(), b.bytes() - ShadowTrailer.FRAME_LENGTH,
                    ShadowTrailer.FRAME_LENGTH));
        } finally {
            reader.join(30_000);
            done.set(true);
            drain.join(5_000);
            if (bare != null) {
                bare.close();
            }
        }
        if (failure.get() != null) {
            if (bareSessions != null && failure.get() instanceof ShadowReplay.ReplayJoinedLateException) {
                payloadOut[0] = null; // (char) the bare subscriber connected A's replay early: refused, not misread
                return;
            }
            throw new AssertionError(failure.get());
        }
    }

    @Test
    public void twoConcurrentReplaysStayApartOnTheSharedStream() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            final List<byte[]> trailer = new ArrayList<>();
            final byte[][] got = new byte[1][];
            concurrent(rig, true, trailer, got, null);
            assertOneTrailer(trailer);
            assertArrayEquals(payload(image(386 * 1024, 1)), got[0]);
        }
        try (ShadowRig rig = new ShadowRig()) {
            // (char): a bare subscription on the replay stream receives both replays' fragments; it also connects a
            // replay publication before the scoped subscription joins, which the join check refuses (never misreads)
            final List<byte[]> trailer = new ArrayList<>();
            final byte[][] got = new byte[1][];
            final Set<Integer> bare = java.util.concurrent.ConcurrentHashMap.newKeySet();
            String outcome;
            try {
                concurrent(rig, true, trailer, got, bare);
                outcome = got[0] == null ? "A refused (joined late)" : "A read";
            } catch (final ShadowReplay.ReplayJoinedLateException late) {
                outcome = "B refused (joined late)";
            }
            assertTrue("(char) a bare subscription sees both replays: " + bare, bare.size() >= 2);
            if (got[0] != null) {
                assertArrayEquals("whatever is read is exact", payload(image(386 * 1024, 1)), got[0]);
            }
            ShadowRig.green("concurrent replays: trailer window 1 message, 386 KB byte-equal; (char) bare subscription"
                    + " saw " + bare.size() + " sessions, scoped reads under it: " + outcome);
        }
        try (ShadowRig rig = new ShadowRig()) {
            nc("replay subscription not session-scoped", "messages from more than one replay", () -> {
                final List<byte[]> trailer = new ArrayList<>();
                try {
                    concurrent(rig, false, trailer, new byte[1][], null);
                } catch (final Exception e) {
                    throw new AssertionError(e);
                }
                assertOneTrailer(trailer);
            });
        }
    }

    /** Opens the late-join window: a foreign subscriber drains the replay before ours is added. */
    static final class LateJoin implements ShadowReplay.Observer {
        final boolean checkOff;

        LateJoin(final boolean checkOff) {
            this.checkOff = checkOff;
        }

        @Override
        public void onReplayOpen(final long recordingId, final long from, final long length) {
        }

        @Override
        public void beforeSubscribe(final long replaySessionId) {
            java.util.concurrent.locks.LockSupport.parkNanos(200_000_000L);
        }

        @Override
        public boolean joinCheckOff() {
            return checkOff;
        }
    }

    static void assertNotCorrupt(final ShadowCatalog.DeepResult r) {
        if (r.kind() == ShadowCatalog.DeepResult.Kind.CORRUPT) {
            throw new AssertionError("a late join is unknown, never CORRUPT: " + r);
        }
    }

    @Test
    public void aLateJoinIsAnErrorNeverCorrupt() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            // larger than the replay window (half the 64 KiB term), so a stalled subscriber holds the replay open
            final ShadowWriter.WriteResult a = ShadowRig.writeOk(rig.writer(R3), image(CHUNK * 12, 1),
                    ShadowKind.ALIGNED, cursor(100));
            final ShadowEntry e = rig.catalog().scan().entry(a.recordingId());
            // a foreign bare subscriber on the replay stream: it connects the replay publication first, reads a few
            // fragments, stalls 400 ms (our subscription joins inside the stall, so it joins past `from`), then drains
            final Subscription bare = rig.fixture.aeron().addSubscription("aeron:ipc", ShadowAlias.REPLAY_STREAM_ID);
            final AtomicBoolean done = new AtomicBoolean();
            final Thread drain = new Thread(() -> {
                // per replay session: 5 fragments, then a 400 ms stall, then drain
                final java.util.Map<Integer, Integer> seen = new java.util.HashMap<>();
                final long[] stallUntil = {0};
                while (!done.get()) {
                    if (System.nanoTime() < stallUntil[0]) {
                        Thread.yield();
                        continue;
                    }
                    if (bare.poll((b, o, l, h) -> {
                        if (seen.merge(h.sessionId(), 1, Integer::sum) == 5) {
                            stallUntil[0] = System.nanoTime() + 400_000_000L;
                        }
                    }, 1) == 0) {
                        Thread.yield();
                    }
                }
            });
            drain.start();
            final ShadowCatalog.DeepResult late;
            try {
                late = rig.catalog(rig.archive(), 0, ShadowRig.GENESIS, 100, Set.of(), new LateJoin(false))
                        .verifyDeep(e);
                assertEquals(late.toString(), ShadowCatalog.DeepResult.Kind.SCAN_ERROR, late.kind());
                assertTrue(late.reason(), late.reason().contains("joined at"));
                assertNotCorrupt(late);
                // let the foreign subscriber's stall from the first replay end, so the next replay gets its own
                java.util.concurrent.locks.LockSupport.parkNanos(800_000_000L);
                nc("replay join-position check off", "a late join is unknown, never CORRUPT", () -> {
                    final ShadowCatalog.DeepResult r = new ShadowCatalog(rig.fixture.aeron(), rig.archive(),
                            ShadowRig.ENGINE, 0, ShadowRig.GENESIS, new ShadowVerdicts(), 100, ShadowRig.CLOCK,
                            ShadowRig.TIMEOUT_NS, new ShadowCounters(), new ShadowReplay(rig.fixture.aeron(),
                            rig.archive(), ShadowRig.CLOCK, true, new LateJoin(true)),
                            java.util.EnumSet.noneOf(NegativeControl.class)).verifyDeep(e);
                    System.out.println("DIAG join check off result " + r);
                    assertNotCorrupt(r);
                });
            } finally {
                done.set(true);
                drain.join(5_000);
                bare.close();
            }
            assertEquals("no verdict was stored by the late join; without the foreign subscriber the copy verifies",
                    ShadowCatalog.DeepResult.Kind.VERIFIED, rig.catalog().verifyDeep(e).kind());
            ShadowRig.green("late join under a foreign subscriber: " + late + ", then VERIFIED");
        }
    }

    /** Stops the next {@code armed} replays from a SECOND archive client right after the image is acquired. */
    static final class StopFromAnotherClient implements ShadowReplay.Observer {
        final AeronArchive other;
        final boolean endCheckOff;
        int armed;
        long sessionId;

        StopFromAnotherClient(final AeronArchive other, final int armed, final boolean endCheckOff) {
            this.other = other;
            this.armed = armed;
            this.endCheckOff = endCheckOff;
        }

        @Override
        public void beforeSubscribe(final long replaySessionId) {
            sessionId = replaySessionId;
        }

        @Override
        public void onReplayOpen(final long recordingId, final long from, final long length) {
            if (armed > 0) {
                armed--;
                other.stopReplay(sessionId);
            }
        }

        @Override
        public boolean endCheckOff() {
            return endCheckOff;
        }
    }

    static void assertEarlyEndIsNotCorrupt(final ShadowCatalog.DeepResult r) {
        if (r.kind() == ShadowCatalog.DeepResult.Kind.CORRUPT) {
            throw new AssertionError("a replay stopped early was read as CORRUPT: " + r);
        }
    }

    /**
     * A replay stopped by another archive client (as {@code stopAllReplays} from the CLI, or a segment
     * read error) ends with end of stream at a partial position. That is SCAN_ERROR with no verdict, never CORRUPT
     * (final) or NO_TRAILER (cached); the next read VERIFIES. Negative control: the end-position check off reads it as CORRUPT.
     */
    @Test
    public void aReplayStoppedEarlyIsAnErrorNeverAVerdict() {
        try (ShadowRig rig = new ShadowRig()) {
            // larger than the replay window, so the replay is still running when it is stopped
            final ShadowWriter.WriteResult a = ShadowRig.writeOk(rig.writer(R3), image(CHUNK * 12, 1),
                    ShadowKind.ALIGNED, cursor(100));
            final io.aeron.archive.client.AeronArchive other = rig.fixture.newArchiveClient(rig.fixture.aeron());
            try {
                final ShadowEntry e = rig.catalog().scan().entry(a.recordingId());
                final StopFromAnotherClient stop = new StopFromAnotherClient(other, 1, false);
                final ShadowCatalog deep = rig.catalog(rig.archive(), 0, ShadowRig.GENESIS, 100, Set.of(), stop);
                final ShadowCatalog.DeepResult r = deep.verifyDeep(e);
                assertEquals(r.toString(), ShadowCatalog.DeepResult.Kind.SCAN_ERROR, r.kind());
                assertEquals("no verdict stored", null, rig.verdicts.get(e.key()));
                assertEquals("the next read verifies", ShadowCatalog.DeepResult.Kind.VERIFIED, deep.verifyDeep(e).kind());
                // the trailer window of a scan: SCAN_ERROR, not NO_TRAILER, and not cached
                final ShadowWriter.WriteResult b = ShadowRig.writeOk(rig.writer(R3, 2, 100, ShadowWriter.Faults.NONE,
                        Set.of(), ShadowRig.CLOCK), image(CHUNK * 12, 2), ShadowKind.ALIGNED, cursor(200));
                final StopFromAnotherClient stopWindow = new StopFromAnotherClient(other, 1, false);
                final ShadowCatalog windows = rig.catalog(rig.archive(), 0, ShadowRig.GENESIS, 100, Set.of(),
                        stopWindow);
                final ShadowEntry first = windows.scan().entry(b.recordingId());
                assertTrue("a stopped window is never NO_TRAILER: " + first, first.state() == ShadowEntry.State.SCAN_ERROR
                        || first.state() == ShadowEntry.State.COMPLETE);
                assertEquals("re-classified on the next scan", ShadowEntry.State.COMPLETE,
                        windows.scan().entry(b.recordingId()).state());
                // Negative control: the end-position check off; up to 10 attempts (the clean end-of-stream path is a race with
                // the client's image close, which reads as "closed before end of stream" = ERROR either way)
                final List<String> seen = new ArrayList<>();
                nc("replay end-position check off", "a replay stopped early was read as CORRUPT", () -> {
                    for (int k = 0; k < 10; k++) {
                        final ShadowCatalog loose = new ShadowCatalog(rig.fixture.aeron(), rig.archive(),
                                ShadowRig.ENGINE, 0, ShadowRig.GENESIS, new ShadowVerdicts(), 100, ShadowRig.CLOCK,
                                ShadowRig.TIMEOUT_NS, new ShadowCounters(), new ShadowReplay(rig.fixture.aeron(),
                                rig.archive(), ShadowRig.CLOCK, true, new StopFromAnotherClient(other, 1, true)),
                                java.util.EnumSet.noneOf(NegativeControl.class));
                        final ShadowCatalog.DeepResult x = loose.verifyDeep(e);
                        seen.add(x.kind().name());
                        assertEarlyEndIsNotCorrupt(x);
                    }
                });
                ShadowRig.green("replay stopped early: " + r + "; window " + first + "; end check off attempts " + seen);
            } finally {
                org.agrona.CloseHelper.quietClose(other);
            }
        }
    }

    @Test
    public void aReplayThatNeverEndsTimesOutAndLeavesNothingBehind() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowHeader h = header(R3, 1, ShadowKind.ALIGNED, 100, GENESIS, 0, new byte[64]);
            final RecordedLogFixture.RawRecording live = rig.fixture.record("aeron:ipc?term-length=64k|"
                    + ShadowAlias.PARAM + h.alias().format(), ShadowAlias.STREAM_ID);
            final UnsafeBuffer hb = new UnsafeBuffer(new byte[ShadowHeader.LENGTH]);
            h.encode(hb, 0);
            live.offer(hb, 0, ShadowHeader.LENGTH);
            final long sessions = rig.fixture.replaySessionCount();
            final int subscriptions = rig.recordingSubscriptions();
            final ShadowReplay replay = new ShadowReplay(rig.fixture.aeron(), rig.archive(), ShadowRig.CLOCK);
            final long t0 = System.nanoTime();
            try {
                // the recording is live and nothing more comes: no end of stream (a paused source)
                replay.readRange(live.recordingId(), 0, AeronArchive.NULL_LENGTH, (b, o, l, x) -> { },
                        ShadowRig.CLOCK.nanoTime() + 500_000_000L);
                fail("no timeout");
            } catch (final ShadowReplay.ReplayTimeoutException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().startsWith("REPLAY_TIMEOUT"));
            }
            assertTrue("within the deadline", System.nanoTime() - t0 < 5_000_000_000L);
            final long until = System.nanoTime() + ShadowRig.TIMEOUT_NS;
            while (rig.fixture.replaySessionCount() != sessions && System.nanoTime() < until) {
                Thread.yield();
            }
            assertEquals("stopReplay was called", sessions, rig.fixture.replaySessionCount());
            assertEquals(subscriptions, rig.recordingSubscriptions());
            assertTrue(sessions >= 0);
            ShadowRig.green("replay timeout: replay sessions back to " + sessions + ", recording subscriptions "
                    + subscriptions);
        }
    }
}
