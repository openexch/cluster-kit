// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Image;
import io.aeron.RethrowingErrorHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.openexchange.cluster.log.Logs.MTU;
import static com.openexchange.cluster.log.Logs.TERM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** RCS S2 4.2: the bound, the failure rule, the public surface and contiguity across images (BLOCKER-1). */
public class ClusterLogReaderBoundTest {

    private static RecordedLogFixture fixture;

    @BeforeClass
    public static void launch() {
        fixture = new RecordedLogFixture();
    }

    @AfterClass
    public static void shutdown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte)(seed * 7 + i);
        }
        return b;
    }

    private record Log(RecordedLogFixture.Recording rec, long[] ends, long stop) { }

    /** Small messages m0..m(n-1); returns their frame ends. */
    private static Log smallMessages(final int n) {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final long[] ends = new long[n];
        for (int i = 0; i < n; i++) {
            ends[i] = rec.writer().message(3, 100 + i, bytes(120, i));
        }
        return new Log(rec, ends, rec.stopRecording());
    }

    private static Image replayAll(final Log log) {
        return fixture.replay(log.rec().recordingId(), 0, AeronArchive.NULL_LENGTH);
    }

    // --------------------------------------------------------------------------------------------------- bound

    @Test
    public void frameCrossingTheBoundIsNotDeliveredNorConsumed() {
        final Log log = smallMessages(4);
        final long bound = log.ends()[0] + 32; // inside m1
        final Image image = replayAll(log);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.pollUntil(reader, image, bound, reader::boundReached);
        assertEquals(List.of(log.ends()[0]), trace.positions());
        assertEquals(log.ends()[0], reader.position());
        assertEquals(log.ends()[0], image.position());
        assertEquals(log.ends()[0], reader.consumedPosition());
        assertTrue(reader.boundReached());
    }

    @Test
    public void characterizationImageChecksTheBoundOnlyAtTheFragmentStart() {
        final Log log = smallMessages(4);
        final long bound = log.ends()[0] + 32;
        final Image image = replayAll(log);
        final List<Long> seen = new ArrayList<>();
        final ControlledFragmentHandler plain = (buffer, offset, length, header) -> {
            seen.add(header.position());
            return ControlledFragmentHandler.Action.CONTINUE;
        };
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (seen.size() < 2 && System.nanoTime() < deadline) {
            image.boundedControlledPoll(plain, bound, 10);
            Thread.yield();
        }
        assertEquals("Image delivers the frame that starts before the bound and ends after it (A0:IMG:590)",
                List.of(log.ends()[0], log.ends()[1]), seen);
        assertTrue(log.ends()[1] > bound);
    }

    @Test
    public void fragmentedMessageAcrossTheBoundKeepsItsBeginAndIsDeliveredOnceAtItsEnd() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final long first = w.message(3, 1, bytes(40, 1));
        final byte[] big = bytes(3 * MTU, 2);
        final long bigEnd = w.message(3, 2, big);
        final long last = w.message(3, 3, bytes(40, 3));
        final long stop = rec.stopRecording();
        final long beginEnd = Logs.beginEnd(first);
        final long bound = beginEnd + 32;
        assertTrue(beginEnd <= bound && bound < bigEnd);

        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.pollUntil(reader, image, bound, reader::boundReached);
        assertEquals(List.of(first), trace.positions());
        assertTrue(reader.midMessage());
        assertEquals(first, reader.position());
        assertEquals(beginEnd, reader.consumedPosition());
        assertEquals(beginEnd, image.position());

        Logs.drain(reader, image, bigEnd);
        assertEquals(List.of(first, bigEnd), trace.positions());
        assertEquals(LogFrame.message(bigEnd, 3, 2, big), trace.frames.get(1));
        assertFalse(reader.midMessage());
        Logs.drain(reader, image, stop);
        assertEquals(List.of(first, bigEnd, last), trace.positions());
    }

    @Test
    public void triggerEndsThePollRightAfterTheAction() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final long m0 = w.message(3, 1, bytes(40, 1));
        final long action = w.action(ClusterAction.SNAPSHOT, 2, 0);
        final long m1 = w.message(3, 3, bytes(40, 3));
        final long stop = rec.stopRecording();
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        trace.breakOnAction = true;
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.pollUntil(reader, image, Long.MAX_VALUE, () -> reader.position() == action);
        assertEquals(List.of(m0, action), trace.positions());
        assertEquals(action, image.position());
        assertEquals(action, reader.consumedPosition());
        trace.breakOnAction = false;
        Logs.drain(reader, image, stop);
        assertEquals(List.of(m0, action, m1), trace.positions());
    }

    @Test
    public void boundBelowTheImagePositionChangesNothing() {
        final Log log = smallMessages(4);
        final Image image = replayAll(log);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.drain(reader, image, log.ends()[1]);
        assertEquals(0, reader.poll(image, log.ends()[0]));
        assertEquals(0, reader.poll(image, log.ends()[1]));
        assertEquals(log.ends()[1], reader.position());
        assertEquals(log.ends()[1], reader.consumedPosition());
        assertEquals(2, reader.count(FrameKind.SESSION_MESSAGE));
        assertFalse(reader.boundReached());
    }

    @Test
    public void fragmentLimitIsRespectedAcrossASplitMessage() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final byte[] big = bytes(3 * MTU, 4);
        final long bigEnd = rec.writer().message(3, 1, big);
        rec.stopRecording();
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 1, 0);
        int polls = 0;
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (reader.position() < bigEnd && System.nanoTime() < deadline) {
            final int n = reader.poll(image, Long.MAX_VALUE);
            assertTrue("one fragment per poll at most", n <= 1);
            if (n == 1) {
                polls++;
                if (reader.position() < bigEnd) {
                    assertTrue(reader.midMessage());
                    assertTrue(trace.frames.isEmpty());
                }
            }
        }
        assertEquals("3 x MTU of payload plus the session header is 4 fragments", 4, polls);
        assertEquals(List.of(LogFrame.message(bigEnd, 3, 1, big)), trace.frames);
    }

    // ------------------------------------------------------------------------------------------- failure rule

    @Test
    public void listenerThrowingOnAnUnfragmentedFrameStopsInFrontOfIt() {
        final Log log = smallMessages(4);
        final Image image = replayAll(log);
        final Trace trace = new Trace();
        trace.throwAt = p -> p == log.ends()[1];
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        try {
            Logs.drain(reader, image, log.stop());
            fail("listener failure must stop the reader");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.LISTENER_FAILED, expected.stop().reason());
            assertSame(trace.thrown, expected.getCause());
            assertSame(trace.thrown, expected.stop().cause());
        }
        assertEquals("the image stays at the start of the failing fragment", log.ends()[0], image.position());
        assertEquals(log.ends()[0], reader.consumedPosition());
        assertEquals(List.of(log.ends()[0]), trace.positions());
        for (int i = 0; i < 2; i++) {
            try {
                reader.poll(image, Long.MAX_VALUE);
                fail("every later poll throws");
            } catch (final ClusterLogReaderStoppedException expected) {
                assertEquals(ReaderStop.Reason.LISTENER_FAILED, expected.stop().reason());
            }
        }
        assertEquals(log.ends()[0], image.position());
    }

    @Test
    public void listenerThrowingOnAnEndFragmentLeavesTheBeginConsumed() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final long first = w.message(3, 1, bytes(40, 1));
        final byte[] big = bytes(3 * MTU, 2);
        final long bigEnd = w.message(3, 2, big);
        final long stop = rec.stopRecording();
        final long endStart = Logs.endFragmentStart(bigEnd, 32 + big.length);
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        trace.throwAt = p -> p == bigEnd;
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        try {
            Logs.drain(reader, image, stop);
            fail("listener failure must stop the reader");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.LISTENER_FAILED, expected.stop().reason());
        }
        assertEquals("the image stays at the start of the END fragment", endStart, image.position());
        assertTrue(endStart > first);
        assertTrue("BEGIN stays consumed: still mid-message after the stop", reader.midMessage());
        assertEquals(first, reader.position());
    }

    @Test
    public void characterizationImageAdvancesPastAFailingFragmentUnderBothErrorHandlers() {
        // Default subscriber error handler: logged and swallowed, image past the fragment.
        final Log log = smallMessages(3);
        final Image image = replayAll(log);
        final RuntimeException boom = new IllegalStateException("characterization: handler failure");
        final ControlledFragmentHandler throwing = (buffer, offset, length, header) -> {
            if (header.position() == log.ends()[1]) {
                throw boom;
            }
            return ControlledFragmentHandler.Action.CONTINUE;
        };
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (image.position() < log.ends()[1] && System.nanoTime() < deadline) {
            image.boundedControlledPoll(throwing, Long.MAX_VALUE, 10);
        }
        assertEquals("default handler: swallowed and the image is past the failing fragment", log.ends()[1],
                image.position());

        // RethrowingErrorHandler (what the service container and ClusterBackup enforce): propagates, still past.
        try (RecordedLogFixture rethrowing = new RecordedLogFixture(RethrowingErrorHandler.INSTANCE)) {
            final RecordedLogFixture.Recording rec = rethrowing.record(TERM, MTU, 0);
            final long[] ends = new long[3];
            for (int i = 0; i < 3; i++) {
                ends[i] = rec.writer().message(3, i, bytes(120, i));
            }
            rec.stopRecording();
            final Image rethrowImage = rethrowing.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
            final ControlledFragmentHandler failing = (buffer, offset, length, header) -> {
                if (header.position() == ends[1]) {
                    throw boom;
                }
                return ControlledFragmentHandler.Action.CONTINUE;
            };
            boolean propagated = false;
            final long until = System.nanoTime() + 10_000_000_000L;
            while (!propagated && System.nanoTime() < until) {
                try {
                    rethrowImage.boundedControlledPoll(failing, Long.MAX_VALUE, 10);
                } catch (final IllegalStateException e) {
                    assertSame(boom, e);
                    propagated = true;
                }
            }
            assertTrue("RethrowingErrorHandler propagates the handler exception", propagated);
            assertEquals("and the image is still past the failing fragment", ends[1], rethrowImage.position());
        }
    }

    // ------------------------------------------------------------------------------------------ public surface

    @Test
    public void readerIsNotAFragmentHandlerSoPollIsTheOnlyEntry() {
        assertFalse(ControlledFragmentHandler.class.isAssignableFrom(ClusterLogReader.class));
        assertFalse(io.aeron.logbuffer.FragmentHandler.class.isAssignableFrom(ClusterLogReader.class));
    }

    @Test
    public void imageNotAtTheStartPositionStopsStartMismatch() {
        final Log log = smallMessages(3);
        final Image image = fixture.replay(log.rec().recordingId(), log.ends()[0], AeronArchive.NULL_LENGTH);
        final ClusterLogReader reader = new ClusterLogReader(new Trace(), 10, 0);
        try {
            reader.poll(image, Long.MAX_VALUE);
            fail("START_MISMATCH expected");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.START_MISMATCH, expected.stop().reason());
            assertEquals(log.ends()[0], expected.stop().framePosition());
        }
        assertTrue(reader.stopped());
    }

    @Test
    public void aDifferentImageStopsImageChanged() {
        final Log log = smallMessages(3);
        final Image a = replayAll(log);
        final Image b = replayAll(log);
        final ClusterLogReader reader = new ClusterLogReader(new Trace(), 10, 0);
        reader.poll(a, log.ends()[0]);
        try {
            reader.poll(b, Long.MAX_VALUE);
            fail("IMAGE_CHANGED expected");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.IMAGE_CHANGED, expected.stop().reason());
        }
    }

    // ------------------------------------------------------------------------------------- contiguity (BLOCKER-1)

    @Test
    public void successorJoiningAcrossAGapStopsStartMismatch() {
        final Log first = smallMessages(2);
        final long gapStart = first.stop() + 64;
        final RecordedLogFixture.Recording second = fixture.record(TERM, MTU, gapStart);
        final long secondEnd = second.writer().message(3, 9, bytes(120, 9));
        second.stopRecording();

        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        final Image a = replayAll(first);
        Logs.drain(reader, a, first.stop());
        final Image b = fixture.replay(second.recordingId(), gapStart, AeronArchive.NULL_LENGTH);
        final ClusterLogReader next = ClusterLogReader.successor(reader, trace, 10);
        assertEquals(first.stop(), next.startPosition());
        try {
            next.poll(b, Long.MAX_VALUE);
            fail("a 64-byte gap must stop the successor");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.START_MISMATCH, expected.stop().reason());
            assertEquals(gapStart, expected.stop().framePosition());
        }
        assertEquals(List.of(first.ends()[0], first.ends()[1]), trace.positions());
        assertTrue(secondEnd > gapStart);
    }

    @Test
    public void characterizationHandBuiltReadersAcceptTheGapSilently() {
        final Log first = smallMessages(2);
        final long gapStart = first.stop() + 64;
        final RecordedLogFixture.Recording second = fixture.record(TERM, MTU, gapStart);
        final long secondEnd = second.writer().message(3, 9, bytes(120, 9));
        second.stopRecording();

        final Trace trace = new Trace();
        final ClusterLogReader r1 = new ClusterLogReader(trace, 10, 0);
        Logs.drain(r1, replayAll(first), first.stop());
        final ClusterLogReader r2 = new ClusterLogReader(trace, 10, gapStart); // built with the image's own start
        Logs.drain(r2, fixture.replay(second.recordingId(), gapStart, AeronArchive.NULL_LENGTH), secondEnd);
        assertEquals("the gap passes unnoticed without successor", List.of(first.ends()[0], first.ends()[1],
                secondEnd), trace.positions());
    }

    @Test
    public void successorAfterALogTruncatedInsideAMessageAbandonsThePartial() {
        final RecordedLogFixture.Recording rec1 = fixture.record(TERM, MTU, 0);
        final long m0 = rec1.writer().message(3, 1, bytes(40, 1));
        rec1.writer().message(3, 2, bytes(3 * MTU, 2));
        rec1.stopRecording();
        final long cut = Logs.beginEnd(m0);
        final RecordedLogFixture.Recording rec2 = fixture.record(TERM, MTU, cut);
        final byte[] n0 = bytes(60, 5);
        final long e0 = rec2.writer().message(3, 5, n0);
        final byte[] n1 = bytes(2 * MTU, 6);
        final long e1 = rec2.writer().message(3, 6, n1);
        rec2.stopRecording();

        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.drain(reader, fixture.replay(rec1.recordingId(), 0, cut), Long.MAX_VALUE);
        assertTrue(reader.midMessage());
        assertEquals(cut, reader.consumedPosition());
        final ClusterLogReader next = ClusterLogReader.successor(reader, trace, 10);
        Logs.drain(next, fixture.replay(rec2.recordingId(), cut, AeronArchive.NULL_LENGTH), e1);
        assertFalse(next.stopped());
        assertEquals(1, next.abandonedPartialCount());
        assertEquals(List.of(LogFrame.message(m0, 3, 1, bytes(40, 1)), LogFrame.message(e0, 3, 5, n0),
                LogFrame.message(e1, 3, 6, n1)), trace.frames);
    }

    @Test
    public void successorAcrossTwoBoundedReplaysSplitInsideAMessageCompletesItOnce() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final long m0 = rec.writer().message(3, 1, bytes(40, 1));
        final byte[] big = bytes(3 * MTU, 2);
        final long bigEnd = rec.writer().message(3, 2, big);
        final long m2 = rec.writer().message(3, 3, bytes(40, 3));
        final long stop = rec.stopRecording();
        final long cut = Logs.beginEnd(m0);

        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.drain(reader, fixture.replay(rec.recordingId(), 0, cut), Long.MAX_VALUE);
        assertTrue(reader.midMessage());
        final ClusterLogReader next = ClusterLogReader.successor(reader, trace, 10);
        Logs.drain(next, fixture.replay(rec.recordingId(), cut, AeronArchive.NULL_LENGTH), stop);
        assertEquals(0, next.abandonedPartialCount());
        assertEquals(List.of(m0, bigEnd, m2), trace.positions());
        assertEquals(LogFrame.message(bigEnd, 3, 2, big), trace.frames.get(1));
        assertEquals(stop, next.position());
        assertEquals(3, next.count(FrameKind.SESSION_MESSAGE));
    }

    @Test
    public void successorOfAStoppedReaderThrowsPreviousStopped() {
        final Log log = smallMessages(2);
        final ClusterLogReader reader = new ClusterLogReader(new Trace(), 10, log.ends()[0]);
        try {
            reader.poll(replayAll(log), Long.MAX_VALUE);
            fail("START_MISMATCH expected");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.START_MISMATCH, expected.stop().reason());
        }
        try {
            ClusterLogReader.successor(reader, new Trace(), 10);
            fail("PREVIOUS_STOPPED expected");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.PREVIOUS_STOPPED, expected.stop().reason());
        }
    }

    @Test
    public void aReaderWithASuccessorCannotBePolledAgain() {
        final Log log = smallMessages(2);
        final ClusterLogReader reader = new ClusterLogReader(new Trace(), 10, 0);
        final Image image = replayAll(log);
        Logs.drain(reader, image, log.stop());
        ClusterLogReader.successor(reader, new Trace(), 10);
        try {
            reader.poll(image, Long.MAX_VALUE);
            fail("the predecessor shares its builder with the successor");
        } catch (final IllegalStateException expected) {
            assertFalse(expected instanceof ClusterLogReaderStoppedException);
        }
    }
}
