// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.TimerEventDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.openexchange.cluster.log.Logs.APP_VERSION;
import static com.openexchange.cluster.log.Logs.MTU;
import static com.openexchange.cluster.log.Logs.TERM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Review (RCS S2 review-kit): cases the S2 suite did not cover, each found by a surviving mutant. GREEN on the
 * reviewed tree.
 */
public class ReviewKitMissingCasesTest {

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

    /** Records positions; throws {@code failure} at {@code failAt}; remembers the term event's time unit. */
    private static final class Listener implements ClusterLogListener {
        final List<Long> positions = new ArrayList<>();
        long failAt = -1;
        Throwable failure;
        TimeUnit termUnit;

        private void at(final long p) {
            if (p == failAt) {
                if (failure instanceof Error e) {
                    throw e;
                }
                throw (RuntimeException)failure;
            }
            positions.add(p);
        }

        @Override
        public void onSessionMessage(final long p, final long id, final long ts, final DirectBuffer b, final int o,
                                     final int l) {
            at(p);
        }

        @Override
        public void onTimerEvent(final long p, final long c, final long ts) {
            at(p);
        }

        @Override
        public void onSessionOpen(final long t, final long p, final long id, final long ts, final int s,
                                  final String ch, final byte[] pr) {
            at(p);
        }

        @Override
        public void onSessionClose(final long t, final long p, final long id, final long ts, final CloseReason r) {
            at(p);
        }

        @Override
        public boolean onClusterAction(final long t, final long p, final long ts, final ClusterAction a,
                                       final int f) {
            at(p);
            return false;
        }

        @Override
        public void onNewLeadershipTermEvent(final long t, final long p, final long ts, final long base,
                                             final int leader, final int session, final TimeUnit unit,
                                             final int app) {
            at(p);
            termUnit = unit;
        }
    }

    private interface Write {
        long write(ClusterLogWriter w);
    }

    /** good message, the frame under test, good message; drained to the end (or the stop). */
    private static long[] row(final Listener listener, final ClusterLogReader[] holder, final Write write) {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final long good = rec.writer().message(5, 1, bytes(40, 9));
        final long frame = write.write(rec.writer());
        final long last = rec.writer().message(5, 2, bytes(40, 10));
        final long stop = rec.stopRecording();
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        holder[0] = new ClusterLogReader(listener, 10, 0);
        Logs.drain(holder[0], image, stop);
        return new long[] {good, frame, last, image.position()};
    }

    /** 3.1 F9 covers RuntimeException OR Error; only RuntimeException was tested (mutant M08 lived). */
    @Test
    public void listenerThrowingAnErrorStopsInFrontOfTheFrame() {
        final Listener listener = new Listener();
        final AssertionError error = new AssertionError("an Error, not a RuntimeException");
        listener.failure = error;
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final long first = rec.writer().message(3, 1, bytes(100, 1));
        listener.failAt = rec.writer().message(3, 2, bytes(100, 2));
        final long stop = rec.stopRecording();
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final ClusterLogReader reader = new ClusterLogReader(listener, 10, 0);
        try {
            Logs.drain(reader, image, stop);
            fail("an Error in the listener must stop the reader");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.LISTENER_FAILED, expected.stop().reason());
            assertSame(error, expected.stop().cause());
        }
        assertEquals("the image stays in front of the failing frame", first, image.position());
        assertEquals(List.of(first), listener.positions);
    }

    /** M3 is "==", not ">=": a declared block LONGER than version 17's stops too (mutant M22 lived). */
    @Test
    public void declaredBlockLongerThanVersion17StopsDecodeFailed() {
        final int[][] templates = {
            {SessionMessageHeaderDecoder.TEMPLATE_ID, SessionMessageHeaderDecoder.BLOCK_LENGTH},
            {TimerEventDecoder.TEMPLATE_ID, TimerEventDecoder.BLOCK_LENGTH},
        };
        for (final int[] t : templates) {
            final Listener listener = new Listener();
            final ClusterLogReader[] reader = new ClusterLogReader[1];
            try {
                row(listener, reader, w -> w.raw(MessageHeaderDecoder.SCHEMA_ID, t[0],
                        MessageHeaderDecoder.SCHEMA_VERSION, t[1] + 8, new byte[t[1] + 8]));
                fail("template " + t[0] + " with block " + (t[1] + 8) + " was accepted");
            } catch (final ClusterLogReaderStoppedException expected) {
                assertEquals(ReaderStop.Reason.DECODE_FAILED, expected.stop().reason());
                assertEquals(t[0], expected.stop().templateId());
            }
            assertEquals(1, listener.positions.size());
        }
    }

    /** ClusterClock.map: a NULL_VAL time unit is milliseconds, not a stop (mutant M21 lived). */
    @Test
    public void termEventWithANullTimeUnitIsDeliveredAsMilliseconds() {
        final Listener listener = new Listener();
        final ClusterLogReader[] reader = new ClusterLogReader[1];
        final long[] ends = row(listener, reader, w -> {
            final long end = w.position() + Logs.aligned(32 + 8 + NewLeadershipTermEventEncoder.BLOCK_LENGTH);
            final UnsafeBuffer body = new UnsafeBuffer(new byte[NewLeadershipTermEventEncoder.BLOCK_LENGTH]);
            new NewLeadershipTermEventEncoder().wrap(body, 0).leadershipTermId(3).logPosition(end).timestamp(1)
                    .termBaseLogPosition(0).leaderMemberId(0).logSessionId(1).appVersion(APP_VERSION);
            body.putInt(40, Integer.MIN_VALUE, ByteOrder.LITTLE_ENDIAN);           // ClusterTimeUnit.NULL_VAL
            return w.raw(MessageHeaderDecoder.SCHEMA_ID, NewLeadershipTermEventDecoder.TEMPLATE_ID,
                    MessageHeaderDecoder.SCHEMA_VERSION, NewLeadershipTermEventDecoder.BLOCK_LENGTH,
                    body.byteArray());
        });
        assertFalse(reader[0].stopped());
        assertEquals(List.of(ends[0], ends[1], ends[2]), listener.positions);
        assertEquals(TimeUnit.MILLISECONDS, listener.termUnit);
    }

    /** successor across two contiguous recordings: counters carried, a stop in the second image is exact. */
    @Test
    public void successorAcrossContiguousRecordingsCarriesCountersAndStopsExactlyInTheSecond() {
        final RecordedLogFixture.Recording one = fixture.record(TERM, MTU, 0);
        final long a = one.writer().message(3, 1, bytes(100, 1));
        final long stopOne = one.stopRecording();
        assertEquals(a, stopOne);
        final RecordedLogFixture.Recording two = fixture.record(TERM, MTU, stopOne);
        final ClusterLogWriter w = two.writer();
        final long b = w.message(3, 2, bytes(100, 2));
        final long unknown = w.raw(MessageHeaderDecoder.SCHEMA_ID, 31, MessageHeaderDecoder.SCHEMA_VERSION, 8,
                new byte[8]);
        w.message(3, 3, bytes(100, 3));
        final long stopTwo = two.stopRecording();

        final Listener listener = new Listener();
        final ClusterLogReader first = new ClusterLogReader(listener, 10, 0);
        Logs.drain(first, fixture.replay(one.recordingId(), 0, AeronArchive.NULL_LENGTH), stopOne);
        assertEquals(1, first.count(FrameKind.SESSION_MESSAGE));
        final ClusterLogReader next = ClusterLogReader.successor(first, listener, 10);
        assertEquals(stopOne, next.startPosition());
        try {
            Logs.drain(next, fixture.replay(two.recordingId(), stopOne, AeronArchive.NULL_LENGTH), stopTwo);
            fail("template 31 must stop the successor");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.UNKNOWN_TEMPLATE, expected.stop().reason());
            assertEquals(unknown, expected.stop().framePosition());
            assertEquals(31, expected.stop().templateId());
        }
        assertEquals(List.of(a, b), listener.positions);
        assertEquals("counters carried across the successor", 2, next.count(FrameKind.SESSION_MESSAGE));
        assertEquals(1, next.unknownTemplateCount());
        assertEquals(b, next.position());
        assertFalse(next.midMessage());
        assertTrue(next.stopped());
    }
}
