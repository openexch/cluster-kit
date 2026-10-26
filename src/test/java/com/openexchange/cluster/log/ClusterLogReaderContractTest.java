// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.LogFrame;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.ClusterTimeUnit;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ClusterActionRequestDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.ScheduleTimerDecoder;
import io.aeron.cluster.codecs.SessionCloseEventDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionOpenEventDecoder;
import io.aeron.cluster.codecs.SessionCloseEventEncoder;
import io.aeron.cluster.codecs.TimerEventDecoder;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import static com.openexchange.cluster.log.Logs.APP_VERSION;
import static com.openexchange.cluster.log.Logs.MTU;
import static com.openexchange.cluster.log.Logs.TERM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RCS S2 4.1: what the reader delivers for every log template against the writer's returned positions, the stop rows
 * of 3.2 (one per row), the abandoned-partial rule F3, a recording that ends inside a message, and the live-oracle leg
 * against a real 1-member cluster (M RCS-M3).
 */
public class ClusterLogReaderContractTest {

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
            b[i] = (byte)(seed + i * 31);
        }
        return b;
    }

    // ------------------------------------------------------------------------------------------- synthetic leg

    @Test
    public void everyTemplateIsDeliveredAtItsFrameEndAsTheWriterReturnedIt() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final List<LogFrame> expected = new ArrayList<>();
        final byte[] principal = "principal-1".getBytes(StandardCharsets.US_ASCII);
        final String channel = "aeron:udp?endpoint=localhost:20999";

        long p = w.newLeadershipTerm(0, 1000, 0, 0, 77, TimeUnit.MILLISECONDS, APP_VERSION);
        expected.add(LogFrame.term(0, p, 1000, 0, 0, 77, TimeUnit.MILLISECONDS, APP_VERSION));
        p = w.sessionOpen(1, 11, 1001, 102, channel, principal);
        expected.add(LogFrame.open(0, p, 1, 1001, 102, channel, principal));
        final byte[] small = bytes(100, 1);
        p = w.message(1, 1002, small);
        expected.add(LogFrame.message(p, 1, 1002, small));
        final byte[] big = bytes(3 * MTU, 2);
        final long beforeBig = w.position();
        final long bigEnd = w.message(1, 1003, big);
        expected.add(LogFrame.message(bigEnd, 1, 1003, big));
        assertTrue("the 3 x MTU message must be fragmented", bigEnd - beforeBig > MTU);
        p = w.timer(42, 1004);
        expected.add(LogFrame.timer(p, 42, 1004));
        p = w.action(ClusterAction.SNAPSHOT, 1005, 0);
        expected.add(LogFrame.action(0, p, 1005, ClusterAction.SNAPSHOT, 0));
        p = w.action(ClusterAction.SNAPSHOT, 1006, 1);
        expected.add(LogFrame.action(0, p, 1006, ClusterAction.SNAPSHOT, 1));
        p = w.actionNullFlags(ClusterAction.SNAPSHOT, 1007);
        expected.add(LogFrame.action(0, p, 1007, ClusterAction.SNAPSHOT, 0));               // null flags -> 0
        p = w.action(ClusterAction.SNAPSHOT, 1008, 2);
        expected.add(LogFrame.action(0, p, 1008, ClusterAction.SNAPSHOT, 2));
        p = w.action(ClusterAction.SUSPEND, 1009, 0);
        expected.add(LogFrame.action(0, p, 1009, ClusterAction.SUSPEND, 0));
        final long termBase = w.position();
        p = w.newLeadershipTerm(1, 1010, termBase, 0, 78, TimeUnit.MILLISECONDS, APP_VERSION);
        expected.add(LogFrame.term(1, p, 1010, termBase, 0, 78, TimeUnit.MILLISECONDS, APP_VERSION));
        p = w.sessionClose(999, 1011, CloseReason.CLIENT_ACTION);                            // never opened
        expected.add(LogFrame.close(1, p, 999, 1011, CloseReason.CLIENT_ACTION));
        p = w.sessionClose(1, 1012, CloseReason.NULL_VAL);                                    // NULL_VAL forwarded
        expected.add(LogFrame.close(1, p, 1, 1012, CloseReason.NULL_VAL));
        final long membership = w.membershipChange(1013);                                    // not delivered
        assertTrue(membership > p);
        int i = 0;
        // Fill the term to less than one 1300-byte message (frame 1376, unfragmented), then write messages until well
        // into the next term: the first one that does not fit pads the term.
        while (w.position() < TERM + 4096) {
            final int length = w.position() < TERM && w.termRemaining() <= 1400 ? 1300 : 300 + (i % 7) * 50;
            final byte[] payload = bytes(length, i);
            p = w.message(2, 2000 + i, payload);
            expected.add(LogFrame.message(p, 2, 2000 + i, payload));
            i++;
        }
        final long stop = rec.stopRecording();

        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.drain(reader, image, stop);

        assertEquals(expected, trace.frames);
        assertEquals(1, reader.count(FrameKind.MEMBERSHIP_CHANGE));
        assertEquals(1, reader.count(FrameKind.SESSION_OPEN));
        assertEquals(2, reader.count(FrameKind.SESSION_CLOSE));
        assertEquals(1, reader.count(FrameKind.TIMER_EVENT));
        assertEquals(5, reader.count(FrameKind.CLUSTER_ACTION));
        assertEquals(2, reader.count(FrameKind.NEW_LEADERSHIP_TERM));
        assertEquals(2 + i, reader.count(FrameKind.SESSION_MESSAGE));
        assertEquals(0, reader.abandonedPartialCount());
        assertEquals(stop, reader.position());
        assertEquals(stop, reader.consumedPosition());
        assertFalse(reader.midMessage());
        // The term rotated with a padding frame that no listener saw: the first frame of term 2 starts at TERM while
        // the frame before it ended short of TERM.
        boolean paddingSkipped = false;
        for (int k = 1; k < trace.frames.size(); k++) {
            final LogFrame prev = trace.frames.get(k - 1);
            final LogFrame cur = trace.frames.get(k);
            if (prev.position() < TERM && cur.position() > TERM) {
                final long curLength = Logs.aligned(32 + 32 + cur.data().length);
                assertEquals("first frame of the second term starts at the term boundary", TERM,
                        cur.position() - curLength);
                paddingSkipped = prev.position() < TERM;
            }
        }
        assertTrue("the log must rotate through a padding frame", paddingSkipped);
    }

    // ------------------------------------------------------------------------------------------------ stop rows

    private interface Bad {
        long write(ClusterLogWriter w);
    }

    private record StopRow(ClusterLogReader reader, Trace trace, Image image, long good, long bad) { }

    /** good message, the bad frame, a good message; read everything; the reader must stop at the bad frame. */
    private static StopRow stopRow(final Bad bad) {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final long good = w.message(5, 1, bytes(40, 9));
        final long badEnd = bad.write(w);
        w.message(5, 2, bytes(40, 10));
        final long stop = rec.stopRecording();
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        try {
            Logs.drain(reader, image, stop);
            fail("reader must stop at the bad frame ending at " + badEnd + "; delivered " + trace.frames);
        } catch (final ClusterLogReaderStoppedException expected) {
            assertSame(reader.stop(), expected.stop());
        }
        assertEquals("only the frame before the bad one is delivered", List.of(good), trace.positions());
        assertEquals("the image stays in front of the bad frame", good, image.position());
        assertEquals(good, reader.position());
        try {
            reader.poll(image, Long.MAX_VALUE);
            fail("a stopped reader must throw on every poll");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertSame(reader.stop(), expected.stop());
        }
        return new StopRow(reader, trace, image, good, badEnd);
    }

    private static void assertStop(final StopRow row, final ReaderStop.Reason reason, final int templateId) {
        assertEquals(reason, row.reader().stop().reason());
        assertEquals(row.bad(), row.reader().stop().framePosition());
        assertEquals(templateId, row.reader().stop().templateId());
    }

    @Test
    public void frameShorterThanAMessageHeaderStopsDecodeFailed() {
        assertStop(stopRow(w -> w.raw(new byte[5])), ReaderStop.Reason.DECODE_FAILED, -1);
    }

    @Test
    public void unknownTemplateStopsAndNothingAfterItIsDelivered() {
        final StopRow row = stopRow(w -> w.raw(111, ScheduleTimerDecoder.TEMPLATE_ID, 17,
                ScheduleTimerDecoder.BLOCK_LENGTH, new byte[ScheduleTimerDecoder.BLOCK_LENGTH]));
        assertStop(row, ReaderStop.Reason.UNKNOWN_TEMPLATE, 31);
        assertEquals(1, row.reader().unknownTemplateCount());
    }

    @Test
    public void foreignSchemaStops() {
        assertStop(stopRow(w -> w.raw(7, 1, 17, 24, new byte[24 + 8])), ReaderStop.Reason.FOREIGN_SCHEMA, 1);
    }

    @Test
    public void schemaVersion18And16Stop() {
        assertStop(stopRow(w -> w.raw(111, TimerEventDecoder.TEMPLATE_ID, 18, 24, new byte[24])),
                ReaderStop.Reason.SCHEMA_VERSION_MISMATCH, 20);
        assertStop(stopRow(w -> w.raw(111, TimerEventDecoder.TEMPLATE_ID, 16, 24, new byte[24])),
                ReaderStop.Reason.SCHEMA_VERSION_MISMATCH, 20);
    }

    @Test
    public void everyLogTemplateWithABodyOneByteShortStopsDecodeFailed() {
        final int[][] templates = {
            {SessionMessageHeaderDecoder.TEMPLATE_ID, SessionMessageHeaderDecoder.BLOCK_LENGTH},
            {TimerEventDecoder.TEMPLATE_ID, TimerEventDecoder.BLOCK_LENGTH},
            {SessionOpenEventDecoder.TEMPLATE_ID, SessionOpenEventDecoder.BLOCK_LENGTH},
            {SessionCloseEventDecoder.TEMPLATE_ID, SessionCloseEventDecoder.BLOCK_LENGTH},
            {ClusterActionRequestDecoder.TEMPLATE_ID, ClusterActionRequestDecoder.BLOCK_LENGTH},
            {NewLeadershipTermEventDecoder.TEMPLATE_ID, NewLeadershipTermEventDecoder.BLOCK_LENGTH},
        };
        for (final int[] t : templates) {
            assertStop(stopRow(w -> w.raw(111, t[0], 17, t[1], new byte[t[1] - 1])),
                    ReaderStop.Reason.DECODE_FAILED, t[0]);
        }
    }

    @Test
    public void declaredBlockLengthOtherThanVersion17StopsDecodeFailed() {
        // A full 24-byte timer body under a header that declares 23: BLA would decode at its fixed offsets anyway.
        assertStop(stopRow(w -> w.raw(111, TimerEventDecoder.TEMPLATE_ID, 17, 23, new byte[24])),
                ReaderStop.Reason.DECODE_FAILED, 20);
    }

    @Test
    public void sessionOpenWhosePrincipalLengthExceedsTheFrameStopsBeforeAllocation() {
        final byte[] channel = "aeron:ipc".getBytes(StandardCharsets.US_ASCII);
        final UnsafeBuffer body = new UnsafeBuffer(new byte[SessionOpenEventDecoder.BLOCK_LENGTH + 4
                + channel.length + 4]);
        int at = SessionOpenEventDecoder.BLOCK_LENGTH;
        body.putInt(at, channel.length, ByteOrder.LITTLE_ENDIAN);
        body.putBytes(at + 4, channel);
        at += 4 + channel.length;
        body.putInt(at, 1_000_000_000, ByteOrder.LITTLE_ENDIAN);
        assertStop(stopRow(w -> w.raw(111, SessionOpenEventDecoder.TEMPLATE_ID, 17,
                SessionOpenEventDecoder.BLOCK_LENGTH, body.byteArray())), ReaderStop.Reason.DECODE_FAILED, 21);
    }

    @Test
    public void undefinedAndNullActionsStopDecodeFailed() {
        assertStop(stopRow(w -> w.actionValue(7, 10, 0)), ReaderStop.Reason.DECODE_FAILED, 23);
        assertStop(stopRow(w -> w.actionValue(Integer.MIN_VALUE, 10, 0)), ReaderStop.Reason.DECODE_FAILED, 23);
    }

    @Test
    public void undefinedCloseReasonStopsDecodeFailed() {
        final UnsafeBuffer body = new UnsafeBuffer(new byte[SessionCloseEventEncoder.BLOCK_LENGTH]);
        new SessionCloseEventEncoder().wrap(body, 0).leadershipTermId(0).clusterSessionId(5).timestamp(3);
        body.putInt(24, 99, ByteOrder.LITTLE_ENDIAN);
        assertStop(stopRow(w -> w.raw(111, SessionCloseEventDecoder.TEMPLATE_ID, 17,
                SessionCloseEventDecoder.BLOCK_LENGTH, body.byteArray())), ReaderStop.Reason.DECODE_FAILED, 22);
    }

    @Test
    public void forgedActionBodyPositionStops() {
        assertStop(stopRow(w -> w.actionWithBodyPosition(ClusterAction.SNAPSHOT, 10, 0, 12_345)),
                ReaderStop.Reason.BODY_POSITION_MISMATCH, 23);
    }

    @Test
    public void forgedTermEventBodyPositionAndUndefinedTimeUnitStop() {
        assertStop(stopRow(w -> w.raw(111, NewLeadershipTermEventDecoder.TEMPLATE_ID, 17,
                NewLeadershipTermEventDecoder.BLOCK_LENGTH, termBody(64, ClusterTimeUnit.MILLIS.value()))),
                ReaderStop.Reason.BODY_POSITION_MISMATCH, 24);
        // A correct body position (this frame's end), so only the undefined time unit (9) is wrong.
        final ToLongFunction<ClusterLogWriter> frameEnd = w -> w.position() + Logs.aligned(32 + 8 + 48);
        assertStop(stopRow(w -> w.raw(111, NewLeadershipTermEventDecoder.TEMPLATE_ID, 17,
                NewLeadershipTermEventDecoder.BLOCK_LENGTH, termBody(frameEnd.applyAsLong(w), 9))),
                ReaderStop.Reason.DECODE_FAILED, 24);
    }

    private static byte[] termBody(final long logPosition, final int timeUnit) {
        final UnsafeBuffer body = new UnsafeBuffer(new byte[NewLeadershipTermEventEncoder.BLOCK_LENGTH]);
        new NewLeadershipTermEventEncoder().wrap(body, 0).leadershipTermId(3).logPosition(logPosition).timestamp(1)
                .termBaseLogPosition(0).leaderMemberId(0).logSessionId(1).appVersion(APP_VERSION);
        body.putInt(40, timeUnit, ByteOrder.LITTLE_ENDIAN);
        return body.byteArray();
    }

    @Test
    public void replayStartedAfterABeginFragmentStopsOrphanWithAPlainReader() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final long first = w.message(5, 1, bytes(40, 1));
        final long bigEnd = w.message(5, 2, bytes(3 * MTU, 2));
        w.message(5, 3, bytes(40, 3));
        final long stop = rec.stopRecording();
        final long beginEnd = Logs.beginEnd(first);
        assertTrue(beginEnd < bigEnd);
        final Image image = fixture.replay(rec.recordingId(), beginEnd, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, beginEnd);
        try {
            Logs.drain(reader, image, stop);
            fail("a continuation with nothing captured must stop the reader");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.ORPHAN_FRAGMENT, expected.stop().reason());
        }
        assertEquals(beginEnd + MTU, reader.stop().framePosition());
        assertTrue(trace.frames.isEmpty());
        assertEquals(beginEnd, image.position());
    }

    @Test
    public void continuationAtTheWrongTermOffsetStopsOrphan() {
        // A BEGIN that fills its term exactly, then an END fragment at offset 0 of the next term: BLA would reset the
        // builder silently (A0:BLA:102-105); the reader stops.
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        while (w.termRemaining() > 2048) {
            w.message(5, 1, bytes(500, 1));
        }
        final long lastWhole = w.position();
        final byte[] message = w.encodeMessage(5, 2, bytes(4000, 2));
        final int beginLength = w.termRemaining() - 32;
        final long beginEnd = w.fragment(FrameDescriptor.BEGIN_FRAG_FLAG, message, 0, beginLength);
        assertEquals("BEGIN fills its term", TERM, beginEnd);
        final long endEnd = w.fragment(FrameDescriptor.END_FRAG_FLAG, message, beginLength, 100);
        final long stop = rec.stopRecording();
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        try {
            Logs.drain(reader, image, stop);
            fail("a continuation at another offset must stop the reader");
        } catch (final ClusterLogReaderStoppedException expected) {
            assertEquals(ReaderStop.Reason.ORPHAN_FRAGMENT, expected.stop().reason());
        }
        assertEquals(endEnd, reader.stop().framePosition());
        assertEquals(lastWhole, reader.position());
        assertTrue(reader.midMessage());
    }

    // ------------------------------------------------------------------------------------- abandoned partial (F3)

    @Test
    public void unfragmentedFrameAfterABeginAbandonsThePartialAndClearsMidMessage() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final byte[] x = w.encodeMessage(5, 1, bytes(500, 1));
        final long xBegin = w.fragment(FrameDescriptor.BEGIN_FRAG_FLAG, x, 0, 200);
        final byte[] yPayload = bytes(60, 2);
        final long y = w.message(5, 2, yPayload);
        final byte[] zPayload = bytes(500, 3);
        final byte[] z = w.encodeMessage(5, 3, zPayload);
        w.fragment(FrameDescriptor.BEGIN_FRAG_FLAG, z, 0, 300);
        final long zEnd = w.fragment(FrameDescriptor.END_FRAG_FLAG, z, 300, z.length - 300);
        final long stop = rec.stopRecording();
        assertEquals(stop, zEnd);

        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.pollUntil(reader, image, xBegin, () -> reader.consumedPosition() == xBegin);
        assertTrue(reader.midMessage());
        Logs.pollUntil(reader, image, y, () -> reader.consumedPosition() == y);
        assertEquals("the unfragmented frame is delivered", List.of(LogFrame.message(y, 5, 2, yPayload)),
                trace.frames);
        assertEquals(1, reader.abandonedPartialCount());
        assertFalse("midMessage is cleared right after the unfragmented frame", reader.midMessage());
        Logs.drain(reader, image, stop);
        assertEquals(List.of(LogFrame.message(y, 5, 2, yPayload), LogFrame.message(zEnd, 5, 3, zPayload)),
                trace.frames);
        assertEquals(1, reader.abandonedPartialCount());
        assertFalse(reader.midMessage());
    }

    @Test
    public void beginAfterABeginAbandonsTheFirstPartial() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final byte[] x = w.encodeMessage(5, 1, bytes(500, 1));
        w.fragment(FrameDescriptor.BEGIN_FRAG_FLAG, x, 0, 200);
        final byte[] zPayload = bytes(500, 3);
        final byte[] z = w.encodeMessage(5, 3, zPayload);
        w.fragment(FrameDescriptor.BEGIN_FRAG_FLAG, z, 0, 300);
        final long zEnd = w.fragment(FrameDescriptor.END_FRAG_FLAG, z, 300, z.length - 300);
        final long stop = rec.stopRecording();
        final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.drain(reader, image, stop);
        assertEquals(List.of(LogFrame.message(zEnd, 5, 3, zPayload)), trace.frames);
        assertEquals(1, reader.abandonedPartialCount());
    }

    // ---------------------------------------------------------------------------------- end of recording mid-message

    @Test
    public void replayEndingAtABeginEndLeavesThePartialCapturedAndNothingDelivered() {
        final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
        final ClusterLogWriter w = rec.writer();
        final long first = w.message(5, 1, bytes(40, 1));
        final long bigEnd = w.message(5, 2, bytes(3 * MTU, 2));
        rec.stopRecording();
        final long cut = Logs.beginEnd(first);
        assertTrue(cut < bigEnd);
        final Image image = fixture.replay(rec.recordingId(), 0, cut);
        final Trace trace = new Trace();
        final ClusterLogReader reader = new ClusterLogReader(trace, 10, 0);
        Logs.drain(reader, image, Long.MAX_VALUE);
        assertTrue("end of stream reached", image.isEndOfStream());
        assertEquals(List.of(first), trace.positions());
        assertTrue(reader.midMessage());
        assertEquals(first, reader.position());
        assertEquals(cut, reader.consumedPosition());
        assertFalse(reader.stopped());
        assertEquals(ReaderStop.NONE, reader.stop());
    }

    // ------------------------------------------------------------------------------- live oracle (M RCS-M3)

    private static void await(final String what, final java.util.function.BooleanSupplier done) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("timed out waiting for " + what);
            }
            Thread.yield();
            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
        }
    }

    private static boolean traced(final java.nio.file.Path file, final String prefix, final String contains) {
        for (final String line : TraceService.read(file)) {
            if (line.startsWith(prefix) && line.contains(contains)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void readerTraceEqualsALiveServiceTraceAcrossASnapshotRestart() throws Exception {
        final java.io.File base = java.nio.file.Files.createTempDirectory("kit-live-oracle-").toFile();
        final java.nio.file.Path traceFile = base.toPath().resolve("service-trace.txt"); // outside the cluster dir
        try (OneMemberCluster node = new OneMemberCluster(new java.io.File(base, "node"))) {
            // ---- term 0: two sessions, a message above MTU, a timer, a client close, SNAPSHOT, STANDBY_SNAPSHOT
            node.start(new TraceService(traceFile));
            final String hexBig;
            try (io.aeron.cluster.client.AeronCluster a = node.connect("alpha");
                 io.aeron.cluster.client.AeronCluster b = node.connect("beta")) {
                OneMemberCluster.send(a, "hello-a".getBytes(StandardCharsets.US_ASCII));
                OneMemberCluster.send(b, "hello-b".getBytes(StandardCharsets.US_ASCII));
                final byte[] big = bytes(4000, 77);
                hexBig = TraceService.hex(big);
                OneMemberCluster.send(a, big);
                OneMemberCluster.send(a, "TIMER-please".getBytes(StandardCharsets.US_ASCII));
                await("timer event", () -> traced(traceFile, "TIMER|", "|" + TraceService.TIMER_ID + "|"));
                b.close();
                await("close of b", () -> traced(traceFile, "CLOSE|", "CLIENT_ACTION"));

                final org.agrona.concurrent.status.AtomicCounter toggle = node.controlToggle(a);
                assertTrue(io.aeron.cluster.ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
                await("snapshot", () -> traced(traceFile, TraceService.SNAPSHOT + "|", ""));
                await("toggle neutral after snapshot", () -> io.aeron.cluster.ClusterControl.ToggleState.get(toggle)
                        == io.aeron.cluster.ClusterControl.ToggleState.NEUTRAL);
                OneMemberCluster.send(a, "after-snapshot-1".getBytes(StandardCharsets.US_ASCII));
                OneMemberCluster.send(a, "after-snapshot-2".getBytes(StandardCharsets.US_ASCII));
                assertTrue(io.aeron.cluster.ClusterControl.ToggleState.STANDBY_SNAPSHOT.toggle(toggle));
                await("toggle neutral after standby", () -> io.aeron.cluster.ClusterControl.ToggleState.get(toggle)
                        == io.aeron.cluster.ClusterControl.ToggleState.NEUTRAL);
                OneMemberCluster.send(a, "after-standby".getBytes(StandardCharsets.US_ASCII));
                await("after-standby message", () -> traced(traceFile, "MSG|",
                        TraceService.hex("after-standby".getBytes(StandardCharsets.US_ASCII))));
            }
            await("both sessions closed", () -> TraceService.read(traceFile).stream()
                    .filter(l -> l.startsWith("CLOSE|")).count() == 2);
            node.stop();

            // ---- restart AFTER the snapshot: snapshot load + tail replay, then a new term with more traffic
            node.start(new TraceService(traceFile));
            await("restart from the snapshot", () -> TraceService.read(traceFile).contains(TraceService.START + "|true"));
            try (io.aeron.cluster.client.AeronCluster c = node.connect("gamma")) {
                OneMemberCluster.send(c, "term2-1".getBytes(StandardCharsets.US_ASCII));
                OneMemberCluster.send(c, "term2-2".getBytes(StandardCharsets.US_ASCII));
                await("term 2 traffic", () -> traced(traceFile, "MSG|",
                        TraceService.hex("term2-2".getBytes(StandardCharsets.US_ASCII))));
            }
            await("gamma closed", () -> TraceService.read(traceFile).stream()
                    .filter(l -> l.startsWith("CLOSE|")).count() >= 3);
            node.stop();

            // ---- merge the service trace by position: first occurrence wins, a replayed record must equal it
            final java.util.Map<String, String> merged = new java.util.LinkedHashMap<>();
            final java.util.List<Long> snapshots = new ArrayList<>();
            final java.util.List<String> starts = new ArrayList<>();
            int replayed = 0;
            for (final String line : TraceService.read(traceFile)) {
                final String[] f = line.split("\\|");
                if (f[0].equals(TraceService.START)) {
                    starts.add(line);
                    continue;
                }
                if (f[0].equals(TraceService.SNAPSHOT)) {
                    snapshots.add(Long.parseLong(f[1]));
                    continue;
                }
                final String key = f[0] + "|" + f[1];
                final String first = merged.putIfAbsent(key, line);
                if (first != null) {
                    assertEquals("a replayed record equals its first occurrence", first, line);
                    replayed++;
                }
            }
            assertEquals(List.of(TraceService.START + "|false", TraceService.START + "|true"), starts);
            assertTrue("the restart replayed the tail after the snapshot", replayed >= 3);
            assertEquals("one ALIGNED snapshot taken", 1, snapshots.size());

            // ---- read the node's log recording(s) through the reader, joined with successor
            final java.util.List<Long> recordings = new ArrayList<>();
            final io.aeron.cluster.RecordingLog recordingLog = new io.aeron.cluster.RecordingLog(node.clusterDir(),
                    false);
            try {
                for (final io.aeron.cluster.RecordingLog.Entry e : recordingLog.entries()) {
                    if (e.type == io.aeron.cluster.RecordingLog.ENTRY_TYPE_TERM && !recordings.contains(e.recordingId)) {
                        recordings.add(e.recordingId);
                    }
                }
            } finally {
                recordingLog.close();
            }
            assertTrue(recordings.size() >= 1);
            final Trace trace = new Trace();
            ClusterLogReader reader = null;
            long lastStop = -1;
            final java.io.File replayDir = new java.io.File(base, "replay");
            try (io.aeron.archive.ArchivingMediaDriver archive = io.aeron.archive.ArchivingMediaDriver.launch(
                    new io.aeron.driver.MediaDriver.Context().aeronDirectoryName(new java.io.File(replayDir, "driver")
                            .getAbsolutePath()).threadingMode(io.aeron.driver.ThreadingMode.SHARED)
                            .dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                    new io.aeron.archive.Archive.Context().aeronDirectoryName(new java.io.File(replayDir, "driver")
                            .getAbsolutePath()).archiveDir(node.archiveDir()).controlChannelEnabled(false)
                            .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                            .replicationChannel("aeron:udp?endpoint=localhost:0").recordingEventsEnabled(false)
                            .threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED));
                 io.aeron.Aeron aeron = io.aeron.Aeron.connect(new io.aeron.Aeron.Context().aeronDirectoryName(
                         new java.io.File(replayDir, "driver").getAbsolutePath()));
                 AeronArchive client = AeronArchive.connect(new AeronArchive.Context().aeron(aeron)
                         .controlRequestChannel("aeron:ipc").controlResponseChannel("aeron:ipc"))) {
                int stream = 2001;
                for (final long recordingId : recordings) {
                    final long start = client.getStartPosition(recordingId);
                    final long stop = client.getStopPosition(recordingId);
                    final int session = (int)client.startReplay(recordingId, start, AeronArchive.NULL_LENGTH,
                            "aeron:ipc", stream);
                    final io.aeron.Subscription sub = aeron.addSubscription(
                            io.aeron.ChannelUri.addSessionId("aeron:ipc", session), stream++);
                    io.aeron.Image image;
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while ((image = sub.imageBySessionId(session)) == null) {
                        assertTrue(System.nanoTime() < deadline);
                        Thread.yield();
                    }
                    reader = reader == null ? new ClusterLogReader(trace, 64, start)
                            : ClusterLogReader.successor(reader, trace, 64);
                    Logs.drain(reader, image, stop);
                    lastStop = stop;
                    sub.close();
                }
            }
            assertEquals(lastStop, reader.consumedPosition());
            assertEquals(lastStop, reader.position());
            assertFalse(reader.midMessage());

            // ---- compare: service-visible reader frames == merged service trace, field by field
            final java.util.List<String> fromReader = new ArrayList<>();
            final java.util.List<Long> aligned = new ArrayList<>();
            final java.util.List<Long> standby = new ArrayList<>();
            for (final LogFrame f : trace.frames) {
                final String r = TraceService.render(f);
                if (r != null) {
                    fromReader.add(r);
                } else if (f.kind() == FrameKind.CLUSTER_ACTION && f.action() == ClusterAction.SNAPSHOT) {
                    (f.flags() == 0 ? aligned : standby).add(f.position());
                }
            }
            assertEquals(String.join("\n", merged.values()), String.join("\n", fromReader));
            assertEquals("ALIGNED action positions are the onTakeSnapshot positions", snapshots, aligned);
            assertEquals("one STANDBY action", 1, standby.size());
            assertFalse("the service took no snapshot at the STANDBY action", snapshots.contains(standby.get(0)));
            assertTrue("the fragmented message is in both traces", String.join("\n", fromReader).contains(hexBig));
            assertTrue("two terms", reader.count(FrameKind.NEW_LEADERSHIP_TERM) >= 2);
            assertEquals(0, reader.abandonedPartialCount());
        } finally {
            org.agrona.IoUtil.delete(base, true);
        }
    }
}
