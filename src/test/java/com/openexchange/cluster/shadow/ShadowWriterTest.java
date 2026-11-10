// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.log.LogCursor;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.openexchange.cluster.shadow.ShadowRig.CHUNK;
import static com.openexchange.cluster.shadow.ShadowRig.ENGINE;
import static com.openexchange.cluster.shadow.ShadowRig.ID;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.cursor;
import static com.openexchange.cluster.shadow.ShadowRig.image;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The synchronous writer (W0-W11) against a real archive (64 KiB terms, the default 1,408-byte IPC
 * MTU: every chunk fragments and terms rotate).
 */
public class ShadowWriterTest {

    static final int PAYLOAD = CHUNK * 5 / 2;

    /** Real time plus an offset the test (or a fault) moves. */
    static final class JumpClock implements NanoClock {
        final AtomicLong offset = new AtomicLong();
        final AtomicLong calls = new AtomicLong();
        volatile long stepPerCall;

        @Override
        public long nanoTime() {
            calls.incrementAndGet();
            return ShadowRig.CLOCK.nanoTime() + offset.addAndGet(stepPerCall);
        }
    }

    @Test
    public void happyPathWritesOneCompleteVerifiableLoadableShadow() {
        try (ShadowRig rig = new ShadowRig()) {
            final long[] trailerProbe = {-1, -1};
            final ShadowWriter w = rig.writer(R3, 1, -1, new ShadowWriter.Faults() {
                @Override
                public void onTrailerOffer(final long recordedPosition, final long publicationPosition) {
                    trailerProbe[0] = recordedPosition;
                    trailerProbe[1] = publicationPosition;
                }
            }, Set.of(), ShadowRig.CLOCK);
            final ShadowImageBuffer image = image(PAYLOAD, 1);
            final LogCursor at = ShadowRig.realCursor(50);
            final ShadowWriter.WriteResult r = ShadowRig.writeOk(w, image, ShadowKind.ALIGNED, at);
            assertTrue("trailer offered only after every chunk was recorded: " + trailerProbe[0] + " < "
                    + trailerProbe[1], trailerProbe[0] >= trailerProbe[1] && trailerProbe[1] > 0);
            assertEquals("descriptor stop == the publication's end", r.bytes(),
                    rig.archive().getStopPosition(r.recordingId()));
            assertEquals("no recording subscription left", 0, rig.recordingSubscriptions());
            final ShadowCatalog catalog = rig.catalog();
            final ShadowCatalog.Scan scan = catalog.scan();
            assertEquals(1, scan.all().size());
            final ShadowEntry e = scan.all().get(0);
            assertEquals(ShadowEntry.State.COMPLETE, e.state());
            assertEquals(e.alias(), e.header().alias());
            assertEquals(r.alias(), e.alias().format());
            assertEquals(ShadowCatalog.DeepResult.Kind.VERIFIED, catalog.verifyDeep(e).kind());
            final UnsafeBuffer into = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD));
            final ShadowImageLoader.LoadResult load = rig.loader().load(e, into);
            assertTrue(load.toString(), load.ok());
            final byte[] got = new byte[load.loaded().length()];
            load.loaded().payload().getBytes(0, got);
            assertArrayEquals(ShadowRig.payload(image), got);
            assertEquals(at, load.loaded().cursor());
            assertEquals(1, rig.counters.writeTotal(ShadowKind.ALIGNED));
            assertEquals(r.bytes(), rig.counters.get(ShadowCounters.Counter.WRITE_BYTES_LAST));
            assertEquals(2, w.nextProducerSeq());
            assertEquals(at.logPosition(), w.lastWrittenPosition());
            ShadowRig.green("writer happy path: " + PAYLOAD + " B payload, " + e.header().chunkCount()
                    + " chunks of " + CHUNK + ", recording [" + e.startPosition() + ", " + e.stopPosition()
                    + "), cursor " + at + ", trailer offered at recorded " + trailerProbe[0] + " >= " + trailerProbe[1]);
        }
    }

    static void assertNoNewShadow(final ShadowRig rig, final int before, final String failure) {
        final int now = rig.shadowRecordingCount();
        if (now != before) {
            throw new AssertionError(failure + " (" + now + " shadow recordings, " + before + " before)");
        }
    }

    @Test
    public void w0RefusesACursorWithUnknownFields() {
        try (ShadowRig rig = new ShadowRig()) {
            final LogCursor bad = new LogCursor(4096, -1, 3, 0);
            final ShadowWriter.WriteResult r = rig.writer(R3).write(image(PAYLOAD, 2), ENGINE, ShadowKind.ALIGNED, bad, ID);
            assertEquals(ShadowWriter.Reason.INVALID_CURSOR, r.reason());
            assertNoNewShadow(rig, 0, "header clusterTime -1 written");
            nc("cursor -1 accepted", "header clusterTime -1 written", () -> {
                final ShadowWriter.WriteResult x = rig.writer(R3, 1, -1, ShadowWriter.Faults.NONE,
                        Set.of(NegativeControl.CURSOR_CHECK_OFF), ShadowRig.CLOCK)
                        .write(image(PAYLOAD, 2), ENGINE, ShadowKind.ALIGNED, bad, ID);
                assertNoNewShadow(rig, 0, "header clusterTime -1 written: " + x);
            });
        }
    }

    @Test
    public void w1RefusesAnUnidentifiedEngine() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            for (final long g : new long[] {0, -7}) {
                final ShadowWriter.WriteResult r = w.write(image(PAYLOAD, 3), ENGINE, ShadowKind.ALIGNED, cursor(4096),
                        new ShadowRig.Id(g, false, 0, 0));
                assertEquals(ShadowWriter.Reason.UNIDENTIFIED, r.reason());
            }
            assertNoNewShadow(rig, 0, "shadow written with genesisId <= 0");
            assertEquals(2, rig.counters.get(ShadowCounters.Counter.WRITE_SKIPPED_UNIDENTIFIED_TOTAL));
            nc("writer accepts genesisId <= 0", "shadow written with a negative genesisId", () -> {
                final ShadowWriter.WriteResult x = rig.writer(R3, 1, -1, ShadowWriter.Faults.NONE,
                        Set.of(NegativeControl.GENESIS_CHECK_OFF), ShadowRig.CLOCK).write(image(PAYLOAD, 3), ENGINE,
                        ShadowKind.ALIGNED, cursor(4096), new ShadowRig.Id(-7, false, 0, 0));
                assertNoNewShadow(rig, 0, "shadow written with a negative genesisId: " + x);
            });
        }
    }

    @Test
    public void w2OneShadowPerPositionPerProducerAcrossARestart() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            ShadowRig.writeOk(w, image(PAYLOAD, 4), ShadowKind.ALIGNED, cursor(4096));
            final ShadowWriter.WriteResult again = w.write(image(PAYLOAD, 4), ENGINE, ShadowKind.LOCAL, cursor(4096), ID);
            assertEquals(ShadowWriter.Reason.NOT_MONOTONE, again.reason());
            // a restarted producer seeded from its startup scan (7.2)
            final ShadowWriter restarted = rig.writer(R3, 2, 4096, ShadowWriter.Faults.NONE, Set.of(), ShadowRig.CLOCK);
            assertEquals(ShadowWriter.Reason.NOT_MONOTONE,
                    restarted.write(image(PAYLOAD, 4), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID).reason());
            ShadowRig.writeOk(restarted, image(PAYLOAD, 5), ShadowKind.ALIGNED, cursor(4096 + 32));
            assertEquals(2, rig.counters.get(ShadowCounters.Counter.WRITE_SKIPPED_NOT_MONOTONE_TOTAL));
            final int count = rig.shadowRecordingCount();
            assertEquals(2, count);
            nc("monotone check off", "two shadows at one position by one producer", () -> {
                final ShadowWriter loose = rig.writer(R3, 3, 4096 + 32, ShadowWriter.Faults.NONE,
                        Set.of(NegativeControl.MONOTONE_CHECK_OFF), ShadowRig.CLOCK);
                loose.write(image(PAYLOAD, 5), ENGINE, ShadowKind.LOCAL, cursor(4096 + 32), ID);
                final ShadowCatalog.Scan s = rig.catalog().scan();
                final long atP = s.atPosition(4096 + 32).copies().stream()
                        .filter(e -> e.producer().equals("R3")).count();
                if (atP > 1) {
                    throw new AssertionError("two shadows at one position by one producer: " + atP);
                }
            });
        }
    }

    @Test
    public void w3RefusesAHeaderItsReaderWouldRefuse() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            assertEquals("HEADER_FIELD_INVALID(kind)",
                    w.write(image(PAYLOAD, 6), ENGINE, null, cursor(4096), ID).detail());
            assertEquals(ShadowWriter.Reason.HEADER_FIELD_INVALID,
                    w.write(image(PAYLOAD, 6), 7, ShadowKind.ALIGNED, cursor(4096), ID).reason());
            assertNoNewShadow(rig, 0, "a refused header was written");
        }
    }

    @Test
    public void w4SelfCheckRunsOnTheBufferBeforeAnythingIsListable() {
        try (ShadowRig rig = new ShadowRig()) {
            final int[] listedDuringCheck = {-1};
            final ShadowSelfCheck refuses = (payload, length) -> {
                listedDuringCheck[0] = rig.shadowRecordingCount();
                return false;
            };
            final ShadowWriter w = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3, refuses, ShadowRig.T,
                    ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS, 1, -1,
                    rig.counters);
            assertEquals(ShadowWriter.Reason.SELF_CHECK_FAILED,
                    w.write(image(PAYLOAD, 7), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID).reason());
            assertSelfCheckSawNothing(rig, listedDuringCheck[0]);
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.SELFCHECK_FAILURES_TOTAL));
            // A self-check that writes into the buffer it was handed
            final ShadowSelfCheck mutates = (payload, length) -> {
                ((org.agrona.MutableDirectBuffer)payload).putByte(length - 1, (byte)(payload.getByte(length - 1) ^ 1));
                return true;
            };
            final ShadowWriter m = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3, mutates, ShadowRig.T,
                    ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS, 1, -1,
                    rig.counters);
            assertEquals(ShadowWriter.Reason.SELF_CHECK_MUTATED_BUFFER,
                    m.write(image(PAYLOAD, 7), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID).reason());
            assertNoNewShadow(rig, 0, "a recording exists after SELF_CHECK_MUTATED_BUFFER");
            nc("self-check after the first offer", "a recording exists after SELF_CHECK_FAILED", () -> {
                final ShadowWriter late = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3, refuses,
                        ShadowRig.T, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS,
                        1, -1, rig.counters, ShadowWriter.Faults.NONE, Set.of(NegativeControl.SELF_CHECK_AFTER_FIRST_OFFER));
                late.write(image(PAYLOAD, 7), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
                assertSelfCheckSawNothing(rig, listedDuringCheck[0]);
            });
        }
    }

    static void assertSelfCheckSawNothing(final ShadowRig rig, final int listedDuringCheck) {
        if (listedDuringCheck != 0) {
            throw new AssertionError("a recording exists after SELF_CHECK_FAILED: " + listedDuringCheck
                    + " listable while the self-check ran");
        }
        assertNoNewShadow(rig, 0, "a recording exists after SELF_CHECK_FAILED");
    }

    /** Closes the publication when message {@code at} is offered. */
    static ShadowWriter.Faults closeAt(final int at) {
        return new ShadowWriter.Faults() {
            @Override
            public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l, final int i) {
                if (i == at) {
                    p.close();
                }
                return p.offer(b, o, l);
            }
        };
    }

    @Test
    public void w7AbortClosesStopsAndPurgesOrReportsTheRefusal() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter.WriteResult r = rig.writer(R3, 1, -1, closeAt(3), Set.of(), ShadowRig.CLOCK)
                    .write(image(PAYLOAD, 8), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            assertEquals(ShadowWriter.Reason.ABORTED, r.reason());
            assertEquals(ShadowWriter.AbortCause.CLOSED, r.abortCause());
            assertEquals(r.toString(), ShadowWriter.Purge.Kind.PURGED, r.purge().kind());
            assertNoNewShadow(rig, 0, "the aborted recording was not purged");
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.WRITE_ABORTED_TOTAL));
            // the purge refused by the archive (ACTIVE_RECORDING injected): reported, and the scan excludes it
            final ShadowWriter.WriteResult refused = rig.writer(R3, 1, -1, new ShadowWriter.Faults() {
                @Override
                public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l,
                                  final int i) {
                    return closeAt(3).offer(p, b, o, l, i);
                }

                @Override
                public void beforePurge(final long recordingId) {
                    throw new ArchiveException("injected", ArchiveException.ACTIVE_RECORDING);
                }
            }, Set.of(), ShadowRig.CLOCK).write(image(PAYLOAD, 8), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            assertEquals(ShadowWriter.Purge.Kind.PURGE_REFUSED, refused.purge().kind());
            assertEquals(ArchiveException.ACTIVE_RECORDING, refused.purge().code());
            final ShadowCatalog.Scan s = rig.catalog().scan();
            assertEquals(1, s.all().size());
            assertEquals(ShadowEntry.State.NO_TRAILER, s.all().get(0).state());
            assertTrue(s.usable().isEmpty());
            assertEquals(0, rig.recordingSubscriptions());
            ShadowRig.green("W7 abort: " + r + " / " + refused + " -> " + s.all().get(0));
        }
    }

    @Test
    public void w7OfferDeadlineAndAbortEndAStall() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            final AtomicInteger offers = new AtomicInteger();
            final ShadowWriter.Faults stall = new ShadowWriter.Faults() {
                @Override
                public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l,
                                  final int i) {
                    offers.incrementAndGet();
                    return Publication.BACK_PRESSURED;
                }
            };
            final JumpClock clock = new JumpClock();
            final ShadowWriter w = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3,
                    new ShadowRig.HashChainSelfCheck(), ShadowRig.T, clock, ShadowRig.TIMEOUT_NS, 100_000_000L,
                    ShadowRig.TIMEOUT_NS, 1, -1, rig.counters, stall, Set.of());
            clock.stepPerCall = 1_000_000L; // every clock read moves 1 ms
            final ShadowWriter.WriteResult r = w.write(image(PAYLOAD, 9), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            clock.stepPerCall = 0;
            assertEquals(ShadowWriter.Reason.ABORTED, r.reason());
            assertEquals(ShadowWriter.AbortCause.OFFER_TIMEOUT, r.abortCause());
            assertTrue("no spin: " + offers.get() + " offers", offers.get() <= 200);
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.WRITE_OFFER_TIMEOUTS_TOTAL));
            // the same stall ended from another thread
            offers.set(0);
            final ShadowWriter slow = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3,
                    new ShadowRig.HashChainSelfCheck(), ShadowRig.T, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS,
                    ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS, 1, -1, rig.counters, stall, Set.of());
            final Thread aborter = new Thread(() -> {
                while (offers.get() < 50) {
                    Thread.onSpinWait();
                }
                slow.requestAbort();
            });
            aborter.start();
            final long t0 = System.nanoTime();
            final ShadowWriter.WriteResult a = slow.write(image(PAYLOAD, 9), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            aborter.join();
            assertEquals(ShadowWriter.Reason.ABORTED, a.reason());
            assertEquals(ShadowWriter.AbortCause.ABORT_REQUESTED, a.abortCause());
            assertTrue("aborted promptly", System.nanoTime() - t0 < ShadowRig.TIMEOUT_NS);
            assertNoNewShadow(rig, 0, "a stalled write left a recording");
            assertEquals(0, rig.recordingSubscriptions());
        }
    }

    /** An abort requested while no write is in flight is a no-op; the next write is not lost. */
    @Test
    public void anAbortWithNoWriteInFlightDoesNotAbortTheNextWrite() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            w.requestAbort();
            w.requestAbort();
            final ShadowWriter.WriteResult r = w.write(image(PAYLOAD, 30), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            assertEquals(r.toString(), ShadowWriter.Reason.OK, r.reason());
            ShadowRig.writeOk(w, image(PAYLOAD, 31), ShadowKind.ALIGNED, cursor(8192));
        }
    }

    @Test
    public void w8RecordTimeoutLeavesATrailerlessRecording() {
        try (ShadowRig rig = new ShadowRig()) {
            final JumpClock clock = new JumpClock();
            final ShadowWriter w = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3,
                    new ShadowRig.HashChainSelfCheck(), ShadowRig.T, clock, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS,
                    100_000_000L, 1, -1, rig.counters, new ShadowWriter.Faults() {
                        @Override
                        public long recordedPosition(final long actual) {
                            // a stalled recorder (the 2026-07-25 class): the writer never sees the chunks recorded,
                            // and the clock jumps 10 ms per look
                            clock.offset.addAndGet(10_000_000L);
                            return Math.min(actual, ShadowHeader.FRAME_LENGTH);
                        }
                    }, Set.of());
            final ShadowWriter.WriteResult r = w.write(image(PAYLOAD, 10), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            assertEquals(r.toString(), ShadowWriter.Reason.RECORD_TIMEOUT, r.reason());
            assertEquals(ShadowWriter.Purge.Kind.NOT_ATTEMPTED, r.purge().kind());
            final long deadline = System.nanoTime() + ShadowRig.TIMEOUT_NS;
            while (rig.archive().getStopPosition(r.recordingId()) == AeronArchive.NULL_POSITION) {
                if (System.nanoTime() > deadline) {
                    fail("stop position never appeared");
                }
                Thread.yield();
            }
            final ShadowCatalog.Scan s = rig.catalog().scan();
            final ShadowEntry e = s.entry(r.recordingId());
            assertTrue("trailer-less by construction, never usable: " + e, e.state() == ShadowEntry.State.NO_TRAILER
                    || e.state() == ShadowEntry.State.TOO_SHORT);
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.WRITE_TIMEOUTS_TOTAL));
            assertEquals(0, rig.recordingSubscriptions());
            ShadowRig.green("W8 record timeout: " + r + " -> " + e);
        }
    }

    @Test
    public void w10AwaitsTheStopPositionAndNeverReportsAFalseMismatch() {
        try (ShadowRig rig = new ShadowRig()) {
            final JumpClock clock = new JumpClock();
            final ShadowWriter w = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3,
                    new ShadowRig.HashChainSelfCheck(), ShadowRig.T, clock, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS,
                    ShadowRig.TIMEOUT_NS, 1, -1, rig.counters, new ShadowWriter.Faults() {
                        @Override
                        public void beforeStopRecording() {
                            clock.stepPerCall = 100_000_000L;
                        }

                        @Override
                        public long stopPosition(final AeronArchive a, final long recordingId) {
                            return AeronArchive.NULL_POSITION; // the archive has not written it yet
                        }
                    }, Set.of());
            final ShadowWriter.WriteResult r = w.write(image(PAYLOAD, 11), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            clock.stepPerCall = 0;
            assertEquals(r.toString(), ShadowWriter.Reason.STOP_TIMEOUT, r.reason());
            assertEquals("the recorded position is counted as written", 4096, w.lastWrittenPosition());
            final long until = System.nanoTime() + ShadowRig.TIMEOUT_NS;
            while (rig.archive().getStopPosition(r.recordingId()) == AeronArchive.NULL_POSITION) {
                assertTrue("stop position never appeared", System.nanoTime() < until);
                Thread.yield();
            }
            assertEquals("left for the scan", ShadowEntry.State.COMPLETE,
                    rig.catalog().scan().entry(r.recordingId()).state());
            // the normal clock: 50 writes, never STOP_MISMATCH (a regression check)
            final ShadowWriter normal = rig.writer(R3, w.nextProducerSeq(), 4096, ShadowWriter.Faults.NONE, Set.of(),
                    ShadowRig.CLOCK);
            for (int i = 1; i <= 50; i++) {
                ShadowRig.writeOk(normal, image(1 + i * 397, 100 + i), ShadowKind.CADENCE, cursor(4096 + 32L * i));
            }
            assertEquals(0, rig.counters.writeFailed(ShadowWriter.Reason.STOP_MISMATCH));
        }
    }

    @Test
    public void recoveryProperty200SeededRunsWithFaults() {
        try (ShadowRig rig = new ShadowRig()) {
            final Random rnd = new Random(0x5EED_FA17L);
            final JumpClock clock = new JumpClock();
            final int[] mode = {0}; // 0 none, 1 abort at chunk, 2 self-check false, 3 stall
            final int[] abortAt = {0};
            final ShadowWriter.Faults faults = new ShadowWriter.Faults() {
                @Override
                public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l,
                                  final int i) {
                    if (mode[0] == 1 && i == abortAt[0]) {
                        p.close();
                    }
                    if (mode[0] == 3) {
                        clock.offset.addAndGet(1_000_000_000L);
                        return Publication.BACK_PRESSURED;
                    }
                    return p.offer(b, o, l);
                }
            };
            final ShadowRig.HashChainSelfCheck real = new ShadowRig.HashChainSelfCheck();
            final ShadowSelfCheck check = (payload, length) -> mode[0] != 2 && real.verify(payload, length);
            final ShadowWriter w = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3, check, ShadowRig.T, clock,
                    ShadowRig.TIMEOUT_NS, 2_000_000_000L, ShadowRig.TIMEOUT_NS, 1, -1, rig.counters, faults, Set.of());
            int ok = 0;
            int failures = 0;
            int recoveries = 0;
            boolean lastFailed = false;
            long p = 4096;
            final List<String> named = new ArrayList<>();
            for (int run = 0; run < 200; run++) {
                final int dice = rnd.nextInt(100);
                mode[0] = lastFailed ? 0 : dice < 30 ? 1 : dice < 40 ? 2 : dice < 45 ? 3 : 0;
                abortAt[0] = 1 + rnd.nextInt(3);
                p += 32;
                final ShadowWriter.WriteResult r = w.write(image(CHUNK * 2 + rnd.nextInt(CHUNK), run), ENGINE,
                        ShadowKind.STANDBY, cursor(p), ID);
                if (r.ok()) {
                    ok++;
                    recoveries += lastFailed ? 1 : 0;
                    lastFailed = false;
                } else {
                    assertTrue("unnamed failure " + r, r.reason() != ShadowWriter.Reason.UNEXPECTED);
                    if (mode[0] == 0) {
                        fail("a fault-free write after a failure failed: " + r);
                    }
                    failures++;
                    lastFailed = true;
                    named.add(r.reason() + (r.abortCause() == null ? "" : "(" + r.abortCause() + ")"));
                }
            }
            assertTrue("non-vacuity", ok > 0 && failures > 0 && recoveries > 0);
            final ShadowCatalog.Scan s = rig.catalog().scan();
            assertEquals("every OK write is one usable position", ok, s.distinctCount());
            assertEquals(0, rig.recordingSubscriptions());
            ShadowRig.green("recovery property: 200 runs, ok=" + ok + " failures=" + failures + " recoveries="
                    + recoveries + " reasons=" + new java.util.TreeMap<>(named.stream().collect(
                    java.util.stream.Collectors.groupingBy(x -> x, java.util.stream.Collectors.counting()))));
        }
    }

    @Test
    public void archiveRefusalsKeepTheirCode() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter.Faults refuse = new ShadowWriter.Faults() {
                @Override
                public ExclusivePublication addPublication(final AeronArchive a, final String c, final int s) {
                    throw new ArchiveException("max recordings", ArchiveException.MAX_RECORDINGS);
                }
            };
            final ShadowWriter.WriteResult r = rig.writer(R3, 1, -1, refuse, Set.of(), ShadowRig.CLOCK)
                    .write(image(PAYLOAD, 12), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID);
            assertRefused8(r);
            nc("ArchiveException relabelled", "ARCHIVE_REFUSED(8) expected, got UNEXPECTED",
                    () -> assertRefused8(rig.writer(R3, 1, -1, refuse, Set.of(NegativeControl.ARCHIVE_EXCEPTION_RELABELLED),
                            ShadowRig.CLOCK).write(image(PAYLOAD, 12), ENGINE, ShadowKind.ALIGNED, cursor(4096), ID)));
        }
    }

    static void assertRefused8(final ShadowWriter.WriteResult r) {
        if (r.reason() != ShadowWriter.Reason.ARCHIVE_REFUSED || r.archiveCode() != ArchiveException.MAX_RECORDINGS) {
            throw new AssertionError("ARCHIVE_REFUSED(8) expected, got " + r.reason());
        }
    }

    @Test
    public void termLengthIsBoundedAndTheUncheckedSelfCheckIsGated() {
        try (ShadowRig rig = new ShadowRig()) {
            for (final String t : new String[] {"32m", "32k", "100k"}) {
                try {
                    new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3, new ShadowRig.HashChainSelfCheck(), t,
                            ShadowRig.CLOCK, 1, 1, 1, 1, -1, rig.counters);
                    fail("term length " + t + " accepted");
                } catch (final IllegalArgumentException e) {
                    assertTrue(e.getMessage(), e.getMessage().contains("[64k, 16m]"));
                }
            }
            assertEquals(2 * 1024 * 1024, new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3,
                    new ShadowRig.HashChainSelfCheck(), "16m", ShadowRig.CLOCK, 1, 1, 1, 1, -1, rig.counters)
                    .chunkLength());
            assertTrue("the unchecked gate is off in this JVM", !ShadowSelfCheck.uncheckedAllowed());
            try {
                new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3, ShadowSelfCheck.UNCHECKED_TEST_ONLY, "64k",
                        ShadowRig.CLOCK, 1, 1, 1, 1, -1, rig.counters);
                fail("UNCHECKED_TEST_ONLY accepted without the property");
            } catch (final IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains(ShadowSelfCheck.UNCHECKED_PROPERTY));
            }
        }
    }
}
