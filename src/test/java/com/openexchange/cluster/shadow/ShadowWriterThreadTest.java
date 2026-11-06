// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.openexchange.cluster.shadow.ShadowRig.ENGINE;
import static com.openexchange.cluster.shadow.ShadowRig.ID;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.cursor;
import static com.openexchange.cluster.shadow.ShadowRig.image;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** RCS S3 11.5: the one-slot writer thread, its busy flag (reset twice per run, C4) and its close-aborts rule. */
public class ShadowWriterThreadTest {

    final java.util.List<Throwable> errors = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** A stand-in for an OutOfMemoryError inside a write. */
    static final class Fatal extends Error {
        Fatal() {
            super("injected Error inside write");
        }
    }

    static boolean await(final java.util.function.BooleanSupplier condition, final long timeoutMs)
            throws InterruptedException {
        final long until = System.nanoTime() + timeoutMs * 1_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                return false;
            }
            Thread.sleep(5);
        }
        return true;
    }

    @Test
    public void aThrowingListenerNeverEndsTheThread() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            final BlockingQueue<Done> done = new ArrayBlockingQueue<>(4);
            final int[] calls = {0};
            final ShadowWriterThread t = new ShadowWriterThread(rig.writer(R3), (req, res) -> {
                if (calls[0]++ == 0) {
                    throw new IllegalStateException("host listener bug");
                }
                done.add(new Done(req, res, Thread.currentThread()));
            }, errors::add, rig.counters);
            try {
                assertTrue(t.offer(ShadowWriterThread.Request.of(image(ShadowRig.CHUNK, 1), ENGINE, ShadowKind.ALIGNED,
                        cursor(4096), ID)));
                assertTrue("isIdle after the throwing listener", await(t::isIdle, 10_000));
                assertTrue(await(() -> errors.size() == 1, 5_000));
                assertEquals("host listener bug", errors.get(0).getMessage());
                assertEquals(1, rig.counters.get(ShadowCounters.Counter.WRITER_THREAD_ERRORS_TOTAL));
                assertTrue("the thread lives on", t.isRunning());
                assertTrue(t.offer(ShadowWriterThread.Request.of(image(ShadowRig.CHUNK, 2), ENGINE, ShadowKind.ALIGNED,
                        cursor(8192), ID)));
                final Done d = done.poll(20, TimeUnit.SECONDS);
                assertNotNull("the second offer is served", d);
                assertTrue(d.result().ok());
            } finally {
                assertTrue(t.close(5_000));
            }
        }
    }

    /** The FIX-2 rule: after an Error the thread refuses offers and is idle; it never accepts-and-loses. */
    static void assertNoDeadThreadAcceptingOffers(final ShadowWriterThread t) throws InterruptedException {
        if (!await(() -> !t.thread().isAlive(), 10_000)) {
            throw new AssertionError("the writer thread did not end on the Error");
        }
        if (!await(t::isIdle, 5_000)) {
            throw new AssertionError("isIdle never became true after the Error");
        }
        if (t.offer(ShadowWriterThread.Request.of(image(64, 9), ENGINE, ShadowKind.LOCAL, cursor(99_999), ID))) {
            throw new AssertionError("a dead writer thread accepted an offer");
        }
    }

    static ShadowWriter.Faults fatalAtFirstOffer() {
        return new ShadowWriter.Faults() {
            @Override
            public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l,
                              final int i) {
                throw new Fatal();
            }
        };
    }

    @Test
    public void anErrorInsideAWriteReachesTheHostAndStopsTheThread() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            final BlockingQueue<Done> done = new ArrayBlockingQueue<>(4);
            final ShadowWriterThread t = new ShadowWriterThread(rig.writer(R3, 1, -1, fatalAtFirstOffer(), Set.of(),
                    ShadowRig.CLOCK), (req, res) -> done.add(new Done(req, res, Thread.currentThread())), errors::add,
                    rig.counters);
            assertTrue(t.offer(ShadowWriterThread.Request.of(image(ShadowRig.CHUNK, 1), ENGINE, ShadowKind.ALIGNED,
                    cursor(4096), ID)));
            assertNoDeadThreadAcceptingOffers(t);
            assertTrue("the host error handler saw the Error: " + errors, errors.stream().anyMatch(e -> e instanceof Fatal));
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.WRITER_THREAD_ERRORS_TOTAL));
            final Done d = done.poll(5, TimeUnit.SECONDS);
            assertNotNull("the buffer came back", d);
            assertEquals(ShadowWriter.Reason.UNEXPECTED, d.result().reason());
            assertFalse(t.isRunning());
            assertTrue(t.close(1_000));
        }
        try (ShadowRig rig = new ShadowRig()) {
            ShadowRig.nc("NC-23 writer thread without the FIX-2 guard", "a dead writer thread accepted an offer", () -> {
                final ShadowWriterThread t = new ShadowWriterThread(rig.writer(R3, 1, -1, fatalAtFirstOffer(),
                        Set.of(), ShadowRig.CLOCK), (req, res) -> { }, errors::add, rig.counters, true);
                assertTrue(t.offer(ShadowWriterThread.Request.of(image(ShadowRig.CHUNK, 1), ENGINE, ShadowKind.ALIGNED,
                        cursor(4096), ID)));
                try {
                    assertNoDeadThreadAcceptingOffers(t);
                } catch (final InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
        }
    }

    record Done(ShadowWriterThread.Request request, ShadowWriter.WriteResult result, Thread thread) {
    }

    @Test
    public void busySlotRefusesThenFreesAfterTheCallbackTwice() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            final CountDownLatch release = new CountDownLatch(1);
            final int[] gate = {1}; // the first write pauses after chunk 1 until released
            final ShadowWriter w = rig.writer(R3, 1, -1, new ShadowWriter.Faults() {
                @Override
                public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l,
                                  final int i) {
                    if (gate[0] == 1 && i == 2) {
                        try {
                            release.await(10, TimeUnit.SECONDS);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return p.offer(b, o, l);
                }
            }, Set.of(), ShadowRig.CLOCK);
            final BlockingQueue<Done> done = new ArrayBlockingQueue<>(8);
            final ShadowWriterThread t = new ShadowWriterThread(w,
                    (req, res) -> done.add(new Done(req, res, Thread.currentThread())), errors::add, rig.counters);
            final Random pace = new Random(0x5EED_7A1L);
            try {
                final ShadowImageBuffer first = image(ShadowRig.CHUNK * 3, 1);
                assertTrue(t.offer(ShadowWriterThread.Request.of(first, ENGINE, ShadowKind.ALIGNED, cursor(4096), ID)));
                Thread.sleep(1 + pace.nextInt(50));
                assertFalse("busy", t.offer(ShadowWriterThread.Request.of(image(64, 2), ENGINE, ShadowKind.STANDBY,
                        cursor(8192), ID)));
                assertFalse(t.isIdle());
                assertEquals(1, rig.counters.get(ShadowCounters.Counter.WRITE_SKIPPED_BUSY_TOTAL));
                release.countDown();
                final Done d1 = done.poll(20, TimeUnit.SECONDS);
                assertNotNull(d1);
                assertTrue(d1.result().toString(), d1.result().ok());
                assertSame("the host's buffer comes back", first, d1.request().image());
                assertSame("callback on the writer thread", t.thread(), d1.thread());
                gate[0] = 0;
                for (int round = 2; round <= 3; round++) {
                    while (!t.isIdle()) {
                        Thread.sleep(1);
                    }
                    Thread.sleep(1 + pace.nextInt(50));
                    final ShadowImageBuffer next = image(ShadowRig.CHUNK + round, round);
                    assertTrue("free again (reset " + round + ")", t.offer(ShadowWriterThread.Request.of(next, ENGINE,
                            ShadowKind.CADENCE, cursor(4096L * round * 2), ID)));
                    final Done d = done.poll(20, TimeUnit.SECONDS);
                    assertNotNull(d);
                    assertTrue(d.result().ok());
                    assertSame(next, d.request().image());
                }
            } finally {
                assertTrue(t.close(5_000));
            }
            assertEquals(3, rig.catalog().scan().distinctCount());
        }
    }

    @Test
    public void requestsFreezeTheIdentityAtOfferTime() {
        final ShadowRig.Id live = new ShadowRig.Id(9, true, 3, 4);
        final ShadowWriterThread.Request r = ShadowWriterThread.Request.of(image(64, 1), ENGINE, ShadowKind.LOCAL,
                cursor(64), live);
        assertEquals(new ShadowWriterThread.Snapshot(9, true, 3, 4), r.identity());
    }

    @Test
    public void closeAbortsAStalledWriteAndLeavesNoPublication() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            final int baselineLogs = rig.fixture.publicationLogFiles().size();
            final ShadowWriter w = rig.writer(R3, 1, -1, new ShadowWriter.Faults() {
                @Override
                public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l,
                                  final int i) {
                    return i >= 1 ? Publication.BACK_PRESSURED : p.offer(b, o, l);
                }
            }, Set.of(), ShadowRig.CLOCK);
            final BlockingQueue<Done> done = new ArrayBlockingQueue<>(2);
            final ShadowWriterThread t = new ShadowWriterThread(w,
                    (req, res) -> done.add(new Done(req, res, Thread.currentThread())), errors::add, rig.counters);
            assertTrue(t.offer(ShadowWriterThread.Request.of(image(ShadowRig.CHUNK * 2, 5), ENGINE, ShadowKind.ALIGNED,
                    cursor(4096), ID)));
            Thread.sleep(1 + new Random(3).nextInt(50));
            final long t0 = System.nanoTime();
            assertTrue("closed within the timeout", t.close(5_000));
            assertTrue(System.nanoTime() - t0 < 5_000_000_000L);
            final Done d = done.poll(1, TimeUnit.SECONDS);
            assertNotNull(d);
            assertEquals(ShadowWriter.Reason.ABORTED, d.result().reason());
            assertEquals(ShadowWriter.AbortCause.ABORT_REQUESTED, d.result().abortCause());
            assertFalse(t.offer(ShadowWriterThread.Request.of(image(64, 1), ENGINE, ShadowKind.LOCAL, cursor(9999), ID)));
            final List<ShadowEntry> left = rig.catalog().scan().all();
            assertTrue("excluded: " + left, left.stream().noneMatch(e -> e.state().usable()));
            assertEquals(0, rig.recordingSubscriptions());
            final long until = System.nanoTime() + 10_000_000_000L;
            while (rig.fixture.publicationLogFiles().size() > baselineLogs && System.nanoTime() < until) {
                Thread.sleep(10);
            }
            assertEquals("no publication left behind", baselineLogs, rig.fixture.publicationLogFiles().size());
        }
    }
}
