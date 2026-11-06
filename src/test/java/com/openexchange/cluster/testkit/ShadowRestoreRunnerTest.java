// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.log.SkipWindow;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** RCS S2 4.4: the runner passes the production wiring and catches every O4 mutation, naming the path. */
public class ShadowRestoreRunnerTest {

    private static final int APP = SemanticVersion.compose(0, 0, 1);
    private static final int STRIDE = 7;
    private static RecordedLog synthetic;
    private static RecordedLog captured;

    @BeforeClass
    public static void logs() {
        synthetic = syntheticLog();
        captured = capturedLog();
    }

    /** 600 frames: three terms, session churn, messages, timers, ALIGNED/STANDBY/other actions. */
    static RecordedLog syntheticLog() {
        final RecordedLog.Builder b = RecordedLog.builder();
        b.term(0, 1, APP);
        final List<Long> open = new ArrayList<>();
        long nextId = 1;
        long ts = 10;
        int frames = 1;
        while (frames < 600) {
            ts++;
            final int k = frames;
            if (k == 250) {
                b.term(1, ts, APP);
            } else if (k == 430) {
                b.term(2, ts, APP);
            } else if (k % 17 == 0 || open.isEmpty()) {
                final long id = nextId++;
                b.open(id, ts, ("p" + id).getBytes(StandardCharsets.US_ASCII));
                open.add(id);
            } else if (k % 23 == 0 && open.size() > 1) {
                b.close(open.remove(0), ts, CloseReason.CLIENT_ACTION);
            } else if (k % 11 == 0) {
                b.timer(1000 + k, ts);
            } else if (k % 29 == 0) {
                b.action(ClusterAction.SNAPSHOT, ts, 0);
            } else if (k % 37 == 0) {
                b.action(ClusterAction.SNAPSHOT, ts, 1);
            } else if (k % 53 == 0) {
                b.action(ClusterAction.SNAPSHOT, ts, 2);
            } else if (k % 61 == 0) {
                b.action(ClusterAction.SUSPEND, ts, 0);
            } else if (k % 62 == 0) {
                b.action(ClusterAction.RESUME, ts, 0);
            } else {
                b.message(open.get(k % open.size()), ts, new byte[] {(byte)k, (byte)(k >> 8), 7});
            }
            frames++;
        }
        final RecordedLog log = b.build();
        assertEquals(600, log.size());
        assertEquals(3, log.count(FrameKind.NEW_LEADERSHIP_TERM));
        return log;
    }

    /** A real archive recording: a fragmented message and two terms (term events and a term rotation). */
    static RecordedLog capturedLog() {
        try (RecordedLogFixture fixture = new RecordedLogFixture()) {
            final int term = 64 * 1024;
            final RecordedLogFixture.Recording rec = fixture.record(term, 1408, 0);
            final ClusterLogWriter w = rec.writer();
            w.newLeadershipTerm(0, 1, 0, 0, 1, TimeUnit.MILLISECONDS, APP);
            w.sessionOpen(1, 1, 2, 102, "aeron:ipc", new byte[] {1, 2});
            w.sessionOpen(2, 2, 3, 102, "aeron:ipc", new byte[] {3});
            int i = 0;
            while (w.position() < term + 8192) {
                if (i == 20) {
                    w.message(1, 10 + i, new byte[3 * 1408]);
                } else if (i == 40) {
                    w.newLeadershipTerm(1, 10 + i, w.position(), 0, 2, TimeUnit.MILLISECONDS, APP);
                } else if (i % 9 == 0) {
                    w.timer(500 + i, 10 + i);
                } else if (i % 13 == 0) {
                    w.action(ClusterAction.SNAPSHOT, 10 + i, i % 2);
                } else if (i == 60) {
                    w.sessionClose(1, 10 + i, CloseReason.CLIENT_ACTION);
                } else {
                    w.message(i < 60 ? 1 : 2, 10 + i, new byte[200 + (i % 5) * 64]);
                }
                i++;
            }
            final long stop = rec.stopRecording();
            final RecordedLog log = RecordedLog.capture(fixture.replay(rec.recordingId(), 0,
                    AeronArchive.NULL_LENGTH), 0, stop);
            assertEquals(stop, log.endPosition());
            assertEquals(2, log.count(FrameKind.NEW_LEADERSHIP_TERM));
            return log;
        }
    }

    // ------------------------------------------------------------------------------------------- production

    @Test
    public void productionWiringPassesOnTheSyntheticLog() {
        final ShadowRestoreRunner.Report report = ShadowRestoreRunner.run(HashChainEngine::new, synthetic, STRIDE,
                ShadowRestoreRunner.Variant.PRODUCTION, HashChainEngine::events);
        assertEquals(600, report.frames());
        assertTrue(report.equal() > 0 && report.skip() > 0 && report.tail() > 0);
        assertEquals(report.pairs(), report.equal() + report.skip() + report.tail());
        assertEquals(HashChainEngine.IMAGE_LENGTH, report.finalImageBytes());
    }

    @Test
    public void productionWiringPassesOnAnArchiveCapturedLog() {
        final ShadowRestoreRunner.Report report = ShadowRestoreRunner.run(HashChainEngine::new, captured, 3,
                ShadowRestoreRunner.Variant.PRODUCTION, HashChainEngine::events);
        assertTrue(report.frames() > 100);
        assertTrue(report.equal() > 0 && report.skip() > 0 && report.tail() > 0);
    }

    // ------------------------------------------------------------------------------------ O4 mutation controls

    private static void assertCaught(final String control, final ShadowRestoreRunner.Variant variant,
                                     final ShadowRestoreRunner.Path path) {
        assertCaught(control, HashChainEngine::new, variant, HashChainEngine::events, path);
    }

    private static <E extends ShadowEngine> void assertCaught(final String control,
                                                              final java.util.function.Supplier<E> fresh,
                                                              final ShadowRestoreRunner.Variant variant,
                                                              final ShadowRestoreRunner.Probe<? super E> probe,
                                                              final ShadowRestoreRunner.Path path) {
        AssertionError caught = null;
        try {
            ShadowRestoreRunner.run(fresh, synthetic, STRIDE, variant, probe);
        } catch (final AssertionError e) {
            caught = e;
        }
        if (caught == null) {
            fail("runner blind to " + control);
        }
        assertTrue(control + ": the failure must name the " + path + " path: " + caught.getMessage(),
                caught.getMessage().startsWith(path.name() + " P_s="));
        assertTrue(caught.getMessage().contains("P_cm="));
    }

    private static ShadowRestoreRunner.Variant gateWithout(final FrameKind kind) {
        return ShadowRestoreRunner.Variant.PRODUCTION.withSkipRule(
                s -> (k, e) -> k != kind && SkipWindow.isEngineMutating(k) && e <= s);
    }

    @Test
    public void gateLessThanInsteadOfLessOrEqual() {
        assertCaught("gate < instead of <=", ShadowRestoreRunner.Variant.PRODUCTION.withSkipRule(
                s -> (k, e) -> SkipWindow.isEngineMutating(k) && e < s), ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void gateWithoutSessionClose() {
        assertCaught("gate without SESSION_CLOSE", gateWithout(FrameKind.SESSION_CLOSE),
                ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void gateWithoutTimerEvent() {
        assertCaught("gate without TIMER_EVENT", gateWithout(FrameKind.TIMER_EVENT), ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void gateWithoutSessionOpen() {
        assertCaught("gate without SESSION_OPEN", gateWithout(FrameKind.SESSION_OPEN), ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void gateWithoutSessionMessage() {
        assertCaught("gate without SESSION_MESSAGE", gateWithout(FrameKind.SESSION_MESSAGE),
                ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void gateThatAlsoDropsTermEvents() {
        // Term events are state-neutral for the engine: only the cursor condition (3.5 condition 2) can see this.
        assertCaught("gate dropping NEW_LEADERSHIP_TERM", ShadowRestoreRunner.Variant.PRODUCTION.withSkipRule(
                s -> (k, e) -> (SkipWindow.isEngineMutating(k) || k == FrameKind.NEW_LEADERSHIP_TERM) && e <= s),
                ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void gateThatAlsoDropsSnapshotActions() {
        // Image and cursor stay equal (a dropped frame still advances the container); only the trigger stream after
        // max(s, c) differs (3.5 condition 4, second half).
        assertCaught("gate dropping CLUSTER_ACTION", ShadowRestoreRunner.Variant.PRODUCTION.withSkipRule(
                s -> (k, e) -> k == FrameKind.CLUSTER_ACTION || (SkipWindow.isEngineMutating(k) && e <= s)),
                ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void tailLegDroppingTimerEvents() {
        assertCaught("tail leg dropping TimerEvent", ShadowRestoreRunner.Variant.PRODUCTION.withTailDecorator(
                downstream -> new Forwarding(downstream) {
                    @Override
                    public void onTimerEvent(final long logPosition, final long correlationId, final long ts) {
                    }
                }), ShadowRestoreRunner.Path.TAIL);
    }

    @Test
    public void tailLegDroppingTermEvents() {
        // A decorator, not a gate: the container never hears of the frame, so only the cursor (condition 2) differs.
        assertCaught("tail leg dropping NEW_LEADERSHIP_TERM", ShadowRestoreRunner.Variant.PRODUCTION.withTailDecorator(
                downstream -> new Forwarding(downstream) {
                    @Override
                    public void onNewLeadershipTermEvent(final long term, final long p, final long ts,
                                                         final long base, final int leader, final int logSession,
                                                         final TimeUnit unit, final int app) {
                    }
                }), ShadowRestoreRunner.Path.TAIL);
    }

    @Test
    public void tailLegDroppingSnapshotActions() {
        // Image and cursor equal; only the trigger stream from max(s, c) on differs (condition 4, second half).
        assertCaught("tail leg dropping CLUSTER_ACTION", ShadowRestoreRunner.Variant.PRODUCTION.withTailDecorator(
                downstream -> new Forwarding(downstream) {
                    @Override
                    public boolean onClusterAction(final long term, final long p, final long ts,
                                                   final ClusterAction action, final int flags) {
                        return false;
                    }
                }), ShadowRestoreRunner.Path.TAIL);
    }

    @Test
    public void tailLegEmptyingTheSessionOpenPrincipal() {
        assertCaught("tail leg replacing the principal", ShadowRestoreRunner.Variant.PRODUCTION.withTailDecorator(
                downstream -> new Forwarding(downstream) {
                    @Override
                    public void onSessionOpen(final long term, final long logPosition, final long id, final long ts,
                                              final int stream, final String channel, final byte[] principal) {
                        downstream.onSessionOpen(term, logPosition, id, ts, stream, channel, new byte[0]);
                    }
                }), ShadowRestoreRunner.Path.TAIL);
    }

    @Test
    public void tailBoundOneFrameShort() {
        final long[] ends = synthetic.positions();
        assertCaught("tail bound c -> previous frame end", ShadowRestoreRunner.Variant.PRODUCTION.withTailBound(c -> {
            long previous = 0;
            for (final long e : ends) {
                if (e >= c) {
                    break;
                }
                previous = e;
            }
            return previous;
        }), ShadowRestoreRunner.Path.TAIL);
    }

    @Test
    public void containerWithTriggerSuppressionOff() {
        // Labelling the SKIP image with the replay position instead of P_s turns the container's suppression off: a
        // snapshot action inside (c, s) then fires (3.5 condition 4).
        assertCaught("trigger suppression off", ShadowRestoreRunner.Variant.PRODUCTION.withImageLabel((s, c) -> c),
                ShadowRestoreRunner.Path.SKIP);
    }

    @Test
    public void probeEventsThatDifferAfterARestore() {
        // State and cursor equal, only the rendered events differ (3.5 condition 3).
        assertCaught("events emitted differently after a restore", TaggingEngine::new,
                ShadowRestoreRunner.Variant.PRODUCTION, TaggingEngine::events, ShadowRestoreRunner.Path.EQUAL);
    }

    @Test
    public void productionVariantIsWhatItSays() {
        final ShadowRestoreRunner.Variant v = ShadowRestoreRunner.Variant.PRODUCTION;
        assertTrue(v.skipRule().apply(100) instanceof SkipWindow);
        assertEquals(100, ((SkipWindow)v.skipRule().apply(100)).skipThrough());
        final ClusterLogListener l = new Forwarding(null);
        assertTrue(v.tailDecorator().apply(l) == l);
        assertEquals(123, v.tailBound().applyAsLong(123));
        assertEquals(7, v.imageLabel().applyAsLong(7, 3));
    }

    /** HashChainEngine whose events carry an "R" tag once it was restored: same state, different event stream. */
    static final class TaggingEngine implements ShadowEngine {
        private final HashChainEngine delegate = new HashChainEngine();
        private boolean restored;

        List<String> events() {
            final List<String> events = new ArrayList<>();
            for (final String e : delegate.events()) {
                events.add(restored ? "R " + e : e);
            }
            return events;
        }

        @Override
        public int engineId() {
            return delegate.engineId();
        }

        @Override
        public void genesis() {
            delegate.genesis();
        }

        @Override
        public void restore(final DirectBuffer image, final int length) {
            delegate.restore(image, length);
            restored = true;
        }

        @Override
        public int serialize(final MutableDirectBuffer dst) {
            return delegate.serialize(dst);
        }

        @Override
        public void onSessionOpen(final long id, final long p, final long ts, final byte[] principal) {
            delegate.onSessionOpen(id, p, ts, principal);
        }

        @Override
        public void onSessionClose(final long id, final long p, final long ts, final CloseReason reason) {
            delegate.onSessionClose(id, p, ts, reason);
        }

        @Override
        public boolean onSessionMessage(final long id, final long p, final long ts, final DirectBuffer buffer,
                                        final int offset, final int length) {
            return delegate.onSessionMessage(id, p, ts, buffer, offset, length);
        }

        @Override
        public int onTimerEvent(final long correlationId, final long p, final long ts) {
            return delegate.onTimerEvent(correlationId, p, ts);
        }

        @Override
        public void onNewLeadershipTermEvent(final long term, final long p, final long ts, final TimeUnit unit) {
            delegate.onNewLeadershipTermEvent(term, p, ts, unit);
        }
    }

    /** Forwards everything; controls override one method. */
    static class Forwarding implements ClusterLogListener {
        final ClusterLogListener downstream;

        Forwarding(final ClusterLogListener downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSessionMessage(final long p, final long id, final long ts, final DirectBuffer buffer,
                                     final int offset, final int length) {
            downstream.onSessionMessage(p, id, ts, buffer, offset, length);
        }

        @Override
        public void onTimerEvent(final long p, final long correlationId, final long ts) {
            downstream.onTimerEvent(p, correlationId, ts);
        }

        @Override
        public void onSessionOpen(final long term, final long p, final long id, final long ts, final int stream,
                                  final String channel, final byte[] principal) {
            downstream.onSessionOpen(term, p, id, ts, stream, channel, principal);
        }

        @Override
        public void onSessionClose(final long term, final long p, final long id, final long ts,
                                   final CloseReason reason) {
            downstream.onSessionClose(term, p, id, ts, reason);
        }

        @Override
        public boolean onClusterAction(final long term, final long p, final long ts, final ClusterAction action,
                                       final int flags) {
            return downstream.onClusterAction(term, p, ts, action, flags);
        }

        @Override
        public void onNewLeadershipTermEvent(final long term, final long p, final long ts, final long base,
                                             final int leader, final int logSession, final TimeUnit unit,
                                             final int app) {
            downstream.onNewLeadershipTermEvent(term, p, ts, base, leader, logSession, unit, app);
        }

        @Override
        public void onSkipped(final FrameKind kind, final long p, final long ts) {
            downstream.onSkipped(kind, p, ts);
        }
    }
}
