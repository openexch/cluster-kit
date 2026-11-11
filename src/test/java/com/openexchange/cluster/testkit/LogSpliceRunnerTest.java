// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ShadowEngine;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The log splice runner: a state-only engine reproduces the genesis replay's image at every later SNAPSHOT action from
 * every action cut; an engine whose behaviour depends on its history (not on its image) is caught at the first later
 * action, naming the cut; a log with no action and no stride is refused as vacuous.
 */
public class LogSpliceRunnerTest {

    private static final int APP = SemanticVersion.compose(0, 0, 1);

    /**
     * The hash-chain engine plus a counter that a genesis-started engine advances by 2 per frame and a restored one by
     * 1: the image is round-trip stable at every cut, but the state after a restore depends on how the engine started.
     */
    static final class HistoryDependent implements ShadowEngine {
        final HashChainEngine inner = new HashChainEngine();
        final boolean leaky;
        boolean fromGenesis;
        long counter;

        HistoryDependent(final boolean leaky) {
            this.leaky = leaky;
        }

        private void step() {
            counter += leaky && fromGenesis ? 2 : 1;
        }

        @Override
        public int engineId() {
            return inner.engineId();
        }

        @Override
        public void genesis() {
            fromGenesis = true;
            inner.genesis();
        }

        @Override
        public void restore(final DirectBuffer image, final int length) {
            inner.restore(image, HashChainEngine.IMAGE_LENGTH);
            counter = image.getLong(HashChainEngine.IMAGE_LENGTH, ByteOrder.LITTLE_ENDIAN);
            fromGenesis = false;
        }

        @Override
        public int serialize(final MutableDirectBuffer dst) {
            final int n = inner.serialize(dst);
            dst.putLong(n, counter, ByteOrder.LITTLE_ENDIAN);
            return n + Long.BYTES;
        }

        @Override
        public void onSessionOpen(final long clusterSessionId, final long logPosition, final long timestamp,
                                  final byte[] encodedPrincipal) {
            step();
            inner.onSessionOpen(clusterSessionId, logPosition, timestamp, encodedPrincipal);
        }

        @Override
        public void onSessionClose(final long clusterSessionId, final long logPosition, final long timestamp,
                                   final CloseReason closeReason) {
            step();
            inner.onSessionClose(clusterSessionId, logPosition, timestamp, closeReason);
        }

        @Override
        public boolean onSessionMessage(final long clusterSessionId, final long logPosition, final long timestamp,
                                        final DirectBuffer buffer, final int offset, final int length) {
            step();
            return inner.onSessionMessage(clusterSessionId, logPosition, timestamp, buffer, offset, length);
        }

        @Override
        public int onTimerEvent(final long correlationId, final long logPosition, final long timestamp) {
            step();
            return inner.onTimerEvent(correlationId, logPosition, timestamp);
        }

        @Override
        public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition,
                                             final long timestamp, final TimeUnit timeUnit) {
            step();
            inner.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
        }
    }

    @Test
    public void aStateOnlyEngineReproducesEveryLaterActionImageFromEveryActionCut() {
        final RecordedLog log = ShadowRestoreRunnerTest.syntheticLog();
        final LogSpliceRunner.Report r = LogSpliceRunner.run(HashChainEngine::new, log,
                LogSpliceRunner.Cuts.actions(), ShadowRestoreRunner.Probe.none());
        assertTrue("actions in the log: " + r, r.actions() >= 10);
        assertEquals("one cut per action", r.actions(), r.cuts());
        assertTrue("comparisons >= cuts: " + r, r.comparisons() >= r.cuts());
        final LogSpliceRunner.Report h = LogSpliceRunner.run(() -> new HistoryDependent(false), log,
                LogSpliceRunner.Cuts.actionsAndStride(50), ShadowRestoreRunner.Probe.none());
        assertTrue("stride adds cuts: " + h, h.cuts() > r.cuts());
        System.out.println("GREEN log splice: " + r + "; with stride 50: " + h);
    }

    @Test
    public void aHistoryDependentEngineIsCaughtAtTheFirstLaterAction() {
        final RecordedLog log = ShadowRestoreRunnerTest.syntheticLog();
        long firstAction = -1;
        long secondAction = -1;
        for (final LogFrame f : log.frames()) {
            if (f.kind() == com.openexchange.cluster.log.FrameKind.CLUSTER_ACTION
                    && f.action() == ClusterAction.SNAPSHOT && (f.flags() == 0 || f.flags() == 1)) {
                if (firstAction < 0) {
                    firstAction = f.position();
                } else if (secondAction < 0) {
                    secondAction = f.position();
                }
            }
        }
        try {
            LogSpliceRunner.run(() -> new HistoryDependent(true), log, LogSpliceRunner.Cuts.actions(),
                    ShadowRestoreRunner.Probe.none());
            fail("a history-dependent engine passed the log splice runner");
        } catch (final AssertionError expected) {
            final String text = "splice at " + firstAction + ": image at action " + secondAction
                    + " differs from the genesis replay (first byte ";
            if (!expected.getMessage().startsWith(text)) {
                throw new AssertionError("expected '" + text + "...', got: " + expected.getMessage(), expected);
            }
            System.out.println("GREEN NC history-dependent engine -> " + expected.getMessage());
        }
    }

    @Test
    public void aLogWithoutActionsAndWithoutAStrideIsVacuous() {
        final RecordedLog.Builder b = RecordedLog.builder();
        b.term(0, 1, APP);
        b.open(1, 2, new byte[] {1});
        b.message(1, 3, new byte[] {9});
        b.timer(7, 4);
        try {
            LogSpliceRunner.run(HashChainEngine::new, b.build(), LogSpliceRunner.Cuts.actions(),
                    ShadowRestoreRunner.Probe.none());
            fail("an empty cut set was accepted");
        } catch (final IllegalArgumentException expected) {
            assertEquals("vacuous: the log holds no SNAPSHOT action and no stride was given", expected.getMessage());
        }
        final LogSpliceRunner.Report r = LogSpliceRunner.run(HashChainEngine::new, b.build(),
                LogSpliceRunner.Cuts.actionsAndStride(1), ShadowRestoreRunner.Probe.none());
        assertEquals(0, r.actions());
        assertTrue("stride cuts compare at the log end: " + r, r.comparisons() >= r.cuts() - 1);
    }
}
