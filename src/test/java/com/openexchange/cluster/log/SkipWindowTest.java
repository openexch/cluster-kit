// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** RCS S2 3.4: the truth table (7 kinds x {e < s, e == s, e > s}) and the gate. */
public class SkipWindowTest {

    private static final long S = 4096;
    private static final EnumSet<FrameKind> MUTATING = EnumSet.of(FrameKind.SESSION_MESSAGE, FrameKind.TIMER_EVENT,
            FrameKind.SESSION_OPEN, FrameKind.SESSION_CLOSE);

    @Test
    public void truthTable() {
        final SkipWindow window = new SkipWindow(S);
        assertEquals(S, window.skipThrough());
        for (final FrameKind kind : FrameKind.values()) {
            final boolean mutating = MUTATING.contains(kind);
            assertEquals(kind + " is engine-mutating", mutating, SkipWindow.isEngineMutating(kind));
            assertEquals(kind + " e < s", mutating, window.drops(kind, S - 32));
            assertEquals(kind + " e == s", mutating, window.drops(kind, S));
            assertFalse(kind + " e > s", window.drops(kind, S + 32));
        }
    }

    @Test
    public void gateForwardsExactlyWhatTheRuleKeeps() {
        final Trace trace = new Trace();
        trace.breakOnAction = true;
        final ClusterLogListener gate = SkipWindow.gate(new SkipWindow(S), trace);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4]);
        for (final long e : new long[] {S - 32, S, S + 32}) {
            gate.onSessionMessage(e, 1, 1, buffer, 0, 4);
            gate.onTimerEvent(e, 1, 1);
            gate.onSessionOpen(0, e, 1, 1, 1, "c", new byte[1]);
            gate.onSessionClose(0, e, 1, 1, CloseReason.CLIENT_ACTION);
            assertTrue("actions always pass and keep the downstream answer",
                    gate.onClusterAction(0, e, 1, ClusterAction.SNAPSHOT, 0));
            gate.onNewLeadershipTermEvent(0, e, 1, 0, 0, 1, TimeUnit.MILLISECONDS, 1);
        }
        // e < s and e == s: actions and term events only (2 x 2); e > s: all six.
        assertEquals(4 + 6, trace.frames.size());
        // Every dropped frame is reported downstream, so a container keeps its position (4 kinds x 2 positions).
        assertEquals(java.util.List.of(
                "SESSION_MESSAGE@" + (S - 32), "TIMER_EVENT@" + (S - 32), "SESSION_OPEN@" + (S - 32),
                "SESSION_CLOSE@" + (S - 32), "SESSION_MESSAGE@" + S, "TIMER_EVENT@" + S, "SESSION_OPEN@" + S,
                "SESSION_CLOSE@" + S), trace.skipped);
        final SkipRule dropActions = (kind, e) -> kind == FrameKind.CLUSTER_ACTION;
        assertFalse("a dropped action returns false",
                SkipWindow.gate(dropActions, trace).onClusterAction(0, S + 64, 1, ClusterAction.SNAPSHOT, 0));
        assertEquals("CLUSTER_ACTION@" + (S + 64), trace.skipped.get(trace.skipped.size() - 1));
        // Nested gates forward the report.
        SkipWindow.gate((k, e) -> false, SkipWindow.gate(new SkipWindow(S), trace)).onTimerEvent(S, 1, 1);
        assertEquals("TIMER_EVENT@" + S, trace.skipped.get(trace.skipped.size() - 1));
    }

    @Test
    public void aListenerThatDoesNotHandleSkipsRefusesThemInsteadOfIgnoringThem() {
        // A decorator that forgets to forward onSkipped must not silently stop a container's cursor.
        final ClusterLogListener forgetful = new ClusterLogListener() {
            @Override
            public void onSessionMessage(final long p, final long id, final long ts, final org.agrona.DirectBuffer b,
                                         final int o, final int l) {
            }

            @Override
            public void onTimerEvent(final long p, final long c, final long ts) {
            }

            @Override
            public void onSessionOpen(final long t, final long p, final long id, final long ts, final int s,
                                      final String ch, final byte[] pr) {
            }

            @Override
            public void onSessionClose(final long t, final long p, final long id, final long ts, final CloseReason r) {
            }

            @Override
            public boolean onClusterAction(final long t, final long p, final long ts, final ClusterAction a,
                                           final int f) {
                return false;
            }

            @Override
            public void onNewLeadershipTermEvent(final long t, final long p, final long ts, final long base,
                                                 final int leader, final int logSession, final TimeUnit unit,
                                                 final int app) {
            }
        };
        try {
            SkipWindow.gate(new SkipWindow(S), forgetful).onTimerEvent(S, 1, 1);
            org.junit.Assert.fail("a dropped frame reaching a listener without onSkipped must be refused");
        } catch (final UnsupportedOperationException expected) {
            assertTrue(expected.getMessage().contains("onSkipped"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeSkipThroughIsRefused() {
        new SkipWindow(-1);
    }
}
