// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.ElectionState;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A member whose catch-up attempts keep failing reads both positions; below the leader's log start it exits 1 with
 * one CRITICAL line naming them, otherwise it says what it found and keeps trying. Driven by a fake election-state
 * supplier, a fake probe and an explicit clock.
 */
public class CatchupWatchdogTest {

    private static final long LIMIT = 3;
    private static final long WAIT_MS = 10_000;
    private static final long WAIT_NS = TimeUnit.MILLISECONDS.toNanos(WAIT_MS);

    private final List<Integer> halts = new CopyOnWriteArrayList<>();
    private final List<String> logs = new CopyOnWriteArrayList<>();
    private final AtomicLong state = new AtomicLong(ElectionState.CANVASS.code());
    private final AtomicLong elections = new AtomicLong(1);
    private final AtomicInteger diagnoses = new AtomicInteger();
    private final AtomicInteger refusalsSeen = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private volatile CatchupWatchdog.Diagnosis answer = new CatchupWatchdog.Diagnosis(3_105_056, 2, 3_145_728, null);
    private long now;

    private final CatchupWatchdog.Probe probe = new CatchupWatchdog.Probe() {
        @Override
        public void onRefusal() {
            refusalsSeen.incrementAndGet();
        }

        @Override
        public void poll() {
        }

        @Override
        public CatchupWatchdog.Diagnosis diagnose() {
            diagnoses.incrementAndGet();
            return answer;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    };

    private EngineLifecycle lifecycle() {
        return new EngineLifecycle("test-node", logs::add, halts::add, 1_000, 50);
    }

    private CatchupWatchdog watchdog(final EngineLifecycle lf) {
        return new CatchupWatchdog(lf, state::get, elections::get, LIMIT, WAIT_MS, probe);
    }

    /** One failed attempt as Aeron runs it: catch-up init, await, then a new election back in canvass. */
    private boolean failedAttempt(final CatchupWatchdog w) {
        state.set(ElectionState.FOLLOWER_REPLAY.code());
        boolean tripped = w.poll(now += 1_000_000);
        state.set(ElectionState.FOLLOWER_CATCHUP_INIT.code());
        tripped |= w.poll(now += 1_000_000);
        state.set(ElectionState.FOLLOWER_CATCHUP_AWAIT.code());
        tripped |= w.poll(now += 1_000_000);
        elections.incrementAndGet();
        state.set(ElectionState.CANVASS.code());
        tripped |= w.poll(now += 1_000_000);
        return tripped;
    }

    @Test
    public void belowTheLeadersLogStartItExitsNamingBothPositions() {
        final EngineLifecycle lf = lifecycle();
        final CatchupWatchdog w = watchdog(lf);
        assertFalse(failedAttempt(w));
        assertFalse(failedAttempt(w));
        assertEquals(2, w.refusals());
        assertEquals("no read before the limit", 0, diagnoses.get());
        assertFalse(lf.stopRequested());
        assertTrue(failedAttempt(w));
        assertTrue(w.tripped());
        assertEquals(1, diagnoses.get());
        assertEquals(3, refusalsSeen.get());
        assertEquals(EngineLifecycle.Cause.FATAL, lf.cause());
        final List<String> critical = logs.stream().filter(l -> l.contains("CRITICAL")).toList();
        assertEquals(logs.toString(), 1, critical.size());
        assertTrue(critical.get(0), critical.get(0).contains("3 catch-up attempt(s) in a row failed")
                && critical.get(0).contains("this member's log position 3105056 is below the log start position"
                + " 3145728 of the leader (member 2)")
                && critical.get(0).contains("ENGINE_CATCHUP_REFUSALS 3")
                && critical.get(0).contains("ENGINE_CATCHUP_REFUSAL_WAIT_MS 10000 ms")
                && critical.get(0).contains("exiting 1"));
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("FATAL (exit 1): member cannot catch up:"
                + " its log position 3105056 is below the leader's log start position 3145728 (leader member 2)")));
        assertFalse("trips once", failedAttempt(w));
        lf.awaitStop();
        lf.finish();
        assertEquals(List.of(1), halts);
    }

    /** The log is there: the failures have another cause, so the member says so and keeps trying. */
    @Test
    public void notBelowTheLogStartItSaysSoAndKeepsTrying() {
        answer = new CatchupWatchdog.Diagnosis(3_200_000, 2, 3_145_728, null);
        final EngineLifecycle lf = lifecycle();
        final CatchupWatchdog w = watchdog(lf);
        for (int i = 0; i < 3; i++) {
            assertFalse(failedAttempt(w));
        }
        assertEquals(1, diagnoses.get());
        assertFalse(lf.stopRequested());
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("this member's log position 3200000 is"
                + " not below the log start position 3145728 of the leader (member 2)")
                && l.contains("not exiting")));
        // checked again after the same number of further failures, and then it is below
        assertFalse(failedAttempt(w));
        assertFalse(failedAttempt(w));
        assertEquals(1, diagnoses.get());
        answer = new CatchupWatchdog.Diagnosis(3_200_000, 1, 3_276_800, null);
        assertTrue(failedAttempt(w));
        assertEquals(2, diagnoses.get());
    }

    /** The leader's archive did not answer: never silent, never an exit on a guess. */
    @Test
    public void anUnreadableLogStartIsReportedAndNeverTrips() {
        answer = CatchupWatchdog.Diagnosis.unknown("the archive of the leader (member 2) did not answer");
        final EngineLifecycle lf = lifecycle();
        final CatchupWatchdog w = watchdog(lf);
        for (int i = 0; i < 9; i++) {
            assertFalse(failedAttempt(w));
        }
        assertEquals(3, diagnoses.get());
        assertFalse(lf.stopRequested());
        assertEquals(3, logs.stream().filter(l -> l.contains("the leader's log start position could not be read"
                + " (the archive of the leader (member 2) did not answer)")).count());
    }

    /** A catch-up that is joined clears the count: three failures spread over joined catch-ups never trip. */
    @Test
    public void aJoinedCatchupClearsTheCount() {
        final EngineLifecycle lf = lifecycle();
        final CatchupWatchdog w = watchdog(lf);
        for (int i = 0; i < 5; i++) {
            assertFalse(failedAttempt(w));
            assertFalse(failedAttempt(w));
            state.set(ElectionState.FOLLOWER_CATCHUP_AWAIT.code());
            assertFalse(w.poll(now += 1_000_000));
            state.set(ElectionState.FOLLOWER_CATCHUP.code());
            assertFalse(w.poll(now += 1_000_000));
            assertEquals(0, w.refusals());
            state.set(ElectionState.CLOSED.code());
            assertFalse(w.poll(now += 1_000_000));
        }
        assertEquals(0, diagnoses.get());
        assertFalse(lf.stopRequested());
    }

    /** The time bound: fewer failures than the limit, but failing for the whole wait. */
    @Test
    public void theWaitBoundsTheCount() {
        final EngineLifecycle lf = lifecycle();
        final CatchupWatchdog w = watchdog(lf);
        assertFalse(failedAttempt(w));
        final long first = now;
        assertFalse(w.poll(first + WAIT_NS - 1));
        assertEquals(0, diagnoses.get());
        assertTrue(w.poll(first + WAIT_NS));
        final String critical = logs.stream().filter(l -> l.contains("CRITICAL")).findFirst().orElseThrow();
        assertTrue(critical, critical.contains("1 catch-up attempt(s) in a row failed over 10000 ms"));
    }

    /** An attempt whose end was not observed in canvass still counts: the election counter moved. */
    @Test
    public void aMissedCanvassIsCountedByTheElectionCounter() {
        final EngineLifecycle lf = lifecycle();
        final CatchupWatchdog w = watchdog(lf);
        for (int i = 0; i < 3; i++) {
            state.set(ElectionState.FOLLOWER_CATCHUP_AWAIT.code());
            w.poll(now += 1_000_000);
            elections.incrementAndGet(); // the next poll already sees the next attempt
        }
        state.set(ElectionState.FOLLOWER_CATCHUP_AWAIT.code());
        assertTrue(w.poll(now += 1_000_000));
    }

    @Test
    public void theStatesArePinnedAgainstTheAeronElectionStates() {
        for (final ElectionState s : ElectionState.values()) {
            final boolean attempt = s == ElectionState.FOLLOWER_CATCHUP_INIT || s == ElectionState.FOLLOWER_CATCHUP_AWAIT;
            final boolean failedExit = s == ElectionState.INIT || QuorumWatchdog.NO_LEADER_STATES.contains(s);
            final boolean before = s == ElectionState.FOLLOWER_LOG_REPLICATION || s == ElectionState.FOLLOWER_REPLAY;
            assertEquals(s.name(), attempt, CatchupWatchdog.isAttempt(s.code()));
            assertEquals(s.name(), failedExit, CatchupWatchdog.isFailedAttemptExit(s.code()));
            assertEquals(s.name(), !attempt && !failedExit && !before, CatchupWatchdog.isJoined(s.code()));
        }
        assertFalse(CatchupWatchdog.isJoined(-1));
        assertFalse(CatchupWatchdog.isFailedAttemptExit(-1));
    }

    @Test
    public void stopClosesTheProbeAndEndsThePolling() {
        final EngineLifecycle lf = lifecycle();
        final CatchupWatchdog w = watchdog(lf);
        assertFalse(failedAttempt(w));
        w.stop(10);
        assertTrue(w.stopped());
        assertEquals(1, closes.get());
        assertFalse(failedAttempt(w));
        assertFalse(failedAttempt(w));
        assertFalse(failedAttempt(w));
        assertEquals(0, diagnoses.get());
    }

    @Test
    public void theBoundsAreRangedSettings() {
        assertEquals("ENGINE_CATCHUP_REFUSALS", EngineLifecycle.CATCHUP_REFUSALS_ENV);
        assertEquals(3L, EngineLifecycle.DEFAULT_CATCHUP_REFUSALS);
        assertEquals(1L, EngineLifecycle.MIN_CATCHUP_REFUSALS);
        assertEquals(1_000L, EngineLifecycle.MAX_CATCHUP_REFUSALS);
        assertEquals("ENGINE_CATCHUP_REFUSAL_WAIT_MS", EngineLifecycle.CATCHUP_REFUSAL_WAIT_ENV);
        assertEquals(30_000L, EngineLifecycle.DEFAULT_CATCHUP_REFUSAL_WAIT_MS);
        assertEquals(1_000L, EngineLifecycle.MIN_CATCHUP_REFUSAL_WAIT_MS);
        assertEquals(3_600_000L, EngineLifecycle.MAX_CATCHUP_REFUSAL_WAIT_MS);
        assertEquals("RETENTION_PROBE_TIMEOUT_MS", LogExtent.PROBE_TIMEOUT_ENV);
        assertEquals(5_000L, LogExtent.DEFAULT_PROBE_TIMEOUT_MS);
        assertEquals("10.0.0.7", LogExtent.hostOf("10.0.0.7:9005"));
        assertEquals("node-a", LogExtent.hostOf("node-a"));
    }
}
