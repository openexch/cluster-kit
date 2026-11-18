// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.ElectionState;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A member that has known a leader and then stays in one open election without a leader for ENGINE_REJOIN_WAIT_MS
 * exits 1 with one CRITICAL line; an election that closes in time, a member that never knew a leader, and a member
 * progressing in a leader-known state never trip it. Driven by a fake election-state supplier and an explicit clock.
 */
public class RejoinWatchdogTest {

    private static final long WAIT_MS = 1_000;
    private static final long WAIT_NS = TimeUnit.MILLISECONDS.toNanos(WAIT_MS);

    private final List<Integer> halts = new CopyOnWriteArrayList<>();
    private final List<String> logs = new CopyOnWriteArrayList<>();
    private final AtomicLong state = new AtomicLong(ElectionState.CLOSED.code());
    private final AtomicLong elections = new AtomicLong(1);

    private EngineLifecycle lifecycle() {
        return new EngineLifecycle("test-node", logs::add, halts::add, 1_000, 50);
    }

    private RejoinWatchdog watchdog(final EngineLifecycle lf) {
        return new RejoinWatchdog(lf, state::get, elections::get, WAIT_MS);
    }

    private long criticalLines() {
        return logs.stream().filter(l -> l.contains("CRITICAL")).count();
    }

    /** The diagnosed wedge: leader known, the leader lost, a new election canvasses and never hears the new one. */
    @Test
    public void tripsOncePastTheWaitAfterALeaderWasKnown() throws Exception {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = watchdog(lf);
        assertFalse(w.poll(0)); // CLOSED after the first election: a leader is known
        assertTrue(w.leaderKnown());
        elections.set(2);
        state.set(ElectionState.CANVASS.code());
        final long t0 = 5 * WAIT_NS;
        assertFalse(w.poll(t0));
        state.set(ElectionState.NOMINATE.code());
        assertFalse(w.poll(t0 + WAIT_NS / 2));
        state.set(ElectionState.CANVASS.code());
        assertFalse(w.poll(t0 + WAIT_NS - 1));
        assertFalse(lf.stopRequested());
        assertTrue(w.poll(t0 + WAIT_NS));
        assertTrue(lf.stopRequested());
        assertEquals(EngineLifecycle.Cause.FATAL, lf.cause());
        assertFalse("trips once", w.poll(t0 + 2 * WAIT_NS));
        assertEquals(logs.toString(), 1, criticalLines());
        final String critical = logs.stream().filter(l -> l.contains("CRITICAL")).findFirst().orElseThrow();
        assertTrue(critical, critical.contains("election 2 open for 1000 ms")
                && critical.contains("election state CANVASS") && critical.contains("ENGINE_REJOIN_WAIT_MS")
                && critical.contains("exiting 1"));
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("FATAL (exit 1)")
                && l.contains("member did not rejoin: election state CANVASS")));
        lf.awaitStop();
        lf.finish();
        assertEquals(List.of(1), halts);
    }

    @Test
    public void anElectionThatClosesInTimeNeverTrips() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = watchdog(lf);
        assertFalse(w.poll(0));
        long t = WAIT_NS;
        for (int round = 0; round < 5; round++) {
            elections.incrementAndGet();
            state.set(ElectionState.CANVASS.code());
            assertFalse(w.poll(t));
            state.set(ElectionState.FOLLOWER_BALLOT.code());
            assertFalse(w.poll(t + WAIT_NS - 1));
            state.set(ElectionState.CLOSED.code());
            assertFalse(w.poll(t + WAIT_NS + 1));
            t += 2 * WAIT_NS;
        }
        assertFalse(lf.stopRequested());
        assertEquals(0, criticalLines());
    }

    @Test
    public void anElectionClosedBetweenTwoPollsRestartsTheClock() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = watchdog(lf);
        assertFalse(w.poll(0));
        elections.set(2);
        state.set(ElectionState.CANVASS.code());
        assertFalse(w.poll(1));
        // election 2 closed and election 3 started between two polls: the counter moved, CLOSED was never seen
        elections.set(3);
        assertFalse(w.poll(WAIT_NS + 1));
        assertFalse(w.poll(2 * WAIT_NS));
        assertTrue(w.poll(2 * WAIT_NS + 1));
    }

    @Test
    public void neverTripsBeforeALeaderWasKnown() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = watchdog(lf);
        for (final ElectionState s : List.of(ElectionState.INIT, ElectionState.CANVASS, ElectionState.NOMINATE,
                ElectionState.CANDIDATE_BALLOT, ElectionState.FOLLOWER_BALLOT)) {
            state.set(s.code());
            for (long t = 0; t <= 3 * WAIT_NS; t += WAIT_NS / 2) {
                assertFalse(s.name(), w.poll(t));
            }
        }
        state.set(-1);
        assertFalse(w.poll(10 * WAIT_NS));
        assertFalse(w.leaderKnown());
        assertFalse(lf.stopRequested());
        assertEquals(0, criticalLines());
    }

    @Test
    public void aLongCatchUpWithALeaderNeverTripsAndAFallBackToCanvassGetsTheFullWait() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = watchdog(lf);
        state.set(ElectionState.FOLLOWER_CATCHUP.code()); // leader known: arms
        for (long t = 0; t <= 5 * WAIT_NS; t += WAIT_NS / 4) {
            assertFalse(w.poll(t));
        }
        assertTrue(w.leaderKnown());
        assertFalse(lf.stopRequested());
        state.set(ElectionState.CANVASS.code()); // same election fell back to canvass after a long catch-up
        assertFalse("the fallback starts a new stretch without a leader", w.poll(5 * WAIT_NS + 1));
        assertFalse(w.poll(6 * WAIT_NS));
        assertTrue("canvass for the whole wait after the fallback trips", w.poll(6 * WAIT_NS + 2));
    }

    @Test
    public void anUnreadableCounterNeverTrips() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = watchdog(lf);
        assertFalse(w.poll(0));
        elections.set(2);
        state.set(-1);
        assertFalse(w.poll(1));
        assertFalse(w.poll(10 * WAIT_NS));
        assertFalse(lf.stopRequested());
    }

    @Test
    public void theRejoinWaitIsAPositiveSettingWithTheProductionDefault() {
        assertEquals(60_000L, EngineLifecycle.DEFAULT_REJOIN_WAIT_MS);
        assertEquals("ENGINE_REJOIN_WAIT_MS", EngineLifecycle.REJOIN_WAIT_ENV);
        assertEquals("engine.rejoin.wait.ms", EngineLifecycle.REJOIN_WAIT_PROP);
        final String env = EngineLifecycle.REJOIN_WAIT_ENV;
        final String prop = EngineLifecycle.REJOIN_WAIT_PROP;
        final long def = EngineLifecycle.DEFAULT_REJOIN_WAIT_MS;
        assertEquals(def, EngineLifecycle.parsePositive(env, null, prop, null, def));
        assertEquals(1_500L, EngineLifecycle.parsePositive(env, "1500", prop, "9", def));
        assertEquals(9L, EngineLifecycle.parsePositive(env, "", prop, "9", def));
        for (final String bad : List.of("0", "-1", "60s", "x")) {
            try {
                EngineLifecycle.parsePositive(env, bad, prop, null, def);
                fail("accepted " + bad);
            } catch (final IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().startsWith(
                        "ENGINE_REJOIN_WAIT_MS must be a positive number of milliseconds"));
            }
        }
        try {
            new RejoinWatchdog(lifecycle(), state::get, elections::get, 0);
            fail("accepted a zero wait");
        } catch (final IllegalArgumentException e) {
            assertEquals("rejoin wait must be positive", e.getMessage());
        }
        try {
            new EngineLifecycle("n", logs::add, halts::add, 1_000, 50, 1_000, 0);
            fail("accepted a zero rejoin wait");
        } catch (final IllegalArgumentException e) {
            assertEquals("lifecycle bounds must be positive", e.getMessage());
        }
    }

    /**
     * Control, through the production wiring ({@code watchQuorum} arms both watchdogs on live daemon threads): the
     * quorum watchdog disarms at the first leader and stays silent for the wedge; only the rejoin watchdog ends it.
     * Without the rejoin watchdog this fails with the stated text.
     */
    @Test
    public void controlTheWedgeAfterALeaderEndsInExitOneThroughWatchQuorum() throws Exception {
        final EngineLifecycle lf = new EngineLifecycle("test-node", logs::add, halts::add, 1_000, 50, 60_000, 200);
        state.set(ElectionState.FOLLOWER_REPLAY.code());
        final QuorumWatchdog quorum = lf.watchQuorum(state::get, elections::get);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!quorum.disarmed() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("the quorum watchdog saw the leader", quorum.disarmed());
        final RejoinWatchdog rejoin = lf.rejoinWatchdog();
        assertNotNull("watchQuorum arms the rejoin watchdog", rejoin);
        while (!rejoin.leaderKnown() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        elections.set(2);
        state.set(ElectionState.CANVASS.code());
        while (!lf.stopRequested() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        if (!lf.stopRequested()) {
            fail("CONTROL: a member that knew a leader and stays in CANVASS past ENGINE_REJOIN_WAIT_MS did not exit"
                    + " (the quorum watchdog is disarmed for the process lifetime)");
        }
        assertEquals(EngineLifecycle.Cause.FATAL, lf.cause());
        assertEquals(logs.toString(), 1, criticalLines());
        lf.awaitStop();
        assertTrue(rejoin.stopped());
        assertFalse("the poller is joined before the close", rejoin.pollerAlive());
        lf.finish();
        assertEquals(List.of(1), halts);
    }
}
