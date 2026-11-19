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

    // ---- the reachability gate ----

    private final java.util.concurrent.atomic.AtomicBoolean quorum = new java.util.concurrent.atomic.AtomicBoolean();
    private static final long WINDOW_MS = 200;
    private static final long WINDOW_NS = TimeUnit.MILLISECONDS.toNanos(WINDOW_MS);

    private RejoinWatchdog gated(final EngineLifecycle lf) {
        return new RejoinWatchdog(lf, state::get, elections::get, WAIT_MS, quorum::get, WINDOW_MS, null);
    }

    private RejoinWatchdog gatedInCanvass(final EngineLifecycle lf) {
        final RejoinWatchdog w = gated(lf);
        state.set(ElectionState.CLOSED.code());
        assertFalse(w.poll(0)); // CLOSED: a leader is known
        elections.set(2);
        state.set(ElectionState.CANVASS.code());
        return w;
    }

    /** Two of three members gone: no evidence of a reachable quorum, so the watchdog never trips. */
    @Test
    public void aGenuineQuorumLossNeverTrips() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = gatedInCanvass(lf);
        quorum.set(false);
        for (long t = 0; t <= 100 * WAIT_NS; t += WAIT_NS / 4) {
            assertFalse("t=" + t, w.poll(t));
        }
        assertFalse(lf.stopRequested());
        assertFalse(w.tripped());
        assertEquals(0, criticalLines());
        assertEquals("said once per stretch: " + logs, 1, logs.stream()
                .filter(l -> l.contains("no quorum of members is reachable") && l.contains("not exiting")).count());
    }

    /** The stuck election with reachable peers (the wedge): trips one wait after the election opened. */
    @Test
    public void aStuckElectionWithAReachableQuorumTrips() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = gatedInCanvass(lf);
        quorum.set(true);
        assertFalse(w.poll(0));
        assertFalse(w.poll(WAIT_NS - 1));
        assertTrue(w.poll(WAIT_NS));
        final String critical = logs.stream().filter(l -> l.contains("CRITICAL")).findFirst().orElseThrow();
        assertTrue(critical, critical.contains("with a quorum of members reachable for 1000 ms"));
    }

    /** Peers that return after a long quorum loss get a full wait to finish the election. */
    @Test
    public void theWaitStartsWhenTheQuorumBecomesReachable() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = gatedInCanvass(lf);
        quorum.set(false);
        assertFalse(w.poll(0));
        assertFalse(w.poll(10 * WAIT_NS));
        quorum.set(true);
        final long back = 10 * WAIT_NS + 1;
        assertFalse("the election has been open for ten waits, the quorum reachable for none", w.poll(back));
        assertFalse(w.poll(back + WAIT_NS - 1));
        assertFalse(lf.stopRequested());
        assertTrue(w.poll(back + WAIT_NS));
    }

    /** One missed answer inside the window keeps the clock; evidence older than the window clears it. */
    @Test
    public void evidenceCountsForTheWindowAndNoLonger() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = gatedInCanvass(lf);
        quorum.set(true);
        assertFalse(w.poll(0));
        quorum.set(false);
        assertFalse(w.poll(WINDOW_NS)); // the last evidence is exactly one window old: still counts
        quorum.set(true);
        assertFalse(w.poll(WAIT_NS - 1));
        assertTrue("a blip inside the window did not restart the clock", w.poll(WAIT_NS));

        final EngineLifecycle lf2 = lifecycle();
        final RejoinWatchdog w2 = gatedInCanvass(lf2);
        quorum.set(true);
        assertFalse(w2.poll(0));
        quorum.set(false);
        assertFalse(w2.poll(WINDOW_NS + 1)); // older than the window: the peers are gone
        quorum.set(true);
        final long back = WAIT_NS / 2;
        assertFalse(w2.poll(back));
        assertFalse("the clock restarted when the quorum came back", w2.poll(WAIT_NS + 1));
        assertFalse(w2.poll(back + WAIT_NS - 1));
        assertTrue(w2.poll(back + WAIT_NS));
    }

    /** A new election stretch asks again: a closed election clears the evidence and the quorum-loss line. */
    @Test
    public void aClosedElectionClearsTheEvidence() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = gatedInCanvass(lf);
        quorum.set(true);
        assertFalse(w.poll(0));
        state.set(ElectionState.CLOSED.code());
        assertFalse(w.poll(WAIT_NS / 2));
        elections.set(3);
        state.set(ElectionState.CANVASS.code());
        quorum.set(false);
        assertFalse(w.poll(WAIT_NS));
        assertFalse(w.poll(3 * WAIT_NS));
        assertFalse(lf.stopRequested());
    }

    /** A check that throws is no evidence; stop closes the probe behind the supplier. */
    @Test
    public void aFailingCheckIsNoEvidenceAndStopClosesTheProbe() {
        final EngineLifecycle lf = lifecycle();
        final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        final RejoinWatchdog w = new RejoinWatchdog(lf, state::get, elections::get, WAIT_MS, () -> {
            throw new IllegalStateException("probe broken");
        }, WINDOW_MS, () -> closed.set(true));
        assertFalse(w.poll(0));
        elections.set(2);
        state.set(ElectionState.CANVASS.code());
        assertFalse(w.poll(1));
        assertFalse(w.poll(5 * WAIT_NS));
        assertFalse(lf.stopRequested());
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("peer reachability check failed")));
        w.stop(10);
        assertTrue(closed.get());
    }

    @Test
    public void thePeerWindowIsARangedSetting() {
        assertEquals("ENGINE_REJOIN_PEER_WINDOW_MS", EngineLifecycle.REJOIN_PEER_WINDOW_ENV);
        assertEquals("engine.rejoin.peer.window.ms", EngineLifecycle.REJOIN_PEER_WINDOW_PROP);
        assertEquals(10_000L, EngineLifecycle.DEFAULT_REJOIN_PEER_WINDOW_MS);
        assertEquals(10_000L, EngineLifecycle.parseRanged("E", null, "p", null, 10_000, 1_000, 600_000));
        assertEquals(1_000L, EngineLifecycle.parseRanged("E", "1000", "p", null, 10_000, 1_000, 600_000));
        assertEquals(600_000L, EngineLifecycle.parseRanged("E", "", "p", "600000", 10_000, 1_000, 600_000));
        for (final String bad : new String[] {"999", "600001", "0", "-1", "abc"}) {
            try {
                EngineLifecycle.parseRanged("ENGINE_REJOIN_PEER_WINDOW_MS", bad, "p", null, 10_000, 1_000, 600_000);
                fail("accepted " + bad);
            } catch (final IllegalArgumentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("ENGINE_REJOIN_PEER_WINDOW_MS")
                        && expected.getMessage().contains("1000..600000"));
            }
        }
    }

    /**
     * A genuine quorum loss with the probe already open: the peers still read reachable for a moment after they
     * died, then never again. Time after the last positive answer is not evidence, whatever the window: with a wait
     * shorter than the window the watchdog still does not trip.
     */
    @Test
    public void unconfirmedTimeInsideTheWindowDoesNotCountTowardTheTrip() {
        final EngineLifecycle lf = lifecycle();
        final RejoinWatchdog w = new RejoinWatchdog(lf, state::get, elections::get, 5_000, quorum::get,
                EngineLifecycle.DEFAULT_REJOIN_PEER_WINDOW_MS, null);
        assertFalse(w.poll(0));
        elections.set(2);
        state.set(ElectionState.CANVASS.code());
        quorum.set(true);
        assertFalse(w.poll(0));
        assertFalse(w.poll(TimeUnit.MILLISECONDS.toNanos(1_000)));
        quorum.set(false); // the peers are gone for good
        for (long ms = 1_050; ms <= 60_000; ms += 50) {
            assertFalse("tripped at " + ms + " ms during a quorum loss: " + logs,
                    w.poll(TimeUnit.MILLISECONDS.toNanos(ms)));
        }
        assertFalse(lf.stopRequested());
        assertEquals(0, criticalLines());
    }
}
