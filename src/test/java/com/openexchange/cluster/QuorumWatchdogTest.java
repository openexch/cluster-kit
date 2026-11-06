// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.ElectionState;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S17 W12 (R-KIT-Q, K3 to K3d): a node whose consensus module never learns of a leader exits loudly after
 * ENGINE_QUORUM_WAIT_MS; a node that has seen any leader-known election state (replay, catch-up, log replication,
 * ready) or CLOSED is disarmed for the process lifetime. Driven with an explicit clock, no thread.
 */
public class QuorumWatchdogTest {

    private static final long WAIT_MS = 1_000;
    private static final long WAIT_NS = TimeUnit.MILLISECONDS.toNanos(WAIT_MS);

    private final List<Integer> halts = new CopyOnWriteArrayList<>();
    private final List<String> logs = new CopyOnWriteArrayList<>();
    private final AtomicLong state = new AtomicLong(ElectionState.CANVASS.code());
    private final AtomicLong elections = new AtomicLong(1);

    private EngineLifecycle lifecycle() {
        return new EngineLifecycle("test-node", logs::add, halts::add, 1_000, 50);
    }

    private QuorumWatchdog watchdog(final EngineLifecycle lf) {
        return new QuorumWatchdog(lf, state::get, elections::get, WAIT_MS);
    }

    private boolean fatalLogged() {
        return logs.stream().anyMatch(l -> l.contains("no cluster quorum observed within " + WAIT_MS
                + " ms since the consensus module launched"));
    }

    @Test
    public void stuckInCanvassForTheWaitExitsOne() throws Exception {
        final EngineLifecycle lf = lifecycle();
        final QuorumWatchdog w = watchdog(lf);
        final long t0 = 1_000_000_000L;
        w.start(t0);
        assertFalse(w.poll(t0 + WAIT_NS - 1));
        assertFalse(lf.stopRequested());
        assertTrue(w.poll(t0 + WAIT_NS));
        assertTrue(lf.stopRequested());
        assertEquals(EngineLifecycle.Cause.FATAL, lf.cause());
        assertTrue(logs.toString(), fatalLogged());
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("FATAL (exit 1)")
                && l.contains("election state CANVASS, elections 1")));
        lf.awaitStop();
        lf.finish();
        assertEquals(List.of(1), halts);
    }

    @Test
    public void leaderKnownStatesNeverHalt() {
        for (final ElectionState s : List.of(ElectionState.FOLLOWER_CATCHUP, ElectionState.LEADER_REPLAY,
                ElectionState.FOLLOWER_LOG_AWAIT, ElectionState.FOLLOWER_REPLAY, ElectionState.CLOSED)) {
            final EngineLifecycle lf = lifecycle();
            final QuorumWatchdog w = watchdog(lf);
            state.set(s.code());
            w.start(0);
            for (long t = 0; t <= 3 * WAIT_NS; t += WAIT_NS / 4) {
                assertFalse(s.name(), w.poll(t));
            }
            assertTrue(s.name(), w.disarmed());
            assertFalse(s.name(), lf.stopRequested());
        }
    }

    @Test
    public void onceALeaderIsKnownTheWatchdogIsDisarmedForGood() {
        final EngineLifecycle lf = lifecycle();
        final QuorumWatchdog w = watchdog(lf);
        w.start(0);
        assertFalse(w.poll(WAIT_NS / 2));
        state.set(ElectionState.FOLLOWER_REPLAY.code());
        assertFalse(w.poll(WAIT_NS / 2 + 1));
        assertTrue(w.disarmed());
        state.set(ElectionState.CANVASS.code()); // a later leader loss is an outage, not a foreign state start
        assertFalse(w.poll(10 * WAIT_NS));
        assertFalse(lf.stopRequested());
    }

    @Test
    public void movesInsideTheNoLeaderSetDoNotRestartTheClock() {
        final EngineLifecycle lf = lifecycle();
        final QuorumWatchdog w = watchdog(lf);
        w.start(0);
        assertFalse(w.poll(WAIT_NS / 3));
        state.set(ElectionState.FOLLOWER_BALLOT.code());
        assertFalse(w.poll(2 * WAIT_NS / 3));
        state.set(ElectionState.CANVASS.code());
        assertFalse(w.poll(WAIT_NS - 1));
        assertTrue("an uninterrupted wait in the set halts", w.poll(WAIT_NS));
        assertTrue(fatalLogged());
    }

    @Test
    public void leavingTheSetWithoutALeaderStateRestartsTheClockOnReentry() {
        final EngineLifecycle lf = lifecycle();
        final QuorumWatchdog w = watchdog(lf);
        w.start(0);
        assertFalse(w.poll(WAIT_NS / 2));
        state.set(-1); // counter not readable: not in the set, not a leader
        assertFalse(w.poll(WAIT_NS));
        assertFalse(w.disarmed());
        state.set(ElectionState.CANVASS.code());
        assertFalse(w.poll(WAIT_NS + 1));
        assertFalse("the clock restarted at the re-entry", w.poll(2 * WAIT_NS));
        assertTrue(w.poll(2 * WAIT_NS + 1));
    }

    @Test
    public void theClockStartsAtStartNotAtConstruction() {
        final EngineLifecycle lf = lifecycle();
        final QuorumWatchdog w = watchdog(lf);
        // the driver wait and the DNS wait pass before ConsensusModule.launch returns: nothing runs before start
        assertFalse(w.poll(0));
        assertFalse(w.poll(2 * WAIT_NS));
        assertFalse(lf.stopRequested());
        final long started = 2 * WAIT_NS;
        w.start(started);
        assertFalse(w.poll(started + WAIT_NS - 1));
        assertTrue(w.poll(started + WAIT_NS));
    }

    @Test
    public void theNoLeaderSetIsPinnedAgainstTheAeronElectionStates() {
        // review V1: INIT is neither (the CM waits there for the service's onStart snapshot load): it stops the clock
        final Set<ElectionState> expected = EnumSet.of(ElectionState.CANVASS,
                ElectionState.NOMINATE, ElectionState.CANDIDATE_BALLOT, ElectionState.FOLLOWER_BALLOT);
        assertEquals(expected, QuorumWatchdog.NO_LEADER_STATES);
        assertFalse(QuorumWatchdog.isNoLeader(ElectionState.INIT.code()));
        assertFalse(QuorumWatchdog.isLeaderKnown(ElectionState.INIT.code()));
        for (final ElectionState s : ElectionState.values()) {
            final boolean noLeader = expected.contains(s);
            final boolean leaderKnown = s.name().startsWith("LEADER_")
                    || (s.name().startsWith("FOLLOWER_") && s != ElectionState.FOLLOWER_BALLOT)
                    || s == ElectionState.CLOSED;
            final boolean waiting = s == ElectionState.INIT;
            assertTrue("unclassified election state " + s + ": re-check the watchdog against this Aeron",
                    (noLeader ? 1 : 0) + (leaderKnown ? 1 : 0) + (waiting ? 1 : 0) == 1);
            assertEquals(s.name(), noLeader, QuorumWatchdog.isNoLeader(s.code()));
            assertEquals(s.name(), leaderKnown, QuorumWatchdog.isLeaderKnown(s.code()));
        }
        assertEquals("the pinned Aeron's state count", 18, ElectionState.values().length);
        assertFalse(QuorumWatchdog.isNoLeader(-1));
        assertFalse(QuorumWatchdog.isLeaderKnown(-1));
    }

    /**
     * K3e (review V1): INIT for 3 x the wait (the service's onStart snapshot load), then CANVASS: the clock starts at
     * the CANVASS entry, so the watchdog trips one full wait after it and not before.
     */
    @Test
    public void aLongInitDwellNeverTripsAndTheClockStartsAtCanvass() {
        final EngineLifecycle lf = lifecycle();
        final QuorumWatchdog w = watchdog(lf);
        state.set(ElectionState.INIT.code());
        w.start(0);
        for (long t = 0; t <= 3 * WAIT_NS; t += WAIT_NS / 4) {
            assertFalse("INIT never trips (t=" + t + ")", w.poll(t));
        }
        assertFalse(w.disarmed());
        final long canvassAt = 3 * WAIT_NS + 1;
        state.set(ElectionState.CANVASS.code());
        assertFalse(w.poll(canvassAt));
        assertFalse(w.poll(canvassAt + WAIT_NS - 1));
        assertFalse(lf.stopRequested());
        assertTrue("one full wait after the CANVASS entry", w.poll(canvassAt + WAIT_NS));
        assertTrue(fatalLogged());
    }

    /** Review V2: awaitStop stops and joins the poller before the components close; no counter read afterwards. */
    @Test
    public void awaitStopStopsThePollerBeforeTheComponentsClose() throws Exception {
        final EngineLifecycle lf = new EngineLifecycle("test-node", logs::add, halts::add, 1_000, 50, 60_000);
        final AtomicLong reads = new AtomicLong();
        final QuorumWatchdog w = lf.watchQuorum(() -> {
            reads.incrementAndGet();
            return ElectionState.CANVASS.code();
        }, elections::get);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (reads.get() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("the poller runs", reads.get() >= 2 && w.pollerAlive());
        lf.onClusterTermination();
        lf.awaitStop();
        assertTrue(w.stopped());
        assertFalse("the poller is joined before the close", w.pollerAlive());
        final long after = reads.get();
        Thread.sleep(300);
        assertEquals("no counter read after awaitStop", after, reads.get());
        assertFalse(w.poll(Long.MAX_VALUE));
        assertEquals(after, reads.get());
    }

    @Test
    public void quorumWaitIsAPositiveSettingWithTheProductionDefault() {
        assertEquals(120_000L, EngineLifecycle.DEFAULT_QUORUM_WAIT_MS);
        assertEquals("ENGINE_QUORUM_WAIT_MS", EngineLifecycle.QUORUM_WAIT_ENV);
        assertEquals("engine.quorum.wait.ms", EngineLifecycle.QUORUM_WAIT_PROP);
    }
}
