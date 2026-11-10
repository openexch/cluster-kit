// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The standby schedule of {@link SnapshotCadence} (interval, lost and ambiguous accounting, no reset by a member
 * snapshot) against a REAL control toggle counter (a fake {@link AtomicCounter} over an {@link UnsafeBuffer}) and a
 * fake clock. The consensus module is played by the test: it resets the toggle to NEUTRAL when it has appended the
 * action (CMA:2575-2581).
 */
public class StandbyCadenceTest {

    private static final long SECOND_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long MINUTE_NS = TimeUnit.MINUTES.toNanos(1);
    private static final long STANDBY_NS = 15 * MINUTE_NS;
    private static final long GIB = 1L << 30;

    private AtomicCounter toggle;

    @Before
    public void setUp() {
        toggle = new AtomicCounter(new UnsafeBuffer(ByteBuffer.allocateDirect(4096)), 0);
        ClusterControl.ToggleState.reset(toggle);
    }

    @After
    public void clearProperty() {
        System.clearProperty("snapshot.standby.interval.minutes");
    }

    private ClusterControl.ToggleState state() {
        return ClusterControl.ToggleState.get(toggle);
    }

    private void cmAppendsAndResets() {
        ClusterControl.ToggleState.reset(toggle);
    }

    /** The schedule must never ask members for a snapshot (the leader pays only the toggle). */
    static void assertNeverMemberSnapshot(final ClusterControl.ToggleState s, final SnapshotCadence c) {
        if (s == ClusterControl.ToggleState.SNAPSHOT || c.requestCount() != 0) {
            throw new AssertionError("member snapshot requested by the standby schedule (toggle=" + s
                    + " requests=" + c.requestCount() + ")");
        }
    }

    static void assertLostCounted(final SnapshotCadence c, final long expected) {
        if (c.standbyLostCount() != expected) {
            throw new AssertionError("lost standby request not counted: lost=" + c.standbyLostCount() + " expected="
                    + expected);
        }
    }

    private static SnapshotCadence standbyOnly(final EnumSet<SnapshotCadence.Control> off) {
        return new SnapshotCadence(0, 0, STANDBY_NS, off);
    }

    // ------------------------------------------------------------------------------------------------- schedule

    private void scheduleTogglesStandbyAtTheInterval(final SnapshotCadence c) {
        c.bind(() -> toggle, 0, 0);
        for (long t = SECOND_NS; t < STANDBY_NS; t += 30 * SECOND_NS) {
            assertEquals(0, c.tick(t, true, 4096));
            assertEquals("standby fired before its interval at " + t, ClusterControl.ToggleState.NEUTRAL, state());
        }
        c.tick(STANDBY_NS, true, 4096);
        assertNeverMemberSnapshot(state(), c);
        assertEquals("standby schedule did not toggle STANDBY_SNAPSHOT at " + STANDBY_NS,
                ClusterControl.ToggleState.STANDBY_SNAPSHOT, state());
        assertEquals(1, c.standbyRequestCount());
        assertTrue("standby request not in flight", c.standbyInFlight());
    }

    @Test
    public void theScheduleTogglesStandbyAtTheIntervalAndNeverSnapshot() {
        scheduleTogglesStandbyAtTheInterval(standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class)));
    }

    @Test
    public void aCmResetToNeutralCompletesItAndRestartsTheClock() {
        final SnapshotCadence c = standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class));
        scheduleTogglesStandbyAtTheInterval(c);
        cmAppendsAndResets();
        final long done = STANDBY_NS + 2 * SECOND_NS;
        c.tick(done, true, 8192);
        assertFalse("NEUTRAL did not complete the standby request", c.standbyInFlight());
        assertEquals(1, c.standbyCompletedCount());
        assertEquals(ClusterControl.ToggleState.NEUTRAL, state());
        c.tick(done + STANDBY_NS - 2 * SECOND_NS, true, 8192);
        assertEquals("standby clock did not restart at completion", ClusterControl.ToggleState.NEUTRAL, state());
        c.tick(done + STANDBY_NS, true, 8192);
        assertEquals("second standby not toggled one interval after completion",
                ClusterControl.ToggleState.STANDBY_SNAPSHOT, state());
        assertEquals(2, c.standbyRequestCount());
        assertNeverMemberSnapshot(state(), c);
    }

    /** An ALIGNED member snapshot does not delay the next STANDBY. */
    @Test
    public void snapshotTakenDoesNotDelayTheNextStandby() {
        final SnapshotCadence c = standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class));
        c.bind(() -> toggle, 0, 0);
        c.tick(MINUTE_NS, true, 4096);
        c.snapshotTaken(4096, 10 * MINUTE_NS);
        c.snapshotTaken(8192, 14 * MINUTE_NS);
        c.tick(STANDBY_NS, true, 8192);
        assertEquals("snapshotTaken delayed the next STANDBY", ClusterControl.ToggleState.STANDBY_SNAPSHOT, state());
    }

    @Test
    public void aSnapshotInFlightDefersIt() {
        final SnapshotCadence c = new SnapshotCadence(GIB, 0, STANDBY_NS, EnumSet.noneOf(SnapshotCadence.Control.class));
        c.bind(() -> toggle, 0, 0);
        assertEquals(1, c.tick(STANDBY_NS, true, GIB));
        assertEquals(ClusterControl.ToggleState.SNAPSHOT, state());
        for (int i = 1; i <= 5; i++) {
            c.tick(STANDBY_NS + i * SECOND_NS, true, GIB);
        }
        // One due standby deferred over five evaluations counts once
        assertEquals("standby_toggle_deferred_total counts evaluations, not deferrals", 1, c.standbyDeferredCount());
        assertEquals(0, c.standbyRequestCount());
        assertEquals(ClusterControl.ToggleState.SNAPSHOT, state());
        // the member snapshot completes; the standby goes on the next evaluation
        c.snapshotTaken(GIB, STANDBY_NS + 6 * SECOND_NS);
        cmAppendsAndResets();
        c.tick(STANDBY_NS + 7 * SECOND_NS, true, GIB);
        assertEquals("deferred standby not toggled once the toggle was free",
                ClusterControl.ToggleState.STANDBY_SNAPSHOT, state());
        assertEquals(1, c.standbyRequestCount());
    }

    @Test
    public void anOperatorHoldingTheToggleDefersIt() {
        final SnapshotCadence c = standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class));
        c.bind(() -> toggle, 0, 0);
        ClusterControl.ToggleState.SUSPEND.toggle(toggle);
        c.tick(STANDBY_NS, true, 4096);
        assertEquals("operator toggle deferral not counted", 1, c.standbyDeferredCount());
        for (int i = 1; i <= 5; i++) {
            c.tick(STANDBY_NS + i * SECOND_NS, true, 4096);
        }
        assertEquals("standby_toggle_deferred_total counts evaluations, not deferrals", 1, c.standbyDeferredCount());
        assertEquals(ClusterControl.ToggleState.SUSPEND, state());
        assertFalse(c.standbyInFlight());
        // the toggle frees: the deferred standby goes, still counted once
        ClusterControl.ToggleState.reset(toggle);
        c.tick(STANDBY_NS + 6 * SECOND_NS, true, 4096);
        assertEquals(ClusterControl.ToggleState.STANDBY_SNAPSHOT, state());
        assertEquals(1, c.standbyDeferredCount());
    }

    // ---------------------------------------------------------------------------------------- lost requests

    private void roleChangeLosesIt(final SnapshotCadence c) {
        scheduleTogglesStandbyAtTheInterval(c);
        c.roleChanged(false);
        assertLostCounted(c, 1);
        assertFalse("roleChanged(false) did not clear the standby request", c.standbyInFlight());
    }

    @Test
    public void roleChangedWithAStandbyInFlightCountsLost() {
        roleChangeLosesIt(standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class)));
    }

    @Test
    public void aToggleOverwrittenToInactiveCountsLost() {
        final SnapshotCadence c = standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class));
        scheduleTogglesStandbyAtTheInterval(c);
        toggle.set(ClusterControl.ToggleState.INACTIVE.code());
        c.tick(STANDBY_NS + SECOND_NS, true, 4096);
        assertLostCounted(c, 1);
        assertFalse(c.standbyInFlight());
        assertEquals(0, c.standbyAmbiguousCount());
    }

    /** SNAPSHOT seen while the standby was in flight is ambiguous, never lost. */
    @Test
    public void aSnapshotSeenInFlightIsAmbiguousNotLost() {
        final SnapshotCadence c = standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class));
        scheduleTogglesStandbyAtTheInterval(c);
        cmAppendsAndResets();
        ClusterControl.ToggleState.SNAPSHOT.toggle(toggle); // an operator right after the CM reset
        c.tick(STANDBY_NS + SECOND_NS, true, 4096);
        assertEquals("SNAPSHOT in flight not counted ambiguous", 1, c.standbyAmbiguousCount());
        assertLostCounted(c, 0);
        assertFalse(c.standbyInFlight());
    }

    @Test
    public void staleCountsLostOnceAndTheScheduleContinues() {
        final SnapshotCadence c = standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class));
        scheduleTogglesStandbyAtTheInterval(c);
        long t = STANDBY_NS;
        for (int i = 0; i < 125; i++) {
            t += SECOND_NS;
            c.tick(t, true, 4096);
        }
        assertLostCounted(c, 1);
        assertFalse("stale standby request not cleared", c.standbyInFlight());
        // the toggle is still held (the CM never answered): the next due standby defers, the lost count stays 1
        c.tick(t + STANDBY_NS, true, 4096);
        assertLostCounted(c, 1);
        assertTrue(c.standbyDeferredCount() >= 1);
        cmAppendsAndResets();
        c.tick(t + STANDBY_NS + SECOND_NS, true, 4096);
        assertEquals("the schedule did not continue after a stale request", 2, c.standbyRequestCount());
        assertEquals(ClusterControl.ToggleState.STANDBY_SNAPSHOT, state());
    }

    // ------------------------------------------------------------------------------------------------- off

    @Test
    public void standbyZeroNeverToggles() {
        final SnapshotCadence c = new SnapshotCadence(0, 0, 0, EnumSet.noneOf(SnapshotCadence.Control.class));
        c.bind(() -> toggle, 0, 0);
        final SnapshotCadence legacy = new SnapshotCadence(0, 0);
        legacy.bind(() -> toggle, 0, 0);
        for (long t = SECOND_NS; t < 3 * 60 * MINUTE_NS; t += 10 * SECOND_NS) {
            c.tick(t, true, 4096);
            legacy.tick(t, true, 4096);
            assertEquals("standby 0 toggled at " + t, ClusterControl.ToggleState.NEUTRAL, state());
        }
        assertEquals(0, c.standbyRequestCount() + legacy.standbyRequestCount());
    }

    @Test
    public void aFollowerNeverToggles() {
        final SnapshotCadence c = standbyOnly(EnumSet.noneOf(SnapshotCadence.Control.class));
        c.bind(() -> toggle, 0, 0);
        for (long t = SECOND_NS; t < 3 * STANDBY_NS; t += 10 * SECOND_NS) {
            c.tick(t, false, 4096);
        }
        assertEquals("a follower toggled the standby", ClusterControl.ToggleState.NEUTRAL, state());
        assertEquals(0, c.standbyRequestCount());
    }

    // ------------------------------------------------------------------------------------------------- env

    @Test
    public void fromEnvDefaultsTo15MinutesAndRefusesOutOfRange() {
        assertEquals(TimeUnit.MINUTES.toNanos(SnapshotCadence.DEFAULT_STANDBY_INTERVAL_MINUTES),
                SnapshotCadence.fromEnv().standbyIntervalNs());
        assertEquals(15, SnapshotCadence.DEFAULT_STANDBY_INTERVAL_MINUTES);
        System.setProperty("snapshot.standby.interval.minutes", "0");
        assertEquals(0, SnapshotCadence.fromEnv().standbyIntervalNs());
        System.setProperty("snapshot.standby.interval.minutes", "1440");
        assertEquals(TimeUnit.MINUTES.toNanos(1440), SnapshotCadence.fromEnv().standbyIntervalNs());
        for (final String bad : new String[] {"1441", "-1", "x"}) {
            System.setProperty("snapshot.standby.interval.minutes", bad);
            try {
                SnapshotCadence.fromEnv();
                fail("snapshot.standby.interval.minutes=" + bad + " accepted");
            } catch (final IllegalArgumentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("snapshot.standby.interval.minutes"));
            }
        }
    }

    // ------------------------------------------------------------------------------------------------- controls

    @Test
    public void negativeControlLostCountingOff() {
        try {
            roleChangeLosesIt(standbyOnly(EnumSet.of(SnapshotCadence.Control.LOST_COUNTING_OFF)));
            fail("NC LOST_COUNTING_OFF: the lost assertion passed with counting off");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("lost standby request not counted"));
            System.out.println("NC LOST_COUNTING_OFF -> " + expected.getMessage());
        }
    }

    @Test
    public void negativeControlStandbyUsesSnapshot() {
        try {
            scheduleTogglesStandbyAtTheInterval(standbyOnly(EnumSet.of(SnapshotCadence.Control.STANDBY_USES_SNAPSHOT)));
            fail("NC STANDBY_USES_SNAPSHOT: the schedule assertion passed with SNAPSHOT");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().startsWith("member snapshot requested by the standby schedule"));
            System.out.println("NC STANDBY_USES_SNAPSHOT -> " + expected.getMessage());
        }
    }
}
