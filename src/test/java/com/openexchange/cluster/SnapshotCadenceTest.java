// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The cadence drives a REAL control toggle here - the same counter and the same
 * {@link ClusterControl.ToggleState} transitions the consensus module reads.
 * Asserting on a boolean flag instead would prove the class talks to itself.
 */
public class SnapshotCadenceTest {

    private static final long GIB = 1L << 30;
    private static final long MINUTE_NS = TimeUnit.MINUTES.toNanos(1);
    private static final long SECOND_NS = TimeUnit.SECONDS.toNanos(1);

    private AtomicCounter toggle;

    @Before
    public void setUp() {
        // A counter the way the consensus module publishes it: NEUTRAL, waiting.
        final UnsafeBuffer values = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));
        toggle = new AtomicCounter(values, 0);
        ClusterControl.ToggleState.reset(toggle);
        assertEquals(ClusterControl.ToggleState.NEUTRAL, ClusterControl.ToggleState.get(toggle));
    }

    @Test
    public void leaderRequestsASnapshotWhenTheLogPassesTheThreshold() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> toggle, 0, 0);

        assertEquals(0, cadence.tick(SECOND_NS, true, GIB - 1));
        assertEquals(ClusterControl.ToggleState.NEUTRAL, ClusterControl.ToggleState.get(toggle));

        assertEquals(1, cadence.tick(2 * SECOND_NS, true, GIB));
        assertEquals(ClusterControl.ToggleState.SNAPSHOT, ClusterControl.ToggleState.get(toggle));
        assertEquals(1, cadence.requestCount());
    }

    /**
     * The bytes are counted from the last snapshot, not from zero. A node that
     * recovered from a snapshot at 4 GiB must not decide it is 4 GiB overdue.
     */
    @Test
    public void bytesAreCountedFromTheRecoveryPosition() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> toggle, 4 * GIB, 0);

        assertEquals(0, cadence.tick(SECOND_NS, true, 4 * GIB + GIB - 1));
        assertEquals(1, cadence.tick(2 * SECOND_NS, true, 5 * GIB));
    }

    /**
     * A follower flipping the toggle would do nothing except leave a stale one
     * behind, which fires a spurious snapshot the moment it is elected.
     */
    @Test
    public void aFollowerNeverTouchesTheToggle() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> toggle, 0, 0);

        assertEquals(0, cadence.tick(SECOND_NS, false, 100 * GIB));
        assertEquals(ClusterControl.ToggleState.NEUTRAL, ClusterControl.ToggleState.get(toggle));
        assertEquals(0, cadence.requestCount());
    }

    /** The timer is the floor for a market with no volume at all. */
    @Test
    public void theTimerFiresInAQuietMarket() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 5 * MINUTE_NS);
        cadence.bind(() -> toggle, 0, 0);

        assertEquals(0, cadence.tick(4 * MINUTE_NS, true, 0));
        assertEquals(1, cadence.tick(5 * MINUTE_NS, true, 0));
        assertEquals(ClusterControl.ToggleState.SNAPSHOT, ClusterControl.ToggleState.get(toggle));
    }

    /** Both triggers off is a cluster that never snapshots - it must still be inert, not spinning. */
    @Test
    public void bothTriggersDisabledMeansNoRequests() {
        final SnapshotCadence cadence = new SnapshotCadence(0, 0);
        cadence.bind(() -> toggle, 0, 0);

        assertEquals(0, cadence.tick(60 * MINUTE_NS, true, 100 * GIB));
        assertEquals(ClusterControl.ToggleState.NEUTRAL, ClusterControl.ToggleState.get(toggle));
    }

    /**
     * A snapshot takes seconds; the evaluation runs every second. Re-requesting
     * while one is in flight would queue nothing (the CAS refuses) but would
     * hammer the log with "deferring" every second, which is how a real problem
     * gets buried.
     */
    @Test
    public void oneRequestAtATime() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> toggle, 0, 0);

        assertEquals(1, cadence.tick(SECOND_NS, true, GIB));
        assertTrue(cadence.requestInFlight());

        // Still SNAPSHOT: the module has not picked it up yet.
        assertEquals(0, cadence.tick(2 * SECOND_NS, true, 2 * GIB));
        assertEquals(0, cadence.tick(3 * SECOND_NS, true, 3 * GIB));
        assertEquals(1, cadence.requestCount());
    }

    /** onTakeSnapshot is what closes the request, on every member. */
    @Test
    public void takingTheSnapshotResetsBothBaselines() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 5 * MINUTE_NS);
        cadence.bind(() -> toggle, 0, 0);

        assertEquals(1, cadence.tick(SECOND_NS, true, GIB));
        cadence.snapshotTaken(GIB, 2 * SECOND_NS);
        ClusterControl.ToggleState.reset(toggle); // what the consensus module does when it finishes

        assertFalse(cadence.requestInFlight());
        assertEquals(0, cadence.logBytesSinceSnapshot(GIB));

        // Neither trigger is due again immediately.
        assertEquals(0, cadence.tick(3 * SECOND_NS, true, GIB + 1));
        // ...and the timer now runs from the snapshot, not from bind.
        assertEquals(0, cadence.tick(5 * MINUTE_NS, true, GIB + 1));
        assertEquals(1, cadence.tick(5 * MINUTE_NS + 2 * SECOND_NS, true, GIB + 1));
    }

    /**
     * A snapshot somebody else asked for (ClusterTool, another node's cadence)
     * still moves this node's baseline: the point is the cluster's rhythm, not
     * this node's bookkeeping.
     */
    @Test
    public void aSnapshotThisNodeDidNotRequestStillCounts() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> toggle, 0, 0);

        cadence.snapshotTaken(GIB / 2, SECOND_NS);
        assertEquals(0, cadence.tick(2 * SECOND_NS, true, GIB));
        assertEquals(1, cadence.tick(3 * SECOND_NS, true, GIB / 2 + GIB));
    }

    /** Losing leadership drops the request: the new leader owns the decision. */
    @Test
    public void losingLeadershipDropsTheRequest() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> toggle, 0, 0);

        assertEquals(1, cadence.tick(SECOND_NS, true, GIB));
        assertTrue(cadence.requestInFlight());

        cadence.roleChanged(false);
        assertFalse(cadence.requestInFlight());
    }

    /**
     * doBackgroundWork runs millions of times a second. Everything past the
     * one-second gate has to be unreachable, or the cadence is a hot-path cost.
     */
    @Test
    public void evaluationIsGatedToOncePerSecond() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> toggle, 0, 0);

        for (long ns = 1; ns < SECOND_NS; ns += SECOND_NS / 10) {
            assertEquals(0, cadence.tick(ns, true, 100 * GIB));
        }
        assertEquals(ClusterControl.ToggleState.NEUTRAL, ClusterControl.ToggleState.get(toggle));
        assertEquals(1, cadence.tick(SECOND_NS, true, 100 * GIB));
    }

    /** No toggle published means no snapshots, and it must be said, not assumed away. */
    @Test
    public void withoutAToggleNothingIsRequested() {
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> null, 0, 0);

        assertEquals(0, cadence.tick(SECOND_NS, true, 100 * GIB));
    }

    /**
     * A typo in the environment must not silently restore the default: the
     * operator would read their own value back from nowhere and believe it.
     */
    @Test
    public void anUnreadableEnvironmentValueIsRefused() {
        assertEquals(7L, SnapshotCadence.longFromEnv(
            "CLUSTER_KIT_ABSENT_ON_PURPOSE", "cluster.kit.absent.on.purpose", 7L));
        try {
            // PATH is always set and never a number.
            SnapshotCadence.longFromEnv("PATH", "cluster.kit.absent.on.purpose", 7L);
            fail("expected a refusal");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("PATH"));
        }
    }

    /** The property is the fallback the engines already use for every other setting. */
    @Test
    public void aSystemPropertyIsReadWhenTheEnvironmentIsSilent() {
        System.setProperty("cluster.kit.test.threshold", "4096");
        try {
            assertEquals(4096L, SnapshotCadence.longFromEnv(
                "CLUSTER_KIT_ABSENT_ON_PURPOSE", "cluster.kit.test.threshold", 7L));
        } finally {
            System.clearProperty("cluster.kit.test.threshold");
        }
    }

    /** "yes" is not false; a switch that cannot be read is refused, not assumed off. */
    @Test
    public void anUnreadableBooleanIsRefused() {
        System.setProperty("cluster.kit.test.flag", "yes");
        try {
            SnapshotCadence.booleanFromEnv(
                "CLUSTER_KIT_ABSENT_ON_PURPOSE", "cluster.kit.test.flag", true);
            fail("expected a refusal");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("true or false"));
        } finally {
            System.clearProperty("cluster.kit.test.flag");
        }
    }

    /**
     * The consensus module may publish the toggle after the service starts. A
     * node that resolved null once and gave up would never snapshot again, and
     * would look perfectly healthy doing it.
     */
    @Test
    public void aToggleThatAppearsLateIsPickedUp() {
        final AtomicCounter[] published = new AtomicCounter[1];
        final SnapshotCadence cadence = new SnapshotCadence(GIB, 0);
        cadence.bind(() -> published[0], 0, 0);

        assertEquals(0, cadence.tick(SECOND_NS, true, 2 * GIB));
        published[0] = toggle;
        assertEquals(1, cadence.tick(2 * SECOND_NS, true, 2 * GIB));
        assertEquals(ClusterControl.ToggleState.SNAPSHOT, ClusterControl.ToggleState.get(toggle));
    }

    @Test
    public void negativeCadenceIsRefused() {
        try {
            new SnapshotCadence(-1, 0);
            fail("expected a refusal");
        } catch (final IllegalArgumentException expected) {
            assertNull(expected.getCause());
        }
    }
}
