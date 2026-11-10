// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.NodeReadiness;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Kit half of the readiness rules: every readiness input with a fake clock; the backup state is never an
 * input, so a query cycle cannot flap {@code /ready}; the {@code READINESS_FROM_BACKUP_STATE} control flaps.
 */
public class ReplicaReadinessTest {

    static final long SHADOW_AGE_MS = 30 * 60_000L;
    static final long TIME_LAG_MS = 30_000;
    static final long MAX_LAG = 64L * 1024 * 1024;

    final long[] nano = {TimeUnit.SECONDS.toNanos(10)};
    final long[] wallMs = {1_700_000_000_000L};
    final long[] backupState = {5};
    final long[] lastWritten = {-1};

    ReplicaReadiness readiness(final long shadowAgeMs, final long timeLagMs, final Set<ReplicaNegativeControl> off) {
        return new ReplicaReadiness(5_000, 30_000, MAX_LAG, shadowAgeMs, timeLagMs, () -> nano[0], () -> wallMs[0],
                () -> backupState[0], () -> lastWritten[0], off);
    }

    ReplicaReadiness readiness() {
        return readiness(SHADOW_AGE_MS, TIME_LAG_MS, EnumSet.noneOf(ReplicaNegativeControl.class));
    }

    /** A healthy, caught-up replica that just wrote a shadow. */
    void healthy(final ReplicaReadiness r) {
        r.restored(ReplicaReadiness.RestorePath.GENESIS);
        r.verified(true, null);
        r.followerTick();
        r.housekeepingTick();
        r.live(true, null);
        r.positions(1 << 20, 1 << 20);
        r.appliedClusterTime(wallMs[0] - 100);
        lastWritten[0] = nano[0];
    }

    static String reason(final NodeReadiness.Probe p) {
        final String d = p.detail();
        final int at = d.indexOf("recovery=") + "recovery=".length();
        final int end = d.indexOf(" restore=");
        return d.substring(at, end);
    }

    void advanceMs(final long ms) {
        nano[0] += TimeUnit.MILLISECONDS.toNanos(ms);
        wallMs[0] += ms;
    }

    @Test
    public void aHealthyReplicaIsReadyAndLive() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        final NodeReadiness.Probe p = r.probe();
        assertTrue(p.detail(), p.ready());
        assertTrue(p.detail(), p.live());
        assertEquals("ready", reason(p));
    }

    /**
     * A continuation that is not a full replay from genesis reads 503 {@code unverified
     * continuation} until the first ALIGNED compare with a member snapshot passed, then 200 at once.
     */
    @Test
    public void anUnverifiedContinuationIsNotReadyUntilTheFirstAlignedCompare() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.restored(ReplicaReadiness.RestorePath.LOCAL_SHADOW);
        r.verified(false, "unverified continuation: restored at 4096, waiting for the first ALIGNED compare");
        final NodeReadiness.Probe p = r.probe();
        if (p.ready() || !"unverified continuation".equals(reason(p))) {
            throw new AssertionError("expected 503 'unverified continuation' before the first ALIGNED compare,"
                    + " got ready=" + p.ready() + " " + p.detail());
        }
        assertTrue(p.detail(), p.detail().contains("waiting for the first ALIGNED compare"));
        r.verified(true, null);
        final NodeReadiness.Probe after = r.probe();
        assertTrue(after.detail(), after.ready());
        assertEquals("ready", reason(after));
        // fail closed: a fresh readiness is not verified until the follower says so
        final ReplicaReadiness fresh = readiness();
        fresh.restored(ReplicaReadiness.RestorePath.LOCAL_SHADOW);
        fresh.followerTick();
        fresh.housekeepingTick();
        fresh.live(true, null);
        fresh.positions(1 << 20, 1 << 20);
        fresh.appliedClusterTime(wallMs[0] - 100);
        assertEquals("unverified continuation", reason(fresh.probe()));
    }

    @Test
    public void bootstrapNotDoneIsNotReady() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        final ReplicaReadiness fresh = readiness();
        fresh.followerTick();
        fresh.live(true, null);
        fresh.bootstrapping("bootstrap: waiting for log recording");
        final NodeReadiness.Probe p = fresh.probe();
        assertFalse(p.detail(), p.ready());
        assertEquals("bootstrap: waiting for log recording", reason(p));
    }

    @Test
    public void aFencedReplicaReads503FencedUntilItExits() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.fenced("FATAL replica: apply fenced at 4096: boom");
        for (int i = 0; i < 5; i++) {
            healthy(r);
            final NodeReadiness.Probe p = r.probe();
            assertFalse(p.detail(), p.ready());
            assertTrue(p.detail(), reason(p).startsWith("fenced:FATAL replica: apply fenced at 4096"));
            advanceMs(1_000);
        }
    }

    @Test
    public void aRefusedBootstrapNeverReads200() {
        final ReplicaReadiness r = readiness();
        r.followerTick();
        r.housekeepingTick();
        r.live(true, null);
        r.positions(0, 0);
        r.refused("FATAL replica: no local shadow and the cluster log starts at 4096 > 0.");
        lastWritten[0] = nano[0];
        final NodeReadiness.Probe p = r.probe();
        assertFalse(p.detail(), p.ready());
        assertTrue(p.detail(), reason(p).startsWith("refused:FATAL replica: no local shadow"));
        assertEquals(ReplicaReadiness.RestorePath.REFUSED, r.restorePath());
        try {
            r.restored(ReplicaReadiness.RestorePath.REFUSED);
            fail("restored(REFUSED) accepted");
        } catch (final IllegalArgumentException expected) {
            // a refusal is never "done"
        }
    }

    @Test
    public void aStaleFollowerIsNotReadyThenNotLive() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        advanceMs(5_000);
        lastWritten[0] = nano[0];
        r.housekeepingTick();
        NodeReadiness.Probe p = r.probe();
        assertEquals("stale-follower", reason(p));
        assertTrue(p.live());
        advanceMs(30_000);
        r.housekeepingTick();
        p = r.probe();
        assertFalse("follower stale past the liveness bound reads live", p.live());
    }

    @Test
    public void livenessNeedsTheHousekeepingTickToo() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        advanceMs(31_000);
        r.followerTick();
        assertFalse("housekeeping stale but live", r.probe().live());
    }

    @Test
    public void noLiveRecordingIsNotReadyWithTheDetail() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.live(false, "awaiting live recording");
        assertEquals("awaiting live recording", reason(r.probe()));
        r.authRejected(true);
        assertEquals("auth rejected", reason(r.probe()));
        r.authRejected(false);
        r.live(true, null);
        assertEquals("ready", reason(r.probe()));
    }

    @Test
    public void applyLagBeyondTheBoundIsNotReady() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.positions(0, MAX_LAG);
        assertEquals("ready", reason(r.probe()));
        r.positions(0, MAX_LAG + 1);
        assertEquals("apply-lag", reason(r.probe()));
    }

    @Test
    public void shadowAgeCountsFromBootUntilTheFirstShadow() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        lastWritten[0] = -1;
        assertEquals("ready inside the first window", "ready", reason(r.probe()));
        advanceMs(SHADOW_AGE_MS + 1);
        r.followerTick();
        r.housekeepingTick();
        r.appliedClusterTime(wallMs[0]);
        assertEquals("shadow-age", reason(r.probe()));
        lastWritten[0] = nano[0];
        assertEquals("ready", reason(r.probe()));
        final ReplicaReadiness disabled = readiness(0, TIME_LAG_MS, EnumSet.noneOf(ReplicaNegativeControl.class));
        healthy(disabled);
        lastWritten[0] = -1;
        advanceMs(10 * SHADOW_AGE_MS);
        disabled.followerTick();
        disabled.housekeepingTick();
        disabled.appliedClusterTime(wallMs[0]);
        assertEquals("REPLICA_READY_SHADOW_AGE_MS=0 must disable the input", "ready", reason(disabled.probe()));
    }

    /**
     * The idle branch is gone. A caught-up replica whose last applied frame is older than the
     * setting is NOT ready: with the leader's drain belt the log ticks every ~10 ms, so a still log is a frozen or
     * partitioned supplier (a quiet cluster and a frozen one look the same from here). 0 disables the input.
     */
    @Test
    public void timeLagFlipsEvenWhenCaughtUp() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.appliedClusterTime(wallMs[0] - TIME_LAG_MS - 1);
        r.positions(4096, 8192);
        NodeReadiness.Probe p = r.probe();
        assertEquals("behind in time and in bytes", "time-lag", reason(p));
        assertFalse(p.ready());
        r.positions(8192, 8192);
        assertCaughtUpTimeLagNotReady(r.probe());
        r.appliedClusterTime(wallMs[0] - TIME_LAG_MS);
        assertEquals("at the bound is ready", "ready", reason(r.probe()));
        final ReplicaReadiness disabled = readiness(SHADOW_AGE_MS, 0, EnumSet.noneOf(ReplicaNegativeControl.class));
        healthy(disabled);
        disabled.appliedClusterTime(wallMs[0] - 10 * TIME_LAG_MS);
        disabled.positions(4096, 8192);
        assertEquals("REPLICA_READY_TIME_LAG_MS=0 must disable the input", "ready", reason(disabled.probe()));
    }

    static void assertCaughtUpTimeLagNotReady(final NodeReadiness.Probe p) {
        if (p.ready() || !"time-lag".equals(reason(p))) {
            throw new AssertionError("a caught-up replica past the time lag stayed ready (" + reason(p) + "): "
                    + p.detail());
        }
    }

    /** Negative control: the idle branch back keeps a frozen, caught-up replica ready. */
    @Test
    public void negativeControlTimeLagIdleAllowed() {
        final ReplicaReadiness r = readiness(SHADOW_AGE_MS, TIME_LAG_MS,
                EnumSet.of(ReplicaNegativeControl.TIME_LAG_IDLE_ALLOWED));
        healthy(r);
        r.appliedClusterTime(wallMs[0] - TIME_LAG_MS - 1);
        r.positions(8192, 8192);
        try {
            assertCaughtUpTimeLagNotReady(r.probe());
            fail("NC TIME_LAG_IDLE_ALLOWED: the idle branch did not keep the frozen replica ready");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith(
                    "a caught-up replica past the time lag stayed ready (ready time-lag-idle)"));
            System.out.println("NC TIME_LAG_IDLE_ALLOWED -> " + expected.getMessage());
        }
    }

    /** The default is 3 s of cluster time (was 30 s), still a setting. */
    @Test
    public void theTimeLagDefaultIsThreeSeconds() {
        assertEquals("default REPLICA_READY_TIME_LAG_MS", 3_000, ReplicaConfig.DEFAULT_READY_TIME_LAG_MS);
        assertEquals(3_000, ReplicaContextsTest.base().build().readyTimeLagMs());
        assertEquals("still a setting", 7_000, ReplicaContextsTest.base().readyTimeLagMs(7_000).build().readyTimeLagMs());
    }

    /**
     * A dead counter with {@code stop > consumed} (the catch-up replay of a stopped L after a backup
     * reset) is not ready and says why; after the re-attach (counter alive) within the byte and time bounds it is.
     */
    @Test
    public void aCatchUpOnAStoppedLogIsNotReadyUntilReattached() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.live(false, ReplicaFollower.notAliveDetail(8192, 4096));
        r.positions(4096, 8192);
        NodeReadiness.Probe p = r.probe();
        assertFalse(p.detail(), p.ready());
        assertEquals("a dead counter with stop > consumed must read the catch-up detail",
                "catching up (backup reset)", reason(p));
        assertEquals("caught up to the stop: waiting for the live recording", "awaiting live recording",
                ReplicaFollower.notAliveDetail(8192, 8192));
        r.live(true, null);
        r.positions(8192, 8192 + 4096);
        r.appliedClusterTime(wallMs[0] - 100);
        p = r.probe();
        assertTrue("re-attached within the lag bounds: " + p.detail(), p.ready());
    }

    @Test
    public void diskFullIsNotReady() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.diskFull("disk full: ARCHIVE_REFUSED(STORAGE_SPACE) at 4096");
        assertTrue(reason(r.probe()).startsWith("disk full"));
        r.diskFull(null);
        assertEquals("ready", reason(r.probe()));
    }

    @Test
    public void stoppingIsNotReady() {
        final ReplicaReadiness r = readiness();
        healthy(r);
        r.stopping();
        assertEquals("stopping", reason(r.probe()));
    }

    // --------------------------------------------------------------------------------------------- flap

    /** One query cycle every 12 s for 60 s, sampled every 50 ms; the live recording stays alive throughout. */
    int sampleQueryCycles(final ReplicaReadiness r) {
        healthy(r);
        int notReady = 0;
        final long[] cycle = {0, 1, 2, 3, 4, 5}; // BACKUP_QUERY .. BACKING_UP: a query leaves BACKING_UP
        for (int t = 0; t < 60_000; t += 50) {
            final int inCycle = t % 12_000;
            backupState[0] = inCycle < 300 ? cycle[Math.min(5, inCycle / 50)] : 5;
            r.followerTick();
            r.housekeepingTick();
            r.appliedClusterTime(wallMs[0]);
            lastWritten[0] = nano[0];
            if (!r.probe().ready()) {
                notReady++;
            }
            advanceMs(50);
        }
        return notReady;
    }

    static void assertNoFlap(final int notReady) {
        if (notReady != 0) {
            throw new AssertionError("ready flapped " + notReady + " times");
        }
    }

    @Test
    public void readyDoesNotFlapOnQuery() {
        assertNoFlap(sampleQueryCycles(readiness()));
    }

    @Test
    public void negativeControlReadinessFromBackupState() {
        try {
            assertNoFlap(sampleQueryCycles(readiness(SHADOW_AGE_MS, TIME_LAG_MS,
                    EnumSet.of(ReplicaNegativeControl.READINESS_FROM_BACKUP_STATE))));
            fail("NC READINESS_FROM_BACKUP_STATE: no flap");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().startsWith("ready flapped "));
            System.out.println("NC READINESS_FROM_BACKUP_STATE -> " + expected.getMessage());
        }
    }
}
