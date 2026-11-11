// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.shadow.ShadowPullSettings;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The replica's pull and re-bootstrap settings, and the readiness of a pulled restore, of a re-bootstrap and of a
 * re-bootstrap not taken.
 */
public class ReplicaPullSettingsTest {

    @Test
    public void defaults() {
        final ReplicaConfig c = ReplicaContextsTest.base().build();
        assertFalse(c.pullSettings().enabled());
        assertEquals(3, c.rebootstrapReplayFailures());
        assertEquals(3, c.rebootstrapMaxConsecutive());
        assertEquals(3_600_000, c.rebootstrapWindowMs());
        assertEquals(2, c.rebootstrapKeep());
        assertEquals(60_000, c.rebootstrapNotTakenLogMs());
        assertEquals("10.0.0.9:9601", c.archiveControlEndpoint());
    }

    @Test
    public void rebootstrapRanges() {
        ReplicaContextsTest.base().rebootstrapReplayFailures(0).build();
        ReplicaContextsTest.base().rebootstrapReplayFailures(1_000).build();
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapReplayFailures(-1),
                "SHADOW_REBOOTSTRAP_REPLAY_FAILURES must be 0..1000: -1");
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapReplayFailures(1_001),
                "SHADOW_REBOOTSTRAP_REPLAY_FAILURES must be 0..1000: 1001");
        ReplicaContextsTest.base().rebootstrapMaxConsecutive(1).build();
        ReplicaContextsTest.base().rebootstrapMaxConsecutive(100).build();
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapMaxConsecutive(0),
                "SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE must be 1..100: 0");
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapMaxConsecutive(101),
                "SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE must be 1..100: 101");
        ReplicaContextsTest.base().rebootstrapWindowMs(60_000).build();
        ReplicaContextsTest.base().rebootstrapWindowMs(604_800_000).build();
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapWindowMs(59_999),
                "SHADOW_REBOOTSTRAP_WINDOW_MS must be 60000..604800000 ms: 59999");
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapWindowMs(604_800_001),
                "SHADOW_REBOOTSTRAP_WINDOW_MS must be 60000..604800000 ms: 604800001");
        ReplicaContextsTest.base().rebootstrapKeep(1).build();
        ReplicaContextsTest.base().rebootstrapKeep(100).build();
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapKeep(0),
                "SHADOW_REBOOTSTRAP_KEEP must be 1..100: 0");
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapKeep(101),
                "SHADOW_REBOOTSTRAP_KEEP must be 1..100: 101");
        ReplicaContextsTest.base().rebootstrapNotTakenLogMs(1_000).build();
        ReplicaContextsTest.base().rebootstrapNotTakenLogMs(86_400_000).build();
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapNotTakenLogMs(999),
                "SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS must be 1000..86400000 ms: 999");
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().rebootstrapNotTakenLogMs(86_400_001),
                "SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS must be 1000..86400000 ms: 86400001");
    }

    @Test
    public void aSourceListNamingThisReplicaIsRefused() {
        final ShadowPullSettings own = ShadowPullSettings.disabled().withSources(List.of(
                new ShadowPullSettings.Endpoint("10.0.0.1", 9101), new ShadowPullSettings.Endpoint("10.0.0.9", 9601)));
        ReplicaOracleSettingsTest.refused(ReplicaContextsTest.base().pullSettings(own),
                "SHADOW_PULL_SOURCES lists this node's own archive endpoint 10.0.0.9:9601");
        final ReplicaConfig ok = ReplicaContextsTest.base().pullSettings(ShadowPullSettings.disabled().withSources(
                List.of(new ShadowPullSettings.Endpoint("10.0.0.1", 9101)))).build();
        assertTrue(ok.pullSettings().enabled());
    }

    static ReplicaReadiness readiness() {
        return new ReplicaReadiness(5_000, 30_000, 1024, 0, 0, SystemNanoClock.INSTANCE, SystemEpochClock.INSTANCE,
                () -> 5, () -> -1, Set.of());
    }

    @Test
    public void aPulledRestoreIsCodeThreeAndARebootstrapReadsRestarting() {
        assertEquals(3, ReplicaReadiness.RestorePath.PULLED_SHADOW.code());
        final ReplicaReadiness r = readiness();
        r.bootstrapping("bootstrap: pulling shadow from [10.0.0.1:9101]");
        assertTrue(r.probe().detail(), r.probe().detail().contains("recovery=bootstrap: pulling shadow from"));
        r.restored(ReplicaReadiness.RestorePath.PULLED_SHADOW);
        assertEquals(ReplicaReadiness.RestorePath.PULLED_SHADOW, r.restorePath());
        assertFalse(r.probe().detail().contains("bootstrap: pulling"));
        r.rebootstrapping();
        r.stopping();
        assertFalse(r.probe().ready());
        assertTrue(r.probe().detail(), r.probe().detail().contains("recovery=rebootstrap: restarting"));
    }

    @Test
    public void aRebootstrapNotTakenIsThePlainReadyReasonUntilCleared() {
        final ReplicaReadiness r = readiness();
        r.restored(ReplicaReadiness.RestorePath.LOCAL_SHADOW);
        assertTrue(r.probe().detail(), r.probe().detail().contains("recovery=unverified continuation"));
        final String text = "re-bootstrap not taken: no source lists a shadow of kind ALIGNED above L.stop=65536";
        r.rebootstrapNotTaken(text);
        assertFalse(r.probe().ready());
        assertTrue(r.probe().detail(), r.probe().detail().contains("recovery=" + text + " restore=LOCAL_SHADOW"));
        r.rebootstrapNotTaken(null);
        assertTrue(r.probe().detail(), r.probe().detail().contains("recovery=unverified continuation"));
        r.rebootstrapNotTaken(text);
        r.rebootstrapping();
        assertTrue("the gate reads first", r.probe().detail().contains("recovery=rebootstrap: restarting"));
    }
}
