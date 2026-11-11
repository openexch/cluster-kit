// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The puller settings: defaults, both ends of each range, the refusal text one step outside, the CRITICAL lines. */
public class ShadowPullSettingsTest {

    static final String OWN = "10.0.0.1:9101";

    static ShadowPullSettings parse(final String... kv) {
        final Map<String, String> env = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            env.put(kv[i], kv[i + 1]);
        }
        return ShadowPullSettings.parse(env::get, OWN);
    }

    static void refused(final String text, final String... kv) {
        try {
            parse(kv);
            fail("accepted: expected '" + text + "'");
        } catch (final IllegalArgumentException expected) {
            assertEquals(text, expected.getMessage());
        }
    }

    @Test
    public void defaults() {
        final ShadowPullSettings s = parse();
        assertFalse(s.enabled());
        assertEquals(List.of(), s.sources());
        assertEquals(5_000, s.periodMs());
        assertEquals(60_000, s.startupWaitMs());
        assertEquals(120_000, s.timeoutMs());
        assertEquals(5_000, s.retryBackoffMs());
        assertEquals(1, s.maxInflight());
        assertEquals(Set.of(ShadowKind.ALIGNED), s.kinds());
        assertNull(s.replicationEndpoint());
        assertNull(s.responseEndpoint());
        assertEquals(List.of("shadow puller disabled: SHADOW_PULL_SOURCES is empty"), s.criticalLines());
    }

    @Test
    public void sources() {
        final ShadowPullSettings s = parse("SHADOW_PULL_SOURCES", "10.0.0.2:9201, 10.0.0.9:9601");
        assertTrue(s.enabled());
        assertEquals(List.of(new ShadowPullSettings.Endpoint("10.0.0.2", 9201),
                new ShadowPullSettings.Endpoint("10.0.0.9", 9601)), s.sources());
        assertEquals(List.of(), s.criticalLines());
        refused("SHADOW_PULL_SOURCES entry '10.0.0.2' is not host:port", "SHADOW_PULL_SOURCES", "10.0.0.2");
        refused("SHADOW_PULL_SOURCES entry '10.0.0.2:0' is not host:port", "SHADOW_PULL_SOURCES", "10.0.0.2:0");
        refused("SHADOW_PULL_SOURCES entry 'a:b:1' is not host:port", "SHADOW_PULL_SOURCES", "a:b:1");
        refused("SHADOW_PULL_SOURCES lists this node's own archive endpoint 10.0.0.1:9101", "SHADOW_PULL_SOURCES",
                "10.0.0.2:9201," + OWN);
        refused("SHADOW_PULL_SOURCES lists 10.0.0.2:9201 twice", "SHADOW_PULL_SOURCES", "10.0.0.2:9201,10.0.0.2:9201");
    }

    static void range(final String name, final long min, final long max) {
        assertTrue(parse(name, String.valueOf(min)) != null);
        assertTrue(parse(name, String.valueOf(max)) != null);
        refused(name + " must be " + min + ".." + max + " ms: " + (min - 1), name, String.valueOf(min - 1));
        refused(name + " must be " + min + ".." + max + " ms: " + (max + 1), name, String.valueOf(max + 1));
        refused(name + " is not an integer: 5s", name, "5s");
    }

    @Test
    public void ranges() {
        range("SHADOW_PULL_PERIOD_MS", 500, 3_600_000);
        range("SHADOW_PULL_STARTUP_WAIT_MS", 1_000, 86_400_000);
        range("SHADOW_PULL_TIMEOUT_MS", 1_000, 86_400_000);
        range("SHADOW_PULL_RETRY_BACKOFF_MS", 100, 3_600_000);
        assertEquals(1, parse("SHADOW_PULL_MAX_INFLIGHT", "1").maxInflight());
        assertEquals(3, parse("SHADOW_PULL_MAX_INFLIGHT", "3").maxInflight());
        refused("SHADOW_PULL_MAX_INFLIGHT must be 1..3: 0", "SHADOW_PULL_MAX_INFLIGHT", "0");
        refused("SHADOW_PULL_MAX_INFLIGHT must be 1..3: 4", "SHADOW_PULL_MAX_INFLIGHT", "4");
        refused("SHADOW_PULL_MAX_INFLIGHT is not an integer: one", "SHADOW_PULL_MAX_INFLIGHT", "one");
    }

    @Test
    public void kinds() {
        final ShadowPullSettings wide = parse("SHADOW_PULL_KINDS", "ALIGNED,STANDBY");
        assertEquals(Set.of(ShadowKind.ALIGNED, ShadowKind.STANDBY), wide.kinds());
        assertTrue(wide.criticalLines().toString(), wide.criticalLines().contains(
                "pulling single-producer kinds ALIGNED,STANDBY: a held source's suspects can be pulled"));
        refused("SHADOW_PULL_KINDS 'ALIGNED,STALE' (expected a comma list of ALIGNED|STANDBY|CADENCE|LOCAL)",
                "SHADOW_PULL_KINDS", "ALIGNED,STALE");
    }

    @Test
    public void bothEndpoints() {
        final ShadowPullSettings s = parse("SHADOW_PULL_REPLICATION_ENDPOINT", "10.0.0.1:9105",
                "SHADOW_PULL_RESPONSE_ENDPOINT", "10.0.0.1:9106");
        assertEquals("aeron:udp?endpoint=10.0.0.1:9105", s.replicationEndpoint().udpChannel());
        assertEquals("aeron:udp?endpoint=10.0.0.1:9106", s.responseEndpoint().udpChannel());
        refused("SHADOW_PULL_REPLICATION_ENDPOINT '9105' is not host:port", "SHADOW_PULL_REPLICATION_ENDPOINT", "9105");
        refused("SHADOW_PULL_RESPONSE_ENDPOINT 'host:' is not host:port", "SHADOW_PULL_RESPONSE_ENDPOINT", "host:");
    }
}
