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

    // ---------------------------------------------------------------------------------------------- spelling

    static final Map<Integer, String> MEMBERS = Map.of(0, "127.0.0.1:9302", 1, "10.0.0.5:9402", 2, "core2:9502");

    /** A table resolver: localhost and 127.0.0.1 are one host; core2 is 10.0.0.7; anything else does not resolve. */
    static Set<java.net.InetAddress> resolve(final String host) throws java.net.UnknownHostException {
        return switch (host) {
            case "localhost", "127.0.0.1" -> Set.of(java.net.InetAddress.getByAddress(host, new byte[] {127, 0, 0, 1}));
            case "10.0.0.5", "core1" -> Set.of(java.net.InetAddress.getByAddress(host, new byte[] {10, 0, 0, 5}));
            case "10.0.0.7", "core2" -> Set.of(java.net.InetAddress.getByAddress(host, new byte[] {10, 0, 0, 7}));
            case "replica" -> Set.of(java.net.InetAddress.getByAddress(host, new byte[] {10, 0, 0, 9}));
            default -> throw new java.net.UnknownHostException(host);
        };
    }

    static ShadowPullSettings.SpellingCheck check(final String sources, final Set<NegativeControl> off) {
        return parse("SHADOW_PULL_SOURCES", sources).checkSpelling(MEMBERS, ShadowPullSettingsTest::resolve, off);
    }

    static void misspelledMemberRefused(final Set<NegativeControl> off) {
        try {
            final ShadowPullSettings.SpellingCheck c = check("localhost:9302", off);
            throw new AssertionError("a differently spelled member endpoint passed the boot check: " + c.bootLine());
        } catch (final IllegalArgumentException refused) {
            assertEquals("SHADOW_PULL_SOURCES entry localhost:9302 names member 0's archive endpoint 127.0.0.1:9302"
                    + " with a different spelling: spell it as the cluster members table does so the leader skip can"
                    + " recognise it", refused.getMessage());
        }
    }

    @Test
    public void aMemberEndpointUnderAnotherSpellingIsRefusedAtBoot() {
        misspelledMemberRefused(Set.of());
        try {
            check("core1:9402", Set.of());
            fail("core1 resolves to member 1's 10.0.0.5");
        } catch (final IllegalArgumentException refused) {
            assertTrue(refused.getMessage(), refused.getMessage().startsWith("SHADOW_PULL_SOURCES entry core1:9402"
                    + " names member 1's archive endpoint 10.0.0.5:9402"));
        }
        ShadowRig.nc("SOURCE_SPELLING_CHECK_OFF", "a differently spelled member endpoint passed the boot check",
                () -> misspelledMemberRefused(Set.of(NegativeControl.SOURCE_SPELLING_CHECK_OFF)));
    }

    @Test
    public void theBootLineClassifiesAndAnUnresolvableHostIsKeptWithACriticalLine() {
        final ShadowPullSettings.SpellingCheck c = check("127.0.0.1:9302,core2:9502,replica:9602,ghost:9702,"
                + "localhost:9999", Set.of());
        assertEquals("SHADOW_PULL_SOURCES: 127.0.0.1:9302 member 0, core2:9502 member 2, replica:9602 non-member,"
                + " ghost:9702 unverified, localhost:9999 non-member", c.bootLine());
        assertEquals(List.of("cannot verify ghost:9702 against the members table: ghost does not resolve"),
                c.criticalLines());
        assertEquals(5, parse("SHADOW_PULL_SOURCES", "127.0.0.1:9302,core2:9502,replica:9602,ghost:9702,"
                + "localhost:9999").sources().size());
        assertEquals("SHADOW_PULL_SOURCES: none", parse().checkSpelling(MEMBERS, ShadowPullSettingsTest::resolve,
                Set.of()).bootLine());
    }

    @Test
    public void anUnresolvableMemberHostOnTheSamePortIsACriticalLine() {
        final Map<Integer, String> members = Map.of(0, "127.0.0.1:9302", 3, "ghost:9602");
        final ShadowPullSettings.SpellingCheck c = parse("SHADOW_PULL_SOURCES", "replica:9602")
                .checkSpelling(members, ShadowPullSettingsTest::resolve, Set.of());
        assertEquals("SHADOW_PULL_SOURCES: replica:9602 non-member", c.bootLine());
        assertEquals(List.of("cannot verify replica:9602 against member 3's archive endpoint ghost:9602: ghost does"
                + " not resolve"), c.criticalLines());
    }

    @Test
    public void theRealResolverSeesLocalhostAsTheLoopbackMember() {
        try {
            parse("SHADOW_PULL_SOURCES", "localhost:9302").checkSpelling(Map.of(0, "127.0.0.1:9302"));
            fail("localhost resolves to 127.0.0.1 on every host this runs on");
        } catch (final IllegalArgumentException refused) {
            assertTrue(refused.getMessage(), refused.getMessage().contains("names member 0's archive endpoint"
                    + " 127.0.0.1:9302 with a different spelling"));
        }
    }
}
