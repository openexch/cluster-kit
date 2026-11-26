// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * S4 T3 (kit part): the capability tokens follow {@code build=}, the old body is a prefix of the new one, the recorded
 * values change after a publish and read 0 before the first one.
 */
public class NodeEndpointCapabilitiesTest {

    private static final String DETAIL = "role=FOLLOWER recovery=ready";
    private NodeEndpoint endpoint;

    @After
    public void stop() {
        if (endpoint != null) {
            endpoint.stop();
        }
    }

    private static MemberCompat.Capabilities capabilities() {
        return new MemberCompat.Capabilities(MemberCompat.of(1, 3), 10, 10, new int[] {4, 2, 1, 3}, new int[] {7},
                "external");
    }

    private NodeEndpoint started(final MemberCompat.Capabilities caps) throws IOException {
        endpoint = new NodeEndpoint(() -> new NodeReadiness.Probe(true, true, DETAIL), null)
                .withBuild(new BuildInfo("0.1.34", "abc1234"));
        if (caps != null) {
            endpoint.capabilities(caps);
        }
        endpoint.start(0);
        return endpoint;
    }

    private static String compatToken(final String body) {
        final int at = body.indexOf(" compat=");
        assertTrue("no compat= token: " + body, at >= 0);
        return body.substring(at + 1, body.indexOf(' ', at + 1));
    }

    @Test
    public void theTokensFollowBuildAndTheOldBodyIsAPrefixOfTheNewOne() throws IOException {
        final String old = DETAIL + " version=0.1.34 build=abc1234";
        final NodeEndpoint e = started(capabilities());
        for (final String path : new String[] {"/health", "/ready"}) {
            final String body = NodeEndpointProbeTest.get(e.port(), path).body();
            assertTrue(body, body.startsWith(old + " "));
            assertEquals(path, old + " compat=g1.3 wire=10-10 behaviors=1,2,3,4 layouts=7 recorded_behavior=0"
                    + " recorded_wire_max=0 recorded_genesis=0 driver=external\n", body);
            assertEquals("compat=g1.3", compatToken(body));
        }
    }

    @Test
    public void theRecordedValuesChangeAfterAPublish() throws IOException {
        final MemberCompat.Capabilities caps = capabilities();
        final NodeEndpoint e = started(caps);
        caps.publish(4, 10, true);
        final String body = NodeEndpointProbeTest.get(e.port(), "/ready").body();
        assertTrue(body, body.endsWith(" recorded_behavior=4 recorded_wire_max=10 recorded_genesis=1"
                + " driver=external\n"));
        final MemberCompat.Capabilities.Recorded before = caps.recorded();
        caps.publish(4, 10, true);
        assertTrue("an unchanged publish keeps the instance", before == caps.recorded());
    }

    /** Negative control: an endpoint without the capabilities supplier has no compat= token. */
    @Test
    public void negativeControlWithoutTheSupplierTheCompatAssertionFails() throws IOException {
        final NodeEndpoint e = started(null);
        final String body = NodeEndpointProbeTest.get(e.port(), "/ready").body();
        final AssertionError failure = assertThrows(AssertionError.class, () -> compatToken(body));
        assertTrue(failure.getMessage(), failure.getMessage().startsWith("no compat= token"));
    }

    @Test
    public void theDriverTokenIsEmbeddedOrExternal() {
        assertThrows(IllegalArgumentException.class, () -> new MemberCompat.Capabilities(MemberCompat.of(1, 1), 10, 10,
                new int[] {1}, new int[] {7}, "EXTERNAL"));
        assertEquals(" compat=g1.1 wire=9-10 behaviors=1 layouts=6,7 recorded_behavior=0 recorded_wire_max=0"
                + " recorded_genesis=0 driver=embedded", new MemberCompat.Capabilities(MemberCompat.of(1, 1), 9, 10,
                new int[] {1}, new int[] {7, 6}, "embedded").get());
    }
}
