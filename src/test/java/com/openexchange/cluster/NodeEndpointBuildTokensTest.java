// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.service.Cluster.Role;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * /health and /ready end with the build identity: {@code version=<v> build=<sha>}. Every existing token keeps its
 * place in front of them, and {@code commit=} stays the commit log position.
 */
public class NodeEndpointBuildTokensTest {

    private NodeEndpoint endpoint;

    @After
    public void stop() {
        if (endpoint != null) {
            endpoint.stop();
        }
    }

    private static final String DETAIL = "role=REPLICA recovery=ready source=-1";

    private NodeEndpoint started(final NodeEndpoint e) throws IOException {
        endpoint = e;
        e.start(0);
        return e;
    }

    @Test
    public void healthAndReadyEndWithTheBuildIdentityAfterTheUnchangedDetail() throws IOException {
        final NodeEndpoint e = started(new NodeEndpoint(() -> new NodeReadiness.Probe(true, true, DETAIL), null)
                .withBuild(new BuildInfo("0.1.22-runtime.20261116.buildid", "d1d79a9")));
        for (final String path : new String[] {"/health", "/ready"}) {
            final NodeEndpointProbeTest.Response r = NodeEndpointProbeTest.get(e.port(), path);
            assertEquals(path, 200, r.status());
            assertEquals(path, DETAIL + " version=0.1.22-runtime.20261116.buildid build=d1d79a9\n", r.body());
        }
    }

    @Test
    public void aNodeWithoutABuildIdentitySaysUnknownNeverEmpty() throws IOException {
        final NodeEndpoint e = started(new NodeEndpoint(() -> new NodeReadiness.Probe(true, false, DETAIL), null));
        final NodeEndpointProbeTest.Response r = NodeEndpointProbeTest.get(e.port(), "/ready");
        assertEquals(503, r.status());
        assertEquals(DETAIL + " version=unknown build=unknown\n", r.body());
        assertEquals(DETAIL + " version=unknown build=unknown\n",
                NodeEndpointProbeTest.get(e.port(), "/health").body());
    }

    @Test
    public void tokenValuesAreSanitizedAndBlankBecomesUnknown() throws IOException {
        final NodeEndpoint e = started(new NodeEndpoint(() -> new NodeReadiness.Probe(true, true, DETAIL), null)
                .withBuild(new BuildInfo("1.0 beta\n=x", "  ")));
        assertEquals(DETAIL + " version=1.0betax build=unknown\n",
                NodeEndpointProbeTest.get(e.port(), "/health").body());
    }

    @Test
    public void theCommitTokenStillReadsTheCommitPositionWhenTheShaStartsWithDigits() throws IOException {
        final AtomicLong now = new AtomicLong(System.nanoTime());
        final NodeReadiness r = new NodeReadiness(30_000, 5_000, now::get);
        final NodeEndpoint e = started(new NodeEndpoint(r, null).withBuild(new BuildInfo("0.1.22", "123456abc")));
        r.started();
        r.roleChanged(Role.FOLLOWER);
        r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, true, true, 0);
        final String body = NodeEndpointProbeTest.get(e.port(), "/ready").body();
        assertTrue(body, body.startsWith(r.describe().substring(0, r.describe().indexOf(" observationAgeMs="))));
        final Matcher m = Pattern.compile("commit=(\\d+)").matcher(body);
        assertTrue(body, m.find());
        assertEquals("the first commit= is the commit log position", "100", m.group(1));
        assertTrue(body, body.endsWith(" maxCheckpointAgeMs=" + NodeReadiness.MAX_CHECKPOINT_AGE_MS
                + " version=0.1.22 build=123456abc\n"));
        assertTrue("no second commit= token: " + body, !m.find());
    }
}
