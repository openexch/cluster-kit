// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The probe-supplier constructor serves /health, /ready, /metrics from one probe per request. */
public class NodeEndpointProbeTest {

    private NodeEndpoint endpoint;

    @After
    public void stop() {
        if (endpoint != null) {
            endpoint.stop();
        }
    }

    record Response(int status, String body) {
    }

    static Response get(final int port, final String path) throws IOException {
        final HttpURLConnection c = (HttpURLConnection)URI.create("http://127.0.0.1:" + port + path).toURL()
                .openConnection();
        final int status = c.getResponseCode();
        try (InputStream in = status >= 400 ? c.getErrorStream() : c.getInputStream()) {
            return new Response(status, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void theProbeSupplierDecidesStatusAndBody() throws IOException {
        final AtomicReference<NodeReadiness.Probe> probe = new AtomicReference<>(
                new NodeReadiness.Probe(true, false, "role=REPLICA recovery=bootstrap: waiting for log recording"));
        final AtomicInteger calls = new AtomicInteger();
        endpoint = new NodeEndpoint(() -> {
            calls.incrementAndGet();
            return probe.get();
        }, () -> "shadow_replica_fenced 0\n");
        endpoint.start(0);
        final int port = endpoint.port();
        assertTrue("bound port " + port, port > 0);

        Response r = get(port, "/ready");
        assertEquals("not ready must be 503", 503, r.status());
        assertTrue(r.body(), r.body().contains("bootstrap: waiting for log recording"));
        assertEquals(200, get(port, "/health").status());

        probe.set(new NodeReadiness.Probe(true, true, "role=REPLICA recovery=ready"));
        r = get(port, "/ready");
        assertEquals(200, r.status());
        assertTrue(r.body(), r.body().contains("recovery=ready"));

        probe.set(new NodeReadiness.Probe(false, false, "role=REPLICA recovery=fenced:boom"));
        assertEquals("not live must be 503", 503, get(port, "/health").status());
        r = get(port, "/ready");
        assertEquals(503, r.status());
        assertTrue(r.body(), r.body().contains("fenced:boom"));

        r = get(port, "/metrics");
        assertEquals(200, r.status());
        assertEquals("shadow_replica_fenced 0\n", r.body());
        assertEquals("one probe call per /health or /ready request", 5, calls.get());
    }

    @Test
    public void theMemberConstructorStillServesNodeReadiness() throws IOException {
        final NodeReadiness readiness = new NodeReadiness();
        endpoint = new NodeEndpoint(readiness, null);
        endpoint.start(0);
        assertEquals("a member that never started is not ready", 503, get(endpoint.port(), "/ready").status());
        assertEquals("no metrics supplier, no /metrics", 404, get(endpoint.port(), "/metrics").status());
        endpoint.stop();
        assertTrue(readiness.describe(), readiness.describe().contains("recovery=stopping"));
        endpoint = null;
    }
}
