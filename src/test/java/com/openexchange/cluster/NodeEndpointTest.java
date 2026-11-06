// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import com.sun.net.httpserver.HttpServer;
import io.aeron.cluster.service.Cluster.Role;
import org.junit.Test;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

public class NodeEndpointTest {
    @Test public void httpSeparatesLiveIdleFromUnreadyRecovery() throws Exception {
        AtomicLong now = new AtomicLong(System.nanoTime());
        NodeReadiness r = new NodeReadiness(30_000, 5_000, now::get);
        NodeEndpoint endpoint = new NodeEndpoint(r, null);
        endpoint.start(0);
        var field = NodeEndpoint.class.getDeclaredField("server"); field.setAccessible(true);
        int port = ((HttpServer) field.get(endpoint)).getAddress().getPort();
        try (HttpClient client = HttpClient.newHttpClient()) {
            r.started(); r.roleChanged(Role.FOLLOWER);
            check(client, port, "/health", 200, "evidence-missing");
            check(client, port, "/ready", 503, "evidence-missing");
            r.observe(42, 7, 2, Role.FOLLOWER, 50, 100, true, true, 0);
            check(client, port, "/ready", 503, "catching-up");
            r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, true, true, 0);
            check(client, port, "/ready", 200, "applied=100 commit=100");
            r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, false, true, 0);
            check(client, port, "/ready", 503, "election");
            check(client, port, "/health", 200, "election");
            r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, true, true, 0);
            now.addAndGet(6_000_000_000L); r.tick();
            check(client, port, "/ready", 503, "stale-observation");
            check(client, port, "/health", 200, "stale-observation");
            r.needsReseed("missing-log"); r.observe(42, 7, 2, Role.FOLLOWER, 100, 100, true, true, 0);
            check(client, port, "/ready", 503, "needs-reseed:missing-log");
        } finally { endpoint.stop(); }
    }

    private static void check(HttpClient client, int port, String path, int status, String body) throws Exception {
        var response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(java.time.Duration.ofSeconds(3)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(response.body(), status, response.statusCode());
        assertTrue(response.body(), response.body().contains(body));
    }
}
