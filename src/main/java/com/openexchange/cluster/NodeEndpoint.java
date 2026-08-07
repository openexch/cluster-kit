// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * The three things a cluster node has to answer for itself: {@code /health},
 * {@code /ready} and {@code /metrics}.
 *
 * <p>They live in the node process because the node owns the facts. Deriving
 * health from outside meant reading another process's Aeron counters and
 * guessing, and guessing is how a dead node kept being reported as running.</p>
 *
 * <p>The JDK's own HTTP server on a daemon thread: no dependency, and nothing
 * shared with the cluster agent thread except the reads the suppliers do. The
 * probes must never take a lock the duty cycle can hold, or a busy node would
 * fail its own liveness check and be killed for being busy.</p>
 */
public final class NodeEndpoint {

    private final NodeReadiness readiness;
    private final Supplier<String> metricsBody;
    private HttpServer server;

    /**
     * @param readiness   the node's own view of itself
     * @param metricsBody Prometheus exposition, or null to serve no metrics
     */
    public NodeEndpoint(final NodeReadiness readiness, final Supplier<String> metricsBody) {
        this.readiness = readiness;
        this.metricsBody = metricsBody;
    }

    public void start(final int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        // 200 while the duty cycle is advancing. A failure here is a restart
        // request: the process is up but no longer doing its job.
        server.createContext("/health", exchange ->
            respond(exchange, readiness.live() ? 200 : 503, readiness.describe()));

        // 200 only when this member can be counted on. A rolling restart waits
        // on this, so a false 200 costs quorum.
        server.createContext("/ready", exchange ->
            respond(exchange, readiness.ready() ? 200 : 503, readiness.describe()));

        if (metricsBody != null) {
            server.createContext("/metrics", exchange -> {
                final byte[] body = metricsBody.get().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set(
                    "Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            });
        }

        server.setExecutor(runnable -> {
            final Thread t = new Thread(runnable, "node-endpoint-http");
            t.setDaemon(true);
            t.start();
        });
        server.start();
        System.out.println("NODE: /health /ready" + (metricsBody != null ? " /metrics" : "")
            + " on port " + port);
    }

    public void stop() {
        readiness.stopping();
        if (server != null) {
            server.stop(0);
        }
    }

    private static void respond(final HttpExchange exchange, final int status, final String detail)
        throws IOException {
        final byte[] body = (detail + "\n").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
