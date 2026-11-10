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

    private final Supplier<NodeReadiness.Probe> probe;
    private final Runnable onStop;
    private final Supplier<String> metricsBody;
    private HttpServer server;

    /**
     * @param readiness   the node's own view of itself
     * @param metricsBody Prometheus exposition, or null to serve no metrics
     */
    public NodeEndpoint(final NodeReadiness readiness, final Supplier<String> metricsBody) {
        this.probe = readiness::probe;
        this.onStop = readiness::stopping;
        this.metricsBody = metricsBody;
    }

    /**
     * A node whose evidence is not member-shaped (the shadow replica) supplies its own probe. One call per
     * request yields both the status and the body, from the same evidence. {@link #stop} only stops the server; the
     * supplier's owner marks itself stopping.
     *
     * @param probe       the node's own view of itself, read once per request
     * @param metricsBody Prometheus exposition, or null to serve no metrics
     */
    public NodeEndpoint(final Supplier<NodeReadiness.Probe> probe, final Supplier<String> metricsBody) {
        this.probe = java.util.Objects.requireNonNull(probe, "probe");
        this.onStop = () -> { };
        this.metricsBody = metricsBody;
    }

    public void start(final int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        // 200 while the duty cycle is advancing. A failure here is a restart
        // request: the process is up but no longer doing its job.
        server.createContext("/health", exchange -> {
            final NodeReadiness.Probe p = probe.get();
            respond(exchange, p.live() ? 200 : 503, p.detail());
        });

        // 200 only when this member can be counted on. A rolling restart waits
        // on this, so a false 200 costs quorum.
        server.createContext("/ready", exchange -> {
            final NodeReadiness.Probe p = probe.get();
            respond(exchange, p.ready() ? 200 : 503, p.detail());
        });

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

    /** The bound port (the one {@link #start} was given, or the ephemeral one for 0); -1 before start. */
    public int port() {
        return server == null ? -1 : server.getAddress().getPort();
    }

    public void stop() {
        onStop.run();
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
