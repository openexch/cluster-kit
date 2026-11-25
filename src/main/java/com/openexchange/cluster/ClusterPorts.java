// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

/**
 * Where a cluster member listens, in one place, because four components have to
 * agree on it independently.
 *
 * <p>The engines build their member endpoint strings from this, and every client
 * that connects to them derives the same numbers on its own: the matching
 * engine, the core engine, the settlement bridge, OMS, the load generators and
 * the backup agent. Each used to carry its own copy of the arithmetic. Copies of
 * a wire contract do not drift loudly; they drift into a process dialling a port
 * nobody is listening on, and the first evidence is a timeout in production.</p>
 *
 * <h2>The stride, and why it defaults to zero</h2>
 *
 * <p>The old formula was {@code portBase + nodeId * 100 + offset}: every member's
 * ports were a function of its member id. That has exactly one meaning, which is
 * that all the members share a host. Under an orchestrator each member has its
 * own address, so all of them should listen on the SAME ports: one set of
 * container ports, one service definition, one network policy, and no per-ordinal
 * port map to keep in step.</p>
 *
 * <p>So the stride is zero by default and the distributed layout is what you get
 * for free. Putting several members on one host is the special case now, and it
 * says so out loud: set {@code CLUSTER_PORT_STRIDE=100} and the old layout comes
 * back exactly.</p>
 *
 * <p>Forgetting it on a shared host is not a silent misconfiguration. The second
 * member tries to bind a port the first one already holds and dies immediately,
 * which is the failure you want: loud, at startup, before anything is trading.</p>
 *
 * <h2>It is a deployment-wide setting</h2>
 *
 * <p>Like the port base and the member host list, every process in the
 * deployment has to be told the same stride. Two members with different values
 * compute different membership strings and never form a cluster; a client with
 * the wrong value dials the wrong port. This is not replicated state and cannot
 * be: it is what a process needs in order to find the cluster in the first place.</p>
 */
public final class ClusterPorts {

    /** Archive control channel. */
    public static final int ARCHIVE_CONTROL_OFFSET = 1;
    /** Ingress, where cluster clients connect. */
    public static final int CLIENT_FACING_OFFSET = 2;
    /** Consensus traffic between members. */
    public static final int MEMBER_FACING_OFFSET = 3;
    /** Cluster log fan-out. */
    public static final int LOG_OFFSET = 4;
    /** Catch-up replay / transfer. */
    public static final int TRANSFER_OFFSET = 5;
    /**
     * The member's HTTP endpoint ({@code /health}, {@code /ready}, {@code /metrics}), TCP. It follows the same rule as
     * the UDP ports, so at the default stride every member serves it on the same port and a co-located set (stride
     * 100) gets distinct ones. It used to be a per-engine base plus the member id, which is a shared-host layout
     * baked into a default: with one member per address it gave each member a different port for no reason.
     */
    public static final int NODE_ENDPOINT_OFFSET = 6;

    /** Members share a host and need distinct ports: the historical layout. */
    public static final int SHARED_HOST_STRIDE = 100;

    /** One member per address, which is what an orchestrator gives you. */
    public static final int DEFAULT_STRIDE = 0;

    private static volatile int resolvedStride = -1;

    private ClusterPorts() {
    }

    /**
     * The configured stride: {@code CLUSTER_PORT_STRIDE}, or
     * {@code -Dcluster.port.stride}, default 0.
     *
     * <p>Resolved once. An unreadable value throws rather than falling back,
     * because a stride that silently reverts to 0 on one host is a member that
     * cannot bind and a client dialling into nothing.</p>
     */
    public static int stride() {
        int stride = resolvedStride;
        if (stride < 0) {
            stride = readStride();
            resolvedStride = stride;
        }
        return stride;
    }

    private static int readStride() {
        String raw = System.getenv("CLUSTER_PORT_STRIDE");
        String source = "CLUSTER_PORT_STRIDE";
        if (raw == null || raw.isBlank()) {
            raw = System.getProperty("cluster.port.stride");
            source = "cluster.port.stride";
        }
        if (raw == null || raw.isBlank()) {
            return DEFAULT_STRIDE;
        }
        final int parsed;
        try {
            parsed = Integer.parseInt(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(source + "=" + raw + " is not a number", e);
        }
        if (parsed < 0) {
            throw new IllegalArgumentException(source + "=" + raw + " must not be negative");
        }
        return parsed;
    }

    /**
     * The port a member listens on, using the configured stride.
     *
     * @param nodeId   the member
     * @param portBase the deployment's base port
     * @param offset   one of the OFFSET constants above
     */
    public static int port(final int nodeId, final int portBase, final int offset) {
        return port(nodeId, portBase, offset, stride());
    }

    /** The same arithmetic with the stride supplied, for tests and for callers that carry their own. */
    public static int port(final int nodeId, final int portBase, final int offset, final int stride) {
        return portBase + (nodeId * stride) + offset;
    }

    /** {@code host:port}, the form Aeron endpoints take. */
    public static String endpoint(final int nodeId, final String host, final int portBase,
                                  final int offset) {
        return host + ":" + port(nodeId, portBase, offset);
    }

    /**
     * The member's HTTP endpoint port ({@link #NODE_ENDPOINT_OFFSET}) under the configured stride, cross-checked
     * against an optional override.
     *
     * @see #nodeEndpointPort(int, int, int, String, String)
     */
    public static int nodeEndpointPort(final int nodeId, final int portBase, final String overrideName,
                                       final String overrideText) {
        return nodeEndpointPort(nodeId, portBase, stride(), overrideName, overrideText);
    }

    /**
     * The member's HTTP endpoint port: {@code portBase + nodeId * stride + NODE_ENDPOINT_OFFSET}.
     *
     * <p>An override (the engine's port env, null or blank when unset) is accepted only when it names the same
     * port. It is kept so a launcher that writes the port out explicitly is checked against the rule rather than
     * trusted: a launcher and an engine that disagree about where the probe listens produce a member that is up but
     * reads as dead, so the boot is refused instead, with both numbers in the text.</p>
     *
     * @param overrideName the setting the override came from, for the refusal text
     * @throws IllegalArgumentException when the override is not a number or disagrees with the computed port, or
     *                                  the computed port is outside 1..65535
     */
    public static int nodeEndpointPort(final int nodeId, final int portBase, final int stride,
                                       final String overrideName, final String overrideText) {
        final long computed = (long) portBase + (long) nodeId * stride + NODE_ENDPOINT_OFFSET;
        final String rule = "port base " + portBase + " + node " + nodeId + " x stride " + stride + " + "
                + NODE_ENDPOINT_OFFSET;
        if (computed < 1 || computed > 65_535) {
            throw new IllegalArgumentException("node endpoint port " + computed + " (" + rule
                    + ") is outside 1..65535");
        }
        if (overrideText == null || overrideText.isBlank()) {
            return (int) computed;
        }
        final int override;
        try {
            override = Integer.parseInt(overrideText.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(overrideName + "=" + overrideText + " is not a number", e);
        }
        if (override != computed) {
            throw new IllegalArgumentException(overrideName + "=" + override
                    + " disagrees with the node endpoint port " + computed + " (" + rule
                    + "); unset it, or fix the port base or CLUSTER_PORT_STRIDE");
        }
        return override;
    }

    /**
     * Test seam: forget the resolved stride so a changed property is read again.
     *
     * <p>Public because the consumers that have to be pinned against this contract live in
     * other repositories: both engines, the settlement bridge and OMS all derive these
     * numbers, and each of their test suites needs to exercise both layouts.</p>
     */
    public static void resetStrideForTest() {
        resolvedStride = -1;
    }
}
