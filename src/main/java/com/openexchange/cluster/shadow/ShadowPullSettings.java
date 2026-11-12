// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * The shadow puller settings, one parser for every host (member and replica). The kit never reads an environment
 * variable: the host passes its lookup ({@code System::getenv} in production) and prefixes the refusal text with its
 * own boot guard ({@code CORE BOOT GUARD: }, {@code CORE REPLICA BOOT GUARD: }).
 *
 * <pre>
 * SHADOW_PULL_SOURCES              empty (disabled)  comma list of host:port archive control endpoints, in order;
 *                                                    a member spelled as the members table spells it ({@link #checkSpelling})
 * SHADOW_PULL_PERIOD_MS            5000              500..3600000
 * SHADOW_PULL_STARTUP_WAIT_MS      60000             1000..86400000 (the one bound of a startup pull)
 * SHADOW_PULL_TIMEOUT_MS           120000            1000..86400000 (one steady-state replication)
 * SHADOW_PULL_RETRY_BACKOFF_MS     5000              100..3600000 (doubles to 8x, resets on success)
 * SHADOW_PULL_MAX_INFLIGHT         1                 1..3
 * SHADOW_PULL_KINDS                ALIGNED           a comma list of ALIGNED|STANDBY|CADENCE|LOCAL
 * SHADOW_PULL_REPLICATION_ENDPOINT empty (host:0)    host:port the source's replay is sent to
 * SHADOW_PULL_RESPONSE_ENDPOINT    empty (host:0)    host:port the source answers this node's requests on
 * </pre>
 *
 * <p>Widening the kinds beyond ALIGNED and an empty source list are allowed with a CRITICAL boot line
 * ({@link #criticalLines}).</p>
 */
public record ShadowPullSettings(List<Endpoint> sources, long periodMs, long startupWaitMs, long timeoutMs,
                                 long retryBackoffMs, int maxInflight, Set<ShadowKind> kinds,
                                 Endpoint replicationEndpoint, Endpoint responseEndpoint) {

    public static final String SOURCES = "SHADOW_PULL_SOURCES";
    public static final String PERIOD_MS = "SHADOW_PULL_PERIOD_MS";
    public static final String STARTUP_WAIT_MS = "SHADOW_PULL_STARTUP_WAIT_MS";
    public static final String TIMEOUT_MS = "SHADOW_PULL_TIMEOUT_MS";
    public static final String RETRY_BACKOFF_MS = "SHADOW_PULL_RETRY_BACKOFF_MS";
    public static final String MAX_INFLIGHT = "SHADOW_PULL_MAX_INFLIGHT";
    public static final String KINDS = "SHADOW_PULL_KINDS";
    public static final String REPLICATION_ENDPOINT = "SHADOW_PULL_REPLICATION_ENDPOINT";
    public static final String RESPONSE_ENDPOINT = "SHADOW_PULL_RESPONSE_ENDPOINT";

    public static final long DEFAULT_PERIOD_MS = 5_000;
    public static final long MIN_PERIOD_MS = 500;
    public static final long MAX_PERIOD_MS = 3_600_000;
    public static final long DEFAULT_STARTUP_WAIT_MS = 60_000;
    public static final long MIN_STARTUP_WAIT_MS = 1_000;
    public static final long MAX_STARTUP_WAIT_MS = 86_400_000;
    public static final long DEFAULT_TIMEOUT_MS = 120_000;
    public static final long MIN_TIMEOUT_MS = 1_000;
    public static final long MAX_TIMEOUT_MS = 86_400_000;
    public static final long DEFAULT_RETRY_BACKOFF_MS = 5_000;
    public static final long MIN_RETRY_BACKOFF_MS = 100;
    public static final long MAX_RETRY_BACKOFF_MS = 3_600_000;
    public static final int DEFAULT_MAX_INFLIGHT = 1;
    public static final int MIN_MAX_INFLIGHT = 1;
    public static final int MAX_MAX_INFLIGHT = 3;
    public static final Set<ShadowKind> DEFAULT_KINDS = Set.of(ShadowKind.ALIGNED);

    /** The CRITICAL boot line of an empty source list. */
    public static final String DISABLED_LINE = "shadow puller disabled: SHADOW_PULL_SOURCES is empty";

    /** One {@code host:port}. */
    public record Endpoint(String host, int port) {
        public Endpoint {
            Objects.requireNonNull(host, "host");
        }

        /** {@code host:port}. */
        public String text() {
            return host + ":" + port;
        }

        /** {@code aeron:udp?endpoint=host:port}. */
        public String udpChannel() {
            return "aeron:udp?endpoint=" + text();
        }

        @Override
        public String toString() {
            return text();
        }

        /** Null when {@code v} is not {@code host:port} with a port 1..65535. */
        static Endpoint tryParse(final String v) {
            if (v == null) {
                return null;
            }
            final String s = v.trim();
            final int colon = s.lastIndexOf(':');
            if (colon <= 0 || colon == s.length() - 1 || s.indexOf(':') != colon) {
                return null;
            }
            final String host = s.substring(0, colon);
            for (int i = 0; i < host.length(); i++) {
                final char c = host.charAt(i);
                if (!(Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_')) {
                    return null;
                }
            }
            final int port;
            try {
                port = Integer.parseInt(s.substring(colon + 1));
            } catch (final NumberFormatException e) {
                return null;
            }
            return port < 1 || port > 65_535 ? null : new Endpoint(host, port);
        }
    }

    public ShadowPullSettings {
        sources = List.copyOf(sources);
        kinds = kinds.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(kinds));
        String refusal = rangeRefusal(PERIOD_MS, periodMs, MIN_PERIOD_MS, MAX_PERIOD_MS);
        refusal = refusal != null ? refusal
                : rangeRefusal(STARTUP_WAIT_MS, startupWaitMs, MIN_STARTUP_WAIT_MS, MAX_STARTUP_WAIT_MS);
        refusal = refusal != null ? refusal : rangeRefusal(TIMEOUT_MS, timeoutMs, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS);
        refusal = refusal != null ? refusal
                : rangeRefusal(RETRY_BACKOFF_MS, retryBackoffMs, MIN_RETRY_BACKOFF_MS, MAX_RETRY_BACKOFF_MS);
        if (refusal == null && (maxInflight < MIN_MAX_INFLIGHT || maxInflight > MAX_MAX_INFLIGHT)) {
            refusal = MAX_INFLIGHT + " must be " + MIN_MAX_INFLIGHT + ".." + MAX_MAX_INFLIGHT + ": " + maxInflight;
        }
        if (refusal == null && kinds.isEmpty()) {
            refusal = KINDS + " '' (expected a comma list of ALIGNED|STANDBY|CADENCE|LOCAL)";
        }
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
    }

    /** Every default, no source (disabled). */
    public static ShadowPullSettings disabled() {
        return new ShadowPullSettings(List.of(), DEFAULT_PERIOD_MS, DEFAULT_STARTUP_WAIT_MS, DEFAULT_TIMEOUT_MS,
                DEFAULT_RETRY_BACKOFF_MS, DEFAULT_MAX_INFLIGHT, DEFAULT_KINDS, null, null);
    }

    public boolean enabled() {
        return !sources.isEmpty();
    }

    /** A copy with these sources (tests and hosts that build the list themselves). */
    public ShadowPullSettings withSources(final List<Endpoint> next) {
        return new ShadowPullSettings(next, periodMs, startupWaitMs, timeoutMs, retryBackoffMs, maxInflight, kinds,
                replicationEndpoint, responseEndpoint);
    }

    /** The CRITICAL lines a host prints at boot: the disabled puller, a kinds widening. */
    public List<String> criticalLines() {
        final List<String> out = new ArrayList<>();
        if (!enabled()) {
            out.add(DISABLED_LINE);
        }
        if (!kinds.equals(DEFAULT_KINDS)) {
            out.add("pulling single-producer kinds " + kindsText(kinds) + ": a held source's suspects can be pulled");
        }
        return out;
    }

    static String kindsText(final Set<ShadowKind> kinds) {
        final StringBuilder b = new StringBuilder();
        for (final ShadowKind k : ShadowKind.values()) {
            if (kinds.contains(k)) {
                b.append(b.length() == 0 ? "" : ",").append(k.name());
            }
        }
        return b.toString();
    }

    /**
     * Parses every setting from {@code env} (absent or blank = the default).
     *
     * @param ownEndpoint this node's own archive control endpoint ({@code host:port}); listing it is a refusal
     * @throws IllegalArgumentException with the refusal text (no host prefix)
     */
    public static ShadowPullSettings parse(final Function<String, String> env, final String ownEndpoint) {
        Objects.requireNonNull(env, "env");
        final List<Endpoint> sources = new ArrayList<>();
        final String raw = blankToNull(env.apply(SOURCES));
        final Endpoint own = Endpoint.tryParse(ownEndpoint);
        if (raw != null) {
            for (final String part : raw.split(",")) {
                final Endpoint e = Endpoint.tryParse(part);
                if (e == null) {
                    throw new IllegalArgumentException(SOURCES + " entry '" + part.trim() + "' is not host:port");
                }
                if (own != null && own.equals(e)) {
                    throw new IllegalArgumentException(SOURCES + " lists this node's own archive endpoint "
                            + e.text());
                }
                if (sources.contains(e)) {
                    throw new IllegalArgumentException(SOURCES + " lists " + e.text() + " twice");
                }
                sources.add(e);
            }
        }
        final long period = longSetting(env, PERIOD_MS, DEFAULT_PERIOD_MS);
        final long startupWait = longSetting(env, STARTUP_WAIT_MS, DEFAULT_STARTUP_WAIT_MS);
        final long timeout = longSetting(env, TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        final long backoff = longSetting(env, RETRY_BACKOFF_MS, DEFAULT_RETRY_BACKOFF_MS);
        final long inflight = longSetting(env, MAX_INFLIGHT, DEFAULT_MAX_INFLIGHT);
        if (inflight < MIN_MAX_INFLIGHT || inflight > MAX_MAX_INFLIGHT) {
            throw new IllegalArgumentException(MAX_INFLIGHT + " must be " + MIN_MAX_INFLIGHT + ".." + MAX_MAX_INFLIGHT
                    + ": " + inflight);
        }
        return new ShadowPullSettings(sources, period, startupWait, timeout, backoff, (int)inflight,
                parseKinds(env.apply(KINDS)), endpointSetting(env, REPLICATION_ENDPOINT),
                endpointSetting(env, RESPONSE_ENDPOINT));
    }

    /** Host name resolution for the boot spelling check (the JDK's in production, a table in tests). */
    @FunctionalInterface
    public interface Resolver {
        Set<java.net.InetAddress> resolve(String host) throws java.net.UnknownHostException;

        /** {@code InetAddress.getAllByName}. */
        Resolver SYSTEM = host -> Set.of(java.net.InetAddress.getAllByName(host));
    }

    /** What the boot spelling check found: the boot line and the CRITICAL lines (an unverifiable entry). */
    public record SpellingCheck(String bootLine, List<String> criticalLines) {
        public SpellingCheck {
            criticalLines = List.copyOf(criticalLines);
        }
    }

    /**
     * The boot check that keeps the leader-source skip honest. The puller compares a source with the leader's archive
     * endpoint by its {@code host:port} text, and the members table spells each member's archive endpoint the way the
     * cluster configuration does; a source that names a member under another spelling ({@code core1:9302} against
     * {@code 10.0.0.5:9302}) would never match and the skip would silently never fire. So every source and every
     * member archive endpoint is resolved once here: a source whose resolved address and port equal a member
     * endpoint's but whose text differs is a refusal; a source or member host that does not resolve is a CRITICAL
     * line and the entry is kept (the puller's unreachable handling owns it from there). The runtime comparison stays
     * a plain text compare (no name resolution on the puller thread). The same rule covers this node's own endpoint
     * under another spelling, since the own endpoint is a member endpoint.
     *
     * @param memberArchiveEndpoints member id to its archive control endpoint as the members table spells it
     * @throws IllegalArgumentException with the refusal text (no host prefix)
     */
    public SpellingCheck checkSpelling(final java.util.Map<Integer, String> memberArchiveEndpoints) {
        return checkSpelling(memberArchiveEndpoints, Resolver.SYSTEM, Set.of());
    }

    SpellingCheck checkSpelling(final java.util.Map<Integer, String> memberArchiveEndpoints, final Resolver resolver,
                                final Set<NegativeControl> off) {
        Objects.requireNonNull(memberArchiveEndpoints, "memberArchiveEndpoints");
        Objects.requireNonNull(resolver, "resolver");
        final List<String> critical = new ArrayList<>();
        final java.util.Map<Integer, Endpoint> members = new java.util.TreeMap<>();
        for (final java.util.Map.Entry<Integer, String> m : memberArchiveEndpoints.entrySet()) {
            final Endpoint e = Endpoint.tryParse(m.getValue());
            if (e == null) {
                critical.add("cannot verify " + SOURCES + " against member " + m.getKey() + "'s archive endpoint '"
                        + m.getValue() + "': not host:port");
            } else {
                members.put(m.getKey(), e);
            }
        }
        final java.util.Map<String, Set<java.net.InetAddress>> resolved = new java.util.HashMap<>();
        final java.util.function.Function<String, Set<java.net.InetAddress>> lookup = host ->
                resolved.computeIfAbsent(host, h -> {
                    try {
                        return resolver.resolve(h);
                    } catch (final java.net.UnknownHostException | RuntimeException e) {
                        return Set.of();
                    }
                });
        final StringBuilder boot = new StringBuilder(SOURCES + ":");
        for (final Endpoint src : sources) {
            Integer textual = null;
            for (final java.util.Map.Entry<Integer, Endpoint> m : members.entrySet()) {
                if (m.getValue().equals(src)) {
                    textual = m.getKey();
                }
            }
            boot.append(boot.charAt(boot.length() - 1) == ':' ? " " : ", ").append(src.text()).append(' ');
            if (textual != null) {
                boot.append("member ").append(textual);
                continue;
            }
            final Set<java.net.InetAddress> srcAddresses = lookup.apply(src.host());
            if (srcAddresses.isEmpty()) {
                critical.add("cannot verify " + src.text() + " against the members table: " + src.host()
                        + " does not resolve");
                boot.append("unverified");
                continue;
            }
            for (final java.util.Map.Entry<Integer, Endpoint> m : members.entrySet()) {
                final Endpoint me = m.getValue();
                if (me.port() != src.port()) {
                    continue;
                }
                final Set<java.net.InetAddress> memberAddresses = lookup.apply(me.host());
                if (memberAddresses.isEmpty()) {
                    critical.add("cannot verify " + src.text() + " against member " + m.getKey()
                            + "'s archive endpoint " + me.text() + ": " + me.host() + " does not resolve");
                    continue;
                }
                if (!java.util.Collections.disjoint(srcAddresses, memberAddresses)
                        && !off.contains(NegativeControl.SOURCE_SPELLING_CHECK_OFF)) {
                    throw new IllegalArgumentException(SOURCES + " entry " + src.text() + " names member "
                            + m.getKey() + "'s archive endpoint " + me.text() + " with a different spelling: spell it"
                            + " as the cluster members table does so the leader skip can recognise it");
                }
            }
            boot.append("non-member");
        }
        if (sources.isEmpty()) {
            boot.append(" none");
        }
        return new SpellingCheck(boot.toString(), critical);
    }

    /** {@code SHADOW_PULL_KINDS}; blank = ALIGNED. */
    public static Set<ShadowKind> parseKinds(final String value) {
        final String v = blankToNull(value);
        if (v == null) {
            return DEFAULT_KINDS;
        }
        final EnumSet<ShadowKind> out = EnumSet.noneOf(ShadowKind.class);
        for (final String part : v.split(",")) {
            ShadowKind k = null;
            for (final ShadowKind c : ShadowKind.values()) {
                if (c.name().equals(part.trim())) {
                    k = c;
                }
            }
            if (k == null) {
                throw new IllegalArgumentException(KINDS + " '" + v + "' (expected a comma list of"
                        + " ALIGNED|STANDBY|CADENCE|LOCAL)");
            }
            out.add(k);
        }
        return Set.copyOf(out);
    }

    private static Endpoint endpointSetting(final Function<String, String> env, final String name) {
        final String v = blankToNull(env.apply(name));
        if (v == null) {
            return null;
        }
        final Endpoint e = Endpoint.tryParse(v);
        if (e == null) {
            throw new IllegalArgumentException(name + " '" + v + "' is not host:port");
        }
        return e;
    }

    private static long longSetting(final Function<String, String> env, final String name, final long dflt) {
        final String v = blankToNull(env.apply(name));
        if (v == null) {
            return dflt;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(name + " is not an integer: " + v);
        }
    }

    private static String rangeRefusal(final String name, final long v, final long min, final long max) {
        return v < min || v > max ? name + " must be " + min + ".." + max + " ms: " + v : null;
    }

    private static String blankToNull(final String v) {
        return v == null || v.isBlank() ? null : v;
    }
}
