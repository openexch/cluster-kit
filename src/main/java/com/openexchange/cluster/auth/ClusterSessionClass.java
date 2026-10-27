// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import java.util.Locale;

/**
 * The session classes of the K6 v1 authority, one registry for both clusters. The codes are the wire values
 * of the money-schema {@code SessionClass} enum (1 GATEWAY .. 7 REPLICA), so one credential format serves
 * Java, C++ and the fixture. The kit never imports an engine enum; an engine maps a code to its own type in
 * one helper guarded by a parity test. {@link #UNSET} (code 0) carries no authority; any code above 7 is
 * UNSET until a later kit release defines it.
 */
public enum ClusterSessionClass {
    UNSET(0),
    GATEWAY(1),
    FORWARDER(2),
    BRIDGE(3),
    ADMIN(4),
    TOOL(5),
    TREASURY(6),
    REPLICA(7);

    /** Highest code this kit defines. */
    public static final int MAX_CODE = 7;

    private static final ClusterSessionClass[] BY_CODE = {UNSET, GATEWAY, FORWARDER, BRIDGE, ADMIN, TOOL, TREASURY, REPLICA};

    private final int code;

    ClusterSessionClass(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Wire code to class; any code outside 1..7 is {@link #UNSET}. Allocation-free. */
    public static ClusterSessionClass of(final int code) {
        return code >= 1 && code <= MAX_CODE ? BY_CODE[code] : UNSET;
    }

    /**
     * Parse a class from its text form: a decimal code (first character a digit) or a name, uppercased with
     * {@link Locale#ROOT}. The RC1 S3 rules: an unknown code or name, and UNSET, are refused with an
     * {@link IllegalArgumentException} ({@link NumberFormatException} for a malformed number).
     */
    public static ClusterSessionClass parse(final String text) {
        final String t = text.trim();
        if (!t.isEmpty() && Character.isDigit(t.charAt(0))) {
            final ClusterSessionClass c = of(Integer.parseInt(t));
            if (c == UNSET) {
                throw new IllegalArgumentException("unknown session class code " + t);
            }
            return c;
        }
        final ClusterSessionClass c;
        try {
            c = valueOf(t.toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown session class " + text);
        }
        if (c == UNSET) {
            throw new IllegalArgumentException("unknown session class " + text);
        }
        return c;
    }
}
