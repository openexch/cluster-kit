// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The scenario text conventions both engines' DSLs share: comments, blank lines, {@code key=value} args. */
public final class ScenarioText {

    private ScenarioText() {
    }

    /** Everything after the first {@code #} is a comment. */
    public static String stripComment(final String line) {
        final int hash = line.indexOf('#');
        return hash >= 0 ? line.substring(0, hash) : line;
    }

    /** The executable lines of a scenario: comments stripped, trimmed, blanks dropped; order kept. */
    public static List<String> executableLines(final List<String> raw) {
        final List<String> out = new ArrayList<>(raw.size());
        for (final String r : raw) {
            final String line = stripComment(r).trim();
            if (!line.isEmpty()) {
                out.add(line);
            }
        }
        return out;
    }

    /** Whitespace-split tokens; token 0 is the verb. */
    public static String[] tokens(final String line) {
        return line.trim().split("\\s+");
    }

    /** {@code key=value} tokens after the verb, in order; tokens without '=' are ignored. */
    public static Map<String, String> parseKv(final String[] tokens) {
        final Map<String, String> kv = new LinkedHashMap<>();
        for (int i = 1; i < tokens.length; i++) {
            final int eq = tokens[i].indexOf('=');
            if (eq > 0) {
                kv.put(tokens[i].substring(0, eq), tokens[i].substring(eq + 1));
            }
        }
        return kv;
    }

    public static String req(final Map<String, String> kv, final String key) {
        final String v = kv.get(key);
        if (v == null) {
            throw new IllegalArgumentException("missing required field '" + key + "'");
        }
        return v;
    }

    public static long reqLong(final Map<String, String> kv, final String key) {
        return Long.parseLong(req(kv, key));
    }

    public static int reqInt(final Map<String, String> kv, final String key) {
        return Integer.parseInt(req(kv, key));
    }
}
