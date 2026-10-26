// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import java.util.Objects;

/** A minimal line-oriented diff with no I/O, so pure runners can use it without reaching {@link GoldenAssert}. */
public final class TextDiff {

    private TextDiff() {
    }

    /** '-' expected, '+' actual, for the lines that differ. */
    public static String diff(final String expected, final String actual) {
        final String[] e = expected.split("\n", -1);
        final String[] a = actual.split("\n", -1);
        final StringBuilder sb = new StringBuilder();
        final int max = Math.max(e.length, a.length);
        for (int i = 0; i < max; i++) {
            final String el = i < e.length ? e[i] : null;
            final String al = i < a.length ? a[i] : null;
            if (!Objects.equals(el, al)) {
                sb.append("  @ line ").append(i + 1).append('\n');
                if (el != null) {
                    sb.append("  - ").append(el).append('\n');
                }
                if (al != null) {
                    sb.append("  + ").append(al).append('\n');
                }
            }
        }
        return sb.toString();
    }
}
