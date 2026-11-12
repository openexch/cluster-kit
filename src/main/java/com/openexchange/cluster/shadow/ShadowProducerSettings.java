// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The follower producer settings of a cluster member, one parser. The kit never reads an environment variable: the
 * host passes its lookup ({@code System::getenv} in production) and prefixes the refusal text with its own boot guard
 * ({@code CORE BOOT GUARD: }). Absent or blank is the default; a non-numeric value is {@code <NAME> is not an
 * integer: <v>}.
 *
 * <pre>
 * SHADOW_PRODUCER                         designated-follower  designated-follower | none
 * SHADOW_TERM_LENGTH                      4m                   64k..16m, a power of two (the writer's own check)
 * SHADOW_PRODUCER_BUFFER_MB               512                  1..2047 (one int-sized off-heap image buffer)
 * SHADOW_WRITE_CONNECT_TIMEOUT_MS         10000                1..86400000
 * SHADOW_WRITE_OFFER_TIMEOUT_MS           30000                1..86400000
 * SHADOW_WRITE_RECORD_TIMEOUT_MS          60000                1..86400000
 * SHADOW_PRODUCER_WINDOW_BUDGET_PCT       50                   1..100
 * SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS   1000                 1..600000 (T_last before the first serialize)
 * SHADOW_PRODUCER_RATE_WINDOW_MS          10000                1000..3600000 (the R_recent horizon)
 * </pre>
 *
 * <p>{@code SHADOW_PRODUCER=none} is allowed with a CRITICAL boot line ({@link #criticalLines}): that member never
 * writes an ALIGNED shadow and the two-producer anchor depends on the other producers.</p>
 */
public record ShadowProducerSettings(Mode mode, String termLength, int bufferMb, long connectTimeoutMs,
                                     long offerTimeoutMs, long recordTimeoutMs, int windowBudgetPct,
                                     long serializeEstimateMs, long rateWindowMs) {

    /** {@code SHADOW_PRODUCER}. */
    public enum Mode {
        DESIGNATED_FOLLOWER("designated-follower"), NONE("none");

        private final String text;

        Mode(final String text) {
            this.text = text;
        }

        public String text() {
            return text;
        }

        /** The mode for a setting value; refusal text without a host prefix. */
        public static Mode parse(final String value) {
            for (final Mode m : values()) {
                if (m.text.equals(value.trim())) {
                    return m;
                }
            }
            throw new IllegalArgumentException(PRODUCER + " '" + value.trim() + "' (expected designated-follower|none)");
        }
    }

    public static final String PRODUCER = "SHADOW_PRODUCER";
    public static final String TERM_LENGTH = "SHADOW_TERM_LENGTH";
    public static final String BUFFER_MB = "SHADOW_PRODUCER_BUFFER_MB";
    public static final String CONNECT_TIMEOUT_MS = "SHADOW_WRITE_CONNECT_TIMEOUT_MS";
    public static final String OFFER_TIMEOUT_MS = "SHADOW_WRITE_OFFER_TIMEOUT_MS";
    public static final String RECORD_TIMEOUT_MS = "SHADOW_WRITE_RECORD_TIMEOUT_MS";
    public static final String WINDOW_BUDGET_PCT = "SHADOW_PRODUCER_WINDOW_BUDGET_PCT";
    public static final String SERIALIZE_ESTIMATE_MS = "SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS";
    public static final String RATE_WINDOW_MS = "SHADOW_PRODUCER_RATE_WINDOW_MS";

    public static final String DEFAULT_TERM_LENGTH = "4m";
    public static final int DEFAULT_BUFFER_MB = 512;
    public static final int MIN_BUFFER_MB = 1;
    public static final int MAX_BUFFER_MB = 2047;
    public static final long DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    public static final long DEFAULT_OFFER_TIMEOUT_MS = 30_000;
    public static final long DEFAULT_RECORD_TIMEOUT_MS = 60_000;
    public static final long MIN_TIMEOUT_MS = 1;
    public static final long MAX_TIMEOUT_MS = 86_400_000;
    public static final int DEFAULT_WINDOW_BUDGET_PCT = 50;
    public static final int MIN_WINDOW_BUDGET_PCT = 1;
    public static final int MAX_WINDOW_BUDGET_PCT = 100;
    public static final long DEFAULT_SERIALIZE_ESTIMATE_MS = 1_000;
    public static final long MIN_SERIALIZE_ESTIMATE_MS = 1;
    public static final long MAX_SERIALIZE_ESTIMATE_MS = 600_000;
    public static final long DEFAULT_RATE_WINDOW_MS = 10_000;
    public static final long MIN_RATE_WINDOW_MS = 1_000;
    public static final long MAX_RATE_WINDOW_MS = 3_600_000;

    /** The CRITICAL boot line of {@code SHADOW_PRODUCER=none}. */
    public static final String NONE_LINE = "SHADOW_PRODUCER=none: this member never writes an ALIGNED: the two-producer"
            + " anchor depends on other producers";

    public ShadowProducerSettings {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(termLength, "termLength");
        String refusal = termLengthRefusal(termLength);
        refusal = refusal != null ? refusal : range(BUFFER_MB, bufferMb, MIN_BUFFER_MB, MAX_BUFFER_MB, "");
        refusal = refusal != null ? refusal
                : range(CONNECT_TIMEOUT_MS, connectTimeoutMs, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, " ms");
        refusal = refusal != null ? refusal
                : range(OFFER_TIMEOUT_MS, offerTimeoutMs, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, " ms");
        refusal = refusal != null ? refusal
                : range(RECORD_TIMEOUT_MS, recordTimeoutMs, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS, " ms");
        refusal = refusal != null ? refusal : range(WINDOW_BUDGET_PCT, windowBudgetPct, MIN_WINDOW_BUDGET_PCT,
                MAX_WINDOW_BUDGET_PCT, "");
        refusal = refusal != null ? refusal : range(SERIALIZE_ESTIMATE_MS, serializeEstimateMs,
                MIN_SERIALIZE_ESTIMATE_MS, MAX_SERIALIZE_ESTIMATE_MS, " ms");
        refusal = refusal != null ? refusal
                : range(RATE_WINDOW_MS, rateWindowMs, MIN_RATE_WINDOW_MS, MAX_RATE_WINDOW_MS, " ms");
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
    }

    /** Every default. */
    public static ShadowProducerSettings defaults() {
        return new ShadowProducerSettings(Mode.DESIGNATED_FOLLOWER, DEFAULT_TERM_LENGTH, DEFAULT_BUFFER_MB,
                DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_OFFER_TIMEOUT_MS, DEFAULT_RECORD_TIMEOUT_MS,
                DEFAULT_WINDOW_BUDGET_PCT, DEFAULT_SERIALIZE_ESTIMATE_MS, DEFAULT_RATE_WINDOW_MS);
    }

    /** True unless {@code SHADOW_PRODUCER=none}. */
    public boolean enabled() {
        return mode == Mode.DESIGNATED_FOLLOWER;
    }

    /** The image buffer in bytes. */
    public int bufferBytes() {
        return (int)Math.min(Integer.MAX_VALUE, (long)bufferMb * 1024 * 1024);
    }

    /** The CRITICAL lines a host prints at boot in shadow mode. */
    public List<String> criticalLines() {
        final List<String> out = new ArrayList<>();
        if (mode == Mode.NONE) {
            out.add(NONE_LINE);
        }
        return out;
    }

    /**
     * Parses every setting from {@code env} (absent or blank = the default).
     *
     * @throws IllegalArgumentException with the refusal text (no host prefix)
     */
    public static ShadowProducerSettings parse(final Function<String, String> env) {
        Objects.requireNonNull(env, "env");
        final String mode = blankToNull(env.apply(PRODUCER));
        final String term = blankToNull(env.apply(TERM_LENGTH));
        final long buffer = longSetting(env, BUFFER_MB, DEFAULT_BUFFER_MB);
        final String bufferRefusal = range(BUFFER_MB, buffer, MIN_BUFFER_MB, MAX_BUFFER_MB, "");
        if (bufferRefusal != null) {
            throw new IllegalArgumentException(bufferRefusal);
        }
        final long budget = longSetting(env, WINDOW_BUDGET_PCT, DEFAULT_WINDOW_BUDGET_PCT);
        final String budgetRefusal = range(WINDOW_BUDGET_PCT, budget, MIN_WINDOW_BUDGET_PCT, MAX_WINDOW_BUDGET_PCT, "");
        if (budgetRefusal != null) {
            throw new IllegalArgumentException(budgetRefusal);
        }
        return new ShadowProducerSettings(mode == null ? Mode.DESIGNATED_FOLLOWER : Mode.parse(mode),
                term == null ? DEFAULT_TERM_LENGTH : term.trim(), (int)buffer,
                longSetting(env, CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS),
                longSetting(env, OFFER_TIMEOUT_MS, DEFAULT_OFFER_TIMEOUT_MS),
                longSetting(env, RECORD_TIMEOUT_MS, DEFAULT_RECORD_TIMEOUT_MS), (int)budget,
                longSetting(env, SERIALIZE_ESTIMATE_MS, DEFAULT_SERIALIZE_ESTIMATE_MS),
                longSetting(env, RATE_WINDOW_MS, DEFAULT_RATE_WINDOW_MS));
    }

    /** The writer's own term-length rule, as a refusal text, or null. */
    static String termLengthRefusal(final String value) {
        final long t;
        try {
            t = ShadowWriter.parseSize(value);
        } catch (final RuntimeException e) {
            return TERM_LENGTH + " " + value + " is not a size (expected <n>, <n>k or <n>m)";
        }
        if (t < ShadowWriter.MIN_TERM_LENGTH || t > ShadowWriter.MAX_TERM_LENGTH || Long.bitCount(t) != 1) {
            return TERM_LENGTH + " " + value + " outside [64k, 16m] or not a power of two (every replay maps 3 x the"
                    + " recording's term length)";
        }
        return null;
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

    private static String range(final String name, final long v, final long min, final long max, final String unit) {
        return v < min || v > max ? name + " must be " + min + ".." + max + unit + ": " + v : null;
    }

    private static String blankToNull(final String v) {
        return v == null || v.isBlank() ? null : v;
    }
}
