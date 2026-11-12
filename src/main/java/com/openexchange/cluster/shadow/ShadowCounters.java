// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The shadow counters, plain longs the host exports (no Aeron counter). Written by the writer
 * thread and the catalog's owner thread, read by the host's exporter: every slot is atomic. Gauges ({@code *_last},
 * {@code catalog_*}, positions) are set, totals incremented.
 */
public final class ShadowCounters {

    public enum Counter {
        WRITE_BYTES_LAST, WRITE_OFFER_NS_LAST, WRITE_RECORD_NS_LAST, WRITE_ABORTED_TOTAL, WRITE_TIMEOUTS_TOTAL,
        WRITE_OFFER_TIMEOUTS_TOTAL, WRITE_SKIPPED_BUSY_TOTAL, WRITE_SKIPPED_UNIDENTIFIED_TOTAL,
        WRITE_SKIPPED_NOT_MONOTONE_TOTAL, SELFCHECK_FAILURES_TOTAL, SELFCHECK_MUTATIONS_TOTAL,
        LAST_WRITE_COMPLETED_NS, CATALOG_SCANS_TOTAL, CATALOG_SCAN_ERRORS_TOTAL, CATALOG_USABLE, CATALOG_VERIFIED,
        CATALOG_DISTINCT, CATALOG_INCOMPLETE, CATALOG_CONFLICT_TOTAL, CATALOG_CORRUPT, CATALOG_CORRUPT_TOTAL,
        CATALOG_FOREIGN, CATALOG_SEQ_REUSE_TOTAL, CATALOG_STARTUP_PASS_DONE, NEWEST_VERIFIED_POSITION,
        NEWEST_USABLE_POSITION, REPLAY_TIMEOUTS_TOTAL, IDENTITY_CROSSCHECK_FAILURES_TOTAL,
        /** A Throwable from a write or the listener on the writer thread. */
        WRITER_THREAD_ERRORS_TOTAL,
        /** A crashed write of this host's producer from before this process, purged by {@link CrashedWriteSweep}. */
        CRASHED_WRITES_PURGED_TOTAL;

        /** The exported name, {@code shadow_<lower>}. */
        public String metricName() {
            return "shadow_" + name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    private final AtomicLongArray values = new AtomicLongArray(Counter.values().length);
    private final AtomicLongArray writeTotalByKind = new AtomicLongArray(ShadowKind.values().length);
    private final AtomicLongArray writeFailedByReason = new AtomicLongArray(ShadowWriter.Reason.values().length);
    private final Map<Integer, Long> purgeRefusedByCode = new ConcurrentHashMap<>();

    public ShadowCounters() {
        values.set(Counter.NEWEST_VERIFIED_POSITION.ordinal(), -1L);
        values.set(Counter.NEWEST_USABLE_POSITION.ordinal(), -1L);
    }

    public long get(final Counter c) {
        return values.get(c.ordinal());
    }

    public void increment(final Counter c) {
        values.incrementAndGet(c.ordinal());
    }

    public void add(final Counter c, final long delta) {
        values.addAndGet(c.ordinal(), delta);
    }

    public void set(final Counter c, final long value) {
        values.set(c.ordinal(), value);
    }

    /** {@code shadow_write_total{kind}}. */
    public long writeTotal(final ShadowKind kind) {
        return writeTotalByKind.get(kind.ordinal());
    }

    void incrementWriteTotal(final ShadowKind kind) {
        writeTotalByKind.incrementAndGet(kind.ordinal());
    }

    /** {@code shadow_write_failed_total{reason}}. */
    public long writeFailed(final ShadowWriter.Reason reason) {
        return writeFailedByReason.get(reason.ordinal());
    }

    void incrementWriteFailed(final ShadowWriter.Reason reason) {
        writeFailedByReason.incrementAndGet(reason.ordinal());
    }

    /** {@code shadow_purge_refused_total{code}}: archive codes, or {@link ShadowCatalog#PURGE_CODE_ANCHOR} etc. */
    public long purgeRefused(final int code) {
        return purgeRefusedByCode.getOrDefault(code, 0L);
    }

    void incrementPurgeRefused(final int code) {
        purgeRefusedByCode.merge(code, 1L, Long::sum);
    }

    /** Every counter as {@code name -> value} (labelled ones as {@code name{label}}), sorted by name. */
    public Map<String, Long> snapshot() {
        final Map<String, Long> out = new TreeMap<>();
        for (final Counter c : Counter.values()) {
            out.put(c.metricName(), get(c));
        }
        for (final ShadowKind k : ShadowKind.values()) {
            out.put("shadow_write_total{kind=" + k + "}", writeTotal(k));
        }
        for (final ShadowWriter.Reason r : ShadowWriter.Reason.values()) {
            out.put("shadow_write_failed_total{reason=" + r + "}", writeFailed(r));
        }
        purgeRefusedByCode.forEach((code, n) -> out.put("shadow_purge_refused_total{code=" + code + "}", n));
        return out;
    }
}
