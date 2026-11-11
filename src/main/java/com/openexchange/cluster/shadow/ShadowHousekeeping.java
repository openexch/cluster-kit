// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.DeepResult;
import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import com.openexchange.cluster.shadow.ShadowEntry.State;
import org.agrona.concurrent.NanoClock;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The member's shadow housekeeping: the hook {@code SnapshotLogPruner.onWake} runs on the pruner thread
 * before every purge decision. One {@link #poll} does, in order:
 *
 * <ol>
 *   <li>a catalog scan every {@code SHADOW_SCAN_PERIOD_MS} (structural verdicts are cached: no replay for an unchanged
 *       descriptor);</li>
 *   <li>at most ONE deep read, and only when {@code SHADOW_DEEP_READ_PACE_MS} passed since the previous one (the
 *       measured 1.8 s tmpfs release): the startup pass (every COMPLETE copy without a verdict,
 *       newest first) is paced the same way, and only after it finished without a SCAN_ERROR does this archive's
 *       {@code startupPassDone} flip, so the floor stays UNKNOWN until then. Never more than one
 *       replay open at a time: every read is synchronous on this thread;</li>
 *   <li>the host's {@code refresh} (the journal tail behind {@code skipSafe});</li>
 *   <li>{@link ShadowRetention#apply} (the one purge owner), then a fresh scan for the floor;</li>
 *   <li>{@link ShadowFloor#requestPurge} when a VERIFIED position appeared that the last poll did not see, or the
 *       floor rose above what the last purge pass applied (C3: "requested" means a VERIFIED shadow appeared or the
 *       floor rose, not that a scan ran). A shadow VERIFIED elsewhere (a restore's own deep read in {@code onStart})
 *       is new to this hook on its first poll (C2).</li>
 * </ol>
 *
 * <p>The startup pass does not gate {@code /ready}: it gates purging only. Never throws an archive refusal: the
 * catalog answers SCAN_ERROR, which keeps the floor UNKNOWN.</p>
 */
public final class ShadowHousekeeping implements Runnable {

    /** The floor and retention shadow counters this package owns, exported by the host through {@code metricsBody()}. */
    public enum Counter {
        FLOOR_POSITION("shadow_floor_position"),
        FLOOR_UNKNOWN("shadow_floor_unknown"),
        FLOOR_LAG_BYTES("shadow_floor_lag_bytes"),
        FLOOR_REGRESSED_TOTAL("shadow_floor_regressed_total"),
        ANCHOR_POSITION("shadow_anchor_position"),
        MARGIN_POSITION("shadow_margin_position"),
        /** The most distinct producers at any covering position of the last evaluation. */
        ANCHOR_WITNESSES("shadow_anchor_witnesses"),
        /** Purge passes that found covering shadows but no two-producer witness (purged nothing). */
        FLOOR_NO_WITNESS_TOTAL("shadow_floor_no_witness_total"),
        RETENTION_PURGED_TOTAL("shadow_retention_purged_total"),
        PINNED_COUNT("shadow_pinned_count"),
        PURGE_REFUSED_PINNED_TOTAL("shadow_purge_refused_pinned_total"),
        DEEP_READS_TOTAL("shadow_deep_reads_total"),
        /** Cumulative milliseconds due deep reads waited for the pace. */
        DEEP_READ_PACED_MS("shadow_deep_read_paced_ms"),
        HOUSEKEEPING_POLLS_TOTAL("shadow_housekeeping_polls_total"),
        PURGE_REQUESTS_TOTAL("shadow_purge_requests_total");

        private final String metricName;

        Counter(final String metricName) {
            this.metricName = metricName;
        }

        public String metricName() {
            return metricName;
        }
    }

    /** Plain longs (no Aeron counter), atomic: written on the pruner thread, read by the host's exporter. */
    public static final class Counters {
        private final AtomicLongArray values = new AtomicLongArray(Counter.values().length);
        private final Map<State, Long> skippedNonUsable = new ConcurrentHashMap<>();

        public Counters() {
            values.set(Counter.ANCHOR_POSITION.ordinal(), -1);
            values.set(Counter.MARGIN_POSITION.ordinal(), -1);
        }

        public long get(final Counter c) {
            return values.get(c.ordinal());
        }

        void set(final Counter c, final long v) {
            values.set(c.ordinal(), v);
        }

        void increment(final Counter c) {
            values.incrementAndGet(c.ordinal());
        }

        void add(final Counter c, final long delta) {
            values.addAndGet(c.ordinal(), delta);
        }

        /** {@code shadow_retention_skipped_non_usable{state}}: cumulative over retention passes. */
        public long skippedNonUsable(final State state) {
            return skippedNonUsable.getOrDefault(state, 0L);
        }

        void addSkippedNonUsable(final State state, final int n) {
            skippedNonUsable.merge(state, (long)n, Long::sum);
        }

        /** Every counter as {@code name -> value}, sorted by name. */
        public Map<String, Long> snapshot() {
            final Map<String, Long> out = new TreeMap<>();
            for (final Counter c : Counter.values()) {
                out.put(c.metricName(), get(c));
            }
            skippedNonUsable.forEach((s, n) -> out.put("shadow_retention_skipped_non_usable{state=" + s + "}", n));
            return out;
        }
    }

    private static final long NEVER = Long.MIN_VALUE;

    private final ShadowCatalog catalog;
    private final ShadowRetention retention;
    private final ShadowFloor floor;
    private final NanoClock clock;
    private final long scanPeriodNs;
    private final long deepReadPaceNs;
    private final Runnable refresh;
    private final Counters counters;

    private Scan scan;
    private long lastScanNs = NEVER;
    private long lastDeepReadNs = NEVER;
    private long deepReadDueSinceNs = NEVER;
    private boolean startupPassDone;
    private boolean passClean = true;
    private Set<Long> seenVerified = new HashSet<>();

    /**
     * @param scanPeriodMs    {@code SHADOW_SCAN_PERIOD_MS}, 100..3600000
     * @param deepReadPaceMs  {@code SHADOW_DEEP_READ_PACE_MS}, 0..3600000
     * @param refresh         run every poll before retention and the floor (the host's journal-tail refresh), or a
     *                        no-op
     */
    public ShadowHousekeeping(final ShadowCatalog catalog, final ShadowRetention retention, final ShadowFloor floor,
                              final NanoClock clock, final long scanPeriodMs, final long deepReadPaceMs,
                              final Runnable refresh, final Counters counters) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.retention = Objects.requireNonNull(retention, "retention");
        this.floor = Objects.requireNonNull(floor, "floor");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (scanPeriodMs < 100 || scanPeriodMs > 3_600_000) {
            throw new IllegalArgumentException("SHADOW_SCAN_PERIOD_MS=" + scanPeriodMs + " out of range [100,3600000]");
        }
        if (deepReadPaceMs < 0 || deepReadPaceMs > 3_600_000) {
            throw new IllegalArgumentException("SHADOW_DEEP_READ_PACE_MS=" + deepReadPaceMs
                    + " out of range [0,3600000]");
        }
        this.scanPeriodNs = TimeUnit.MILLISECONDS.toNanos(scanPeriodMs);
        this.deepReadPaceNs = TimeUnit.MILLISECONDS.toNanos(deepReadPaceMs);
        this.refresh = Objects.requireNonNull(refresh, "refresh");
        this.counters = Objects.requireNonNull(counters, "counters");
    }

    /** {@code SnapshotLogPruner.onWake(housekeeping)}. */
    @Override
    public void run() {
        poll();
    }

    /** One housekeeping step (class Javadoc). Never throws an archive error. */
    public void poll() {
        counters.increment(Counter.HOUSEKEEPING_POLLS_TOTAL);
        final long now = clock.nanoTime();
        if (scan == null || now - lastScanNs >= scanPeriodNs) {
            rescan();
            lastScanNs = now;
        }

        final ShadowEntry pending = newestUnverified(scan);
        if (pending != null) {
            if (deepReadDueSinceNs == NEVER) {
                deepReadDueSinceNs = now;
            }
            if (lastDeepReadNs == NEVER || now - lastDeepReadNs >= deepReadPaceNs) {
                counters.add(Counter.DEEP_READ_PACED_MS, TimeUnit.NANOSECONDS.toMillis(now - deepReadDueSinceNs));
                lastDeepReadNs = now;
                counters.increment(Counter.DEEP_READS_TOTAL);
                final DeepResult r = catalog.verifyDeep(pending);
                passClean &= r.kind() != DeepResult.Kind.SCAN_ERROR;
                if (r.kind() != DeepResult.Kind.VERIFIED) {
                    System.out.println("[SHADOW-HK] deep read of rec=" + pending.recordingId() + " P="
                            + pending.header().logPosition() + ": " + r);
                }
                rescan();
                // the next copy is due from now: its wait for the pace is what shadow_deep_read_paced_ms counts
                deepReadDueSinceNs = newestUnverified(scan) == null ? NEVER : now;
            }
        } else {
            deepReadDueSinceNs = NEVER;
        }

        if (!startupPassDone && newestUnverified(scan) == null) {
            if (passClean && scan.errors() == 0) {
                catalog.verdicts().markStartupPassDone(catalog.archiveId());
                startupPassDone = true;
                rescan();
                System.out.println("[SHADOW-HK] startup pass done: " + scan.verified().size() + " VERIFIED, "
                        + scan.conflicts().size() + " CONFLICT, errors=" + scan.errors());
            } else {
                // a SCAN_ERROR somewhere in this pass: run the pass again (verdicts are cached, so only the
                // unknown copies are read again)
                passClean = true;
            }
        }

        refresh.run();

        if (scan.known()) {
            final ShadowRetention.Pass pass = retention.apply(scan);
            if (!pass.purged().isEmpty()) {
                rescan();
            }
        }
        floor.observe(scan);

        if (scan.known()) {
            boolean appeared = false;
            final Set<Long> verifiedNow = new HashSet<>();
            for (final LogicalShadow v : scan.verified()) {
                verifiedNow.add(v.logPosition());
                appeared |= !seenVerified.contains(v.logPosition());
            }
            seenVerified = verifiedNow;
            final ShadowFloor.Evaluation e = floor.evaluate(scan);
            floor.publish(e);
            if (appeared || e.floor() > Math.max(0, floor.lastApplied())) {
                if (!floor.purgeRequested()) {
                    counters.increment(Counter.PURGE_REQUESTS_TOTAL);
                }
                floor.requestPurge();
            }
        } else {
            floor.publish(floor.evaluate(scan));
        }
    }

    /** The latest scan this hook took, or null before the first poll. */
    public Scan scan() {
        return scan;
    }

    public boolean startupPassDone() {
        return startupPassDone;
    }

    public Counters counters() {
        return counters;
    }

    private void rescan() {
        scan = catalog.scan();
        passClean &= scan.errors() == 0;
    }

    /** The newest COMPLETE copy without a deep verdict (the next deep read), or null. */
    private static ShadowEntry newestUnverified(final Scan s) {
        ShadowEntry best = null;
        for (final ShadowEntry e : s.all()) {
            if (e.state() == State.COMPLETE
                    && (best == null || e.header().logPosition() > best.header().logPosition())) {
                best = e;
            }
        }
        return best;
    }
}
