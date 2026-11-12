// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The follower producer's two service-thread bounds, pure and single-threaded (the service thread samples and
 * decides).
 *
 * <p>Window: while a follower serializes, its log subscribers stop, the log image's window stops advancing and the
 * leader's publication is paced by the minimum subscriber (Min flow control): the leader's offers see back pressure
 * once {@code R x T_ser > W}. The guard produces iff {@code R_recent x T_last <= W x budgetPct / 100}, where
 * {@code R_recent} is the log growth over the last {@code rateWindowMs} from the samples the host takes about once a
 * second ({@link #sample}, a ring of {@value #RING}), {@code T_last} is this node's last serialize duration (seeded
 * with the estimate before the first) and {@code W} is the log channel's receiver window. A horizon with fewer than
 * two samples has no rate and produces (no estimate is not a stall).</p>
 *
 * <p>Liveness: the service's Aeron client conductor is invoked from the service duty cycle, so a serialize longer
 * than the client's inter-service timeout ({@code Aeron.Context.interServiceTimeoutNs()}, the driver's client
 * liveness timeout) kills the node. The guard refuses when {@code T_last > liveness / 2}; the host passes the value
 * its client context reports, never an assumed one, and an unknown value ({@code <= 0}) refuses.</p>
 *
 * <p>A wrong estimate costs one back-pressure blip on the leader, never correctness; a deferred position is not
 * retried at another position.</p>
 */
public final class ProducerWindowGuard {

    /** The sample ring length. */
    public static final int RING = 64;
    /** Samples closer together than this are ignored (the host may call more often). */
    public static final long SAMPLE_SPACING_NS = TimeUnit.SECONDS.toNanos(1);

    /** What the guard answers for one ClusterAction. */
    public enum Decision {
        PRODUCE, LIVENESS_BOUND, WINDOW_BUDGET
    }

    private final long windowBytes;
    private final int budgetPct;
    private final long rateWindowNs;
    private final Set<NegativeControl> off;
    private final long[] sampleNs = new long[RING];
    private final long[] samplePosition = new long[RING];
    private int count;
    private int next;
    private long lastSerializeNs;
    private boolean measured;

    /**
     * @param windowBytes         the log channel's receiver window ({@code rcv-wnd}), bytes, > 0
     * @param budgetPct           1..100
     * @param rateWindowMs        the {@code R_recent} horizon, {@value ShadowProducerSettings#MIN_RATE_WINDOW_MS}..
     *                            {@value ShadowProducerSettings#MAX_RATE_WINDOW_MS}
     * @param serializeEstimateMs {@code T_last} before the first measured serialize, 1..
     *                            {@value ShadowProducerSettings#MAX_SERIALIZE_ESTIMATE_MS}
     */
    public ProducerWindowGuard(final long windowBytes, final int budgetPct, final long rateWindowMs,
                               final long serializeEstimateMs) {
        this(windowBytes, budgetPct, rateWindowMs, serializeEstimateMs, EnumSet.noneOf(NegativeControl.class));
    }

    ProducerWindowGuard(final long windowBytes, final int budgetPct, final long rateWindowMs,
                        final long serializeEstimateMs, final Set<NegativeControl> off) {
        if (windowBytes <= 0) {
            throw new IllegalArgumentException("windowBytes must be > 0: " + windowBytes);
        }
        if (budgetPct < ShadowProducerSettings.MIN_WINDOW_BUDGET_PCT
                || budgetPct > ShadowProducerSettings.MAX_WINDOW_BUDGET_PCT) {
            throw new IllegalArgumentException(ShadowProducerSettings.WINDOW_BUDGET_PCT + " must be "
                    + ShadowProducerSettings.MIN_WINDOW_BUDGET_PCT + ".." + ShadowProducerSettings.MAX_WINDOW_BUDGET_PCT
                    + ": " + budgetPct);
        }
        if (rateWindowMs < ShadowProducerSettings.MIN_RATE_WINDOW_MS
                || rateWindowMs > ShadowProducerSettings.MAX_RATE_WINDOW_MS) {
            throw new IllegalArgumentException(ShadowProducerSettings.RATE_WINDOW_MS + " must be "
                    + ShadowProducerSettings.MIN_RATE_WINDOW_MS + ".." + ShadowProducerSettings.MAX_RATE_WINDOW_MS
                    + " ms: " + rateWindowMs);
        }
        if (serializeEstimateMs < ShadowProducerSettings.MIN_SERIALIZE_ESTIMATE_MS
                || serializeEstimateMs > ShadowProducerSettings.MAX_SERIALIZE_ESTIMATE_MS) {
            throw new IllegalArgumentException(ShadowProducerSettings.SERIALIZE_ESTIMATE_MS + " must be "
                    + ShadowProducerSettings.MIN_SERIALIZE_ESTIMATE_MS + ".."
                    + ShadowProducerSettings.MAX_SERIALIZE_ESTIMATE_MS + " ms: " + serializeEstimateMs);
        }
        this.windowBytes = windowBytes;
        this.budgetPct = budgetPct;
        this.rateWindowNs = TimeUnit.MILLISECONDS.toNanos(rateWindowMs);
        this.lastSerializeNs = TimeUnit.MILLISECONDS.toNanos(serializeEstimateMs);
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
    }

    /** True when {@code R x T > W x pct / 100}: the serialize would outlast the budgeted window. */
    public static boolean defer(final double rateBytesPerSecond, final long serializeNs, final long windowBytes,
                                final int budgetPct) {
        final double bytesDuring = rateBytesPerSecond * (serializeNs / 1e9);
        return bytesDuring > windowBytes * (budgetPct / 100.0);
    }

    /** True when {@code T > liveness / 2}, or the liveness timeout is unknown ({@code <= 0}). */
    public static boolean livenessBound(final long serializeNs, final long livenessTimeoutNs) {
        return livenessTimeoutNs <= 0 || serializeNs > livenessTimeoutNs / 2;
    }

    /** One {@code (now, log position)} sample; ignored when within {@link #SAMPLE_SPACING_NS} of the last. */
    public boolean sample(final long nowNs, final long logPosition) {
        if (count > 0) {
            final int last = (next - 1 + RING) % RING;
            if (nowNs - sampleNs[last] < SAMPLE_SPACING_NS) {
                return false;
            }
        }
        sampleNs[next] = nowNs;
        samplePosition[next] = logPosition;
        next = (next + 1) % RING;
        count = Math.min(RING, count + 1);
        return true;
    }

    /** {@code R_recent} in bytes per second over the horizon ending now; 0 with fewer than two samples in it. */
    public double rateBytesPerSecond(final long nowNs) {
        int oldest = -1;
        int newest = -1;
        for (int i = 0; i < count; i++) {
            final int at = (next - 1 - i + RING) % RING;
            if (nowNs - sampleNs[at] > rateWindowNs) {
                break;
            }
            if (newest < 0) {
                newest = at;
            }
            oldest = at;
        }
        if (newest < 0 || oldest == newest) {
            return 0;
        }
        final long dt = sampleNs[newest] - sampleNs[oldest];
        final long bytes = samplePosition[newest] - samplePosition[oldest];
        return dt <= 0 || bytes <= 0 ? 0 : bytes * 1e9 / dt;
    }

    /** A completed serialize: the next decision's {@code T_last}. */
    public void serialized(final long durationNs) {
        if (durationNs >= 0) {
            lastSerializeNs = durationNs;
            measured = true;
        }
    }

    /** {@code T_last} in ns (the estimate until the first {@link #serialized}). */
    public long lastSerializeNs() {
        return lastSerializeNs;
    }

    /** True once a serialize was measured in this process. */
    public boolean measured() {
        return measured;
    }

    public long windowBytes() {
        return windowBytes;
    }

    public int budgetPct() {
        return budgetPct;
    }

    /** The liveness leg first, then the window leg. */
    public Decision decide(final long nowNs, final long livenessTimeoutNs) {
        if (!off.contains(NegativeControl.LIVENESS_BOUND_OFF) && livenessBound(lastSerializeNs, livenessTimeoutNs)) {
            return Decision.LIVENESS_BOUND;
        }
        if (!off.contains(NegativeControl.WINDOW_GUARD_OFF)
                && defer(rateBytesPerSecond(nowNs), lastSerializeNs, windowBytes, budgetPct)) {
            return Decision.WINDOW_BUDGET;
        }
        return Decision.PRODUCE;
    }

    /** {@code R=<B/s> T=<ms> W=<B> budget=<pct>%}: what a skip line names. */
    public String describe(final long nowNs) {
        return "R=" + Math.round(rateBytesPerSecond(nowNs)) + "B/s T=" + TimeUnit.NANOSECONDS.toMillis(lastSerializeNs)
                + "ms W=" + windowBytes + "B budget=" + budgetPct + "%";
    }
}
