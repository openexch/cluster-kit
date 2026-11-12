// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * The floor-side two-producer witness alarm, evaluated on every member after each floor evaluation in shadow mode
 * under the {@code two-producers} anchor rule (never only on the producer: the node that is down, or does not know
 * it is designated, cannot raise it).
 *
 * <p>When the newest VERIFIED position holding an ALIGNED copy has fewer than two witnesses (distinct producers among
 * its VERIFIED ALIGNED copies, the floor's own rule) for longer than the window (the host passes
 * {@code 2 x SNAPSHOT_INTERVAL_MINUTES}), one CRITICAL line per window names the position, the producers it has and
 * the missing kind ({@code R} replica, {@code F} designated follower), and {@link #missing} reads 1 for each missing
 * kind while the alarm holds. The first evaluation with two witnesses clears it. One mechanism covers a down
 * designated follower, a dead or lagging replica, a leader id that stays unknown, a leader id mismatch and a producer
 * whose writes keep failing. Readiness never reads it.</p>
 *
 * <p>Single-threaded (the host's housekeeping thread); the gauges are volatile for the exporter.</p>
 */
public final class WitnessAlarm {

    /** What one evaluation found. */
    public record Result(boolean holding, String criticalLine, long position, Set<String> producers,
                         Set<ProducerKind> missing) {
        public Result {
            producers = Set.copyOf(producers);
            missing = missing.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(missing));
        }
    }

    private final long windowNs;
    private final Set<NegativeControl> off;
    private long sinceNs = -1;
    private long lastLineNs = -1;
    private volatile boolean missingReplica;
    private volatile boolean missingFollower;

    /** @param windowNs how long fewer than two witnesses may last before the alarm, > 0 */
    public WitnessAlarm(final long windowNs) {
        this(windowNs, EnumSet.noneOf(NegativeControl.class));
    }

    WitnessAlarm(final long windowNs, final Set<NegativeControl> off) {
        if (windowNs <= 0) {
            throw new IllegalArgumentException("witness alarm window must be > 0 ns: " + windowNs);
        }
        this.windowNs = windowNs;
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
    }

    /** The newest VERIFIED position group that holds at least one VERIFIED ALIGNED copy, or null. */
    public static LogicalShadow newestAligned(final ShadowCatalog.Scan scan) {
        if (scan == null) {
            return null;
        }
        for (final LogicalShadow v : scan.verified()) {
            if (ShadowFloor.witnessCopies(v, false, new TreeSet<>()) > 0) {
                return v;
            }
        }
        return null;
    }

    /** One evaluation over the floor's scan; a scan that cannot answer leaves the state unchanged. */
    public Result observe(final long nowNs, final ShadowCatalog.Scan scan) {
        if (scan == null || !scan.known()) {
            return new Result(holding(), null, -1, Set.of(), currentMissing());
        }
        return observe(nowNs, newestAligned(scan));
    }

    /** One evaluation of the newest ALIGNED position group ({@code null}: none). */
    public Result observe(final long nowNs, final LogicalShadow newest) {
        final Set<String> producers = new TreeSet<>();
        if (newest != null) {
            ShadowFloor.witnessCopies(newest, false, producers);
        }
        final long position = newest == null ? -1 : newest.logPosition();
        if (producers.size() >= 2 || off.contains(NegativeControl.WITNESS_ALARM_OFF)) {
            if (sinceNs >= 0 && lastLineNs >= 0 && producers.size() >= 2) {
                System.out.println("[SHADOW-FLOOR] two-producer anchor restored: newest ALIGNED at P=" + position
                        + " has producers " + producers);
            }
            sinceNs = -1;
            lastLineNs = -1;
            missingReplica = false;
            missingFollower = false;
            return new Result(false, null, position, producers, Set.of());
        }
        final EnumSet<ProducerKind> missing = EnumSet.allOf(ProducerKind.class);
        for (final String p : producers) {
            final ProducerKind k = p.isEmpty() ? null : ProducerKind.ofLetter(p.charAt(0));
            if (k != null) {
                missing.remove(k);
            }
        }
        if (sinceNs < 0) {
            sinceNs = nowNs;
        }
        if (nowNs - sinceNs < windowNs) {
            return new Result(false, null, position, producers, missing);
        }
        missingReplica = missing.contains(ProducerKind.REPLICA);
        missingFollower = missing.contains(ProducerKind.FOLLOWER);
        String line = null;
        if (lastLineNs < 0 || nowNs - lastLineNs >= windowNs) {
            lastLineNs = nowNs;
            line = "no two-producer anchor for " + TimeUnit.NANOSECONDS.toMinutes(nowNs - sinceNs) + " min: newest"
                    + " ALIGNED at P=" + position + " has producers " + producers + "; missing kind " + kinds(missing);
        }
        return new Result(true, line, position, producers, missing);
    }

    /** {@code shadow_floor_witness_missing{kind}}: 1 while the alarm holds and that kind is missing. */
    public int missing(final ProducerKind kind) {
        return kind == ProducerKind.REPLICA ? (missingReplica ? 1 : 0) : (missingFollower ? 1 : 0);
    }

    /** True while the alarm holds. */
    public boolean holding() {
        return missingReplica || missingFollower;
    }

    private Set<ProducerKind> currentMissing() {
        final EnumSet<ProducerKind> m = EnumSet.noneOf(ProducerKind.class);
        if (missingReplica) {
            m.add(ProducerKind.REPLICA);
        }
        if (missingFollower) {
            m.add(ProducerKind.FOLLOWER);
        }
        return m;
    }

    static String kinds(final Set<ProducerKind> missing) {
        final StringBuilder b = new StringBuilder();
        for (final ProducerKind k : List.of(ProducerKind.REPLICA, ProducerKind.FOLLOWER)) {
            if (missing.contains(k)) {
                b.append(b.length() == 0 ? "" : ", ").append(k.letter()).append(" (")
                        .append(k == ProducerKind.REPLICA ? "replica" : "designated follower").append(')');
            }
        }
        return b.toString();
    }
}
