// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.Scan;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * The member's purge watermark (RCS S4 9.1): the R03-literal floor (D11) with a restorable margin (S3 carry 1, review
 * B1, D-S4-13). Plugged into {@code SnapshotLogPruner.watermark} in BOTH {@code RCS_MODE}s; {@code ArchiveHousekeeping}
 * purges to {@code min(latest snapshot, this)}.
 *
 * <p>Precondition (a fallback needs one): the table below applies only when the newest snapshot group at P_cm holds a
 * STUB service payload. When it holds a FULL service image the log below P_cm is not needed to restore, so the answer
 * is P_cm (today's pruning, unchanged). When the kind cannot be read the answer is 0 (purge nothing, fail closed).
 * An {@code off}-mode node still meets stubs: the MF-1 skip-window snapshot and a mode switch from {@code shadow}.</p>
 *
 * <pre>
 * P_cm &lt; 0                      -> NO_SNAPSHOT, 0
 * newest group FULL             -> FULL_IMAGE, P_cm (today's pruning)
 * newest group kind unreadable  -> IMAGE_UNKNOWN, 0 (purge nothing)
 * newest group STUB, then:
 * !scan.known()                 -> UNKNOWN, 0 (purge nothing)
 * skip     = scan.newestAtOrAbove(P_cm + 1): the ONE shadow above P_cm a restore tries (SKIP, D-S4-3)
 * R        = VERIFIED v with v.P_s <= P_cm, or v == skip (FOUND) and skipSafe(v)   (restorable by THIS node)
 * covering = { v in R : v.P_s >= P_cm }; empty -> 0 (no purge below P_cm without a covering local shadow)
 * anchor   = the OLDEST covering shadow
 * margin none              -> P_cm
 * margin previous-verified -> prev = newest v in R below anchor; none -> 0; else min(P_cm, prev.P_s)
 * </pre>
 *
 * <p>{@code restorable} mirrors the restore order exactly (review FIX-1): SKIP tries only the newest usable position
 * above P_cm and abandons SKIP for the boot when it is CONFLICT, not skip-safe or refused, so an older VERIFIED shadow
 * above P_cm is never a restore path while a newer one exists, whatever its own journal seq. Skip-safety is per
 * candidate ({@code localDurableSeq >= candidate.journalSeq}), so an older shadow can be safe while the newest is not;
 * counting it would let {@code SHADOW_FLOOR_MARGIN=none} purge to P_cm with no way back.</p>
 *
 * <p>Only VERIFIED shadows this node can restore from feed it (hard rule 12.7); UNKNOWN means purge nothing. It is
 * recomputed on every call; a lower answer than the log's current start (the last applied purge point, read from the
 * archive through {@code logStartPosition}, never from memory, C4) is counted {@code shadow_floor_regressed_total}
 * and changes nothing on disk, since nothing can un-purge.</p>
 *
 * <p>Threads: {@link #observe} (the housekeeping hook) and {@link #getAsLong} (the purge pass) both run on the pruner
 * thread; the fields are volatile so a host's exporter may read them from elsewhere.</p>
 */
public final class ShadowFloor implements LongSupplier {

    /** {@code SHADOW_FLOOR_MARGIN} (S4 3.5). */
    public enum Margin {
        /** Keep the log from the newest restorable shadow below the anchor (default while one producer exists). */
        PREVIOUS_VERIFIED("previous-verified"),
        /** Prune to P_cm once a covering shadow exists (S7's D14 anchors). */
        NONE("none");

        private final String text;

        Margin(final String text) {
            this.text = text;
        }

        public String text() {
            return text;
        }

        /** @throws IllegalArgumentException with the S4 3.5 refusal text */
        public static Margin parse(final String value) {
            for (final Margin m : values()) {
                if (m.text.equals(value)) {
                    return m;
                }
            }
            throw new IllegalArgumentException("SHADOW_FLOOR_MARGIN '" + value
                    + "' (expected previous-verified|none)");
        }
    }

    /** The newest snapshot group's service payload, as the host reads it ({@link StubPeek}). */
    public enum NewestImage {
        /** A full service image: the floor answers P_cm (today's pruning). */
        FULL,
        /** A stub: the 9.1 table applies. */
        STUB,
        /** The host could not read the payload kind: purge nothing. */
        UNKNOWN
    }

    /** One evaluation of the table above. Positions are -1 where absent. */
    public record Evaluation(Kind kind, long latestSnapshotPosition, long floor, LogicalShadow anchor,
                             LogicalShadow margin, List<LogicalShadow> restorable) {

        public enum Kind {
            /** The scan cannot answer: errors, or the startup pass is not done. Purge nothing. */
            UNKNOWN,
            /** No valid CM snapshot: purge nothing. */
            NO_SNAPSHOT,
            /** The newest group is a full service image: the floor is P_cm (today's pruning, the precondition). */
            FULL_IMAGE,
            /** The newest group's payload kind could not be read: purge nothing. */
            IMAGE_UNKNOWN,
            /** No restorable VERIFIED shadow at or above P_cm (R03 D2 literal): purge nothing. */
            NO_COVERING,
            /** A covering anchor but no restorable shadow below it while the margin is on: purge nothing. */
            NO_MARGIN,
            /** A floor above zero may apply. */
            FLOOR
        }

        public Evaluation {
            restorable = List.copyOf(restorable);
        }

        public long anchorPosition() {
            return anchor == null ? -1 : anchor.logPosition();
        }

        public long marginPosition() {
            return margin == null ? -1 : margin.logPosition();
        }

        @Override
        public String toString() {
            return "Floor{" + kind + " floor=" + floor + " P_cm=" + latestSnapshotPosition + " anchor="
                    + anchorPosition() + " margin=" + marginPosition() + " restorable="
                    + restorable.stream().map(LogicalShadow::logPosition).toList() + "}";
        }
    }

    private final LongSupplier latestSnapshotPosition;
    private final Supplier<NewestImage> newestImage;
    private final Predicate<ShadowHeader> skipSafe;
    private final Margin margin;
    private final LongSupplier logStartPosition;
    private final ShadowHousekeeping.Counters counters;
    private final Set<NegativeControl> off;

    private volatile Scan scan;
    private volatile Evaluation lastEvaluation;
    private volatile long lastApplied = -1;
    private volatile boolean purgeRequested;

    /**
     * @param latestSnapshotPosition P_cm: this node's newest valid CM snapshot position, or -1 (the host reads its
     *                               {@code RecordingLog}; this package never opens one)
     * @param newestImage            the newest group's service payload kind (the precondition of the table)
     * @param skipSafe               S4 8.1 over this node's journal tail: may a shadow above P_cm be SKIP-restored here
     * @param margin                 {@code SHADOW_FLOOR_MARGIN}
     * @param logStartPosition       the local log recording's start position, read from the archive on each call
     */
    public ShadowFloor(final LongSupplier latestSnapshotPosition, final Supplier<NewestImage> newestImage,
                       final Predicate<ShadowHeader> skipSafe, final Margin margin,
                       final LongSupplier logStartPosition, final ShadowHousekeeping.Counters counters) {
        this(latestSnapshotPosition, newestImage, skipSafe, margin, logStartPosition, counters,
                EnumSet.noneOf(NegativeControl.class));
    }

    ShadowFloor(final LongSupplier latestSnapshotPosition, final Supplier<NewestImage> newestImage,
                final Predicate<ShadowHeader> skipSafe, final Margin margin, final LongSupplier logStartPosition,
                final ShadowHousekeeping.Counters counters, final Set<NegativeControl> off) {
        this.latestSnapshotPosition = Objects.requireNonNull(latestSnapshotPosition, "latestSnapshotPosition");
        this.newestImage = Objects.requireNonNull(newestImage, "newestImage");
        this.skipSafe = Objects.requireNonNull(skipSafe, "skipSafe");
        this.margin = Objects.requireNonNull(margin, "margin");
        this.logStartPosition = Objects.requireNonNull(logStartPosition, "logStartPosition");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
        counters.set(ShadowHousekeeping.Counter.FLOOR_UNKNOWN, 1);
    }

    public Margin margin() {
        return margin;
    }

    /** The scan the next evaluation reads (the housekeeping hook's latest). */
    public void observe(final Scan latest) {
        this.scan = latest;
    }

    /** The scan the floor reads now, or null before the first {@link #observe}. */
    public Scan scan() {
        return scan;
    }

    /** Evaluates the current inputs without applying anything (housekeeping, retention pins, status). */
    public Evaluation evaluate() {
        return evaluate(scan);
    }

    /** Evaluates {@code s} against the current P_cm and newest group kind. */
    public Evaluation evaluate(final Scan s) {
        final long pCm = latestSnapshotPosition.getAsLong();
        if (pCm < 0) {
            return new Evaluation(Evaluation.Kind.NO_SNAPSHOT, pCm, 0, null, null, List.of());
        }
        NewestImage kind;
        try {
            kind = newestImage.get();
        } catch (final RuntimeException unreadable) {
            kind = NewestImage.UNKNOWN;
        }
        return evaluate(pCm, kind == null ? NewestImage.UNKNOWN : kind, s);
    }

    /** The whole table of the class Javadoc, precondition first. Pure. */
    public Evaluation evaluate(final long pCm, final NewestImage kind, final Scan s) {
        if (pCm < 0) {
            return new Evaluation(Evaluation.Kind.NO_SNAPSHOT, pCm, 0, null, null, List.of());
        }
        if (off.contains(NegativeControl.FLOOR_PRECONDITION_OFF)) {
            // the control: every newest group read as a full image, the pre-D2 off-mode pruning
            return new Evaluation(Evaluation.Kind.FULL_IMAGE, pCm, pCm, null, null, List.of());
        }
        switch (Objects.requireNonNull(kind, "kind")) {
            case FULL:
                return new Evaluation(Evaluation.Kind.FULL_IMAGE, pCm, pCm, null, null, List.of());
            case UNKNOWN:
                return new Evaluation(Evaluation.Kind.IMAGE_UNKNOWN, pCm, 0, null, null, List.of());
            default:
                return evaluate(pCm, s);
        }
    }

    /** The 9.1 table for a STUB newest group (the precondition already held). Pure. */
    public Evaluation evaluate(final long pCm, final Scan s) {
        if (s == null || !s.known()) {
            return new Evaluation(Evaluation.Kind.UNKNOWN, pCm, 0, null, null, List.of());
        }
        final List<LogicalShadow> source = off.contains(NegativeControl.FLOOR_READS_USABLE) ? s.usable() : s.verified();
        final long skipTarget = skipTarget(pCm, s);
        final List<LogicalShadow> restorable = new ArrayList<>();
        for (final LogicalShadow v : source) {
            if (restorable(v, pCm, skipTarget)) {
                restorable.add(v);
            }
        }
        if (pCm < 0) {
            return new Evaluation(Evaluation.Kind.NO_SNAPSHOT, pCm, 0, null, null, restorable);
        }
        LogicalShadow anchor = null;
        if (off.contains(NegativeControl.FLOOR_NEWEST_COVERING_ONLY)) {
            // the pre-B1 reading: verifiedAtOrAbove answers the NEWEST verified >= P_cm, restorable or not
            if (!source.isEmpty() && source.get(0).logPosition() >= pCm
                    && restorable(source.get(0), pCm, skipTarget)) {
                anchor = source.get(0);
            }
        } else {
            for (final LogicalShadow v : restorable) {
                if (v.logPosition() >= pCm && (anchor == null || v.logPosition() < anchor.logPosition())) {
                    anchor = v;
                }
            }
        }
        if (anchor == null) {
            return new Evaluation(Evaluation.Kind.NO_COVERING, pCm, 0, null, null, restorable);
        }
        if (margin == Margin.NONE || off.contains(NegativeControl.FLOOR_MARGIN_IGNORED)) {
            return new Evaluation(Evaluation.Kind.FLOOR, pCm, pCm, anchor, null, restorable);
        }
        final List<LogicalShadow> marginSource =
                off.contains(NegativeControl.FLOOR_MARGIN_OVER_ALL_VERIFIED) ? source : restorable;
        LogicalShadow prev = null;
        for (final LogicalShadow v : marginSource) {
            if (v.logPosition() < anchor.logPosition() && (prev == null || v.logPosition() > prev.logPosition())) {
                prev = v;
            }
        }
        if (prev == null) {
            return new Evaluation(Evaluation.Kind.NO_MARGIN, pCm, 0, anchor, null, restorable);
        }
        return new Evaluation(Evaluation.Kind.FLOOR, pCm, Math.min(pCm, prev.logPosition()), anchor, prev,
                restorable);
    }

    /**
     * The position SKIP would try ({@code newestAtOrAbove(P_cm + 1)} FOUND), or -1 when SKIP has no candidate (EMPTY,
     * or CONFLICT: SKIP is abandoned). Under {@link NegativeControl#FLOOR_SKIP_ANY_ABOVE} every position above P_cm
     * answers itself (the pre-FIX-1 reading).
     */
    private long skipTarget(final long pCm, final Scan s) {
        if (off.contains(NegativeControl.FLOOR_SKIP_ANY_ABOVE)) {
            return Long.MIN_VALUE;
        }
        final ShadowCatalog.Answer a = s.newestAtOrAbove(pCm + 1);
        return a.kind() == ShadowCatalog.Answer.Kind.FOUND ? a.shadow().logPosition() : -1;
    }

    /**
     * {@code restorable(v)} of S4 9.1: an EQUAL or TAIL base at or below P_cm, or the ONE shadow SKIP tries when it is
     * skip-safe on this node (review FIX-1).
     */
    private boolean restorable(final LogicalShadow v, final long pCm, final long skipTarget) {
        if (v.logPosition() <= pCm) {
            return true;
        }
        final boolean isSkipTarget = skipTarget == Long.MIN_VALUE || v.logPosition() == skipTarget;
        return isSkipTarget && skipSafe.test(v.representative().header());
    }

    /**
     * The purge pass: evaluates, records and returns the floor. Consumes {@link #purgeRequested}.
     */
    @Override
    public long getAsLong() {
        purgeRequested = false;
        final Evaluation e = evaluate();
        lastEvaluation = e;
        lastApplied = e.floor();
        publish(e);
        long logStart;
        try {
            logStart = logStartPosition.getAsLong();
        } catch (final RuntimeException unreadable) {
            logStart = -1;
        }
        if (logStart > 0 && e.floor() < logStart) {
            counters.increment(ShadowHousekeeping.Counter.FLOOR_REGRESSED_TOTAL);
            System.out.println("[SHADOW-FLOOR] floor " + e.floor() + " is below the log start " + logStart
                    + " (already purged; nothing can un-purge): " + e);
        }
        return e.floor();
    }

    /** Housekeeping: a VERIFIED shadow appeared or the floor rose; the next pruner wake runs a purge pass. */
    public void requestPurge() {
        purgeRequested = true;
    }

    /** The pruner's wake predicate ({@code SnapshotLogPruner.wake(floor::purgeRequested)}). Not consuming. */
    public boolean purgeRequested() {
        return purgeRequested;
    }

    /** The floor the last purge pass returned, or -1 before the first. */
    public long lastApplied() {
        return lastApplied;
    }

    /** The evaluation the last purge pass used, or null. */
    public Evaluation lastEvaluation() {
        return lastEvaluation;
    }

    void publish(final Evaluation e) {
        counters.set(ShadowHousekeeping.Counter.FLOOR_UNKNOWN,
                e.kind() == Evaluation.Kind.UNKNOWN || e.kind() == Evaluation.Kind.IMAGE_UNKNOWN ? 1 : 0);
        counters.set(ShadowHousekeeping.Counter.FLOOR_POSITION, e.floor());
        counters.set(ShadowHousekeeping.Counter.FLOOR_LAG_BYTES,
                e.latestSnapshotPosition() < 0 ? 0 : Math.max(0, e.latestSnapshotPosition() - e.floor()));
        counters.set(ShadowHousekeeping.Counter.ANCHOR_POSITION, e.anchorPosition());
        counters.set(ShadowHousekeeping.Counter.MARGIN_POSITION, e.marginPosition());
    }
}
