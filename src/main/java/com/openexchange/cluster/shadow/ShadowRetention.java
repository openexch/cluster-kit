// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.PurgeResult;
import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import com.openexchange.cluster.shadow.ShadowEntry.State;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ONE purge owner of the shadows in one archive (RCS S4 9.2, S3 carries 2 and 3, D-S4-5, D-S4-6). Runs on the
 * housekeeping thread only. It is the only production path that deletes a shadow recording; the call-site scans in
 * both trees hold that ({@code ShadowPurgeCallSiteTest}, {@code NoPurgeCallSiteInAssetsTest}). An operator purge
 * reaches it through the node endpoint ({@link #purge}), never over the archive control channel.
 *
 * <ul>
 *   <li><b>Pinned</b> (never purged by retention; {@link #purge} refuses them with {@link #PURGE_CODE_PINNED} unless
 *       {@code force}): the floor's anchor and margin shadow (9.1), every restorable VERIFIED shadow between them,
 *       and the newest VERIFIED shadow (the S3 anchor rule, which {@link ShadowCatalog#purge} also enforces).
 *       Pins are recomputed from each scan (C4).</li>
 *   <li><b>Retention N</b>: keep the {@code SHADOW_RETAIN_N} newest usable POSITIONS plus the pins; purge the
 *       COMPLETE or VERIFIED copies of older positions.</li>
 *   <li><b>Never by retention</b>: CONFLICT (deleting one side picks a winner; CRITICAL while any exists), FOREIGN,
 *       CORRUPT, HEADER_INVALID, TRAILER_MISMATCH, TRAILER_VERSION_UNSUPPORTED, ALIAS_* (evidence), SCAN_ERROR
 *       (unknown), ACTIVE (being written), TOO_SHORT and NO_TRAILER (a crashed write the writer owns). They are
 *       counted {@code shadow_retention_skipped_non_usable{state}}; removing them is an operator action
 *       ({@code force} plus a reason).</li>
 *   <li>Nothing is purged while the scan cannot answer ({@code !scan.known()}).</li>
 * </ul>
 *
 * <p>One instance per archive per process (C9): a second construction over the same archive id is refused; the
 * clearing path is to stop the second owner ({@link #close} releases the archive).</p>
 */
public final class ShadowRetention implements AutoCloseable {

    /** {@code PURGE_REFUSED(PINNED)}: an anchor, margin or between-shadow of the floor, or the newest VERIFIED. */
    public static final int PURGE_CODE_PINNED = -1004;

    private static final Set<Long> OWNED_ARCHIVES = ConcurrentHashMap.newKeySet();

    /** What one retention pass did. */
    public record Pass(boolean ran, Set<Long> pinnedPositions, List<PurgeResult> purged, List<PurgeResult> refused,
                       Map<State, Integer> skippedNonUsable, int conflicts) {
        public Pass {
            pinnedPositions = Set.copyOf(pinnedPositions);
            purged = List.copyOf(purged);
            refused = List.copyOf(refused);
            skippedNonUsable = Map.copyOf(skippedNonUsable);
        }

        static Pass notRun() {
            return new Pass(false, Set.of(), List.of(), List.of(), Map.of(), 0);
        }
    }

    private final ShadowCatalog catalog;
    private final ShadowFloor floor;
    private final int retainN;
    private final ShadowHousekeeping.Counters counters;
    private final Set<NegativeControl> off;
    private volatile boolean closed;

    /**
     * @param retainN {@code SHADOW_RETAIN_N}, 1..1000
     * @throws IllegalStateException when another retention owns this archive in this process (C9)
     */
    public ShadowRetention(final ShadowCatalog catalog, final ShadowFloor floor, final int retainN,
                           final ShadowHousekeeping.Counters counters) {
        this(catalog, floor, retainN, counters, EnumSet.noneOf(NegativeControl.class));
    }

    ShadowRetention(final ShadowCatalog catalog, final ShadowFloor floor, final int retainN,
                    final ShadowHousekeeping.Counters counters, final Set<NegativeControl> off) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.floor = Objects.requireNonNull(floor, "floor");
        if (retainN < 1 || retainN > 1000) {
            throw new IllegalArgumentException("SHADOW_RETAIN_N=" + retainN + " out of range [1,1000]");
        }
        this.retainN = retainN;
        this.counters = Objects.requireNonNull(counters, "counters");
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
        if (!OWNED_ARCHIVES.add(catalog.archiveId())) {
            throw new IllegalStateException("ShadowRetention: archive " + catalog.archiveId()
                    + " already has a purge owner in this process (one per archive, C9): stop the second owner");
        }
    }

    public int retainN() {
        return retainN;
    }

    /** The pins of S4 9.2 over {@code scan} and the floor evaluation of the same scan. */
    public Set<Long> pins(final Scan scan, final ShadowFloor.Evaluation e) {
        final Set<Long> pins = new TreeSet<>();
        if (!scan.verified().isEmpty()) {
            pins.add(scan.verified().get(0).logPosition());
        }
        if (e.anchor() != null) {
            pins.add(e.anchor().logPosition());
        }
        if (e.margin() != null) {
            pins.add(e.margin().logPosition());
        }
        if (e.anchor() != null && e.margin() != null) {
            for (final LogicalShadow v : e.restorable()) {
                if (v.logPosition() > e.margin().logPosition() && v.logPosition() < e.anchor().logPosition()) {
                    pins.add(v.logPosition());
                }
            }
        }
        return pins;
    }

    /**
     * One retention pass over {@code scan} (the housekeeping hook's latest). Purges through
     * {@link ShadowCatalog#purge} (fresh scan, anchor rule, archive code kept).
     */
    public Pass apply(final Scan scan) {
        if (closed || scan == null || !scan.known()) {
            return Pass.notRun();
        }
        final ShadowFloor.Evaluation e = floor.evaluate(scan);
        final Set<Long> pins = pins(scan, e);
        counters.set(ShadowHousekeeping.Counter.PINNED_COUNT, pins.size());

        final Set<Long> keep = new TreeSet<>(pins);
        for (int i = 0; i < scan.usable().size() && i < retainN; i++) {
            keep.add(scan.usable().get(i).logPosition());
        }

        final List<PurgeResult> purged = new ArrayList<>();
        final List<PurgeResult> refused = new ArrayList<>();
        final Map<State, Integer> skipped = new java.util.EnumMap<>(State.class);
        for (final ShadowEntry entry : scan.all()) {
            final State s = entry.state();
            final boolean conflictPurge = s == State.CONFLICT && off.contains(NegativeControl.RETENTION_PURGES_CONFLICT);
            if (!s.usable() && !conflictPurge) {
                skipped.merge(s, 1, Integer::sum);
                continue;
            }
            final long p = entry.header().logPosition();
            if (keep.contains(p)) {
                continue;
            }
            final PurgeResult r = catalog.purge(entry.recordingId(), false);
            if (r.purged()) {
                purged.add(r);
                counters.increment(ShadowHousekeeping.Counter.RETENTION_PURGED_TOTAL);
                System.out.println("[SHADOW-RETENTION] purged rec=" + entry.recordingId() + " P=" + p
                        + " (beyond SHADOW_RETAIN_N=" + retainN + ", not pinned): " + r.detail());
            } else {
                refused.add(r);
                System.out.println("[SHADOW-RETENTION] purge of rec=" + entry.recordingId() + " P=" + p
                        + " refused code=" + r.code() + ": " + r.detail());
            }
        }
        skipped.forEach(counters::addSkippedNonUsable);
        if (!scan.conflicts().isEmpty()) {
            System.err.println("[SHADOW-RETENTION] CRITICAL: " + scan.conflicts().size()
                    + " shadow position(s) in CONFLICT " + scan.conflicts().stream().map(LogicalShadow::logPosition)
                    .toList() + ": retention never resolves a conflict; an operator purges one side with force and"
                    + " a reason");
        }
        return new Pass(true, pins, purged, refused, skipped, scan.conflicts().size());
    }

    /**
     * The operator's purge, routed here by the node endpoint (R07/R08). Refuses a pinned position with
     * {@link #PURGE_CODE_PINNED} unless {@code force}; otherwise {@link ShadowCatalog#purge} decides (anchor rule,
     * UNKNOWN, archive codes).
     */
    public PurgeResult purge(final long recordingId, final boolean force) {
        if (!force) {
            final Scan s = catalog.scan();
            final ShadowEntry target = s.entry(recordingId);
            if (target != null && target.header() != null && s.known()) {
                final ShadowFloor.Evaluation e = floor.evaluate(s);
                if (pins(s, e).contains(target.header().logPosition())) {
                    counters.increment(ShadowHousekeeping.Counter.PURGE_REFUSED_PINNED_TOTAL);
                    return new PurgeResult(PurgeResult.Kind.REFUSED, PURGE_CODE_PINNED,
                            "PURGE_REFUSED(PINNED): P=" + target.header().logPosition() + " is pinned by the floor "
                                    + e, target);
                }
            }
        }
        return catalog.purge(recordingId, force);
    }

    /** Releases the archive for another owner (C9 clearing path). */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            OWNED_ARCHIVES.remove(catalog.archiveId());
        }
    }
}
