// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * The replica's one fence latch and what each {@link FenceFamily} does after it. Safe from any thread; the first
 * fence wins, every later one is ignored. Order, on the thread that fenced:
 *
 * <pre>
 * every family  latch (the reason and the family), CRITICAL line, readiness 503 (fenced:, or refused: for a
 *               cluster identity refusal), abort the in-flight write
 * MISMATCH      write the mismatch dump (an I/O failure never changes the decision), enter the hold, tell the host;
 *               HOLD_OFF control: exit 1 at once
 * HISTORY       ask the housekeeping thread to purge own shadows above {@link #historyPurgeBound} (the last EQUAL
 *               position or the restore point, whichever is higher) and WAIT for it, at most
 *               REPLICA_FENCE_PURGE_WAIT_MS (timeout: "history fence: purge of n suspect shadows did not finish in
 *               t ms: wipe BASE_DIR"), then exit 1
 * FAULT         exit 1
 * </pre>
 */
final class ReplicaFence {

    /** What the latch changes outside this class. */
    interface Effects {
        void fenced(String reason);

        void refused(String reason);

        void abortWrite();
    }

    /** The housekeeping thread's one-slot suspect purge. */
    interface SuspectPurge {
        Pending request(long above, String reason);
    }

    /** One suspect purge request. */
    interface Pending {
        /** The number of shadows purged, or -1 when it did not finish within {@code timeoutMs}. */
        int await(long timeoutMs) throws InterruptedException;

        /** Suspect shadows found by the purge so far, -1 before it scanned. */
        int suspects();
    }

    private final ReplicaFollower.Halt halt;
    private final Effects effects;
    private final MismatchHold hold;
    private final MismatchDump dump;
    private final long purgeWaitMs;
    private final Set<ReplicaNegativeControl> off;

    private volatile SuspectPurge purge;
    private volatile String reason;
    private volatile FenceFamily family;
    /** Test seam: a dump failure exits instead of holding (the rule this class must never have). */
    volatile boolean dumpFailureExits;

    ReplicaFence(final ReplicaFollower.Halt halt, final Effects effects, final MismatchHold hold,
                 final MismatchDump dump, final long purgeWaitMs, final Set<ReplicaNegativeControl> off) {
        this.halt = Objects.requireNonNull(halt, "halt");
        this.effects = Objects.requireNonNull(effects, "effects");
        this.hold = Objects.requireNonNull(hold, "hold");
        this.dump = dump;
        if (purgeWaitMs <= 0) {
            throw new IllegalArgumentException("REPLICA_FENCE_PURGE_WAIT_MS must be > 0: " + purgeWaitMs);
        }
        this.purgeWaitMs = purgeWaitMs;
        this.off = off.isEmpty() ? EnumSet.noneOf(ReplicaNegativeControl.class) : EnumSet.copyOf(off);
    }

    /**
     * The history fence's purge lower bound: own shadows strictly above it are suspect, the ones at or below it are
     * kept. The last EQUAL position, raised to the restore point: on a LOCAL_SHADOW start the shadows at and below
     * the restore point were written by an earlier process life (the one restored from among them) and may be the
     * last copy of the old history, while {@code verifiedAt} reads -1 until this life's first EQUAL. A genesis start
     * restored at 0 (everything above 0 is suspect until an EQUAL); -1 before bootstrap.
     */
    static long historyPurgeBound(final long verifiedAt, final long restoredPosition) {
        return Math.max(verifiedAt, restoredPosition);
    }

    /** The housekeeping thread's purge, armed after it is built. */
    void suspectPurge(final SuspectPurge port) {
        this.purge = Objects.requireNonNull(port, "port");
    }

    /**
     * Latch and act. {@code refused} reads {@code refused:<reason>} instead of {@code fenced:<reason>} (a cluster
     * identity refusal). {@code purgeAbove} is {@link #historyPurgeBound} (HISTORY purges above it and keeps the
     * shadows at or below it); {@code evidence} the mismatch to dump (MISMATCH), or null.
     *
     * @return false when another fence was already latched (nothing done)
     */
    boolean fence(final FenceFamily f, final String why, final boolean refused, final long purgeAbove,
                  final AlignmentTrust.MismatchEvidence evidence) {
        Objects.requireNonNull(f, "family");
        synchronized (this) {
            if (reason != null) {
                return false;
            }
            reason = why;
            family = f;
        }
        System.err.println("[REPLICA] CRITICAL: " + why);
        if (refused) {
            effects.refused(why);
        } else {
            effects.fenced(why);
        }
        effects.abortWrite();
        switch (f) {
            case MISMATCH -> mismatch(why, evidence);
            case HISTORY -> history(why, purgeAbove);
            default -> halt.fatal(why);
        }
        return true;
    }

    private void mismatch(final String why, final AlignmentTrust.MismatchEvidence evidence) {
        if (off.contains(ReplicaNegativeControl.HOLD_OFF)) {
            System.err.println("[REPLICA] NC HOLD_OFF: an oracle mismatch exits at once");
            halt.fatal(why);
            return;
        }
        if (dump != null && evidence != null) {
            final boolean complete = dump.write(evidence);
            if (!complete && dumpFailureExits) {
                halt.fatal("FATAL replica: mismatch dump failed (" + why + ")");
                return;
            }
        }
        hold.enter(why);
        System.err.println("[REPLICA] CRITICAL: oracle mismatch hold: nothing is applied or written; /ready reads"
                + " fenced, /metrics and the mismatch dump stay served until the operator stops the process"
                + " (adjudicate offline, then wipe or keep; a restart re-latches)");
        halt.hold(why);
    }

    private void history(final String why, final long purgeAbove) {
        if (off.contains(ReplicaNegativeControl.SUSPECT_PURGE_OFF)) {
            System.err.println("[REPLICA] NC SUSPECT_PURGE_OFF: shadows above " + purgeAbove + " kept");
            halt.fatal(why);
            return;
        }
        final SuspectPurge p = purge;
        if (p == null) {
            System.err.println("[REPLICA] CRITICAL: history fence: no housekeeping thread to purge the shadows above "
                    + purgeAbove + ": wipe BASE_DIR");
            halt.fatal(why);
            return;
        }
        System.err.println("[REPLICA] history fence: purging own shadows above " + purgeAbove
                + " (the last EQUAL position or the restore point), kept at or below " + purgeAbove
                + ", before the exit");
        final Pending pending = p.request(purgeAbove, why);
        if (off.contains(ReplicaNegativeControl.SUSPECT_PURGE_NOT_AWAITED)) {
            System.err.println("[REPLICA] NC SUSPECT_PURGE_NOT_AWAITED: the exit does not wait for the purge");
            halt.fatal(why);
            return;
        }
        int purged;
        try {
            purged = pending.await(purgeWaitMs);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            purged = -1;
        }
        if (purged < 0) {
            final int n = pending.suspects();
            System.err.println("[REPLICA] CRITICAL: history fence: purge of " + (n < 0 ? "?" : Integer.toString(n))
                    + " suspect shadows did not finish in " + purgeWaitMs + " ms: wipe BASE_DIR");
        }
        halt.fatal(why);
    }

    /** Housekeeping pass: the hold's own bound. */
    void tickHold() {
        hold.tick();
    }

    String reason() {
        return reason;
    }

    FenceFamily family() {
        return family;
    }

    boolean holding() {
        return hold.holding();
    }

    long heldMs() {
        return hold.heldMs();
    }

    boolean dumpIncomplete() {
        return dump != null && dump.incomplete();
    }
}
