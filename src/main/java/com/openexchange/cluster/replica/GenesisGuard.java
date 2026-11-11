// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.agrona.concurrent.NanoClock;

import java.util.Objects;

/**
 * The cluster identity check: the replica follows the cluster its
 * local shadows belong to. Follower thread only; pure (its {@link Source} reads the backup events and the archive).
 *
 * <p>The genesis compare uses the member service snapshot the most recent backup response NAMED (the live
 * cluster's), resolved to the local retrieved copy, never the local RecordingLog's newest entry by position: with
 * {@code cluster/} kept that log mixes both histories and its newest entry can be the OLD history's snapshot. It runs
 * again for every named snapshot it has not compared yet; it is never latched for good. {@link #checked} (the
 * housekeeping gate) is true after the first real compare and false again while a newer named snapshot is
 * unchecked.</p>
 *
 * <pre>
 * PASSED      the newest named snapshot holds the local genesis (or nothing to protect: no local shadow genesis)
 * WAITING     nothing named yet, the named copy not retrieved yet, or it was taken before the ADOPT (genesis 0): the
 *             attach may proceed, {@link #checked} stays false
 * UNREADABLE  the named copy did not decode: not ready, no attach, retried at the next paced pass
 * REFUSED     FATAL replica: cluster genesis &lt;g2&gt; != local shadows &lt;g1&gt;; wipe BASE_DIR (or the term
 *             contradiction text)
 * </pre>
 *
 * <p>{@link #check} evaluates at most once per {@link AlignmentTrust#CHECK_PACE_NS}, like the attach path; between
 * evaluations it returns the last outcome.</p>
 */
final class GenesisGuard {

    enum Outcome { PASSED, WAITING, UNREADABLE, REFUSED }

    /** A service-0 member snapshot a backup response named. */
    record Named(long position, long leadershipTermId, long termBaseLogPosition) {
    }

    interface Source {
        /** The newest service-0 member snapshot a backup response of this process named, null when none. */
        Named newestNamed();

        /** The local recording id of the retrieved copy of {@code n}, -1 while it is not retrieved. */
        long localCopy(Named n);

        /** The genesisId the copy holds (0: taken before the ADOPT); throws when it does not decode. */
        long genesisOf(long recordingId);

        /** Null when consistent, else the refusal text (the copy's RecordingLog entry against its TERM entry). */
        String termContradiction(long recordingId);

        /** Why the local copy of {@code n} was refused by its content (never read again), null when it was not. */
        default String refusalOf(final Named n) {
            return null;
        }
    }

    private final Source source;
    private final NanoClock clock;
    private final long paceNs;
    private final boolean off;

    private Outcome last = Outcome.WAITING;
    private long nextCheckNs;
    private boolean comparedOnce;
    private volatile boolean checked;
    private volatile String detail = "genesis check: awaiting the backup's recording log update";
    private String refusal;
    private Named lastCompared;
    private long preAdoptRecording = Long.MIN_VALUE;
    private String lastLogged;
    private volatile long evaluationsTotal;
    private volatile long comparesTotal;
    /** The newest named snapshot has no usable local copy (not retrieved, partial, undecodable). */
    private volatile boolean namedCopyUnusable;
    /** Genesis decodes that completed (progress toward verification for the stall clock). */
    private volatile long completedComparesTotal;

    GenesisGuard(final Source source, final NanoClock clock, final long paceNs, final boolean off) {
        this.source = Objects.requireNonNull(source, "source");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.paceNs = paceNs;
        this.off = off;
    }

    /**
     * The cluster identity check against {@code localGenesis} (the local shadows' genesis, 0 when there are none),
     * paced.
     */
    Outcome check(final long localGenesis) {
        final long now = clock.nanoTime();
        if (now - nextCheckNs < 0) {
            return last;
        }
        nextCheckNs = now + paceNs;
        evaluationsTotal++;
        namedCopyUnusable = false;
        last = evaluate(localGenesis);
        return last;
    }

    private Outcome evaluate(final long localGenesis) {
        if (off) {
            if (!checked) {
                System.err.println("[REPLICA] NC GENESIS_CHECK_OFF: the cluster genesis and term checks are skipped");
            }
            comparedOnce = true;
            checked = true;
            detail = null;
            return Outcome.PASSED;
        }
        if (refusal != null) {
            return Outcome.REFUSED;
        }
        if (localGenesis <= 0) {
            if (!checked) {
                System.out.println("[REPLICA] genesis check: no local shadow genesis (a genesis start), nothing to"
                        + " protect");
            }
            comparedOnce = true;
            checked = true;
            detail = null;
            return Outcome.PASSED;
        }
        final Named n = source.newestNamed();
        if (n == null) {
            return waiting("genesis check: no member snapshot named by a backup response yet; local shadows genesis "
                    + localGenesis);
        }
        if (n.equals(lastCompared)) {
            checked = true;
            return Outcome.PASSED;
        }
        checked = false; // a named snapshot not compared yet
        final long rec = source.localCopy(n);
        if (rec < 0) {
            namedCopyUnusable = true; // named, no usable copy (a partial one is never resolved, ClusterIdentityCheck.pick)
            final String refused = source.refusalOf(n);
            if (refused != null) {
                return waiting("genesis check: named member snapshot at " + n.position() + " refused: " + refused
                        + "; local shadows genesis " + localGenesis);
            }
            return waiting("genesis check: the member snapshot at " + n.position() + " (term " + n.leadershipTermId()
                    + ") named by the backup is not retrieved yet; local shadows genesis " + localGenesis);
        }
        if (rec == preAdoptRecording) {
            return Outcome.WAITING;
        }
        final String term = source.termContradiction(rec);
        if (term != null) {
            return refuse(term);
        }
        final long g2;
        comparesTotal++;
        try {
            g2 = source.genesisOf(rec);
        } catch (final RuntimeException unreadable) {
            final String text = "genesis check: the retrieved member snapshot rec=" + rec + " at " + n.position()
                    + " did not decode: " + unreadable;
            if (!text.equals(lastLogged)) {
                lastLogged = text;
                System.err.println("[REPLICA] CRITICAL: " + text + " (not ready)");
            }
            detail = text;
            namedCopyUnusable = true; // the caller never caches this copy; the next pass resolves again
            return Outcome.UNREADABLE;
        }
        completedComparesTotal++;
        if (g2 == 0) {
            preAdoptRecording = rec;
            return waiting("genesis check: the member snapshot at " + n.position() + " (term " + n.leadershipTermId()
                    + ") was taken before the ADOPT; re-checked at the next named snapshot, local shadows genesis "
                    + localGenesis);
        }
        if (g2 != localGenesis) {
            return refuse("FATAL replica: cluster genesis " + g2 + " != local shadows " + localGenesis
                    + "; wipe BASE_DIR");
        }
        System.out.println("[REPLICA] genesis check: the named member snapshot at " + n.position() + " (term "
                + n.leadershipTermId() + ", rec=" + rec + ") holds genesis " + g2 + ", local shadows " + localGenesis
                + ": same cluster");
        lastCompared = n;
        comparedOnce = true;
        checked = true;
        detail = null;
        return Outcome.PASSED;
    }

    private Outcome waiting(final String text) {
        detail = text;
        if (!text.equals(lastLogged)) {
            lastLogged = text;
            System.out.println("[REPLICA] " + text);
        }
        return Outcome.WAITING;
    }

    private Outcome refuse(final String text) {
        refusal = text;
        detail = text;
        return Outcome.REFUSED;
    }

    /** The housekeeping gate: a real compare passed and no newer named snapshot is unchecked. */
    boolean checked() {
        return checked;
    }

    /** Readiness gate: a real compare passed once in this process (or nothing to protect, or the control). */
    boolean comparedOnce() {
        return comparedOnce;
    }

    /** The readiness detail while not {@link #comparedOnce}, null after. */
    String detail() {
        return detail;
    }

    String refusal() {
        return refusal;
    }

    /** Paced evaluations. */
    long evaluationsTotal() {
        return evaluationsTotal;
    }

    /** The newest named member snapshot has no usable local copy at the last evaluation. */
    boolean namedCopyUnusable() {
        return namedCopyUnusable;
    }

    /** Genesis decodes of a named copy that completed. */
    long completedComparesTotal() {
        return completedComparesTotal;
    }

    /** Genesis decodes attempted. */
    long comparesTotal() {
        return comparesTotal;
    }
}
