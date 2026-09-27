// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.service.Cluster.Role;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Service duty-cycle liveness and bounded, term-fenced catch-up evidence.
 * Only the service thread supplies observations, after application callbacks return.
 * Readers see one immutable observation. No network, disk access or waiting is done here.
 * A role or a manually asserted boolean never establishes readiness.
 */
public final class NodeReadiness {
    public static final long DEFAULT_LIVENESS_STALE_MS = 30_000;
    public static final long DEFAULT_READINESS_STALE_MS = 5_000;
    public static final long MAX_CHECKPOINT_AGE_MS = 1_000;
    public static final long MAX_APPLY_LAG_BYTES = 64 * 1024;

    private final long livenessStaleNs;
    private final long readinessStaleNs;
    private final LongSupplier clock;
    private volatile long lastTickNs;
    private volatile boolean ticked;
    private volatile boolean started;
    private volatile boolean stopping;
    private volatile String terminalReason;
    private volatile Role role;
    private volatile Evidence evidence;
    // Service-thread-owned checkpoint state. Hold a target until applied rather
    // than chasing a moving commit position forever under continuous load.
    private long target = -1;
    private long targetObservedNs;
    private long verified = -1;
    private long verifiedObservedNs;

    private record Evidence(long source, long term, long elections, Role role,
                            long applied, long commit, long checkpoint, long checkpointObservedNs,
                            long observedNs, long consensusAgeMs, String recovery) { }

    /** One response including status and diagnostic body from the same evidence. */
    public record Probe(boolean live, boolean ready, String detail) { }

    public NodeReadiness() {
        this(DEFAULT_LIVENESS_STALE_MS, DEFAULT_READINESS_STALE_MS);
    }

    public NodeReadiness(final long livenessStaleMs, final long readinessStaleMs) {
        this(livenessStaleMs, readinessStaleMs, System::nanoTime);
    }

    NodeReadiness(final long livenessStaleMs, final long readinessStaleMs, final LongSupplier clock) {
        if (readinessStaleMs <= 0 || livenessStaleMs < readinessStaleMs) {
            throw new IllegalArgumentException("invalid freshness bounds");
        }
        this.livenessStaleNs = TimeUnit.MILLISECONDS.toNanos(livenessStaleMs);
        this.readinessStaleNs = TimeUnit.MILLISECONDS.toNanos(readinessStaleMs);
        this.clock = clock;
    }

    /** Called even when idle; a lack of business events is not a liveness failure. */
    public void tick() { lastTickNs = clock.getAsLong(); ticked = true; }
    public void started() { started = true; catchingUp(); tick(); }

    /** Compatibility only: a boolean cannot supply missing application evidence. */
    @Deprecated public void catchUpComplete() { }
    public void catchingUp() { target = verified = -1; evidence = null; }

    public void roleChanged(final Role newRole) {
        if (newRole != role) { catchingUp(); role = newRole; }
    }

    public void stopping() { stopping = true; }

    /** Terminal for this process incarnation; restarting does not itself repair state. */
    public void needsReseed(final String reason) {
        terminalReason = reason == null ? "unspecified" : reason;
    }

    /** Withdraw evidence when its source disappears, closes or changes during a read. */
    public void unavailable(final String reason) {
        target = verified = -1;
        evidence = new Evidence(-1, -1, -1, role, -1, -1, -1, 0,
                clock.getAsLong(), -1, reason);
    }

    /**
     * Observe one consensus incarnation/term, with applied and committed positions
     * in the SAME cluster log byte space. Consensus age comes from the consensus
     * agent heartbeat, not from movement of the commit counter.
     */
    public void observe(final long source, final long term, final long elections, final Role observedRole,
                        final long applied, final long commit, final boolean electionClosed,
                        final boolean active, final long consensusAgeMs) {
        roleChanged(observedRole);
        final long now = clock.getAsLong();
        final Evidence previous = evidence;
        if (previous == null || previous.source != source || previous.term != term
                || previous.elections != elections || previous.applied > applied || previous.commit > commit) {
            target = verified = -1;
        }
        String recovery = "observing";
        if (!electionClosed || (observedRole != Role.FOLLOWER && observedRole != Role.LEADER)) {
            recovery = "election";
        } else if (!active) {
            recovery = "consensus-inactive";
        } else if (source < 0 || term < 0 || elections < 0 || applied < 0 || commit < 0) {
            recovery = "unknown-position";
        } else if (consensusAgeMs < 0 || TimeUnit.MILLISECONDS.toNanos(consensusAgeMs) >= readinessStaleNs) {
            recovery = "stale-consensus";
        }
        if (!recovery.equals("observing")) {
            target = verified = -1;
        } else {
            if (target < 0) { target = commit; targetObservedNs = now; }
            if (applied >= target) {
                verified = target;
                verifiedObservedNs = targetObservedNs;
                target = commit;
                targetObservedNs = now;
                // Idle and already caught up: the same position is fresh evidence.
                if (applied >= commit) { verified = commit; verifiedObservedNs = now; }
            }
            recovery = verified < 0 ? "catching-up" : "observed";
        }
        evidence = new Evidence(source, term, elections, observedRole, applied, commit,
                verified, verifiedObservedNs, now, consensusAgeMs, recovery);
    }

    public boolean live() { return ticked && age(clock.getAsLong(), lastTickNs) < livenessStaleNs; }
    public boolean ready() { return reason(evidence, clock.getAsLong()).equals("ready"); }
    public String describe() { return probe().detail; }

    public Probe probe() {
        final Evidence e = evidence;
        final long now = clock.getAsLong();
        final String reason = reason(e, now);
        final String detail = "role=" + (e == null ? role : e.role) + " recovery=" + reason
                + " source=" + (e == null ? -1 : e.source)
                + " term=" + (e == null ? -1 : e.term)
                + " elections=" + (e == null ? -1 : e.elections)
                + " applied=" + (e == null ? -1 : e.applied)
                + " commit=" + (e == null ? -1 : e.commit)
                + " checkpoint=" + (e == null ? -1 : e.checkpoint)
                + " observationAgeMs=" + (e == null ? -1 : TimeUnit.NANOSECONDS.toMillis(age(now, e.observedNs)))
                + " consensusAgeMs=" + (e == null ? -1 : e.consensusAgeMs)
                + " maxLagBytes=" + MAX_APPLY_LAG_BYTES
                + " maxCheckpointAgeMs=" + MAX_CHECKPOINT_AGE_MS;
        return new Probe(ticked && age(now, lastTickNs) < livenessStaleNs, reason.equals("ready"), detail);
    }

    private String reason(final Evidence e, final long now) {
        if (stopping) { return "stopping"; }
        if (terminalReason != null) { return "needs-reseed:" + terminalReason; }
        if (!started) { return "starting"; }
        if (!ticked || age(now, lastTickNs) >= readinessStaleNs) { return "stale-duty-cycle"; }
        if (e == null || e.role != role) { return "evidence-missing"; }
        if (age(now, e.observedNs) >= readinessStaleNs) { return "stale-observation"; }
        if (!e.recovery.equals("observed")) { return e.recovery; }
        if (e.commit > e.applied && e.commit - e.applied > MAX_APPLY_LAG_BYTES) { return "apply-lag"; }
        if (e.checkpoint < 0 || age(now, e.checkpointObservedNs)
                >= TimeUnit.MILLISECONDS.toNanos(MAX_CHECKPOINT_AGE_MS)) { return "stale-checkpoint"; }
        if (TimeUnit.MILLISECONDS.toNanos(e.consensusAgeMs) + age(now, e.observedNs)
                >= readinessStaleNs) { return "stale-consensus"; }
        return "ready";
    }

    private static long age(final long now, final long then) {
        final long elapsed = now - then;
        return elapsed < 0 ? Long.MAX_VALUE : elapsed;
    }
}
