// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.cluster.ClusterControl;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.concurrent.TimeUnit;

/**
 * What makes the cluster snapshot itself, with nobody watching it.
 *
 * <p>Aeron gives a clustered service {@code onTakeSnapshot} and nothing that
 * calls it. Until this class existed the rhythm lived in an external process
 * (the admin gateway's Go scheduler), which is why an engine downloaded on its
 * own never snapshotted, never truncated its log, and eventually filled its
 * disk. On 2026-07-25 that is exactly what happened to the core engine and
 * the money path stopped for seventeen hours. Durability cannot depend on a
 * daemon somebody else has to run.</p>
 *
 * <h2>The decision is the leader's, the snapshot is the cluster's</h2>
 *
 * <p>Every member evaluates nothing; only the leader does. Aeron's consensus
 * module reads the control toggle <b>only</b> while {@code role == LEADER},
 * {@code election == null} and the module is ACTIVE (verified in the bytecode
 * of {@code ConsensusModuleAgent.slowTickWork} on 1.51.0 and 1.52.2) - a
 * follower that flips it changes nothing and leaves a stale toggle behind. And
 * that is the right shape anyway: the toggle appends a SNAPSHOT action to the
 * log, so every member snapshots at the SAME log position. A per-node byte
 * counter deciding for itself would scatter snapshots across positions and give
 * the cluster no common point to recover from.</p>
 *
 * <h2>Why bytes first, and a timer underneath</h2>
 *
 * <p>A fixed interval silently caps throughput: between two snapshots the whole
 * log has to fit in tmpfs, and at 400k orders/s five minutes is ~13.5 GB
 * against a 7.8 GB /dev/shm. The byte trigger is what holds the
 * ledger-stays-in-RAM decision at ANY rate. The timer is only a floor for quiet
 * markets, so a slow day still leaves a recent restore point.</p>
 *
 * <h2>What this is NOT</h2>
 *
 * <p>Not replicated state. The threshold is read from this node's environment
 * and the timer runs off this node's monotonic clock, which would be a
 * violation if either could change what the state machine computes. Neither
 * can: the decision only flips a counter, and the resulting snapshot enters the
 * log as an ordered cluster action. Two members configured differently produce
 * a different snapshot RHYTHM after a leader change, never a different ledger.</p>
 *
 * <h2>The standby schedule</h2>
 *
 * <p>A second, independent rhythm for the shadow replica: every {@code SNAPSHOT_STANDBY_INTERVAL_MINUTES} the leader
 * toggles {@code STANDBY_SNAPSHOT}. The consensus module appends one {@code SNAPSHOT} action with flags 1 and returns
 * the toggle to NEUTRAL (CMA:2575-2581); members ignore flags 1 (CSA:1094-1097), so no member snapshots and the leader
 * pays one action frame. Completion is the toggle reading NEUTRAL again, never {@code onTakeSnapshot} (members do not
 * take one). A member snapshot does NOT reset the standby clock: the replica's STANDBY shadows come
 * every interval whatever the member cadence does.</p>
 *
 * <p>Lost-request accounting: a request whose toggle reads INACTIVE (a demoted leader's toggle, CMA:1382), one still held
 * after {@link #REQUEST_STALE_NS}, or one in flight when this node stops leading, can never be appended: counted
 * {@code standby_toggle_lost_total} and cleared, so the schedule continues. A SNAPSHOT (or another operator action)
 * seen while the standby was in flight means the CM reset it and someone toggled again within the evaluation: that is
 * {@code standby_toggle_ambiguous_total}, not lost. A due standby that cannot take the toggle is
 * {@code standby_toggle_deferred_total} (once per due standby) and is retried on the next evaluation.</p>
 */
public final class SnapshotCadence {

    /** 1 GiB of cluster log since the last snapshot. Same number the gateway used. */
    public static final long DEFAULT_LOG_BYTE_THRESHOLD = 1L << 30;

    /** Quiet-market floor. Same five minutes the gateway used. */
    public static final long DEFAULT_INTERVAL_MINUTES = 5;

    /** One STANDBY action every 15 minutes; 0 disables. */
    public static final long DEFAULT_STANDBY_INTERVAL_MINUTES = 15;

    /** {@code SNAPSHOT_STANDBY_INTERVAL_MINUTES} upper bound: one day. */
    public static final long MAX_STANDBY_INTERVAL_MINUTES = 1440;

    /** Package seam for the standby schedule negative controls; production passes none. */
    enum Control {
        /** NC: lost-request counting off (a lost request is cleared but not counted). */
        LOST_COUNTING_OFF,
        /** NC: the standby schedule toggles SNAPSHOT (every member would snapshot). */
        STANDBY_USES_SNAPSHOT
    }

    /**
     * How often the trigger is evaluated. {@code doBackgroundWork} runs on every
     * duty cycle - millions of times a second - so everything below this gate
     * has to stay off the hot path.
     */
    private static final long EVALUATE_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * A snapshot request that has not come back after this long is a fault, not
     * a slow snapshot: the log keeps growing while nothing is being written.
     */
    private static final long REQUEST_STALE_NS = TimeUnit.MINUTES.toNanos(2);

    private final long byteThreshold;
    private final long intervalNs;
    private final long standbyIntervalNs;
    private final java.util.Set<Control> off;

    private java.util.function.Supplier<AtomicCounter> toggleResolver = () -> null;
    private AtomicCounter controlToggle;
    // Volatile because /metrics reads them from the HTTP thread while the duty
    // cycle writes them. Both are single-writer: a torn long here would only
    // misreport a gauge, but a scrape must never reach into Cluster itself,
    // which is scoped to the service thread.
    private volatile long lastSnapshotPosition;
    private volatile long observedLogPosition;
    private volatile long requestCount;
    private long lastSnapshotNs;
    private long lastEvaluatedNs;
    private long requestedAtNs;
    private boolean requestInFlight;
    private long staleWarnings;

    // Standby schedule: single writer (the duty cycle), volatile for the /metrics scrape.
    private long lastStandbyNs;
    private long standbyRequestedAtNs;
    private volatile boolean standbyInFlight;
    private volatile long standbyRequests;
    private volatile long standbyCompleted;
    private volatile long standbyDeferred;
    /** The current due standby was already counted deferred; cleared when it is toggled or dropped. */
    private boolean standbyDeferPending;
    private volatile long standbyLost;
    private volatile long standbyAmbiguous;

    /**
     * @param byteThreshold cluster-log bytes since the last snapshot that force a new
     *                      one, or 0 to disable the byte trigger
     * @param intervalNs    quiet-market floor, or 0 to disable the timer
     */
    public SnapshotCadence(final long byteThreshold, final long intervalNs) {
        this(byteThreshold, intervalNs, 0);
    }

    /**
     * @param standbyIntervalNs the standby schedule, or 0 to disable it (the two-argument constructor)
     */
    public SnapshotCadence(final long byteThreshold, final long intervalNs, final long standbyIntervalNs) {
        this(byteThreshold, intervalNs, standbyIntervalNs, java.util.EnumSet.noneOf(Control.class));
    }

    SnapshotCadence(final long byteThreshold, final long intervalNs, final long standbyIntervalNs,
                    final java.util.Set<Control> off) {
        if (byteThreshold < 0 || intervalNs < 0 || standbyIntervalNs < 0) {
            throw new IllegalArgumentException(
                "negative snapshot cadence: byteThreshold=" + byteThreshold + " intervalNs=" + intervalNs
                    + " standbyIntervalNs=" + standbyIntervalNs);
        }
        this.byteThreshold = byteThreshold;
        this.intervalNs = intervalNs;
        this.standbyIntervalNs = standbyIntervalNs;
        this.off = off.isEmpty() ? java.util.EnumSet.noneOf(Control.class) : java.util.EnumSet.copyOf(off);
    }

    /**
     * Build from the environment, refusing anything it cannot read.
     *
     * <p>{@code SNAPSHOT_LOG_BYTES} and {@code SNAPSHOT_INTERVAL_MINUTES}, both
     * defaulted to what the gateway ran with, each also readable as a system
     * property ({@code snapshot.log.bytes}, {@code snapshot.interval.minutes})
     * the way the engines already treat {@code CLUSTER_NODE} / {@code node.id}.
     * An unparseable value throws rather than falling back: a typo that silently
     * restores the default is a cluster that looks configured and is not, and
     * this is the setting whose absence costs the ledger.</p>
     */
    public static SnapshotCadence fromEnv() {
        final long bytes = longFromEnv("SNAPSHOT_LOG_BYTES", "snapshot.log.bytes",
            DEFAULT_LOG_BYTE_THRESHOLD);
        final long minutes = longFromEnv("SNAPSHOT_INTERVAL_MINUTES", "snapshot.interval.minutes",
            DEFAULT_INTERVAL_MINUTES);
        final long standbyMinutes = longFromEnv("SNAPSHOT_STANDBY_INTERVAL_MINUTES",
            "snapshot.standby.interval.minutes", DEFAULT_STANDBY_INTERVAL_MINUTES);
        if (standbyMinutes > MAX_STANDBY_INTERVAL_MINUTES) {
            throw new IllegalArgumentException("SNAPSHOT_STANDBY_INTERVAL_MINUTES / snapshot.standby.interval.minutes="
                + standbyMinutes + " out of range [0," + MAX_STANDBY_INTERVAL_MINUTES + "]");
        }
        if (bytes == 0 && minutes == 0) {
            System.out.println("[CADENCE] WARNING: both SNAPSHOT_LOG_BYTES and "
                + "SNAPSHOT_INTERVAL_MINUTES are 0 - this cluster will NEVER snapshot on its own. "
                + "Its log grows without bound and the disk fills. Only do this when something "
                + "else is taking snapshots.");
        }
        return new SnapshotCadence(bytes, TimeUnit.MINUTES.toNanos(minutes),
            TimeUnit.MINUTES.toNanos(standbyMinutes));
    }

    static long longFromEnv(final String envName, final String propName, final long fallback) {
        String raw = System.getenv(envName);
        String source = envName;
        if (raw == null || raw.isBlank()) {
            raw = System.getProperty(propName);
            source = propName;
        }
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        final long parsed;
        try {
            parsed = Long.parseLong(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(source + "=" + raw + " is not a number", e);
        }
        if (parsed < 0) {
            throw new IllegalArgumentException(source + "=" + raw + " must not be negative");
        }
        return parsed;
    }

    /**
     * A boolean switch read the same way: environment first, then system
     * property. Anything other than "true"/"false" is refused rather than
     * quietly read as false, which is the reading that turns durability off.
     */
    public static boolean booleanFromEnv(final String envName, final String propName,
                                         final boolean fallback) {
        String raw = System.getenv(envName);
        String source = envName;
        if (raw == null || raw.isBlank()) {
            raw = System.getProperty(propName);
            source = propName;
        }
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        final String value = raw.trim();
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(source + "=" + raw + " must be true or false");
    }

    /**
     * Find the consensus module's control toggle through this node's own Aeron
     * client. Same counter {@code ClusterTool snapshot} writes; no second process,
     * no cluster directory to locate.
     *
     * @return the toggle, or null when the consensus module has not published one
     *         yet (the caller must treat that as "cannot snapshot", never as "fine")
     */
    public static AtomicCounter findControlToggle(final Aeron aeron, final int clusterId) {
        return ClusterControl.findControlToggle(aeron.countersReader(), clusterId);
    }

    /**
     * Arm the cadence once the service is running.
     *
     * <p>The toggle arrives as a resolver rather than a counter because the
     * consensus module may not have published it when the service starts, and a
     * node that resolved null once must not spend the rest of its life unable to
     * snapshot. It is retried, quietly, until it appears.</p>
     *
     * @param toggleResolver how to find the consensus module's control toggle
     * @param logPosition    where the log is now: the recovery position at startup,
     *                       which is the last snapshot's position on a node that
     *                       recovered from one, and 0 at genesis
     * @param nowNs          monotonic now
     */
    public void bind(final java.util.function.Supplier<AtomicCounter> toggleResolver,
                     final long logPosition, final long nowNs) {
        this.toggleResolver = toggleResolver;
        this.lastSnapshotPosition = logPosition;
        // Seed the CLOCK, never a success. A node that restarts must not claim a
        // snapshot it never took, and must not fire one on every boot either; a
        // busy cluster still fires at once, on bytes, which is the case that matters.
        this.lastSnapshotNs = nowNs;
        this.lastEvaluatedNs = nowNs;
        this.lastStandbyNs = nowNs;
        System.out.println("[CADENCE] armed: byteThreshold=" + byteThreshold
            + " intervalMinutes=" + TimeUnit.NANOSECONDS.toMinutes(intervalNs)
            + " standbyIntervalMinutes=" + TimeUnit.NANOSECONDS.toMinutes(standbyIntervalNs)
            + " fromLogPosition=" + logPosition);
    }

    /**
     * The toggle, resolving it if this is the first time it is needed. Null means
     * the consensus module has not published one, which counts as "cannot
     * snapshot" - never as "no need to".
     */
    private AtomicCounter toggle() {
        if (controlToggle == null) {
            controlToggle = toggleResolver.get();
            if (controlToggle != null) {
                System.out.println("[CADENCE] control toggle found (counterId="
                    + controlToggle.id() + ")");
            }
        }
        return controlToggle;
    }

    /**
     * Called from {@code doBackgroundWork} on every duty cycle. Does nothing at
     * all more than once a second, and nothing ever on a follower.
     *
     * @param isLeader    whether this member is the leader right now
     * @param logPosition the service's current log position
     * @return 1 when a snapshot was requested, else 0 (the duty cycle's work count)
     */
    public int tick(final long nowNs, final boolean isLeader, final long logPosition) {
        if (nowNs - lastEvaluatedNs < EVALUATE_INTERVAL_NS) {
            return 0;
        }
        lastEvaluatedNs = nowNs;
        observedLogPosition = logPosition;

        if (standbyInFlight) {
            checkStandbyInFlight(nowNs);
        }
        if (requestInFlight) {
            checkInFlight(nowNs);
            if (isLeader && standbyDue(nowNs) && !standbyInFlight) {
                // the member snapshot holds the toggle: the standby waits for the next evaluation
                deferStandby();
            }
            return 0;
        }
        if (!isLeader || toggle() == null) {
            return 0;
        }

        final String reason = dueReason(nowNs, logPosition);
        if (reason != null) {
            return request(nowNs, reason) ? 1 : 0;
        }
        if (!standbyInFlight && standbyDue(nowNs)) {
            requestStandby(nowNs);
        }
        return 0;
    }

    private boolean standbyDue(final long nowNs) {
        return standbyIntervalNs > 0 && nowNs - lastStandbyNs >= standbyIntervalNs;
    }

    /**
     * Toggle STANDBY_SNAPSHOT (CAS from NEUTRAL, CCTL:125). A failed CAS means a SNAPSHOT or an operator holds the
     * toggle: deferred, retried on the next evaluation. Not a duty-cycle work count: no member snapshot follows.
     */
    private void requestStandby(final long nowNs) {
        final ClusterControl.ToggleState wanted = off.contains(Control.STANDBY_USES_SNAPSHOT)
            ? ClusterControl.ToggleState.SNAPSHOT : ClusterControl.ToggleState.STANDBY_SNAPSHOT;
        if (!wanted.toggle(controlToggle)) {
            deferStandby();
            return;
        }
        standbyDeferPending = false;
        standbyInFlight = true;
        standbyRequestedAtNs = nowNs;
        standbyRequests++;
        System.out.println("[CADENCE] standby snapshot requested #" + standbyRequests + " ("
            + TimeUnit.NANOSECONDS.toSeconds(nowNs - lastStandbyNs) + "s since the last; interval "
            + TimeUnit.NANOSECONDS.toMinutes(standbyIntervalNs) + "m)");
    }

    /** One count per due standby that found the toggle held, however many evaluations it waits. */
    private void deferStandby() {
        if (!standbyDeferPending) {
            standbyDeferPending = true;
            standbyDeferred++;
        }
    }

    /** Standby completion and lost-request accounting: NEUTRAL completes; INACTIVE or stale is lost; any other action is ambiguous. */
    private void checkStandbyInFlight(final long nowNs) {
        final ClusterControl.ToggleState state = controlToggle == null
            ? null : ClusterControl.ToggleState.get(controlToggle);
        if (state == ClusterControl.ToggleState.NEUTRAL) {
            standbyInFlight = false;
            standbyCompleted++;
            lastStandbyNs = nowNs;
            return;
        }
        if (state == ClusterControl.ToggleState.STANDBY_SNAPSHOT) {
            if (nowNs - standbyRequestedAtNs >= REQUEST_STALE_NS) {
                lost(nowNs, "still STANDBY_SNAPSHOT after "
                    + TimeUnit.NANOSECONDS.toSeconds(REQUEST_STALE_NS) + "s (stale)");
            }
            return;
        }
        if (state == null || state == ClusterControl.ToggleState.INACTIVE) {
            lost(nowNs, "toggle " + state + " (the request can never be appended)");
            return;
        }
        // The CM appended and reset, and an operator toggled again within the evaluation
        standbyInFlight = false;
        standbyAmbiguous++;
        lastStandbyNs = nowNs;
        System.out.println("[CADENCE] standby snapshot request ambiguous: toggle reads " + state
            + " while the standby was in flight (completed, then another action was toggled)");
    }

    private void lost(final long nowNs, final String why) {
        standbyInFlight = false;
        lastStandbyNs = nowNs;
        if (!off.contains(Control.LOST_COUNTING_OFF)) {
            standbyLost++;
        }
        System.err.println("[CADENCE] CRITICAL: standby snapshot request lost: " + why
            + " (standby_toggle_lost_total=" + standbyLost + "); the replica's LOCAL fallback covers the gap");
    }

    /**
     * Why a snapshot is due, or null.
     *
     * <p>Bytes are reported first even when both triggers agree: "the log grew
     * past the threshold" and "five minutes passed" are different operational
     * facts, and hiding the first behind the second is how a cluster whose
     * throughput has climbed looks like one merely ticking over.</p>
     */
    private String dueReason(final long nowNs, final long logPosition) {
        final long grown = logPosition - lastSnapshotPosition;
        if (byteThreshold > 0 && grown >= byteThreshold) {
            return "log grew " + grown + " bytes since the last snapshot (threshold " + byteThreshold + ")";
        }
        if (intervalNs > 0 && nowNs - lastSnapshotNs >= intervalNs) {
            return TimeUnit.NANOSECONDS.toSeconds(nowNs - lastSnapshotNs)
                + "s since the last snapshot (interval "
                + TimeUnit.NANOSECONDS.toMinutes(intervalNs) + "m)";
        }
        return null;
    }

    /**
     * Flip the toggle. The CAS from NEUTRAL is what makes this safe against a
     * concurrent {@code ClusterTool snapshot}: whoever loses simply does not
     * request a second one.
     */
    private boolean request(final long nowNs, final String reason) {
        if (!ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle)) {
            // Not NEUTRAL: either someone else is mid-request or the module is
            // not accepting control actions. Wait for the next evaluation.
            System.out.println("[CADENCE] snapshot due (" + reason + ") but the control toggle is "
                + ClusterControl.ToggleState.get(controlToggle) + " - deferring");
            return false;
        }
        requestInFlight = true;
        requestedAtNs = nowNs;
        requestCount++;
        System.out.println("[CADENCE] snapshot requested #" + requestCount + ": " + reason);
        return true;
    }

    /**
     * Watch a request that has not completed.
     *
     * <p>The consensus module returns the toggle to NEUTRAL when the snapshot
     * finishes, so a toggle still reading SNAPSHOT after two minutes means the
     * request was accepted and the snapshot is not happening. The counter is NOT
     * reset from here: fighting the consensus module for its own toggle would
     * turn a stall into a corruption. It is reported, loudly, every two minutes -
     * a cluster that stops snapshotting must never do it quietly again.</p>
     */
    private void checkInFlight(final long nowNs) {
        final ClusterControl.ToggleState state = controlToggle == null
            ? null : ClusterControl.ToggleState.get(controlToggle);
        if (state == ClusterControl.ToggleState.NEUTRAL) {
            // Accepted and finished. onTakeSnapshot normally clears the request
            // before this is reached; this covers the case where it did not
            // (another service's snapshot, or a role change mid-request).
            requestInFlight = false;
            return;
        }
        if (nowNs - requestedAtNs >= REQUEST_STALE_NS) {
            staleWarnings++;
            requestedAtNs = nowNs;
            System.err.println("[CADENCE] CRITICAL: snapshot requested "
                + TimeUnit.NANOSECONDS.toSeconds(REQUEST_STALE_NS * staleWarnings)
                + "s ago has not completed (toggle=" + state + ") - the cluster log is growing "
                + "and no restore point is being written");
        }
    }

    /**
     * A snapshot was taken at {@code logPosition}. Called on EVERY member from
     * {@code onTakeSnapshot}, whatever asked for it, so the baseline follows the
     * cluster rather than this node's own requests. The standby clock is NOT reset: the
     * replica's STANDBY shadows keep their own rhythm whatever the member cadence does.
     */
    public void snapshotTaken(final long logPosition, final long nowNs) {
        lastSnapshotPosition = logPosition;
        lastSnapshotNs = nowNs;
        requestInFlight = false;
    }

    /**
     * Role changed. A member that stops being leader drops any request it was
     * waiting on: the new leader owns the decision, and this node's toggle is
     * dead paper until it leads again.
     */
    public void roleChanged(final boolean isLeader) {
        if (!isLeader) {
            requestInFlight = false;
            standbyDeferPending = false; // the due standby is the next leader's; a new leadership counts afresh
            if (standbyInFlight) {
                // Lost: a follower's toggle is deactivated (CMA:1382) and re-activated NEUTRAL on the next
                // leadership (CMA:1913): this request can never be appended
                lost(lastEvaluatedNs, "role changed to follower with the request in flight");
            }
        }
    }

    /** Log bytes accumulated since the last snapshot, for tests. */
    public long logBytesSinceSnapshot(final long logPosition) {
        return logPosition - lastSnapshotPosition;
    }

    /**
     * Log bytes accumulated since the last snapshot, as of the last evaluation.
     *
     * <p>Safe to scrape from another thread, which the argument-taking version is
     * not: reading it needs the log position, and that lives in {@code Cluster},
     * which belongs to the service thread alone.</p>
     */
    public long logBytesSinceSnapshot() {
        return observedLogPosition - lastSnapshotPosition;
    }

    /** How many snapshots this node has requested while leading. */
    public long requestCount() {
        return requestCount;
    }

    /** Whether a request is outstanding. */
    public boolean requestInFlight() {
        return requestInFlight;
    }

    /** The standby schedule, 0 when disabled. */
    public long standbyIntervalNs() {
        return standbyIntervalNs;
    }

    /** {@code standby_requests_total}: STANDBY_SNAPSHOT toggles this node made while leading. */
    public long standbyRequestCount() {
        return standbyRequests;
    }

    /** Standby requests the CM completed (the toggle read NEUTRAL again). */
    public long standbyCompletedCount() {
        return standbyCompleted;
    }

    /** {@code standby_toggle_deferred_total}: due standbys that could not take the toggle, each counted once. */
    public long standbyDeferredCount() {
        return standbyDeferred;
    }

    /** {@code standby_toggle_lost_total}. */
    public long standbyLostCount() {
        return standbyLost;
    }

    /** {@code standby_toggle_ambiguous_total}. */
    public long standbyAmbiguousCount() {
        return standbyAmbiguous;
    }

    /** Whether a standby request is outstanding. */
    public boolean standbyInFlight() {
        return standbyInFlight;
    }
}
