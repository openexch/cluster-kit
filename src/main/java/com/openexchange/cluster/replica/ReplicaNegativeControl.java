// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

/**
 * One rule switched off per negative control. Refused without {@value ReplicaFaults#PROPERTY}.
 * Each is used by a test whose failure text proves the rule is what the test sees.
 */
public enum ReplicaNegativeControl {
    /** Apply fence: an apply failure is swallowed; the reader restarts at the last consumed position. */
    FENCE_OFF,
    /** Triggers: the trigger listener ignores flags 1 (no STANDBY shadow). */
    TRIGGER_IGNORES_STANDBY,
    /** Readiness computed from {@code state == BACKING_UP} (a rule that flaps at every backup query). */
    READINESS_FROM_BACKUP_STATE,
    /** The LOCAL fallback decided on the wall clock instead of cluster time. */
    LOCAL_ON_WALL_CLOCK,
    /** Bootstrap: genesis without checking the log recording starts at 0. */
    GENESIS_PRECONDITION_OFF,
    /** Bootstrap: the post-restore {@code matches} cross-check skipped. */
    MATCHES_OFF,
    /** Bootstrap: a failed restore is followed by another restore attempt in the same process. */
    TWO_RESTORES_ALLOWED,
    /** Housekeeping: the log purge ignores the floor's previous-verified margin (purges to the newest VERIFIED). */
    FLOOR_MARGIN_IGNORED,
    /** Housekeeping: the retrieved member snapshot set trim does nothing. */
    SET_TRIM_OFF,
    /** Readiness: the dropped idle branch is back; a caught-up replica of a frozen log stays ready. */
    TIME_LAG_IDLE_ALLOWED,
    /**
     * Genesis: the cluster genesis and term checks before the first attach are skipped, and the alignment compare
     * calls a differing member copy DIFFERENT without decoding its genesis (another history reads as a byte mismatch).
     */
    GENESIS_CHECK_OFF,
    /**
     * The byte-equal alignment trust is off. A start that is not a full replay from genesis is trusted at once, and
     * an ALIGNED shadow is written without comparing it with the member snapshot (the rule before the alignment trust).
     */
    ALIGN_COMPARE_OFF,
    /** Apply: the follower drops every session open frame before the engine sees it (a replica-side divergence). */
    SKIP_SESSION_OPEN,
    /**
     * Freshness: STANDBY and LOCAL are written while a named member snapshot above the last EQUAL position is still
     * uncompared (only {@code trusted} is required, the rule before the freshness check).
     */
    STANDBY_FRESHNESS_OFF,
    /** Compare: a member copy is compared without opening its envelope first (a torn copy reads as a mismatch). */
    ENVELOPE_CHECK_OFF,
    /** Mismatch: a byte mismatch exits 1 at once instead of holding (the rule before the hold). */
    HOLD_OFF,
    /** History: the applied-past check runs only while UNVERIFIED (the rule before it ran in both trust states). */
    TICK_UNVERIFIED_ONLY,
    /** Housekeeping: the retrieved member set the oracle disagreed with is not pinned (trimmed as any other set). */
    MISMATCH_PIN_OFF,
    /** History fence: own shadows above the purge bound are kept (no suspect purge before the exit). */
    SUSPECT_PURGE_OFF,
    /** History fence: the suspect purge is requested but not awaited (the exit races it). */
    SUSPECT_PURGE_NOT_AWAITED
}
