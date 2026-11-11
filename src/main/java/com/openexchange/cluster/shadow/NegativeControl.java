// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/**
 * The rules the shadow negative controls switch off, one at a time, through package-private constructors only:
 * every public constructor and method runs with none of them. A kit test in this package passes a set; nothing outside
 * the package can reach it. Each control must make its test fail with the named text (C6).
 */
enum NegativeControl {
    /** Completeness by the length equation instead of the trailer rule (3.3 (3)). */
    TRAILER_RULE_REPLACED_BY_LENGTH_EQUATION,
    /** One logical shadow per recording instead of per position (6.2 step 4). */
    DEDUPE_BY_RECORDING,
    /** A disagreeing position keeps its newest recording usable instead of excluding every copy. */
    CONFLICT_NEWEST_WINS,
    /** The group key includes producer and kind, so cross-producer disagreement is two shadows. */
    GROUP_KEY_INCLUDES_PRODUCER_AND_KIND,
    /** The structural cache is keyed by recording id only, not by the descriptor. */
    STRUCTURAL_CACHE_BY_RECORDING_ID,
    /** A CORRUPT verdict is ignored by the next scan. */
    CORRUPT_CLEARED_BY_SCAN,
    /** {@code INVALID_POSITION} on the trailer window is mapped to SCAN_ERROR instead of NO_TRAILER. */
    INVALID_POSITION_AS_SCAN_ERROR,
    /** {@code purge} decides on the last scan instead of a fresh one. */
    PURGE_ON_STALE_SCAN,
    /** The self-check runs after the first offer. */
    SELF_CHECK_AFTER_FIRST_OFFER,
    /** No one-shadow-per-position-per-producer check (W2). */
    MONOTONE_CHECK_OFF,
    /** A cursor with unknown fields is accepted (W0). */
    CURSOR_CHECK_OFF,
    /** An {@code ArchiveException} at W6 is relabelled UNEXPECTED. */
    ARCHIVE_EXCEPTION_RELABELLED,
    /** The writer accepts {@code genesisId <= 0} (W1). */
    GENESIS_CHECK_OFF,
    /** The writer computes the payload digest with CRC32C again (the pre-amendment format). */
    DIGEST_AS_CRC32C,
    /** The floor precondition off: every newest group is read as a full image, so a stub is pruned to P_cm. */
    FLOOR_PRECONDITION_OFF,
    /** The floor ignores {@code SHADOW_FLOOR_MARGIN} and prunes to P_cm. */
    FLOOR_MARGIN_IGNORED,
    /** The margin shadow is taken over all VERIFIED, not over the restorable ones. */
    FLOOR_MARGIN_OVER_ALL_VERIFIED,
    /** Covering is the newest VERIFIED {@code >= P_cm} only ({@code verifiedAtOrAbove}). */
    FLOOR_NEWEST_COVERING_ONLY,
    /** The floor reads usable (COMPLETE) shadows instead of VERIFIED ones. */
    FLOOR_READS_USABLE,
    /**
     * Every skip-safe VERIFIED shadow above P_cm counts as restorable, not only the one SKIP tries
     * ({@code newestAtOrAbove(P_cm + 1)}).
     */
    FLOOR_SKIP_ANY_ABOVE,
    /** Retention purges CONFLICT copies like usable ones (deletion picks a winner). */
    RETENTION_PURGES_CONFLICT,
    /** Retention does not pin the floor's anchor position (the witnessed pair is purged like any other). */
    RETENTION_ANCHOR_PIN_OFF,
    /** The floor's anchor rule is read as any-verified whatever the setting says. */
    ANCHOR_RULE_IGNORED,
    /** Witnesses are counted per recording instead of per distinct producer. */
    WITNESS_BY_RECORDING,
    /** Copies of any kind witness an anchor, not only ALIGNED ones. */
    ANCHOR_ANY_KIND,
    /** Puller: a copy that ended without SYNC and short of the source is judged for content. */
    INCOMPLETE_AS_CONTENT,
    /** Puller: an ACTIVE source recording (stop position NULL) is a candidate. */
    PULL_ACTIVE_ALLOWED,
    /** Puller: an archive refusal is counted as a corrupt copy and the source refused for the process. */
    PULL_RETRY_AS_CONTENT,
    /** Puller: a content refusal is retried with backoff instead of refusing the source recording. */
    PULL_CONTENT_AS_RETRY,
    /** Puller: the leader gate is off (the leader lists and pulls). */
    LEADER_GATE_OFF,
    /** Puller: the leader gate skips new pulls but lets in-flight ones finish. */
    LEADER_FINISHES_INFLIGHT,
    /** Puller: discovery dedupes by producer and seq instead of the full alias. */
    DEDUPE_BY_SEQ,
    /** Puller: discovery dedupes against every local copy, evidence states included. */
    DEDUPE_AGAINST_EVIDENCE,
    /** Puller: every kind is a candidate whatever {@code SHADOW_PULL_KINDS} says. */
    PULL_KINDS_ANY,
    /**
     * Puller: steady-state discovery skips every position at or below the newest VERIFIED one, a second producer's
     * ALIGNED at a held position included (the second witness is never pulled).
     */
    WITNESS_PULL_OFF
}
