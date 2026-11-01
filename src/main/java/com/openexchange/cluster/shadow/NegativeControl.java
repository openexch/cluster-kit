// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/**
 * The rules the RCS S3 negative controls (11.9) switch off, one at a time, through package-private constructors only:
 * every public constructor and method runs with none of them. A kit test in this package passes a set; nothing outside
 * the package can reach it. Each control must make its test fail with the named text (C6).
 */
enum NegativeControl {
    /** NC-3: completeness by the R 2.5 length equation instead of the trailer rule (3.3 (3)). */
    TRAILER_RULE_REPLACED_BY_LENGTH_EQUATION,
    /** NC-5: one logical shadow per recording instead of per position (6.2 step 4). */
    DEDUPE_BY_RECORDING,
    /** NC-6a: a disagreeing position keeps its newest recording usable instead of excluding every copy. */
    CONFLICT_NEWEST_WINS,
    /** NC-6b: the group key includes producer and kind, so cross-producer disagreement is two shadows. */
    GROUP_KEY_INCLUDES_PRODUCER_AND_KIND,
    /** NC-13: the structural cache is keyed by recording id only, not by the descriptor. */
    STRUCTURAL_CACHE_BY_RECORDING_ID,
    /** NC-16: a CORRUPT verdict is ignored by the next scan. */
    CORRUPT_CLEARED_BY_SCAN,
    /** NC-19: {@code INVALID_POSITION} on the trailer window is mapped to SCAN_ERROR instead of NO_TRAILER. */
    INVALID_POSITION_AS_SCAN_ERROR,
    /** NC-20: {@code purge} decides on the last scan instead of a fresh one. */
    PURGE_ON_STALE_SCAN,
    /** NC-4: the self-check runs after the first offer. */
    SELF_CHECK_AFTER_FIRST_OFFER,
    /** NC-10: no one-shadow-per-position-per-producer check (W2). */
    MONOTONE_CHECK_OFF,
    /** NC-12: a cursor with unknown fields is accepted (W0). */
    CURSOR_CHECK_OFF,
    /** NC-14: an {@code ArchiveException} at W6 is relabelled UNEXPECTED. */
    ARCHIVE_EXCEPTION_RELABELLED,
    /** NC-9 (kit leg): the writer accepts {@code genesisId <= 0} (W1). */
    GENESIS_CHECK_OFF,
    /** NC-Q0: the writer computes the payload digest with CRC32C again (the pre-amendment format). */
    DIGEST_AS_CRC32C
}
