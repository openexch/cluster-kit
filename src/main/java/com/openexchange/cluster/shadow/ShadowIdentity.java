// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.Objects;

/**
 * Whether a shadow belongs to this cluster's history. Identity is the replicated {@code genesisId}
 * plus the term base of the shadow's term as this host knows it: a member builds {@link TermBaseLookup} from
 * its {@code RecordingLog} term entries (read side), a replica from the term events its reader saw, so a replica
 * shadow passes a member's check with no term 0 anywhere (RF-f). Pure: no clock, no I/O.
 */
public final class ShadowIdentity {

    private ShadowIdentity() {
    }

    /** {@code termBaseOf(termId)}: the term base this host knows for a term, or -1 when it does not know the term. */
    @FunctionalInterface
    public interface TermBaseLookup {
        long termBaseOf(long leadershipTermId);
    }

    /** This host: engine, cluster, recorded genesis ({@code 0} = unknown here: replica bootstrap only), its terms. */
    public record LocalIdentity(int engineId, int clusterId, long genesisId, TermBaseLookup terms) {
        public LocalIdentity {
            Objects.requireNonNull(terms, "terms");
        }
    }

    public enum Outcome { OK, ENGINE_MISMATCH, CLUSTER_MISMATCH, UNIDENTIFIED, GENESIS_MISMATCH, GENESIS_UNVERIFIED,
        HISTORY_MISMATCH, HISTORY_UNVERIFIED }

    /**
     * STRICT (default): OK only. ALLOW_UNVERIFIED_HISTORY: OK or HISTORY_UNVERIFIED. BOOTSTRAP_ADOPT: OK,
     * HISTORY_UNVERIFIED or GENESIS_UNVERIFIED, valid only together with {@link #matches} after the restore (the ADOPT
     * frame is inside the shadow, so that is the only check left).
     */
    public enum Policy { STRICT, ALLOW_UNVERIFIED_HISTORY, BOOTSTRAP_ADOPT }

    /** Rows I1-I8, first match wins. */
    public static Outcome check(final ShadowHeader h, final LocalIdentity local) {
        if (h.engineId() != local.engineId()) {
            return Outcome.ENGINE_MISMATCH;
        }
        if (h.clusterId() != local.clusterId()) {
            return Outcome.CLUSTER_MISMATCH;
        }
        if (h.genesisId() <= 0) {
            return Outcome.UNIDENTIFIED;
        }
        // I4 as `local != 0` (the contract's `local > 0`): a negative local nonce must not fall through to OK
        if (local.genesisId() != 0 && h.genesisId() != local.genesisId()) {
            return Outcome.GENESIS_MISMATCH;
        }
        if (local.genesisId() == 0) {
            return Outcome.GENESIS_UNVERIFIED;
        }
        final long termBase = local.terms().termBaseOf(h.leadershipTermId());
        if (termBase == -1) {
            return Outcome.HISTORY_UNVERIFIED;
        }
        if (termBase != h.termBaseLogPosition()) {
            return Outcome.HISTORY_MISMATCH;
        }
        return Outcome.OK;
    }

    /**
     * Whether {@code outcome} is accepted under {@code policy}. A local genesis below 0 never reaches I5 as "unknown":
     * it is not 0, so every shadow (H10: {@code > 0}) differs from it and is refused at I4, and the host's boot guard
     * ({@link #requireProducible}) stops a producer on it.
     */
    public static boolean accepted(final Outcome outcome, final Policy policy) {
        return switch (outcome) {
            case OK -> true;
            case HISTORY_UNVERIFIED -> policy == Policy.ALLOW_UNVERIFIED_HISTORY || policy == Policy.BOOTSTRAP_ADOPT;
            case GENESIS_UNVERIFIED -> policy == Policy.BOOTSTRAP_ADOPT;
            default -> false;
        };
    }

    /** {@link #check} then {@link #accepted}. */
    public static boolean admits(final ShadowHeader h, final LocalIdentity local, final Policy policy) {
        return accepted(check(h, local), policy);
    }

    /**
     * 5.4, the mandatory post-restore cross-check: the RESTORED engine's {@code genesisId}, {@code journalEnabled} and
     * {@code journalSeq} must equal the header's. Null when they do; else the first differing field with both values
     * ({@code IDENTITY_CROSS_CHECK_FAILED}: the host discards the container and the engine and treats it as CRITICAL).
     */
    public static String mismatch(final ShadowHeader h, final ShadowIdentitySource restored) {
        if (restored.genesisId() != h.genesisId()) {
            return "genesisId header=" + h.genesisId() + " restored=" + restored.genesisId();
        }
        final int journal = restored.journalEnabled() ? 1 : 0;
        if (journal != h.journalEnabled()) {
            return "journalEnabled header=" + h.journalEnabled() + " restored=" + journal;
        }
        if (restored.journalSeq() != h.journalSeq()) {
            return "journalSeq header=" + h.journalSeq() + " restored=" + restored.journalSeq();
        }
        return null;
    }

    /** {@link #mismatch} is null. */
    public static boolean matches(final ShadowHeader h, final ShadowIdentitySource restored) {
        return mismatch(h, restored) == null;
    }

    /** 5.4 with the counter: on a mismatch {@code shadow_identity_crosscheck_failures_total} +1. */
    public static boolean matches(final ShadowHeader h, final ShadowIdentitySource restored,
                                  final ShadowCounters counters) {
        final boolean ok = matches(h, restored);
        if (!ok) {
            counters.increment(ShadowCounters.Counter.IDENTITY_CROSSCHECK_FAILURES_TOTAL);
        }
        return ok;
    }

    /**
     * The host boot guard: a producer (the replica, a follower) calls it once the engine is restored or has
     * replayed the ADOPT, before arming any trigger.
     *
     * @throws IllegalStateException for {@code genesisId <= 0}
     */
    public static void requireProducible(final long genesisId) {
        if (genesisId <= 0) {
            throw new IllegalStateException("shadow producer refused: recorded genesisId <= 0 (" + genesisId
                    + "); produce only after the engine is restored or has replayed its ADOPT");
        }
    }
}
