// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.shadow.ShadowIdentity;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code leadershipTermId -> termBaseLogPosition} as this replica knows it: the seed is the
 * restored shadow's header, every later entry comes from a {@code NewLeadershipTermEvent} frame the follower applied.
 * The backup's own {@code RecordingLog} is never consulted for identity (it lacks terms, CBA:916-966). Written on the
 * follower thread only; read by any thread (the catalog and the host's checks).
 *
 * <p>A term seen with two different bases is impossible inside one log: {@link #observe} throws, and the follower
 * fences on it (the history it is applying is not one history).</p>
 */
public final class ReplicaTermIndex implements ShadowIdentity.TermBaseLookup {

    private final Map<Long, Long> bases = new ConcurrentHashMap<>();
    private volatile long seededTerm = -1;

    /** The restored shadow's term (its header), before any frame. */
    public void seed(final long leadershipTermId, final long termBaseLogPosition) {
        put(leadershipTermId, termBaseLogPosition, "seed");
        seededTerm = leadershipTermId;
    }

    /**
     * One applied term event.
     *
     * @throws IllegalStateException when the term is already known with another base
     */
    public void observe(final long leadershipTermId, final long termBaseLogPosition) {
        put(leadershipTermId, termBaseLogPosition, "term event");
    }

    private void put(final long termId, final long base, final String source) {
        if (termId < 0 || base < 0) {
            throw new IllegalArgumentException(source + ": term " + termId + " base " + base + " must be >= 0");
        }
        final Long previous = bases.putIfAbsent(termId, base);
        if (previous != null && previous != base) {
            throw new IllegalStateException("term " + termId + " seen with term base " + previous + " and " + base
                    + " (" + source + "): the replica is not applying one history");
        }
    }

    /** -1 when the term was never seen here (the catalog reads HISTORY_UNVERIFIED). */
    @Override
    public long termBaseOf(final long leadershipTermId) {
        final Long base = bases.get(leadershipTermId);
        return base == null ? -1 : base;
    }

    public int size() {
        return bases.size();
    }

    public long seededTerm() {
        return seededTerm;
    }

    /** The identity this replica checks shadows against once its genesis is known (STRICT after bootstrap). */
    public ShadowIdentity.LocalIdentity local(final int engineId, final int clusterId, final long genesisId) {
        return new ShadowIdentity.LocalIdentity(engineId, clusterId, genesisId, this);
    }

    @Override
    public String toString() {
        return "ReplicaTermIndex" + new TreeMap<>(bases);
    }
}
