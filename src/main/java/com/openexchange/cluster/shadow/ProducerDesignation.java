// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.cluster.ClusterMember;

import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Which cluster follower writes the ALIGNED shadow for a term: the member whose id follows the leader's id in
 * ascending order over the static members table, wrapping to the smallest. The members table is configuration
 * (the consensus context's {@code clusterMembers}), identical on every node by Aeron's static membership, so every
 * node that knows the same leader id computes the same designated member. A one-member cluster designates nobody
 * (the leader never produces); a two-member cluster designates its only follower.
 *
 * <p>Designation is a node-local decision: it chooses whether THIS node serializes a copy of its own engine image
 * into its own archive, never a replicated value. The leader id it reads comes from the leader's own
 * {@code CommitPosition} message ({@code ConsensusReadiness.leader()}), cross-checked by the host against the log's
 * {@code NewLeadershipTermEvent} when that frame was seen.</p>
 *
 * <p>Pure and immutable; the archive endpoint lookup serves the puller's leader-source skip.</p>
 */
public final class ProducerDesignation {

    /** No member designated: unknown leader, a leader id outside the table, or a one-member cluster. */
    public static final int NONE = -1;

    private final int[] ids;
    private final TreeMap<Integer, String> archiveEndpoints = new TreeMap<>();

    /** @param memberIds the static members table's ids, any order, no duplicates, each >= 0 */
    public ProducerDesignation(final int... memberIds) {
        Objects.requireNonNull(memberIds, "memberIds");
        final int[] sorted = memberIds.clone();
        Arrays.sort(sorted);
        for (int i = 0; i < sorted.length; i++) {
            if (sorted[i] < 0) {
                throw new IllegalArgumentException("member id must be >= 0: " + sorted[i]);
            }
            if (i > 0 && sorted[i] == sorted[i - 1]) {
                throw new IllegalArgumentException("member id " + sorted[i] + " is listed twice");
            }
        }
        this.ids = sorted;
    }

    /** From the consensus context's {@code clusterMembers} string: ids and archive control endpoints. */
    public static ProducerDesignation ofClusterMembers(final String clusterMembers) {
        final ClusterMember[] members = ClusterMember.parse(clusterMembers);
        final int[] ids = new int[members.length];
        for (int i = 0; i < members.length; i++) {
            ids[i] = members[i].id();
        }
        final ProducerDesignation d = new ProducerDesignation(ids);
        for (final ClusterMember m : members) {
            d.archiveEndpoints.put(m.id(), m.archiveEndpoint());
        }
        return d;
    }

    /**
     * The designated member for a term whose leader is {@code leaderMemberId}, or {@link #NONE} when the leader is
     * unknown ({@code < 0}), not in the table, or alone.
     */
    public int designated(final int leaderMemberId) {
        if (leaderMemberId < 0 || ids.length < 2 || Arrays.binarySearch(ids, leaderMemberId) < 0) {
            return NONE;
        }
        for (final int id : ids) {
            if (id > leaderMemberId) {
                return id;
            }
        }
        return ids[0];
    }

    /** True when {@code self} is the designated member for the term led by {@code leaderMemberId}. */
    public boolean isDesignated(final int self, final int leaderMemberId) {
        final int d = designated(leaderMemberId);
        return d != NONE && d == self;
    }

    /** The archive control endpoint the members table spells for {@code memberId}, or null (unknown member). */
    public String archiveEndpoint(final int memberId) {
        return archiveEndpoints.get(memberId);
    }

    /** The members table ids, ascending. */
    public int[] memberIds() {
        return ids.clone();
    }
}
