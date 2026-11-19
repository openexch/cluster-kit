// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.ClusterMembersExtendedResponseDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.cluster.service.ConsensusModuleProxy;
import org.agrona.CloseHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * How far every member of the cluster has got, asked from inside the node.
 *
 * <p>Retention needs this and cannot get it any other way. Each node purges its
 * own archive, and purging past a member that has not received the log strands
 * it; so the purge point has to be bounded by the slowest member. But a member
 * only knows its own position. The one process that knows everyone's is the
 * LEADER's consensus module, which tracks each follower's append position in
 * order to commit at all.</p>
 *
 * <p>The gateway used to answer this by reading all three nodes' counter files
 * off the local disk, which works exactly once: when the three nodes are on one
 * machine. This asks the consensus module instead, over the same IPC control
 * channel the service container already talks to it on. No peer's filesystem,
 * no side channel, nothing that stops working when the members are three pods
 * on three hosts.</p>
 *
 * <h2>What comes back</h2>
 *
 * <p>{@code ClusterMembersExtendedResponse} carries, per member, the member id,
 * its log position and the time its last append was seen. The last of those is
 * what makes the answer safe to use: a member's reported position does not
 * decay, so a member that has stopped responding keeps reporting the number it
 * reached before it died. Retention must treat that as "unknown and behind",
 * never as current, which is why the freshness travels with the position.</p>
 *
 * <p>Answered by whichever node is asked, about what IT believes. Only the
 * leader maintains real follower positions, so member positions are read from
 * the leader's answer alone. A follower's answer is still asked for, at each
 * purge pass, for the two things it does know: who leads, and every member's
 * archive endpoint.</p>
 *
 * <h2>Where it runs</h2>
 *
 * <p>Not on the duty cycle. This sends a request and polls for a reply, and the
 * thread that matches orders must never wait for anything. It is built for a
 * background thread and it says so by blocking. The pruner asks from its own
 * thread: every wake on the leader, every purge pass on a follower.</p>
 */
public final class ClusterMemberPositions implements AutoCloseable {

    /** One member's replicated position, and whether it can be trusted. */
    public static final class Member {
        public final int memberId;
        public final long logPosition;
        /** Nanos, on the consensus module's clock, of the last append seen from this member. */
        public final long timeOfLastAppendNs;
        /** The member's archive control endpoint ({@code host:port}), or null when the answer did not carry it. */
        public final String archiveEndpoint;

        Member(final int memberId, final long logPosition, final long timeOfLastAppendNs) {
            this(memberId, logPosition, timeOfLastAppendNs, null);
        }

        Member(final int memberId, final long logPosition, final long timeOfLastAppendNs,
               final String archiveEndpoint) {
            this.memberId = memberId;
            this.logPosition = logPosition;
            this.timeOfLastAppendNs = timeOfLastAppendNs;
            this.archiveEndpoint = archiveEndpoint;
        }

        @Override
        public String toString() {
            return "member" + memberId + "@" + logPosition;
        }
    }

    /** A whole answer: the members, and when the leader believed it. */
    public static final class Snapshot {
        public final long currentTimeNs;
        public final int leaderMemberId;
        public final List<Member> members;
        /** The member that answered, or -1 when not known. */
        public final int memberId;

        Snapshot(final long currentTimeNs, final int leaderMemberId, final List<Member> members) {
            this(currentTimeNs, leaderMemberId, -1, members);
        }

        Snapshot(final long currentTimeNs, final int leaderMemberId, final int memberId,
                 final List<Member> members) {
            this.currentTimeNs = currentTimeNs;
            this.leaderMemberId = leaderMemberId;
            this.memberId = memberId;
            this.members = members;
        }

        /** The entry of {@code id}, or null. */
        public Member member(final int id) {
            for (final Member m : members) {
                if (m.memberId == id) {
                    return m;
                }
            }
            return null;
        }
    }

    private static final int FRAGMENT_LIMIT = 10;

    private final Aeron aeron;
    private final String controlChannel;
    private final int serviceStreamId;
    private final int consensusModuleStreamId;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ClusterMembersExtendedResponseDecoder extendedDecoder =
        new ClusterMembersExtendedResponseDecoder();

    private ConsensusModuleProxy proxy;
    private Subscription subscription;
    private Publication publication;

    private long awaitedCorrelationId = Aeron.NULL_VALUE;
    private Snapshot received;

    public ClusterMemberPositions(final Aeron aeron, final String controlChannel,
                                  final int serviceStreamId, final int consensusModuleStreamId) {
        this.aeron = aeron;
        this.controlChannel = controlChannel;
        this.serviceStreamId = serviceStreamId;
        this.consensusModuleStreamId = consensusModuleStreamId;
    }

    /** Build one from the container's own context, which already holds all four values. */
    public static ClusterMemberPositions from(final Aeron aeron,
                                              final ClusteredServiceContainer.Context ctx) {
        return new ClusterMemberPositions(aeron, ctx.controlChannel(), ctx.serviceStreamId(),
            ctx.consensusModuleStreamId());
    }

    /**
     * Ask, and wait for the answer.
     *
     * @param timeoutMs how long to wait before giving up
     * @return the members, or null when the consensus module did not answer in time.
     *         Null means "unknown", and retention must treat unknown as a reason to
     *         hold, never as a reason to purge.
     */
    public Snapshot query(final long timeoutMs) {
        ensureConnected();

        final long correlationId = aeron.nextCorrelationId();
        awaitedCorrelationId = correlationId;
        received = null;

        if (!proxy.clusterMembersQuery(correlationId)) {
            return null; // back-pressured; the caller retries on its own rhythm
        }

        final long deadlineMs = System.currentTimeMillis() + timeoutMs;
        while (received == null) {
            if (subscription.poll(this::onFragment, FRAGMENT_LIMIT) == 0) {
                if (System.currentTimeMillis() > deadlineMs) {
                    return null;
                }
                Thread.onSpinWait();
            }
        }
        return received;
    }

    private void ensureConnected() {
        if (subscription == null) {
            // The service container reads this same stream and ignores templates it
            // does not know (its adapter's switch has no default action), so a second
            // subscriber here costs it nothing.
            subscription = aeron.addSubscription(controlChannel, serviceStreamId);
        }
        if (proxy == null) {
            publication = aeron.addPublication(controlChannel, consensusModuleStreamId);
            proxy = new ConsensusModuleProxy(publication);
        }
    }

    private void onFragment(final org.agrona.DirectBuffer buffer, final int offset,
                            final int length, final io.aeron.logbuffer.Header header) {
        if (received != null || length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.schemaId() != ClusterMembersExtendedResponseDecoder.SCHEMA_ID
            || headerDecoder.templateId() != ClusterMembersExtendedResponseDecoder.TEMPLATE_ID) {
            // Everything else on this stream belongs to the service container.
            return;
        }

        extendedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        if (extendedDecoder.correlationId() != awaitedCorrelationId) {
            return; // an answer to somebody else's question
        }

        final long currentTimeNs = extendedDecoder.currentTimeNs();
        final int leaderMemberId = extendedDecoder.leaderMemberId();
        final int responderId = extendedDecoder.memberId();

        final List<Member> members = new ArrayList<>(3);
        for (final ClusterMembersExtendedResponseDecoder.ActiveMembersDecoder active
                : extendedDecoder.activeMembers()) {
            final int id = active.memberId();
            final long logPosition = active.logPosition();
            final long timeOfLastAppendNs = active.timeOfLastAppendNs();
            // Each group entry ends with five variable-length endpoint strings. SBE only
            // advances to the next entry past the ones that were READ (or skipped); leaving
            // them unread made every entry after the first decode the previous entry's
            // endpoint bytes as its ids and positions (member ids 1818321775 / 808857650,
            // positions in the 10^18 range).
            active.skipIngressEndpoint();
            active.skipConsensusEndpoint();
            active.skipLogEndpoint();
            active.skipCatchupEndpoint();
            members.add(new Member(id, logPosition, timeOfLastAppendNs, active.archiveEndpoint()));
        }

        received = new Snapshot(currentTimeNs, leaderMemberId, responderId,
            Collections.unmodifiableList(members));
    }

    @Override
    public void close() {
        CloseHelper.quietClose(proxy);
        CloseHelper.quietClose(subscription);
        proxy = null;
        publication = null;
        subscription = null;
    }
}
