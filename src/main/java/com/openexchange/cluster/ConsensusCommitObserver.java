// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.CommitPositionDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/** Passive, untethered view of the same consensus stream consumed by this member. */
final class ConsensusCommitObserver implements AutoCloseable {
    static final int FRAGMENT_LIMIT = 32;
    private final ConsensusModule.Context context;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommitPositionDecoder commitDecoder = new CommitPositionDecoder();
    private final io.aeron.logbuffer.FragmentHandler handler = this::onFragment;
    private Subscription subscription;
    private ClusterMember[] members;
    private ConsensusReadiness.LeaderCheckpoint checkpoint;
    private long pollingNs;
    private int leaderId = -1;
    private boolean ambiguous;

    ConsensusCommitObserver(final ConsensusModule.Context context) { this.context = context; }

    /** Startup only. Aeron registration is never performed by poll(). */
    void start(final Aeron aeron) {
        if (subscription != null) { throw new IllegalStateException("observer already started"); }
        members = ClusterMember.parse(context.clusterMembers());
        final ClusterMember local = ClusterMember.determineMember(members, context.clusterMemberId(),
                context.memberEndpoints());
        final ChannelUri channel = ChannelUri.parse(context.consensusChannel());
        if (!channel.containsKey("endpoint")) { channel.put("endpoint", local.consensusEndpoint()); }
        channel.put("tether", "false");
        channel.put("rejoin", "true");
        subscription = aeron.addSubscription(channel.toString(), context.consensusStreamId());
    }

    ConsensusReadiness.LeaderCheckpoint poll(final long nowNs) {
        if (subscription == null || subscription.isClosed() || !subscription.isConnected()) { return null; }
        pollingNs = nowNs;
        // A full batch means the observer may be behind; never call its buffered
        // checkpoint fresh until it has reached the available end of the stream.
        if (subscription.poll(handler, FRAGMENT_LIMIT) == FRAGMENT_LIMIT || ambiguous) { return null; }
        return checkpoint;
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH + CommitPositionDecoder.BLOCK_LENGTH) { return; }
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.schemaId() != CommitPositionDecoder.SCHEMA_ID
                || headerDecoder.templateId() != CommitPositionDecoder.TEMPLATE_ID
                || headerDecoder.blockLength() < CommitPositionDecoder.BLOCK_LENGTH
                || headerDecoder.blockLength() > length - MessageHeaderDecoder.ENCODED_LENGTH) { return; }
        commitDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        final int memberId = commitDecoder.leaderMemberId();
        if (memberId == context.clusterMemberId() || ClusterMember.findMember(members, memberId) == null) { return; }
        accept(commitDecoder.leadershipTermId(), memberId, ((Image)header.context()).correlationId(),
                commitDecoder.logPosition(), pollingNs);
    }

    void accept(final long term, final int member, final long source, final long position, final long nowNs) {
        if (term < 0 || source < 0 || position < 0) { return; }
        if (checkpoint != null && term < checkpoint.term()) { return; }
        if (checkpoint == null || term > checkpoint.term()) {
            leaderId = member; ambiguous = false;
        } else if (leaderId != member) {
            ambiguous = true; return;
        }
        if (checkpoint != null && term == checkpoint.term() && position < checkpoint.position()) { return; }
        checkpoint = new ConsensusReadiness.LeaderCheckpoint(term, source, position, nowNs);
    }

    @Override public void close() {
        checkpoint = null;
        if (subscription != null) { subscription.close(); subscription = null; }
    }
}
