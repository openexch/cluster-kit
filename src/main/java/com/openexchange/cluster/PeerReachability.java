// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.ConsensusModule;
import org.agrona.CloseHelper;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Whether a quorum of cluster members is reachable from this member right now, read from the transport and not
 * from the election: the evidence that separates "the peers are up and the election is stuck" from "the peers are
 * gone".
 *
 * <p>Signal: this class holds one publication of its own to every peer's consensus endpoint, on the consensus
 * stream. A publication is connected while the peer's driver answers it with status messages, which needs the peer's
 * driver to be up, the peer's consensus module to be subscribed and both directions of the path to work. A peer
 * whose process is gone, or whose host is cut off, stops answering and the publication disconnects after the driver's
 * publication connection timeout. This member counts itself, so the quorum of a three member cluster needs one
 * connected peer. Nothing is ever offered on these publications: a peer sees one more idle image on its consensus
 * subscription.</p>
 *
 * <p>The signal is outbound on purpose. In the stuck-election case the watchdog exists for, the member's receiver
 * ignores a peer's consensus frames, so nothing inbound distinguishes that peer from a dead one; the status
 * messages for this member's own publications still arrive on the sender side.</p>
 *
 * <p>It owns a separate Aeron client on the consensus module's driver directory (the module's own client is single
 * threaded and belongs to its agent), so it works the same with an embedded driver and an external one. The client
 * and the publications are created on the first question and closed by {@link #close}. When they cannot be created
 * the answer is false (no evidence), said once through the log.</p>
 *
 * <p>Asked and closed by the rejoin watchdog under its poll lock; its own client's error handler runs on that
 * client's conductor thread, so the methods take this object's lock.</p>
 */
final class PeerReachability implements BooleanSupplier, AutoCloseable {
    private final String aeronDirectoryName;
    private final String consensusChannel;
    private final int consensusStreamId;
    private final ClusterMember[] members;
    private final int memberId;
    private final Consumer<String> log;
    private Aeron aeron;
    private Publication[] publications;
    private boolean unavailable;
    private boolean closed;
    private volatile int lastReachable = 1;

    PeerReachability(final String aeronDirectoryName, final String consensusChannel, final int consensusStreamId,
                     final String clusterMembers, final int memberId, final Consumer<String> log) {
        this.aeronDirectoryName = aeronDirectoryName;
        this.consensusChannel = consensusChannel;
        this.consensusStreamId = consensusStreamId;
        this.members = ClusterMember.parse(clusterMembers);
        this.memberId = memberId;
        this.log = log;
    }

    static PeerReachability of(final ConsensusModule.Context context, final Consumer<String> log) {
        return new PeerReachability(context.aeronDirectoryName(), context.consensusChannel(),
                context.consensusStreamId(), context.clusterMembers(), context.clusterMemberId(), log);
    }

    /** Members a quorum needs: more than half. */
    int quorum() {
        return members.length / 2 + 1;
    }

    int memberCount() {
        return members.length;
    }

    /** Members counted reachable by the last question, this member included. */
    int lastReachable() {
        return lastReachable;
    }

    /** True when this member plus the peers that answer its consensus publications form a quorum. */
    @Override
    public synchronized boolean getAsBoolean() {
        if (closed) {
            return false;
        }
        if (members.length <= 1) {
            return true;
        }
        if (!ensureOpen()) {
            lastReachable = 1;
            return false;
        }
        int reachable = 1;
        for (final Publication publication : publications) {
            if (publication != null && !publication.isClosed() && publication.isConnected()) {
                reachable++;
            }
        }
        lastReachable = reachable;
        return reachable >= quorum();
    }

    private boolean ensureOpen() {
        if (publications != null) {
            return true;
        }
        if (unavailable) {
            return false;
        }
        try {
            aeron = Aeron.connect(new Aeron.Context()
                    .aeronDirectoryName(aeronDirectoryName)
                    .clientName("rejoin-peer-reachability")
                    // never Aeron's default handler: it exits the JVM on a driver timeout, and the member's own
                    // clients already decide what a lost driver means
                    .errorHandler(this::onError));
            final Publication[] opened = new Publication[members.length];
            for (int i = 0; i < members.length; i++) {
                if (members[i].id() == memberId) {
                    continue;
                }
                final ChannelUri uri = ChannelUri.parse(consensusChannel);
                uri.put(CommonContext.ENDPOINT_PARAM_NAME, members[i].consensusEndpoint());
                // exclusive: a session of its own, so it never shares (and keeps alive) the consensus module's
                // publication to the same endpoint in the same driver
                opened[i] = aeron.addExclusivePublication(uri.toString(), consensusStreamId);
            }
            publications = opened;
            return true;
        } catch (final RuntimeException e) {
            giveUp("could not open the peer reachability probe: " + e);
            return false;
        }
    }

    private synchronized void onError(final Throwable t) {
        giveUp("the peer reachability probe's client failed: " + t);
    }

    private void giveUp(final String why) {
        if (!unavailable) {
            unavailable = true;
            log.accept("rejoin watchdog: " + why + "; without it there is no evidence of reachable peers and the"
                    + " watchdog does not trip");
        }
        final Aeron client = aeron;
        aeron = null;
        publications = null;
        // closing from the client's own error handler is safe: close only signals its conductor
        CloseHelper.quietClose(client);
    }

    @Override
    public synchronized void close() {
        closed = true;
        final Aeron client = aeron;
        aeron = null;
        publications = null;
        CloseHelper.quietClose(client);
    }
}
