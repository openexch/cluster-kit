// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Whether a quorum of cluster members is reachable from this member right now, read from the transport and not
 * from the election: the evidence that separates "the peers are up and the election is stuck" from "the peers are
 * gone".
 *
 * <p>Signal: a peer counts as reachable only once its consensus module has left startup recovery and its transport
 * answers. Every member opens one participation subscription on its own consensus endpoint, on a stream of its own
 * ({@link #PROBE_STREAM_ID_ENV}, default {@value #DEFAULT_PROBE_STREAM_ID}), once its consensus module state reads
 * running (ACTIVE, SUSPENDED or SNAPSHOT, {@link #isRunning}), and keeps it until this probe closes. Nothing ever
 * polls it: it exists so that this member's driver answers the peers' probes. This class also holds one publication of its own to every peer's consensus endpoint on that
 * participation stream. A publication is connected while the peer's driver answers it with status messages, which
 * needs the peer's driver to be up, the peer's participation subscription to exist and both directions of the path
 * to work. A peer whose process is gone, or whose host is cut off, stops answering and the publication disconnects
 * after the driver's publication connection timeout. This member counts itself, so the quorum of a three member
 * cluster needs one connected peer. Nothing is ever offered on these publications: a peer sees one idle image on its
 * participation subscription.</p>
 *
 * <p>Why not the consensus stream: Aeron's consensus module subscribes to the consensus stream when it is
 * constructed, then blocks in startup recovery until its clustered services have loaded their snapshots, and only
 * after that creates its election and goes ACTIVE. A service restore can take minutes (a shadow restore pulls from a
 * peer). A probe on the consensus stream reads such a peer reachable although it cannot vote, and the lone survivor of
 * a quorum loss would restart itself in the middle of the restore it may be serving. On the participation stream a
 * restoring peer reads unreachable, so the survivor stays up. A running peer reads reachable as before, including one
 * in the stuck election the rejoin watchdog exists for: its module state is ACTIVE. A peer on an older kit, which has
 * no participation subscription, reads unreachable: with mixed versions the watchdog does not trip (it fails safe).
 * A member's own election state is never a reason to skip its subscription: only its module state decides, and a
 * module state that is not running (INIT, QUITTING, TERMINATING, CLOSED, or an unavailable counter) never opens
 * it.</p>
 *
 * <p>The signal is outbound on purpose. In the stuck-election case the watchdog exists for, the member's receiver
 * ignores a peer's consensus frames, so nothing inbound distinguishes that peer from a dead one; the status
 * messages for this member's own publications still arrive on the sender side.</p>
 *
 * <p>It owns a separate Aeron client on the consensus module's driver directory (the module's own client is single
 * threaded and belongs to its agent), so it works the same with an embedded driver and an external one. The client
 * is created by the first {@link #tick} that reads a running module state, or by the first question, whichever comes
 * first; the subscription with it, the publications on the first question. Everything is closed by {@link #close}.
 * When they cannot be created, or the client fails, the answer is false (no evidence), said once through the log,
 * and the subscription is not opened again: this member's peers then read it unreachable.</p>
 *
 * <p>The probe publications carry no data, so they are opened on a small sparse term ({@link #PROBE_TERM_LENGTH_ENV},
 * default 64 KiB) and not on the consensus channel's term: a driver default of 16 MiB costs three terms of log buffer
 * on this member and as much again for the image it creates on each peer, for every peer, until the member stops.</p>
 *
 * <p>Ticked and asked by the rejoin watchdog under its poll lock, and the methods take this object's lock. The
 * client's error handler runs on that client's conductor thread while the conductor holds the client lock; it takes
 * neither this lock nor closes the client (both could deadlock against a poller waiting on the client lock, and a
 * close from the conductor would join its own thread). It records the failure, and the poller gives up and closes
 * the client on its next tick or question.</p>
 */
final class PeerReachability implements BooleanSupplier, AutoCloseable {
    /** Term length of the probe publications, bytes: a power of two in {@link #MIN_PROBE_TERM_LENGTH}..{@link #MAX_PROBE_TERM_LENGTH}. */
    static final String PROBE_TERM_LENGTH_ENV = "ENGINE_REJOIN_PROBE_TERM_LENGTH";
    static final String PROBE_TERM_LENGTH_PROP = "engine.rejoin.probe.term.length";
    static final int DEFAULT_PROBE_TERM_LENGTH = 64 * 1024;
    static final int MIN_PROBE_TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    static final int MAX_PROBE_TERM_LENGTH = LogBufferDescriptor.TERM_MAX_LENGTH;
    /**
     * Stream id of the participation subscription on each member's consensus endpoint and of the probes aimed at it:
     * a whole number in 1..{@link Integer#MAX_VALUE}, different from the consensus stream id.
     */
    static final String PROBE_STREAM_ID_ENV = "ENGINE_REJOIN_PROBE_STREAM_ID";
    static final String PROBE_STREAM_ID_PROP = "engine.rejoin.probe.stream.id";
    static final int DEFAULT_PROBE_STREAM_ID = 4306;

    private final String aeronDirectoryName;
    private final String consensusChannel;
    private final int probeStreamId;
    private final ClusterMember[] members;
    private final int memberId;
    private final Consumer<String> log;
    private final int probeTermLength;
    private final LongSupplier moduleState;
    private Aeron aeron;
    private Publication[] publications;
    private Subscription participation;
    private boolean unavailable;
    private boolean closed;
    /**
     * Set by the client's error handler on the client conductor thread, which neither takes this object's lock nor
     * closes the client (closing joins the conductor's own thread, and the conductor holds the client lock that a
     * poller inside this lock may be waiting for); the next {@link #tick} or question gives up and closes.
     */
    private volatile String clientFailure;
    private volatile int lastReachable = 1;

    /**
     * @param probeStreamId the participation stream: this member's own subscription and the peers' probes
     * @param moduleState   this member's consensus module state ({@code ConsensusModule.State} codes); the
     *                      participation subscription opens only while it reads a running state
     *                      ({@link #isRunning}: ACTIVE, SUSPENDED or SNAPSHOT)
     */
    PeerReachability(final String aeronDirectoryName, final String consensusChannel, final int probeStreamId,
                     final String clusterMembers, final int memberId, final Consumer<String> log,
                     final int probeTermLength, final LongSupplier moduleState) {
        LogBufferDescriptor.checkTermLength(probeTermLength);
        if (probeStreamId <= 0) {
            throw new IllegalArgumentException(PROBE_STREAM_ID_ENV + " must be positive, got " + probeStreamId);
        }
        this.probeTermLength = probeTermLength;
        this.aeronDirectoryName = aeronDirectoryName;
        this.consensusChannel = consensusChannel;
        this.probeStreamId = probeStreamId;
        this.members = ClusterMember.parse(clusterMembers);
        this.memberId = memberId;
        this.log = log;
        this.moduleState = java.util.Objects.requireNonNull(moduleState, "moduleState");
    }

    /** The probe of the member {@code context} configures, on {@code probeStreamId}. */
    static PeerReachability of(final ConsensusModule.Context context, final Consumer<String> log,
                               final int probeStreamId) {
        return new PeerReachability(context.aeronDirectoryName(), context.consensusChannel(), probeStreamId,
                context.clusterMembers(), context.clusterMemberId(), log, probeTermLength(),
                moduleState(context.moduleStateCounter()));
    }

    /**
     * The module state read from {@code counter}: a missing counter reads unavailable and a closed one CLOSED, and
     * neither is a running state, so neither opens the participation subscription.
     */
    static LongSupplier moduleState(final Counter counter) {
        return QuorumWatchdog.counterValue(counter, ConsensusModule.State.CLOSED.code());
    }

    /**
     * Whether a consensus module in state {@code code} has left startup recovery and is running: ACTIVE, SUSPENDED
     * or SNAPSHOT. INIT (still recovering), QUITTING, TERMINATING, CLOSED and any value that is not a state (an
     * unavailable counter) are not: a member that cannot vote, or is going away, must not read reachable to its
     * peers.
     */
    static boolean isRunning(final long code) {
        return code == ConsensusModule.State.ACTIVE.code() || code == ConsensusModule.State.SUSPENDED.code()
                || code == ConsensusModule.State.SNAPSHOT.code();
    }

    /** {@link #PROBE_TERM_LENGTH_ENV}, else the default; a value that is not a power of two in range refuses boot. */
    static int probeTermLength() {
        return probeTermLength(System.getenv(PROBE_TERM_LENGTH_ENV), System.getProperty(PROBE_TERM_LENGTH_PROP));
    }

    static int probeTermLength(final String envValue, final String propValue) {
        final int value = (int) EngineLifecycle.parseRanged(PROBE_TERM_LENGTH_ENV, envValue, PROBE_TERM_LENGTH_PROP,
                propValue, DEFAULT_PROBE_TERM_LENGTH, MIN_PROBE_TERM_LENGTH, MAX_PROBE_TERM_LENGTH);
        if (Integer.bitCount(value) != 1) {
            throw new IllegalArgumentException(PROBE_TERM_LENGTH_ENV + " must be a power of two in "
                    + MIN_PROBE_TERM_LENGTH + ".." + MAX_PROBE_TERM_LENGTH + ", got " + value);
        }
        return value;
    }

    /** {@link #PROBE_STREAM_ID_ENV}, else the default; a value that is not a whole number in range refuses boot. */
    static int probeStreamId() {
        return probeStreamId(System.getenv(PROBE_STREAM_ID_ENV), System.getProperty(PROBE_STREAM_ID_PROP));
    }

    static int probeStreamId(final String envValue, final String propValue) {
        return (int) EngineLifecycle.parseRanged(PROBE_STREAM_ID_ENV, envValue, PROBE_STREAM_ID_PROP, propValue,
                DEFAULT_PROBE_STREAM_ID, 1, Integer.MAX_VALUE);
    }

    /**
     * Why this participation stream refuses to arm next to this consensus stream, or null when it is fine: on the
     * consensus stream the probe would read a peer still in startup recovery as reachable again.
     */
    static String probeStreamRefusal(final int probeStreamId, final int consensusStreamId) {
        if (probeStreamId == consensusStreamId) {
            return PROBE_STREAM_ID_ENV + " (" + probeStreamId + ") must differ from the consensus stream id ("
                    + ConsensusModule.Configuration.CONSENSUS_STREAM_ID_PROP_NAME + ", " + consensusStreamId
                    + "): a peer whose consensus module is still in startup recovery already subscribes to the"
                    + " consensus stream and would count as reachable";
        }
        return null;
    }

    /**
     * The participation subscription's channel: the consensus channel with this member's consensus endpoint when it
     * names none, exactly as the consensus module derives its own consensus subscription.
     */
    static String participationChannel(final String consensusChannel, final String ownConsensusEndpoint) {
        final ChannelUri uri = ChannelUri.parse(consensusChannel);
        if (!uri.containsKey(CommonContext.ENDPOINT_PARAM_NAME)) {
            uri.put(CommonContext.ENDPOINT_PARAM_NAME, ownConsensusEndpoint);
        }
        return uri.toString();
    }

    /** The probe's channel: the consensus channel aimed at {@code endpoint}, on the probe's own small sparse term. */
    static String probeChannel(final String consensusChannel, final String endpoint, final int termLength) {
        final ChannelUri uri = ChannelUri.parse(consensusChannel);
        uri.put(CommonContext.ENDPOINT_PARAM_NAME, endpoint);
        uri.put(CommonContext.TERM_LENGTH_PARAM_NAME, Integer.toString(termLength));
        uri.put(CommonContext.SPARSE_PARAM_NAME, "true");
        return uri.toString();
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

    /** Whether this member's participation subscription is open (tests). */
    synchronized boolean participating() {
        return participation != null;
    }

    /** Whether this probe's own client is open (tests). */
    synchronized boolean clientOpen() {
        return aeron != null && !aeron.isClosed();
    }

    /**
     * Once per poll, whether or not an election is open: opens this member's participation subscription as soon as
     * its consensus module is running ({@link #isRunning}), and gives up on a client that failed. Cheap once open.
     */
    public synchronized void tick() {
        checkClientFailure();
        ensureParticipating();
    }

    /** True when this member plus the peers that answer its probes on the participation stream form a quorum. */
    @Override
    public synchronized boolean getAsBoolean() {
        if (closed) {
            return false;
        }
        checkClientFailure();
        ensureParticipating();
        if (members.length <= 1) {
            return true;
        }
        if (!ensurePublications()) {
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

    private void ensureParticipating() {
        if (closed || unavailable || participation != null || members.length <= 1) {
            return;
        }
        if (!isRunning(moduleState.getAsLong())) {
            return; // still in startup recovery, or going away: a member that cannot vote must not read reachable
        }
        if (!ensureClient()) {
            return;
        }
        try {
            participation = aeron.addSubscription(
                    participationChannel(consensusChannel, self().consensusEndpoint()), probeStreamId);
        } catch (final RuntimeException e) {
            giveUp("could not open the peer reachability participation subscription: " + e);
        }
    }

    private ClusterMember self() {
        for (final ClusterMember member : members) {
            if (member.id() == memberId) {
                return member;
            }
        }
        throw new IllegalStateException("member " + memberId + " is not in the cluster members");
    }

    private boolean ensureClient() {
        if (aeron != null) {
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
            return true;
        } catch (final RuntimeException e) {
            giveUp("could not open the peer reachability probe: " + e);
            return false;
        }
    }

    private boolean ensurePublications() {
        if (publications != null) {
            return true;
        }
        if (!ensureClient()) {
            return false;
        }
        try {
            final Publication[] opened = new Publication[members.length];
            for (int i = 0; i < members.length; i++) {
                if (members[i].id() == memberId) {
                    continue;
                }
                // exclusive: a session of its own, so it never shares a publication with anything else in the driver
                opened[i] = aeron.addExclusivePublication(
                        probeChannel(consensusChannel, members[i].consensusEndpoint(), probeTermLength),
                        probeStreamId);
            }
            publications = opened;
            return true;
        } catch (final RuntimeException e) {
            giveUp("could not open the peer reachability probe: " + e);
            return false;
        }
    }

    /**
     * The client's error handler, on the client conductor thread: only records the first failure. It takes no lock
     * and closes nothing (see {@link #clientFailure}).
     */
    void onError(final Throwable t) {
        if (clientFailure == null) {
            clientFailure = "the peer reachability probe's client failed: " + t;
        }
    }

    /** On the poller, under this lock: a failure the client's error handler recorded gives up here. */
    private void checkClientFailure() {
        final String failure = clientFailure;
        if (failure != null && !unavailable) {
            giveUp(failure);
        }
    }

    /** On the poller, under this lock; never on the client's conductor thread. */
    private void giveUp(final String why) {
        if (!unavailable) {
            unavailable = true;
            log.accept("rejoin watchdog: " + why + "; without it there is no evidence of reachable peers and the"
                    + " watchdog does not trip, and this member's peers read it unreachable (their watchdogs do not"
                    + " count it)");
        }
        final Aeron client = aeron;
        aeron = null;
        publications = null;
        participation = null;
        CloseHelper.quietClose(client);
    }

    @Override
    public synchronized void close() {
        closed = true;
        final Aeron client = aeron;
        aeron = null;
        publications = null;
        participation = null;
        CloseHelper.quietClose(client);
    }
}
