// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.codecs.CommitPositionDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A bounded, loud exit for a member that can never catch up: its log position is below the leader's log start.
 *
 * <p>Why: a member that was down while the others purged their logs past its position restarts, finds the leader,
 * and asks for a catch-up replay from its own position. The leader's archive refuses every such replay (the
 * requested start is below the recording's start position), the member times out waiting to join the catch-up log,
 * starts a new election and asks again, for ever. It is in a leader-known state most of the time, so neither the
 * {@link QuorumWatchdog} nor the {@link RejoinWatchdog} ever trips: the member sits not ready and says nothing but a
 * timeout warning. Only a reseed helps it, and nobody is told.</p>
 *
 * <p>Rule: a catch-up attempt is the election entering FOLLOWER_CATCHUP_INIT or FOLLOWER_CATCHUP_AWAIT. An attempt
 * that ends in a new election or in a no-leader state, without ever reaching FOLLOWER_CATCHUP, failed. After
 * {@link EngineLifecycle#CATCHUP_REFUSALS_ENV} failed attempts in a row, or once attempts have been failing for
 * {@link EngineLifecycle#CATCHUP_REFUSAL_WAIT_ENV}, the watchdog reads both positions: this member's log position
 * from its own archive (a member with no log recording, such as one whose directories were wiped, counts at its
 * newest snapshot's position, or 0), and the leader's log start position from the leader's archive (the leader and its log
 * recording are named by the leader's own NewLeadershipTerm message on the consensus stream). When the member's
 * position is below the leader's log start it logs one {@code CRITICAL} line naming both positions and records
 * {@link EngineLifecycle#fatal} with exit 1. A supervisor that restarts the member gets the same exit again after
 * the same number of attempts: the member is loud until it is reseeded.</p>
 *
 * <p>When the positions say otherwise, or the leader's log start cannot be read, the watchdog does not exit: it
 * logs one line with what it found and checks again after the same number of further failures (or the same wait).
 * A catch-up that is joined (FOLLOWER_CATCHUP and later, or CLOSED) clears the count.</p>
 *
 * <p>The poller reads the election counters under the poll lock and stops before the components close
 * ({@link EngineLifecycle#awaitStop}). The position check runs outside the lock, on archive clients of its own that
 * share one Aeron client on the consensus module's driver directory (embedded or external driver alike); it is
 * opened at the first failed attempt, so a member that never fails a catch-up opens nothing.</p>
 */
public final class CatchupWatchdog {
    static final long POLL_INTERVAL_MS = 50;

    /** What the position check found; {@code unknown} names the reason when a position could not be read. */
    record Diagnosis(long ownPosition, int leaderMemberId, long leaderLogStart, String unknown) {
        static Diagnosis unknown(final String why) {
            return new Diagnosis(-1, -1, -1, why);
        }

        boolean belowLeaderLogStart() {
            return unknown == null && ownPosition >= 0 && leaderLogStart > ownPosition;
        }
    }

    /** The reads behind the watchdog; the production one is {@link ArchiveProbe}. */
    interface Probe extends AutoCloseable {
        /** A catch-up attempt just failed: start whatever {@link #diagnose} needs. */
        void onRefusal();

        /** Called on every poll once started. */
        void poll();

        Diagnosis diagnose();

        @Override
        void close();
    }

    private final EngineLifecycle lifecycle;
    private final LongSupplier electionState;
    private final LongSupplier elections;
    private final long refusalLimit;
    private final long waitMs;
    private final long waitNs;
    private final Probe probe;
    private volatile boolean tripped;
    private volatile boolean stopped;
    private volatile Thread poller;
    private volatile long refusals;
    private boolean inAttempt;
    private long attemptElection;
    private long firstRefusalNs;
    private long nextCheckAtRefusals;
    private long nextCheckAtNs;

    CatchupWatchdog(final EngineLifecycle lifecycle, final LongSupplier electionState, final LongSupplier elections,
                    final long refusalLimit, final long waitMs, final Probe probe) {
        if (refusalLimit <= 0 || waitMs <= 0) {
            throw new IllegalArgumentException("catch-up refusal bounds must be positive");
        }
        this.lifecycle = lifecycle;
        this.electionState = electionState;
        this.elections = elections;
        this.refusalLimit = refusalLimit;
        this.waitMs = waitMs;
        this.waitNs = TimeUnit.MILLISECONDS.toNanos(waitMs);
        this.probe = probe;
    }

    static CatchupWatchdog of(final EngineLifecycle lifecycle, final ConsensusModule.Context context,
                              final long refusalLimit, final long waitMs) {
        return new CatchupWatchdog(lifecycle,
                QuorumWatchdog.counterValue(context.electionStateCounter(), ElectionState.CLOSED.code()),
                QuorumWatchdog.counterValue(context.electionCounter(), QuorumWatchdog.STATE_UNAVAILABLE),
                refusalLimit, waitMs, new ArchiveProbe(context, lifecycle::log, LogExtent.probeTimeoutMs()));
    }

    static boolean isAttempt(final long code) {
        return code == ElectionState.FOLLOWER_CATCHUP_INIT.code() || code == ElectionState.FOLLOWER_CATCHUP_AWAIT.code();
    }

    /** The states that end an attempt without the catch-up log: a new election's opening, or no leader. */
    static boolean isFailedAttemptExit(final long code) {
        return code == ElectionState.INIT.code() || QuorumWatchdog.isNoLeader(code);
    }

    /** The states in which the member follows or leads: the catch-up was joined, or was never needed. */
    static boolean isJoined(final long code) {
        return QuorumWatchdog.isLeaderKnown(code) && !isAttempt(code)
                && code != ElectionState.FOLLOWER_LOG_REPLICATION.code()
                && code != ElectionState.FOLLOWER_REPLAY.code();
    }

    public boolean tripped() {
        return tripped;
    }

    public boolean stopped() {
        return stopped;
    }

    /** Failed catch-up attempts in a row, as counted so far. */
    public long refusals() {
        return refusals;
    }

    boolean pollerAlive() {
        final Thread t = poller;
        return t != null && t.isAlive();
    }

    /** Stop polling for good, under the poll lock, join the poller for at most {@code joinMs}, close the probe. */
    public void stop(final long joinMs) {
        synchronized (this) {
            stopped = true;
        }
        final Thread t = poller;
        if (t != null && t != Thread.currentThread()) {
            t.interrupt();
            try {
                t.join(joinMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        probe.close();
    }

    /** One observation; true when this call tripped the watchdog (the lifecycle's fatal fault is recorded). */
    public boolean poll(final long nowNs) {
        if (!observe(nowNs)) {
            return false;
        }
        final Diagnosis diagnosis;
        try {
            diagnosis = probe.diagnose();
        } catch (final RuntimeException e) {
            return conclude(Diagnosis.unknown(String.valueOf(e)), nowNs);
        }
        return conclude(diagnosis, nowNs);
    }

    /** Count under the lock; true when the positions are due to be read. */
    private synchronized boolean observe(final long nowNs) {
        if (tripped || stopped) {
            return false;
        }
        final long code = electionState.getAsLong();
        final long election = elections.getAsLong();
        final boolean attempt = isAttempt(code);
        if (inAttempt && (election != attemptElection || isFailedAttemptExit(code))) {
            // the attempt ended without the catch-up log: Aeron timed out joining it and opened a new election
            if (refusals == 0) {
                firstRefusalNs = nowNs;
                nextCheckAtRefusals = refusalLimit;
                nextCheckAtNs = nowNs + waitNs;
            }
            refusals++;
            inAttempt = false;
            probe.onRefusal();
        }
        if (isJoined(code)) {
            refusals = 0;
            inAttempt = false;
        } else if (attempt && !inAttempt) {
            inAttempt = true;
            attemptElection = election;
        }
        if (refusals == 0) {
            return false;
        }
        probe.poll();
        return refusals >= nextCheckAtRefusals || nowNs - nextCheckAtNs >= 0;
    }

    private synchronized boolean conclude(final Diagnosis d, final long nowNs) {
        if (tripped || stopped) {
            return false;
        }
        final long failingMs = TimeUnit.NANOSECONDS.toMillis(nowNs - firstRefusalNs);
        final String counted = refusals + " catch-up attempt(s) in a row failed over " + failingMs + " ms";
        if (!d.belowLeaderLogStart()) {
            nextCheckAtRefusals = refusals + refusalLimit;
            nextCheckAtNs = nowNs + waitNs;
            lifecycle.log("catch-up watchdog: " + counted + "; " + (d.unknown() != null
                    ? "the leader's log start position could not be read (" + d.unknown() + ")"
                    : "this member's log position " + d.ownPosition() + " is not below the log start position "
                            + d.leaderLogStart() + " of the leader (member " + d.leaderMemberId() + ")")
                    + "; not exiting, still trying to catch up");
            return false;
        }
        tripped = true;
        lifecycle.log("CRITICAL catch-up watchdog: " + counted + ": this member's log position " + d.ownPosition()
                + " is below the log start position " + d.leaderLogStart() + " of the leader (member "
                + d.leaderMemberId() + "). The leader no longer holds the log this member needs, so it can never"
                + " catch up from the log and has to be reseeded (" + EngineLifecycle.CATCHUP_REFUSALS_ENV + " "
                + refusalLimit + ", " + EngineLifecycle.CATCHUP_REFUSAL_WAIT_ENV + " " + waitMs + " ms); exiting "
                + EngineLifecycle.EXIT_FATAL);
        lifecycle.fatal(EngineLifecycle.EXIT_FATAL, "member cannot catch up: its log position " + d.ownPosition()
                + " is below the leader's log start position " + d.leaderLogStart() + " (leader member "
                + d.leaderMemberId() + ")");
        return true;
    }

    /** Poll every 50 ms on a daemon thread until tripped or stopping: an attempt lasts one election timeout. */
    CatchupWatchdog startDaemon(final String name) {
        final Thread t = new Thread(() -> {
            while (!tripped && !stopped && !lifecycle.stopRequested()) {
                if (poll(System.nanoTime())) {
                    return;
                }
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (final InterruptedException e) {
                    return;
                }
            }
        }, name + "-catchup-watchdog");
        t.setDaemon(true);
        poller = t;
        t.start();
        return this;
    }

    /**
     * The production reads. The leader is whoever names itself in a NewLeadershipTerm or CommitPosition message on
     * this member's consensus endpoint (a passive, untethered subscription next to the consensus module's own); its
     * archive endpoint comes from the cluster member string.
     */
    static final class ArchiveProbe implements Probe {
        private static final int FRAGMENT_LIMIT = 32;
        private final ConsensusModule.Context context;
        private final Consumer<String> log;
        private final long timeoutMs;
        private final ClusterMember[] members;
        private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        private final NewLeadershipTermDecoder termDecoder = new NewLeadershipTermDecoder();
        private final CommitPositionDecoder commitDecoder = new CommitPositionDecoder();
        private final FragmentHandler handler = this::onFragment;
        private Aeron aeron;
        private Subscription subscription;
        private String failure;
        private boolean closed;
        private int leaderMemberId = -1;
        private long leaderRecordingId = -1;

        ArchiveProbe(final ConsensusModule.Context context, final Consumer<String> log, final long timeoutMs) {
            this.context = context;
            this.log = log;
            this.timeoutMs = timeoutMs;
            this.members = ClusterMember.parse(context.clusterMembers());
        }

        @Override
        public synchronized void onRefusal() {
            if (aeron != null || failure != null || closed) {
                return;
            }
            try {
                aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(context.aeronDirectoryName())
                        .clientName("catchup-watchdog")
                        // never Aeron's default handler: it exits the JVM on a driver timeout
                        .errorHandler(this::onError));
                final ClusterMember local = ClusterMember.findMember(members, context.clusterMemberId());
                final ChannelUri channel = ChannelUri.parse(context.consensusChannel());
                if (!channel.containsKey(CommonContext.ENDPOINT_PARAM_NAME)) {
                    channel.put(CommonContext.ENDPOINT_PARAM_NAME, local.consensusEndpoint());
                }
                channel.put("tether", "false");
                channel.put("rejoin", "true");
                subscription = aeron.addSubscription(channel.toString(), context.consensusStreamId());
            } catch (final RuntimeException e) {
                fail("could not open the catch-up watchdog's client: " + e);
            }
        }

        private void onError(final Throwable t) {
            synchronized (this) {
                fail("the catch-up watchdog's client failed: " + t);
            }
        }

        private void fail(final String why) {
            if (failure == null) {
                failure = why;
                log.accept("catch-up watchdog: " + why);
            }
            final Aeron client = aeron;
            aeron = null;
            subscription = null;
            CloseHelper.quietClose(client);
        }

        @Override
        public synchronized void poll() {
            if (subscription != null && !subscription.isClosed()) {
                subscription.poll(handler, FRAGMENT_LIMIT);
            }
        }

        private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            headerDecoder.wrap(buffer, offset);
            if (headerDecoder.schemaId() != NewLeadershipTermDecoder.SCHEMA_ID
                    || headerDecoder.blockLength() > length - MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            final int templateId = headerDecoder.templateId();
            if (templateId == NewLeadershipTermDecoder.TEMPLATE_ID
                    && headerDecoder.blockLength() >= NewLeadershipTermDecoder.BLOCK_LENGTH) {
                termDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                accept(termDecoder.leaderMemberId(), termDecoder.leaderRecordingId());
            } else if (templateId == CommitPositionDecoder.TEMPLATE_ID
                    && headerDecoder.blockLength() >= CommitPositionDecoder.BLOCK_LENGTH) {
                commitDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                accept(commitDecoder.leaderMemberId(), -1);
            }
        }

        private void accept(final int memberId, final long recordingId) {
            if (memberId == context.clusterMemberId() || ClusterMember.findMember(members, memberId) == null) {
                return;
            }
            if (memberId != leaderMemberId) {
                leaderMemberId = memberId;
                leaderRecordingId = recordingId;
            } else if (recordingId >= 0) {
                leaderRecordingId = recordingId;
            }
        }

        @Override
        public Diagnosis diagnose() {
            final Aeron client;
            final int leaderId;
            final long recordingId;
            synchronized (this) {
                if (closed) {
                    return Diagnosis.unknown("the watchdog is stopping");
                }
                if (aeron == null) {
                    return Diagnosis.unknown(failure == null ? "the watchdog's client is not open" : failure);
                }
                if (subscription != null && !subscription.isClosed()) {
                    subscription.poll(handler, FRAGMENT_LIMIT);
                }
                client = aeron;
                leaderId = leaderMemberId;
                recordingId = leaderRecordingId;
            }
            if (leaderId < 0) {
                return Diagnosis.unknown("no leader has named itself on the consensus stream yet");
            }
            final ClusterMember leader = ClusterMember.findMember(members, leaderId);
            final ClusterMember local = ClusterMember.findMember(members, context.clusterMemberId());
            final long own;
            final AeronArchive.Context cm = context.archiveContext();
            try (AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                    .controlRequestChannel(cm.controlRequestChannel())
                    .controlRequestStreamId(cm.controlRequestStreamId())
                    .controlResponseChannel(cm.controlResponseChannel())
                    .controlResponseStreamId(cm.controlResponseStreamId())
                    .credentialsSupplier(cm.credentialsSupplier())
                    .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(timeoutMs))
                    .aeron(client)
                    .ownsAeronClient(false))) {
                // a member with no log recording (wiped, or never recorded) counts at its snapshot, or 0
                own = LogExtent.localPosition(archive, context.clusterDir());
            } catch (final RuntimeException e) {
                return Diagnosis.unknown("this member's own log position could not be read: " + e);
            }
            try (AeronArchive archive = AeronArchive.connect(LogExtent.remoteContext(client, null,
                    leader.archiveEndpoint(), LogExtent.hostOf(local.archiveEndpoint()), timeoutMs,
                    t -> log.accept("catch-up watchdog: reading the leader's archive: " + t)))) {
                LogExtent extent = recordingId >= 0 ? LogExtent.byId(archive, recordingId) : null;
                if (extent == null) {
                    extent = LogExtent.byStream(archive, context.logStreamId());
                }
                if (extent == null) {
                    return Diagnosis.unknown("the archive of the leader (member " + leaderId + ") at "
                            + leader.archiveEndpoint() + " holds no log recording");
                }
                return new Diagnosis(own, leaderId, extent.startPosition, null);
            } catch (final RuntimeException e) {
                return Diagnosis.unknown("the archive of the leader (member " + leaderId + ") at "
                        + leader.archiveEndpoint() + " did not answer: " + e);
            }
        }

        @Override
        public void close() {
            final Aeron client;
            synchronized (this) {
                closed = true;
                client = aeron;
                aeron = null;
                subscription = null;
            }
            CloseHelper.quietClose(client);
        }
    }
}
