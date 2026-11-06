// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.cluster.VersionValidator;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@code ClusteredServiceAgent} semantics for one {@link ShadowEngine}, without an agent, a role or a {@code Cluster}:
 * position and cluster time are set from the frame before the engine call (A0:CSA:493-613), the term event's app
 * version is validated (A0:CSA:592-598), snapshot actions become {@link ShadowTriggerListener} calls instead of
 * snapshots. There is no session map: every entry is routed by the frame's id, so an unknown close is forwarded and
 * the engine treats it as a no-op, and a clashing open is the engine's to refuse.
 *
 * <p>Hard rules: it never schedules, cancels, publishes or connects, and holds no {@code Cluster}, timer port,
 * publication, Aeron client or archive client (its field types are this package, Agrona buffers, the JDK, codec enums
 * and {@code io.aeron.cluster.VersionValidator}, a pure function). Positions are frame ends, time is the frame
 * timestamp; nothing reads a clock. One apply function per frame kind: the engine entry.</p>
 *
 * <p>Image position and SKIP: {@code restore(image, length, imagePosition, replayFrom)} lets frames resume before the
 * image ({@code replayFrom.logPosition() < imagePosition}, a gate drops the engine-mutating ones). While the cursor is
 * behind the image no snapshot trigger fires (the engine already holds a later state) and {@code serialize} refuses.
 * A gate reports what it drops through {@link #onSkipped}, so the cursor still advances on every frame. The container
 * enforces the window itself: an engine-mutating frame reaches the engine iff {@code p > imagePosition}, and a skip
 * is accepted only for an engine-mutating frame with {@code p <= imagePosition}.</p>
 */
public final class ReplayContainer implements ClusterLogListener {

    private static final int FLAGS_ALIGNED = 0;
    private static final int FLAGS_STANDBY = 1;

    private final ShadowEngine engine;
    private final int appVersion;
    private final VersionValidator validator;
    private final ShadowTriggerListener triggers;
    private final long[] counts = new long[FrameKind.values().length];

    private boolean started;
    private boolean failed;
    private ReplayStoppedException failure;
    private long imagePosition;
    private long logPosition;
    private long clusterTime = -1L;
    private long leadershipTermId = -1L;
    private long termBaseLogPosition = -1L;
    private long ignoredActionCount;
    private long suppressedTriggerCount;
    private long skippedFrameCount;

    /**
     * @param appVersion the member's {@code ClusteredServiceContainer.Context.appVersion()}
     * @param validator  the member's {@code appVersionValidator()}
     */
    public ReplayContainer(final ShadowEngine engine, final int appVersion, final VersionValidator validator,
                           final ShadowTriggerListener triggers) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.appVersion = appVersion;
        this.validator = Objects.requireNonNull(validator, "validator");
        this.triggers = Objects.requireNonNull(triggers, "triggers");
    }

    // ------------------------------------------------------------------------------------------------------ start

    /** {@code engine.genesis()}; cursor {@link LogCursor#GENESIS}, image position 0. */
    public void genesis() {
        checkStartable();
        started = true;
        try {
            engine.genesis();
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "genesis");
        }
        apply(LogCursor.GENESIS);
        imagePosition = 0;
    }

    /**
     * @param imagePosition the log position the engine image holds the state at
     * @param replayFrom    where frames resume: at or before {@code imagePosition}; the first frame must end after
     *                      {@code replayFrom.logPosition()}
     */
    public void restore(final DirectBuffer image, final int length, final long imagePosition,
                        final LogCursor replayFrom) {
        Objects.requireNonNull(replayFrom, "replayFrom");
        checkStartable();
        if (imagePosition < 0) {
            throw new IllegalArgumentException("imagePosition must be >= 0: " + imagePosition);
        }
        if (replayFrom.logPosition() > imagePosition) {
            throw new ReplayStoppedException(ReplayStoppedException.Reason.REPLAY_AFTER_IMAGE,
                    "replayFrom=" + replayFrom.logPosition() + " imagePosition=" + imagePosition, null);
        }
        started = true;
        try {
            engine.restore(image, length);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "restore");
        }
        apply(replayFrom);
        this.imagePosition = imagePosition;
    }

    /** The engine image at index 0; refused while the cursor is behind the image (SKIP window). */
    public int serialize(final MutableDirectBuffer dst) {
        checkRunning();
        if (logPosition < imagePosition) {
            throw new ReplayStoppedException(ReplayStoppedException.Reason.INSIDE_SKIP_WINDOW,
                    "logPosition=" + logPosition + " imagePosition=" + imagePosition, null);
        }
        try {
            return engine.serialize(dst);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "serialize");
        }
    }

    // ------------------------------------------------------------------------------------------------- callbacks

    @Override
    public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                 final DirectBuffer buffer, final int offset, final int length) {
        enterApply(logPosition, timestamp, FrameKind.SESSION_MESSAGE);
        try {
            engine.onSessionMessage(clusterSessionId, logPosition, timestamp, buffer, offset, length);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "onSessionMessage@" + logPosition);
        }
    }

    @Override
    public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
        enterApply(logPosition, timestamp, FrameKind.TIMER_EVENT);
        try {
            engine.onTimerEvent(correlationId, logPosition, timestamp);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "onTimerEvent@" + logPosition);
        }
    }

    /** The response stream and channel are dropped: a replayer never connects. */
    @Override
    public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                              final long timestamp, final int responseStreamId, final String responseChannel,
                              final byte[] encodedPrincipal) {
        enterApply(logPosition, timestamp, FrameKind.SESSION_OPEN);
        try {
            engine.onSessionOpen(clusterSessionId, logPosition, timestamp, encodedPrincipal);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "onSessionOpen@" + logPosition);
        }
    }

    /** Forwarded for every id: no session map, an unknown id is the engine's no-op. */
    @Override
    public void onSessionClose(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                               final long timestamp, final CloseReason closeReason) {
        enterApply(logPosition, timestamp, FrameKind.SESSION_CLOSE);
        try {
            engine.onSessionClose(clusterSessionId, logPosition, timestamp, closeReason);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "onSessionClose@" + logPosition);
        }
    }

    /**
     * Never reaches the engine. SNAPSHOT with flags 0 or 1 calls the trigger listener unless the frame is behind the
     * image (then suppressed and counted). Any other flags (members test {@code flags & standbySnapshotFlags} and the
     * only defined flags are 0 and 1, A0:CM:132-137), SUSPEND and RESUME are counted and ignored.
     */
    @Override
    public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                   final ClusterAction action, final int flags) {
        enter(logPosition, timestamp, FrameKind.CLUSTER_ACTION);
        if (action != ClusterAction.SNAPSHOT || (flags != FLAGS_ALIGNED && flags != FLAGS_STANDBY)) {
            ignoredActionCount++;
            return false;
        }
        if (logPosition < imagePosition) {
            suppressedTriggerCount++;
            return false;
        }
        try {
            return triggers.onSnapshotAction(logPosition, leadershipTermId, timestamp, flags);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "trigger@" + logPosition);
        }
    }

    @Override
    public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                         final long termBaseLogPosition, final int leaderMemberId,
                                         final int logSessionId, final TimeUnit timeUnit, final int logAppVersion) {
        checkFrame(logPosition);
        if (!validator.isVersionCompatible(appVersion, logAppVersion)) {
            throw latch(ReplayStoppedException.Reason.INCOMPATIBLE_APP_VERSION,
                    "container=" + appVersion + " log=" + logAppVersion + " @" + logPosition, null);
        }
        if (timeUnit != TimeUnit.MILLISECONDS) {
            throw latch(ReplayStoppedException.Reason.UNSUPPORTED_TIME_UNIT, timeUnit + " @" + logPosition, null);
        }
        this.logPosition = logPosition;
        this.clusterTime = timestamp;
        this.leadershipTermId = leadershipTermId;
        this.termBaseLogPosition = termBaseLogPosition;
        counts[FrameKind.NEW_LEADERSHIP_TERM.ordinal()]++;
        try {
            engine.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
        } catch (final RuntimeException | Error cause) {
            throw latch(cause, "onNewLeadershipTermEvent@" + logPosition);
        }
    }

    /**
     * A gate in front dropped the frame: position and cluster time advance (as CSA sets them before the service's own
     * SKIP gate drops the engine call), the engine is not called, no trigger fires. Only what a correct SKIP window can
     * drop is accepted: an engine-mutating frame the image already holds ({@code p <= imagePosition}). Anything else
     * (a term event, an action, a frame after the image, any skip after genesis) stops {@code SKIP_OUTSIDE_IMAGE}.
     */
    @Override
    public void onSkipped(final FrameKind kind, final long logPosition, final long timestamp) {
        checkFrame(logPosition);
        if (!SkipWindow.isEngineMutating(kind) || logPosition > imagePosition) {
            throw latch(ReplayStoppedException.Reason.SKIP_OUTSIDE_IMAGE,
                    kind + "@" + logPosition + " imagePosition=" + imagePosition, null);
        }
        enter(logPosition, timestamp, kind);
        skippedFrameCount++;
    }

    // --------------------------------------------------------------------------------------------------- queries

    public LogCursor cursor() {
        return new LogCursor(logPosition, clusterTime, leadershipTermId, termBaseLogPosition);
    }

    public long imagePosition() {
        return imagePosition;
    }

    public long logPosition() {
        return logPosition;
    }

    public long clusterTime() {
        return clusterTime;
    }

    public long leadershipTermId() {
        return leadershipTermId;
    }

    public long termBaseLogPosition() {
        return termBaseLogPosition;
    }

    public boolean failed() {
        return failed;
    }

    /** Frames of {@code kind} the container accepted, applied or skipped ({@link #skippedFrameCount()} counts skips). */
    public long count(final FrameKind kind) {
        return counts[kind.ordinal()];
    }

    public long ignoredActionCount() {
        return ignoredActionCount;
    }

    public long suppressedTriggerCount() {
        return suppressedTriggerCount;
    }

    /** Frames a gate in front dropped ({@link #onSkipped}). */
    public long skippedFrameCount() {
        return skippedFrameCount;
    }

    // --------------------------------------------------------------------------------------------------- private

    private void enter(final long position, final long timestamp, final FrameKind kind) {
        checkFrame(position);
        logPosition = position;
        clusterTime = timestamp;
        counts[kind.ordinal()]++;
    }

    /** An engine apply: the engine must not already hold the frame (every legal path drops {@code p <= image}). */
    private void enterApply(final long position, final long timestamp, final FrameKind kind) {
        checkFrame(position);
        if (position <= imagePosition) {
            throw latch(ReplayStoppedException.Reason.APPLY_INSIDE_IMAGE,
                    kind + "@" + position + " imagePosition=" + imagePosition, null);
        }
        enter(position, timestamp, kind);
    }

    private void checkFrame(final long position) {
        checkRunning();
        if (position <= logPosition) {
            throw latch(ReplayStoppedException.Reason.NON_MONOTONIC_POSITION,
                    "frame=" + position + " cursor=" + logPosition, null);
        }
    }

    private void checkRunning() {
        if (failed) {
            throw new ReplayStoppedException(ReplayStoppedException.Reason.FAILED, "container failed", failure);
        }
        if (!started) {
            throw new ReplayStoppedException(ReplayStoppedException.Reason.NOT_STARTED,
                    "genesis or restore first", null);
        }
    }

    private void checkStartable() {
        if (failed) {
            throw new ReplayStoppedException(ReplayStoppedException.Reason.FAILED, "container failed", failure);
        }
        if (started) {
            throw new ReplayStoppedException(ReplayStoppedException.Reason.ALREADY_STARTED,
                    "genesis/restore once per container", null);
        }
    }

    private void apply(final LogCursor cursor) {
        logPosition = cursor.logPosition();
        clusterTime = cursor.clusterTime();
        leadershipTermId = cursor.leadershipTermId();
        termBaseLogPosition = cursor.termBaseLogPosition();
    }

    private ReplayStoppedException latch(final Throwable cause, final String where) {
        return latch(ReplayStoppedException.Reason.ENGINE_FAILED, where, cause);
    }

    private ReplayStoppedException latch(final ReplayStoppedException.Reason reason, final String message,
                                         final Throwable cause) {
        failed = true;
        failure = new ReplayStoppedException(reason, message, cause);
        return failure;
    }
}
