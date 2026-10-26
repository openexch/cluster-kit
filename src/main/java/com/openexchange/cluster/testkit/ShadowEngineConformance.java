// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.ShadowEngine;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.concurrent.TimeUnit;

/**
 * The rules every {@link ShadowEngine} must satisfy, run against any engine supplier over a sample {@link RecordedLog}.
 * Frames go straight to the engine entries (no container), so a failure is the engine's.
 * <ol>
 *   <li>R1: genesis once; a second genesis, a restore after genesis, a restore after a restore and a restore after
 *       any {@code on*} are refused (with a valid image, which a pristine engine accepts).</li>
 *   <li>R2: {@code serialize} twice gives equal bytes and changes no later output (image and events).</li>
 *   <li>R3: {@code restore(serialize())} then {@code serialize} gives the same bytes; a splice at every frame of the
 *       sample ends with the unspliced image.</li>
 *   <li>R4: a term event leaves the image unchanged and emits no event.</li>
 *   <li>R5: a close of an id never opened leaves the image unchanged and emits no event.</li>
 *   <li>R6: if sessions are replicated, a clashing open (the sample's first open, twice) throws.</li>
 *   <li>R7: after {@code provokeFailure}, every entry including {@code serialize} throws
 *       {@link IllegalStateException}.</li>
 * </ol>
 * JUnit-free: a violation throws {@link AssertionError} naming the rule.
 */
public final class ShadowEngineConformance {

    private ShadowEngineConformance() {
    }

    /**
     * @param sessionsReplicated whether the engine keeps a replicated session table (then R6 applies)
     * @param provokeFailure     drives the engine into its failed state (may throw)
     * @param emittedEvents      total events emitted so far by engines from this supplier
     */
    public record Options<E>(boolean sessionsReplicated, Consumer<? super E> provokeFailure,
                             Supplier<Integer> emittedEvents) {
        public Options {
            Objects.requireNonNull(provokeFailure, "provokeFailure");
            Objects.requireNonNull(emittedEvents, "emittedEvents");
        }
    }

    public record Report(int frames, int splices) { }

    public static <E extends ShadowEngine> Report run(final Supplier<? extends E> fresh, final RecordedLog sample,
                                                      final Options<E> options) {
        Objects.requireNonNull(fresh, "fresh");
        Objects.requireNonNull(sample, "sample");
        Objects.requireNonNull(options, "options");
        if (sample.size() == 0) {
            throw new IllegalArgumentException("empty sample");
        }
        final ExpandableArrayBuffer scratch = new ExpandableArrayBuffer(256);
        final List<LogFrame> frames = sample.frames();

        // ---- R1
        final E genesisEngine = fresh.get();
        genesisEngine.genesis();
        final byte[] genesisImage = image(genesisEngine, scratch);
        final E control = fresh.get();
        call("R1 control: a pristine engine must accept the genesis image", () -> {
            control.restore(new UnsafeBuffer(genesisImage), genesisImage.length);
        });
        refused("R1: second genesis", () -> genesisEngine.genesis());
        final E afterGenesis = fresh.get();
        afterGenesis.genesis();
        refused("R1: restore after genesis", () -> afterGenesis.restore(new UnsafeBuffer(genesisImage),
                genesisImage.length));
        refused("R1: restore after restore", () -> control.restore(new UnsafeBuffer(genesisImage),
                genesisImage.length));
        final E genesisAfterRestore = fresh.get();
        genesisAfterRestore.restore(new UnsafeBuffer(genesisImage), genesisImage.length);
        refused("R1: genesis after restore", genesisAfterRestore::genesis);
        final Set<FrameKind> kindsTried = new HashSet<>();
        for (final LogFrame frame : frames) {
            if (frame.kind() == FrameKind.CLUSTER_ACTION || !kindsTried.add(frame.kind())) {
                continue;
            }
            final E touched = fresh.get();
            frame.deliver(feed(touched));
            refused("R1: restore after " + frame.kind(), () -> touched.restore(new UnsafeBuffer(genesisImage),
                    genesisImage.length));
        }

        // ---- R2
        final int half = frames.size() / 2;
        final E straight = fresh.get();
        straight.genesis();
        final int eventsBeforeStraight = options.emittedEvents().get();
        replay(frames, 0, frames.size(), straight);
        final int straightEvents = options.emittedEvents().get() - eventsBeforeStraight;
        final byte[] straightImage = image(straight, scratch);

        final E twice = fresh.get();
        twice.genesis();
        final int eventsBeforeTwice = options.emittedEvents().get();
        replay(frames, 0, half, twice);
        final byte[] first = image(twice, scratch);
        final byte[] second = image(twice, scratch);
        if (!Arrays.equals(first, second)) {
            throw new AssertionError("R2: two serializations of the same state differ (after " + half + " frames)");
        }
        replay(frames, half, frames.size(), twice);
        if (!Arrays.equals(straightImage, image(twice, scratch))) {
            throw new AssertionError("R2: serialize changed later state (final image differs)");
        }
        if (options.emittedEvents().get() - eventsBeforeTwice != straightEvents) {
            throw new AssertionError("R2: serialize changed later output (event count differs)");
        }

        // ---- R3
        int splices = 0;
        for (int k = 0; k <= frames.size(); k++) {
            final E prefix = fresh.get();
            prefix.genesis();
            replay(frames, 0, k, prefix);
            final byte[] atCut = image(prefix, scratch);
            final E restored = fresh.get();
            restored.restore(new UnsafeBuffer(atCut), atCut.length);
            if (!Arrays.equals(atCut, image(restored, scratch))) {
                throw new AssertionError("R3: restore(serialize()) does not reproduce the bytes at frame " + k);
            }
            replay(frames, k, frames.size(), restored);
            if (!Arrays.equals(straightImage, image(restored, scratch))) {
                throw new AssertionError("R3: splice at frame " + k + " (" + describe(frames, k)
                        + ") ends with a different image");
            }
            splices++;
        }

        // ---- R4, R5 on the state after the whole sample
        final LogFrame last = frames.get(frames.size() - 1);
        final long nextPosition = last.position() + 64;
        final E neutral = fresh.get();
        neutral.genesis();
        replay(frames, 0, frames.size(), neutral);
        final byte[] before = image(neutral, scratch);
        int events = options.emittedEvents().get();
        neutral.onNewLeadershipTermEvent(termOf(frames) + 1, nextPosition, last.timestamp(), TimeUnit.MILLISECONDS);
        if (!Arrays.equals(before, image(neutral, scratch))) {
            throw new AssertionError("R4: a term event changed the image");
        }
        if (options.emittedEvents().get() != events) {
            throw new AssertionError("R4: a term event emitted an event");
        }
        final long neverOpened = neverOpenedId(frames);
        events = options.emittedEvents().get();
        neutral.onSessionClose(neverOpened, nextPosition + 64, last.timestamp(), CloseReason.CLIENT_ACTION);
        if (!Arrays.equals(before, image(neutral, scratch))) {
            throw new AssertionError("R5: a close of never-opened id " + neverOpened + " changed the image");
        }
        if (options.emittedEvents().get() != events) {
            throw new AssertionError("R5: a close of never-opened id " + neverOpened + " emitted an event");
        }

        // ---- R6
        if (options.sessionsReplicated()) {
            final LogFrame open = frames.stream().filter(f -> f.kind() == FrameKind.SESSION_OPEN).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("R6 needs a SessionOpen in the sample"));
            final E clash = fresh.get();
            clash.genesis();
            clash.onSessionOpen(open.clusterSessionId(), open.position(), open.timestamp(), open.data());
            boolean threw = false;
            try {
                clash.onSessionOpen(open.clusterSessionId(), open.position() + 64, open.timestamp(), open.data());
            } catch (final RuntimeException expected) {
                threw = true;
            }
            if (!threw) {
                throw new AssertionError("R6: a clashing open of id " + open.clusterSessionId() + " was accepted");
            }
        }

        // ---- R7
        final E failing = fresh.get();
        failing.genesis();
        replay(frames, 0, half, failing);
        try {
            options.provokeFailure().accept(failing);
        } catch (final RuntimeException expected) {
            // the provocation itself may be the throwing entry
        }
        final byte[] anyBytes = new byte[] {1};
        final long p = nextPosition + 128;
        failed("R7: genesis", failing::genesis);
        failed("R7: restore", () -> failing.restore(new UnsafeBuffer(genesisImage), genesisImage.length));
        failed("R7: serialize", () -> failing.serialize(scratch));
        failed("R7: onSessionOpen", () -> failing.onSessionOpen(neverOpened, p, 0, anyBytes));
        failed("R7: onSessionClose", () -> failing.onSessionClose(neverOpened, p, 0, CloseReason.CLIENT_ACTION));
        failed("R7: onSessionMessage", () -> failing.onSessionMessage(neverOpened, p, 0,
                new UnsafeBuffer(anyBytes), 0, 1));
        failed("R7: onTimerEvent", () -> failing.onTimerEvent(1, p, 0));
        failed("R7: onNewLeadershipTermEvent", () -> failing.onNewLeadershipTermEvent(1, p, 0,
                TimeUnit.MILLISECONDS));
        return new Report(frames.size(), splices);
    }

    // --------------------------------------------------------------------------------------------------- private

    private static void replay(final List<LogFrame> frames, final int from, final int to, final ShadowEngine engine) {
        final ClusterLogListener feed = feed(engine);
        for (int i = from; i < to; i++) {
            frames.get(i).deliver(feed);
        }
    }

    /** Frames straight to the engine entries; actions never reach an engine. */
    private static ClusterLogListener feed(final ShadowEngine engine) {
        return new ClusterLogListener() {
            @Override
            public void onSessionMessage(final long logPosition, final long clusterSessionId, final long timestamp,
                                         final DirectBuffer buffer, final int offset, final int length) {
                engine.onSessionMessage(clusterSessionId, logPosition, timestamp, buffer, offset, length);
            }

            @Override
            public void onTimerEvent(final long logPosition, final long correlationId, final long timestamp) {
                engine.onTimerEvent(correlationId, logPosition, timestamp);
            }

            @Override
            public void onSessionOpen(final long leadershipTermId, final long logPosition, final long clusterSessionId,
                                      final long timestamp, final int responseStreamId, final String responseChannel,
                                      final byte[] encodedPrincipal) {
                engine.onSessionOpen(clusterSessionId, logPosition, timestamp, encodedPrincipal);
            }

            @Override
            public void onSessionClose(final long leadershipTermId, final long logPosition,
                                       final long clusterSessionId, final long timestamp,
                                       final CloseReason closeReason) {
                engine.onSessionClose(clusterSessionId, logPosition, timestamp, closeReason);
            }

            @Override
            public boolean onClusterAction(final long leadershipTermId, final long logPosition, final long timestamp,
                                           final ClusterAction action, final int flags) {
                return false;
            }

            @Override
            public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition,
                                                 final long timestamp, final long termBaseLogPosition,
                                                 final int leaderMemberId, final int logSessionId,
                                                 final TimeUnit timeUnit, final int appVersion) {
                engine.onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, timeUnit);
            }
        };
    }

    private static byte[] image(final ShadowEngine engine, final ExpandableArrayBuffer scratch) {
        final int length = engine.serialize(scratch);
        final byte[] bytes = new byte[length];
        scratch.getBytes(0, bytes);
        return bytes;
    }

    private static long termOf(final List<LogFrame> frames) {
        long term = 0;
        for (final LogFrame f : frames) {
            if (f.kind() == FrameKind.NEW_LEADERSHIP_TERM) {
                term = f.leadershipTermId();
            }
        }
        return term;
    }

    private static long neverOpenedId(final List<LogFrame> frames) {
        long max = 0;
        for (final LogFrame f : frames) {
            max = Math.max(max, f.clusterSessionId());
        }
        return max + 1_000_003L;
    }

    private static String describe(final List<LogFrame> frames, final int k) {
        return k < frames.size() ? "before " + frames.get(k) : "at the end";
    }

    private static void call(final String what, final Runnable action) {
        try {
            action.run();
        } catch (final RuntimeException failure) {
            final AssertionError error = new AssertionError(what + ": " + failure);
            error.initCause(failure);
            throw error;
        }
    }

    private static void refused(final String what, final Runnable action) {
        try {
            action.run();
        } catch (final RuntimeException expected) {
            return;
        }
        throw new AssertionError(what + " was accepted");
    }

    private static void failed(final String what, final Runnable action) {
        try {
            action.run();
        } catch (final IllegalStateException expected) {
            return;
        } catch (final RuntimeException other) {
            throw new AssertionError(what + " after the provoked failure threw " + other
                    + ", not IllegalStateException");
        }
        throw new AssertionError(what + " after the provoked failure was accepted");
    }
}
