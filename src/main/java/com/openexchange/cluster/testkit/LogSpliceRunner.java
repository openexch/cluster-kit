// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowEngine;
import io.aeron.cluster.codecs.ClusterAction;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Splice equivalence at the positions a replica actually shadows at: for every cut k (by default every SNAPSHOT
 * action frame end, flags 0 or 1, of a recorded log), a fresh engine restored from the genesis replay's image at k
 * and resumed from the log must produce, at EVERY later action a, exactly the image the genesis replay had at a (what
 * every later ALIGNED or STANDBY shadow must equal), with the same cursor, and at the log end the same image, cursor
 * and events after k.
 *
 * <p>The genesis replay's image is checked reproducible and round-trip stable at every cut (as
 * {@link ShadowRestoreRunner}). Only the EQUAL path (restore at k, resume at k); the SKIP and TAIL paths stay
 * {@link ShadowRestoreRunner}'s. An empty cut set is refused: {@code vacuous: the log holds no SNAPSHOT action and no
 * stride was given}.</p>
 *
 * <p>JUnit-free: a divergence throws {@link AssertionError}
 * {@code splice at <k>: image at action <a> differs from the genesis replay (first byte <n> of <len>)}.</p>
 */
public final class LogSpliceRunner {

    /** Which positions are cut at. */
    public static final class Cuts {
        private final int stride;

        private Cuts(final int stride) {
            this.stride = stride;
        }

        /** Every SNAPSHOT action frame end (flags 0 or 1). */
        public static Cuts actions() {
            return new Cuts(0);
        }

        /** Every SNAPSHOT action frame end plus every {@code stride}-th frame end. */
        public static Cuts actionsAndStride(final int stride) {
            if (stride <= 0) {
                throw new IllegalArgumentException("stride must be > 0: " + stride);
            }
            return new Cuts(stride);
        }

        int stride() {
            return stride;
        }
    }

    /**
     * {@code cuts} restores, {@code checkpoints} the actions plus the log end, {@code comparisons} image compares made
     * after a cut (callers assert {@code comparisons >= cuts}).
     */
    public record Report(int frames, int actions, int cuts, int checkpoints, int comparisons, int finalImageBytes) {
    }

    private LogSpliceRunner() {
    }

    public static <E extends ShadowEngine> Report run(final Supplier<? extends E> fresh, final RecordedLog log,
                                                      final Cuts cuts, final ShadowRestoreRunner.Probe<? super E> probe) {
        Objects.requireNonNull(fresh, "fresh");
        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(cuts, "cuts");
        Objects.requireNonNull(probe, "probe");
        if (log.startPosition() != 0) {
            throw new IllegalArgumentException("the runner replays from genesis: log must start at 0, not "
                    + log.startPosition());
        }
        final List<LogFrame> frames = log.frames();
        final TreeSet<Long> actions = new TreeSet<>();
        final TreeSet<Long> cutSet = new TreeSet<>();
        for (int i = 0; i < frames.size(); i++) {
            final LogFrame f = frames.get(i);
            if (f.kind() == FrameKind.CLUSTER_ACTION && f.action() == ClusterAction.SNAPSHOT
                    && (f.flags() == 0 || f.flags() == 1)) {
                actions.add(f.position());
                cutSet.add(f.position());
            }
            if (cuts.stride() > 0 && (i + 1) % cuts.stride() == 0) {
                cutSet.add(f.position());
            }
        }
        if (cutSet.isEmpty()) {
            throw new IllegalArgumentException("vacuous: the log holds no SNAPSHOT action and no stride was given");
        }
        final long end = log.endPosition();
        final TreeSet<Long> points = new TreeSet<>(cutSet);
        points.addAll(actions);
        points.add(end);

        // the genesis replay: image, cursor and event count at every point
        final ExpandableArrayBuffer scratch = new ExpandableArrayBuffer(256);
        final E baselineEngine = fresh.get();
        final ReplayContainer baseline = new ReplayContainer(baselineEngine, ShadowRestoreRunner.DEFAULT_APP_VERSION,
                ShadowRestoreRunner.DEFAULT_VALIDATOR, (p, t, ts, fl) -> false);
        baseline.genesis();
        final long[] at = points.stream().mapToLong(Long::longValue).toArray();
        final byte[][] image = new byte[at.length][];
        final LogCursor[] cursor = new LogCursor[at.length];
        final int[] eventsAt = new int[at.length];
        long previous = 0;
        for (int i = 0; i < at.length; i++) {
            log.replay(previous, at[i], baseline);
            previous = at[i];
            image[i] = imageOf(baseline, scratch, "genesis replay at " + at[i]);
            if (cutSet.contains(at[i])) {
                if (!Arrays.equals(image[i], imageOf(baseline, scratch, "genesis replay at " + at[i]))) {
                    throw new AssertionError("genesis replay image at " + at[i] + " is not reproducible");
                }
                final E roundTrip = fresh.get();
                roundTrip.restore(new UnsafeBuffer(image[i]), image[i].length);
                if (!Arrays.equals(image[i], copy(scratch, roundTrip.serialize(scratch)))) {
                    throw new AssertionError("genesis replay image at " + at[i] + " is not round-trip stable");
                }
            }
            cursor[i] = baseline.cursor();
            eventsAt[i] = probe.events(baselineEngine).size();
        }
        final List<String> baselineEvents = List.copyOf(probe.events(baselineEngine));

        int comparisons = 0;
        int cutCount = 0;
        for (int k = 0; k < at.length; k++) {
            if (!cutSet.contains(at[k])) {
                continue;
            }
            cutCount++;
            final long cut = at[k];
            final E engine = fresh.get();
            final ReplayContainer leg = new ReplayContainer(engine, ShadowRestoreRunner.DEFAULT_APP_VERSION,
                    ShadowRestoreRunner.DEFAULT_VALIDATOR, (p, t, ts, fl) -> false);
            final int eventsMark;
            try {
                leg.restore(new UnsafeBuffer(image[k]), image[k].length, cut, cursor[k]);
                eventsMark = probe.events(engine).size();
                long from = cut;
                for (int a = k + 1; a < at.length; a++) {
                    if (!actions.contains(at[a]) && at[a] != end) {
                        continue;
                    }
                    log.replay(from, at[a], leg);
                    from = at[a];
                    final String what = actions.contains(at[a]) ? "action " + at[a] : "the log end " + at[a];
                    final byte[] got = imageOf(leg, scratch, "splice at " + cut + " at " + what);
                    if (!Arrays.equals(image[a], got)) {
                        throw new AssertionError("splice at " + cut + ": image at " + what + " differs from the"
                                + " genesis replay (first byte " + firstDifference(image[a], got) + " of "
                                + image[a].length + ")");
                    }
                    if (!cursor[a].equals(leg.cursor())) {
                        throw new AssertionError("splice at " + cut + ": cursor at " + what + " is " + leg.cursor()
                                + ", the genesis replay " + cursor[a]);
                    }
                    comparisons++;
                }
            } catch (final RuntimeException failure) {
                final AssertionError error = new AssertionError("splice at " + cut + ": leg failed: " + failure);
                error.initCause(failure);
                throw error;
            }
            final List<String> legEvents = probe.events(engine);
            final List<String> legSuffix = legEvents.subList(eventsMark, legEvents.size());
            final List<String> baselineSuffix = baselineEvents.subList(eventsAt[k], baselineEvents.size());
            if (!baselineSuffix.equals(legSuffix)) {
                throw new AssertionError("splice at " + cut + ": events after " + cut + " differ:\n"
                        + TextDiff.diff(String.join("\n", baselineSuffix), String.join("\n", legSuffix)));
            }
        }
        return new Report(frames.size(), actions.size(), cutCount, actions.size() + 1, comparisons,
                image[at.length - 1].length);
    }

    private static int firstDifference(final byte[] a, final byte[] b) {
        final int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return n;
    }

    private static byte[] imageOf(final ReplayContainer container, final ExpandableArrayBuffer scratch,
                                  final String where) {
        try {
            return copy(scratch, container.serialize(scratch));
        } catch (final RuntimeException failure) {
            final AssertionError error = new AssertionError(where + ": serialize failed: " + failure);
            error.initCause(failure);
            throw error;
        }
    }

    private static byte[] copy(final ExpandableArrayBuffer buffer, final int length) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }
}
