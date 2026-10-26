// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ClusterLogListener;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.log.SkipRule;
import com.openexchange.cluster.log.SkipWindow;
import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.VersionValidator;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongBinaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Restore-and-resume equivalence at log positions: for pairs (P_s, P_cm) of frame ends on a stride, a fresh engine
 * restored from the baseline image at P_s, resumed from the log at P_cm, must end exactly as the uninterrupted
 * genesis replay. Three paths:
 * <ul>
 *   <li>EQUAL (s == c): {@code restore(img(s), s, cursor(s))}, then frames e > c.</li>
 *   <li>SKIP (s > c): {@code restore(img(s), s, cursor(c))}, then frames e > c through {@code gate(skipRule(s))}.</li>
 *   <li>TAIL (s < c): {@code restore(img(s), s, cursor(s))}, a tail leg s < e <= tailBound(c) through the tail
 *       decorator, then frames e > c.</li>
 * </ul>
 * Pass iff (1) the final image bytes equal the baseline's, (2) the final cursor (position, term, term base, cluster
 * time) equals the baseline's, (3) the probe's events emitted after max(s, c) equal the baseline's suffix, and (4) in
 * SKIP no snapshot trigger fired before s, and on every path the triggers from the first replayed frame at or after
 * max(s, c) on match the baseline's. The baseline image is checked reproducible and round-trip stable at every point.
 *
 * <p>JUnit-free: a divergence throws {@link AssertionError} naming the path, P_s and P_cm.</p>
 */
public final class ShadowRestoreRunner {

    /** Aeron's container default app version 0.0.1 and validator (A0:CSC:751, A0:CM:984-988). */
    public static final int DEFAULT_APP_VERSION = org.agrona.SemanticVersion.compose(0, 0, 1);
    public static final VersionValidator DEFAULT_VALIDATOR = AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;

    public enum Path { EQUAL, SKIP, TAIL }

    private ShadowRestoreRunner() {
    }

    /**
     * What the legs put in front of the container. {@link #PRODUCTION} is the shipped wiring; tests substitute one
     * element to show the runner sees the defect.
     *
     * @param skipRule      the SKIP gate's rule for a given P_s
     * @param tailDecorator wraps the container for the TAIL leg (s, tailBound(c)]
     * @param tailBound     maps P_cm to the TAIL leg's last frame end
     * @param imageLabel    the image position a SKIP leg declares to {@code restore}, from (P_s, P_cm)
     */
    public record Variant(LongFunction<SkipRule> skipRule, UnaryOperator<ClusterLogListener> tailDecorator,
                          LongUnaryOperator tailBound, LongBinaryOperator imageLabel) {

        public static final Variant PRODUCTION = new Variant(SkipWindow::new, UnaryOperator.identity(),
                LongUnaryOperator.identity(), (s, c) -> s);

        public Variant {
            Objects.requireNonNull(skipRule, "skipRule");
            Objects.requireNonNull(tailDecorator, "tailDecorator");
            Objects.requireNonNull(tailBound, "tailBound");
            Objects.requireNonNull(imageLabel, "imageLabel");
        }

        public Variant withSkipRule(final LongFunction<SkipRule> rule) {
            return new Variant(rule, tailDecorator, tailBound, imageLabel);
        }

        public Variant withTailDecorator(final UnaryOperator<ClusterLogListener> decorator) {
            return new Variant(skipRule, decorator, tailBound, imageLabel);
        }

        public Variant withTailBound(final LongUnaryOperator bound) {
            return new Variant(skipRule, tailDecorator, bound, imageLabel);
        }

        public Variant withImageLabel(final LongBinaryOperator label) {
            return new Variant(skipRule, tailDecorator, tailBound, label);
        }
    }

    /** Rendered events emitted so far by an engine, in order; {@link #none()} for engines without events. */
    @FunctionalInterface
    public interface Probe<E> {
        List<String> events(E engine);

        static <E> Probe<E> none() {
            return engine -> List.of();
        }
    }

    public record Report(int frames, int pairs, int equal, int skip, int tail, int finalImageBytes) { }

    private record Trigger(long logPosition, long leadershipTermId, long timestamp, int flags) { }

    private static final class RecordingTriggers implements ShadowTriggerListener {
        final List<Trigger> fired = new ArrayList<>();

        @Override
        public boolean onSnapshotAction(final long logPosition, final long leadershipTermId, final long timestamp,
                                        final int flags) {
            fired.add(new Trigger(logPosition, leadershipTermId, timestamp, flags));
            return false;
        }
    }

    public static <E extends ShadowEngine> Report run(final Supplier<? extends E> fresh, final RecordedLog log,
                                                      final int stride, final Variant variant,
                                                      final Probe<? super E> probe) {
        Objects.requireNonNull(fresh, "fresh");
        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(variant, "variant");
        Objects.requireNonNull(probe, "probe");
        if (stride <= 0) {
            throw new IllegalArgumentException("stride must be > 0: " + stride);
        }
        if (log.startPosition() != 0) {
            throw new IllegalArgumentException("the runner replays from genesis: log must start at 0, not "
                    + log.startPosition());
        }
        final long[] ends = log.positions();
        if (ends.length == 0) {
            throw new IllegalArgumentException("empty log");
        }

        // B = {0} + frame ends; points = every stride-th element of B plus the last.
        final long[] b = new long[ends.length + 1];
        System.arraycopy(ends, 0, b, 1, ends.length);
        final List<Integer> pointIndexes = new ArrayList<>();
        for (int i = 0; i < b.length; i += stride) {
            pointIndexes.add(i);
        }
        if (pointIndexes.get(pointIndexes.size() - 1) != b.length - 1) {
            pointIndexes.add(b.length - 1);
        }

        // Baseline: genesis, every frame in order; image, cursor and event count at every point.
        final ExpandableArrayBuffer scratch = new ExpandableArrayBuffer(256);
        final E baselineEngine = fresh.get();
        final RecordingTriggers baselineTriggers = new RecordingTriggers();
        final ReplayContainer baseline = new ReplayContainer(baselineEngine, DEFAULT_APP_VERSION, DEFAULT_VALIDATOR,
                baselineTriggers);
        baseline.genesis();
        final int points = pointIndexes.size();
        final long[] pointPosition = new long[points];
        final byte[][] image = new byte[points][];
        final LogCursor[] cursor = new LogCursor[points];
        final int[] eventsAt = new int[points];
        int bIndex = 0;
        for (int p = 0; p < points; p++) {
            final int target = pointIndexes.get(p);
            if (target > bIndex) {
                log.replay(b[bIndex], b[target], baseline);
                bIndex = target;
            }
            pointPosition[p] = b[target];
            image[p] = imageOf(baseline, scratch, "baseline at " + b[target]);
            if (!Arrays.equals(image[p], imageOf(baseline, scratch, "baseline at " + b[target]))) {
                throw new AssertionError("baseline image at " + b[target] + " is not reproducible");
            }
            final E roundTrip = fresh.get();
            roundTrip.restore(new UnsafeBuffer(image[p]), image[p].length);
            final int length = roundTrip.serialize(scratch);
            if (!Arrays.equals(image[p], copy(scratch, length))) {
                throw new AssertionError("baseline image at " + b[target] + " is not round-trip stable");
            }
            cursor[p] = baseline.cursor();
            eventsAt[p] = probe.events(baselineEngine).size();
        }
        final byte[] finalImage = image[points - 1];
        final LogCursor finalCursor = cursor[points - 1];
        final List<String> baselineEvents = List.copyOf(probe.events(baselineEngine));
        final List<Trigger> allTriggers = List.copyOf(baselineTriggers.fired);

        int equal = 0;
        int skip = 0;
        int tail = 0;
        for (int si = 0; si < points; si++) {
            for (int ci = 0; ci < points; ci++) {
                final long s = pointPosition[si];
                final long c = pointPosition[ci];
                final Path path = s == c ? Path.EQUAL : s > c ? Path.SKIP : Path.TAIL;
                final String where = path + " P_s=" + s + " P_cm=" + c;
                final E engine = fresh.get();
                final RecordingTriggers triggers = new RecordingTriggers();
                final ReplayContainer container = new ReplayContainer(engine, DEFAULT_APP_VERSION, DEFAULT_VALIDATOR,
                        triggers);
                final long max = Math.max(s, c);
                final int eventsMark;
                try {
                    final UnsafeBuffer img = new UnsafeBuffer(image[si]);
                    switch (path) {
                        case EQUAL -> {
                            container.restore(img, image[si].length, s, cursor[si]);
                            eventsMark = probe.events(engine).size();
                            log.replay(c, Long.MAX_VALUE, container);
                            equal++;
                        }
                        case SKIP -> {
                            container.restore(img, image[si].length, variant.imageLabel().applyAsLong(s, c),
                                    cursor[ci]);
                            final ClusterLogListener gated = SkipWindow.gate(variant.skipRule().apply(s), container);
                            log.replay(c, s, gated);
                            eventsMark = probe.events(engine).size();
                            log.replay(s, Long.MAX_VALUE, gated);
                            skip++;
                        }
                        default -> {
                            container.restore(img, image[si].length, s, cursor[si]);
                            log.replay(s, variant.tailBound().applyAsLong(c), variant.tailDecorator().apply(container));
                            eventsMark = probe.events(engine).size();
                            log.replay(c, Long.MAX_VALUE, container);
                            tail++;
                        }
                    }
                } catch (final RuntimeException failure) {
                    final AssertionError error = new AssertionError(where + ": leg failed: " + failure);
                    error.initCause(failure);
                    throw error;
                }

                // (1) image bytes
                final byte[] got = imageOf(container, scratch, where);
                if (!Arrays.equals(finalImage, got)) {
                    throw new AssertionError(where + ": final image bytes differ from the baseline");
                }
                // (2) cursor
                final LogCursor gotCursor = container.cursor();
                if (!finalCursor.equals(gotCursor)) {
                    throw new AssertionError(where + ": final cursor " + gotCursor + " differs from the baseline "
                            + finalCursor);
                }
                // (3) events after max(s, c)
                final List<String> legEvents = probe.events(engine);
                final List<String> legSuffix = legEvents.subList(eventsMark, legEvents.size());
                final int baselineMark = eventsAt[Math.max(si, ci)];
                final List<String> baselineSuffix = baselineEvents.subList(baselineMark, baselineEvents.size());
                if (!baselineSuffix.equals(legSuffix)) {
                    throw new AssertionError(where + ": events after " + max + " differ:\n"
                            + TextDiff.diff(String.join("\n", baselineSuffix), String.join("\n", legSuffix)));
                }
                // (4) triggers
                if (path == Path.SKIP) {
                    for (final Trigger t : triggers.fired) {
                        if (t.logPosition() < s) {
                            throw new AssertionError(where + ": snapshot trigger fired at " + t.logPosition()
                                    + " inside the SKIP window (image at " + s + ")");
                        }
                    }
                }
                final long from = path == Path.EQUAL ? c + 1 : max;
                final List<Trigger> expected = from(allTriggers, from);
                final List<Trigger> actual = from(triggers.fired, from);
                if (!expected.equals(actual)) {
                    throw new AssertionError(where + ": snapshot triggers from " + from + " are " + actual
                            + ", baseline " + expected);
                }
            }
        }
        return new Report(ends.length, equal + skip + tail, equal, skip, tail, finalImage.length);
    }

    private static List<Trigger> from(final List<Trigger> triggers, final long position) {
        final List<Trigger> result = new ArrayList<>();
        for (final Trigger t : triggers) {
            if (t.logPosition() >= position) {
                result.add(t);
            }
        }
        return result;
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
