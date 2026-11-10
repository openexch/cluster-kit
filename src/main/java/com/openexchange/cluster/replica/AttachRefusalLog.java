// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.agrona.concurrent.NanoClock;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * The follower's log of refused replay attaches ({@code archive.startReplay} of L throwing). Follower thread only.
 *
 * <p>Deduplicated on the reason text (the exception's string with the archive's correlation id
 * removed, so each attempt's new id does not make a new line): one line per distinct reason, plus a count line every
 * {@link #COUNT_PERIOD_MS} while refusals continue, and a last count when an attach succeeds. The readiness detail and
 * {@code replica_attach_failures_total} still see every refusal.</p>
 */
final class AttachRefusalLog {

    /** The period of the count line (a log cadence, not a bound). */
    static final long COUNT_PERIOD_MS = 30_000;
    /** Distinct reasons remembered (least recently seen dropped first): a memory guard for the log only. */
    private static final int REASONS_REMEMBERED = 256;
    private static final Pattern CORRELATION_ID = Pattern.compile("correlationId=-?\\d+");

    private final PrintStream out;
    private final NanoClock clock;
    private final long periodNs = TimeUnit.MILLISECONDS.toNanos(COUNT_PERIOD_MS);
    private final Map<String, Boolean> seen = new LinkedHashMap<>(16, 0.75f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
            return size() > REASONS_REMEMBERED;
        }
    };
    private final Map<String, Boolean> windowReasons = new LinkedHashMap<>();
    private long windowStartNs;
    private long windowCount;
    private long total;
    private String lastReason;

    AttachRefusalLog(final PrintStream out, final NanoClock clock) {
        this.out = Objects.requireNonNull(out, "out");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** The reason text of a refusal: {@code text} without the archive's correlation id. */
    static String reason(final String text) {
        return CORRELATION_ID.matcher(text).replaceAll("correlationId=*");
    }

    /** One refused attach of {@code l} from {@code from}; {@code text} is the exception's string. */
    void refused(final long l, final long from, final String text) {
        final long now = clock.nanoTime();
        if (windowCount > 0 && now - windowStartNs >= periodNs) {
            count(now);
        }
        final String reason = reason(text);
        if (windowCount == 0) {
            windowStartNs = now;
        }
        windowCount++;
        total++;
        windowReasons.put(reason, Boolean.TRUE);
        lastReason = reason;
        if (seen.put(reason, Boolean.TRUE) == null) {
            out.println("[REPLICA] replay of L=" + l + " from " + from + " refused: " + text
                    + " (first of this reason; counted every " + COUNT_PERIOD_MS + " ms)");
        }
    }

    /** An attach succeeded: the pending count is logged. */
    void attached() {
        if (windowCount > 0) {
            count(clock.nanoTime());
        }
    }

    private void count(final long now) {
        out.println("[REPLICA] replay refusals: " + windowCount + " refused attaches in the last "
                + TimeUnit.NANOSECONDS.toMillis(now - windowStartNs) + " ms, " + windowReasons.size()
                + " distinct reasons (total " + total + "); last: " + lastReason);
        windowCount = 0;
        windowReasons.clear();
    }
}
