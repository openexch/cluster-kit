// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The attach-refusal log line is deduplicated on its reason text (the archive's correlation id
 * removed): one line per distinct reason, plus a count every 30 s, not one line per correlation id.
 */
public class AttachRefusalLogTest {

    static final int REFUSALS = 241;
    static final long BACKOFF_MS = 250;

    static String refusal(final long correlationId, final String reason) {
        return "io.aeron.archive.client.ArchiveException: ERROR - response for correlationId=" + correlationId
                + ", error: " + reason;
    }

    @Test
    public void oneLinePerDistinctReasonPlusAPeriodicCount() {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final long[] now = {1};
        final AttachRefusalLog log = new AttachRefusalLog(new PrintStream(bytes, true, StandardCharsets.UTF_8),
                () -> now[0]);
        final String[] reasons = {"max concurrent replays reached: 1", "requested replay start position=8864 must be"
                + " less than the limit position=4096 for recording 3"};
        for (int i = 0; i < REFUSALS; i++) {
            now[0] += TimeUnit.MILLISECONDS.toNanos(BACKOFF_MS);
            log.refused(3, 8864, refusal(1_000 + i, reasons[i % 2]));
        }
        final List<String> lines = Arrays.stream(bytes.toString(StandardCharsets.UTF_8).split("\n"))
                .filter(s -> !s.isEmpty()).collect(Collectors.toList());
        final long reasonLines = lines.stream().filter(s -> s.contains(" refused: ")).count();
        if (reasonLines != reasons.length) {
            throw new AssertionError(reasonLines + " attach-refusal lines for " + reasons.length
                    + " distinct reasons in " + REFUSALS + " refusals (expected " + reasons.length + ")");
        }
        final List<String> counts = lines.stream().filter(s -> s.startsWith("[REPLICA] replay refusals: "))
                .collect(Collectors.toList());
        final long seconds = REFUSALS * BACKOFF_MS / 1_000;
        assertEquals("a count every 30 s over " + seconds + " s: " + counts, seconds / 30, counts.size());
        assertTrue(counts.get(0), counts.get(0).contains(" 120 refused attaches in the last 30000 ms"));
        assertTrue(counts.get(0), counts.get(0).contains("2 distinct reasons"));
        log.attached();
        assertEquals("a pending count is flushed on attach", counts.size() + 1L, bytes.toString(StandardCharsets.UTF_8)
                .lines().filter(s -> s.startsWith("[REPLICA] replay refusals: ")).count());
        System.out.println("GREEN " + REFUSALS + " refusals over " + seconds + " s: " + reasonLines
                + " reason lines, " + counts.size() + " count lines");
    }
}
