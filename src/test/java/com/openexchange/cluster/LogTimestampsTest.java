// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** The time at the start of every log line: its format, which lines get it, and that the text is not changed. */
public class LogTimestampsTest {

    private static final Pattern STAMPED = Pattern.compile(
        "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z .*");

    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final PrintStream out = LogTimestamps.stamping(new PrintStream(bytes, true, StandardCharsets.UTF_8));

    private String[] lines() {
        return bytes.toString(StandardCharsets.UTF_8).split("\n", -1);
    }

    @Test
    public void theFormatIsIso8601UtcWithMilliseconds() {
        assertEquals("1970-01-01T00:00:00.000Z", LogTimestamps.format(0));
        assertEquals("2026-11-27T11:44:31.654Z", LogTimestamps.format(Instant.parse("2026-11-27T11:44:31.654Z")
            .toEpochMilli()));
        assertEquals("2024-02-29T23:59:59.999Z", LogTimestamps.format(Instant.parse("2024-02-29T23:59:59.999Z")
            .toEpochMilli()));
        assertEquals("2100-03-01T00:00:00.000Z", LogTimestamps.format(Instant.parse("2100-03-01T00:00:00Z")
            .toEpochMilli()));
        final Random random = new Random(7);
        for (int i = 0; i < 200_000; i++) {
            final long ms = (long) (random.nextDouble() * 4_102_444_800_000L); // 1970 to 2100
            final String text = LogTimestamps.format(ms);
            assertEquals(text, ms, Instant.parse(text).toEpochMilli());
            assertEquals(LogTimestamps.LENGTH, text.length());
        }
        // what a standard parser reads back is what was written
        final String now = LogTimestamps.now();
        assertEquals(now, Instant.parse(now).toEpochMilli(),
            DateTimeFormatter.ISO_INSTANT.parse(now, Instant::from).toEpochMilli());
        assertTrue(Math.abs(Instant.parse(now).toEpochMilli() - System.currentTimeMillis()) < 5_000);
    }

    @Test
    public void everyLineGetsItsTimeAndKeepsItsText() {
        out.println("[SNAPSHOT] onTakeSnapshot called");
        out.println("[JOURNAL-RETENTION] settlement: mode=readers state=VALID");
        out.printf("METRICS: node /metrics on port %d%n", 9100);
        out.print("SERVICE: part one, ");
        out.print("part two");
        out.println();
        out.print('x');
        out.write('y');
        out.println();
        final String[] lines = lines();
        assertEquals(6, lines.length);
        for (int i = 0; i < 5; i++) {
            assertTrue(lines[i], STAMPED.matcher(lines[i]).matches());
        }
        assertEquals("[SNAPSHOT] onTakeSnapshot called", lines[0].substring(LogTimestamps.LENGTH + 1));
        assertTrue(lines[1].contains("[JOURNAL-RETENTION] settlement: mode=readers state=VALID"));
        assertEquals("METRICS: node /metrics on port 9100", lines[2].substring(LogTimestamps.LENGTH + 1));
        assertEquals("one time for a line written in parts", "SERVICE: part one, part two",
            lines[3].substring(LogTimestamps.LENGTH + 1));
        assertEquals("xy", lines[4].substring(LogTimestamps.LENGTH + 1));
        assertEquals("", lines[5]);
    }

    /**
     * The head line of a throwable is a line like any other and gets its time, whether it is printed alone
     * ({@code println(t)}), by a bare {@code printStackTrace()}, or with a message. Only the frames, the
     * {@code Caused by:} and the {@code Suppressed:} lines are continuations.
     */
    @Test
    public void aThrowablesHeadLineGetsItsTimeAndOnlyItsFramesAreContinuations() {
        final IllegalStateException t = new IllegalStateException("outer", new IllegalArgumentException("inner"));
        t.addSuppressed(new java.io.IOException("closing"));
        out.println(t);
        t.printStackTrace(out);
        new Error().printStackTrace(out);
        out.println("a.b.cError: a message that only looks like a throwable");
        out.println("Suppressed: at the line start");
        final String[] lines = lines();
        assertEquals("java.lang.IllegalStateException: outer", lines[0].substring(LogTimestamps.LENGTH + 1));
        assertTrue(lines[0], STAMPED.matcher(lines[0]).matches());
        assertTrue(lines[1], STAMPED.matcher(lines[1]).matches());
        assertEquals("java.lang.IllegalStateException: outer", lines[1].substring(LogTimestamps.LENGTH + 1));
        int frames = 0;
        int causes = 0;
        int suppressed = 0;
        int heads = 0;
        for (int i = 2; i < lines.length - 3; i++) {
            final String line = lines[i];
            if (STAMPED.matcher(line).matches()) {
                heads++;
                assertEquals("the only other head is the bare Error", "java.lang.Error",
                    line.substring(LogTimestamps.LENGTH + 1));
                continue;
            }
            assertTrue("a line without a time is a continuation: " + line, line.startsWith("\t")
                || line.startsWith("Caused by: "));
            if (line.startsWith("\tat ")) {
                frames++;
            }
            if (line.equals("Caused by: java.lang.IllegalArgumentException: inner")) {
                causes++;
            }
            if (line.equals("\tSuppressed: java.io.IOException: closing")) {
                suppressed++;
            }
        }
        assertTrue(frames > 6);
        assertEquals(1, causes);
        assertEquals(1, suppressed);
        assertEquals(1, heads);
        assertTrue(lines[lines.length - 3], STAMPED.matcher(lines[lines.length - 3]).matches());
        assertTrue(lines[lines.length - 3].endsWith(" a.b.cError: a message that only looks like a throwable"));
        assertEquals("Suppressed: at the line start", lines[lines.length - 2]);
    }

    /**
     * Each complete line reaches the stream below in ONE write (time, text, newline), so the out and the err
     * wrapper over one file never interleave inside a line.
     */
    @Test
    public void aLineIsOneWriteAndConcurrentWritersNeverInterleaveInsideALine() throws Exception {
        final java.util.List<String> writes = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        final java.io.OutputStream sinkBytes = new java.io.OutputStream() {
            @Override
            public void write(final int b) {
                writes.add(String.valueOf((char) b));
            }

            @Override
            public void write(final byte[] b, final int off, final int len) {
                writes.add(new String(b, off, len, StandardCharsets.UTF_8));
            }
        };
        // no buffering below: every write of a wrapper is one write of the sink
        final PrintStream sink = new PrintStream(sinkBytes, true, StandardCharsets.UTF_8);
        final PrintStream o = LogTimestamps.stamping(sink);
        final PrintStream e = LogTimestamps.stamping(sink);
        o.print("[TAG] in ");
        o.print("three ");
        o.println("parts");
        assertEquals("one write for a line that arrived in parts", 1, writes.size());
        assertTrue(writes.get(0), writes.get(0).endsWith(" [TAG] in three parts\n"));
        writes.clear();

        final int threads = 6;
        final int perThread = 2_000;
        final Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int id = t;
            final PrintStream stream = t % 2 == 0 ? o : e;
            ts[t] = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    if (i % 3 == 0) {
                        stream.printf("[W%d] line %d of %s%n", id, i, "a formatted line written in several parts");
                    } else {
                        stream.println("[W" + id + "] line " + i + " of a plain line");
                    }
                }
            });
        }
        for (final Thread t : ts) {
            t.start();
        }
        for (final Thread t : ts) {
            t.join();
        }
        assertEquals("one write per line", threads * perThread, writes.size());
        final int[] next = new int[threads];
        final Pattern whole = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z \\[W(\\d)\\] line (\\d+) of [a-z ]+\n$");
        for (final String w : writes) {
            final java.util.regex.Matcher m = whole.matcher(w);
            assertTrue("a whole line with one time: " + w, m.matches());
            final int id = Integer.parseInt(m.group(1));
            assertEquals("writer " + id + " in order", next[id]++, Integer.parseInt(m.group(2)));
        }
    }

    /**
     * One 20 KB print of many lines with a stack trace in it, larger than the buffer and than the chunks a
     * PrintStream writes in: every head line gets one time, every continuation none, and no byte is lost.
     */
    @Test
    public void aTwentyKilobyteMultiLinePrintIsStampedLineByLine() {
        final java.io.StringWriter trace = new java.io.StringWriter();
        new IllegalStateException("deep", new RuntimeException("cause")).printStackTrace(
            new java.io.PrintWriter(trace, true));
        final StringBuilder text = new StringBuilder();
        int n = 0;
        while (text.length() < 20_000) {
            text.append("[BLOCK] line ").append(n++).append(" with some text to fill it up to a useful length\n");
            if (n % 40 == 0) {
                text.append(trace);
            }
        }
        final String printed = text.toString();
        assertTrue(printed.length() > 20_000 && printed.contains("\tat ") && printed.contains("Caused by: "));
        out.print(printed);
        final String[] expected = printed.split("\n", -1);
        final String[] lines = lines();
        assertEquals(expected.length, lines.length);
        for (int i = 0; i < expected.length; i++) {
            final String e = expected[i];
            final boolean continuation = e.isEmpty() || e.startsWith("\t") || e.startsWith("Caused by: ");
            if (continuation) {
                assertEquals("line " + i, e, lines[i]);
            } else {
                assertTrue("line " + i + ": " + lines[i], STAMPED.matcher(lines[i]).matches());
                assertEquals("line " + i, e, lines[i].substring(LogTimestamps.LENGTH + 1));
            }
        }
    }

    /** A line longer than the buffer: the time once, at its start, and every byte of it; then the next line. */
    @Test
    public void aLineLongerThanTheBufferGetsOneTimeAndLosesNothing() {
        final String longLine = "[LONG] " + "x".repeat(3 * LogTimestamps.LINE_BUFFER + 17);
        final String longContinuation = "\t" + "y".repeat(2 * LogTimestamps.LINE_BUFFER + 5);
        out.println(longLine);
        out.println(longContinuation);
        out.println("[NEXT] short");
        final String[] lines = lines();
        assertEquals(4, lines.length);
        assertTrue(STAMPED.matcher(lines[0]).matches());
        assertEquals(longLine, lines[0].substring(LogTimestamps.LENGTH + 1));
        assertEquals("no time inside a long continuation line", longContinuation, lines[1]);
        assertEquals("[NEXT] short", lines[2].substring(LogTimestamps.LENGTH + 1));
    }

    /** A line without its newline is written on flush, with its time; the rest of it follows without one. */
    @Test
    public void aPartialLineIsWrittenOnFlush() {
        out.print("[PART] begun");
        assertEquals("held until its newline or a flush", "", bytes.toString(StandardCharsets.UTF_8));
        out.flush();
        final String first = bytes.toString(StandardCharsets.UTF_8);
        assertTrue(first, first.endsWith(" [PART] begun") && STAMPED.matcher(first).matches());
        out.println(", and ended");
        final String[] lines = lines();
        assertEquals(2, lines.length);
        assertEquals("[PART] begun, and ended", lines[0].substring(LogTimestamps.LENGTH + 1));
    }

    @Test
    public void aLineThatCarriesItsTimeIsNotStampedTwiceAndAnEmptyLineStaysEmpty() {
        final String own = LogTimestamps.format(1_800_000_000_000L) + " [ERROR] AeronCluster - Aeron error #1";
        out.println(own);
        out.println();
        out.println("  indented detail");
        out.println("2026-11-27 11:44:31.654 [INFO] another format is a line like any other");
        final String[] lines = lines();
        assertEquals(own, lines[0]);
        assertEquals("", lines[1]);
        assertEquals("  indented detail", lines[2]);
        assertTrue(lines[3], STAMPED.matcher(lines[3]).matches());
        assertTrue(lines[3].endsWith(" 2026-11-27 11:44:31.654 [INFO] another format is a line like any other"));
    }

    @Test
    public void textOutsideAsciiPassesUnchanged() {
        out.println("[TAG] größe 価格 ✓");
        assertEquals("[TAG] größe 価格 ✓", lines()[0].substring(LogTimestamps.LENGTH + 1));
    }

    @Test
    public void installWrapsBothStreamsOnce() {
        final PrintStream savedOut = System.out;
        final PrintStream savedErr = System.err;
        final ByteArrayOutputStream o = new ByteArrayOutputStream();
        final ByteArrayOutputStream e = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(o, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(e, true, StandardCharsets.UTF_8));
            LogTimestamps.install();
            final PrintStream wrappedOut = System.out;
            final PrintStream wrappedErr = System.err;
            LogTimestamps.install();
            assertSame("a second install changes nothing", wrappedOut, System.out);
            assertSame(wrappedErr, System.err);
            System.out.println("[PRUNE] watching for snapshots");
            System.err.println("SERVICE CRITICAL: something");
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
        }
        final String outLine = o.toString(StandardCharsets.UTF_8).trim();
        final String errLine = e.toString(StandardCharsets.UTF_8).trim();
        assertTrue(outLine, STAMPED.matcher(outLine).matches() && outLine.endsWith(" [PRUNE] watching for snapshots"));
        assertTrue(errLine, STAMPED.matcher(errLine).matches() && errLine.endsWith(" SERVICE CRITICAL: something"));
    }
}
