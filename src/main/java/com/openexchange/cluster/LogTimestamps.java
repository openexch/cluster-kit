// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * One time format for every line a process writes to its log: ISO-8601 in UTC with milliseconds at the start of the
 * line, {@code 2026-11-27T11:44:31.654Z [SNAPSHOT] ...}. A log viewer can then order and search the lines of every
 * member by time without knowing which component wrote them.
 *
 * <p>{@link #install()} wraps {@code System.out} and {@code System.err} once, at the start of a process. Every line
 * written through them gets the time it was completed, whoever wrote it (the engine's {@code [TAG]} lines, a
 * library, the head line of a printed stack trace), with three exceptions that keep the line as it is:</p>
 * <ul>
 *   <li>a line that already starts with a time in this format (a logger that stamps its own lines with
 *       {@link #now()});</li>
 *   <li>a continuation line, which belongs to the line above it: it starts with a tab or a space (a stack frame,
 *       an indented block), with {@code Caused by: } or with {@code Suppressed: };</li>
 *   <li>an empty line.</li>
 * </ul>
 *
 * <p>The text after the time is not changed, so whatever looks for a tag or a phrase in a line still finds it.</p>
 *
 * <h2>How a line reaches the stream below</h2>
 *
 * <p>A line is collected in a buffer of {@value #LINE_BUFFER} bytes and handed to the stream below in one write
 * (time, text and newline together) when its newline arrives, then that stream is flushed. Two threads, or the
 * out and the err wrapper over one file, therefore never interleave inside a line. Whether a line gets a time is
 * decided then, from its first bytes, however many writes it arrived in. A line without a newline stays in the
 * buffer until its newline, or until {@code flush()} is called on the stream, which writes what there is. A line
 * longer than the buffer is written in pieces of the buffer's size as it fills: the time once, before the first
 * piece, and another thread's line may then fall between two pieces. Nothing is allocated per line.</p>
 */
public final class LogTimestamps {

    /** The length of a time: {@code 2026-11-27T11:44:31.654Z}. */
    public static final int LENGTH = 24;

    /** The bytes of one line that are collected before it is written; a longer line is written in pieces. */
    static final int LINE_BUFFER = 8192;

    private static final byte[] CAUSED_BY = "Caused by: ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUPPRESSED = "Suppressed: ".getBytes(StandardCharsets.US_ASCII);

    private LogTimestamps() {
    }

    /**
     * Wrap {@code System.out} and {@code System.err} so every line starts with its time. Idempotent. Call first in
     * a process's {@code main}, before any thread writes.
     */
    public static synchronized void install() {
        if (!(System.out instanceof Stamped)) {
            System.setOut(stamping(System.out));
        }
        if (!(System.err instanceof Stamped)) {
            System.setErr(stamping(System.err));
        }
    }

    /** A stream that writes to {@code target} with the time at the start of every line (see the class comment). */
    public static PrintStream stamping(final PrintStream target) {
        return new Stamped(target);
    }

    /** The time now, in the format of the lines. */
    public static String now() {
        return format(System.currentTimeMillis());
    }

    /** {@code epochMs} in the format of the lines. */
    public static String format(final long epochMs) {
        final byte[] out = new byte[LENGTH];
        format(epochMs, out, 0);
        return new String(out, StandardCharsets.US_ASCII);
    }

    /** Writes the {@value #LENGTH} bytes of {@code epochMs} into {@code out} at {@code offset}; no allocation. */
    static void format(final long epochMs, final byte[] out, final int offset) {
        final long days = Math.floorDiv(epochMs, 86_400_000L);
        final int msOfDay = (int) Math.floorMod(epochMs, 86_400_000L);
        // days since 1970-01-01 to a civil date (the proleptic Gregorian calendar)
        final long z = days + 719_468;
        final long era = Math.floorDiv(z, 146_097);
        final int dayOfEra = (int) (z - era * 146_097);
        final int yearOfEra = (dayOfEra - dayOfEra / 1_460 + dayOfEra / 36_524 - dayOfEra / 146_096) / 365;
        final int dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100);
        final int mp = (5 * dayOfYear + 2) / 153;
        final int day = dayOfYear - (153 * mp + 2) / 5 + 1;
        final int month = mp < 10 ? mp + 3 : mp - 9;
        final int year = (int) (yearOfEra + era * 400) + (month <= 2 ? 1 : 0);
        digits(out, offset, year, 4);
        out[offset + 4] = '-';
        digits(out, offset + 5, month, 2);
        out[offset + 7] = '-';
        digits(out, offset + 8, day, 2);
        out[offset + 10] = 'T';
        digits(out, offset + 11, msOfDay / 3_600_000, 2);
        out[offset + 13] = ':';
        digits(out, offset + 14, msOfDay / 60_000 % 60, 2);
        out[offset + 16] = ':';
        digits(out, offset + 17, msOfDay / 1_000 % 60, 2);
        out[offset + 19] = '.';
        digits(out, offset + 20, msOfDay % 1_000, 3);
        out[offset + 23] = 'Z';
    }

    private static void digits(final byte[] out, final int offset, final int value, final int width) {
        int v = value;
        for (int i = width - 1; i >= 0; i--) {
            out[offset + i] = (byte) ('0' + v % 10);
            v /= 10;
        }
    }

    /** Whether {@code [from, to)} starts with a time in the format of the lines. */
    static boolean startsWithTime(final byte[] b, final int from, final int to) {
        if (to - from < LENGTH) {
            return false;
        }
        for (int i = 0; i < LENGTH; i++) {
            final byte c = b[from + i];
            final boolean ok = switch (i) {
                case 4, 7 -> c == '-';
                case 10 -> c == 'T';
                case 13, 16 -> c == ':';
                case 19 -> c == '.';
                case 23 -> c == 'Z';
                default -> c >= '0' && c <= '9';
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private static final class Stamped extends PrintStream {
        Stamped(final PrintStream target) {
            // no auto flush here: the stamper flushes the stream below itself, once per complete line
            super(new Stamper(target), false, target.charset());
        }
    }

    /**
     * Collects one line and writes it to the stream below in one write, with the time before it unless the line
     * is one of the three exceptions.
     */
    private static final class Stamper extends OutputStream {
        private static final int STAMP = LENGTH + 1;

        private final OutputStream out;
        /** {@code [0, STAMP)} is kept for the time; the line's bytes follow. */
        private final byte[] line = new byte[STAMP + LINE_BUFFER];
        private int length;
        /** A piece of the current line was already written (a flush, or a line longer than the buffer). */
        private boolean begun;

        Stamper(final OutputStream out) {
            this.out = out;
            line[LENGTH] = ' ';
        }

        @Override
        public synchronized void write(final int b) throws IOException {
            add((byte) b);
        }

        @Override
        public synchronized void write(final byte[] b, final int off, final int len) throws IOException {
            for (int i = off, end = off + len; i < end; i++) {
                add(b[i]);
            }
        }

        private void add(final byte c) throws IOException {
            if (length == LINE_BUFFER) {
                emit(); // a line longer than the buffer: this piece now, the rest follows
            }
            line[STAMP + length++] = c;
            if (c == '\n') {
                emit();
                begun = false;
                out.flush();
            }
        }

        /** Writes what is collected in one write: the time first when this is the start of a line that gets one. */
        private void emit() throws IOException {
            if (length == 0) {
                return;
            }
            if (!begun && stamped()) {
                format(System.currentTimeMillis(), line, 0);
                out.write(line, 0, STAMP + length);
            } else {
                out.write(line, STAMP, length);
            }
            begun = true;
            length = 0;
        }

        /** Whether the collected start of a line gets a time. */
        private boolean stamped() {
            final byte first = line[STAMP];
            if (first == '\n' || first == '\r' || first == '\t' || first == ' ') {
                return false;
            }
            final int end = STAMP + length;
            return !startsWith(line, STAMP, end, CAUSED_BY) && !startsWith(line, STAMP, end, SUPPRESSED)
                && !startsWithTime(line, STAMP, end);
        }

        private static boolean startsWith(final byte[] b, final int from, final int to, final byte[] prefix) {
            if (to - from < prefix.length) {
                return false;
            }
            for (int i = 0; i < prefix.length; i++) {
                if (b[from + i] != prefix[i]) {
                    return false;
                }
            }
            return true;
        }

        /** Writes a line that has no newline yet, as far as it is collected, and flushes the stream below. */
        @Override
        public synchronized void flush() throws IOException {
            emit();
            out.flush();
        }

        @Override
        public synchronized void close() throws IOException {
            emit();
            out.close();
        }
    }
}
