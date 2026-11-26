// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.cluster.shadow.ShadowReplay;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import org.agrona.concurrent.NanoClock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Disk reclamation for a member's journal recordings, and the only class that purges one. A journal record is
 * deleted from a member only when every registered reader ({@link JournalReaders}) has durably applied it, and the
 * member's own journal tail reads the same afterwards. A purge cannot be undone, so every unknown answers "hold", and
 * nothing is remembered between two passes: each pass works from the reader files, the archive and the member's
 * identity as they are now.
 *
 * <h2>The bound</h2>
 *
 * <p>The key is {@code (genesisId, journalSeq)} per journal. {@link #evaluate} gives the state of one pass:</p>
 * <table>
 *   <caption>State and bound</caption>
 *   <tr><th>Condition</th><th>State</th><th>Bound</th></tr>
 *   <tr><td>mode {@code off}</td><td>OFF</td><td>none, nothing is purged</td></tr>
 *   <tr><td>no recording of this genesis with bytes</td><td>NO_JOURNAL</td><td>none, nothing to do</td></tr>
 *   <tr><td>the member has no recorded genesis yet</td><td>UNBOUND</td><td>hold</td></tr>
 *   <tr><td>no reader file</td><td>NO_READERS</td><td>hold</td></tr>
 *   <tr><td>a required reader has no file</td><td>MISSING</td><td>hold</td></tr>
 *   <tr><td>a reader file is invalid</td><td>INVALID</td><td>hold</td></tr>
 *   <tr><td>a reader file names another engine, cluster, genesis, journal or reader</td><td>FOREIGN</td><td>hold</td></tr>
 *   <tr><td>the member's own journal tail cannot be read</td><td>TAIL_UNKNOWN</td><td>hold</td></tr>
 *   <tr><td>a reader's bound is above the member's own tail</td><td>AHEAD</td><td>hold</td></tr>
 *   <tr><td>the off-box watermark is required and not VALID</td><td>OFFBOX_HOLD</td><td>hold</td></tr>
 *   <tr><td>otherwise</td><td>VALID</td><td>{@code safeSeq} = the minimum over the readers of
 *       {@code max(applied, released)}</td></tr>
 * </table>
 *
 * <h2>From {@code journalSeq} to bytes</h2>
 *
 * <p>The recording is its own index. A purge removes whole segments, and a segment base is a term boundary, so a
 * frame starts there: the first record at a base {@code B} is read through a bounded, session-scoped replay. Inside
 * one recording {@code journalSeq} only rises, so every record below {@code B} has a sequence below that record's.
 * {@code B} qualifies when the record is a whole record of the journal's schema, its sequence is at most
 * {@code safeSeq + 1}, its epoch is the member's genesis (where the journal has one), its log position is at or below
 * the off-box floor (where that is required), and its sequence fits between the other records read from the
 * recording in the same pass. A sequence that does not rise marks the recording an ANOMALY: it is not purged. An
 * unreadable record at a segment base does not qualify that base, and an unreadable record at the recording's start
 * leaves the recording alone for the pass; both are counted with the anomalies and said on a CRITICAL line, so a
 * journal that is VALID and still cannot be cut is seen.</p>
 *
 * <ul>
 *   <li>An active recording is cut to the greatest qualifying base that leaves two whole segments below its head.</li>
 *   <li>A stopped recording whose last sequence is at or below {@code safeSeq} is deleted whole, unless it is the
 *       recording that carries the member's tail: the tail a member's restore rules read is the same after a pass
 *       as before it. Any other stopped recording is cut to the greatest qualifying base at or below the base of its
 *       last segment, so its last record stays readable.</li>
 *   <li>A recording whose records carry another epoch is never touched.</li>
 * </ul>
 */
public final class JournalRetention {

    static final String LOG = "[JOURNAL-RETENTION] ";

    /** What a member does with one journal. The code is the value of the mode gauge. */
    public enum Mode {
        OFF(0, "off"), REPORT(1, "report"), READERS(2, "readers");

        private final int code;
        private final String text;

        Mode(final int code, final String text) {
            this.code = code;
            this.text = text;
        }

        public int code() {
            return code;
        }

        public String text() {
            return text;
        }
    }

    /** What one pass found. The code is the value of the state gauge. */
    public enum State {
        VALID(0), OFF(1), NO_JOURNAL(2), UNBOUND(3), NO_READERS(4), INVALID(5), FOREIGN(6), TAIL_UNKNOWN(7), AHEAD(8),
        MISSING(9), OFFBOX_HOLD(10);

        private final int code;

        State(final int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    /**
     * Negative controls only: each switches one rule off so a test can show that its assertion sees the rule. Static
     * because the class runs inside an engine a test cannot reach into; a test sets one and clears it in
     * {@code finally}. Never set in production code.
     */
    public static final class Seams {
        /** NO_READERS is "no constraint": the bound is the member's own tail. */
        public static volatile boolean noReadersIsNoConstraint;
        /** A reader bound above the member's tail is accepted. */
        public static volatile boolean aheadCheckOff;
        /** The bound is the maximum over the readers instead of the minimum. */
        public static volatile boolean minIsMax;
        /** A recording or a boundary of another epoch is treated as this genesis'. */
        public static volatile boolean epochCheckOff;
        /** An active recording is cut up to its head segment. */
        public static volatile boolean clampOff;
        /** A stopped recording is deleted whole even when it carries the member's tail. */
        public static volatile boolean tailCoverOff;
        /** The off-box floor is not applied to a boundary or to a last record. */
        public static volatile boolean offBoxBoundOff;

        private Seams() {
        }

        public static void reset() {
            noReadersIsNoConstraint = false;
            aheadCheckOff = false;
            minIsMax = false;
            epochCheckOff = false;
            clampOff = false;
            tailCoverOff = false;
            offBoxBoundOff = false;
        }
    }

    // ------------------------------------------------------------------ settings

    /** The node-local settings of journal retention, read once at boot; a bad value refuses the start. */
    public static final class Settings {
        public static final String MODE_ENV_PREFIX = "JOURNAL_RETENTION_";
        public static final String MODE_PROP_PREFIX = "journal.retention.";
        public static final String REQUIRED_ENV_PREFIX = "JOURNAL_RETENTION_REQUIRED_READERS_";
        public static final String REQUIRED_PROP_PREFIX = "journal.retention.required.readers.";
        public static final String POLL_MS_ENV = "JOURNAL_RETENTION_POLL_MS";
        public static final String POLL_MS_PROP = "journal.retention.poll.ms";
        public static final long DEFAULT_POLL_MS = 60_000;
        public static final long MIN_POLL_MS = 1_000;
        public static final long MAX_POLL_MS = 3_600_000;
        public static final String PASS_TIMEOUT_MS_ENV = "JOURNAL_RETENTION_PASS_TIMEOUT_MS";
        public static final String PASS_TIMEOUT_MS_PROP = "journal.retention.pass.timeout.ms";
        public static final long DEFAULT_PASS_TIMEOUT_MS = 10_000;
        public static final long MIN_PASS_TIMEOUT_MS = 1_000;
        public static final long MAX_PASS_TIMEOUT_MS = 600_000;
        /** Retained bytes of one journal above which a CRITICAL line is printed each pass. An alarm, not a limit. */
        public static final String ALARM_BYTES_ENV = "JOURNAL_RETENTION_ALARM_BYTES";
        public static final String ALARM_BYTES_PROP = "journal.retention.alarm.bytes";
        public static final long DEFAULT_ALARM_BYTES = 2L << 30;
        public static final long MIN_ALARM_BYTES = 1L << 20;
        public static final long MAX_ALARM_BYTES = 1L << 40;
        /** Age of the oldest reader file above which a CRITICAL line is printed. It does not change the bound. */
        public static final String STALE_MS_ENV = "JOURNAL_RETENTION_STALE_MS";
        public static final String STALE_MS_PROP = "journal.retention.stale.ms";
        public static final long DEFAULT_STALE_MS = 3_600_000;
        public static final long MIN_STALE_MS = 1_000;
        public static final long MAX_STALE_MS = 604_800_000;

        /** A setting's raw text by its env name and its property name; null when unset. */
        @FunctionalInterface
        public interface Source {
            Source PROCESS = (env, prop) -> {
                final String e = System.getenv(env);
                return e != null && !e.isEmpty() ? e : System.getProperty(prop);
            };

            String get(String env, String prop);
        }

        private final Map<String, Mode> modes;
        private final Map<String, Set<String>> requiredReaders;
        private final long pollMs;
        private final long passTimeoutMs;
        private final long alarmBytes;
        private final long staleMs;

        private Settings(final Source source, final String... journals) {
            final Map<String, Mode> m = new LinkedHashMap<>();
            final Map<String, Set<String>> r = new LinkedHashMap<>();
            for (final String journal : journals) {
                final String upper = journal.toUpperCase(Locale.ROOT);
                m.put(journal, mode(MODE_ENV_PREFIX + upper, source.get(MODE_ENV_PREFIX + upper,
                    MODE_PROP_PREFIX + journal)));
                r.put(journal, names(REQUIRED_ENV_PREFIX + upper, source.get(REQUIRED_ENV_PREFIX + upper,
                    REQUIRED_PROP_PREFIX + journal)));
            }
            this.modes = Collections.unmodifiableMap(m);
            this.requiredReaders = Collections.unmodifiableMap(r);
            this.pollMs = ranged(source, POLL_MS_ENV, POLL_MS_PROP, DEFAULT_POLL_MS, MIN_POLL_MS, MAX_POLL_MS);
            this.passTimeoutMs = ranged(source, PASS_TIMEOUT_MS_ENV, PASS_TIMEOUT_MS_PROP, DEFAULT_PASS_TIMEOUT_MS,
                MIN_PASS_TIMEOUT_MS, MAX_PASS_TIMEOUT_MS);
            this.alarmBytes = ranged(source, ALARM_BYTES_ENV, ALARM_BYTES_PROP, DEFAULT_ALARM_BYTES, MIN_ALARM_BYTES,
                MAX_ALARM_BYTES);
            this.staleMs = ranged(source, STALE_MS_ENV, STALE_MS_PROP, DEFAULT_STALE_MS, MIN_STALE_MS, MAX_STALE_MS);
        }

        /**
         * The settings of this process for the named journals (environment first, then the system property, then the
         * default).
         *
         * @throws IllegalArgumentException naming a malformed or out-of-range setting
         */
        public static Settings fromEnv(final String... journals) {
            return new Settings(Source.PROCESS, journals);
        }

        /** As {@link #fromEnv} from a given source (tests; a host that parses its settings itself). */
        public static Settings parse(final Source source, final String... journals) {
            return new Settings(Objects.requireNonNull(source, "source"), journals);
        }

        public Mode mode(final String journal) {
            final Mode m = modes.get(journal);
            if (m == null) {
                throw new IllegalArgumentException("no settings were read for journal '" + journal + "'");
            }
            return m;
        }

        public Set<String> requiredReaders(final String journal) {
            final Set<String> r = requiredReaders.get(journal);
            return r == null ? Set.of() : r;
        }

        public long pollMs() {
            return pollMs;
        }

        public long passTimeoutMs() {
            return passTimeoutMs;
        }

        public long alarmBytes() {
            return alarmBytes;
        }

        public long staleMs() {
            return staleMs;
        }

        private static Mode mode(final String name, final String raw) {
            if (raw == null || raw.isBlank()) {
                return Mode.OFF;
            }
            for (final Mode m : Mode.values()) {
                if (m.text.equals(raw.trim())) {
                    return m;
                }
            }
            throw new IllegalArgumentException(name + " must be off, report or readers, got '" + raw + "'");
        }

        private static Set<String> names(final String name, final String raw) {
            if (raw == null || raw.isBlank()) {
                return Set.of();
            }
            final Set<String> out = new TreeSet<>();
            for (final String part : raw.split(",", -1)) {
                final String n = part.trim();
                if (!JournalReaders.validName(n)) {
                    throw new IllegalArgumentException(name + " must be reader names separated by commas, got '" + raw
                        + "'");
                }
                out.add(n);
            }
            return Collections.unmodifiableSet(out);
        }

        private static long ranged(final Source source, final String env, final String prop, final long dflt,
                                   final long min, final long max) {
            final String raw = source.get(env, prop);
            if (raw == null || raw.isBlank()) {
                return dflt;
            }
            final long value;
            try {
                value = Long.parseLong(raw.trim());
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException(env + " must be a number in " + min + ".." + max + ", got '" + raw
                    + "'");
            }
            if (value < min || value > max) {
                throw new IllegalArgumentException(env + " must be " + min + ".." + max + ", got '" + raw + "'");
            }
            return value;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            for (final Map.Entry<String, Mode> e : modes.entrySet()) {
                final String upper = e.getKey().toUpperCase(Locale.ROOT);
                sb.append(MODE_ENV_PREFIX).append(upper).append('=').append(e.getValue().text()).append(' ')
                    .append(REQUIRED_ENV_PREFIX).append(upper).append('=')
                    .append(String.join(",", requiredReaders.get(e.getKey()))).append(' ');
            }
            return sb.append(POLL_MS_ENV).append('=').append(pollMs).append(' ').append(PASS_TIMEOUT_MS_ENV)
                .append('=').append(passTimeoutMs).append(' ').append(ALARM_BYTES_ENV).append('=').append(alarmBytes)
                .append(' ').append(STALE_MS_ENV).append('=').append(staleMs).toString();
        }
    }

    // ------------------------------------------------------------------ the archive, as the pass sees it

    /** One recording of the journal's stream. {@code end} is the stop position, or the recorded position when active. */
    public record Recording(long id, long start, long end, boolean active, int termLength, int segmentLength) {
        public long bytes() {
            return Math.max(0, end - start);
        }

        long segmentBase(final long position) {
            return AeronArchive.segmentFileBasePosition(start, position, termLength, segmentLength);
        }
    }

    /** The key of one record read from a recording, or why it could not be read. */
    public record Peek(JournalProfile.Key key, String unreadable) {
        public static Peek of(final JournalProfile.Key key) {
            return new Peek(Objects.requireNonNull(key, "key"), null);
        }

        public static Peek unreadable(final String why) {
            return new Peek(null, Objects.requireNonNull(why, "why"));
        }

        public boolean readable() {
            return key != null;
        }
    }

    /** What a pass asks of the member's archive. {@link Aeron} is the production one. */
    public interface Archive {
        /** Every recording on the profile's stream. */
        List<Recording> list();

        /** The first record at {@code position}, a position where a frame starts. */
        Peek first(Recording recording, long position, long deadlineNs);

        /** The last record of the recording. */
        Peek last(Recording recording, long deadlineNs);

        /** Deletes the segments below {@code newStartPosition}. @throws RuntimeException the archive refused */
        void purgeSegments(long recordingId, long newStartPosition);

        /** Deletes a stopped recording. @throws RuntimeException the archive refused */
        void purgeRecording(long recordingId);
    }

    /**
     * The member's own archive: listings, one-record replays ({@link ShadowReplay}, bounded and session scoped, one at
     * a time) and the two purge calls. A refused or failed replay is an unreadable record; any other failure (a lost
     * driver, a closed client) propagates to the caller.
     */
    public static final class AeronPort implements Archive {
        private static final int LIST_BATCH = 100;

        private final AeronArchive archive;
        private final ShadowReplay replay;
        private final JournalProfile profile;

        public AeronPort(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                         final JournalProfile profile) {
            this.archive = Objects.requireNonNull(archive, "archive");
            this.replay = new ShadowReplay(Objects.requireNonNull(aeron, "aeron"), archive,
                Objects.requireNonNull(clock, "clock"));
            this.profile = Objects.requireNonNull(profile, "profile");
        }

        @Override
        public List<Recording> list() {
            final List<long[]> rows = new ArrayList<>();
            long from = 0;
            while (true) {
                final int before = rows.size();
                final int n = archive.listRecordingsForUri(from, LIST_BATCH, profile.channelFragment(),
                    profile.streamId(), (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                        startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                        sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> rows.add(new long[] {
                            recordingId, startPosition, stopPosition, termBufferLength, segmentFileLength}));
                if (n == 0 || rows.size() == before) {
                    break;
                }
                from = rows.get(rows.size() - 1)[0] + 1;
            }
            final List<Recording> out = new ArrayList<>(rows.size());
            for (final long[] r : rows) {
                long end = r[2];
                boolean active = false;
                if (end == AeronArchive.NULL_POSITION) {
                    end = archive.getRecordingPosition(r[0]);
                    if (end == AeronArchive.NULL_POSITION) {
                        // stopped between the listing and now: its descriptor has the stop position
                        end = archive.getStopPosition(r[0]);
                    } else {
                        active = true;
                    }
                }
                out.add(new Recording(r[0], r[1], end, active, (int) r[3], (int) r[4]));
            }
            return out;
        }

        @Override
        public Peek first(final Recording r, final long position, final long deadlineNs) {
            final long window = Math.min(profile.maxFrameLength(), r.end() - position)
                & -FrameDescriptor.FRAME_ALIGNMENT;
            if (window < FrameDescriptor.FRAME_ALIGNMENT) {
                return Peek.unreadable("no whole frame at " + position + " below the end " + r.end());
            }
            final Peek[] found = {null};
            try {
                // the frame's length is not known before it is read: the window is the journal's largest record and
                // the read stops after the first message
                replay.readRange(r.id(), position, window, (buffer, offset, length, header) -> {
                    if (found[0] != null) {
                        return;
                    }
                    final int aligned = BitUtil.align(header.frameLength(), FrameDescriptor.FRAME_ALIGNMENT);
                    if (header.position() - aligned != position) {
                        found[0] = Peek.unreadable("the first message ends at " + header.position() + " with a frame of "
                            + aligned + " bytes: it does not start at " + position);
                        return;
                    }
                    try {
                        found[0] = Peek.of(profile.key(buffer, offset, length, header.frameLength()));
                    } catch (final JournalProfile.NotARecord notARecord) {
                        found[0] = Peek.unreadable(notARecord.getMessage());
                    }
                }, () -> found[0] != null, deadlineNs);
            } catch (final ArchiveException | ShadowReplay.ReplayTimeoutException
                           | ShadowReplay.ReplayJoinedLateException | ShadowReplay.ReplayEndedEarlyException
                           | IllegalStateException failed) {
                return Peek.unreadable(String.valueOf(failed));
            }
            return found[0] != null ? found[0] : Peek.unreadable("no message at " + position);
        }

        @Override
        public Peek last(final Recording r, final long deadlineNs) {
            final long stop = r.end();
            try {
                // the largest record first (a journal of one record size answers with one replay), then every
                // shorter aligned length: the archive refuses a start that is not a frame header
                final int max = profile.maxFrameLength();
                final int steps = max / FrameDescriptor.FRAME_ALIGNMENT;
                for (int i = 0; i < steps; i++) {
                    final int w = i == 0 ? max : i * FrameDescriptor.FRAME_ALIGNMENT;
                    final long from = stop - w;
                    if (from < r.start()) {
                        continue;
                    }
                    final Peek[] found = {null};
                    final int[] messages = {0};
                    final int[] frameLength = {0};
                    try {
                        replay.readRange(r.id(), from, w, (buffer, offset, length, header) -> {
                            messages[0]++;
                            frameLength[0] = header.frameLength();
                            try {
                                found[0] = Peek.of(profile.key(buffer, offset, length, header.frameLength()));
                            } catch (final JournalProfile.NotARecord notARecord) {
                                found[0] = Peek.unreadable(notARecord.getMessage());
                            }
                        }, deadlineNs);
                    } catch (final ArchiveException notAFrame) {
                        if (notAFrame.errorCode() == ArchiveException.INVALID_POSITION) {
                            continue; // not a frame header: another window
                        }
                        throw notAFrame;
                    }
                    if (messages[0] == 1 && found[0] != null
                        && BitUtil.align(frameLength[0], FrameDescriptor.FRAME_ALIGNMENT) == w) {
                        return found[0];
                    }
                }
                // the end is not one record frame (padding at a term end): the term holding the end, frame aligned
                final long termStart = Math.max(r.start(), (stop - 1) & -(long) r.termLength());
                final Peek[] found = {null};
                replay.readRange(r.id(), termStart, stop - termStart, (buffer, offset, length, header) -> {
                    try {
                        found[0] = Peek.of(profile.key(buffer, offset, length, header.frameLength()));
                    } catch (final JournalProfile.NotARecord notARecord) {
                        found[0] = Peek.unreadable(notARecord.getMessage());
                    }
                }, deadlineNs);
                return found[0] != null ? found[0]
                    : Peek.unreadable("no journal message in the last term of recording " + r.id());
            } catch (final ArchiveException | ShadowReplay.ReplayTimeoutException
                           | ShadowReplay.ReplayJoinedLateException | ShadowReplay.ReplayEndedEarlyException
                           | IllegalStateException failed) {
                return Peek.unreadable(String.valueOf(failed));
            }
        }

        @Override
        public void purgeSegments(final long recordingId, final long newStartPosition) {
            archive.purgeSegments(recordingId, newStartPosition);
        }

        @Override
        public void purgeRecording(final long recordingId) {
            archive.purgeRecording(recordingId);
        }
    }

    // ------------------------------------------------------------------ evaluation

    /** The member's own durable journal tail of this genesis. */
    public sealed interface Tail permits TailKnown, TailNone, TailUnknown {
    }

    /** The largest {@code journalSeq} over the member's recordings of this genesis. */
    public record TailKnown(long seq) implements Tail {
    }

    /** No recording of this genesis with bytes. */
    public record TailNone() implements Tail {
    }

    /** Not readable now. */
    public record TailUnknown(String reason) implements Tail {
    }

    /** The off-box bound of one pass: the watermark's mode and its evaluation; {@link #NONE} when it is not wired. */
    public record OffBox(OffBoxWatermark.Mode mode, OffBoxWatermark.Evaluation evaluation) {
        public static final OffBox NONE = new OffBox(OffBoxWatermark.Mode.OFF, null);
    }

    /** The answer of one evaluation. */
    public static final class Evaluation {
        public final Mode mode;
        public final State state;
        /** The reader, or the other subject, the state is about; null when there is none. */
        public final String subject;
        /** Why the state is not VALID, in words; null when it is VALID or OFF. */
        public final String detail;
        /** Every record with {@code journalSeq <= safeSeq} is applied by every reader; -1 when not VALID. */
        public final long safeSeq;
        /** The log position a purged record may not exceed; {@link OffBoxWatermark#NO_CONSTRAINT} when unbounded. */
        public final long logFloor;

        Evaluation(final Mode mode, final State state, final String subject, final String detail, final long safeSeq,
                   final long logFloor) {
            this.mode = mode;
            this.state = state;
            this.subject = subject;
            this.detail = detail;
            this.safeSeq = safeSeq;
            this.logFloor = logFloor;
        }

        private static Evaluation hold(final Mode mode, final State state, final String subject, final String detail) {
            return new Evaluation(mode, state, subject, detail, -1, OffBoxWatermark.NO_CONSTRAINT);
        }

        /** Whether a pass in mode {@code readers} purges on this answer. */
        public boolean purges() {
            return mode == Mode.READERS && state == State.VALID;
        }

        @Override
        public String toString() {
            return "mode=" + mode.text() + " state=" + state + (subject == null ? "" : "(" + subject + ")")
                + " safeSeq=" + safeSeq + (detail == null ? "" : " (" + detail + ")");
        }
    }

    /**
     * The state and the bound of one pass, by the table of the class comment. A pure function: the same inputs give
     * the same answer, and nothing from an earlier call is used.
     *
     * @param mode            this member's mode for the journal
     * @param journal         the journal's name
     * @param engine          this member's engine name
     * @param clusterId       this member's cluster id
     * @param genesisId       the genesis id this member has recorded, 0 while it has none
     * @param readers         the files of the journal's reader directory
     * @param requiredReaders reader names that must have a valid file
     * @param anyBytes        whether any recording on the journal's stream holds bytes
     * @param tail            the member's own journal tail of this genesis
     * @param offBox          the off-box watermark of this pass
     */
    public static Evaluation evaluate(final Mode mode, final String journal, final String engine, final int clusterId,
                                      final long genesisId, final List<JournalReaders.Entry> readers,
                                      final Set<String> requiredReaders, final boolean anyBytes, final Tail tail,
                                      final OffBox offBox) {
        if (mode == Mode.OFF) {
            return Evaluation.hold(mode, State.OFF, null, null);
        }
        if (!anyBytes) {
            return Evaluation.hold(mode, State.NO_JOURNAL, null, "no recording with bytes on the journal's stream");
        }
        if (genesisId == 0) {
            return Evaluation.hold(mode, State.UNBOUND, null, "this member has no recorded genesis id yet");
        }
        if (tail instanceof TailNone) {
            return Evaluation.hold(mode, State.NO_JOURNAL, null, "no recording of genesis " + genesisId
                + " with bytes on the journal's stream");
        }
        final boolean unconstrained = readers.isEmpty() && Seams.noReadersIsNoConstraint;
        if (readers.isEmpty() && !unconstrained) {
            return Evaluation.hold(mode, State.NO_READERS, null, "no reader is registered for the " + journal
                + " journal: nothing is known to be applied");
        }
        final Set<String> present = new HashSet<>();
        for (final JournalReaders.Entry e : readers) {
            present.add(e.name());
        }
        for (final String required : new TreeSet<>(requiredReaders)) {
            if (!present.contains(required) && !unconstrained) {
                return Evaluation.hold(mode, State.MISSING, required, "the required reader " + required
                    + " has no file");
            }
        }
        for (final JournalReaders.Entry e : readers) {
            if (!e.read().valid()) {
                return Evaluation.hold(mode, State.INVALID, e.name(), "the file of reader " + e.name() + " is invalid: "
                    + e.read().invalid);
            }
        }
        for (final JournalReaders.Entry e : readers) {
            final JournalReaders.Record r = e.read().record;
            if (!r.engine.equals(engine) || r.clusterId != clusterId || r.genesisId != genesisId
                || !r.journal.equals(journal) || !r.reader.equals(e.name())) {
                return Evaluation.hold(mode, State.FOREIGN, e.name(), "the file of reader " + e.name()
                    + " is for engine=" + r.engine + " cluster_id=" + r.clusterId + " genesis_id=" + r.genesisId
                    + " journal=" + r.journal + " reader=" + r.reader + ", this member is engine=" + engine
                    + " cluster_id=" + clusterId + " genesis_id=" + genesisId + " journal=" + journal);
            }
        }
        if (tail instanceof TailUnknown unknown) {
            return Evaluation.hold(mode, State.TAIL_UNKNOWN, null, "this member's own journal tail could not be read: "
                + unknown.reason());
        }
        final long tailSeq = ((TailKnown) tail).seq();
        long safeSeq = unconstrained ? tailSeq : Seams.minIsMax ? Long.MIN_VALUE : Long.MAX_VALUE;
        for (final JournalReaders.Entry e : readers) {
            final JournalReaders.Record r = e.read().record;
            if (!Seams.aheadCheckOff && r.bound() > tailSeq) {
                return Evaluation.hold(mode, State.AHEAD, e.name(), "reader " + e.name() + " is at journalSeq "
                    + r.bound() + ", above this member's own journal tail " + tailSeq
                    + " (wrong unit, wrong genesis, or this member is behind)");
            }
            safeSeq = Seams.minIsMax ? Math.max(safeSeq, r.bound()) : Math.min(safeSeq, r.bound());
        }
        long logFloor = OffBoxWatermark.NO_CONSTRAINT;
        if (offBox != null && offBox.mode() == OffBoxWatermark.Mode.REQUIRED) {
            final OffBoxWatermark.Evaluation e = offBox.evaluation();
            if (e == null || e.state != OffBoxWatermark.State.VALID) {
                return Evaluation.hold(mode, State.OFFBOX_HOLD, null, "the off-box watermark is "
                    + (e == null ? "not evaluated" : e.state + (e.detail == null ? "" : ": " + e.detail)) + " ("
                    + OffBoxWatermark.MODE_ENV + "=required holds the journal)");
            }
            logFloor = e.floor;
        }
        return new Evaluation(mode, State.VALID, null, null, safeSeq, logFloor);
    }

    // ------------------------------------------------------------------ one pass

    /** One registered reader as a pass read it. */
    public record ReaderStatus(String name, long appliedSeq, long releasedSeq, long ageMs) {
    }

    /** What one pass found and did. */
    public static final class Report {
        public final String journal;
        public final Evaluation evaluation;
        /** The member's own journal tail of this genesis, -1 when it was not read. */
        public final long tailSeq;
        /** The sum over the stream's recordings of end less start, after this pass's purges. */
        public final long retainedBytes;
        /** Recordings on the stream that hold bytes, after this pass's purges. */
        public final int recordings;
        /** Recordings that carry another epoch: never touched. */
        public final int recordingsForeign;
        /**
         * Recordings with an anomaly in this pass: a sequence that does not rise (not purged), an unreadable record
         * at the start (not purged this pass), or an unreadable record at a segment base (that base does not qualify).
         */
        public final List<Long> anomalies;
        /** The age of the oldest valid reader file in ms, -1 when there is none. */
        public final long oldestReaderAgeMs;
        public final List<ReaderStatus> readers;
        public final long purgedBytes;
        public final int purges;
        public final int purgeFailures;
        /** True when the pass stopped at its deadline before every recording was looked at. */
        public final boolean budgetExceeded;
        /** The text for {@code /health} when the journal is held under {@code readers}; else null. */
        public final String problem;

        Report(final String journal, final Evaluation evaluation, final long tailSeq, final long retainedBytes,
               final int recordings, final int recordingsForeign, final List<Long> anomalies,
               final long oldestReaderAgeMs, final List<ReaderStatus> readers, final long purgedBytes,
               final int purges, final int purgeFailures, final boolean budgetExceeded, final String problem) {
            this.journal = journal;
            this.evaluation = evaluation;
            this.tailSeq = tailSeq;
            this.retainedBytes = retainedBytes;
            this.recordings = recordings;
            this.recordingsForeign = recordingsForeign;
            this.anomalies = List.copyOf(anomalies);
            this.oldestReaderAgeMs = oldestReaderAgeMs;
            this.readers = List.copyOf(readers);
            this.purgedBytes = purgedBytes;
            this.purges = purges;
            this.purgeFailures = purgeFailures;
            this.budgetExceeded = budgetExceeded;
            this.problem = problem;
        }

        @Override
        public String toString() {
            return journal + ": " + evaluation + " tail=" + tailSeq + " retainedBytes=" + retainedBytes
                + " recordings=" + recordings + " foreign=" + recordingsForeign + " anomalies=" + anomalies
                + " purgedBytes=" + purgedBytes + " purgeFailures=" + purgeFailures
                + (budgetExceeded ? " (stopped at the pass budget)" : "");
        }
    }

    private static final int PURGE_RETRIES = 3;
    private static final long PURGE_RETRY_NS = 20_000_000L;

    private final JournalProfile profile;
    private final File clusterDir;
    private final String engine;
    private final int clusterId;
    private final Mode mode;
    private final Set<String> requiredReaders;
    private final long alarmBytes;
    private final long staleMs;
    private final NanoClock clock;
    private final Consumer<String> out;
    private final Consumer<String> err;

    /**
     * @param profile         the journal
     * @param clusterDir      the member's cluster directory (it holds {@code retention/journal-readers})
     * @param engine          {@link JournalReaders#ENGINE_CORE} or {@link JournalReaders#ENGINE_MATCH}
     * @param clusterId       this member's cluster id
     * @param mode            this member's mode for the journal
     * @param requiredReaders reader names that must have a valid file
     * @param alarmBytes      retained bytes above which a CRITICAL line is printed
     * @param staleMs         reader file age above which a CRITICAL line is printed
     * @param clock           the clock of the pass deadline
     * @param out             where an informational line goes
     * @param err             where a CRITICAL line goes
     */
    public JournalRetention(final JournalProfile profile, final File clusterDir, final String engine,
                            final int clusterId, final Mode mode, final Set<String> requiredReaders,
                            final long alarmBytes, final long staleMs, final NanoClock clock,
                            final Consumer<String> out, final Consumer<String> err) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.clusterDir = Objects.requireNonNull(clusterDir, "clusterDir");
        if (!JournalReaders.ENGINE_CORE.equals(engine) && !JournalReaders.ENGINE_MATCH.equals(engine)) {
            throw new IllegalArgumentException("engine must be core or match, got '" + engine + "'");
        }
        this.engine = engine;
        this.clusterId = clusterId;
        this.mode = Objects.requireNonNull(mode, "mode");
        this.requiredReaders = Set.copyOf(requiredReaders);
        this.alarmBytes = alarmBytes;
        this.staleMs = staleMs;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.out = Objects.requireNonNull(out, "out");
        this.err = Objects.requireNonNull(err, "err");
    }

    public JournalProfile profile() {
        return profile;
    }

    public Mode mode() {
        return mode;
    }

    /** The largest last sequence over the recordings of this genesis (the member's own durable journal tail). */
    public Tail tail(final Archive archive, final long genesisId, final long deadlineNs) {
        return new Survey(archive.list(), archive, genesisId, deadlineNs).tail;
    }

    /** The last record of every recording with bytes, and what follows from them. */
    private final class Survey {
        final List<Recording> withBytes = new ArrayList<>();
        final Map<Long, JournalProfile.Key> lastOfOwn = new LinkedHashMap<>();
        final List<Recording> own = new ArrayList<>();
        final List<Recording> foreign = new ArrayList<>();
        final Tail tail;

        Survey(final List<Recording> listed, final Archive archive, final long genesisId, final long deadlineNs) {
            for (final Recording r : listed) {
                if (r.bytes() > 0) {
                    withBytes.add(r);
                }
            }
            String unknown = null;
            long max = -1;
            for (final Recording r : withBytes) {
                final Peek last = archive.last(r, deadlineNs);
                if (!last.readable()) {
                    unknown = "last record of recording " + r.id() + " [" + r.start() + "," + r.end() + "): "
                        + last.unreadable();
                    break;
                }
                if (profile.hasEpoch() && last.key().epoch() != genesisId && !Seams.epochCheckOff) {
                    foreign.add(r);
                    continue;
                }
                own.add(r);
                lastOfOwn.put(r.id(), last.key());
                max = Math.max(max, last.key().journalSeq());
            }
            tail = unknown != null ? new TailUnknown(unknown) : own.isEmpty() ? new TailNone() : new TailKnown(max);
        }
    }

    /**
     * One pass over the journal: read the reader files and the recordings, work out the bound, and (mode
     * {@code readers}) purge below it. Each purge is whole and independent; a pass that stops at its deadline has
     * purged what it purged.
     *
     * @param archive    the member's archive
     * @param genesisId  the genesis id this member has recorded, 0 while it has none
     * @param offBox     the off-box watermark of this pass
     * @param nowMs      the wall clock, for the reader file ages
     * @param deadlineNs the pass's deadline, an absolute value of this object's clock
     * @throws RuntimeException the archive could not be listed or the driver is gone (nothing was purged by the
     *                          failing step)
     */
    public Report pass(final Archive archive, final long genesisId, final OffBox offBox, final long nowMs,
                       final long deadlineNs) {
        final List<Recording> listed = archive.list();
        final Set<Long> listedIds = new HashSet<>();
        long retained = 0;
        int withBytes = 0;
        for (final Recording r : listed) {
            listedIds.add(r.id());
            retained += r.bytes();
            if (r.bytes() > 0) {
                withBytes++;
            }
        }
        if (mode == Mode.OFF) {
            final Evaluation off = evaluate(mode, profile.journal(), engine, clusterId, genesisId, List.of(), Set.of(),
                withBytes > 0, new TailNone(), offBox);
            return new Report(profile.journal(), off, -1, retained, withBytes, 0, List.of(), -1, List.of(), 0, 0, 0,
                false, null);
        }

        List<JournalReaders.Entry> entries;
        String listFailure = null;
        try {
            entries = JournalReaders.list(clusterDir, profile.journal());
        } catch (final IOException | RuntimeException failed) {
            entries = List.of();
            listFailure = String.valueOf(failed);
        }
        final List<ReaderStatus> readers = new ArrayList<>();
        long oldestAgeMs = -1;
        for (final JournalReaders.Entry e : entries) {
            if (e.read().valid()) {
                final JournalReaders.Record r = e.read().record;
                final long ageMs = Math.max(0, nowMs - r.writtenAtEpochMs);
                readers.add(new ReaderStatus(e.name(), r.appliedJournalSeq, r.releasedUpToJournalSeq, ageMs));
                oldestAgeMs = Math.max(oldestAgeMs, ageMs);
            }
        }

        final Survey survey = withBytes > 0 && genesisId != 0
            ? new Survey(listed, archive, genesisId, deadlineNs) : null;
        final Tail tail = survey == null ? new TailNone() : survey.tail;
        final long tailSeq = tail instanceof TailKnown known ? known.seq() : -1;
        final Evaluation evaluation = listFailure != null && withBytes > 0 && genesisId != 0
            ? Evaluation.hold(mode, State.INVALID, "(directory)", "the reader directory could not be listed: "
                + listFailure)
            : evaluate(mode, profile.journal(), engine, clusterId, genesisId, entries, requiredReaders, withBytes > 0,
                // an unbound member's tail is not read: the table answers UNBOUND before it looks at the tail
                survey == null && withBytes > 0 ? new TailUnknown("not read") : tail, offBox);

        long purgedBytes = 0;
        int purges = 0;
        int failures = 0;
        int purgedWhole = 0;
        boolean budgetExceeded = false;
        final List<Long> anomalies = new ArrayList<>();
        if (evaluation.state == State.VALID) {
            final Recording keeper = keeper(survey);
            for (final Recording r : survey.own) {
                if (clock.nanoTime() - deadlineNs > 0) {
                    budgetExceeded = true;
                    break;
                }
                final JournalProfile.Key last = survey.lastOfOwn.get(r.id());
                final long target;
                final boolean whole = !r.active() && last.journalSeq() <= evaluation.safeSeq
                    && logPositionAllowed(last.logPosition(), evaluation.logFloor)
                    && (r != keeper || Seams.tailCoverOff);
                if (whole) {
                    target = r.end();
                } else {
                    try {
                        target = greatestQualifyingBase(archive, r, last, evaluation, genesisId, deadlineNs,
                            anomalies);
                    } catch (final Anomaly anomaly) {
                        flag(anomalies, r);
                        err.accept(LOG + "CRITICAL: " + profile.journal() + " journal recording " + r.id()
                            + " ANOMALY: " + anomaly.getMessage() + ". The recording is not purged; an operator has to"
                            + " look at it.");
                        continue;
                    }
                }
                if (target <= r.start()) {
                    continue;
                }
                final long bytes = target - r.start();
                final String what = profile.journal() + " journal recording " + r.id() + (whole
                    ? ": stopped, last journalSeq " + last.journalSeq() + ", whole recording [" + r.start() + ","
                        + r.end() + ")"
                    : ": start " + r.start() + " -> " + target) + " (" + bytes + " bytes; safeSeq "
                    + evaluation.safeSeq + ", tail " + tailSeq + ")";
                if (mode != Mode.READERS) {
                    out.accept(LOG + "report: would purge " + what + ". Nothing purged.");
                    continue;
                }
                if (!listedIds.contains(r.id())) {
                    throw new IllegalStateException("recording " + r.id() + " was not listed on the journal's stream");
                }
                final String refused = purge(archive, r.id(), whole ? -1 : target, deadlineNs);
                if (refused != null) {
                    failures++;
                    out.accept(LOG + "the archive refused to purge " + what + ": " + refused
                        + ". Nothing changed; the next pass tries again.");
                    continue;
                }
                purges++;
                purgedBytes += bytes;
                if (whole) {
                    purgedWhole++;
                }
                out.accept(LOG + "purged " + what + ".");
            }
        }

        final int foreign = survey == null ? 0 : survey.foreign.size();
        final String problem = problem(evaluation, anomalies);
        final Report report = new Report(profile.journal(), evaluation, tailSeq, retained - purgedBytes,
            withBytes - purgedWhole, foreign, anomalies, oldestAgeMs, readers, purgedBytes, purges, failures,
            budgetExceeded, problem);
        lines(report, entries);
        return report;
    }

    /** The recording that carries the member's tail: the largest last sequence; an active one, then the newest. */
    private Recording keeper(final Survey survey) {
        Recording best = null;
        long bestSeq = Long.MIN_VALUE;
        for (final Recording r : survey.own) {
            final long seq = survey.lastOfOwn.get(r.id()).journalSeq();
            if (best == null || seq > bestSeq || (seq == bestSeq && (r.active() && !best.active()
                || (r.active() == best.active() && r.id() > best.id())))) {
                best = r;
                bestSeq = seq;
            }
        }
        return best;
    }

    private static boolean logPositionAllowed(final long logPosition, final long logFloor) {
        if (logFloor == OffBoxWatermark.NO_CONSTRAINT || Seams.offBoxBoundOff) {
            return true;
        }
        return logPosition != JournalProfile.NO_LOG_POSITION && logPosition <= logFloor;
    }

    private static void flag(final List<Long> anomalies, final Recording r) {
        if (!anomalies.contains(r.id())) {
            anomalies.add(r.id());
        }
    }

    /** The sequence of a recording does not rise with its positions. */
    private static final class Anomaly extends Exception {
        private static final long serialVersionUID = 1L;

        Anomaly(final String message) {
            super(message, null, false, false);
        }
    }

    /**
     * The greatest segment base of {@code r} that qualifies, or {@code r.start()} when none does. Looks at the
     * highest candidate first (the steady state), then bisects, one replay at a time.
     */
    private long greatestQualifyingBase(final Archive archive, final Recording r, final JournalProfile.Key last,
                                        final Evaluation evaluation, final long genesisId, final long deadlineNs,
                                        final List<Long> anomalies)
        throws Anomaly {
        final long startBase = r.segmentBase(r.start());
        final long max;
        if (r.active()) {
            final long headBase = r.segmentBase(r.end());
            max = Seams.clampOff ? headBase : headBase - 2L * r.segmentLength();
        } else {
            // the last segment always stays, so the last record stays readable
            max = r.segmentBase(r.end() - 1);
        }
        final long candidates = (max - startBase) / r.segmentLength();
        if (candidates < 1) {
            return r.start();
        }
        final TreeMap<Long, Long> seqAt = new TreeMap<>();
        final Peek atStart = archive.first(r, r.start(), deadlineNs);
        if (!atStart.readable()) {
            flag(anomalies, r);
            err.accept(LOG + "CRITICAL: " + profile.journal() + " journal recording " + r.id() + " ANOMALY: the record"
                + " at its start " + r.start() + " is unreadable (" + atStart.unreadable() + "): not purged this pass."
                + " If it stays unreadable an operator has to look at the recording.");
            return r.start();
        }
        note(seqAt, r, r.start(), atStart.key().journalSeq(), last);
        long lo = 1;
        long hi = candidates;
        long best = 0;
        boolean first = true;
        while (lo <= hi) {
            final long k = first ? hi : lo + (hi - lo + 1) / 2;
            first = false;
            if (qualifies(archive, r, startBase + k * r.segmentLength(), seqAt, last, evaluation, genesisId,
                deadlineNs, anomalies)) {
                best = k;
                lo = k + 1;
            } else {
                hi = k - 1;
            }
        }
        return best == 0 ? r.start() : startBase + best * r.segmentLength();
    }

    private boolean qualifies(final Archive archive, final Recording r, final long base,
                              final TreeMap<Long, Long> seqAt, final JournalProfile.Key last,
                              final Evaluation evaluation, final long genesisId, final long deadlineNs,
                              final List<Long> anomalies)
        throws Anomaly {
        if (clock.nanoTime() - deadlineNs > 0) {
            return false;
        }
        final Peek peek = archive.first(r, base, deadlineNs);
        if (!peek.readable()) {
            flag(anomalies, r);
            err.accept(LOG + "CRITICAL: " + profile.journal() + " journal recording " + r.id() + " ANOMALY: the record"
                + " at segment base " + base + " is unreadable (" + peek.unreadable() + "): that base does not"
                + " qualify. The recording is cut no further than a lower base until it reads; if it stays unreadable"
                + " an operator has to look at the recording.");
            return false;
        }
        final JournalProfile.Key key = peek.key();
        note(seqAt, r, base, key.journalSeq(), last);
        if (key.journalSeq() < 1 || key.journalSeq() - 1 > evaluation.safeSeq) {
            return false;
        }
        if (profile.hasEpoch() && key.epoch() != genesisId && !Seams.epochCheckOff) {
            return false;
        }
        return logPositionAllowed(key.logPosition(), evaluation.logFloor);
    }

    /** Records one read and holds it against every other read of the recording in this pass. */
    private static void note(final TreeMap<Long, Long> seqAt, final Recording r, final long position, final long seq,
                             final JournalProfile.Key last) throws Anomaly {
        final Map.Entry<Long, Long> below = seqAt.lowerEntry(position);
        final Map.Entry<Long, Long> above = seqAt.higherEntry(position);
        if (below != null && below.getValue() >= seq) {
            throw new Anomaly("journalSeq " + seq + " at position " + position + " is not above journalSeq "
                + below.getValue() + " at the lower position " + below.getKey());
        }
        if (above != null && above.getValue() <= seq) {
            throw new Anomaly("journalSeq " + seq + " at position " + position + " is not below journalSeq "
                + above.getValue() + " at the higher position " + above.getKey());
        }
        if (seq > last.journalSeq()) {
            throw new Anomaly("journalSeq " + seq + " at position " + position + " is above the recording's last"
                + " journalSeq " + last.journalSeq());
        }
        seqAt.put(position, seq);
    }

    /**
     * One purge; null when done, else the archive's refusal. A refusal that names a replay is tried again a few
     * times: the replay may be this pass's own read, which the archive is still closing.
     */
    private String purge(final Archive archive, final long recordingId, final long newStart, final long deadlineNs) {
        for (int attempt = 0; ; attempt++) {
            try {
                if (newStart < 0) {
                    archive.purgeRecording(recordingId);
                } else {
                    archive.purgeSegments(recordingId, newStart);
                }
                return null;
            } catch (final ArchiveException refused) {
                final String message = String.valueOf(refused.getMessage());
                if (attempt >= PURGE_RETRIES || !message.contains("replay")
                    || clock.nanoTime() + PURGE_RETRY_NS - deadlineNs > 0) {
                    return message;
                }
                java.util.concurrent.locks.LockSupport.parkNanos(PURGE_RETRY_NS);
            }
        }
    }

    private String problem(final Evaluation e, final List<Long> anomalies) {
        if (mode != Mode.READERS) {
            return null;
        }
        final String setting = Settings.MODE_ENV_PREFIX + profile.journal().toUpperCase(Locale.ROOT);
        if (e.state != State.VALID && e.state != State.NO_JOURNAL) {
            return "journal retention " + profile.journal() + " " + e.state + ": " + e.detail + " (" + setting
                + "=readers holds the journal)";
        }
        if (!anomalies.isEmpty()) {
            return "journal retention " + profile.journal() + " ANOMALY: recordings " + anomalies
                + " cannot be cut as far as their readers allow (a journalSeq that does not rise, or an unreadable"
                + " record at the start or at a segment base)";
        }
        return null;
    }

    /** The lines of one pass: the status, and the alarms. */
    private void lines(final Report report, final List<JournalReaders.Entry> entries) {
        final Evaluation e = report.evaluation;
        out.accept(LOG + report);
        final String journal = profile.journal();
        final String setting = Settings.MODE_ENV_PREFIX + journal.toUpperCase(Locale.ROOT);
        final String consequence = mode == Mode.READERS ? "This member purges nothing of the " + journal + " journal."
            : "Under " + setting + "=readers this member would purge nothing of the " + journal + " journal.";
        switch (e.state) {
            case NO_READERS:
                err.accept(LOG + "CRITICAL: no reader is registered for the " + journal + " journal: every record is"
                    + " held and the disk grows. " + consequence + " Register its readers (JournalReaderTool set).");
                break;
            case MISSING:
                err.accept(LOG + "CRITICAL: the required reader " + e.subject + " of the " + journal + " journal has"
                    + " no file: every record is held. " + consequence + " Register it (JournalReaderTool set).");
                break;
            case INVALID:
                err.accept(LOG + "CRITICAL: " + journal + " journal reader file INVALID: " + e.detail + ". "
                    + consequence + " The next JournalReaderTool set replaces it.");
                break;
            case FOREIGN:
                err.accept(LOG + "CRITICAL: " + journal + " journal reader file FOREIGN: " + e.detail + ". "
                    + consequence + " Remove it (JournalReaderTool remove), then set it for this member.");
                break;
            default:
                break;
        }
        for (final JournalReaders.Entry entry : entries) {
            if (entry.read().valid() && entry.read().record.released()) {
                final JournalReaders.Record r = entry.read().record;
                err.accept(LOG + "CRITICAL: hold of reader " + r.reader + " of the " + journal + " journal released by"
                    + " operator up to journalSeq " + r.releasedUpToJournalSeq + " (applied " + r.appliedJournalSeq
                    + "): records in (" + r.appliedJournalSeq + ", " + r.releasedUpToJournalSeq + "] may be purged"
                    + " before that reader applied them; reason: " + r.releaseReason);
            }
        }
        if (report.retainedBytes > alarmBytes) {
            err.accept(LOG + "CRITICAL: this member holds " + report.retainedBytes + " bytes of the " + journal
                + " journal (above " + Settings.ALARM_BYTES_ENV + " " + alarmBytes + "): state " + e.state
                + ", safeSeq " + e.safeSeq + ", tail " + report.tailSeq + ". The disk fills unless the readers catch"
                + " up or an operator releases the hold (JournalReaderTool release).");
        }
        if (report.oldestReaderAgeMs > staleMs) {
            err.accept(LOG + "CRITICAL: a reader file of the " + journal + " journal was written "
                + report.oldestReaderAgeMs + " ms ago (above " + Settings.STALE_MS_ENV + " " + staleMs
                + "): its cursor is not being delivered. Readers: " + report.readers);
        }
    }
}
