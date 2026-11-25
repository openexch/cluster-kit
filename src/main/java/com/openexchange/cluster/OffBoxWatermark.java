// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The off-box watermark: "this cluster's log is durably stored off every host of the cluster below position X".
 *
 * <p>The fact is about storage outside the cluster and it bounds a node-local delete, so it is not carried in the
 * replicated log. It lives in one small file per member, {@code <clusterDir>/retention/offbox-watermark}, written by
 * a command run on the member's host ({@link OffBoxWatermarkTool}). The log pruner reads the file on every pass and
 * never purges past it when {@link #MODE_ENV} is {@code required}.</p>
 *
 * <p>A purge cannot be undone, so every doubtful input holds the log: a lost file, an unreadable file, a file of
 * another cluster, a position above this member's commit position. Nothing is remembered between two evaluations;
 * the answer is always worked out from the file and the commit position as they are now.</p>
 *
 * <h2>The evaluation</h2>
 *
 * <table>
 *   <caption>State and floor by mode and file</caption>
 *   <tr><th>Mode</th><th>File and identity</th><th>State</th><th>Floor</th></tr>
 *   <tr><td>off</td><td>absent</td><td>OFF</td><td>no constraint</td></tr>
 *   <tr><td>off</td><td>present (it appeared after the start)</td><td>IGNORED</td><td>no constraint</td></tr>
 *   <tr><td>required, report</td><td>absent</td><td>ABSENT</td><td>0: hold everything</td></tr>
 *   <tr><td>required, report</td><td>invalid</td><td>INVALID</td><td>0</td></tr>
 *   <tr><td>required, report</td><td>this member has no recorded genesis id yet</td><td>UNBOUND</td><td>0</td></tr>
 *   <tr><td>required, report</td><td>engine, cluster id or genesis id differs</td><td>FOREIGN</td><td>0</td></tr>
 *   <tr><td>required, report</td><td>position above this member's commit position, or that position cannot be
 *       read</td><td>AHEAD</td><td>0</td></tr>
 *   <tr><td>required, report</td><td>valid</td><td>VALID</td><td>the greater of the durable position and the
 *       operator's release</td></tr>
 * </table>
 *
 * <p>{@code report} works out the same floor and does not apply it. Under {@code off}, a file that is present when
 * the member starts refuses the start ({@link #checkBoot}): an explicit absence is the only way to "no constraint".</p>
 *
 * <h2>The file</h2>
 *
 * <p>Text, one {@code key=value} per line, LF line ends, the keys in this order:</p>
 * <pre>
 * format=offbox-watermark/1
 * engine=core
 * cluster_id=0
 * genesis_id=17
 * log_position_bytes=1048576
 * released_up_to_bytes=-1
 * release_reason=
 * seq=3
 * written_at_epoch_ms=1764000000000
 * source=archiver
 * sha256=...
 * </pre>
 *
 * <p>{@code log_position_bytes} is in bytes of the cluster log, the unit of the snapshot positions in
 * {@code recording.log}. {@code sha256} covers every byte above its own line. An unknown key, a missing key, a key
 * out of order, a number out of range, a CR, or a digest that does not match makes the file invalid.</p>
 */
public final class OffBoxWatermark {

    /** {@code off} (default), {@code report} or {@code required}. */
    public static final String MODE_ENV = "RETENTION_OFFBOX";
    public static final String MODE_PROP = "retention.offbox";
    /** Held bytes above which a CRITICAL line is printed on every pass. An alarm threshold, not a limit. */
    public static final String ALARM_BYTES_ENV = "RETENTION_OFFBOX_ALARM_BYTES";
    public static final String ALARM_BYTES_PROP = "retention.offbox.alarm.bytes";
    public static final long DEFAULT_ALARM_BYTES = 2L << 30;
    public static final long MIN_ALARM_BYTES = 1L << 20;
    public static final long MAX_ALARM_BYTES = 1L << 40;
    /** File age above which a CRITICAL line is printed on every pass. It does not change the floor. */
    public static final String STALE_MS_ENV = "RETENTION_OFFBOX_STALE_MS";
    public static final String STALE_MS_PROP = "retention.offbox.stale.ms";
    public static final long DEFAULT_STALE_MS = 3_600_000;
    public static final long MIN_STALE_MS = 1_000;
    public static final long MAX_STALE_MS = 604_800_000;

    public static final String DIRECTORY = "retention";
    public static final String FILE_NAME = "offbox-watermark";
    public static final String FORMAT = "offbox-watermark/1";
    public static final String ENGINE_CORE = "core";
    public static final String ENGINE_MATCH = "match";
    /** The floor that bounds nothing. */
    public static final long NO_CONSTRAINT = Long.MAX_VALUE;
    /** {@code released_up_to_bytes} when no release is in force. */
    public static final long NO_RELEASE = -1;
    static final int MAX_SOURCE_LENGTH = 64;
    static final int MAX_REASON_LENGTH = 200;
    /** A valid file is a few hundred bytes; anything much larger is not this file. */
    private static final int MAX_FILE_BYTES = 4096;

    private static final String[] KEYS = {
        "format", "engine", "cluster_id", "genesis_id", "log_position_bytes", "released_up_to_bytes",
        "release_reason", "seq", "written_at_epoch_ms", "source", "sha256"
    };

    /** What the pruner does with the watermark. The code is the value of the mode gauge. */
    public enum Mode {
        OFF(0, "off"), REPORT(1, "report"), REQUIRED(2, "required");

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

    /** What one evaluation found. The code is the value of the state gauge. */
    public enum State {
        VALID(0), ABSENT(1), INVALID(2), UNBOUND(3), FOREIGN(4), AHEAD(5), IGNORED(6), OFF(7);

        private final int code;

        State(final int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    /** The content of a valid file. */
    public static final class Record {
        public final String engine;
        public final int clusterId;
        public final long genesisId;
        public final long logPositionBytes;
        public final long releasedUpToBytes;
        public final String releaseReason;
        public final long seq;
        public final long writtenAtEpochMs;
        public final String source;

        public Record(final String engine, final int clusterId, final long genesisId, final long logPositionBytes,
                      final long releasedUpToBytes, final String releaseReason, final long seq,
                      final long writtenAtEpochMs, final String source) {
            this.engine = engine;
            this.clusterId = clusterId;
            this.genesisId = genesisId;
            this.logPositionBytes = logPositionBytes;
            this.releasedUpToBytes = releasedUpToBytes;
            this.releaseReason = releaseReason;
            this.seq = seq;
            this.writtenAtEpochMs = writtenAtEpochMs;
            this.source = source;
        }

        /** The position this member may purge up to: the durable position, or the operator's release above it. */
        public long floor() {
            return Math.max(logPositionBytes, releasedUpToBytes);
        }

        /** Why this content cannot be written, or null. The same rules a reader applies. */
        public String refusal() {
            if (!ENGINE_CORE.equals(engine) && !ENGINE_MATCH.equals(engine)) {
                return "engine must be core or match, got '" + engine + "'";
            }
            if (clusterId < 0) {
                return "cluster_id must be 0 or above, got " + clusterId;
            }
            if (genesisId <= 0) {
                return "genesis_id must be above 0, got " + genesisId;
            }
            if (logPositionBytes < 0) {
                return "log_position_bytes must be 0 or above, got " + logPositionBytes;
            }
            if (releasedUpToBytes < NO_RELEASE) {
                return "released_up_to_bytes must be -1 or a position, got " + releasedUpToBytes;
            }
            if (releaseReason == null || releaseReason.length() > MAX_REASON_LENGTH
                || !printable(releaseReason, true)) {
                return "release_reason must be at most " + MAX_REASON_LENGTH + " printable ASCII characters";
            }
            if ((releasedUpToBytes == NO_RELEASE) != releaseReason.isEmpty()) {
                return "release_reason must be given with a release and only with one";
            }
            if (seq < 1) {
                return "seq must be 1 or above, got " + seq;
            }
            if (writtenAtEpochMs < 0) {
                return "written_at_epoch_ms must be 0 or above, got " + writtenAtEpochMs;
            }
            if (source == null || source.isEmpty() || source.length() > MAX_SOURCE_LENGTH
                || !printable(source, false)) {
                return "source must be 1.." + MAX_SOURCE_LENGTH + " printable ASCII characters without a space";
            }
            return null;
        }

        /** The file's bytes, digest line included. */
        public byte[] encode() {
            final String body = "format=" + FORMAT + "\n"
                + "engine=" + engine + "\n"
                + "cluster_id=" + clusterId + "\n"
                + "genesis_id=" + genesisId + "\n"
                + "log_position_bytes=" + logPositionBytes + "\n"
                + "released_up_to_bytes=" + releasedUpToBytes + "\n"
                + "release_reason=" + releaseReason + "\n"
                + "seq=" + seq + "\n"
                + "written_at_epoch_ms=" + writtenAtEpochMs + "\n"
                + "source=" + source + "\n";
            final byte[] covered = body.getBytes(StandardCharsets.US_ASCII);
            return (body + "sha256=" + sha256Hex(covered, covered.length) + "\n").getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public String toString() {
            return "engine=" + engine + " cluster_id=" + clusterId + " genesis_id=" + genesisId
                + " log_position_bytes=" + logPositionBytes + " released_up_to_bytes=" + releasedUpToBytes
                + " seq=" + seq + " written_at_epoch_ms=" + writtenAtEpochMs + " source=" + source;
        }
    }

    /** One read of the file: absent, invalid (with the reason), or a record. */
    public static final class Read {
        public final Record record;
        public final String invalid;

        private Read(final Record record, final String invalid) {
            this.record = record;
            this.invalid = invalid;
        }

        public boolean absent() {
            return record == null && invalid == null;
        }

        public boolean valid() {
            return record != null;
        }
    }

    /** The answer of one evaluation. */
    public static final class Evaluation {
        public final Mode mode;
        public final State state;
        /** The floor of the table: 0 holds everything, {@link #NO_CONSTRAINT} under {@code off}. */
        public final long floor;
        /** The record read, when the file parsed (also for FOREIGN and AHEAD); else null. */
        public final Record record;
        /** Why the state is not VALID, in words; null when it is VALID or OFF. */
        public final String detail;

        Evaluation(final Mode mode, final State state, final long floor, final Record record, final String detail) {
            this.mode = mode;
            this.state = state;
            this.floor = floor;
            this.record = record;
            this.detail = detail;
        }

        /** The bound the purge takes: the floor under {@code required}, nothing under {@code off} and {@code report}. */
        public long appliedFloor() {
            return mode == Mode.REQUIRED ? floor : NO_CONSTRAINT;
        }

        /** Under {@code required} every state but VALID is a hold the operator must see. */
        public boolean holdsAsError() {
            return mode == Mode.REQUIRED && state != State.VALID;
        }

        @Override
        public String toString() {
            return "mode=" + mode.text() + " state=" + state + " floor=" + describe(floor)
                + (detail == null ? "" : " (" + detail + ")");
        }
    }

    // Test seams, the negative controls of the three rules that keep a doubtful file from releasing log.
    static volatile boolean skipDigestForTest;
    static volatile boolean absentIsNoConstraintForTest;
    static volatile boolean aheadCheckOffForTest;

    private final File clusterDir;
    private final String engine;
    private final int clusterId;
    private final LongSupplier genesisId;
    private final Mode mode;
    private final long alarmBytes;
    private final long staleMs;

    private volatile State lastState;
    private volatile long lastPositionBytes = -1;
    private volatile long lastReleasedBytes = NO_RELEASE;
    private volatile long lastAgeMs = -1;
    private volatile long lastHeldBytes;

    /**
     * The watermark of this member, with the mode and the alarm thresholds read from the settings (environment
     * first, then the system property, then the default).
     *
     * @param clusterDir the member's cluster directory, the one holding {@code recording.log}
     * @param engine     {@link #ENGINE_CORE} or {@link #ENGINE_MATCH}
     * @param clusterId  this member's cluster id
     * @param genesisId  the genesis id this member has recorded, 0 while it has none
     * @throws IllegalArgumentException naming a malformed or out-of-range setting
     */
    public OffBoxWatermark(final File clusterDir, final String engine, final int clusterId,
                           final LongSupplier genesisId) {
        this(clusterDir, engine, clusterId, genesisId, modeSetting(), alarmBytesSetting(), staleMsSetting());
    }

    /** As above with the settings given (tests; a host that parses the settings itself). */
    public OffBoxWatermark(final File clusterDir, final String engine, final int clusterId,
                           final LongSupplier genesisId, final Mode mode, final long alarmBytes,
                           final long staleMs) {
        if (!ENGINE_CORE.equals(engine) && !ENGINE_MATCH.equals(engine)) {
            throw new IllegalArgumentException("engine must be core or match, got '" + engine + "'");
        }
        this.clusterDir = java.util.Objects.requireNonNull(clusterDir, "clusterDir");
        this.engine = engine;
        this.clusterId = clusterId;
        this.genesisId = java.util.Objects.requireNonNull(genesisId, "genesisId");
        this.mode = java.util.Objects.requireNonNull(mode, "mode");
        this.alarmBytes = alarmBytes;
        this.staleMs = staleMs;
        this.lastState = mode == Mode.OFF ? State.OFF : State.ABSENT;
    }

    // ------------------------------------------------------------------ settings

    /**
     * Parse and range check the three settings without wiring anything, so a bad value refuses the start.
     *
     * @throws IllegalArgumentException naming the setting and its values or range
     */
    public static void checkSettings() {
        modeSetting();
        alarmBytesSetting();
        staleMsSetting();
    }

    /**
     * The boot check that needs the cluster directory: {@code off} with a watermark file present refuses the start.
     * An explicit absence is the only way to "no constraint"; a setting that was lost must not become one.
     *
     * @throws IllegalArgumentException with the refusal line
     */
    public static void checkBoot(final File clusterDir) {
        checkSettings();
        final String refusal = offWithFileRefusal(modeSetting(), clusterDir);
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
    }

    /** The refusal line for {@code off} with a file present, or null. */
    static String offWithFileRefusal(final Mode mode, final File clusterDir) {
        final Path file = file(clusterDir);
        if (mode == Mode.OFF && Files.exists(file)) {
            return MODE_ENV + "=off but " + file + " exists: set " + MODE_ENV + "=required, or remove the file with"
                + " OffBoxWatermarkTool clear";
        }
        return null;
    }

    public static Mode modeSetting() {
        return parseMode(System.getenv(MODE_ENV), System.getProperty(MODE_PROP));
    }

    static Mode parseMode(final String envValue, final String propValue) {
        final String raw = envValue != null && !envValue.isEmpty() ? envValue
            : propValue != null && !propValue.isEmpty() ? propValue : null;
        if (raw == null) {
            return Mode.OFF;
        }
        for (final Mode m : Mode.values()) {
            if (m.text.equals(raw.trim())) {
                return m;
            }
        }
        throw new IllegalArgumentException(MODE_ENV + " must be off, report or required, got '" + raw + "'");
    }

    static long alarmBytesSetting() {
        return EngineLifecycle.rangedSetting(ALARM_BYTES_ENV, ALARM_BYTES_PROP, DEFAULT_ALARM_BYTES,
            MIN_ALARM_BYTES, MAX_ALARM_BYTES);
    }

    static long staleMsSetting() {
        return EngineLifecycle.rangedSetting(STALE_MS_ENV, STALE_MS_PROP, DEFAULT_STALE_MS, MIN_STALE_MS,
            MAX_STALE_MS);
    }

    // ------------------------------------------------------------------ the file

    /** Where the file of a member lives. */
    public static Path file(final File clusterDir) {
        return clusterDir.toPath().resolve(DIRECTORY).resolve(FILE_NAME);
    }

    /** Read the member's file. Never throws for a file that is missing or malformed: both are answers. */
    public static Read read(final File clusterDir) {
        final Path file = file(clusterDir);
        final byte[] bytes;
        try {
            if (Files.size(file) > MAX_FILE_BYTES) {
                return new Read(null, "the file is larger than " + MAX_FILE_BYTES + " bytes");
            }
            bytes = Files.readAllBytes(file);
        } catch (final NoSuchFileException absent) {
            return new Read(null, null);
        } catch (final IOException e) {
            // a file that exists and cannot be read is not an absent file
            return new Read(null, "the file could not be read: " + e);
        }
        return parse(bytes);
    }

    /** Parse the bytes of a file. */
    static Read parse(final byte[] bytes) {
        for (final byte b : bytes) {
            if (b == '\r') {
                return new Read(null, "CR in the file: line ends must be LF");
            }
            if (b != '\n' && (b < 0x20 || b > 0x7e)) {
                return new Read(null, "a byte outside printable ASCII");
            }
        }
        if (bytes.length == 0 || bytes[bytes.length - 1] != '\n') {
            return new Read(null, "the file does not end with a line end");
        }
        final String text = new String(bytes, StandardCharsets.US_ASCII);
        final String[] lines = text.substring(0, text.length() - 1).split("\n", -1);
        if (lines.length != KEYS.length) {
            return new Read(null, "expected " + KEYS.length + " lines, found " + lines.length
                + " (a key is missing or unknown)");
        }
        final String[] values = new String[KEYS.length];
        for (int i = 0; i < KEYS.length; i++) {
            final String prefix = KEYS[i] + "=";
            if (!lines[i].startsWith(prefix)) {
                return new Read(null, "line " + (i + 1) + " must be the key " + KEYS[i]
                    + " (unknown key, missing key or wrong order)");
            }
            values[i] = lines[i].substring(prefix.length());
        }
        if (!FORMAT.equals(values[0])) {
            return new Read(null, "format must be " + FORMAT + ", got '" + values[0] + "'");
        }
        final int coveredLength = text.lastIndexOf("sha256=");
        final String expected = sha256Hex(bytes, coveredLength);
        if (!skipDigestForTest && !expected.equals(values[10])) {
            return new Read(null, "sha256 does not match the content");
        }
        final Record record;
        try {
            record = new Record(values[1], Math.toIntExact(number("cluster_id", values[2])),
                number("genesis_id", values[3]), number("log_position_bytes", values[4]),
                number("released_up_to_bytes", values[5]), values[6], number("seq", values[7]),
                number("written_at_epoch_ms", values[8]), values[9]);
        } catch (final ArithmeticException | IllegalArgumentException e) {
            return new Read(null, e.getMessage());
        }
        final String refusal = record.refusal();
        return refusal != null ? new Read(null, refusal) : new Read(record, null);
    }

    /** A number in its one canonical spelling: no sign but a leading minus, no leading zeros, no spaces. */
    private static long number(final String key, final String raw) {
        final long value;
        try {
            value = Long.parseLong(raw);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(key + " is not a number: '" + raw + "'");
        }
        if (!String.valueOf(value).equals(raw)) {
            throw new IllegalArgumentException(key + " is not written canonically: '" + raw + "'");
        }
        return value;
    }

    private static boolean printable(final String s, final boolean spaceAllowed) {
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c > 0x7e || c < 0x20 || (c == ' ' && !spaceAllowed)) {
                return false;
            }
        }
        return true;
    }

    static String sha256Hex(final byte[] bytes, final int length) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, 0, length);
            final StringBuilder hex = new StringBuilder(64);
            for (final byte b : digest.digest()) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            }
            return hex.toString();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Replace the member's file: temp file, fsync, atomic rename, fsync of the directory. A failure before the
     * rename leaves the previous file as it was.
     *
     * @throws IllegalArgumentException when the record breaks a rule of the format
     */
    public static void write(final File clusterDir, final Record record) throws IOException {
        write(clusterDir, record, () -> { });
    }

    /** As above; {@code beforeRename} runs between the fsync of the temp file and the rename (tests: a crash). */
    static void write(final File clusterDir, final Record record, final Runnable beforeRename) throws IOException {
        final String refusal = record.refusal();
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
        final Path file = file(clusterDir);
        final Path dir = file.getParent();
        Files.createDirectories(dir);
        final Path temp = dir.resolve(FILE_NAME + ".tmp");
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
            final ByteBuffer bytes = ByteBuffer.wrap(record.encode());
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        }
        beforeRename.run();
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        syncDirectory(dir);
    }

    /** Remove the member's file and make the removal durable. */
    static void delete(final File clusterDir) throws IOException {
        final Path file = file(clusterDir);
        Files.deleteIfExists(file);
        syncDirectory(file.getParent());
    }

    private static void syncDirectory(final Path dir) throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    // ------------------------------------------------------------------ evaluation

    /**
     * The state and the floor for one read of the file, by the table of the class comment. A pure function: the
     * same inputs give the same answer, and nothing from an earlier call is used.
     *
     * @param mode           this member's mode
     * @param file           what {@link #read} returned
     * @param engine         this member's engine name
     * @param clusterId      this member's cluster id
     * @param genesisId      the genesis id this member has recorded, 0 or below while it has none
     * @param commitPosition this member's commit position, below 0 when it could not be read
     */
    public static Evaluation evaluate(final Mode mode, final Read file, final String engine, final int clusterId,
                                      final long genesisId, final long commitPosition) {
        if (mode == Mode.OFF) {
            return file.absent()
                ? new Evaluation(mode, State.OFF, NO_CONSTRAINT, null, null)
                : new Evaluation(mode, State.IGNORED, NO_CONSTRAINT, file.record,
                    "a watermark file exists and " + MODE_ENV + "=off ignores it");
        }
        if (file.absent()) {
            return new Evaluation(mode, State.ABSENT, absentIsNoConstraintForTest ? NO_CONSTRAINT : 0, null,
                "no watermark file: nothing is known to be off-box");
        }
        if (!file.valid()) {
            return new Evaluation(mode, State.INVALID, 0, null, file.invalid);
        }
        final Record r = file.record;
        if (genesisId <= 0) {
            return new Evaluation(mode, State.UNBOUND, 0, r, "this member has no recorded genesis id yet");
        }
        if (!r.engine.equals(engine) || r.clusterId != clusterId || r.genesisId != genesisId) {
            return new Evaluation(mode, State.FOREIGN, 0, r, "the file is for engine=" + r.engine + " cluster_id="
                + r.clusterId + " genesis_id=" + r.genesisId + ", this member is engine=" + engine + " cluster_id="
                + clusterId + " genesis_id=" + genesisId);
        }
        if (!aheadCheckOffForTest) {
            if (commitPosition < 0) {
                return new Evaluation(mode, State.AHEAD, 0, r,
                    "this member's commit position could not be read, so the watermark cannot be checked against it");
            }
            if (r.logPositionBytes > commitPosition) {
                return new Evaluation(mode, State.AHEAD, 0, r, "log_position_bytes " + r.logPositionBytes
                    + " is above this member's commit position " + commitPosition);
            }
        }
        return new Evaluation(mode, State.VALID, r.floor(), r, null);
    }

    /** Read the file now and evaluate it for this member. Updates the gauges. */
    public Evaluation evaluate(final long commitPosition, final long nowMs) {
        final Evaluation e = evaluate(mode, read(clusterDir), engine, clusterId, genesisId.getAsLong(),
            commitPosition);
        lastState = e.state;
        lastPositionBytes = e.state == State.VALID ? e.record.logPositionBytes : -1;
        lastReleasedBytes = e.state == State.VALID ? e.record.releasedUpToBytes : NO_RELEASE;
        lastAgeMs = e.record == null ? -1 : Math.max(0, nowMs - e.record.writtenAtEpochMs);
        return e;
    }

    /**
     * The lines of one purge pass, and the held-bytes gauge.
     *
     * @param e         the evaluation the pass used
     * @param otherFloor the lowest of the pass's other bounds (own floor, retention floor, newest snapshot)
     * @param nowMs     the wall clock
     * @param out       where an informational line goes
     * @param err       where a CRITICAL line goes
     */
    public void reportPass(final Evaluation e, final long otherFloor, final long nowMs, final Consumer<String> out,
                           final Consumer<String> err) {
        if (e.state == State.OFF) {
            lastHeldBytes = 0;
            return; // no off-box consumer: nothing to say, as before the watermark existed
        }
        final Record r = e.record;
        final long ageMs = r == null ? -1 : Math.max(0, nowMs - r.writtenAtEpochMs);
        final long held = e.floor == NO_CONSTRAINT || otherFloor == NO_CONSTRAINT ? 0
            : Math.max(0, otherFloor - e.floor);
        lastHeldBytes = held;
        out.accept("[RETENTION] off-box: floor " + describe(e.floor) + " state " + e.state + " position "
            + (r == null ? -1 : r.logPositionBytes) + " seq " + (r == null ? -1 : r.seq) + " age " + ageMs
            + " source " + (r == null ? "-" : r.source));
        if (e.state == State.IGNORED) {
            err.accept("[RETENTION] CRITICAL: " + file(clusterDir) + " exists and " + MODE_ENV + "=off ignores it:"
                + " this member purges without an off-box bound. Set " + MODE_ENV + "=required and restart, or"
                + " remove the file with OffBoxWatermarkTool clear.");
            return;
        }
        if (e.state != State.VALID) {
            err.accept("[RETENTION] CRITICAL: off-box watermark " + e.state + ": " + e.detail + ". "
                + (mode == Mode.REQUIRED ? "This member purges nothing until a correct watermark is set"
                    + " (OffBoxWatermarkTool set)." : "Under " + MODE_ENV + "=required this member would purge"
                    + " nothing."));
        } else if (r.releasedUpToBytes > r.logPositionBytes) {
            err.accept("[RETENTION] CRITICAL: off-box hold released by operator up to " + r.releasedUpToBytes
                + " (durable " + r.logPositionBytes + "): log in [" + r.logPositionBytes + ", "
                + r.releasedUpToBytes + ") may be purged without an off-box copy; reason: " + r.releaseReason);
        }
        if (mode == Mode.REPORT) {
            out.accept("[RETENTION] " + MODE_ENV + "=report: the off-box floor " + describe(e.floor) + " ("
                + e.state + ") is not applied");
        }
        if (held > alarmBytes) {
            err.accept("[RETENTION] CRITICAL: " + held + " bytes of log are held for the off-box copy (above "
                + ALARM_BYTES_ENV + " " + alarmBytes + "): the off-box position is " + describe(e.floor)
                + " and this member could otherwise purge to " + otherFloor + ". The disk fills unless the"
                + " archiver catches up or an operator releases the hold (OffBoxWatermarkTool release).");
        }
        if (ageMs > staleMs) {
            err.accept("[RETENTION] CRITICAL: the off-box watermark was written " + ageMs + " ms ago (above "
                + STALE_MS_ENV + " " + staleMs + "), by " + r.source + " with seq " + r.seq
                + ": the archiver is not refreshing it.");
        }
    }

    static String describe(final long floor) {
        return floor == NO_CONSTRAINT ? "(none)" : String.valueOf(floor);
    }

    // ------------------------------------------------------------------ gauges

    public Mode mode() {
        return mode;
    }

    /** The state of the last evaluation. */
    public State state() {
        return lastState;
    }

    /** {@code log_position_bytes} of the last evaluation, -1 when it was not VALID. */
    public long positionBytes() {
        return lastPositionBytes;
    }

    /** The log the last purge pass kept only for the off-box copy, in bytes. */
    public long heldBytes() {
        return lastHeldBytes;
    }

    /** The age of the file at the last evaluation in ms, -1 when no record was read. */
    public long ageMs() {
        return lastAgeMs;
    }

    /** {@code released_up_to_bytes} of the last evaluation, -1 when no release is in force. */
    public long releasedBytes() {
        return lastReleasedBytes;
    }

    /** The gauges every engine exports, with the name each one takes after the engine's prefix. */
    public enum Gauge {
        MODE("retention_offbox_mode", "Off-box retention mode (0 off, 1 report, 2 required)"),
        STATE("retention_offbox_state", "Off-box watermark state (0 VALID, 1 ABSENT, 2 INVALID, 3 UNBOUND,"
            + " 4 FOREIGN, 5 AHEAD, 6 IGNORED, 7 OFF)"),
        POSITION_BYTES("retention_offbox_position_bytes", "Log position durably off-box (-1 when not VALID)"),
        HELD_BYTES("retention_offbox_held_bytes", "Log bytes the last purge pass kept only for the off-box copy"),
        AGE_MS("retention_offbox_age_ms", "Age of the off-box watermark file (-1 when none was read)"),
        RELEASED_BYTES("retention_offbox_released_bytes", "Position an operator released the hold up to (-1: none)");

        private final String metricName;
        private final String help;

        Gauge(final String metricName, final String help) {
            this.metricName = metricName;
            this.help = help;
        }

        public String metricName() {
            return metricName;
        }

        public String help() {
            return help;
        }

        /** The value for {@code watermark}; a member whose pruner is not wired reports OFF and no position. */
        public long read(final OffBoxWatermark watermark) {
            if (watermark == null) {
                return switch (this) {
                    case MODE -> Mode.OFF.code();
                    case STATE -> State.OFF.code();
                    case HELD_BYTES -> 0;
                    default -> -1;
                };
            }
            return switch (this) {
                case MODE -> watermark.mode.code();
                case STATE -> watermark.lastState.code();
                case POSITION_BYTES -> watermark.lastPositionBytes;
                case HELD_BYTES -> watermark.lastHeldBytes;
                case AGE_MS -> watermark.lastAgeMs;
                case RELEASED_BYTES -> watermark.lastReleasedBytes;
            };
        }
    }
}
