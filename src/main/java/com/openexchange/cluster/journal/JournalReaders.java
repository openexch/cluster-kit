// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The registered readers of a member's journals: one small file per journal and reader,
 * {@code <clusterDir>/retention/journal-readers/<journal>/<reader>}, written on the member's host by
 * {@link JournalReaderTool}. A file says "this reader has durably applied every record of this genesis with
 * {@code journalSeq <= applied_journal_seq}". {@link JournalRetention} reads the files on every pass and never purges a
 * record a registered reader has not applied. A reader with no file is not protected.
 *
 * <h2>The file</h2>
 *
 * <p>Text, one {@code key=value} per line, LF line ends, the keys in this order (the off-box watermark's discipline:
 * temp file, fsync, atomic rename, fsync of the directory):</p>
 * <pre>
 * format=journal-reader/1
 * engine=core
 * cluster_id=0
 * genesis_id=17
 * journal=order
 * reader=oe-projector
 * applied_journal_seq=1200
 * released_up_to_journal_seq=-1
 * release_reason=
 * seq=3
 * written_at_epoch_ms=1764000000000
 * source=console
 * sha256=...
 * </pre>
 *
 * <p>{@code sha256} covers every byte above its own line. An unknown key, a missing key, a key out of order, a number
 * out of range or not in its canonical spelling, a CR, or a digest that does not match makes the file invalid, and an
 * invalid file holds the journal. The digest guards the file; the identity is the genesis id, the journal name and
 * the reader name.</p>
 */
public final class JournalReaders {

    public static final String DIRECTORY = "retention";
    public static final String READERS_DIRECTORY = "journal-readers";
    public static final String FORMAT = "journal-reader/1";
    public static final String ENGINE_CORE = "core";
    public static final String ENGINE_MATCH = "match";
    /** {@code released_up_to_journal_seq} when no release is in force. */
    public static final long NO_RELEASE = -1;
    static final int MAX_NAME_LENGTH = 64;
    static final int MAX_SOURCE_LENGTH = 64;
    static final int MAX_REASON_LENGTH = 200;
    /** A valid file is a few hundred bytes; anything much larger is not this file. */
    private static final int MAX_FILE_BYTES = 4096;
    private static final String TEMP_SUFFIX = ".tmp";

    private static final String[] KEYS = {
        "format", "engine", "cluster_id", "genesis_id", "journal", "reader", "applied_journal_seq",
        "released_up_to_journal_seq", "release_reason", "seq", "written_at_epoch_ms", "source", "sha256"
    };

    /** Test seam, the negative control of the digest rule. */
    static volatile boolean skipDigestForTest;

    private JournalReaders() {
    }

    /**
     * A journal or reader name: 1..64 of {@code A-Z a-z 0-9 . _ -}, starting with a letter or a digit, not ending
     * with {@code .tmp}. The name is a file name on the member and a label value in the metrics, so nothing else is
     * accepted.
     */
    public static boolean validName(final String name) {
        if (name == null || name.isEmpty() || name.length() > MAX_NAME_LENGTH || name.endsWith(TEMP_SUFFIX)) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            final boolean alnum = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
            if (!alnum && (i == 0 || (c != '.' && c != '_' && c != '-'))) {
                return false;
            }
        }
        return true;
    }

    /**
     * The journals of an engine that take registered readers. The tool refuses any other name, so a mistyped journal
     * cannot register a reader in a directory no member reads.
     */
    public static java.util.Set<String> journalsOf(final String engine) {
        return ENGINE_CORE.equals(engine) ? java.util.Set.of("money", "order") : java.util.Set.of();
    }

    /** The content of a valid file. */
    public static final class Record {
        public final String engine;
        public final int clusterId;
        public final long genesisId;
        public final String journal;
        public final String reader;
        public final long appliedJournalSeq;
        public final long releasedUpToJournalSeq;
        public final String releaseReason;
        public final long seq;
        public final long writtenAtEpochMs;
        public final String source;

        public Record(final String engine, final int clusterId, final long genesisId, final String journal,
                      final String reader, final long appliedJournalSeq, final long releasedUpToJournalSeq,
                      final String releaseReason, final long seq, final long writtenAtEpochMs, final String source) {
            this.engine = engine;
            this.clusterId = clusterId;
            this.genesisId = genesisId;
            this.journal = journal;
            this.reader = reader;
            this.appliedJournalSeq = appliedJournalSeq;
            this.releasedUpToJournalSeq = releasedUpToJournalSeq;
            this.releaseReason = releaseReason;
            this.seq = seq;
            this.writtenAtEpochMs = writtenAtEpochMs;
            this.source = source;
        }

        /** The sequence the member may purge up to for this reader: what it applied, or the operator's release. */
        public long bound() {
            return Math.max(appliedJournalSeq, releasedUpToJournalSeq);
        }

        /** Whether an operator's release is in force (above what the reader applied). */
        public boolean released() {
            return releasedUpToJournalSeq > appliedJournalSeq;
        }

        /** Why this content cannot be written, or null. The same rules a reader of the file applies. */
        public String refusal() {
            if (!ENGINE_CORE.equals(engine) && !ENGINE_MATCH.equals(engine)) {
                return "engine must be core or match, got '" + engine + "'";
            }
            if (clusterId < 0) {
                return "cluster_id must be 0 or above, got " + clusterId;
            }
            if (genesisId == 0) {
                return "genesis_id must not be 0";
            }
            if (!validName(journal)) {
                return "journal must be a name (1.." + MAX_NAME_LENGTH + " of A-Z a-z 0-9 . _ -), got '" + journal + "'";
            }
            if (!validName(reader)) {
                return "reader must be a name (1.." + MAX_NAME_LENGTH + " of A-Z a-z 0-9 . _ -, not ending with .tmp),"
                    + " got '" + reader + "'";
            }
            if (appliedJournalSeq < 0) {
                return "applied_journal_seq must be 0 or above, got " + appliedJournalSeq;
            }
            if (releasedUpToJournalSeq < NO_RELEASE) {
                return "released_up_to_journal_seq must be -1 or a sequence, got " + releasedUpToJournalSeq;
            }
            if (releaseReason == null || releaseReason.length() > MAX_REASON_LENGTH
                || !printable(releaseReason, true)) {
                return "release_reason must be at most " + MAX_REASON_LENGTH + " printable ASCII characters";
            }
            if ((releasedUpToJournalSeq == NO_RELEASE) != releaseReason.isEmpty()) {
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
                + "journal=" + journal + "\n"
                + "reader=" + reader + "\n"
                + "applied_journal_seq=" + appliedJournalSeq + "\n"
                + "released_up_to_journal_seq=" + releasedUpToJournalSeq + "\n"
                + "release_reason=" + releaseReason + "\n"
                + "seq=" + seq + "\n"
                + "written_at_epoch_ms=" + writtenAtEpochMs + "\n"
                + "source=" + source + "\n";
            final byte[] covered = body.getBytes(StandardCharsets.US_ASCII);
            return (body + "sha256=" + sha256Hex(covered, covered.length) + "\n").getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public String toString() {
            return "engine=" + engine + " cluster_id=" + clusterId + " genesis_id=" + genesisId + " journal=" + journal
                + " reader=" + reader + " applied_journal_seq=" + appliedJournalSeq + " released_up_to_journal_seq="
                + releasedUpToJournalSeq + " seq=" + seq + " written_at_epoch_ms=" + writtenAtEpochMs + " source="
                + source;
        }
    }

    /** One read of a reader's file: absent, invalid (with the reason), or a record. */
    public static final class Read {
        public final Record record;
        public final String invalid;

        private Read(final Record record, final String invalid) {
            this.record = record;
            this.invalid = invalid;
        }

        static Read of(final Record record) {
            return new Read(record, null);
        }

        static Read invalid(final String why) {
            return new Read(null, why);
        }

        public boolean absent() {
            return record == null && invalid == null;
        }

        public boolean valid() {
            return record != null;
        }
    }

    /** One file of a journal's reader directory: the name it has there, and what it holds. */
    public record Entry(String name, Read read) {
    }

    // ------------------------------------------------------------------ paths

    /** The directory holding the reader files of one journal. */
    public static Path directory(final File clusterDir, final String journal) {
        return clusterDir.toPath().resolve(DIRECTORY).resolve(READERS_DIRECTORY).resolve(journal);
    }

    /** Where the file of one reader of one journal lives. */
    public static Path file(final File clusterDir, final String journal, final String reader) {
        if (!validName(journal) || !validName(reader)) {
            throw new IllegalArgumentException("journal '" + journal + "' and reader '" + reader + "' must be names");
        }
        return directory(clusterDir, journal).resolve(reader);
    }

    // ------------------------------------------------------------------ reading

    /** Read one reader's file. Never throws for a file that is missing or malformed: both are answers. */
    public static Read read(final File clusterDir, final String journal, final String reader) {
        return read(file(clusterDir, journal, reader));
    }

    static Read read(final Path file) {
        final byte[] bytes;
        try {
            if (Files.size(file) > MAX_FILE_BYTES) {
                return Read.invalid("the file is larger than " + MAX_FILE_BYTES + " bytes");
            }
            bytes = Files.readAllBytes(file);
        } catch (final NoSuchFileException absent) {
            return new Read(null, null);
        } catch (final IOException e) {
            // a file that exists and cannot be read is not an absent file
            return Read.invalid("the file could not be read: " + e);
        }
        return parse(bytes);
    }

    /**
     * Every file of a journal's reader directory, by name. A missing directory is an empty list. A temp file of a
     * write in flight ({@code *.tmp}) is not a reader. Anything else that is not a regular file with a reader's name
     * is listed as an invalid entry, so it holds the journal instead of being skipped.
     *
     * @throws IOException the directory exists and could not be listed (the caller holds)
     */
    public static List<Entry> list(final File clusterDir, final String journal) throws IOException {
        final Path dir = directory(clusterDir, journal);
        final List<Entry> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (final Path p : stream) {
                final String name = p.getFileName().toString();
                if (name.endsWith(TEMP_SUFFIX)) {
                    continue;
                }
                if (!validName(name)) {
                    entries.add(new Entry(name, Read.invalid("the file name is not a reader name")));
                } else if (!Files.isRegularFile(p)) {
                    entries.add(new Entry(name, Read.invalid("not a regular file")));
                } else {
                    final Read read = read(p);
                    // deleted between the listing and the read: no longer a registered reader
                    if (!read.absent()) {
                        entries.add(new Entry(name, read));
                    }
                }
            }
        } catch (final NoSuchFileException | NotDirectoryException absent) {
            if (Files.exists(dir)) {
                throw new IOException(dir + " is not a directory");
            }
            return List.of();
        }
        entries.sort(Comparator.comparing(Entry::name));
        return entries;
    }

    /** Parse the bytes of a file. */
    static Read parse(final byte[] bytes) {
        for (final byte b : bytes) {
            if (b == '\r') {
                return Read.invalid("CR in the file: line ends must be LF");
            }
            if (b != '\n' && (b < 0x20 || b > 0x7e)) {
                return Read.invalid("a byte outside printable ASCII");
            }
        }
        if (bytes.length == 0 || bytes[bytes.length - 1] != '\n') {
            return Read.invalid("the file does not end with a line end");
        }
        final String text = new String(bytes, StandardCharsets.US_ASCII);
        final String[] lines = text.substring(0, text.length() - 1).split("\n", -1);
        if (lines.length != KEYS.length) {
            return Read.invalid("expected " + KEYS.length + " lines, found " + lines.length
                + " (a key is missing or unknown)");
        }
        final String[] values = new String[KEYS.length];
        for (int i = 0; i < KEYS.length; i++) {
            final String prefix = KEYS[i] + "=";
            if (!lines[i].startsWith(prefix)) {
                return Read.invalid("line " + (i + 1) + " must be the key " + KEYS[i]
                    + " (unknown key, missing key or wrong order)");
            }
            values[i] = lines[i].substring(prefix.length());
        }
        if (!FORMAT.equals(values[0])) {
            return Read.invalid("format must be " + FORMAT + ", got '" + values[0] + "'");
        }
        final int coveredLength = text.lastIndexOf("sha256=");
        if (!skipDigestForTest && !sha256Hex(bytes, coveredLength).equals(values[12])) {
            return Read.invalid("sha256 does not match the content");
        }
        final Record record;
        try {
            record = new Record(values[1], Math.toIntExact(number("cluster_id", values[2])),
                number("genesis_id", values[3]), values[4], values[5], number("applied_journal_seq", values[6]),
                number("released_up_to_journal_seq", values[7]), values[8], number("seq", values[9]),
                number("written_at_epoch_ms", values[10]), values[11]);
        } catch (final ArithmeticException | IllegalArgumentException e) {
            return Read.invalid(e.getMessage());
        }
        final String refusal = record.refusal();
        return refusal != null ? Read.invalid(refusal) : Read.of(record);
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

    // ------------------------------------------------------------------ writing

    /**
     * Replace a reader's file: temp file, fsync, atomic rename, fsync of the directory. A failure before the rename
     * leaves the previous file as it was.
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
        final Path file = file(clusterDir, record.journal, record.reader);
        final Path dir = file.getParent();
        Files.createDirectories(dir);
        final Path temp = dir.resolve(record.reader + TEMP_SUFFIX);
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

    /** Remove a reader's file and make the removal durable. */
    static void delete(final File clusterDir, final String journal, final String reader) throws IOException {
        final Path file = file(clusterDir, journal, reader);
        Files.deleteIfExists(file);
        if (Files.isDirectory(file.getParent())) {
            syncDirectory(file.getParent());
        }
    }

    private static void syncDirectory(final Path dir) throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}
