// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The command that registers a reader of a member's journal and delivers its cursor ({@link JournalReaders}). It
 * runs on the member's host, against that member's cluster directory; the files are never shared between hosts.
 *
 * <pre>
 * set     --cluster-dir DIR --engine core|match --cluster-id N --genesis-id G --journal J --reader R
 *         --applied-journal-seq S --source NAME [--allow-lower]
 * release --cluster-dir DIR --genesis-id G --journal J --reader R --up-to-journal-seq S --reason TEXT
 * remove  --cluster-dir DIR --genesis-id G --journal J --reader R --reason TEXT
 * show    --cluster-dir DIR
 * </pre>
 *
 * <p>Registering a reader is its first {@code set} (the value 0 is legal), done before the reader first starts. A
 * reader with no file is not protected. Every value is a named flag with its unit in the name. A positional
 * argument, an unknown flag, a missing flag or a repeated flag is a usage error. Exit codes: 0 done, 64 usage,
 * 65 refused input, 1 I/O failure.</p>
 */
public final class JournalReaderTool {

    public static final int EXIT_OK = 0;
    public static final int EXIT_IO = 1;
    public static final int EXIT_USAGE = 64;
    public static final int EXIT_REFUSED = 65;

    static final String USAGE = String.join("\n",
        "Usage: JournalReaderTool <command> <flags>",
        "  set     --cluster-dir <dir> --engine core|match --cluster-id <n> --genesis-id <g> --journal <j>",
        "          --reader <r> --applied-journal-seq <s> --source <name> [--allow-lower]",
        "  release --cluster-dir <dir> --genesis-id <g> --journal <j> --reader <r> --up-to-journal-seq <s>",
        "          --reason <text>",
        "  remove  --cluster-dir <dir> --genesis-id <g> --journal <j> --reader <r> --reason <text>",
        "  show    --cluster-dir <dir>",
        "Exit codes: 0 done, 64 usage, 65 refused input, 1 I/O failure.");

    private static final String LOG = "[JOURNAL-READER] ";
    private static final String CLUSTER_DIR = "--cluster-dir";
    private static final String ENGINE = "--engine";
    private static final String CLUSTER_ID = "--cluster-id";
    private static final String GENESIS_ID = "--genesis-id";
    private static final String JOURNAL = "--journal";
    private static final String READER = "--reader";
    private static final String APPLIED = "--applied-journal-seq";
    private static final String SOURCE = "--source";
    private static final String ALLOW_LOWER = "--allow-lower";
    private static final String UP_TO = "--up-to-journal-seq";
    private static final String REASON = "--reason";

    /** A usage error (exit 64) or a refused input (exit 65). */
    private static final class Stop extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final int exit;

        Stop(final int exit, final String message) {
            super(message);
            this.exit = exit;
        }
    }

    private JournalReaderTool() {
    }

    public static void main(final String[] args) {
        System.exit(run(args, System.out, System.err, System::currentTimeMillis));
    }

    /** The command without the exit, for tests and for a host that calls it in process. */
    public static int run(final String[] args, final PrintStream out, final PrintStream err,
                          final LongSupplier clock) {
        try {
            if (args.length == 0) {
                throw new Stop(EXIT_USAGE, "no command");
            }
            switch (args[0]) {
                case "set":
                    return set(flags(args, Set.of(CLUSTER_DIR, ENGINE, CLUSTER_ID, GENESIS_ID, JOURNAL, READER, APPLIED,
                        SOURCE), Set.of(ALLOW_LOWER)), out, err, clock);
                case "release":
                    return release(flags(args, Set.of(CLUSTER_DIR, GENESIS_ID, JOURNAL, READER, UP_TO, REASON),
                        Set.of()), out, err, clock);
                case "remove":
                    return remove(flags(args, Set.of(CLUSTER_DIR, GENESIS_ID, JOURNAL, READER, REASON), Set.of()),
                        out, err);
                case "show":
                    return show(flags(args, Set.of(CLUSTER_DIR), Set.of()), out, err);
                default:
                    throw new Stop(EXIT_USAGE, "unknown command '" + args[0] + "'");
            }
        } catch (final Stop stop) {
            err.println(LOG + (stop.exit == EXIT_USAGE ? "usage: " : "refused: ") + stop.getMessage());
            if (stop.exit == EXIT_USAGE) {
                err.println(USAGE);
            }
            return stop.exit;
        } catch (final IOException | java.io.UncheckedIOException e) {
            err.println(LOG + "FAILED: " + e);
            return EXIT_IO;
        }
    }

    /**
     * The flags after the command. {@code valued} flags take the next argument as their value and are all required;
     * {@code switches} take none and are optional (present maps to "true").
     */
    private static Map<String, String> flags(final String[] args, final Set<String> valued,
                                             final Set<String> switches) {
        final Map<String, String> found = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            final String flag = args[i];
            if (!flag.startsWith("--")) {
                throw new Stop(EXIT_USAGE, "positional argument '" + flag + "': every value is a named flag");
            }
            if (found.containsKey(flag)) {
                throw new Stop(EXIT_USAGE, "flag " + flag + " given twice");
            }
            if (switches.contains(flag)) {
                found.put(flag, "true");
            } else if (valued.contains(flag)) {
                if (i + 1 >= args.length) {
                    throw new Stop(EXIT_USAGE, "flag " + flag + " needs a value");
                }
                found.put(flag, args[++i]);
            } else {
                throw new Stop(EXIT_USAGE, "unknown flag " + flag + " for " + args[0]);
            }
        }
        for (final String flag : valued) {
            if (!found.containsKey(flag)) {
                throw new Stop(EXIT_USAGE, "missing flag " + flag);
            }
        }
        return found;
    }

    private static long number(final Map<String, String> flags, final String flag) {
        final String raw = flags.get(flag);
        try {
            return Long.parseLong(raw);
        } catch (final NumberFormatException e) {
            throw new Stop(EXIT_REFUSED, flag + " is not a number: '" + raw + "'");
        }
    }

    private static File clusterDir(final Map<String, String> flags) throws IOException {
        final File dir = new File(flags.get(CLUSTER_DIR));
        if (!dir.isDirectory()) {
            throw new IOException(CLUSTER_DIR + " " + dir + " is not a directory");
        }
        return dir;
    }

    /** The journal and reader names of a command, checked as names (a path can never leave the reader directory). */
    private static String name(final Map<String, String> flags, final String flag) {
        final String value = flags.get(flag);
        if (!JournalReaders.validName(value)) {
            throw new Stop(EXIT_REFUSED, flag + " must be a name (1.." + JournalReaders.MAX_NAME_LENGTH
                + " of A-Z a-z 0-9 . _ -, starting with a letter or digit, not ending with .tmp), got '" + value + "'");
        }
        return value;
    }

    private static long next(final long seq) {
        return seq == Long.MAX_VALUE ? seq : seq + 1;
    }

    private static int set(final Map<String, String> flags, final PrintStream out, final PrintStream err,
                           final LongSupplier clock) throws IOException {
        final String engine = flags.get(ENGINE);
        final long clusterIdLong = number(flags, CLUSTER_ID);
        final long genesisId = number(flags, GENESIS_ID);
        final long applied = number(flags, APPLIED);
        final String journal = name(flags, JOURNAL);
        final String reader = name(flags, READER);
        if (!JournalReaders.ENGINE_CORE.equals(engine) && !JournalReaders.ENGINE_MATCH.equals(engine)) {
            throw new Stop(EXIT_REFUSED, ENGINE + " must be core or match, got '" + engine + "'");
        }
        if (!JournalReaders.journalsOf(engine).contains(journal)) {
            throw new Stop(EXIT_REFUSED, JOURNAL + " '" + journal + "' is not a journal of engine " + engine
                + " (its journals: " + new java.util.TreeSet<>(JournalReaders.journalsOf(engine)) + ")");
        }
        if (clusterIdLong < 0 || clusterIdLong > Integer.MAX_VALUE) {
            throw new Stop(EXIT_REFUSED, CLUSTER_ID + " must be 0.." + Integer.MAX_VALUE + ", got " + clusterIdLong);
        }
        final int clusterId = (int) clusterIdLong;
        final File dir = clusterDir(flags);
        final JournalReaders.Read existing = JournalReaders.read(dir, journal, reader);
        long seq = 1;
        long released = JournalReaders.NO_RELEASE;
        String reason = "";
        if (existing.valid()) {
            final JournalReaders.Record old = existing.record;
            if (!old.engine.equals(engine) || old.clusterId != clusterId || old.genesisId != genesisId
                || !old.journal.equals(journal) || !old.reader.equals(reader)) {
                throw new Stop(EXIT_REFUSED, "the file is for engine=" + old.engine + " cluster_id=" + old.clusterId
                    + " genesis_id=" + old.genesisId + " journal=" + old.journal + " reader=" + old.reader
                    + ", asked engine=" + engine + " cluster_id=" + clusterId + " genesis_id=" + genesisId
                    + " journal=" + journal + " reader=" + reader + ": remove it first");
            }
            if (applied < old.appliedJournalSeq) {
                if (!flags.containsKey(ALLOW_LOWER)) {
                    throw new Stop(EXIT_REFUSED, APPLIED + " " + applied + " is below the file's "
                        + old.appliedJournalSeq + ": a lower sequence needs " + ALLOW_LOWER);
                }
                err.println(LOG + "CRITICAL: reader " + reader + " of the " + journal + " journal went backwards: its"
                    + " applied journalSeq goes down from " + old.appliedJournalSeq + " to " + applied + " (source "
                    + flags.get(SOURCE) + "). Records in (" + applied + ", " + old.appliedJournalSeq + "] may already"
                    + " be purged on this member.");
            }
            seq = next(old.seq);
            if (applied < old.releasedUpToJournalSeq) {
                // the release is still above what the reader applied: it stays until a set reaches it
                released = old.releasedUpToJournalSeq;
                reason = old.releaseReason;
            }
        } else if (!existing.absent()) {
            err.println(LOG + "replacing an invalid file (" + existing.invalid + "): seq starts again at 1");
        }
        final JournalReaders.Record record = new JournalReaders.Record(engine, clusterId, genesisId, journal, reader,
            applied, released, reason, seq, clock.getAsLong(), flags.get(SOURCE));
        final String refusal = record.refusal();
        if (refusal != null) {
            throw new Stop(EXIT_REFUSED, refusal);
        }
        JournalReaders.write(dir, record);
        out.println(LOG + "set: " + record);
        return EXIT_OK;
    }

    private static int release(final Map<String, String> flags, final PrintStream out, final PrintStream err,
                               final LongSupplier clock) throws IOException {
        final long genesisId = number(flags, GENESIS_ID);
        final long upTo = number(flags, UP_TO);
        final String reason = flags.get(REASON);
        final String journal = name(flags, JOURNAL);
        final String reader = name(flags, READER);
        final File dir = clusterDir(flags);
        final JournalReaders.Record old = existingFor(dir, journal, reader, genesisId, "release");
        if (upTo <= old.appliedJournalSeq) {
            throw new Stop(EXIT_REFUSED, UP_TO + " " + upTo + " is not above the reader's applied journalSeq "
                + old.appliedJournalSeq + ": nothing to release");
        }
        if (reason.isEmpty()) {
            throw new Stop(EXIT_REFUSED, REASON + " must not be empty");
        }
        final JournalReaders.Record record = new JournalReaders.Record(old.engine, old.clusterId, old.genesisId,
            old.journal, old.reader, old.appliedJournalSeq, upTo, reason, next(old.seq), clock.getAsLong(), old.source);
        final String refusal = record.refusal();
        if (refusal != null) {
            throw new Stop(EXIT_REFUSED, refusal);
        }
        JournalReaders.write(dir, record);
        err.println(LOG + "CRITICAL: hold of reader " + reader + " of the " + journal + " journal released by operator"
            + " up to journalSeq " + upTo + " (applied " + old.appliedJournalSeq + "): records in ("
            + old.appliedJournalSeq + ", " + upTo + "] may be purged on this member before that reader applied them;"
            + " reason: " + reason);
        out.println(LOG + "release: " + record);
        return EXIT_OK;
    }

    private static int remove(final Map<String, String> flags, final PrintStream out, final PrintStream err)
        throws IOException {
        final long genesisId = number(flags, GENESIS_ID);
        final String reason = flags.get(REASON);
        final String journal = name(flags, JOURNAL);
        final String reader = name(flags, READER);
        final File dir = clusterDir(flags);
        if (reason.isEmpty()) {
            throw new Stop(EXIT_REFUSED, REASON + " must not be empty");
        }
        final JournalReaders.Read read = JournalReaders.read(dir, journal, reader);
        if (read.absent()) {
            out.println(LOG + "remove: no reader file at " + JournalReaders.file(dir, journal, reader));
            return EXIT_OK;
        }
        if (read.valid() && read.record.genesisId != genesisId) {
            throw new Stop(EXIT_REFUSED, "genesis mismatch: asked " + genesisId + ", the file has "
                + read.record.genesisId);
        }
        JournalReaders.delete(dir, journal, reader);
        err.println(LOG + "CRITICAL: reader " + reader + " of the " + journal + " journal is no longer registered on"
            + " this member (" + (read.valid() ? "was " + read.record : "its file was invalid: " + read.invalid)
            + "): its records are no longer held for it; reason: " + reason);
        out.println(LOG + "removed: " + JournalReaders.file(dir, journal, reader));
        return EXIT_OK;
    }

    private static int show(final Map<String, String> flags, final PrintStream out, final PrintStream err)
        throws IOException {
        final File dir = clusterDir(flags);
        final Path root = dir.toPath().resolve(JournalReaders.DIRECTORY).resolve(JournalReaders.READERS_DIRECTORY);
        if (!Files.isDirectory(root)) {
            out.println(LOG + "no reader is registered under " + root);
            return EXIT_OK;
        }
        final List<String> journals = new java.util.ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
            for (final Path p : stream) {
                journals.add(p.getFileName().toString());
            }
        }
        java.util.Collections.sort(journals);
        int invalid = 0;
        int shown = 0;
        for (final String journal : journals) {
            if (!JournalReaders.validName(journal)) {
                err.println(LOG + "refused: " + root.resolve(journal) + " is not a journal directory");
                invalid++;
                continue;
            }
            for (final JournalReaders.Entry e : JournalReaders.list(dir, journal)) {
                if (!e.read().valid()) {
                    err.println(LOG + "refused: the file at " + JournalReaders.directory(dir, journal).resolve(e.name())
                        + " is invalid: " + e.read().invalid);
                    invalid++;
                    continue;
                }
                out.println("# " + JournalReaders.file(dir, journal, e.name()));
                out.print(new String(e.read().record.encode(), StandardCharsets.US_ASCII));
                shown++;
            }
        }
        if (shown == 0 && invalid == 0) {
            out.println(LOG + "no reader is registered under " + root);
        }
        return invalid > 0 ? EXIT_REFUSED : EXIT_OK;
    }

    /** The valid file of a reader, which must carry {@code genesisId}. */
    private static JournalReaders.Record existingFor(final File dir, final String journal, final String reader,
                                                     final long genesisId, final String command) {
        final JournalReaders.Read read = JournalReaders.read(dir, journal, reader);
        final Path file = JournalReaders.file(dir, journal, reader);
        if (read.absent()) {
            throw new Stop(EXIT_REFUSED, "no reader file at " + file + ": " + command + " needs one (set it first)");
        }
        if (!read.valid()) {
            throw new Stop(EXIT_REFUSED, "the file at " + file + " is invalid (" + read.invalid + "): " + command
                + " needs a valid file (set replaces an invalid one)");
        }
        if (read.record.genesisId != genesisId) {
            throw new Stop(EXIT_REFUSED, "genesis mismatch: asked " + genesisId + ", the file has "
                + read.record.genesisId);
        }
        return read.record;
    }
}
