// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The command that writes a member's off-box watermark ({@link OffBoxWatermark}). It runs on the member's host,
 * against that member's cluster directory; the file is never shared between hosts.
 *
 * <pre>
 * set     --cluster-dir DIR --engine core|match --cluster-id N --genesis-id G
 *         --log-position-bytes P --source NAME [--allow-lower]
 * release --cluster-dir DIR --genesis-id G --up-to-log-position-bytes P --reason TEXT
 * show    --cluster-dir DIR
 * clear   --cluster-dir DIR --genesis-id G
 * </pre>
 *
 * <p>Every value is a named flag with its unit in the name. A positional argument, an unknown flag, a missing flag
 * or a repeated flag is a usage error. Exit codes: 0 done, 64 usage, 65 refused input, 1 I/O failure.</p>
 */
public final class OffBoxWatermarkTool {

    public static final int EXIT_OK = 0;
    public static final int EXIT_IO = 1;
    public static final int EXIT_USAGE = 64;
    public static final int EXIT_REFUSED = 65;

    static final String USAGE = String.join("\n",
        "Usage: OffBoxWatermarkTool <command> <flags>",
        "  set     --cluster-dir <dir> --engine core|match --cluster-id <n> --genesis-id <g>",
        "          --log-position-bytes <p> --source <name> [--allow-lower]",
        "  release --cluster-dir <dir> --genesis-id <g> --up-to-log-position-bytes <p> --reason <text>",
        "  show    --cluster-dir <dir>",
        "  clear   --cluster-dir <dir> --genesis-id <g>",
        "Exit codes: 0 done, 64 usage, 65 refused input, 1 I/O failure.");

    private static final String CLUSTER_DIR = "--cluster-dir";
    private static final String ENGINE = "--engine";
    private static final String CLUSTER_ID = "--cluster-id";
    private static final String GENESIS_ID = "--genesis-id";
    private static final String LOG_POSITION = "--log-position-bytes";
    private static final String SOURCE = "--source";
    private static final String ALLOW_LOWER = "--allow-lower";
    private static final String UP_TO = "--up-to-log-position-bytes";
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

    private OffBoxWatermarkTool() {
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
                    return set(flags(args, Set.of(CLUSTER_DIR, ENGINE, CLUSTER_ID, GENESIS_ID, LOG_POSITION, SOURCE),
                        Set.of(ALLOW_LOWER)), out, err, clock);
                case "release":
                    return release(flags(args, Set.of(CLUSTER_DIR, GENESIS_ID, UP_TO, REASON), Set.of()), out, err,
                        clock);
                case "show":
                    return show(flags(args, Set.of(CLUSTER_DIR), Set.of()), out, err);
                case "clear":
                    return clear(flags(args, Set.of(CLUSTER_DIR, GENESIS_ID), Set.of()), out);
                default:
                    throw new Stop(EXIT_USAGE, "unknown command '" + args[0] + "'");
            }
        } catch (final Stop stop) {
            err.println("[OFFBOX] " + (stop.exit == EXIT_USAGE ? "usage: " : "refused: ") + stop.getMessage());
            if (stop.exit == EXIT_USAGE) {
                err.println(USAGE);
            }
            return stop.exit;
        } catch (final IOException | java.io.UncheckedIOException e) {
            err.println("[OFFBOX] FAILED: " + e);
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

    private static int set(final Map<String, String> flags, final PrintStream out, final PrintStream err,
                           final LongSupplier clock) throws IOException {
        final String engine = flags.get(ENGINE);
        final long clusterIdLong = number(flags, CLUSTER_ID);
        final long genesisId = number(flags, GENESIS_ID);
        final long position = number(flags, LOG_POSITION);
        if (clusterIdLong < 0 || clusterIdLong > Integer.MAX_VALUE) {
            throw new Stop(EXIT_REFUSED, CLUSTER_ID + " must be 0.." + Integer.MAX_VALUE + ", got " + clusterIdLong);
        }
        final int clusterId = (int) clusterIdLong;
        final File dir = clusterDir(flags);
        final OffBoxWatermark.Read existing = OffBoxWatermark.read(dir);
        long seq = 1;
        long released = OffBoxWatermark.NO_RELEASE;
        String reason = "";
        if (existing.valid()) {
            final OffBoxWatermark.Record old = existing.record;
            if (!old.engine.equals(engine) || old.clusterId != clusterId || old.genesisId != genesisId) {
                throw new Stop(EXIT_REFUSED, "the file is for engine=" + old.engine + " cluster_id=" + old.clusterId
                    + " genesis_id=" + old.genesisId + ", asked engine=" + engine + " cluster_id=" + clusterId
                    + " genesis_id=" + genesisId + ": clear it first");
            }
            if (position < old.logPositionBytes) {
                if (!flags.containsKey(ALLOW_LOWER)) {
                    throw new Stop(EXIT_REFUSED, LOG_POSITION + " " + position + " is below the file's "
                        + old.logPositionBytes + ": a lower position needs " + ALLOW_LOWER);
                }
                err.println("[OFFBOX] CRITICAL: the off-box copy lost data: the durable position goes down from "
                    + old.logPositionBytes + " to " + position + " (source " + flags.get(SOURCE) + ")");
            }
            seq = old.seq == Long.MAX_VALUE ? old.seq : old.seq + 1;
            if (position < old.releasedUpToBytes) {
                // the release is still above the durable position: it stays until a set reaches it
                released = old.releasedUpToBytes;
                reason = old.releaseReason;
            }
        } else if (!existing.absent()) {
            err.println("[OFFBOX] replacing an invalid file (" + existing.invalid + "): seq starts again at 1");
        }
        final OffBoxWatermark.Record record = new OffBoxWatermark.Record(engine, clusterId, genesisId, position,
            released, reason, seq, clock.getAsLong(), flags.get(SOURCE));
        final String refusal = record.refusal();
        if (refusal != null) {
            throw new Stop(EXIT_REFUSED, refusal);
        }
        OffBoxWatermark.write(dir, record);
        out.println("[OFFBOX] set: " + record);
        return EXIT_OK;
    }

    private static int release(final Map<String, String> flags, final PrintStream out, final PrintStream err,
                               final LongSupplier clock) throws IOException {
        final long genesisId = number(flags, GENESIS_ID);
        final long upTo = number(flags, UP_TO);
        final String reason = flags.get(REASON);
        final File dir = clusterDir(flags);
        final OffBoxWatermark.Record old = existingFor(dir, genesisId, "release");
        if (upTo <= old.logPositionBytes) {
            throw new Stop(EXIT_REFUSED, UP_TO + " " + upTo + " is not above the durable position "
                + old.logPositionBytes + ": nothing to release");
        }
        if (reason.isEmpty()) {
            throw new Stop(EXIT_REFUSED, REASON + " must not be empty");
        }
        final OffBoxWatermark.Record record = new OffBoxWatermark.Record(old.engine, old.clusterId, old.genesisId,
            old.logPositionBytes, upTo, reason, old.seq == Long.MAX_VALUE ? old.seq : old.seq + 1,
            clock.getAsLong(), old.source);
        final String refusal = record.refusal();
        if (refusal != null) {
            throw new Stop(EXIT_REFUSED, refusal);
        }
        OffBoxWatermark.write(dir, record);
        err.println("[OFFBOX] CRITICAL: off-box hold released by operator up to " + upTo + " (durable "
            + old.logPositionBytes + "): log in [" + old.logPositionBytes + ", " + upTo
            + ") may be purged on this member without an off-box copy; reason: " + reason);
        out.println("[OFFBOX] release: " + record);
        return EXIT_OK;
    }

    private static int show(final Map<String, String> flags, final PrintStream out, final PrintStream err)
        throws IOException {
        final File dir = clusterDir(flags);
        final OffBoxWatermark.Read read = OffBoxWatermark.read(dir);
        if (read.absent()) {
            out.println("[OFFBOX] no watermark file at " + OffBoxWatermark.file(dir));
            return EXIT_OK;
        }
        if (!read.valid()) {
            err.println("[OFFBOX] refused: the file at " + OffBoxWatermark.file(dir) + " is invalid: "
                + read.invalid);
            return EXIT_REFUSED;
        }
        out.print(new String(read.record.encode(), java.nio.charset.StandardCharsets.US_ASCII));
        return EXIT_OK;
    }

    private static int clear(final Map<String, String> flags, final PrintStream out) throws IOException {
        final long genesisId = number(flags, GENESIS_ID);
        final File dir = clusterDir(flags);
        if (OffBoxWatermark.read(dir).absent()) {
            out.println("[OFFBOX] clear: no watermark file at " + OffBoxWatermark.file(dir));
            return EXIT_OK;
        }
        final OffBoxWatermark.Record old = existingFor(dir, genesisId, "clear");
        OffBoxWatermark.delete(dir);
        out.println("[OFFBOX] cleared: " + old);
        return EXIT_OK;
    }

    /** The valid file of {@code dir}, which must carry {@code genesisId}. */
    private static OffBoxWatermark.Record existingFor(final File dir, final long genesisId, final String command) {
        final OffBoxWatermark.Read read = OffBoxWatermark.read(dir);
        if (read.absent()) {
            throw new Stop(EXIT_REFUSED, "no watermark file at " + OffBoxWatermark.file(dir) + ": " + command
                + " needs one (set it first)");
        }
        if (!read.valid()) {
            throw new Stop(EXIT_REFUSED, "the file at " + OffBoxWatermark.file(dir) + " is invalid ("
                + read.invalid + "): " + command + " needs a valid file (set replaces an invalid one)");
        }
        if (read.record.genesisId != genesisId) {
            throw new Stop(EXIT_REFUSED, "genesis mismatch: asked " + genesisId + ", the file has "
                + read.record.genesisId);
        }
        return read.record;
    }
}
