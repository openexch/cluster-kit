// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochClock;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Objects;

/**
 * The evidence of an oracle mismatch, kept as plain FILES under {@code REPLICA_MISMATCH_DIR/<P>/} (never archive
 * recordings: nothing a shadow catalog or a puller reads can mistake them for a shadow):
 *
 * <pre>
 * replica.image  the held image (envelope + payload), unless above REPLICA_MISMATCH_DUMP_MAX_MB (then truncated=true)
 * member.image   the retrieved member copy, same cap
 * report.txt     positions, term, both geneses, lengths, the first differing byte, both SHA-256 digests, the member
 *                copy's recording id, the newest member snapshot the backup named; always attempted
 * </pre>
 *
 * <p>A dump that already holds {@code report.txt} for P (a restarted replica re-latching at the same position) is not
 * rewritten: the report gains a {@code re-latched at <wall time>} line. An I/O failure never throws and never changes
 * the hold decision: the line {@code mismatch dump at <P> incomplete: <io error>} is logged CRITICAL, whatever part
 * of the report can be written is written, and {@link #incomplete} reads true ({@code shadow_oracle_dump_incomplete}).</p>
 */
public final class MismatchDump {

    /** The file operations, a seam for the disk-full tests. */
    interface Io {
        void createDirectories(Path dir) throws IOException;

        boolean exists(Path file);

        void write(Path file, DirectBuffer buffer, int length) throws IOException;

        void writeText(Path file, String text, boolean append) throws IOException;
    }

    static final Io FILES = new Io() {
        @Override
        public void createDirectories(final Path dir) throws IOException {
            Files.createDirectories(dir);
        }

        @Override
        public boolean exists(final Path file) {
            return Files.exists(file);
        }

        @Override
        public void write(final Path file, final DirectBuffer buffer, final int length) throws IOException {
            final byte[] chunk = new byte[64 * 1024];
            try (OutputStream out = Files.newOutputStream(file)) {
                for (int at = 0; at < length; at += chunk.length) {
                    final int n = Math.min(chunk.length, length - at);
                    buffer.getBytes(at, chunk, 0, n);
                    out.write(chunk, 0, n);
                }
            }
        }

        @Override
        public void writeText(final Path file, final String text, final boolean append) throws IOException {
            Files.writeString(file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        }
    };

    private final Path dir;
    private final int maxMb;
    private final long maxBytes;
    private final EpochClock wall;
    private final Io io;
    private volatile boolean incomplete;

    MismatchDump(final File dir, final int maxMb, final EpochClock wall, final Io io) {
        this.dir = Objects.requireNonNull(dir, "dir").toPath();
        if (maxMb < 0) {
            throw new IllegalArgumentException("REPLICA_MISMATCH_DUMP_MAX_MB must be >= 0: " + maxMb);
        }
        this.maxMb = maxMb;
        this.maxBytes = (long)maxMb * 1024 * 1024;
        this.wall = Objects.requireNonNull(wall, "wall");
        this.io = Objects.requireNonNull(io, "io");
    }

    /**
     * Boot check: the dump dir is created and a probe file written and removed, so a dump never finds a read-only
     * or missing dir at the worst moment.
     *
     * @throws IllegalArgumentException {@code REPLICA_MISMATCH_DIR is not writable: <path>}
     */
    public static void requireWritable(final File dir) {
        try {
            Files.createDirectories(dir.toPath());
            final Path probe = Files.createTempFile(dir.toPath(), ".writable", ".probe");
            Files.delete(probe);
        } catch (final IOException | RuntimeException notWritable) {
            throw new IllegalArgumentException("REPLICA_MISMATCH_DIR is not writable: " + dir.getAbsolutePath(),
                    notWritable);
        }
    }

    /** True when every file was written. Never throws. */
    boolean write(final AlignmentTrust.MismatchEvidence e) {
        final Path at = dir.resolve(Long.toString(e.position()));
        final Path report = at.resolve("report.txt");
        final String now = Instant.ofEpochMilli(wall.time()).toString();
        try {
            io.createDirectories(at);
            if (io.exists(report)) {
                io.writeText(report, "re-latched at " + now + "\n", true);
                System.err.println("[REPLICA] CRITICAL: mismatch dump at " + e.position() + " already present ("
                        + report + "): re-latched, not rewritten");
                return true;
            }
        } catch (final IOException | RuntimeException failed) {
            return failed(e.position(), failed, null, null);
        }
        final StringBuilder text = new StringBuilder(1024);
        text.append("position=").append(e.position()).append('\n')
                .append("leadership_term=").append(e.leadershipTermId()).append('\n')
                .append("replica_genesis=").append(e.replicaGenesis()).append('\n')
                .append("member_genesis=").append(e.memberGenesis() == Long.MIN_VALUE ? "not decoded"
                        : Long.toString(e.memberGenesis())).append('\n')
                .append("replica_length=").append(e.replicaLength()).append('\n')
                .append("member_length=").append(e.memberLength()).append('\n')
                .append("first_difference=").append(e.firstDifference()).append('\n')
                .append("detail=").append(e.detail()).append('\n')
                .append("replica_sha256=").append(sha256(e.replicaImage(), e.replicaLength())).append('\n')
                .append("member_sha256=").append(sha256(e.memberImage(), e.memberLength())).append('\n')
                .append("member_recording_id=").append(e.memberRecordingId()).append('\n')
                .append("newest_member_snapshot_named_by_backup=").append(e.newestNamed()).append('\n')
                .append("section_offsets=not exposed by the engine codec\n")
                .append("latched_at=").append(now).append('\n');
        final boolean imagesOff = maxMb == 0;
        final boolean truncated = !imagesOff && (e.replicaLength() > maxBytes || e.memberLength() > maxBytes);
        text.append("images=").append(imagesOff ? "off (REPLICA_MISMATCH_DUMP_MAX_MB=0)" : "on").append('\n')
                .append("truncated=").append(truncated).append('\n');
        try {
            if (!imagesOff && e.replicaLength() <= maxBytes) {
                io.write(at.resolve("replica.image"), e.replicaImage(), e.replicaLength());
            }
            if (!imagesOff && e.memberLength() <= maxBytes) {
                io.write(at.resolve("member.image"), e.memberImage(), e.memberLength());
            }
        } catch (final IOException | RuntimeException failed) {
            return failed(e.position(), failed, report, text);
        }
        try {
            io.writeText(report, text.toString(), false);
        } catch (final IOException | RuntimeException failed) {
            return failed(e.position(), failed, null, null);
        }
        System.err.println("[REPLICA] CRITICAL: mismatch dump written to " + at + " (replica "
                + e.replicaLength() + " B, member " + e.memberLength() + " B" + (truncated ? ", truncated" : "")
                + ")");
        return true;
    }

    private boolean failed(final long position, final Exception cause, final Path report, final StringBuilder text) {
        incomplete = true;
        System.err.println("[REPLICA] CRITICAL: mismatch dump at " + position + " incomplete: " + cause);
        if (report != null && text != null) {
            try {
                text.append("incomplete=").append(cause).append('\n');
                io.writeText(report, text.toString(), false);
            } catch (final IOException | RuntimeException alsoFailed) {
                System.err.println("[REPLICA] CRITICAL: mismatch report at " + position + " not written: "
                        + alsoFailed);
            }
        }
        return false;
    }

    /** {@code shadow_oracle_dump_incomplete}. */
    boolean incomplete() {
        return incomplete;
    }

    static String sha256(final DirectBuffer buffer, final int length) {
        if (buffer == null) {
            return "none";
        }
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            final byte[] chunk = new byte[64 * 1024];
            for (int at = 0; at < length; at += chunk.length) {
                final int n = Math.min(chunk.length, length - at);
                buffer.getBytes(at, chunk, 0, n);
                md.update(chunk, 0, n);
            }
            final StringBuilder hex = new StringBuilder(64);
            for (final byte b : md.digest()) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (final NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
