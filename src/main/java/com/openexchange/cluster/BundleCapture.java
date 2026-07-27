// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import io.aeron.AeronVersion;
import io.aeron.version.Version;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Ships one snapshot and the log behind it out of a live node, as a self-contained
 * bundle on real disk.
 *
 * <p>This is step 01 of the durable ledger archive. It changes NO purging
 * behaviour: it only starts producing bundles, so the ledger gains durability
 * before anything begins deleting on the strength of it.
 *
 * <p>A bundle is one snapshot plus the log segments from the previous bundle's
 * position up to this snapshot's position. Bundles chain, so any contiguous run
 * replays as one continuous history. The chaining is why {@link StagingArchive}
 * is persistent - see that class for the Aeron constraint that forces it.
 *
 * <pre>
 * &lt;bundleRoot&gt;/2026-07-26T00-05-00Z-pos-1057814752/
 *     manifest.json        positions, build, schema, checksums, AND "recordings"
 *     snapshot/            consensus module + service recordings
 *     log/                 segments (previousPosition .. snapshotPosition)
 *     recording-log.txt    RecordingLog entries at capture time
 * </pre>
 *
 * <p><b>Why the manifest carries recording descriptors.</b> A {@code .rec} file does not state where
 * it starts, where it stops, or how its frames are shaped; Aeron keeps that in the archive catalog,
 * which stays on the box. Worse, the catalog entries for the recordings a bundle carries are
 * DELETED by this class's own staging sweep a few lines after their files are copied out. The
 * descriptors are therefore written into the manifest at the one moment both exist. Without them a
 * bundle passes its checksum, uploads cleanly, and cannot be opened by anything.</p>
 *
 * <p><b>Ordering obligation.</b> Run this BEFORE archive housekeeping purges the
 * log below the snapshot, or the range this bundle needs is already gone. The
 * admin gateway snapshots and reclaims in one operation, and capture sits between
 * those two steps; the ordering is held structurally by the purge watermark,
 * which never passes the last position durable in S3.
 *
 * <p><b>Staging is reclaimed here, not by the caller.</b> A capture leaves two
 * kinds of residue in the staging archive, with two different lifetimes, and only
 * this class knows which is which: the snapshot recordings are scratch and go
 * immediately, the log is the chain anchor and only its prefix is reclaimable,
 * bounded by {@code --watermark=N}. See {@code reclaimStaging}.
 */
public final class BundleCapture {

    /**
     * The one thing this machinery cannot know by itself: which wire schema the
     * engine speaks.
     *
     * <p>It is payload, not decoration. A snapshot serialized by one build may
     * not deserialize in another and log semantics move, so a bundle that cannot
     * name its schema is one a replay must refuse. Each engine supplies its own
     * generated constants through a thin main class in its own repo; everything
     * else here is engine-agnostic and lives in exactly one place.
     */
    public record EngineSchema(int id, int version) {
    }

    /**
     * One recording in the bundle, described well enough to be served again.
     *
     * <p>Aeron keeps this in the archive catalog, which stays on the box; the {@code .rec} files
     * carry none of it. Without these fields a restore cannot say where a recording starts, where it
     * stops, which file holds a given position, or how the frames are shaped — so the bundle is a
     * pile of bytes rather than something an archive can replay.</p>
     *
     * <p>{@code serviceId} is -1 for the log. Everything else is verbatim from the staging catalog,
     * except the log's {@code stopPosition}, which is clamped to the position this bundle was cut at.</p>
     */
    record BundledRecording(String role, int serviceId, StagingArchive.Descriptor descriptor) {

        static BundledRecording log(final StagingArchive.Descriptor d, final long bundlePosition) {
            require(d, "log");
            final StagingArchive.Descriptor clamped = new StagingArchive.Descriptor(
                    d.recordingId(), d.startPosition(), Math.min(d.stopPosition(), bundlePosition),
                    d.initialTermId(), d.segmentFileLength(), d.termBufferLength(), d.mtuLength(),
                    d.sessionId(), d.streamId(), d.strippedChannel(), d.originalChannel(),
                    d.sourceIdentity());
            return new BundledRecording("log", -1, clamped);
        }

        static BundledRecording snapshot(final StagingArchive.Descriptor d, final int serviceId) {
            require(d, "snapshot");
            return new BundledRecording("snapshot", serviceId, d);
        }

        /**
         * A bundle whose recordings cannot be described is not a bundle. Failing here loses one
         * capture; shipping the files without them produces something that looks complete, passes
         * its checksum, and cannot be opened.
         */
        private static void require(final StagingArchive.Descriptor d, final String role) {
            if (d == null) {
                throw new IllegalStateException("[BUNDLE] no catalog entry for the staged " + role
                        + " recording - refusing to write a bundle that cannot be replayed");
            }
        }

        String toJson() {
            final StagingArchive.Descriptor d = descriptor;
            return "    {\n"
                    + "      \"role\": \"" + role + "\",\n"
                    + (serviceId >= 0 ? "      \"serviceId\": " + serviceId + ",\n" : "")
                    + "      \"recordingId\": " + d.recordingId() + ",\n"
                    + "      \"startPosition\": " + d.startPosition() + ",\n"
                    + "      \"stopPosition\": " + d.stopPosition() + ",\n"
                    + "      \"initialTermId\": " + d.initialTermId() + ",\n"
                    + "      \"segmentFileLength\": " + d.segmentFileLength() + ",\n"
                    + "      \"termBufferLength\": " + d.termBufferLength() + ",\n"
                    + "      \"mtuLength\": " + d.mtuLength() + ",\n"
                    + "      \"sessionId\": " + d.sessionId() + ",\n"
                    + "      \"streamId\": " + d.streamId() + ",\n"
                    + "      \"strippedChannel\": \"" + escape(d.strippedChannel()) + "\",\n"
                    + "      \"originalChannel\": \"" + escape(d.originalChannel()) + "\",\n"
                    + "      \"sourceIdentity\": \"" + escape(d.sourceIdentity()) + "\"\n"
                    + "    }";
        }

        /** Aeron channel URIs are not JSON-safe by construction; escape what matters. */
        private static String escape(final String value) {
            if (value == null) {
                return "";
            }
            final StringBuilder sb = new StringBuilder(value.length() + 8);
            for (int i = 0; i < value.length(); i++) {
                final char c = value.charAt(i);
                switch (c) {
                    case '"'  -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default   -> {
                        if (c < 0x20) {
                            sb.append(String.format("\\u%04x", (int) c));
                        } else {
                            sb.append(c);
                        }
                    }
                }
            }
            return sb.toString();
        }
    }

    private static final DateTimeFormatter BUNDLE_STAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss'Z'").withZone(ZoneOffset.UTC);

    /** Outcome of one capture, for the caller's log and the admin gateway's parser. */
    public static final class Result {
        public final boolean captured;
        public final String reason;
        public final File bundleDir;
        public final long snapshotPosition;
        public final long previousPosition;
        public final long logBytes;

        Result(final boolean captured, final String reason, final File bundleDir,
               final long snapshotPosition, final long previousPosition, final long logBytes) {
            this.captured = captured;
            this.reason = reason;
            this.bundleDir = bundleDir;
            this.snapshotPosition = snapshotPosition;
            this.previousPosition = previousPosition;
            this.logBytes = logBytes;
        }

        static Result skipped(final String reason, final long snapshotPosition) {
            return new Result(false, reason, null, snapshotPosition, -1, 0);
        }

        @Override
        public String toString() {
            return captured
                    ? "Result{captured, dir=" + bundleDir.getName()
                        + ", range=" + previousPosition + ".." + snapshotPosition
                        + ", logBytes=" + logBytes + "}"
                    : "Result{skipped: " + reason + ", snapshotPosition=" + snapshotPosition + "}";
        }
    }

    private BundleCapture() {
    }

    /**
     * Capture the newest snapshot on this node, if it is newer than the last one
     * captured.
     *
     * @param clusterDir        the node's cluster directory (holds recording.log)
     * @param srcControlChannel the node archive's UDP control channel
     * @param stagingRoot       persistent staging archive location (real disk)
     * @param bundleRoot        where bundle directories are written
     * @param cluster           cluster name for the manifest
     * @param nodeId            node id for the manifest
     * @param durablePosition   the caller's watermark: the position it has verified
     *                          durable in S3, below which staged log segments are
     *                          reclaimable. -1 means "no external constraint known",
     *                          which reclaims nothing rather than everything.
     */
    public static Result capture(final File clusterDir, final String srcControlChannel,
                                 final File stagingRoot, final File bundleRoot,
                                 final String cluster, final int nodeId,
                                 final EngineSchema schema,
                                 final long durablePosition) throws IOException {

        final SnapshotSelection selection = readLatestSnapshot(clusterDir);
        if (selection == null) {
            return Result.skipped("no valid snapshot in recording.log", -1);
        }

        final StagingState state = StagingState.load(stagingRoot);

        if (selection.logPosition <= state.lastBundledPosition) {
            return Result.skipped("snapshot already captured", selection.logPosition);
        }

        // A position that moved BACKWARDS means the source is not the cluster this
        // staging archive has been tracking - a re-form from genesis, or a reseed
        // from a different node. Chaining across that boundary would produce a
        // bundle whose log does not continue its own manifest's previousPosition,
        // and the break would only be discovered during a restore. Refuse instead.
        if (state.lastBundledPosition >= 0 && selection.logPosition < state.lastBundledPosition) {
            throw new IllegalStateException("[BUNDLE] snapshot position "
                    + selection.logPosition + " is BELOW the last captured position "
                    + state.lastBundledPosition + " - the source cluster was re-formed or "
                    + "reseeded. Archive this staging directory and start a new chain; do not "
                    + "extend across the break.");
        }

        //noinspection ResultOfMethodCallIgnored
        bundleRoot.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        stagingRoot.mkdirs();

        final Instant capturedAt = Instant.now();
        final File bundleDir = new File(bundleRoot,
                BUNDLE_STAMP.format(capturedAt) + "-pos-" + selection.logPosition);
        final File snapshotDir = new File(bundleDir, "snapshot");
        final File logDir = new File(bundleDir, "log");
        //noinspection ResultOfMethodCallIgnored
        snapshotDir.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        logDir.mkdirs();

        final long logBytes;
        final long snapshotBytes;
        final List<Long> stagedSnapshotRecordings = new ArrayList<>();
        final List<BundledRecording> recordings = new ArrayList<>();

        try (StagingArchive staging = StagingArchive.launch(stagingRoot)) {
            // Snapshot recordings are COMPLETE, so they replicate whole. There is one
            // per service plus one for the consensus module; a restore needs all of them.
            for (final RecordingLog.Entry entry : selection.snapshotEntries) {
                final long staged = staging.replicate(srcControlChannel, entry.recordingId,
                        Aeron.NULL_VALUE, AeronArchive.NULL_POSITION);
                stagedSnapshotRecordings.add(staged);
                System.out.println("[BUNDLE] replicated snapshot recording "
                        + entry.recordingId + " (serviceId=" + entry.serviceId + ") -> " + staged);
            }

            // The log is LIVE, so it replicates with an explicit stop position -
            // without one this would follow the recording forever and never return.
            // Extending the existing staged recording is what makes the copy resume
            // exactly where the previous bundle ended.
            final long dstLogRecordingId = staging.replicate(srcControlChannel,
                    selection.logRecordingId, state.dstLogRecordingId, selection.logPosition);

            final long stagedStop = staging.stopPosition(dstLogRecordingId);
            if (stagedStop < selection.logPosition) {
                throw new IllegalStateException("[BUNDLE] staged log stops at " + stagedStop
                        + " but the snapshot is at " + selection.logPosition
                        + " - refusing to write a bundle whose log does not reach its snapshot");
            }
            System.out.println("[BUNDLE] replicated log " + selection.logRecordingId
                    + " -> " + dstLogRecordingId + " up to " + selection.logPosition);

            logBytes = copySegments(staging.archiveDir(), dstLogRecordingId,
                    state.lastBundledPosition, selection.logPosition, logDir);

            long snapshot = 0;
            for (final long staged : stagedSnapshotRecordings) {
                snapshot += copyRecordingFiles(staging.archiveDir(), staged, snapshotDir);
            }
            snapshotBytes = snapshot;

            state.dstLogRecordingId = dstLogRecordingId;

            // Describe the recordings BEFORE the sweep below deletes their catalog entries. This is
            // the only moment both exist: the files are in the bundle and their descriptors are still
            // valid. One line later purgeRecording marks them DELETED, and nothing outside this
            // archive can ever reconstruct what shape those bytes are in.
            //
            // The log is clamped to the position this bundle was cut at. Staging keeps recording into
            // it, so its live stopPosition runs ahead of what the bundle actually carries, and a
            // restore told otherwise would ask for bytes that are not there.
            recordings.add(BundledRecording.log(
                    staging.describe(dstLogRecordingId), selection.logPosition));
            for (int i = 0; i < selection.snapshotEntries.size(); i++) {
                final RecordingLog.Entry entry = selection.snapshotEntries.get(i);
                recordings.add(BundledRecording.snapshot(
                        staging.describe(stagedSnapshotRecordings.get(i)), entry.serviceId));
            }

            reclaimStaging(staging, dstLogRecordingId, durablePosition, selection.logPosition);
        }

        Files.copy(clusterDir.toPath().resolve("recording.log"),
                bundleDir.toPath().resolve("recording-log.txt"),
                StandardCopyOption.REPLACE_EXISTING);

        writeManifest(bundleDir, cluster, nodeId, selection, state.lastBundledPosition,
                capturedAt, logBytes, snapshotBytes, snapshotDir, logDir, schema, recordings);

        final long previousPosition = state.lastBundledPosition;
        state.lastBundledPosition = selection.logPosition;
        state.save(stagingRoot);

        return new Result(true, "captured", bundleDir, selection.logPosition,
                previousPosition, logBytes);
    }

    /**
     * Give back the staging space this capture and its predecessors no longer need.
     *
     * <p>Two different lifetimes, so two different rules.
     *
     * <p>The snapshot recordings are SCRATCH. They exist only to be copied into the
     * bundle directory a few lines above, and nothing ever reads the staged
     * original again. They are therefore deleted outright, with no reference to the
     * durable watermark: the copy that matters is already on disk, and if this
     * bundle never reaches S3 the next capture simply replicates them again. Left
     * alone they were the larger leak of the two, because each capture adds a fresh
     * set and {@link StagingArchive#purgeBelow} cannot remove a catalog entry.
     *
     * <p>It sweeps everything that is not the log rather than deleting the ids this
     * run happens to hold, which is what makes it self-healing. A capture killed
     * between replicating a snapshot and copying it out leaves recordings no later
     * run has a reference to; so does any version of this code that shipped without
     * reclamation. Both are the same garbage, and a sweep collects them on the next
     * successful capture instead of requiring somebody to go in by hand.
     *
     * <p>The log recording is the CHAIN ANCHOR and must survive, so only its prefix
     * is reclaimable, bounded by what the caller has verified durable in S3. The
     * clamp to {@code capturedPosition} is defensive rather than expected: a
     * watermark above the range we just copied out would mean the caller believes
     * something is durable that this node has not even staged yet, and the cheap
     * response to a watermark that cannot be true is to not act on it.
     *
     * <p>Never throws. A capture that produced a good bundle and then failed to
     * tidy up has still done the job that protects the ledger; turning that into a
     * failed capture would stop bundles entirely, which is the outcome this whole
     * mechanism exists to prevent. Failures are printed instead, and the space is
     * reclaimed on the next round.
     */
    private static void reclaimStaging(final StagingArchive staging,
                                       final long logRecordingId,
                                       final long durablePosition,
                                       final long capturedPosition) {
        try {
            final List<Long> deleted = staging.purgeAllExcept(logRecordingId);
            System.out.println("[BUNDLE] staged snapshot recordings deleted: "
                    + deleted.size() + " " + deleted);
        } catch (final Exception e) {
            System.out.println("[BUNDLE] could not sweep staged snapshot recordings, "
                    + "they will occupy staging until the next successful capture: " + e);
        }

        if (durablePosition < 0) {
            System.out.println("[BUNDLE] staged log kept whole: caller reported no durable "
                    + "position, so nothing below it is known to be safe to drop");
            return;
        }

        final long bound = Math.min(durablePosition, capturedPosition);
        if (bound < durablePosition) {
            System.out.println("[BUNDLE] durable position " + durablePosition
                    + " is ABOVE the position just captured (" + capturedPosition
                    + "); reclaiming only to the captured position. A watermark this "
                    + "node cannot account for is a caller bug, not a licence to delete.");
        }

        try {
            final long purged = staging.purgeBelow(logRecordingId, bound);
            System.out.println("[BUNDLE] staged log reclaimed below " + bound
                    + ": " + purged + " segment(s)");
        } catch (final Exception e) {
            System.out.println("[BUNDLE] could not reclaim staged log below "
                    + bound + ": " + e);
        }
    }

    // ---- recording.log ----

    private static final class SnapshotSelection {
        final long logPosition;
        final long logRecordingId;
        final List<RecordingLog.Entry> snapshotEntries;

        SnapshotSelection(final long logPosition, final long logRecordingId,
                          final List<RecordingLog.Entry> snapshotEntries) {
            this.logPosition = logPosition;
            this.logRecordingId = logRecordingId;
            this.snapshotEntries = snapshotEntries;
        }
    }

    /**
     * The newest VALID snapshot group: every entry sharing the highest snapshot
     * position (consensus module plus one per service). An invalidated entry is one
     * Aeron has logically removed and is not a restore point.
     */
    private static SnapshotSelection readLatestSnapshot(final File clusterDir) {
        final List<RecordingLog.Entry> snapshots = new ArrayList<>();
        final long logRecordingId;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid) {
                    snapshots.add(entry);
                }
            }
            logRecordingId = recordingLog.findLastTermRecordingId();
        }

        if (snapshots.isEmpty() || logRecordingId == Aeron.NULL_VALUE) {
            return null;
        }

        final long latest = snapshots.stream()
                .map(e -> e.logPosition)
                .max(Comparator.naturalOrder())
                .orElseThrow();

        final List<RecordingLog.Entry> group = snapshots.stream()
                .filter(e -> e.logPosition == latest)
                .toList();

        return new SnapshotSelection(latest, logRecordingId, group);
    }

    // ---- staging state ----

    /**
     * Where the chain stands. Kept beside the staging archive because it describes
     * that archive's contents; losing one without the other is what would silently
     * restart the chain.
     */
    static final class StagingState {
        long dstLogRecordingId = Aeron.NULL_VALUE;
        long lastBundledPosition = -1;

        static StagingState load(final File stagingRoot) throws IOException {
            final Path file = stagingRoot.toPath().resolve("bundle-state.properties");
            final StagingState state = new StagingState();
            if (!Files.exists(file)) {
                return state;
            }
            final Properties props = new Properties();
            try (InputStream in = Files.newInputStream(file)) {
                props.load(in);
            }
            state.dstLogRecordingId = Long.parseLong(
                    props.getProperty("dstLogRecordingId", String.valueOf(Aeron.NULL_VALUE)));
            state.lastBundledPosition = Long.parseLong(
                    props.getProperty("lastBundledPosition", "-1"));
            return state;
        }

        void save(final File stagingRoot) throws IOException {
            final Properties props = new Properties();
            props.setProperty("dstLogRecordingId", String.valueOf(dstLogRecordingId));
            props.setProperty("lastBundledPosition", String.valueOf(lastBundledPosition));

            // Write-then-rename: a torn state file would either restart the chain or
            // point at a recording that does not exist.
            final Path tmp = stagingRoot.toPath().resolve("bundle-state.properties.tmp");
            try (var out = Files.newOutputStream(tmp)) {
                props.store(out, "Assets Engine bundle chain state");
            }
            Files.move(tmp, stagingRoot.toPath().resolve("bundle-state.properties"),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    // ---- file extraction ----

    /**
     * Copy the staged log segments covering {@code from..to}.
     *
     * <p>Whole segments, not byte ranges: a segment is the unit Aeron reads back, and
     * a partial one is not replayable. The first bundle of a chain has no lower bound
     * and takes everything the staging archive holds.
     *
     * @return bytes actually written to the bundle (compressed)
     */
    static long copySegments(final File archiveDir, final long recordingId,
                                     final long from, final long to, final File targetDir)
            throws IOException {
        final String prefix = recordingId + "-";
        final File[] files = archiveDir.listFiles((dir, name) ->
                name.startsWith(prefix) && name.endsWith(".rec"));
        if (files == null) {
            return 0;
        }

        long bytes = 0;
        for (final File file : files) {
            final long segmentBase = segmentBasePosition(file.getName(), prefix);
            if (segmentBase < 0) {
                continue;
            }
            // A segment belongs to this bundle if any of it lies above the previous
            // bundle's position and its start is below this snapshot's position.
            final long segmentEnd = segmentBase + file.length();
            if (segmentEnd <= from || segmentBase >= to) {
                continue;
            }
            bytes += compressInto(file, targetDir);
        }
        return bytes;
    }

    /** Every {@code .rec} file of a recording; snapshots are copied whole. */
    private static long copyRecordingFiles(final File archiveDir, final long recordingId,
                                           final File targetDir) throws IOException {
        final String prefix = recordingId + "-";
        final File[] files = archiveDir.listFiles((dir, name) ->
                name.startsWith(prefix) && name.endsWith(".rec"));
        if (files == null) {
            return 0;
        }
        long bytes = 0;
        for (final File file : files) {
            bytes += compressInto(file, targetDir);
        }
        return bytes;
    }

    /**
     * Gzip a recording file into the bundle.
     *
     * <p>Aeron pre-allocates segment files and leaves them full of holes, so a plain
     * copy is a trap that has already cost this project once: the source occupies
     * 36 MB of blocks and the copy lands as 193 MB of real bytes, because copying
     * does not preserve sparseness. Uploading that inflation would multiply the S3
     * bill and the transfer time by the same factor for no data at all.
     *
     * <p>Compressing solves it without needing hole-aware copying: the pre-allocated
     * zeroes collapse to almost nothing, and the genuine log data shrinks too. It
     * also makes bundle size a function of real content, which is what the cost
     * model in the design assumes.
     *
     * @return bytes written
     */
    static long compressInto(final File source, final File targetDir) throws IOException {
        final Path target = targetDir.toPath().resolve(source.getName() + ".gz");
        try (InputStream in = Files.newInputStream(source.toPath());
             var out = new java.util.zip.GZIPOutputStream(
                     Files.newOutputStream(target), 64 * 1024)) {
            in.transferTo(out);
        }
        return Files.size(target);
    }

    /** Aeron names segments {@code <recordingId>-<basePosition>.rec}. */
    private static long segmentBasePosition(final String fileName, final String prefix) {
        final int dot = fileName.lastIndexOf(".rec");
        if (dot < 0) {
            return -1;
        }
        try {
            return Long.parseLong(fileName.substring(prefix.length(), dot));
        } catch (final NumberFormatException e) {
            return -1;
        }
    }

    // ---- manifest ----

    /**
     * The manifest is what makes a bundle openable in six months.
     *
     * <p>{@code buildSha} and {@code schemaId} are payload, not metadata: a snapshot
     * serialized by one build may not deserialize in another, and log semantics move.
     * A replay tool must check them and refuse a mismatched tree.
     */
    private static void writeManifest(final File bundleDir, final String cluster, final int nodeId,
                                      final SnapshotSelection selection, final long previousPosition,
                                      final Instant capturedAt, final long logBytes,
                                      final long snapshotBytes,
                                      final File snapshotDir, final File logDir,
                                      final EngineSchema schema,
                                      final List<BundledRecording> recordings) throws IOException {
        final StringBuilder recordingsJson = new StringBuilder("[\n");
        for (int i = 0; i < recordings.size(); i++) {
            recordingsJson.append(recordings.get(i).toJson());
            recordingsJson.append(i < recordings.size() - 1 ? ",\n" : "\n");
        }
        recordingsJson.append("  ]");

        final String json = "{\n"
                + "  \"cluster\": \"" + cluster + "\",\n"
                + "  \"nodeId\": " + nodeId + ",\n"
                + "  \"snapshotPosition\": " + selection.logPosition + ",\n"
                + "  \"previousPosition\": " + previousPosition + ",\n"
                + "  \"capturedAt\": \"" + capturedAt + "\",\n"
                + "  \"buildSha\": \"" + buildSha() + "\",\n"
                + "  \"schemaId\": " + schema.id() + ",\n"
                + "  \"schemaVersion\": " + schema.version() + ",\n"
                + "  \"aeronVersion\": \"" + aeronVersion() + "\",\n"
                + "  \"logBytes\": " + logBytes + ",\n"
                + "  \"snapshotBytes\": " + snapshotBytes + ",\n"
                + "  \"compression\": \"gzip\",\n"
                + "  \"recordings\": " + recordingsJson + ",\n"
                + "  \"sha256\": {\n"
                + "    \"snapshot\": \"" + digestDirectory(snapshotDir) + "\",\n"
                + "    \"log\": \"" + digestDirectory(logDir) + "\"\n"
                + "  }\n"
                + "}\n";

        Files.writeString(bundleDir.toPath().resolve("manifest.json"), json,
                StandardCharsets.UTF_8);
    }

    /**
     * The Aeron the engine is ACTUALLY running, read at runtime.
     *
     * <p>Not {@code AeronVersion.VERSION}. That field is a compile-time constant,
     * so javac inlines it into this class at build time and every bundle from
     * every engine would report whatever version THIS library was compiled
     * against. It is a defect the shared module created: while the machinery was
     * duplicated, each copy compiled against its own engine's Aeron and happened
     * to be right.
     *
     * <p>Caught by reading a real manifest: the matching engine runs 1.52.2 and
     * its first bundle claimed 1.51.0. Provenance that is confidently wrong is
     * worse than provenance that is missing — a replay would boot the wrong
     * Aeron and debug a different system.
     *
     * <p>The instance methods are virtual calls, resolved against whatever Aeron
     * is on the classpath at run time, which is the whole point.
     */
    static String aeronVersion() {
        final Version version = new AeronVersion();
        return version.majorVersion() + "." + version.minorVersion() + "." + version.patchVersion();
    }

    /**
     * The build that produced this bundle, read from the running jar's manifest.
     *
     * <p>Returns the literal {@code "unknown"} when the jar was not stamped, rather
     * than inventing a value. A bundle that cannot name its build is one a replay
     * must refuse, and saying so is the point.
     */
    static String buildSha() {
        try {
            final Class<?> self = BundleCapture.class;
            final String path = self.getResource(self.getSimpleName() + ".class").toString();
            if (!path.startsWith("jar:")) {
                return "unknown";
            }
            final String manifestPath = path.substring(0, path.lastIndexOf('!') + 1)
                    + "/META-INF/MANIFEST.MF";
            try (InputStream in = new java.net.URL(manifestPath).openStream()) {
                final Attributes attrs = new Manifest(in).getMainAttributes();
                final String sha = attrs.getValue("Build-Sha");
                return sha == null || sha.isBlank() ? "unknown" : sha;
            }
        } catch (final Exception e) {
            return "unknown";
        }
    }

    /**
     * A single digest over every file in a directory, name-ordered so it is stable
     * across filesystems that do not agree on listing order.
     */
    private static String digestDirectory(final File dir) throws IOException {
        final File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return "";
        }
        java.util.Arrays.sort(files, Comparator.comparing(File::getName));

        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (final File file : files) {
                digest.update(file.getName().getBytes(StandardCharsets.UTF_8));
                try (InputStream in = Files.newInputStream(file.toPath());
                     DigestInputStream digestIn = new DigestInputStream(in, digest)) {
                    final byte[] buffer = new byte[64 * 1024];
                    while (digestIn.read(buffer) != -1) {
                        // digest consumes the stream
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (final java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    // ---- CLI ----

    /** Named for the same reason as housekeeping's: a stray positional cannot become a position. */
    private static final String WATERMARK_FLAG = "--watermark=";

    /**
     * Extract {@code --watermark=N}, or -1 when absent or unusable.
     *
     * <p>The default is the OPPOSITE of {@link ArchiveHousekeeping#watermarkFrom},
     * deliberately. Housekeeping defaults to {@link Long#MAX_VALUE} because it has
     * a second bound underneath it, the latest valid snapshot, so an absent
     * watermark still cannot delete anything recovery needs. The staged log has no
     * such floor: it is a private copy whose only protection is this number. An
     * absent watermark there has to mean "reclaim nothing" or the first caller that
     * forgets the flag truncates the chain anchor.
     *
     * <p>Scans past the six positional arguments only, so a future positional can
     * never be read as a watermark.
     */
    static long watermarkFrom(final String[] args) {
        for (int i = 6; i < args.length; i++) {
            if (!args[i].startsWith(WATERMARK_FLAG)) {
                continue;
            }
            final String value = args[i].substring(WATERMARK_FLAG.length());
            try {
                final long parsed = Long.parseLong(value);
                if (parsed < 0) {
                    return -1;
                }
                return parsed;
            } catch (final NumberFormatException e) {
                System.err.println("[BUNDLE] FAILED: unparseable "
                        + WATERMARK_FLAG + "value: " + value);
                System.exit(2);
            }
        }
        return -1;
    }

    /**
     * Invoked per node by the admin gateway, between the snapshot and the reclaim.
     *
     * <p>Not a {@code main} itself: the schema is engine-specific, so each engine
     * keeps a thin entry point in its own repo that supplies its generated
     * constants and calls this. That is the whole seam between shared machinery
     * and engine identity.
     *
     * <p>Usage from that entry point: {@code run(args, schema)} with argv
     * {@code <clusterDir> <srcControlChannel> <stagingRoot> <bundleRoot>
     * <cluster> <nodeId> [--watermark=N]}
     */
    public static void run(final String[] args, final EngineSchema schema) {
        if (args.length < 6) {
            System.err.println("Usage: <clusterDir> <srcControlChannel> "
                    + "<stagingRoot> <bundleRoot> <cluster> <nodeId> [--watermark=N]");
            System.exit(2);
        }

        try {
            final Result result = capture(new File(args[0]), args[1], new File(args[2]),
                    new File(args[3]), args[4], Integer.parseInt(args[5]), schema,
                    watermarkFrom(args));
            System.out.println("[BUNDLE] " + result);
            if (!result.captured) {
                System.exit(0);
            }
        } catch (final Exception e) {
            System.err.println("[BUNDLE] FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
