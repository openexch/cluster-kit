// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.shadow.ShadowReplay;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A real archive with 64 KiB segments, two journal profiles shaped like the core's (order: one 256-byte frame per
 * record with an epoch; money: three record lengths, no epoch, the log position at the end of the block), writers
 * that record journal records, and a member's cluster directory for the reader files. The member is engine core,
 * cluster 0, genesis {@link #GENESIS}.
 */
final class JournalRig implements AutoCloseable {

    static final int SEG = 64 * 1024;
    static final String CHANNEL = "aeron:ipc?term-length=64k";
    static final long GENESIS = 17;
    static final long TIMEOUT_NS = 20_000_000_000L;
    static final JournalProfile ORDER = new JournalProfile("order", 4102, "aeron:ipc", 5, 8, 16, Map.of(1, 24), 1, 256);
    static final JournalProfile MONEY = new JournalProfile("money", 4101, "aeron:ipc", 3, 8, JournalProfile.NO_EPOCH,
        Map.of(1, 36, 2, 52, 3, 52, 4, 101), 2, 160);
    /** Order records per 64 KiB segment. */
    static final int ORDER_PER_SEGMENT = SEG / 256;

    final File dir;
    final File clusterDir;
    private final ArchivingMediaDriver driver;
    final Aeron aeron;
    final AeronArchive archive;
    final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    private final List<AutoCloseable> resources = new ArrayList<>();

    JournalRig() {
        try {
            dir = Files.createTempDirectory("kit-journal-").toFile();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        clusterDir = new File(dir, "cluster");
        clusterDir.mkdirs();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        driver = ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true),
            new Archive.Context().aeronDirectoryName(aeronDir).archiveDir(new File(dir, "archive"))
                .controlChannelEnabled(false)
                .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                .replicationChannel("aeron:udp?endpoint=localhost:0").recordingEventsEnabled(false)
                .segmentFileLength(SEG).threadingMode(ArchiveThreadingMode.SHARED).deleteArchiveOnStart(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        archive = AeronArchive.connect(new AeronArchive.Context().aeron(aeron).controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc"));
    }

    /** A client context for one more archive session on this rig's driver (the agent connects its own). */
    AeronArchive.Context archiveContext() {
        return new AeronArchive.Context().aeron(aeron).controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc");
    }

    @Override
    public void close() {
        for (int i = resources.size() - 1; i >= 0; i--) {
            CloseHelper.quietClose(resources.get(i));
        }
        CloseHelper.quietCloseAll(archive, aeron, driver);
        IoUtil.delete(dir, true);
    }

    // ------------------------------------------------------------------ writing

    /** One record's bytes for {@code profile}: an order record, or one of three money record shapes by sequence. */
    static int encode(final JournalProfile profile, final UnsafeBuffer buffer, final long seq, final long epoch,
                      final long logPosition, final int schemaId) {
        final int template;
        final int block;
        final int version;
        if (profile.hasEpoch()) {
            template = 1;
            block = 208;
            version = 1;
        } else {
            template = seq % 3 == 0 ? 4 : seq % 3 == 1 ? 2 : 1;
            block = template == 4 ? 101 : template == 2 ? 52 : 36;
            version = 2;
        }
        buffer.setMemory(0, 8 + block, (byte) 0);
        buffer.putShort(0, (short) block, ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(2, (short) template, ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(4, (short) schemaId, ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(6, (short) version, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(8, seq, ByteOrder.LITTLE_ENDIAN);
        if (profile.hasEpoch()) {
            buffer.putLong(16, epoch, ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(24, logPosition, ByteOrder.LITTLE_ENDIAN);
        } else {
            buffer.putLong(8 + block - 8, logPosition, ByteOrder.LITTLE_ENDIAN);
        }
        return 8 + block;
    }

    /** The log position this rig gives the record {@code seq}: rising with the sequence. */
    static long logPositionOf(final long seq) {
        return 1_000 + seq * 64;
    }

    /** A recorded publication of one journal: one process run's recording. */
    final class Writer {
        final JournalProfile profile;
        final long recordingId;
        private final ExclusivePublication publication;
        private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);
        private boolean stopped;

        Writer(final JournalProfile profile) {
            this.profile = profile;
            publication = archive.addRecordedExclusivePublication(CHANNEL, profile.streamId());
            final CountersReader counters = aeron.countersReader();
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            int counterId;
            while ((counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
                archive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
                await(deadline, "the recording counter");
            }
            while (!publication.isConnected()) {
                await(deadline, "the recorded publication to connect");
            }
            recordingId = RecordingPos.getRecordingId(counters, counterId);
            resources.add(() -> {
                if (!stopped) {
                    CloseHelper.quietClose(publication);
                }
            });
        }

        /** Records {@code from..to} of this genesis. */
        Writer append(final long from, final long to) {
            return append(from, to, GENESIS);
        }

        Writer append(final long from, final long to, final long epoch) {
            for (long seq = from; seq <= to; seq++) {
                one(seq, epoch, logPositionOf(seq), profile.schemaId());
            }
            return sync();
        }

        /** One record with every field given (a wrong schema id, a sequence that does not rise). */
        Writer one(final long seq, final long epoch, final long logPosition, final int schemaId) {
            final int length = encode(profile, buffer, seq, epoch, logPosition, schemaId);
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (publication.offer(buffer, 0, length) < 0) {
                await(deadline, "an offer");
            }
            return this;
        }

        /** Waits until the archive has recorded everything offered. */
        Writer sync() {
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (archive.getRecordingPosition(recordingId) < publication.position()) {
                await(deadline, "recording " + recordingId + " to reach " + publication.position());
            }
            return this;
        }

        /** The recording's end: the publication's position, or the stop position once stopped. */
        long position() {
            return stopped ? archive.getStopPosition(recordingId) : publication.position();
        }

        /** Stops the recording (the process run ended). */
        Writer stop() {
            sync();
            archive.stopRecording(publication);
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (archive.getStopPosition(recordingId) == AeronArchive.NULL_POSITION) {
                await(deadline, "the stop position of recording " + recordingId);
            }
            publication.close();
            stopped = true;
            return this;
        }
    }

    Writer writer(final JournalProfile profile) {
        return new Writer(profile);
    }

    static void green(final String what) {
        System.out.println("GREEN " + what);
    }

    static void await(final long deadlineNs, final String what) {
        if (System.nanoTime() - deadlineNs > 0) {
            throw new AssertionError("timed out waiting for " + what);
        }
        Thread.yield();
    }

    // ------------------------------------------------------------------ the member

    JournalRetention.AeronPort port(final JournalProfile profile) {
        return new JournalRetention.AeronPort(aeron, archive, SystemNanoClock.INSTANCE, profile);
    }

    JournalRetention retention(final JournalProfile profile, final JournalRetention.Mode mode) {
        return retention(profile, mode, Set.of(), JournalRetention.Settings.DEFAULT_ALARM_BYTES,
            JournalRetention.Settings.DEFAULT_STALE_MS);
    }

    JournalRetention retention(final JournalProfile profile, final JournalRetention.Mode mode,
                               final Set<String> required, final long alarmBytes, final long staleMs) {
        final PrintStream out = new PrintStream(outBytes, true);
        final PrintStream err = new PrintStream(errBytes, true);
        return new JournalRetention(profile, clusterDir, "core", 0, mode, required, alarmBytes, staleMs,
            SystemNanoClock.INSTANCE, line -> {
                out.println(line);
                System.out.println(line);
            }, line -> {
                err.println(line);
                System.out.println(line);
            });
    }

    /** One pass in mode {@code readers} with no off-box bound. */
    JournalRetention.Report pass(final JournalProfile profile) {
        return pass(retention(profile, JournalRetention.Mode.READERS), JournalRetention.OffBox.NONE);
    }

    JournalRetention.Report pass(final JournalRetention retention, final JournalRetention.OffBox offBox) {
        return retention.pass(port(retention.profile()), GENESIS, offBox, System.currentTimeMillis(),
            System.nanoTime() + TIMEOUT_NS);
    }

    String out() {
        return outBytes.toString();
    }

    String err() {
        return errBytes.toString();
    }

    /** Registers or advances a reader through the tool; fails the test when the tool refuses. */
    void set(final JournalProfile profile, final String reader, final long applied) {
        final ByteArrayOutputStream err = new ByteArrayOutputStream();
        final int exit = JournalReaderTool.run(new String[] {"set", "--cluster-dir", clusterDir.getAbsolutePath(),
            "--engine", "core", "--cluster-id", "0", "--genesis-id", String.valueOf(GENESIS), "--journal",
            profile.journal(), "--reader", reader, "--applied-journal-seq", String.valueOf(applied), "--source",
            "test"}, new PrintStream(new ByteArrayOutputStream()), new PrintStream(err), System::currentTimeMillis);
        if (exit != 0) {
            throw new AssertionError("JournalReaderTool set exit " + exit + ": " + err);
        }
    }

    // ------------------------------------------------------------------ reading back

    /** The archive's view of the journal's recordings and every record each one still holds. */
    JournalAudit.Snapshot snapshot(final JournalProfile profile) {
        final JournalAudit.Snapshot snapshot = new JournalAudit.Snapshot();
        final ShadowReplay replay = new ShadowReplay(aeron, archive, SystemNanoClock.INSTANCE);
        for (final JournalRetention.Recording r : port(profile).list()) {
            final List<JournalAudit.Rec> records = new ArrayList<>();
            if (r.end() > r.start()) {
                replay.readAll(r.id(), r.start(), r.end(), (buffer, offset, length, header) -> {
                    final int aligned = (header.frameLength() + 31) & -32;
                    final int block = buffer.getShort(offset, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
                    final long seq = buffer.getLong(offset + 8, ByteOrder.LITTLE_ENDIAN);
                    final long epoch = profile.hasEpoch() ? buffer.getLong(offset + 16, ByteOrder.LITTLE_ENDIAN) : 0;
                    final long logPosition = profile.hasEpoch() ? buffer.getLong(offset + 24, ByteOrder.LITTLE_ENDIAN)
                        : buffer.getLong(offset + block, ByteOrder.LITTLE_ENDIAN);
                    records.add(new JournalAudit.Rec(header.position() - aligned, seq, epoch, logPosition));
                }, System.nanoTime() + TIMEOUT_NS);
            }
            snapshot.recordings.put(r.id(), new JournalAudit.Recording(r.id(), r.start(), r.end(), r.active(),
                r.segmentLength(), records));
        }
        return snapshot;
    }

    long start(final long recordingId) {
        return archive.getStartPosition(recordingId);
    }
}
