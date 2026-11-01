// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * An in-process {@link ArchivingMediaDriver} in a temp directory, for recording synthetic cluster logs and replaying
 * them as real {@link Image}s: IPC {@link ExclusivePublication}s with the term length, MTU and initial position as
 * parameters, recorded locally; replays through {@code AeronArchive.startReplay}, so replayed positions equal recorded
 * ones (A0:ACD:893-897). The subscriber error handler of the fixture's client is a parameter: Aeron's default (logs
 * and swallows a fragment handler exception) or, for example, {@code RethrowingErrorHandler} (what the service
 * container and ClusterBackup enforce). Test-only: it owns an archive by design.
 *
 * <p>RCS S3 W4: with a control port (constructor {@link #RecordedLogFixture(ErrorHandler, int)}) the archive also
 * listens on {@code aeron:udp?endpoint=localhost:<port>}, so it can be the SOURCE of a real replication
 * ({@link #controlChannel()}); {@link #record(String, int)} records a publication on a caller's channel and stream (the
 * shadow tests' raw path); {@link #replaySessionCount()} reads the archive's replay-session counter and
 * {@link #publicationLogFiles()} lists the driver's publication log buffers (the /dev/shm budget test).</p>
 */
public final class RecordedLogFixture implements AutoCloseable {

    private static final long AWAIT_NS = 10_000_000_000L;
    private static final int FIRST_STREAM_ID = 1001;
    private static final int INITIAL_TERM_ID = 7;

    private final File dir;
    private final ArchivingMediaDriver driver;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final List<AutoCloseable> resources = new ArrayList<>();
    private final String aeronDirectoryName;
    private final String controlChannel;
    private int nextStreamId = FIRST_STREAM_ID;

    /** Aeron's default subscriber error handler. */
    public RecordedLogFixture() {
        this(null);
    }

    /** @param subscriberErrorHandler the fixture client's subscriber error handler; null keeps Aeron's default */
    public RecordedLogFixture(final ErrorHandler subscriberErrorHandler) {
        this(subscriberErrorHandler, 0);
    }

    /**
     * @param subscriberErrorHandler the fixture client's subscriber error handler; null keeps Aeron's default
     * @param controlPort            0: no UDP control channel (IPC only); else the archive's UDP control channel
     *                               listens on {@code localhost:<controlPort>} (a replication source)
     */
    public RecordedLogFixture(final ErrorHandler subscriberErrorHandler, final int controlPort) {
        try {
            dir = Files.createTempDirectory("kit-recorded-log-").toFile();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        this.aeronDirectoryName = aeronDir;
        this.controlChannel = controlPort > 0 ? "aeron:udp?endpoint=localhost:" + controlPort : null;
        ArchivingMediaDriver launched = null;
        Aeron client = null;
        AeronArchive archiveClient = null;
        try {
            final Archive.Context archiveContext = new Archive.Context();
            if (controlChannel != null) {
                archiveContext.controlChannelEnabled(true).controlChannel(controlChannel);
            } else {
                archiveContext.controlChannelEnabled(false);
            }
            launched = ArchivingMediaDriver.launch(
                    new MediaDriver.Context()
                            .aeronDirectoryName(aeronDir)
                            .threadingMode(ThreadingMode.SHARED)
                            .dirDeleteOnStart(true)
                            .dirDeleteOnShutdown(true),
                    archiveContext
                            .aeronDirectoryName(aeronDir)
                            .archiveDir(new File(dir, "archive"))
                            .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                            .replicationChannel("aeron:udp?endpoint=localhost:0")
                            .recordingEventsEnabled(false)
                            .threadingMode(ArchiveThreadingMode.SHARED)
                            .deleteArchiveOnStart(true));
            final Aeron.Context clientContext = new Aeron.Context().aeronDirectoryName(aeronDir);
            if (subscriberErrorHandler != null) {
                clientContext.subscriberErrorHandler(subscriberErrorHandler);
            }
            client = Aeron.connect(clientContext);
            archiveClient = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(client)
                    .controlRequestChannel("aeron:ipc")
                    .controlResponseChannel("aeron:ipc"));
        } catch (final RuntimeException | Error e) {
            CloseHelper.quietCloseAll(archiveClient, client, launched);
            IoUtil.delete(dir, true);
            throw e;
        }
        this.driver = launched;
        this.aeron = client;
        this.archive = archiveClient;
    }

    public Aeron aeron() {
        return aeron;
    }

    public AeronArchive archive() {
        return archive;
    }

    /** The archive's UDP control channel, or null when constructed without a control port. */
    public String controlChannel() {
        return controlChannel;
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    /** A new Aeron client on this fixture's driver (the caller closes it). */
    public Aeron newClient() {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
    }

    /** A new archive client over IPC on {@code client} (the caller closes it). */
    public AeronArchive newArchiveClient(final Aeron client) {
        return AeronArchive.connect(new AeronArchive.Context()
                .aeron(client)
                .controlRequestChannel("aeron:ipc")
                .controlResponseChannel("aeron:ipc"));
    }

    /** The archive's replay-session counter (type 112), or -1 when the archive exposes none. */
    public long replaySessionCount() {
        final CountersReader counters = aeron.countersReader();
        final long[] value = {-1};
        counters.forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == io.aeron.AeronCounters.ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID) {
                value[0] = counters.getCounterValue(counterId);
            }
        });
        return value[0];
    }

    /** {@code <aeronDir>/publications/*.logbuffer}: every publication log buffer the driver has on disk now. */
    public List<Path> publicationLogFiles() {
        final Path publications = new File(aeronDirectoryName, "publications").toPath();
        if (!Files.isDirectory(publications)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(publications)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".logbuffer")).sorted().toList();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * A recorded exclusive publication on the caller's channel and stream (the raw path of the shadow tests), awaited
     * until the archive records it and it is connected.
     */
    public RawRecording record(final String channel, final int streamId) {
        final ExclusivePublication publication = archive.addRecordedExclusivePublication(channel, streamId);
        resources.add(publication);
        final CountersReader counters = aeron.countersReader();
        final long deadline = System.nanoTime() + AWAIT_NS;
        int counterId;
        while ((counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
                archive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
            await(deadline, "recording counter for session " + publication.sessionId());
        }
        while (!publication.isConnected()) {
            await(deadline, "recorded publication to connect");
        }
        return new RawRecording(publication, RecordingPos.getRecordingId(counters, counterId), counterId);
    }

    /** One raw recorded publication: offer what you like, then {@link #stopRecording}. */
    public final class RawRecording {
        private final ExclusivePublication publication;
        private final long recordingId;
        private final int counterId;

        private RawRecording(final ExclusivePublication publication, final long recordingId, final int counterId) {
            this.publication = publication;
            this.recordingId = recordingId;
            this.counterId = counterId;
        }

        public ExclusivePublication publication() {
            return publication;
        }

        public long recordingId() {
            return recordingId;
        }

        /** Offers one message, retrying back-pressure for up to 10 s; returns the new position. */
        public long offer(final org.agrona.DirectBuffer buffer, final int offset, final int length) {
            final long deadline = System.nanoTime() + AWAIT_NS;
            long result;
            while ((result = publication.offer(buffer, offset, length)) < 0) {
                if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.NOT_CONNECTED
                        || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                    throw new IllegalStateException("offer failed: " + result);
                }
                await(deadline, "offer");
            }
            return result;
        }

        /** One offer attempt, no retry: {@code ADMIN_ACTION} means padding was written and the term rotated. */
        public long offerOnce(final org.agrona.DirectBuffer buffer, final int offset, final int length) {
            return publication.offer(buffer, offset, length);
        }

        public int counterId() {
            return counterId;
        }

        /** Waits until everything offered is recorded, stops the recording, returns its stop position. */
        public long stopRecording() {
            final long target = publication.position();
            final long deadline = System.nanoTime() + AWAIT_NS;
            while (archive.getRecordingPosition(recordingId) < target) {
                await(deadline, "recording " + recordingId + " to reach " + target);
            }
            archive.stopRecording(publication);
            long stop;
            while ((stop = archive.getStopPosition(recordingId)) == AeronArchive.NULL_POSITION) {
                await(deadline, "stop position of recording " + recordingId);
            }
            publication.close();
            return stop;
        }
    }

    /**
     * A new recorded IPC publication.
     *
     * @param initialPosition frame aligned; the recording (and every replay of it) starts there
     */
    public Recording record(final int termLength, final int mtu, final long initialPosition) {
        final int streamId = nextStreamId++;
        final ChannelUriStringBuilder uri = new ChannelUriStringBuilder()
                .media("ipc")
                .termLength(termLength)
                .mtu(mtu)
                .initialPosition(initialPosition, INITIAL_TERM_ID, termLength);
        final ExclusivePublication publication = archive.addRecordedExclusivePublication(uri.build(), streamId);
        resources.add(publication);
        final CountersReader counters = aeron.countersReader();
        final long deadline = System.nanoTime() + AWAIT_NS;
        int counterId;
        while ((counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
                archive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
            await(deadline, "recording counter for session " + publication.sessionId());
        }
        while (!publication.isConnected()) {
            await(deadline, "recorded publication to connect");
        }
        return new Recording(publication, RecordingPos.getRecordingId(counters, counterId), initialPosition);
    }

    /**
     * A replay of {@code [from, from + length)} (or to the stop position with {@code AeronArchive.NULL_LENGTH}) as an
     * image positioned at {@code from}.
     */
    public Image replay(final long recordingId, final long from, final long length) {
        final int streamId = nextStreamId++;
        final String channel = "aeron:ipc";
        final int sessionId = (int)archive.startReplay(recordingId, from, length, channel, streamId);
        final Subscription subscription = aeron.addSubscription(ChannelUri.addSessionId(channel, sessionId), streamId);
        resources.add(subscription);
        final long deadline = System.nanoTime() + AWAIT_NS;
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null) {
            await(deadline, "replay image for recording " + recordingId);
        }
        return image;
    }

    @Override
    public void close() {
        for (int i = resources.size() - 1; i >= 0; i--) {
            CloseHelper.quietClose(resources.get(i));
        }
        CloseHelper.quietCloseAll(archive, aeron, driver);
        IoUtil.delete(dir, true);
    }

    /** One recorded publication. */
    public final class Recording {
        private final ExclusivePublication publication;
        private final ClusterLogWriter writer;
        private final long recordingId;
        private final long startPosition;
        private long stopPosition = -1;

        private Recording(final ExclusivePublication publication, final long recordingId, final long startPosition) {
            this.publication = publication;
            this.writer = new ClusterLogWriter(publication);
            this.recordingId = recordingId;
            this.startPosition = startPosition;
        }

        public ClusterLogWriter writer() {
            return writer;
        }

        public ExclusivePublication publication() {
            return publication;
        }

        public long recordingId() {
            return recordingId;
        }

        public long startPosition() {
            return startPosition;
        }

        /** Waits until everything written is recorded, stops the recording, returns its stop position. */
        public long stopRecording() {
            if (stopPosition >= 0) {
                return stopPosition;
            }
            final long target = publication.position();
            final long deadline = System.nanoTime() + AWAIT_NS;
            while (archive.getRecordingPosition(recordingId) < target) {
                await(deadline, "recording " + recordingId + " to reach " + target);
            }
            archive.stopRecording(publication);
            long stop;
            while ((stop = archive.getStopPosition(recordingId)) == AeronArchive.NULL_POSITION) {
                await(deadline, "stop position of recording " + recordingId);
            }
            stopPosition = stop;
            return stop;
        }
    }

    private static void await(final long deadline, final String what) {
        if (System.nanoTime() > deadline) {
            throw new IllegalStateException("timed out waiting for " + what);
        }
        Thread.onSpinWait();
        Thread.yield();
    }
}
