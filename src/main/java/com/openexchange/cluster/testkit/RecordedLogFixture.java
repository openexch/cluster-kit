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
import java.util.ArrayList;
import java.util.List;

/**
 * An in-process {@link ArchivingMediaDriver} in a temp directory, for recording synthetic cluster logs and replaying
 * them as real {@link Image}s: IPC {@link ExclusivePublication}s with the term length, MTU and initial position as
 * parameters, recorded locally; replays through {@code AeronArchive.startReplay}, so replayed positions equal recorded
 * ones (A0:ACD:893-897). The subscriber error handler of the fixture's client is a parameter: Aeron's default (logs
 * and swallows a fragment handler exception) or, for example, {@code RethrowingErrorHandler} (what the service
 * container and ClusterBackup enforce). Test-only: it owns an archive by design.
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
    private int nextStreamId = FIRST_STREAM_ID;

    /** Aeron's default subscriber error handler. */
    public RecordedLogFixture() {
        this(null);
    }

    /** @param subscriberErrorHandler the fixture client's subscriber error handler; null keeps Aeron's default */
    public RecordedLogFixture(final ErrorHandler subscriberErrorHandler) {
        try {
            dir = Files.createTempDirectory("kit-recorded-log-").toFile();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        ArchivingMediaDriver launched = null;
        Aeron client = null;
        AeronArchive archiveClient = null;
        try {
            launched = ArchivingMediaDriver.launch(
                    new MediaDriver.Context()
                            .aeronDirectoryName(aeronDir)
                            .threadingMode(ThreadingMode.SHARED)
                            .dirDeleteOnStart(true)
                            .dirDeleteOnShutdown(true),
                    new Archive.Context()
                            .aeronDirectoryName(aeronDir)
                            .archiveDir(new File(dir, "archive"))
                            .controlChannelEnabled(false)
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
