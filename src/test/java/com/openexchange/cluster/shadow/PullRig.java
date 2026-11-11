// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Archive nodes for the pull tests: each one an {@link ArchivingMediaDriver} with its own driver dir, a UDP archive
 * control channel on {@code localhost:<port>} (a replication source), a replication channel and an archive client
 * response channel on UDP (a replication destination), and an archive dir that survives {@link Node#stop} so a
 * stopped source can come back with its recordings ({@link Node#start(boolean)} with {@code wipe=false}).
 */
final class PullRig {

    private PullRig() {
    }

    /** A UDP port that is free right now. */
    static int freeUdpPort() {
        for (int attempt = 0; attempt < 100; attempt++) {
            final int p = 21_000 + (int)(Math.random() * 18_000);
            try (DatagramSocket ignored = new DatagramSocket(p)) {
                return p;
            } catch (final IOException taken) {
                // next
            }
        }
        throw new IllegalStateException("no free UDP port");
    }

    static File tempDir(final String prefix) {
        try {
            return Files.createTempDirectory(prefix).toFile();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One archive node; {@link #close} deletes its dir. */
    static final class Node implements AutoCloseable {
        final String name;
        final File dir;
        final int controlPort;
        final int maxReplays;
        private ArchivingMediaDriver driver;
        private Aeron aeron;
        private AeronArchive archive;
        private final List<AutoCloseable> extra = new ArrayList<>();

        Node(final String name, final int maxReplays) {
            this.name = name;
            this.dir = tempDir("kit-pull-" + name + "-");
            this.controlPort = freeUdpPort();
            this.maxReplays = maxReplays;
            start(true);
        }

        void start(final boolean wipe) {
            final String aeronDir = new File(dir, "driver").getAbsolutePath();
            driver = ArchivingMediaDriver.launch(
                    new MediaDriver.Context()
                            .aeronDirectoryName(aeronDir)
                            .threadingMode(ThreadingMode.SHARED)
                            .sharedIdleStrategy(new BackoffIdleStrategy())
                            .dirDeleteOnStart(true)
                            .dirDeleteOnShutdown(true),
                    new Archive.Context()
                            .aeronDirectoryName(aeronDir)
                            .archiveDir(new File(dir, "archive"))
                            .controlChannel("aeron:udp?endpoint=" + endpoint())
                            .localControlChannel("aeron:ipc")
                            .archiveClientContext(new AeronArchive.Context()
                                    .controlResponseChannel("aeron:udp?endpoint=localhost:0"))
                            .replicationChannel("aeron:udp?endpoint=localhost:0")
                            .maxConcurrentReplays(maxReplays)
                            .recordingEventsEnabled(false)
                            .threadingMode(ArchiveThreadingMode.SHARED)
                            .deleteArchiveOnStart(wipe));
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            archive = AeronArchive.connect(localContext());
        }

        /** A local IPC archive client context on this node's driver (the caller sets the Aeron client). */
        AeronArchive.Context localContext() {
            return new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel("aeron:ipc")
                    .controlResponseChannel("aeron:ipc");
        }

        String endpoint() {
            return "localhost:" + controlPort;
        }

        Aeron aeron() {
            return aeron;
        }

        AeronArchive archive() {
            return archive;
        }

        boolean running() {
            return driver != null;
        }

        /** A shadow through the real writer: {@code length} payload bytes seeded by {@code seed}; returns its id. */
        long write(final ShadowProducer p, final long seq, final ShadowKind kind, final long position,
                   final int length, final long seed, final String term) {
            final ShadowWriter w = new ShadowWriter(aeron, archive, p, new ShadowRig.HashChainSelfCheck(), term,
                    ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS, 6 * ShadowRig.TIMEOUT_NS, seq, -1,
                    new ShadowCounters());
            return ShadowRig.writeOk(w, ShadowRig.image(length, seed), kind,
                    new com.openexchange.cluster.log.LogCursor(position, 1_000 + position, 3, 0)).recordingId();
        }

        /**
         * A raw shadow (header, chunks, optional trailer) on its alias channel; {@code flipAt >= 0} flips one payload
         * byte after the digest was computed; {@code stop=false} leaves it ACTIVE. Returns the recording id.
         */
        long raw(final ShadowHeader h, final byte[] payload, final boolean trailer, final int flipAt,
                 final boolean stop) {
            final String channel = "aeron:ipc?term-length=" + ShadowRig.T + "|" + ShadowAlias.PARAM + h.alias().format();
            final io.aeron.ExclusivePublication pub = archive.addRecordedExclusivePublication(channel,
                    ShadowAlias.STREAM_ID);
            final long deadline = System.nanoTime() + ShadowRig.TIMEOUT_NS;
            final org.agrona.concurrent.status.CountersReader counters = aeron.countersReader();
            int counterId;
            while ((counterId = io.aeron.archive.status.RecordingPos.findCounterIdBySession(counters,
                    pub.sessionId(), archive.archiveId())) == org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID
                    || !pub.isConnected()) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("raw recording did not start");
                }
                Thread.onSpinWait();
            }
            final long recordingId = io.aeron.archive.status.RecordingPos.getRecordingId(counters, counterId);
            final org.agrona.concurrent.UnsafeBuffer hb = new org.agrona.concurrent.UnsafeBuffer(
                    new byte[ShadowHeader.LENGTH]);
            final int headerCrc = h.encode(hb, 0);
            offer(pub, hb, 0, ShadowHeader.LENGTH);
            final byte[] body = payload.clone();
            if (flipAt >= 0) {
                body[flipAt] ^= 0x01;
            }
            final org.agrona.concurrent.UnsafeBuffer pb = new org.agrona.concurrent.UnsafeBuffer(body);
            for (int off = 0; off < body.length; off += h.chunkLength()) {
                offer(pub, pb, off, Math.min(h.chunkLength(), body.length - off));
            }
            if (trailer) {
                final org.agrona.concurrent.UnsafeBuffer tb = new org.agrona.concurrent.UnsafeBuffer(
                        new byte[ShadowTrailer.LENGTH]);
                ShadowTrailer.of(h, headerCrc).encode(tb, 0);
                offer(pub, tb, 0, ShadowTrailer.LENGTH);
            }
            if (!stop) {
                keep(pub);
                return recordingId;
            }
            final long target = pub.position();
            while (archive.getRecordingPosition(recordingId) < target) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("raw recording not recorded");
                }
                Thread.onSpinWait();
            }
            archive.stopRecording(pub);
            while (archive.getStopPosition(recordingId) == AeronArchive.NULL_POSITION) {
                Thread.onSpinWait();
            }
            pub.close();
            return recordingId;
        }

        private static void offer(final io.aeron.ExclusivePublication pub, final org.agrona.DirectBuffer b,
                                  final int off, final int len) {
            final long deadline = System.nanoTime() + ShadowRig.TIMEOUT_NS;
            while (pub.offer(b, off, len) < 0) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("raw offer timed out");
                }
                Thread.onSpinWait();
            }
        }

        void keep(final AutoCloseable c) {
            extra.add(c);
        }

        /** Stops the driver, the archive and the clients; the archive dir stays. */
        void stop() {
            for (final AutoCloseable c : extra) {
                CloseHelper.quietClose(c);
            }
            extra.clear();
            CloseHelper.quietCloseAll(archive, aeron, driver);
            archive = null;
            aeron = null;
            driver = null;
        }

        @Override
        public void close() {
            stop();
            IoUtil.delete(dir, true);
        }
    }
}
