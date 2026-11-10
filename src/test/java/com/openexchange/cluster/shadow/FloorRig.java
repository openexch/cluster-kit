// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.ArchiveHousekeeping;
import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.codecs.ClusterTimeUnit;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerEncoder;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.IoUtil;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.openexchange.cluster.shadow.ShadowRig.CHUNK;
import static com.openexchange.cluster.shadow.ShadowRig.ENGINE;
import static com.openexchange.cluster.shadow.ShadowRig.GENESIS;
import static com.openexchange.cluster.shadow.ShadowRig.R3;

/**
 * Floor rig: a {@link ShadowRig} archive whose segment files are 64 KiB (so a purge below a
 * position really deletes segments), a member-shaped cluster dir ({@code recording.log} with one term entry and one
 * CM + service snapshot group at P_cm), a stopped log recording of {@link #LOG_BYTES} bytes, and raw shadows at chosen
 * positions with chosen journal seqs. The floor reads the archive's log start, never memory (C4).
 *
 * <p>The service snapshot recording is shaped like Aeron's (marker BEGIN, marker END, then the service payload) and
 * holds an enveloped stub by default, or a v5-shaped full image ({@code new FloorRig(true)}); the floor's
 * precondition (D2) reads it with the real {@link StubPeek}, as the member does.</p>
 */
final class FloorRig implements AutoCloseable {

    static final int SEGMENT = 64 * 1024;
    static final long LOG_BYTES = 1_200_000;
    static final long P_CM = 800_000;
    static final long P_1 = 300_000;
    static final long P_2 = 500_000;
    static final long P_B = 900_000;
    static final long P_A = 1_000_000;
    /** This node's durable journal seq: a shadow with {@code journalSeq <= DURABLE} is skip-safe. */
    static final long DURABLE = 50;
    static final long SAFE_SEQ = 41;
    static final long UNSAFE_SEQ = 99;
    static final int LEN = 1_500;

    final ShadowRig rig;
    final File clusterDir;
    final long logRecordingId;
    final ShadowHousekeeping.Counters hk = new ShadowHousekeeping.Counters();
    private long seq = 1;

    /** The newest group's service payload (EMPTY adds a recording with markers only). */
    enum Service { STUB, FULL, EMPTY }

    FloorRig() {
        this(false);
    }

    /** @param fullImage the newest group's service payload is a full image (else a stub) */
    FloorRig(final boolean fullImage) {
        this(fullImage ? Service.FULL : Service.STUB);
    }

    FloorRig(final Service service) {
        final String key = "aeron.archive.segment.file.length";
        final String before = System.getProperty(key);
        System.setProperty(key, String.valueOf(SEGMENT));
        try {
            rig = new ShadowRig();
        } finally {
            if (before == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, before);
            }
        }
        try {
            clusterDir = Files.createTempDirectory("kit-floor-cluster-").toFile();
        } catch (final IOException e) {
            rig.close();
            throw new UncheckedIOException(e);
        }
        final RecordedLogFixture.RawRecording log = rig.fixture.record("aeron:ipc?term-length=64k", 4242);
        final UnsafeBuffer chunk = new UnsafeBuffer(new byte[992]);
        while (log.publication().position() < LOG_BYTES) {
            log.offer(chunk, 0, chunk.capacity());
        }
        log.stopRecording();
        logRecordingId = log.recordingId();
        final long cm = smallRecording(4243);
        final long svc = serviceSnapshot(4244, switch (service) {
            case FULL -> fullImage();
            case STUB -> stubImage();
            case EMPTY -> null;
        });
        try (RecordingLog rl = new RecordingLog(clusterDir, true)) {
            rl.appendTerm(logRecordingId, 0, 0, 0);
            rl.appendSnapshot(cm, 0, 0, P_CM, 0, ConsensusModule.Configuration.SERVICE_ID);
            rl.appendSnapshot(svc, 0, 0, P_CM, 0, 0);
        }
        final long segmentFile = segmentFileLength();
        if (segmentFile != SEGMENT) {
            throw new IllegalStateException("the rig needs " + SEGMENT + "-byte segments, the archive gave "
                    + segmentFile);
        }
    }

    private long smallRecording(final int streamId) {
        final RecordedLogFixture.RawRecording r = rig.fixture.record("aeron:ipc?term-length=64k", streamId);
        r.offer(new UnsafeBuffer(new byte[64]), 0, 64);
        r.stopRecording();
        return r.recordingId();
    }

    /** @param payload the service message after the END marker, or null for none (an empty service payload) */
    private long serviceSnapshot(final int streamId, final byte[] payload) {
        final RecordedLogFixture.RawRecording r = rig.fixture.record("aeron:ipc?term-length=64k", streamId);
        final UnsafeBuffer m = new UnsafeBuffer(new byte[128]);
        r.offer(m, 0, marker(m, SnapshotMark.BEGIN));
        r.offer(m, 0, marker(m, SnapshotMark.END));
        if (payload != null) {
            r.offer(new UnsafeBuffer(payload), 0, payload.length);
        }
        r.stopRecording();
        return r.recordingId();
    }

    private static int marker(final UnsafeBuffer b, final SnapshotMark mark) {
        final SnapshotMarkerEncoder e = new SnapshotMarkerEncoder();
        e.wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                .typeId(ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID)
                .logPosition(P_CM)
                .leadershipTermId(0)
                .index(0)
                .mark(mark)
                .timeUnit(ClusterTimeUnit.MILLIS)
                .appVersion(1);
        return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
    }

    static byte[] stubImage() {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[ShadowStub.ENVELOPED_LENGTH]);
        new ShadowStub(ENGINE, 0, GENESIS, 0, P_CM, -1, -1).encodeEnveloped(new SnapshotEnvelope(), b);
        return b.byteArray();
    }

    /** A v5-shaped enveloped payload: layout tag -5 first, then filler. */
    static byte[] fullImage() {
        final int payload = 512;
        final UnsafeBuffer b = new UnsafeBuffer(new byte[SnapshotEnvelope.envelopedLength(payload)]);
        b.putLong(SnapshotEnvelope.payloadOffset(), -5L);
        for (int i = 8; i < payload; i++) {
            b.putByte(SnapshotEnvelope.payloadOffset() + i, (byte)i);
        }
        new SnapshotEnvelope().seal(b, payload);
        return b.byteArray();
    }

    /** The floor's precondition the way the member reads it: peek the newest group's service recording. */
    ShadowFloor.NewestImage newestImage() {
        long newest = -1;
        long svc = -1;
        try (RecordingLog rl = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : rl.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.logPosition >= newest
                        && e.serviceId != ConsensusModule.Configuration.SERVICE_ID) {
                    newest = e.logPosition;
                    svc = e.recordingId;
                }
            }
        }
        final long[] range = {-1, -1};
        archive().listRecording(svc, (a, b, c, d, e, startPosition, stopPosition, h, i, j, k, l, m, n, o, p) -> {
            range[0] = startPosition;
            range[1] = stopPosition;
        });
        final StubPeek.Kind kind = StubPeek.peek(archive().context().aeron(), archive(), SystemNanoClock.INSTANCE, svc,
                range[0], range[1], SystemNanoClock.INSTANCE.nanoTime() + TimeUnit.SECONDS.toNanos(30));
        return switch (kind) {
            case STUB -> ShadowFloor.NewestImage.STUB;
            case FULL -> ShadowFloor.NewestImage.FULL;
            case UNKNOWN -> ShadowFloor.NewestImage.UNKNOWN;
        };
    }

    AeronArchive archive() {
        return rig.archive();
    }

    /** P_cm as the pruner sees it: the newest valid snapshot entry. */
    long latestSnapshotPosition() {
        long latest = -1;
        try (RecordingLog rl = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : rl.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid) {
                    latest = Math.max(latest, e.logPosition);
                }
            }
        }
        return latest;
    }

    /** The log recording's start position, read from the archive. */
    long logStart() {
        final long[] start = {-1};
        archive().listRecording(logRecordingId, (a, b, c, d, e, startPosition, g, h, i, j, k, l, m, n, o, p) ->
                start[0] = startPosition);
        return start[0];
    }

    long segmentFileLength() {
        final long[] len = {-1};
        archive().listRecording(logRecordingId, (a, b, c, d, e, f, g, h, segmentFileLength, j, k, l, m, n, o, p) ->
                len[0] = segmentFileLength);
        return len[0];
    }

    static Predicate<ShadowHeader> skipSafe() {
        return h -> !h.journalEnabledFlag() || DURABLE >= h.journalSeq();
    }

    ShadowFloor floor(final ShadowFloor.Margin margin, final Set<NegativeControl> off) {
        return new ShadowFloor(this::latestSnapshotPosition, this::newestImage, skipSafe(), margin, this::logStart, hk,
                off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off));
    }

    ShadowFloor floor() {
        return floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, Set.of());
    }

    static byte[] payload(final long seed) {
        return ShadowRig.payload(ShadowRig.image(LEN, seed));
    }

    /** A complete raw shadow at {@code p} with {@code journalSeq}; returns its recording id. */
    long shadow(final long p, final long journalSeq) {
        return shadow(p, journalSeq, payload(p), GENESIS);
    }

    long shadow(final long p, final long journalSeq, final byte[] payload, final long genesis) {
        final ShadowHeader h = new ShadowHeader(ENGINE, 0, ShadowKind.ALIGNED.code(), R3.kind().code(),
                R3.producerId(), seq++, genesis, p, 3, 0, 1_000 + p, 1, journalSeq, payload.length,
                ShadowRig.digest(payload), CHUNK, 7);
        return rig.raw(h, payload, ShadowRig.Raw.COMPLETE).recordingId();
    }

    /** A second copy at {@code p} with different content: the position becomes CONFLICT. */
    long conflictAt(final long p, final long journalSeq) {
        return shadow(p, journalSeq, payload(p + 7), GENESIS);
    }

    /** What the pruner's purge pass does: {@code purgeBelow(clusterDir, archive, floor.getAsLong())}. */
    ArchiveHousekeeping.Result purge(final ShadowFloor floor) {
        return ArchiveHousekeeping.purgeBelow(clusterDir, archive(), floor.getAsLong());
    }

    @Override
    public void close() {
        rig.close();
        IoUtil.delete(clusterDir, true);
    }
}
