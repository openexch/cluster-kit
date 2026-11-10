// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.log.ReplayContainer;
import com.openexchange.cluster.log.ShadowTriggerListener;
import com.openexchange.cluster.testkit.HashChainEngine;
import com.openexchange.cluster.testkit.RecordedLog;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.AppVersionValidator;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Random;
import java.util.Set;

/**
 * Shadow test rig: a real {@link RecordedLogFixture} archive, writers, catalogs and loaders over it, HashChainEngine-
 * shaped payloads with a real self-check, and the raw path (header, chunks, optional trailer, offered by hand) for the
 * crash shapes and corruptions the writer never produces.
 */
final class ShadowRig implements AutoCloseable {

    static final String T = "64k";
    static final int TERM = 64 * 1024;
    static final int MTU = 1408;
    static final int CHUNK = TERM / 8;
    static final int APP = SemanticVersion.compose(0, 0, 1);
    static final long GENESIS = 0x5EEDL;
    static final NanoClock CLOCK = SystemNanoClock.INSTANCE;
    static final long TIMEOUT_NS = 10_000_000_000L;
    static final ShadowProducer R3 = new ShadowProducer(0, ProducerKind.REPLICA, 3);
    static final ShadowProducer F1 = new ShadowProducer(0, ProducerKind.FOLLOWER, 1);
    static final int ENGINE = HashChainEngine.ENGINE_ID;

    /** Identity values as one engine read reports them. */
    record Id(long genesisId, boolean journalEnabled, long journalSeq, int payloadLayoutHint)
            implements ShadowIdentitySource {
    }

    static final Id ID = new Id(GENESIS, true, 41, 7);

    final RecordedLogFixture fixture;
    final ShadowCounters counters = new ShadowCounters();
    final ShadowVerdicts verdicts = new ShadowVerdicts();

    ShadowRig() {
        this(0);
    }

    ShadowRig(final int controlPort) {
        fixture = new RecordedLogFixture(null, controlPort);
    }

    AeronArchive archive() {
        return fixture.archive();
    }

    ShadowWriter writer(final ShadowProducer p) {
        return writer(p, 1, -1, ShadowWriter.Faults.NONE, Set.of(), CLOCK);
    }

    ShadowWriter writer(final ShadowProducer p, final long seq, final long last, final ShadowWriter.Faults faults,
                        final Set<NegativeControl> off, final NanoClock clock) {
        return new ShadowWriter(fixture.aeron(), fixture.archive(), p, new HashChainSelfCheck(), T, clock, TIMEOUT_NS,
                TIMEOUT_NS, TIMEOUT_NS, seq, last, counters, faults, off.isEmpty()
                ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off));
    }

    ShadowCatalog catalog() {
        return catalog(ShadowCatalog.DEFAULT_LIST_BATCH, Set.of());
    }

    ShadowCatalog catalog(final int listBatch, final Set<NegativeControl> off) {
        return catalog(fixture.archive(), 0, GENESIS, listBatch, off, ShadowReplay.Observer.NONE);
    }

    ShadowCatalog catalog(final AeronArchive archive, final int clusterId, final long genesis, final int listBatch,
                          final Set<NegativeControl> off, final ShadowReplay.Observer observer) {
        return new ShadowCatalog(fixture.aeron(), archive, ENGINE, clusterId, genesis, verdicts, listBatch, CLOCK,
                TIMEOUT_NS, counters, new ShadowReplay(fixture.aeron(), archive, CLOCK, true, observer),
                off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off));
    }

    ShadowCatalog catalog(final ShadowReplay.Observer observer, final String replayChannel) {
        return new ShadowCatalog(fixture.aeron(), fixture.archive(), ENGINE, 0, GENESIS, verdicts, 100, CLOCK,
                TIMEOUT_NS, counters, new ShadowReplay(fixture.aeron(), fixture.archive(), CLOCK, true, observer,
                replayChannel), EnumSet.noneOf(NegativeControl.class));
    }

    ShadowImageLoader loader() {
        return new ShadowImageLoader(fixture.aeron(), fixture.archive(), verdicts, CLOCK, TIMEOUT_NS, counters);
    }

    @Override
    public void close() {
        fixture.close();
    }

    // ------------------------------------------------------------------------------------------------- payloads

    /** A HashChainEngine image (20 bytes) followed by seeded filler: {@code length} payload bytes behind the slot. */
    static ShadowImageBuffer image(final int length, final long seed) {
        final ShadowImageBuffer image = new ShadowImageBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(
                ShadowHeader.LENGTH + Math.max(length, HashChainEngine.IMAGE_LENGTH) + 64)));
        final HashChainEngine engine = new HashChainEngine();
        engine.genesis();
        engine.onTimerEvent(seed, 1, 1);
        final int n = engine.serialize(image.payloadView());
        final Random r = new Random(seed);
        for (int i = n; i < length; i++) {
            image.payloadView().putByte(i, (byte)r.nextInt());
        }
        image.setPayloadLength(length);
        return image;
    }

    static byte[] payload(final ShadowImageBuffer image) {
        final byte[] out = new byte[image.payloadLength()];
        image.payloadView().getBytes(0, out);
        return out;
    }

    /** The real self-check round trip, over a scratch HashChainEngine: restore, re-serialize, compare. */
    static final class HashChainSelfCheck implements ShadowSelfCheck {
        int calls;

        @Override
        public boolean verify(final DirectBuffer payload, final int length) {
            calls++;
            if (length < HashChainEngine.IMAGE_LENGTH) {
                return false;
            }
            final HashChainEngine scratch = new HashChainEngine();
            scratch.restore(payload, HashChainEngine.IMAGE_LENGTH);
            final UnsafeBuffer again = new UnsafeBuffer(new byte[HashChainEngine.IMAGE_LENGTH]);
            scratch.serialize(again);
            for (int i = 0; i < HashChainEngine.IMAGE_LENGTH; i++) {
                if (again.getByte(i) != payload.getByte(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** A cursor from a real ReplayContainer over a synthetic log: term 3 at base 0, then {@code frames} messages. */
    static LogCursor realCursor(final int frames) {
        final RecordedLog.Builder b = RecordedLog.builder();
        b.term(3, 1_000, APP);
        for (int i = 0; i < frames; i++) {
            b.message(7, 1_001 + i, new byte[] {(byte)i, 1, 2, 3});
        }
        final RecordedLog log = b.build();
        final ReplayContainer container = new ReplayContainer(new HashChainEngine(), APP,
                AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR, ShadowTriggerListener.NONE);
        container.genesis();
        log.replay(-1, log.endPosition(), container);
        return container.cursor();
    }

    static LogCursor cursor(final long p) {
        return new LogCursor(p, 1_000 + p, 3, 0);
    }

    // ---------------------------------------------------------------------------------------------- raw path

    /** How a raw recording deviates from what the writer produces. */
    record Raw(boolean trailer, int flipPayloadByte, boolean stop) {
        static final Raw COMPLETE = new Raw(true, -1, true);
        static final Raw NO_TRAILER = new Raw(false, -1, true);

        static Raw flip(final int at) {
            return new Raw(true, at, true);
        }
    }

    /** What a raw recording holds. */
    record RawShadow(long recordingId, ShadowHeader header, byte[] payload, long stop) {
    }

    static ShadowHeader header(final ShadowProducer p, final long seq, final ShadowKind kind, final long logPosition,
                               final long genesis, final int clusterId, final byte[] payload) {
        return new ShadowHeader(ENGINE, clusterId, kind.code(), p.kind().code(), p.producerId(), seq, genesis,
                logPosition, 3, 0, 1_000 + logPosition, 1, 41, payload.length, digest(payload), CHUNK, 7);
    }

    /** The shadow payload digest (amendment Q0: SHA-256, first 8 bytes). */
    static long digest(final byte[] payload) {
        return PayloadDigest.of(new UnsafeBuffer(payload), 0, payload.length);
    }

    static int crc(final byte[] payload) {
        return Crc32c.of(new UnsafeBuffer(payload), 0, payload.length);
    }

    /**
     * Header, chunks of {@link #CHUNK} and (optionally) the trailer, on the alias the header implies, recorded by the
     * fixture's archive. A flipped payload byte is applied AFTER the header CRC and payload CRC were computed (the
     * chunk-corruption shape the writer never produces).
     */
    RawShadow raw(final ShadowHeader h, final byte[] payload, final Raw raw) {
        final String channel = "aeron:ipc?term-length=" + T + "|" + ShadowAlias.PARAM + h.alias().format();
        final RecordedLogFixture.RawRecording rec = fixture.record(channel, ShadowAlias.STREAM_ID);
        final UnsafeBuffer hb = new UnsafeBuffer(new byte[ShadowHeader.LENGTH]);
        final int headerCrc = h.encode(hb, 0);
        rec.offer(hb, 0, ShadowHeader.LENGTH);
        final byte[] body = payload.clone();
        if (raw.flipPayloadByte() >= 0) {
            body[raw.flipPayloadByte()] ^= 0x01;
        }
        final UnsafeBuffer pb = new UnsafeBuffer(body);
        for (int off = 0; off < body.length; off += h.chunkLength()) {
            rec.offer(pb, off, Math.min(h.chunkLength(), body.length - off));
        }
        if (raw.trailer()) {
            final UnsafeBuffer tb = new UnsafeBuffer(new byte[ShadowTrailer.LENGTH]);
            ShadowTrailer.of(h, headerCrc).encode(tb, 0);
            rec.offer(tb, 0, ShadowTrailer.LENGTH);
        }
        final long stop = raw.stop() ? rec.stopRecording() : -1;
        return new RawShadow(rec.recordingId(), h, payload, stop);
    }

    /** Raw bytes offered on an alias channel without any header discipline. */
    long rawMessages(final String alias, final byte[]... messages) {
        final RecordedLogFixture.RawRecording rec = fixture.record(
                "aeron:ipc?term-length=" + T + "|" + ShadowAlias.PARAM + alias, ShadowAlias.STREAM_ID);
        for (final byte[] m : messages) {
            rec.offer(new UnsafeBuffer(m), 0, m.length);
        }
        rec.stopRecording();
        return rec.recordingId();
    }

    /** Writes {@code payload} through the real writer at {@code p}; asserts OK. */
    static ShadowWriter.WriteResult writeOk(final ShadowWriter w, final ShadowImageBuffer image, final ShadowKind kind,
                                            final LogCursor at) {
        final ShadowWriter.WriteResult r = w.write(image, ENGINE, kind, at, ID);
        if (!r.ok()) {
            throw new AssertionError("write at " + at.logPosition() + " failed: " + r);
        }
        return r;
    }

    static void copy(final MutableDirectBuffer dst, final byte[] src) {
        dst.putBytes(0, src);
    }

    /**
     * A negative control (C6): {@code body} runs a test's own assertion with one rule switched off and must fail with
     * an {@link AssertionError} containing {@code named}; the line {@code NC <id> -> <message>} is printed.
     */
    static String nc(final String id, final String named, final Runnable body) {
        try {
            body.run();
        } catch (final AssertionError e) {
            final String msg = String.valueOf(e.getMessage());
            if (!msg.contains(named)) {
                throw new AssertionError("negative control " + id + " failed for the wrong reason: <" + msg
                        + ">, expected <" + named + ">", e);
            }
            System.out.println("NC " + id + " -> " + msg);
            return msg;
        }
        throw new AssertionError("negative control " + id + " did not fail: the assertion cannot see its rule");
    }

    static void green(final String what) {
        System.out.println("GREEN " + what);
    }

    int recordingSubscriptions() {
        final int[] n = {0};
        fixture.archive().listRecordingSubscriptions(0, 1000, "", ShadowAlias.STREAM_ID, true,
                (controlSessionId, correlationId, subscriptionId, streamId, strippedChannel) -> n[0]++);
        return n[0];
    }

    int shadowRecordingCount() {
        final int[] n = {0};
        fixture.archive().listRecordingsForUri(0, 1000, ShadowAlias.prefix(ENGINE), ShadowAlias.STREAM_ID,
                (a, b, c, d, e, f, g, h, i, j, k, l, m, nn, o, pp) -> n[0]++);
        return n[0];
    }
}
