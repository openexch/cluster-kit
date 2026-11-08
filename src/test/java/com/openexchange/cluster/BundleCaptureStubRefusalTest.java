// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import com.openexchange.cluster.shadow.ShadowStub;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.codecs.ClusterTimeUnit;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerEncoder;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RCS S4 11.12 (D19, G18): a cluster dir whose newest snapshot group's service recording holds an enveloped stub is
 * refused with the D19 text, the staging chain is not advanced and nothing is copied; a v5-shaped payload (any
 * non-stub tag) is captured as today. Real archives and a real replication from a source archive with a UDP control
 * channel; the service snapshot recording carries Aeron's container frames (marker BEGIN, marker END) before the
 * service bytes, as {@code ClusteredServiceAgent.snapshotState} writes them.
 *
 * <p>Lives in {@code com.openexchange.cluster} (not {@code cluster.shadow}, contract section 2) because the negative
 * control is the package-private {@code capture(..., stubPeek=false)} seam of {@link BundleCapture}.</p>
 */
public class BundleCaptureStubRefusalTest {

    static final BundleCapture.EngineSchema SCHEMA = new BundleCapture.EngineSchema(901, 5);

    static int freeUdpPort() {
        try (DatagramSocket s = new DatagramSocket(0)) {
            return s.getLocalPort();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A source node: a log recording, and a cluster dir per service payload. */
    static final class Source implements AutoCloseable {
        final RecordedLogFixture fixture = new RecordedLogFixture(null, freeUdpPort());
        final File root;
        final long logRecordingId;
        final long logStop;

        Source() throws IOException {
            root = Files.createTempDirectory("kit-bundle-stub-").toFile();
            final RecordedLogFixture.RawRecording log = fixture.record("aeron:ipc?term-length=64k", 5001);
            final UnsafeBuffer chunk = new UnsafeBuffer(new byte[992]);
            for (int i = 0; i < 40; i++) {
                chunk.putInt(0, i);
                log.offer(chunk, 0, chunk.capacity());
            }
            logStop = log.stopRecording();
            logRecordingId = log.recordingId();
        }

        /** A cluster dir whose newest group is (CM recording, service recording holding {@code servicePayload}). */
        File clusterDir(final String name, final byte[] servicePayload, final int streamId) {
            final File dir = new File(root, name);
            assertTrue(dir.mkdirs());
            final RecordedLogFixture.RawRecording cm = fixture.record("aeron:ipc?term-length=64k", streamId);
            cm.offer(new UnsafeBuffer(new byte[96]), 0, 96);
            cm.stopRecording();
            final RecordedLogFixture.RawRecording svc = fixture.record("aeron:ipc?term-length=64k", streamId + 1);
            final UnsafeBuffer m = new UnsafeBuffer(new byte[128]);
            svc.offer(m, 0, marker(m, SnapshotMark.BEGIN));
            svc.offer(m, 0, marker(m, SnapshotMark.END));
            svc.offer(new UnsafeBuffer(servicePayload), 0, servicePayload.length);
            svc.stopRecording();
            try (RecordingLog rl = new RecordingLog(dir, true)) {
                rl.appendTerm(logRecordingId, 0, 0, 0);
                rl.appendSnapshot(cm.recordingId(), 0, 0, logStop, 0, ConsensusModule.Configuration.SERVICE_ID);
                rl.appendSnapshot(svc.recordingId(), 0, 0, logStop, 0, 0);
            }
            return dir;
        }

        int marker(final UnsafeBuffer b, final SnapshotMark mark) {
            final SnapshotMarkerEncoder e = new SnapshotMarkerEncoder();
            e.wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
                    .typeId(ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID)
                    .logPosition(logStop)
                    .leadershipTermId(0)
                    .index(0)
                    .mark(mark)
                    .timeUnit(ClusterTimeUnit.MILLIS)
                    .appVersion(1);
            return MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
        }

        @Override
        public void close() {
            fixture.close();
            IoUtil.delete(root, true);
        }
    }

    static byte[] stubImage(final long pCm) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[ShadowStub.ENVELOPED_LENGTH]);
        new ShadowStub(1, 0, 0x5EEDL, 0, pCm, -1, -1).encodeEnveloped(new SnapshotEnvelope(), b);
        return b.byteArray();
    }

    /** A v5-shaped enveloped payload: layout tag -5 first, then filler. */
    static byte[] v5Image() {
        final int payload = 512;
        final UnsafeBuffer b = new UnsafeBuffer(new byte[SnapshotEnvelope.envelopedLength(payload)]);
        b.putLong(SnapshotEnvelope.payloadOffset(), -5L);
        for (int i = 8; i < payload; i++) {
            b.putByte(SnapshotEnvelope.payloadOffset() + i, (byte)i);
        }
        new SnapshotEnvelope().seal(b, payload);
        return b.byteArray();
    }

    static BundleCapture.Result capture(final Source src, final File clusterDir, final File work,
                                        final boolean peek) throws IOException {
        return BundleCapture.capture(clusterDir, src.fixture.controlChannel(), new File(work, "staging"),
                new File(work, "bundles"), "test", 0, SCHEMA, -1, peek);
    }

    static void assertNoStubBundle(final BundleCapture.Result r, final long p) {
        if (r.captured) {
            throw new AssertionError("bundle captured with a stub snapshot at " + p + " (" + r + ")");
        }
    }

    static boolean empty(final File dir) {
        final String[] names = dir.list();
        return names == null || names.length == 0;
    }

    @Test
    public void aStubOnlyBundleIsRefusedAndAFullImageIsCaptured() throws IOException {
        try (Source src = new Source()) {
            final File stubDir = src.clusterDir("stub", stubImage(src.logStop), 6001);
            final File work = new File(src.root, "work-stub");
            final long refusedBefore = BundleCapture.stubRefusals();
            final BundleCapture.Result refused = capture(src, stubDir, work, true);
            assertNoStubBundle(refused, src.logStop);
            assertEquals(BundleCapture.stubRefusal(src.logStop), refused.reason);
            assertEquals("snapshot at " + src.logStop + " is a stub (RCS); a bundle needs a full image or the "
                    + "covering shadow, which BundleCapture cannot capture yet (D19)", refused.reason);
            assertEquals(src.logStop, refused.snapshotPosition);
            assertEquals(refusedBefore + 1, BundleCapture.stubRefusals());
            assertTrue("nothing copied", empty(new File(work, "bundles")));
            final BundleCapture.StagingState state = BundleCapture.StagingState.load(new File(work, "staging"));
            assertEquals("the staging chain did not advance", -1, state.lastBundledPosition);
            // a second attempt is refused again (the state did not move), not "already captured"
            assertEquals(BundleCapture.stubRefusal(src.logStop), capture(src, stubDir, work, true).reason);
            System.out.println("GREEN 11.12 stub refused: " + refused);

            final File fullDir = src.clusterDir("full", v5Image(), 6011);
            final File fullWork = new File(src.root, "work-full");
            final BundleCapture.Result captured = capture(src, fullDir, fullWork, true);
            assertTrue(String.valueOf(captured), captured.captured);
            assertEquals(src.logStop, captured.snapshotPosition);
            assertFalse(empty(new File(fullWork, "bundles")));
            assertEquals(src.logStop,
                    BundleCapture.StagingState.load(new File(fullWork, "staging")).lastBundledPosition);
            System.out.println("GREEN 11.12 v5 captured: " + captured);

            // negative control: the peek off -> the stub is bundled as if it were a ledger
            final File ncWork = new File(src.root, "work-nc");
            final BundleCapture.Result unpeeked = capture(src, stubDir, ncWork, false);
            try {
                assertNoStubBundle(unpeeked, src.logStop);
                throw new IllegalStateException("negative control did not fail: the assertion cannot see D19");
            } catch (final AssertionError expected) {
                assertTrue(expected.getMessage(), expected.getMessage()
                        .contains("bundle captured with a stub snapshot at " + src.logStop));
                System.out.println("NC S4-11.12 peek off -> " + expected.getMessage());
            }
        }
    }

    @Test
    public void theSchemaNamesTheStubTag() {
        assertEquals(ShadowStub.STUB_TAG, SCHEMA.stubTag());
        assertEquals(-100L, new BundleCapture.EngineSchema(1, 1).stubTag());
    }
}
