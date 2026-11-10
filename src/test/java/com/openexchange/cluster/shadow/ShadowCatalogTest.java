// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.StagingArchive;
import com.openexchange.cluster.shadow.ShadowCatalog.Answer;
import com.openexchange.cluster.shadow.ShadowCatalog.Floor;
import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import com.openexchange.cluster.shadow.ShadowEntry.State;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.File;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.openexchange.cluster.shadow.ShadowRig.CHUNK;
import static com.openexchange.cluster.shadow.ShadowRig.ENGINE;
import static com.openexchange.cluster.shadow.ShadowRig.F1;
import static com.openexchange.cluster.shadow.ShadowRig.GENESIS;
import static com.openexchange.cluster.shadow.ShadowRig.ID;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.Raw;
import static com.openexchange.cluster.shadow.ShadowRig.cursor;
import static com.openexchange.cluster.shadow.ShadowRig.header;
import static com.openexchange.cluster.shadow.ShadowRig.image;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static com.openexchange.cluster.shadow.ShadowRig.payload;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The catalog against a real archive and a real replication. Raw recordings (offered by hand) build the
 * crash shapes, the corruptions and the disagreeing copies the writer never produces.
 */
public class ShadowCatalogTest {

    static final int LEN = CHUNK * 2 + 777;

    static int freeUdpPort() {
        try (DatagramSocket s = new DatagramSocket(0)) {
            return s.getLocalPort();
        } catch (final java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A destination archive for replication, deleted on close. */
    static final class Staging implements AutoCloseable {
        final File dir;
        final StagingArchive archive;

        Staging() throws java.io.IOException {
            dir = Files.createTempDirectory("kit-shadow-staging-").toFile();
            archive = StagingArchive.launch(dir);
        }

        Aeron aeron() {
            return archive.archive().context().aeron();
        }

        @Override
        public void close() {
            archive.close();
            IoUtil.delete(dir, true);
        }
    }

    /** Counts replay opens (the scan's replay budget). */
    static final class Opens implements ShadowReplay.Observer {
        final AtomicInteger n = new AtomicInteger();

        @Override
        public void onReplayOpen(final long recordingId, final long from, final long length) {
            n.incrementAndGet();
        }
    }

    static ShadowWriter.WriteResult write(final ShadowWriter w, final long p, final ShadowKind kind, final long seed) {
        return ShadowRig.writeOk(w, image(LEN, seed), kind, cursor(p));
    }

    // ---------------------------------------------------------------------------------------------------- (a)

    @Test
    public void threeShadowsAndACrashShapeWithPagingAndQueries() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            write(w, 100, ShadowKind.ALIGNED, 1);
            write(w, 200, ShadowKind.CADENCE, 2);
            write(w, 300, ShadowKind.STANDBY, 3);
            final byte[] crash = payload(image(LEN, 4));
            rig.raw(header(R3, 4, ShadowKind.ALIGNED, 400, GENESIS, 0, crash), crash, Raw.NO_TRAILER);
            final ShadowCatalog catalog = rig.catalog(2, Set.of());
            final Scan s = catalog.scan();
            assertEquals(4, s.all().size());
            assertEquals(3, s.usable().size());
            assertEquals(3, s.distinctCount());
            assertEquals(1, s.count(State.NO_TRAILER));
            assertEquals(List.of(300L, 200L, 100L), s.usable().stream().map(LogicalShadow::logPosition).toList());
            assertEquals(Answer.Kind.FOUND, s.equalAt(200).kind());
            assertEquals(Answer.Kind.EMPTY, s.equalAt(400).kind());
            assertEquals(300, s.newestAtOrAbove(150).shadow().logPosition());
            assertEquals(200, s.newestBelow(300, 0).shadow().logPosition());
            assertEquals(Answer.Kind.EMPTY, s.newestBelow(100, 0).kind());
            assertEquals("no floor before the startup pass", Floor.Kind.UNKNOWN, s.verifiedAtOrAbove(0).kind());
            assertTrue(s.verified().isEmpty());
            final Scan v = catalog.verifyAllComplete();
            assertEquals(Floor.Kind.FOUND, v.verifiedAtOrAbove(0).kind());
            assertEquals(300, v.verifiedAtOrAbove(250).position());
            assertEquals(Floor.Kind.NONE, v.verifiedAtOrAbove(301).kind());
            assertEquals(300, v.verifiedNewest().position());
            assertEquals(3, v.verified().size());
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.CATALOG_STARTUP_PASS_DONE));
            assertEquals(300, rig.counters.get(ShadowCounters.Counter.NEWEST_VERIFIED_POSITION));
            ShadowRig.green("catalog (a): " + v.all());
        }
    }

    // ------------------------------------------------------------------------------------------- crash shapes

    static void assertFloorKnown(final Scan s) {
        if (s.verifiedAtOrAbove(0).kind() == Floor.Kind.UNKNOWN) {
            throw new AssertionError("floor unknown on a classifiable recording: errors=" + s.errors() + " "
                    + s.all());
        }
    }

    @Test
    public void crashShapesAreClassifiedNotErrors() {
        try (ShadowRig rig = new ShadowRig()) {
            write(rig.writer(R3), 100, ShadowKind.ALIGNED, 1);
            // (i) the last frame is a full 1,408-byte fragment: stop - 96 falls inside it
            final byte[] torn = payload(image(CHUNK + 2 * 1376, 5));
            final ShadowRig.RawShadow i = rig.raw(header(R3, 5, ShadowKind.ALIGNED, 500, GENESIS, 0, torn), torn,
                    Raw.NO_TRAILER);
            // (ii) the last 96 bytes are a padding frame: the window replays zero messages
            final ShadowHeader ph = header(R3, 6, ShadowKind.ALIGNED, 600, GENESIS, 0, new byte[64]);
            final RecordedLogFixture.RawRecording pad = rig.fixture.record("aeron:ipc?term-length=64k|"
                    + ShadowAlias.PARAM + ph.alias().format(), ShadowAlias.STREAM_ID);
            final UnsafeBuffer hb = new UnsafeBuffer(new byte[ShadowHeader.LENGTH]);
            ph.encode(hb, 0);
            pad.offer(hb, 0, ShadowHeader.LENGTH);
            final UnsafeBuffer filler = new UnsafeBuffer(new byte[1376]);
            for (int k = 0; k < 46; k++) {
                pad.offer(filler, 0, 1376);
            }
            pad.offer(filler, 0, 480);
            assertEquals(ShadowRig.TERM - 96, pad.publication().position());
            assertEquals("padding written, the term rotated", ExclusivePublication.ADMIN_ACTION,
                    pad.offerOnce(filler, 0, 1376));
            final long padStop = pad.stopRecording();
            assertEquals(ShadowRig.TERM, padStop);
            final ShadowCatalog catalog = rig.catalog();
            final Scan s = catalog.verifyAllComplete();
            final ShadowEntry ei = s.entry(i.recordingId());
            assertEquals(ei.toString(), State.NO_TRAILER, ei.state());
            assertEquals("INVALID_POSITION at stop - 96", ei.reason());
            final ShadowEntry eii = s.entry(pad.recordingId());
            assertEquals(eii.toString(), State.NO_TRAILER, eii.state());
            assertEquals("trailer window delivered 0 message(s)", eii.reason());
            assertEquals(0, s.errors());
            assertFloorKnown(s);
            nc("INVALID_POSITION mapped to SCAN_ERROR", "floor unknown on a classifiable recording", () -> {
                final ShadowCatalog bad = rig.catalog(100, Set.of(NegativeControl.INVALID_POSITION_AS_SCAN_ERROR));
                assertFloorKnown(bad.verifyAllComplete());
            });
            ShadowRig.green("crash shapes: (i) " + ei + " (ii) " + eii);
        }
    }

    @Test
    public void aLostArchiveConnectionIsScanErrorAndRecovers() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            for (int k = 1; k <= 4; k++) {
                write(w, 100L * k, ShadowKind.ALIGNED, k);
            }
            final AeronArchive doomed = rig.fixture.newArchiveClient(rig.fixture.aeron());
            final AtomicInteger opens = new AtomicInteger();
            final ShadowCatalog catalog = rig.catalog(doomed, 0, GENESIS, 100, Set.of(), (id, from, len) -> {
                if (opens.incrementAndGet() == 2) {
                    doomed.close(); // the control channel goes away mid-scan
                }
            });
            final Scan s = catalog.scan();
            assertTrue("errors " + s.all(), s.errors() > 0);
            assertTrue(s.all().stream().allMatch(e -> e.state() == State.SCAN_ERROR || e.state() == State.COMPLETE));
            assertEquals(Floor.Kind.UNKNOWN, s.verifiedAtOrAbove(0).kind());
            final AeronArchive fresh = rig.fixture.newArchiveClient(rig.fixture.aeron());
            try {
                final Scan again = rig.catalog(fresh, 0, GENESIS, 100, Set.of(), ShadowReplay.Observer.NONE)
                        .verifyAllComplete();
                assertEquals(0, again.errors());
                assertEquals(4, again.verified().size());
                ShadowRig.green("lost connection: " + s.errors() + " SCAN_ERROR, recovered to " + again.verified().size()
                        + " VERIFIED");
            } finally {
                CloseHelper.quietClose(fresh);
            }
        }
    }

    // --------------------------------------------------------------------------------------- active and cache

    @Test
    public void activeRecordingNeverUsable() throws Exception {
        try (ShadowRig rig = new ShadowRig()) {
            final CountDownLatch paused = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final ShadowWriter w = rig.writer(R3, 1, -1, new ShadowWriter.Faults() {
                @Override
                public long offer(final ExclusivePublication p, final DirectBuffer b, final int o, final int l,
                                  final int i) {
                    if (i == 2) {
                        paused.countDown();
                        try {
                            release.await(20, TimeUnit.SECONDS);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return p.offer(b, o, l);
                }
            }, Set.of(), ShadowRig.CLOCK);
            final AtomicReference<ShadowWriter.WriteResult> result = new AtomicReference<>();
            final Thread writer = new Thread(() -> result.set(w.write(image(LEN, 1), ENGINE, ShadowKind.ALIGNED,
                    cursor(100), ID)));
            writer.start();
            assertTrue(paused.await(20, TimeUnit.SECONDS));
            final Opens opens = new Opens();
            final ShadowCatalog catalog = rig.catalog(rig.archive(), 0, GENESIS, 100, Set.of(), opens);
            final Scan s = catalog.scan();
            assertEquals(1, s.all().size());
            assertEquals(State.ACTIVE, s.all().get(0).state());
            assertTrue(s.usable().isEmpty());
            assertEquals("the ACTIVE poll opened no replay", 0, opens.n.get());
            Thread.sleep(1 + new java.util.Random(9).nextInt(50));
            release.countDown();
            writer.join(20_000);
            assertTrue(String.valueOf(result.get()), result.get().ok());
            final Scan later = catalog.scan();
            assertEquals(State.COMPLETE, later.all().get(0).state());
            assertEquals(2, opens.n.get());
        }
    }

    @Test
    public void unchangedDescriptorsOpenNoReplay() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            for (int k = 1; k <= 3; k++) {
                write(w, 100L * k, ShadowKind.CADENCE, k);
            }
            final Opens opens = new Opens();
            final ShadowCatalog catalog = rig.catalog(rig.archive(), 0, GENESIS, 100, Set.of(), opens);
            catalog.scan();
            assertEquals(6, opens.n.get());
            catalog.scan();
            assertEquals("second scan over an unchanged set", 6, opens.n.get());
            write(w, 400, ShadowKind.CADENCE, 4);
            final Scan s = catalog.scan();
            assertEquals("a new shadow costs exactly two replays", 8, opens.n.get());
            assertEquals(4, s.usable().size());
        }
    }

    // ----------------------------------------------------------------------------------- chunk corruption, G7

    /** A pruner stand-in: the floor it would purge the log below, and the rule that the floor is VERIFIED. */
    static void assertFloorVerified(final Scan s, final boolean scanOnly) {
        final long floor = scanOnly ? (s.newestAtOrAbove(0).kind() == Answer.Kind.FOUND
                ? s.newestAtOrAbove(0).shadow().logPosition() : -1) : s.verifiedAtOrAbove(0).position();
        if (floor < 0) {
            return;
        }
        final LogicalShadow at = s.atPosition(floor);
        if (at == null || at.copies().stream().noneMatch(e -> e.state() == State.VERIFIED)) {
            throw new AssertionError("log purged below an unverified shadow at " + floor + ": " + at);
        }
    }

    @Test
    public void aCorruptMiddleChunkIsCompleteToTheScanCorruptToTheDeepReadAndStaysCorrupt() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            final ShadowWriter.WriteResult older = write(w, 100, ShadowKind.ALIGNED, 1);
            final ShadowCatalog catalog = rig.catalog();
            catalog.verifyAllComplete();
            final byte[] good = payload(image(LEN, 2));
            final ShadowRig.RawShadow bad = rig.raw(header(R3, 2, ShadowKind.ALIGNED, 500, GENESIS, 0, good), good,
                    Raw.flip(CHUNK + 100));
            final Scan s1 = catalog.scan();
            assertEquals(State.COMPLETE, s1.entry(bad.recordingId()).state());
            assertEquals("in usable", 500, s1.usable().get(0).logPosition());
            assertFalse("not in verified", s1.verified().stream().anyMatch(x -> x.logPosition() == 500));
            assertFloorVerified(s1, false);
            nc("floor consumes scan-only usable", "log purged below an unverified shadow",
                    () -> assertFloorVerified(s1, true));
            final ShadowCatalog.DeepResult deep = catalog.verifyDeep(s1.entry(bad.recordingId()));
            assertEquals(deep.toString(), ShadowCatalog.DeepResult.Kind.CORRUPT, deep.kind());
            assertTrue(deep.reason(), deep.reason().startsWith("payload digest"));
            for (int k = 0; k < 10; k++) {
                final Scan sk = catalog.scan();
                assertEquals(State.CORRUPT, sk.entry(bad.recordingId()).state());
                assertEquals(1, rig.counters.get(ShadowCounters.Counter.CATALOG_CORRUPT));
            }
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.CATALOG_CORRUPT_TOTAL));
            assertEquals(ShadowImageLoader.LoadResult.Kind.REFUSED, rig.loader().load(catalog.scan()
                    .entry(bad.recordingId()), new UnsafeBuffer(new byte[LEN])).kind());
            assertEquals(ShadowImageLoader.LoadResult.Kind.CORRUPT, rig.loader().load(s1.entry(bad.recordingId()),
                    new UnsafeBuffer(new byte[LEN])).kind());
            nc("CORRUPT cleared by the next scan", "CORRUPT entry re-labelled COMPLETE on scan 2", () -> {
                final State st = rig.catalog(100, Set.of(NegativeControl.CORRUPT_CLEARED_BY_SCAN)).scan()
                        .entry(bad.recordingId()).state();
                if (st != State.CORRUPT) {
                    throw new AssertionError("CORRUPT entry re-labelled " + st + " on scan 2");
                }
            });
            // the anchor falls through the CORRUPT newest to the older VERIFIED copy
            final ShadowCatalog.PurgeResult refused = catalog.purge(older.recordingId(), false);
            assertEquals(refused.toString(), ShadowCatalog.PURGE_CODE_ANCHOR, refused.code());
            final ShadowWriter.WriteResult newer = write(w, 600, ShadowKind.ALIGNED, 3);
            catalog.verifyDeep(catalog.scan().entry(newer.recordingId()));
            assertTrue(catalog.purge(older.recordingId(), false).purged());
            assertFloorVerified(catalog.scan(), false);
        }
    }

    // ---------------------------------------------------------------------------------------- (b) replication

    @Test
    public void aReplicatedCopyKeepsTheAliasAndALateExtensionGetsANewKey() throws Exception {
        try (ShadowRig rig = new ShadowRig(freeUdpPort()); Staging staging = new Staging()) {
            final ShadowWriter.WriteResult src = write(rig.writer(R3), 100, ShadowKind.ALIGNED, 1);
            final String control = rig.fixture.controlChannel();
            final long copy = staging.archive.replicate(control, src.recordingId(), Aeron.NULL_VALUE,
                    AeronArchive.NULL_POSITION);
            final String[] channels = new String[2];
            final long[] positions = new long[2];
            staging.archive.archive().listRecording(copy, (a, b, id, st, sp, start, stop, it, sfl, tbl, mtu, sid, str,
                                                           stripped, original, source) -> {
                channels[0] = stripped;
                channels[1] = original;
                positions[0] = start;
                positions[1] = stop;
            });
            assertTrue(channels[0], channels[0].contains(ShadowAlias.PARAM + src.alias()));
            assertTrue(channels[1], channels[1].contains(ShadowAlias.PARAM + src.alias()));
            assertEquals(0, positions[0]);
            assertEquals(src.bytes(), positions[1]);
            final ShadowVerdicts verdicts = new ShadowVerdicts();
            final ShadowCatalog remote = new ShadowCatalog(staging.aeron(), staging.archive.archive(), ENGINE, 0,
                    GENESIS, verdicts, 100, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, new ShadowCounters());
            final Scan s = remote.verifyAllComplete();
            assertEquals(State.VERIFIED, s.entry(copy).state());
            final UnsafeBuffer into = new UnsafeBuffer(ByteBuffer.allocateDirect(LEN));
            final ShadowImageLoader.LoadResult load = new ShadowImageLoader(staging.aeron(), staging.archive.archive(),
                    verdicts, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, new ShadowCounters()).load(s.entry(copy), into);
            assertTrue(load.ok());
            final byte[] got = new byte[LEN];
            into.getBytes(0, got);
            assertArrayEquals(payload(image(LEN, 1)), got);

            // late arrival: a partial copy stopped at a frame boundary inside chunk 2, then extended to the end
            final long inChunk2 = ShadowHeader.FRAME_LENGTH + (5L * 1408 + 1344) + 2 * 1408;
            final long partial = staging.archive.replicate(control, src.recordingId(), Aeron.NULL_VALUE, inChunk2);
            final ShadowCatalog dst = new ShadowCatalog(staging.aeron(), staging.archive.archive(), ENGINE, 0, GENESIS,
                    verdicts, 100, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, new ShadowCounters());
            final ShadowCatalog stale = new ShadowCatalog(staging.aeron(), staging.archive.archive(), ENGINE, 0,
                    GENESIS, verdicts, 100, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, new ShadowCounters(),
                    new ShadowReplay(staging.aeron(), staging.archive.archive(), ShadowRig.CLOCK),
                    EnumSet.of(NegativeControl.STRUCTURAL_CACHE_BY_RECORDING_ID));
            final ShadowEntry p0 = dst.scan().entry(partial);
            stale.scan();
            assertEquals(p0.toString(), State.NO_TRAILER, p0.state());
            assertEquals(inChunk2, p0.stopPosition());
            Thread.sleep(1_000); // the rest arrives seconds later (C2)
            staging.archive.replicate(control, src.recordingId(), partial, AeronArchive.NULL_POSITION);
            final ShadowEntry p1 = dst.scan().entry(partial);
            assertExtendedReclassified(p1);
            nc("structural cache keyed by recording id only", "extended recording reused the partial copy's verdict",
                    () -> assertExtendedReclassified(stale.scan().entry(partial)));
            ShadowRig.green("replicated copy " + s.entry(copy) + "; partial " + p0 + " -> extended " + p1);
        }
    }

    static void assertExtendedReclassified(final ShadowEntry e) {
        if (e.state() != State.COMPLETE) {
            throw new AssertionError("extended recording reused the partial copy's verdict: " + e);
        }
    }

    @Test
    public void duplicateCopiesCountOnce() throws Exception {
        try (ShadowRig rig = new ShadowRig(freeUdpPort()); Staging staging = new Staging()) {
            final ShadowWriter.WriteResult src = write(rig.writer(R3), 100, ShadowKind.ALIGNED, 1);
            final long c1 = staging.archive.replicate(rig.fixture.controlChannel(), src.recordingId(), Aeron.NULL_VALUE,
                    AeronArchive.NULL_POSITION);
            final long c2 = staging.archive.replicate(rig.fixture.controlChannel(), src.recordingId(), Aeron.NULL_VALUE,
                    AeronArchive.NULL_POSITION);
            assertTrue(c1 != c2);
            final ShadowCatalog dst = new ShadowCatalog(staging.aeron(), staging.archive.archive(), ENGINE, 0, GENESIS,
                    new ShadowVerdicts(), 100, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, new ShadowCounters());
            final Scan s = dst.scan();
            assertDistinctOne(s);
            assertEquals(2, s.atPosition(100).copies().size());
            assertEquals(1, s.atPosition(100).producers().size());
            assertEquals(Answer.Kind.FOUND, s.equalAt(100).kind());
            nc("dedupe by recording", "distinctCount 2, expected 1", () -> assertDistinctOne(
                    new ShadowCatalog(staging.aeron(), staging.archive.archive(), ENGINE, 0, GENESIS,
                            new ShadowVerdicts(), 100, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, new ShadowCounters(),
                            new ShadowReplay(staging.aeron(), staging.archive.archive(), ShadowRig.CLOCK),
                            EnumSet.of(NegativeControl.DEDUPE_BY_RECORDING)).scan()));
        }
    }

    static void assertDistinctOne(final Scan s) {
        if (s.distinctCount() != 1) {
            throw new AssertionError("distinctCount " + s.distinctCount() + ", expected 1");
        }
    }

    // ---------------------------------------------------------------------------------- conflicts and agreement

    static void assertNoUsableAtConflict(final Scan s, final long p) {
        if (s.usable().stream().anyMatch(x -> x.logPosition() == p)) {
            throw new AssertionError("a CONFLICT copy appeared in usable at " + p + ": " + s.usable());
        }
    }

    static void assertNoTwoCrcsUsable(final Scan s, final long p) {
        final long crcs = s.usable().stream().filter(x -> x.logPosition() == p)
                .flatMap(x -> x.copies().stream()).map(e -> e.header().payloadDigest()).distinct().count();
        if (crcs > 1) {
            throw new AssertionError("two usable shadows at one position with different CRCs: " + s.usable());
        }
    }

    @Test
    public void crossProducerDisagreementIsAConflictNeverAWinner() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            write(w, 100, ShadowKind.ALIGNED, 1);
            write(w, 200, ShadowKind.ALIGNED, 2);
            write(w, 300, ShadowKind.ALIGNED, 3);
            final ShadowCatalog catalog = rig.catalog();
            assertEquals(3, catalog.scan().distinctCount());
            final byte[] other = payload(image(LEN, 3));
            other[LEN - 1] ^= 0x40; // one byte differs, CRCs recomputed: a self-consistent different state
            rig.raw(header(F1, 1, ShadowKind.ALIGNED, 300, GENESIS, 0, other), other, Raw.COMPLETE);
            final long before = rig.counters.get(ShadowCounters.Counter.CATALOG_CONFLICT_TOTAL);
            final Scan s = catalog.scan();
            assertEquals(Answer.Kind.CONFLICT, s.equalAt(300).kind());
            assertEquals(Answer.Kind.CONFLICT, s.newestAtOrAbove(150).kind());
            assertEquals(Answer.Kind.CONFLICT, s.newestBelow(301, 250).kind());
            assertEquals(2, s.distinctCount());
            assertEquals(before + 1, rig.counters.get(ShadowCounters.Counter.CATALOG_CONFLICT_TOTAL));
            assertEquals(1, s.conflicts().size());
            assertEquals(Set.of("R3", "F1"), s.conflicts().get(0).producers().keySet());
            assertEquals(2, s.conflicts().get(0).copies().stream().map(e -> e.header().payloadDigest()).distinct().count());
            assertNoUsableAtConflict(s, 300);
            nc("group key includes producer and kind", "two usable shadows at one position with different CRCs",
                    () -> assertNoTwoCrcsUsable(rig.catalog(100,
                            Set.of(NegativeControl.GROUP_KEY_INCLUDES_PRODUCER_AND_KIND)).scan(), 300));
            // a third copy with the ORIGINAL bytes joins the group: still CONFLICT, never majority
            final byte[] original = payload(image(LEN, 3));
            rig.raw(header(R3, 9, ShadowKind.LOCAL, 300, GENESIS, 0, original), original, Raw.COMPLETE);
            final Scan s3 = catalog.scan();
            assertEquals(Answer.Kind.CONFLICT, s3.equalAt(300).kind());
            assertEquals(3, s3.conflicts().get(0).copies().size());
            ShadowRig.green("cross-producer conflict: " + s3.conflicts());
        }
    }

    /** A payload that ends with the CRC32C of what precedes it (as a kit {@code SnapshotEnvelope} image does). */
    static byte[] selfSealed(final long seed) {
        final byte[] p = payload(image(LEN, seed));
        final java.util.zip.CRC32C c = new java.util.zip.CRC32C();
        c.update(p, 0, p.length - 4);
        final int v = (int)c.getValue();
        p[p.length - 4] = (byte)v;
        p[p.length - 3] = (byte)(v >>> 8);
        p[p.length - 2] = (byte)(v >>> 16);
        p[p.length - 1] = (byte)(v >>> 24);
        return p;
    }

    /** Two producers write the two images at one position through real writers; {@code off} switches a rule off. */
    static Scan twoProducersAt(final ShadowRig rig, final long p, final byte[] a, final byte[] b,
                               final Set<NegativeControl> off) {
        for (final Object[] w : new Object[][] {{R3, a}, {F1, b}}) {
            final ShadowImageBuffer image = image(LEN, 1);
            image.payloadView().putBytes(0, (byte[])w[1]);
            image.setPayloadLength(((byte[])w[1]).length);
            ShadowRig.writeOk(rig.writer((ShadowProducer)w[0], 1, -1, ShadowWriter.Faults.NONE, off, ShadowRig.CLOCK),
                    image, ShadowKind.ALIGNED, cursor(p));
        }
        return rig.catalog().scan();
    }

    static void assertConflictAt(final Scan s, final long p) {
        if (s.equalAt(p).kind() != Answer.Kind.CONFLICT) {
            throw new AssertionError("two self-consistent images with different state agree at " + p + ": "
                    + s.atPosition(p));
        }
    }

    /**
     * Amendment Q0 (2026-11-01), flipped from the characterization that found it: two DIFFERENT payloads that each end
     * in their own CRC32C (every core image) have EQUAL CRC32C (the residue property), but different SHA-256 digests,
     * so two producers that wrote them at one position are a CONFLICT. NC-Q0: the writers computing the digest with
     * CRC32C again make the two images agree.
     */
    @Test
    public void twoDifferentSelfSealedPayloadsAreAConflict() {
        final byte[] a = selfSealed(1);
        final byte[] b = selfSealed(2);
        assertTrue(!java.util.Arrays.equals(a, b));
        assertEquals("(char) the residue: equal CRC32C for different self-sealed payloads", ShadowRig.crc(a),
                ShadowRig.crc(b));
        assertTrue("the SHA-256 digests differ", ShadowRig.digest(a) != ShadowRig.digest(b));
        try (ShadowRig rig = new ShadowRig()) {
            final Scan s = twoProducersAt(rig, 800, a, b, Set.of());
            assertConflictAt(s, 800);
            assertTrue(s.verified().isEmpty() && s.usable().isEmpty());
        }
        try (ShadowRig rig = new ShadowRig()) {
            nc("NC-Q0 payload digest computed with CRC32C", "two self-consistent images with different state agree",
                    () -> assertConflictAt(twoProducersAt(rig, 800, a, b,
                            Set.of(NegativeControl.DIGEST_AS_CRC32C)), 800));
        }
        ShadowRig.green("Q0 flipped: CRC32C " + Integer.toHexString(ShadowRig.crc(a)) + " for both, digests "
                + Long.toHexString(ShadowRig.digest(a)) + " / " + Long.toHexString(ShadowRig.digest(b)) + ": CONFLICT");
    }

    /**
     * Amendment Q0, ME shape: {@code [DURABLE_MAGIC][version][length][CRC32C][payload]} (match {@code SnapshotCodec}).
     * Two self-sealed images of one length and different state have different digests.
     */
    @Test
    public void twoDifferentDurableMagicImagesHaveDifferentDigests() {
        final byte[][] images = new byte[2][];
        for (int k = 0; k < 2; k++) {
            final byte[] payload = new byte[4096];
            new java.util.Random(k).nextBytes(payload);
            final java.util.zip.CRC32C c = new java.util.zip.CRC32C();
            c.update(payload);
            final org.agrona.concurrent.UnsafeBuffer img = new org.agrona.concurrent.UnsafeBuffer(new byte[20 + 4096]);
            img.putLong(0, -0x434d444c45444745L);
            img.putInt(8, 2);
            img.putInt(12, payload.length);
            img.putInt(16, (int)c.getValue());
            img.putBytes(20, payload);
            images[k] = img.byteArray();
        }
        assertEquals(images[0].length, images[1].length);
        assertTrue("different state, different digest", ShadowRig.digest(images[0]) != ShadowRig.digest(images[1]));
        ShadowRig.green("Q0 ME shape: DURABLE_MAGIC digests " + Long.toHexString(ShadowRig.digest(images[0])) + " / "
                + Long.toHexString(ShadowRig.digest(images[1])) + " (CRC32C " + Integer.toHexString(ShadowRig.crc(
                images[0])) + " / " + Integer.toHexString(ShadowRig.crc(images[1])) + ")");
    }

    @Test
    public void sameProducerDisagreementIsAConflictToo() {
        try (ShadowRig rig = new ShadowRig()) {
            final byte[] a = payload(image(LEN, 7));
            final byte[] b = a.clone();
            b[10] ^= 1;
            rig.raw(header(R3, 1, ShadowKind.ALIGNED, 700, GENESIS, 0, a), a, Raw.COMPLETE);
            rig.raw(header(R3, 2, ShadowKind.ALIGNED, 700, GENESIS, 0, b), b, Raw.COMPLETE);
            final Scan s = rig.catalog().scan();
            assertEquals(Answer.Kind.CONFLICT, s.equalAt(700).kind());
            assertNoUsableAtConflict(s, 700);
            nc("conflict picks the newer recording", "a CONFLICT copy appeared in usable",
                    () -> assertNoUsableAtConflict(rig.catalog(100, Set.of(NegativeControl.CONFLICT_NEWEST_WINS))
                            .scan(), 700));
        }
    }

    @Test
    public void twoProducersAgreeingAtOnePositionAreOneShadow() {
        try (ShadowRig rig = new ShadowRig()) {
            write(rig.writer(R3), 300, ShadowKind.ALIGNED, 3);
            final byte[] same = payload(image(LEN, 3));
            rig.raw(header(F1, 1, ShadowKind.LOCAL, 300, GENESIS, 0, same), same, Raw.COMPLETE);
            final ShadowCatalog catalog = rig.catalog();
            final Scan s = catalog.verifyAllComplete();
            assertEquals(1, s.distinctCount());
            assertTrue(s.conflicts().isEmpty());
            assertEquals(Set.of("R3", "F1"), s.atPosition(300).producers().keySet());
            assertTrue("D14 input: an agreeing VERIFIED group with two producers", s.verified().get(0).producers().size() >= 2);
        }
    }

    @Test
    public void foreignShadowsAreListedCountedAndExcluded() {
        try (ShadowRig rig = new ShadowRig()) {
            write(rig.writer(R3), 100, ShadowKind.ALIGNED, 1);
            final byte[] p = payload(image(LEN, 8));
            rig.raw(header(R3, 2, ShadowKind.ALIGNED, 200, 0x6EED, 0, p), p, Raw.COMPLETE);
            rig.raw(header(new ShadowProducer(7, ProducerKind.REPLICA, 3), 1, ShadowKind.ALIGNED, 300, GENESIS, 7, p), p,
                    Raw.COMPLETE);
            final Scan s = rig.catalog().scan();
            assertEquals(3, s.all().size());
            assertEquals(2, s.count(State.FOREIGN));
            assertEquals(2, s.foreign());
            assertEquals(2, rig.counters.get(ShadowCounters.Counter.CATALOG_FOREIGN));
            assertNotNull(s.firstForeign());
            assertEquals(1, s.usable().size());
            assertEquals(100, s.usable().get(0).logPosition());
        }
    }

    @Test
    public void aBootstrapCatalogThatSeesTwoGenesesTrustsNeither() {
        try (ShadowRig rig = new ShadowRig()) {
            write(rig.writer(R3), 100, ShadowKind.ALIGNED, 1);
            final ShadowCatalog bootstrap = rig.catalog(rig.archive(), 0, 0, 100, Set.of(), ShadowReplay.Observer.NONE);
            assertEquals("one genesis: usable", 1, bootstrap.scan().usable().size());
            final byte[] p = payload(image(LEN, 8));
            rig.raw(header(R3, 2, ShadowKind.ALIGNED, 200, 0x6EED, 0, p), p, Raw.COMPLETE);
            final Scan s = bootstrap.scan();
            assertTrue("two geneses: nothing usable " + s.all(), s.usable().isEmpty());
            assertEquals(2, s.count(State.FOREIGN));
            assertTrue(s.all().get(0).reason(), s.all().get(0).reason().startsWith("ambiguous genesis"));
        }
    }

    @Test
    public void aReusedProducerSeqIsADiagnosticNotAnIdentity() {
        try (ShadowRig rig = new ShadowRig()) {
            final byte[] a = payload(image(LEN, 1));
            final byte[] b = payload(image(LEN, 2));
            rig.raw(header(R3, 5, ShadowKind.ALIGNED, 100, GENESIS, 0, a), a, Raw.COMPLETE);
            rig.raw(header(R3, 5, ShadowKind.ALIGNED, 200, GENESIS, 0, b), b, Raw.COMPLETE);
            final Scan s = rig.catalog().scan();
            assertEquals(2, s.usable().size());
            assertEquals(1, s.seqReuse());
            assertEquals(1, rig.counters.get(ShadowCounters.Counter.CATALOG_SEQ_REUSE_TOTAL));
        }
    }

    // --------------------------------------------------------------------------------------------------- purge

    @Test
    public void purgeOfACopyWithAReplayInProgressSurfacesActiveRecording() {
        try (ShadowRig rig = new ShadowRig()) {
            final ShadowWriter w = rig.writer(R3);
            // larger than the replay publication's window (half of the 64 KiB term), so an unread replay stays open
            final ShadowWriter.WriteResult oldest = ShadowRig.writeOk(w, image(CHUNK * 6, 1), ShadowKind.ALIGNED,
                    cursor(100));
            write(w, 200, ShadowKind.ALIGNED, 2);
            final ShadowCatalog catalog = rig.catalog();
            catalog.verifyAllComplete();
            // held open: no subscriber yet, so the archive keeps the session waiting for its connection (5 s)
            final long replay = rig.archive().startReplay(oldest.recordingId(), 0, AeronArchive.NULL_LENGTH,
                    "aeron:ipc?linger=0", 5555);
            try {
                final ShadowCatalog.PurgeResult busy = catalog.purge(oldest.recordingId(), false);
                assertEquals(busy.toString(), ShadowCatalog.PurgeResult.Kind.REFUSED, busy.kind());
                assertEquals(busy.toString(), io.aeron.archive.client.ArchiveException.ACTIVE_RECORDING, busy.code());
                assertEquals(1, rig.counters.purgeRefused(io.aeron.archive.client.ArchiveException.ACTIVE_RECORDING));
                ShadowRig.green("purge during a replay: " + busy);
            } finally {
                rig.archive().stopReplay(replay);
            }
        }
    }

    static void assertAnchorSurvives(final ShadowCatalog c, final long p) {
        final LogicalShadow at = c.scan().atPosition(p);
        if (at == null || at.copies().isEmpty()) {
            throw new AssertionError("second copy of the anchor deleted at " + p);
        }
    }

    @Test
    public void purgeRescansRefusesTheLastAnchorCopyAndSurfacesArchiveRefusals() {
        try (ShadowRig rig = new ShadowRig()) {
            write(rig.writer(R3), 100, ShadowKind.ALIGNED, 1);
            final byte[] p = payload(image(LEN, 2));
            final List<Long> copies = new ArrayList<>();
            final List<Long> stale = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                copies.add(rig.raw(header(R3, 2, ShadowKind.ALIGNED, 200, GENESIS, 0, p), p, Raw.COMPLETE).recordingId());
                stale.add(rig.raw(header(F1, 2, ShadowKind.ALIGNED, 300, GENESIS, 0, p), p, Raw.COMPLETE).recordingId());
            }
            // the stale-scan catalog only sees 300's copies once they exist; first the real catalog over 200 and 300
            final ShadowCatalog catalog = rig.catalog();
            catalog.verifyAllComplete();
            assertTrue(catalog.purge(stale.get(0), false).purged());
            final ShadowCatalog.PurgeResult second = catalog.purge(stale.get(1), false);
            assertEquals(second.toString(), ShadowCatalog.PURGE_CODE_ANCHOR, second.code());
            assertAnchorSurvives(catalog, 300);
            // after 300's last copy is force-purged, 200 is the anchor with two VERIFIED copies: the GREEN refusal
            assertTrue(catalog.purge(stale.get(1), true).purged());
            assertTrue(catalog.purge(copies.get(0), false).purged());
            final ShadowCatalog.PurgeResult last200 = catalog.purge(copies.get(1), false);
            assertEquals("the last VERIFIED copy of the anchor at 200 is refused: " + last200,
                    ShadowCatalog.PURGE_CODE_ANCHOR, last200.code());
            assertAnchorSurvives(catalog, 200);
            // Negative control: a third copy at 200, then two purges decided on one stale scan delete both
            copies.add(rig.raw(header(R3, 2, ShadowKind.ALIGNED, 200, GENESIS, 0, p), p, Raw.COMPLETE).recordingId());
            nc("purge decides on a stale scan", "second copy of the anchor deleted", () -> {
                final ShadowCatalog loose = rig.catalog(100, Set.of(NegativeControl.PURGE_ON_STALE_SCAN));
                loose.verifyAllComplete();
                loose.purge(copies.get(1), false);
                loose.purge(copies.get(2), false);
                assertAnchorSurvives(catalog, 200);
            });
        }
    }

    /**
     * The case the anchor rule exists for: the anchor has one VERIFIED copy A and one COMPLETE copy B (not yet
     * deep-read, possibly torn). purge(A) is refused (B does not count); purge(B) is allowed; then a COMPLETE sibling
     * C arrives between two purges (a late replication, C2) and only its verification frees A.
     */
    @Test
    public void theLastVerifiedCopyIsRefusedWhileItsSiblingIsOnlyComplete() {
        try (ShadowRig rig = new ShadowRig()) {
            write(rig.writer(R3), 100, ShadowKind.ALIGNED, 1);
            final byte[] p = payload(image(LEN, 3));
            final long a = rig.raw(header(F1, 1, ShadowKind.ALIGNED, 300, GENESIS, 0, p), p, Raw.COMPLETE).recordingId();
            final ShadowCatalog catalog = rig.catalog();
            catalog.verifyAllComplete();
            final long b = rig.raw(header(F1, 1, ShadowKind.ALIGNED, 300, GENESIS, 0, p), p, Raw.COMPLETE).recordingId();
            final Scan s = catalog.scan();
            assertEquals(State.VERIFIED, s.entry(a).state());
            assertEquals(State.COMPLETE, s.entry(b).state());
            final ShadowCatalog.PurgeResult refusedA = catalog.purge(a, false);
            assertEquals("A is the last VERIFIED copy of the anchor (B is only COMPLETE): " + refusedA,
                    ShadowCatalog.PURGE_CODE_ANCHOR, refusedA.code());
            final ShadowCatalog.PurgeResult purgedB = catalog.purge(b, false);
            assertTrue("B (COMPLETE, A stays VERIFIED) may go: " + purgedB, purgedB.purged());
            assertNull(catalog.scan().entry(b));
            // C arrives between two purges
            final ShadowCatalog.PurgeResult againA = catalog.purge(a, false);
            assertEquals(ShadowCatalog.PURGE_CODE_ANCHOR, againA.code());
            final long c = rig.raw(header(F1, 1, ShadowKind.ALIGNED, 300, GENESIS, 0, p), p, Raw.COMPLETE).recordingId();
            assertEquals("C COMPLETE does not free A", ShadowCatalog.PURGE_CODE_ANCHOR, catalog.purge(a, false).code());
            assertEquals(ShadowCatalog.DeepResult.Kind.VERIFIED, catalog.verifyDeep(catalog.scan().entry(c)).kind());
            assertTrue("C VERIFIED frees A", catalog.purge(a, false).purged());
            assertEquals("and C is now the last VERIFIED copy", ShadowCatalog.PURGE_CODE_ANCHOR,
                    catalog.purge(c, false).code());
        }
    }

    /** A trailer of a newer version keeps its own meaning, it does not read as a crashed write. */
    @Test
    public void aNewerTrailerVersionIsNotNoTrailer() {
        try (ShadowRig rig = new ShadowRig()) {
            final byte[] p = payload(image(LEN, 4));
            final ShadowHeader h = header(R3, 1, ShadowKind.ALIGNED, 100, GENESIS, 0, p);
            final UnsafeBuffer hb = new UnsafeBuffer(new byte[ShadowHeader.LENGTH]);
            final int headerCrc = h.encode(hb, 0);
            final UnsafeBuffer tb = new UnsafeBuffer(new byte[ShadowTrailer.LENGTH]);
            ShadowTrailer.of(h, headerCrc).encode(tb, 0);
            tb.putInt(8, 2, java.nio.ByteOrder.LITTLE_ENDIAN);
            ShadowTrailer.sealCrc(tb, 0);
            final byte[][] messages = new byte[2 + (p.length + CHUNK - 1) / CHUNK][];
            messages[0] = hb.byteArray();
            int k = 1;
            for (int off = 0; off < p.length; off += CHUNK) {
                messages[k++] = java.util.Arrays.copyOfRange(p, off, Math.min(p.length, off + CHUNK));
            }
            messages[k] = tb.byteArray();
            final long id = rig.rawMessages(h.alias().format(), messages);
            final ShadowEntry e = rig.catalog().scan().entry(id);
            assertEquals(e.toString(), State.TRAILER_VERSION_UNSUPPORTED, e.state());
            assertEquals("TRAILER_VERSION_UNSUPPORTED", e.reason());
        }
    }
}
