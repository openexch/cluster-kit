// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Header (rules H0-H14) and trailer (T1-T5) codecs. */
public class ShadowImageCodecTest {

    static final ShadowHeader.Options TEST = ShadowHeader.Options.forEngine(99);

    static ShadowHeader sample(final Random r) {
        return new ShadowHeader(r.nextBoolean() ? 1 : 2, r.nextInt(100), 1 + r.nextInt(4), 1 + r.nextInt(2),
                r.nextInt(10), 1 + r.nextInt(1_000), 1 + (r.nextLong() & 0x7FFF_FFFF_FFFFL), 1_000_000 + r.nextInt(1 << 20),
                r.nextInt(50), r.nextInt(1_000_000), r.nextLong() & 0xFFFF_FFFFFFFL, r.nextInt(2),
                r.nextLong() & 0xFFFF_FFFFL, 1 + r.nextInt(400_000_000), r.nextLong(), 1 << (16 + r.nextInt(5)),
                r.nextInt());
    }

    static UnsafeBuffer encoded(final ShadowHeader h) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[ShadowHeader.LENGTH]);
        h.encode(b, 0);
        return b;
    }

    @Test
    public void headerRoundTripsOverSeededSets() {
        final Random r = new Random(0x5EED_C0DEL);
        for (int i = 0; i < 2_000; i++) {
            final ShadowHeader h = sample(r);
            final ShadowHeader.Decoded d = ShadowHeader.decode(encoded(h), 0, ShadowHeader.LENGTH,
                    ShadowHeader.Options.PRODUCTION);
            assertTrue(String.valueOf(d.problem()), d.ok());
            assertEquals(h, d.header());
        }
    }

    /** Every single-bit flip over [0, 128) is refused; inside [0, 124) the reason is HEADER_CORRUPT. */
    static List<String> acceptedFlips(final boolean crcRule) {
        final ShadowHeader h = sample(new Random(3));
        final List<String> accepted = new ArrayList<>();
        for (int bit = 0; bit < ShadowHeader.LENGTH * 8; bit++) {
            final UnsafeBuffer b = encoded(h);
            b.putByte(bit / 8, (byte)(b.getByte(bit / 8) ^ (1 << (bit % 8))));
            final ShadowHeader.Decoded d = crcRule
                    ? ShadowHeader.decode(b, 0, ShadowHeader.LENGTH, ShadowHeader.Options.PRODUCTION)
                    : decodeWithoutCrc(b);
            if (d.ok()) {
                accepted.add("flipped bit accepted at offset " + bit / 8 + " bit " + bit % 8);
            } else if (crcRule && bit / 8 < ShadowHeader.CRC_OFFSET
                    && d.problem().reason() != ShadowHeader.Reason.HEADER_CORRUPT) {
                accepted.add("flip at offset " + bit / 8 + " refused as " + d.problem() + ", not HEADER_CORRUPT");
            }
        }
        return accepted;
    }

    /** Negative control: H1 switched off (the CRC is recomputed over the flipped bytes before decoding). */
    static ShadowHeader.Decoded decodeWithoutCrc(final UnsafeBuffer flipped) {
        final UnsafeBuffer copy = new UnsafeBuffer(new byte[ShadowHeader.LENGTH]);
        copy.putBytes(0, flipped, 0, ShadowHeader.LENGTH);
        ShadowHeader.sealCrc(copy, 0);
        return ShadowHeader.decode(copy, 0, ShadowHeader.LENGTH, ShadowHeader.Options.PRODUCTION);
    }

    @Test
    public void everySingleBitFlipIsRefused() {
        final List<String> accepted = acceptedFlips(true);
        assertTrue(accepted.toString(), accepted.isEmpty());
    }

    @Test
    public void negativeControlNc2HeaderCrcNotChecked() {
        nc("header CRC not checked", "flipped bit accepted at offset", () -> {
            final List<String> accepted = acceptedFlips(false);
            assertTrue(accepted.isEmpty() ? "" : accepted.get(0), accepted.isEmpty());
        });
    }

    /** A header that passes the CRC (recomputed) but fails one rule. */
    static ShadowHeader.Problem rule(final Consumer<UnsafeBuffer> mutate) {
        final UnsafeBuffer b = encoded(sample(new Random(11)));
        mutate.accept(b);
        ShadowHeader.sealCrc(b, 0);
        final ShadowHeader.Decoded d = ShadowHeader.decode(b, 0, ShadowHeader.LENGTH, ShadowHeader.Options.PRODUCTION);
        assertNotNull("rule not applied", d.problem());
        return d.problem();
    }

    static void putInt(final UnsafeBuffer b, final int at, final int v) {
        b.putInt(at, v, ByteOrder.LITTLE_ENDIAN);
    }

    static void putLong(final UnsafeBuffer b, final int at, final long v) {
        b.putLong(at, v, ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    public void eachRuleH0ToH14RefusesWithItsReason() {
        final UnsafeBuffer ok = encoded(sample(new Random(11)));
        assertEquals(ShadowHeader.Reason.HEADER_LENGTH_MISMATCH,
                ShadowHeader.decode(ok, 0, 127, ShadowHeader.Options.PRODUCTION).problem().reason());
        final UnsafeBuffer big = new UnsafeBuffer(new byte[129]);
        big.putBytes(0, ok, 0, 128);
        assertEquals(ShadowHeader.Reason.HEADER_LENGTH_MISMATCH,
                ShadowHeader.decode(big, 0, 129, ShadowHeader.Options.PRODUCTION).problem().reason());
        assertEquals("H1 magic", "HEADER_CORRUPT", rule(b -> putInt(b, ShadowHeader.O_MAGIC, 0x4F585349)).toString());
        assertEquals("H2", "HEADER_VERSION_UNSUPPORTED", rule(b -> putInt(b, ShadowHeader.O_VERSION, 2)).toString());
        assertEquals("H3", "HEADER_CORRUPT", rule(b -> putInt(b, ShadowHeader.O_HEADER_LENGTH, 112)).toString());
        assertEquals("HEADER_FIELD_INVALID(engineId)", rule(b -> putInt(b, ShadowHeader.O_ENGINE, 99)).toString());
        assertEquals("HEADER_FIELD_INVALID(engineId)", rule(b -> putInt(b, ShadowHeader.O_ENGINE, 3)).toString());
        assertEquals("HEADER_FIELD_INVALID(clusterId)", rule(b -> putInt(b, ShadowHeader.O_CLUSTER, -1)).toString());
        assertEquals("HEADER_FIELD_INVALID(kind)", rule(b -> putInt(b, ShadowHeader.O_KIND, 0)).toString());
        assertEquals("HEADER_FIELD_INVALID(kind)", rule(b -> putInt(b, ShadowHeader.O_KIND, 5)).toString());
        assertEquals("HEADER_FIELD_INVALID(producerKind)",
                rule(b -> putInt(b, ShadowHeader.O_PRODUCER_KIND, 3)).toString());
        assertEquals("HEADER_FIELD_INVALID(producerId)",
                rule(b -> putInt(b, ShadowHeader.O_PRODUCER_ID, -1)).toString());
        assertEquals("HEADER_FIELD_INVALID(producerSeq)",
                rule(b -> putLong(b, ShadowHeader.O_PRODUCER_SEQ, 0)).toString());
        assertEquals("H10", "UNIDENTIFIED", rule(b -> putLong(b, ShadowHeader.O_GENESIS, 0)).toString());
        assertEquals("H10", "UNIDENTIFIED", rule(b -> putLong(b, ShadowHeader.O_GENESIS, -42)).toString());
        assertEquals("HEADER_FIELD_INVALID(logPosition)", rule(b -> {
            putLong(b, ShadowHeader.O_TERM_BASE, 2_000_000_000L);
        }).toString());
        assertEquals("HEADER_FIELD_INVALID(term)", rule(b -> putLong(b, ShadowHeader.O_TERM_ID, -1)).toString());
        assertEquals("HEADER_FIELD_INVALID(term)", rule(b -> putLong(b, ShadowHeader.O_TERM_BASE, -1)).toString());
        assertEquals("HEADER_FIELD_INVALID(clusterTime)",
                rule(b -> putLong(b, ShadowHeader.O_CLUSTER_TIME, -1)).toString());
        assertEquals("HEADER_FIELD_INVALID(journalEnabled)",
                rule(b -> putInt(b, ShadowHeader.O_JOURNAL_ENABLED, 2)).toString());
        assertEquals("HEADER_FIELD_INVALID(reserved)", rule(b -> putInt(b, ShadowHeader.O_RESERVED0, 1)).toString());
        assertEquals("HEADER_FIELD_INVALID(reserved)",
                rule(b -> b.putByte(ShadowHeader.O_RESERVED1 + 3, (byte)1)).toString());
        assertEquals("HEADER_FIELD_INVALID(journalSeq)",
                rule(b -> putLong(b, ShadowHeader.O_JOURNAL_SEQ, -1)).toString());
        assertEquals("HEADER_FIELD_INVALID(payloadLength)",
                rule(b -> putLong(b, ShadowHeader.O_PAYLOAD_LENGTH, 0)).toString());
        assertEquals("HEADER_FIELD_INVALID(payloadLength)",
                rule(b -> putLong(b, ShadowHeader.O_PAYLOAD_LENGTH, Integer.MAX_VALUE)).toString());
        assertEquals("HEADER_FIELD_INVALID(chunkLength)",
                rule(b -> putInt(b, ShadowHeader.O_CHUNK_LENGTH, 0)).toString());
        // engine 99 only under the test option; payloadLayoutHint any int32
        final UnsafeBuffer t = encoded(sample(new Random(11)));
        putInt(t, ShadowHeader.O_ENGINE, 99);
        ShadowHeader.sealCrc(t, 0);
        assertTrue(ShadowHeader.decode(t, 0, 128, TEST).ok());
        for (final int hint : new int[] {Integer.MIN_VALUE, -1, 0, 4, Integer.MAX_VALUE}) {
            final UnsafeBuffer x = encoded(sample(new Random(11)));
            putInt(x, ShadowHeader.O_LAYOUT_HINT, hint);
            ShadowHeader.sealCrc(x, 0);
            assertTrue("hint " + hint, ShadowHeader.decode(x, 0, 128, ShadowHeader.Options.PRODUCTION).ok());
        }
    }

    /** The digest definition (SHA-256, first 8 bytes, big-endian) and its absolute offsets. */
    @Test
    public void theDigestIsSha256First8BytesAtHeader104AndTrailer24() {
        final byte[] abc = "abc".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        assertEquals("FIPS 180-2 vector SHA-256(\"abc\") = ba7816bf8f01cfea...", 0xBA7816BF8F01CFEAL,
                PayloadDigest.of(new UnsafeBuffer(abc), 0, abc.length));
        final ShadowHeader h = sample(new Random(21));
        final ShadowHeader withDigest = new ShadowHeader(h.engineId(), h.clusterId(), h.kind(), h.producerKind(),
                h.producerId(), h.producerSeq(), h.genesisId(), h.logPosition(), h.leadershipTermId(),
                h.termBaseLogPosition(), h.clusterTime(), h.journalEnabled(), h.journalSeq(), h.payloadLength(),
                0xBA7816BF8F01CFEAL, h.chunkLength(), h.payloadLayoutHint());
        final UnsafeBuffer hb = encoded(withDigest);
        assertEquals("header offset 104", 0xBA7816BF8F01CFEAL, hb.getLong(104, ByteOrder.LITTLE_ENDIAN));
        assertEquals(0xBA7816BF8F01CFEAL, ShadowHeader.decode(hb, 0, 128, ShadowHeader.Options.PRODUCTION).header()
                .payloadDigest());
        final UnsafeBuffer tb = new UnsafeBuffer(new byte[ShadowTrailer.LENGTH]);
        ShadowTrailer.of(withDigest, 7).encode(tb, 0);
        assertEquals("trailer offset 24", 0xBA7816BF8F01CFEAL, tb.getLong(24, ByteOrder.LITTLE_ENDIAN));
        assertEquals(0xBA7816BF8F01CFEAL, ShadowTrailer.decode(tb, 0, 64).trailer().payloadDigest());
    }

    @Test
    public void trailerRoundTripAndRulesT1ToT5() {
        final ShadowHeader h = sample(new Random(5));
        final int headerCrc = encoded(h).getInt(ShadowHeader.CRC_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final ShadowTrailer t = ShadowTrailer.of(h, headerCrc);
        final UnsafeBuffer b = new UnsafeBuffer(new byte[ShadowTrailer.LENGTH]);
        t.encode(b, 0);
        final ShadowTrailer.Decoded d = ShadowTrailer.decode(b, 0, ShadowTrailer.LENGTH);
        assertTrue(d.ok());
        assertEquals(t, d.trailer());
        assertNull(d.trailer().mismatch(h, headerCrc));
        // T1: a flipped bit (CRC), or the wrong length
        final UnsafeBuffer flip = copy(b);
        flip.putByte(20, (byte)(flip.getByte(20) ^ 4));
        assertEquals(ShadowTrailer.Reason.NOT_A_TRAILER, ShadowTrailer.decode(flip, 0, 64).problem().reason());
        assertEquals(ShadowTrailer.Reason.NOT_A_TRAILER, ShadowTrailer.decode(b, 0, 63).problem().reason());
        // T2
        final UnsafeBuffer v2 = copy(b);
        v2.putInt(8, 2, ByteOrder.LITTLE_ENDIAN);
        ShadowTrailer.sealCrc(v2, 0);
        assertEquals(ShadowTrailer.Reason.TRAILER_VERSION_UNSUPPORTED, ShadowTrailer.decode(v2, 0, 64).problem().reason());
        // T5
        final UnsafeBuffer r = copy(b);
        r.putByte(57, (byte)1);
        ShadowTrailer.sealCrc(r, 0);
        assertEquals("TRAILER_MISMATCH(reserved)", ShadowTrailer.decode(r, 0, 64).problem().toString());
        // T3: each copied field off by one; T4: chunk count
        final List<ShadowTrailer> off = List.of(
                new ShadowTrailer(headerCrc + 1, t.payloadLength(), t.payloadDigest(), t.chunkCount(), t.logPosition(), t.producerSeq(), t.engineId()),
                new ShadowTrailer(headerCrc, t.payloadLength() + 1, t.payloadDigest(), t.chunkCount(), t.logPosition(), t.producerSeq(), t.engineId()),
                new ShadowTrailer(headerCrc, t.payloadLength(), t.payloadDigest() + 1, t.chunkCount(), t.logPosition(), t.producerSeq(), t.engineId()),
                new ShadowTrailer(headerCrc, t.payloadLength(), t.payloadDigest(), t.chunkCount() + 1, t.logPosition(), t.producerSeq(), t.engineId()),
                new ShadowTrailer(headerCrc, t.payloadLength(), t.payloadDigest(), t.chunkCount(), t.logPosition() + 1, t.producerSeq(), t.engineId()),
                new ShadowTrailer(headerCrc, t.payloadLength(), t.payloadDigest(), t.chunkCount(), t.logPosition(), t.producerSeq() + 1, t.engineId()),
                new ShadowTrailer(headerCrc, t.payloadLength(), t.payloadDigest(), t.chunkCount(), t.logPosition(), t.producerSeq(), t.engineId() + 1));
        final String[] fields = {"headerCrc32c", "payloadLength", "payloadDigest", "chunkCount", "logPosition",
                "producerSeq", "engineId"};
        for (int i = 0; i < off.size(); i++) {
            assertEquals("TRAILER_MISMATCH(" + fields[i] + ")", String.valueOf(off.get(i).mismatch(h, headerCrc)));
        }
    }

    private static UnsafeBuffer copy(final UnsafeBuffer b) {
        final UnsafeBuffer c = new UnsafeBuffer(new byte[b.capacity()]);
        c.putBytes(0, b, 0, b.capacity());
        return c;
    }
}
