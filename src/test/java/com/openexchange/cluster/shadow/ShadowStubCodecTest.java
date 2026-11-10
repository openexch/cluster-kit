// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.SnapshotEnvelope;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The stub round trip and decode rules S0-S8, each refused by its rule; a stub
 * reader refuses a v5-shaped payload ({@code NOT_A_STUB}); the stub rides in the kit envelope, so a torn stub never
 * decodes.
 */
public class ShadowStubCodecTest {

    static final int CORE = 1;
    static final int CLUSTER = 7;
    static final ShadowStub STUB = new ShadowStub(CORE, CLUSTER, 0x5EEDL, 4, 1_048_576, 1_000_000, 12);

    static UnsafeBuffer encoded(final ShadowStub s) {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[ShadowStub.LENGTH]);
        assertEquals(ShadowStub.LENGTH, s.encode(b, 0));
        return b;
    }

    static void assertRefused(final ShadowStub.Decoded d, final ShadowStub.Rule rule, final String text) {
        if (d.ok()) {
            fail("expected " + rule + " (" + text + "), decoded " + d.stub());
        }
        assertEquals(d.refusal().toString(), rule, d.refusal().rule());
        assertTrue(d.refusal().toString(), d.refusal().toString().contains(rule.name()));
        assertTrue(d.refusal().toString(), d.refusal().toString().contains(text));
        assertNull(d.stub());
        System.out.println("REFUSED " + d.refusal());
    }

    @Test
    public void roundTripIsExactAndSixtyFourBytes() {
        final UnsafeBuffer b = encoded(STUB);
        final ShadowStub.Decoded d = ShadowStub.decode(b, 0, ShadowStub.LENGTH, CORE, CLUSTER);
        assertTrue(String.valueOf(d.refusal()), d.ok());
        assertEquals(STUB, d.stub());
        assertEquals(64, ShadowStub.LENGTH);
        assertEquals(84, ShadowStub.ENVELOPED_LENGTH);
        assertEquals(-100L, ShadowStub.STUB_TAG);
        assertEquals(-100L, b.getLong(0, ByteOrder.LITTLE_ENDIAN));
        assertEquals(1, b.getInt(8, ByteOrder.LITTLE_ENDIAN));
        assertEquals(0, b.getInt(20, ByteOrder.LITTLE_ENDIAN));
        assertEquals(1_048_576, b.getLong(40, ByteOrder.LITTLE_ENDIAN));
        // hints at -1 (none) are legal; genesis 0 (pre-ADOPT) is legal
        final ShadowStub noHints = new ShadowStub(CORE, CLUSTER, 0, 0, 0, -1, -1);
        assertEquals(noHints, ShadowStub.decode(encoded(noHints), 0, 64, CORE, CLUSTER).stub());
        ShadowRig.green("stub round trip: " + STUB);
    }

    @Test
    public void envelopeRoundTripAndTornStub() {
        final SnapshotEnvelope env = new SnapshotEnvelope();
        final UnsafeBuffer b = new UnsafeBuffer(new byte[ShadowStub.ENVELOPED_LENGTH]);
        assertEquals(ShadowStub.ENVELOPED_LENGTH, STUB.encodeEnveloped(env, b));
        assertTrue(ShadowStub.isStubPayload(b, SnapshotEnvelope.payloadOffset(), ShadowStub.LENGTH));
        assertEquals(STUB, ShadowStub.openEnveloped(env, b, ShadowStub.ENVELOPED_LENGTH, CORE, CLUSTER).stub());
        b.putByte(SnapshotEnvelope.payloadOffset() + 41, (byte)(b.getByte(SnapshotEnvelope.payloadOffset() + 41) ^ 1));
        try {
            ShadowStub.openEnveloped(env, b, ShadowStub.ENVELOPED_LENGTH, CORE, CLUSTER);
            fail("a torn stub must not decode");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("CRC32C mismatch"));
        }
        ShadowRig.green("enveloped stub: CRC covers it");
    }

    @Test
    public void everyRuleRefusesByName() {
        // S0
        assertRefused(ShadowStub.decode(encoded(STUB), 0, 63, CORE, CLUSTER), ShadowStub.Rule.STUB_LENGTH, "S0");
        // S1
        UnsafeBuffer b = encoded(STUB);
        b.putLong(0, -5L, ByteOrder.LITTLE_ENDIAN);
        assertRefused(ShadowStub.decode(b, 0, 64, CORE, CLUSTER), ShadowStub.Rule.NOT_A_STUB, "S1");
        // S2
        b = encoded(STUB);
        b.putInt(8, 2, ByteOrder.LITTLE_ENDIAN);
        assertRefused(ShadowStub.decode(b, 0, 64, CORE, CLUSTER), ShadowStub.Rule.STUB_VERSION_UNSUPPORTED, "S2");
        // S3
        assertRefused(ShadowStub.decode(encoded(STUB), 0, 64, 2, CLUSTER), ShadowStub.Rule.STUB_ENGINE,
                "engineId 1, host 2");
        // S4
        assertRefused(ShadowStub.decode(encoded(STUB), 0, 64, CORE, 8), ShadowStub.Rule.STUB_CLUSTER,
                "clusterId 7, host 8");
        // S5-S8: bytes the writer never writes, forged after encoding
        final int[] offsets = {40, 32, 24, 48, 56};
        final ShadowStub.Rule[] rules = {ShadowStub.Rule.STUB_POSITION, ShadowStub.Rule.STUB_TERM,
            ShadowStub.Rule.STUB_GENESIS, ShadowStub.Rule.STUB_HINT, ShadowStub.Rule.STUB_HINT};
        final String[] ids = {"S5", "S6", "S7", "S8", "S8"};
        for (int i = 0; i < offsets.length; i++) {
            b = encoded(STUB);
            b.putLong(offsets[i], -2L, ByteOrder.LITTLE_ENDIAN);
            assertRefused(ShadowStub.decode(b, 0, 64, CORE, CLUSTER), rules[i], ids[i]);
        }
        ShadowRig.green("rules S0-S8 each refused by name");
    }

    @Test
    public void theWriterRefusesWhatTheReaderRefuses() {
        final ShadowStub[] bad = {
            new ShadowStub(CORE, CLUSTER, 1, 0, -1, -1, -1),
            new ShadowStub(CORE, CLUSTER, 1, -1, 0, -1, -1),
            new ShadowStub(CORE, CLUSTER, -1, 0, 0, -1, -1),
            new ShadowStub(CORE, CLUSTER, 1, 0, 0, -2, -1),
            new ShadowStub(CORE, CLUSTER, 1, 0, 0, -1, -2)};
        final ShadowStub.Rule[] rules = {ShadowStub.Rule.STUB_POSITION, ShadowStub.Rule.STUB_TERM,
            ShadowStub.Rule.STUB_GENESIS, ShadowStub.Rule.STUB_HINT, ShadowStub.Rule.STUB_HINT};
        for (int i = 0; i < bad.length; i++) {
            try {
                bad[i].encode(new UnsafeBuffer(new byte[64]), 0);
                fail("encode accepted " + bad[i]);
            } catch (final ShadowStub.InvalidStubException e) {
                assertEquals(rules[i], e.refusal().rule());
                assertTrue(e.getMessage(), e.getMessage().contains(rules[i].id()));
            }
        }
        ShadowRig.green("one table: the writer refuses S5-S8");
    }

    @Test
    public void aStubReaderRefusesAV5Payload() {
        // a v5-shaped payload: the layout tag -5 first, as BalanceSnapshotCodec writes it
        final UnsafeBuffer v5 = new UnsafeBuffer(new byte[4096]);
        v5.putLong(0, -5L);
        assertFalse(ShadowStub.isStubPayload(v5, 0, 4096));
        assertRefused(ShadowStub.decode(v5, 0, 64, CORE, CLUSTER), ShadowStub.Rule.NOT_A_STUB, "tag -5");
        // a real-length v5 image is refused by S0 first (the rules are evaluated in order)
        assertRefused(ShadowStub.decode(v5, 0, 4096, CORE, CLUSTER), ShadowStub.Rule.STUB_LENGTH, "length 4096");
        assertFalse(ShadowStub.isStubPayload(v5, 0, 7));
        ShadowRig.green("v5 payload is not a stub");
    }
}
