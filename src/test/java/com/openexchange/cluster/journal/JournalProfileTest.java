// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** The key of a record as a profile reads it from one message, and every message that is not a record. */
public class JournalProfileTest {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);

    private JournalProfile.Key key(final JournalProfile p, final int length) throws Exception {
        return p.key(buffer, 0, length, length + 32);
    }

    private String notARecord(final JournalProfile p, final int length, final int frameLength) {
        return assertThrows(JournalProfile.NotARecord.class, () -> p.key(buffer, 0, length, frameLength)).getMessage();
    }

    @Test
    public void anOrderRecordHasSequenceEpochAndLogPositionAtFixedOffsets() throws Exception {
        final int length = JournalRig.encode(JournalRig.ORDER, buffer, 41, 17, 9_000, 5);
        assertEquals(216, length);
        final JournalProfile.Key k = key(JournalRig.ORDER, length);
        assertEquals(41, k.journalSeq());
        assertEquals(17, k.epoch());
        assertEquals(9_000, k.logPosition());
        assertEquals(1, k.templateId());
    }

    @Test
    public void aMoneyRecordHasItsLogPositionAtTheEndOfItsBlockFromVersion2() throws Exception {
        for (final long seq : new long[] {3, 4, 5}) { // settle, deposit, opening balance
            final int length = JournalRig.encode(JournalRig.MONEY, buffer, seq, 0, 7_000 + seq, 3);
            final JournalProfile.Key k = key(JournalRig.MONEY, length);
            assertEquals(seq, k.journalSeq());
            assertEquals(0, k.epoch());
            assertEquals(7_000 + seq, k.logPosition());
        }
        // a version 1 record ends before the log position: its key has none
        final int v2 = JournalRig.encode(JournalRig.MONEY, buffer, 4, 0, 7_004, 3);
        buffer.putShort(0, (short) 44, ByteOrder.LITTLE_ENDIAN); // the version 1 block of a deposit
        buffer.putShort(6, (short) 1, ByteOrder.LITTLE_ENDIAN);
        final JournalProfile.Key v1 = key(JournalRig.MONEY, v2 - 8);
        assertEquals(4, v1.journalSeq());
        assertEquals(JournalProfile.NO_LOG_POSITION, v1.logPosition());
        // a template the profile does not list has none either
        JournalRig.encode(JournalRig.MONEY, buffer, 4, 0, 7_004, 3);
        buffer.putShort(2, (short) 9, ByteOrder.LITTLE_ENDIAN);
        assertEquals(JournalProfile.NO_LOG_POSITION, key(JournalRig.MONEY, v2).logPosition());
    }

    @Test
    public void whatIsNotARecord() {
        final int length = JournalRig.encode(JournalRig.ORDER, buffer, 41, 17, 9_000, 9);
        assertTrue(notARecord(JournalRig.ORDER, length, length + 32).contains("schema id 9, the journal's is 5"));
        JournalRig.encode(JournalRig.ORDER, buffer, 41, 17, 9_000, 5);
        assertTrue(notARecord(JournalRig.ORDER, length, length + 64).contains("not one whole frame"));
        assertTrue(notARecord(JournalRig.ORDER, 4, 36).contains("no SBE header"));
        assertTrue(notARecord(JournalRig.ORDER, 12, 44).contains("ends before journalSeq"));
        assertTrue(notARecord(JournalRig.ORDER, 20, 52).contains("ends before the epoch"));
        assertTrue(notARecord(JournalRig.MONEY, length, length + 32).contains("larger than the journal's largest"));
    }

    @Test
    public void aProfileRefusesOffsetsInsideTheHeader() {
        assertThrows(IllegalArgumentException.class, () -> new JournalProfile("order", 1, "aeron:ipc", 5, 0, 16,
            Map.of(), 1, 256));
        assertThrows(IllegalArgumentException.class, () -> new JournalProfile("order", 1, "aeron:ipc", 5, 8, 4,
            Map.of(), 1, 256));
        assertThrows(IllegalArgumentException.class, () -> new JournalProfile("order", 1, "aeron:ipc", 5, 8, 16,
            Map.of(1, 2), 1, 256));
        assertThrows(IllegalArgumentException.class, () -> new JournalProfile("order", 1, "aeron:ipc", 5, 8, 16,
            Map.of(), 1, 100));
        assertThrows(IllegalArgumentException.class, () -> new JournalProfile("or der", 1, "aeron:ipc", 5, 8, 16,
            Map.of(), 1, 256));
        assertThrows(IllegalArgumentException.class, () -> new JournalProfile("order", 1, "aeron:ipc", 0, 8, 16,
            Map.of(), 1, 256));
    }
}
