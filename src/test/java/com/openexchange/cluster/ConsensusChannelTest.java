// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.logbuffer.FrameDescriptor;
import org.junit.After;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The consensus channel's term: a default small enough for its messages, and one rule for every bad value. */
public class ConsensusChannelTest {

    @After
    public void tearDown() {
        System.clearProperty(ConsensusChannel.PROPERTY);
    }

    private static ConsensusChannel parse(final String value) {
        return ConsensusChannel.parse(Map.of(ConsensusChannel.SETTING, value)::get);
    }

    @Test
    public void unsetIsTheOneMegabyteDefault() {
        final ConsensusChannel c = ConsensusChannel.parse(n -> null);
        assertEquals("1m", c.termLengthText());
        assertEquals(1024 * 1024, c.termLength());
        assertTrue(c.derived());
        assertEquals("aeron:udp?term-length=1m", c.uri());
        assertEquals("consensus term 1m default", c.line());
        assertTrue("blank is unset", parse("  ").derived());
    }

    @Test
    public void aSetValueIsTakenAsGiven() {
        final ConsensusChannel c = parse(" 64k ");
        assertEquals(64 * 1024, c.termLength());
        assertFalse(c.derived());
        assertEquals("aeron:udp?term-length=64k", c.uri());
        assertEquals("consensus term 64k set", c.line());
        assertEquals(16L * 1024 * 1024, parse("16m").termLength());
        assertEquals(1L << 30, parse("1g").termLength());
    }

    @Test
    public void badValuesAreRefusedWithTheSettingNamed() {
        refused("sixteen", "TRANSPORT_CONSENSUS_TERM_LENGTH 'sixteen' is not a size");
        refused("32k", "TRANSPORT_CONSENSUS_TERM_LENGTH 32k outside [64k, 1g]");
        refused("2g", "outside [64k, 1g]");
        refused("3m", "TRANSPORT_CONSENSUS_TERM_LENGTH 3m is not a power of two");
        refused("-1m", "TRANSPORT_CONSENSUS_TERM_LENGTH");
    }

    /**
     * The largest message a member publishes on the consensus channel fits the smallest legal term with room to
     * spare, so the 64k floor (Aeron's own) is the only floor needed. The number is pinned so a codec change that
     * grows it is seen here.
     */
    @Test
    public void theLargestConsensusMessageFitsTheSmallestTerm() {
        // header 8 + CatchupPosition block 20 + var-data length 4 + a 259-character endpoint
        assertEquals(291, ConsensusChannel.LARGEST_MEMBER_MESSAGE_BYTES);
        assertEquals(8 * 1024, FrameDescriptor.computeMaxMessageLength(64 * 1024));
        assertEquals(128 * 1024, FrameDescriptor.computeMaxMessageLength(1024 * 1024));
        assertTrue(FrameDescriptor.computeMaxMessageLength(64 * 1024) > ConsensusChannel.LARGEST_MEMBER_MESSAGE_BYTES);
    }

    @Test
    public void theTermIsAppliedToAChannelReplacingAnyOther() {
        final ConsensusChannel c = parse("4m");
        assertEquals("aeron:udp?endpoint=10.0.0.1:9303|term-length=4m",
                c.applyTo("aeron:udp?endpoint=10.0.0.1:9303"));
        assertEquals("aeron:udp?term-length=4m", c.applyTo("aeron:udp?term-length=16m"));
    }

    @Test
    public void theSystemPropertyIsReadWhenTheEnvironmentIsSilent() {
        System.setProperty(ConsensusChannel.PROPERTY, "2m");
        assertEquals(2L * 1024 * 1024, ConsensusChannel.fromEnvironment().termLength());
        System.setProperty(ConsensusChannel.PROPERTY, "nope");
        try {
            ConsensusChannel.fromEnvironment();
            fail("expected a refusal");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("'nope' is not a size"));
        }
    }

    private static void refused(final String value, final String text) {
        try {
            parse(value);
            fail("expected a refusal for " + value);
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(text));
        }
    }
}
