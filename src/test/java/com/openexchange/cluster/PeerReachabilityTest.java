// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PeerReachabilityTest {
    private static final String CONSENSUS = "aeron:udp?term-length=16m|mtu=1408";

    @Test
    public void theProbeRunsOnItsOwnSmallSparseTermNotTheConsensusTerm() {
        final ChannelUri uri = ChannelUri.parse(PeerReachability.probeChannel(CONSENSUS, "10.0.0.2:20111",
                PeerReachability.DEFAULT_PROBE_TERM_LENGTH));

        assertEquals("10.0.0.2:20111", uri.get(CommonContext.ENDPOINT_PARAM_NAME));
        assertEquals("65536", uri.get(CommonContext.TERM_LENGTH_PARAM_NAME));
        assertEquals("true", uri.get(CommonContext.SPARSE_PARAM_NAME));
        assertEquals("the rest of the consensus channel is kept", "1408", uri.get(CommonContext.MTU_LENGTH_PARAM_NAME));
    }

    @Test
    public void theProbeTermLengthIsASettingWithA64KibDefault() {
        assertEquals(64 * 1024, PeerReachability.probeTermLength(null, null));
        assertEquals(1 << 20, PeerReachability.probeTermLength("1048576", null));
        assertEquals(128 * 1024, PeerReachability.probeTermLength(null, "131072"));
    }

    @Test
    public void aProbeTermLengthTheDriverWouldRefuseRefusesTheBoot() {
        final IllegalArgumentException notPowerOfTwo = assertThrows(IllegalArgumentException.class,
                () -> PeerReachability.probeTermLength("100000", null));
        assertTrue(notPowerOfTwo.getMessage(), notPowerOfTwo.getMessage().contains(PeerReachability.PROBE_TERM_LENGTH_ENV));
        assertThrows(IllegalArgumentException.class, () -> PeerReachability.probeTermLength("32768", null));
        assertThrows(IllegalArgumentException.class, () -> PeerReachability.probeTermLength("64k", null));
    }
}
