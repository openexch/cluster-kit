// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The port arithmetic is a wire contract: four components derive it
 * independently, so what is pinned here is the NUMBERS, not the shape of the
 * code.
 */
public class ClusterPortsTest {

    private static final String PROP = "cluster.port.stride";

    @After
    public void tearDown() {
        System.clearProperty(PROP);
        ClusterPorts.resetStrideForTest();
    }

    /**
     * The default is the distributed layout: every member on the same ports,
     * because every member has its own address.
     */
    @Test
    public void byDefaultEveryMemberListensOnTheSamePorts() {
        assertEquals(0, ClusterPorts.stride());
        for (int nodeId = 0; nodeId < 3; nodeId++) {
            assertEquals(9002, ClusterPorts.port(nodeId, 9000, ClusterPorts.CLIENT_FACING_OFFSET));
            assertEquals(9003, ClusterPorts.port(nodeId, 9000, ClusterPorts.MEMBER_FACING_OFFSET));
        }
    }

    /**
     * The historical layout, exactly. Several members on one host still need
     * distinct ports, and this is the number that produced today's demo box.
     */
    @Test
    public void aSharedHostStrideReproducesTheOldLayout() {
        System.setProperty(PROP, Integer.toString(ClusterPorts.SHARED_HOST_STRIDE));
        ClusterPorts.resetStrideForTest();

        assertEquals(9002, ClusterPorts.port(0, 9000, ClusterPorts.CLIENT_FACING_OFFSET));
        assertEquals(9102, ClusterPorts.port(1, 9000, ClusterPorts.CLIENT_FACING_OFFSET));
        assertEquals(9202, ClusterPorts.port(2, 9000, ClusterPorts.CLIENT_FACING_OFFSET));
        assertEquals(9201, ClusterPorts.port(2, 9000, ClusterPorts.ARCHIVE_CONTROL_OFFSET));
        assertEquals(9205, ClusterPorts.port(2, 9000, ClusterPorts.TRANSFER_OFFSET));
    }

    /** The pure form takes its stride, so a test never depends on the environment. */
    @Test
    public void theStrideCanBeSuppliedDirectly() {
        assertEquals(9102, ClusterPorts.port(1, 9000, ClusterPorts.CLIENT_FACING_OFFSET, 100));
        assertEquals(9002, ClusterPorts.port(1, 9000, ClusterPorts.CLIENT_FACING_OFFSET, 0));
    }

    @Test
    public void endpointsAreHostAndPort() {
        assertEquals("node-1.match:9003",
            ClusterPorts.endpoint(1, "node-1.match", 9000, ClusterPorts.MEMBER_FACING_OFFSET));
    }

    /**
     * A stride that cannot be read is refused at startup. Falling back to 0 on a
     * shared host would mean the second member cannot bind and every client
     * dials into nothing, which is a worse way to find out.
     */
    @Test
    public void anUnreadableStrideIsRefused() {
        System.setProperty(PROP, "onehundred");
        ClusterPorts.resetStrideForTest();
        try {
            ClusterPorts.stride();
            fail("expected a refusal");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(PROP));
        }
    }

    @Test
    public void aNegativeStrideIsRefused() {
        System.setProperty(PROP, "-100");
        ClusterPorts.resetStrideForTest();
        try {
            ClusterPorts.stride();
            fail("expected a refusal");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("negative"));
        }
    }

    /** The offsets themselves are the contract; changing one silently repoints a component. */
    @Test
    public void theOffsetsAreFixed() {
        assertEquals(1, ClusterPorts.ARCHIVE_CONTROL_OFFSET);
        assertEquals(2, ClusterPorts.CLIENT_FACING_OFFSET);
        assertEquals(3, ClusterPorts.MEMBER_FACING_OFFSET);
        assertEquals(4, ClusterPorts.LOG_OFFSET);
        assertEquals(5, ClusterPorts.TRANSFER_OFFSET);
    }
}
