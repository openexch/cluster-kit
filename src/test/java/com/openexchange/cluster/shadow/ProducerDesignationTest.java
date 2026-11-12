// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The designated follower: the member after the leader in ascending id order, wrapping; never the leader. */
public class ProducerDesignationTest {

    @Test
    public void nextIdAfterTheLeaderWrapping() {
        final ProducerDesignation d = new ProducerDesignation(2, 0, 1);
        assertEquals(1, d.designated(0));
        assertEquals(2, d.designated(1));
        assertEquals("wraps to the smallest", 0, d.designated(2));
        for (final int leader : new int[] {0, 1, 2}) {
            assertFalse("never the leader", d.isDesignated(leader, leader));
        }
        assertTrue(d.isDesignated(1, 0));
        assertFalse(d.isDesignated(2, 0));
        assertArrayEquals(new int[] {0, 1, 2}, d.memberIds());
    }

    @Test
    public void sparseIdsTwoMembersAndOneMember() {
        final ProducerDesignation sparse = new ProducerDesignation(5, 9, 7);
        assertEquals(7, sparse.designated(5));
        assertEquals(9, sparse.designated(7));
        assertEquals(5, sparse.designated(9));
        assertEquals("a two-member cluster designates its only follower", 1, new ProducerDesignation(0, 1).designated(0));
        assertEquals(0, new ProducerDesignation(0, 1).designated(1));
        assertEquals("a one-member cluster designates nobody", ProducerDesignation.NONE,
                new ProducerDesignation(0).designated(0));
    }

    @Test
    public void unknownOrForeignLeaderDesignatesNobody() {
        final ProducerDesignation d = new ProducerDesignation(0, 1, 2);
        assertEquals(ProducerDesignation.NONE, d.designated(-1));
        assertEquals(ProducerDesignation.NONE, d.designated(3));
        assertFalse(d.isDesignated(0, -1));
    }

    @Test
    public void fromTheClusterMembersStringWithArchiveEndpoints() {
        final ProducerDesignation d = ProducerDesignation.ofClusterMembers(
                "0,h0:1,h0:2,h0:3,h0:4,core0:9302|1,h1:1,h1:2,h1:3,h1:4,10.0.0.5:9302|"
                        + "2,h2:1,h2:2,h2:3,h2:4,core2:9302");
        assertEquals(2, d.designated(1));
        assertEquals("10.0.0.5:9302", d.archiveEndpoint(1));
        assertEquals("core0:9302", d.archiveEndpoint(0));
        assertNull(d.archiveEndpoint(7));
    }

    @Test
    public void duplicateAndNegativeIdsAreRefused() {
        try {
            new ProducerDesignation(0, 1, 1);
            fail("duplicate accepted");
        } catch (final IllegalArgumentException e) {
            assertEquals("member id 1 is listed twice", e.getMessage());
        }
        try {
            new ProducerDesignation(-2, 1);
            fail("negative accepted");
        } catch (final IllegalArgumentException e) {
            assertEquals("member id must be >= 0: -2", e.getMessage());
        }
    }
}
