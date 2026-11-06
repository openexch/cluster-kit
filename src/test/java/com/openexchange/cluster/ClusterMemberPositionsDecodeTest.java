// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.codecs.ClusterMembersExtendedResponseEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * The consensus module's ClusterMembersExtendedResponse carries five variable-length endpoint
 * strings per member. Until 2026-10-25 the decode loop read the fixed fields and moved on, so every
 * member after the first was decoded out of the previous member's endpoint bytes (ids like
 * 1818321775, positions around 3.5e18) and the retention report reasoned about nobody real. This pins
 * a three-member response decoding to the three members that were encoded.
 */
public class ClusterMemberPositionsDecodeTest {

    @Test
    public void threeMembersWithEndpointsDecodeAsThreeMembers() throws Exception {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final ClusterMembersExtendedResponseEncoder enc = new ClusterMembersExtendedResponseEncoder();
        enc.wrapAndApplyHeader(buffer, 0, header)
            .correlationId(77L).currentTimeNs(1_000_050L).leaderMemberId(0).memberId(0);
        final long[] positions = {512L, 480L, 496L};
        final long[] lastAppend = {1_000_000L, 999_000L, 998_000L};
        final ClusterMembersExtendedResponseEncoder.ActiveMembersEncoder group = enc.activeMembersCount(3);
        for (int i = 0; i < 3; i++) {
            group.next().leadershipTermId(3L).logPosition(positions[i]).timeOfLastAppendNs(lastAppend[i]).memberId(i)
                .ingressEndpoint("localhost:2" + i + "002").consensusEndpoint("localhost:2" + i + "003")
                .logEndpoint("localhost:2" + i + "004").catchupEndpoint("localhost:2" + i + "005")
                .archiveEndpoint("localhost:2" + i + "001");
        }
        enc.passiveMembersCount(0);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();

        final ClusterMemberPositions positionsClient = new ClusterMemberPositions(null, "aeron:ipc", 0, 0);
        final Field awaited = ClusterMemberPositions.class.getDeclaredField("awaitedCorrelationId");
        awaited.setAccessible(true);
        awaited.setLong(positionsClient, 77L);
        final Method onFragment = ClusterMemberPositions.class.getDeclaredMethod("onFragment",
            org.agrona.DirectBuffer.class, int.class, int.class, io.aeron.logbuffer.Header.class);
        onFragment.setAccessible(true);
        onFragment.invoke(positionsClient, buffer, 0, length, null);
        final Field receivedField = ClusterMemberPositions.class.getDeclaredField("received");
        receivedField.setAccessible(true);
        final ClusterMemberPositions.Snapshot snap = (ClusterMemberPositions.Snapshot) receivedField.get(positionsClient);
        assertNotNull("response decoded", snap);
        assertEquals(1_000_050L, snap.currentTimeNs);
        assertEquals(0, snap.leaderMemberId);
        assertEquals(3, snap.members.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("member id " + i, i, snap.members.get(i).memberId);
            assertEquals("position " + i, positions[i], snap.members.get(i).logPosition);
            assertEquals("last append " + i, lastAppend[i], snap.members.get(i).timeOfLastAppendNs);
        }
    }
}
