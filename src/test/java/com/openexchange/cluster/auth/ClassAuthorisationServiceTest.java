// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import org.junit.Test;

import static com.openexchange.cluster.auth.ClassAuthorisationService.ADMIN_REQUEST;
import static com.openexchange.cluster.auth.ClassAuthorisationService.BACKUP_QUERY;
import static com.openexchange.cluster.auth.ClassAuthorisationService.CLUSTER_PROTOCOL_ID;
import static com.openexchange.cluster.auth.ClassAuthorisationService.HEARTBEAT_REQUEST;
import static com.openexchange.cluster.auth.ClassAuthorisationService.STANDBY_SNAPSHOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** RC2 S6 9.1: the class-table authorisation service with the ME table (REPLICA -> BackupQuery only, A14). */
public class ClassAuthorisationServiceTest {

    private static final ClassAuthorisationService ME_TABLE = ClassAuthorisationService.builder()
            .allow(ClusterSessionClass.REPLICA, CLUSTER_PROTOCOL_ID, BACKUP_QUERY)
            .build();

    private static byte[] any(final ClusterSessionClass c) {
        return ClusterPrincipal.anyUser(c, 1).encode();
    }

    @Test
    public void theAeronIdsAreTheOnesTheConsensusModuleAsksFor() {
        assertEquals(io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID, CLUSTER_PROTOCOL_ID);
        assertEquals(77, BACKUP_QUERY);
        assertEquals(79, HEARTBEAT_REQUEST);
        assertEquals(81, STANDBY_SNAPSHOT);
        assertEquals(26, ADMIN_REQUEST);
    }

    @Test
    public void replicaMayQueryBackupsAndNothingElse() {
        final ClassAuthorisationService s = ClassAuthorisationService.builder()
                .allow(ClusterSessionClass.REPLICA, CLUSTER_PROTOCOL_ID, BACKUP_QUERY).build();
        assertTrue(s.isAuthorised(CLUSTER_PROTOCOL_ID, BACKUP_QUERY, null, any(ClusterSessionClass.REPLICA)));
        assertFalse(s.isAuthorised(CLUSTER_PROTOCOL_ID, HEARTBEAT_REQUEST, null, any(ClusterSessionClass.REPLICA)));
        assertFalse(s.isAuthorised(CLUSTER_PROTOCOL_ID, STANDBY_SNAPSHOT, null, any(ClusterSessionClass.REPLICA)));
        assertFalse(s.isAuthorised(CLUSTER_PROTOCOL_ID, ADMIN_REQUEST, io.aeron.cluster.codecs.AdminRequestType.SNAPSHOT,
                any(ClusterSessionClass.REPLICA)));
        assertFalse("another protocol with the same action id", s.isAuthorised(901, BACKUP_QUERY, null,
                any(ClusterSessionClass.REPLICA)));
        assertEquals(1, s.grantedCount());
        assertEquals(4, s.deniedCount());
        assertEquals(1, s.deniedCount(HEARTBEAT_REQUEST));
        assertEquals(0, s.deniedCount(BACKUP_QUERY));
    }

    @Test
    public void everyOtherClassAndNoneAreDeniedBackupQuery() {
        for (ClusterSessionClass c : ClusterSessionClass.values()) {
            if (c == ClusterSessionClass.UNSET || c == ClusterSessionClass.REPLICA) {
                continue;
            }
            for (int action : new int[] {BACKUP_QUERY, HEARTBEAT_REQUEST, STANDBY_SNAPSHOT, ADMIN_REQUEST}) {
                assertFalse(c + " action " + action, ME_TABLE.isAuthorised(CLUSTER_PROTOCOL_ID, action, null, any(c)));
            }
        }
        assertFalse(ME_TABLE.isAuthorised(CLUSTER_PROTOCOL_ID, BACKUP_QUERY, null, null));
        assertFalse(ME_TABLE.isAuthorised(CLUSTER_PROTOCOL_ID, BACKUP_QUERY, null, new byte[0]));
        final byte[] malformedReplica = any(ClusterSessionClass.REPLICA);
        malformedReplica[0] = 9; // version 9: NONE
        assertFalse(ME_TABLE.isAuthorised(CLUSTER_PROTOCOL_ID, BACKUP_QUERY, null, malformedReplica));
        assertTrue(ME_TABLE.allows(ClusterSessionClass.REPLICA, CLUSTER_PROTOCOL_ID, BACKUP_QUERY));
        assertFalse(ME_TABLE.allows(ClusterSessionClass.UNSET, CLUSTER_PROTOCOL_ID, BACKUP_QUERY));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unsetCannotBeGranted() {
        ClassAuthorisationService.builder().allow(ClusterSessionClass.UNSET, CLUSTER_PROTOCOL_ID, BACKUP_QUERY);
    }
}
