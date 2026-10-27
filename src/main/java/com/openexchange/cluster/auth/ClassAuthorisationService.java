// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import io.aeron.security.AuthorisationService;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The consensus module's authorisation service by session class: {@code isAuthorised(protocolId, actionId,
 * type, principal)} reads the class code from the principal in place ({@link ClusterPrincipal#classCodeOf})
 * and looks the (protocolId, actionId) pair up in an immutable per-class table supplied by the engine. An
 * unknown class (NONE included) or an unlisted pair is denied. The {@code type} argument (e.g. the admin
 * request type) is not consulted: a listed pair allows every type.
 */
public final class ClassAuthorisationService implements AuthorisationService {

    /** Aeron cluster codec schema id (BackupQuery, HeartbeatRequest, StandbySnapshot, AdminRequest). */
    public static final int CLUSTER_PROTOCOL_ID = io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID;
    public static final int BACKUP_QUERY = io.aeron.cluster.codecs.BackupQueryDecoder.TEMPLATE_ID;
    public static final int HEARTBEAT_REQUEST = io.aeron.cluster.codecs.HeartbeatRequestDecoder.TEMPLATE_ID;
    public static final int STANDBY_SNAPSHOT = io.aeron.cluster.codecs.StandbySnapshotDecoder.TEMPLATE_ID;
    public static final int ADMIN_REQUEST = io.aeron.cluster.codecs.AdminRequestDecoder.TEMPLATE_ID;

    private static final int ACTION_SLOTS = 128;

    /** Per class code 0..7: sorted (protocolId << 32 | actionId) keys. */
    private final long[][] allowed;
    private final AtomicLong denied = new AtomicLong();
    private final AtomicLong granted = new AtomicLong();
    private final AtomicLongArray deniedByAction = new AtomicLongArray(ACTION_SLOTS);

    private ClassAuthorisationService(final long[][] allowed) {
        this.allowed = allowed;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Collects (class, protocolId, actionId) rows; {@link #build} freezes them. */
    public static final class Builder {
        private final long[][] rows = new long[ClusterSessionClass.MAX_CODE + 1][0];

        private Builder() {
        }

        public Builder allow(final ClusterSessionClass c, final int protocolId, final int actionId) {
            if (c == null || c == ClusterSessionClass.UNSET) {
                throw new IllegalArgumentException("UNSET cannot be granted an action");
            }
            final long[] old = rows[c.code()];
            final long[] grown = Arrays.copyOf(old, old.length + 1);
            grown[old.length] = key(protocolId, actionId);
            rows[c.code()] = grown;
            return this;
        }

        public ClassAuthorisationService build() {
            final long[][] frozen = new long[rows.length][];
            for (int i = 0; i < rows.length; i++) {
                frozen[i] = rows[i].clone();
                Arrays.sort(frozen[i]);
            }
            return new ClassAuthorisationService(frozen);
        }
    }

    private static long key(final int protocolId, final int actionId) {
        return ((long) protocolId << 32) | (actionId & 0xFFFF_FFFFL);
    }

    @Override
    public boolean isAuthorised(final int protocolId, final int actionId, final Object type,
                                final byte[] encodedPrincipal) {
        final int code = ClusterPrincipal.classCodeOf(encodedPrincipal);
        final boolean ok = code != 0 && Arrays.binarySearch(allowed[code], key(protocolId, actionId)) >= 0;
        if (ok) {
            granted.incrementAndGet();
        } else {
            denied.incrementAndGet();
            if (protocolId == CLUSTER_PROTOCOL_ID && actionId >= 0 && actionId < ACTION_SLOTS) {
                deniedByAction.incrementAndGet(actionId);
            }
        }
        return ok;
    }

    /** Is the pair in the class's row (no counting)? */
    public boolean allows(final ClusterSessionClass c, final int protocolId, final int actionId) {
        return c != ClusterSessionClass.UNSET && Arrays.binarySearch(allowed[c.code()], key(protocolId, actionId)) >= 0;
    }

    public long deniedCount() {
        return denied.get();
    }

    public long grantedCount() {
        return granted.get();
    }

    /** Denials of one Aeron cluster action (e.g. {@link #BACKUP_QUERY}); 0 for ids outside 0..127. */
    public long deniedCount(final int clusterActionId) {
        return clusterActionId >= 0 && clusterActionId < ACTION_SLOTS ? deniedByAction.get(clusterActionId) : 0;
    }
}
