// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * What the authenticator mints for an accepted session and what every member reads from the SessionOpen log
 * event or a restored container session: the session class, its user scope and an identity for logs.
 * {@code [u8 version=1][u8 class][u8 flags][u8 reserved][i32 identity LE][u16 userCount LE][i64 userId LE x userCount]},
 * at most 4096 bytes (Aeron's principal limit), so at most 510 ids. These are the RC1 S3 bytes; they ride the
 * replicated log and the AE snapshot, so they never change.
 *
 * <p>Encode writes reserved 0 and flags 1 (ANY user, count 0) or 0 (1..510 ascending unique ids). Decode keeps
 * RC1's leniency exactly: it ignores the reserved byte and flag bits 1-7 and checks version, class 1..7, the
 * length, ANY-with-count-0 / scoped-with-count-above-0 and ascending unique ids; anything else is
 * {@link #NONE}. It never throws.</p>
 */
public final class ClusterPrincipal {

    public static final int VERSION = 1;
    public static final int FLAG_USER_SCOPE_ANY = 1;
    public static final int MAX_USERS = 510;
    public static final int FIXED_LENGTH = 1 + 1 + 1 + 1 + 4 + 2;

    /** The principal of a session whose bytes were missing or malformed: authorises nothing. */
    public static final ClusterPrincipal NONE = new ClusterPrincipal(ClusterSessionClass.UNSET, 0, 0, new long[0]);

    private final ClusterSessionClass sessionClass;
    private final int flags;
    private final int identity;
    private final long[] userIds; // ascending, unique

    private ClusterPrincipal(final ClusterSessionClass sessionClass, final int flags, final int identity,
                             final long[] userIds) {
        this.sessionClass = sessionClass;
        this.flags = flags;
        this.identity = identity;
        this.userIds = userIds;
    }

    /** A principal that may act for ANY user. */
    public static ClusterPrincipal anyUser(final ClusterSessionClass sessionClass, final int identity) {
        requireClass(sessionClass);
        return new ClusterPrincipal(sessionClass, FLAG_USER_SCOPE_ANY, identity, new long[0]);
    }

    /** A principal bound to an explicit user set (sorted; duplicates refused). */
    public static ClusterPrincipal forUsers(final ClusterSessionClass sessionClass, final int identity,
                                            final long... users) {
        requireClass(sessionClass);
        if (users.length == 0 || users.length > MAX_USERS) {
            throw new IllegalArgumentException("user scope needs 1.." + MAX_USERS + " ids, got " + users.length);
        }
        final long[] sorted = users.clone();
        Arrays.sort(sorted);
        for (int i = 1; i < sorted.length; i++) {
            if (sorted[i] == sorted[i - 1]) {
                throw new IllegalArgumentException("duplicate userId " + sorted[i]);
            }
        }
        return new ClusterPrincipal(sessionClass, 0, identity, sorted);
    }

    private static void requireClass(final ClusterSessionClass sessionClass) {
        if (sessionClass == null || sessionClass == ClusterSessionClass.UNSET) {
            throw new IllegalArgumentException("a principal needs a real session class, got " + sessionClass);
        }
    }

    public byte[] encode() {
        final ByteBuffer b = ByteBuffer.allocate(FIXED_LENGTH + 8 * userIds.length).order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) VERSION).put((byte) sessionClass.code()).put((byte) flags).put((byte) 0)
                .putInt(identity).putShort((short) userIds.length);
        for (final long u : userIds) {
            b.putLong(u);
        }
        return b.array();
    }

    /** Never throws: anything this build cannot read is {@link #NONE}. */
    public static ClusterPrincipal decode(final byte[] bytes) {
        if (classCodeOf(bytes) == 0) {
            return NONE;
        }
        final ClusterSessionClass c = ClusterSessionClass.of(bytes[1] & 0xFF);
        final int flags = bytes[2] & 0xFF;
        final int identity = intLe(bytes, 4);
        final int count = (bytes[8] & 0xFF) | (bytes[9] & 0xFF) << 8;
        final long[] users = new long[count];
        for (int i = 0; i < count; i++) {
            users[i] = longLe(bytes, FIXED_LENGTH + 8 * i);
        }
        return new ClusterPrincipal(c, flags, identity, users);
    }

    /**
     * The class code {@link #decode} would give, read in place with no allocation (the ME hot path); 0 for
     * null, foreign or malformed bytes.
     */
    public static int classCodeOf(final byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_LENGTH) {
            return 0;
        }
        if ((bytes[0] & 0xFF) != VERSION) {
            return 0;
        }
        final int code = bytes[1] & 0xFF;
        if (code < 1 || code > ClusterSessionClass.MAX_CODE) {
            return 0;
        }
        final int flags = bytes[2] & 0xFF;
        final int count = (bytes[8] & 0xFF) | (bytes[9] & 0xFF) << 8;
        if (count > MAX_USERS || bytes.length != FIXED_LENGTH + 8L * count) {
            return 0;
        }
        final boolean any = (flags & FLAG_USER_SCOPE_ANY) != 0;
        if (any ? count != 0 : count == 0) {
            return 0;
        }
        long prior = 0;
        for (int i = 0; i < count; i++) {
            final long u = longLe(bytes, FIXED_LENGTH + 8 * i);
            if (i > 0 && u <= prior) {
                return 0; // not ascending-unique: foreign bytes
            }
            prior = u;
        }
        return code;
    }

    private static int intLe(final byte[] b, final int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8 | (b[at + 2] & 0xFF) << 16 | (b[at + 3] & 0xFF) << 24;
    }

    private static long longLe(final byte[] b, final int at) {
        return (intLe(b, at) & 0xFFFF_FFFFL) | ((long) intLe(b, at + 4)) << 32;
    }

    public ClusterSessionClass sessionClass() {
        return sessionClass;
    }

    public int classCode() {
        return sessionClass.code();
    }

    public int flags() {
        return flags;
    }

    public int identity() {
        return identity;
    }

    public boolean anyUser() {
        return (flags & FLAG_USER_SCOPE_ANY) != 0;
    }

    /** Explicit scope (empty when ANY). Copy. */
    public long[] userIds() {
        return userIds.clone();
    }

    public boolean allowsUser(final long userId) {
        if (sessionClass == ClusterSessionClass.UNSET) {
            return false;
        }
        return anyUser() || Arrays.binarySearch(userIds, userId) >= 0;
    }

    @Override
    public String toString() {
        return "ClusterPrincipal{" + sessionClass + " identity=" + identity
                + (anyUser() ? " users=ANY" : " users=" + Arrays.toString(userIds)) + "}";
    }
}
