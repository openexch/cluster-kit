// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC2 S6 9.1: the principal every member reads from SessionOpen; decode leniency and the hot-path class read. */
public class ClusterPrincipalTest {

    private static byte[] p(final int... b) {
        final byte[] r = new byte[b.length];
        for (int i = 0; i < b.length; i++) {
            r[i] = (byte) b[i];
        }
        return r;
    }

    @Test
    public void anyUserRoundTripsAndAllowsEveryone() {
        final ClusterPrincipal any = ClusterPrincipal.anyUser(ClusterSessionClass.GATEWAY, 42);
        assertArrayEquals(p(1, 1, 1, 0, 42, 0, 0, 0, 0, 0), any.encode());
        final ClusterPrincipal d = ClusterPrincipal.decode(any.encode());
        assertEquals(ClusterSessionClass.GATEWAY, d.sessionClass());
        assertEquals(42, d.identity());
        assertTrue(d.anyUser());
        assertTrue(d.allowsUser(1) && d.allowsUser(Long.MAX_VALUE));
    }

    @Test
    public void scopedPrincipalSortsAndRefusesDuplicates() {
        final ClusterPrincipal s = ClusterPrincipal.forUsers(ClusterSessionClass.TREASURY, 7, 30, 10, 20);
        final ClusterPrincipal d = ClusterPrincipal.decode(s.encode());
        assertArrayEquals(new long[] {10, 20, 30}, d.userIds());
        assertTrue(d.allowsUser(20));
        assertFalse(d.allowsUser(25));
        assertFalse(d.anyUser());
        assertEquals(0, d.flags());
        assertThrows(IllegalArgumentException.class, () -> ClusterPrincipal.forUsers(ClusterSessionClass.TREASURY, 7, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> ClusterPrincipal.forUsers(ClusterSessionClass.TREASURY, 7));
        assertThrows(IllegalArgumentException.class,
                () -> ClusterPrincipal.forUsers(ClusterSessionClass.TREASURY, 7, new long[ClusterPrincipal.MAX_USERS + 1]));
        assertThrows(IllegalArgumentException.class, () -> ClusterPrincipal.anyUser(ClusterSessionClass.UNSET, 1));
    }

    @Test
    public void decodeIgnoresTheReservedByteAndFlagBitsOneToSeven() {
        final ClusterPrincipal reserved = ClusterPrincipal.decode(p(1, 4, 1, 0x7f, 1, 0, 0, 0, 0, 0));
        assertEquals(ClusterSessionClass.ADMIN, reserved.sessionClass());
        final ClusterPrincipal flags3 = ClusterPrincipal.decode(p(1, 4, 0x03, 0, 1, 0, 0, 0, 0, 0));
        assertEquals(ClusterSessionClass.ADMIN, flags3.sessionClass());
        assertTrue(flags3.anyUser());
        assertEquals(3, flags3.flags());
        final ClusterPrincipal scoped2 = ClusterPrincipal.decode(p(1, 6, 0x02, 0x55, 9, 0, 0, 0, 1, 0, 5, 0, 0, 0, 0, 0, 0, 0));
        assertEquals(ClusterSessionClass.TREASURY, scoped2.sessionClass());
        assertArrayEquals(new long[] {5}, scoped2.userIds());
        assertSame("flags 0x02 with count 0 has no scope", ClusterPrincipal.NONE,
                ClusterPrincipal.decode(p(1, 4, 0x02, 0, 1, 0, 0, 0, 0, 0)));
    }

    @Test
    public void foreignBytesDecodeToNone() {
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(null));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(new byte[0]));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(p(9, 1, 1, 0, 0, 0, 0, 0, 0, 0)));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(p(1, 0, 1, 0, 0, 0, 0, 0, 0, 0)));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(p(1, 8, 1, 0, 0, 0, 0, 0, 0, 0)));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(p(1, 1, 0, 0, 0, 0, 0, 0, 0, 0)));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(p(1, 6, 1, 0, 9, 0, 0, 0, 1, 0, 5, 0, 0, 0, 0, 0, 0, 0)));
        final byte[] scoped = ClusterPrincipal.forUsers(ClusterSessionClass.TOOL, 1, 5, 6).encode();
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(Arrays.copyOf(scoped, 20)));
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(Arrays.copyOf(scoped, 27)));
        final byte[] unsorted = scoped.clone();
        unsorted[10] = 9;
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(unsorted));
        final byte[] dup = scoped.clone();
        dup[18] = 5;
        assertSame(ClusterPrincipal.NONE, ClusterPrincipal.decode(dup));
        assertFalse(ClusterPrincipal.NONE.allowsUser(1));
        assertEquals(0, ClusterPrincipal.NONE.classCode());
        assertEquals(ClusterSessionClass.REPLICA,
                ClusterPrincipal.decode(ClusterPrincipal.anyUser(ClusterSessionClass.REPLICA, 3).encode()).sessionClass());
    }

    @Test
    public void classCodeOfEqualsDecodeOnAFuzzCorpus() {
        assertEquals(0, ClusterPrincipal.classCodeOf(null));
        final SplittableRandom r = new SplittableRandom(0x5E6L);
        int nonZero = 0;
        for (int i = 0; i < 200_000; i++) {
            final byte[] b;
            if (i % 3 == 0) {
                // valid-ish: start from a real principal and flip one byte
                final int count = r.nextInt(4);
                final long[] users = new long[count];
                for (int u = 0; u < count; u++) {
                    users[u] = r.nextLong(-50, 50) + 100L * u;
                }
                final ClusterSessionClass c = ClusterSessionClass.of(1 + r.nextInt(7));
                b = count == 0 ? ClusterPrincipal.anyUser(c, r.nextInt()).encode()
                        : ClusterPrincipal.forUsers(c, r.nextInt(), users).encode();
                if (r.nextBoolean()) {
                    b[r.nextInt(b.length)] = (byte) r.nextInt(256);
                }
            } else {
                b = new byte[r.nextInt(40)];
                for (int k = 0; k < b.length; k++) {
                    b[k] = (byte) (k < 2 ? r.nextInt(9) : r.nextInt(256));
                }
                if (b.length >= 10) {
                    b[8] = (byte) ((b.length - 10) / 8);
                    b[9] = 0;
                }
            }
            final int code = ClusterPrincipal.classCodeOf(b);
            assertEquals(Arrays.toString(b), ClusterPrincipal.decode(b).classCode(), code);
            if (code != 0) {
                nonZero++;
            }
        }
        assertTrue("corpus reaches valid principals: " + nonZero, nonZero > 10_000);
    }

    @Test
    public void classCodeOfAllocatesNothing() {
        final byte[] scoped = ClusterPrincipal.forUsers(ClusterSessionClass.FORWARDER, 1, 3, 5, 8).encode();
        final byte[] any = ClusterPrincipal.anyUser(ClusterSessionClass.ADMIN, 1).encode();
        final com.sun.management.ThreadMXBean mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long sink = 0;
        for (int i = 0; i < 200_000; i++) { // warm up past JIT
            sink += ClusterPrincipal.classCodeOf(scoped) + ClusterPrincipal.classCodeOf(any);
        }
        final long tid = Thread.currentThread().getId();
        final long before = mx.getThreadAllocatedBytes(tid);
        for (int i = 0; i < 1_000_000; i++) {
            sink += ClusterPrincipal.classCodeOf(scoped) + ClusterPrincipal.classCodeOf(any) + ClusterPrincipal.classCodeOf(null);
        }
        final long allocated = mx.getThreadAllocatedBytes(tid) - before;
        assertEquals(6_000_000, sink - 1_200_000);
        assertTrue("3M calls allocated " + allocated + " bytes", allocated < 4096);
    }
}
