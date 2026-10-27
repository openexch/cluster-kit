// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.HexFormat;

import static com.openexchange.cluster.auth.AuthTestPolicies.ME;
import static com.openexchange.cluster.auth.AuthTestPolicies.SA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC2 S6 9.1: the 38-byte K6 v1 credential, one kit copy (RC1 S3 bytes). */
public class ClusterCredentialsTest {

    private static final byte[] SECRET = HexFormat.of().parseHex(SA);

    @After
    public void clear() {
        System.clearProperty(ME.credentialProperty());
    }

    @Test
    public void roundTripsAndParsesByNameRootAndCode() {
        final ClusterCredentials c = new ClusterCredentials(ClusterSessionClass.BRIDGE, 3, SECRET);
        assertEquals(ClusterCredentials.ENCODED_LENGTH, c.encode().length);
        final ClusterCredentials d = ClusterCredentials.decode(c.encode());
        assertEquals(ClusterSessionClass.BRIDGE, d.sessionClass());
        assertEquals(3, d.classCode());
        assertEquals(3, d.keyId());
        assertArrayEquals(SECRET, d.secret());
        assertArrayEquals(c.encode(), ClusterCredentials.fromSpec("3:BRIDGE:" + SA).encode());
        assertArrayEquals(c.encode(), ClusterCredentials.fromSpec("3:bridge:" + SA).encode());
        assertArrayEquals(c.encode(), ClusterCredentials.fromSpec(" 3 : 3 : " + SA).encode());
        assertArrayEquals(c.encode(), c.supplier().encodedCredentials());
        assertEquals(0, c.supplier().onChallenge(new byte[4]).length);
        assertFalse("never prints the secret", c.toString().contains(SA));
    }

    @Test
    public void goldenBytes() {
        // [u8 1][u8 class][i32 keyId LE][32 B]: the S11 C++ goldens and credentials.py produce the same bytes.
        assertEquals("010206000000" + SA, HexFormat.of().formatHex(ClusterCredentials.fromSpec("6:FORWARDER:" + SA).encode()));
        assertEquals("0104feffffff" + SA, HexFormat.of().formatHex(ClusterCredentials.fromSpec("-2:ADMIN:" + SA).encode()));
        assertEquals("0107ffffff7f" + SA, HexFormat.of().formatHex(ClusterCredentials.fromSpec("2147483647:REPLICA:" + SA).encode()));
    }

    @Test
    public void rootUppercasingSurvivesATurkishDefaultLocale() {
        final java.util.Locale prior = java.util.Locale.getDefault();
        java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"));
        try {
            assertEquals(ClusterSessionClass.ADMIN, ClusterCredentials.fromSpec("4:admin:" + SA).sessionClass());
            assertEquals(ClusterSessionClass.REPLICA, ClusterCredentials.fromSpec("7:replica:" + SA).sessionClass());
            assertEquals(ClusterSessionClass.BRIDGE, ClusterCredentials.fromSpec("3:bridge:" + SA).sessionClass());
        } finally {
            java.util.Locale.setDefault(prior);
        }
    }

    @Test
    public void foreignBytesAndBadSpecsAreRefused() {
        assertNull(ClusterCredentials.decode(null));
        assertNull(ClusterCredentials.decode(new byte[0]));
        assertNull(ClusterCredentials.decode(new byte[ClusterCredentials.ENCODED_LENGTH - 1]));
        assertNull(ClusterCredentials.decode(new byte[ClusterCredentials.ENCODED_LENGTH + 1]));
        final byte[] good = new ClusterCredentials(ClusterSessionClass.TOOL, 1, SECRET).encode();
        final byte[] badVersion = good.clone();
        badVersion[0] = 2;
        assertNull(ClusterCredentials.decode(badVersion));
        for (int code : new int[] {0, 8, 255}) {
            final byte[] badClass = good.clone();
            badClass[1] = (byte) code;
            assertNull("class " + code, ClusterCredentials.decode(badClass));
        }
        assertThrows(IllegalArgumentException.class, () -> new ClusterCredentials(ClusterSessionClass.UNSET, 1, SECRET));
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterCredentials(ClusterSessionClass.TOOL, 1, Arrays.copyOf(SECRET, 16)));
        for (String bad : new String[] {"1:TOOL", "1:NOBODY:" + SA, "1:9:" + SA, "1:0:" + SA, "1:UNSET:" + SA,
                "1:ADMIN:abcd", "x:ADMIN:" + SA, "1:ADMIN:" + SA + ":9"}) {
            assertThrows(bad, IllegalArgumentException.class, () -> ClusterCredentials.fromSpec(bad));
        }
    }

    @Test
    public void environmentAndTheClientStartRule() {
        assertNull(ClusterCredentials.fromEnvironment(ME));
        final IllegalStateException unset = assertThrows(IllegalStateException.class,
                () -> ClusterCredentials.requireFromEnvironment(ME, ClusterSessionClass.REPLICA));
        assertTrue(unset.getMessage(), unset.getMessage().contains("ME_AUTH_CREDENTIAL"));
        System.setProperty(ME.credentialProperty(), "7:FORWARDER:" + SA);
        assertEquals(ClusterSessionClass.FORWARDER, ClusterCredentials.fromEnvironment(ME).sessionClass());
        final IllegalStateException wrong = assertThrows(IllegalStateException.class,
                () -> ClusterCredentials.requireFromEnvironment(ME, ClusterSessionClass.REPLICA));
        assertTrue(wrong.getMessage(), wrong.getMessage().contains("FORWARDER"));
        assertFalse(wrong.getMessage().contains(SA));
        System.setProperty(ME.credentialProperty(), "7:REPLICA:" + SA);
        assertEquals(7, ClusterCredentials.requireFromEnvironment(ME, ClusterSessionClass.REPLICA).keyId());
        System.setProperty(ME.credentialProperty(), "7:REPLICA:zz");
        assertThrows(IllegalStateException.class, () -> ClusterCredentials.requireFromEnvironment(ME));
    }
}
