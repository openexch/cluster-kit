// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import org.junit.After;
import org.junit.Test;

import java.util.HexFormat;

import static com.openexchange.cluster.auth.AuthTestPolicies.AE;
import static com.openexchange.cluster.auth.AuthTestPolicies.ME;
import static com.openexchange.cluster.auth.AuthTestPolicies.SA;
import static com.openexchange.cluster.auth.AuthTestPolicies.SB;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC2 S6 9.1: the node-local key table (contract 2.4 rules 1-6), both engine policies, decision A16. */
public class AuthKeyTableTest {

    @After
    public void clear() {
        System.clearProperty(ME.keysProperty());
    }

    private static IllegalArgumentException refused(final String spec, final AuthPolicy policy) {
        final IllegalArgumentException e = assertThrows(spec, IllegalArgumentException.class,
                () -> AuthKeyTable.fromSpec(spec, policy));
        assertFalse("the secret never appears in a refusal: " + e.getMessage(), e.getMessage().contains(SA));
        assertFalse(e.getMessage().contains(SB));
        return e;
    }

    @Test
    public void rule1BlankEntriesSkippedAndEntriesTrimmed() {
        final AuthKeyTable t = AuthKeyTable.fromSpec(" ;;1:ADMIN:" + SA + "::users=* ; ;2:TREASURY:" + SB
                + ":77:users=7|9|8 ;", AE);
        assertEquals(2, t.size());
        final ClusterPrincipal admin = t.authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + SA));
        assertTrue(admin.anyUser());
        assertEquals(1, admin.identity());
        final ClusterPrincipal treasury = t.authenticate(ClusterCredentials.fromSpec("2:TREASURY:" + SB));
        assertEquals(77, treasury.identity());
        assertArrayEquals(new long[] {7, 8, 9}, treasury.userIds());
    }

    @Test
    public void rule2And3JavaSplitPaddedPartsAndIdentityDefault() {
        final AuthKeyTable t = AuthKeyTable.fromSpec(" 5 : forwarder : " + SA + " :   : users= * ", ME);
        final ClusterPrincipal p = t.authenticate(ClusterCredentials.fromSpec("5:FORWARDER:" + SA));
        assertNotNull(p);
        assertEquals("blank identity is keyId", 5, p.identity());
        assertEquals(ClusterSessionClass.FORWARDER, p.sessionClass());
        // Java split drops trailing empties: a trailing ':' leaves three parts, i.e. no users field (A16)
        assertTrue(refused("1:ADMIN:" + SA + ":", AE).getMessage().contains("no users field"));
        assertTrue(refused("1:ADMIN:" + SA + "::users=*:x", AE).getMessage().contains("malformed"));
        assertTrue(refused("1:ADMIN", AE).getMessage().contains("entry 1"));
    }

    @Test
    public void rule4UsersFieldIsMandatoryA16() {
        final IllegalArgumentException none = refused("1:ADMIN:" + SA, AE);
        assertTrue(none.getMessage(), none.getMessage().contains("ASSETS_AUTH_KEYS entry 1 (keyId 1)"));
        assertTrue(none.getMessage(), none.getMessage().contains("users=*"));
        final IllegalArgumentException empty = refused("1:ADMIN:" + SA + "::users=", AE);
        assertTrue(empty.getMessage(), empty.getMessage().contains("entry 1 (keyId 1)"));
        assertTrue(empty.getMessage(), empty.getMessage().contains("users=*"));
        final IllegalArgumentException third = refused("1:ADMIN:" + SA + "::users=*;2:TOOL:" + SA
                + "::users=*;;7:TOOL:" + SB + ":7", AE);
        assertTrue(third.getMessage(), third.getMessage().contains("ASSETS_AUTH_KEYS entry 4 (keyId 7): no users field"));
        refused("1:ADMIN:" + SA + "::users=7||9", AE);
        refused("1:ADMIN:" + SA + "::users=7|", AE);
        refused("1:ADMIN:" + SA + "::users=*|7", AE);
        refused("1:ADMIN:" + SA + "::users=x", AE);
        refused("1:ADMIN:" + SA + "::users=7|7", AE);
        refused("1:ADMIN:" + SA + "::scope=7", AE);
        final byte[] star = AuthKeyTable.fromSpec("1:ADMIN:" + SA + "::users=*", AE).principalOf(1).encode();
        assertArrayEquals("users=* mints the RC1 implicit any-user bytes",
                ClusterPrincipal.anyUser(ClusterSessionClass.ADMIN, 1).encode(), star);
        assertEquals("01040100010000000000", HexFormat.of().formatHex(star));
    }

    @Test
    public void rule5ClassByCodeOrRootName() {
        final java.util.Locale prior = java.util.Locale.getDefault();
        java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"));
        try {
            final AuthKeyTable t = AuthKeyTable.fromSpec("1:admin:" + SA + "::users=*;2:4:" + SB + "::users=*", ME);
            assertEquals(ClusterSessionClass.ADMIN, t.principalOf(1).sessionClass());
            assertEquals(ClusterSessionClass.ADMIN, t.principalOf(2).sessionClass());
        } finally {
            java.util.Locale.setDefault(prior);
        }
    }

    @Test
    public void rule6PolicyClassSetUserScopeDuplicatesAndSecrets() {
        for (String cls : new String[] {"BRIDGE", "TOOL", "TREASURY", "3", "5", "6"}) {
            final IllegalArgumentException e = refused("4:" + cls + ":" + SA + "::users=*", ME);
            assertTrue(e.getMessage(), e.getMessage().contains("ME_AUTH_KEYS entry 1 (keyId 4)"));
            assertTrue(e.getMessage(), e.getMessage().contains("not allowed"));
        }
        final IllegalArgumentException scoped = refused("2:FORWARDER:" + SA + "::users=7", ME);
        assertTrue(scoped.getMessage(), scoped.getMessage().contains("users=*"));
        assertNotNull(AuthKeyTable.fromSpec("2:FORWARDER:" + SA + "::users=*", ME).principalOf(2));
        assertNotNull(AuthKeyTable.fromSpec("2:TREASURY:" + SA + "::users=7", AE).principalOf(2));
        refused("1:ADMIN:" + SA + "::users=*;1:TOOL:" + SB + "::users=*", AE);
        refused("1:ADMIN:abcd::users=*", AE);
        refused("1:ADMIN:" + SA + "00::users=*", AE);
        refused("1:ADMIN:zz" + SA.substring(2) + "::users=*", AE);
        refused("1:NOBODY:" + SA + "::users=*", AE);
        refused("1:0:" + SA + "::users=*", AE);
        refused("1:UNSET:" + SA + "::users=*", AE);
        refused("x:ADMIN:" + SA + "::users=*", AE);
        refused("1:ADMIN:" + SA + ":y:users=*", AE);
        final StringBuilder ids = new StringBuilder();
        for (int i = 1; i <= 511; i++) {
            ids.append(i == 1 ? "" : "|").append(i);
        }
        refused("6:TREASURY:" + SA + "::users=" + ids, AE);
    }

    @Test
    public void authenticateNeedsKeyClassAndSecret() {
        final AuthKeyTable t = AuthKeyTable.fromSpec("1:ADMIN:" + SA + "::users=*", AE);
        assertNull(t.authenticate(null));
        assertNull(t.authenticate(ClusterCredentials.fromSpec("2:ADMIN:" + SA)));
        assertNull(t.authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + SB)));
        assertNull(t.authenticate(ClusterCredentials.fromSpec("1:TOOL:" + SA)));
        assertNotNull(t.authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + SA)));
    }

    @Test
    public void anAbsentTableIsEmptyAndAuthenticatesNobody() {
        assertEquals(0, AuthKeyTable.fromSpec(null, ME).size());
        assertEquals(0, AuthKeyTable.fromSpec("  ", ME).size());
        assertEquals(0, AuthKeyTable.fromEnvironment(ME).size());
        assertNull(AuthKeyTable.empty(ME).authenticate(ClusterCredentials.fromSpec("1:ADMIN:" + SA)));
        System.setProperty(ME.keysProperty(), "4:ADMIN:" + SA + "::users=*");
        assertEquals(1, AuthKeyTable.fromEnvironment(ME).size());
    }
}
