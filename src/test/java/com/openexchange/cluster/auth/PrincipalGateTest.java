// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S17 W12 (K1): R0 is one kit rule for both engines. A session whose replicated principal has no class (null,
 * empty, foreign or malformed bytes) can only be pre-S6 history or a session id Aeron does not know; the gate throws
 * the one exception both engines handle, with the one CRITICAL text the T2 rows match.
 */
public class PrincipalGateTest {

    private static final String PREFIX =
            "CRITICAL: principal-less session in log: pre-S6 history, clean genesis required (";

    private static void assertRefused(final byte[] bytes) {
        final PrincipalLessSessionException e = assertThrows(PrincipalLessSessionException.class,
                () -> PrincipalGate.requireClass(bytes, "session open", 42L, 6304L));
        assertTrue(e.getMessage(), e.getMessage().startsWith(PREFIX));
        assertEquals("CRITICAL: " + PrincipalGate.PRINCIPAL_LESS_MESSAGE
                + " (session open, clusterSessionId=42, logPosition=6304)", e.getMessage());
        assertTrue("an IllegalStateException, as the engines' callback fences expect",
                e instanceof IllegalStateException);
    }

    @Test
    public void nullPrincipalIsRefused() {
        assertRefused(null);
    }

    @Test
    public void emptyPrincipalIsRefused() {
        assertRefused(new byte[0]);
    }

    @Test
    public void foreignPrincipalBytesAreRefused() {
        assertRefused(new byte[] {9, 4, 1, 0, 0, 0, 0, 0, 0, 0});
    }

    @Test
    public void theMessageNamesWhereSessionAndPosition() {
        final PrincipalLessSessionException e = assertThrows(PrincipalLessSessionException.class,
                () -> PrincipalGate.requireClass(new byte[0], "restored session", -1L, 819328L));
        assertEquals(PrincipalGate.criticalLine("restored session", -1L, 819328L), e.getMessage());
        assertTrue(e.getMessage(), e.getMessage().endsWith("(restored session, clusterSessionId=-1, logPosition=819328)"));
    }

    @Test
    public void aMintedPrincipalReturnsItsClass() {
        for (final ClusterSessionClass c : ClusterSessionClass.values()) {
            if (c == ClusterSessionClass.UNSET) {
                continue;
            }
            final byte[] bytes = ClusterPrincipal.anyUser(c, 7).encode();
            assertEquals(c.name(), c.code(), PrincipalGate.requireClass(bytes, "ingress frame", 1L, 2L));
        }
    }
}
