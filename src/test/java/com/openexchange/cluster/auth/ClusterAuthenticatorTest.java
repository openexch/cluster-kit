// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import io.aeron.security.SessionProxy;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static com.openexchange.cluster.auth.AuthTestPolicies.AE;
import static com.openexchange.cluster.auth.AuthTestPolicies.ME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** RC2 S6 9.1: the kit authenticator, driven the way SessionManager drives it (the RC1 S3 cases plus the 2.5 additions). */
public class ClusterAuthenticatorTest {

    private static final String SECRET = "ab".repeat(32);

    static final class Proxy implements SessionProxy {
        final long id;
        byte[] principal;
        boolean rejected;
        boolean refuseAuthenticateOnce;

        Proxy(final long id) {
            this.id = id;
        }

        public long sessionId() { return id; }
        public boolean challenge(final byte[] c) { throw new UnsupportedOperationException(); }

        public boolean authenticate(final byte[] encodedPrincipal) {
            if (refuseAuthenticateOnce) {
                refuseAuthenticateOnce = false;
                return false;
            }
            principal = encodedPrincipal;
            return true;
        }

        public void reject() { rejected = true; }
    }

    private static ClusterAuthenticator authenticator() {
        return new ClusterAuthenticator(AuthKeyTable.fromSpec("1:ADMIN:" + SECRET + "::users=*;2:TREASURY:" + SECRET
                + ":9:users=7", AE));
    }

    private static byte[] cred(final String spec) {
        return ClusterCredentials.fromSpec(spec).encode();
    }

    @Test
    public void aKnownKeyMintsItsPrincipalIntoTheSession() {
        final ClusterAuthenticator a = authenticator();
        final Proxy p = new Proxy(11);
        a.onConnectRequest(11, cred("2:TREASURY:" + SECRET), 1_000);
        a.onConnectedSession(p, 1_001);
        assertFalse(p.rejected);
        final ClusterPrincipal minted = ClusterPrincipal.decode(p.principal);
        assertEquals(ClusterSessionClass.TREASURY, minted.sessionClass());
        assertEquals(9, minted.identity());
        assertTrue(minted.allowsUser(7));
        assertFalse(minted.allowsUser(8));
        assertEquals(1, a.acceptedCount());
    }

    @Test
    public void unknownKeyWrongSecretMissingCredentialsAndChallengesAreRejected() {
        final ClusterAuthenticator a = authenticator();
        final Proxy unknown = new Proxy(1);
        a.onConnectRequest(1, cred("5:ADMIN:" + SECRET), 0);
        a.onConnectedSession(unknown, 0);
        assertTrue(unknown.rejected);
        final Proxy wrong = new Proxy(2);
        a.onConnectRequest(2, cred("1:ADMIN:" + "cd".repeat(32)), 0);
        a.onConnectedSession(wrong, 0);
        assertTrue(wrong.rejected);
        final Proxy none = new Proxy(3);
        a.onConnectRequest(3, new byte[0], 0); // Aeron's NullCredentialsSupplier
        a.onConnectedSession(none, 0);
        assertTrue(none.rejected);
        final Proxy challenged = new Proxy(4);
        a.onChallengedSession(challenged, 0);
        assertTrue(challenged.rejected);
        final Proxy mismatch = new Proxy(5);
        a.onConnectRequest(5, cred("1:TOOL:" + SECRET), 0);
        a.onConnectedSession(mismatch, 0);
        assertTrue(mismatch.rejected);
        assertEquals(5, a.rejectedCount());
        assertEquals(0, a.acceptedCount());
        assertNull(unknown.principal);
    }

    @Test
    public void anAuthenticateThatIsNotYetPossibleIsRetriedOnTheNextCycle() {
        final ClusterAuthenticator a = authenticator();
        final Proxy p = new Proxy(5);
        p.refuseAuthenticateOnce = true;
        a.onConnectRequest(5, cred("1:ADMIN:" + SECRET), 0);
        a.onConnectedSession(p, 0);
        assertNull("not yet", p.principal);
        assertFalse(p.rejected);
        a.onConnectedSession(p, 1);
        assertEquals(ClusterSessionClass.ADMIN, ClusterPrincipal.decode(p.principal).sessionClass());
        assertEquals(1, a.acceptedCount());
    }

    @Test
    public void anEmptyTableRejectsEveryoneAndSaysWhereTheTableIsRead() {
        final PrintStream prior = System.err;
        final ByteArrayOutputStream err = new ByteArrayOutputStream();
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
        try {
            final ClusterAuthenticator a = new ClusterAuthenticator(AuthKeyTable.empty(ME));
            for (int i = 0; i < 3; i++) {
                final Proxy p = new Proxy(6 + i);
                a.onConnectRequest(6 + i, cred("1:ADMIN:" + SECRET), 0);
                a.onConnectedSession(p, 0);
                assertTrue(p.rejected);
            }
        } finally {
            System.setErr(prior);
        }
        final String log = err.toString(StandardCharsets.UTF_8);
        assertTrue(log, log.contains("ME_AUTH_KEYS") && log.contains("match.auth.keys"));
        assertEquals("logged once", log.indexOf("EMPTY"), log.lastIndexOf("EMPTY"));
    }

    @Test
    public void aRepeatedRequestReplacesThePendingDecision() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(7, cred("1:ADMIN:" + SECRET), 0);
        a.onConnectRequest(7, new byte[0], 1); // the same session id now presents nothing
        final Proxy p = new Proxy(7);
        a.onConnectedSession(p, 2);
        assertTrue("the later, failing request decides", p.rejected);
        a.onConnectRequest(8, cred("1:TOOL:" + SECRET), 0);
        a.onConnectRequest(8, cred("1:ADMIN:" + SECRET), 1);
        final Proxy q = new Proxy(8);
        a.onConnectedSession(q, 2);
        assertEquals(ClusterSessionClass.ADMIN, ClusterPrincipal.decode(q.principal).sessionClass());
        assertEquals(0, a.pendingCount());
    }

    @Test
    public void thePendingMapIsCappedOldestDropped() {
        final ClusterAuthenticator a = new ClusterAuthenticator(
                AuthKeyTable.fromSpec("1:ADMIN:" + SECRET + "::users=*", AE), 3);
        for (long id = 1; id <= 5; id++) {
            a.onConnectRequest(id, cred("1:ADMIN:" + SECRET), 0); // never reach onConnectedSession
        }
        assertEquals(3, a.pendingCount());
        assertEquals(2, a.pendingDroppedCount());
        final Proxy oldest = new Proxy(1);
        a.onConnectedSession(oldest, 0);
        assertTrue("a dropped entry is rejected, never accepted", oldest.rejected);
        final Proxy newest = new Proxy(5);
        a.onConnectedSession(newest, 0);
        assertFalse(newest.rejected);
        assertEquals(1, a.acceptedCount());
    }
}
