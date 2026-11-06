// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import io.aeron.Aeron;
import org.junit.Test;

import static com.openexchange.cluster.auth.AuthTestPolicies.ME;
import static com.openexchange.cluster.auth.AuthTestPolicies.SA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S6 review DEFECT-1 (verifier-added). Aeron 1.53 gives EVERY backup-style request (BackupQuery,
 * HeartbeatRequest, StandbySnapshot) the same session id: {@code SessionManager.onBackupAction} builds
 * {@code new ClusterSession(memberId, NULL_VALUE, ...)} and calls {@code authenticator.onConnectRequest(session.id(), ...)},
 * then {@code processPendingSessions} calls {@code onConnectedSession} for whichever pending backup session's
 * response publication connects first. The authenticator's pending map is keyed by session id, so two concurrent
 * backup requests share one slot: a credential-less request whose response channel connects first receives the
 * REPLICA principal minted for the real backup, and passes {@code isAuthorised(BackupQuery)}.
 *
 * <p>RED on the first S6 candidate (the pending map keyed by id). Fixed fail-closed in {@link ClusterAuthenticator}:
 * a -1 session is authenticated only when its request is the one and only -1 request inside the window.</p>
 */
public class ClusterAuthenticatorBackupSessionIdTest {

    private static final long BACKUP_SESSION_ID = Aeron.NULL_VALUE; // what SessionManager.onBackupAction uses

    private static final long WINDOW_MS = 11_000;

    private static ClusterAuthenticator authenticator() {
        return new ClusterAuthenticator(AuthKeyTable.fromSpec("7:REPLICA:" + SA + "::users=*", ME),
                ClusterAuthenticator.DEFAULT_MAX_PENDING, () -> WINDOW_MS);
    }

    private static ClusterAuthenticatorTest.Proxy backupSession() {
        return new ClusterAuthenticatorTest.Proxy(BACKUP_SESSION_ID);
    }

    private static byte[] replica() {
        return ClusterCredentials.fromSpec("7:REPLICA:" + SA).encode();
    }

    @Test
    public void aCredentialLessBackupRequestNeverReceivesTheReplicaPrincipal() {
        final ClusterAuthenticator a = authenticator();
        // 1. an unauthenticated peer sends BackupQuery; its response channel is not connected yet
        a.onConnectRequest(BACKUP_SESSION_ID, null, 1_000);
        // 2. the real backup sends BackupQuery with the REPLICA credential
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_001);
        // 3. the peer's response channel connects first: SessionManager hands its session to the authenticator
        final ClusterAuthenticatorTest.Proxy peer = new ClusterAuthenticatorTest.Proxy(BACKUP_SESSION_ID);
        a.onConnectedSession(peer, 1_002);
        assertNull("the credential-less peer was authenticated with the REPLICA principal",
                peer.principal);
        assertTrue("the credential-less peer must be rejected", peer.rejected);
    }

    /** Guard (passes today): one backup request at a time is authenticated as before. */
    @Test
    public void aLoneReplicaBackupRequestIsAuthenticated() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_000);
        final ClusterAuthenticatorTest.Proxy backup = new ClusterAuthenticatorTest.Proxy(BACKUP_SESSION_ID);
        a.onConnectedSession(backup, 1_001);
        assertFalse(backup.rejected);
        assertEquals(7, ClusterPrincipal.classCodeOf(backup.principal));
    }

    @Test
    public void theRealBackupConnectingFirstIsAlsoRejectedWhileAnotherRequestIsPending() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(BACKUP_SESSION_ID, null, 1_000);
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_001);
        final ClusterAuthenticatorTest.Proxy backup = backupSession();
        a.onConnectedSession(backup, 1_002);
        assertTrue("ambiguous: rejected, the backup retries", backup.rejected);
        assertNull(backup.principal);
        final ClusterAuthenticatorTest.Proxy peer = backupSession();
        a.onConnectedSession(peer, 1_003);
        assertTrue(peer.rejected);
        assertNull(peer.principal);
        assertEquals(2, a.backupAmbiguousCount());
    }

    @Test
    public void twoAcceptedReplicaRequestsInTheWindowAreBothRejected() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_000);
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_001);
        final ClusterAuthenticatorTest.Proxy first = backupSession();
        final ClusterAuthenticatorTest.Proxy second = backupSession();
        a.onConnectedSession(first, 1_002);
        a.onConnectedSession(second, 1_003);
        assertTrue(first.rejected && second.rejected);
        assertNull(first.principal);
        assertNull(second.principal);
    }

    @Test
    public void afterTheWindowALoneRequestIsAcceptedAgain() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(BACKUP_SESSION_ID, null, 1_000); // a peer whose response channel never connects
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 2_000);
        final ClusterAuthenticatorTest.Proxy inside = backupSession();
        a.onConnectedSession(inside, 2_001);
        assertTrue("inside the window: ambiguous", inside.rejected);
        // both requests age out; the backup's next query is alone again
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 2_000 + WINDOW_MS + 1);
        assertEquals(1, a.backupRequestCount());
        final ClusterAuthenticatorTest.Proxy after = backupSession();
        a.onConnectedSession(after, 2_000 + WINDOW_MS + 2);
        assertFalse(after.rejected);
        assertEquals(7, ClusterPrincipal.classCodeOf(after.principal));
    }

    @Test
    public void aPrincipalIsHandedOutOnceAndTheRequestStaysInTheWindow() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_000);
        final ClusterAuthenticatorTest.Proxy backup = backupSession();
        a.onConnectedSession(backup, 1_001);
        assertEquals(7, ClusterPrincipal.classCodeOf(backup.principal));
        // a credential-less session that connects later in the window cannot reuse it
        final ClusterAuthenticatorTest.Proxy late = backupSession();
        a.onConnectedSession(late, 1_002);
        assertTrue(late.rejected);
        assertNull(late.principal);
        // a later request in the window makes everything ambiguous until it ages out
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_003);
        final ClusterAuthenticatorTest.Proxy next = backupSession();
        a.onConnectedSession(next, 1_004);
        assertTrue(next.rejected);
    }

    @Test
    public void anAuthenticateNotYetPossibleIsRetriedUnderTheSameRule() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(BACKUP_SESSION_ID, replica(), 1_000);
        final ClusterAuthenticatorTest.Proxy backup = backupSession();
        backup.refuseAuthenticateOnce = true;
        a.onConnectedSession(backup, 1_001);
        assertNull(backup.principal);
        assertFalse(backup.rejected);
        a.onConnectedSession(backup, 1_002);
        assertEquals(7, ClusterPrincipal.classCodeOf(backup.principal));
    }

    @Test
    public void clientSessionsAreUnaffectedByBackupRequests() {
        final ClusterAuthenticator a = authenticator();
        a.onConnectRequest(BACKUP_SESSION_ID, null, 1_000);
        a.onConnectRequest(BACKUP_SESSION_ID, null, 1_001);
        a.onConnectRequest(5, replica(), 1_002);
        final ClusterAuthenticatorTest.Proxy client = new ClusterAuthenticatorTest.Proxy(5);
        a.onConnectedSession(client, 1_003);
        assertEquals(7, ClusterPrincipal.classCodeOf(client.principal));
    }

    /**
     * Concurrency: interleaved valid (REPLICA) and invalid (no credential, wrong secret, wrong class) -1 requests,
     * with their sessions connecting in random order and repeatedly, as SessionManager's reverse walk does.
     * Invariants: no session of an invalid request ever holds a principal; when a session receives one, no other
     * -1 request whose session could still be pending (Aeron's lifetime, here window - 1 s) existed at that moment,
     * so the principal cannot belong to another request; a lone valid request is authenticated when it connects.
     */
    @Test
    public void interleavedValidAndInvalidRequestsNeverLeakAPrincipal() {
        final java.util.SplittableRandom r = new java.util.SplittableRandom(0xD1FEC71L);
        final byte[][] invalid = {null, new byte[0],
                ClusterCredentials.fromSpec("7:REPLICA:" + "ee".repeat(32)).encode(),
                ClusterCredentials.fromSpec("7:ADMIN:" + SA).encode()};
        int alonesAccepted = 0;
        int leaksChecked = 0;
        for (int round = 0; round < 2_000; round++) {
            final ClusterAuthenticator a = authenticator();
            long now = 10_000;
            final java.util.List<Object[]> live = new java.util.ArrayList<>(); // {proxy, valid, requestedAt}
            final java.util.Map<Object[], long[]> grants = new java.util.HashMap<>(); // {time, requests so far}
            final int requests = 1 + r.nextInt(4);
            for (int i = 0; i < requests; i++) {
                now += r.nextInt(r.nextBoolean() ? 50 : (int) WINDOW_MS + 500);
                final boolean valid = r.nextBoolean();
                a.onConnectRequest(BACKUP_SESSION_ID, valid ? replica() : invalid[r.nextInt(invalid.length)], now);
                live.add(new Object[] {backupSession(), valid, now});
                // some sessions connect right away, in random order and possibly more than once
                for (int k = 0; k < r.nextInt(3); k++) {
                    final Object[] s = live.get(r.nextInt(live.size()));
                    final ClusterAuthenticatorTest.Proxy p = (ClusterAuthenticatorTest.Proxy) s[0];
                    if (p.principal == null && !p.rejected && (long) s[2] + WINDOW_MS - 1_000 >= now) {
                        a.onConnectedSession(p, now);
                        if (p.principal != null) {
                            grants.put(s, new long[] {now, live.size()});
                        }
                    }
                }
            }
            for (Object[] s : live) {
                final ClusterAuthenticatorTest.Proxy p = (ClusterAuthenticatorTest.Proxy) s[0];
                if (p.principal == null && !p.rejected && (long) s[2] + WINDOW_MS - 1_000 >= now) {
                    a.onConnectedSession(p, now);
                    if (p.principal != null) {
                        grants.put(s, new long[] {now, live.size()});
                    }
                }
            }
            for (Object[] s : live) {
                final ClusterAuthenticatorTest.Proxy p = (ClusterAuthenticatorTest.Proxy) s[0];
                if (!(boolean) s[1]) {
                    assertNull("round " + round + ": an invalid request's session holds a principal", p.principal);
                    leaksChecked++;
                }
            }
            for (java.util.Map.Entry<Object[], long[]> g : grants.entrySet()) {
                final long at0 = g.getValue()[0];
                for (int i = 0; i < g.getValue()[1]; i++) { // the requests that existed when it was granted
                    final Object[] o = live.get(i);
                    if (o != g.getKey()) {
                        assertFalse("round " + round + ": a principal was handed out while another request's"
                                + " session could still be pending", (long) o[2] + WINDOW_MS - 1_000 >= at0);
                    }
                }
            }
            if (requests == 1 && (boolean) live.get(0)[1]) {
                assertEquals("a lone valid request is authenticated", 7,
                        ClusterPrincipal.classCodeOf(((ClusterAuthenticatorTest.Proxy) live.get(0)[0]).principal));
                alonesAccepted++;
            }
        }
        assertTrue("the corpus reaches lone valid requests: " + alonesAccepted, alonesAccepted > 100);
        assertTrue(leaksChecked > 1_000);
    }
}
