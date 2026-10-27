// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import io.aeron.Aeron;
import io.aeron.security.Authenticator;
import io.aeron.security.SessionProxy;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * The consensus module's authenticator (K6 v1), one copy for both engines: it looks the presented
 * credentials up in the node-local {@link AuthKeyTable} and mints the session's {@link ClusterPrincipal},
 * which Aeron appends to the SessionOpen log event so every member derives the same session authority.
 * Node-local input, replicated output. No challenge in v1: a challenged session is rejected. An empty key
 * table rejects everyone, loudly, instead of degrading to an unauthenticated mode.
 *
 * <p>Client sessions have unique ids. A repeated connect request for the same id replaces its pending entry,
 * and the pending map is capped (oldest dropped, so that session is rejected when it connects): a session that
 * never reaches {@link #onConnectedSession} cannot leak an entry.</p>
 *
 * <p><b>Backup-style requests (RC2 S6 review DEFECT-1).</b> Aeron 1.53 builds every BackupQuery,
 * HeartbeatRequest and StandbySnapshot session with the same id, {@link Aeron#NULL_VALUE}, and hands this
 * authenticator only the id ({@code SessionManager.onBackupAction}, {@code processPendingSessions}); the
 * {@link SessionProxy} carries no credential, so a decision cannot be bound to its request through Aeron's API.
 * The rule is therefore FAIL CLOSED: every -1 request is remembered, accepted or not, for a window that outlives
 * Aeron's own lifetime of the pending session (the consensus module's session timeout plus a margin), and
 * consumption does not shorten it. A -1 session is authenticated only when exactly one -1 request is inside the
 * window, that request was accepted and its principal was not handed out yet. Any other state (two or more
 * requests, a rejected one, an already used one, an overflowed record list) rejects the session; the backup
 * retries on its next query. A forger can delay the backup by at most one window; it can never receive a
 * principal minted for another request.</p>
 *
 * <p>Runs on the consensus module agent thread; the counters may be read from any thread.</p>
 */
public final class ClusterAuthenticator implements Authenticator {

    /** Pending (authenticated, not yet connected) client sessions kept at most. */
    public static final int DEFAULT_MAX_PENDING = 1024;
    /** Backup-style (-1) requests remembered at most; past it every -1 session is rejected until the window clears. */
    public static final int MAX_BACKUP_REQUESTS = 1024;
    /** Aeron's default consensus-module session timeout (10 s) plus a 1 s margin. */
    public static final long DEFAULT_BACKUP_REQUEST_WINDOW_MS = 11_000;
    /** The id Aeron gives every backup-style session. */
    public static final long BACKUP_SESSION_ID = Aeron.NULL_VALUE;

    /** One backup-style request: when it arrived, what it presented, whether its principal was handed out. */
    private static final class BackupRequest {
        final long atMs;
        final byte[] principal; // null: rejected credentials
        boolean used;

        BackupRequest(final long atMs, final byte[] principal) {
            this.atMs = atMs;
            this.principal = principal;
        }
    }

    private final AuthKeyTable keys;
    private final int maxPending;
    private final LongSupplier backupWindowMs;
    private final LinkedHashMap<Long, byte[]> pending = new LinkedHashMap<>();
    private final ArrayDeque<BackupRequest> backupRequests = new ArrayDeque<>();
    private long backupOverflowUntilMs = Long.MIN_VALUE;
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong pendingDropped = new AtomicLong();
    private final AtomicLong backupAmbiguous = new AtomicLong();
    private boolean warnedEmptyTable;

    public ClusterAuthenticator(final AuthKeyTable keys) {
        this(keys, DEFAULT_MAX_PENDING, () -> DEFAULT_BACKUP_REQUEST_WINDOW_MS);
    }

    public ClusterAuthenticator(final AuthKeyTable keys, final int maxPending) {
        this(keys, maxPending, () -> DEFAULT_BACKUP_REQUEST_WINDOW_MS);
    }

    /**
     * @param backupWindowMs how long a backup-style request is remembered; must be at least the consensus
     *                       module's session timeout (a supplier, so it can read the context once it is concluded)
     */
    public ClusterAuthenticator(final AuthKeyTable keys, final int maxPending, final LongSupplier backupWindowMs) {
        if (keys == null) {
            throw new IllegalArgumentException("a key table is required (an empty one rejects everyone)");
        }
        if (maxPending < 1) {
            throw new IllegalArgumentException("maxPending must be positive, got " + maxPending);
        }
        this.keys = keys;
        this.maxPending = maxPending;
        this.backupWindowMs = backupWindowMs;
    }

    /** The window for a consensus module whose session timeout is {@code sessionTimeoutNs}: timeout + 1 s. */
    public static long backupWindowMsFor(final long sessionTimeoutNs) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(sessionTimeoutNs) + 1_000;
    }

    @Override
    public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
        final ClusterPrincipal principal = keys.authenticate(ClusterCredentials.decode(encodedCredentials));
        if (principal == null) {
            warnIfEmpty();
        }
        if (sessionId == BACKUP_SESSION_ID) {
            expireBackupRequests(nowMs);
            if (backupRequests.size() >= MAX_BACKUP_REQUESTS) {
                backupOverflowUntilMs = nowMs + backupWindowMs.getAsLong();
            } else {
                backupRequests.addLast(new BackupRequest(nowMs, principal == null ? null : principal.encode()));
            }
            return;
        }
        pending.remove(sessionId); // a repeated request replaces the earlier decision
        if (principal != null) {
            pending.put(sessionId, principal.encode());
            if (pending.size() > maxPending) {
                final Iterator<Map.Entry<Long, byte[]>> oldest = pending.entrySet().iterator();
                oldest.next();
                oldest.remove();
                pendingDropped.incrementAndGet();
            }
        }
    }

    private void warnIfEmpty() {
        if (keys.size() == 0 && !warnedEmptyTable) {
            warnedEmptyTable = true;
            final AuthPolicy p = keys.policy();
            System.err.println("AUTH: the " + p.engine() + " key table is EMPTY (" + p.keysEnv() + " / -D"
                    + p.keysProperty() + " not set); every session is rejected until it is configured");
        }
    }

    private void expireBackupRequests(final long nowMs) {
        final long window = backupWindowMs.getAsLong();
        while (!backupRequests.isEmpty() && nowMs - backupRequests.peekFirst().atMs > window) {
            backupRequests.pollFirst();
        }
    }

    @Override
    public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
        if (sessionId != BACKUP_SESSION_ID) {
            pending.remove(sessionId); // v1 never challenges, so a response is foreign input
        }
    }

    @Override
    public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs) {
        if (sessionProxy.sessionId() == BACKUP_SESSION_ID) {
            onConnectedBackupSession(sessionProxy, nowMs);
            return;
        }
        final byte[] principal = pending.remove(sessionProxy.sessionId());
        if (principal != null) {
            if (sessionProxy.authenticate(principal)) {
                accepted.incrementAndGet();
            } else {
                pending.put(sessionProxy.sessionId(), principal); // not yet: retried next cycle
            }
        } else {
            rejected.incrementAndGet();
            sessionProxy.reject();
        }
    }

    /** DEFECT-1: authenticate a -1 session only when its request is the one and only request in the window. */
    private void onConnectedBackupSession(final SessionProxy sessionProxy, final long nowMs) {
        expireBackupRequests(nowMs);
        final BackupRequest only = backupRequests.size() == 1 ? backupRequests.peekFirst() : null;
        if (nowMs > backupOverflowUntilMs && only != null && only.principal != null && !only.used) {
            if (sessionProxy.authenticate(only.principal)) {
                only.used = true; // stays in the window: a later -1 session in it is ambiguous
                accepted.incrementAndGet();
            }
            return; // not yet: retried next cycle with the same rule
        }
        if (backupRequests.size() > 1 || nowMs <= backupOverflowUntilMs) {
            backupAmbiguous.incrementAndGet();
        }
        rejected.incrementAndGet();
        sessionProxy.reject();
    }

    @Override
    public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs) {
        rejected.incrementAndGet();
        sessionProxy.reject();
    }

    public AuthKeyTable keys() {
        return keys;
    }

    public long acceptedCount() {
        return accepted.get();
    }

    public long rejectedCount() {
        return rejected.get();
    }

    /** Pending entries dropped by the cap (each such session is rejected when it connects). */
    public long pendingDroppedCount() {
        return pendingDropped.get();
    }

    /** Backup-style sessions rejected because their request could not be told apart from another one. */
    public long backupAmbiguousCount() {
        return backupAmbiguous.get();
    }

    /** Current pending client-session entries; for tests. */
    public int pendingCount() {
        return pending.size();
    }

    /** Backup-style requests inside the window; for tests. */
    public int backupRequestCount() {
        return backupRequests.size();
    }
}
