// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deep verdicts for the life of the process: {@code VERIFIED} or {@code CORRUPT(reason)} for one
 * immutable recording. Never persisted (a verdict file beside the archive would be a second registry). The only
 * object the shadow package shares between threads: thread-safe, values immutable.
 *
 * <p>Key: the contract's {@code (recordingId, startPosition, stopPosition)} plus the archive id and the header CRC.
 * The archive id keeps a remote catalog's recording 5 (discovery) from answering for the local recording 5 when one
 * process shares this cache; the header CRC makes a recording id reused after an archive wipe a different key. A
 * stopped recording cannot change, so the key changes only when the recording does (C4).</p>
 */
public final class ShadowVerdicts {

    public record Key(long archiveId, long recordingId, long startPosition, long stopPosition, int headerCrc32c) {
    }

    public enum Kind { VERIFIED, CORRUPT }

    public record Verdict(Kind kind, String reason) {
        public static final Verdict VERIFIED = new Verdict(Kind.VERIFIED, null);

        public static Verdict corrupt(final String reason) {
            return new Verdict(Kind.CORRUPT, reason);
        }
    }

    private final Map<Key, Verdict> verdicts = new ConcurrentHashMap<>();
    private final Set<Long> startupPassDone = ConcurrentHashMap.newKeySet();

    public Verdict get(final Key key) {
        return verdicts.get(key);
    }

    /**
     * Stores a verdict. CORRUPT is final for its key: a later VERIFIED for the same key (a re-read that happened to
     * pass) never replaces it.
     *
     * @return the verdict stored afterwards
     */
    public Verdict put(final Key key, final Verdict verdict) {
        return verdicts.merge(key, verdict, (old, fresh) -> old.kind() == Kind.CORRUPT ? old : fresh);
    }

    /** Whether {@code verifyAllComplete} finished without a SCAN_ERROR for this archive in this process. */
    public boolean startupPassDone(final long archiveId) {
        return startupPassDone.contains(archiveId);
    }

    void markStartupPassDone(final long archiveId) {
        startupPassDone.add(archiveId);
    }

    public int size() {
        return verdicts.size();
    }
}
