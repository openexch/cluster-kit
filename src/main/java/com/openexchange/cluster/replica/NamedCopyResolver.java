// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The local copy of the member service snapshot a backup response named, and its
 * genesis decode, for {@link GenesisGuard}. Follower thread only; pure (its {@link Source} reads the backup events and
 * the archive).
 *
 * <p>A resolved copy is cached until its decode fails (the next pass resolves again, so a later complete copy is
 * used). Candidates read and found not to be the named copy are never read again.</p>
 *
 * <p>A copy whose content is refused ({@link ClusterIdentityCheck.CopyRefused}: no END marker, a
 * refused stub, the envelope or image decode) joins that set as well, whichever path named it (the backup's
 * RecordingLog update or the archive scan): a stopped recording never changes, so it is read once, not replayed in
 * full on every pass. An archive error (any other exception) still retries at the next paced pass.</p>
 */
final class NamedCopyResolver {

    interface Source {
        /** The copy the backup's RecordingLog update names for {@code n}, -1 when none. */
        long retrievedCopy(GenesisGuard.Named n);

        /** The stopped, non-empty recordings on the snapshot streams ({@link ClusterIdentityCheck#candidates}). */
        List<Long> candidates();

        /** The snapshot markers of one recording ({@link ClusterIdentityCheck#markers}). */
        ClusterIdentityCheck.Markers markers(long recordingId);

        /**
         * The genesis the copy holds ({@link ClusterIdentityCheck#genesisOf}).
         *
         * @throws ClusterIdentityCheck.CopyRefused when the content was read and refused
         * @throws RuntimeException                 on an archive error
         */
        long genesisOf(long recordingId);
    }

    private final Source source;
    /** Retrieved snapshot candidates already read and found not to be the named copy. */
    private final Set<Long> notIt = new HashSet<>();
    private GenesisGuard.Named resolvedNamed;
    private long resolvedCopy = -1;
    /** The named snapshot whose copy was last refused by its content, and why. */
    private GenesisGuard.Named refusedNamed;
    private String refusedReason;

    NamedCopyResolver(final Source source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    /** The local copy of {@code n}: the backup's RecordingLog update, else an archive scan by marker; -1 when none. */
    long resolve(final GenesisGuard.Named n) {
        if (n.equals(resolvedNamed) && resolvedCopy >= 0) {
            return resolvedCopy;
        }
        long id = source.retrievedCopy(n);
        if (id >= 0 && notIt.contains(id)) {
            id = -1; // refused once, never read again; look for another copy
        }
        if (id < 0) {
            id = ClusterIdentityCheck.pick(source.candidates(), source::markers, n, notIt);
        }
        if (id >= 0) {
            resolvedNamed = n;
            resolvedCopy = id;
        }
        return id;
    }

    /**
     * The genesis of the copy {@code recordingId}; a failed decode is never cached, and a refused one is never read
     * again.
     */
    long genesisOf(final long recordingId) {
        try {
            return source.genesisOf(recordingId);
        } catch (final ClusterIdentityCheck.CopyRefused refused) {
            if (resolvedCopy == recordingId && resolvedNamed != null) {
                refusedNamed = resolvedNamed;
                refusedReason = "rec " + recordingId + ": " + refused.getMessage();
            }
            forget(recordingId);
            if (notIt.add(recordingId)) {
                System.err.println("[REPLICA] CRITICAL: retrieved snapshot recording " + recordingId + " is refused by"
                        + " its content (" + refused.getMessage() + "); never read again");
            }
            throw refused;
        } catch (final RuntimeException undecodable) {
            forget(recordingId); // a copy whose decode fails is never cached
            throw undecodable;
        }
    }

    private void forget(final long recordingId) {
        if (resolvedCopy == recordingId) {
            resolvedCopy = -1;
            resolvedNamed = null;
        }
    }

    /** Why the copy of {@code n} was refused by its content, null when no copy of it was. */
    String refusalOf(final GenesisGuard.Named n) {
        return n.equals(refusedNamed) ? refusedReason : null;
    }

    /** Candidates never read again (tests). */
    Set<Long> notIt() {
        return java.util.Collections.unmodifiableSet(notIt);
    }
}
