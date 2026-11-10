// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.RecordingLog;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The retrieved member snapshot sets: group the backup {@code RecordingLog}'s valid SNAPSHOT
 * entries by {@code logPosition}, keep the newest {@code REPLICA_RETRIEVED_SETS_KEEP}, and purge every recording of
 * an older set. The RecordingLog itself is never edited: the backup agent is its only writer (CBA:916-976), 1.53.1
 * has only {@code invalidateLatestSnapshot()} (RL:804), and the agent reads only {@code getLatestSnapshot(serviceId)}
 * (CBA:502-507), so dangling older entries affect nothing it does. A reseed from the replica dir must use the newest
 * set only.
 *
 * <p>{@link #purgeOrphans} purges the recordings a {@code cluster/} wipe orphaned (found once at
 * bootstrap by {@code ClusterIdentityCheck.orphans}), re-checked before each purge.</p>
 *
 * <p>This class and {@code ShadowRetention} are the only purge call sites of the replica package (allow-listed by
 * name in {@code ShadowPurgeCallSiteTest}). Archive codes are surfaced; {@code ACTIVE_RECORDING} is retried on the
 * next pass.</p>
 */
public final class RetrievedSetTrim {

    /** One pass: what is kept and what this pass purged. */
    public record Pass(int setsPresent, long bytesPresent, int setsPurged, List<Long> purgedRecordings,
                       List<String> refusals, List<Long> keptPositions) {
    }

    private final int keep;
    private final Set<Long> countedPositions = new HashSet<>();
    private long setsPurgedTotal;

    public RetrievedSetTrim(final int keep) {
        if (keep < 1) {
            throw new IllegalArgumentException("REPLICA_RETRIEVED_SETS_KEEP must be >= 1: " + keep);
        }
        this.keep = keep;
    }

    /** The valid SNAPSHOT entries of the backup RecordingLog, newest position first (read side, as AH:148-155). */
    static TreeMap<Long, List<RecordingLog.Entry>> sets(final File clusterDir) {
        final TreeMap<Long, List<RecordingLog.Entry>> byPosition = new TreeMap<>(java.util.Comparator.reverseOrder());
        if (!new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            return byPosition;
        }
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid) {
                    byPosition.computeIfAbsent(e.logPosition, k -> new ArrayList<>()).add(e);
                }
            }
        }
        return byPosition;
    }

    /**
     * @param purge false for the SET_TRIM_OFF control: measure, purge nothing
     */
    public Pass apply(final File clusterDir, final AeronArchive archive, final boolean purge) {
        final TreeMap<Long, List<RecordingLog.Entry>> sets = sets(clusterDir);
        int index = 0;
        int present = 0;
        long bytes = 0;
        int purgedSets = 0;
        final List<Long> purged = new ArrayList<>();
        final List<String> refusals = new ArrayList<>();
        final List<Long> kept = new ArrayList<>();
        for (final Map.Entry<Long, List<RecordingLog.Entry>> set : sets.entrySet()) {
            final boolean older = index++ >= keep;
            int remaining = 0;
            long setBytes = 0;
            for (final RecordingLog.Entry e : set.getValue()) {
                final long length = length(archive, e.recordingId);
                if (length < 0) {
                    continue; // not in the archive (purged earlier)
                }
                if (older && purge) {
                    try {
                        archive.purgeRecording(e.recordingId);
                        purged.add(e.recordingId);
                        continue;
                    } catch (final ArchiveException refused) {
                        refusals.add("rec=" + e.recordingId + " P=" + set.getKey() + " code=" + refused.errorCode()
                                + (refused.errorCode() == ArchiveException.ACTIVE_RECORDING ? " (retried next pass)"
                                : "") + ": " + refused.getMessage());
                    }
                }
                remaining++;
                setBytes += length;
            }
            if (remaining > 0) {
                present++;
                bytes += setBytes;
                if (!older) {
                    kept.add(set.getKey());
                }
            } else if (older && countedPositions.add(set.getKey())) {
                purgedSets++;
            }
        }
        setsPurgedTotal += purgedSets;
        for (final String r : refusals) {
            System.err.println("[REPLICA-HK] retrieved set purge refused: " + r);
        }
        if (!purged.isEmpty()) {
            System.out.println("[REPLICA-HK] retrieved member snapshot sets trimmed to the newest " + keep
                    + ": purged recordings " + purged + " (RecordingLog entries left in place)");
        }
        return new Pass(present, bytes, purgedSets, List.copyOf(purged), List.copyOf(refusals), List.copyOf(kept));
    }

    public long setsPurgedTotal() {
        return setsPurgedTotal;
    }

    /**
     * Purges the bootstrap's orphan candidates (the old L and the old retrieved sets after a
     * {@code cluster/} wipe; the replica owns its archive). Each id is re-checked first: still neither the
     * live L nor referenced by the RecordingLog, still on a cluster stream, still stopped. Returns the ids that are
     * gone (purged now, or already absent); a refused purge stays for the next pass, with its archive code.
     */
    public List<Long> purgeOrphans(final File clusterDir, final AeronArchive archive, final long liveLog,
                                   final java.util.Collection<Long> candidates, final Set<Integer> streams) {
        final List<Long> gone = new ArrayList<>();
        if (candidates.isEmpty()) {
            return gone;
        }
        final Set<Long> referenced = ClusterIdentityCheck.referenced(clusterDir);
        for (final long id : candidates) {
            final int[] stream = {-1};
            final long[] stop = {-1};
            if (archive.listRecording(id, (controlSessionId, correlationId, rid, startTimestamp, stopTimestamp,
                    startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                    sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                stream[0] = streamId;
                stop[0] = stopPosition;
            }) == 0) {
                gone.add(id);
                continue;
            }
            if (!ClusterIdentityCheck.isOrphanCandidate(id, liveLog, stop[0], stream[0], streams, referenced)) {
                System.err.println("[REPLICA-HK] orphan candidate rec=" + id + " is no longer an orphan (L=" + liveLog
                        + " referenced=" + referenced.contains(id) + " stop=" + stop[0] + " stream=" + stream[0]
                        + "): kept");
                gone.add(id); // dropped from the candidates, never purged
                continue;
            }
            try {
                archive.purgeRecording(id);
                gone.add(id);
                System.out.println("[REPLICA-HK] purged orphan recording rec=" + id + " (stream " + stream[0]
                        + ", not referenced by the backup RecordingLog, L=" + liveLog + ")");
            } catch (final ArchiveException refused) {
                System.err.println("[REPLICA-HK] orphan purge refused: rec=" + id + " code=" + refused.errorCode()
                        + " (retried next pass): " + refused.getMessage());
            }
        }
        return gone;
    }

    private static long length(final AeronArchive archive, final long recordingId) {
        final long[] d = {-1};
        final int found = archive.listRecording(recordingId,
                (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, startPosition, stopPosition,
                 initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                 strippedChannel, originalChannel, sourceIdentity) ->
                        d[0] = stopPosition < 0 ? 0 : stopPosition - startPosition);
        return found == 0 ? -1 : d[0];
    }
}
