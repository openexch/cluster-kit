// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.SnapshotEnvelope;
import com.openexchange.cluster.shadow.ShadowReplay;
import com.openexchange.cluster.shadow.ShadowStub;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerDecoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * What the replica reads from its own backup {@code RecordingLog} and archive to know it follows
 * the cluster its local shadows belong to. Read only: it never purges (the orphan purge is {@link RetrievedSetTrim}'s).
 *
 * <ul>
 *   <li>{@link #newestServiceSnapshot}: the newest valid SNAPSHOT entry of service 0 (the members' one service).</li>
 *   <li>{@link #termContradiction}: that entry's leadership term and term base against the RecordingLog's TERM entry of
 *       the same term. The backup listener does not carry the BackupResponse's last term fields (CBA:463-571 passes
 *       members, the supplier and the snapshots only), so the snapshot entries the agent appended from the response
 *       (CBA:938-951) are the response's term evidence.</li>
 *   <li>{@link #genesisOf}: the genesis of a retrieved service snapshot: a stub carries it; a full image is decoded by
 *       the engine binding through a scratch engine ({@link ReplicaEngine#imageGenesisId}).</li>
 *   <li>{@link #orphans}: recordings on the cluster log and snapshot streams that are neither the live L nor referenced
 *       by the RecordingLog, older than L and stopped (the old L and the old sets after a {@code cluster/} wipe).</li>
 * </ul>
 */
final class ClusterIdentityCheck {

    /** The members' service id (one clustered service). */
    static final int SERVICE_ID = 0;

    private ClusterIdentityCheck() {
    }

    /**
     * The copy was read in full and its content refused it (no END marker, a refused stub, or the
     * envelope or image decode failed). A stopped recording never changes, so the same bytes are refused again on every
     * read: the caller puts it in the not-the-named-copy set. An archive error (the recording could not be read) is any
     * other {@link RuntimeException} and is retried.
     */
    static final class CopyRefused extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        CopyRefused(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * The newest valid service-0 SNAPSHOT entry of the backup RecordingLog ({@code log}: the follower pass's parse),
     * null when there is none or no log.
     */
    static RecordingLog.Entry newestServiceSnapshot(final RecordingLog log) {
        if (log == null) {
            return null;
        }
        RecordingLog.Entry newest = null;
        for (final RecordingLog.Entry e : log.entries()) {
            if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == SERVICE_ID
                    && (newest == null || e.logPosition > newest.logPosition)) {
                newest = e;
            }
        }
        return newest;
    }

    /**
     * Null when consistent, else the refusal text: the RecordingLog holds a TERM entry for the snapshot's leadership
     * term with another term base (two histories behind one log).
     */
    static String termContradiction(final RecordingLog log, final RecordingLog.Entry snapshot) {
        if (snapshot == null || log == null) {
            return null;
        }
        final RecordingLog.Entry term = log.findTermEntry(snapshot.leadershipTermId);
        final RecordingLog.Entry last = log.findLastTerm();
        if (term != null && term.termBaseLogPosition != snapshot.termBaseLogPosition) {
            return "FATAL replica: the retrieved member snapshot at " + snapshot.logPosition + " names term "
                    + snapshot.leadershipTermId + " base " + snapshot.termBaseLogPosition
                    + " but the backup RecordingLog's term " + term.leadershipTermId + " starts at "
                    + term.termBaseLogPosition + " (last term " + (last == null ? "none" : last.leadershipTermId
                    + " base " + last.termBaseLogPosition) + "): two cluster histories; wipe BASE_DIR";
        }
        if (last != null && snapshot.leadershipTermId == last.leadershipTermId
                && snapshot.logPosition < last.termBaseLogPosition) {
            return "FATAL replica: the retrieved member snapshot at " + snapshot.logPosition + " of term "
                    + snapshot.leadershipTermId + " lies below the backup RecordingLog's last term base "
                    + last.termBaseLogPosition + ": two cluster histories; wipe BASE_DIR";
        }
        return null;
    }

    /**
     * The genesisId of the service snapshot {@code recordingId}: its service payload (every message after Aeron's END
     * marker, reassembled) is a stub (its own genesis field) or a full image (the binding's scratch decode).
     *
     * @throws CopyRefused      when the payload was read and refused (stub, envelope or image decode)
     * @throws RuntimeException when the recording cannot be read (an archive error, retried)
     */
    static long genesisOf(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                          final long recordingId, final long deadlineNs, final ReplicaEngine binding,
                          final int clusterId) {
        final UnsafeBuffer image = servicePayload(aeron, archive, clock, recordingId, deadlineNs);
        return genesisOfPayload(image, image.capacity(), binding, clusterId, recordingId);
    }

    /**
     * The one genesis decoder of a member copy, shared by the genesis check and the alignment compare: a stub
     * carries its own genesis; a full image is decoded by the binding through a scratch engine.
     *
     * @throws CopyRefused when the payload is refused (stub, envelope or image decode)
     */
    static long genesisOfPayload(final org.agrona.DirectBuffer image, final int length, final ReplicaEngine binding,
                                 final int clusterId, final long recordingId) {
        if (length == ShadowStub.ENVELOPED_LENGTH && SnapshotEnvelope.isEnveloped(image, length)
                && ShadowStub.peekTag(image, SnapshotEnvelope.payloadOffset()) == ShadowStub.STUB_TAG) {
            final ShadowStub.Decoded d = ShadowStub.openEnveloped(new SnapshotEnvelope(), image, length,
                    binding.engineId(), clusterId);
            if (!d.ok()) {
                throw new CopyRefused("retrieved snapshot recording " + recordingId + " is a refused stub: "
                        + d.refusal(), null);
            }
            return d.stub().genesisId();
        }
        try {
            return binding.imageGenesisId(image, length);
        } catch (final RuntimeException undecodable) {
            throw new CopyRefused("retrieved snapshot recording " + recordingId + " (" + length + " payload bytes) did"
                    + " not decode: " + undecodable, undecodable);
        }
    }

    /**
     * The service payload of the retrieved service snapshot {@code recordingId}: every message after Aeron's END
     * marker, reassembled (the bytes a member's {@code onTakeSnapshot} wrote), as a buffer of exactly that length.
     *
     * @throws CopyRefused      when the stopped recording was read in full and has no END marker
     * @throws RuntimeException when the recording cannot be read or is still recording
     */
    static UnsafeBuffer servicePayload(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                                       final long recordingId, final long deadlineNs) {
        final long[] range = {-1, -1};
        if (archive.listRecording(recordingId, (controlSessionId, correlationId, id, startTimestamp, stopTimestamp,
                startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId,
                streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            range[0] = startPosition;
            range[1] = stopPosition;
        }) == 0) {
            throw new IllegalStateException("retrieved snapshot recording " + recordingId + " is not in the archive");
        }
        if (range[1] < 0) {
            throw new IllegalStateException("retrieved snapshot recording " + recordingId + " is still recording");
        }
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final SnapshotMarkerDecoder marker = new SnapshotMarkerDecoder();
        final boolean[] ended = {false};
        final ExpandableArrayBuffer payload = new ExpandableArrayBuffer(4096);
        final int[] length = {0};
        new ShadowReplay(aeron, archive, clock).readAll(recordingId, range[0], range[1],
                (buffer, offset, len, h) -> {
                    if (!ended[0]) {
                        if (len >= MessageHeaderDecoder.ENCODED_LENGTH) {
                            header.wrap(buffer, offset);
                            if (header.schemaId() == MessageHeaderDecoder.SCHEMA_ID
                                    && header.templateId() == SnapshotMarkerDecoder.TEMPLATE_ID) {
                                marker.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                        header.blockLength(), header.version());
                                ended[0] = marker.mark() == SnapshotMark.END;
                            }
                        }
                        return;
                    }
                    payload.putBytes(length[0], buffer, offset, len);
                    length[0] += len;
                }, deadlineNs);
        if (!ended[0]) {
            throw new CopyRefused("retrieved snapshot recording " + recordingId + " has no END marker", null);
        }
        return new UnsafeBuffer(payload.byteArray(), 0, length[0]);
    }

    /**
     * The local copy of the member service snapshot a backup response named, found
     * in this archive even when the backup never recorded it in its RecordingLog (with {@code cluster/} kept the
     * supplier refuses the live-log replay at the old stop and the cycle resets before UPDATE_RECORDING_LOG). A
     * candidate is a stopped, non-empty recording on {@code streams}; it is the copy when its BEGIN snapshot marker is a
     * service snapshot ({@code ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID}) at the named log position and
     * leadership term AND its END marker is there too (a backup reset during SNAPSHOT_RETRIEVE leaves a stopped
     * copy with BEGIN and no END; it is never the copy). Aeron writes END before the service payload
     * ({@code ClusteredServiceAgent.snapshotState} then {@code service.onTakeSnapshot}), so a copy cut inside the payload
     * passes here and is refused by its decode (the envelope's length and CRC), which the caller never caches.
     *
     * <p>Newest first ({@link #pick}). A candidate read and found to be another snapshot, or a partial copy of the
     * named one (logged CRITICAL once), is added to {@code notIt} and never read again; a candidate that could not be
     * read (an archive error) is not, so it is judged again at the next resolve. A copy whose content
     * is refused later (envelope or decode, {@link CopyRefused}) joins {@code notIt} too ({@link NamedCopyResolver}).
     * This method lists the stopped, non-empty recordings on {@code streams}.</p>
     */
    static List<Long> candidates(final AeronArchive archive, final Set<Integer> streams) {
        final List<Long> candidates = new ArrayList<>();
        long from = 0;
        while (true) {
            final long[] next = {from};
            final int n = archive.listRecordings(from, 100, (controlSessionId, correlationId, id, startTimestamp,
                    stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                    mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                next[0] = Math.max(next[0], id + 1);
                if (streams.contains(streamId) && stopPosition >= 0 && stopPosition > startPosition) {
                    candidates.add(id);
                }
            });
            if (n == 0) {
                break;
            }
            from = next[0];
        }
        return candidates;
    }

    /**
     * The named-copy choice over {@code candidates} (pure: {@code markersOf} reads one recording's markers), newest
     * first, skipping {@code notIt}. See {@link #candidates} for what a candidate is and when one joins {@code notIt}.
     */
    static long pick(final List<Long> candidates, final java.util.function.LongFunction<Markers> markersOf,
                     final GenesisGuard.Named named, final Set<Long> notIt) {
        final List<Long> sorted = new ArrayList<>(candidates);
        sorted.sort(java.util.Comparator.reverseOrder());
        for (final long id : sorted) {
            if (notIt.contains(id)) {
                continue;
            }
            final Markers m = markersOf.apply(id);
            switch (judge(m, named)) {
                case IT:
                    return id;
                case PARTIAL:
                    System.err.println("[REPLICA] CRITICAL: retrieved snapshot recording " + id + " has the BEGIN"
                            + " marker of the named member snapshot at " + named.position() + " (term "
                            + named.leadershipTermId() + ") and no END marker: a partial copy (a backup reset during"
                            + " SNAPSHOT_RETRIEVE), never used");
                    // a stopped recording is never extended by the backup (each retrieval replicates into a new
                    // recording, MultipleRecordingReplication.addRecording(src, NULL_VALUE, ...)): never read again
                    notIt.add(id);
                    break;
                case UNREAD:
                    break; // an archive error: judged again at the next resolve
                default:
                    notIt.add(id);
            }
        }
        return -1;
    }

    /** What one candidate is for the named snapshot. */
    enum CopyVerdict { IT, PARTIAL, NOT_IT, UNREAD }

    /**
     * The snapshot markers of one recording: the BEGIN marker's {typeId, logPosition, leadershipTermId} and whether a
     * matching END marker follows. {@link #UNREAD} when the recording could not be read.
     */
    record Markers(long typeId, long logPosition, long leadershipTermId, boolean end) {
        static final Markers UNREAD = new Markers(Long.MIN_VALUE, -1, -1, false);
        static final Markers NONE = new Markers(Long.MIN_VALUE + 1, -1, -1, false);
    }

    /** The copy needs the named BEGIN (service type, position, term) and its END. */
    static CopyVerdict judge(final Markers m, final GenesisGuard.Named named) {
        if (m == Markers.UNREAD) {
            return CopyVerdict.UNREAD;
        }
        if (m.typeId() != io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID
                || m.logPosition() != named.position() || m.leadershipTermId() != named.leadershipTermId()) {
            return CopyVerdict.NOT_IT;
        }
        return m.end() ? CopyVerdict.IT : CopyVerdict.PARTIAL;
    }

    /** The BEGIN marker of {@code recordingId} and whether its END marker (same type, position, term) is recorded. */
    static Markers markers(final Aeron aeron, final AeronArchive archive, final NanoClock clock,
                           final long recordingId, final long deadlineNs) {
        final long[] range = {-1, -1};
        try {
            if (archive.listRecording(recordingId, (controlSessionId, correlationId, id, startTimestamp,
                    stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                    mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                range[0] = startPosition;
                range[1] = stopPosition;
            }) == 0 || range[1] < 0) {
                return Markers.UNREAD;
            }
        } catch (final RuntimeException unreadable) {
            return Markers.UNREAD;
        }
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final SnapshotMarkerDecoder marker = new SnapshotMarkerDecoder();
        final long[] begin = {Long.MIN_VALUE, -1, -1};
        final boolean[] end = {false};
        try {
            new ShadowReplay(aeron, archive, clock).readAll(recordingId, range[0], range[1],
                    (buffer, offset, len, h) -> {
                        if (end[0] || len < MessageHeaderDecoder.ENCODED_LENGTH) {
                            return;
                        }
                        header.wrap(buffer, offset);
                        if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID
                                || header.templateId() != SnapshotMarkerDecoder.TEMPLATE_ID) {
                            return;
                        }
                        marker.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(),
                                header.version());
                        if (begin[0] == Long.MIN_VALUE) {
                            if (marker.mark() == SnapshotMark.BEGIN) {
                                begin[0] = marker.typeId();
                                begin[1] = marker.logPosition();
                                begin[2] = marker.leadershipTermId();
                            }
                        } else if (marker.mark() == SnapshotMark.END && marker.typeId() == begin[0]
                                && marker.logPosition() == begin[1] && marker.leadershipTermId() == begin[2]) {
                            end[0] = true;
                        }
                    }, () -> end[0], deadlineNs);
        } catch (final RuntimeException unreadable) {
            return Markers.UNREAD;
        }
        return begin[0] == Long.MIN_VALUE ? Markers.NONE : new Markers(begin[0], begin[1], begin[2], end[0]);
    }

    /** The term check for a copy the RecordingLog names (by recording id); null when it names none. */
    static String termContradictionOf(final RecordingLog log, final long recordingId) {
        if (log == null) {
            return null;
        }
        for (final RecordingLog.Entry e : log.entries()) {
            if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.recordingId == recordingId) {
                return termContradiction(log, e);
            }
        }
        return null;
    }

    /**
     * The retrieved member service snapshots as the alignment trust reads them: the backup RecordingLog's valid
     * service-0 SNAPSHOT entries and their payloads in the local archive (follower thread, its own archive client).
     */
    static AlignmentTrust.MemberSnapshots retrieved(final Aeron aeron, final AeronArchive archive,
                                                    final NanoClock clock, final RecordingLogView view,
                                                    final long loadTimeoutNs,
                                                    final java.util.function.LongSupplier newestReported,
                                                    final java.util.function.LongSupplier retrievalProgress,
                                                    final ReplicaEngine binding, final int clusterId) {
        return new AlignmentTrust.MemberSnapshots() {
            @Override
            public long recordingId(final long position, final long leadershipTermId) {
                return entryRecording(view.get(), position, leadershipTermId);
            }

            @Override
            public long genesisOf(final org.agrona.DirectBuffer copy, final int length) {
                return genesisOfPayload(copy, length, binding, clusterId, -1);
            }

            @Override
            public long newestReported() {
                return newestReported.getAsLong();
            }

            @Override
            public long retrievalProgress() {
                return retrievalProgress.getAsLong();
            }

            @Override
            public long[] positions() {
                final RecordingLog log = view.get();
                if (log == null) {
                    return new long[0];
                }
                final java.util.TreeSet<Long> out = new java.util.TreeSet<>();
                for (final RecordingLog.Entry e : log.entries()) {
                    if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == SERVICE_ID) {
                        out.add(e.logPosition);
                    }
                }
                return out.stream().mapToLong(Long::longValue).toArray();
            }

            @Override
            public org.agrona.DirectBuffer payload(final long position) {
                return payload(position, -1);
            }

            @Override
            public long[] termsAt(final long position) {
                final java.util.TreeSet<Long> terms = new java.util.TreeSet<>();
                final RecordingLog log = view.get();
                if (log != null) {
                    for (final RecordingLog.Entry e : log.entries()) {
                        if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == SERVICE_ID
                                && e.logPosition == position) {
                            terms.add(e.leadershipTermId);
                        }
                    }
                }
                return terms.stream().mapToLong(Long::longValue).toArray();
            }

            /** By position AND term ({@code -1}: any); the last matching valid entry. */
            @Override
            public org.agrona.DirectBuffer payload(final long position, final long leadershipTermId) {
                final long recordingId = entryRecording(view.get(), position, leadershipTermId);
                if (recordingId < 0 || archive.getStopPosition(recordingId) < 0) {
                    return null;
                }
                return servicePayload(aeron, archive, clock, recordingId, clock.nanoTime() + loadTimeoutNs);
            }
        };
    }

    /** The recording of the last valid service-0 SNAPSHOT entry at {@code position} and term ({@code -1}: any). */
    static long entryRecording(final RecordingLog log, final long position, final long leadershipTermId) {
        long recordingId = -1;
        if (log != null) {
            for (final RecordingLog.Entry e : log.entries()) {
                if (e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.isValid && e.serviceId == SERVICE_ID
                        && e.logPosition == position
                        && (leadershipTermId < 0 || e.leadershipTermId == leadershipTermId)) {
                    recordingId = e.recordingId;
                }
            }
        }
        return recordingId;
    }

    /**
     * The bytes recorded so far by this archive's ACTIVE recordings on the snapshot streams (the
     * backup's member snapshot replications in flight), read from the {@code rec-pos} counters; 0 when none is active.
     * Any change between two reads is a retrieval making progress.
     */
    static long retrievalProgress(final org.agrona.concurrent.status.CountersReader counters,
                                  final Set<Integer> snapshotStreams) {
        final long[] sum = {0};
        counters.forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == io.aeron.archive.status.RecordingPos.RECORDING_POSITION_TYPE_ID
                    && snapshotStreams.contains(recPosStreamId(label))) {
                sum[0] += counters.getCounterValue(counterId);
            }
        });
        return sum[0];
    }

    /** The stream id in a {@code rec-pos: <recordingId> <sessionId> <streamId> <channel>} label, -1 when not one. */
    static int recPosStreamId(final String label) {
        if (label == null || !label.startsWith(io.aeron.archive.status.RecordingPos.NAME + ": ")) {
            return -1;
        }
        final String[] parts = label.substring(io.aeron.archive.status.RecordingPos.NAME.length() + 2).split(" ", 4);
        if (parts.length < 3) {
            return -1;
        }
        try {
            return Integer.parseInt(parts[2]);
        } catch (final NumberFormatException notOne) {
            return -1;
        }
    }

    /**
     * Stopped recordings on {@code streams} below {@code liveLog} ({@code aboveL} false, the bootstrap pass) or
     * above it ({@code aboveL} true: housekeeping, which purges one only after two passes with the backup
     * BACKING_UP, since a set the backup is retrieving right now has a higher id than L) that the RecordingLog does
     * not reference. Never L, never a shadow (another stream).
     */
    static List<Long> orphans(final AeronArchive archive, final File clusterDir, final long liveLog,
                              final Set<Integer> streams, final boolean aboveL) {
        final Set<Long> referenced = referenced(clusterDir);
        final List<Long> out = new ArrayList<>();
        long from = 0;
        while (aboveL || from < liveLog) {
            final long[] next = {from};
            final int n = archive.listRecordings(from, 100, (controlSessionId, correlationId, id, startTimestamp,
                    stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                    mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                next[0] = Math.max(next[0], id + 1);
                if ((aboveL ? id > liveLog : id < liveLog)
                        && isOrphanCandidate(id, liveLog, stopPosition, streamId, streams, referenced)) {
                    out.add(id);
                }
            });
            if (n == 0) {
                break;
            }
            from = next[0];
        }
        return out;
    }

    /**
     * Whether one listed recording is an orphan candidate: on the configured
     * cluster streams ({@code ReplicaConfig.clusterStreams()}, never a literal), stopped, not L, not referenced by the
     * backup RecordingLog. Ids above L qualify too (sets a failed backup cycle retrieved and never recorded);
     * the caller decides when an above-L candidate is old enough to purge.
     */
    static boolean isOrphanCandidate(final long id, final long liveLog, final long stopPosition, final int streamId,
                                     final Set<Integer> streams, final Set<Long> referenced) {
        return id != liveLog && stopPosition >= 0 && streams.contains(streamId) && !referenced.contains(id);
    }

    /** Every recording id the RecordingLog names, any entry type, valid or not. */
    static Set<Long> referenced(final File clusterDir) {
        final Set<Long> ids = new HashSet<>();
        if (!new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            return ids;
        }
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (final RecordingLog.Entry e : log.entries()) {
                ids.add(e.recordingId);
            }
        }
        return ids;
    }
}
