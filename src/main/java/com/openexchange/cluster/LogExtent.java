// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import org.agrona.ErrorHandler;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Where a member's cluster log recording starts and how far it reaches, read from an archive: the member's own
 * (over its local control channel) or a peer's (over the peer's archive control endpoint, the fifth endpoint of
 * the cluster member string).
 *
 * <p>A log purge moves the recording's start position forward. That start position is the one fact about retention
 * every member can read from every other member, whoever leads: it lives in the archive catalog on disk, not in a
 * consensus module's memory, so it survives a leader change and a restart.</p>
 */
public final class LogExtent {
    /** Stream id of archive control requests on a peer's endpoint (Aeron's default, the one the members use). */
    private static final int REMOTE_CONTROL_STREAM_ID = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;

    /**
     * How long one read of a peer archive's log start may take (100..60000 ms): the follower's read before a purge
     * and the catch-up watchdog's read after failed attempts.
     */
    public static final String PROBE_TIMEOUT_ENV = "RETENTION_PROBE_TIMEOUT_MS";
    public static final String PROBE_TIMEOUT_PROP = "retention.probe.timeout.ms";
    public static final long DEFAULT_PROBE_TIMEOUT_MS = 5_000;
    public static final long MIN_PROBE_TIMEOUT_MS = 100;
    public static final long MAX_PROBE_TIMEOUT_MS = 60_000;

    /** The setting, range checked: a value outside the range refuses the start. */
    public static long probeTimeoutMs() {
        return EngineLifecycle.rangedSetting(PROBE_TIMEOUT_ENV, PROBE_TIMEOUT_PROP, DEFAULT_PROBE_TIMEOUT_MS,
                MIN_PROBE_TIMEOUT_MS, MAX_PROBE_TIMEOUT_MS);
    }

    public final long recordingId;
    public final long startPosition;
    /** The stop position, or the recorded position of a live recording; -1 when it could not be read. */
    public final long position;
    public final int streamId;

    LogExtent(final long recordingId, final long startPosition, final long position, final int streamId) {
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.position = position;
        this.streamId = streamId;
    }

    @Override
    public String toString() {
        return "recording " + recordingId + " [" + startPosition + ", " + position + ")";
    }

    /**
     * This member's own log recording: the recording of the last term in {@code clusterDir}'s recording log, read
     * through {@code archive} (a client of the member's own archive).
     *
     * @throws IllegalStateException when the recording log names no log recording or the archive does not hold it
     */
    public static LogExtent local(final AeronArchive archive, final File clusterDir) {
        final long recordingId;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            recordingId = recordingLog.findLastTermRecordingId();
        }
        if (recordingId < 0) {
            throw new IllegalStateException("no cluster log recording in " + clusterDir);
        }
        final LogExtent extent = byId(archive, recordingId);
        if (extent == null) {
            throw new IllegalStateException("log recording " + recordingId + " is not in the archive");
        }
        return extent;
    }

    /**
     * How far this member's own log reaches: the position of its log recording. A member whose recording log names
     * no log recording at all (new or wiped directories, or seeded with a snapshot and nothing after it) has no log:
     * its position is that of its newest valid snapshot, or 0 without one, which is where it asks the leader to
     * replay from.
     *
     * @throws IllegalStateException when the recording log names a log recording the archive does not hold
     */
    public static long localPosition(final AeronArchive archive, final File clusterDir) {
        long newestSnapshot = 0;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            if (recordingLog.findLastTermRecordingId() >= 0) {
                return local(archive, clusterDir).position;
            }
            for (final RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid) {
                    newestSnapshot = Math.max(newestSnapshot, entry.logPosition);
                }
            }
        }
        return newestSnapshot;
    }

    /** The recording {@code recordingId} of {@code archive}, or null when the archive does not hold it. */
    public static LogExtent byId(final AeronArchive archive, final long recordingId) {
        final long[] found = {-1, -1, -1};
        final int count = archive.listRecording(recordingId, (controlSessionId, correlationId, id, startTimestamp,
                stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            found[0] = startPosition;
            found[1] = stopPosition;
            found[2] = streamId;
        });
        if (count == 0) {
            return null;
        }
        long position = found[1];
        if (position == AeronArchive.NULL_POSITION) {
            position = archive.getRecordingPosition(recordingId);
        }
        return new LogExtent(recordingId, found[0], position, (int)found[2]);
    }

    /**
     * The log recording of the archive behind {@code archive} when its id is not known: among the recordings on
     * {@code logStreamId}, the live one (no stop position) with the highest id, else the one that reaches furthest.
     * A leader's log recording is live. Null when the archive holds no recording on that stream.
     */
    public static LogExtent byStream(final AeronArchive archive, final int logStreamId) {
        final long[] best = {-1, -1, -1};
        final boolean[] bestLive = {false};
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", logStreamId, (controlSessionId, correlationId, id,
                startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength,
                termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                sourceIdentity) -> {
            final boolean live = stopPosition == AeronArchive.NULL_POSITION;
            final boolean better = best[0] < 0
                    || (live && !bestLive[0])
                    || (live == bestLive[0] && (live ? id > best[0] : stopPosition > best[2]));
            if (better) {
                best[0] = id;
                best[1] = startPosition;
                best[2] = stopPosition;
                bestLive[0] = live;
            }
        });
        if (best[0] < 0) {
            return null;
        }
        return new LogExtent(best[0], best[1], bestLive[0] ? -1 : best[2], logStreamId);
    }

    /**
     * A client context for a peer's archive: requests to {@code archiveEndpoint}, responses to a free port on
     * {@code responseHost} (this member's own address as the peers know it).
     *
     * @param aeron        the client to run on, or null to let the archive client open (and own) one on
     *                     {@code aeronDirectoryName}; both work with an embedded driver and an external one
     * @param errorHandler for a client this context opens; never Aeron's default, which exits the JVM on a driver
     *                     timeout
     */
    public static AeronArchive.Context remoteContext(final Aeron aeron, final String aeronDirectoryName,
                                                     final String archiveEndpoint, final String responseHost,
                                                     final long timeoutMs, final ErrorHandler errorHandler) {
        final AeronArchive.Context context = new AeronArchive.Context()
                .controlRequestChannel("aeron:udp?endpoint=" + archiveEndpoint)
                .controlRequestStreamId(REMOTE_CONTROL_STREAM_ID)
                .controlResponseChannel("aeron:udp?endpoint=" + responseHost + ":0")
                .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(timeoutMs))
                .errorHandler(errorHandler);
        if (aeron != null) {
            context.aeron(aeron).ownsAeronClient(false);
        } else {
            context.aeronDirectoryName(aeronDirectoryName);
        }
        return context;
    }

    /** The host part of a {@code host:port} endpoint. */
    public static String hostOf(final String endpoint) {
        final int colon = endpoint.lastIndexOf(':');
        return colon < 0 ? endpoint : endpoint.substring(0, colon);
    }
}
