// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.ClusterBackupEventsListener;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.RecordingLog;
import org.agrona.concurrent.NanoClock;

import java.util.List;
import java.util.Objects;

/**
 * {@link ClusterBackupEventsListener} into volatile fields and counters. Called on the backup agent's
 * thread; read by the follower, the readiness probe and {@code /metrics}. The failure kinds are kept apart: an
 * {@code AUTHENTICATION_REJECTED} is {@code backup_auth_rejected_total}, never "no supplier"; an archive error on the
 * supplier side (a refused or failed replay) is {@code backup_replay_failures_total}. An optional delegate (a test's
 * listener) is called after the fields are updated.
 */
public final class BackupEvents implements ClusterBackupEventsListener {

    /** The text CBA:869 and CBA:911 start a supplier replay refusal with (MAX_REPLAYS, a pruned position, ...). */
    static final String LIVE_LOG_REPLAY_FAILED = "Live log replay failed";

    private final NanoClock clock;
    private final ClusterBackupEventsListener delegate;

    private volatile long queries;
    private volatile long responses;
    private volatile long resets;
    private volatile long authRejected;
    private volatile long replayFailures;
    private volatile long otherFailures;
    private volatile boolean authRejectedSinceResponse;
    private volatile String lastFailure;
    private volatile long lastFailureNs = -1;
    private volatile int supplierMemberId = -1;
    private volatile boolean supplierIsLeader;
    private volatile boolean supplierWasLeaderEver;
    private volatile int leaderMemberId = -1;
    private volatile long lastResponseNs = -1;
    private volatile long snapshotsToRetrieve;
    /** The newest service-0 member snapshot position a response named, -1 before (retrieved or not). */
    private volatile long newestReportedServiceSnapshot = -1;
    /** The newest service-0 member snapshot a response named (kept when a later response names none). */
    private volatile GenesisGuard.Named newestNamed;
    /** The local recording id of each retrieved service-0 snapshot, keyed by its position and term. */
    private final java.util.concurrent.ConcurrentHashMap<String, Long> retrievedCopies =
            new java.util.concurrent.ConcurrentHashMap<>();
    private volatile long recordingLogUpdates;
    private volatile long snapshotsRetrieved;
    private volatile long liveRecordingId = -1;
    private volatile int liveCounterId = -1;
    private volatile long liveLogPosition = -1;
    private volatile long lastLiveProgressNs = -1;

    public BackupEvents(final NanoClock clock) {
        this(clock, null);
    }

    public BackupEvents(final NanoClock clock, final ClusterBackupEventsListener delegate) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.delegate = delegate;
    }

    @Override
    public void onBackupQuery() {
        queries++;
        if (delegate != null) {
            delegate.onBackupQuery();
        }
    }

    @Override
    public void onPossibleFailure(final Exception ex) {
        resets++;
        lastFailure = String.valueOf(ex);
        lastFailureNs = clock.nanoTime();
        final String text = String.valueOf(ex.getMessage());
        if (text.contains("AUTHENTICATION_REJECTED")) {
            authRejected++;
            authRejectedSinceResponse = true;
            System.err.println("[REPLICA-BACKUP] AUTHENTICATION_REJECTED by a member (#" + authRejected + "): " + text
                    + " (a REPLICA credential missing from the members' key table, or a second backup query inside"
                    + " a member's backup window)");
        } else if (ex instanceof ArchiveException || text.contains(LIVE_LOG_REPLAY_FAILED)) {
            // the supplier's archive refused or failed the bounded replay: Aeron 1.53.1 wraps that refusal in a
            // ClusterException "Live log replay failed ..." (CBA:869, CBA:909-913), not an ArchiveException
            replayFailures++;
            System.err.println("[REPLICA-BACKUP] supplier archive failure (#" + replayFailures + "): " + ex);
        } else {
            otherFailures++;
            System.err.println("[REPLICA-BACKUP] backup reset: " + ex);
        }
        if (delegate != null) {
            delegate.onPossibleFailure(ex);
        }
    }

    @Override
    public void onBackupResponse(final ClusterMember[] clusterMembers, final ClusterMember logSourceMember,
                                 final List<RecordingLog.Snapshot> snapshotsToRetrieve) {
        responses++;
        lastResponseNs = clock.nanoTime();
        authRejectedSinceResponse = false;
        if (logSourceMember != null) {
            supplierMemberId = logSourceMember.id();
            supplierIsLeader = logSourceMember.isLeader();
            supplierWasLeaderEver |= supplierIsLeader;
        }
        if (clusterMembers != null) {
            for (final ClusterMember m : clusterMembers) {
                if (m.isLeader()) {
                    leaderMemberId = m.id();
                }
            }
        }
        this.snapshotsToRetrieve = snapshotsToRetrieve == null ? 0 : snapshotsToRetrieve.size();
        if (snapshotsToRetrieve != null) {
            RecordingLog.Snapshot newest = null;
            for (final RecordingLog.Snapshot s : snapshotsToRetrieve) {
                if (s.serviceId() == ClusterIdentityCheck.SERVICE_ID) {
                    if (s.logPosition() > newestReportedServiceSnapshot) {
                        newestReportedServiceSnapshot = s.logPosition();
                    }
                    if (newest == null || s.logPosition() > newest.logPosition()) {
                        newest = s;
                    }
                }
            }
            final GenesisGuard.Named was = newestNamed;
            if (newest != null && (was == null || was.position() != newest.logPosition()
                    || was.leadershipTermId() != newest.leadershipTermId())) {
                newestNamed = new GenesisGuard.Named(newest.logPosition(), newest.leadershipTermId(),
                        newest.termBaseLogPosition());
            }
        }
        if (delegate != null) {
            delegate.onBackupResponse(clusterMembers, logSourceMember, snapshotsToRetrieve);
        }
    }

    @Override
    public void onUpdatedRecordingLog(final RecordingLog recordingLog,
                                      final List<RecordingLog.Snapshot> snapshotsRetrieved) {
        recordingLogUpdates++;
        this.snapshotsRetrieved += snapshotsRetrieved == null ? 0 : snapshotsRetrieved.size();
        if (snapshotsRetrieved != null) {
            for (final RecordingLog.Snapshot s : snapshotsRetrieved) {
                if (s.serviceId() == ClusterIdentityCheck.SERVICE_ID) {
                    retrievedCopies.put(key(s.logPosition(), s.leadershipTermId()), s.recordingId());
                }
            }
        }
        if (delegate != null) {
            delegate.onUpdatedRecordingLog(recordingLog, snapshotsRetrieved);
        }
    }

    @Override
    public void onLiveLogProgress(final long recordingId, final long recordingPosCounterId, final long logPosition) {
        liveRecordingId = recordingId;
        liveCounterId = (int)recordingPosCounterId;
        liveLogPosition = logPosition;
        lastLiveProgressNs = clock.nanoTime();
        if (delegate != null) {
            delegate.onLiveLogProgress(recordingId, recordingPosCounterId, logPosition);
        }
    }

    /**
     * The newest member service snapshot position a BackupResponse named, -1 before. A failed backup
     * cycle never records it in the RecordingLog, so the alignment trust reads it here to start its verify wait.
     */
    public long newestReportedServiceSnapshot() {
        return newestReportedServiceSnapshot;
    }

    /** The newest service-0 member snapshot a backup response named, null before. */
    GenesisGuard.Named newestNamed() {
        return newestNamed;
    }

    /** The local recording id the backup recorded for the retrieved snapshot at {@code position} of a term, -1. */
    long retrievedCopy(final long position, final long leadershipTermId) {
        return retrievedCopies.getOrDefault(key(position, leadershipTermId), -1L);
    }

    private static String key(final long position, final long leadershipTermId) {
        return position + ":" + leadershipTermId;
    }

    public long queries() {
        return queries;
    }

    public long responses() {
        return responses;
    }

    /** {@code onPossibleFailure} calls: each is followed by a backup reset (CBA:303-312). */
    public long resets() {
        return resets;
    }

    public long authRejected() {
        return authRejected;
    }

    public long replayFailures() {
        return replayFailures;
    }

    public long otherFailures() {
        return otherFailures;
    }

    public boolean authRejectedSinceResponse() {
        return authRejectedSinceResponse;
    }

    public String lastFailure() {
        return lastFailure;
    }

    public long lastFailureNs() {
        return lastFailureNs;
    }

    public int supplierMemberId() {
        return supplierMemberId;
    }

    public boolean supplierIsLeader() {
        return supplierIsLeader;
    }

    /** Whether any response in this process named the leader as the log supplier (the FOLLOWER supplier assertion). */
    public boolean supplierWasLeaderEver() {
        return supplierWasLeaderEver;
    }

    public int leaderMemberId() {
        return leaderMemberId;
    }

    public long lastResponseNs() {
        return lastResponseNs;
    }

    public long snapshotsToRetrieve() {
        return snapshotsToRetrieve;
    }

    public long recordingLogUpdates() {
        return recordingLogUpdates;
    }

    public long snapshotsRetrieved() {
        return snapshotsRetrieved;
    }

    /** L as the backup last reported it, -1 before the first {@code onLiveLogProgress}. */
    public long liveRecordingId() {
        return liveRecordingId;
    }

    public int liveCounterId() {
        return liveCounterId;
    }

    public long liveLogPosition() {
        return liveLogPosition;
    }

    public long lastLiveProgressNs() {
        return lastLiveProgressNs;
    }
}
