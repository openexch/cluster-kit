// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.shadow.ShadowCounters;
import com.openexchange.cluster.shadow.ShadowKind;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Plain Prometheus exposition of the replica: its own longs, the shadow {@link ShadowCounters} and the
 * {@code ShadowHousekeeping.Counters}. Reads volatile fields only, from the endpoint's HTTP thread; never touches the
 * engine. Commit-vs-applied lag needs the leader's commit position, which the replica cannot see between queries:
 * a monitor reads it from members.
 */
public final class ReplicaMetrics {

    private final ReplicaFollower follower;
    private final BackupEvents events;
    private final ReplicaReadiness readiness;
    private final ShadowCounters shadowCounters;
    private final LongSupplier backupState;
    private final NanoClock clock;
    private final EpochClock wall;
    private volatile ReplicaHousekeeping housekeeping;
    private volatile LongSupplier selfCheckMsLast = () -> -1;

    public ReplicaMetrics(final ReplicaFollower follower, final BackupEvents events, final ReplicaReadiness readiness,
                          final ShadowCounters shadowCounters, final LongSupplier backupState, final NanoClock clock,
                          final EpochClock wall) {
        this.follower = Objects.requireNonNull(follower, "follower");
        this.events = Objects.requireNonNull(events, "events");
        this.readiness = Objects.requireNonNull(readiness, "readiness");
        this.shadowCounters = Objects.requireNonNull(shadowCounters, "shadowCounters");
        this.backupState = Objects.requireNonNull(backupState, "backupState");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.wall = Objects.requireNonNull(wall, "wall");
    }

    public void housekeeping(final ReplicaHousekeeping housekeeping) {
        this.housekeeping = housekeeping;
    }

    /** The host's self-check timing ({@code shadow_selfcheck_ms_last}). */
    public void selfCheckMsLast(final LongSupplier supplier) {
        this.selfCheckMsLast = Objects.requireNonNull(supplier, "supplier");
    }

    public String body() {
        final StringBuilder sb = new StringBuilder(4096);
        final long applied = follower.appliedPosition();
        final long recorded = follower.recordedPosition();
        line(sb, "shadow_replica_applied_position", applied);
        line(sb, "shadow_replica_recorded_position", recorded);
        line(sb, "shadow_replica_internal_lag_bytes", applied < 0 || recorded < 0 ? -1 : Math.max(0, recorded - applied));
        final long act = follower.appliedClusterTime();
        line(sb, "shadow_replica_time_lag_ms", act < 0 ? -1 : wall.time() - act);
        line(sb, "shadow_replica_backup_state", backupState.getAsLong());
        line(sb, "shadow_replica_backup_queries_total", events.queries());
        line(sb, "shadow_replica_backup_responses_total", events.responses());
        line(sb, "shadow_replica_backup_resets_total", events.resets());
        line(sb, "shadow_replica_backup_auth_rejected_total", events.authRejected());
        line(sb, "shadow_replica_backup_replay_failures_total", events.replayFailures());
        line(sb, "shadow_replica_backup_replay_below_start_total", events.replayBelowStart());
        line(sb, "shadow_replica_rebootstrap_total", follower.rebootstrapTotal());
        line(sb, "shadow_replica_rebootstrap_not_taken_total", follower.rebootstrapNotTakenTotal());
        final com.openexchange.cluster.shadow.ShadowPuller puller = follower.lastPuller();
        if (puller != null) {
            for (final com.openexchange.cluster.shadow.ShadowPuller.Counter c
                    : com.openexchange.cluster.shadow.ShadowPuller.Counter.values()) {
                line(sb, c.metricName(), puller.counters().get(c));
            }
            puller.counters().refusedByReason().forEach((reason, n) ->
                    line(sb, "shadow_pull_refused_total{reason=\"" + reason + "\"}", n));
        }
        line(sb, "shadow_replica_live_recording_alive", follower.liveRecordingAlive() ? 1 : 0);
        line(sb, "shadow_replica_reader_reattach_total", follower.reattachTotal());
        line(sb, "replica_attach_attempts_total", follower.attachAttemptsTotal());
        line(sb, "replica_attach_failures_total", follower.attachFailuresTotal());
        line(sb, "shadow_replica_restore_path", readiness.restorePath().code());
        line(sb, "shadow_replica_restore_attempts", follower.restoreAttempts());
        line(sb, "shadow_replica_restored_position", follower.restoredPosition());
        line(sb, "shadow_replica_fenced", follower.fenced() == null ? 0 : 1);
        line(sb, "shadow_replica_supplier_member_id", events.supplierMemberId());
        line(sb, "shadow_replica_supplier_is_leader", events.supplierIsLeader() ? 1 : 0);
        line(sb, "shadow_replica_terms_known", follower.termIndex().size());
        final ShadowTriggerPolicy p = follower.policy();
        long newestWrite = -1;
        for (final ShadowKind k : new ShadowKind[] {ShadowKind.ALIGNED, ShadowKind.STANDBY, ShadowKind.LOCAL}) {
            line(sb, "shadow_trigger_total{kind=\"" + k + "\"}", p == null ? 0 : p.triggerTotal(k));
            line(sb, "shadow_trigger_skipped_busy_total{kind=\"" + k + "\"}", p == null ? 0 : p.skippedBusyTotal(k));
            line(sb, "shadow_last_position{kind=\"" + k + "\"}", p == null ? -1 : p.lastPosition(k));
        }
        if (p != null) {
            newestWrite = p.lastWriteCompletedNs();
        }
        line(sb, "shadow_trigger_fallback_total", p == null ? 0 : p.fallbackTotal());
        line(sb, "shadow_trigger_unidentified_total", p == null ? 0 : p.unidentifiedTotal());
        line(sb, "shadow_last_age_ms", newestWrite < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - newestWrite));
        line(sb, "shadow_serialize_ms_last", p == null ? -1 : p.serializeMsLast());
        line(sb, "shadow_aligned_blocked_ms_last", p == null ? -1 : p.alignedBlockedMsLast());
        line(sb, "shadow_selfcheck_ms_last", selfCheckMsLast.getAsLong());
        final ReplicaHousekeeping hk = housekeeping;
        line(sb, "shadow_retrieved_sets", hk == null ? -1 : hk.retrievedSets());
        line(sb, "shadow_retrieved_sets_purged_total", hk == null ? 0 : hk.retrievedSetsPurgedTotal());
        line(sb, "shadow_retrieved_bytes", hk == null ? -1 : hk.retrievedBytes());
        line(sb, "shadow_log_purge_position", hk == null ? -1 : hk.logPurgePosition());
        line(sb, "shadow_replica_housekeeping_errors_total", hk == null ? 0 : hk.errorsTotal());
        line(sb, "shadow_replica_crashed_writes_purged_total", hk == null ? 0 : hk.crashedWritesPurgedTotal());
        line(sb, "shadow_replica_orphan_recordings", follower.orphanRecordings().size());
        final AlignmentTrust trust = follower.trust();
        line(sb, "shadow_replica_verified", trust != null && trust.trusted() ? 1 : 0);
        line(sb, "shadow_align_compares_total", trust == null ? 0 : trust.comparesTotal());
        line(sb, "shadow_align_equal_total", trust == null ? 0 : trust.equalTotal());
        line(sb, "shadow_align_unverifiable_total", trust == null ? 0 : trust.unverifiableTotal());
        line(sb, "shadow_align_superseded_total", trust == null ? 0 : trust.supersededTotal());
        line(sb, "shadow_align_stub_compares_total", trust == null ? 0 : trust.stubComparesTotal());
        line(sb, "shadow_align_ambiguous_total", trust == null ? 0 : trust.ambiguousTotal());
        line(sb, "shadow_align_compare_ms_last", trust == null ? -1 : trust.compareMsLast());
        line(sb, "shadow_align_verified_position", trust == null ? -1 : trust.verifiedAt());
        line(sb, "shadow_align_held", p != null && p.holding() ? 1 : 0);
        line(sb, "shadow_align_verify_timeouts_total", p == null ? 0 : p.verifyTimeouts());
        line(sb, "shadow_trigger_untrusted_total", p == null ? 0 : p.untrustedTotal());
        line(sb, "shadow_trigger_after_fence_total", p == null ? 0 : p.afterFenceDroppedTotal());
        // the live oracle (the trust's compare verdicts and the mismatch hold); match_total is equal_total by name
        line(sb, "shadow_oracle_match_total", trust == null ? 0 : trust.equalTotal());
        line(sb, "shadow_oracle_mismatch_total", trust == null ? 0 : trust.mismatchTotal());
        line(sb, "shadow_oracle_unreadable_total", trust == null ? 0 : trust.unreadableTotal());
        line(sb, "shadow_oracle_other_genesis_total", trust == null ? 0 : trust.otherGenesisTotal());
        line(sb, "shadow_oracle_pre_adopt_total", trust == null ? 0 : trust.preAdoptTotal());
        line(sb, "shadow_oracle_stale_named_total", trust == null ? 0 : trust.staleNamedTotal());
        line(sb, "shadow_oracle_mismatch_position", trust == null ? -1 : trust.mismatchAt());
        line(sb, "shadow_oracle_mismatch_latched", follower.holding() ? 1 : 0);
        line(sb, "shadow_oracle_hold_ms", follower.heldMs());
        line(sb, "shadow_oracle_dump_incomplete", follower.dumpIncomplete() ? 1 : 0);
        line(sb, "shadow_poisoned_purged_total", hk == null ? 0 : hk.poisonedPurgedTotal());
        line(sb, "shadow_align_actions_ring", trust == null ? 0 : trust.actionsRing());
        for (final Map.Entry<String, Long> e : shadowCounters.snapshot().entrySet()) {
            line(sb, e.getKey(), e.getValue());
        }
        if (hk != null) {
            for (final Map.Entry<String, Long> e : hk.counters().snapshot().entrySet()) {
                line(sb, e.getKey(), e.getValue());
            }
        }
        return sb.toString();
    }

    private static void line(final StringBuilder sb, final String name, final long value) {
        sb.append(name).append(' ').append(value).append('\n');
    }
}
