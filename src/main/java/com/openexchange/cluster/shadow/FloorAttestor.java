// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Publishes this member's {@link FloorAttestation} in its own archive, on the housekeeping (pruner) thread, after
 * every catalog scan: the one way a member's deep verdicts leave its process. Every member runs it whatever its role
 * (a statement about the local archive is true on a leader too; only the current leader reads the others').
 *
 * <ul>
 *   <li>At arming (the first step of an instance, whatever the scan says) every marker found in the archive is
 *       retracted: it was written by an earlier process, whose verdicts are gone.</li>
 *   <li>Only a scan that can answer feeds it ({@code scan.known()}): VERIFIED then means a deep read of this
 *       process.</li>
 *   <li>The stated facts are read from VERIFIED copies only, ALIGNED for the witnesses ({@link ShadowFloor}'s own
 *       witness rule), and never from a position in CONFLICT (the scan lists none as verified).</li>
 *   <li>A marker is written when the facts changed; then every other marker in this archive is purged (one marker per
 *       archive: an older statement of this member, or a peer's marker that came with a copied archive directory).</li>
 *   <li>When no position is witnessed any more (a copy read CORRUPT after a restart, an operator purge), the marker is
 *       retracted: purged, nothing written.</li>
 * </ul>
 *
 * <p>The purge here touches recordings on {@link FloorAttestation#STREAM_ID} only, never a shadow: the shadows keep
 * their one purge owner ({@link ShadowRetention}). A failed step is logged and counted, and the next poll starts over
 * from the archive's listing (nothing is remembered that the archive does not show, C4). Never throws.</p>
 */
public final class FloorAttestor {

    /** What a scan states: the witnessed anchor, its witnesses and the margin position (-1: none). */
    public record Facts(long anchor, int witnesses, long margin) {
    }

    private final AeronArchive archive;
    private final int engineId;
    private final int clusterId;
    private final long genesisId;
    private final int memberId;
    private final int minWitnesses;
    private final NanoClock clock;
    private final long timeoutNs;
    private final ShadowHousekeeping.Counters counters;
    private final Set<NegativeControl> off;
    private Facts lastObserved;
    private boolean settled;
    private boolean cleared;

    /**
     * @param archive      a client of this member's own archive, used on the housekeeping thread only
     * @param genesisId    the recorded genesis (above 0: the attestor is built after the ADOPT)
     * @param memberId     this member's cluster member id
     * @param minWitnesses the fewest distinct producers a position needs before it is attested: 2 under
     *                     {@code two-producers}, 1 under {@code any-verified}
     * @param timeoutNs    the bound of each archive wait of one marker write
     */
    public FloorAttestor(final AeronArchive archive, final int engineId, final int clusterId, final long genesisId,
                         final int memberId, final int minWitnesses, final NanoClock clock, final long timeoutNs,
                         final ShadowHousekeeping.Counters counters) {
        this(archive, engineId, clusterId, genesisId, memberId, minWitnesses, clock, timeoutNs, counters,
                EnumSet.noneOf(NegativeControl.class));
    }

    FloorAttestor(final AeronArchive archive, final int engineId, final int clusterId, final long genesisId,
                  final int memberId, final int minWitnesses, final NanoClock clock, final long timeoutNs,
                  final ShadowHousekeeping.Counters counters, final Set<NegativeControl> off) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.engineId = engineId;
        this.clusterId = clusterId;
        if (genesisId <= 0 || memberId < 0 || minWitnesses < 1 || timeoutNs <= 0) {
            throw new IllegalArgumentException("attestor: genesisId " + genesisId + ", memberId " + memberId
                    + ", minWitnesses " + minWitnesses + ", timeoutNs " + timeoutNs);
        }
        this.genesisId = genesisId;
        this.memberId = memberId;
        this.minWitnesses = minWitnesses;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.timeoutNs = timeoutNs;
        this.counters = Objects.requireNonNull(counters, "counters");
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
        counters.set(ShadowHousekeeping.Counter.ATTESTED_ANCHOR_POSITION, -1);
    }

    /**
     * The facts of {@code scan}, or null when no position has {@code minWitnesses} witnesses. Pure.
     *
     * @param readUsable the negative control: COMPLETE copies count like VERIFIED ones
     */
    static Facts facts(final Scan scan, final int minWitnesses, final boolean readUsable) {
        final List<LogicalShadow> source = readUsable ? scan.usable() : scan.verified();
        LogicalShadow anchor = null;
        int witnesses = 0;
        for (final LogicalShadow v : source) {
            final Set<String> producers = new TreeSet<>();
            ShadowFloor.witnessCopies(v, false, producers);
            final int w = readUsable ? usableProducers(v) : producers.size();
            if (w >= minWitnesses && (anchor == null || v.logPosition() > anchor.logPosition())) {
                anchor = v;
                witnesses = w;
            }
        }
        if (anchor == null) {
            return null;
        }
        long margin = -1;
        for (final LogicalShadow v : source) {
            if (v.logPosition() < anchor.logPosition()) {
                margin = Math.max(margin, v.logPosition());
            }
        }
        return new Facts(anchor.logPosition(), witnesses, margin);
    }

    private static int usableProducers(final LogicalShadow v) {
        final Set<String> producers = new TreeSet<>();
        for (final ShadowEntry e : v.copies()) {
            if (e.state().usable() && e.header() != null && e.header().kind() == ShadowKind.ALIGNED.code()) {
                producers.add(e.producer());
            }
        }
        return producers.size();
    }

    /** One step after a catalog scan (class Javadoc). Never throws. */
    public void observe(final Scan scan) {
        if (!cleared && !off.contains(NegativeControl.ATTEST_KEEPS_STALE)) {
            // a marker in this archive now was written by an earlier process: its verdicts died with that process,
            // and this one has verified nothing yet (its startup pass may find a copy CORRUPT)
            try {
                final int stale = retractAll(archive, engineId);
                cleared = true;
                if (stale > 0) {
                    counters.add(ShadowHousekeeping.Counter.ATTESTATIONS_RETRACTED_TOTAL, stale);
                    System.out.println("[SHADOW-ATTEST] member " + memberId + " retracted " + stale + " marker(s) of"
                            + " an earlier process at arming: nothing is attested before this process verified it");
                }
            } catch (final RuntimeException failed) {
                counters.increment(ShadowHousekeeping.Counter.ATTEST_ERRORS_TOTAL);
                System.err.println("[SHADOW-ATTEST] member " + memberId + ": the stale markers could not be"
                        + " retracted, tried again at the next scan: " + failed);
                return;
            }
        }
        if (scan == null || (!scan.known() && !off.contains(NegativeControl.ATTEST_UNKNOWN_SCAN))) {
            return;
        }
        final Facts facts = facts(scan, minWitnesses, off.contains(NegativeControl.ATTEST_READS_USABLE));
        if (settled && Objects.equals(facts, lastObserved)) {
            return;
        }
        try {
            settle(facts);
            lastObserved = facts;
            settled = true;
        } catch (final RuntimeException failed) {
            settled = false;
            counters.increment(ShadowHousekeeping.Counter.ATTEST_ERRORS_TOTAL);
            System.err.println("[SHADOW-ATTEST] member " + memberId + ": the attestation step failed, tried again at"
                    + " the next scan: " + failed);
        }
    }

    /** Makes the archive's markers state exactly {@code facts}: one marker of this member, or none. */
    private void settle(final Facts facts) {
        final List<long[]> ids = new ArrayList<>();
        final List<FloorAttestation> parsed = new ArrayList<>();
        list(archive, engineId, ids, parsed);
        long maxSeq = 0;
        int current = -1;
        for (int i = 0; i < ids.size(); i++) {
            final FloorAttestation a = parsed.get(i);
            if (a == null || !mine(a)) {
                continue;
            }
            maxSeq = Math.max(maxSeq, a.seq());
            if (facts != null && a.anchor() == facts.anchor() && a.witnesses() == facts.witnesses()
                    && a.margin() == facts.margin() && ids.get(i)[1] != AeronArchive.NULL_POSITION
                    && (current < 0 || a.seq() > parsed.get(current).seq())) {
                current = i;
            }
        }
        long keep = current < 0 ? -1 : ids.get(current)[0];
        if (facts != null && current < 0) {
            final FloorAttestation next = new FloorAttestation(engineId, clusterId, genesisId, memberId,
                    facts.anchor(), facts.witnesses(), facts.margin(), maxSeq + 1);
            keep = write(next);
            counters.increment(ShadowHousekeeping.Counter.ATTESTATIONS_TOTAL);
            System.out.println("[SHADOW-ATTEST] member " + memberId + " attests anchor=" + facts.anchor()
                    + " witnesses=" + facts.witnesses() + " margin=" + facts.margin() + " (seq " + next.seq()
                    + ", rec=" + keep + "): VERIFIED ALIGNED copies in this archive");
        }
        for (final long[] id : ids) {
            if (id[0] != keep && id[1] != AeronArchive.NULL_POSITION) {
                archive.purgeRecording(id[0]);
                if (facts == null) {
                    counters.increment(ShadowHousekeeping.Counter.ATTESTATIONS_RETRACTED_TOTAL);
                    System.out.println("[SHADOW-ATTEST] member " + memberId + " retracts its attestation rec=" + id[0]
                            + ": no position of this archive has " + minWitnesses + " VERIFIED ALIGNED witness(es)"
                            + " any more");
                }
            }
        }
        counters.set(ShadowHousekeeping.Counter.ATTESTED_ANCHOR_POSITION, facts == null ? -1 : facts.anchor());
    }

    /**
     * Purges every stopped marker recording of {@code engineId} in {@code archive}'s archive; returns how many. For a
     * host whose attestor is not built (the remote floor switched off, a mode without shadows): a marker must never
     * outlive the process that verified what it states. A marker still being recorded is left (no reader accepts it).
     *
     * @throws RuntimeException an archive refusal: the caller tries again at its next wake
     */
    public static int retractAll(final AeronArchive archive, final int engineId) {
        final List<long[]> ids = new ArrayList<>();
        list(archive, engineId, ids, new ArrayList<>());
        int purged = 0;
        for (final long[] id : ids) {
            if (id[1] != AeronArchive.NULL_POSITION) {
                archive.purgeRecording(id[0]);
                purged++;
            }
        }
        return purged;
    }

    private boolean mine(final FloorAttestation a) {
        return a.engineId() == engineId && a.clusterId() == clusterId && a.genesisId() == genesisId
                && a.memberId() == memberId;
    }

    /** Every marker recording of the engine in this archive: {@code {recordingId, stopPosition}} and its parse. */
    private static void list(final AeronArchive archive, final int engineId, final List<long[]> ids,
                             final List<FloorAttestation> parsed) {
        long from = 0;
        while (true) {
            final int before = ids.size();
            final int n = archive.listRecordingsForUri(from, ShadowCatalog.DEFAULT_LIST_BATCH,
                    FloorAttestation.prefix(engineId), FloorAttestation.STREAM_ID,
                    (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                     stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                     strippedChannel, originalChannel, sourceIdentity) -> {
                        ids.add(new long[] {recordingId, stopPosition});
                        parsed.add(FloorAttestation.tryParse(originalChannel));
                    });
            if (n < ShadowCatalog.DEFAULT_LIST_BATCH || ids.size() == before) {
                return;
            }
            from = ids.get(ids.size() - 1)[0] + 1;
        }
    }

    /** Records the (empty) marker and waits for its stop; returns its recording id. */
    private long write(final FloorAttestation attestation) {
        final long deadline = clock.nanoTime() + timeoutNs;
        final CountersReader aeronCounters = archive.context().aeron().countersReader();
        final ExclusivePublication publication = archive.addRecordedExclusivePublication(attestation.channel(),
                FloorAttestation.STREAM_ID);
        long recordingId = -1;
        try {
            int counterId;
            while ((counterId = RecordingPos.findCounterIdBySession(aeronCounters, publication.sessionId(),
                    archive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
                await(deadline, "the marker's recording did not start");
            }
            recordingId = RecordingPos.getRecordingId(aeronCounters, counterId);
        } finally {
            try {
                archive.stopRecording(publication);
            } finally {
                CloseHelper.quietClose(publication);
            }
        }
        final long id = recordingId;
        final long[] stop = {AeronArchive.NULL_POSITION};
        while (true) {
            archive.listRecording(id, (controlSessionId, correlationId, rid, startTimestamp, stopTimestamp,
                                       startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                       mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                       sourceIdentity) -> stop[0] = stopPosition);
            if (stop[0] != AeronArchive.NULL_POSITION) {
                return id;
            }
            await(deadline, "the marker rec=" + id + " did not stop");
        }
    }

    private void await(final long deadline, final String what) {
        if (clock.nanoTime() - deadline >= 0) {
            throw new IllegalStateException(what + " within its bound");
        }
        try {
            Thread.sleep(1);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(what + ": interrupted");
        }
    }
}
