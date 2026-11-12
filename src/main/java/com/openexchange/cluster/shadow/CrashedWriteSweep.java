// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Removes the crashed writes of one producer. A {@code kill -9} in the middle of a write leaves a TOO_SHORT or
 * NO_TRAILER recording that retention never purges (it is "a crashed write the writer owns") and that the writer
 * purges only when it aborted the write itself; nothing else would ever remove it. The sweep purges, through
 * {@link ShadowRetention#purge} (the archive's one shadow purge owner), this producer's TOO_SHORT and NO_TRAILER
 * copies whose producer seq is below {@code firstProducerSeq}: the seq this process's writer was seeded with, so no
 * write of this process can be in flight on them. Never another producer's copy (evidence for its operator), never a
 * usable copy, once per recording, on a scan that can answer. A refusal is logged once per recording and retried on
 * the next sweep.
 *
 * <p>The seq comes from the header when one was written, else from the alias (a crash before the header was
 * recorded leaves a header-less TOO_SHORT recording whose alias still carries the seq). One instance per host
 * process, used on the host's housekeeping thread only; {@link #purgedTotal()} is volatile for the exporter.</p>
 */
public final class CrashedWriteSweep {

    private final ShadowProducer producer;
    private final String logPrefix;
    private final Consumer<String> log;
    private final ShadowCounters counters;
    private final Set<NegativeControl> off;
    private final Set<Long> swept = new HashSet<>();
    private final Set<Long> refusedLogged = new HashSet<>();
    private volatile long purgedTotal;

    /**
     * @param producer  this host's producer identity (kind and id)
     * @param logPrefix the host's log prefix, e.g. {@code [REPLICA-HK]}
     * @param log       the host's line sink ({@code System.out::println} in production)
     * @param counters  the host's shadow counters ({@code shadow_crashed_writes_purged_total}), or null
     */
    public CrashedWriteSweep(final ShadowProducer producer, final String logPrefix, final Consumer<String> log,
                             final ShadowCounters counters) {
        this(producer, logPrefix, log, counters, EnumSet.noneOf(NegativeControl.class));
    }

    CrashedWriteSweep(final ShadowProducer producer, final String logPrefix, final Consumer<String> log,
                      final ShadowCounters counters, final Set<NegativeControl> off) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.logPrefix = Objects.requireNonNull(logPrefix, "logPrefix");
        this.log = Objects.requireNonNull(log, "log");
        this.counters = counters;
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
    }

    /**
     * One sweep over {@code scan}. Does nothing when the scan cannot answer, {@code firstProducerSeq <= 0} (the writer
     * is not armed yet) or there is no retention owner.
     *
     * @return the number of recordings purged by this call
     */
    public int sweep(final ShadowCatalog.Scan scan, final long firstProducerSeq, final ShadowRetention retention) {
        if (scan == null || !scan.known() || firstProducerSeq <= 0 || retention == null) {
            return 0;
        }
        int purged = 0;
        for (final ShadowEntry e : scan.all()) {
            if (e.state() != ShadowEntry.State.TOO_SHORT && e.state() != ShadowEntry.State.NO_TRAILER) {
                continue;
            }
            final ShadowAlias a = e.alias();
            if (a == null || swept.contains(e.recordingId())) {
                continue;
            }
            final boolean own = a.producerKind() == producer.kind().code() && a.producerId() == producer.producerId();
            if (!own && !off.contains(NegativeControl.SWEEP_OTHER_PRODUCERS)) {
                continue;
            }
            final long seq = e.header() != null ? e.header().producerSeq() : a.producerSeq();
            if (seq >= firstProducerSeq) {
                continue;
            }
            final ShadowCatalog.PurgeResult r = retention.purge(e.recordingId(), false);
            final String who = String.valueOf(ProducerKind.of(a.producerKind()) == null ? '?'
                    : ProducerKind.of(a.producerKind()).letter()) + a.producerId();
            if (r.purged()) {
                swept.add(e.recordingId()); // the cached scan may list it until the next re-scan
                purged++;
                purgedTotal++;
                if (counters != null) {
                    counters.increment(ShadowCounters.Counter.CRASHED_WRITES_PURGED_TOTAL);
                }
                log.accept(logPrefix + " purged a crashed write of " + who + " from before this process: rec="
                        + e.recordingId() + " seq=" + seq + " state=" + e.state() + " P=" + a.logPosition());
            } else if (refusedLogged.add(e.recordingId())) {
                log.accept(logPrefix + " crashed write rec=" + e.recordingId() + " of " + who + " not purged: "
                        + r.detail() + " (code " + r.code() + ", retried next pass)");
            }
        }
        return purged;
    }

    /** {@code shadow_crashed_writes_purged_total} of this process. */
    public long purgedTotal() {
        return purgedTotal;
    }

    public ShadowProducer producer() {
        return producer;
    }
}
