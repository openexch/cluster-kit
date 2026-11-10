// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.log.LogCursor;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;

/**
 * A verified load of one shadow into a host buffer. The loader never calls {@code restore}: the host
 * runs, in this order and all mandatory, {@link ShadowIdentity#check} under its policy, {@code container.restore(
 * payload, length, header.logPosition, cursor)} (EQUAL) or with {@code replayFrom = cursor(P_cm)} (SKIP), then
 * {@link ShadowIdentity#matches} against the RESTORED engine; a failure discards the container and the engine. A
 * {@link LoadedShadow} is not trusted until that last step returned true. The load is a deep read: a pass stores
 * VERIFIED, a failure CORRUPT, so the catalog agrees. One owner thread per instance.
 */
public final class ShadowImageLoader {

    /** The payload at {@code into[0, length)} and the cursor the header carries. */
    public record LoadedShadow(ShadowHeader header, DirectBuffer payload, int length, LogCursor cursor) {
    }

    public record LoadResult(Kind kind, LoadedShadow loaded, String reason, int code) {
        public enum Kind { LOADED, REFUSED, REFUSED_TOO_LARGE, CORRUPT, SCAN_ERROR }

        public boolean ok() {
            return kind == Kind.LOADED;
        }
    }

    private final ShadowReplay replay;
    private final ShadowVerdicts verdicts;
    private final NanoClock clock;
    private final long readTimeoutNs;
    private final ShadowCounters counters;

    public ShadowImageLoader(final Aeron aeron, final AeronArchive archive, final ShadowVerdicts verdicts,
                             final NanoClock clock, final long readTimeoutNs, final ShadowCounters counters) {
        this.replay = new ShadowReplay(aeron, archive, clock);
        this.verdicts = Objects.requireNonNull(verdicts, "verdicts");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (readTimeoutNs <= 0) {
            throw new IllegalArgumentException("readTimeoutNs must be > 0: " + readTimeoutNs);
        }
        this.readTimeoutNs = readTimeoutNs;
        this.counters = Objects.requireNonNull(counters, "counters");
    }

    /**
     * @param entry a COMPLETE or VERIFIED entry from a scan of the same archive
     * @param into  the host buffer; the payload lands at 0
     */
    public LoadResult load(final ShadowEntry entry, final MutableDirectBuffer into) {
        if (!entry.state().usable() || entry.header() == null) {
            return new LoadResult(LoadResult.Kind.REFUSED, null, "state " + entry.state(), -1);
        }
        final ShadowVerdicts.Verdict known = verdicts.get(entry.key());
        if (known != null && known.kind() == ShadowVerdicts.Kind.CORRUPT) {
            return new LoadResult(LoadResult.Kind.CORRUPT, null, known.reason(), -1);
        }
        final ShadowHeader h = entry.header();
        if (h.payloadLength() > into.capacity()) {
            return new LoadResult(LoadResult.Kind.REFUSED_TOO_LARGE, null,
                    "payload " + h.payloadLength() + " bytes, buffer " + into.capacity(), -1);
        }
        final DeepRead.Outcome o = DeepRead.read(replay, entry, into, clock.nanoTime() + readTimeoutNs, counters);
        final ShadowCatalog.DeepResult r = ShadowCatalog.record(verdicts, counters, entry, o);
        return switch (r.kind()) {
            case VERIFIED -> new LoadResult(LoadResult.Kind.LOADED, new LoadedShadow(h,
                    new UnsafeBuffer(into, 0, (int)h.payloadLength()), (int)h.payloadLength(),
                    new LogCursor(h.logPosition(), h.clusterTime(), h.leadershipTermId(), h.termBaseLogPosition())),
                    null, -1);
            case CORRUPT -> new LoadResult(LoadResult.Kind.CORRUPT, null, r.reason(), -1);
            default -> new LoadResult(LoadResult.Kind.SCAN_ERROR, null, r.reason(), r.code());
        };
    }
}
