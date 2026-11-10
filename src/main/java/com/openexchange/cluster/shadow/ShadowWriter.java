// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.log.LogCursor;
import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * The synchronous shadow write: one recorded {@link ExclusivePublication} on
 * {@code aeron:ipc?term-length=<T>|alias=<alias>}, stream {@link ShadowAlias#STREAM_ID}: the header, the enveloped
 * engine image in chunks of {@code maxMessageLength}, and, only after the archive recorded every chunk, the trailer;
 * then {@code stopRecording} and the catalog's stop position awaited. Every wait has a deadline from the injected
 * clock and polls the abort flag ({@link #requestAbort}); every result is a named {@link Reason}; an
 * {@link ArchiveException} keeps its code (C8). No latch: every write is independent (C9). The writer leaves no open
 * publication after any result.
 *
 * <p>Never on a cluster service thread in production (G6): hosts run it on {@link ShadowWriterThread}. Never launches
 * a driver, never schedules, holds no {@code Cluster}. Positions, term and cluster time come from the {@link LogCursor}
 * the host captured after the frame at P_s; identity values from the engine's {@link ShadowIdentitySource} read at the
 * same instant; no clock value reaches a header field.</p>
 */
public final class ShadowWriter {

    public static final long MIN_TERM_LENGTH = 64 * 1024;
    public static final long MAX_TERM_LENGTH = 16 * 1024 * 1024;
    public static final long DEFAULT_CONNECT_TIMEOUT_NS = 10_000_000_000L;
    public static final long DEFAULT_OFFER_TIMEOUT_NS = 30_000_000_000L;
    public static final long DEFAULT_RECORD_TIMEOUT_NS = 60_000_000_000L;

    public enum Reason { OK, INVALID_CURSOR, UNIDENTIFIED, NOT_MONOTONE, HEADER_FIELD_INVALID, SELF_CHECK_FAILED,
        SELF_CHECK_MUTATED_BUFFER, ARCHIVE_REFUSED, CONNECT_TIMEOUT, ABORTED, RECORD_TIMEOUT, STOP_TIMEOUT,
        STOP_MISMATCH, UNEXPECTED }

    /** Why an in-flight write was aborted (7.4 W7). */
    public enum AbortCause { NOT_CONNECTED, CLOSED, MAX_POSITION_EXCEEDED, OFFER_TIMEOUT, ABORT_REQUESTED,
        RECORDING_STOPPED }

    /** The W7 cleanup's purge, inside an ABORTED / UNEXPECTED / CONNECT_TIMEOUT result. */
    public record Purge(Kind kind, int code, String detail) {
        public enum Kind { NOT_ATTEMPTED, PURGED, PURGE_REFUSED, STOP_NOT_SEEN }

        static final Purge NOT_ATTEMPTED = new Purge(Kind.NOT_ATTEMPTED, -1, null);
    }

    public record WriteResult(Reason reason, String detail, AbortCause abortCause, int archiveCode, long recordingId,
                              String alias, long bytes, long offerNs, long recordNs, Purge purge) {
        public boolean ok() {
            return reason == Reason.OK;
        }

        static WriteResult refused(final Reason reason, final String detail) {
            return new WriteResult(reason, detail, null, -1, -1, null, 0, 0, 0, Purge.NOT_ATTEMPTED);
        }

        @Override
        public String toString() {
            return reason + (abortCause == null ? "" : "(" + abortCause + ")")
                    + (archiveCode >= 0 ? "(" + archiveCode + ")" : "") + (detail == null ? "" : " " + detail)
                    + (recordingId >= 0 ? " rec=" + recordingId : "")
                    + (purge.kind() == Purge.Kind.NOT_ATTEMPTED ? "" : " purge=" + purge.kind()
                    + (purge.code() >= 0 ? "(" + purge.code() + ")" : ""));
        }
    }

    /** Package fault seams for the 11.4-11.7 tests (W4 of the contract); production uses {@link #NONE}. */
    interface Faults {
        Faults NONE = new Faults() { };

        default ExclusivePublication addPublication(final AeronArchive archive, final String channel,
                                                    final int streamId) {
            return archive.addRecordedExclusivePublication(channel, streamId);
        }

        /** @param messageIndex 0 = header, 1..n chunks, n + 1 trailer */
        default long offer(final ExclusivePublication publication, final DirectBuffer buffer, final int offset,
                           final int length, final int messageIndex) {
            return publication.offer(buffer, offset, length);
        }

        default void beforePurge(final long recordingId) {
        }

        /** Called right before the trailer is offered with the archive's recorded position at that moment. */
        default void onTrailerOffer(final long recordedPosition, final long publicationPosition) {
        }

        /** Called after the trailer is recorded, right before {@code stopRecording} (W10). */
        default void beforeStopRecording() {
        }

        /** The recorded position the writer sees (a test simulates a stalled recorder). */
        default long recordedPosition(final long actual) {
            return actual;
        }

        /** The catalog stop position the writer sees (a test simulates the archive writing it late). */
        default long stopPosition(final AeronArchive archive, final long recordingId) {
            return archive.getStopPosition(recordingId);
        }
    }

    private static final class Abort extends RuntimeException {
        final AbortCause cause;

        Abort(final AbortCause cause, final String message) {
            super(message, null, false, false);
            this.cause = cause;
        }
    }

    private static final class Timeout extends RuntimeException {
        final Reason reason;

        Timeout(final Reason reason, final String message) {
            super(message, null, false, false);
            this.reason = reason;
        }
    }

    private final Aeron aeron;
    private final AeronArchive archive;
    private final ShadowProducer producer;
    private final ShadowSelfCheck selfCheck;
    private final String ipcTermLength;
    private final int termLength;
    private final int chunkLength;
    private final NanoClock clock;
    private final long connectTimeoutNs;
    private final long offerTimeoutNs;
    private final long recordTimeoutNs;
    private final ShadowCounters counters;
    private final Faults faults;
    private final Set<NegativeControl> off;
    // An abort names the write in flight; with none in flight it is a no-op (the next write is not lost)
    private final java.util.concurrent.atomic.AtomicLong inFlight = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong abortedWrite = new java.util.concurrent.atomic.AtomicLong();
    private long writeIds;
    private final IdleStrategy idle = new BackoffIdleStrategy(10, 20, 1_000, 1_000_000);
    private final UnsafeBuffer trailerBuffer = new UnsafeBuffer(new byte[ShadowTrailer.LENGTH]);
    private long producerSeq;
    private long lastWrittenPosition;

    /**
     * @param ipcTermLength              {@code SHADOW_TERM_LENGTH}, {@code 64k}..{@code 16m} (every reader on
     *                                   every host maps 3 x T per replay)
     * @param initialProducerSeq         {@code max(producerSeq) + 1} of this producer's local copies, 1 when none (7.2)
     * @param initialLastWrittenPosition {@code max(logPosition)} of this producer's local copies, -1 when none
     */
    public ShadowWriter(final Aeron aeron, final AeronArchive archive, final ShadowProducer producer,
                        final ShadowSelfCheck selfCheck, final String ipcTermLength, final NanoClock clock,
                        final long connectTimeoutNs, final long offerTimeoutNs, final long recordTimeoutNs,
                        final long initialProducerSeq, final long initialLastWrittenPosition,
                        final ShadowCounters counters) {
        this(aeron, archive, producer, selfCheck, ipcTermLength, clock, connectTimeoutNs, offerTimeoutNs,
                recordTimeoutNs, initialProducerSeq, initialLastWrittenPosition, counters, Faults.NONE,
                EnumSet.noneOf(NegativeControl.class));
    }

    ShadowWriter(final Aeron aeron, final AeronArchive archive, final ShadowProducer producer,
                 final ShadowSelfCheck selfCheck, final String ipcTermLength, final NanoClock clock,
                 final long connectTimeoutNs, final long offerTimeoutNs, final long recordTimeoutNs,
                 final long initialProducerSeq, final long initialLastWrittenPosition, final ShadowCounters counters,
                 final Faults faults, final Set<NegativeControl> off) {
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.archive = Objects.requireNonNull(archive, "archive");
        this.producer = Objects.requireNonNull(producer, "producer");
        this.selfCheck = Objects.requireNonNull(selfCheck, "selfCheck");
        if (selfCheck == ShadowSelfCheck.UNCHECKED_TEST_ONLY && !ShadowSelfCheck.uncheckedAllowed()) {
            throw new IllegalArgumentException("ShadowSelfCheck.UNCHECKED_TEST_ONLY needs -D"
                    + ShadowSelfCheck.UNCHECKED_PROPERTY + "=true: a production writer without a self-check is a"
                    + " configuration error");
        }
        final long t = parseSize(Objects.requireNonNull(ipcTermLength, "ipcTermLength"));
        if (t < MIN_TERM_LENGTH || t > MAX_TERM_LENGTH || Long.bitCount(t) != 1) {
            throw new IllegalArgumentException("SHADOW_TERM_LENGTH " + ipcTermLength + " outside [64k, 16m] or not a"
                    + " power of two (every replay maps 3 x the recording's term length)");
        }
        this.ipcTermLength = ipcTermLength;
        this.termLength = (int)t;
        this.chunkLength = FrameDescriptor.computeMaxMessageLength(termLength);
        this.clock = Objects.requireNonNull(clock, "clock");
        if (connectTimeoutNs <= 0 || offerTimeoutNs <= 0 || recordTimeoutNs <= 0) {
            throw new IllegalArgumentException("timeouts must be > 0");
        }
        this.connectTimeoutNs = connectTimeoutNs;
        this.offerTimeoutNs = offerTimeoutNs;
        this.recordTimeoutNs = recordTimeoutNs;
        if (initialProducerSeq < 1) {
            throw new IllegalArgumentException("initialProducerSeq must be >= 1: " + initialProducerSeq);
        }
        this.producerSeq = initialProducerSeq;
        this.lastWrittenPosition = initialLastWrittenPosition;
        this.counters = Objects.requireNonNull(counters, "counters");
        this.faults = Objects.requireNonNull(faults, "faults");
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
    }

    static long parseSize(final String s) {
        final String v = s.trim().toLowerCase(java.util.Locale.ROOT);
        long mult = 1;
        String digits = v;
        if (v.endsWith("k")) {
            mult = 1024;
            digits = v.substring(0, v.length() - 1);
        } else if (v.endsWith("m")) {
            mult = 1024 * 1024;
            digits = v.substring(0, v.length() - 1);
        } else if (v.endsWith("g")) {
            mult = 1024L * 1024 * 1024;
            digits = v.substring(0, v.length() - 1);
        }
        try {
            return Math.multiplyExact(Long.parseLong(digits), mult);
        } catch (final NumberFormatException | ArithmeticException e) {
            throw new IllegalArgumentException("not a size: " + s, e);
        }
    }

    public ShadowProducer producer() {
        return producer;
    }

    /** {@code publication.maxMessageLength()} for {@code SHADOW_TERM_LENGTH}: the chunk length every write uses. */
    public int chunkLength() {
        return chunkLength;
    }

    public long nextProducerSeq() {
        return producerSeq;
    }

    public long lastWrittenPosition() {
        return lastWrittenPosition;
    }

    /**
     * Polled by every wait loop: the write in flight ends ABORTED(ABORT_REQUESTED). With no write in flight it does
     * nothing, so it never aborts a later write; {@code ShadowWriterThread.close} repeats it while it
     * joins.
     */
    public void requestAbort() {
        final long id = inFlight.get();
        if (id != 0) {
            abortedWrite.set(id);
        }
    }

    private boolean aborted() {
        final long id = inFlight.get();
        return id != 0 && abortedWrite.get() == id;
    }

    // ------------------------------------------------------------------------------------------------------ write

    public WriteResult write(final ShadowImageBuffer image, final int engineId, final ShadowKind kind,
                             final LogCursor at, final ShadowIdentitySource id) {
        inFlight.set(++writeIds);
        try {
            final WriteResult r = doWrite(image, engineId, kind, at, id);
            if (r.ok()) {
                counters.incrementWriteTotal(kind);
                counters.set(ShadowCounters.Counter.WRITE_BYTES_LAST, r.bytes());
                counters.set(ShadowCounters.Counter.WRITE_OFFER_NS_LAST, r.offerNs());
                counters.set(ShadowCounters.Counter.WRITE_RECORD_NS_LAST, r.recordNs());
                counters.set(ShadowCounters.Counter.LAST_WRITE_COMPLETED_NS, clock.nanoTime());
            } else {
                counters.incrementWriteFailed(r.reason());
                switch (r.reason()) {
                    case ABORTED -> counters.increment(ShadowCounters.Counter.WRITE_ABORTED_TOTAL);
                    case RECORD_TIMEOUT, STOP_TIMEOUT -> counters.increment(ShadowCounters.Counter.WRITE_TIMEOUTS_TOTAL);
                    default -> { }
                }
                if (r.abortCause() == AbortCause.OFFER_TIMEOUT) {
                    counters.increment(ShadowCounters.Counter.WRITE_OFFER_TIMEOUTS_TOTAL);
                }
            }
            return r;
        } finally {
            inFlight.set(0);
        }
    }

    private WriteResult doWrite(final ShadowImageBuffer image, final int engineId, final ShadowKind kind,
                                final LogCursor at, final ShadowIdentitySource id) {
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(at, "at");
        Objects.requireNonNull(id, "id");
        final int payloadLength = image.payloadLength();
        // W0
        if (!off.contains(NegativeControl.CURSOR_CHECK_OFF) && (payloadLength <= 0 || at.logPosition() < 0
                || at.clusterTime() < 0 || at.leadershipTermId() < 0 || at.termBaseLogPosition() < 0)) {
            return WriteResult.refused(Reason.INVALID_CURSOR, "payloadLength=" + payloadLength + " cursor=" + at);
        }
        // W1, read once: the source is the engine's live state
        final long genesisId = id.genesisId();
        final boolean genesisOff = off.contains(NegativeControl.GENESIS_CHECK_OFF);
        if (genesisId <= 0 && !genesisOff) {
            counters.increment(ShadowCounters.Counter.WRITE_SKIPPED_UNIDENTIFIED_TOTAL);
            return WriteResult.refused(Reason.UNIDENTIFIED, "genesisId=" + genesisId + " (no genesis adopted yet)");
        }
        // W2
        if (at.logPosition() <= lastWrittenPosition && !off.contains(NegativeControl.MONOTONE_CHECK_OFF)) {
            counters.increment(ShadowCounters.Counter.WRITE_SKIPPED_NOT_MONOTONE_TOTAL);
            return WriteResult.refused(Reason.NOT_MONOTONE, "P_s=" + at.logPosition() + " lastWritten="
                    + lastWrittenPosition);
        }
        // W3
        final ShadowHeader draft = new ShadowHeader(engineId, producer.clusterId(), kind == null ? 0 : kind.code(),
                producer.kind().code(), producer.producerId(), producerSeq, genesisId, at.logPosition(),
                at.leadershipTermId(), at.termBaseLogPosition(), at.clusterTime(), id.journalEnabled() ? 1 : 0,
                id.journalSeq(), payloadLength, 0, chunkLength, id.payloadLayoutHint());
        final ShadowHeader.Problem problem = draft.validate(ShadowHeader.Options.forEngine(engineId));
        final boolean cursorOff = off.contains(NegativeControl.CURSOR_CHECK_OFF) && problem != null
                && ("clusterTime".equals(problem.field()) || "term".equals(problem.field())
                || "logPosition".equals(problem.field()));
        if (problem != null && !cursorOff && !(genesisOff && problem.reason() == ShadowHeader.Reason.UNIDENTIFIED)) {
            return WriteResult.refused(problem.reason() == ShadowHeader.Reason.UNIDENTIFIED
                    ? Reason.UNIDENTIFIED : Reason.HEADER_FIELD_INVALID, problem.toString());
        }
        final String alias;
        try {
            alias = genesisOff ? draft.alias().format(ShadowAlias.engineToken(engineId),
                    producer.kind().letter(), false) : draft.alias().format();
        } catch (final IllegalArgumentException | IllegalStateException e) {
            return WriteResult.refused(Reason.HEADER_FIELD_INVALID, "alias: " + e.getMessage());
        }
        final DirectBuffer payload = image.payloadView();
        final boolean selfCheckLate = off.contains(NegativeControl.SELF_CHECK_AFTER_FIRST_OFFER);
        // W4
        // The image's identity is the SHA-256 payload digest, computed before and after the self-check
        final long digestBefore = digestOf(payload, payloadLength);
        if (!selfCheckLate) {
            final WriteResult refused = selfCheck(payload, payloadLength, digestBefore);
            if (refused != null) {
                return refused;
            }
        }
        // W5
        final ShadowHeader header = new ShadowHeader(draft.engineId(), draft.clusterId(), draft.kind(),
                draft.producerKind(), draft.producerId(), draft.producerSeq(), draft.genesisId(), draft.logPosition(),
                draft.leadershipTermId(), draft.termBaseLogPosition(), draft.clusterTime(), draft.journalEnabled(),
                draft.journalSeq(), draft.payloadLength(), digestBefore, draft.chunkLength(), draft.payloadLayoutHint());
        final int headerCrc = header.encode(image.buffer(), 0);
        ShadowTrailer.of(header, headerCrc).encode(trailerBuffer, 0);
        final String channel = ShadowAlias.channel(alias, ipcTermLength);
        // W6
        final ExclusivePublication publication;
        try {
            publication = faults.addPublication(archive, channel, ShadowAlias.STREAM_ID);
        } catch (final ArchiveException ae) {
            if (off.contains(NegativeControl.ARCHIVE_EXCEPTION_RELABELLED)) {
                return WriteResult.refused(Reason.UNEXPECTED, ae.toString());
            }
            return new WriteResult(Reason.ARCHIVE_REFUSED, ae.getMessage(), null, ae.errorCode(), -1, alias, 0, 0, 0,
                    Purge.NOT_ATTEMPTED);
        } catch (final RuntimeException re) {
            return WriteResult.refused(Reason.UNEXPECTED, re.toString());
        }
        long recordingId = -1;
        int counterId = CountersReader.NULL_COUNTER_ID;
        boolean stopped = false;
        try {
            final CountersReader counters = aeron.countersReader();
            final long connectDeadline = clock.nanoTime() + connectTimeoutNs;
            try {
                while ((counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
                        archive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
                    awaitStep(connectDeadline, Reason.CONNECT_TIMEOUT, "recording counter");
                }
                recordingId = RecordingPos.getRecordingId(counters, counterId);
                while (!publication.isConnected()) {
                    awaitStep(connectDeadline, Reason.CONNECT_TIMEOUT, "recorded publication to connect");
                }
            } catch (final Timeout t) {
                return cleanup(publication, channel, recordingId, Reason.CONNECT_TIMEOUT, null, t.getMessage(), alias);
            } catch (final Abort a) {
                return cleanup(publication, channel, recordingId, Reason.ABORTED, a.cause, a.getMessage(), alias);
            }
            if (publication.maxMessageLength() != chunkLength) {
                return cleanup(publication, channel, recordingId, Reason.UNEXPECTED, null, "publication"
                        + " maxMessageLength " + publication.maxMessageLength() + " != " + chunkLength, alias);
            }
            // W7
            final long offerStart = clock.nanoTime();
            final int chunks = (int)header.chunkCount();
            final long recordDeadline;
            try {
                offer(publication, image.buffer(), 0, ShadowHeader.LENGTH, 0);
                if (selfCheckLate) {
                    final WriteResult refused = selfCheck(payload, payloadLength, digestBefore);
                    if (refused != null) {
                        return cleanup(publication, channel, recordingId, refused.reason(), null, refused.detail(),
                                alias);
                    }
                }
                for (int i = 0; i < chunks; i++) {
                    final int offset = ShadowHeader.LENGTH + i * chunkLength;
                    final int length = Math.min(chunkLength, payloadLength - i * chunkLength);
                    offer(publication, image.buffer(), offset, length, 1 + i);
                }
            } catch (final Abort a) {
                return cleanup(publication, channel, recordingId, Reason.ABORTED, a.cause, a.getMessage(), alias);
            }
            final long offerNs = clock.nanoTime() - offerStart;
            // W8
            final long recordStart = clock.nanoTime();
            recordDeadline = recordStart + recordTimeoutNs;
            boolean trailerOffered = false;
            try {
                awaitRecorded(publication, counterId, recordingId, recordDeadline);
                // W9
                faults.onTrailerOffer(counters.getCounterValue(counterId), publication.position());
                offer(publication, trailerBuffer, 0, ShadowTrailer.LENGTH, chunks + 1);
                trailerOffered = true;
                awaitRecorded(publication, counterId, recordingId, recordDeadline);
            } catch (final Timeout t) {
                // W8: trailer-less by construction (the trailer is offered only after the chunks are recorded); do NOT
                // purge: close so the archive can drain what was offered, then drop the recording subscription
                CloseHelper.quietClose(publication);
                quietStopRecording(channel, publication.sessionId());
                if (trailerOffered) {
                    // W9: the trailer is out and the archive may still record it, so the recording may become
                    // COMPLETE: count the position as written (no second shadow at it, no seq reuse)
                    lastWrittenPosition = at.logPosition();
                    producerSeq++;
                }
                return new WriteResult(Reason.RECORD_TIMEOUT, t.getMessage() + (trailerOffered
                        ? " (after the trailer was offered: the recording may still complete)" : ""), null, -1,
                        recordingId, alias, 0, offerNs, 0, Purge.NOT_ATTEMPTED);
            } catch (final Abort a) {
                return cleanup(publication, channel, recordingId, Reason.ABORTED, a.cause, a.getMessage(), alias);
            }
            // W10
            final long end = publication.position();
            faults.beforeStopRecording();
            archive.stopRecording(publication);
            stopped = true;
            long stop;
            try {
                while ((stop = faults.stopPosition(archive, recordingId)) == AeronArchive.NULL_POSITION) {
                    awaitStep(recordDeadline, Reason.STOP_TIMEOUT, "stop position of recording " + recordingId);
                }
            } catch (final Timeout t) {
                CloseHelper.quietClose(publication);
                lastWrittenPosition = at.logPosition(); // header, chunks and trailer are recorded: left for the scan
                producerSeq++;
                return new WriteResult(Reason.STOP_TIMEOUT, t.getMessage(), null, -1, recordingId, alias, 0, offerNs,
                        clock.nanoTime() - recordStart, Purge.NOT_ATTEMPTED);
            } catch (final Abort a) {
                CloseHelper.quietClose(publication);
                lastWrittenPosition = at.logPosition();
                producerSeq++;
                return new WriteResult(Reason.STOP_TIMEOUT, "aborted while awaiting the stop position: "
                        + a.getMessage(), null, -1, recordingId, alias, 0, offerNs, clock.nanoTime() - recordStart,
                        Purge.NOT_ATTEMPTED);
            }
            CloseHelper.quietClose(publication);
            final long recordNs = clock.nanoTime() - recordStart;
            if (stop != end) {
                return new WriteResult(Reason.STOP_MISMATCH, "archive stop " + stop + " publication " + end, null, -1,
                        recordingId, alias, 0, offerNs, recordNs, Purge.NOT_ATTEMPTED);
            }
            // W11
            lastWrittenPosition = at.logPosition();
            producerSeq++;
            return new WriteResult(Reason.OK, null, null, -1, recordingId, alias, end, offerNs, recordNs,
                    Purge.NOT_ATTEMPTED);
        } catch (final ArchiveException ae) {
            if (stopped) {
                CloseHelper.quietClose(publication);
                return new WriteResult(Reason.ARCHIVE_REFUSED, ae.getMessage(), null, ae.errorCode(), recordingId,
                        alias, 0, 0, 0, Purge.NOT_ATTEMPTED);
            }
            final WriteResult c = cleanup(publication, channel, recordingId, Reason.ARCHIVE_REFUSED, null,
                    ae.getMessage(), alias);
            return new WriteResult(c.reason(), c.detail(), null, ae.errorCode(), c.recordingId(), alias, 0, 0, 0,
                    c.purge());
        } catch (final RuntimeException re) {
            return cleanup(publication, channel, recordingId, Reason.UNEXPECTED, null, re.toString(), alias);
        } finally {
            CloseHelper.quietClose(publication);
        }
    }

    private long digestOf(final DirectBuffer payload, final int payloadLength) {
        if (off.contains(NegativeControl.DIGEST_AS_CRC32C)) {
            return Crc32c.of(payload, 0, payloadLength) & 0xFFFF_FFFFL; // NC-Q0: the pre-amendment CRC32C
        }
        return PayloadDigest.of(payload, 0, payloadLength);
    }

    private WriteResult selfCheck(final DirectBuffer payload, final int payloadLength, final long digestBefore) {
        boolean passed;
        String why = null;
        try {
            passed = selfCheck.verify(payload, payloadLength);
        } catch (final RuntimeException e) {
            passed = false;
            why = e.toString();
        }
        if (!passed) {
            counters.increment(ShadowCounters.Counter.SELFCHECK_FAILURES_TOTAL);
            return WriteResult.refused(Reason.SELF_CHECK_FAILED, why);
        }
        final long digestAfter = digestOf(payload, payloadLength);
        if (digestAfter != digestBefore) {
            counters.increment(ShadowCounters.Counter.SELFCHECK_MUTATIONS_TOTAL);
            return WriteResult.refused(Reason.SELF_CHECK_MUTATED_BUFFER,
                    String.format("payload digest %016x before the self-check, %016x after", digestBefore, digestAfter));
        }
        return null;
    }

    /** W7 back-pressure rule: retry until {@code offerTimeoutNs} from the last positive offer; abort otherwise. */
    private void offer(final ExclusivePublication publication, final DirectBuffer buffer, final int offset,
                       final int length, final int messageIndex) {
        long deadline = clock.nanoTime() + offerTimeoutNs;
        idle.reset();
        while (true) {
            if (aborted()) {
                throw new Abort(AbortCause.ABORT_REQUESTED, "abort requested at message " + messageIndex);
            }
            final long result = faults.offer(publication, buffer, offset, length, messageIndex);
            if (result > 0) {
                return;
            }
            if (result == Publication.NOT_CONNECTED) {
                throw new Abort(AbortCause.NOT_CONNECTED, "NOT_CONNECTED at message " + messageIndex);
            }
            if (result == Publication.CLOSED) {
                throw new Abort(AbortCause.CLOSED, "CLOSED at message " + messageIndex);
            }
            if (result == Publication.MAX_POSITION_EXCEEDED) {
                throw new Abort(AbortCause.MAX_POSITION_EXCEEDED, "MAX_POSITION_EXCEEDED at message " + messageIndex);
            }
            if (clock.nanoTime() - deadline > 0) {
                throw new Abort(AbortCause.OFFER_TIMEOUT, "back-pressured past " + offerTimeoutNs + " ns at message "
                        + messageIndex);
            }
            idle.idle(0);
        }
    }

    private void awaitRecorded(final ExclusivePublication publication, final int counterId, final long recordingId,
                               final long deadline) {
        final CountersReader counters = aeron.countersReader();
        final long target = publication.position();
        idle.reset();
        while (faults.recordedPosition(counters.getCounterValue(counterId)) < target) {
            if (!RecordingPos.isActive(counters, counterId, recordingId)) {
                throw new Abort(AbortCause.RECORDING_STOPPED, "recording " + recordingId + " stopped before "
                        + target);
            }
            awaitStep(deadline, Reason.RECORD_TIMEOUT, "recording " + recordingId + " to reach " + target);
        }
    }

    private void awaitStep(final long deadline, final Reason onTimeout, final String what) {
        if (aborted()) {
            throw new Abort(AbortCause.ABORT_REQUESTED, "abort requested while waiting for " + what);
        }
        if (clock.nanoTime() - deadline > 0) {
            throw new Timeout(onTimeout, "timed out waiting for " + what);
        }
        idle.idle(0);
    }

    /**
     * W7 cleanup: stop the recording and close the publication, await the catalog stop position within
     * {@code recordTimeoutNs}, then purge; a purge refusal is reported inside the result, never swallowed, and the
     * trailer-less recording is left for the scan (NO_TRAILER, excluded).
     */
    private WriteResult cleanup(final ExclusivePublication publication, final String channel, final long recordingId,
                                final Reason reason, final AbortCause cause, final String detail, final String alias) {
        // close first: the archive records what was offered and stops the recording at end of stream, so the
        // aborted recording is a deterministic trailer-less shape; then drop the recording subscription
        CloseHelper.quietClose(publication);
        Purge purge = Purge.NOT_ATTEMPTED;
        if (recordingId < 0) {
            quietStopRecording(channel, publication.sessionId());
        } else {
            final long deadline = clock.nanoTime() + recordTimeoutNs;
            try {
                while (archive.getStopPosition(recordingId) == AeronArchive.NULL_POSITION) {
                    if (clock.nanoTime() - deadline > 0) {
                        throw new Timeout(Reason.STOP_TIMEOUT, "stop position not seen");
                    }
                    idle.idle(0);
                }
                quietStopRecording(channel, publication.sessionId());
                faults.beforePurge(recordingId);
                archive.purgeRecording(recordingId);
                purge = new Purge(Purge.Kind.PURGED, -1, null);
            } catch (final ArchiveException ae) {
                purge = new Purge(Purge.Kind.PURGE_REFUSED, ae.errorCode(), ae.getMessage());
            } catch (final Timeout t) {
                quietStopRecording(channel, publication.sessionId());
                purge = new Purge(Purge.Kind.STOP_NOT_SEEN, -1, t.getMessage());
            } catch (final RuntimeException re) {
                purge = new Purge(Purge.Kind.PURGE_REFUSED, -1, re.toString());
            }
        }
        return new WriteResult(reason, detail, cause, -1, recordingId, alias, 0, 0, 0, purge);
    }

    private void quietStopRecording(final String channel, final int sessionId) {
        try {
            archive.tryStopRecording(ChannelUri.addSessionId(channel, sessionId), ShadowAlias.STREAM_ID);
        } catch (final RuntimeException ignore) {
            // the recording subscription is already gone or the archive is unreachable; the scan classifies the rest
        }
    }
}
