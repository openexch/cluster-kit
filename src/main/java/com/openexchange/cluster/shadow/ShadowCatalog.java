// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowEntry.State;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import org.agrona.concurrent.NanoClock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The shadows one archive holds, derived from the archive catalog on every scan (the archive is
 * the only registry). The archive client may be local (member housekeeping, replica) or remote (discovery).
 *
 * <ul>
 *   <li>{@link #scan}: lists {@code alias=oxs.v1.<engine>.} on {@link ShadowAlias#STREAM_ID}, classifies each recording
 *       structurally (trailer window, header window, T3/T4, alias == header), reuses the structural verdict of an
 *       unchanged descriptor (no replay), groups COMPLETE copies by position and content, marks FOREIGN and CONFLICT,
 *       overlays the process-lifetime deep verdicts. Never throws: an archive refusal or timeout is SCAN_ERROR for that
 *       entry, and a scan with errors answers every floor query "unknown".</li>
 *   <li>{@link #verifyDeep}, {@link #verifyAllComplete}: the deep CRC read, once per immutable recording per process.</li>
 *   <li>{@link #purge}: re-scans, then refuses the last VERIFIED copy of the anchor (the newest VERIFIED position).</li>
 * </ul>
 *
 * <p>G7: only VERIFIED copies move a floor or authorise a purge: {@link Scan#verifiedAtOrAbove} and
 * {@link Scan#verifiedNewest} read VERIFIED copies only; {@link Scan#equalAt}, {@link Scan#newestAtOrAbove},
 * {@link Scan#newestBelow} read COMPLETE or VERIFIED copies and are for restore attempts, which the loader re-verifies.
 * One owner thread per instance (not synchronised); {@link ShadowVerdicts} is the shared, thread-safe part. Never
 * opens {@code RecordingLog}, never launches a driver, never schedules.</p>
 */
public final class ShadowCatalog {

    public static final int DEFAULT_LIST_BATCH = 100;
    /** {@code PURGE_REFUSED(ANCHOR)}: the last VERIFIED copy of the anchor, or no VERIFIED anchor at all. */
    public static final int PURGE_CODE_ANCHOR = -1001;
    /** {@code PURGE_REFUSED(UNKNOWN)}: the fresh scan had errors, the startup pass is not done, or the target is
     *  SCAN_ERROR. */
    public static final int PURGE_CODE_UNKNOWN = -1002;
    public static final int PURGE_CODE_NOT_FOUND = -1003;
    public static final int SCAN_CODE_TIMEOUT = -2001;
    public static final int SCAN_CODE_IMAGE_CLOSED = -2002;
    public static final int SCAN_CODE_CLIENT = -2003;
    /** The replay ended before its range: unknown, retried next scan, never cached. */
    public static final int SCAN_CODE_ENDED_EARLY = -2004;

    /** A restore-attempt query: one logical shadow, nothing, or a conflict the host must treat as loud. */
    public record Answer(Kind kind, LogicalShadow shadow) {
        public enum Kind { FOUND, EMPTY, CONFLICT }

        static final Answer EMPTY = new Answer(Kind.EMPTY, null);
    }

    /** A floor query (VERIFIED only): FOUND, NONE, or UNKNOWN while the scan has errors or the startup pass is not
     *  done ({@code -1}, "do not raise"). */
    public record Floor(Kind kind, LogicalShadow shadow) {
        public enum Kind { FOUND, NONE, UNKNOWN }

        public long position() {
            return kind == Kind.FOUND ? shadow.logPosition() : -1L;
        }
    }

    public record ForeignPair(int clusterId, long genesisId) {
    }

    /** One scan: every entry, and the query surface over it. Immutable. */
    public record Scan(List<ShadowEntry> all, List<LogicalShadow> usable, List<LogicalShadow> verified,
                       List<LogicalShadow> conflicts, int errors, Map<State, Integer> counts, boolean startupPassDone,
                       int foreign, ForeignPair firstForeign, int seqReuse) {

        public Scan {
            all = List.copyOf(all);
            usable = List.copyOf(usable);
            verified = List.copyOf(verified);
            conflicts = List.copyOf(conflicts);
            counts = Map.copyOf(counts);
        }

        public int count(final State state) {
            return counts.getOrDefault(state, 0);
        }

        /** Whether this scan may answer a floor query with a number. */
        public boolean known() {
            return errors == 0 && startupPassDone;
        }

        public ShadowEntry entry(final long recordingId) {
            for (final ShadowEntry e : all) {
                if (e.recordingId() == recordingId) {
                    return e;
                }
            }
            return null;
        }

        /** EQUAL restore attempt: total, {@code CONFLICT} is loud. */
        public Answer equalAt(final long p) {
            for (final LogicalShadow c : conflicts) {
                if (c.logPosition() == p) {
                    return new Answer(Answer.Kind.CONFLICT, c);
                }
            }
            for (final LogicalShadow s : usable) {
                if (s.logPosition() == p) {
                    return new Answer(Answer.Kind.FOUND, s);
                }
            }
            return Answer.EMPTY;
        }

        /** SKIP restore attempt: the newest position {@code >= p}; CONFLICT if that position is in conflict. */
        public Answer newestAtOrAbove(final long p) {
            return newestIn(p, Long.MAX_VALUE, true);
        }

        /** TAIL restore attempt: the newest position in {@code [floor, p)}; same CONFLICT rule. */
        public Answer newestBelow(final long p, final long floor) {
            return newestIn(floor, p, false);
        }

        private Answer newestIn(final long low, final long high, final boolean highInclusive) {
            long best = -1;
            for (final LogicalShadow s : usable) {
                if (in(s.logPosition(), low, high, highInclusive)) {
                    best = Math.max(best, s.logPosition());
                }
            }
            for (final LogicalShadow c : conflicts) {
                if (in(c.logPosition(), low, high, highInclusive)) {
                    best = Math.max(best, c.logPosition());
                }
            }
            return best < 0 ? Answer.EMPTY : equalAt(best);
        }

        private static boolean in(final long p, final long low, final long high, final boolean highInclusive) {
            return p >= low && (highInclusive ? p <= high : p < high);
        }

        /** Every copy at {@code p} whatever its state (dedupe, {@code DualProducerCrcTest}, {@code shadow list}). */
        public LogicalShadow atPosition(final long p) {
            final List<ShadowEntry> copies = new ArrayList<>();
            boolean conflict = false;
            ShadowHeader first = null;
            for (final ShadowEntry e : all) {
                if (e.header() != null && e.header().logPosition() == p
                        && e.state() != State.HEADER_INVALID && e.state() != State.ALIAS_MISMATCH) {
                    copies.add(e);
                    conflict |= e.state() == State.CONFLICT;
                    first = first == null ? e.header() : first;
                }
            }
            return first == null ? null
                    : new LogicalShadow(first.engineId(), first.clusterId(), first.genesisId(), p, copies, conflict);
        }

        /** {@code ShadowFloor} first branch and the bundle stub refusal: the newest VERIFIED position {@code >= p}. */
        public Floor verifiedAtOrAbove(final long p) {
            if (!known()) {
                return new Floor(Floor.Kind.UNKNOWN, null);
            }
            for (final LogicalShadow s : verified) {
                if (s.logPosition() >= p) {
                    return new Floor(Floor.Kind.FOUND, s);
                }
            }
            return new Floor(Floor.Kind.NONE, null);
        }

        /** Retention anchor and age alarms: the highest VERIFIED position. */
        public Floor verifiedNewest() {
            if (!known()) {
                return new Floor(Floor.Kind.UNKNOWN, null);
            }
            return verified.isEmpty() ? new Floor(Floor.Kind.NONE, null) : new Floor(Floor.Kind.FOUND, verified.get(0));
        }

        /**
         * Retention N: logical shadows, which are positions: every agreeing position group is one
         * entry of {@code usable}, however many recordings hold it.
         */
        public int distinctCount() {
            return usable.size();
        }
    }

    /** What {@link #verifyDeep} found. */
    public record DeepResult(Kind kind, String reason, int code) {
        public enum Kind { VERIFIED, CORRUPT, SCAN_ERROR, NOT_VERIFIABLE }
    }

    /** What {@link #purge} did. {@code code}: an archive code (e.g. 2 ACTIVE_RECORDING) or a {@code PURGE_CODE_*}. */
    public record PurgeResult(Kind kind, int code, String detail, ShadowEntry target) {
        public enum Kind { PURGED, REFUSED }

        public boolean purged() {
            return kind == Kind.PURGED;
        }
    }

    private record Descriptor(long recordingId, long startPosition, long stopPosition, String originalChannel) {
    }

    private record Cached(Descriptor descriptor, ShadowEntry entry) {
    }

    private final AeronArchive archive;
    private final long archiveId;
    private final int engineId;
    private final int clusterId;
    private final long genesisId;
    private final ShadowVerdicts verdicts;
    private final int listBatch;
    private final NanoClock clock;
    private final long replayTimeoutNs;
    private final ShadowCounters counters;
    private final ShadowReplay replay;
    private final Set<NegativeControl> off;
    private final ShadowHeader.Options options;
    private final String prefix;
    private Map<Long, Cached> cache = new HashMap<>();
    private Scan lastScan;
    private boolean readBounded;
    private long readDeadlineNs;
    private boolean holdUnknown;
    private Map<Long, Cached> heldUnknown;

    /**
     * @param genesisId 0 = unknown (replica bootstrap only); otherwise COMPLETE copies of another genesis are FOREIGN
     */
    public ShadowCatalog(final Aeron aeron, final AeronArchive archive, final int engineId, final int clusterId,
                         final long genesisId, final ShadowVerdicts verdicts, final int listBatch,
                         final NanoClock clock, final long replayTimeoutNs, final ShadowCounters counters) {
        this(aeron, archive, engineId, clusterId, genesisId, verdicts, listBatch, clock, replayTimeoutNs, counters,
                new ShadowReplay(aeron, archive, clock), EnumSet.noneOf(NegativeControl.class));
    }

    ShadowCatalog(final Aeron aeron, final AeronArchive archive, final int engineId, final int clusterId,
                  final long genesisId, final ShadowVerdicts verdicts, final int listBatch, final NanoClock clock,
                  final long replayTimeoutNs, final ShadowCounters counters, final ShadowReplay replay,
                  final Set<NegativeControl> off) {
        Objects.requireNonNull(aeron, "aeron");
        this.archive = Objects.requireNonNull(archive, "archive");
        this.archiveId = archive.archiveId();
        this.prefix = ShadowAlias.prefix(engineId);
        this.engineId = engineId;
        if (clusterId < 0) {
            throw new IllegalArgumentException("clusterId must be >= 0: " + clusterId);
        }
        if (genesisId < 0) {
            throw new IllegalArgumentException("genesisId must be >= 0 (0 = unknown): " + genesisId);
        }
        if (listBatch < 1) {
            throw new IllegalArgumentException("listBatch must be >= 1: " + listBatch);
        }
        if (replayTimeoutNs <= 0) {
            throw new IllegalArgumentException("replayTimeoutNs must be > 0: " + replayTimeoutNs);
        }
        this.clusterId = clusterId;
        this.genesisId = genesisId;
        this.verdicts = Objects.requireNonNull(verdicts, "verdicts");
        this.listBatch = listBatch;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.replayTimeoutNs = replayTimeoutNs;
        this.counters = Objects.requireNonNull(counters, "counters");
        this.replay = Objects.requireNonNull(replay, "replay");
        this.off = Objects.requireNonNull(off, "off").isEmpty()
                ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
        this.options = ShadowHeader.Options.forEngine(engineId);
    }

    public long archiveId() {
        return archiveId;
    }

    public ShadowVerdicts verdicts() {
        return verdicts;
    }

    /** The last scan of this instance, or null before the first. */
    public Scan lastScan() {
        return lastScan;
    }

    // ------------------------------------------------------------------------------------------------------- scan

    /**
     * {@link #scan()} inside the caller's bound: every structural read ends by {@code deadlineNs} (an absolute value
     * of the injected clock) or its own replay timeout, whichever is first, and a descriptor not read by then is
     * SCAN_ERROR ("not read"). Unknown, never corrupt: the next scan reads it again.
     */
    public Scan scan(final long deadlineNs) {
        readBounded = true;
        readDeadlineNs = deadlineNs;
        try {
            return scan();
        } finally {
            readBounded = false;
        }
    }

    /**
     * While on, the descriptors the first scan after this call found SCAN_ERROR are not read again by the later
     * scans of this instance while their descriptor is unchanged (a recording whose segment file is gone costs the
     * archive's error answer on every read). For one bounded caller (a startup pull, a member's restore): turned off
     * when it ends, and every call drops what an earlier one held. Recordings created after that first scan are
     * always read. A held entry stays SCAN_ERROR: never usable, so holding it can only cost a candidate, never add one.
     */
    public void holdUnknown(final boolean on) {
        holdUnknown = on;
        heldUnknown = null;
    }

    /** 6.2. Never throws. */
    public Scan scan() {
        counters.increment(ShadowCounters.Counter.CATALOG_SCANS_TOTAL);
        final List<Descriptor> descriptors;
        try {
            descriptors = list();
        } catch (final RuntimeException listingFailed) {
            counters.increment(ShadowCounters.Counter.CATALOG_SCAN_ERRORS_TOTAL);
            final Map<State, Integer> counts = new EnumMap<>(State.class);
            counts.put(State.SCAN_ERROR, 1);
            lastScan = new Scan(List.of(), List.of(), List.of(), List.of(), 1, counts, false, 0, null, 0);
            return lastScan;
        }
        final Map<Long, Cached> nextCache = new HashMap<>();
        final List<ShadowEntry> entries = new ArrayList<>(descriptors.size());
        for (final Descriptor d : descriptors) {
            final Cached cached = cache.get(d.recordingId());
            final ShadowEntry e;
            final Cached held = heldUnknown == null ? null : heldUnknown.get(d.recordingId());
            if (cached != null && (off.contains(NegativeControl.STRUCTURAL_CACHE_BY_RECORDING_ID)
                    || cached.descriptor().equals(d))) {
                e = cached.entry();
            } else if (held != null && held.descriptor().equals(d)) {
                e = held.entry();
            } else if (readBounded && clock.nanoTime() - readDeadlineNs >= 0) {
                e = entry(d, ShadowAlias.tryParse(ShadowAlias.aliasOfChannel(d.originalChannel())), null, null, 0,
                        null, State.SCAN_ERROR, "not read: the caller's deadline passed", SCAN_CODE_TIMEOUT);
            } else {
                e = classify(d);
            }
            if (e.state() != State.ACTIVE && e.state() != State.SCAN_ERROR) {
                nextCache.put(d.recordingId(), cached != null && cached.entry() == e ? cached : new Cached(d, e));
            }
            entries.add(e);
        }
        cache = nextCache;
        if (holdUnknown && heldUnknown == null) {
            heldUnknown = new HashMap<>();
            for (int i = 0; i < descriptors.size(); i++) {
                if (entries.get(i).state() == State.SCAN_ERROR) {
                    heldUnknown.put(descriptors.get(i).recordingId(), new Cached(descriptors.get(i), entries.get(i)));
                }
            }
        }
        lastScan = assemble(entries);
        publish(lastScan);
        return lastScan;
    }

    private List<Descriptor> list() {
        final List<Descriptor> out = new ArrayList<>();
        long from = 0;
        while (true) {
            final List<Descriptor> page = new ArrayList<>();
            final int n = archive.listRecordingsForUri(from, listBatch, prefix, ShadowAlias.STREAM_ID,
                    (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                     stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                     strippedChannel, originalChannel, sourceIdentity) ->
                            page.add(new Descriptor(recordingId, startPosition, stopPosition, originalChannel)));
            out.addAll(page);
            if (n < listBatch || page.isEmpty()) {
                return out;
            }
            from = page.get(page.size() - 1).recordingId() + 1;
        }
    }

    private ShadowEntry entry(final Descriptor d, final ShadowAlias alias, final ShadowHeader header,
                              final byte[] headerBytes, final int headerCrc, final ShadowTrailer trailer,
                              final State state, final String reason, final int code) {
        return new ShadowEntry(archiveId, d.recordingId(), d.startPosition(), d.stopPosition(), d.originalChannel(),
                alias, header, headerBytes, headerCrc, trailer, state, reason, code);
    }

    /** 6.2 step 3: one stopped recording, by the trailer rule of 3.3. */
    private ShadowEntry classify(final Descriptor d) {
        final String aliasText = ShadowAlias.aliasOfChannel(d.originalChannel());
        ShadowAlias alias;
        try {
            alias = ShadowAlias.parse(aliasText);
        } catch (final ShadowAlias.InvalidAliasException e) {
            return entry(d, null, null, null, 0, null, State.ALIAS_UNPARSEABLE, e.getMessage(), -1);
        }
        if (d.stopPosition() == AeronArchive.NULL_POSITION) {
            return entry(d, alias, null, null, 0, null, State.ACTIVE, null, -1);
        }
        if (d.stopPosition() - d.startPosition() < ShadowHeader.FRAME_LENGTH + ShadowTrailer.FRAME_LENGTH) {
            return entry(d, alias, null, null, 0, null, State.TOO_SHORT,
                    (d.stopPosition() - d.startPosition()) + " bytes", -1);
        }
        final boolean equation = off.contains(NegativeControl.TRAILER_RULE_REPLACED_BY_LENGTH_EQUATION);
        ShadowTrailer trailer = null;
        if (!equation) {
            final Window w = window(d.recordingId(), d.stopPosition() - ShadowTrailer.FRAME_LENGTH,
                    ShadowTrailer.FRAME_LENGTH, true);
            if (w.error != null) {
                return w.error.apply(this, d, alias);
            }
            if (w.messages.size() != 1) {
                return entry(d, alias, null, null, 0, null, State.NO_TRAILER,
                        "trailer window delivered " + w.messages.size() + " message(s)", -1);
            }
            final byte[] bytes = w.messages.get(0);
            final ShadowTrailer.Decoded t = ShadowTrailer.decode(new org.agrona.concurrent.UnsafeBuffer(bytes), 0,
                    bytes.length);
            if (!t.ok()) {
                final State s = switch (t.problem().reason()) {
                    case TRAILER_MISMATCH -> State.TRAILER_MISMATCH;
                    case TRAILER_VERSION_UNSUPPORTED -> State.TRAILER_VERSION_UNSUPPORTED;
                    default -> State.NO_TRAILER;
                };
                return entry(d, alias, null, null, 0, null, s, t.problem().toString(), -1);
            }
            trailer = t.trailer();
        }
        final Window hw = window(d.recordingId(), d.startPosition(), ShadowHeader.FRAME_LENGTH, false);
        if (hw.error != null) {
            return hw.error.apply(this, d, alias);
        }
        if (hw.messages.size() != 1) {
            return entry(d, alias, null, null, 0, trailer, State.HEADER_INVALID,
                    "header window delivered " + hw.messages.size() + " message(s)", -1);
        }
        final byte[] headerBytes = hw.messages.get(0);
        final org.agrona.concurrent.UnsafeBuffer hb = new org.agrona.concurrent.UnsafeBuffer(headerBytes);
        final ShadowHeader.Decoded h = ShadowHeader.decode(hb, 0, headerBytes.length, options);
        if (!h.ok()) {
            return entry(d, alias, null, headerBytes, 0, trailer, State.HEADER_INVALID, h.problem().toString(), -1);
        }
        final ShadowHeader header = h.header();
        final int headerCrc = ShadowHeader.storedCrc(hb, 0);
        if (equation) {
            final long expected = ShadowHeader.FRAME_LENGTH + header.payloadLength() + ShadowTrailer.FRAME_LENGTH;
            if (d.stopPosition() - d.startPosition() != expected) {
                return entry(d, alias, header, headerBytes, headerCrc, null, State.NO_TRAILER, "length equation", -1);
            }
        } else {
            final ShadowTrailer.Problem m = trailer.mismatch(header, headerCrc);
            if (m != null) {
                return entry(d, alias, header, headerBytes, headerCrc, trailer, State.TRAILER_MISMATCH, m.toString(),
                        -1);
            }
        }
        if (!alias.equals(header.alias())) {
            return entry(d, alias, header, headerBytes, headerCrc, trailer, State.ALIAS_MISMATCH,
                    "alias " + aliasText + " header " + header.alias().format(), -1);
        }
        return entry(d, alias, header, headerBytes, headerCrc, trailer, State.COMPLETE, null, -1);
    }

    @FunctionalInterface
    private interface ErrorEntry {
        ShadowEntry apply(ShadowCatalog catalog, Descriptor d, ShadowAlias alias);
    }

    private static final class Window {
        final List<byte[]> messages = new ArrayList<>();
        ErrorEntry error;
    }

    /** A bounded window replay; archive refusals and timeouts become an error entry, never an exception. */
    private Window window(final long recordingId, final long from, final int length, final boolean trailerWindow) {
        final Window w = new Window();
        try {
            replay.readRange(recordingId, from, length, (buffer, offset, len, header) -> {
                final byte[] bytes = new byte[len];
                buffer.getBytes(offset, bytes);
                w.messages.add(bytes);
            }, readDeadline());
        } catch (final ArchiveException ae) {
            if (trailerWindow && ae.errorCode() == ArchiveException.INVALID_POSITION
                    && !off.contains(NegativeControl.INVALID_POSITION_AS_SCAN_ERROR)) {
                // 6.2 step 3: the offset falls inside a torn frame: classifiable, not an error
                w.error = (c, d, alias) -> c.entry(d, alias, null, null, 0, null, State.NO_TRAILER,
                        "INVALID_POSITION at stop - 96", ae.errorCode());
            } else {
                w.error = (c, d, alias) -> c.entry(d, alias, null, null, 0, null, State.SCAN_ERROR,
                        "archive: " + ae.getMessage(), ae.errorCode());
            }
        } catch (final ShadowReplay.ReplayTimeoutException te) {
            counters.increment(ShadowCounters.Counter.REPLAY_TIMEOUTS_TOTAL);
            w.error = (c, d, alias) -> c.entry(d, alias, null, null, 0, null, State.SCAN_ERROR, te.getMessage(),
                    SCAN_CODE_TIMEOUT);
        } catch (final ShadowReplay.ReplayEndedEarlyException early) {
            w.error = (c, d, alias) -> c.entry(d, alias, null, null, 0, null, State.SCAN_ERROR, early.getMessage(),
                    SCAN_CODE_ENDED_EARLY);
        } catch (final IllegalStateException closed) {
            w.error = (c, d, alias) -> c.entry(d, alias, null, null, 0, null, State.SCAN_ERROR, closed.getMessage(),
                    SCAN_CODE_IMAGE_CLOSED);
        } catch (final RuntimeException other) {
            w.error = (c, d, alias) -> c.entry(d, alias, null, null, 0, null, State.SCAN_ERROR, other.toString(),
                    SCAN_CODE_CLIENT);
        }
        return w;
    }

    /** The deadline of one structural read: the replay timeout, or the bounded caller's deadline when earlier. */
    private long readDeadline() {
        final long now = clock.nanoTime();
        return readBounded ? now + Math.min(replayTimeoutNs, Math.max(0, readDeadlineNs - now))
                : now + replayTimeoutNs;
    }

    /** 6.2 steps 4-7 over the structural entries. */
    private Scan assemble(final List<ShadowEntry> structural) {
        final List<ShadowEntry> all = new ArrayList<>(structural.size());
        int foreign = 0;
        ForeignPair firstForeign = null;
        // step 5 first (a copy of another cluster or genesis is never grouped with ours)
        for (final ShadowEntry e : structural) {
            if (e.state() == State.COMPLETE && (e.header().clusterId() != clusterId
                    || (genesisId != 0 && e.header().genesisId() != genesisId))) {
                foreign++;
                if (firstForeign == null) {
                    firstForeign = new ForeignPair(e.header().clusterId(), e.header().genesisId());
                }
                all.add(e.with(State.FOREIGN, "c" + e.header().clusterId() + ".g" + e.header().genesisId()));
            } else {
                all.add(e);
            }
        }
        // a bootstrap catalog (genesis unknown) that sees COMPLETE copies of more than one genesis cannot tell which
        // cluster it belongs to: every such copy is FOREIGN (loud), never a restore candidate
        if (genesisId == 0) {
            final java.util.Set<Long> geneses = new java.util.TreeSet<>();
            for (final ShadowEntry e : all) {
                if (e.state() == State.COMPLETE) {
                    geneses.add(e.header().genesisId());
                }
            }
            if (geneses.size() > 1) {
                for (int i = 0; i < all.size(); i++) {
                    final ShadowEntry e = all.get(i);
                    if (e.state() == State.COMPLETE) {
                        foreign++;
                        if (firstForeign == null) {
                            firstForeign = new ForeignPair(e.header().clusterId(), e.header().genesisId());
                        }
                        all.set(i, e.with(State.FOREIGN, "ambiguous genesis on a bootstrap catalog: " + geneses));
                    }
                }
            }
        }
        // step 4: position groups over COMPLETE copies
        final Map<List<Long>, List<Integer>> groups = new LinkedHashMap<>();
        for (int i = 0; i < all.size(); i++) {
            final ShadowEntry e = all.get(i);
            if (e.state() == State.COMPLETE) {
                groups.computeIfAbsent(groupKey(e), k -> new ArrayList<>()).add(i);
            }
        }
        final List<List<Integer>> agreeing = new ArrayList<>();
        final List<List<Integer>> conflicting = new ArrayList<>();
        for (final List<Integer> g : groups.values()) {
            if (agree(all, g)) {
                agreeing.add(g);
            } else if (off.contains(NegativeControl.CONFLICT_NEWEST_WINS)) {
                int newest = g.get(0);
                for (final int i : g) {
                    newest = all.get(i).recordingId() > all.get(newest).recordingId() ? i : newest;
                }
                final List<Integer> rest = new ArrayList<>(g);
                rest.remove(Integer.valueOf(newest));
                agreeing.add(List.of(newest));
                if (!rest.isEmpty()) {
                    conflicting.add(rest);
                }
            } else {
                conflicting.add(g);
            }
        }
        final List<LogicalShadow> conflicts = new ArrayList<>();
        for (final List<Integer> g : conflicting) {
            final List<ShadowEntry> copies = new ArrayList<>();
            for (final int i : g) {
                final ShadowEntry c = all.get(i).with(State.CONFLICT, "copies at one position disagree");
                all.set(i, c);
                copies.add(c);
            }
            conflicts.add(logical(copies, true));
        }
        // step 6: the deep verdicts of this process (after grouping, so a CORRUPT copy leaves its group's others)
        for (final List<Integer> g : agreeing) {
            for (final int i : g) {
                final ShadowEntry e = all.get(i);
                final ShadowVerdicts.Verdict v = verdicts.get(e.key());
                if (v == null) {
                    continue;
                }
                if (v.kind() == ShadowVerdicts.Kind.VERIFIED) {
                    all.set(i, e.with(State.VERIFIED, null));
                } else if (!off.contains(NegativeControl.CORRUPT_CLEARED_BY_SCAN)) {
                    all.set(i, e.with(State.CORRUPT, v.reason()));
                }
            }
        }
        final List<LogicalShadow> usable = new ArrayList<>();
        final List<LogicalShadow> verified = new ArrayList<>();
        for (final List<Integer> g : agreeing) {
            final List<ShadowEntry> copies = new ArrayList<>();
            for (final int i : g) {
                if (all.get(i).state().usable()) {
                    copies.add(all.get(i));
                }
            }
            if (!copies.isEmpty()) {
                final LogicalShadow s = logical(copies, false);
                usable.add(s);
                if (s.verified()) {
                    verified.add(s);
                }
            }
        }
        final Comparator<LogicalShadow> newestFirst =
                Comparator.comparingLong(LogicalShadow::logPosition).reversed();
        usable.sort(newestFirst);
        verified.sort(newestFirst);
        conflicts.sort(newestFirst);
        final Map<State, Integer> counts = new EnumMap<>(State.class);
        int errors = 0;
        for (final ShadowEntry e : all) {
            counts.merge(e.state(), 1, Integer::sum);
            errors += e.state() == State.SCAN_ERROR ? 1 : 0;
        }
        final boolean startupPassDone = verdicts.startupPassDone(archiveId);
        return new Scan(all, usable, verified, conflicts, errors, counts, startupPassDone, foreign, firstForeign,
                seqReuse(all));
    }

    private List<Long> groupKey(final ShadowEntry e) {
        final ShadowHeader h = e.header();
        final List<Long> key = new ArrayList<>(List.of((long)h.engineId(), (long)h.clusterId(), h.genesisId(),
                h.logPosition()));
        if (off.contains(NegativeControl.DEDUPE_BY_RECORDING)) {
            key.add(e.recordingId());
        }
        if (off.contains(NegativeControl.GROUP_KEY_INCLUDES_PRODUCER_AND_KIND)) {
            key.add((long)h.producerKind());
            key.add((long)h.producerId());
            key.add((long)h.kind());
        }
        return key;
    }

    /** Content agreement inside a position group (producer, kind, seq, layout hint are copy attributes). */
    private static boolean agree(final List<ShadowEntry> all, final List<Integer> group) {
        final ShadowHeader a = all.get(group.get(0)).header();
        for (final int i : group) {
            final ShadowHeader b = all.get(i).header();
            if (a.payloadLength() != b.payloadLength() || a.payloadDigest() != b.payloadDigest()
                    || a.leadershipTermId() != b.leadershipTermId()
                    || a.termBaseLogPosition() != b.termBaseLogPosition() || a.clusterTime() != b.clusterTime()
                    || a.journalEnabled() != b.journalEnabled() || a.journalSeq() != b.journalSeq()) {
                return false;
            }
        }
        return true;
    }

    private static LogicalShadow logical(final List<ShadowEntry> copies, final boolean conflict) {
        final ShadowHeader h = copies.get(0).header();
        return new LogicalShadow(h.engineId(), h.clusterId(), h.genesisId(), h.logPosition(), copies, conflict);
    }

    /** {@code (producer, seq)} used at more than one position: a diagnostic, never identity. */
    private static int seqReuse(final List<ShadowEntry> all) {
        final Map<String, Set<Long>> positions = new HashMap<>();
        for (final ShadowEntry e : all) {
            if (e.header() != null && e.state() != State.HEADER_INVALID) {
                positions.computeIfAbsent(e.producer() + "." + e.header().producerSeq(), k -> new java.util.HashSet<>())
                        .add(e.header().logPosition());
            }
        }
        int reuse = 0;
        for (final Set<Long> p : positions.values()) {
            reuse += p.size() > 1 ? 1 : 0;
        }
        return reuse;
    }

    private void publish(final Scan s) {
        counters.add(ShadowCounters.Counter.CATALOG_SCAN_ERRORS_TOTAL, s.errors());
        counters.set(ShadowCounters.Counter.CATALOG_USABLE, s.usable().size());
        counters.set(ShadowCounters.Counter.CATALOG_VERIFIED, s.verified().size());
        counters.set(ShadowCounters.Counter.CATALOG_DISTINCT, s.distinctCount());
        counters.set(ShadowCounters.Counter.CATALOG_INCOMPLETE, s.count(State.ACTIVE) + s.count(State.TOO_SHORT)
                + s.count(State.NO_TRAILER) + s.count(State.HEADER_INVALID) + s.count(State.TRAILER_MISMATCH)
                + s.count(State.TRAILER_VERSION_UNSUPPORTED)
                + s.count(State.ALIAS_MISMATCH) + s.count(State.ALIAS_UNPARSEABLE));
        counters.add(ShadowCounters.Counter.CATALOG_CONFLICT_TOTAL, s.conflicts().size());
        counters.set(ShadowCounters.Counter.CATALOG_CORRUPT, s.count(State.CORRUPT));
        counters.set(ShadowCounters.Counter.CATALOG_FOREIGN, s.foreign());
        counters.add(ShadowCounters.Counter.CATALOG_SEQ_REUSE_TOTAL, s.seqReuse());
        counters.set(ShadowCounters.Counter.CATALOG_STARTUP_PASS_DONE, s.startupPassDone() ? 1 : 0);
        counters.set(ShadowCounters.Counter.NEWEST_VERIFIED_POSITION,
                s.verified().isEmpty() ? -1 : s.verified().get(0).logPosition());
        counters.set(ShadowCounters.Counter.NEWEST_USABLE_POSITION,
                s.usable().isEmpty() ? -1 : s.usable().get(0).logPosition());
    }

    // ------------------------------------------------------------------------------------------------ deep reads

    /**
     * 6.4: the full read of one copy. Pass stores VERIFIED for its key, failure CORRUPT (final for the key); an archive
     * refusal or timeout stores nothing and answers SCAN_ERROR (unknown is not corrupt).
     */
    public DeepResult verifyDeep(final ShadowEntry e) {
        final State s = e.state();
        if (!(s == State.COMPLETE || s == State.VERIFIED || s == State.CORRUPT || s == State.FOREIGN
                || s == State.CONFLICT) || e.header() == null) {
            return new DeepResult(DeepResult.Kind.NOT_VERIFIABLE, s.name(), -1);
        }
        final ShadowVerdicts.Verdict known = verdicts.get(e.key());
        if (known != null) {
            return known.kind() == ShadowVerdicts.Kind.VERIFIED
                    ? new DeepResult(DeepResult.Kind.VERIFIED, null, -1)
                    : new DeepResult(DeepResult.Kind.CORRUPT, known.reason(), -1);
        }
        final DeepRead.Outcome o = DeepRead.read(replay, e, null, clock.nanoTime() + replayTimeoutNs, counters);
        return record(verdicts, counters, e, o);
    }

    static DeepResult record(final ShadowVerdicts verdicts, final ShadowCounters counters, final ShadowEntry e,
                             final DeepRead.Outcome o) {
        switch (o.kind()) {
            case OK -> {
                verdicts.put(e.key(), ShadowVerdicts.Verdict.VERIFIED);
                final ShadowVerdicts.Verdict now = verdicts.get(e.key());
                return now.kind() == ShadowVerdicts.Kind.VERIFIED ? new DeepResult(DeepResult.Kind.VERIFIED, null, -1)
                        : new DeepResult(DeepResult.Kind.CORRUPT, now.reason(), -1);
            }
            case CORRUPT -> {
                final ShadowVerdicts.Verdict before = verdicts.get(e.key());
                verdicts.put(e.key(), ShadowVerdicts.Verdict.corrupt(o.reason()));
                if (before == null || before.kind() != ShadowVerdicts.Kind.CORRUPT) {
                    counters.increment(ShadowCounters.Counter.CATALOG_CORRUPT_TOTAL);
                }
                return new DeepResult(DeepResult.Kind.CORRUPT, o.reason(), -1);
            }
            default -> {
                counters.increment(ShadowCounters.Counter.CATALOG_SCAN_ERRORS_TOTAL);
                return new DeepResult(DeepResult.Kind.SCAN_ERROR, o.reason(), o.code());
            }
        }
    }

    /**
     * 6.4, the host's startup obligation: scan, deep-verify every COMPLETE copy without a verdict (newest position
     * first), mark the startup pass done for this archive when no SCAN_ERROR occurred, then scan again (no replay for
     * unchanged descriptors) and return that scan. Until it finished, floor queries answer UNKNOWN.
     */
    public Scan verifyAllComplete() {
        final Scan first = scan();
        boolean clean = first.errors() == 0;
        final List<ShadowEntry> todo = new ArrayList<>();
        for (final ShadowEntry e : first.all()) {
            if (e.state() == State.COMPLETE) {
                todo.add(e);
            }
        }
        todo.sort(Comparator.comparingLong((ShadowEntry e) -> e.header().logPosition()).reversed());
        for (final ShadowEntry e : todo) {
            clean &= verifyDeep(e).kind() != DeepResult.Kind.SCAN_ERROR;
        }
        if (clean) {
            verdicts.markStartupPassDone(archiveId);
        }
        return scan();
    }

    // ------------------------------------------------------------------------------------------------------ purge

    /**
     * 6.5 (the last-VERIFIED-anchor rule is enforced here and nowhere else): re-scans, then refuses with
     * {@link #PURGE_CODE_ANCHOR} the last VERIFIED copy of the anchor (the newest VERIFIED position; a CORRUPT or
     * CONFLICT newest does not anchor), and any COMPLETE or VERIFIED copy while no VERIFIED anchor exists; refuses with
     * {@link #PURGE_CODE_UNKNOWN} while the fresh scan has errors or the startup pass is not done. {@code force}
     * skips the anchor rule (an operator decision) but still re-scans and reports what it deletes. The archive's own
     * refusal (e.g. {@code ACTIVE_RECORDING} 2 while a replay is in progress) is surfaced with its code.
     * Non-usable targets (FOREIGN, CONFLICT, CORRUPT, NO_TRAILER, ...) are not guarded by the anchor rule: deleting one
     * side of a CONFLICT makes the other side usable on the next scan, so resolving a CONFLICT by purge is an operator
     * action, never a retention one (which states retention may purge is {@link ShadowRetention}'s).
     */
    public PurgeResult purge(final long recordingId, final boolean force) {
        final Scan s = off.contains(NegativeControl.PURGE_ON_STALE_SCAN) && lastScan != null ? lastScan : scan();
        final ShadowEntry target = s.entry(recordingId);
        if (target == null) {
            return refuse(PURGE_CODE_NOT_FOUND, "recording " + recordingId + " is not a shadow of this catalog", null);
        }
        if (!force) {
            if (!s.known() || target.state() == State.SCAN_ERROR) {
                return refuse(PURGE_CODE_UNKNOWN, "scan errors=" + s.errors() + " startupPassDone="
                        + s.startupPassDone() + " target=" + target.state(), target);
            }
            if (target.state().usable()) {
                final List<LogicalShadow> verified = s.verified();
                if (verified.isEmpty()) {
                    return refuse(PURGE_CODE_ANCHOR, "no VERIFIED shadow anchors a purge", target);
                }
                final LogicalShadow anchor = verified.get(0);
                if (target.header().logPosition() == anchor.logPosition()) {
                    long remaining = 0;
                    for (final ShadowEntry c : anchor.copies()) {
                        remaining += c.recordingId() != recordingId && c.state() == State.VERIFIED ? 1 : 0;
                    }
                    if (remaining == 0) {
                        return refuse(PURGE_CODE_ANCHOR, "last VERIFIED copy of the anchor P="
                                + anchor.logPosition(), target);
                    }
                }
            }
        }
        try {
            archive.purgeRecording(recordingId);
        } catch (final ArchiveException ae) {
            return refuse(ae.errorCode(), "archive: " + ae.getMessage(), target);
        }
        return new PurgeResult(PurgeResult.Kind.PURGED, 0, (force ? "forced purge of " : "purged ") + target, target);
    }

    private PurgeResult refuse(final int code, final String detail, final ShadowEntry target) {
        counters.incrementPurgeRefused(code);
        return new PurgeResult(PurgeResult.Kind.REFUSED, code, detail, target);
    }
}
