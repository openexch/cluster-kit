// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowPullSettings.Endpoint;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NanoClock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The LEADER's floor behind shadows it does not hold: the one place the rule "only VERIFIED shadows this node
 * can restore from feed the floor" is relaxed, for the current leader only. The leader never
 * pulls (a pull records into the leader's archive, whose recorder also records the log) and does not produce,
 * so its local floor stays where it was when it was elected; with the follower rule (never past the leader's log
 * start) no member would purge while leadership is stable.
 *
 * <h2>How the leader learns the remote VERIFIED set</h2>
 * <p>It lists each source of {@code SHADOW_PULL_SOURCES} over its archive control channel for the member's
 * {@link FloorAttestation} marker ({@code listRecordingsForUri} on {@link FloorAttestation#STREAM_ID}): control
 * traffic only, a request and a few descriptors per period. Nothing is replicated, nothing replayed, nothing is
 * written to the leader's archive. VERIFIED is the attesting member's own verdict (its deep reads), carried by the
 * marker; the leader does not re-check it (that would need the bytes: a replay or a pull) and does not need to: the
 * old leader's restore pulls a copy and deep-reads it locally before it is used, so a wrong attestation can cost
 * availability of that restore (a loud halt), never a wrong state.</p>
 *
 * <h2>The floor</h2>
 * <pre>
 * view      = per source, the newest stopped marker of its last listing that names the member whose archive the
 *             source is (never this member), cluster and genesis the host's; a listing older than maxAge (counted
 *             from its start) feeds nothing; empty while this node does not lead
 * accept(i) = a_i &lt;= P_cm                         (restorable by EQUAL or TAIL, never by SKIP)
 *             and w_i &gt;= 2 under two-producers     (1 under any-verified)
 * f_i       = a_i                    margin none
 *             min(a_i, v_i)          margin previous-verified; no v_i: 0
 * remote    = min(P_cm, the holders-th largest f_i)   (holders = SHADOW_LEADER_REMOTE_HOLDERS, default 1); else 0
 * </pre>
 * <p>{@link ShadowFloor} purges behind {@code max(local floor, remote)} while this node leads, the newest group is a
 * STUB and its own scan can answer. Followers and the replica never read it.</p>
 *
 * <p>Threads: {@link #poll} and {@link #evaluate} on the housekeeping (pruner) thread, which never waits here (a
 * control session's connect is several round trips: stepped once per pruner wake it would outlive its own deadline,
 * and waited for it would stall the purge pass behind a source that is down); the listings run on this class's own
 * thread and are published under a lock. Never opens a {@code RecordingLog}, never replays, never replicates, never purges.</p>
 */
public final class RemoteFloor implements AutoCloseable {

    /** One evaluation. {@code holders} are the accepted attestations, best floor first. */
    public record Result(Kind kind, long pCm, long floor, List<FloorAttestation> holders, int listed, String detail) {

        public enum Kind {
            /** No listing yet in this leadership, or the last one is older than the age bound: nothing. */
            NO_VIEW,
            /** The sources that answered hold no marker of this cluster and genesis. */
            NO_ATTESTATION,
            /** Markers exist, none passes the anchor rule at or below P_cm. */
            NOT_ACCEPTED,
            /** Accepted markers exist, fewer than {@code holders} carry a floor above zero. */
            TOO_FEW_HOLDERS,
            /** A floor above zero. */
            FLOOR
        }

        public Result {
            holders = List.copyOf(holders);
        }

        @Override
        public String toString() {
            return "RemoteFloor{" + kind + " floor=" + floor + " P_cm=" + pCm + " holders=" + holders + " listed="
                    + listed + (detail == null ? "" : " " + detail) + "}";
        }
    }

    /** One source: its listing client, when it is next listed, and what its last listing showed. */
    private static final class Source {
        final Endpoint endpoint;
        /** The cluster member whose archive this is, or -1 (not a member: nothing it lists is accepted). */
        final int memberId;
        AeronArchive.AsyncConnect connecting;
        AeronArchive client;
        long nextListNs;
        boolean listedOnce;
        FloorAttestation listed;
        long listedNs;
        String problem;

        Source(final Endpoint endpoint, final int memberId) {
            this.endpoint = endpoint;
            this.memberId = memberId;
        }
    }

    private final Aeron aeron;
    private final int engineId;
    private final int clusterId;
    private final LongSupplier genesisId;
    private final int memberId;
    private final List<Source> sources = new ArrayList<>();
    private final String responseChannel;
    private final long periodNs;
    private final long maxAgeNs;
    private final int holders;
    private final NanoClock clock;
    private final ShadowHousekeeping.Counters counters;
    private final Set<NegativeControl> off;

    /** Guards the published listings, the role and its epoch (pruner thread and listing thread). */
    private final Object lock = new Object();
    private List<FloorAttestation> injected;
    private long injectedNs;
    private boolean leading;
    /** Moves at every role change: a listing started in another leadership is never published. */
    private int epoch;
    private volatile boolean running = true;
    private Thread thread;

    /**
     * @param aeron           the host's Aeron client
     * @param genesisId       the host's recorded genesis (markers of another genesis are not read)
     * @param memberId        this member's id (its own marker never counts)
     * @param sources         {@code SHADOW_PULL_SOURCES}: the other members' archive control endpoints
     * @param memberOfSource  {@code host:port} of a member's archive endpoint, spelled as the source is, to its member
     *                        id: a marker counts only when it names the member whose archive it was listed from (a
     *                        marker that came with a copied archive directory is refused); a source that is no
     *                        member's archive contributes nothing
     * @param responseChannel the listing clients' response channel ({@code aeron:udp?endpoint=<host>:0})
     * @param periodMs        each source is listed at most this often, counted from the start of its last listing;
     *                        also the bound of one connect and of one listing answer
     * @param maxAgeMs        {@code SHADOW_LEADER_REMOTE_MAX_AGE_MS}: a listing older than this feeds nothing
     * @param holders         {@code SHADOW_LEADER_REMOTE_HOLDERS}: how many members must attest the floor
     */
    public RemoteFloor(final Aeron aeron, final int engineId, final int clusterId, final LongSupplier genesisId,
                       final int memberId, final List<Endpoint> sources, final Map<String, Integer> memberOfSource,
                       final String responseChannel, final long periodMs, final long maxAgeMs, final int holders,
                       final NanoClock clock, final ShadowHousekeeping.Counters counters) {
        this(aeron, engineId, clusterId, genesisId, memberId, sources, memberOfSource, responseChannel, periodMs,
                maxAgeMs, holders, clock, counters, EnumSet.noneOf(NegativeControl.class));
    }

    RemoteFloor(final Aeron aeron, final int engineId, final int clusterId, final LongSupplier genesisId,
                final int memberId, final List<Endpoint> sources, final Map<String, Integer> memberOfSource,
                final String responseChannel, final long periodMs, final long maxAgeMs, final int holders,
                final NanoClock clock, final ShadowHousekeeping.Counters counters, final Set<NegativeControl> off) {
        this.aeron = aeron;
        this.engineId = engineId;
        this.clusterId = clusterId;
        this.genesisId = Objects.requireNonNull(genesisId, "genesisId");
        this.memberId = memberId;
        Objects.requireNonNull(memberOfSource, "memberOfSource");
        for (final Endpoint e : sources) {
            this.sources.add(new Source(e, memberOfSource.getOrDefault(e.text(), -1)));
        }
        this.responseChannel = Objects.requireNonNull(responseChannel, "responseChannel");
        if (periodMs <= 0 || maxAgeMs <= 0 || holders < 1) {
            throw new IllegalArgumentException("remote floor: periodMs " + periodMs + ", maxAgeMs " + maxAgeMs
                    + ", holders " + holders);
        }
        this.periodNs = TimeUnit.MILLISECONDS.toNanos(periodMs);
        this.maxAgeNs = TimeUnit.MILLISECONDS.toNanos(maxAgeMs);
        this.holders = holders;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.off = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
    }

    public int holders() {
        return holders;
    }

    /**
     * The housekeeping thread's step, and it never waits: it tells the listing thread the role. On the change to
     * LEADER the thread ({@code shadow-remote-floor}, started at the first one) lists every source at once and then
     * each one a period after the start of its last listing; on the change away every published listing is dropped
     * here, at once, and the thread closes its clients (the next leadership starts from fresh listings). All archive
     * traffic, and every wait for a source that is slow or down, is on that thread. Never throws.
     */
    public void poll(final boolean leaderNow) {
        synchronized (lock) {
            if (leaderNow != leading) {
                leading = leaderNow;
                epoch++;
                if (!leaderNow) {
                    injected = null;
                    for (final Source s : sources) {
                        s.listed = null;
                        s.listedOnce = false;
                    }
                }
            }
            if (leaderNow && thread == null && running && !sources.isEmpty()) {
                thread = new Thread(this::loop, "shadow-remote-floor");
                thread.setDaemon(true);
                thread.start();
            }
        }
    }

    private void loop() {
        final long sleepMs = Math.max(1, Math.min(100, TimeUnit.NANOSECONDS.toMillis(periodNs) / 10));
        int seenEpoch = -1;
        try {
            while (running) {
                final int e;
                final boolean lead;
                synchronized (lock) {
                    e = epoch;
                    lead = leading;
                }
                if (!lead) {
                    closeClients();
                } else {
                    if (e != seenEpoch) {
                        final long now = clock.nanoTime();
                        for (final Source s : sources) {
                            s.nextListNs = now;
                        }
                    }
                    for (final Source s : sources) {
                        if (running) {
                            step(s, e);
                        }
                    }
                }
                seenEpoch = e;
                Thread.sleep(sleepMs);
            }
        } catch (final InterruptedException stopped) {
            Thread.currentThread().interrupt();
        } finally {
            closeClients();
        }
    }

    /** Installs a view beside the listings (the tests' entry). */
    void accept(final List<FloorAttestation> listed, final long atNs) {
        synchronized (lock) {
            injected = List.copyOf(listed);
            injectedNs = atNs;
        }
    }

    /** Every marker a listing (of any age) or {@link #accept} gave, or null when nothing was listed yet. */
    List<FloorAttestation> view() {
        return view(false);
    }

    private List<FloorAttestation> view(final boolean freshOnly) {
        synchronized (lock) {
            return viewLocked(freshOnly);
        }
    }

    private List<FloorAttestation> viewLocked(final boolean freshOnly) {
        final long now = clock.nanoTime();
        final boolean any = !freshOnly || off.contains(NegativeControl.REMOTE_FLOOR_STALE_VIEW);
        boolean listed = false;
        final List<FloorAttestation> out = new ArrayList<>();
        if (injected != null && (any || now - injectedNs <= maxAgeNs)) {
            listed = true;
            out.addAll(injected);
        }
        for (final Source s : sources) {
            if (s.listedOnce && (any || now - s.listedNs <= maxAgeNs)) {
                listed = true;
                if (s.listed != null) {
                    out.add(s.listed);
                }
            }
        }
        return listed ? out : null;
    }

    /** The remote floor for {@code pCm} under the host's margin and anchor rule (class Javadoc). */
    public Result evaluate(final long pCm, final ShadowFloor.Margin margin, final ShadowFloor.AnchorRule rule) {
        final List<FloorAttestation> v = view(true);
        if (v == null) {
            return new Result(Result.Kind.NO_VIEW, pCm, 0, List.of(), 0, view(false) == null ? "no listing yet"
                    : "every listing is older than " + TimeUnit.NANOSECONDS.toMillis(maxAgeNs) + " ms");
        }
        return evaluate(pCm, margin, rule, holders, memberId, clusterId, genesisId.getAsLong(), engineId, v, off);
    }

    /** The table of the class Javadoc over one view. Pure. */
    static Result evaluate(final long pCm, final ShadowFloor.Margin margin, final ShadowFloor.AnchorRule rule,
                           final int holders, final int selfMemberId, final int clusterId, final long genesisId,
                           final int engineId, final List<FloorAttestation> view, final Set<NegativeControl> off) {
        final int needed = rule == ShadowFloor.AnchorRule.TWO_PRODUCERS
                && !off.contains(NegativeControl.REMOTE_FLOOR_ONE_WITNESS) ? 2 : 1;
        final List<FloorAttestation> accepted = new ArrayList<>();
        final Map<FloorAttestation, Long> floors = new HashMap<>();
        int own = 0;
        for (final FloorAttestation a : view) {
            final boolean ours = a.engineId() == engineId && a.clusterId() == clusterId && a.genesisId() == genesisId
                    && a.memberId() != selfMemberId;
            if (!ours && !off.contains(NegativeControl.REMOTE_FLOOR_FOREIGN_ACCEPTED)) {
                continue;
            }
            own++;
            if (a.anchor() > pCm && !off.contains(NegativeControl.REMOTE_FLOOR_ANCHOR_ABOVE_PCM)) {
                continue;
            }
            if (a.witnesses() < needed) {
                continue;
            }
            final long f = margin == ShadowFloor.Margin.NONE || off.contains(NegativeControl.REMOTE_FLOOR_MARGIN_IGNORED)
                    ? a.anchor() : a.margin() < 0 ? 0 : Math.min(a.anchor(), a.margin());
            accepted.add(a);
            floors.put(a, f);
        }
        if (own == 0) {
            return new Result(Result.Kind.NO_ATTESTATION, pCm, 0, List.of(), view.size(), null);
        }
        if (accepted.isEmpty()) {
            return new Result(Result.Kind.NOT_ACCEPTED, pCm, 0, List.of(), own, "none at or below P_cm with "
                    + needed + " witness(es)");
        }
        accepted.sort(Comparator.comparingLong((FloorAttestation a) -> floors.get(a)).reversed()
                .thenComparingInt(FloorAttestation::memberId));
        final long kth = accepted.size() < holders ? 0 : floors.get(accepted.get(holders - 1));
        final long floor = Math.min(pCm, kth);
        if (floor <= 0) {
            return new Result(Result.Kind.TOO_FEW_HOLDERS, pCm, 0, accepted, own, accepted.size() + " accepted, "
                    + holders + " holder(s) with a floor above zero needed (a margin shadow missing counts as zero)");
        }
        return new Result(Result.Kind.FLOOR, pCm, floor, accepted, own, null);
    }

    /**
     * One source on the listing thread: nothing while it is not due; else connect (bounded by the period), list, and
     * publish under {@code epoch}. The listing is stamped with its START, so its age never understates how old the
     * facts are, and the next one is due a period after that start. A source that failed is tried again a period
     * after the failure; its last listing stays and ages out.
     */
    private void step(final Source s, final int e) throws InterruptedException {
        final long startNs = clock.nanoTime();
        if (startNs - s.nextListNs < 0) {
            return;
        }
        try {
            if (s.client == null) {
                s.connecting = AeronArchive.asyncConnect(new AeronArchive.Context()
                        .aeron(aeron)
                        .ownsAeronClient(false)
                        .controlRequestChannel(s.endpoint.udpChannel())
                        .controlRequestStreamId(AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT)
                        .controlResponseChannel(responseChannel)
                        .controlResponseStreamId(FloorAttestation.LISTING_RESPONSE_STREAM_ID)
                        .messageTimeoutNs(periodNs));
                // the connect throws once its own deadline (the period) has passed
                while ((s.client = s.connecting.poll()) == null) {
                    if (!running) {
                        return;
                    }
                    Thread.sleep(1);
                }
                s.connecting = null;
            }
            final FloorAttestation best = list(s);
            synchronized (lock) {
                if (epoch == e && leading) {
                    s.listed = best;
                    s.listedNs = startNs;
                    s.listedOnce = true;
                }
            }
            s.nextListNs = startNs + periodNs;
            counters.increment(ShadowHousekeeping.Counter.LEADER_REMOTE_LISTINGS_TOTAL);
            if (s.problem != null) {
                s.problem = null;
                System.out.println("[SHADOW-FLOOR] leader: source " + s.endpoint.text() + " answers its attestation"
                        + " listing again");
            }
        } catch (final RuntimeException unreachable) {
            CloseHelper.quietCloseAll(s.connecting, s.client);
            s.connecting = null;
            s.client = null;
            s.nextListNs = clock.nanoTime() + periodNs;
            counters.increment(ShadowHousekeeping.Counter.LEADER_REMOTE_SOURCE_UNREACHABLE_TOTAL);
            if (s.problem == null) {
                System.err.println("[SHADOW-FLOOR] leader: source " + s.endpoint.text() + " did not answer its"
                        + " attestation listing (its shadows do not count until it does): " + unreachable);
            }
            s.problem = String.valueOf(unreachable);
        }
    }

    /**
     * The newest marker of {@code s}'s archive that may count: stopped, this cluster and genesis, and naming the
     * member whose archive this is. Null when there is none.
     */
    private FloorAttestation list(final Source s) {
        final long genesis = genesisId.getAsLong();
        final boolean foreign = off.contains(NegativeControl.REMOTE_FLOOR_FOREIGN_ACCEPTED);
        final List<Long> ids = new ArrayList<>();
        final FloorAttestation[] best = {null};
        long from = 0;
        while (true) {
            final int before = ids.size();
            final int n = s.client.listRecordingsForUri(from, ShadowCatalog.DEFAULT_LIST_BATCH,
                    FloorAttestation.prefix(engineId), FloorAttestation.STREAM_ID,
                    (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                     stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId,
                     streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                        ids.add(recordingId);
                        final FloorAttestation a = FloorAttestation.tryParse(originalChannel);
                        if (a == null) {
                            return;
                        }
                        if (stopPosition == AeronArchive.NULL_POSITION
                                && !off.contains(NegativeControl.REMOTE_FLOOR_ACTIVE_ACCEPTED)) {
                            return; // being written, or left by a write that never finished: not a statement
                        }
                        if ((a.genesisId() != genesis || a.clusterId() != clusterId) && !foreign) {
                            return;
                        }
                        if (a.memberId() != s.memberId && !off.contains(NegativeControl.REMOTE_FLOOR_SOURCE_UNBOUND)) {
                            return; // a marker of another member in this archive (a copied directory): refused
                        }
                        if (best[0] == null || a.seq() > best[0].seq()) {
                            best[0] = a;
                        }
                    });
            if (n < ShadowCatalog.DEFAULT_LIST_BATCH || ids.size() == before) {
                return best[0];
            }
            from = ids.get(ids.size() - 1) + 1;
        }
    }

    /** The listing thread's clients (its own thread only). */
    private void closeClients() {
        for (final Source s : sources) {
            CloseHelper.quietCloseAll(s.connecting, s.client);
            s.connecting = null;
            s.client = null;
            s.problem = null;
        }
    }

    /**
     * Stops the listing thread (it closes its clients on its way out; a thread that does not stop within a period
     * plus 5 s is left to, logged) and drops every listing.
     */
    @Override
    public void close() {
        running = false;
        final Thread t;
        synchronized (lock) {
            t = thread;
            injected = null;
            for (final Source s : sources) {
                s.listed = null;
                s.listedOnce = false;
            }
        }
        if (t != null && t != Thread.currentThread()) {
            final long waitMs = TimeUnit.NANOSECONDS.toMillis(periodNs) + 5_000;
            try {
                t.join(waitMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) {
                System.err.println("[SHADOW-FLOOR] shadow-remote-floor did not stop within " + waitMs + " ms; it"
                        + " closes its listing clients when its listing returns");
            }
        }
    }
}
