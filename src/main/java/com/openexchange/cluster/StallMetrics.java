// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.AeronCounters;
import org.agrona.concurrent.status.CountersReader;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * The duty-cycle and snapshot-duration counters Aeron keeps for one member, as series of the member's
 * {@code /metrics}: how long the consensus module, the clustered service, the archive conductor and the media
 * driver's agents took for their slowest work cycle, how often a cycle went over its threshold, and the same for a
 * snapshot. They answer "does a snapshot stall this member, and for how long" on a running box.
 *
 * <h2>The series</h2>
 *
 * <p>{@code <p>} is the engine's prefix ({@code match}, {@code core}). Durations are decimal seconds (Aeron counts
 * nanoseconds), like the engines' other duration series.</p>
 * <table>
 *   <caption>Series by source</caption>
 *   <tr><th>Source (Aeron counter type)</th><th>Maximum</th><th>Over threshold</th></tr>
 *   <tr><td>consensus module cycle (216, 217)</td><td>{@code <p>_consensus_module_max_cycle_time}</td>
 *       <td>{@code <p>_consensus_module_cycle_time_exceeded}</td></tr>
 *   <tr><td>clustered service cycle (218, 219)</td><td>{@code <p>_service_max_cycle_time}</td>
 *       <td>{@code <p>_service_cycle_time_exceeded}</td></tr>
 *   <tr><td>snapshot, all services, as the consensus module times it (234, 235)</td>
 *       <td>{@code <p>_snapshot_total_max_duration}</td><td>{@code <p>_snapshot_total_duration_exceeded}</td></tr>
 *   <tr><td>snapshot of this service, as its container times it (236, 237)</td>
 *       <td>{@code <p>_snapshot_service_max_duration}</td><td>{@code <p>_snapshot_service_duration_exceeded}</td></tr>
 *   <tr><td>driver conductor cycle (system counters 26, 27)</td><td>{@code <p>_driver_conductor_max_cycle_time}</td>
 *       <td>{@code <p>_driver_conductor_cycle_time_exceeded}</td></tr>
 *   <tr><td>driver sender cycle (28, 29)</td><td>{@code <p>_driver_sender_max_cycle_time}</td>
 *       <td>{@code <p>_driver_sender_cycle_time_exceeded}</td></tr>
 *   <tr><td>driver receiver cycle (30, 31)</td><td>{@code <p>_driver_receiver_max_cycle_time}</td>
 *       <td>{@code <p>_driver_receiver_cycle_time_exceeded}</td></tr>
 *   <tr><td>archive agent cycle (103, 104), one series per archive and agent, labels {@code archive_id} and
 *       {@code agent} ({@code conductor}; with dedicated archive threads also {@code recorder} and
 *       {@code replayer})</td>
 *       <td>{@code <p>_archive_max_cycle_time}</td><td>{@code <p>_archive_cycle_time_exceeded}</td></tr>
 * </table>
 *
 * <p>A maximum {@code M} gives three gauges:</p>
 * <ul>
 *   <li>{@code M_seconds}: Aeron's counter, the maximum since the component started (it only rises);</li>
 *   <li>{@code M_last_rise_age_seconds}: seconds since the sampler last saw it rise, -1 when it has not risen since
 *       the sampler first read it;</li>
 *   <li>{@code M_recent_seconds}: {@code M_seconds} when it rose within the last {@value #WINDOW_SECONDS} seconds,
 *       else 0. A cycle slower than every cycle before it shows here; a slow cycle below the standing maximum does
 *       not (Aeron keeps one maximum), and shows in the over-threshold series when it was over the threshold.</li>
 * </ul>
 *
 * <p>An over-threshold count {@code C} gives one counter and two gauges:</p>
 * <ul>
 *   <li>{@code C_total}: Aeron's counter, cycles (or snapshots) over the component's threshold;</li>
 *   <li>{@code C_last_age_seconds}: seconds since the sampler last saw it rise, -1 as above;</li>
 *   <li>{@code C_recent}: the rise of {@code C_total} between the newest sample and the one
 *       {@value #WINDOW_SECONDS} samples before it, that is over the last {@value #WINDOW_SECONDS} sampling
 *       intervals of one second.</li>
 * </ul>
 *
 * <p>A source whose Aeron counter does not exist on this member (a component not launched, an Aeron build without
 * it) has no series. {@code <p>_stall_sampler_age_seconds} is the time since the sampler last read the counters,
 * -1 before the first read: a sampler that stopped is seen there and not as flat series.</p>
 *
 * <h2>Cost</h2>
 *
 * <p>The counters are read once a second by {@link #sample()} on this object's own daemon thread
 * ({@link #start()}), or on a thread the host calls it from; never on a cluster service, consensus or driver
 * thread. {@link #open} reads them through its own read-only mapping of the driver's counters file, so no Aeron
 * client is opened and none is depended on. One read is a volatile load from the driver's counters file per
 * source (14, and two per archive agent: 18 to 26 on a member), two more to check that the counter is still the one
 * that was found, and no allocation. A source not found yet is looked for every
 * {@value #RESCAN_EVERY} seconds by walking the counter metadata, without allocation. {@link #render} allocates, on
 * the scrape thread.</p>
 */
public final class StallMetrics {

    /** The length of the "recent" window. */
    public static final int WINDOW_SECONDS = 300;
    static final long SAMPLE_INTERVAL_MS = 1_000;
    static final int RESCAN_EVERY = 5;
    /**
     * Archive agents of one member that are tracked: an archive has one (shared threading) or three (dedicated:
     * conductor, recorder, replayer), and a member has the cluster's archive and maybe an engine's journal archive.
     */
    static final int MAX_ARCHIVE_AGENTS = 12;
    private static final String[] ARCHIVE_AGENTS = {"conductor", "recorder", "replayer", "other"};

    private static final int NOT_FOUND = -1;
    private static final int SYSTEM_COUNTER_TYPE_ID = 0;

    private enum Scope { CLUSTER, SERVICE, SYSTEM, ARCHIVE }

    /** One Aeron counter this object follows. All fields are guarded by the object's monitor. */
    private static final class Source {
        final String name;
        final String help;
        final boolean max;
        final Scope scope;
        /** The counter type id, or the fixed counter id of a system counter. */
        final int typeId;
        int counterId = NOT_FOUND;
        /** The registration id the counter had when it was found: a slot given to another counter has another. */
        long registrationId;
        /** Archive sources: the archive's id and which of its agents ({@link #ARCHIVE_AGENTS}). */
        long label;
        int agent;
        long value;
        long lastChangeMs;
        /** Counts only: the value at each of the last {@link #WINDOW_SECONDS} + 1 samples. */
        final long[] ring;
        int ringAt;

        Source(final String name, final String help, final boolean max, final Scope scope, final int typeId) {
            this.name = name;
            this.help = help;
            this.max = max;
            this.scope = scope;
            this.typeId = typeId;
            // one more sample than intervals: newest less oldest spans WINDOW_SECONDS sampling intervals
            this.ring = max ? null : new long[WINDOW_SECONDS + 1];
        }
    }

    private final String prefix;
    private final CountersReader counters;
    private final int clusterId;
    private final int serviceId;
    private final LongSupplier epochMs;
    private final Source[] sources;
    /** The mapping of the driver's counters file when this object made it ({@link #open}); else null. */
    private java.nio.MappedByteBuffer ownMapping;
    private long lastSampleMs = -1;
    private long samples;
    private volatile boolean running;
    private boolean closed;
    private Thread thread;

    /**
     * The series of the member whose media driver runs in {@code aeronDirectoryName}, read through a read-only
     * mapping of that driver's counters file which this object owns. It does not depend on any Aeron client of the
     * process: a client that closes (and unmaps its own view of the file) while the sampler runs cannot hurt it, in
     * either driver mode. {@link #close()} stops the sampler and releases the mapping.
     *
     * @param prefix             the engine's metric prefix ({@code match}, {@code core})
     * @param aeronDirectoryName the directory of this member's media driver (embedded or external)
     * @param clusterId          this member's cluster id
     * @param serviceId          the clustered service's id (0 for a member with one service)
     * @throws RuntimeException the counters file is not there, or is of another layout version
     */
    public static StallMetrics open(final String prefix, final String aeronDirectoryName, final int clusterId,
                                    final int serviceId) {
        final java.io.File cnc = new java.io.File(aeronDirectoryName, io.aeron.CncFileDescriptor.CNC_FILE);
        final java.nio.MappedByteBuffer mapping = org.agrona.IoUtil.mapExistingFile(cnc,
            java.nio.channels.FileChannel.MapMode.READ_ONLY, "the media driver's counters file");
        try {
            final org.agrona.DirectBuffer meta = io.aeron.CncFileDescriptor.createMetaDataBuffer(mapping);
            io.aeron.CncFileDescriptor.checkVersion(meta.getInt(io.aeron.CncFileDescriptor.cncVersionOffset(0)));
            final CountersReader counters = new CountersReader(
                io.aeron.CncFileDescriptor.createCountersMetaDataBuffer(mapping, meta),
                io.aeron.CncFileDescriptor.createCountersValuesBuffer(mapping, meta),
                java.nio.charset.StandardCharsets.US_ASCII);
            final StallMetrics metrics = new StallMetrics(prefix, counters, clusterId, serviceId);
            metrics.ownMapping = mapping;
            return metrics;
        } catch (final RuntimeException e) {
            org.agrona.IoUtil.unmap(mapping);
            throw e;
        }
    }

    /**
     * The series read through {@code counters}. The caller owns their lifetime: {@link #stop()} must return before
     * the Aeron client they come from is closed (a closed client unmaps the counters file). {@link #open} has no
     * such condition and is what an engine uses.
     *
     * @param prefix    the engine's metric prefix ({@code match}, {@code core})
     * @param counters  the counters of this member's media driver
     * @param clusterId this member's cluster id
     * @param serviceId the clustered service's id (0 for a member with one service)
     */
    public StallMetrics(final String prefix, final CountersReader counters, final int clusterId, final int serviceId) {
        this(prefix, counters, clusterId, serviceId, System::currentTimeMillis);
    }

    StallMetrics(final String prefix, final CountersReader counters, final int clusterId, final int serviceId,
                 final LongSupplier epochMs) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.clusterId = clusterId;
        this.serviceId = serviceId;
        this.epochMs = Objects.requireNonNull(epochMs, "epochMs");
        final java.util.List<Source> all = new java.util.ArrayList<>();
        pair(all, "consensus_module_max_cycle_time", "consensus_module_cycle_time_exceeded",
            "the consensus module's work cycle", Scope.CLUSTER, AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID,
            AeronCounters.CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID);
        pair(all, "service_max_cycle_time", "service_cycle_time_exceeded", "the clustered service's work cycle",
            Scope.SERVICE, AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID,
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID);
        pair(all, "snapshot_total_max_duration", "snapshot_total_duration_exceeded",
            "a snapshot of every service, as the consensus module times it", Scope.CLUSTER,
            AeronCounters.CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID,
            AeronCounters.CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID);
        pair(all, "snapshot_service_max_duration", "snapshot_service_duration_exceeded",
            "this service's snapshot, as its container times it", Scope.SERVICE,
            AeronCounters.CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
            AeronCounters.CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID);
        pair(all, "driver_conductor_max_cycle_time", "driver_conductor_cycle_time_exceeded",
            "the media driver conductor's work cycle", Scope.SYSTEM,
            AeronCounters.SYSTEM_COUNTER_ID_CONDUCTOR_MAX_CYCLE_TIME,
            AeronCounters.SYSTEM_COUNTER_ID_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED);
        pair(all, "driver_sender_max_cycle_time", "driver_sender_cycle_time_exceeded",
            "the media driver sender's work cycle", Scope.SYSTEM,
            AeronCounters.SYSTEM_COUNTER_ID_SENDER_MAX_CYCLE_TIME,
            AeronCounters.SYSTEM_COUNTER_ID_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED);
        pair(all, "driver_receiver_max_cycle_time", "driver_receiver_cycle_time_exceeded",
            "the media driver receiver's work cycle", Scope.SYSTEM,
            AeronCounters.SYSTEM_COUNTER_ID_RECEIVER_MAX_CYCLE_TIME,
            AeronCounters.SYSTEM_COUNTER_ID_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED);
        for (int i = 0; i < MAX_ARCHIVE_AGENTS; i++) {
            pair(all, "archive_max_cycle_time", "archive_cycle_time_exceeded", "an archive agent's work cycle",
                Scope.ARCHIVE, AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID,
                AeronCounters.ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID);
        }
        this.sources = all.toArray(new Source[0]);
    }

    private static void pair(final java.util.List<Source> all, final String maxName, final String countName,
                             final String what, final Scope scope, final int maxTypeId, final int countTypeId) {
        all.add(new Source(maxName, "The longest time of " + what, true, scope, maxTypeId));
        all.add(new Source(countName, "Times " + what + " took longer than its threshold", false, scope,
            countTypeId));
    }

    // ------------------------------------------------------------------ sampling

    /** Start the sampling thread. Idempotent. */
    public synchronized void start() {
        if (running || closed) {
            return;
        }
        running = true;
        thread = new Thread(this::run, "stall-metrics-sampler");
        thread.setDaemon(true);
        thread.start();
    }

    /** Stop the sampling thread and wait for it (at most two seconds); true when it has ended. */
    public boolean stop() {
        final Thread t;
        synchronized (this) {
            running = false;
            t = thread;
            thread = null;
        }
        if (t != null && t != Thread.currentThread()) {
            t.interrupt();
            try {
                t.join(2_000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return !t.isAlive();
        }
        return true;
    }

    /**
     * {@link #stop()}, then release the mapping {@link #open} made. The series are gone afterwards. A mapping whose
     * sampler did not end in time is left mapped: it is never released under a thread that may read it.
     */
    public void close() {
        final boolean ended = stop();
        synchronized (this) {
            closed = true;
            final java.nio.MappedByteBuffer mapping = ownMapping;
            ownMapping = null;
            if (mapping != null && ended) {
                org.agrona.IoUtil.unmap(mapping);
            }
        }
    }

    private void run() {
        try {
            while (running) {
                sampleIfRunning();
                Thread.sleep(SAMPLE_INTERVAL_MS);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final Throwable t) {
            // nothing may leave this thread into the member; the age gauge shows that the sampler stopped
            System.err.println("[STALL-METRICS] CRITICAL: the sampler stopped, the duty-cycle series of this member"
                + " are frozen until it restarts: " + t);
        }
    }

    private synchronized void sampleIfRunning() {
        if (running) {
            sample();
        }
    }

    /**
     * Read every counter once. Called once a second by the thread of {@link #start()}; a host that has its own
     * housekeeping thread may call it from there at the same rate instead (the window is counted in samples).
     * Never call it from a cluster service, consensus or driver thread.
     */
    public synchronized void sample() {
        if (closed) {
            return;
        }
        final long nowMs = epochMs.getAsLong();
        final boolean rescan = samples % RESCAN_EVERY == 0;
        for (final Source s : sources) {
            if (s.counterId != NOT_FOUND && !stillThere(s)) {
                s.counterId = NOT_FOUND;
            }
            if (s.counterId == NOT_FOUND) {
                if (!rescan || !find(s)) {
                    continue;
                }
                // the first read is the baseline: when the counter moved before it is not known
                s.value = counters.getCounterValue(s.counterId);
                s.lastChangeMs = 0;
                if (s.ring != null) {
                    java.util.Arrays.fill(s.ring, s.value);
                    s.ringAt = 0;
                }
                continue;
            }
            final long value = counters.getCounterValue(s.counterId);
            if (value > s.value) {
                s.lastChangeMs = nowMs;
            }
            s.value = value;
            if (s.ring != null) {
                s.ring[s.ringAt] = value;
                s.ringAt = (s.ringAt + 1) % s.ring.length;
            }
        }
        samples++;
        lastSampleMs = nowMs;
    }

    private boolean stillThere(final Source s) {
        if (counters.getCounterState(s.counterId) != CountersReader.RECORD_ALLOCATED
            || counters.getCounterRegistrationId(s.counterId) != s.registrationId) {
            return false; // freed, or the slot now holds another counter (maybe of the same type)
        }
        return counters.getCounterTypeId(s.counterId) == (s.scope == Scope.SYSTEM ? SYSTEM_COUNTER_TYPE_ID
            : s.typeId);
    }

    /** Looks for the source's counter without allocating; true when found. */
    private boolean find(final Source s) {
        if (s.scope == Scope.SYSTEM) {
            // a system counter's id is fixed; typeId holds it
            if (s.typeId <= counters.maxCounterId()
                && counters.getCounterState(s.typeId) == CountersReader.RECORD_ALLOCATED
                && counters.getCounterTypeId(s.typeId) == SYSTEM_COUNTER_TYPE_ID) {
                s.counterId = s.typeId;
                s.registrationId = counters.getCounterRegistrationId(s.typeId);
                return true;
            }
            return false;
        }
        final org.agrona.concurrent.AtomicBuffer meta = counters.metaDataBuffer();
        for (int id = 0, max = counters.maxCounterId(); id <= max; id++) {
            final int state = counters.getCounterState(id);
            if (state == CountersReader.RECORD_UNUSED) {
                break;
            }
            if (state != CountersReader.RECORD_ALLOCATED || counters.getCounterTypeId(id) != s.typeId) {
                continue;
            }
            final int key = CountersReader.metaDataOffset(id) + CountersReader.KEY_OFFSET;
            if (s.scope == Scope.CLUSTER && meta.getInt(key) != clusterId) {
                continue;
            }
            if (s.scope == Scope.SERVICE && (meta.getInt(key) != clusterId
                || meta.getInt(key + Integer.BYTES) != serviceId)) {
                continue;
            }
            if (s.scope == Scope.ARCHIVE) {
                if (takenByAnother(s, id)) {
                    continue;
                }
                s.label = meta.getLong(key);
                s.agent = archiveAgent(meta, id);
            }
            s.counterId = id;
            s.registrationId = counters.getCounterRegistrationId(id);
            return true;
        }
        return false;
    }

    /**
     * Which agent of an archive a cycle counter belongs to, from its label ({@code archive-conductor ...},
     * {@code archive-recorder ...}, {@code archive-replayer ...}); read as bytes, no allocation.
     */
    private static int archiveAgent(final org.agrona.concurrent.AtomicBuffer meta, final int counterId) {
        final int label = CountersReader.metaDataOffset(counterId) + CountersReader.LABEL_OFFSET;
        final int length = meta.getInt(label);
        final int text = label + Integer.BYTES;
        // "archive-" is eight bytes; the agents differ in the first or the third letter after it
        if (length < 11 || meta.getByte(text + 7) != '-') {
            return 3;
        }
        if (meta.getByte(text + 8) == 'c') {
            return 0;
        }
        return meta.getByte(text + 10) == 'c' ? 1 : meta.getByte(text + 10) == 'p' ? 2 : 3;
    }

    /** Whether another source of the same series already follows this counter (one source per archive agent). */
    private boolean takenByAnother(final Source s, final int counterId) {
        for (final Source other : sources) {
            if (other != s && other.typeId == s.typeId && other.scope == s.scope && other.counterId == counterId) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------ what the member shows

    /** The series in Prometheus text, appended to {@code sb}. Any thread but a hot one. */
    public synchronized void render(final StringBuilder sb) {
        if (closed) {
            return;
        }
        final long nowMs = epochMs.getAsLong();
        for (int i = 0; i < sources.length; i++) {
            final Source first = sources[i];
            if (firstOfItsName(i)) {
                final String base = prefix + "_" + first.name;
                for (int part = 0; part < 3; part++) {
                    boolean head = true;
                    for (int j = i; j < sources.length; j++) {
                        final Source s = sources[j];
                        if (s.counterId == NOT_FOUND || !s.name.equals(first.name)) {
                            continue;
                        }
                        part(sb, head, base, part, s, nowMs);
                        head = false;
                    }
                }
            }
        }
        series(sb, true, prefix + "_stall_sampler_age_seconds", "gauge", "Seconds since the duty-cycle counters were"
            + " last read (-1: never)", "", Long.toString(lastSampleMs < 0 ? -1
                : Math.max(0, nowMs - lastSampleMs) / 1_000));
    }

    private boolean firstOfItsName(final int index) {
        for (int j = 0; j < index; j++) {
            if (sources[j].name.equals(sources[index].name)) {
                return false;
            }
        }
        return true;
    }

    /** One of the three series of a source; every series of one name is written together, under one head. */
    private static void part(final StringBuilder sb, final boolean head, final String base, final int part,
                             final Source s, final long nowMs) {
        final String label = s.scope == Scope.ARCHIVE ? "{archive_id=\"" + s.label + "\",agent=\""
            + ARCHIVE_AGENTS[s.agent] + "\"}" : "";
        final long ageSeconds = s.lastChangeMs == 0 ? -1 : Math.max(0, nowMs - s.lastChangeMs) / 1_000;
        if (s.max) {
            if (part == 0) {
                series(sb, head, base + "_seconds", "gauge", s.help + " since it started", label, seconds(s.value));
            } else if (part == 1) {
                series(sb, head, base + "_last_rise_age_seconds", "gauge", "Seconds since " + base + "_seconds last"
                    + " rose (-1: not since this member began to sample it)", label, Long.toString(ageSeconds));
            } else {
                final boolean recent = ageSeconds >= 0 && ageSeconds <= WINDOW_SECONDS;
                series(sb, head, base + "_recent_seconds", "gauge", base + "_seconds when a sample saw it rise at"
                    + " most " + WINDOW_SECONDS + " s ago (wall clock), else 0", label,
                    recent ? seconds(s.value) : "0");
            }
        } else if (part == 0) {
            series(sb, head, base + "_total", "counter", s.help, label, Long.toString(s.value));
        } else if (part == 1) {
            series(sb, head, base + "_last_age_seconds", "gauge", "Seconds since " + base + "_total last rose (-1:"
                + " not since this member began to sample it)", label, Long.toString(ageSeconds));
        } else {
            // the ring holds WINDOW_SECONDS + 1 samples and the oldest is the one the next sample overwrites
            series(sb, head, base + "_recent", "gauge", "Rise of " + base + "_total between the newest sample and the"
                + " one " + WINDOW_SECONDS + " samples before it (one sample a second: the last " + WINDOW_SECONDS
                + " s; since the first sample while there are fewer)", label,
                Long.toString(Math.max(0, s.value - s.ring[s.ringAt])));
        }
    }

    /** Nanoseconds as decimal seconds, the way the engines print their other durations. */
    private static String seconds(final long nanos) {
        return Double.toString(nanos / 1e9);
    }

    /** {@link #render(StringBuilder)} as a string. */
    public String renderMetrics() {
        final StringBuilder sb = new StringBuilder(4096);
        render(sb);
        return sb.toString();
    }

    private static void series(final StringBuilder sb, final boolean head, final String name, final String type,
                               final String help, final String label, final String value) {
        if (head) {
            sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
            sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        }
        sb.append(name).append(label).append(' ').append(value).append('\n');
    }
}
