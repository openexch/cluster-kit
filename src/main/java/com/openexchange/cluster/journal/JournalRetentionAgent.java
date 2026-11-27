// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.EngineLifecycle;
import com.openexchange.cluster.OffBoxWatermark;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The thread that runs {@link JournalRetention} inside a member: thread {@code journal-retention}, its own archive
 * session, one pass per journal every {@code JOURNAL_RETENTION_POLL_MS}. It never runs on the service thread and it
 * does not depend on the log pruner. Outside processes only deliver the readers' cursors
 * ({@link JournalReaderTool}); the member purges itself.
 *
 * <p>Nothing latches: a pass that fails (an archive call, a reader directory) is reported, the archive session is
 * dropped and the next pass starts from nothing. A lost media driver is handed to the Aeron client's error handler,
 * where the engine's driver-loss rule decides. The thread also runs when every journal's mode is {@code off}: it
 * then only lists the recordings, so the retained bytes of a journal nobody purges are still visible.</p>
 *
 * <p>Nothing leaves the thread into the member, and nothing ends it silently. Each journal's pass is on its own: an
 * exception or an error out of one journal's pass is counted and said for that journal ({@code _pass_failures_total},
 * a CRITICAL line, the {@code /health} text) and the other journal is still passed. The thread's own state is the
 * gauge {@code <prefix>_journal_retention_agent_state} ({@link AgentState}); a thread that ended on something the
 * loop could not handle reads DEAD there and on {@code /health} until the member restarts.</p>
 */
public final class JournalRetentionAgent {

    /** One journal as the last pass left it, for the gauges. */
    public static final class Status {
        public final String journal;
        public final JournalRetention.Mode mode;
        /** The last pass's report; null until the first pass ends. */
        public final JournalRetention.Report report;
        public final long purgedBytesTotal;
        public final long purgeFailuresTotal;
        /** The wall clock at the end of the last completed pass, or at the agent's start before the first. */
        public final long lastPassMs;
        /** Passes over this journal that failed (an exception or an error out of the pass). */
        public final long passFailuresTotal;
        /** Why this journal's last pass failed; null when it completed. The report is then the last completed one. */
        public final String failure;
        /** How long the last completed pass over this journal took, in ms; -1 before the first. */
        public final long lastPassDurationMs;
        /** Bytes the last completed pass asked the archive to replay to read the recordings' last records. */
        public final long tailReadBytes;
        /** The same over every completed pass of this process. */
        public final long tailReadBytesTotal;

        Status(final String journal, final JournalRetention.Mode mode, final JournalRetention.Report report,
               final long purgedBytesTotal, final long purgeFailuresTotal, final long lastPassMs,
               final long passFailuresTotal, final String failure) {
            this(journal, mode, report, purgedBytesTotal, purgeFailuresTotal, lastPassMs, passFailuresTotal, failure,
                -1, 0, 0);
        }

        Status(final String journal, final JournalRetention.Mode mode, final JournalRetention.Report report,
               final long purgedBytesTotal, final long purgeFailuresTotal, final long lastPassMs,
               final long passFailuresTotal, final String failure, final long lastPassDurationMs,
               final long tailReadBytes, final long tailReadBytesTotal) {
            this.lastPassDurationMs = lastPassDurationMs;
            this.tailReadBytes = tailReadBytes;
            this.tailReadBytesTotal = tailReadBytesTotal;
            this.journal = journal;
            this.mode = mode;
            this.report = report;
            this.purgedBytesTotal = purgedBytesTotal;
            this.purgeFailuresTotal = purgeFailuresTotal;
            this.lastPassMs = lastPassMs;
            this.passFailuresTotal = passFailuresTotal;
            this.failure = failure;
        }
    }

    /** The thread as a gauge value: what {@code <prefix>_journal_retention_agent_state} shows. */
    public enum AgentState {
        /** Not started yet, or stopped by {@link #stop}. */
        NOT_RUNNING(0),
        /** The thread runs and its last attempt went through for every journal (or none was due yet). */
        RUNNING(1),
        /** The thread runs and its last attempt failed for a journal, or before any journal (no archive session). */
        PASS_FAILING(2),
        /** The thread ended on something it could not handle: nothing is purged until the member restarts. */
        DEAD(3),
        /** The engine could not construct or start the agent ({@link #renderNotStarted}). */
        START_FAILED(4);

        private final int code;

        AgentState(final int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    private final File clusterDir;
    private final AeronArchive.Context archiveContext;
    private final String engine;
    private final int clusterId;
    private final LongSupplier genesisId;
    private final JournalRetention.Settings settings;
    private final List<JournalRetention> retentions = new ArrayList<>();
    private final Map<String, Status> status = new LinkedHashMap<>();
    private final NanoClock clock = SystemNanoClock.INSTANCE;
    private final AtomicBoolean driverLossReported = new AtomicBoolean();
    private final Consumer<String> out;
    private final Consumer<String> err;

    private OffBoxWatermark.Mode offBoxMode = OffBoxWatermark.Mode.OFF;
    private LongSupplier commitPosition = () -> -1;
    private volatile String passFailure;
    private volatile AgentState agentState = AgentState.NOT_RUNNING;
    private volatile String death;
    /** Test seam: runs at the start of each journal's pass (a pass that throws). */
    volatile java.util.function.Consumer<String> beforePass = journal -> { };
    private volatile boolean running;
    /** The thread that may pass; a thread that is not this one any more ends without touching anything. */
    private volatile Thread thread;

    /**
     * @param clusterDir     the member's cluster directory
     * @param archiveContext the member's own archive client context (the agent connects its own session with it)
     * @param engine         {@link JournalReaders#ENGINE_CORE} or {@link JournalReaders#ENGINE_MATCH}
     * @param clusterId      this member's cluster id
     * @param genesisId      the genesis id this member has recorded, 0 while it has none
     * @param settings       the settings, read for every profile's journal
     * @param profiles       the member's journals
     */
    public JournalRetentionAgent(final File clusterDir, final AeronArchive.Context archiveContext, final String engine,
                                 final int clusterId, final LongSupplier genesisId,
                                 final JournalRetention.Settings settings, final List<JournalProfile> profiles) {
        this(clusterDir, archiveContext, engine, clusterId, genesisId, settings, profiles, System.out::println,
            System.err::println);
    }

    JournalRetentionAgent(final File clusterDir, final AeronArchive.Context archiveContext, final String engine,
                          final int clusterId, final LongSupplier genesisId, final JournalRetention.Settings settings,
                          final List<JournalProfile> profiles, final Consumer<String> out,
                          final Consumer<String> err) {
        this.clusterDir = Objects.requireNonNull(clusterDir, "clusterDir");
        this.archiveContext = Objects.requireNonNull(archiveContext, "archiveContext");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.clusterId = clusterId;
        this.genesisId = Objects.requireNonNull(genesisId, "genesisId");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.out = Objects.requireNonNull(out, "out");
        this.err = Objects.requireNonNull(err, "err");
        final long nowMs = System.currentTimeMillis();
        for (final JournalProfile profile : profiles) {
            final JournalRetention.Mode mode = settings.mode(profile.journal());
            retentions.add(new JournalRetention(profile, clusterDir, engine, clusterId, mode,
                settings.requiredReaders(profile.journal()), settings.alarmBytes(), settings.staleMs(), clock, out,
                err));
            status.put(profile.journal(), new Status(profile.journal(), mode, null, 0, 0, nowMs, 0, null));
        }
    }

    /**
     * The off-box bound: under {@code required} no record is purged whose log position is above the off-box floor.
     * The floor is worked out on every pass from the member's watermark file by {@link OffBoxWatermark#evaluate}, the
     * function the log pruner uses.
     *
     * @param mode           the member's {@code RETENTION_OFFBOX} mode
     * @param commitPosition the member's commit position, below 0 when it cannot be read
     */
    public JournalRetentionAgent offBox(final OffBoxWatermark.Mode mode, final LongSupplier commitPosition) {
        this.offBoxMode = Objects.requireNonNull(mode, "mode");
        this.commitPosition = Objects.requireNonNull(commitPosition, "commitPosition");
        return this;
    }

    /** Start the thread. Idempotent. */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        death = null;
        agentState = AgentState.RUNNING;
        thread = new Thread(this::run, "journal-retention");
        thread.setDaemon(true);
        thread.start();
    }

    /** Stop the thread; its archive session closes on the way out. */
    public synchronized void stop() {
        running = false;
        if (agentState != AgentState.DEAD) {
            agentState = AgentState.NOT_RUNNING;
        }
        final Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
        thread = null;
    }

    /** Whether the calling thread is the agent's thread still: not stopped, and not replaced by a later start. */
    private boolean mine() {
        return running && thread == Thread.currentThread();
    }

    private void run() {
        AeronArchive archive = null;
        // The archive port of each journal, kept while this thread's archive session lives, so a pass replays only
        // what was recorded since the pass before (RecordingTail). A local: no other thread, and no later thread of
        // this agent, can reach a port; a thread started after a stop begins with none.
        final Map<String, JournalRetention.AeronPort> ports = new LinkedHashMap<>();
        try {
            out.accept(JournalRetention.LOG + "started: " + settings + " (clusterDir=" + clusterDir + ")");
            while (mine()) {
                Thread.sleep(settings.pollMs());
                if (!mine()) {
                    return;
                }
                Throwable failed = null;
                try {
                    if (archive == null) {
                        archive = AeronArchive.connect(archiveContext.clone());
                    }
                    failed = passAll(archive, ports);
                    passFailure = null; // the session connected; a journal's own failure is in its status
                    if (failed == null) {
                        agentState = AgentState.RUNNING;
                    }
                } catch (final Throwable t) {
                    // before any journal was looked at: no archive session
                    failed = t;
                    if (stopping(t)) {
                        return;
                    }
                    passFailure = "journal retention pass failed: " + t;
                    err.accept(JournalRetention.LOG + "CRITICAL: the journal retention pass failed before any journal"
                        + " was looked at, nothing is purged until a pass succeeds: " + t);
                }
                if (failed != null) {
                    if (stopping(failed)) {
                        return;
                    }
                    agentState = AgentState.PASS_FAILING;
                    reportDriverLoss(failed);
                    // the session may be the broken part: the next pass connects a new one, with new ports
                    CloseHelper.quietClose(archive);
                    archive = null;
                    ports.clear();
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final Throwable t) {
            // nothing may leave this thread into the member; what the loop could not handle ends the thread, loudly
            if (mine()) {
                death = "journal retention thread stopped: " + t;
                agentState = AgentState.DEAD;
                try {
                    err.accept(JournalRetention.LOG + "CRITICAL: the journal retention thread stopped, this member"
                        + " purges nothing of its journals until it is restarted: " + t);
                } catch (final Throwable ignore) {
                    // the state and the /health text say it
                }
            }
        } finally {
            // a thread that a later start replaced leaves the state to its successor
            final Thread current = thread;
            if (agentState != AgentState.DEAD && (current == null || current == Thread.currentThread())) {
                agentState = AgentState.NOT_RUNNING;
            }
            ports.clear();
            CloseHelper.quietClose(archive);
        }
    }

    /** Whether a failure is {@link #stop} interrupting a call in flight: a stop, not a failure. */
    private boolean stopping(final Throwable t) {
        return !mine() || t instanceof InterruptedException || Thread.currentThread().isInterrupted();
    }

    /**
     * One pass over every journal. A journal whose pass throws (an exception or an error) is reported and counted,
     * and the next journal is still passed: one journal's failure never stops the other's purge.
     *
     * @return the first failure, or null when every journal's pass completed
     */
    private Throwable passAll(final AeronArchive archive, final Map<String, JournalRetention.AeronPort> ports) {
        Throwable first = null;
        for (final JournalRetention retention : retentions) {
            final String journal = retention.profile().journal();
            try {
                if (!mine()) {
                    return new InterruptedException("the journal retention thread was stopped");
                }
                passOne(archive, retention, ports);
            } catch (final Throwable t) {
                if (stopping(t)) {
                    return t;
                }
                if (first == null) {
                    first = t;
                }
                final String failure = "journal retention " + journal + " pass failed: " + t;
                synchronized (status) {
                    final Status before = status.get(journal);
                    status.put(journal, new Status(journal, retention.mode(), before.report, before.purgedBytesTotal,
                        before.purgeFailuresTotal, before.lastPassMs, before.passFailuresTotal + 1, failure,
                        before.lastPassDurationMs, before.tailReadBytes, before.tailReadBytesTotal));
                }
                err.accept(JournalRetention.LOG + "CRITICAL: the pass over the " + journal + " journal failed, nothing"
                    + " more of it is purged until a pass succeeds: " + t);
            }
        }
        return first;
    }

    private void passOne(final AeronArchive archive, final JournalRetention retention,
                         final Map<String, JournalRetention.AeronPort> ports) {
        final String journal = retention.profile().journal();
        beforePass.accept(journal);
        final long genesis = genesisId.getAsLong();
        final long nowMs = System.currentTimeMillis();
        JournalRetention.OffBox offBox = JournalRetention.OffBox.NONE;
        if (offBoxMode != OffBoxWatermark.Mode.OFF && retention.mode() != JournalRetention.Mode.OFF) {
            final OffBoxWatermark.Evaluation e = OffBoxWatermark.evaluate(offBoxMode,
                OffBoxWatermark.read(clusterDir), engine, clusterId, genesis, commitPosition.getAsLong());
            offBox = new JournalRetention.OffBox(offBoxMode, e);
            if (offBoxMode == OffBoxWatermark.Mode.REPORT) {
                out.accept(JournalRetention.LOG + OffBoxWatermark.MODE_ENV + "=report: the off-box floor (" + e
                    + ") is not applied to the " + journal + " journal");
            }
        }
        final JournalRetention.AeronPort port = ports.computeIfAbsent(journal, j -> new JournalRetention.AeronPort(
            archive.context().aeron(), archive, clock, retention.profile()));
        final long startNs = clock.nanoTime();
        final long replayedBefore = port.tail().replayBytes();
        final JournalRetention.Report report = retention.pass(port, genesis, offBox, nowMs,
            startNs + settings.passTimeoutMs() * 1_000_000L);
        final long durationMs = (clock.nanoTime() - startNs) / 1_000_000L;
        final long tailReadBytes = port.tail().replayBytes() - replayedBefore;
        if (!mine()) {
            return; // stopped or replaced while the pass ran: its successor reports
        }
        synchronized (status) {
            final Status before = status.get(journal);
            status.put(journal, new Status(journal, retention.mode(), report,
                before.purgedBytesTotal + report.purgedBytes, before.purgeFailuresTotal + report.purgeFailures,
                System.currentTimeMillis(), before.passFailuresTotal, null, durationMs, tailReadBytes,
                before.tailReadBytesTotal + tailReadBytes));
        }
    }

    /** A lost media driver, seen first by this thread: hand it to the Aeron client's error handler, once. */
    private void reportDriverLoss(final Throwable failure) {
        Throwable loss = failure;
        for (int depth = 0; loss != null && depth < 8 && !EngineLifecycle.isDriverLoss(loss); depth++) {
            loss = loss.getCause();
        }
        if (loss == null || !EngineLifecycle.isDriverLoss(loss) || !driverLossReported.compareAndSet(false, true)) {
            return;
        }
        final io.aeron.Aeron client = archiveContext.aeron();
        final ErrorHandler handler = client != null ? client.context().errorHandler() : archiveContext.errorHandler();
        err.accept(JournalRetention.LOG + "CRITICAL: the media driver is gone (seen first by a journal retention call),"
            + " reporting it to the client's error handler: " + loss);
        if (handler == null) {
            return;
        }
        try {
            handler.onError(loss);
        } catch (final RuntimeException handlerFailed) {
            err.accept(JournalRetention.LOG + "CRITICAL: the client's error handler failed on the driver loss: "
                + handlerFailed);
        }
    }

    // ------------------------------------------------------------------ what the member shows

    /** One journal as the last pass left it. */
    public Status status(final String journal) {
        synchronized (status) {
            return status.get(journal);
        }
    }

    /**
     * The text for {@code /health}: why a journal in mode {@code readers} is held, or that the pass fails; null when
     * there is nothing to say. A hold never makes a member unready: a member that keeps its journal serves.
     */
    public String lastError() {
        final StringBuilder sb = new StringBuilder();
        final String dead = death;
        if (dead != null) {
            sb.append(dead);
        }
        final String failure = passFailure;
        if (failure != null) {
            sb.append(sb.length() == 0 ? "" : "; ").append(failure);
        }
        synchronized (status) {
            for (final Status s : status.values()) {
                if (s.failure != null) {
                    sb.append(sb.length() == 0 ? "" : "; ").append(s.failure);
                } else if (s.report != null && s.report.problem != null) {
                    sb.append(sb.length() == 0 ? "" : "; ").append(s.report.problem);
                }
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /**
     * The gauges of every journal in Prometheus text, each series labelled {@code journal}; the per-reader series
     * also {@code reader}. {@code prefix} is the engine's metric prefix ({@code core}).
     */
    public String renderMetrics(final String prefix) {
        final List<Status> all;
        synchronized (status) {
            all = new ArrayList<>(status.values());
        }
        final long nowMs = System.currentTimeMillis();
        final String base = prefix + "_journal_retention_";
        final StringBuilder sb = new StringBuilder(2048);
        agentState(sb, prefix, agentState);
        family(sb, base + "mode", "gauge", "Journal retention mode (0 off, 1 report, 2 readers)", all,
            s -> s.mode.code());
        family(sb, base + "state", "gauge", "Journal retention state of the last pass (0 VALID, 1 OFF, 2 NO_JOURNAL,"
            + " 3 UNBOUND, 4 NO_READERS, 5 INVALID, 6 FOREIGN, 7 TAIL_UNKNOWN, 8 AHEAD, 9 MISSING, 10 OFFBOX_HOLD;"
            + " -1 before the first pass)", all, s -> s.report == null ? -1 : s.report.evaluation.state.code());
        family(sb, base + "safe_seq", "gauge", "journalSeq every registered reader has applied (-1 when not VALID)",
            all, s -> s.report == null ? -1 : s.report.evaluation.safeSeq);
        family(sb, base + "tail_seq", "gauge", "This member's own durable journal tail (-1 when not read)", all,
            s -> s.report == null ? -1 : s.report.tailSeq);
        family(sb, base + "retained_bytes", "gauge", "Bytes this member's archive holds of the journal", all,
            s -> s.report == null ? -1 : s.report.retainedBytes);
        family(sb, base + "recordings", "gauge", "Recordings of the journal that hold bytes", all,
            s -> s.report == null ? -1 : s.report.recordings);
        family(sb, base + "recordings_foreign", "gauge", "Recordings of another genesis on the journal's stream"
            + " (never touched)", all, s -> s.report == null ? -1 : s.report.recordingsForeign);
        family(sb, base + "recordings_anomaly", "gauge", "Recordings with an anomaly in the last pass: a journalSeq"
            + " that does not rise (not purged), or an unreadable record at the start or at a segment base", all,
            s -> s.report == null ? -1 : s.report.anomalies.size());
        family(sb, base + "oldest_reader_age_ms", "gauge", "Age of the oldest reader file (-1 when there is none)",
            all, s -> s.report == null ? -1 : s.report.oldestReaderAgeMs);
        family(sb, base + "purged_bytes_total", "counter", "Journal bytes this process purged", all,
            s -> s.purgedBytesTotal);
        family(sb, base + "purge_failures_total", "counter", "Purges the archive refused (tried again on a later"
            + " pass)", all, s -> s.purgeFailuresTotal);
        family(sb, base + "pass_failures_total", "counter", "Passes over the journal that failed (an exception or an"
            + " error out of the pass)", all, s -> s.passFailuresTotal);
        family(sb, base + "last_pass_age_ms", "gauge", "Time since the last completed pass (since the start before"
            + " the first)", all, s -> Math.max(0, nowMs - s.lastPassMs));
        family(sb, base + "last_pass_duration_ms", "gauge", "Time the last completed pass over the journal took"
            + " (-1 before the first)", all, s -> s.lastPassDurationMs);
        family(sb, base + "tail_read_bytes", "gauge", "Bytes the last completed pass asked the archive to replay to"
            + " read the last record of each recording (0 when no recording grew; up to one term per recording on a"
            + " first look)", all, s -> s.tailReadBytes);
        family(sb, base + "tail_read_bytes_total", "counter", "Bytes this process asked the archive to replay to read"
            + " the last records, over every completed pass", all, s -> s.tailReadBytesTotal);
        readers(sb, base + "reader_applied_seq", "journalSeq a registered reader has applied", all, true);
        readers(sb, base + "reader_released_seq", "journalSeq an operator released a reader's hold up to (-1: none)",
            all, false);
        return sb.toString();
    }

    /** The thread's state as the last line of defence sees it. */
    public AgentState agentState() {
        return agentState;
    }

    /**
     * The one series an engine exports when it could not construct or start the agent, so the failure is a value on
     * {@code /metrics} and not a missing family: {@code <prefix>_journal_retention_agent_state 4}.
     */
    public static String renderNotStarted(final String prefix) {
        final StringBuilder sb = new StringBuilder(256);
        agentState(sb, prefix, AgentState.START_FAILED);
        return sb.toString();
    }

    private static void agentState(final StringBuilder sb, final String prefix, final AgentState state) {
        final String name = prefix + "_journal_retention_agent_state";
        sb.append("# HELP ").append(name).append(" Journal retention thread (0 not running, 1 running, 2 the last pass"
            + " failed, 3 dead until the member restarts, 4 did not start)\n");
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append(' ').append(state.code()).append('\n');
    }

    private interface Value {
        long of(Status s);
    }

    private static void family(final StringBuilder sb, final String name, final String type, final String help,
                               final List<Status> all, final Value value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        for (final Status s : all) {
            sb.append(name).append("{journal=\"").append(s.journal).append("\"} ").append(value.of(s)).append('\n');
        }
    }

    private static void readers(final StringBuilder sb, final String name, final String help, final List<Status> all,
                                final boolean applied) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        for (final Status s : all) {
            if (s.report == null) {
                continue;
            }
            for (final JournalRetention.ReaderStatus r : s.report.readers) {
                // journal and reader names are checked names (letters, digits, . _ -): no escaping is needed
                sb.append(name).append("{journal=\"").append(s.journal).append("\",reader=\"").append(r.name())
                    .append("\"} ").append(applied ? r.appliedSeq() : r.releasedSeq()).append('\n');
            }
        }
    }
}
