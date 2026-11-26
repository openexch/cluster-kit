// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.OffBoxWatermark;
import com.openexchange.cluster.journal.JournalRetention.Mode;
import com.openexchange.cluster.journal.JournalRetention.Settings;
import com.openexchange.cluster.journal.JournalRetention.State;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The agent thread on a real archive: it purges on its own, on its own archive session, from the reader files alone;
 * its gauges and its {@code /health} text follow the passes; the settings refuse a bad value. Nothing here calls a
 * pass: the test writes a reader file and waits for the archive's start position.
 */
public class JournalRetentionAgentTest {

    private JournalRig rig;
    private JournalRetentionAgent agent;
    private final List<String> out = new CopyOnWriteArrayList<>();
    private final List<String> err = new CopyOnWriteArrayList<>();
    private volatile long genesis = JournalRig.GENESIS;

    @Before
    public void setUp() {
        rig = new JournalRig();
    }

    @After
    public void tearDown() {
        if (agent != null) {
            agent.stop();
        }
        rig.close();
    }

    private static Settings settings(final Map<String, String> env) {
        return Settings.parse((e, p) -> env.get(e), "money", "order");
    }

    private JournalRetentionAgent agent(final Map<String, String> env) {
        agent = new JournalRetentionAgent(rig.clusterDir, rig.archiveContext(), "core", 0, () -> genesis,
            settings(env), List.of(JournalRig.MONEY, JournalRig.ORDER), line -> {
                out.add(line);
                System.out.println(line);
            }, line -> {
                err.add(line);
                System.out.println(line);
            });
        return agent;
    }

    private static void await(final BooleanSupplier done, final String what) {
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (!done.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out waiting for " + what);
            }
            try {
                Thread.sleep(20);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private State state(final String journal) {
        final JournalRetentionAgent.Status s = agent.status(journal);
        return s.report == null ? null : s.report.evaluation.state;
    }

    @Test
    public void theAgentPurgesByItselfFromTheReaderFiles() {
        final JournalRig.Writer order = rig.writer(JournalRig.ORDER).append(1, 2_600);
        agent(Map.of("JOURNAL_RETENTION_ORDER", "readers", "JOURNAL_RETENTION_POLL_MS", "1000")).start();
        // no reader is registered: held, CRITICAL, said on /health; the money journal is off and dark
        await(() -> state("order") == State.NO_READERS, "the first pass");
        assertEquals(0, rig.start(order.recordingId));
        assertTrue(String.valueOf(agent.lastError()), agent.lastError().contains("journal retention order NO_READERS"));
        assertTrue(err.toString(), err.stream().anyMatch(l -> l.contains("CRITICAL: no reader is registered for the"
            + " order journal")));
        assertEquals(State.OFF, state("money"));
        String metrics = agent.renderMetrics("core");
        assertTrue(metrics, metrics.contains("core_journal_retention_mode{journal=\"order\"} 2\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_mode{journal=\"money\"} 0\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_state{journal=\"order\"} 4\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_safe_seq{journal=\"order\"} -1\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_tail_seq{journal=\"order\"} 2600\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_retained_bytes{journal=\"order\"} "
            + 2_600L * 256 + "\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_recordings{journal=\"order\"} 1\n"));
        assertTrue(metrics, metrics.contains("# TYPE core_journal_retention_purged_bytes_total counter\n"));

        // the reader registers and moves: the start position rises with no call from here
        rig.set(JournalRig.ORDER, "oe-projector", 1_000);
        await(() -> rig.start(order.recordingId) == 3L * JournalRig.SEG, "the start to reach base 3 (journalSeq 769)");
        await(() -> state("order") == State.VALID, "a VALID pass");
        assertNull(agent.lastError());
        rig.set(JournalRig.ORDER, "oe-projector", 2_600);
        final long clamp = (order.position() & -(long) JournalRig.SEG) - 2L * JournalRig.SEG;
        await(() -> rig.start(order.recordingId) == clamp, "the start to reach two segments below the head");
        await(() -> agent.status("order").purgedBytesTotal == clamp, "the purged bytes total");
        metrics = agent.renderMetrics("core");
        assertTrue(metrics, metrics.contains("core_journal_retention_state{journal=\"order\"} 0\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_safe_seq{journal=\"order\"} 2600\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_purged_bytes_total{journal=\"order\"} " + clamp
            + "\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_purge_failures_total{journal=\"order\"} 0\n"));
        assertTrue(metrics, metrics.contains(
            "core_journal_retention_reader_applied_seq{journal=\"order\",reader=\"oe-projector\"} 2600\n"));
        assertTrue(metrics, metrics.contains(
            "core_journal_retention_reader_released_seq{journal=\"order\",reader=\"oe-projector\"} -1\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_last_pass_age_ms{journal=\"order\"} "));
        assertTrue(metrics, metrics.contains("core_journal_retention_recordings_foreign{journal=\"order\"} 0\n"));
        assertTrue("the thread is named", Thread.getAllStackTraces().keySet().stream()
            .anyMatch(t -> t.getName().equals("journal-retention")));
        assertTrue(metrics, metrics.contains("core_journal_retention_agent_state 1\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_pass_failures_total{journal=\"order\"} 0\n"));
        assertEquals(JournalRetentionAgent.AgentState.RUNNING, agent.agentState());
        agent.stop();
        await(() -> Thread.getAllStackTraces().keySet().stream().noneMatch(t -> t.getName().equals("journal-retention")),
            "the thread to end");
        assertEquals(JournalRetentionAgent.AgentState.NOT_RUNNING, agent.agentState());
    }

    @Test
    public void aRequiredReaderWithoutAFileHoldsAndTheOffBoxBoundIsApplied() throws Exception {
        final JournalRig.Writer order = rig.writer(JournalRig.ORDER).append(1, 2_600);
        rig.set(JournalRig.ORDER, "audit", 2_600);
        agent(Map.of("JOURNAL_RETENTION_ORDER", "readers", "JOURNAL_RETENTION_POLL_MS", "1000",
            "JOURNAL_RETENTION_REQUIRED_READERS_ORDER", "oe-projector,audit"))
            .offBox(OffBoxWatermark.Mode.REQUIRED, () -> JournalRig.logPositionOf(2_600)).start();
        await(() -> state("order") == State.MISSING, "the hold for the required reader");
        assertEquals(0, rig.start(order.recordingId));
        rig.set(JournalRig.ORDER, "oe-projector", 2_600);
        // both readers are registered now; no off-box watermark is set: still a hold
        await(() -> state("order") == State.OFFBOX_HOLD, "the hold for the off-box watermark");
        assertEquals(0, rig.start(order.recordingId));
        // the off-box copy reaches journalSeq 600: base 2 (journalSeq 513) goes, base 3 (769) stays
        OffBoxWatermark.write(rig.clusterDir, new OffBoxWatermark.Record("core", 0, JournalRig.GENESIS,
            JournalRig.logPositionOf(600), -1, "", 1, System.currentTimeMillis(), "archiver"));
        await(() -> rig.start(order.recordingId) == 2L * JournalRig.SEG, "the start to reach base 2");
        await(() -> state("order") == State.VALID, "a VALID pass");
        Thread.sleep(1_500); // one more pass: it must not go past the off-box floor
        assertEquals(2L * JournalRig.SEG, rig.start(order.recordingId));
    }

    @Test
    public void anUnboundMemberHoldsUntilItHasAGenesis() {
        final JournalRig.Writer order = rig.writer(JournalRig.ORDER).append(1, 2_600);
        rig.set(JournalRig.ORDER, "oe-projector", 2_600);
        genesis = 0;
        agent(Map.of("JOURNAL_RETENTION_ORDER", "readers", "JOURNAL_RETENTION_POLL_MS", "1000")).start();
        await(() -> state("order") == State.UNBOUND, "the unbound hold");
        assertEquals(0, rig.start(order.recordingId));
        genesis = JournalRig.GENESIS;
        await(() -> rig.start(order.recordingId) > 0, "the start to rise once the member is bound");
    }

    @Test
    public void offModeStillShowsWhatTheJournalHolds() {
        rig.writer(JournalRig.ORDER).append(1, 600);
        agent(Map.of("JOURNAL_RETENTION_POLL_MS", "1000")).start();
        await(() -> state("order") == State.OFF, "the first pass");
        final String metrics = agent.renderMetrics("core");
        assertTrue(metrics, metrics.contains("core_journal_retention_state{journal=\"order\"} 1\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_retained_bytes{journal=\"order\"} " + 600L * 256
            + "\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_retained_bytes{journal=\"money\"} 0\n"));
        assertNull(agent.lastError());
        assertTrue("off prints no alarm: " + err, err.isEmpty());
    }

    @Test
    public void settingsDefaultsAndRefusals() {
        final Settings d = settings(Map.of());
        assertEquals(Mode.OFF, d.mode("money"));
        assertEquals(Mode.OFF, d.mode("order"));
        assertEquals(60_000, d.pollMs());
        assertEquals(10_000, d.passTimeoutMs());
        assertEquals(2_147_483_648L, d.alarmBytes());
        assertEquals(3_600_000, d.staleMs());
        assertEquals(Set.of(), d.requiredReaders("order"));
        final Settings s = settings(Map.of("JOURNAL_RETENTION_MONEY", "report", "JOURNAL_RETENTION_ORDER", " readers ",
            "JOURNAL_RETENTION_REQUIRED_READERS_MONEY", "money-projector", "JOURNAL_RETENTION_REQUIRED_READERS_ORDER",
            "oe-projector, audit", "JOURNAL_RETENTION_POLL_MS", "1000", "JOURNAL_RETENTION_PASS_TIMEOUT_MS", "600000",
            "JOURNAL_RETENTION_ALARM_BYTES", "1048576", "JOURNAL_RETENTION_STALE_MS", "604800000"));
        assertEquals(Mode.REPORT, s.mode("money"));
        assertEquals(Mode.READERS, s.mode("order"));
        assertEquals(Set.of("money-projector"), s.requiredReaders("money"));
        assertEquals(Set.of("oe-projector", "audit"), s.requiredReaders("order"));
        assertNotNull(s.toString());
        for (final String[] bad : new String[][] {
            {"JOURNAL_RETENTION_MONEY", "on", "JOURNAL_RETENTION_MONEY must be off, report or readers, got 'on'"},
            {"JOURNAL_RETENTION_ORDER", "READERS", "JOURNAL_RETENTION_ORDER must be off, report or readers"},
            {"JOURNAL_RETENTION_POLL_MS", "999", "JOURNAL_RETENTION_POLL_MS must be 1000..3600000, got '999'"},
            {"JOURNAL_RETENTION_POLL_MS", "3600001", "JOURNAL_RETENTION_POLL_MS must be 1000..3600000"},
            {"JOURNAL_RETENTION_POLL_MS", "1s", "JOURNAL_RETENTION_POLL_MS must be a number in 1000..3600000"},
            {"JOURNAL_RETENTION_PASS_TIMEOUT_MS", "999", "JOURNAL_RETENTION_PASS_TIMEOUT_MS must be 1000..600000"},
            {"JOURNAL_RETENTION_PASS_TIMEOUT_MS", "600001", "JOURNAL_RETENTION_PASS_TIMEOUT_MS must be 1000..600000"},
            {"JOURNAL_RETENTION_ALARM_BYTES", "1048575", "JOURNAL_RETENTION_ALARM_BYTES must be 1048576..1099511627776"},
            {"JOURNAL_RETENTION_ALARM_BYTES", "1099511627777", "JOURNAL_RETENTION_ALARM_BYTES must be"},
            {"JOURNAL_RETENTION_STALE_MS", "999", "JOURNAL_RETENTION_STALE_MS must be 1000..604800000"},
            {"JOURNAL_RETENTION_STALE_MS", "604800001", "JOURNAL_RETENTION_STALE_MS must be 1000..604800000"},
            {"JOURNAL_RETENTION_REQUIRED_READERS_ORDER", "a b", "JOURNAL_RETENTION_REQUIRED_READERS_ORDER must be"},
            {"JOURNAL_RETENTION_REQUIRED_READERS_MONEY", "a,,b", "JOURNAL_RETENTION_REQUIRED_READERS_MONEY must be"},
        }) {
            final String message = assertThrows(bad[0] + "=" + bad[1], IllegalArgumentException.class,
                () -> settings(Map.of(bad[0], bad[1]))).getMessage();
            assertTrue(message, message.contains(bad[2]));
        }
        assertThrows("a journal nobody read settings for", IllegalArgumentException.class, () -> d.mode("me"));
    }

    private static boolean threadAlive() {
        return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals("journal-retention"));
    }

    /** One journal's pass throwing (an Error, not an Exception) never stops the other journal's pass or the thread. */
    @Test
    public void anErrorOutOfOneJournalsPassIsCountedAndTheOtherJournalIsStillPurged() {
        rig.writer(JournalRig.MONEY).append(1, 4_000);
        final JournalRig.Writer order = rig.writer(JournalRig.ORDER).append(1, 2_600);
        rig.set(JournalRig.MONEY, "money-projector", 4_000);
        rig.set(JournalRig.ORDER, "oe-projector", 2_600);
        agent(Map.of("JOURNAL_RETENTION_MONEY", "readers", "JOURNAL_RETENTION_ORDER", "readers",
            "JOURNAL_RETENTION_POLL_MS", "1000"));
        assertEquals(JournalRetentionAgent.AgentState.NOT_RUNNING, agent.agentState());
        agent.beforePass = journal -> {
            if ("money".equals(journal)) {
                throw new StackOverflowError("a fault in the money pass");
            }
        };
        agent.start();
        // money is passed first and throws; order is still passed and purged
        final long clamp = (order.position() & -(long) JournalRig.SEG) - 2L * JournalRig.SEG;
        await(() -> rig.start(order.recordingId) == clamp, "the order journal to be purged although the money pass throws");
        await(() -> agent.status("money").passFailuresTotal >= 2, "two failed money passes");
        assertTrue("the thread lives", threadAlive());
        assertEquals(JournalRetentionAgent.AgentState.PASS_FAILING, agent.agentState());
        assertTrue(agent.status("money").failure, agent.status("money").failure.contains(
            "journal retention money pass failed: java.lang.StackOverflowError: a fault in the money pass"));
        assertNull("the order journal's status is its own", agent.status("order").failure);
        assertEquals(State.VALID, state("order"));
        assertTrue(agent.lastError(), agent.lastError().contains("journal retention money pass failed"));
        assertTrue(err.toString(), err.stream().anyMatch(l -> l.contains("CRITICAL: the pass over the money journal"
            + " failed")));
        String metrics = agent.renderMetrics("core");
        assertTrue(metrics, metrics.contains("core_journal_retention_agent_state 2\n"));
        assertFalse(metrics, metrics.contains("core_journal_retention_pass_failures_total{journal=\"money\"} 0\n"));
        assertTrue(metrics, metrics.contains("core_journal_retention_pass_failures_total{journal=\"order\"} 0\n"));
        // nothing latches: the fault goes, the money journal is purged, the state is RUNNING again
        agent.beforePass = journal -> { };
        await(() -> state("money") == State.VALID && agent.status("money").failure == null
            && agent.status("money").purgedBytesTotal > 0, "the money journal to be purged after the fault");
        await(() -> agent.agentState() == JournalRetentionAgent.AgentState.RUNNING, "the state to be RUNNING again");
        assertNull(agent.lastError());
    }

    /** What the loop cannot handle ends the thread, and the member can see that it did. */
    @Test
    public void aThreadThatDiesSaysSoOnTheGaugeAndOnHealthAndThrowsNothing() {
        rig.writer(JournalRig.ORDER).append(1, 600);
        final java.util.concurrent.atomic.AtomicBoolean logBroken = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicReference<Throwable> uncaught =
            new java.util.concurrent.atomic.AtomicReference<>();
        final Thread.UncaughtExceptionHandler before = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> uncaught.set(e));
        try {
            agent = new JournalRetentionAgent(rig.clusterDir, rig.archiveContext(), "core", 0, () -> genesis,
                settings(Map.of("JOURNAL_RETENTION_ORDER", "readers", "JOURNAL_RETENTION_POLL_MS", "1000")),
                List.of(JournalRig.MONEY, JournalRig.ORDER), out::add, line -> {
                    err.add(line);
                    if (logBroken.get() && line.contains("pass")) {
                        throw new OutOfMemoryError("the log is broken too");
                    }
                });
            agent.beforePass = journal -> {
                throw new IllegalStateException("a fault in every pass");
            };
            logBroken.set(true);
            agent.start();
            await(() -> agent.agentState() == JournalRetentionAgent.AgentState.DEAD, "the thread to die");
            await(() -> !threadAlive(), "the thread to end");
            assertTrue(agent.lastError(), agent.lastError().contains("journal retention thread stopped:"
                + " java.lang.OutOfMemoryError: the log is broken too"));
            assertTrue(agent.renderMetrics("core"), agent.renderMetrics("core").contains(
                "core_journal_retention_agent_state 3\n"));
            assertTrue(err.toString(), err.stream().anyMatch(l -> l.contains("CRITICAL: the journal retention thread"
                + " stopped, this member purges nothing of its journals until it is restarted")));
            assertNull("nothing left the thread: " + uncaught.get(), uncaught.get());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(before);
        }
    }

    @Test
    public void anEngineThatCouldNotStartTheAgentStillExportsASeries() {
        final String text = JournalRetentionAgent.renderNotStarted("core");
        assertTrue(text, text.contains("# TYPE core_journal_retention_agent_state gauge\n"));
        assertTrue(text, text.endsWith("core_journal_retention_agent_state 4\n"));
    }
}
