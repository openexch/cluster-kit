// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.Counter;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * {@link StallMetrics} on a real media driver and archive (their own cycle counters), with the cluster's counters
 * allocated here under Aeron's type ids and keys so the test moves them: which counter each series follows, the
 * baseline, a rise and its age, the recent window, a source that is absent, and the text a scrape gets.
 */
public class StallMetricsTest {

    private static final int CLUSTER_ID = 7;
    private File dir;
    private ArchivingMediaDriver driver;
    private Aeron aeron;
    private final List<Counter> allocated = new ArrayList<>();
    private long nowMs = 1_000_000_000_000L;

    @Before
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("kit-stall-").toFile();
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        driver = ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true)
                // a freed counter slot is given out again at once (the reallocation test)
                .counterFreeToReuseTimeoutNs(0),
            new Archive.Context().aeronDirectoryName(aeronDir).archiveDir(new File(dir, "archive"))
                .controlChannelEnabled(false).archiveId(4242)
                .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                .replicationChannel("aeron:udp?endpoint=localhost:0").recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED).deleteArchiveOnStart(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
    }

    @After
    public void tearDown() {
        CloseHelper.quietCloseAll(allocated);
        CloseHelper.quietCloseAll(aeron, driver);
        IoUtil.delete(dir, true);
    }

    /** A counter keyed the way Aeron keys a consensus module counter (cluster id) or a service's (then service id). */
    private Counter clusterCounter(final int typeId, final int clusterId, final int serviceId) {
        final UnsafeBuffer key = new UnsafeBuffer(new byte[8]);
        key.putInt(0, clusterId);
        key.putInt(4, serviceId);
        final UnsafeBuffer label = new UnsafeBuffer(("test counter " + typeId).getBytes());
        final Counter c = aeron.addCounter(typeId, key, 0, serviceId < 0 ? 4 : 8, label, 0, label.capacity());
        allocated.add(c);
        final CountersReader counters = aeron.countersReader();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (counters.getCounterState(c.id()) != CountersReader.RECORD_ALLOCATED) {
            assertTrue(System.nanoTime() < deadline);
            Thread.yield();
        }
        return c;
    }

    private StallMetrics metrics() {
        return new StallMetrics("match", aeron.countersReader(), CLUSTER_ID, 0, () -> nowMs);
    }

    private static String value(final String body, final String series) {
        for (final String line : body.split("\n")) {
            if (line.startsWith(series + " ")) {
                return line.substring(series.length() + 1);
            }
        }
        return null;
    }

    private void seconds(final StallMetrics m, final int n) {
        for (int i = 0; i < n; i++) {
            nowMs += 1_000;
            m.sample();
        }
    }

    @Test
    public void theFixedSystemCounterIdsAreTheDriversCycleCounters() {
        final CountersReader counters = aeron.countersReader();
        assertTrue(counters.getCounterLabel(26), counters.getCounterLabel(26).contains("Conductor max cycle time"));
        assertTrue(counters.getCounterLabel(27), counters.getCounterLabel(27).contains("Conductor work cycle exceeded"));
        assertTrue(counters.getCounterLabel(28), counters.getCounterLabel(28).contains("Sender max cycle time"));
        assertTrue(counters.getCounterLabel(29), counters.getCounterLabel(29).contains("Sender work cycle exceeded"));
        assertTrue(counters.getCounterLabel(30), counters.getCounterLabel(30).contains("Receiver max cycle time"));
        assertTrue(counters.getCounterLabel(31), counters.getCounterLabel(31).contains("Receiver work cycle exceeded"));
    }

    @Test
    public void aMaximumItsRiseItsAgeAndTheRecentWindow() {
        final Counter other = clusterCounter(AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID, CLUSTER_ID + 1, -1);
        final Counter max = clusterCounter(AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID, CLUSTER_ID, -1);
        other.setOrdered(999_000_000_000L);
        max.setOrdered(2_500_000L); // 2.5 ms before the sampler's first read
        final StallMetrics m = metrics();
        assertEquals("-1", value(m.renderMetrics(), "match_stall_sampler_age_seconds"));
        m.sample();
        String body = m.renderMetrics();
        assertEquals("the counter of this cluster id", "0.0025",
            value(body, "match_consensus_module_max_cycle_time_seconds"));
        assertEquals("the first read is the baseline, not a rise", "-1",
            value(body, "match_consensus_module_max_cycle_time_last_rise_age_seconds"));
        assertEquals("0", value(body, "match_consensus_module_max_cycle_time_recent_seconds"));
        assertEquals("0", value(body, "match_stall_sampler_age_seconds"));

        seconds(m, 10);
        max.setOrdered(1_400_000_000L); // a 1.4 s cycle
        seconds(m, 1);
        nowMs += 42_000; // 42 s later, no sample in between: the age is from the sample that saw the rise
        body = m.renderMetrics();
        assertEquals("1.4", value(body, "match_consensus_module_max_cycle_time_seconds"));
        assertEquals("42", value(body, "match_consensus_module_max_cycle_time_last_rise_age_seconds"));
        assertEquals("1.4", value(body, "match_consensus_module_max_cycle_time_recent_seconds"));
        assertEquals("42", value(body, "match_stall_sampler_age_seconds"));

        seconds(m, StallMetrics.WINDOW_SECONDS - 42);
        assertEquals("on the edge of the window", "1.4",
            value(m.renderMetrics(), "match_consensus_module_max_cycle_time_recent_seconds"));
        seconds(m, 1);
        body = m.renderMetrics();
        assertEquals("the maximum stays", "1.4", value(body, "match_consensus_module_max_cycle_time_seconds"));
        assertEquals("out of the window", "0", value(body, "match_consensus_module_max_cycle_time_recent_seconds"));
        assertEquals(String.valueOf(StallMetrics.WINDOW_SECONDS + 1),
            value(body, "match_consensus_module_max_cycle_time_last_rise_age_seconds"));
    }

    @Test
    public void anOverThresholdCountItsLastRiseAndTheRecentWindow() {
        final Counter otherService = clusterCounter(
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID, CLUSTER_ID, 1);
        final Counter exceeded = clusterCounter(
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID, CLUSTER_ID, 0);
        otherService.setOrdered(77);
        exceeded.setOrdered(5);
        final StallMetrics m = metrics();
        m.sample();
        String body = m.renderMetrics();
        assertEquals("the counter of service 0", "5", value(body, "match_service_cycle_time_exceeded_total"));
        assertEquals("0", value(body, "match_service_cycle_time_exceeded_recent"));
        assertEquals("-1", value(body, "match_service_cycle_time_exceeded_last_age_seconds"));
        assertTrue(body, body.contains("# TYPE match_service_cycle_time_exceeded_total counter\n"));

        seconds(m, 3);
        exceeded.setOrdered(8);
        seconds(m, 100);
        exceeded.setOrdered(9);
        seconds(m, 1);
        body = m.renderMetrics();
        assertEquals("9", value(body, "match_service_cycle_time_exceeded_total"));
        assertEquals("both rises are in the window", "4", value(body, "match_service_cycle_time_exceeded_recent"));
        assertEquals("0", value(body, "match_service_cycle_time_exceeded_last_age_seconds"));

        // the first rise was seen by the sample 100 samples ago. A rise seen by sample A is inside the window of
        // the samples A .. A + 299: the window's oldest sample is then older than A
        seconds(m, StallMetrics.WINDOW_SECONDS - 102);
        assertEquals("298 samples after the one that saw the first rise", "4",
            value(m.renderMetrics(), "match_service_cycle_time_exceeded_recent"));
        seconds(m, 1);
        assertEquals("299 samples after it: the window's oldest sample is the one before the rise", "4",
            value(m.renderMetrics(), "match_service_cycle_time_exceeded_recent"));
        seconds(m, 1);
        assertEquals("300 samples after it: the first rise left the window of 300 intervals", "1",
            value(m.renderMetrics(), "match_service_cycle_time_exceeded_recent"));
        seconds(m, 99);
        assertEquals("299 samples after the one that saw the second rise", "1",
            value(m.renderMetrics(), "match_service_cycle_time_exceeded_recent"));
        seconds(m, 1);
        body = m.renderMetrics();
        assertEquals("0", value(body, "match_service_cycle_time_exceeded_recent"));
        assertEquals("9", value(body, "match_service_cycle_time_exceeded_total"));
        assertEquals(String.valueOf(StallMetrics.WINDOW_SECONDS),
            value(body, "match_service_cycle_time_exceeded_last_age_seconds"));
        assertTrue(body, body.contains("# HELP match_service_cycle_time_exceeded_recent Rise of"
            + " match_service_cycle_time_exceeded_total between the newest sample and the one 300 samples before"
            + " it"));
    }

    @Test
    public void everySourceOfARealDriverAndArchiveAndNoneThatIsAbsent() {
        final StallMetrics m = metrics();
        m.sample();
        String body = m.renderMetrics();
        for (final String agent : new String[] {"conductor", "sender", "receiver"}) {
            assertTrue(body, value(body, "match_driver_" + agent + "_max_cycle_time_seconds") != null);
            assertTrue(body, value(body, "match_driver_" + agent + "_cycle_time_exceeded_total") != null);
        }
        assertTrue(body, value(body, "match_archive_max_cycle_time_seconds{archive_id=\"4242\",agent=\"conductor\"}") != null);
        assertTrue(body, value(body, "match_archive_cycle_time_exceeded_total{archive_id=\"4242\",agent=\"conductor\"}") != null);
        assertTrue(body, value(body, "match_archive_cycle_time_exceeded_recent{archive_id=\"4242\",agent=\"conductor\"}") != null);
        // no cluster on this driver: its sources have no series, and no head either
        assertFalse(body, body.contains("match_consensus_module_"));
        assertFalse(body, body.contains("match_service_"));
        assertFalse(body, body.contains("match_snapshot_"));

        // the cluster's counters appear later (the service container launches after the sampler started)
        final Counter total = clusterCounter(AeronCounters.CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID, CLUSTER_ID,
            -1);
        final Counter service = clusterCounter(AeronCounters.CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
            CLUSTER_ID, 0);
        clusterCounter(AeronCounters.CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID, CLUSTER_ID, -1);
        clusterCounter(AeronCounters.CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID, CLUSTER_ID, 0);
        clusterCounter(AeronCounters.CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID, CLUSTER_ID, -1);
        clusterCounter(AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID, CLUSTER_ID, 0);
        seconds(m, StallMetrics.RESCAN_EVERY);
        total.setOrdered(3_000_000_000L);
        service.setOrdered(2_750_000_000L);
        seconds(m, 2);
        body = m.renderMetrics();
        assertEquals("3.0", value(body, "match_snapshot_total_max_duration_seconds"));
        assertEquals("3.0", value(body, "match_snapshot_total_max_duration_recent_seconds"));
        assertEquals("1", value(body, "match_snapshot_total_max_duration_last_rise_age_seconds"));
        assertEquals("2.75", value(body, "match_snapshot_service_max_duration_seconds"));
        assertEquals("0", value(body, "match_snapshot_total_duration_exceeded_total"));
        assertEquals("0", value(body, "match_snapshot_service_duration_exceeded_total"));
        assertEquals("0", value(body, "match_consensus_module_cycle_time_exceeded_total"));
        assertEquals("0.0", value(body, "match_service_max_cycle_time_seconds"));

        // every series of one name stands together under one HELP and one TYPE line
        final List<String> names = new ArrayList<>();
        String current = null;
        for (final String line : body.split("\n")) {
            if (line.startsWith("# TYPE ")) {
                current = line.split(" ")[2];
                assertFalse("one TYPE line for " + current, names.contains(current));
                names.add(current);
            } else if (!line.startsWith("#")) {
                final String name = line.substring(0, line.indexOf(line.contains("{") ? '{' : ' '));
                assertEquals(line, current, name);
            }
        }

        // a counter that goes away has no series any more
        total.close();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (aeron.countersReader().getCounterState(total.id()) == CountersReader.RECORD_ALLOCATED) {
            assertTrue(System.nanoTime() < deadline);
            Thread.yield();
        }
        seconds(m, 1);
        assertFalse(m.renderMetrics().contains("match_snapshot_total_max_duration_seconds"));
    }

    /**
     * The engine's way in: a mapping of the driver's counters file that this object owns. An Aeron client of the
     * process that closes while the sampler runs does not take the counters away from it.
     */
    @Test
    public void openReadsTheDriversCountersFileAndOutlivesAClientOfTheProcess() throws Exception {
        final Aeron other = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeron.context()
            .aeronDirectoryName()));
        final Counter max = clusterCounter(AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID, CLUSTER_ID, -1);
        final StallMetrics m = StallMetrics.open("match", aeron.context().aeronDirectoryName(), CLUSTER_ID, 0);
        try {
            m.start();
            final long deadline = System.nanoTime() + 10_000_000_000L;
            while (value(m.renderMetrics(), "match_consensus_module_max_cycle_time_seconds") == null) {
                assertTrue(System.nanoTime() < deadline);
                Thread.sleep(20);
            }
            other.close();
            max.setOrdered(730_000_000L);
            while (!"0.73".equals(value(m.renderMetrics(), "match_consensus_module_max_cycle_time_seconds"))) {
                assertTrue(m.renderMetrics(), System.nanoTime() < deadline);
                Thread.sleep(20);
            }
            assertTrue(value(m.renderMetrics(), "match_archive_max_cycle_time_seconds{archive_id=\"4242\",agent=\"conductor\"}")
                != null);
        } finally {
            m.close();
        }
        assertEquals("closed: no series, and no read of a released mapping", "", m.renderMetrics());
        m.sample();
        m.start();
        assertEquals("", m.renderMetrics());
        assertThrows(RuntimeException.class, () -> StallMetrics.open("match", new File(dir, "no-driver")
            .getAbsolutePath(), CLUSTER_ID, 0));
    }

    /**
     * A counter is freed and its slot given to another counter of the same type (a component that was closed and
     * launched again): the series follows the new counter from a new baseline, with the new counter's labels. Its
     * first value is not a rise.
     */
    @Test
    public void aSlotGivenToAnotherCounterOfTheSameTypeIsLocatedAgain() {
        final Counter first = archiveCounter(AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID, 71, "archive-recorder max");
        first.setOrdered(5_000_000L);
        final StallMetrics m = metrics();
        m.sample();
        final String before = "match_archive_max_cycle_time_seconds{archive_id=\"71\",agent=\"recorder\"}";
        assertEquals("0.005", value(m.renderMetrics(), before));
        final int slot = first.id();
        first.close();
        allocated.remove(first);
        final long deadline = System.nanoTime() + 5_000_000_000L;
        Counter second;
        // the freed slot comes back for the next counter (free-to-reuse timeout 0)
        while (true) {
            second = archiveCounter(AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID, 72, "archive-replayer max");
            if (second.id() == slot) {
                break;
            }
            assertTrue("the slot " + slot + " was not given out again", System.nanoTime() < deadline);
        }
        second.setOrdered(9_000_000L);
        seconds(m, StallMetrics.RESCAN_EVERY);
        final String body = m.renderMetrics();
        assertEquals("the old archive's series is gone", null, value(body, before));
        final String after = "{archive_id=\"72\",agent=\"replayer\"}";
        assertEquals("0.009", value(body, "match_archive_max_cycle_time_seconds" + after));
        assertEquals("a new baseline, not a rise from 5 ms to 9 ms", "-1",
            value(body, "match_archive_max_cycle_time_last_rise_age_seconds" + after));
        assertEquals("0", value(body, "match_archive_max_cycle_time_recent_seconds" + after));
    }

    private Counter archiveCounter(final int typeId, final long archiveId, final String label) {
        final UnsafeBuffer key = new UnsafeBuffer(new byte[8]);
        key.putLong(0, archiveId);
        final UnsafeBuffer text = new UnsafeBuffer(label.getBytes());
        final Counter c = aeron.addCounter(typeId, key, 0, 8, text, 0, text.capacity());
        allocated.add(c);
        final CountersReader counters = aeron.countersReader();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (counters.getCounterState(c.id()) != CountersReader.RECORD_ALLOCATED) {
            assertTrue(System.nanoTime() < deadline);
            Thread.yield();
        }
        return c;
    }

    @Test
    public void theSamplerThreadReadsOnceASecondAndStops() throws Exception {
        final Counter max = clusterCounter(AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID, CLUSTER_ID, -1);
        final StallMetrics m = new StallMetrics("core", aeron.countersReader(), CLUSTER_ID, 0);
        m.start();
        m.start();
        try {
            final long deadline = System.nanoTime() + 10_000_000_000L;
            while (value(m.renderMetrics(), "core_consensus_module_max_cycle_time_seconds") == null) {
                assertTrue(System.nanoTime() < deadline);
                Thread.sleep(20);
            }
            max.setOrdered(250_000_000L);
            while (!"0.25".equals(value(m.renderMetrics(), "core_consensus_module_max_cycle_time_recent_seconds"))) {
                assertTrue(m.renderMetrics(), System.nanoTime() < deadline);
                Thread.sleep(20);
            }
        } finally {
            m.stop();
        }
        boolean alive = false;
        for (final Thread t : Thread.getAllStackTraces().keySet()) {
            alive |= t.getName().equals("stall-metrics-sampler") && t.isAlive();
        }
        assertFalse("the sampler thread ended before stop returned", alive);
    }
}
