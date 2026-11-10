// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.openexchange.cluster.shadow.ShadowRig.ENGINE;
import static com.openexchange.cluster.shadow.ShadowRig.ID;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.cursor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The shm budget (incident 2026-08-11), measured rather than assumed. Four new shadows at
 * {@code T = 4m}: a scan opens exactly 8 window replays and a startup pass 4 full reads; the driver's publication log
 * files are sampled at every replay open and every 100 ms afterwards. What holds on Aeron 1.53 (and what this test
 * asserts): every replay log is sparse (allocated bytes, the tmpfs cost, are the touched pages, not {@code 3 x T});
 * a window replay touches at most a few pages; a full read touches at most its recording; every replay log is gone
 * within 10 s of the scan. The (char) control repeats the scan on the bare {@code aeron:ipc} channel: the replay-log
 * lifetime and allocation are the same, so {@code linger=0} is NOT what ends an IPC replay log, and the contract's
 * "at most one replay log at a time, none 100 ms after the scan" is false (README deviation; the real per-poll cost
 * is the allocated column).
 */
public class ShadowShmBudgetTest {

    static final String T4M = "4m";
    static final long T = 4L * 1024 * 1024;

    record Sample(int replayLogs, long apparent, long allocated) {
    }

    /** Allocated bytes of a file through {@code stat} (blocks x block size), or -1 when unavailable. */
    static long allocated(final Path p) {
        try {
            final Process stat = new ProcessBuilder("stat", "-c", "%b %B", p.toString()).redirectErrorStream(true)
                    .start();
            final String out = new String(stat.getInputStream().readAllBytes(), StandardCharsets.US_ASCII).trim();
            stat.waitFor();
            final String[] f = out.split(" ");
            return Long.parseLong(f[0]) * Long.parseLong(f[1]);
        } catch (final IOException | InterruptedException | RuntimeException e) {
            return -1;
        }
    }

    static Sample sample(final ShadowRig rig, final Set<Path> baseline) {
        final List<Path> now = new ArrayList<>(rig.fixture.publicationLogFiles());
        now.removeAll(baseline);
        long apparent = 0;
        long alloc = 0;
        for (final Path p : now) {
            try {
                apparent += Files.size(p);
                alloc += allocated(p);
            } catch (final IOException e) {
                // deleted between the listing and the stat: no longer held
            }
        }
        return new Sample(now.size(), apparent, alloc);
    }

    record Run(String channel, List<Sample> atOpen, Sample end, long goneAfterMs, long deepAllocatedMax) {
    }

    static Run run(final ShadowRig rig, final String replayChannel) throws Exception {
        final Set<Path> baseline = new HashSet<>(rig.fixture.publicationLogFiles());
        final List<Sample> atOpen = new ArrayList<>();
        final ShadowCatalog catalog = rig.catalog((id, from, len) -> atOpen.add(sample(rig, baseline)),
                replayChannel);
        final ShadowCatalog.Scan s = catalog.scan();
        assertEquals(4, s.usable().size());
        final Sample end = sample(rig, baseline);
        final long t0 = System.nanoTime();
        long gone = -1;
        while (System.nanoTime() - t0 < 10_000_000_000L) {
            if (sample(rig, baseline).replayLogs() == 0) {
                gone = (System.nanoTime() - t0) / 1_000_000L;
                break;
            }
            Thread.sleep(100);
        }
        // the startup pass: 4 full reads; the allocation of each read's log is visible at the next open and at the end
        final int before = atOpen.size();
        catalog.verifyAllComplete();
        long deepMax = sample(rig, baseline).allocated();
        for (int i = before; i < atOpen.size(); i++) {
            deepMax = Math.max(deepMax, atOpen.get(i).allocated());
        }
        return new Run(replayChannel, atOpen.subList(0, before), end, gone, deepMax);
    }

    static void write4(final ShadowRig rig) {
        final ShadowWriter w = new ShadowWriter(rig.fixture.aeron(), rig.archive(), R3,
                new ShadowRig.HashChainSelfCheck(), T4M, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, ShadowRig.TIMEOUT_NS,
                ShadowRig.TIMEOUT_NS, 1, -1, rig.counters);
        for (int k = 1; k <= 4; k++) {
            final ShadowWriter.WriteResult r = w.write(ShadowRig.image(600 * 1024 + k, k), ENGINE, ShadowKind.CADENCE,
                    cursor(100L * k), ID);
            assertTrue(r.toString(), r.ok());
        }
    }

    @Test
    public void replayLogsAreSparseBoundedAndGoneWithinTenSeconds() throws Exception {
        final Run green;
        try (ShadowRig rig = new ShadowRig()) {
            write4(rig);
            green = run(rig, ShadowAlias.REPLAY_CHANNEL);
        }
        final Run control;
        try (ShadowRig rig = new ShadowRig()) {
            write4(rig);
            control = run(rig, "aeron:ipc");
        }
        System.out.println("BUDGET " + green);
        System.out.println("BUDGET (char) " + control);
        assertEquals("8 window replays (2 per new shadow), serial", 8, green.atOpen().size());
        for (int i = 0; i < green.atOpen().size(); i++) {
            final Sample s = green.atOpen().get(i);
            assertTrue("one more replay log per replay opened: " + s, s.replayLogs() <= i + 1);
            assertEquals("each replay log maps 3 x T plus metadata", (i + 1) * (3 * T + 4096), s.apparent());
            assertTrue("sparse: window replays touch at most 64 KiB each: " + s, s.allocated() >= 0
                    && s.allocated() <= (i + 1) * 64L * 1024);
        }
        assertTrue("a full read touches at most its 600 KiB recording per log: " + green.deepAllocatedMax(),
                green.deepAllocatedMax() <= 4 * (700L * 1024));
        assertTrue("every replay log is gone within 10 s: " + green.goneAfterMs(), green.goneAfterMs() >= 0);
        // (char): the bare channel behaves the same, so linger=0 is not what frees an IPC replay log
        assertEquals(green.atOpen().size(), control.atOpen().size());
        assertTrue("(char) bare channel also sparse and also gone within 10 s: " + control,
                control.goneAfterMs() >= 0 && control.end().allocated() <= 8 * 64L * 1024);
        ShadowRig.green("budget (4 shadows, T=4m): per poll " + green.end().replayLogs() + " replay logs, apparent "
                + green.end().apparent() + " B, allocated " + green.end().allocated() + " B, gone after "
                + green.goneAfterMs() + " ms; startup pass max allocated " + green.deepAllocatedMax() + " B; (char) "
                + "bare aeron:ipc: " + control.end().replayLogs() + " logs, allocated " + control.end().allocated()
                + " B, gone after " + control.goneAfterMs() + " ms");
    }
}
