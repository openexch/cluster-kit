// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.testkit.NeverSchedulesScan;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * One purge owner for journal recordings. In the kit's {@code journal} package only {@link JournalRetention} (its
 * archive port) calls an archive delete, and a pass only ever names recording ids it listed on the profile's journal
 * stream: over many archives of many shapes, every id a purge call named was returned by a listing of that stream,
 * and no recording of another stream in the same archive changed.
 */
public class JournalPurgeCallSiteTest {

    private static final String PKG = "com/openexchange/cluster/journal/";
    private static final Set<String> DELETES = Set.of("purgeSegments", "purgeRecording", "truncateRecording",
        "detachSegments", "deleteDetachedSegments", "migrateSegments");

    @After
    public void tearDown() {
        JournalRetention.Seams.reset();
    }

    @Test
    public void onlyTheRetentionCallsAnArchiveDeleteInTheJournalPackage() throws URISyntaxException {
        final Path main = Path.of(JournalRetention.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanDirectory(main, n -> n.startsWith(PKG),
            new NeverSchedulesScan.Rules(Set.of(), List.of(), Set.of(),
                Map.of("io/aeron/archive/client/AeronArchive", DELETES), Map.of(), Map.of()));
        assertTrue("scanned " + report.classes(), report.classes().contains(PKG + "JournalRetentionAgent")
            && report.classes().contains(PKG + "JournalReaderTool") && report.classes().contains(PKG + "JournalReaders"));
        final Set<String> callers = new TreeSet<>();
        for (final NeverSchedulesScan.Finding f : report.findings()) {
            callers.add(f.className() + " -> " + f.reference());
        }
        assertEquals(Set.of(PKG + "JournalRetention$AeronPort -> io/aeron/archive/client/AeronArchive.purgeSegments",
            PKG + "JournalRetention$AeronPort -> io/aeron/archive/client/AeronArchive.purgeRecording"), callers);
    }

    @Test
    public void aPassOnlyNamesRecordingIdsItListedOnTheJournalsStream() throws Exception {
        final File clusterDir = Files.createTempDirectory("journal-ids-").toFile();
        try {
            final Random random = new Random(20261125);
            int purgeCalls = 0;
            for (int round = 0; round < 300; round++) {
                final FakeJournalArchive archive = new FakeJournalArchive();
                long id = 0;
                long head = 0;
                final int runs = 1 + random.nextInt(5);
                for (int run = 0; run < runs; run++) {
                    // recordings of other streams (the log, snapshots, shadows) sit between the journal's
                    for (int other = random.nextInt(3); other > 0; other--) {
                        archive.addOtherStream(id++, 1 + random.nextInt(200));
                    }
                    final long from = Math.max(1, head - random.nextInt(100));
                    head = Math.max(head, from) + random.nextInt(400);
                    final long epoch = random.nextInt(6) == 0 ? 99 : JournalRig.GENESIS;
                    archive.add(id++, run == runs - 1 && random.nextBoolean(), epoch, from, head);
                }
                final Set<Long> otherStream = new TreeSet<>();
                for (long i = 0; i < id; i++) {
                    otherStream.add(i);
                }
                final long cursor = random.nextInt((int) head + 50);
                final ByteArrayOutputStream toolErr = new ByteArrayOutputStream();
                org.agrona.IoUtil.delete(new File(clusterDir, "retention"), true);
                assertEquals(toolErr.toString(), 0, JournalReaderTool.run(new String[] {"set", "--cluster-dir",
                    clusterDir.getAbsolutePath(), "--engine", "core", "--cluster-id", "0", "--genesis-id",
                    String.valueOf(JournalRig.GENESIS), "--journal", "order", "--reader", "r",
                    "--applied-journal-seq", String.valueOf(cursor), "--source", "t"},
                    new PrintStream(new ByteArrayOutputStream()), new PrintStream(toolErr), () -> 1L));
                final JournalAudit.Snapshot before = archive.snapshot();
                new JournalRetention(JournalRig.ORDER, clusterDir, "core", 0, JournalRetention.Mode.READERS, Set.of(),
                    1L << 40, 3_600_000, SystemNanoClock.INSTANCE, l -> { }, l -> { })
                    .pass(archive, JournalRig.GENESIS, JournalRetention.OffBox.NONE, 1L,
                        System.nanoTime() + 10_000_000_000L);
                for (final long purged : archive.purgedIds) {
                    if (!archive.listed.contains(purged)) {
                        throw new AssertionError("round " + round + ": a purge named recording " + purged
                            + ", which no listing of the journal's stream returned (listed " + archive.listed + ")");
                    }
                }
                otherStream.removeAll(archive.listed);
                for (final long other : otherStream) {
                    assertTrue("round " + round + ": recording " + other + " of another stream is gone",
                        archive.exists(other));
                    assertEquals("round " + round + ": recording " + other + " of another stream moved", 0,
                        archive.start(other));
                }
                // and what it did purge broke no rule
                JournalAudit.check(before, archive.snapshot(), new JournalAudit.Truth().cursor("r", cursor));
                JournalAudit.checkKept(before, archive.snapshot(), new JournalAudit.Truth().cursor("r", cursor),
                    cursor);
                purgeCalls += archive.purges.size();
            }
            assertTrue("the rounds purged: " + purgeCalls, purgeCalls > 100);
            System.out.println("GREEN 300 archives, " + purgeCalls + " purge calls, every id from a listing of the"
                + " journal's stream");
        } finally {
            org.agrona.IoUtil.delete(clusterDir, true);
        }
    }
}
