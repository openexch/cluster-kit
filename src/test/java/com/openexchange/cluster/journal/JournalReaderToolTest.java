// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** The command's interface: named flags only, the four exit codes, lower values, release and remove. */
public class JournalReaderToolTest {

    private File clusterDir;
    private String dir;
    private ByteArrayOutputStream out;
    private ByteArrayOutputStream err;
    private long now = 1_764_000_000_000L;

    @Before
    public void setUp() throws Exception {
        clusterDir = Files.createTempDirectory("journal-reader-tool-").toFile();
        dir = clusterDir.getAbsolutePath();
    }

    @After
    public void tearDown() {
        org.agrona.IoUtil.delete(clusterDir, true);
    }

    private int run(final String... args) {
        out = new ByteArrayOutputStream();
        err = new ByteArrayOutputStream();
        return JournalReaderTool.run(args, new PrintStream(out), new PrintStream(err), () -> now);
    }

    private int set(final String journal, final String reader, final long applied, final String... more) {
        final String[] base = {"set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0", "--genesis-id",
            "17", "--journal", journal, "--reader", reader, "--applied-journal-seq", String.valueOf(applied),
            "--source", "console"};
        final String[] args = new String[base.length + more.length];
        System.arraycopy(base, 0, args, 0, base.length);
        System.arraycopy(more, 0, args, base.length, more.length);
        return run(args);
    }

    private int release(final long upTo, final String reason) {
        return run("release", "--cluster-dir", dir, "--genesis-id", "17", "--journal", "order", "--reader",
            "oe-projector", "--up-to-journal-seq", String.valueOf(upTo), "--reason", reason);
    }

    private JournalReaders.Record file(final String journal, final String reader) {
        final JournalReaders.Read read = JournalReaders.read(clusterDir, journal, reader);
        assertTrue(String.valueOf(read.invalid), read.valid());
        return read.record;
    }

    @Test
    public void registeringIsTheFirstSetAndZeroIsLegal() {
        assertEquals(0, set("order", "oe-projector", 0));
        assertEquals(0, set("money", "money-projector", 0));
        JournalReaders.Record r = file("order", "oe-projector");
        assertEquals(0, r.appliedJournalSeq);
        assertEquals(1, r.seq);
        assertEquals(now, r.writtenAtEpochMs);
        assertEquals("console", r.source);
        now += 5;
        assertEquals(0, set("order", "oe-projector", 40));
        r = file("order", "oe-projector");
        assertEquals(40, r.appliedJournalSeq);
        assertEquals(2, r.seq);
        assertEquals(now, r.writtenAtEpochMs);
        assertEquals("the same value again is a refresh", 0, set("order", "oe-projector", 40));
        assertEquals(3, file("order", "oe-projector").seq);
        assertEquals("the money reader is its own file", 0, file("money", "money-projector").appliedJournalSeq);
    }

    @Test
    public void usageErrorsExit64() {
        assertEquals(64, run());
        assertEquals(64, run("purge"));
        assertEquals("a positional value", 64, run("set", dir, "core", "0", "17", "order", "oe-projector", "5"));
        assertTrue(err.toString(), err.toString().contains("every value is a named flag"));
        assertEquals("an unknown flag", 64, set("order", "oe-projector", 5, "--force"));
        assertEquals("a repeated flag", 64, set("order", "oe-projector", 5, "--source", "x"));
        assertEquals("a missing flag", 64, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
            "--genesis-id", "17", "--journal", "order", "--reader", "oe-projector", "--source", "console"));
        assertTrue(err.toString(), err.toString().contains("missing flag --applied-journal-seq"));
        assertEquals("a flag without its value", 64, run("show", "--cluster-dir"));
        assertEquals("release has no --allow-lower", 64, run("release", "--cluster-dir", dir, "--genesis-id", "17",
            "--journal", "order", "--reader", "oe-projector", "--up-to-journal-seq", "5", "--reason", "x",
            "--allow-lower"));
        assertEquals("no file was written by a usage error", 0,
            new File(clusterDir, "retention").exists() ? 1 : 0);
    }

    /** The matching engine's one journal: its settlement journal, registered and read back like the core's. */
    @Test
    public void matchRegistersAReaderOfItsSettlementJournal() {
        assertEquals(java.util.Set.of("settlement"), JournalReaders.journalsOf(JournalReaders.ENGINE_MATCH));
        assertEquals(java.util.Set.of("money", "order"), JournalReaders.journalsOf(JournalReaders.ENGINE_CORE));
        assertEquals(java.util.Set.of(), JournalReaders.journalsOf("oms"));
        assertEquals(0, run("set", "--cluster-dir", dir, "--engine", "match", "--cluster-id", "0",
            "--genesis-id", "17", "--journal", "settlement", "--reader", "ct-bridge", "--applied-journal-seq",
            "12", "--source", "c"));
        final JournalReaders.Read read = JournalReaders.read(new java.io.File(dir), "settlement", "ct-bridge");
        assertTrue(String.valueOf(read.invalid), read.valid());
        assertEquals("match", read.record.engine);
        assertEquals("settlement", read.record.journal);
        assertEquals(12, read.record.appliedJournalSeq);
    }

    @Test
    public void refusedInputExits65() {
        assertEquals(65, set("order", "oe-projector", -1));
        assertTrue(err.toString(), err.toString().contains("applied_journal_seq must be 0 or above"));
        assertEquals(65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0", "--genesis-id",
            "17", "--journal", "order", "--reader", "oe-projector", "--applied-journal-seq", "12k", "--source", "c"));
        assertEquals("genesis 0", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
            "--genesis-id", "0", "--journal", "order", "--reader", "oe-projector", "--applied-journal-seq", "1",
            "--source", "c"));
        assertEquals("another engine", 65, run("set", "--cluster-dir", dir, "--engine", "oms", "--cluster-id", "0",
            "--genesis-id", "17", "--journal", "order", "--reader", "oe-projector", "--applied-journal-seq", "1",
            "--source", "c"));
        assertEquals("a journal the engine does not have", 65, set("orders", "oe-projector", 1));
        assertTrue(err.toString(), err.toString().contains("is not a journal of engine core"));
        assertEquals("the core's journal names are not match's", 65, run("set", "--cluster-dir", dir,
            "--engine", "match", "--cluster-id", "0", "--genesis-id", "17", "--journal", "order", "--reader", "r",
            "--applied-journal-seq", "1", "--source", "c"));
        assertTrue(err.toString(), err.toString().contains("'order' is not a journal of engine match (its journals:"
            + " [settlement])"));
        assertEquals("match's journal name is not the core's", 65, run("set", "--cluster-dir", dir, "--engine",
            "core", "--cluster-id", "0", "--genesis-id", "17", "--journal", "settlement", "--reader", "r",
            "--applied-journal-seq", "1", "--source", "c"));
        assertEquals("a reader name that is a path", 65, set("order", "../../x", 1));
        assertEquals("a reader name with a space", 65, set("order", "oe projector", 1));
        assertEquals("a cluster id out of range", 65, run("set", "--cluster-dir", dir, "--engine", "core",
            "--cluster-id", "-1", "--genesis-id", "17", "--journal", "order", "--reader", "r",
            "--applied-journal-seq", "1", "--source", "c"));
        assertEquals("a source with a space", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id",
            "0", "--genesis-id", "17", "--journal", "order", "--reader", "r", "--applied-journal-seq", "1", "--source",
            "a b"));
        assertFalse("nothing was written by a refused input", new File(clusterDir, "retention").exists());
        assertEquals("release needs a file", 65, release(10, "why"));
        assertEquals("a directory that does not exist is an I/O failure", 1, run("show", "--cluster-dir",
            dir + "/nope"));
    }

    @Test
    public void aLowerValueNeedsAllowLowerAndSaysSo() {
        assertEquals(0, set("order", "oe-projector", 100));
        assertEquals(65, set("order", "oe-projector", 99));
        assertTrue(err.toString(), err.toString().contains("a lower sequence needs --allow-lower"));
        assertEquals(100, file("order", "oe-projector").appliedJournalSeq);
        assertEquals(0, set("order", "oe-projector", 99, "--allow-lower"));
        assertTrue(err.toString(), err.toString().contains("CRITICAL: reader oe-projector of the order journal went"
            + " backwards: its applied journalSeq goes down from 100 to 99"));
        assertEquals(99, file("order", "oe-projector").appliedJournalSeq);
    }

    @Test
    public void anotherIdentityIsRefusedUntilTheFileIsRemoved() {
        assertEquals(0, set("order", "oe-projector", 100));
        assertEquals("another genesis", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
            "--genesis-id", "18", "--journal", "order", "--reader", "oe-projector", "--applied-journal-seq", "1",
            "--source", "c"));
        assertTrue(err.toString(), err.toString().contains("remove it first"));
        assertEquals("another cluster", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "1",
            "--genesis-id", "17", "--journal", "order", "--reader", "oe-projector", "--applied-journal-seq", "200",
            "--source", "c"));
        assertEquals("remove names the file's genesis", 65, run("remove", "--cluster-dir", dir, "--genesis-id", "18",
            "--journal", "order", "--reader", "oe-projector", "--reason", "new genesis"));
        assertEquals("remove needs a reason", 65, run("remove", "--cluster-dir", dir, "--genesis-id", "17",
            "--journal", "order", "--reader", "oe-projector", "--reason", ""));
        assertEquals(0, run("remove", "--cluster-dir", dir, "--genesis-id", "17", "--journal", "order", "--reader",
            "oe-projector", "--reason", "new genesis"));
        assertTrue(err.toString(), err.toString().contains("CRITICAL: reader oe-projector of the order journal is no"
            + " longer registered on this member"));
        assertTrue(JournalReaders.read(clusterDir, "order", "oe-projector").absent());
        assertEquals("removing what is not there is done", 0, run("remove", "--cluster-dir", dir, "--genesis-id", "17",
            "--journal", "order", "--reader", "oe-projector", "--reason", "again"));
        assertEquals(0, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0", "--genesis-id",
            "18", "--journal", "order", "--reader", "oe-projector", "--applied-journal-seq", "1", "--source", "c"));
        assertEquals(18, file("order", "oe-projector").genesisId);
        assertEquals("seq starts again", 1, file("order", "oe-projector").seq);
    }

    /** C4: a release is cleared by a set at or above it, and that works a second time in the same run. */
    @Test
    public void aReleaseIsClearedByASetAtOrAboveItTwice() {
        assertEquals(0, set("order", "oe-projector", 100));
        for (int round = 0; round < 2; round++) {
            final long applied = file("order", "oe-projector").appliedJournalSeq;
            final long upTo = applied + 500;
            assertEquals("round " + round + ": not above the applied value", 65, release(applied, "why"));
            assertEquals("round " + round + ": an empty reason", 65, release(upTo, ""));
            assertEquals("round " + round, 0, release(upTo, "reader is dead " + round));
            assertTrue(err.toString(), err.toString().contains("CRITICAL: hold of reader oe-projector of the order"
                + " journal released by operator up to journalSeq " + upTo + " (applied " + applied + ")"));
            JournalReaders.Record r = file("order", "oe-projector");
            assertEquals(upTo, r.releasedUpToJournalSeq);
            assertEquals("reader is dead " + round, r.releaseReason);
            assertEquals(upTo, r.bound());
            assertTrue(r.released());
            // a set below the release keeps it
            assertEquals(0, set("order", "oe-projector", upTo - 1));
            r = file("order", "oe-projector");
            assertEquals(upTo - 1, r.appliedJournalSeq);
            assertEquals("round " + round + ": the release stays until a set reaches it", upTo,
                r.releasedUpToJournalSeq);
            // a set at the release clears it
            assertEquals(0, set("order", "oe-projector", upTo));
            r = file("order", "oe-projector");
            assertEquals("round " + round + ": cleared", JournalReaders.NO_RELEASE, r.releasedUpToJournalSeq);
            assertEquals("", r.releaseReason);
            assertFalse(r.released());
            assertEquals(upTo, r.bound());
        }
    }

    @Test
    public void anInvalidFileIsReplacedBySetAndCanBeRemoved() throws Exception {
        assertEquals(0, set("order", "oe-projector", 100));
        final java.nio.file.Path file = JournalReaders.file(clusterDir, "order", "oe-projector");
        Files.write(file, "garbage\n".getBytes(StandardCharsets.US_ASCII));
        assertEquals("release needs a valid file", 65, release(500, "why"));
        assertEquals("show refuses an invalid file", 65, run("show", "--cluster-dir", dir));
        assertTrue(err.toString(), err.toString().contains("is invalid"));
        assertEquals("remove takes an invalid file away", 0, run("remove", "--cluster-dir", dir, "--genesis-id", "17",
            "--journal", "order", "--reader", "oe-projector", "--reason", "corrupt"));
        assertTrue(JournalReaders.read(clusterDir, "order", "oe-projector").absent());
        Files.write(file, "garbage\n".getBytes(StandardCharsets.US_ASCII));
        assertEquals(0, set("order", "oe-projector", 7));
        assertTrue(err.toString(), err.toString().contains("replacing an invalid file"));
        assertEquals(7, file("order", "oe-projector").appliedJournalSeq);
        assertEquals(1, file("order", "oe-projector").seq);
    }

    @Test
    public void showPrintsEveryReaderOfEveryJournal() {
        assertEquals(0, run("show", "--cluster-dir", dir));
        assertTrue(out.toString(), out.toString().contains("no reader is registered"));
        assertEquals(0, set("order", "oe-projector", 100));
        assertEquals(0, set("money", "money-projector", 7));
        assertEquals(0, run("show", "--cluster-dir", dir));
        final String shown = out.toString();
        assertTrue(shown, shown.contains("journal=money\nreader=money-projector\napplied_journal_seq=7\n"));
        assertTrue(shown, shown.contains("journal=order\nreader=oe-projector\napplied_journal_seq=100\n"));
        assertTrue(shown, shown.indexOf("journal=money") < shown.indexOf("journal=order"));
    }
}
