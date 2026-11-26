// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** The reader file: the round trip, every invalid shape, the listing of a journal's directory, a crash in a write. */
public class JournalReadersFileTest {

    private File clusterDir;

    @Before
    public void setUp() throws Exception {
        clusterDir = Files.createTempDirectory("journal-readers-").toFile();
    }

    @After
    public void tearDown() {
        JournalReaders.skipDigestForTest = false;
        org.agrona.IoUtil.delete(clusterDir, true);
    }

    private static JournalReaders.Record record(final long applied) {
        return new JournalReaders.Record("core", 0, 17, "order", "oe-projector", applied, -1, "", 3,
            1_764_000_000_000L, "console");
    }

    private Path file() {
        return JournalReaders.file(clusterDir, "order", "oe-projector");
    }

    private JournalReaders.Read read() {
        return JournalReaders.read(clusterDir, "order", "oe-projector");
    }

    private static String invalid(final String text) {
        final JournalReaders.Read read = JournalReaders.parse(text.getBytes(StandardCharsets.US_ASCII));
        assertFalse("must be invalid: " + text, read.valid());
        assertFalse(read.absent());
        return read.invalid;
    }

    private static String body(final JournalReaders.Record r) {
        final String all = new String(r.encode(), StandardCharsets.US_ASCII);
        return all.substring(0, all.lastIndexOf("sha256="));
    }

    private static String signed(final String body) {
        final byte[] covered = body.getBytes(StandardCharsets.US_ASCII);
        return body + "sha256=" + JournalReaders.sha256Hex(covered, covered.length) + "\n";
    }

    @Test
    public void roundTripAndTheExactBytes() throws Exception {
        assertTrue("no file is an absent read", read().absent());
        JournalReaders.write(clusterDir, record(1200));
        final JournalReaders.Read read = read();
        assertTrue(String.valueOf(read.invalid), read.valid());
        assertEquals(1200, read.record.appliedJournalSeq);
        assertEquals("oe-projector", read.record.reader);
        assertEquals(1200, read.record.bound());
        assertFalse(read.record.released());
        final String text = new String(Files.readAllBytes(file()), StandardCharsets.US_ASCII);
        assertTrue(text, text.startsWith("format=journal-reader/1\nengine=core\ncluster_id=0\ngenesis_id=17\n"
            + "journal=order\nreader=oe-projector\napplied_journal_seq=1200\nreleased_up_to_journal_seq=-1\n"
            + "release_reason=\nseq=3\nwritten_at_epoch_ms=1764000000000\nsource=console\nsha256="));
        assertArrayEquals(record(1200).encode(), Files.readAllBytes(file()));
        assertEquals(file(), clusterDir.toPath().resolve("retention/journal-readers/order/oe-projector"));
        // a release above the applied value is the bound
        final JournalReaders.Record released = new JournalReaders.Record("core", 0, 17, "order", "oe-projector", 1200,
            5000, "reader retired", 4, 1, "console");
        JournalReaders.write(clusterDir, released);
        assertEquals(5000, read().record.bound());
        assertTrue(read().record.released());
    }

    @Test
    public void everyInvalidShapeIsInvalid() {
        final String good = body(record(7));
        assertTrue(JournalReaders.parse(signed(good).getBytes(StandardCharsets.US_ASCII)).valid());
        assertTrue(invalid("").contains("line end"));
        assertTrue(invalid(signed(good).replace("\n", "\r\n")).contains("CR"));
        assertTrue(invalid(signed(good) + "extra=1\n").contains("expected 13 lines"));
        assertTrue(invalid(signed(good.replace("reader=oe-projector\n", ""))).contains("expected 13 lines"));
        assertTrue(invalid(signed(good.replace("journal=order\nreader=oe-projector\n",
            "reader=oe-projector\njournal=order\n"))).contains("must be the key journal"));
        assertTrue(invalid(signed(good.replace("journal-reader/1", "journal-reader/2"))).contains("format must be"));
        assertTrue(invalid(signed(good.replace("applied_journal_seq=7", "applied_journal_seq=07")))
            .contains("not written canonically"));
        assertTrue(invalid(signed(good.replace("applied_journal_seq=7", "applied_journal_seq=x")))
            .contains("not a number"));
        assertTrue(invalid(signed(good.replace("applied_journal_seq=7", "applied_journal_seq=-1")))
            .contains("applied_journal_seq must be 0 or above"));
        assertTrue(invalid(signed(good.replace("genesis_id=17", "genesis_id=0"))).contains("genesis_id must not be 0"));
        assertTrue(invalid(signed(good.replace("engine=core", "engine=oms"))).contains("engine must be core or match"));
        assertTrue(invalid(signed(good.replace("cluster_id=0", "cluster_id=-1"))).contains("cluster_id"));
        assertTrue(invalid(signed(good.replace("cluster_id=0", "cluster_id=4294967296"))).contains("overflow"));
        assertTrue(invalid(signed(good.replace("reader=oe-projector", "reader=oe projector"))).contains("reader must be"));
        assertTrue(invalid(signed(good.replace("reader=oe-projector", "reader=../x"))).contains("reader must be"));
        assertTrue(invalid(signed(good.replace("reader=oe-projector", "reader=x.tmp"))).contains("reader must be"));
        assertTrue(invalid(signed(good.replace("journal=order", "journal="))).contains("journal must be"));
        assertTrue(invalid(signed(good.replace("released_up_to_journal_seq=-1", "released_up_to_journal_seq=-2")))
            .contains("released_up_to_journal_seq"));
        assertTrue(invalid(signed(good.replace("released_up_to_journal_seq=-1", "released_up_to_journal_seq=9")))
            .contains("release_reason must be given with a release"));
        assertTrue(invalid(signed(good.replace("release_reason=", "release_reason=why"))).contains("only with one"));
        assertTrue(invalid(signed(good.replace("seq=3", "seq=0"))).contains("seq must be 1 or above"));
        assertTrue(invalid(signed(good.replace("source=console", "source="))).contains("source must be"));
        assertTrue(invalid(signed(good.replace("source=console", "source=a b"))).contains("source must be"));
        assertTrue(invalid(new String(new byte[] {'f', (byte) 0x80, '\n'}, StandardCharsets.ISO_8859_1)) != null);
        final String big = signed(good) + "x".repeat(5000);
        assertNotNull(invalid(big));
    }

    /** T5's control: an edited sequence under an unchanged digest is refused by the digest and by nothing else. */
    @Test
    public void anEditedSequenceIsRefusedByTheDigest() {
        final String edited = new String(record(7).encode(), StandardCharsets.US_ASCII)
            .replace("applied_journal_seq=7", "applied_journal_seq=7000");
        final Runnable assertion = () -> {
            final JournalReaders.Read read = JournalReaders.parse(edited.getBytes(StandardCharsets.US_ASCII));
            if (read.valid()) {
                throw new AssertionError("an edited applied_journal_seq (" + read.record.appliedJournalSeq
                    + ") was accepted: the sha256 line did not guard it");
            }
            assertEquals("sha256 does not match the content", read.invalid);
        };
        assertion.run();
        JournalReaders.skipDigestForTest = true;
        try {
            JournalAudit.nc("reader file digest", "sha256", assertion);
        } finally {
            JournalReaders.skipDigestForTest = false;
        }
    }

    @Test
    public void theListingOfAJournalsDirectory() throws Exception {
        assertEquals("no directory is no reader", List.of(), JournalReaders.list(clusterDir, "order"));
        JournalReaders.write(clusterDir, record(10));
        JournalReaders.write(clusterDir, new JournalReaders.Record("core", 0, 17, "order", "audit", 4, -1, "", 1, 1,
            "console"));
        final Path dir = JournalReaders.directory(clusterDir, "order");
        Files.write(dir.resolve("oe-projector.tmp"), new byte[] {1, 2, 3}); // a write in flight: not a reader
        Files.write(dir.resolve("broken"), "format=journal-reader/1\n".getBytes(StandardCharsets.US_ASCII));
        Files.write(dir.resolve("has space"), new byte[0]);
        Files.createDirectory(dir.resolve("subdir"));
        final List<JournalReaders.Entry> entries = JournalReaders.list(clusterDir, "order");
        assertEquals(5, entries.size());
        assertEquals("audit", entries.get(0).name());
        assertTrue(entries.get(0).read().valid());
        assertEquals("broken", entries.get(1).name());
        assertFalse(entries.get(1).read().valid());
        assertEquals("has space", entries.get(2).name());
        assertEquals("the file name is not a reader name", entries.get(2).read().invalid);
        assertEquals("oe-projector", entries.get(3).name());
        assertEquals(10, entries.get(3).read().record.appliedJournalSeq);
        assertEquals("subdir", entries.get(4).name());
        assertEquals("not a regular file", entries.get(4).read().invalid);
        assertEquals("the other journal has its own directory", List.of(), JournalReaders.list(clusterDir, "money"));
    }

    @Test
    public void aCrashBeforeTheRenameLeavesThePreviousFile() throws Exception {
        JournalReaders.write(clusterDir, record(10));
        try {
            JournalReaders.write(clusterDir, record(99), () -> {
                throw new IllegalStateException("crash");
            });
        } catch (final IllegalStateException expected) {
            // the temp file is written and synced, the rename never ran
        }
        assertEquals(10, read().record.appliedJournalSeq);
        assertEquals("the temp file is not a reader", 1, JournalReaders.list(clusterDir, "order").size());
        JournalReaders.write(clusterDir, record(99));
        assertEquals(99, read().record.appliedJournalSeq);
    }

    @Test
    public void namesAreFileNamesAndLabelValues() {
        for (final String ok : new String[] {"money-projector", "oe-projector", "a", "A1._-", "x".repeat(64)}) {
            assertTrue(ok, JournalReaders.validName(ok));
        }
        for (final String bad : new String[] {"", " ", "a b", "a/b", "..", ".hidden", "-x", "x\"y", "x.tmp",
            "x".repeat(65), "é"}) {
            assertFalse(bad, JournalReaders.validName(bad));
        }
        assertFalse(JournalReaders.validName(null));
    }
}
