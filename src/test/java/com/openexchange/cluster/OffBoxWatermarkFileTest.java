// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The watermark file: what is written is read back, every malformed shape is invalid (never absent, never a
 * position), and a write that dies before its rename leaves the previous file as it was.
 */
public class OffBoxWatermarkFileTest {

    private File clusterDir;

    @Before
    public void setUp() throws Exception {
        clusterDir = Files.createTempDirectory("offbox-file-").toFile();
    }

    @After
    public void tearDown() {
        OffBoxWatermark.skipDigestForTest = false;
        org.agrona.IoUtil.delete(clusterDir, true);
    }

    static OffBoxWatermark.Record record(final long position) {
        return new OffBoxWatermark.Record("core", 0, 17, position, -1, "", 1, 1_764_000_000_000L, "archiver");
    }

    private Path file() {
        return OffBoxWatermark.file(clusterDir);
    }

    private static String text(final OffBoxWatermark.Record r) {
        return new String(r.encode(), StandardCharsets.US_ASCII);
    }

    /** {@code text} with its digest line recomputed, so only the edited rule can make it invalid. */
    private static byte[] sealed(final String body) {
        final byte[] covered = body.getBytes(StandardCharsets.US_ASCII);
        return (body + "sha256=" + OffBoxWatermark.sha256Hex(covered, covered.length) + "\n")
            .getBytes(StandardCharsets.US_ASCII);
    }

    private static String body(final OffBoxWatermark.Record r) {
        final String t = text(r);
        return t.substring(0, t.lastIndexOf("sha256="));
    }

    private static void assertInvalid(final String what, final byte[] bytes, final String named) {
        final OffBoxWatermark.Read read = OffBoxWatermark.parse(bytes);
        assertFalse(what + ": must not be valid", read.valid());
        assertFalse(what + ": must not be absent", read.absent());
        assertTrue(what + ": reason <" + read.invalid + "> names <" + named + ">", read.invalid.contains(named));
    }

    @Test
    public void roundTrip() throws Exception {
        final OffBoxWatermark.Record written = new OffBoxWatermark.Record("match", 3, 9_000_000_000L, 1_048_576,
            2_097_152, "archiver down since Monday, disk at 91%", 42, 1_764_000_000_123L, "bundle-uploader");
        OffBoxWatermark.write(clusterDir, written);
        final OffBoxWatermark.Read read = OffBoxWatermark.read(clusterDir);
        assertTrue(String.valueOf(read.invalid), read.valid());
        assertEquals(text(written), text(read.record));
        assertEquals(2_097_152, read.record.floor());
        assertEquals("the layout of the format, key by key",
            "format=offbox-watermark/1\nengine=match\ncluster_id=3\ngenesis_id=9000000000\n"
                + "log_position_bytes=1048576\nreleased_up_to_bytes=2097152\n"
                + "release_reason=archiver down since Monday, disk at 91%\nseq=42\n"
                + "written_at_epoch_ms=1764000000123\nsource=bundle-uploader\n", body(written));
        assertFalse("no temp file is left behind", Files.exists(file().resolveSibling("offbox-watermark.tmp")));
    }

    @Test
    public void absentIsAbsentAndNothingElse() {
        final OffBoxWatermark.Read read = OffBoxWatermark.read(clusterDir);
        assertTrue(read.absent());
        assertFalse(read.valid());
    }

    @Test
    public void everyMalformedShapeIsInvalid() {
        final OffBoxWatermark.Record r = record(4096);
        final String body = body(r);
        assertTrue(OffBoxWatermark.parse(sealed(body)).valid());

        assertInvalid("unknown key", sealed(body + "lineage_id=4\n"), "lines");
        assertInvalid("unknown key in place of a known one",
            sealed(body.replace("source=archiver", "origin=archiver")), "must be the key source");
        assertInvalid("missing key", sealed(body.replace("seq=1\n", "")), "lines");
        assertInvalid("wrong order", sealed(body.replace("engine=core\ncluster_id=0\n", "cluster_id=0\nengine=core\n")),
            "must be the key engine");
        assertInvalid("format of another version", sealed(body.replace("offbox-watermark/1", "offbox-watermark/2")),
            "format must be");
        assertInvalid("unknown engine", sealed(body.replace("engine=core", "engine=oms")), "engine must be");
        assertInvalid("negative position", sealed(body.replace("log_position_bytes=4096", "log_position_bytes=-1")),
            "log_position_bytes");
        assertInvalid("genesis 0", sealed(body.replace("genesis_id=17", "genesis_id=0")), "genesis_id");
        assertInvalid("seq 0", sealed(body.replace("seq=1", "seq=0")), "seq");
        assertInvalid("release below -1",
            sealed(body.replace("released_up_to_bytes=-1", "released_up_to_bytes=-2")), "released_up_to_bytes");
        assertInvalid("release without a reason",
            sealed(body.replace("released_up_to_bytes=-1", "released_up_to_bytes=8192")), "release_reason");
        assertInvalid("not a number", sealed(body.replace("log_position_bytes=4096", "log_position_bytes=4k")),
            "not a number");
        assertInvalid("beyond int64",
            sealed(body.replace("log_position_bytes=4096", "log_position_bytes=9223372036854775808")), "not a number");
        assertInvalid("a second spelling of a number",
            sealed(body.replace("log_position_bytes=4096", "log_position_bytes=+4096")), "canonically");
        assertInvalid("cluster id beyond int32", sealed(body.replace("cluster_id=0", "cluster_id=4294967296")),
            "integer overflow");
        assertInvalid("source with a space", sealed(body.replace("source=archiver", "source=the archiver")), "source");
        assertInvalid("empty source", sealed(body.replace("source=archiver", "source=")), "source");
        assertInvalid("CRLF", sealed(body.replace("\n", "\r\n")), "CR");
        assertInvalid("no final line end", text(r).substring(0, text(r).length() - 1).getBytes(StandardCharsets.US_ASCII),
            "line end");
        assertInvalid("empty file", new byte[0], "line end");
        assertInvalid("half a file", text(r).substring(0, 60).getBytes(StandardCharsets.US_ASCII), "line");
        assertInvalid("digest of other content",
            text(r).replace("log_position_bytes=4096", "log_position_bytes=4097").getBytes(StandardCharsets.US_ASCII),
            "sha256");
    }

    /** The digest is what stops an edited position; a reader that skips it accepts the edit. */
    @Test
    public void anEditedPositionIsCaughtByTheDigest() {
        final byte[] edited = text(record(4096)).replace("log_position_bytes=4096", "log_position_bytes=999999999")
            .getBytes(StandardCharsets.US_ASCII);
        assertEditedPositionRefused(edited);

        OffBoxWatermark.skipDigestForTest = true;
        try {
            assertEditedPositionRefused(edited);
            fail("negative control did not fail: the assertion cannot see the digest rule");
        } catch (final AssertionError expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("sha256"));
        } finally {
            OffBoxWatermark.skipDigestForTest = false;
        }
    }

    private static void assertEditedPositionRefused(final byte[] edited) {
        final OffBoxWatermark.Read read = OffBoxWatermark.parse(edited);
        if (read.valid()) {
            throw new AssertionError("a file whose position was edited to " + read.record.logPositionBytes
                + " was accepted: the sha256 line was not checked");
        }
    }

    @Test
    public void aCrashBetweenTempAndRenameLeavesTheOldFileValid() throws Exception {
        OffBoxWatermark.write(clusterDir, record(4096));
        final byte[] before = Files.readAllBytes(file());
        try {
            OffBoxWatermark.write(clusterDir, record(8192), () -> {
                throw new IllegalStateException("crash before the rename");
            });
            fail("the write must not complete");
        } catch (final IllegalStateException crashed) {
            assertEquals("crash before the rename", crashed.getMessage());
        }
        assertEquals(new String(before, StandardCharsets.US_ASCII),
            new String(Files.readAllBytes(file()), StandardCharsets.US_ASCII));
        final OffBoxWatermark.Read read = OffBoxWatermark.read(clusterDir);
        assertTrue(read.valid());
        assertEquals("the old position stands", 4096, read.record.logPositionBytes);
        // the leftover temp file decides nothing, and the next write replaces it
        OffBoxWatermark.write(clusterDir, record(8192));
        assertEquals(8192, OffBoxWatermark.read(clusterDir).record.logPositionBytes);
    }

    @Test
    public void aRecordThatBreaksTheFormatIsNotWritten() throws Exception {
        try {
            OffBoxWatermark.write(clusterDir, new OffBoxWatermark.Record("core", 0, 0, 1, -1, "", 1, 1, "a"));
            fail("genesis 0 must not be written");
        } catch (final IllegalArgumentException refused) {
            assertNotNull(refused.getMessage());
        }
        assertTrue(OffBoxWatermark.read(clusterDir).absent());
    }

    @Test
    public void aFileThatCannotBeReadIsInvalidNotAbsent() throws Exception {
        Files.createDirectories(file()); // a directory where the file should be
        final OffBoxWatermark.Read read = OffBoxWatermark.read(clusterDir);
        assertFalse(read.absent());
        assertFalse(read.valid());
    }
}
