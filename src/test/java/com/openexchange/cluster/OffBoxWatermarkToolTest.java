// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** The command's interface: named flags only, and the four exit codes. */
public class OffBoxWatermarkToolTest {

    private File clusterDir;
    private String dir;
    private ByteArrayOutputStream out;
    private ByteArrayOutputStream err;
    private long now = 1_764_000_000_000L;

    @Before
    public void setUp() throws Exception {
        clusterDir = Files.createTempDirectory("offbox-tool-").toFile();
        dir = clusterDir.getAbsolutePath();
    }

    @After
    public void tearDown() {
        org.agrona.IoUtil.delete(clusterDir, true);
    }

    private int run(final String... args) {
        out = new ByteArrayOutputStream();
        err = new ByteArrayOutputStream();
        return OffBoxWatermarkTool.run(args, new PrintStream(out), new PrintStream(err), () -> now);
    }

    private int set(final long position, final String... more) {
        final String[] base = {"set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0", "--genesis-id",
            "17", "--log-position-bytes", String.valueOf(position), "--source", "archiver"};
        final String[] args = new String[base.length + more.length];
        System.arraycopy(base, 0, args, 0, base.length);
        System.arraycopy(more, 0, args, base.length, more.length);
        return run(args);
    }

    private OffBoxWatermark.Record file() {
        final OffBoxWatermark.Read read = OffBoxWatermark.read(clusterDir);
        assertTrue(String.valueOf(read.invalid), read.valid());
        return read.record;
    }

    private byte[] bytes() throws Exception {
        return Files.readAllBytes(OffBoxWatermark.file(clusterDir));
    }

    @Test
    public void setWritesAndCountsUp() {
        assertEquals(0, set(4096));
        assertEquals(4096, file().logPositionBytes);
        assertEquals(1, file().seq);
        assertEquals(now, file().writtenAtEpochMs);
        now += 5;
        assertEquals("the same value again is a refresh", 0, set(4096));
        assertEquals(2, file().seq);
        assertEquals(now, file().writtenAtEpochMs);
        assertEquals(0, set(8192));
        assertEquals(3, file().seq);
        assertEquals(8192, file().logPositionBytes);
    }

    @Test
    public void usageErrorsExit64AndWriteNothing() {
        assertEquals(64, run());
        assertEquals(64, run("purge", "--cluster-dir", dir));
        assertEquals("a positional argument", 64, run("set", dir, "core", "0", "17", "4096", "archiver"));
        assertTrue(err.toString(), err.toString().contains("positional argument"));
        assertTrue(err.toString(), err.toString().contains("Usage: OffBoxWatermarkTool"));
        assertEquals("a positional argument after the flags", 64, set(4096, "4097"));
        assertEquals("an unknown flag", 64, set(4096, "--force"));
        assertEquals("a repeated flag", 64, set(4096, "--log-position-bytes", "1"));
        assertTrue(err.toString(), err.toString().contains("given twice"));
        assertEquals("a missing flag", 64, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
            "--genesis-id", "17", "--source", "archiver"));
        assertTrue(err.toString(), err.toString().contains("missing flag --log-position-bytes"));
        assertEquals("a flag with no value", 64, run("show", "--cluster-dir"));
        assertEquals("a flag of another command", 64, run("show", "--cluster-dir", dir, "--genesis-id", "17"));
        assertEquals("clear needs the genesis", 64, run("clear", "--cluster-dir", dir));
        assertEquals("release needs a reason", 64, run("release", "--cluster-dir", dir, "--genesis-id", "17",
            "--up-to-log-position-bytes", "9"));
        assertTrue("nothing was written", OffBoxWatermark.read(clusterDir).absent());
    }

    @Test
    public void refusedInputExits65AndLeavesTheFileUntouched() throws Exception {
        assertEquals("not a number", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
            "--genesis-id", "17", "--log-position-bytes", "4k", "--source", "archiver"));
        assertEquals("a negative position", 65, set(-5));
        assertEquals("genesis 0", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id", "0",
            "--genesis-id", "0", "--log-position-bytes", "1", "--source", "archiver"));
        assertEquals("an unknown engine", 65, run("set", "--cluster-dir", dir, "--engine", "oms", "--cluster-id",
            "0", "--genesis-id", "17", "--log-position-bytes", "1", "--source", "archiver"));
        assertEquals("a source with a space", 65, run("set", "--cluster-dir", dir, "--engine", "core",
            "--cluster-id", "0", "--genesis-id", "17", "--log-position-bytes", "1", "--source", "the archiver"));
        assertEquals("release with no file", 65, run("release", "--cluster-dir", dir, "--genesis-id", "17",
            "--up-to-log-position-bytes", "9", "--reason", "x"));
        assertTrue(OffBoxWatermark.read(clusterDir).absent());

        assertEquals(0, set(4096));
        final byte[] before = bytes();

        assertEquals("a lower position without the flag", 65, set(4095));
        assertTrue(err.toString(), err.toString().contains("needs --allow-lower"));
        assertEquals("another genesis", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id",
            "0", "--genesis-id", "18", "--log-position-bytes", "8192", "--source", "archiver"));
        assertTrue(err.toString(), err.toString().contains("clear it first"));
        assertEquals("another engine", 65, run("set", "--cluster-dir", dir, "--engine", "match", "--cluster-id",
            "0", "--genesis-id", "17", "--log-position-bytes", "8192", "--source", "archiver"));
        assertEquals("another cluster id", 65, run("set", "--cluster-dir", dir, "--engine", "core", "--cluster-id",
            "1", "--genesis-id", "17", "--log-position-bytes", "8192", "--source", "archiver"));
        assertEquals("release with the wrong genesis", 65, run("release", "--cluster-dir", dir, "--genesis-id",
            "18", "--up-to-log-position-bytes", "9999", "--reason", "x"));
        assertTrue(err.toString(), err.toString().contains("genesis mismatch: asked 18, the file has 17"));
        assertEquals("release not above the durable position", 65, run("release", "--cluster-dir", dir,
            "--genesis-id", "17", "--up-to-log-position-bytes", "4096", "--reason", "x"));
        assertEquals("release with a multi-line reason", 65, run("release", "--cluster-dir", dir, "--genesis-id",
            "17", "--up-to-log-position-bytes", "9999", "--reason", "a\nb"));
        assertEquals("clear with the wrong genesis", 65, run("clear", "--cluster-dir", dir, "--genesis-id", "18"));
        assertArrayEquals("every refusal left the file untouched", before, bytes());
    }

    @Test
    public void aLowerPositionNeedsTheFlagAndSaysSo() {
        assertEquals(0, set(8192));
        assertEquals(0, set(4096, "--allow-lower"));
        assertTrue(err.toString(), err.toString().contains("CRITICAL: the off-box copy lost data"));
        assertEquals(4096, file().logPositionBytes);
        assertEquals(2, file().seq);
    }

    @Test
    public void releaseShowAndClear() throws Exception {
        assertEquals(0, set(4096));
        assertEquals(0, run("release", "--cluster-dir", dir, "--genesis-id", "17", "--up-to-log-position-bytes",
            "65536", "--reason", "archiver dead, disk at 93%"));
        assertTrue(err.toString(), err.toString().contains("off-box hold released by operator up to 65536"
            + " (durable 4096)"));
        assertEquals(65536, file().releasedUpToBytes);
        assertEquals(65536, file().floor());
        assertEquals("archiver dead, disk at 93%", file().releaseReason);

        assertEquals(0, run("show", "--cluster-dir", dir));
        assertEquals(new String(bytes(), StandardCharsets.US_ASCII), out.toString());

        assertEquals(0, run("clear", "--cluster-dir", dir, "--genesis-id", "17"));
        assertTrue(OffBoxWatermark.read(clusterDir).absent());
        assertEquals("show with no file", 0, run("show", "--cluster-dir", dir));
        assertTrue(out.toString(), out.toString().contains("no watermark file"));
        assertEquals("clear with no file is done already", 0, run("clear", "--cluster-dir", dir, "--genesis-id",
            "17"));
    }

    @Test
    public void anInvalidFileIsShownAsInvalidAndReplacedBySet() throws Exception {
        Files.createDirectories(OffBoxWatermark.file(clusterDir).getParent());
        Files.writeString(OffBoxWatermark.file(clusterDir), "format=offbox-watermark/1\n");
        assertEquals(65, run("show", "--cluster-dir", dir));
        assertEquals(65, run("clear", "--cluster-dir", dir, "--genesis-id", "17"));
        assertEquals(65, run("release", "--cluster-dir", dir, "--genesis-id", "17", "--up-to-log-position-bytes",
            "9", "--reason", "x"));
        assertEquals("the next set replaces it", 0, set(4096));
        assertEquals(1, file().seq);
    }

    @Test
    public void ioFailureExits1() throws Exception {
        final File notADirectory = new File(clusterDir, "plain-file");
        Files.writeString(notADirectory.toPath(), "x");
        assertEquals(1, run("set", "--cluster-dir", notADirectory.getAbsolutePath(), "--engine", "core",
            "--cluster-id", "0", "--genesis-id", "17", "--log-position-bytes", "1", "--source", "archiver"));
        assertTrue(err.toString(), err.toString().contains("FAILED"));
        assertEquals(1, run("show", "--cluster-dir", new File(clusterDir, "missing").getAbsolutePath()));
        assertFalse(new File(clusterDir, "missing").exists());
    }
}
