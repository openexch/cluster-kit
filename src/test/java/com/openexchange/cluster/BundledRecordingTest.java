// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The bundle's record of how to read its own files.
 *
 * <p>Segment files state none of this, and the catalog entry that does is deleted by the staging
 * sweep moments after the files are copied out. Get it wrong and the bundle still passes its
 * checksum, still uploads, and cannot be opened — which is the failure mode worth testing for.
 */
public class BundledRecordingTest {

    private static StagingArchive.Descriptor descriptor(long recordingId, long start, long stop) {
        return new StagingArchive.Descriptor(recordingId, start, stop,
                -1045384303, 67108864, 67108864, 1408, 1118418112, 106,
                "aeron:ipc?session-id=1118418112",
                "aeron:ipc?term-length=67108864|session-id=1118418112",
                "aeron:ipc");
    }

    /**
     * The log keeps recording into staging after the bundle is cut, so its live stopPosition runs
     * ahead of the bytes the bundle actually carries. A restore told the live number asks for data
     * that is not there.
     */
    @Test
    public void logStopPositionIsClampedToTheBundlePosition() {
        final BundleCapture.BundledRecording r = BundleCapture.BundledRecording.log(
                descriptor(2, 2147483648L, 3007693888L), 2197283840L);

        assertEquals("clamped to the position this bundle was cut at",
                2197283840L, r.descriptor().stopPosition());
        assertEquals("the lower bound is untouched", 2147483648L, r.descriptor().startPosition());
        assertEquals("log", r.role());
        assertEquals("the log has no serviceId", -1, r.serviceId());
    }

    /** A stopPosition already at or below the bundle position must be left alone, not raised. */
    @Test
    public void clampNeverRaisesAStopPosition() {
        final BundleCapture.BundledRecording r = BundleCapture.BundledRecording.log(
                descriptor(2, 0L, 500L), 2197283840L);
        assertEquals(500L, r.descriptor().stopPosition());
    }

    /** Snapshots are complete when captured, so they are carried verbatim, tagged by service. */
    @Test
    public void snapshotKeepsItsDescriptorAndCarriesItsServiceId() {
        final BundleCapture.BundledRecording r =
                BundleCapture.BundledRecording.snapshot(descriptor(469, 0L, 26848L), 0);

        assertEquals("snapshot", r.role());
        assertEquals(0, r.serviceId());
        assertEquals(26848L, r.descriptor().stopPosition());
        assertEquals(469L, r.descriptor().recordingId());
    }

    /**
     * The staging sweep deletes catalog entries, so a null descriptor means the describe happened on
     * the wrong side of it. Failing here costs one capture; carrying on writes a bundle that looks
     * complete and cannot be replayed.
     */
    @Test
    public void aMissingDescriptorRefusesTheBundle() {
        try {
            BundleCapture.BundledRecording.log(null, 100L);
            fail("expected a refusal when the staged log has no catalog entry");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("cannot be replayed"));
        }
        try {
            BundleCapture.BundledRecording.snapshot(null, 0);
            fail("expected a refusal when a staged snapshot has no catalog entry");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("cannot be replayed"));
        }
    }

    /**
     * Aeron channel URIs carry {@code | ? =} and are pasted straight into the manifest. Nothing in
     * that set needs escaping, but a quote or a backslash would silently produce invalid JSON, and
     * the manifest is the one file a restore must be able to parse.
     */
    @Test
    public void channelsAreEmittedAsValidJson() {
        final StagingArchive.Descriptor awkward = new StagingArchive.Descriptor(
                7, 0, 64, 1, 4096, 4096, 1408, 5, 100,
                "aeron:udp?endpoint=\"host\"|alias=a\\b", "aeron:udp?tags=76|alias=log", "aeron:ipc");

        final String json = BundleCapture.BundledRecording.snapshot(awkward, 1).toJson();

        assertTrue("quotes escaped", json.contains("endpoint=\\\"host\\\""));
        assertTrue("backslash escaped", json.contains("alias=a\\\\b"));
        assertTrue("the pipe in a channel URI is left alone", json.contains("tags=76|alias=log"));
        assertTrue("serviceId is present on a snapshot", json.contains("\"serviceId\": 1"));
        assertBalancedQuotes(json);
    }

    /** The log omits serviceId rather than emitting -1, which would read as a real service. */
    @Test
    public void theLogOmitsServiceId() {
        final String json = BundleCapture.BundledRecording
                .log(descriptor(2, 0, 64), 64).toJson();
        assertFalse(json.contains("serviceId"));
        assertTrue(json.contains("\"role\": \"log\""));
    }

    /** Every field a restore needs must be in the emitted object. */
    @Test
    public void everyFieldARestoreNeedsIsEmitted() {
        final String json = BundleCapture.BundledRecording
                .snapshot(descriptor(469, 0, 26848), 0).toJson();

        for (final String field : new String[] {
                "recordingId", "startPosition", "stopPosition", "initialTermId",
                "segmentFileLength", "termBufferLength", "mtuLength",
                "sessionId", "streamId", "strippedChannel", "originalChannel", "sourceIdentity"}) {
            assertTrue("missing " + field + " - a restore cannot serve the recording without it",
                    json.contains("\"" + field + "\""));
        }
    }

    private static void assertBalancedQuotes(final String json) {
        int unescaped = 0;
        for (int i = 0; i < json.length(); i++) {
            if (json.charAt(i) == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                unescaped++;
            }
        }
        assertEquals("unbalanced quotes means the manifest will not parse", 0, unescaped % 2);
    }
}
