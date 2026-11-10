// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.cluster.RecordingLog;
import org.agrona.IoUtil;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * The follower's RecordingLog view parses the file at most once per pass and again only when
 * the file changed; and the retrieval-progress {@code rec-pos} label parse picks the stream id.
 */
public class RecordingLogViewTest {

    @Test
    public void theRecordingLogIsParsedOncePerPassAndOnlyWhenItChanged() throws Exception {
        final File dir = Files.createTempDirectory("replica-recording-log-view").toFile();
        try {
            final RecordingLogView view = new RecordingLogView(dir);
            view.beginPass();
            assertNull("no file yet", view.get());
            try (RecordingLog log = new RecordingLog(dir, true)) {
                log.appendTerm(7, 0, 0, 1);
            }
            view.beginPass();
            assertEquals(7, view.get().findLastTermRecordingId());
            view.get();
            view.get();
            for (int pass = 0; pass < 5; pass++) {
                view.beginPass();
                view.get();
            }
            if (view.parses() != 1) {
                throw new AssertionError("an unchanged RecordingLog was parsed " + view.parses() + " times");
            }
            final long g = view.generation();
            Thread.sleep(5); // a distinct modification time on coarse clocks
            try (RecordingLog log = new RecordingLog(dir, false)) {
                log.appendTerm(7, 1, 4096, 2);
            }
            view.get();
            assertEquals("not re-read inside the same pass", 1, view.parses());
            view.beginPass();
            assertEquals(1, view.get().findLastTerm().leadershipTermId);
            assertEquals("re-read once after the change", 2, view.parses());
            assertEquals(g + 1, view.generation());
        } finally {
            IoUtil.delete(dir, true);
        }
    }

    @Test
    public void theRecPosLabelNamesTheStream() {
        assertEquals(106, ClusterIdentityCheck.recPosStreamId(
                "rec-pos: 12 -55 106 aeron:udp?endpoint=localhost:1 - archiveId=3"));
        assertEquals(100, ClusterIdentityCheck.recPosStreamId("rec-pos: 1 2 100 aeron:ipc"));
        assertEquals(-1, ClusterIdentityCheck.recPosStreamId("rcv-pos: 1 2 106 aeron:ipc"));
        assertEquals(-1, ClusterIdentityCheck.recPosStreamId("rec-pos: 1 2"));
        assertEquals(-1, ClusterIdentityCheck.recPosStreamId(null));
    }
}
