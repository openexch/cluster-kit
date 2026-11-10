// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.cluster.RecordingLog;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/**
 * The follower thread's view of the backup {@code RecordingLog}. The file is looked at at most
 * once per follower pass ({@link #beginPass}) and parsed again only when it changed (size or modification time); every
 * reader on the follower thread in that pass (L's id, the cluster identity check, the alignment member snapshot lookups) shares the one
 * parse. A parse that fails (the agent may be appending) keeps the last good one, and the next pass tries again.
 * Follower thread only. The returned log is closed (its entries stay in memory).
 */
final class RecordingLogView {

    private final File clusterDir;
    private final File file;
    private RecordingLog log;
    private long size = -1;
    private long modifiedNs = Long.MIN_VALUE;
    private boolean lookedThisPass;
    private long parses;
    private long generation;

    RecordingLogView(final File clusterDir) {
        this.clusterDir = clusterDir;
        this.file = new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME);
    }

    /** A new follower pass: the next {@link #get} looks at the file again. */
    void beginPass() {
        lookedThisPass = false;
    }

    /** The parsed RecordingLog (closed, entries in memory), null when the file does not exist or never parsed. */
    RecordingLog get() {
        if (lookedThisPass) {
            return log;
        }
        lookedThisPass = true;
        final BasicFileAttributes attrs;
        try {
            attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        } catch (final IOException missing) {
            if (log != null) {
                generation++;
            }
            log = null;
            size = -1;
            modifiedNs = Long.MIN_VALUE;
            return null;
        }
        final long s = attrs.size();
        final long m = attrs.lastModifiedTime().to(TimeUnit.NANOSECONDS);
        if (log != null && s == size && m == modifiedNs) {
            return log;
        }
        try (RecordingLog fresh = new RecordingLog(clusterDir, false)) {
            parses++;
            log = fresh;
            size = s;
            modifiedNs = m;
            generation++;
        } catch (final RuntimeException partial) {
            // the agent may be appending: keep the last good parse; the next pass looks again
        }
        return log;
    }

    /** How many times the file was parsed (a test probe). */
    long parses() {
        return parses;
    }

    /** Changes whenever {@link #get} returns a different parse (or the file vanished). */
    long generation() {
        return generation;
    }
}
