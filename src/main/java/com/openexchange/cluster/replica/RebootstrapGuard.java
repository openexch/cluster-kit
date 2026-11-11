// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * The re-bootstrap loop guard and the moved-aside dir names. {@code BASE_DIR/rebootstrap.count} holds one epoch
 * millisecond per re-bootstrap this node decided; entries older than {@code SHADOW_REBOOTSTRAP_WINDOW_MS} age out. It is
 * node-local restart policy (when this process may restart itself), never replicated state. The moved-aside dirs are
 * {@code BASE_DIR/cluster.rebootstrap-<utc>}; {@link #pruneMoved} keeps the newest {@code SHADOW_REBOOTSTRAP_KEEP} of
 * them and never touches {@code cluster/}.
 */
final class RebootstrapGuard {

    static final String FILE = "rebootstrap.count";
    static final String MOVED_PREFIX = "cluster.rebootstrap-";
    private static final DateTimeFormatter UTC = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'")
            .withZone(ZoneOffset.UTC);

    private RebootstrapGuard() {
    }

    /** The re-bootstraps recorded inside the window ending at {@code nowMs}. */
    static List<Long> recent(final File baseDir, final long nowMs, final long windowMs) {
        final Path f = new File(baseDir, FILE).toPath();
        final List<Long> out = new ArrayList<>();
        if (!Files.exists(f)) {
            return out;
        }
        final List<String> lines;
        try {
            lines = Files.readAllLines(f, StandardCharsets.US_ASCII);
        } catch (final IOException e) {
            throw new UncheckedIOException("re-bootstrap marker " + f + " is unreadable", e);
        }
        for (final String line : lines) {
            final String t = line.trim();
            if (t.isEmpty()) {
                continue;
            }
            final long at;
            try {
                at = Long.parseLong(t);
            } catch (final NumberFormatException e) {
                throw new IllegalStateException("re-bootstrap marker " + f + " holds '" + t + "' (expected epoch ms"
                        + " per line); remove the file to reset the loop guard");
            }
            if (nowMs - at < windowMs) {
                out.add(at);
            }
        }
        return out;
    }

    /**
     * Null when one more re-bootstrap is allowed, else the loop refusal text: the window already holds
     * {@code maxConsecutive} of them.
     */
    static String check(final File baseDir, final long nowMs, final int maxConsecutive, final long windowMs) {
        final int n = recent(baseDir, nowMs, windowMs).size();
        if (n < maxConsecutive) {
            return null;
        }
        return "re-bootstrap loop: " + (n + 1) + " in " + windowMs + " ms; members prune faster than this replica"
                + " catches up";
    }

    /** Records one re-bootstrap at {@code nowMs}, dropping the entries outside the window (write, then move). */
    static void record(final File baseDir, final long nowMs, final long windowMs) {
        final List<Long> kept = recent(baseDir, nowMs, windowMs);
        kept.add(nowMs);
        final StringBuilder b = new StringBuilder();
        for (final long at : kept) {
            b.append(at).append('\n');
        }
        final Path f = new File(baseDir, FILE).toPath();
        final Path tmp = new File(baseDir, FILE + ".tmp").toPath();
        try {
            Files.createDirectories(baseDir.toPath());
            Files.writeString(tmp, b, StandardCharsets.US_ASCII);
            Files.move(tmp, f, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (final IOException e) {
            throw new UncheckedIOException("re-bootstrap marker " + f + " could not be written", e);
        }
    }

    /** {@code cluster.rebootstrap-<yyyyMMdd'T'HHmmssSSS'Z'>}. */
    static String dirName(final long nowMs) {
        return MOVED_PREFIX + UTC.format(Instant.ofEpochMilli(nowMs));
    }

    /** Moves {@code BASE_DIR/cluster} to {@code BASE_DIR/<dirName>}; false when there is no {@code cluster/}. */
    static boolean moveAside(final File clusterDir, final String dirName) {
        if (!clusterDir.exists()) {
            return false;
        }
        final Path target = new File(clusterDir.getParentFile(), dirName).toPath();
        try {
            Files.move(clusterDir.toPath(), target, StandardCopyOption.ATOMIC_MOVE);
        } catch (final IOException e) {
            throw new UncheckedIOException("could not move " + clusterDir + " to " + target, e);
        }
        return true;
    }

    /** Deletes the oldest moved-aside dirs beyond {@code keep}; returns the deleted names. */
    static List<String> pruneMoved(final File baseDir, final int keep) {
        final List<String> moved = new ArrayList<>();
        final String[] names = baseDir.list();
        if (names != null) {
            for (final String n : names) {
                if (n.startsWith(MOVED_PREFIX) && new File(baseDir, n).isDirectory()) {
                    moved.add(n);
                }
            }
        }
        moved.sort(null); // the UTC stamp sorts by time
        final List<String> deleted = new ArrayList<>();
        for (int i = 0; i < moved.size() - keep; i++) {
            org.agrona.IoUtil.delete(new File(baseDir, moved.get(i)), true);
            deleted.add(moved.get(i));
        }
        return deleted;
    }
}
