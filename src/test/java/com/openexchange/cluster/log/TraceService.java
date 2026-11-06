// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The live oracle (M RCS-M3): a service that appends one record per callback, keyed by {@code cluster.logPosition()},
 * to an append-only file outside the cluster directory, so the trace survives a node restart. A message whose payload
 * starts with {@code TIMER} schedules timer {@link #TIMER_ID} 20 ms ahead. It writes nothing into its snapshots.
 *
 * <p>Record lines: {@code KIND|position|fields}; {@link #render} produces the same line from a reader-delivered frame,
 * restricted to what the service sees.</p>
 */
final class TraceService implements ClusteredService {

    static final long TIMER_ID = 4242;
    static final String START = "START";
    static final String SNAPSHOT = "SNAPSHOT";

    private final Path file;
    private Cluster cluster;

    TraceService(final Path file) {
        this.file = file;
    }

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        append(START + "|" + (snapshotImage != null));
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        append(open(cluster.logPosition(), session.id(), timestamp, session.responseStreamId(),
                session.responseChannel(), session.encodedPrincipal()));
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        append(close(cluster.logPosition(), session.id(), timestamp, closeReason));
    }

    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                 final int offset, final int length, final Header header) {
        final byte[] payload = new byte[length];
        buffer.getBytes(offset, payload);
        append(message(cluster.logPosition(), session == null ? -1 : session.id(), timestamp, payload));
        if (new String(payload, StandardCharsets.US_ASCII).startsWith("TIMER")) {
            cluster.scheduleTimer(TIMER_ID, cluster.time() + 20);
        }
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        append(timer(cluster.logPosition(), correlationId, timestamp));
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        append(SNAPSHOT + "|" + cluster.logPosition());
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
    }

    @Override
    public void onTerminate(final Cluster cluster) {
    }

    @Override
    public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                         final long termBaseLogPosition, final int leaderMemberId,
                                         final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
        append(term(cluster.logPosition(), leadershipTermId, timestamp, termBaseLogPosition, leaderMemberId,
                logSessionId, timeUnit, appVersion) + "|arg=" + logPosition);
    }

    // ------------------------------------------------------------------------------------------- record format

    static String open(final long p, final long id, final long ts, final int stream, final String channel,
                       final byte[] principal) {
        return "OPEN|" + p + "|" + id + "|" + ts + "|" + stream + "|" + channel + "|" + hex(principal);
    }

    static String close(final long p, final long id, final long ts, final CloseReason reason) {
        return "CLOSE|" + p + "|" + id + "|" + ts + "|" + reason;
    }

    static String message(final long p, final long id, final long ts, final byte[] payload) {
        return "MSG|" + p + "|" + id + "|" + ts + "|" + hex(payload);
    }

    static String timer(final long p, final long correlationId, final long ts) {
        return "TIMER|" + p + "|" + correlationId + "|" + ts;
    }

    static String term(final long p, final long term, final long ts, final long base, final int leader,
                       final int logSession, final TimeUnit unit, final int app) {
        return "TERM|" + p + "|" + term + "|" + ts + "|" + base + "|" + leader + "|" + logSession + "|" + unit
                + "|" + app;
    }

    /** The service-visible record of a reader-delivered frame; null for kinds a service never sees (actions). */
    static String render(final com.openexchange.cluster.testkit.LogFrame f) {
        return switch (f.kind()) {
            case SESSION_OPEN -> open(f.position(), f.clusterSessionId(), f.timestamp(), f.responseStreamId(),
                    f.responseChannel(), f.data());
            case SESSION_CLOSE -> close(f.position(), f.clusterSessionId(), f.timestamp(), f.closeReason());
            case SESSION_MESSAGE -> message(f.position(), f.clusterSessionId(), f.timestamp(), f.data());
            case TIMER_EVENT -> timer(f.position(), f.correlationId(), f.timestamp());
            case NEW_LEADERSHIP_TERM -> term(f.position(), f.leadershipTermId(), f.timestamp(),
                    f.termBaseLogPosition(), f.leaderMemberId(), f.logSessionId(), f.timeUnit(), f.appVersion())
                    + "|arg=" + f.position();
            default -> null;
        };
    }

    static String hex(final byte[] bytes) {
        return bytes == null ? "null" : HexFormat.of().formatHex(bytes);
    }

    /** Complete lines written so far (a trailing partial line is left out). */
    static List<String> read(final Path file) {
        if (!Files.exists(file)) {
            return List.of();
        }
        final String content;
        try {
            content = Files.readString(file, StandardCharsets.US_ASCII);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final List<String> lines = new ArrayList<>(List.of(content.split("\n", -1)));
        lines.remove(lines.size() - 1); // after the last '\n': empty, or a partial line
        return lines;
    }

    private void append(final String line) {
        try {
            Files.writeString(file, line + "\n", StandardCharsets.US_ASCII, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
