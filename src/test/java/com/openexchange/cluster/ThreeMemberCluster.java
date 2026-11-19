// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Test harness: three cluster members in one process, each with its own embedded driver, archive, consensus module
 * and one counting service, on free localhost ports under {@code baseDir}. The log runs over UDP with 64 KiB terms
 * and 64 KiB archive segments, so a few hundred small messages cross several segment boundaries and a purge really
 * removes files. Each member wires the kit's {@link SnapshotLogPruner} the way the engines do. A member can be
 * stopped and started again on its own directories.
 */
final class ThreeMemberCluster implements AutoCloseable {

    static final int MEMBERS = 3;
    static final int SEGMENT = 64 * 1024;
    private static final int PORTS_PER_MEMBER = 5;

    private final File baseDir;
    private final String members;
    private final int[] ports;
    private final Node[] nodes = new Node[MEMBERS];
    private MediaDriver clientDriver;
    private AeronCluster client;
    /** Applied to every pruner at start (the tests shorten the poll and set the bound). */
    java.util.function.Consumer<SnapshotLogPruner> prunerSetup = p -> { };
    /**
     * False (default): each member runs Aeron's clustered media driver, the embedded driver mode. True: the media
     * driver is launched on its own and the archive and the consensus module connect to its directory as clients,
     * the way a member runs next to an external aeronmd.
     */
    boolean standAloneDriver;
    /** Whether a started member wires the retention floor ({@code reportWatermark}), as both engines do. */
    boolean wireRetention = true;
    /**
     * True: the archive context handed to the pruner names a driver directory that does not exist. The engines hand
     * the pruner a clone of the container's archive context, whose directory name is whatever it was when the
     * context was built; next to an external driver that is not always the driver's directory. The context's Aeron
     * client is the live one either way.
     */
    boolean staleArchiveDirectoryName;

    /** One member's components and what its service has applied. */
    static final class Node {
        final int id;
        /** Embedded mode: driver, archive and consensus module in one; null in the stand-alone driver mode. */
        ClusteredMediaDriver driver;
        /** Stand-alone driver mode: the three launched one by one, the way a member runs next to an aeronmd. */
        MediaDriver mediaDriver;
        Archive archive;
        ConsensusModule module;
        ClusteredServiceContainer container;
        CountingService service;
        SnapshotLogPruner pruner;
        EngineLifecycle lifecycle;
        final List<String> log = new CopyOnWriteArrayList<>();
        final List<Integer> halts = new CopyOnWriteArrayList<>();
        final List<Throwable> errors = new CopyOnWriteArrayList<>();

        Node(final int id) {
            this.id = id;
        }

        boolean running() {
            return module != null;
        }

        ConsensusModule.Context consensus() {
            return module.context();
        }

        boolean isLeader() {
            return running() && consensus().clusterNodeRoleCounter().get() == Cluster.Role.LEADER.code();
        }

        long electionState() {
            return consensus().electionStateCounter().get();
        }

        boolean logged(final String text) {
            return log.stream().anyMatch(l -> l.contains(text));
        }
    }

    /** Counts session messages; the snapshot is the count. */
    static final class CountingService implements ClusteredService {
        volatile long applied;
        volatile long lastPosition;
        volatile Cluster.Role role = Cluster.Role.FOLLOWER;
        volatile Cluster cluster;

        @Override
        public void onStart(final Cluster cluster, final Image snapshotImage) {
            this.cluster = cluster;
            if (snapshotImage != null) {
                final long[] loaded = {-1};
                while (loaded[0] < 0 && !snapshotImage.isEndOfStream()) {
                    snapshotImage.poll((buffer, offset, length, header) -> loaded[0] = buffer.getLong(offset), 1);
                    cluster.idleStrategy().idle();
                }
                applied = Math.max(0, loaded[0]);
            }
        }

        @Override
        public void onSessionOpen(final ClientSession session, final long timestamp) {
        }

        @Override
        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        }

        @Override
        public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                     final int offset, final int length, final Header header) {
            applied++;
            lastPosition = header.position();
        }

        @Override
        public void onTimerEvent(final long correlationId, final long timestamp) {
        }

        @Override
        public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[Long.BYTES]);
            buffer.putLong(0, applied);
            while (snapshotPublication.offer(buffer, 0, Long.BYTES) < 0) {
                cluster.idleStrategy().idle();
            }
        }

        @Override
        public void onRoleChange(final Cluster.Role newRole) {
            role = newRole;
        }

        @Override
        public void onTerminate(final Cluster cluster) {
        }
    }

    ThreeMemberCluster(final File baseDir) {
        this.baseDir = baseDir;
        ports = freePorts(MEMBERS * PORTS_PER_MEMBER);
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MEMBERS; i++) {
            sb.append(i);
            for (int p = 0; p < PORTS_PER_MEMBER; p++) {
                sb.append(",localhost:").append(port(i, p));
            }
            sb.append('|');
        }
        members = sb.substring(0, sb.length() - 1);
        for (int i = 0; i < MEMBERS; i++) {
            nodes[i] = new Node(i);
        }
    }

    int port(final int member, final int slot) {
        return ports[member * PORTS_PER_MEMBER + slot];
    }

    /** The consensus endpoint port of {@code member} (slot 1 of the member string). */
    int consensusPort(final int member) {
        return port(member, 1);
    }

    String members() {
        return members;
    }

    Node node(final int id) {
        return nodes[id];
    }

    File memberDir(final int id) {
        return new File(baseDir, "member" + id);
    }

    String aeronDir(final int id) {
        return new File(memberDir(id), "driver").getAbsolutePath();
    }

    File clusterDir(final int id) {
        return new File(memberDir(id), "cluster");
    }

    AeronArchive.Context localArchive(final int id) {
        return new AeronArchive.Context()
                .controlRequestChannel("aeron:ipc")
                .controlResponseChannel("aeron:ipc")
                .aeronDirectoryName(aeronDir(id));
    }

    void startAll() {
        for (int i = 0; i < MEMBERS; i++) {
            start(i);
        }
    }

    /** Start (or start again, on the same directories) one member; {@code lifecycle} may be null. */
    Node start(final int id) {
        return start(id, null);
    }

    Node start(final int id, final EngineLifecycle lifecycle) {
        final Node n = nodes[id];
        n.lifecycle = lifecycle;
        n.service = new CountingService();
        final File clusterDir = clusterDir(id);
        final MediaDriver.Context driverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir(id))
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new BackoffIdleStrategy())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        final Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(aeronDir(id))
                .archiveDir(new File(memberDir(id), "archive"))
                .controlChannel("aeron:udp?endpoint=localhost:" + port(id, 4))
                .localControlChannel("aeron:ipc")
                .archiveClientContext(new AeronArchive.Context()
                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"))
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .segmentFileLength(SEGMENT)
                .errorHandler(n.errors::add)
                .threadingMode(ArchiveThreadingMode.SHARED);
        final ConsensusModule.Context consensusContext = new ConsensusModule.Context()
                .aeronDirectoryName(aeronDir(id))
                .clusterMemberId(id)
                .clusterMembers(members)
                .clusterDir(clusterDir)
                .archiveContext(localArchive(id))
                .logChannel("aeron:udp?term-length=64k")
                .ingressChannel("aeron:udp")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .serviceCount(1)
                .idleStrategySupplier(BackoffIdleStrategy::new)
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .errorHandler(n.errors::add);
        if (standAloneDriver) {
            n.mediaDriver = MediaDriver.launch(driverContext);
            n.archive = Archive.launch(archiveContext);
            n.module = ConsensusModule.launch(consensusContext);
        } else {
            n.driver = ClusteredMediaDriver.launch(driverContext, archiveContext, consensusContext);
            n.module = n.driver.consensusModule();
        }
        if (lifecycle != null) {
            lifecycle.watchQuorum(n.consensus());
        }
        n.container = ClusteredServiceContainer.launch(new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir(id))
                .archiveContext(localArchive(id))
                .clusterDir(clusterDir)
                .clusteredService(n.service)
                .serviceId(0)
                .idleStrategySupplier(BackoffIdleStrategy::new)
                .errorHandler(n.errors::add));
        // the engines' wiring: one pruner per member, caught-up gate, the retention report on the leader
        final ClusteredServiceContainer.Context cc = n.container.context();
        final CountingService service = n.service;
        final AeronArchive.Context prunerArchive = cc.archiveContext().clone();
        if (staleArchiveDirectoryName) {
            prunerArchive.aeronDirectoryName(new File(baseDir, "no-such-driver-" + id).getAbsolutePath());
        }
        n.pruner = new SnapshotLogPruner(clusterDir, prunerArchive, cc.aeron().countersReader(), cc.clusterId(),
                () -> true);
        if (wireRetention) {
            n.pruner.reportWatermark(ClusterMemberPositions.from(cc.aeron(), cc),
                    () -> service.role == Cluster.Role.LEADER);
        }
        prunerSetup.accept(n.pruner);
        n.pruner.start();
        return n;
    }

    /** Stop one member: its position on disk is what a killed member restarts from. */
    void stop(final int id) {
        final Node n = nodes[id];
        if (n.pruner != null) {
            n.pruner.stop();
        }
        if (n.lifecycle != null) {
            n.lifecycle.onClusterTermination();
            n.lifecycle.awaitStop();
        }
        CloseHelper.closeAll(n.container, n.module, n.archive, n.mediaDriver, n.driver);
        n.container = null;
        n.module = null;
        n.archive = null;
        n.mediaDriver = null;
        n.driver = null;
        n.pruner = null;
    }

    Node leader() {
        for (final Node n : nodes) {
            if (n.isLeader() && n.electionState() == ElectionState.CLOSED.code()) {
                return n;
            }
        }
        return null;
    }

    Node awaitLeader(final long timeoutMs) {
        final Node[] found = new Node[1];
        await("a leader", timeoutMs, () -> (found[0] = leader()) != null);
        return found[0];
    }

    /** The client runs on its own driver so no member's stop takes it down. */
    AeronCluster client() {
        final String dir = new File(baseDir, "client-driver").getAbsolutePath();
        if (clientDriver == null) {
            clientDriver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(dir)
                    .threadingMode(ThreadingMode.SHARED)
                    .sharedIdleStrategy(new BackoffIdleStrategy())
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
        }
        if (client == null) {
            final StringBuilder ingress = new StringBuilder();
            for (int i = 0; i < MEMBERS; i++) {
                ingress.append(i).append("=localhost:").append(port(i, 0)).append(i < MEMBERS - 1 ? "," : "");
            }
            client = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(dir)
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(ingress.toString())
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) -> { })
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(20)));
        }
        return client;
    }

    /** Drop the client session; the next {@link #send} connects again and finds the current leader. */
    void reconnectClient() {
        CloseHelper.quietClose(client);
        client = null;
    }

    /** Send {@code count} messages of {@code size} bytes through the cluster client. */
    void send(final int count, final int size) {
        final AeronCluster c = client();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[size]);
        for (int i = 0; i < count; i++) {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            long result;
            while ((result = c.offer(buffer, 0, size)) < 0) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("ingress offer timed out at message " + i + " (offer result "
                            + result + "): " + describe());
                }
                c.pollEgress();
                Thread.yield();
            }
            if ((i & 15) == 0) {
                c.pollEgress();
                c.sendKeepAlive();
            }
        }
    }

    /** Every member's role, election state and errors, for a failure message. */
    String describe() {
        final StringBuilder sb = new StringBuilder();
        for (final Node n : nodes) {
            sb.append("member ").append(n.id);
            if (n.running()) {
                sb.append(n.isLeader() ? " leader" : " follower").append(" election state ")
                        .append(n.electionState()).append(" elections ").append(n.consensus().electionCounter().get())
                        .append(" applied ").append(n.service.applied);
            } else {
                sb.append(" stopped");
            }
            sb.append(" errors ").append(n.errors).append("; ");
        }
        return sb.toString();
    }

    /** Ask the leader's consensus module for a snapshot and wait for its snapshot counter to move. */
    void snapshot(final Node leader, final long timeoutMs) {
        final ConsensusModule.Context ctx = leader.consensus();
        final long before = ctx.snapshotCounter().get();
        final AtomicCounter toggle = ClusterControl.findControlToggle(ctx.aeron().countersReader(), ctx.clusterId());
        await("the control toggle to be neutral", timeoutMs,
                () -> ClusterControl.ToggleState.get(toggle) == ClusterControl.ToggleState.NEUTRAL);
        if (!ClusterControl.ToggleState.SNAPSHOT.toggle(toggle)) {
            throw new IllegalStateException("the snapshot toggle was refused");
        }
        await("the snapshot", timeoutMs, () -> ctx.snapshotCounter().get() > before);
    }

    /** Start and stop position of {@code id}'s log recording, read from its own archive. */
    long[] logExtent(final int id) {
        final long recordingId;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir(id), false)) {
            recordingId = recordingLog.findLastTermRecordingId();
        }
        final long[] extent = {-1, -1};
        try (AeronArchive archive = AeronArchive.connect(localArchive(id))) {
            archive.listRecording(recordingId, (controlSessionId, correlationId, id1, startTimestamp, stopTimestamp,
                    startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                    sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                extent[0] = startPosition;
                extent[1] = stopPosition;
            });
            if (extent[1] == AeronArchive.NULL_POSITION) {
                extent[1] = archive.getRecordingPosition(recordingId);
            }
        }
        return extent;
    }

    static void await(final String what, final long timeoutMs, final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out after " + timeoutMs + " ms waiting for " + what);
            }
            try {
                Thread.sleep(20);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for " + what);
            }
        }
    }

    @Override
    public void close() {
        CloseHelper.quietClose(client);
        client = null;
        for (int i = 0; i < MEMBERS; i++) {
            if (nodes[i].running()) {
                try {
                    stop(i);
                } catch (final RuntimeException e) {
                    e.printStackTrace();
                }
            }
        }
        CloseHelper.quietClose(clientDriver);
        clientDriver = null;
    }

    static int[] freePorts(final int n) {
        final DatagramSocket[] sockets = new DatagramSocket[n];
        final int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new DatagramSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            for (final DatagramSocket s : sockets) {
                if (s != null) {
                    s.close();
                }
            }
        }
        return ports;
    }
}
