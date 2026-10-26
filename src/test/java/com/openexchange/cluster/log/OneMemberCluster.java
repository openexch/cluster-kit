// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.security.Authenticator;
import io.aeron.security.SessionProxy;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Test harness: one cluster member in-process (embedded driver, archive, consensus module, one service container) on
 * free ports under {@code baseDir}, restartable on the same directories. The log is IPC, ingress and egress UDP; the
 * authenticator makes each client's credentials its principal. Backoff idle everywhere.
 */
final class OneMemberCluster implements AutoCloseable {

    private final File baseDir;
    private final String members;
    private final int ingressPort;
    private ClusteredMediaDriver driver;
    private ClusteredServiceContainer container;

    OneMemberCluster(final File baseDir) {
        this.baseDir = baseDir;
        final int[] ports = freePorts(5);
        ingressPort = ports[0];
        members = "0,localhost:" + ports[0] + ",localhost:" + ports[1] + ",localhost:" + ports[2] + ",localhost:"
                + ports[3] + ",localhost:" + ports[4];
    }

    void start(final ClusteredService service) {
        final String aeronDir = new File(baseDir, "driver").getAbsolutePath();
        final File clusterDir = new File(baseDir, "cluster");
        final AeronArchive.Context archiveClient = new AeronArchive.Context()
                .controlRequestChannel("aeron:ipc")
                .controlResponseChannel("aeron:ipc")
                .aeronDirectoryName(aeronDir);
        driver = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                        .aeronDirectoryName(aeronDir)
                        .threadingMode(ThreadingMode.SHARED)
                        .sharedIdleStrategy(new BackoffIdleStrategy())
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true),
                new Archive.Context()
                        .aeronDirectoryName(aeronDir)
                        .archiveDir(new File(baseDir, "archive"))
                        .controlChannelEnabled(false)
                        .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .recordingEventsEnabled(false)
                        .threadingMode(ArchiveThreadingMode.SHARED),
                new ConsensusModule.Context()
                        .aeronDirectoryName(aeronDir)
                        .clusterMemberId(0)
                        .clusterMembers(members)
                        .clusterDir(clusterDir)
                        .archiveContext(archiveClient.clone())
                        .logChannel("aeron:ipc?term-length=256k")
                        .ingressChannel("aeron:udp")
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .serviceCount(1)
                        .idleStrategySupplier(BackoffIdleStrategy::new)
                        .authenticatorSupplier(PrincipalIsCredentials::new)
                        .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                        .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                        .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                        .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                        .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                        .errorHandler(Throwable::printStackTrace));
        container = ClusteredServiceContainer.launch(new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .archiveContext(archiveClient.clone())
                .clusterDir(clusterDir)
                .clusteredService(service)
                .serviceId(0)
                .idleStrategySupplier(BackoffIdleStrategy::new)
                .errorHandler(Throwable::printStackTrace));
    }

    void stop() {
        CloseHelper.closeAll(container, driver);
        container = null;
        driver = null;
    }

    File clusterDir() {
        return new File(baseDir, "cluster");
    }

    File archiveDir() {
        return new File(baseDir, "archive");
    }

    AeronCluster connect(final String principal) {
        return AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(new File(baseDir, "driver").getAbsolutePath())
                .ingressChannel("aeron:udp")
                .ingressEndpoints("0=localhost:" + ingressPort)
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .egressListener(new EgressListener() {
                    @Override
                    public void onMessage(final long clusterSessionId, final long timestamp,
                                          final org.agrona.DirectBuffer buffer, final int offset, final int length,
                                          final io.aeron.logbuffer.Header header) {
                    }
                })
                .credentialsSupplier(new io.aeron.security.CredentialsSupplier() {
                    @Override
                    public byte[] encodedCredentials() {
                        return principal.getBytes(StandardCharsets.US_ASCII);
                    }

                    @Override
                    public byte[] onChallenge(final byte[] encodedChallenge) {
                        return new byte[0];
                    }
                })
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
    }

    static void send(final AeronCluster client, final byte[] payload) {
        final UnsafeBuffer buffer = new UnsafeBuffer(payload);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (client.offer(buffer, 0, payload.length) < 0) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("ingress offer timed out");
            }
            client.pollEgress();
            Thread.yield();
        }
    }

    AtomicCounter controlToggle(final AeronCluster client) {
        return ClusterControl.findControlToggle(client.context().aeron().countersReader(), 0);
    }

    @Override
    public void close() {
        stop();
    }

    private static int[] freePorts(final int n) {
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

    /** Authenticates every session, with its credentials as the encoded principal. */
    static final class PrincipalIsCredentials implements Authenticator {
        private final Map<Long, byte[]> credentials = new ConcurrentHashMap<>();

        @Override
        public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
            credentials.put(sessionId, encodedCredentials);
        }

        @Override
        public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs) {
        }

        @Override
        public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs) {
            final byte[] principal = credentials.remove(sessionProxy.sessionId());
            sessionProxy.authenticate(principal == null ? new byte[0] : principal);
        }

        @Override
        public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs) {
        }
    }
}
