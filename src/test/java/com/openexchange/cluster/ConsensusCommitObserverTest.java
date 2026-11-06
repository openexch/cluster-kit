// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.CommitPositionEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import java.net.DatagramSocket;
import java.nio.file.Files;
import static org.junit.Assert.*;

public class ConsensusCommitObserverTest {
    @Test public void passiveWireObserverFencesTermsLeadersAndBufferedBacklog() throws Exception {
        final int port;
        try (var socket = new DatagramSocket(0)) { port = socket.getLocalPort(); }
        final var directory = Files.createTempDirectory("consensus-observer-");
        final String channel = "aeron:udp?endpoint=127.0.0.1:" + port;
        try (var driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(directory.toString())
                    .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             var aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
             var consensus = aeron.addSubscription(channel, 19);
             var publication = aeron.addPublication(channel, 19);
             var observer = new ConsensusCommitObserver(new ConsensusModule.Context().clusterMemberId(1)
                    .consensusStreamId(19).clusterMembers(
                        "0,localhost:1,localhost:2,localhost:3,localhost:4,localhost:5|"
                        + "1,localhost:1,127.0.0.1:" + port + ",localhost:3,localhost:4,localhost:5|"
                        + "2,localhost:1,localhost:6,localhost:3,localhost:4,localhost:5"))) {
            observer.start(aeron);
            send(publication, consensus, 7, 0, 100);
            var proof = await(observer, consensus, 7, 100);
            assertTrue(proof.source() >= 0);
            assertTrue(proof.observedNs() > 0);
            // Same-term older commit cannot replace the established bound.
            send(publication, consensus, 7, 0, 99);
            send(publication, consensus, 6, 0, 1000);
            send(publication, consensus, 7, 0, 101);
            await(observer, consensus, 7, 101);
            // A different claimed leader in one term withdraws authority.
            send(publication, consensus, 7, 2, 102);
            long deadline = System.nanoTime() + 100_000_000;
            while (System.nanoTime() < deadline) { consensus.poll((b,o,l,h) -> {}, 100); observer.poll(System.nanoTime()); }
            assertNull(observer.poll(System.nanoTime()));
            send(publication, consensus, 8, 2, 200);
            await(observer, consensus, 8, 200);
            for (int i = 0; i < 40; i++) { send(publication, consensus, 8, 2, 201 + i); }
            // Wait for receipt on the normal subscriber, leaving the observer unread.
            deadline = System.nanoTime() + 100_000_000;
            while (System.nanoTime() < deadline) { consensus.poll((b,o,l,h) -> {}, 100); }
            assertNull("full batch must not certify an undrained observer", observer.poll(System.nanoTime()));
            await(observer, consensus, 8, 240);
        } finally {
            if (Files.exists(directory)) {
                try (var paths = Files.walk(directory)) {
                    for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) { Files.delete(path); }
                }
            }
        }
    }

    private static void send(Publication publication, Subscription consensus, long term, int member, long position) {
        var buffer = new UnsafeBuffer(new byte[64]);
        var encoder = new CommitPositionEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.leadershipTermId(term).leaderMemberId(member).logPosition(position);
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (publication.offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength()) < 0) {
            if (System.nanoTime() >= deadline) { fail("test consensus publication did not connect"); }
            consensus.poll((b,o,l,h) -> {}, 100); Thread.yield();
        }
        consensus.poll((b,o,l,h) -> {}, 100);
    }

    private static ConsensusReadiness.LeaderCheckpoint await(ConsensusCommitObserver observer,
            Subscription consensus, long term, long position) {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (System.nanoTime() < deadline) {
            consensus.poll((b,o,l,h) -> {}, 100);
            var proof = observer.poll(System.nanoTime());
            if (proof != null && proof.term() == term && proof.position() == position) { return proof; }
            Thread.yield();
        }
        throw new AssertionError("current leader commit did not reach passive observer");
    }
}
