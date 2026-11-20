// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.cluster.ConsensusModule;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PeerReachabilityTest {
    private static final String CONSENSUS = "aeron:udp?term-length=16m|mtu=1408";

    @Test
    public void theProbeRunsOnItsOwnSmallSparseTermNotTheConsensusTerm() {
        final ChannelUri uri = ChannelUri.parse(PeerReachability.probeChannel(CONSENSUS, "10.0.0.2:20111",
                PeerReachability.DEFAULT_PROBE_TERM_LENGTH));

        assertEquals("10.0.0.2:20111", uri.get(CommonContext.ENDPOINT_PARAM_NAME));
        assertEquals("65536", uri.get(CommonContext.TERM_LENGTH_PARAM_NAME));
        assertEquals("true", uri.get(CommonContext.SPARSE_PARAM_NAME));
        assertEquals("the rest of the consensus channel is kept", "1408", uri.get(CommonContext.MTU_LENGTH_PARAM_NAME));
    }

    @Test
    public void theProbeTermLengthIsASettingWithA64KibDefault() {
        assertEquals(64 * 1024, PeerReachability.probeTermLength(null, null));
        assertEquals(1 << 20, PeerReachability.probeTermLength("1048576", null));
        assertEquals(128 * 1024, PeerReachability.probeTermLength(null, "131072"));
    }

    @Test
    public void aProbeTermLengthTheDriverWouldRefuseRefusesTheBoot() {
        final IllegalArgumentException notPowerOfTwo = assertThrows(IllegalArgumentException.class,
                () -> PeerReachability.probeTermLength("100000", null));
        assertTrue(notPowerOfTwo.getMessage(), notPowerOfTwo.getMessage().contains(PeerReachability.PROBE_TERM_LENGTH_ENV));
        assertThrows(IllegalArgumentException.class, () -> PeerReachability.probeTermLength("32768", null));
        assertThrows(IllegalArgumentException.class, () -> PeerReachability.probeTermLength("64k", null));
    }

    @Test
    public void theProbeStreamIdIsASettingWithItsOwnDefault() {
        assertEquals(4306, PeerReachability.DEFAULT_PROBE_STREAM_ID);
        assertEquals(4306, PeerReachability.probeStreamId(null, null));
        assertEquals(4306, PeerReachability.probeStreamId("", null));
        assertEquals(9000, PeerReachability.probeStreamId("9000", "7"));
        assertEquals(7, PeerReachability.probeStreamId(null, "7"));
        assertEquals(1, PeerReachability.probeStreamId("1", null));
        assertEquals(Integer.MAX_VALUE, PeerReachability.probeStreamId(String.valueOf(Integer.MAX_VALUE), null));
    }

    @Test
    public void aProbeStreamIdOutsideTheRangeRefusesTheBoot() {
        for (final String bad : new String[] {"0", "-1", "abc", "1.5", "2147483648", "4306x"}) {
            final IllegalArgumentException e = assertThrows(bad, IllegalArgumentException.class,
                    () -> PeerReachability.probeStreamId(bad, null));
            assertTrue(e.getMessage(), e.getMessage().contains(PeerReachability.PROBE_STREAM_ID_ENV));
        }
        assertThrows(IllegalArgumentException.class, () -> PeerReachability.probeStreamId(null, "0"));
    }

    @Test
    public void theProbeStreamMustDifferFromTheConsensusStream() {
        assertNull(PeerReachability.probeStreamRefusal(PeerReachability.DEFAULT_PROBE_STREAM_ID,
                ConsensusModule.Configuration.CONSENSUS_STREAM_ID_DEFAULT));
        final String refusal = PeerReachability.probeStreamRefusal(108, 108);
        assertTrue(String.valueOf(refusal), refusal != null
                && refusal.contains("ENGINE_REJOIN_PROBE_STREAM_ID (108)")
                && refusal.contains(ConsensusModule.Configuration.CONSENSUS_STREAM_ID_PROP_NAME));
    }

    /** Derived as the consensus module derives its own consensus subscription. */
    @Test
    public void theParticipationChannelIsTheConsensusChannelOnThisMembersEndpoint() {
        final ChannelUri uri = ChannelUri.parse(PeerReachability.participationChannel(CONSENSUS, "10.0.0.1:20111"));
        assertEquals("10.0.0.1:20111", uri.get(CommonContext.ENDPOINT_PARAM_NAME));
        assertEquals("1408", uri.get(CommonContext.MTU_LENGTH_PARAM_NAME));
        assertEquals("16m", uri.get(CommonContext.TERM_LENGTH_PARAM_NAME));
        final ChannelUri explicit = ChannelUri.parse(PeerReachability.participationChannel(
                "aeron:udp?endpoint=0.0.0.0:20111", "10.0.0.1:20111"));
        assertEquals("an endpoint the consensus channel names is kept", "0.0.0.0:20111",
                explicit.get(CommonContext.ENDPOINT_PARAM_NAME));
    }

    /**
     * Two members on a real driver: member 1 reads member 0 reachable only once member 0's consensus module state has
     * left INIT (member 0 then opens its participation subscription from its tick), never while it is in startup
     * recovery.
     */
    @Test(timeout = 60_000)
    public void aPeerCountsReachableOnlyOnceItsModuleLeftStartupRecovery() throws Exception {
        final File dir = Files.createTempDirectory("kit-peer-reachability-").toFile();
        final int[] ports = ThreeMemberCluster.freePorts(10);
        final String members = "0,localhost:" + ports[0] + ",localhost:" + ports[1] + ",localhost:" + ports[2]
                + ",localhost:" + ports[3] + ",localhost:" + ports[4]
                + "|1,localhost:" + ports[5] + ",localhost:" + ports[6] + ",localhost:" + ports[7]
                + ",localhost:" + ports[8] + ",localhost:" + ports[9];
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        final List<String> log = new CopyOnWriteArrayList<>();
        final AtomicLong state0 = new AtomicLong(ConsensusModule.State.INIT.code());
        final AtomicLong state1 = new AtomicLong(ConsensusModule.State.ACTIVE.code());
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             PeerReachability member0 = new PeerReachability(aeronDir, "aeron:udp", 4306, members, 0, log::add,
                     PeerReachability.DEFAULT_PROBE_TERM_LENGTH, state0::get);
             PeerReachability member1 = new PeerReachability(aeronDir, "aeron:udp", 4306, members, 1, log::add,
                     PeerReachability.DEFAULT_PROBE_TERM_LENGTH, state1::get)) {
            assertEquals(2, member1.quorum());
            member0.tick();
            assertFalse("a module in INIT opens no participation subscription", member0.participating());
            member1.tick();
            assertTrue(member1.participating());
            // member 0 is restoring: its driver is up, nothing answers member 1's probe on the participation stream
            final long until = System.nanoTime() + 2_000_000_000L;
            while (System.nanoTime() < until) {
                member0.tick();
                assertFalse("a peer in startup recovery read reachable", member1.getAsBoolean());
                Thread.sleep(50);
            }
            assertFalse(member0.participating());
            assertEquals(1, member1.lastReachable());
            state0.set(ConsensusModule.State.ACTIVE.code());
            member0.tick();
            assertTrue(member0.participating());
            ThreeMemberCluster.await("member 1 to read member 0 reachable", 20_000, member1::getAsBoolean);
            assertEquals(2, member1.lastReachable());
            ThreeMemberCluster.await("member 0 to read member 1 reachable", 20_000, member0::getAsBoolean);
            // kept until the probe closes, whatever the module state does next
            state0.set(ConsensusModule.State.INIT.code());
            member0.tick();
            assertTrue(member0.participating());
            assertTrue(member1.getAsBoolean());
            assertTrue(log.toString(), log.isEmpty());
        } finally {
            org.agrona.IoUtil.delete(dir, true);
        }
    }

    @Test
    public void aClosedProbeNeitherOpensNorAnswers() {
        final AtomicLong state = new AtomicLong(ConsensusModule.State.ACTIVE.code());
        final PeerReachability probe = new PeerReachability("/nonexistent-aeron-dir", "aeron:udp", 4306,
                "0,a:1,a:2,a:3,a:4,a:5|1,b:1,b:2,b:3,b:4,b:5", 0, line -> { },
                PeerReachability.DEFAULT_PROBE_TERM_LENGTH, state::get);
        probe.close();
        probe.tick();
        assertFalse(probe.participating());
        assertFalse(probe.getAsBoolean());
    }

    @Test
    public void onlyARunningModuleStateCountsAsRunning() {
        for (final ConsensusModule.State state : ConsensusModule.State.values()) {
            final boolean running = state == ConsensusModule.State.ACTIVE || state == ConsensusModule.State.SUSPENDED
                    || state == ConsensusModule.State.SNAPSHOT;
            assertEquals(state.name(), running, PeerReachability.isRunning(state.code()));
        }
        assertFalse("an unavailable counter", PeerReachability.isRunning(QuorumWatchdog.STATE_UNAVAILABLE));
        assertFalse("a missing counter reads unavailable",
                PeerReachability.isRunning(PeerReachability.moduleState(null).getAsLong()));
        assertFalse(PeerReachability.isRunning(99));
    }

    /**
     * A module state that is not running never opens the participation subscription: had the probe tried, its client
     * on a directory without a driver would have failed and said so through the log.
     */
    @Test
    public void aModuleThatIsNotRunningOpensNoParticipationSubscription() {
        final long[] notRunning = {ConsensusModule.State.INIT.code(), ConsensusModule.State.QUITTING.code(),
                ConsensusModule.State.TERMINATING.code(), ConsensusModule.State.CLOSED.code(),
                QuorumWatchdog.STATE_UNAVAILABLE, PeerReachability.moduleState(null).getAsLong()};
        for (final long code : notRunning) {
            final List<String> log = new CopyOnWriteArrayList<>();
            try (PeerReachability probe = new PeerReachability("/nonexistent-aeron-dir", "aeron:udp", 4306,
                    "0,a:1,a:2,a:3,a:4,a:5|1,b:1,b:2,b:3,b:4,b:5", 0, log::add,
                    PeerReachability.DEFAULT_PROBE_TERM_LENGTH, () -> code)) {
                probe.tick();
                probe.tick();
                assertFalse("state " + code, probe.participating());
                assertFalse("state " + code, probe.clientOpen());
                assertTrue("state " + code + " tried to open the client: " + log, log.isEmpty());
            }
        }
    }

    /**
     * The client's error handler runs on the client conductor thread, holding the client lock: it must return without
     * this probe's lock and without closing the client. The poller gives up and closes on its next tick, once.
     */
    @Test(timeout = 60_000)
    public void aClientFailureIsHandledOnThePollerNotOnTheConductor() throws Exception {
        final File dir = Files.createTempDirectory("kit-peer-reachability-").toFile();
        final int[] ports = ThreeMemberCluster.freePorts(10);
        final String members = "0,localhost:" + ports[0] + ",localhost:" + ports[1] + ",localhost:" + ports[2]
                + ",localhost:" + ports[3] + ",localhost:" + ports[4]
                + "|1,localhost:" + ports[5] + ",localhost:" + ports[6] + ",localhost:" + ports[7]
                + ",localhost:" + ports[8] + ",localhost:" + ports[9];
        final String aeronDir = new File(dir, "driver").getAbsolutePath();
        final List<String> log = new CopyOnWriteArrayList<>();
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             PeerReachability probe = new PeerReachability(aeronDir, "aeron:udp", 4306, members, 0, log::add,
                     PeerReachability.DEFAULT_PROBE_TERM_LENGTH, () -> ConsensusModule.State.ACTIVE.code())) {
            probe.tick();
            assertTrue(probe.participating());
            assertTrue(probe.clientOpen());
            // the poller holds the probe's lock while the conductor reports an error: the handler must still return
            final Thread conductor = new Thread(() -> probe.onError(new IllegalStateException("driver gone")),
                    "fake-client-conductor");
            synchronized (probe) {
                conductor.start();
                conductor.join(5_000);
                assertFalse("the error handler blocked on the probe's lock", conductor.isAlive());
            }
            assertTrue("the error handler closed nothing", probe.clientOpen());
            assertTrue(probe.participating());
            assertTrue(log.toString(), log.isEmpty());
            probe.tick();
            assertFalse(probe.participating());
            assertFalse("the poller closed the client", probe.clientOpen());
            assertEquals(log.toString(), 1, log.size());
            assertTrue(log.get(0), log.get(0).contains("client failed: java.lang.IllegalStateException: driver gone")
                    && log.get(0).contains("peers read it unreachable"));
            probe.onError(new IllegalStateException("again"));
            probe.tick();
            assertFalse(probe.getAsBoolean());
            assertFalse(probe.participating());
            assertEquals("said once", 1, log.size());
        } finally {
            org.agrona.IoUtil.delete(dir, true);
        }
    }
}
