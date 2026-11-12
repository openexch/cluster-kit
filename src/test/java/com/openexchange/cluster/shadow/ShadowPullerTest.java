// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowPuller.Counter;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static com.openexchange.cluster.shadow.ShadowRig.ENGINE;
import static com.openexchange.cluster.shadow.ShadowRig.F1;
import static com.openexchange.cluster.shadow.ShadowRig.GENESIS;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The shadow puller over two real archives (embedded drivers, UDP archive control on localhost): discovery filters,
 * the completion rule (SYNC or equal lengths), every refusal class with its retry or its process-lifetime refusal,
 * the leader gate with the abort of an in-flight pull, the dedupe by full alias against usable copies only, and the
 * one-shot startup pull with its report. Each negative control switches one rule off and the row's own assertion
 * fails with the named text.
 */
public class ShadowPullerTest {

    static final long PERIOD_MS = 500;
    static final int SMALL = 4_000;
    static final int BIG = 200_000_000;

    /** Two nodes and a puller on the destination. */
    static final class Pair implements AutoCloseable {
        final PullRig.Node src;
        final PullRig.Node dst;
        final ShadowVerdicts verdicts = new ShadowVerdicts();
        final ShadowCounters counters = new ShadowCounters();
        ShadowPuller puller;

        Pair(final int srcMaxReplays) {
            src = new PullRig.Node("src", srcMaxReplays);
            dst = new PullRig.Node("dst", 10);
        }

        ShadowPullSettings settings(final long timeoutMs, final int inflight, final boolean fixedEndpoints) {
            return new ShadowPullSettings(List.of(new ShadowPullSettings.Endpoint("localhost", src.controlPort)),
                    PERIOD_MS, 60_000, timeoutMs, 100, inflight, Set.of(ShadowKind.ALIGNED),
                    fixedEndpoints ? new ShadowPullSettings.Endpoint("localhost", PullRig.freeUdpPort()) : null,
                    fixedEndpoints ? new ShadowPullSettings.Endpoint("localhost", PullRig.freeUdpPort()) : null);
        }

        ShadowPuller puller(final ShadowPullSettings s, final long genesis, final BooleanSupplier leader,
                            final Set<NegativeControl> off) {
            return new ShadowPuller(dst.aeron(), dst.localContext(), ENGINE, 0, () -> genesis, leader, "localhost", s,
                    verdicts, counters, ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, "shadow-puller-test",
                    off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off));
        }

        ShadowPuller start(final ShadowPullSettings s, final BooleanSupplier leader, final Set<NegativeControl> off) {
            puller = puller(s, GENESIS, leader, off);
            puller.start();
            return puller;
        }

        long get(final Counter c) {
            return puller.counters().get(c);
        }

        long refused(final String reason) {
            return puller.counters().refused(reason);
        }

        /** Every destination copy of {@code alias} (any state), from a fresh catalog scan. */
        List<ShadowEntry> copies(final ShadowAlias alias) {
            final ShadowCatalog c = new ShadowCatalog(dst.aeron(), dst.archive(), ENGINE, 0, GENESIS, verdicts, 100,
                    ShadowRig.CLOCK, ShadowRig.TIMEOUT_NS, new ShadowCounters());
            final List<ShadowEntry> out = new ArrayList<>();
            for (final ShadowEntry e : c.scan().all()) {
                if (alias.equals(e.alias())) {
                    out.add(e);
                }
            }
            return out;
        }

        /** Waits until a destination recording passed {@code bytes} (a replication is under way). */
        void awaitDestinationProgress(final long bytes) {
            final CountersReader r = dst.aeron().countersReader();
            await("a destination recording past " + bytes + " bytes", () -> {
                final boolean[] found = {false};
                r.forEach((counterId, typeId, keyBuffer, label) -> {
                    if (typeId == RecordingPos.RECORDING_POSITION_TYPE_ID && r.getCounterValue(counterId) > bytes) {
                        found[0] = true;
                    }
                });
                return found[0];
            }, 30_000);
        }

        @Override
        public void close() {
            CloseHelper.quietCloseAll(puller, dst, src);
        }
    }

    static ShadowAlias aliasOf(final PullRig.Node n, final long recordingId) {
        final String[] ch = {null};
        n.archive().listRecording(recordingId, (a, b, c, d, e, f, g, h, i, j, k, l, m, nn, oc, p) -> ch[0] = oc);
        return ShadowAlias.parse(ShadowAlias.aliasOfChannel(ch[0]));
    }

    static void await(final String what, final BooleanSupplier c, final long ms) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
        while (!c.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(what + " (not within " + ms + " ms)");
            }
            sleep(10);
        }
    }

    static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    static ShadowHeader header(final ShadowProducer p, final long seq, final ShadowKind kind, final long position,
                               final long genesis, final byte[] payload) {
        return ShadowRig.header(p, seq, kind, position, genesis, 0, payload);
    }

    static byte[] payload(final long seed) {
        return ShadowRig.payload(ShadowRig.image(SMALL, seed));
    }

    // ------------------------------------------------------------------------------------------------ discovery

    /**
     * An ALIGNED is pulled over both fixed endpoints, alias preserved, VERIFIED locally; a STANDBY, an ACTIVE
     * recording and another genesis are never candidates.
     */
    static void discovery(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            final long aligned = p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            p.src.write(R3, 2, ShadowKind.STANDBY, 2_000, SMALL, 2, "64k");
            final byte[] active = payload(3);
            p.src.raw(header(R3, 3, ShadowKind.ALIGNED, 3_000, GENESIS, active), active, false, -1, false);
            final byte[] foreign = payload(4);
            p.src.raw(header(R3, 4, ShadowKind.ALIGNED, 4_000, 777, foreign), foreign, true, -1, true);
            final ShadowAlias alias = aliasOf(p.src, aligned);
            p.start(p.settings(120_000, 3, true), () -> false, off);
            await("pull_total == 1", () -> p.get(Counter.PULL_TOTAL) >= 1, 20_000);
            sleep(3 * PERIOD_MS);
            final ShadowAlias standby = new ShadowAlias(ENGINE, 0, GENESIS, 2_000, 3, R3.kind().code(), 3, 2,
                    ShadowKind.STANDBY.code());
            if (!p.copies(standby).isEmpty()) {
                throw new AssertionError("a STANDBY shadow was pulled under the default kinds");
            }
            if (p.get(Counter.STARTED_TOTAL) != 1) {
                throw new AssertionError("an ACTIVE source was pulled and never completed: started "
                        + p.get(Counter.STARTED_TOTAL));
            }
            final List<ShadowEntry> local = p.copies(alias);
            assertEquals(local.toString(), 1, local.size());
            assertEquals("alias preserved", alias, local.get(0).alias());
            assertEquals(ShadowEntry.State.VERIFIED, local.get(0).state());
            assertEquals(1, p.get(Counter.PULL_TOTAL));
            assertTrue(p.get(Counter.PULL_BYTES_TOTAL) > SMALL);
            ShadowRig.green("puller discovery: pulled " + local.get(0) + " over fixed endpoints "
                    + p.puller.settings().replicationEndpoint() + " / " + p.puller.settings().responseEndpoint()
                    + "; STANDBY, ACTIVE and genesis 777 never candidates");
        }
    }

    @Test(timeout = 120_000)
    public void pullsAnAlignedAndNothingElse() {
        discovery(Set.of());
        nc("PULL_KINDS_ANY", "a STANDBY shadow was pulled under the default kinds",
                () -> discovery(Set.of(NegativeControl.PULL_KINDS_ANY)));
        nc("PULL_ACTIVE_ALLOWED", "an ACTIVE source was pulled and never completed",
                () -> discovery(Set.of(NegativeControl.PULL_ACTIVE_ALLOWED)));
    }

    // ------------------------------------------------------------------------------------------------ MAX_REPLAYS

    static void maxReplays(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            final long aligned = p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            // ten live-following replays of an ACTIVE recording hold every replay slot of the source
            final ExclusivePublication live = p.src.archive().addRecordedExclusivePublication("aeron:ipc", 4400);
            p.src.keep(live);
            await("live recording", live::isConnected, 10_000);
            while (live.offer(new UnsafeBuffer(new byte[64]), 0, 64) < 0) {
                Thread.onSpinWait();
            }
            final long[] liveRec = {-1};
            await("the live recording is listed", () -> {
                p.src.archive().listRecordingsForUri(0, 10, "aeron:ipc", 4400,
                        (a, b, id, d, e, f, g, h, i, j, k, l, m, n, o, pp) -> liveRec[0] = id);
                return liveRec[0] >= 0;
            }, 10_000);
            final List<Long> sessions = new ArrayList<>();
            final List<Subscription> held = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final long s = p.src.archive().startReplay(liveRec[0], 0, AeronArchive.NULL_LENGTH,
                        "aeron:ipc", 4401);
                sessions.add(s);
                held.add(p.src.aeron().addSubscription(ChannelUri.addSessionId("aeron:ipc", (int)s), 4401));
            }
            p.start(p.settings(120_000, 1, false), () -> false, off);
            await("a GENERIC refusal with the source's text", () -> p.refused("GENERIC") >= 1
                    || p.refused("CORRUPT") >= 1, 20_000);
            if (p.refused("CORRUPT") != 0 || !p.puller.contentRefused().isEmpty()) {
                throw new AssertionError("an archive refusal was counted as a corrupt copy: " + p.puller.lastRefusal());
            }
            assertTrue(p.puller.lastRefusal(), p.puller.lastRefusal().contains("max concurrent replays reached"));
            for (final long s : sessions) {
                p.src.archive().stopReplay(s);
            }
            held.forEach(Subscription::close);
            await("pulled after the replays were released", () -> p.get(Counter.PULL_TOTAL) == 1, 20_000);
            assertTrue("retried: " + p.get(Counter.RETRIES_TOTAL), p.get(Counter.RETRIES_TOTAL) >= 1);
            assertEquals(ShadowEntry.State.VERIFIED, p.copies(aliasOf(p.src, aligned)).get(0).state());
            ShadowRig.green("puller MAX_REPLAYS: GENERIC '" + p.puller.lastRefusal() + "', retried "
                    + p.get(Counter.RETRIES_TOTAL) + "x, pulled after the release");
        }
    }

    @Test(timeout = 120_000)
    public void aSourceRefusalIsRetriedNeverContent() {
        maxReplays(Set.of());
        nc("PULL_RETRY_AS_CONTENT", "an archive refusal was counted as a corrupt copy",
                () -> maxReplays(Set.of(NegativeControl.PULL_RETRY_AS_CONTENT)));
    }

    // ------------------------------------------------------------------------------------------------ INCOMPLETE

    /**
     * The source archive stopped mid-copy. Aeron ends such a replication one of two ways, by timing: the replay image
     * closes and the session ends with no error and no SYNC (INCOMPLETE), or the destination's control session to the
     * source reports an error ({@code not connected}, an archive refusal). Both are retried, neither is content; the
     * scenario is repeated (up to three times) until the INCOMPLETE shape was seen, so the control below always meets
     * the rule it switches off.
     */
    static void killMidCopy(final Set<NegativeControl> off) {
        for (int attempt = 1; ; attempt++) {
            if (killMidCopyOnce(off, attempt)) {
                return;
            }
            if (attempt == 3) {
                throw new AssertionError("the source loss never ended as INCOMPLETE in 3 attempts");
            }
        }
    }

    private static boolean killMidCopyOnce(final Set<NegativeControl> off, final int attempt) {
        try (Pair p = new Pair(10)) {
            final long big = p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, BIG, 1, "4m");
            final ShadowAlias alias = aliasOf(p.src, big);
            p.start(p.settings(120_000, 1, false), () -> false, off);
            p.awaitDestinationProgress(16L * 1024 * 1024);
            p.src.stop();
            await("the copy ended short of the source", () -> p.refused("INCOMPLETE") >= 1
                    || p.refused("GENERIC") >= 1 || !p.puller.contentRefused().isEmpty(), 60_000);
            if (!p.puller.contentRefused().isEmpty() || p.refused("CORRUPT") != 0) {
                throw new AssertionError("a crashed source copy was refused for content: " + p.puller.lastRefusal());
            }
            if (p.refused("INCOMPLETE") == 0) {
                System.out.println("[puller test] attempt " + attempt + ": the source loss was answered as an archive"
                        + " refusal (" + p.puller.lastRefusal() + "), not content; repeating for the INCOMPLETE shape");
                return false;
            }
            await("the partial copy purged", () -> p.copies(alias).isEmpty(), 20_000);
            p.src.start(false);
            await("the same source re-pulled after it returned", () -> p.get(Counter.PULL_TOTAL) == 1, 60_000);
            assertEquals(ShadowEntry.State.VERIFIED, p.copies(alias).get(0).state());
            assertEquals(0, p.refused("CORRUPT"));
            ShadowRig.green("puller kill mid-copy: INCOMPLETE '" + p.puller.lastRefusal() + "', partial purged,"
                    + " re-pulled after the source returned");
            return true;
        }
    }

    @Test(timeout = 240_000)
    public void aCrashedSourceIsIncompleteAndRetried() {
        killMidCopy(Set.of());
        nc("INCOMPLETE_AS_CONTENT", "a crashed source copy was refused for content",
                () -> killMidCopy(Set.of(NegativeControl.INCOMPLETE_AS_CONTENT)));
    }

    // ------------------------------------------------------------------------------------------------ CONTENT

    static void corruptSource(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            final byte[] body = payload(5);
            final long rec = p.src.raw(header(R3, 1, ShadowKind.ALIGNED, 1_000, GENESIS, body), body, true,
                    100, true);
            final ShadowAlias alias = aliasOf(p.src, rec);
            p.start(p.settings(120_000, 1, false), () -> false, off);
            await("refused for content", () -> p.refused("CORRUPT") >= 1, 20_000);
            await("the local copy purged", () -> p.copies(alias).isEmpty(), 20_000);
            sleep(3 * PERIOD_MS + 500);
            if (p.get(Counter.STARTED_TOTAL) != 1) {
                throw new AssertionError("a corrupt source copy was pulled again: started "
                        + p.get(Counter.STARTED_TOTAL));
            }
            assertEquals(Set.of("localhost:" + p.src.controlPort + "#" + rec), p.puller.contentRefused());
            assertEquals(0, p.get(Counter.PULL_TOTAL));
            ShadowRig.green("puller content: '" + p.puller.lastRefusal() + "', purged, never pulled again");
        }
    }

    @Test(timeout = 120_000)
    public void aCorruptSourceIsRefusedForTheProcess() {
        corruptSource(Set.of());
        nc("PULL_CONTENT_AS_RETRY", "a corrupt source copy was pulled again",
                () -> corruptSource(Set.of(NegativeControl.PULL_CONTENT_AS_RETRY)));
    }

    // ------------------------------------------------------------------------------------------------ TIMEOUT

    @Test(timeout = 180_000)
    public void aReplicationThatOutlivesItsBoundIsStoppedPurgedAndRetried() {
        try (Pair p = new Pair(10)) {
            final long big = p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, 100_000_000, 1, "4m");
            final ShadowAlias alias = aliasOf(p.src, big);
            p.start(p.settings(1_000, 1, false), () -> false, Set.of());
            p.awaitDestinationProgress(16L * 1024 * 1024);
            p.src.stop(); // the destination image stays alive for its liveness timeout: the bound fires first
            await("REFUSED(TIMEOUT)", () -> p.refused("TIMEOUT") >= 1, 20_000);
            assertEquals("no INCOMPLETE: the bound fired first", 0, p.refused("INCOMPLETE"));
            await("the partial copy purged", () -> p.copies(alias).isEmpty(), 30_000);
            p.src.start(false);
            await("retried and pulled", () -> p.get(Counter.PULL_TOTAL) == 1, 60_000);
            assertTrue(p.puller.contentRefused().isEmpty());
            ShadowRig.green("puller timeout: '" + p.puller.lastRefusal() + "', purged, pulled on a retry");
        }
    }

    // ------------------------------------------------------------------------------------------------ leader gate

    static void leaderNeverPulls(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            final AtomicBoolean leader = new AtomicBoolean(true);
            p.start(p.settings(120_000, 1, false), leader::get, off);
            sleep(4 * PERIOD_MS);
            if (p.get(Counter.PULL_TOTAL) != 0 || p.get(Counter.STARTED_TOTAL) != 0) {
                throw new AssertionError("the leader pulled: shadow_pull_total=" + p.get(Counter.PULL_TOTAL)
                        + " while LEADER");
            }
            assertTrue("skipped passes", p.get(Counter.SKIPPED_LEADER_TOTAL) > 0);
            leader.set(false);
            await("pulled after stepping down", () -> p.get(Counter.PULL_TOTAL) == 1, 20_000);
            ShadowRig.green("puller leader gate: 0 pulls while LEADER (" + p.get(Counter.SKIPPED_LEADER_TOTAL)
                    + " skipped passes), pulled after the role change");
        }
    }

    @Test(timeout = 120_000)
    public void theLeaderNeverPulls() {
        leaderNeverPulls(Set.of());
        nc("LEADER_GATE_OFF", "the leader pulled", () -> leaderNeverPulls(Set.of(NegativeControl.LEADER_GATE_OFF)));
    }

    static void leaderAbortsInflight(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            final long big = p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, BIG, 1, "4m");
            final ShadowAlias alias = aliasOf(p.src, big);
            final AtomicBoolean leader = new AtomicBoolean(false);
            p.start(p.settings(120_000, 1, false), leader::get, off);
            await("a pull in flight", () -> p.get(Counter.INFLIGHT) == 1, 20_000);
            leader.set(true);
            sleep(3_000);
            if (p.get(Counter.PULL_TOTAL) != 0) {
                throw new AssertionError("pull_total moved while LEADER: " + p.get(Counter.PULL_TOTAL));
            }
            assertEquals(1, p.get(Counter.ABORTED_LEADER_TOTAL));
            await("the aborted partial purged", () -> p.copies(alias).isEmpty(), 20_000);
            leader.set(false);
            await("pulled after stepping down", () -> p.get(Counter.PULL_TOTAL) == 1, 60_000);
            ShadowRig.green("puller abort on leadership: aborted 1, purged, pulled after the role change");
        }
    }

    @Test(timeout = 240_000)
    public void anInflightPullIsAbortedWhenTheNodeBecomesLeader() {
        leaderAbortsInflight(Set.of());
        nc("LEADER_FINISHES_INFLIGHT", "pull_total moved while LEADER",
                () -> leaderAbortsInflight(Set.of(NegativeControl.LEADER_FINISHES_INFLIGHT)));
    }

    // ------------------------------------------------------------------------------------------------ dedupe

    static void seqReuse(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            p.dst.write(R3, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k"); // the wiped producer's old (R3, s1)
            final long reused = p.src.write(R3, 1, ShadowKind.ALIGNED, 2_000, SMALL, 2, "64k");
            p.start(p.settings(120_000, 1, false), () -> false, off);
            try {
                await("pulled", () -> p.get(Counter.PULL_TOTAL) == 1, 6_000);
            } catch (final AssertionError notPulled) {
                throw new AssertionError("a reused seq hid a new shadow: " + notPulled.getMessage());
            }
            assertEquals(ShadowEntry.State.VERIFIED, p.copies(aliasOf(p.src, reused)).get(0).state());
            ShadowRig.green("puller dedupe: (R3, s1) reused at a higher position was pulled");
        }
    }

    @Test(timeout = 120_000)
    public void aReusedSeqAtANewPositionIsPulled() {
        seqReuse(Set.of());
        nc("DEDUPE_BY_SEQ", "a reused seq hid a new shadow", () -> seqReuse(Set.of(NegativeControl.DEDUPE_BY_SEQ)));
    }

    static void partialHidesNothing(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            final byte[] body = payload(6);
            final ShadowHeader h = header(R3, 1, ShadowKind.ALIGNED, 1_000, GENESIS, body);
            p.dst.raw(h, body, false, -1, true); // a crashed copy with the same alias (evidence, NO_TRAILER)
            p.src.raw(h, body, true, -1, true);
            assertEquals(ShadowEntry.State.NO_TRAILER, p.copies(h.alias()).get(0).state());
            p.start(p.settings(120_000, 1, false), () -> false, off);
            try {
                await("pulled", () -> p.get(Counter.PULL_TOTAL) == 1, 6_000);
            } catch (final AssertionError notPulled) {
                throw new AssertionError("a partial copy hid its source: " + notPulled.getMessage());
            }
            ShadowRig.green("puller dedupe: a NO_TRAILER local copy with the same alias did not hide the source");
        }
    }

    @Test(timeout = 120_000)
    public void aPartialLocalCopyNeverHidesItsSource() {
        partialHidesNothing(Set.of());
        nc("DEDUPE_AGAINST_EVIDENCE", "a partial copy hid its source",
                () -> partialHidesNothing(Set.of(NegativeControl.DEDUPE_AGAINST_EVIDENCE)));
    }

    // ------------------------------------------------------------------------------------------------ witnesses

    static final ShadowProducer F2 = new ShadowProducer(0, ProducerKind.FOLLOWER, 2);

    /**
     * A second producer's ALIGNED at a position this node already holds VERIFIED (one producer there) is pulled: the
     * second witness of the two-producer anchor. A third producer at that position and an older position never held
     * are not candidates.
     */
    static void secondWitness(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            final long first = p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            p.start(p.settings(120_000, 1, false), () -> false, off);
            await("the first producer's copy pulled", () -> p.get(Counter.PULL_TOTAL) == 1, 20_000);
            assertEquals(ShadowEntry.State.VERIFIED, p.copies(aliasOf(p.src, first)).get(0).state());
            final long second = p.src.write(F1, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            try {
                await("the second producer's copy pulled", () -> p.get(Counter.PULL_TOTAL) == 2, 6_000);
            } catch (final AssertionError notPulled) {
                throw new AssertionError("a second producer's copy at a held position was never pulled: "
                        + notPulled.getMessage());
            }
            assertEquals(ShadowEntry.State.VERIFIED, p.copies(aliasOf(p.src, second)).get(0).state());
            final long third = p.src.write(F2, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            p.src.write(F1, 2, ShadowKind.ALIGNED, 500, SMALL, 5, "64k");
            sleep(3 * PERIOD_MS + 500);
            assertEquals("nothing more at a witnessed position, nothing below it", 2,
                    p.get(Counter.STARTED_TOTAL));
            assertTrue("a third producer was pulled", p.copies(aliasOf(p.src, third)).isEmpty());
            ShadowRig.green("puller second witness: F1 pulled at the held R3 position 1000; F2 there and an older"
                    + " position not candidates");
        }
    }

    @Test(timeout = 120_000)
    public void aSecondProducerAtAHeldPositionIsPulledOnce() {
        secondWitness(Set.of());
        nc("WITNESS_PULL_OFF", "a second producer's copy at a held position was never pulled",
                () -> secondWitness(Set.of(NegativeControl.WITNESS_PULL_OFF)));
    }

    /** The completion rule alone: SYNC, or a stopped local copy of the source's length; nothing else. */
    @Test
    public void aCopyIsCompleteOnSyncOrEqualLengthsOnly() {
        assertTrue("SYNC", ShadowPuller.completeCopy(true, -1, 4_096));
        assertTrue("equal lengths without SYNC", ShadowPuller.completeCopy(false, 4_096, 4_096));
        assertFalse("short without SYNC", ShadowPuller.completeCopy(false, 4_095, 4_096));
        assertFalse("longer without SYNC", ShadowPuller.completeCopy(false, 4_097, 4_096));
        assertFalse("not stopped yet", ShadowPuller.completeCopy(false, -1, 4_096));
        assertFalse("source length unknown", ShadowPuller.completeCopy(false, 4_096, -1));
        ShadowRig.green("puller completion rule: SYNC or equal lengths only");
    }

    // ------------------------------------------------------------------------------------------------ CONFLICT

    @Test(timeout = 120_000)
    public void twoProducersDisagreeingIsAConflictThatIsKept() {
        try (Pair p = new Pair(10)) {
            final byte[] mine = payload(7);
            final byte[] theirs = payload(8);
            p.dst.raw(header(F1, 1, ShadowKind.ALIGNED, 1_000, GENESIS, mine), mine, true, -1, true);
            final ShadowHeader h = header(R3, 1, ShadowKind.ALIGNED, 1_000, GENESIS, theirs);
            p.src.raw(h, theirs, true, -1, true);
            p.start(p.settings(120_000, 1, false), () -> false, Set.of());
            await("the CONFLICT kept", () -> !p.puller.conflictKept().isEmpty(), 20_000);
            sleep(3 * PERIOD_MS + 500);
            assertEquals("never pulled again", 1, p.get(Counter.STARTED_TOTAL));
            final List<ShadowEntry> copies = p.copies(h.alias());
            assertEquals(copies.toString(), 1, copies.size());
            assertEquals(ShadowEntry.State.CONFLICT, copies.get(0).state());
            assertEquals(0, p.get(Counter.PULL_TOTAL));
            assertTrue(p.puller.contentRefused().isEmpty());
            ShadowRig.green("puller conflict: kept " + copies.get(0));
        }
    }

    // ------------------------------------------------------------------------------------------------ pullOnce

    @Test(timeout = 180_000)
    public void theStartupPullReportsWhatItTried() throws Exception {
        try (Pair p = new Pair(10)) {
            final ShadowPuller disabled = p.puller(ShadowPullSettings.disabled(), GENESIS, () -> false, Set.of());
            assertEquals("disabled", disabled.pullOnce(a -> 0, System.nanoTime()).text());
            disabled.close();

            // a target: the newest ALIGNED at or above 1500 (genesis unknown, a bootstrap)
            p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            p.src.write(R3, 2, ShadowKind.ALIGNED, 2_000, SMALL, 2, "64k");
            try (ShadowPuller once = p.puller(p.settings(120_000, 1, false), 0, () -> false, Set.of())) {
                final ShadowPuller.Report r = once.pullOnce(a -> a.logPosition() >= 1_500 ? 0 : -1,
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
                assertEquals(r.text(), ShadowPuller.Report.Kind.VERIFIED, r.kind());
                assertEquals(2_000, r.pulled().header().logPosition());
                assertEquals("tried 1 sources, 1 candidates, refused 0, last: VERIFIED", r.text());
            }

            // two geneses across the candidates: nothing pulled
            final byte[] other = payload(9);
            p.src.raw(header(R3, 3, ShadowKind.ALIGNED, 3_000, 777, other), other, true, -1, true);
            try (ShadowPuller once = p.puller(p.settings(120_000, 1, false), 0, () -> false, Set.of())) {
                final ShadowPuller.Report r = once.pullOnce(a -> 0, System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(10));
                assertEquals(r.toString(), ShadowPuller.Report.Kind.GENESES, r.kind());
                assertEquals(Set.of(GENESIS, 777L), r.geneses());
            }

            // the deadline is the one bound: an in-flight copy is stopped and reported as TIMEOUT
            final long big = p.src.write(R3, 4, ShadowKind.ALIGNED, 4_000, BIG, 3, "4m");
            final ShadowAlias bigAlias = aliasOf(p.src, big);
            final AtomicReference<ShadowPuller.Report> result = new AtomicReference<>();
            final ShadowPuller once = p.puller(p.settings(120_000, 1, false), GENESIS, () -> false, Set.of());
            final Thread t = new Thread(() -> result.set(once.pullOnce(a -> a.logPosition() == 4_000 ? 0 : -1,
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(4))));
            t.start();
            p.awaitDestinationProgress(16L * 1024 * 1024);
            p.src.stop();
            t.join(30_000);
            once.close();
            assertNotNull(result.get());
            assertEquals("tried 1 sources, 1 candidates, refused 0, last: TIMEOUT", result.get().text());
            await("the stopped copy purged", () -> p.copies(bigAlias).isEmpty(), 20_000);
            ShadowRig.green("puller startup pull: VERIFIED by target, GENESES refused, deadline TIMEOUT '"
                    + result.get().text() + "'");
        }
    }

    // ------------------------------------------------------------------------------------------- leader source

    /** The source is the current leader's archive: not listed, counted; listed and pulled once the leader changed. */
    static void leaderSourceSkipped(final Set<NegativeControl> off) {
        try (Pair p = new Pair(10)) {
            p.src.write(R3, 1, ShadowKind.ALIGNED, 1_000, SMALL, 1, "64k");
            final AtomicReference<String> leaderArchive = new AtomicReference<>("localhost:" + p.src.controlPort);
            p.puller = p.puller(p.settings(120_000, 1, false), GENESIS, () -> false, off);
            p.puller.deferredSource(leaderArchive::get);
            p.puller.start();
            await("discovery passes with the leader source deferred",
                    () -> p.get(Counter.SOURCE_SKIPPED_LEADER_TOTAL) >= 2 || p.get(Counter.STARTED_TOTAL) > 0, 20_000);
            sleep(2 * PERIOD_MS);
            if (p.get(Counter.STARTED_TOTAL) != 0 || p.get(Counter.PULL_TOTAL) != 0) {
                throw new AssertionError("a pull was started from the leader's archive: started="
                        + p.get(Counter.STARTED_TOTAL) + " pulled=" + p.get(Counter.PULL_TOTAL));
            }
            assertTrue("skips counted", p.get(Counter.SOURCE_SKIPPED_LEADER_TOTAL) >= 2);
            assertEquals("the leader gate is a different rule", 0, p.get(Counter.SKIPPED_LEADER_TOTAL));
            leaderArchive.set(null);
            await("pulled once the source is no longer the leader's", () -> p.get(Counter.PULL_TOTAL) == 1, 20_000);
            final long skipped = p.get(Counter.SOURCE_SKIPPED_LEADER_TOTAL);
            leaderArchive.set("localhost:1"); // another member leads: the source is listed
            sleep(3 * PERIOD_MS);
            assertEquals("a different leader endpoint skips nothing", skipped,
                    p.get(Counter.SOURCE_SKIPPED_LEADER_TOTAL));
            ShadowRig.green("puller leader source: skipped " + skipped + " passes while the source was the leader's"
                    + " archive, 0 pulls, pulled after the leader changed");
        }
    }

    @Test(timeout = 120_000)
    public void theLeadersArchiveIsNeverASource() {
        leaderSourceSkipped(Set.of());
        nc("LEADER_SOURCE_SKIP_OFF", "a pull was started from the leader's archive",
                () -> leaderSourceSkipped(Set.of(NegativeControl.LEADER_SOURCE_SKIP_OFF)));
    }
}
