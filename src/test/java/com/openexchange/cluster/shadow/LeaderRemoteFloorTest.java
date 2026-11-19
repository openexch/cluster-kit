// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.shadow.ShadowCatalog.Scan;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.openexchange.cluster.shadow.FloorRig.P_1;
import static com.openexchange.cluster.shadow.FloorRig.P_2;
import static com.openexchange.cluster.shadow.FloorRig.P_A;
import static com.openexchange.cluster.shadow.FloorRig.P_CM;
import static com.openexchange.cluster.shadow.FloorRig.SAFE_SEQ;
import static com.openexchange.cluster.shadow.ShadowRig.ENGINE;
import static com.openexchange.cluster.shadow.ShadowRig.F1;
import static com.openexchange.cluster.shadow.ShadowRig.GENESIS;
import static com.openexchange.cluster.shadow.ShadowRig.R3;
import static com.openexchange.cluster.shadow.ShadowRig.nc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The leader's floor behind shadows the other members hold (the one relaxation of rule 12.7, for the current leader
 * only). Three real archives: a FOLLOWER archive holding shadows (a {@link FloorRig}: its {@link FloorAttestor}
 * publishes the marker from its own verified scan), and a LEADER archive holding a log, a stub group at P_cm and no
 * shadow at all (a second {@link FloorRig}), whose {@link RemoteFloor} lists the follower's archive over its UDP
 * control channel. The leader's pruner pass ({@code ArchiveHousekeeping.purgeBelow}) really deletes the log below the
 * floor. Each guard has its negative control, and each control turns its own assertion RED with the named text (C6).
 */
public class LeaderRemoteFloorTest {

    private static final int LEADER_ID = 0;
    private static final int FOLLOWER_ID = 1;
    private static final long PERIOD_MS = 200;
    private static final long MAX_AGE_MS = 60_000;

    private static FloorAttestation att(final int member, final long anchor, final int witnesses, final long margin) {
        return new FloorAttestation(ENGINE, 0, GENESIS, member, anchor, witnesses, margin, 1);
    }

    private static RemoteFloor.Result eval(final ShadowFloor.Margin margin, final ShadowFloor.AnchorRule rule,
                                           final int holders, final Set<NegativeControl> off,
                                           final FloorAttestation... view) {
        return RemoteFloor.evaluate(P_CM, margin, rule, holders, LEADER_ID, 0, GENESIS, ENGINE, List.of(view),
                off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off));
    }

    private static RemoteFloor.Result eval(final Set<NegativeControl> off, final FloorAttestation... view) {
        return eval(ShadowFloor.Margin.PREVIOUS_VERIFIED, ShadowFloor.AnchorRule.TWO_PRODUCERS, 1, off, view);
    }

    // ------------------------------------------------------------------------------------------- the table (pure)

    @Test
    public void theTableOverAttestations() {
        // two producers at P_cm, a margin shadow below: the floor is the margin position
        RemoteFloor.Result r = eval(Set.of(), att(1, P_CM, 2, P_2));
        assertEquals(r.toString(), RemoteFloor.Result.Kind.FLOOR, r.kind());
        assertEquals(P_2, r.floor());
        // margin none: the anchor itself
        assertEquals(P_CM, eval(ShadowFloor.Margin.NONE, ShadowFloor.AnchorRule.TWO_PRODUCERS, 1, Set.of(),
                att(1, P_CM, 2, P_2)).floor());
        // an anchor below P_cm (the attestation lags the newest snapshot) still restores by TAIL: floor at its margin
        assertEquals(P_1, eval(Set.of(), att(1, P_2, 2, P_1)).floor());
        // no margin shadow: nothing
        r = eval(Set.of(), att(1, P_CM, 2, -1));
        assertEquals(RemoteFloor.Result.Kind.TOO_FEW_HOLDERS, r.kind());
        assertEquals(0, r.floor());
        // the best of two members by default; with holders=2 the floor both attest
        assertEquals(P_2, eval(Set.of(), att(1, P_CM, 2, P_2), att(2, P_CM, 2, P_1)).floor());
        assertEquals(P_1, eval(ShadowFloor.Margin.PREVIOUS_VERIFIED, ShadowFloor.AnchorRule.TWO_PRODUCERS, 2,
                Set.of(), att(1, P_CM, 2, P_2), att(2, P_CM, 2, P_1)).floor());
        assertEquals("holders=2 with one member attesting", 0, eval(ShadowFloor.Margin.PREVIOUS_VERIFIED,
                ShadowFloor.AnchorRule.TWO_PRODUCERS, 2, Set.of(), att(1, P_CM, 2, P_2)).floor());
        // any-verified: one producer is enough
        assertEquals(P_2, eval(ShadowFloor.Margin.PREVIOUS_VERIFIED, ShadowFloor.AnchorRule.ANY_VERIFIED, 1,
                Set.of(), att(1, P_CM, 1, P_2)).floor());
        assertEquals(RemoteFloor.Result.Kind.NO_ATTESTATION, eval(Set.of()).kind());
        ShadowRig.green("leader remote floor: the table");
    }

    private static void assertNoFloor(final String named, final RemoteFloor.Result r) {
        if (r.floor() > 0) {
            throw new AssertionError(named + ": remote floor " + r.floor() + " from " + r);
        }
    }

    @Test
    public void oneProducerNeverAnchorsTheLeader() {
        final String named = "one producer's remote shadow anchored the leader's purge";
        final RemoteFloor.Result r = eval(Set.of(), att(1, P_CM, 1, P_2));
        assertEquals(RemoteFloor.Result.Kind.NOT_ACCEPTED, r.kind());
        assertNoFloor(named, r);
        nc("REMOTE_FLOOR_ONE_WITNESS", named,
                () -> assertNoFloor(named, eval(Set.of(NegativeControl.REMOTE_FLOOR_ONE_WITNESS), att(1, P_CM, 1, P_2))));
    }

    @Test
    public void theMarginHoldsTheLeaderFloor() {
        final String named = "the remote floor ignored the margin";
        final Runnable[] check = new Runnable[1];
        final Set<NegativeControl> off = EnumSet.noneOf(NegativeControl.class);
        check[0] = () -> {
            final RemoteFloor.Result r = eval(off, att(1, P_CM, 2, P_2));
            if (r.floor() != P_2) {
                throw new AssertionError(named + ": floor " + r.floor() + ", margin shadow at " + P_2);
            }
            assertNoFloor(named, eval(off, att(1, P_CM, 2, -1)));
        };
        check[0].run();
        off.add(NegativeControl.REMOTE_FLOOR_MARGIN_IGNORED);
        nc("REMOTE_FLOOR_MARGIN_IGNORED", named, check[0]);
    }

    @Test
    public void anAnchorAbovePcmIsNotAccepted() {
        final String named = "a remote anchor above P_cm moved the leader's floor";
        assertNoFloor(named, eval(Set.of(), att(1, P_A, 2, P_2)));
        nc("REMOTE_FLOOR_ANCHOR_ABOVE_PCM", named, () -> assertNoFloor(named,
                eval(Set.of(NegativeControl.REMOTE_FLOOR_ANCHOR_ABOVE_PCM), att(1, P_A, 2, P_2))));
    }

    @Test
    public void foreignMarkersNeverCount() {
        final String named = "a foreign attestation moved the leader's floor";
        final FloorAttestation otherGenesis = new FloorAttestation(ENGINE, 0, GENESIS + 1, 1, P_CM, 2, P_2, 1);
        final FloorAttestation otherCluster = new FloorAttestation(ENGINE, 7, GENESIS, 1, P_CM, 2, P_2, 1);
        final FloorAttestation self = att(LEADER_ID, P_CM, 2, P_2);
        for (final FloorAttestation a : List.of(otherGenesis, otherCluster, self)) {
            assertNoFloor(named, eval(Set.of(), a));
            nc("REMOTE_FLOOR_FOREIGN_ACCEPTED " + a, named, () -> assertNoFloor(named,
                    eval(Set.of(NegativeControl.REMOTE_FLOOR_FOREIGN_ACCEPTED), a)));
        }
    }

    @Test
    public void markerAliasRoundTripAndRefusals() {
        final FloorAttestation a = new FloorAttestation(ENGINE, 3, GENESIS, 2, 22_846_528L, 2, 11_000_000L, 9);
        assertEquals(a, FloorAttestation.tryParse(a.channel()));
        final FloorAttestation none = new FloorAttestation(ENGINE, 0, GENESIS, 0, 0, 1, -1, 1);
        assertEquals(none, FloorAttestation.tryParse(none.channel()));
        assertTrue(a.channel(), a.channel().contains(FloorAttestation.prefix(ENGINE)));
        final String ok = a.format();
        for (final String bad : new String[] {
            ok.replace(".w2.", ".w0."), ok.replace(".w2.", ".w02."), ok.replace(".w2.", ".w-2."),
            ok.replace(".w2.", "."), ok + ".x1", ok.replace(".a22846528.", ".a1."), ok.replace(".g", ".g0"),
            ok.replace("oxf.v1.", "oxs.v1."), ok.replace(".s9", ".s0"), ok.replace(".m2.", ".m."),
            ok.replace(".a22846528.", ".a99999999999999999999999.")}) {
            assertNull(bad, FloorAttestation.tryParse("aeron:ipc?term-length=64k|alias=" + bad));
        }
        assertNull(FloorAttestation.tryParse("aeron:ipc?term-length=64k"));
        assertNull(FloorAttestation.tryParse(null));
        ShadowRig.green("attestation alias: " + ok);
    }

    // ------------------------------------------------------------------------------------- real archives, real purge

    /** The follower: a floor rig whose archive answers on UDP, with its attestor over its own verified scan. */
    private static final class Follower implements AutoCloseable {
        final FloorRig rig;
        final ShadowCatalog catalog;
        final int port;
        final int memberId;

        Follower() {
            this(FOLLOWER_ID);
        }

        Follower(final int memberId) {
            this.memberId = memberId;
            port = PullRig.freeUdpPort();
            rig = new FloorRig(FloorRig.Service.STUB, port);
            catalog = rig.rig.catalog();
        }

        FloorAttestor attestor(final int minWitnesses, final Set<NegativeControl> off) {
            return new FloorAttestor(rig.archive(), ENGINE, 0, GENESIS, memberId, minWitnesses, ShadowRig.CLOCK,
                    ShadowRig.TIMEOUT_NS, rig.hk, off.isEmpty() ? EnumSet.noneOf(NegativeControl.class)
                    : EnumSet.copyOf(off));
        }

        Scan verifiedScan() {
            catalog.verifyAllComplete();
            return catalog.scan();
        }

        List<FloorAttestation> markers() {
            final List<FloorAttestation> out = new ArrayList<>();
            rig.archive().listRecordingsForUri(0, 1000, FloorAttestation.prefix(ENGINE), FloorAttestation.STREAM_ID,
                    (a, b, c, d, e, f, g, h, i, j, k, l, m, n, originalChannel, p) ->
                            out.add(FloorAttestation.tryParse(originalChannel)));
            return out;
        }

        @Override
        public void close() {
            rig.close();
        }
    }

    /** The leader: log, stub group at P_cm, no shadow; its floor with the remote floor wired to the follower. */
    private static final class Leader implements AutoCloseable {
        final FloorRig rig = new FloorRig();
        final AtomicBoolean leads = new AtomicBoolean(true);
        final AtomicLong nowOffsetNs = new AtomicLong();
        RemoteFloor remote;
        ShadowFloor floor;

        Leader wire(final Follower follower, final ShadowFloor.AnchorRule rule, final Set<NegativeControl> off) {
            return wire(java.util.Map.of(follower.port, follower.memberId), 1, rule, off);
        }

        /** @param port the UDP control port of the one source (a port nobody listens on: a source that is down) */
        Leader wire(final int port, final ShadowFloor.AnchorRule rule, final Set<NegativeControl> off) {
            return wire(java.util.Map.of(port, FOLLOWER_ID), 1, rule, off);
        }

        /** @param members UDP control port of each source to the member whose archive it is */
        Leader wire(final java.util.Map<Integer, Integer> members, final int holders,
                    final ShadowFloor.AnchorRule rule, final Set<NegativeControl> off) {
            final Set<NegativeControl> o = off.isEmpty() ? EnumSet.noneOf(NegativeControl.class) : EnumSet.copyOf(off);
            final List<ShadowPullSettings.Endpoint> sources = new ArrayList<>();
            final java.util.Map<String, Integer> memberOf = new java.util.HashMap<>();
            members.forEach((port, member) -> {
                sources.add(new ShadowPullSettings.Endpoint("localhost", port));
                memberOf.put("localhost:" + port, member);
            });
            remote = new RemoteFloor(rig.rig.fixture.aeron(), ENGINE, 0, () -> GENESIS, LEADER_ID, sources, memberOf,
                    "aeron:udp?endpoint=localhost:0", PERIOD_MS, MAX_AGE_MS, holders,
                    () -> ShadowRig.CLOCK.nanoTime() + nowOffsetNs.get(), rig.hk, o);
            floor = rig.floor(ShadowFloor.Margin.PREVIOUS_VERIFIED, rule, o).leaderRemote(leads::get, remote);
            final ShadowCatalog own = rig.rig.catalog();
            own.verifyAllComplete();
            floor.observe(own.scan());
            return this;
        }

        /** One housekeeping step and one purge pass, as the pruner thread runs them. */
        long pass() {
            floor.pollRemote();
            rig.purge(floor);
            return rig.logStart();
        }

        /** Listings until the follower's marker is in the view (the listing is asynchronous to its write). */
        void awaitView() throws InterruptedException {
            awaitView(1);
        }

        /** Polls until every source was listed, whatever it showed. */
        void awaitListed(final int sources) throws InterruptedException {
            final long deadline = System.nanoTime() + ShadowRig.TIMEOUT_NS;
            final long before = rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_LISTINGS_TOTAL);
            while (rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_LISTINGS_TOTAL) < before + 2L * sources) {
                if (System.nanoTime() - deadline > 0) {
                    throw new AssertionError("the leader never listed its " + sources + " source(s)");
                }
                Thread.sleep(PERIOD_MS / 4);
                floor.pollRemote();
            }
        }

        void awaitView(final int markers) throws InterruptedException {
            final long deadline = System.nanoTime() + ShadowRig.TIMEOUT_NS;
            while (remote.view() == null || remote.view().size() < markers) {
                if (System.nanoTime() - deadline > 0) {
                    throw new AssertionError("the leader never listed the follower's attestation");
                }
                Thread.sleep(PERIOD_MS / 4);
                floor.pollRemote();
            }
        }

        @Override
        public void close() {
            remote.close();
            rig.close();
        }
    }

    /** The follower holds F1 and R3 ALIGNED at P_cm with one digest, and one copy at P_2 below. */
    private static void twoProducers(final Follower f) {
        final byte[] image = FloorRig.payload(11);
        f.rig.shadow(P_2, SAFE_SEQ, FloorRig.payload(7), R3, ShadowKind.ALIGNED);
        f.rig.shadow(P_CM, SAFE_SEQ, image, R3, ShadowKind.ALIGNED);
        f.rig.shadow(P_CM, SAFE_SEQ, image, F1, ShadowKind.ALIGNED);
    }

    @Test
    public void theLeaderPurgesBehindTwoRemoteProducersAndNeverBehindOne() throws Exception {
        try (Follower f = new Follower(); Leader l = new Leader()) {
            l.wire(f, ShadowFloor.AnchorRule.TWO_PRODUCERS, Set.of());
            // one producer on the follower: nothing is attested under two-producers, the leader purges nothing
            f.rig.shadow(P_2, SAFE_SEQ, FloorRig.payload(7), R3, ShadowKind.ALIGNED);
            final byte[] image = FloorRig.payload(11);
            f.rig.shadow(P_CM, SAFE_SEQ, image, R3, ShadowKind.ALIGNED);
            final FloorAttestor attestor = f.attestor(2, Set.of());
            attestor.observe(f.verifiedScan());
            assertTrue("one producer: no marker " + f.markers(), f.markers().isEmpty());
            Thread.sleep(2 * PERIOD_MS);
            assertEquals("one remote producer: the leader's log stays whole", 0, l.pass());
            assertEquals(0, l.rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_FLOOR_APPLIED_TOTAL));

            // the second producer arrives later (C2): the follower attests, the leader's next listing sees it
            f.rig.shadow(P_CM, SAFE_SEQ, image, F1, ShadowKind.ALIGNED);
            attestor.observe(f.verifiedScan());
            assertEquals(List.of(new FloorAttestation(ENGINE, 0, GENESIS, FOLLOWER_ID, P_CM, 2, P_2, 1)), f.markers());
            assertEquals(P_CM, f.rig.hk.get(ShadowHousekeeping.Counter.ATTESTED_ANCHOR_POSITION));
            l.awaitView();
            final long start = l.pass();
            assertTrue("the leader's log start moved behind the remote margin shadow: " + start,
                    start > 0 && start <= P_2 && start > P_2 - FloorRig.SEGMENT);
            assertEquals(P_2, l.floor.lastApplied());
            assertEquals(P_2, l.rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_FLOOR_POSITION));
            assertEquals(1, l.rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_HOLDERS));
            assertEquals(1, l.rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_FLOOR_APPLIED_TOTAL));
            assertEquals(P_2, l.rig.hk.get(ShadowHousekeeping.Counter.FLOOR_POSITION));
            // the local table alone still says "nothing": the leader holds no shadow
            assertEquals(ShadowFloor.Evaluation.Kind.NO_COVERING, l.floor.evaluate().kind());
            assertEquals(0, l.floor.evaluate().floor());
            // what a restart of this leader needs is on the follower, at or above the leader's log start
            assertTrue(P_2 >= start);
            ShadowRig.green("leader remote floor: log start " + start + " behind the follower's attested shadows");

            // the attestation is retracted when its position is no longer witnessed (C4), and written again (twice)
            for (int round = 0; round < 2; round++) {
                final Scan none = new Scan(List.of(), List.of(), List.of(), List.of(), 0, java.util.Map.of(), true, 0,
                        null, 0);
                attestor.observe(none);
                assertTrue("retracted: " + f.markers(), f.markers().isEmpty());
                assertEquals(-1, f.rig.hk.get(ShadowHousekeeping.Counter.ATTESTED_ANCHOR_POSITION));
                attestor.observe(f.verifiedScan());
                assertEquals(1, f.markers().size());
                assertEquals(P_CM, f.markers().get(0).anchor());
            }
            assertTrue(f.rig.hk.get(ShadowHousekeeping.Counter.ATTESTATIONS_RETRACTED_TOTAL) >= 2);
        }
    }

    /** The oracle of the role and view guards: a node that must not use remote shadows keeps its whole log. */
    private static void assertLogWhole(final String named, final Leader l) {
        final long start = l.pass();
        if (start > 0) {
            throw new AssertionError(named + ": log start " + start + ", floor " + l.floor.lastApplied());
        }
    }

    private void guarded(final NegativeControl control, final String named, final java.util.function.Consumer<Leader> arrange)
            throws Exception {
        for (final boolean controlOn : new boolean[] {false, true}) {
            try (Follower f = new Follower(); Leader l = new Leader()) {
                twoProducers(f);
                f.attestor(2, Set.of()).observe(f.verifiedScan());
                l.wire(f, ShadowFloor.AnchorRule.TWO_PRODUCERS, controlOn ? Set.of(control) : Set.of());
                l.awaitView();
                arrange.accept(l);
                if (controlOn) {
                    nc(control.name(), named, () -> assertLogWhole(named, l));
                } else {
                    assertLogWhole(named, l);
                }
            }
        }
    }

    @Test
    public void aFollowerNeverPurgesBehindRemoteShadows() throws Exception {
        // a leader change mid-way: the view was listed while leading, the purge pass runs after the role changed
        guarded(NegativeControl.REMOTE_FLOOR_ANY_ROLE, "a follower purged behind shadows it does not hold",
                l -> l.leads.set(false));
    }

    @Test
    public void aStaleViewFeedsNothing() {
        final String named = "the leader purged behind a stale view of the other members";
        for (final boolean controlOn : new boolean[] {false, true}) {
            try (Leader l = new Leader()) {
                // the one source is down (nobody listens on its port): the last view it gave is all there is
                l.wire(PullRig.freeUdpPort(), ShadowFloor.AnchorRule.TWO_PRODUCERS,
                        controlOn ? Set.of(NegativeControl.REMOTE_FLOOR_STALE_VIEW) : Set.of());
                // the wake never waits for it: every step returns at once, the failure is counted when the
                // connect's own deadline (the period) has passed
                long slowestNs = 0;
                final long deadline = System.nanoTime() + ShadowRig.TIMEOUT_NS;
                while (l.rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_SOURCE_UNREACHABLE_TOTAL) < 2) {
                    assertTrue("the down source is never counted", System.nanoTime() - deadline < 0);
                    final long t0 = System.nanoTime();
                    l.floor.pollRemote();
                    slowestNs = Math.max(slowestNs, System.nanoTime() - t0);
                    try {
                        Thread.sleep(10);
                    } catch (final InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
                assertNull("a source that is down gives no view", l.remote.view());
                assertTrue("a source that is down stalled a wake for " + slowestNs / 1_000_000 + " ms (period "
                        + PERIOD_MS + " ms)", slowestNs < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(PERIOD_MS) / 2);
                System.out.println("GREEN leader remote floor: source down, slowest wake step " + slowestNs / 1_000
                        + " us");
                l.remote.accept(List.of(att(FOLLOWER_ID, P_CM, 2, P_2)), ShadowRig.CLOCK.nanoTime());
                assertEquals("a fresh view feeds the floor", P_2, l.floor.effectiveFloor(l.floor.evaluate()));
                l.nowOffsetNs.set(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(MAX_AGE_MS + 1));
                if (controlOn) {
                    nc(NegativeControl.REMOTE_FLOOR_STALE_VIEW.name(), named, () -> assertLogWhole(named, l));
                } else {
                    assertLogWhole(named, l);
                    assertEquals(RemoteFloor.Result.Kind.NO_VIEW, l.floor.remoteResult(l.floor.evaluate()).kind());
                }
            }
        }
    }

    @Test
    public void theLocalPreconditionStillHolds() throws Exception {
        // the leader's own scan cannot answer (startup pass not done): the floor is UNKNOWN, remote shadows or not
        guarded(NegativeControl.REMOTE_FLOOR_PRECONDITION_OFF,
                "the leader purged behind remote shadows while its own floor was unknown",
                l -> l.floor.observe(new Scan(List.of(), List.of(), List.of(), List.of(), 0, java.util.Map.of(),
                        false, 0, null, 0)));
    }

    /** A stopped raw marker recording with any content the alias can state (what no attestor of this archive wrote). */
    private static void rawMarker(final Follower f, final FloorAttestation a, final boolean stop) {
        final com.openexchange.cluster.testkit.RecordedLogFixture.RawRecording rec =
                f.rig.rig.fixture.record(a.channel(), FloorAttestation.STREAM_ID);
        if (stop) {
            rec.stopRecording();
        }
    }

    private void refusedMarker(final NegativeControl control, final String named, final FloorAttestation marker,
                               final boolean stopped) throws Exception {
        for (final boolean controlOn : new boolean[] {false, true}) {
            try (Follower f = new Follower(); Leader l = new Leader()) {
                rawMarker(f, marker, stopped);
                l.wire(f, ShadowFloor.AnchorRule.TWO_PRODUCERS, controlOn ? Set.of(control) : Set.of());
                l.awaitListed(1);
                if (controlOn) {
                    nc(control.name(), named, () -> assertLogWhole(named, l));
                } else {
                    assertTrue("listed, nothing accepted: " + l.remote.view(), l.remote.view().isEmpty());
                    assertLogWhole(named, l);
                }
            }
        }
    }

    @Test
    public void aCopiedPeerMarkerIsRefused() throws Exception {
        // member 1's archive lists a marker that names member 2 (it came with a copied archive directory)
        refusedMarker(NegativeControl.REMOTE_FLOOR_SOURCE_UNBOUND,
                "a marker listed from another member's archive moved the leader's floor", att(2, P_CM, 2, P_2), true);
    }

    @Test
    public void anUnstoppedMarkerIsNoStatement() throws Exception {
        refusedMarker(NegativeControl.REMOTE_FLOOR_ACTIVE_ACCEPTED,
                "a marker that is still being recorded moved the leader's floor", att(FOLLOWER_ID, P_CM, 2, P_2),
                false);
    }

    @Test
    public void anotherEnginesMarkerNeverCounts() throws Exception {
        final String named = "a foreign attestation moved the leader's floor";
        final FloorAttestation core = new FloorAttestation(ShadowHeader.ENGINE_CORE, 0, GENESIS, 1, P_CM, 2, P_2, 1);
        assertNoFloor(named, eval(Set.of(), core));
        nc("REMOTE_FLOOR_FOREIGN_ACCEPTED engine", named, () -> assertNoFloor(named,
                eval(Set.of(NegativeControl.REMOTE_FLOOR_FOREIGN_ACCEPTED), core)));
        // over real archives: the listing asks for this engine's markers only
        try (Follower f = new Follower(); Leader l = new Leader()) {
            rawMarker(f, core, true);
            l.wire(f, ShadowFloor.AnchorRule.TWO_PRODUCERS, Set.of());
            l.awaitListed(1);
            assertTrue(String.valueOf(l.remote.view()), l.remote.view().isEmpty());
            assertLogWhole(named, l);
        }
    }

    @Test
    public void twoHoldersOverRealArchives() throws Exception {
        try (Follower f1 = new Follower(1); Follower f2 = new Follower(2); Leader l = new Leader()) {
            twoProducers(f1);
            f1.attestor(2, Set.of()).observe(f1.verifiedScan());
            l.wire(java.util.Map.of(f1.port, 1, f2.port, 2), 2, ShadowFloor.AnchorRule.TWO_PRODUCERS, Set.of());
            l.awaitView(1);
            l.awaitListed(2);
            assertLogWhole("holders=2 and one member attests", l);
            assertEquals(RemoteFloor.Result.Kind.TOO_FEW_HOLDERS, l.floor.remoteResult(l.floor.evaluate()).kind());
            // the second member attests later (C2); its margin shadow is lower: the floor both attest
            final byte[] image = FloorRig.payload(11);
            f2.rig.shadow(P_1, SAFE_SEQ, FloorRig.payload(5), R3, ShadowKind.ALIGNED);
            f2.rig.shadow(P_CM, SAFE_SEQ, image, R3, ShadowKind.ALIGNED);
            f2.rig.shadow(P_CM, SAFE_SEQ, image, F1, ShadowKind.ALIGNED);
            f2.attestor(2, Set.of()).observe(f2.verifiedScan());
            l.awaitView(2);
            final long start = l.pass();
            assertEquals(P_1, l.floor.lastApplied());
            assertTrue("log start behind the floor both members attest: " + start,
                    start > 0 && start <= P_1 && start > P_1 - FloorRig.SEGMENT);
            assertEquals(2, l.rig.hk.get(ShadowHousekeeping.Counter.LEADER_REMOTE_HOLDERS));
            ShadowRig.green("leader remote floor: holders=2, log start " + start);
        }
    }

    @Test
    public void aMarkerNeverOutlivesTheProcessThatVerifiedIt() {
        final String named = "a marker of an earlier process is still published";
        for (final boolean controlOn : new boolean[] {false, true}) {
            try (Follower f = new Follower()) {
                twoProducers(f);
                f.attestor(2, Set.of()).observe(f.verifiedScan());
                assertEquals(1, f.markers().size());
                // the member restarts: a new attestor, its startup pass not done yet (the scan cannot answer)
                final Scan s = f.catalog.scan();
                final Scan startup = new Scan(s.all(), s.usable(), s.verified(), s.conflicts(), 0, s.counts(), false,
                        0, null, 0);
                final FloorAttestor restarted = f.attestor(2, controlOn ? Set.of(NegativeControl.ATTEST_KEEPS_STALE)
                        : Set.of());
                final Runnable check = () -> {
                    restarted.observe(startup);
                    if (!f.markers().isEmpty()) {
                        throw new AssertionError(named + ": " + f.markers());
                    }
                };
                if (controlOn) {
                    nc("ATTEST_KEEPS_STALE", named, check);
                    continue;
                }
                check.run();
                // once its own startup pass verified the copies it attests again, with a fresh marker
                restarted.observe(f.verifiedScan());
                assertEquals(1, f.markers().size());
                // a host that builds no attestor (the remote floor switched off) retracts what an earlier one left
                assertEquals(1, FloorAttestor.retractAll(f.rig.archive(), ENGINE));
                assertTrue(f.markers().isEmpty());
                assertEquals(0, FloorAttestor.retractAll(f.rig.archive(), ENGINE));
                // a marker still being recorded is left alone, and no attestor step fails on it
                rawMarker(f, att(FOLLOWER_ID, P_CM, 2, P_2), false);
                assertEquals(0, FloorAttestor.retractAll(f.rig.archive(), ENGINE));
                final long errors = f.rig.hk.get(ShadowHousekeeping.Counter.ATTEST_ERRORS_TOTAL);
                f.attestor(2, Set.of()).observe(f.verifiedScan());
                assertEquals(errors, f.rig.hk.get(ShadowHousekeeping.Counter.ATTEST_ERRORS_TOTAL));
                assertEquals(2, f.markers().size());
                ShadowRig.green("attestor: stale markers retracted at arming and by retractAll");
            }
        }
    }

    @Test
    public void theAttestorReadsVerifiedCopiesOfAKnownScanOnly() {
        try (Follower f = new Follower()) {
            twoProducers(f);
            final String unverified = "attested copies no deep read verified";
            final Runnable[] check = new Runnable[1];
            final Set<NegativeControl> off = EnumSet.noneOf(NegativeControl.class);
            // COMPLETE, not yet VERIFIED (no deep read ran): nothing may be attested
            check[0] = () -> {
                final Scan complete = f.catalog.scan();
                assertTrue(complete.verified().isEmpty());
                assertFalse(complete.usable().isEmpty());
                final Scan known = new Scan(complete.all(), complete.usable(), complete.verified(),
                        complete.conflicts(), 0, complete.counts(), true, 0, null, 0);
                f.attestor(2, off).observe(known);
                if (!f.markers().isEmpty()) {
                    throw new AssertionError(unverified + ": " + f.markers());
                }
            };
            check[0].run();
            off.add(NegativeControl.ATTEST_READS_USABLE);
            nc("ATTEST_READS_USABLE", unverified, check[0]);
            f.attestor(2, Set.of()).observe(new Scan(List.of(), List.of(), List.of(), List.of(), 0,
                    java.util.Map.of(), true, 0, null, 0));
            assertTrue(f.markers().isEmpty());

            // VERIFIED, but the scan cannot answer (an error entry): nothing may be attested
            final String unknown = "attested from a scan that cannot answer";
            off.clear();
            f.catalog.verifyAllComplete();
            check[0] = () -> {
                final Scan s = f.catalog.scan();
                assertFalse(s.verified().isEmpty());
                final Scan errors = new Scan(s.all(), s.usable(), s.verified(), s.conflicts(), 1, s.counts(), true, 0,
                        null, 0);
                assertFalse(errors.known());
                f.attestor(2, off).observe(errors);
                if (!f.markers().isEmpty()) {
                    throw new AssertionError(unknown + ": " + f.markers());
                }
            };
            check[0].run();
            off.add(NegativeControl.ATTEST_UNKNOWN_SCAN);
            nc("ATTEST_UNKNOWN_SCAN", unknown, check[0]);

            // a CONFLICT at the position (a third copy with another digest): never attested, and a marker is retracted
            final FloorAttestor attestor = f.attestor(2, Set.of());
            attestor.observe(f.verifiedScan());
            assertEquals(1, f.markers().size());
            f.rig.shadow(P_CM, SAFE_SEQ, FloorRig.payload(99), new ShadowProducer(0, ProducerKind.FOLLOWER, 2),
                    ShadowKind.ALIGNED);
            final Scan conflict = f.verifiedScan();
            assertEquals(1, conflict.conflicts().size());
            attestor.observe(conflict);
            assertNotNull(f.markers());
            assertTrue("a CONFLICT position stays attested: " + f.markers(), f.markers().isEmpty());
            ShadowRig.green("attestor: VERIFIED copies of a known scan only, CONFLICT retracts");
        }
    }
}
