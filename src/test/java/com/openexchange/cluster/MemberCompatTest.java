// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.VersionValidator;
import org.agrona.SemanticVersion;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntBinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * S4 T1: the member version, its validator and the {@code member-compat.seen} file.
 */
public class MemberCompatTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final MemberCompat OWN = MemberCompat.of(1, 300);

    @Test
    public void composesTheAeronAppVersionAndPrintsGenerationDotLevel() {
        assertEquals(SemanticVersion.compose(1, 1, 44), OWN.appVersion());
        assertEquals("g1.300", OWN.toString());
        assertEquals(OWN, MemberCompat.fromAppVersion(OWN.appVersion()));
        assertEquals("g1.65535", MemberCompat.fromAppVersion(MemberCompat.of(1, 65_535).appVersion()).toString());
        assertEquals("g255.0", MemberCompat.fromAppVersion(MemberCompat.of(255, 0).appVersion()).toString());
        // every pre-S4 build carries Aeron's default 0.0.1: generation 0
        assertEquals("g0.1", MemberCompat.fromAppVersion(SemanticVersion.compose(0, 0, 1)).toString());
    }

    @Test
    public void aGenerationOrLevelOutOfRangeIsABuildTimeError() {
        assertThrows(IllegalArgumentException.class, () -> MemberCompat.of(0, 1));
        assertThrows(IllegalArgumentException.class, () -> MemberCompat.of(256, 1));
        assertThrows(IllegalArgumentException.class, () -> MemberCompat.of(1, -1));
        assertThrows(IllegalArgumentException.class, () -> MemberCompat.of(1, 65_536));
    }

    /** The validator table; {@code v} is the validator under test (ours, or a negative control). */
    private static void assertTable(final VersionValidator v) {
        final int own = OWN.appVersion();
        assertTrue("equal", v.isVersionCompatible(own, MemberCompat.of(1, 300).appVersion()));
        assertTrue("+1", v.isVersionCompatible(own, MemberCompat.of(1, 301).appVersion()));
        assertTrue("-1", v.isVersionCompatible(own, MemberCompat.of(1, 299).appVersion()));
        assertFalse("+2 must be refused", v.isVersionCompatible(own, MemberCompat.of(1, 302).appVersion()));
        assertFalse("-2 must be refused", v.isVersionCompatible(own, MemberCompat.of(1, 298).appVersion()));
        assertFalse("other generation", v.isVersionCompatible(own, MemberCompat.of(2, 300).appVersion()));
        assertFalse("pre-S4 0.0.1", v.isVersionCompatible(own, SemanticVersion.compose(0, 0, 1)));
    }

    @Test
    public void theValidatorAcceptsOneLevelEitherWayOfOneGeneration() {
        assertTable(MemberCompat.validator(OWN, (a, b) -> { }));
    }

    /** Negative control: a validator with the window widened to 2 fails the +2 row by name. */
    @Test
    public void negativeControlAWindowOfTwoFailsThePlusTwoRow() {
        final IntBinaryOperator widened = (own, other) -> {
            final MemberCompat a = MemberCompat.fromAppVersion(own);
            final MemberCompat b = MemberCompat.fromAppVersion(other);
            return a.generation() == b.generation() && Math.abs(a.level() - b.level()) <= 2 ? 1 : 0;
        };
        final AssertionError e = assertThrows(AssertionError.class,
                () -> assertTable((own, other) -> widened.applyAsInt(own, other) == 1));
        assertEquals("+2 must be refused", e.getMessage());
    }

    /** Negative control: Aeron's default validator (major only) fails the +2 row too. */
    @Test
    public void negativeControlAeronsDefaultMajorOnlyValidatorFailsThePlusTwoRow() {
        final VersionValidator aeronDefault = io.aeron.cluster.AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
        final AssertionError e = assertThrows(AssertionError.class, () -> assertTable(aeronDefault));
        assertEquals("+2 must be refused", e.getMessage());
    }

    @Test
    public void onRefusalIsCalledOnceBeforeFalseIsReturned() {
        final List<String> calls = new ArrayList<>();
        final boolean[] returned = {false};
        final VersionValidator v = MemberCompat.validator(OWN, (own, other) -> {
            assertFalse("onRefusal runs before the validator returns", returned[0]);
            calls.add(own + "->" + other);
        });
        final boolean result = v.isVersionCompatible(OWN.appVersion(), MemberCompat.of(1, 302).appVersion());
        returned[0] = true;
        assertFalse(result);
        assertEquals(List.of("g1.300->g1.302"), calls);
        assertTrue(v.isVersionCompatible(OWN.appVersion(), MemberCompat.of(1, 301).appVersion()));
        assertEquals("an accepted version calls nothing", 1, calls.size());
    }

    @Test
    public void theRefusalTextIsOneLineNamingBothVersionsTheBuildAndTheRecovery() {
        assertEquals("member version incompatible: this build is g1.300 (build abc1234), the cluster's leader, log or"
                        + " snapshot is g1.302; a cluster runs level N and N-1 of one generation; install a compatible"
                        + " build, a restart cannot fix this",
                MemberCompat.refusalText(OWN, "abc1234", MemberCompat.of(1, 302)));
    }

    @Test
    public void theLifecycleRefusalStopsWithExit78AndTheAeronFatalThatFollowsDoesNotTurnItInto1() {
        final List<String> log = new ArrayList<>();
        final int[] halted = {-1};
        final EngineLifecycle lifecycle = new EngineLifecycle("Test node", log::add, s -> halted[0] = s, 5_000, 50);
        final VersionValidator v = MemberCompat.validator(OWN,
                MemberCompat.fatalOnRefusal(lifecycle, new BuildInfo("1.0", "abc1234")));
        assertFalse(v.isVersionCompatible(OWN.appVersion(), MemberCompat.of(1, 302).appVersion()));
        // what Aeron does next: a FATAL error through the lifecycle's handler
        lifecycle.onError(new io.aeron.cluster.client.ClusterException("incompatible version",
                io.aeron.exceptions.AeronException.Category.FATAL), true);
        lifecycle.finish();
        assertEquals(78, halted[0]);
        assertEquals("Test node FATAL (exit 78): "
                + MemberCompat.refusalText(OWN, "abc1234", MemberCompat.of(1, 302))
                + " - node exiting, backstop in 50 ms", log.get(0));
    }

    // ------------------------------------------------------------------ the seen file

    @Test
    public void theSeenFileRoundTripsAndCarriesItsDigest() throws Exception {
        final File dir = tmp.newFolder("cluster");
        MemberCompat.writeSeen(dir, new MemberCompat.Seen(1, 299, 301, 300));
        final String text = Files.readString(MemberCompat.seenFile(dir));
        assertTrue(text, text.startsWith("format=member-compat-seen/2\ngeneration=1\nlevel_min=299\nlevel_max=301\nlast_own_level=300\n"
                + "sha256="));
        assertEquals(new MemberCompat.Seen(1, 299, 301, 300), MemberCompat.readSeenOrNull(dir));
        assertNull(MemberCompat.readSeenOrNull(tmp.newFolder("empty")));
    }

    @Test
    public void anEditedOrTornSeenFileIsRefusedNamingTheFile() throws Exception {
        final File dir = tmp.newFolder("cluster");
        MemberCompat.writeSeen(dir, new MemberCompat.Seen(1, 300, 300, 300));
        final String good = Files.readString(MemberCompat.seenFile(dir));
        Files.writeString(MemberCompat.seenFile(dir), good.replace("level_max=300", "level_max=302"));
        final String refusal = MemberCompat.checkBeforeLaunch(dir, OWN, "abc1234");
        assertNotNull(refusal);
        assertTrue(refusal, refusal.contains(MemberCompat.seenFile(dir).toString()));
        assertTrue(refusal, refusal.contains("sha256 mismatch"));
        Files.writeString(MemberCompat.seenFile(dir), good.substring(0, 30));
        final String torn = MemberCompat.checkBeforeLaunch(dir, OWN, "abc1234");
        assertTrue(torn, torn.contains("is unreadable"));
    }

    @Test
    public void theValidatorWidensTheFileOnlyWhenItAcceptsAVersionOutsideTheRange() throws Exception {
        final File dir = tmp.newFolder("cluster");
        final VersionValidator v = MemberCompat.validator(OWN, (a, b) -> { }, dir);
        assertTrue(v.isVersionCompatible(OWN.appVersion(), OWN.appVersion()));
        assertEquals(new MemberCompat.Seen(1, 300, 300, 300), MemberCompat.readSeenOrNull(dir));
        assertTrue(v.isVersionCompatible(OWN.appVersion(), MemberCompat.of(1, 301).appVersion()));
        assertEquals(new MemberCompat.Seen(1, 300, 301, 300), MemberCompat.readSeenOrNull(dir));
        final long mtime = Files.getLastModifiedTime(MemberCompat.seenFile(dir)).toMillis();
        Files.setLastModifiedTime(MemberCompat.seenFile(dir), java.nio.file.attribute.FileTime.fromMillis(1_000));
        assertTrue(v.isVersionCompatible(OWN.appVersion(), MemberCompat.of(1, 300).appVersion()));
        assertEquals("inside the range: not rewritten", 1_000,
                Files.getLastModifiedTime(MemberCompat.seenFile(dir)).toMillis());
        assertTrue(mtime > 0);
        assertTrue(v.isVersionCompatible(OWN.appVersion(), MemberCompat.of(1, 299).appVersion()));
        assertEquals("never narrows", new MemberCompat.Seen(1, 299, 301, 300), MemberCompat.readSeenOrNull(dir));
        assertFalse(v.isVersionCompatible(OWN.appVersion(), MemberCompat.of(1, 302).appVersion()));
        assertEquals("a refused version is not recorded", new MemberCompat.Seen(1, 299, 301, 300),
                MemberCompat.readSeenOrNull(dir));
    }

    @Test
    public void theReplaceIsAtomicAndLeavesNoTemporaryFile() throws Exception {
        final File dir = tmp.newFolder("cluster");
        // a temp file left by a crash before the rename does not matter
        Files.writeString(dir.toPath().resolve(MemberCompat.SEEN_FILE + ".tmp"), "garbage");
        MemberCompat.writeSeen(dir, new MemberCompat.Seen(1, 300, 300, 300));
        MemberCompat.writeSeen(dir, new MemberCompat.Seen(1, 300, 301, 300));
        assertFalse(Files.exists(dir.toPath().resolve(MemberCompat.SEEN_FILE + ".tmp")));
        assertEquals(new MemberCompat.Seen(1, 300, 301, 300), MemberCompat.readSeenOrNull(dir));
        try (var names = Files.list(dir.toPath())) {
            assertEquals(List.of(MemberCompat.SEEN_FILE), names.map(p -> p.getFileName().toString()).toList());
        }
    }

    @Test
    public void checkBeforeLaunchPassesAMissingFileAndWritesTheOwnVersionWhenTheDirectoryExists() throws Exception {
        final File dir = tmp.newFolder("cluster");
        assertNull(MemberCompat.checkBeforeLaunch(dir, OWN, "abc1234"));
        assertEquals(new MemberCompat.Seen(1, 300, 300, 300), MemberCompat.readSeenOrNull(dir));
        final File absent = new File(tmp.getRoot(), "not-yet");
        assertNull(MemberCompat.checkBeforeLaunch(absent, OWN, "abc1234"));
        assertFalse("a directory Aeron has not created yet is left alone", absent.exists());
    }

    @Test
    public void checkBeforeLaunchRefusesTwoLevelsFromTheLastSeenOrAnotherGeneration() throws Exception {
        final File dir = tmp.newFolder("cluster");
        MemberCompat.writeSeen(dir, new MemberCompat.Seen(1, 297, 298, 298));
        assertEquals("member version incompatible: this build is g1.300 (build abc1234), the cluster's last seen level"
                        + " is g1.298; a cluster runs level N and N-1 of one generation; install a compatible build, a"
                        + " restart cannot fix this",
                MemberCompat.checkBeforeLaunch(dir, OWN, "abc1234"));
        assertNull(MemberCompat.checkBeforeLaunch(dir, MemberCompat.of(1, 299), "abc1234"));
        assertNull(MemberCompat.checkBeforeLaunch(dir, MemberCompat.of(1, 297), "abc1234"));
        assertNotNull("two below level_max", MemberCompat.checkBeforeLaunch(dir, MemberCompat.of(1, 296), "x"));
        MemberCompat.writeSeen(dir, new MemberCompat.Seen(2, 300, 300, 300));
        final String other = MemberCompat.checkBeforeLaunch(dir, OWN, "abc1234");
        assertTrue(other, other.contains("last seen level is g2.300"));
    }

    /**
     * Verifier row M1: during N to N+1 a follower on N records level_max=N+1 from an N+1 leader; a build two levels
     * above what this member last ran (N+2) must not pass because it is one away from level_max.
     */
    @Test
    public void aBuildTwoLevelsAboveTheLevelThisMemberLastRanIsRefusedEvenWithinOneOfLevelMax() throws Exception {
        final File dir = tmp.newFolder("cluster");
        final MemberCompat n = MemberCompat.of(1, 300);
        assertNull("the member launches on N", MemberCompat.checkBeforeLaunch(dir, n, "abc1234"));
        final VersionValidator v = MemberCompat.validator(n, (a, b) -> { }, dir);
        assertTrue("an N+1 leader is accepted", v.isVersionCompatible(n.appVersion(), MemberCompat.of(1, 301).appVersion()));
        assertEquals(new MemberCompat.Seen(1, 300, 301, 300), MemberCompat.readSeenOrNull(dir));
        assertEquals("member version incompatible: this build is g1.302 (build abc1234), this member last ran g1.300;"
                        + " a member moves up one level at a time; a cluster runs level N and N-1 of one generation;"
                        + " install a compatible build, a restart cannot fix this",
                MemberCompat.checkBeforeLaunch(dir, MemberCompat.of(1, 302), "abc1234"));
        assertNull("N+1 launches", MemberCompat.checkBeforeLaunch(dir, MemberCompat.of(1, 301), "abc1234"));
        assertEquals("the launch is recorded", new MemberCompat.Seen(1, 300, 301, 301), MemberCompat.readSeenOrNull(dir));
        assertNull("back to N (a rollback) launches", MemberCompat.checkBeforeLaunch(dir, n, "abc1234"));
        assertEquals(300, MemberCompat.readSeenOrNull(dir).lastOwnLevel());
    }

    @Test
    public void aFormatOneSeenFileIsRefusedWithItsOwnText() throws Exception {
        final File dir = tmp.newFolder("cluster");
        final String body = "format=member-compat-seen/1\ngeneration=1\nlevel_min=300\nlevel_max=300\n";
        final byte[] covered = body.getBytes(StandardCharsets.US_ASCII);
        Files.writeString(MemberCompat.seenFile(dir), body + "sha256=" + OffBoxWatermark.sha256Hex(covered,
                covered.length) + "\n");
        final String refusal = MemberCompat.checkBeforeLaunch(dir, OWN, "abc1234");
        assertTrue(refusal, refusal.contains("format member-compat-seen/1 was written by a pre-release build"));
        assertTrue(refusal, refusal.contains(MemberCompat.seenFile(dir).toString()));
    }

    /**
     * Verifier row M2: a seen file the validator cannot write never throws into Aeron; the node stops with 78
     * naming the file (here through a real lifecycle's refusal).
     */
    @Test
    public void anUnwritableClusterDirectoryStopsTheNodeWith78NamingTheFile() throws Exception {
        final File dir = tmp.newFolder("cluster");
        assertTrue(dir.setWritable(false, false));
        try {
            final List<String> log = new ArrayList<>();
            final int[] halted = {-1};
            final EngineLifecycle lifecycle = new EngineLifecycle("Test node", log::add, s -> halted[0] = s, 5_000, 50);
            final VersionValidator v = MemberCompat.validator(OWN,
                    MemberCompat.fatalOnRefusal(lifecycle, new BuildInfo("1.0", "abc1234")), dir);
            final boolean[] threw = {false};
            boolean result = true;
            try {
                result = v.isVersionCompatible(OWN.appVersion(), OWN.appVersion());
            } catch (final RuntimeException e) {
                threw[0] = true;
            }
            assertFalse("never throws into Aeron", threw[0]);
            assertFalse("a file fault refuses", result);
            lifecycle.finish();
            assertEquals(78, halted[0]);
            assertTrue(log.get(0), log.get(0).startsWith("Test node FATAL (exit 78): member version check: cannot write "
                    + MemberCompat.seenFile(dir)));
        } finally {
            dir.setWritable(true, false);
        }
    }

    @Test
    public void aNodeWithoutAnInstalledRefusalRefusesEveryVersion() {
        final io.aeron.cluster.ConsensusModule.Context cm = new io.aeron.cluster.ConsensusModule.Context()
                .clusterDir(new File(tmp.getRoot(), "cluster"));
        final io.aeron.cluster.service.ClusteredServiceContainer.Context svc =
                new io.aeron.cluster.service.ClusteredServiceContainer.Context();
        final VersionValidator closed = MemberCompat.install(cm, List.of(svc), OWN, MemberCompat.NOT_INSTALLED);
        assertFalse("fail closed: even its own version", closed.isVersionCompatible(OWN.appVersion(), OWN.appVersion()));
        assertEquals(OWN.appVersion(), cm.appVersion());
        assertEquals(OWN.appVersion(), svc.appVersion());
        final VersionValidator open = MemberCompat.install(cm, List.of(svc), OWN, MemberCompat.testRefusal("rig"));
        assertTrue(open.isVersionCompatible(OWN.appVersion(), OWN.appVersion()));
        org.junit.Assert.assertSame(open, cm.appVersionValidator());
        org.junit.Assert.assertSame(open, svc.appVersionValidator());
    }

    @Test
    public void bootCheckRecordsExit78AndThrowsBeforeTheLaunch() throws Exception {
        final File dir = tmp.newFolder("cluster");
        MemberCompat.writeSeen(dir, new MemberCompat.Seen(1, 300, 300, 300));
        final List<String> log = new ArrayList<>();
        final int[] halted = {-1};
        final EngineLifecycle lifecycle = new EngineLifecycle("Test node", log::add, s -> halted[0] = s, 5_000, 50);
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> MemberCompat.bootCheck(lifecycle,
                dir, MemberCompat.of(1, 302), new BuildInfo("1.0", "abc1234")));
        assertTrue(e.getMessage(), e.getMessage().contains("this build is g1.302"));
        lifecycle.finish();
        assertEquals(78, halted[0]);
        MemberCompat.bootCheck(lifecycle, dir, MemberCompat.of(1, 301), new BuildInfo("1.0", "abc1234"));
    }

    @Test
    public void anOversizedSeenFileIsUnreadable() throws Exception {
        final File dir = tmp.newFolder("cluster");
        Files.write(MemberCompat.seenFile(dir), new byte[2048]);
        assertThrows(IllegalStateException.class, () -> MemberCompat.readSeenOrNull(dir));
        Files.write(MemberCompat.seenFile(dir), "x".getBytes(StandardCharsets.US_ASCII));
        assertThrows(IllegalStateException.class, () -> MemberCompat.readSeenOrNull(dir));
    }

    // ------------------------------------------------------------------ manifest

    private static MemberCompat.Manifest manifest(final int floor, final int top, final int... behaviors) {
        return MemberCompat.Manifest.builder().wire(floor, top).behaviors(behaviors).layout(6, 1).layout(7, 4)
                .journal("orders", 7, 3).build();
    }

    @Test
    public void theManifestTextRoundTrips() {
        final MemberCompat.Manifest m = manifest(10, 10, 1, 2, 3, 4);
        assertEquals("format=member-compat-manifest/1\nwire_floor=10\nwire_top=10\nbehaviors=1,2,3,4\n"
                + "layout=6 since_behavior=1\nlayout=7 since_behavior=4\njournal=orders schema=7 version=3\n", m.text());
        assertEquals(m, MemberCompat.Manifest.parse(m.text()));
        assertEquals(7, m.layoutWrittenAt(4));
        assertEquals(6, m.layoutWrittenAt(3));
    }

    @Test
    public void theManifestRules() {
        final MemberCompat g12 = MemberCompat.of(1, 2);
        final String prev = manifest(10, 11, 1, 2, 3, 4).text();
        final MemberCompat.Manifest build = manifest(10, 11, 2, 3, 4, 5);
        assertEquals(List.of(), MemberCompat.manifestFailures(g12, build, build.text(), prev));
        // one extra behaviour at the old level
        final List<String> changed = MemberCompat.manifestFailures(g12, manifest(10, 11, 2, 3, 4, 5, 6),
                build.text(), prev);
        assertTrue(changed.toString(), changed.get(0).startsWith(MemberCompat.MANIFEST_CHANGED));
        final MemberCompat.Manifest floorMoved = manifest(11, 11, 2, 3, 4, 5);
        assertEquals(List.of("level rule: the wire floor moved from 10 to 11 (the floor never moves within a"
                + " generation)"), MemberCompat.manifestFailures(g12, floorMoved, floorMoved.text(), prev));
        final MemberCompat.Manifest dropsTop = manifest(10, 10, 1, 2, 3);
        final List<String> f = MemberCompat.manifestFailures(g12, dropsTop, dropsTop.text(), prev);
        assertTrue(f.toString(), f.contains("level rule: the build does not run behaviour 4, the highest of the"
                + " previous level"));
        assertTrue(f.toString(), f.contains("level rule: the wire top fell from 11 to 10"));
        final MemberCompat.Manifest noLayout = MemberCompat.Manifest.builder().wire(10, 10).behaviors(4, 5)
                .layout(8, 5).journal("orders", 7, 3).build();
        assertTrue(MemberCompat.manifestFailures(g12, noLayout, noLayout.text(), prev).contains(
                "level rule: the build does not read layout 7, the one the previous level writes at behaviour 4"));
        assertTrue(MemberCompat.manifestFailures(g12, build, null, null).get(0).startsWith(
                "no committed manifest for g1.2"));
    }
}
