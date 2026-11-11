// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.agrona.DirectBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * What a fence does after it latched, pure (the policy and trust of {@link OracleVerdictTest}, a fake host that logs
 * every call, the fake clock). A same-history byte mismatch HOLDS (never exits, readiness 503 with the mismatch text,
 * nothing offered, the dump written, a dump failure never changes the decision, REPLICA_MISMATCH_HOLD_MS bounds it
 * when non-zero); a history fence asks the housekeeping thread for the suspect purge and waits for it BEFORE the exit.
 */
public class MismatchHoldTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    final OracleVerdictTest o = new OracleVerdictTest();
    final List<String> calls = new ArrayList<>();
    final ReplicaFollower.Halt halt = new ReplicaFollower.Halt() {
        @Override
        public void fatal(final String reason) {
            calls.add("fatal:" + reason);
        }

        @Override
        public void hold(final String reason) {
            calls.add("hold:" + reason);
        }
    };
    ReplicaReadiness readiness;
    ReplicaFence fence;
    AlignmentTrust trust;
    ShadowTriggerPolicy policy;
    File dumpDir;
    /** The position the process restored from: 0 a genesis start, above 0 a LOCAL_SHADOW start. */
    long restoredAt;
    /** Test seam: the history purge bound is the last EQUAL position only (the bound before the restore point). */
    boolean verifiedAtOnly;

    void wire(final long holdMs, final Set<ReplicaNegativeControl> off, final MismatchDump.Io io) throws IOException {
        dumpDir = tmp.newFolder("mismatch");
        readiness = new ReplicaReadiness(5_000, 30_000, 1L << 20, 0, 0, () -> o.t.now[0], () -> 1_000L,
                () -> -1, () -> -1, EnumSet.noneOf(ReplicaNegativeControl.class));
        readiness.restored(restoredAt == 0 ? ReplicaReadiness.RestorePath.GENESIS
                : ReplicaReadiness.RestorePath.LOCAL_SHADOW);
        fence = new ReplicaFence(halt, new ReplicaFence.Effects() {
            @Override
            public void fenced(final String reason) {
                readiness.fenced(reason);
            }

            @Override
            public void refused(final String reason) {
                readiness.refused(reason);
            }

            @Override
            public void abortWrite() {
                calls.add("abort");
            }
        }, new MismatchHold(holdMs, () -> o.t.now[0], halt), new MismatchDump(dumpDir, 2048, () -> 1_000L, io),
                30_000, off);
        trust = o.trust(restoredAt == 0, restoredAt, off);
        o.t.onFence = new ShadowTriggerPolicy.Fence() {
            @Override
            public void fence(final String reason) {
                fence(FenceFamily.FAULT, reason);
            }

            @Override
            public void fence(final FenceFamily family, final String reason) {
                fence.fence(family, reason, false, verifiedAtOnly ? trust.verifiedAt()
                                : ReplicaFence.historyPurgeBound(trust.verifiedAt(), restoredAt),
                        family == FenceFamily.MISMATCH ? trust.mismatchEvidence() : null);
            }
        };
        policy = o.policy(trust, off);
        policy.fencedProbe(() -> fence.reason() != null);
    }

    void mismatch() {
        o.retrieved.put(OracleVerdictTest.P, o.copy(true, OracleVerdictTest.GENESIS));
        policy.onSnapshotAction(OracleVerdictTest.P, 1, 1_000, 0);
        policy.producePending();
        policy.pollHeld(OracleVerdictTest.P);
    }

    static String captureErr(final Runnable r) {
        final PrintStream saved = System.err;
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        System.setErr(new PrintStream(bytes, true, StandardCharsets.UTF_8));
        try {
            r.run();
        } finally {
            System.setErr(saved);
        }
        final String text = bytes.toString(StandardCharsets.UTF_8);
        saved.print(text);
        return text;
    }

    void holdsRow(final Set<ReplicaNegativeControl> off) throws IOException {
        wire(0, off, MismatchDump.FILES);
        mismatch();
        if (calls.stream().anyMatch(c -> c.startsWith("fatal:"))) {
            throw new AssertionError("the process exited before the metrics were read: " + calls);
        }
        final String text = "oracle mismatch at " + OracleVerdictTest.P + ": first difference at byte ";
        assertTrue(calls.toString(), calls.stream().anyMatch(c -> c.startsWith("hold:" + text)));
        assertTrue(fence.holding());
        assertTrue("nothing offered", o.t.writer.offered.isEmpty());
        final String probe = readiness.probe().detail();
        assertTrue(probe, probe.contains("recovery=fenced:" + text));
        assertTrue(!readiness.probe().ready());
        // 0 holds until stopped
        o.t.now[0] += TimeUnit.DAYS.toNanos(30);
        fence.tickHold();
        assertTrue("hold 0 never exits: " + calls, calls.stream().noneMatch(c -> c.startsWith("fatal:")));
        // a later STANDBY trigger is dropped and counted
        final long dropped = policy.afterFenceDroppedTotal();
        policy.onSnapshotAction(OracleVerdictTest.P + 512, 1, 2_000, 1);
        policy.producePending();
        assertEquals(dropped + 1, policy.afterFenceDroppedTotal());
        assertTrue(o.t.writer.offered.isEmpty());
        // the dump
        final Path at = dumpDir.toPath().resolve(Long.toString(OracleVerdictTest.P));
        final String report = Files.readString(at.resolve("report.txt"));
        for (final String field : new String[] {"position=" + OracleVerdictTest.P, "replica_genesis=7",
                "member_genesis=7", "first_difference=", "replica_sha256=", "member_sha256=",
                "member_recording_id=" + (1000 + OracleVerdictTest.P), "truncated=false"}) {
            assertTrue(field + " in " + report, report.contains(field));
        }
        assertEquals(o.t.serializeLength, Files.size(at.resolve("replica.image")));
        assertEquals(o.t.serializeLength, Files.size(at.resolve("member.image")));
        assertTrue(!fence.dumpIncomplete());
    }

    @Test
    public void aByteMismatchHoldsServesItsReadinessTextAndWritesTheDump() throws IOException {
        holdsRow(EnumSet.noneOf(ReplicaNegativeControl.class));
        System.out.println("GREEN mismatch hold: " + calls);
    }

    @Test
    public void controlHoldOffExitsAtTheMismatch() {
        OracleVerdictTest.expectControl("HOLD_OFF", () -> fresh(m -> m.holdsRow(
                EnumSet.of(ReplicaNegativeControl.HOLD_OFF))), "the process exited before the metrics were read");
    }

    interface Row {
        void run(MismatchHoldTest m) throws IOException;
    }

    /** A control runs its row on a fresh instance (own fixture, own temporary folder). */
    static void fresh(final Row row) {
        final MismatchHoldTest m = new MismatchHoldTest();
        try {
            m.tmp.create();
            row.run(m);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        } finally {
            m.tmp.delete();
        }
    }

    @Test
    public void aNonZeroHoldEndsWithExit1AfterItsBound() throws IOException {
        wire(5_000, EnumSet.noneOf(ReplicaNegativeControl.class), MismatchDump.FILES);
        mismatch();
        o.t.now[0] += TimeUnit.MILLISECONDS.toNanos(4_999);
        fence.tickHold();
        assertTrue(calls.toString(), calls.stream().noneMatch(c -> c.startsWith("fatal:")));
        o.t.now[0] += TimeUnit.MILLISECONDS.toNanos(2);
        fence.tickHold();
        fence.tickHold();
        assertEquals(calls.toString(), 1, calls.stream().filter(c -> c.startsWith(
                "fatal:FATAL replica: the oracle mismatch hold ended after REPLICA_MISMATCH_HOLD_MS=5000")).count());
    }

    /** A disk that refuses every image file. */
    static final MismatchDump.Io FULL_DISK = new MismatchDump.Io() {
        @Override
        public void createDirectories(final Path dir) throws IOException {
            MismatchDump.FILES.createDirectories(dir);
        }

        @Override
        public boolean exists(final Path file) {
            return MismatchDump.FILES.exists(file);
        }

        @Override
        public void write(final Path file, final DirectBuffer buffer, final int length) throws IOException {
            throw new IOException("No space left on device");
        }

        @Override
        public void writeText(final Path file, final String text, final boolean append) throws IOException {
            MismatchDump.FILES.writeText(file, text, append);
        }
    };

    void fullDiskRow(final boolean dumpFailureExits) throws IOException {
        wire(0, EnumSet.noneOf(ReplicaNegativeControl.class), FULL_DISK);
        fence.dumpFailureExits = dumpFailureExits;
        final String err = captureErr(this::mismatch);
        if (!fence.holding() || calls.stream().anyMatch(c -> c.startsWith("fatal:"))) {
            throw new AssertionError("dump dir full prevented the hold: " + calls);
        }
        assertTrue(fence.dumpIncomplete());
        assertTrue(err, err.contains("CRITICAL: mismatch dump at " + OracleVerdictTest.P + " incomplete: "));
        assertTrue("the report is still attempted", Files.readString(dumpDir.toPath()
                .resolve(OracleVerdictTest.P + "/report.txt")).contains("incomplete="));
    }

    @Test
    public void aDumpFailureNeverChangesTheHold() throws IOException {
        fullDiskRow(false);
        System.out.println("GREEN dump failure: hold entered, incomplete=1");
    }

    @Test
    public void controlDumpFailureExitsPreventsTheHold() {
        OracleVerdictTest.expectControl("dump failure exits (seam)", () -> fresh(m -> m.fullDiskRow(true)),
                "dump dir full prevented the hold");
    }

    @Test
    public void anUnwritableDumpDirIsRefusedAtBoot() throws IOException {
        final File notADir = tmp.newFile("a-file");
        try {
            MismatchDump.requireWritable(new File(notADir, "mismatch"));
            fail("an unwritable dump dir was accepted");
        } catch (final IllegalArgumentException expected) {
            assertEquals("REPLICA_MISMATCH_DIR is not writable: " + new File(notADir, "mismatch").getAbsolutePath(),
                    expected.getMessage());
        }
    }

    @Test
    public void aSecondLatchAtTheSamePositionAppendsReLatched() throws IOException {
        wire(0, EnumSet.noneOf(ReplicaNegativeControl.class), MismatchDump.FILES);
        mismatch();
        final MismatchDump again = new MismatchDump(dumpDir, 2048, () -> 2_000L, MismatchDump.FILES);
        assertTrue(again.write(trust.mismatchEvidence()));
        final String report = Files.readString(dumpDir.toPath().resolve(OracleVerdictTest.P + "/report.txt"));
        assertTrue(report, report.contains("re-latched at 1970-01-01T00:00:02Z"));
    }

    // ------------------------------------------------------------------------------------------------ history

    /** Own shadows on disk at the fence, by position; the fake purge removes the ones above its bound. */
    final java.util.TreeSet<Long> ownShadows = new java.util.TreeSet<>();
    final List<Long> purgedShadows = new ArrayList<>();
    String historyErr;

    void historyRow(final Set<ReplicaNegativeControl> off, final int awaitResult) throws IOException {
        wire(0, off, MismatchDump.FILES);
        final long[] above = {Long.MIN_VALUE};
        fence.suspectPurge((a, reason) -> {
            calls.add("purge-request");
            above[0] = a;
            final java.util.SortedSet<Long> suspect = ownShadows.tailSet(a, false);
            purgedShadows.addAll(suspect);
            suspect.clear();
            return new ReplicaFence.Pending() {
                @Override
                public int await(final long timeoutMs) {
                    calls.add("purge-await " + timeoutMs);
                    return awaitResult;
                }

                @Override
                public int suspects() {
                    return 2;
                }
            };
        });
        o.retrieved.put(OracleVerdictTest.P, o.copy(true, 99));
        final String err = captureErr(() -> {
            policy.onSnapshotAction(OracleVerdictTest.P, 1, 1_000, 0);
            policy.producePending();
        });
        historyErr = err;
        final int fatal = indexOf("fatal:");
        final int awaited = indexOf("purge-await");
        if (fatal < 0 || awaited < 0 || awaited > fatal) {
            throw new AssertionError("fatal called before the suspect purge completed: " + calls);
        }
        assertEquals("the purge bound", verifiedAtOnly ? trust.verifiedAt()
                : Math.max(trust.verifiedAt(), restoredAt), above[0]);
        assertTrue(calls.get(fatal), calls.get(fatal).contains("differs from member snapshot"));
        assertTrue(!fence.holding());
        if (awaitResult < 0) {
            assertTrue(err, err.contains("history fence: purge of 2 suspect shadows did not finish in 30000 ms:"
                    + " wipe BASE_DIR"));
        }
    }

    int indexOf(final String prefix) {
        for (int i = 0; i < calls.size(); i++) {
            if (calls.get(i).startsWith(prefix)) {
                return i;
            }
        }
        return -1;
    }

    @Test
    public void anotherGenesisAwaitsTheSuspectPurgeBeforeTheExit() throws IOException {
        historyRow(EnumSet.noneOf(ReplicaNegativeControl.class), 1);
        System.out.println("GREEN history fence order: " + calls);
    }

    @Test
    public void aHistoryFenceOnAGenesisStartPurgesEveryOwnShadowAboveZero() throws IOException {
        ownShadows.addAll(List.of(1024L, 2048L, 3072L));
        historyRow(EnumSet.noneOf(ReplicaNegativeControl.class), 3);
        assertEquals("a genesis start keeps nothing above 0", List.of(1024L, 2048L, 3072L), purgedShadows);
        assertTrue(ownShadows.toString(), ownShadows.isEmpty());
        System.out.println("GREEN history fence on a genesis start: purged " + purgedShadows);
    }

    /** A LOCAL_SHADOW start at {@code R}, no EQUAL yet in this life, then another genesis at P above R. */
    void restoreStartRow() throws IOException {
        final long r = 2048;
        restoredAt = r;
        ownShadows.addAll(List.of(1024L, r, 3072L));
        historyRow(EnumSet.noneOf(ReplicaNegativeControl.class), 1);
        assertEquals("no EQUAL in this process life", -1, trust.verifiedAt());
        if (!ownShadows.contains(1024L) || !ownShadows.contains(r)) {
            throw new AssertionError("a history fence on a restore start purged a shadow at or below the restore"
                    + " point " + r + ": purged " + purgedShadows + ", kept " + ownShadows);
        }
        assertEquals("the shadows above the restore point are suspect", List.of(3072L), purgedShadows);
        assertTrue(historyErr, historyErr.contains("kept at or below " + r));
    }

    @Test
    public void aHistoryFenceOnARestoreStartKeepsTheShadowsAtOrBelowTheRestorePoint() throws IOException {
        restoreStartRow();
        System.out.println("GREEN history fence on a restore start: purged " + purgedShadows + ", kept "
                + ownShadows);
    }

    @Test
    public void controlVerifiedAtOnlyBoundPurgesTheShadowsAtOrBelowTheRestorePoint() {
        OracleVerdictTest.expectControl("history purge bound is verifiedAt only (seam)", () -> fresh(m -> {
            m.verifiedAtOnly = true;
            m.restoreStartRow();
        }), "a history fence on a restore start purged a shadow at or below the restore point");
    }

    @Test
    public void aSuspectPurgeTimeoutLogsTheWipeLineThenExits() throws IOException {
        historyRow(EnumSet.noneOf(ReplicaNegativeControl.class), -1);
    }

    @Test
    public void controlPurgeNotAwaitedExitsFirst() {
        OracleVerdictTest.expectControl("SUSPECT_PURGE_NOT_AWAITED", () -> fresh(m -> m.historyRow(
                EnumSet.of(ReplicaNegativeControl.SUSPECT_PURGE_NOT_AWAITED), 1)),
                "fatal called before the suspect purge completed");
    }
}
