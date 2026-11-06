// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import com.openexchange.cluster.testkit.ClusterLogWriter;
import com.openexchange.cluster.testkit.ForbiddenAgnosticProbe;
import com.openexchange.cluster.testkit.ForbiddenClockProbe;
import com.openexchange.cluster.testkit.ForbiddenFieldProbe;
import com.openexchange.cluster.testkit.HashChainEngine;
import com.openexchange.cluster.testkit.NeverSchedulesScan;
import com.openexchange.cluster.testkit.RecordedLogFixture;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.VersionValidator;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.openexchange.cluster.log.Logs.MTU;
import static com.openexchange.cluster.log.Logs.TERM;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** RCS S2 4.3: every 3.3 row, the app-version and time-unit rules, the SKIP window and RF-6 (never schedules). */
public class ReplayContainerSemanticsTest {

    private static final int APP = SemanticVersion.compose(0, 0, 1);
    private static final VersionValidator DEFAULT = AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;

    /** Records every engine call with its arguments. */
    static class ProbeEngine implements ShadowEngine {
        final List<String> calls = new ArrayList<>();
        RuntimeException throwOnMessage;
        byte[] image = {1, 2, 3};

        @Override
        public int engineId() {
            return 77;
        }

        @Override
        public void genesis() {
            calls.add("genesis");
        }

        @Override
        public void restore(final DirectBuffer img, final int length) {
            final byte[] b = new byte[length];
            img.getBytes(0, b);
            calls.add("restore " + Arrays.toString(b));
        }

        @Override
        public int serialize(final MutableDirectBuffer dst) {
            calls.add("serialize");
            dst.putBytes(0, image);
            return image.length;
        }

        @Override
        public void onSessionOpen(final long id, final long logPosition, final long timestamp, final byte[] principal) {
            calls.add("open id=" + id + " pos=" + logPosition + " ts=" + timestamp + " principal="
                    + Arrays.toString(principal));
        }

        @Override
        public void onSessionClose(final long id, final long logPosition, final long timestamp,
                                   final CloseReason closeReason) {
            calls.add("close id=" + id + " pos=" + logPosition + " ts=" + timestamp + " reason=" + closeReason);
        }

        @Override
        public boolean onSessionMessage(final long id, final long logPosition, final long timestamp,
                                        final DirectBuffer buffer, final int offset, final int length) {
            if (throwOnMessage != null) {
                throw throwOnMessage;
            }
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b);
            calls.add("msg id=" + id + " pos=" + logPosition + " ts=" + timestamp + " bytes=" + Arrays.toString(b));
            return true;
        }

        @Override
        public int onTimerEvent(final long correlationId, final long logPosition, final long timestamp) {
            calls.add("timer corr=" + correlationId + " pos=" + logPosition + " ts=" + timestamp);
            return 5;
        }

        @Override
        public void onNewLeadershipTermEvent(final long term, final long logPosition, final long timestamp,
                                             final TimeUnit timeUnit) {
            calls.add("term " + term + " pos=" + logPosition + " ts=" + timestamp + " " + timeUnit);
        }
    }

    static final class Triggers implements ShadowTriggerListener {
        final List<String> fired = new ArrayList<>();
        boolean answer;

        @Override
        public boolean onSnapshotAction(final long logPosition, final long term, final long timestamp,
                                        final int flags) {
            fired.add(logPosition + "/" + term + "/" + timestamp + "/" + flags);
            return answer;
        }
    }

    private static ReplayStoppedException.Reason refusal(final Runnable call) {
        try {
            call.run();
        } catch (final ReplayStoppedException e) {
            return e.reason();
        }
        fail("call was accepted");
        return null;
    }

    // ------------------------------------------------------------------------------------------------ 3.3 rows

    @Test
    public void everyCallbackForwardsTheFrameEndAndTheFrameTimestamp() {
        final ProbeEngine engine = new ProbeEngine();
        final Triggers triggers = new Triggers();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, triggers);
        c.genesis();
        assertEquals(LogCursor.GENESIS, c.cursor());
        assertEquals(0, c.imagePosition());

        c.onNewLeadershipTermEvent(3, 96, 1000, 0, 0, 1, TimeUnit.MILLISECONDS, APP);
        assertEquals(new LogCursor(96, 1000, 3, 0), c.cursor());
        c.onSessionOpen(3, 256, 11, 1001, 102, "aeron:udp?endpoint=x:1", new byte[] {4, 5});
        assertEquals(new LogCursor(256, 1001, 3, 0), c.cursor());
        c.onSessionMessage(352, 11, 1002, new UnsafeBuffer(new byte[] {9, 9, 7}), 1, 2);
        c.onTimerEvent(416, 42, 1003);
        c.onSessionClose(3, 480, 11, 1004, CloseReason.CLIENT_ACTION);
        c.onSessionClose(3, 544, 999, 1005, CloseReason.NULL_VAL);  // never opened: forwarded, no map
        assertEquals(List.of("genesis",
                "term 3 pos=96 ts=1000 MILLISECONDS",
                "open id=11 pos=256 ts=1001 principal=[4, 5]",
                "msg id=11 pos=352 ts=1002 bytes=[9, 7]",
                "timer corr=42 pos=416 ts=1003",
                "close id=11 pos=480 ts=1004 reason=CLIENT_ACTION",
                "close id=999 pos=544 ts=1005 reason=NULL_VAL"), engine.calls);
        assertEquals(new LogCursor(544, 1005, 3, 0), c.cursor());
        assertEquals(544, c.logPosition());
        assertEquals(1005, c.clusterTime());
        assertEquals(1, c.count(FrameKind.SESSION_OPEN));
        assertEquals(2, c.count(FrameKind.SESSION_CLOSE));
        assertEquals(1, c.count(FrameKind.SESSION_MESSAGE));
        assertEquals(1, c.count(FrameKind.TIMER_EVENT));
        assertEquals(1, c.count(FrameKind.NEW_LEADERSHIP_TERM));
    }

    @Test
    public void actionsNeverReachTheEngineAndOnlySnapshotFlags0And1Trigger() {
        final ProbeEngine engine = new ProbeEngine();
        final Triggers triggers = new Triggers();
        triggers.answer = true;
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, triggers);
        c.genesis();
        assertTrue(c.onClusterAction(3, 64, 10, ClusterAction.SNAPSHOT, 0));
        assertTrue(c.onClusterAction(3, 128, 11, ClusterAction.SNAPSHOT, 1));
        assertFalse(c.onClusterAction(3, 192, 12, ClusterAction.SNAPSHOT, 2));
        assertFalse(c.onClusterAction(3, 256, 13, ClusterAction.SUSPEND, 0));
        assertFalse(c.onClusterAction(3, 320, 14, ClusterAction.RESUME, 0));
        assertEquals(List.of("64/3/10/0", "128/3/11/1"), triggers.fired);
        assertEquals(3, c.ignoredActionCount());
        assertEquals(0, c.suppressedTriggerCount());
        assertEquals(List.of("genesis"), engine.calls);
        assertEquals(new LogCursor(320, 14, -1, -1), c.cursor());
        assertEquals(5, c.count(FrameKind.CLUSTER_ACTION));
    }

    @Test
    public void nonMonotonicPositionStopsAndLatches() {
        final ProbeEngine engine = new ProbeEngine();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, ShadowTriggerListener.NONE);
        c.genesis();
        c.onTimerEvent(128, 1, 1);
        assertEquals(ReplayStoppedException.Reason.NON_MONOTONIC_POSITION, refusal(() -> c.onTimerEvent(128, 2, 2)));
        assertTrue(c.failed());
        assertEquals(ReplayStoppedException.Reason.FAILED, refusal(() -> c.onTimerEvent(256, 3, 3)));
        assertEquals(List.of("genesis", "timer corr=1 pos=128 ts=1"), engine.calls);
    }

    @Test
    public void callsBeforeStartAndASecondStartAreRefused() {
        final ProbeEngine engine = new ProbeEngine();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, ShadowTriggerListener.NONE);
        assertEquals(ReplayStoppedException.Reason.NOT_STARTED, refusal(() -> c.onTimerEvent(64, 1, 1)));
        assertEquals(ReplayStoppedException.Reason.NOT_STARTED, refusal(() -> c.serialize(new ExpandableArrayBuffer())));
        assertEquals(ReplayStoppedException.Reason.NOT_STARTED,
                refusal(() -> c.onClusterAction(0, 64, 1, ClusterAction.SNAPSHOT, 0)));
        assertFalse("NOT_STARTED does not latch", c.failed());
        c.genesis();
        assertEquals(ReplayStoppedException.Reason.ALREADY_STARTED, refusal(c::genesis));
        assertEquals(ReplayStoppedException.Reason.ALREADY_STARTED,
                refusal(() -> c.restore(new UnsafeBuffer(new byte[1]), 1, 0, LogCursor.GENESIS)));
        assertEquals(List.of("genesis"), engine.calls);
    }

    @Test
    public void anEngineThrowLatchesTheContainer() {
        final ProbeEngine engine = new ProbeEngine();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, ShadowTriggerListener.NONE);
        c.genesis();
        final IllegalStateException boom = new IllegalStateException("engine refuses");
        engine.throwOnMessage = boom;
        try {
            c.onSessionMessage(64, 1, 1, new UnsafeBuffer(new byte[1]), 0, 1);
            fail("engine throw must surface");
        } catch (final ReplayStoppedException e) {
            assertEquals(ReplayStoppedException.Reason.ENGINE_FAILED, e.reason());
            assertTrue(e.getCause() == boom);
        }
        assertTrue(c.failed());
        engine.throwOnMessage = null;
        assertEquals(ReplayStoppedException.Reason.FAILED, refusal(() -> c.onTimerEvent(128, 1, 1)));
        assertEquals(ReplayStoppedException.Reason.FAILED, refusal(() -> c.serialize(new ExpandableArrayBuffer())));
        assertEquals(ReplayStoppedException.Reason.FAILED,
                refusal(() -> c.onNewLeadershipTermEvent(1, 192, 1, 0, 0, 1, TimeUnit.MILLISECONDS, APP)));
        assertEquals(List.of("genesis"), engine.calls);
    }

    @Test
    public void aRestoreThrowLatches() {
        final ShadowEngine refusing = new ProbeEngine() {
            @Override
            public void restore(final DirectBuffer img, final int length) {
                throw new IllegalStateException("bad image");
            }
        };
        final ReplayContainer c = new ReplayContainer(refusing, APP, DEFAULT, ShadowTriggerListener.NONE);
        assertEquals(ReplayStoppedException.Reason.ENGINE_FAILED,
                refusal(() -> c.restore(new UnsafeBuffer(new byte[1]), 1, 0, LogCursor.GENESIS)));
        assertTrue(c.failed());
        assertEquals(ReplayStoppedException.Reason.FAILED, refusal(c::genesis));
    }

    // ----------------------------------------------------------------------------------- app version, time unit

    @Test
    public void defaultValidatorAcceptsSameMajorAndRefusesAnotherWithNoEngineCall() {
        assertTrue(DEFAULT.isVersionCompatible(APP, SemanticVersion.compose(0, 7, 3)));
        assertFalse(DEFAULT.isVersionCompatible(APP, SemanticVersion.compose(1, 0, 0)));

        final ProbeEngine ok = new ProbeEngine();
        final ReplayContainer accepting = new ReplayContainer(ok, APP, DEFAULT, ShadowTriggerListener.NONE);
        accepting.genesis();
        accepting.onNewLeadershipTermEvent(1, 96, 5, 0, 0, 1, TimeUnit.MILLISECONDS, SemanticVersion.compose(0, 7, 3));
        assertEquals(List.of("genesis", "term 1 pos=96 ts=5 MILLISECONDS"), ok.calls);

        final ProbeEngine refused = new ProbeEngine();
        final ReplayContainer refusing = new ReplayContainer(refused, APP, DEFAULT, ShadowTriggerListener.NONE);
        refusing.genesis();
        assertEquals(ReplayStoppedException.Reason.INCOMPATIBLE_APP_VERSION,
                refusal(() -> refusing.onNewLeadershipTermEvent(1, 96, 5, 0, 0, 1, TimeUnit.MILLISECONDS,
                        SemanticVersion.compose(1, 0, 0))));
        assertEquals(List.of("genesis"), refused.calls);
        assertTrue(refusing.failed());
        assertEquals(LogCursor.GENESIS, refusing.cursor());
    }

    @Test
    public void microsecondsStopUnsupportedTimeUnit() {
        final ProbeEngine engine = new ProbeEngine();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, ShadowTriggerListener.NONE);
        c.genesis();
        assertEquals(ReplayStoppedException.Reason.UNSUPPORTED_TIME_UNIT,
                refusal(() -> c.onNewLeadershipTermEvent(1, 96, 5, 0, 0, 1, TimeUnit.MICROSECONDS, APP)));
        assertEquals(List.of("genesis"), engine.calls);
        assertTrue(c.failed());
    }

    // ------------------------------------------------------------------------------------------- SKIP window

    @Test
    public void skipWindowSuppressesTriggersAndRefusesSerializeUntilTheImagePosition() {
        final long cm = 1024;
        final long s = 2048;
        final ProbeEngine refused = new ProbeEngine();
        final ReplayContainer bad = new ReplayContainer(refused, APP, DEFAULT, ShadowTriggerListener.NONE);
        assertEquals(ReplayStoppedException.Reason.REPLAY_AFTER_IMAGE,
                refusal(() -> bad.restore(new UnsafeBuffer(new byte[] {1}), 1, s, new LogCursor(s + 32, 1, 2, 0))));
        assertTrue("nothing restored", refused.calls.isEmpty());

        final ProbeEngine engine = new ProbeEngine();
        final Triggers triggers = new Triggers();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, triggers);
        final LogCursor atCm = new LogCursor(cm, 77, 2, 0);
        c.restore(new UnsafeBuffer(new byte[] {8, 9}), 2, s, atCm);
        assertEquals(atCm, c.cursor());
        assertEquals(s, c.imagePosition());
        assertEquals(List.of("restore [8, 9]"), engine.calls);
        assertEquals(ReplayStoppedException.Reason.INSIDE_SKIP_WINDOW,
                refusal(() -> c.serialize(new ExpandableArrayBuffer())));
        assertFalse("INSIDE_SKIP_WINDOW does not latch", c.failed());

        assertFalse(c.onClusterAction(2, cm + 512, 80, ClusterAction.SNAPSHOT, 0));      // inside (c, s)
        assertFalse(c.onClusterAction(2, cm + 576, 81, ClusterAction.SNAPSHOT, 1));
        assertEquals(2, c.suppressedTriggerCount());
        assertTrue(triggers.fired.isEmpty());
        assertEquals(ReplayStoppedException.Reason.INSIDE_SKIP_WINDOW,
                refusal(() -> c.serialize(new ExpandableArrayBuffer())));

        c.onClusterAction(2, s, 82, ClusterAction.SNAPSHOT, 0);                           // first frame p >= s
        assertEquals(List.of(s + "/2/82/0"), triggers.fired);
        assertEquals(3, c.serialize(new ExpandableArrayBuffer()));
        assertEquals(2, c.suppressedTriggerCount());
    }

    @Test
    public void framesAGateDropsStillAdvanceTheCursorButNeverReachTheEngine() {
        final long cm = 1024;
        final long s = 2048;
        final ProbeEngine engine = new ProbeEngine();
        final Triggers triggers = new Triggers();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, triggers);
        c.restore(new UnsafeBuffer(new byte[] {8}), 1, s, new LogCursor(cm, 77, 2, 0));
        final ClusterLogListener gated = SkipWindow.gate(new SkipWindow(s), c);
        gated.onSessionMessage(cm + 256, 1, 90, new UnsafeBuffer(new byte[1]), 0, 1);
        gated.onTimerEvent(cm + 512, 1, 91);
        gated.onSessionClose(2, s, 1, 92, CloseReason.CLIENT_ACTION);      // the frame at s itself: dropped
        assertEquals(new LogCursor(s, 92, 2, 0), c.cursor());
        assertEquals(3, c.skippedFrameCount());
        assertEquals(List.of("restore [8]"), engine.calls);
        assertEquals("the state at s is the image: serialize works at s", 3, c.serialize(new ExpandableArrayBuffer()));
        assertEquals(ReplayStoppedException.Reason.NON_MONOTONIC_POSITION,
                refusal(() -> gated.onTimerEvent(s, 1, 93)));
    }

    @Test
    public void restoreWithReplayFromEqualToTheImageIsAccepted() {
        final ProbeEngine engine = new ProbeEngine();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, ShadowTriggerListener.NONE);
        c.restore(new UnsafeBuffer(new byte[] {1}), 1, 512, new LogCursor(512, 5, 1, 0));
        assertEquals(3, c.serialize(new ExpandableArrayBuffer()));
        assertEquals(ReplayStoppedException.Reason.NON_MONOTONIC_POSITION,
                refusal(() -> c.onTimerEvent(512, 1, 1)));
    }

    // -------------------------------------------------------------------------------------------- integration

    @Test
    public void triggerBreakOnARealRecordingAndSerializeEqualsAFreshReplayToThatPosition() {
        try (RecordedLogFixture fixture = new RecordedLogFixture()) {
            final RecordedLogFixture.Recording rec = fixture.record(TERM, MTU, 0);
            final ClusterLogWriter w = rec.writer();
            w.newLeadershipTerm(0, 1, 0, 0, 1, TimeUnit.MILLISECONDS, APP);
            w.sessionOpen(1, 1, 2, 102, "aeron:ipc", new byte[] {1, 2, 3});
            for (int i = 0; i < 20; i++) {
                w.message(1, 3 + i, new byte[] {(byte)i, 1, 2});
            }
            w.message(1, 30, new byte[3 * MTU]);
            w.timer(9, 31);
            final long snapshotAt = w.action(ClusterAction.SNAPSHOT, 32, 0);
            for (int i = 0; i < 10; i++) {
                w.message(1, 40 + i, new byte[] {(byte)i});
            }
            final long stop = rec.stopRecording();

            final HashChainEngine engine = new HashChainEngine();
            final List<Long> triggered = new ArrayList<>();
            final ReplayContainer container = new ReplayContainer(engine, APP, DEFAULT,
                    (p, term, ts, flags) -> triggered.add(p));
            container.genesis();
            final ClusterLogReader reader = new ClusterLogReader(container, 64, 0);
            final Image image = fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH);
            Logs.pollUntil(reader, image, stop, () -> !triggered.isEmpty());
            assertEquals(List.of(snapshotAt), triggered);
            assertEquals(snapshotAt, reader.position());
            assertEquals(snapshotAt, image.position());
            final ExpandableArrayBuffer atTrigger = new ExpandableArrayBuffer();
            final int length = container.serialize(atTrigger);

            final HashChainEngine freshEngine = new HashChainEngine();
            final ReplayContainer fresh = new ReplayContainer(freshEngine, APP, DEFAULT, ShadowTriggerListener.NONE);
            fresh.genesis();
            final ClusterLogReader freshReader = new ClusterLogReader(fresh, 64, 0);
            Logs.drain(freshReader, fixture.replay(rec.recordingId(), 0, AeronArchive.NULL_LENGTH), snapshotAt);
            final ExpandableArrayBuffer expected = new ExpandableArrayBuffer();
            assertEquals(length, fresh.serialize(expected));
            assertArrayEquals(Arrays.copyOf(expected.byteArray(), length), Arrays.copyOf(atTrigger.byteArray(),
                    length));
            assertEquals(freshEngine.count(), engine.count());
            assertTrue(engine.count() > 20);
        }
    }

    // ------------------------------------------------------------------------------ RF-6: never schedules (kit)

    /** Everything the kit half may reach: cluster.log and the pure new testkit classes (nested included). */
    private static final Pattern KIT_INPUT = Pattern.compile("com/openexchange/cluster/log/[^/]+"
            + "|com/openexchange/cluster/testkit/(RecordedLog|LogFrame|HashChainEngine|ShadowRestoreRunner"
            + "|ShadowEngineConformance|TextDiff)(\\$.*)?");
    /** The kit half's roots; the scan is their transitive closure inside com/openexchange/. */
    private static final List<Class<?>> KIT_ROOTS = List.of(ReplayContainer.class, ClusterLogReader.class,
            SkipWindow.class, ShadowEngine.class, LogCursor.class, ReaderStop.class, ClusterLogReaderStoppedException.class,
            ReplayStoppedException.class, com.openexchange.cluster.testkit.ShadowRestoreRunner.class,
            com.openexchange.cluster.testkit.ShadowEngineConformance.class, HashChainEngine.class,
            com.openexchange.cluster.testkit.RecordedLog.class, com.openexchange.cluster.testkit.LogFrame.class);

    static Path classesOf(final Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (final URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] classBytes(final String internalName) {
        try (InputStream in = ReplayContainerSemanticsTest.class.getClassLoader()
                .getResourceAsStream(internalName + ".class")) {
            if (in == null) {
                throw new IllegalStateException("no class file for " + internalName);
            }
            return in.readAllBytes();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void replayContainerNeverSchedules() {
        // Transitive: the union of the closures from every kit root, inside com/openexchange/ (fail closed on a class
        // that cannot be resolved). The closure must stay inside the kit input: nothing reaches GoldenAssert, the
        // fixture, the writer or the scanner.
        final java.util.Set<String> reached = new java.util.TreeSet<>();
        final List<NeverSchedulesScan.Finding> findings = new ArrayList<>();
        for (final Class<?> root : KIT_ROOTS) {
            final NeverSchedulesScan.Report closure = NeverSchedulesScan.scanClosure(classesOf(root),
                    root.getName().replace('.', '/'), "com/openexchange/", NeverSchedulesScan.Rules.kit());
            reached.addAll(closure.classes());
            findings.addAll(closure.findings());
        }
        for (final String name : reached) {
            assertTrue("kit closure leaves the kit input: " + name, KIT_INPUT.matcher(name).matches());
        }
        assertTrue("kit closure is not vacuous: " + reached.size(), reached.size() >= 25);
        assertTrue("kit RF-6 + agnostic closure must be clean: " + findings, findings.isEmpty());
        // And every class of the input directory, reached or not.
        final NeverSchedulesScan.Report report = NeverSchedulesScan.scanDirectory(classesOf(ReplayContainer.class),
                name -> KIT_INPUT.matcher(name).matches(), NeverSchedulesScan.Rules.kit());
        for (final String required : List.of("FrameKind", "ClusterLogListener", "ClusterLogReader", "ReaderStop",
                "ClusterLogReaderStoppedException", "ShadowEngine", "ReplayContainer", "LogCursor",
                "ShadowTriggerListener", "ReplayStoppedException", "SkipRule", "SkipWindow")) {
            assertTrue("scan input must include " + required,
                    report.classes().contains("com/openexchange/cluster/log/" + required));
        }
        for (final String required : List.of("RecordedLog", "LogFrame", "HashChainEngine", "ShadowRestoreRunner",
                "ShadowEngineConformance")) {
            assertTrue("scan input must include " + required,
                    report.classes().contains("com/openexchange/cluster/testkit/" + required));
        }
        assertFalse(report.classes().contains("com/openexchange/cluster/testkit/RecordedLogFixture"));
        assertTrue("kit RF-6 + agnostic scan must be clean: " + report.findings(), report.clean());

        // Positive controls: the scan can see each kind of reference.
        final NeverSchedulesScan.Rules kit = NeverSchedulesScan.Rules.kit();
        assertTrue("CSA calls scheduleTimer", NeverSchedulesScan.scanClass(
                "io/aeron/cluster/service/ClusteredServiceAgent",
                classBytes("io/aeron/cluster/service/ClusteredServiceAgent"), kit).stream()
                .anyMatch(f -> f.reference().endsWith(".scheduleTimer")));
        assertControl(ForbiddenFieldProbe.class, "io/aeron/cluster/service/Cluster");
        assertControl(ForbiddenClockProbe.class, "java/lang/System.currentTimeMillis");
        assertControl(ForbiddenAgnosticProbe.class, "com.openexchange.assets.");

        // Behavioural: three timer events reach the engine three times; nothing is re-armed.
        final ProbeEngine engine = new ProbeEngine();
        final ReplayContainer c = new ReplayContainer(engine, APP, DEFAULT, ShadowTriggerListener.NONE);
        c.genesis();
        c.onTimerEvent(64, 1, 1);
        c.onTimerEvent(128, 1, 2);
        c.onTimerEvent(192, 2, 3);
        assertEquals(List.of("genesis", "timer corr=1 pos=64 ts=1", "timer corr=1 pos=128 ts=2",
                "timer corr=2 pos=192 ts=3"), engine.calls);

        // Field types: this package, Agrona buffers, the JDK, codec enums, VersionValidator. Nothing else.
        for (final Field field : ReplayContainer.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType().isPrimitive()) {
                continue;
            }
            Class<?> type = field.getType();
            while (type.isArray()) {
                type = type.getComponentType();
            }
            final String name = type.getName();
            // Explicit list: no java.* type is needed (java.util.Timer, java.time.Clock, LongSupplier would pass a
            // package prefix).
            final boolean allowed = type.isPrimitive()
                    || name.startsWith("com.openexchange.cluster.log.")
                    || name.equals("org.agrona.DirectBuffer") || name.equals("org.agrona.MutableDirectBuffer")
                    || (name.startsWith("io.aeron.cluster.codecs.") && type.isEnum())
                    || type == VersionValidator.class;
            assertTrue("ReplayContainer field " + field.getName() + " has a forbidden type " + name, allowed);
        }
    }

    private static void assertControl(final Class<?> probe, final String reference) {
        final String internal = probe.getName().replace('.', '/');
        final List<NeverSchedulesScan.Finding> findings = NeverSchedulesScan.scanDirectory(classesOf(probe),
                internal::equals, NeverSchedulesScan.Rules.kit()).findings();
        assertTrue(probe.getSimpleName() + " must be flagged for " + reference + ": " + findings,
                findings.stream().anyMatch(f -> f.reference().startsWith(reference)));
    }
}
