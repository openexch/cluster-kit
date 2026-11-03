// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.DriverTimeoutException;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC2 S16 W9: the exit decision of {@link EngineLifecycle}, in-JVM with a recording halter. */
public class EngineLifecycleTest {

    private final List<Integer> halts = new CopyOnWriteArrayList<>();
    private final List<String> logs = new CopyOnWriteArrayList<>();

    private EngineLifecycle lifecycle(final long shutdownMs, final long graceMs) {
        return new EngineLifecycle("test-node", logs::add, halts::add, shutdownMs, graceMs);
    }

    @Test
    public void driverDeathWithAnExternalDriverHaltsOneAfterTheCloseNeverZero() {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        assertTrue(lf.onError(new DriverTimeoutException("MediaDriver keepalive: age=10090ms > timeout=10000ms"), true));
        assertTrue("the fault wakes the main thread", lf.stopRequested());
        lf.awaitStop();
        lf.finish();
        assertEquals(List.of(1), halts);
        assertEquals(EngineLifecycle.Cause.FATAL, lf.cause());
    }

    @Test
    public void fatalAeronErrorWithAnEmbeddedDriverIsNotAFault() {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        assertFalse(lf.onError(new DriverTimeoutException("x"), false));
        assertFalse(lf.onError(new AeronException("warn", AeronException.Category.WARN), true));
        assertFalse(lf.stopRequested());
        assertTrue(halts.isEmpty());
    }

    @Test
    public void unexpectedAgentTerminationIsAFaultExpectedIsNot() {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        assertFalse(lf.onError(new AgentTerminationException("cluster shutdown", true), false));
        assertFalse(lf.stopRequested());
        assertTrue(lf.onError(new AgentTerminationException("unexpected Aeron close"), false));
        lf.finish();
        assertEquals(List.of(1), halts);
    }

    @Test
    public void clusterTerminationAloneExitsZero() {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        lf.onClusterTermination();
        lf.awaitStop();
        lf.finish();
        assertEquals(List.of(0), halts);
    }

    @Test
    public void unexpectedTerminationBehindTheTerminationHookUpgradesToOne() {
        // Aeron runs the termination hook first and reports the unexpected AgentTerminationException after it.
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        lf.onClusterTermination();
        assertTrue(lf.onError(new AgentTerminationException("unexpected Aeron close"), true));
        lf.finish();
        assertEquals(List.of(1), halts);
    }

    @Test
    public void signalStopIsNeverUpgradedAndFinishDoesNotHalt() throws Exception {
        final EngineLifecycle lf = lifecycle(5_000, 60_000);
        final Thread hook = new Thread(lf::onJvmShutdown);
        hook.start();
        lf.awaitStop();
        assertFalse("a fault during the signal close is logged, not recorded",
                lf.onError(new DriverTimeoutException("x"), true));
        lf.finish();
        hook.join(2_000);
        assertFalse("the hook returns once the close is done", hook.isAlive());
        assertTrue("SIGTERM exits with the signal status, no halt", halts.isEmpty());
        assertEquals(EngineLifecycle.Cause.SIGNAL, lf.cause());
    }

    @Test
    public void shutdownHookIsBoundedWhenTheCloseHangs() throws Exception {
        final EngineLifecycle lf = lifecycle(300, 60_000);
        final long t0 = System.nanoTime();
        final Thread hook = new Thread(lf::onJvmShutdown);
        hook.start();
        hook.join(5_000);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertFalse(hook.isAlive());
        assertTrue("bounded by the shutdown timeout: " + ms, ms >= 290 && ms < 3_000);
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("clean close exceeded 300 ms")));
    }

    @Test
    public void backstopHaltsWithTheFaultStatusWhenTheCloseHangs() throws Exception {
        final EngineLifecycle lf = lifecycle(1_000, 200);
        assertTrue(lf.fatal(2, "hot loop"));
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (halts.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(List.of(2), halts);
        assertFalse("first cause wins", lf.fatal(1, "later"));
    }

    @Test
    public void startupFailureHaltsOneAndLogsTheCause() {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        lf.startupFailed(new IllegalStateException("No external media driver found at /x after 2000ms"));
        assertEquals(List.of(1), halts);
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("No external media driver found")));
    }

    @Test
    public void startupFailureDuringASignalStopDoesNotHalt() {
        final EngineLifecycle lf = lifecycle(100, 60_000);
        new Thread(lf::onJvmShutdown).start();
        lf.awaitStop();
        lf.startupFailed(new IllegalStateException("stop requested while waiting"));
        assertTrue(halts.isEmpty());
    }

    @Test
    public void finishWithoutAStopCauseIsNotClean() {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        lf.finish();
        assertEquals(List.of(1), halts);
    }

    @Test
    public void aFatalFaultNeverHasStatusZero() {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        assertThrows(IllegalArgumentException.class, () -> lf.fatal(0, "x"));
    }

    @Test
    public void driverWaitTimesOutWithinTheBoundAndReturnsOnceCncExists() throws Exception {
        final Path dir = Files.createTempDirectory("lifecycle-cnc-");
        try {
            final EngineLifecycle lf = lifecycle(1_000, 60_000);
            final long t0 = System.nanoTime();
            final IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> lf.awaitExternalDriver(dir.toString(), 300));
            final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(e.getMessage(), e.getMessage().contains("after 300ms (TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS)"));
            assertTrue("bounded: " + ms, ms >= 290 && ms < 3_000);
            Files.createFile(dir.resolve("cnc.dat"));
            lf.awaitExternalDriver(dir.toString(), 300);
        } finally {
            Files.deleteIfExists(dir.resolve("cnc.dat"));
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void driverWaitAbortsOnAStop() throws Exception {
        final Path dir = Files.createTempDirectory("lifecycle-cnc-");
        try {
            final EngineLifecycle lf = lifecycle(1_000, 60_000);
            new Thread(lf::onJvmShutdown).start();
            final long t0 = System.nanoTime();
            final IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> lf.awaitExternalDriver(dir.toString(), 60_000));
            assertTrue(e.getMessage(), e.getMessage().contains("stop requested"));
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) < 3_000);
            lf.finish();
        } finally {
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void settingsAreEnvThenPropertyThenDefaultAndRefuseNonPositive() {
        assertEquals(30_000, EngineLifecycle.parsePositive("E", null, "p", null, 30_000));
        assertEquals(7, EngineLifecycle.parsePositive("E", "", "p", "7", 30_000));
        assertEquals(5, EngineLifecycle.parsePositive("E", " 5 ", "p", "7", 30_000));
        for (final String bad : new String[] {"0", "-1", "abc", "1.5"}) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> EngineLifecycle.parsePositive("ENGINE_SHUTDOWN_TIMEOUT_MS", bad, "p", null, 1));
            assertTrue(e.getMessage(), e.getMessage().startsWith("ENGINE_SHUTDOWN_TIMEOUT_MS must be a positive"));
        }
        assertEquals(EngineLifecycle.DEFAULT_SHUTDOWN_TIMEOUT_MS, 4_000);
        assertEquals(EngineLifecycle.DEFAULT_FATAL_EXIT_GRACE_MS, 5_000);
        assertEquals(EngineLifecycle.DEFAULT_DRIVER_WAIT_MS, 30_000);
    }

    @Test
    public void mainThreadInterruptIsAFaultNotASignalStop() throws Exception {
        final EngineLifecycle lf = lifecycle(1_000, 60_000);
        final Thread main = new Thread(() -> {
            lf.awaitStop();
            lf.finish();
        });
        main.start();
        Thread.sleep(100);
        main.interrupt();
        main.join(5_000);
        assertEquals(EngineLifecycle.Cause.FATAL, lf.cause());
        assertEquals(List.of(1), halts);
        assertTrue(logs.toString(), logs.stream().anyMatch(l -> l.contains("main thread interrupted")));
    }

    @Test
    public void sigtermAfterAFaultWithAHangingCloseHaltsWithTheFaultStatus() throws Exception {
        // the shutdown bound (300 ms) ends before the fatal grace (60 s): the hook must not let the JVM exit 143
        final EngineLifecycle lf = lifecycle(300, 60_000);
        assertTrue(lf.fatal(1, "driver died"));
        final Thread hook = new Thread(lf::onJvmShutdown);
        hook.start();
        hook.join(5_000);
        assertFalse(hook.isAlive());
        assertEquals(List.of(1), halts);
        assertEquals(EngineLifecycle.Cause.FATAL, lf.cause());
    }

    @Test
    public void closeFailureDuringASignalStopIsNotReportedClean() throws Exception {
        final EngineLifecycle lf = lifecycle(5_000, 60_000);
        final Thread hook = new Thread(lf::onJvmShutdown);
        hook.start();
        lf.awaitStop();
        lf.startupFailed(new IllegalStateException("archive close failed"));
        hook.join(2_000);
        assertTrue(halts.isEmpty());
        assertTrue(logs.toString(), logs.stream().anyMatch(
                l -> l.contains("stopped on SIGTERM, close failed: java.lang.IllegalStateException: archive close failed")));
        assertFalse(logs.toString(), logs.stream().anyMatch(l -> l.contains("stopped cleanly")));
    }
}
