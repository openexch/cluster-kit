// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.DriverTimeoutException;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RC2 S16 W9: the production {@link EngineLifecycle#install} path in a child JVM, so the exit status is the real one
 * (shutdown hook, {@link Runtime#halt}). Each mode is the shape of an engine main: a non-daemon application thread
 * stays alive (as the ME's publishers do), so nothing ends the JVM except the lifecycle.
 */
public class EngineLifecycleProcessTest {

    private static Process child(final Path log, final String mode, final String... env) throws Exception {
        final ProcessBuilder pb = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), EngineLifecycleProcessTest.class.getName(), mode);
        for (int i = 0; i < env.length; i += 2) {
            pb.environment().put(env[i], env[i + 1]);
        }
        return pb.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static void awaitLine(final Path log, final String line, final long timeoutMs) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (Files.exists(log) && Files.readString(log).contains(line)) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no '" + line + "' in " + Files.readString(log));
    }

    private static long[] exitAfter(final Process p, final long t0, final long boundMs) throws Exception {
        final boolean exited = p.waitFor(boundMs, TimeUnit.MILLISECONDS);
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        if (!exited) {
            p.destroyForcibly();
            p.waitFor(5, TimeUnit.SECONDS);
            return new long[] {-1, ms};
        }
        return new long[] {p.exitValue(), ms};
    }

    private String run(final String mode, final boolean sigterm, final long boundMs, final List<Integer> codes,
                       final String... env) throws Exception {
        final Path log = Files.createTempFile("lifecycle-" + mode + "-", ".log");
        final Process p = child(log, mode, env);
        try {
            awaitLine(log, "READY", 15_000);
            final long t0 = System.nanoTime();
            if (sigterm) {
                p.destroy(); // SIGTERM
            }
            final long[] r = exitAfter(p, t0, boundMs);
            final String out = Files.readString(log);
            assertTrue(mode + " must exit within " + boundMs + " ms (took " + r[1] + " ms): " + out, r[0] >= 0);
            assertTrue(mode + " exit " + r[0] + " not in " + codes + ": " + out, codes.contains((int) r[0]));
            return out;
        } finally {
            p.destroyForcibly();
            Files.deleteIfExists(log);
        }
    }

    @Test
    public void sigtermClosesCleanlyAndExits143() throws Exception {
        final String out = run("close-fast", true, 5_000, List.of(143));
        // logged before the hook is released, so it is never lost to the JVM exit
        assertTrue(out, out.contains("child stopped cleanly on JVM shutdown (SIGTERM)"));
    }

    @Test
    public void sigtermWithAHangingCloseStillExitsWithinTheShutdownTimeout() throws Exception {
        final String out = run("close-hangs", true, 4_000, List.of(143), EngineLifecycle.SHUTDOWN_TIMEOUT_ENV, "500");
        assertTrue(out, out.contains("clean close exceeded 500 ms"));
    }

    @Test
    public void fatalFaultExitsOneEvenWhenTheMainThreadReturnsNormally() throws Exception {
        final String out = run("fatal", false, 10_000, List.of(1));
        assertTrue(out, out.contains("child closed after FATAL"));
    }

    @Test
    public void driverDeathHaltsOneOnTheReportingThreadWithoutTheClose() throws Exception {
        final String out = run("driver-lost", false, 5_000, List.of(1));
        assertTrue(out, out.contains("halting now, no component close"));
        assertFalse("the reporting thread never got back to its Aeron client: " + out,
                out.contains("ERROR HANDLER RETURNED"));
        assertFalse("no component close after driver loss: " + out, out.contains("child closed after FATAL"));
    }

    @Test
    public void fatalFaultWithAHangingCloseExitsOneAfterTheGrace() throws Exception {
        run("fatal-close-hangs", false, 5_000, List.of(1), EngineLifecycle.FATAL_EXIT_GRACE_ENV, "300");
    }

    @Test
    public void absentDriverExitsOneWithinTheDriverWait() throws Exception {
        run("no-driver", false, 5_000, List.of(1), EngineLifecycle.DRIVER_WAIT_ENV, "500");
    }

    @Test
    public void clusterTerminationExitsZero() throws Exception {
        run("cluster-terminated", false, 10_000, List.of(0));
    }

    @Test
    public void malformedSettingRefusesTheStartWithOne() throws Exception {
        final Path log = Files.createTempFile("lifecycle-badsetting-", ".log");
        final Process p = child(log, "close-fast", EngineLifecycle.SHUTDOWN_TIMEOUT_ENV, "soon");
        try {
            final long[] r = exitAfter(p, System.nanoTime(), 10_000);
            final String out = Files.readString(log);
            assertEquals(out, 1, r[0]);
            assertTrue(out, out.contains("ENGINE_SHUTDOWN_TIMEOUT_MS must be a positive number"));
        } finally {
            p.destroyForcibly();
            Files.deleteIfExists(log);
        }
    }

    @Test
    public void malformedDriverWaitRefusesTheStartInEveryDriverMode() throws Exception {
        // the child never waits for a driver (embedded shape): install() alone must refuse the value
        final Path log = Files.createTempFile("lifecycle-baddriverwait-", ".log");
        final Process p = child(log, "close-fast", EngineLifecycle.DRIVER_WAIT_ENV, "0");
        try {
            final long[] r = exitAfter(p, System.nanoTime(), 10_000);
            final String out = Files.readString(log);
            assertEquals(out, 1, r[0]);
            assertTrue(out, out.contains("TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS must be a positive number"));
        } finally {
            p.destroyForcibly();
            Files.deleteIfExists(log);
        }
    }

    /** Child: the engine-main shape. */
    public static void main(final String[] args) throws Exception {
        final String mode = args[0];
        final EngineLifecycle lifecycle;
        try {
            lifecycle = EngineLifecycle.install("child", System.out::println);
        } catch (final IllegalArgumentException e) {
            // The engines construct the lifecycle first; a refused setting is a refused start.
            System.out.println(e.getMessage());
            Runtime.getRuntime().halt(1);
            return;
        }
        final Thread app = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1_000);
                } catch (final InterruptedException ignored) {
                    // a non-daemon application thread that ignores everything
                }
            }
        }, "non-daemon-app");
        app.start();
        try {
            if (mode.equals("no-driver")) {
                final Path dir = Files.createTempDirectory("lifecycle-nodriver-");
                System.out.println("READY");
                lifecycle.awaitExternalDriver(dir.toString(), EngineLifecycle.driverWaitMs());
            }
            System.out.println("READY");
            switch (mode) {
                case "fatal", "fatal-close-hangs" -> new Thread(() -> lifecycle.onError(
                        new AeronException("fatal", AeronException.Category.FATAL), true)).start();
                case "driver-lost" -> new Thread(() -> {
                    lifecycle.onError(
                            new DriverTimeoutException("MediaDriver keepalive: age=10090ms > timeout=10000ms"), true);
                    System.out.println("ERROR HANDLER RETURNED");
                }).start();
                case "cluster-terminated" -> new Thread(lifecycle::onClusterTermination).start();
                default -> { }
            }
            lifecycle.awaitStop();
            if (mode.endsWith("close-hangs")) {
                Thread.sleep(60_000);
            }
            Thread.sleep(50); // the components' close
        } catch (final Throwable t) {
            lifecycle.startupFailed(t);
            return;
        }
        lifecycle.finish();
    }
}
