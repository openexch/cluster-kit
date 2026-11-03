// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.CncFileDescriptor;
import io.aeron.exceptions.AeronException;
import org.agrona.concurrent.AgentTerminationException;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Process lifecycle of an engine node (match and assets run the same code): how the node stops and which exit
 * status the process manager sees.
 *
 * <p>Rules (RC2 S16 W9, PO 2026-11-03):
 * <ul>
 *   <li>A fatal fault (a FATAL Aeron error with an external driver, i.e. the media driver died; an agent that
 *       terminated unexpectedly; a startup failure such as no CnC within the driver wait) exits with a non-zero
 *       status, never 0: the components are closed on the main thread, then the process halts with the fault's
 *       status; a daemon backstop halts with the same status after the fatal grace if the close hangs.</li>
 *   <li>SIGTERM (any JVM shutdown) stops the node cleanly: the JVM shutdown hook wakes the main thread, which
 *       closes the components, and waits at most the shutdown timeout for that close; the JVM then exits with the
 *       signal's status (143), whether or not the close finished.</li>
 *   <li>A cluster-requested termination (the consensus module / service container termination hook with no fault
 *       recorded) exits 0.</li>
 *   <li>The first stop cause wins, except that a fault upgrades a cluster termination (Aeron runs the termination
 *       hook before it reports the unexpected agent termination behind it). Nothing upgrades a signal stop.</li>
 * </ul>
 *
 * <p>This replaces Agrona's {@code ShutdownSignalBarrier} in the engines. That barrier's static JVM hook waits,
 * without a bound, for {@code close()} on every barrier it signalled, and the engines never called it: SIGTERM and
 * an uncaught startup exception both hung the JVM (agrona 2.6.0 {@code ShutdownSignalBarrier.java:96-100, 244-279}),
 * and a fault that let the main thread return ended the JVM with status 0 before the halt backstop ran.
 *
 * <p>All bounds are process settings (environment first, then system property, then the default), never
 * replicated state; a malformed or non-positive value refuses the start.
 */
public final class EngineLifecycle {
    /** Bound on the clean close after SIGTERM: below the console's 5 s cluster-node StopTimeout. */
    public static final String SHUTDOWN_TIMEOUT_ENV = "ENGINE_SHUTDOWN_TIMEOUT_MS";
    public static final String SHUTDOWN_TIMEOUT_PROP = "engine.shutdown.timeout.ms";
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 4_000;
    /** After a fatal fault: how long the close may take before the backstop halts with the fault's status. */
    public static final String FATAL_EXIT_GRACE_ENV = "ENGINE_FATAL_EXIT_GRACE_MS";
    public static final String FATAL_EXIT_GRACE_PROP = "engine.fatal.exit.grace.ms";
    public static final long DEFAULT_FATAL_EXIT_GRACE_MS = 5_000;
    /** External driver mode: how long the start waits for the driver's cnc.dat before it exits 1. */
    public static final String DRIVER_WAIT_ENV = "TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS";
    public static final String DRIVER_WAIT_PROP = "transport.external.driver.timeout.ms";
    public static final long DEFAULT_DRIVER_WAIT_MS = 30_000;

    /** Status of a fatal fault (driver death, unexpected agent termination, startup failure); C13 per PO-3. */
    public static final int EXIT_FATAL = 1;
    /** Status of a cluster-requested termination. */
    public static final int EXIT_CLUSTER_TERMINATED = 0;

    /** Why the node is stopping. */
    public enum Cause { NONE, SIGNAL, CLUSTER_TERMINATED, FATAL }

    private record Stop(Cause cause, int status, String reason) { }

    private static final Stop RUNNING = new Stop(Cause.NONE, -1, "");

    private final String name;
    private final Consumer<String> log;
    private final IntConsumer halter;
    private final long shutdownTimeoutMs;
    private final long fatalExitGraceMs;
    private final AtomicReference<Stop> stop = new AtomicReference<>(RUNNING);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private final AtomicBoolean halted = new AtomicBoolean();
    /** Set when a component close threw during the stop, so a signal stop is not reported clean. */
    private volatile String closeFailure;

    EngineLifecycle(final String name, final Consumer<String> log, final IntConsumer halter,
                    final long shutdownTimeoutMs, final long fatalExitGraceMs) {
        if (shutdownTimeoutMs <= 0 || fatalExitGraceMs <= 0) {
            throw new IllegalArgumentException("lifecycle bounds must be positive");
        }
        this.name = name;
        this.log = log;
        this.halter = halter;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.fatalExitGraceMs = fatalExitGraceMs;
    }

    /**
     * The production lifecycle: bounds from the settings, {@link Runtime#halt(int)} as the exit, and a JVM shutdown
     * hook registered for the signal stop. Call first in the node's bootstrap, before any thread starts.
     */
    public static EngineLifecycle install(final String name, final Consumer<String> log) {
        // All three settings are parsed here, in every driver mode: a malformed one refuses the start.
        final long shutdownMs = shutdownTimeoutMs();
        final long graceMs = fatalExitGraceMs();
        driverWaitMs();
        final EngineLifecycle lifecycle = new EngineLifecycle(name, log, status -> Runtime.getRuntime().halt(status),
                shutdownMs, graceMs);
        Runtime.getRuntime().addShutdownHook(new Thread(lifecycle::onJvmShutdown, name + "-shutdown"));
        return lifecycle;
    }

    public static long shutdownTimeoutMs() {
        return positiveSetting(SHUTDOWN_TIMEOUT_ENV, SHUTDOWN_TIMEOUT_PROP, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    public static long fatalExitGraceMs() {
        return positiveSetting(FATAL_EXIT_GRACE_ENV, FATAL_EXIT_GRACE_PROP, DEFAULT_FATAL_EXIT_GRACE_MS);
    }

    public static long driverWaitMs() {
        return positiveSetting(DRIVER_WAIT_ENV, DRIVER_WAIT_PROP, DEFAULT_DRIVER_WAIT_MS);
    }

    static long positiveSetting(final String env, final String prop, final long defaultValue) {
        return parsePositive(env, System.getenv(env), prop, System.getProperty(prop), defaultValue);
    }

    static long parsePositive(final String env, final String envValue, final String prop, final String propValue,
                              final long defaultValue) {
        final String raw;
        final String source;
        if (envValue != null && !envValue.isEmpty()) {
            raw = envValue;
            source = env;
        } else if (propValue != null && !propValue.isEmpty()) {
            raw = propValue;
            source = prop;
        } else {
            return defaultValue;
        }
        final long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(source + " must be a positive number of milliseconds, got '" + raw + "'");
        }
        if (value <= 0) {
            throw new IllegalArgumentException(source + " must be a positive number of milliseconds, got '" + raw + "'");
        }
        return value;
    }

    public Cause cause() {
        return stop.get().cause();
    }

    public boolean stopRequested() {
        return stopLatch.getCount() == 0;
    }

    /**
     * Main thread: block until a stop cause is recorded. An interrupt has no JVM shutdown behind it, so it is a fault
     * (exit 1), not a signal stop.
     */
    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (final InterruptedException e) {
            fatal(EXIT_FATAL, "main thread interrupted");
            Thread.currentThread().interrupt();
        }
    }

    /** The consensus module's and the service container's termination hook. */
    public void onClusterTermination() {
        record(new Stop(Cause.CLUSTER_TERMINATED, EXIT_CLUSTER_TERMINATED, "cluster termination hook"));
    }

    /**
     * A fatal fault: record it (unless a signal stop is already in progress), wake the main thread and start the
     * backstop that halts with {@code status} after the fatal grace. Returns true when this call recorded the fault.
     */
    public boolean fatal(final int status, final String reason) {
        if (status == 0) {
            throw new IllegalArgumentException("a fatal fault never exits 0");
        }
        final Stop fault = new Stop(Cause.FATAL, status, reason);
        if (!record(fault)) {
            return false;
        }
        log.accept(name + " FATAL (exit " + status + "): " + reason + " - node exiting, backstop in "
                + fatalExitGraceMs + " ms");
        final Thread backstop = new Thread(() -> {
            if (!awaitQuietly(closedLatch, fatalExitGraceMs)) {
                log.accept(name + " fatal close exceeded " + fatalExitGraceMs + " ms - halting with " + status);
            }
            halt(status);
        }, name + "-fatal-exit");
        backstop.setDaemon(true);
        backstop.start();
        return true;
    }

    /**
     * Classify an error the node's Aeron error handler saw. A FATAL Aeron error with an external driver (the driver
     * died or stopped answering) and an agent that terminated unexpectedly are fatal faults (exit 1); everything
     * else is only logged by the caller.
     */
    public boolean onError(final Throwable t, final boolean externalDriver) {
        if (externalDriver && t instanceof AeronException ae && ae.category() == AeronException.Category.FATAL) {
            return fatal(EXIT_FATAL, "FATAL Aeron error in EXTERNAL driver mode: " + t);
        }
        if (t instanceof AgentTerminationException ate && !ate.isExpected()) {
            return fatal(EXIT_FATAL, "agent terminated unexpectedly: " + t);
        }
        return false;
    }

    /**
     * A failure before or during the launch (no CnC within the driver wait, a launch exception): print it, then
     * finish. Unless a signal stop is in progress this halts with {@link #EXIT_FATAL}.
     */
    public void startupFailed(final Throwable t) {
        final StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        log.accept(name + " failed: " + sw);
        closeFailure = String.valueOf(t);
        fatal(EXIT_FATAL, "start, launch or close failed: " + t);
        finish();
    }

    /**
     * Main thread, after the components are closed: decide the exit. A fault halts with its status, a cluster
     * termination halts with 0; a signal stop returns (the JVM is already exiting with the signal's status).
     */
    public void finish() {
        final Stop s = stop.get();
        // Log before releasing the shutdown hook: once it returns the JVM may exit at any instant.
        final int status = switch (s.cause()) {
            case SIGNAL -> {
                final String failure = closeFailure;
                log.accept(failure == null ? name + " stopped cleanly on " + s.reason()
                        : name + " stopped on SIGTERM, close failed: " + failure);
                yield -1;
            }
            case FATAL -> {
                log.accept(name + " closed after FATAL: " + s.reason() + " - exit " + s.status());
                yield s.status();
            }
            case CLUSTER_TERMINATED -> {
                log.accept(name + " closed after " + s.reason() + " - exit " + s.status());
                yield s.status();
            }
            case NONE -> {
                // The main thread got here without a stop cause: the node is not running, so it must not look clean.
                log.accept(name + " main thread finished without a stop cause - exit " + EXIT_FATAL);
                yield EXIT_FATAL;
            }
        };
        System.out.flush();
        System.err.flush();
        closedLatch.countDown();
        if (status >= 0) {
            halt(status);
        }
    }

    /** JVM shutdown hook body (SIGTERM, SIGINT, System.exit): wake the main thread and bound its close. */
    void onJvmShutdown() {
        record(new Stop(Cause.SIGNAL, -1, "JVM shutdown (SIGTERM)"));
        if (!awaitQuietly(closedLatch, shutdownTimeoutMs)) {
            log.accept(name + " clean close exceeded " + shutdownTimeoutMs + " ms (" + SHUTDOWN_TIMEOUT_ENV
                    + ") - exiting without it");
        }
        // A fault recorded before the signal keeps its status: returning would let the JVM exit 143 before the
        // backstop halts with the fault's status.
        final Stop s = stop.get();
        if (s.cause() == Cause.FATAL) {
            halt(s.status());
        }
    }

    /**
     * External driver mode: wait for the driver's cnc.dat for at most {@code timeoutMs}; throws on timeout or when a
     * stop is requested meanwhile.
     */
    public void awaitExternalDriver(final String aeronDir, final long timeoutMs) {
        final File cncFile = new File(aeronDir, CncFileDescriptor.CNC_FILE);
        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!cncFile.exists()) {
            if (stopRequested()) {
                throw new IllegalStateException("stop requested while waiting for the external media driver at "
                        + aeronDir);
            }
            if (System.nanoTime() - deadlineNs > 0) {
                throw new IllegalStateException("No external media driver found at " + aeronDir + " after "
                        + timeoutMs + "ms (" + DRIVER_WAIT_ENV + ")");
            }
            try {
                stopLatch.await(100, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for external media driver at " + aeronDir, e);
            }
        }
    }

    private boolean record(final Stop next) {
        while (true) {
            final Stop current = stop.get();
            final boolean allowed = current.cause() == Cause.NONE
                    || (current.cause() == Cause.CLUSTER_TERMINATED && next.cause() == Cause.FATAL);
            if (!allowed) {
                return false;
            }
            if (stop.compareAndSet(current, next)) {
                stopLatch.countDown();
                return true;
            }
        }
    }

    /** One halt per process: the main thread's and the backstop's are the same decision. */
    private void halt(final int status) {
        if (!halted.compareAndSet(false, true)) {
            return;
        }
        System.out.flush();
        System.err.flush();
        halter.accept(status);
    }

    private static boolean awaitQuietly(final CountDownLatch latch, final long timeoutMs) {
        try {
            return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return latch.getCount() == 0;
        }
    }
}
