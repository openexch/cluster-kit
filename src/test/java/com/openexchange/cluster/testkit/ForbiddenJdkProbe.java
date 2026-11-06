// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

/** Review positive control: JDK and Agrona ways to schedule, read a clock or read the environment. */
@SuppressWarnings("unused")
public final class ForbiddenJdkProbe {
    private java.util.concurrent.ScheduledExecutorService scheduler;
    private java.util.Timer timer;
    private java.time.Clock clock;
    private org.agrona.concurrent.EpochNanoClock nanoClock;

    public static long all() throws InterruptedException {
        Thread.sleep(0);
        final long a = java.time.Instant.now().toEpochMilli();
        final long b = Long.getLong("x", 0L) + Integer.getInteger("y", 0) + (Boolean.getBoolean("z") ? 1 : 0);
        final int c = System.getProperties().size();
        final long d = new org.agrona.concurrent.SystemNanoClock().nanoTime();
        return a + b + c + d;
    }
}
