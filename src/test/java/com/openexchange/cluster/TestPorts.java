// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * UDP ports for the test rigs, taken BELOW the kernel's ephemeral range.
 *
 * <p>A port the kernel handed out for {@code bind(0)} and the test then released lies inside the ephemeral range, so
 * the kernel can hand it again to the next {@code bind(0)} before the rig binds it: every {@code endpoint=localhost:0}
 * channel (archive control responses, replication, egress) of a member started first, or any other process on the
 * box. The later bind then fails with {@code Address already in use}. A port below the range is never given to a
 * {@code bind(0)}, so only another explicit bind of that same port can take it. Each port is checked free when picked,
 * and a JVM-wide cursor never hands the same port out twice in a row of calls.</p>
 */
public final class TestPorts {

    /** The lowest port handed out. */
    static final int LOW = 10_000;
    private static final AtomicInteger CURSOR = new AtomicInteger(-1);

    private TestPorts() {
    }

    /** The first port of the kernel's ephemeral range ({@code net.ipv4.ip_local_port_range}; 32768 when unknown). */
    public static int ephemeralLow() {
        try {
            // a line read, not Files.readString: procfs reports size 0 and a size-driven read can come back short
            final String[] range;
            final Path file = Path.of("/proc/sys/net/ipv4/ip_local_port_range");
            try (java.io.BufferedReader r = Files.newBufferedReader(file)) {
                range = r.readLine().trim().split("\\s+");
            }
            final int low = Integer.parseInt(range[0]);
            return low > LOW ? low : 32_768;
        } catch (final IOException | RuntimeException unknown) {
            return 32_768;
        }
    }

    /** One free UDP port below the ephemeral range. */
    public static int freeUdpPort() {
        return freeUdpPorts(1)[0];
    }

    /** {@code n} distinct UDP ports below the ephemeral range, each free when picked. */
    public static int[] freeUdpPorts(final int n) {
        final int high = ephemeralLow();
        final int span = high - LOW;
        if (span < 10 * n) {
            throw new IllegalStateException("the ephemeral range starts at " + high + ": no room below it for " + n
                    + " test ports");
        }
        CURSOR.compareAndSet(-1, ThreadLocalRandom.current().nextInt(span));
        final int[] ports = new int[n];
        int found = 0;
        for (int tries = 0; found < n && tries < span; tries++) {
            final int p = LOW + Math.floorMod(CURSOR.getAndIncrement(), span);
            if (free(p)) {
                ports[found++] = p;
            }
        }
        if (found < n) {
            throw new IllegalStateException("no " + n + " free UDP ports in [" + LOW + ", " + high + ")");
        }
        return ports;
    }

    private static boolean free(final int port) {
        try (DatagramSocket ignored = new DatagramSocket(port)) {
            return true;
        } catch (final IOException taken) {
            return false;
        }
    }
}
