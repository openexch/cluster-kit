// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * The cost of the SHA-256 payload digest at the shadow test image sizes (the core shadow of the round trip, the
 * 386 KiB concurrent-replay image, the 600 KiB budget images) and at a measured 100k-order snapshot (38.6 MB), over an
 * off-heap buffer as the writer uses it. Prints {@code COST} lines; asserts nothing about speed (measure, do not tune).
 * The writer digests the payload twice (before and after the self-check).
 */
public class PayloadDigestCostTest {

    @Test
    public void measureShaThroughputAtTheShadowImageSizes() {
        final int[] sizes = {57_847, 395_264, 614_400, 38_600_000};
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(sizes[sizes.length - 1]));
        final byte[] fill = new byte[1 << 16];
        new Random(7).nextBytes(fill);
        for (int off = 0; off < buffer.capacity(); off += fill.length) {
            buffer.putBytes(off, fill, 0, Math.min(fill.length, buffer.capacity() - off));
        }
        long sink = 0;
        for (final int size : sizes) {
            final int runs = size > 10_000_000 ? 7 : 101;
            final long[] ns = new long[runs];
            for (int r = 0; r < runs; r++) {
                final long t0 = System.nanoTime();
                sink += PayloadDigest.of(buffer, 0, size);
                ns[r] = System.nanoTime() - t0;
            }
            Arrays.sort(ns);
            final long median = ns[runs / 2];
            final double mbs = size / 1e6 / (median / 1e9);
            System.out.printf("COST sha256 payload digest: %,d B median %.3f ms = %.0f MB/s (best %.3f ms); writer"
                    + " (2 passes) %.2f ms%n", size, median / 1e6, mbs, ns[0] / 1e6, 2 * median / 1e6);
            assertTrue(mbs > 0);
        }
        assertTrue(sink != 1);
    }
}
