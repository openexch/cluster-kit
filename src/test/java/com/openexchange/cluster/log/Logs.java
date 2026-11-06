// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import io.aeron.Image;

/** Test helpers shared by the reader tests. */
final class Logs {

    static final int TERM = 64 * 1024;
    static final int MTU = 1408;
    static final int MAX_PAYLOAD = MTU - 32;
    static final int APP_VERSION = org.agrona.SemanticVersion.compose(0, 0, 1);

    private Logs() {
    }

    /** Polls until the reader has consumed {@code to}, the image ends, or 10 s pass. */
    static void drain(final ClusterLogReader reader, final Image image, final long to) {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (reader.consumedPosition() < to) {
            final int n = reader.poll(image, to);
            if (n == 0) {
                if (image.isEndOfStream() || image.isClosed()) {
                    return;
                }
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("reader did not reach " + to + " (at " + reader.consumedPosition() + ")");
                }
                Thread.yield();
            }
        }
    }

    /** Polls with {@code bound} until {@code done} holds (replays arrive asynchronously), or 10 s pass. */
    static void pollUntil(final ClusterLogReader reader, final Image image, final long bound,
                          final java.util.function.BooleanSupplier done) {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (!done.getAsBoolean()) {
            reader.poll(image, bound);
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not reached; reader at " + reader.consumedPosition());
            }
            Thread.yield();
        }
    }

    static int aligned(final int length) {
        return (length + 31) & -32;
    }

    /** Frame end of the BEGIN fragment of a message that starts at {@code start} and is longer than one MTU. */
    static long beginEnd(final long start) {
        return start + MTU;
    }

    /** Start of the END fragment of a fragmented message {@code messageLength} long (session header included). */
    static long endFragmentStart(final long messageEnd, final int messageLength) {
        final int fragments = (messageLength + MAX_PAYLOAD - 1) / MAX_PAYLOAD;
        final int last = messageLength - (fragments - 1) * MAX_PAYLOAD;
        return messageEnd - aligned(32 + last);
    }
}
