// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.IdleStrategy;

/**
 * Reads a snapshot image to its end. A replay may not have delivered its first fragment yet, and may pause
 * between chunks; stopping at the first empty poll restores nothing, or a truncated payload. This reader
 * polls until {@code isEndOfStream()}, drains what came with it, and throws when the image closes first, so
 * a missing or cut snapshot is never mistaken for an empty or a complete state. The loop the assets engine
 * runs, moved once into the kit behind a {@link Source} seam so a test can script an image.
 */
public final class SnapshotImageReader {

    public static final int FRAGMENT_LIMIT = 64;

    /** The four image calls the reader uses. */
    public interface Source {
        int poll(FragmentHandler handler, int fragmentLimit);

        boolean isEndOfStream();

        boolean isClosed();

        long position();
    }

    private SnapshotImageReader() {
    }

    /** Adapter for a live Aeron snapshot image. */
    public static Source of(final Image image) {
        return new Source() {
            public int poll(final FragmentHandler handler, final int fragmentLimit) {
                return image.poll(handler, fragmentLimit);
            }

            public boolean isEndOfStream() {
                return image.isEndOfStream();
            }

            public boolean isClosed() {
                return image.isClosed();
            }

            public long position() {
                return image.position();
            }
        };
    }

    /** What one read delivered. */
    public record Result(long bytes, int polls, long position) {
    }

    /**
     * Poll {@code source} into {@code handler} until end of stream, then drain the fragments delivered with
     * it. An empty poll on a closed image throws {@link IllegalStateException} naming the bytes received and
     * the image position; any other empty poll idles and polls again.
     */
    public static Result readFully(final Source source, final FragmentHandler handler, final IdleStrategy idleStrategy) {
        final long[] bytes = {0};
        final FragmentHandler counting = (buffer, offset, length, header) -> {
            bytes[0] += length;
            handler.onFragment(buffer, offset, length, header);
        };
        idleStrategy.reset();
        int polls = 0;
        while (true) {
            final int fragments = source.poll(counting, FRAGMENT_LIMIT);
            polls++;
            if (source.isEndOfStream()) {
                while (source.poll(counting, FRAGMENT_LIMIT) > 0) {
                    polls++;
                }
                return new Result(bytes[0], polls, source.position());
            }
            if (fragments == 0 && source.isClosed()) {
                throw new IllegalStateException("snapshot image closed before end-of-stream: " + bytes[0]
                        + " byte(s) received at position " + source.position());
            }
            idleStrategy.idle(fragments);
        }
    }
}
