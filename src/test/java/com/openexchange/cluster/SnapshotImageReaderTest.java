// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC2 S6 9.1 (M Q12): the kit snapshot loader waits for end of stream. */
public class SnapshotImageReaderTest {

    /** One scripted poll: the fragments it delivers and whether end of stream / close holds after it. */
    private record Step(String[] fragments, boolean eos, boolean closed) {
    }

    private static Step frags(final String... f) {
        return new Step(f, false, false);
    }

    private static Step eos(final String... f) {
        return new Step(f, true, false);
    }

    private static Step closed() {
        return new Step(new String[0], false, true);
    }

    /** A scripted image: each poll consumes one step; EOS fragments become available on the poll that sets EOS. */
    private static class Script implements SnapshotImageReader.Source {
        final Deque<Step> steps = new ArrayDeque<>();
        boolean eos;
        boolean closed;
        long position;
        final Deque<String> trailing = new ArrayDeque<>();

        Script(final Step... s) {
            for (Step x : s) {
                steps.add(x);
            }
        }

        public int poll(final FragmentHandler handler, final int limit) {
            if (!trailing.isEmpty()) {
                final String f = trailing.poll();
                deliver(handler, f);
                return 1;
            }
            final Step s = steps.poll();
            if (s == null) {
                return 0;
            }
            int n = 0;
            for (String f : s.fragments) {
                if (n < limit) {
                    deliver(handler, f);
                    n++;
                }
            }
            eos |= s.eos;
            closed |= s.closed;
            return n;
        }

        private void deliver(final FragmentHandler handler, final String f) {
            final byte[] b = f.getBytes(StandardCharsets.US_ASCII);
            handler.onFragment(new UnsafeBuffer(b), 0, b.length, null);
            position += b.length;
        }

        public boolean isEndOfStream() {
            return eos;
        }

        public boolean isClosed() {
            return closed;
        }

        public long position() {
            return position;
        }
    }

    private static String read(final SnapshotImageReader.Source s) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        SnapshotImageReader.readFully(s, (buffer, offset, length, header) -> {
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b);
            out.writeBytes(b);
        }, NoOpIdleStrategy.INSTANCE);
        return out.toString(StandardCharsets.US_ASCII);
    }

    @Test
    public void emptyStartThenChunksYieldsTheWholePayload() {
        assertEquals("AB", read(new Script(frags(), frags("A"), frags(), frags(), eos("B"))));
    }

    @Test
    public void aPauseBetweenChunksIsNotTheEnd() {
        assertEquals("AB", read(new Script(frags("A"), frags(), eos("B"))));
    }

    @Test
    public void fragmentsDeliveredWithEndOfStreamAreDrained() {
        // the trailing fragments become visible only after the EOS poll
        final Script late = new Script(frags("A"), eos("B")) {
            boolean armed;

            @Override
            public int poll(final FragmentHandler handler, final int limit) {
                if (eos && !armed) {
                    armed = true;
                    trailing.add("C");
                    trailing.add("D");
                }
                return super.poll(handler, limit);
            }
        };
        assertEquals("ABCD", read(late));
    }

    @Test
    public void closedWithoutEndOfStreamThrowsNamingBytesAndPosition() {
        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> read(new Script(frags("AAA"), frags(), closed())));
        assertTrue(e.getMessage(), e.getMessage().contains("3 byte(s)") && e.getMessage().contains("position 3"));
    }

    @Test
    public void reportsBytesAndPosition() {
        final SnapshotImageReader.Result r = SnapshotImageReader.readFully(new Script(frags("AB"), eos("CDE")),
                (buffer, offset, length, header) -> { }, NoOpIdleStrategy.INSTANCE);
        assertEquals(5, r.bytes());
        assertEquals(5, r.position());
    }
}
