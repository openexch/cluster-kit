// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * The boot probe of the socket buffers the kernel grants a UDP socket. A per-channel {@code so-rcvbuf} or
 * {@code so-sndbuf} on an Aeron channel URI is set with {@code setOption} and never read back, and the kernel caps it
 * at {@code net.core.rmem_max} / {@code net.core.wmem_max} without an error; Aeron's own startup warning compares the
 * driver CONTEXT value against the OS maximum and never sees a URI value. So a host probes before its driver is
 * launched or connected, with a scratch socket as Aeron's own probe does: {@code setOption(SO_RCVBUF, configured)},
 * then {@code getOption}; the same for {@code SO_SNDBUF}. The kernel limits are kernel-wide, so the probe measures
 * what the log socket will get under an embedded driver and under an external driver on the same host alike.
 *
 * <p>The rule: refuse iff {@code granted < configured}. The kernel doubles a socket buffer request for its own
 * bookkeeping and {@code getsockopt} reports the doubled value (socket(7)), but the JDK halves {@code SO_RCVBUF} and
 * {@code SO_SNDBUF} on Linux before it returns them, so through {@code getOption} the probe reads
 * {@code min(request, max)}: it refuses exactly when {@code net.core.rmem_max} (or {@code wmem_max}) is below the
 * configured value, and the text asks for {@code max >= configured}. Aeron's own probe reads the OS maximum through
 * the same JDK call.</p>
 */
public final class SocketBufferProbe {

    /** {@code net.core.rmem_max}. */
    public static final String RMEM_MAX = "net.core.rmem_max";
    /** {@code net.core.wmem_max}. */
    public static final String WMEM_MAX = "net.core.wmem_max";

    /** The kernel as the probe sees it; a fake in tests. */
    public interface Kernel {
        /** The SO_RCVBUF the kernel reports after a request of {@code requested} bytes. */
        long grantedRcvbuf(long requested) throws IOException;

        /** The SO_SNDBUF the kernel reports after a request of {@code requested} bytes. */
        long grantedSndbuf(long requested) throws IOException;

        /** The current value of a sysctl ({@code net.core.rmem_max}), or {@code unknown}. */
        String sysctl(String name);
    }

    /** A scratch IPv4 UDP socket and {@code /proc/sys} (Linux; {@code unknown} elsewhere). */
    public static final Kernel SYSTEM = new Kernel() {
        @Override
        public long grantedRcvbuf(final long requested) throws IOException {
            try (DatagramChannel c = DatagramChannel.open(StandardProtocolFamily.INET)) {
                c.setOption(StandardSocketOptions.SO_RCVBUF, (int)Math.min(Integer.MAX_VALUE, requested));
                return c.getOption(StandardSocketOptions.SO_RCVBUF);
            }
        }

        @Override
        public long grantedSndbuf(final long requested) throws IOException {
            try (DatagramChannel c = DatagramChannel.open(StandardProtocolFamily.INET)) {
                c.setOption(StandardSocketOptions.SO_SNDBUF, (int)Math.min(Integer.MAX_VALUE, requested));
                return c.getOption(StandardSocketOptions.SO_SNDBUF);
            }
        }

        @Override
        public String sysctl(final String name) {
            final Path p = Path.of("/proc/sys", name.replace('.', '/'));
            // a procfs file reports size 0: read the stream (Files.readString reads by size and returns one byte)
            try (java.io.InputStream in = Files.newInputStream(p)) {
                final String v = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.US_ASCII).trim();
                return v.isEmpty() ? "unknown" : v;
            } catch (final IOException | RuntimeException e) {
                return "unknown";
            }
        }
    };

    /** One buffer to probe: the setting's name, its text as configured, its bytes, and what else to lower. */
    public record Request(String setting, String text, long bytes, String sysctl, String alsoLower) {
        public Request {
            Objects.requireNonNull(setting, "setting");
            Objects.requireNonNull(text, "text");
            Objects.requireNonNull(sysctl, "sysctl");
            if (bytes <= 0) {
                throw new IllegalArgumentException(setting + " must be > 0 B: " + bytes);
            }
        }
    }

    /** What the probe measured for one request. */
    public record Result(Request request, long granted, String current) {
        public boolean refused() {
            return granted < request.bytes();
        }

        /**
         * {@code <setting>=<text> (<bytes> B) but the kernel granted <granted> B (<sysctl>=<current>): set <sysctl> >=
         * <bytes> (sysctl -w <sysctl>=<bytes>, persist in /etc/sysctl.d) or lower <setting>[ and <alsoLower>]}.
         */
        public String refusalText() {
            final Request r = request;
            return r.setting() + "=" + r.text() + " (" + r.bytes() + " B) but the kernel granted " + granted + " B ("
                    + r.sysctl() + "=" + current + "): set " + r.sysctl() + " >= " + r.bytes() + " (sysctl -w "
                    + r.sysctl() + "=" + r.bytes() + ", persist in /etc/sysctl.d) or lower " + r.setting()
                    + (r.alsoLower() == null || r.alsoLower().isEmpty() ? "" : " and " + r.alsoLower());
        }

        /** {@code <setting> requested <bytes> B, granted <granted> B (<sysctl>=<current>)}: the boot line. */
        public String line() {
            return request.setting() + " requested " + request.bytes() + " B, granted " + granted + " B ("
                    + request.sysctl() + "=" + current + ")";
        }
    }

    private SocketBufferProbe() {
    }

    /** Probes the receive buffer request. */
    public static Result rcvbuf(final Kernel kernel, final Request request) throws IOException {
        return new Result(request, kernel.grantedRcvbuf(request.bytes()), kernel.sysctl(request.sysctl()));
    }

    /** Probes the send buffer request. */
    public static Result sndbuf(final Kernel kernel, final Request request) throws IOException {
        return new Result(request, kernel.grantedSndbuf(request.bytes()), kernel.sysctl(request.sysctl()));
    }

    /**
     * Probes both and returns the first refusal text (receive first), or null when the kernel granted both in full.
     *
     * @throws IOException when a scratch socket cannot be opened (the host refuses the boot with it: unverifiable)
     */
    public static String refusal(final Kernel kernel, final Request rcv, final Request snd) throws IOException {
        return refusal(kernel, rcv, snd, false);
    }

    /** The negative-control seam: {@code probeOff} answers null whatever the kernel granted (tests only). */
    static String refusal(final Kernel kernel, final Request rcv, final Request snd, final boolean probeOff)
            throws IOException {
        if (probeOff) {
            return null;
        }
        final Result r = rcvbuf(kernel, rcv);
        if (r.refused()) {
            return r.refusalText();
        }
        final Result s = sndbuf(kernel, snd);
        return s.refused() ? s.refusalText() : null;
    }
}
