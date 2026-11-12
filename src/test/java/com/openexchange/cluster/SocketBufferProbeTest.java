// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The kernel socket buffer probe: a fake kernel that doubles and caps as Linux does, the refusal text naming the
 * sysctl, the current and the needed value, and the real kernel of the box the test runs on. The negative control
 * switches the probe off and the row's own assertion fails with the named text.
 */
public class SocketBufferProbeTest {

    static final long M16 = 16L * 1024 * 1024;
    static final SocketBufferProbe.Request RCV = new SocketBufferProbe.Request("TRANSPORT_LOG_SO_RCVBUF", "16m", M16,
            SocketBufferProbe.RMEM_MAX, "TRANSPORT_LOG_RCV_WINDOW");
    static final SocketBufferProbe.Request SND = new SocketBufferProbe.Request("TRANSPORT_LOG_SO_SNDBUF", "16m", M16,
            SocketBufferProbe.WMEM_MAX, null);

    /** Linux as the JDK reports it: the kernel doubles, the JDK halves, so min(request, max). */
    static SocketBufferProbe.Kernel linux(final long rmemMax, final long wmemMax) {
        final LongUnaryOperator r = req -> Math.min(req, rmemMax);
        final LongUnaryOperator w = req -> Math.min(req, wmemMax);
        return new SocketBufferProbe.Kernel() {
            @Override
            public long grantedRcvbuf(final long requested) {
                return r.applyAsLong(requested);
            }

            @Override
            public long grantedSndbuf(final long requested) {
                return w.applyAsLong(requested);
            }

            @Override
            public String sysctl(final String name) {
                return String.valueOf(name.equals(SocketBufferProbe.RMEM_MAX) ? rmemMax : wmemMax);
            }
        };
    }

    static String cappedRefused(final boolean probeOff) throws IOException {
        final String text = SocketBufferProbe.refusal(linux(4_194_304, M16), RCV, SND, probeOff);
        if (text == null) {
            throw new AssertionError("a capped socket buffer booted silently: rmem_max=4194304, request " + M16);
        }
        return text;
    }

    @Test
    public void aCappedReceiveBufferIsRefusedNamingTheSysctl() throws IOException {
        assertEquals("TRANSPORT_LOG_SO_RCVBUF=16m (16777216 B) but the kernel granted 4194304 B"
                + " (net.core.rmem_max=4194304): set net.core.rmem_max >= 16777216 (sysctl -w"
                + " net.core.rmem_max=16777216, persist in /etc/sysctl.d) or lower TRANSPORT_LOG_SO_RCVBUF and"
                + " TRANSPORT_LOG_RCV_WINDOW", cappedRefused(false));
        try {
            cappedRefused(true);
            fail("negative control SOCKET_PROBE_OFF did not fail");
        } catch (final AssertionError e) {
            assertTrue(e.getMessage(), e.getMessage().contains("a capped socket buffer booted silently"));
            System.out.println("NC SOCKET_PROBE_OFF -> " + e.getMessage());
        }
    }

    @Test
    public void theSendBufferAndTheLinuxBoundary() throws IOException {
        assertEquals("TRANSPORT_LOG_SO_SNDBUF=16m (16777216 B) but the kernel granted 2097152 B"
                + " (net.core.wmem_max=2097152): set net.core.wmem_max >= 16777216 (sysctl -w"
                + " net.core.wmem_max=16777216, persist in /etc/sysctl.d) or lower TRANSPORT_LOG_SO_SNDBUF",
                SocketBufferProbe.refusal(linux(M16, 2_097_152), RCV, SND));
        // granted = min(req, max) through the JDK: max = configured is the boundary
        assertNull(SocketBufferProbe.refusal(linux(M16, M16), RCV, SND));
        assertTrue(SocketBufferProbe.refusal(linux(M16 - 1, M16), RCV, SND).startsWith("TRANSPORT_LOG_SO_RCVBUF"));
        assertTrue(SocketBufferProbe.refusal(linux(M16 / 2, M16 / 2), RCV, SND).startsWith("TRANSPORT_LOG_SO_RCVBUF"));
    }

    @Test
    public void theRealKernel() throws IOException {
        final String rmem = SocketBufferProbe.SYSTEM.sysctl(SocketBufferProbe.RMEM_MAX);
        final String wmem = SocketBufferProbe.SYSTEM.sysctl(SocketBufferProbe.WMEM_MAX);
        final SocketBufferProbe.Result r16 = SocketBufferProbe.rcvbuf(SocketBufferProbe.SYSTEM, RCV);
        final SocketBufferProbe.Result s16 = SocketBufferProbe.sndbuf(SocketBufferProbe.SYSTEM, SND);
        final SocketBufferProbe.Request m64 = new SocketBufferProbe.Request("TRANSPORT_LOG_SO_RCVBUF", "64m",
                4 * M16, SocketBufferProbe.RMEM_MAX, "TRANSPORT_LOG_RCV_WINDOW");
        final SocketBufferProbe.Result r64 = SocketBufferProbe.rcvbuf(SocketBufferProbe.SYSTEM, m64);
        System.out.println("PROBE kernel " + System.getProperty("os.name") + " " + System.getProperty("os.version")
                + ": " + r16.line() + "; " + s16.line() + "; " + r64.line() + "; wmem_max=" + wmem);
        Assume.assumeFalse("no /proc/sys on this OS", "unknown".equals(rmem));
        final long max = Long.parseLong(rmem);
        if (max < 4 * M16) {
            assertTrue("a 64m request above rmem_max " + max + " must be refused", r64.refused());
            assertEquals("the JDK reports min(request, rmem_max)", max, r64.granted());
            assertTrue(r64.refusalText(), r64.refusalText().contains("(net.core.rmem_max=" + max + ")"));
        }
        Assume.assumeTrue("net.core.rmem_max " + max + " < 16777216 on this box: the 16m row needs the sysctl",
                max >= M16 && Long.parseLong(wmem) >= M16);
        assertTrue(r16.line(), !r16.refused());
        assertTrue(s16.line(), !s16.refused());
        assertNull(SocketBufferProbe.refusal(SocketBufferProbe.SYSTEM, RCV, SND));
    }
}
