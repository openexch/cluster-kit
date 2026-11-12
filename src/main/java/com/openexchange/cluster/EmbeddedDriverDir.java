// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.exceptions.ActiveDriverException;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The embedded driver directory check, one rule for every host that launches an embedded media driver (the
 * replica and the cluster member). A directory is checked before it is replaced: Aeron's
 * {@code dirDeleteOnStart(false)} path reads the CnC heartbeat and refuses a directory whose driver heartbeat is at
 * most {@code driverTimeoutMs} old with {@code ActiveDriverException}, and deletes and recreates a stale one.
 *
 * <p>Why the bound is seconds and not Aeron's embedded habit of 500 ms: a driver writes its heartbeat once per timer
 * tick ({@code aeron.timer.interval}, 1 s by default, an aeronmd the same), so a live aeronmd's heartbeat is up to
 * about a second old and a 500 ms bound reads it as stale about half the time, deletes its directory and starts a
 * second driver over it. The default is Aeron's driver timeout (10 s). The trade-off: the directory of a driver that
 * crashed is treated as stale only after the bound, so a host restarted sooner than that after a kill refuses and
 * does not start until it is (the process manager's restart delay must cover it). External driver mode never calls
 * this: the host only connects.</p>
 */
public final class EmbeddedDriverDir {

    public static final long DEFAULT_ACTIVE_MS = 10_000;
    public static final long MIN_ACTIVE_MS = 2_000;
    public static final long MAX_ACTIVE_MS = 600_000;

    private EmbeddedDriverDir() {
    }

    /** {@code <setting> must be 2000..600000 ms: <v>}, or null when in range. */
    public static String rangeRefusal(final String setting, final long activeMs) {
        return activeMs < MIN_ACTIVE_MS || activeMs > MAX_ACTIVE_MS
                ? setting + " must be " + MIN_ACTIVE_MS + ".." + MAX_ACTIVE_MS + " ms: " + activeMs : null;
    }

    /** The check on a driver context: never delete on start blindly, refuse a heartbeat at most {@code activeMs} old. */
    public static MediaDriver.Context guard(final MediaDriver.Context context, final long activeMs) {
        final String refusal = rangeRefusal("driver directory bound", activeMs);
        if (refusal != null) {
            throw new IllegalArgumentException(refusal);
        }
        return context.dirDeleteOnStart(false).driverTimeoutMs(activeMs);
    }

    /** A live driver found in a directory. */
    public record Live(String dir, long heartbeatAgeMs, long boundMs) {
        /**
         * {@code AERON_DIR <dir> has a live media driver (<others>; heartbeat <age> ms old, bound <bound> ms): stop it
         * or point this <self> at its own directory}.
         */
        public String text(final String others, final String self) {
            return "AERON_DIR " + dir + " has a live media driver (" + others + "; heartbeat "
                    + (heartbeatAgeMs < 0 ? "unknown" : String.valueOf(heartbeatAgeMs)) + " ms old, bound " + boundMs
                    + " ms): stop it or point this " + self + " at its own directory";
        }
    }

    /** The refusal of a launch over a live driver's directory; the message is the host's text without its prefix. */
    public static final class LiveDriverException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient Live live;

        LiveDriverException(final Live live, final String text, final Throwable cause) {
            super(text, cause);
            this.live = live;
        }

        public Live live() {
            return live;
        }
    }

    /**
     * The live driver in {@code dir} as Aeron's own check sees it with bound {@code activeMs}, or null when the
     * directory has no CnC file or its heartbeat is older than the bound (stale).
     */
    public static Live probe(final String dir, final long activeMs) {
        final File d = new File(Objects.requireNonNull(dir, "dir"));
        if (!d.isDirectory()) {
            return null;
        }
        final AtomicLong age = new AtomicLong(-1);
        final boolean active = CommonContext.isDriverActive(d, activeMs, line -> {
            final int at = line.indexOf("(ms): ");
            if (at >= 0) {
                try {
                    age.set(Long.parseLong(line.substring(at + 6).trim()));
                } catch (final NumberFormatException ignored) {
                    // the age stays unknown; the verdict is Aeron's
                }
            }
        });
        return active ? new Live(d.getAbsolutePath(), age.get(), activeMs) : null;
    }

    /**
     * Probes {@code dir} first (a live driver refuses before anything is launched), then launches; an
     * {@code ActiveDriverException} from the launch itself (a driver that started in between) is refused the same
     * way. The live driver's directory is never touched.
     *
     * @param others what else may own the directory, e.g. {@code another core member's embedded driver or an aeronmd}
     * @param self   the host noun, e.g. {@code member}
     * @throws LiveDriverException with the {@link Live#text} message
     */
    public static <T> T launch(final String dir, final long activeMs, final Supplier<T> launch, final String others,
                               final String self) {
        final Live before = probe(dir, activeMs);
        if (before != null) {
            throw new LiveDriverException(before, before.text(others, self), null);
        }
        try {
            return launch.get();
        } catch (final ActiveDriverException refused) {
            final Live after = probe(dir, activeMs);
            final Live live = after != null ? after : new Live(new File(dir).getAbsolutePath(), -1, activeMs);
            throw new LiveDriverException(live, live.text(others, self), refused);
        }
    }
}
