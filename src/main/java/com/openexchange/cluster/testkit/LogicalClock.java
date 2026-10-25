// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

/**
 * Deterministic, injectable timestamp source for scenarios. The value handed to an engine's apply is
 * fully controlled here, never wall clock, so replays see an identical timestamp sequence. The
 * scenario {@code CLOCK <n>} verb sets the absolute value; {@code TICK <n>} advances it.
 */
public final class LogicalClock {

    public static final long DEFAULT_START = 1000L;

    private long current;

    public LogicalClock() {
        this(DEFAULT_START);
    }

    public LogicalClock(final long start) {
        this.current = start;
    }

    public long now() {
        return current;
    }

    public void set(final long value) {
        this.current = value;
    }

    /** Advance by {@code delta} (negative is refused: log time never goes backwards in a scenario). */
    public long advance(final long delta) {
        if (delta < 0) {
            throw new IllegalArgumentException("clock cannot go backwards: " + delta);
        }
        current += delta;
        return current;
    }
}
