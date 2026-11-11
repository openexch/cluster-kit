// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import org.agrona.concurrent.NanoClock;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The oracle mismatch hold: entered once on a same-history byte mismatch, ticked by the housekeeping thread (which
 * keeps running while the replica holds). {@code REPLICA_MISMATCH_HOLD_MS} 0 holds until the operator stops the
 * process; a non-zero value ends the hold with exit 1 after that long (gives a restart loop back to operators who want
 * one). Driver loss is not handled here: the Aeron error handler stays armed and halts at once.
 */
final class MismatchHold {

    private final long holdMs;
    private final long holdNs;
    private final NanoClock clock;
    private final ReplicaFollower.Halt halt;

    private volatile long sinceNs = -1;
    private volatile String reason;
    private boolean ended;

    MismatchHold(final long holdMs, final NanoClock clock, final ReplicaFollower.Halt halt) {
        if (holdMs < 0) {
            throw new IllegalArgumentException("REPLICA_MISMATCH_HOLD_MS must be >= 0: " + holdMs);
        }
        this.holdMs = holdMs;
        this.holdNs = TimeUnit.MILLISECONDS.toNanos(holdMs);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.halt = Objects.requireNonNull(halt, "halt");
    }

    /** The hold starts now (once; a second call is ignored). */
    synchronized void enter(final String why) {
        if (sinceNs >= 0) {
            return;
        }
        reason = why;
        sinceNs = clock.nanoTime();
    }

    /** Housekeeping pass: ends the hold with exit 1 once {@code REPLICA_MISMATCH_HOLD_MS} elapsed (0 never). */
    void tick() {
        final long since = sinceNs;
        if (since < 0 || holdNs == 0) {
            return;
        }
        synchronized (this) {
            if (ended || clock.nanoTime() - since < holdNs) {
                return;
            }
            ended = true;
        }
        halt.fatal("FATAL replica: the oracle mismatch hold ended after REPLICA_MISMATCH_HOLD_MS=" + holdMs + " ("
                + reason + ")");
    }

    boolean holding() {
        return sinceNs >= 0;
    }

    /** {@code shadow_oracle_hold_ms}: time held so far, -1 when not holding. */
    long heldMs() {
        final long since = sinceNs;
        return since < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - since);
    }
}
