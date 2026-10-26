// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

/** A {@link ReplayContainer} refused a call; reasons that leave the container failed are noted per value. */
public final class ReplayStoppedException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public enum Reason {
        /** An entry before {@code genesis}/{@code restore}. */
        NOT_STARTED,
        /** A second {@code genesis}/{@code restore}. */
        ALREADY_STARTED,
        /** {@code restore} with {@code replayFrom} newer than the image: frames would be missing. Nothing restored. */
        REPLAY_AFTER_IMAGE,
        /** Term event app version refused by the validator. Latches. */
        INCOMPATIBLE_APP_VERSION,
        /** Term event time unit other than milliseconds. Latches. */
        UNSUPPORTED_TIME_UNIT,
        /** A frame position not after the cursor. Latches. */
        NON_MONOTONIC_POSITION,
        /**
         * {@code onSkipped} of a frame no correct SKIP window drops: a kind that is not engine-mutating, or a frame
         * after the image position (the engine does not hold it). Latches.
         */
        SKIP_OUTSIDE_IMAGE,
        /** An engine-mutating frame at or before the image position: the engine already holds it. Latches. */
        APPLY_INSIDE_IMAGE,
        /** {@code serialize} while the cursor is behind the engine image (SKIP window). */
        INSIDE_SKIP_WINDOW,
        /** The engine (or a trigger listener) threw; the cause is attached. Latches. */
        ENGINE_FAILED,
        /** Any call after a latching stop. */
        FAILED
    }

    private final Reason reason;

    public ReplayStoppedException(final Reason reason, final String message, final Throwable cause) {
        super(reason + ": " + message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
