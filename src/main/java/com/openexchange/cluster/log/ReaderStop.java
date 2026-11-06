// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

/**
 * Why a {@link ClusterLogReader} stopped: the reason, the position of the frame (or image) it stopped at, the
 * template id when one was decoded ({@code -1} otherwise) and the cause when something threw.
 */
public record ReaderStop(Reason reason, long framePosition, int templateId, Throwable cause) {

    /** The value while a reader is running. */
    public static final ReaderStop NONE = new ReaderStop(Reason.NONE, -1L, -1, null);

    public enum Reason {
        NONE,
        /** The first image does not start where the reader was told to start (A0:CSA:863-873). */
        START_MISMATCH,
        /** {@code poll} was handed a different image than the one the reader is bound to. */
        IMAGE_CHANGED,
        /** {@code successor} of a reader that had already stopped. */
        PREVIOUS_STOPPED,
        FOREIGN_SCHEMA,
        SCHEMA_VERSION_MISMATCH,
        UNKNOWN_TEMPLATE,
        DECODE_FAILED,
        BODY_POSITION_MISMATCH,
        ORPHAN_FRAGMENT,
        LISTENER_FAILED
    }

    public ReaderStop {
        if (reason == null) {
            throw new IllegalArgumentException("reason is required");
        }
    }

    @Override
    public String toString() {
        return reason + "@" + framePosition + (templateId >= 0 ? " template=" + templateId : "")
                + (cause != null ? " cause=" + cause : "");
    }
}
