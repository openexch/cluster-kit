// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

/**
 * The principal rule, one kit copy for both engines: a session without a valid principal reached the log, a restored
 * container session or a message. Thrown by {@link PrincipalGate}; the engine's live service prints the message,
 * fences its callbacks and fail-fasts; a log reader or a shadow host decides for itself.
 */
public final class PrincipalLessSessionException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public PrincipalLessSessionException(final String message) {
        super(message);
    }
}
