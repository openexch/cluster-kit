// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

/**
 * RC2 S6 R0 as one kit rule for both engines (RC2 S17 W12, R-KIT-R0). Authentication is always on and every live
 * session is minted with a real class ({@link ClusterPrincipal} refuses {@link ClusterSessionClass#UNSET} at
 * encode), so a principal-less session in the replicated log, in a restored container session or behind an ingress
 * frame can only be pre-S6 history or a session id Aeron does not know. The decision reads replicated bytes only
 * (the principal Aeron hands with the session), so every member refuses at the same log position.
 *
 * <p>The check allocates nothing on the accepted path (the ME calls it per ingress frame).
 */
public final class PrincipalGate {
    public static final String PRINCIPAL_LESS_MESSAGE =
            "principal-less session in log: pre-S6 history, clean genesis required";

    private PrincipalGate() {
    }

    /**
     * The session class code of {@code encodedPrincipal} when it is a valid principal; otherwise throws
     * {@link PrincipalLessSessionException} with {@link #criticalLine}.
     *
     * @param where       the site: "session open", "restored session" or "ingress frame"
     * @param sessionId   the cluster session id, -1 when unknown
     * @param logPosition the log position of the entry, -1 when unknown
     */
    public static int requireClass(final byte[] encodedPrincipal, final String where, final long sessionId,
                                   final long logPosition) {
        final int code = ClusterPrincipal.classCodeOf(encodedPrincipal);
        if (code > 0) {
            return code;
        }
        throw new PrincipalLessSessionException(criticalLine(where, sessionId, logPosition));
    }

    /** The CRITICAL line both engines print before they fail-fast; the T2 rows match it. */
    public static String criticalLine(final String where, final long sessionId, final long logPosition) {
        return "CRITICAL: " + PRINCIPAL_LESS_MESSAGE + " (" + where + ", clusterSessionId=" + sessionId
                + ", logPosition=" + logPosition + ")";
    }
}
