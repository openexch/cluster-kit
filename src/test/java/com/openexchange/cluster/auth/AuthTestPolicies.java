// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import java.util.HexFormat;

/** The two engine policies as the engines declare them (contract 2.7), rebuilt here so the kit tests need no engine. */
final class AuthTestPolicies {

    /** core: classes 1..7, explicit user scope allowed. */
    static final AuthPolicy CORE = AuthPolicy.of("core", "core.auth.keys", "CORE_AUTH_KEYS",
            "core.auth.credential", "CORE_AUTH_CREDENTIAL", true,
            ClusterSessionClass.GATEWAY, ClusterSessionClass.FORWARDER, ClusterSessionClass.BRIDGE,
            ClusterSessionClass.ADMIN, ClusterSessionClass.TOOL, ClusterSessionClass.TREASURY,
            ClusterSessionClass.REPLICA);

    /** match: GATEWAY, FORWARDER, ADMIN, REPLICA; users=* only. */
    static final AuthPolicy ME = AuthPolicy.of("match", "match.auth.keys", "ME_AUTH_KEYS",
            "match.auth.credential", "ME_AUTH_CREDENTIAL", false,
            ClusterSessionClass.GATEWAY, ClusterSessionClass.FORWARDER, ClusterSessionClass.ADMIN,
            ClusterSessionClass.REPLICA);

    /** bytes 00..1f and a0..bf, as in auth-parity-v1.txt. */
    static final String SA;
    static final String SB;

    static {
        final byte[] a = new byte[32];
        final byte[] b = new byte[32];
        for (int i = 0; i < 32; i++) {
            a[i] = (byte) i;
            b[i] = (byte) (0xA0 + i);
        }
        SA = HexFormat.of().formatHex(a);
        SB = HexFormat.of().formatHex(b);
    }

    private AuthTestPolicies() {
    }
}
