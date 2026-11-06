// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/** Why a shadow was written (RCS S3 3.1 {@code kind}); an attribute of a copy, never part of its identity (6.2). */
public enum ShadowKind {
    ALIGNED(1), STANDBY(2), CADENCE(3), LOCAL(4);

    private final int code;

    ShadowKind(final int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** The kind for a wire code, or null (decoders never throw on an unknown value, 3.4). */
    public static ShadowKind of(final int code) {
        for (final ShadowKind k : values()) {
            if (k.code == code) {
                return k;
            }
        }
        return null;
    }
}
