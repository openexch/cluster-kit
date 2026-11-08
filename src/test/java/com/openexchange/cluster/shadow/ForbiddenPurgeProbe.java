// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.archive.client.AeronArchive;

/** Positive control for {@code ShadowPurgeCallSiteTest}: a second shadow purge path outside {@link ShadowRetention}. */
public final class ForbiddenPurgeProbe {
    public static void purge(final AeronArchive archive, final long recordingId) {
        archive.purgeRecording(recordingId);
    }
}
