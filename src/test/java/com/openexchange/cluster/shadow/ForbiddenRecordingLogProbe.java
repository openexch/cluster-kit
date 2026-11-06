// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.cluster.RecordingLog;

/** Positive control for {@code Rules.shadow()}: a {@code RecordingLog} class constant (G2: the archive is the registry). */
public final class ForbiddenRecordingLogProbe {
    public static Class<?> type() {
        return RecordingLog.class;
    }
}
