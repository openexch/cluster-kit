// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

/** Positive control for the never-schedules scan: reads the wall clock through {@code System.currentTimeMillis}. */
public final class ForbiddenClockProbe {
    public static long now() {
        return System.currentTimeMillis();
    }
}
