// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

/**
 * Positive control for the kit half's agnostic list: names a core engine type reflectively, under the current and the
 * former package name.
 */
public final class ForbiddenAgnosticProbe {
    public static Class<?> load() throws ClassNotFoundException {
        return Class.forName("com.openexchange.core.domain.Probe");
    }

    public static Class<?> loadFormer() throws ClassNotFoundException {
        return Class.forName("com.openexchange.assets.domain.Probe");
    }
}
