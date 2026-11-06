// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

/** Positive control for the kit half's agnostic list: names an assets type reflectively. */
public final class ForbiddenAgnosticProbe {
    public static Class<?> load() throws ClassNotFoundException {
        return Class.forName("com.openexchange.assets.domain.Probe");
    }
}
