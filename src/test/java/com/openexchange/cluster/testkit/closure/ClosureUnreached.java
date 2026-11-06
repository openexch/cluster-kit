// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit.closure;

/** Closure-scan fixture: same package, never referenced from the root: must not be scanned. */
public final class ClosureUnreached {
    public static long now() {
        return System.currentTimeMillis();
    }
}
