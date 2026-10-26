// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit.closure;

/** Closure-scan fixture: the forbidden reference sits two hops from the root. */
public final class ClosureLeaf {
    static long tick() {
        return System.nanoTime();
    }
}
