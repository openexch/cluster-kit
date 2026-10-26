// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit.closure;

/** Closure-scan fixture: reaches the leaf through a static call and a parameter type. */
public final class ClosureMiddle {
    public long run() {
        return use(null);
    }

    private static long use(final ClosureLeaf leaf) {
        return leaf == null ? ClosureLeaf.tick() : 0;
    }
}
