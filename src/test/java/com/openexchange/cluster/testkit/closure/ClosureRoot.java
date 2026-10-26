// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit.closure;

/** Closure-scan fixture: root -> middle -> leaf, where only the leaf reads the clock. */
public final class ClosureRoot {
    private final ClosureMiddle middle = new ClosureMiddle();

    public long run() {
        return middle.run() + java.util.Objects.hashCode(this);
    }
}
