// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit.closure;

/** Review closure fixture: the root lives in test-classes, its in-prefix reference in main classes. */
public final class ClosureCrossRoot {
    public static long position() {
        return com.openexchange.cluster.log.LogCursor.GENESIS.logPosition();
    }
}
