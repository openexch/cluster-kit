// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

/** Which frames a gate drops before they reach the apply function. */
@FunctionalInterface
public interface SkipRule {
    boolean drops(FrameKind kind, long frameEnd);
}
