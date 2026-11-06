// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

/** Told about every snapshot action a member would act on, at or after the engine image position. */
@FunctionalInterface
public interface ShadowTriggerListener {

    ShadowTriggerListener NONE = (logPosition, leadershipTermId, timestamp, flags) -> false;

    /**
     * @param flags 0 ALIGNED (every member snapshots), 1 STANDBY (standby members only)
     * @return true to end the current reader poll right after this frame
     */
    boolean onSnapshotAction(long logPosition, long leadershipTermId, long timestamp, int flags);
}
