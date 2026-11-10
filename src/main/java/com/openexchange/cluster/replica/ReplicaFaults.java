// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.FrameKind;
import com.openexchange.cluster.log.LogCursor;
import com.openexchange.cluster.shadow.ShadowImageBuffer;

/**
 * Fault seams for the replica tests ({@code Faults.throwAtFrame}, {@code Faults.mutateImage}); production
 * uses {@link #NONE}. {@link ReplicaConfig.Builder#build} refuses anything else unless the system property
 * {@value #PROPERTY} is {@code true}, which hosts never set. This interface is the one place in
 * {@code cluster.replica} that reads a system property (scan rule {@code Rules.replica()}).
 */
public interface ReplicaFaults {

    String PROPERTY = "oxs.replica.negative.controls";

    ReplicaFaults NONE = new ReplicaFaults() {
        @Override
        public String toString() {
            return "NONE";
        }
    };

    /** Called on the follower thread before a frame reaches the container; a throw is an apply failure. */
    default void beforeFrame(final FrameKind kind, final long logPosition) {
    }

    /** Called after serialize and before the hand-off to the writer thread ({@code mutateImage}). */
    default void beforeOffer(final ShadowImageBuffer image, final LogCursor at) {
    }

    /**
     * The named-copy test rig: called once with the backup's context before the backup launches. The rig keeps it to
     * reset the backup during SNAPSHOT_RETRIEVE (its replication progress timeout, read by the agent at each snapshot
     * retrieval, ClusterBackupAgent:722-732). Production ({@link #NONE}) does nothing.
     */
    default void backupContext(final io.aeron.cluster.ClusterBackup.Context context) {
    }

    /** Whether test hooks may be used in this JVM. */
    static boolean allowed() {
        return Boolean.parseBoolean(System.getProperty(PROPERTY));
    }
}
