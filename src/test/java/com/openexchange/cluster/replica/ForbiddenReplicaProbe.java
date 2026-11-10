// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;

/** Positive controls for {@code ReplicaPackageScanTest}: everything the replica rules forbid, in one class. */
final class ForbiddenReplicaProbe {

    private ForbiddenReplicaProbe() {
    }

    static Object launchDriver() {
        return MediaDriver.launch();
    }

    static Object launchBackup(final ClusterBackup.Context ctx) {
        return ClusterBackup.launch(ctx);
    }

    static long wallClock() {
        return System.nanoTime();
    }

    static boolean schedule(final Cluster cluster) {
        return cluster.scheduleTimer(1, 2);
    }

    static void purge(final AeronArchive archive, final long recordingId) {
        archive.purgeRecording(recordingId);
    }
}
