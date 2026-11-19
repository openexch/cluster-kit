// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

/** Test-side pruner knobs, in one place. */
final class RetentionTestSetup {
    private RetentionTestSetup() {
    }

    /** Wake every 200 ms instead of every 5 s. */
    static void fastPoll(final SnapshotLogPruner pruner) {
        pruner.pollIntervalMs(200);
    }
}
