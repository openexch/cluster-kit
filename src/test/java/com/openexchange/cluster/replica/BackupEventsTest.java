// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.archive.client.ArchiveException;
import io.aeron.cluster.client.ClusterException;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * The backup's failure kinds stay apart. Aeron 1.53.1 reports a supplier archive that
 * refuses or fails the bounded live-log replay as a {@code ClusterException("Live log replay failed ...")}
 * (CBA:869, CBA:909-913), not an {@code ArchiveException}; it is {@code backup_replay_failures_total}, a
 * re-bootstrap detector's input, never an "other" reset.
 */
public class BackupEventsTest {

    @Test
    public void aLiveLogReplayRefusalIsAReplayFailure() {
        final BackupEvents e = new BackupEvents(SystemNanoClock.INSTANCE);
        e.onPossibleFailure(new ClusterException("Live log replay failed (replaySessionId=-1): errorMessage=max"
                + " concurrent replays reached 10, errorCode=MAX_REPLAYS"));
        e.onPossibleFailure(new ClusterException("Live log replay failed: ERROR"));
        assertEquals("backup_replay_failures_total counts the supplier's replay refusals", 2, e.replayFailures());
        assertEquals(0, e.otherFailures());
        assertEquals(2, e.resets());
    }

    @Test
    public void theOtherKindsStayApart() {
        final BackupEvents e = new BackupEvents(SystemNanoClock.INSTANCE);
        e.onPossibleFailure(new ClusterException("AUTHENTICATION_REJECTED: Not authorised for BackupQuery"));
        e.onPossibleFailure(new ArchiveException("unknown recording", ArchiveException.UNKNOWN_RECORDING));
        e.onPossibleFailure(new ClusterException("log recording counter unexpectedly unavailable",
                io.aeron.exceptions.AeronException.Category.WARN));
        assertEquals(1, e.authRejected());
        assertEquals(1, e.replayFailures());
        assertEquals(1, e.otherFailures());
    }
}
