// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

/**
 * The one timer facility a clustered service may use, as the engine sees it. Mirrors Aeron's
 * {@code Cluster.scheduleTimer} / {@code cancelTimer} contract: a timer is identified by a
 * correlation id, scheduling an id again reschedules it, and the deadline is cluster time in the
 * cluster's time unit. Both engines' production adapters delegate to the real {@code Cluster}; the
 * testkit's {@link com.openexchange.cluster.testkit.FakeTimerService} implements the same type, so a
 * harness can drive deadlines deterministically.
 *
 * <p>Both methods return {@code false} when the request could not be enqueued yet (back pressure);
 * the production adapter retries with the service idle strategy, exactly as a direct call would.</p>
 */
public interface TimerPort {

    boolean schedule(long correlationId, long deadline);

    boolean cancel(long correlationId);
}
