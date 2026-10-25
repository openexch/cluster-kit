// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** RC1 S2: the fake must model what the cluster does, or the harness proves nothing about timers. */
public class FakeTimerServiceTest {

    @Test
    public void firesInDeadlineThenIdOrderWithTheDeadlineAsTimestamp() {
        final FakeTimerService timers = new FakeTimerService();
        timers.schedule(7, 300);
        timers.schedule(3, 100);
        timers.schedule(9, 100);
        timers.schedule(1, 500);
        final List<String> seen = new ArrayList<>();
        assertEquals(3, timers.fireDue(300, (id, ts) -> seen.add(id + "@" + ts)));
        assertEquals(List.of("3@100", "9@100", "7@300"), seen);
        assertEquals(1, timers.pendingCount());
        assertTrue(timers.isPending(1));
    }

    @Test
    public void reschedulingAnIdMovesItAndCancelRemovesIt() {
        final FakeTimerService timers = new FakeTimerService();
        timers.schedule(5, 100);
        timers.schedule(5, 900);
        assertEquals(1, timers.pendingCount());
        assertEquals(900, timers.deadlineOf(5));
        assertEquals(0, timers.fireDue(500, (id, ts) -> { }));
        assertTrue(timers.cancel(5));
        assertFalse(timers.cancel(5));
        assertEquals(0, timers.pendingCount());
    }

    @Test
    public void aHandlerThatReArmsIsServedInTheSamePassUntilNothingIsDue() {
        final FakeTimerService timers = new FakeTimerService();
        final long sweep = 1_000_000L;
        timers.schedule(sweep, 100);
        final List<Long> fires = new ArrayList<>();
        final int fired = timers.fireDue(350, (id, ts) -> {
            fires.add(ts);
            timers.schedule(sweep, ts + 100); // the production sweep pattern: re-arm from the callback
        });
        assertEquals(List.of(100L, 200L, 300L), fires);
        assertEquals(3, fired);
        assertEquals("the re-arm past now stays pending", 400, timers.deadlineOf(sweep));
    }
}
