// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.TimerPort;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Deterministic stand-in for the cluster timer service. Pending timers are ordered by
 * {@code (deadline, correlationId)}; {@link #fireDue} delivers every timer whose deadline is at or
 * before {@code nowMs}, in that order, each with {@code timestamp = its own deadline} (the cluster
 * delivers a TimerEvent as its own log entry carrying its own timestamp, and later commands see a
 * later clock). A handler that re-arms inside the callback (the production sweep pattern) is honoured
 * within the same call until nothing is due. Scheduling an id again reschedules it, as Aeron does.
 */
public final class FakeTimerService implements TimerPort {

    /** Receives a fired timer: its correlation id and the timestamp it fires with (its deadline). */
    public interface Handler {
        void onTimer(long correlationId, long timestamp);
    }

    private final TreeMap<Long, TreeSet<Long>> byDeadline = new TreeMap<>();
    private final TreeMap<Long, Long> deadlineById = new TreeMap<>();
    private long fired;

    @Override
    public boolean schedule(final long correlationId, final long deadline) {
        cancel(correlationId);
        deadlineById.put(correlationId, deadline);
        byDeadline.computeIfAbsent(deadline, d -> new TreeSet<>()).add(correlationId);
        return true;
    }

    @Override
    public boolean cancel(final long correlationId) {
        final Long old = deadlineById.remove(correlationId);
        if (old == null) {
            return false;
        }
        final TreeSet<Long> ids = byDeadline.get(old);
        ids.remove(correlationId);
        if (ids.isEmpty()) {
            byDeadline.remove(old);
        }
        return true;
    }

    /**
     * Fire every timer due at or before {@code nowMs}, ascending by (deadline, id); timers armed by the
     * handler are considered in the same pass.
     *
     * @return how many timers fired
     */
    public int fireDue(final long nowMs, final Handler handler) {
        int count = 0;
        while (!byDeadline.isEmpty()) {
            final long deadline = byDeadline.firstKey();
            if (deadline > nowMs) {
                break;
            }
            final long id = byDeadline.get(deadline).first();
            cancel(id);
            fired++;
            count++;
            handler.onTimer(id, deadline);
        }
        return count;
    }

    public boolean isPending(final long correlationId) {
        return deadlineById.containsKey(correlationId);
    }

    /** Deadline of a pending timer, or {@code Long.MIN_VALUE} when none is armed under that id. */
    public long deadlineOf(final long correlationId) {
        final Long d = deadlineById.get(correlationId);
        return d == null ? Long.MIN_VALUE : d;
    }

    public int pendingCount() {
        return deadlineById.size();
    }

    /** Pending timers as {@code {deadline, correlationId}} pairs in firing order (for state comparisons). */
    public List<long[]> pending() {
        final List<long[]> out = new ArrayList<>(deadlineById.size());
        for (final var e : byDeadline.entrySet()) {
            for (final long id : e.getValue()) {
                out.add(new long[] {e.getKey(), id});
            }
        }
        return out;
    }

    public long firedCount() {
        return fired;
    }
}
