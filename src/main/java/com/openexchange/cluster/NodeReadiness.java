// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.service.Cluster.Role;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * What a cluster node answers when an orchestrator asks "are you alive?" and
 * "may I send you work, or restart the next one?".
 *
 * <p>This exists because the orchestrator replaces the supervisor. A rolling
 * restart is one pod at a time, and each step waits for the previous pod to
 * report ready. If ready means "the port is open", the orchestrator will happily
 * restart the second node while the first is still catching up, and a
 * three-member cluster loses quorum. So readiness here means <b>caught up</b>,
 * and everything it cannot prove counts against it.</p>
 *
 * <h2>Liveness is not readiness</h2>
 *
 * <p>Liveness answers "should this process be killed". Readiness answers
 * "should traffic and rolling restarts wait for it". They are separate because
 * a node that is replaying the log is perfectly healthy and must not be killed,
 * yet must not be counted as a working member either.</p>
 *
 * <h2>The tick is the point</h2>
 *
 * <p>Both answers hang off {@link #tick()}, called from the duty cycle. A JVM
 * whose agent thread has stopped still accepts TCP connections and still
 * answers an HTTP handler on another thread: that is exactly how a process that
 * had OOMed kept reporting itself up for hours. A stale tick is therefore
 * treated as dead rather than as missing information.</p>
 *
 * <h2>What this cannot see</h2>
 *
 * <p>Aeron gives the service no explicit "the log replay finished" callback. A
 * node counts as caught up once it has both started and observed a role while
 * live; engines that can prove it earlier or later should call
 * {@link #catchUpComplete()} / {@link #catchingUp()} themselves. Where nothing
 * is known, the answer is NOT ready, never "probably fine".</p>
 */
public final class NodeReadiness {

    /** A tick older than this means the duty cycle is wedged; the node is dead. */
    public static final long DEFAULT_LIVENESS_STALE_MS = 30_000;

    /** A tick older than this means the node cannot be trusted to be current. */
    public static final long DEFAULT_READINESS_STALE_MS = 5_000;

    private final long livenessStaleMs;
    private final long readinessStaleMs;
    private final AtomicLong lastTickMs = new AtomicLong();
    private final AtomicReference<Role> role = new AtomicReference<>();
    private volatile boolean started;
    private volatile boolean caughtUp;
    private volatile boolean stopping;

    public NodeReadiness() {
        this(DEFAULT_LIVENESS_STALE_MS, DEFAULT_READINESS_STALE_MS);
    }

    public NodeReadiness(final long livenessStaleMs, final long readinessStaleMs) {
        this.livenessStaleMs = livenessStaleMs;
        this.readinessStaleMs = readinessStaleMs;
    }

    /**
     * Called from the duty cycle, including on idle. This is the heartbeat both
     * answers are built on, so it must be reached on every cycle rather than
     * only when there is work: a quiet market is not a wedged node.
     */
    public void tick() {
        lastTickMs.set(System.currentTimeMillis());
    }

    /** The service finished {@code onStart}: snapshot loaded, log replay may follow. */
    public void started() {
        started = true;
        tick();
    }

    /** The node has consumed the log up to the cluster's committed position. */
    public void catchUpComplete() {
        caughtUp = true;
    }

    /** The node is behind again (rejoin, replay, a leadership term it missed). */
    public void catchingUp() {
        caughtUp = false;
    }

    /**
     * A role was observed. CANDIDATE means an election is in flight, which is
     * the one time a member is emphatically not ready to be counted on, and
     * also the moment a rolling restart most wants to pause.
     */
    public void roleChanged(final Role newRole) {
        role.set(newRole);
        if (newRole == Role.LEADER || newRole == Role.FOLLOWER) {
            caughtUp = true;
        }
        tick();
    }

    /** Shutdown began: stop advertising readiness before the process goes away. */
    public void stopping() {
        stopping = true;
    }

    /**
     * Liveness. False only when the duty cycle has stopped advancing, which is
     * the state an external supervisor could never distinguish from healthy.
     */
    public boolean live() {
        return sinceTickMs() < livenessStaleMs;
    }

    /** Readiness, in the orchestrator's sense: safe to route to, safe to move on from. */
    public boolean ready() {
        if (stopping || !started || !caughtUp) {
            return false;
        }
        final Role current = role.get();
        if (current != Role.LEADER && current != Role.FOLLOWER) {
            return false;
        }
        return sinceTickMs() < readinessStaleMs;
    }

    /** Why the current answer is what it is, for the body of the probe response. */
    public String describe() {
        if (stopping) {
            return "stopping";
        }
        if (!started) {
            return "starting";
        }
        if (!caughtUp) {
            return "catching up";
        }
        final Role current = role.get();
        if (current == null) {
            return "role unknown";
        }
        if (current != Role.LEADER && current != Role.FOLLOWER) {
            return "election in progress (" + current + ")";
        }
        final long stale = sinceTickMs();
        if (stale >= readinessStaleMs) {
            return "duty cycle last advanced " + stale + "ms ago";
        }
        return current.toString().toLowerCase() + ", caught up";
    }

    private long sinceTickMs() {
        final long last = lastTickMs.get();
        if (last == 0) {
            return Long.MAX_VALUE; // never ticked: unknown counts against us
        }
        return System.currentTimeMillis() - last;
    }
}
