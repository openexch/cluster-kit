// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

/** Test-side lifecycle construction, in one place. */
final class RejoinTestSetup {
    private RejoinTestSetup() {
    }

    static EngineLifecycle lifecycle(final String name, final ThreeMemberCluster.Node n, final long rejoinWaitMs) {
        return new EngineLifecycle(name, n.log::add, n.halts::add, 4_000, 2_000, 120_000, rejoinWaitMs);
    }

    static EngineLifecycle lifecycle(final String name, final ThreeMemberCluster.Node n, final long rejoinWaitMs,
                                     final long catchupRefusals, final long catchupRefusalWaitMs) {
        return new EngineLifecycle(name, n.log::add, n.halts::add, 4_000, 2_000, 120_000, rejoinWaitMs,
                EngineLifecycle.DEFAULT_REJOIN_PEER_WINDOW_MS, catchupRefusals, catchupRefusalWaitMs);
    }

    static EngineLifecycle leaderElectLifecycle(final String name, final ThreeMemberCluster.Node n,
                                                final long rejoinWaitMs, final long peerWindowMs,
                                                final long leaderElectWaitMs) {
        return new EngineLifecycle(name, n.log::add, n.halts::add, 4_000, 2_000, 120_000, rejoinWaitMs, peerWindowMs,
                leaderElectWaitMs, EngineLifecycle.DEFAULT_CATCHUP_REFUSALS,
                EngineLifecycle.DEFAULT_CATCHUP_REFUSAL_WAIT_MS);
    }
}
