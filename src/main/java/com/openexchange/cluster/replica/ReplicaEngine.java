// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.log.ShadowEngine;
import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentitySource;
import com.openexchange.cluster.shadow.ShadowSelfCheck;
import io.aeron.cluster.VersionValidator;

/**
 * What an engine gives the kit replica: the members' applier, its identity, the scratch self-check and the members'
 * container app version and validator. Assets binds {@code AssetsLogApplier},
 * {@code AssetsShadowIdentity} and {@code ApplierSelfCheck}; the matching engine binds its own.
 *
 * <p>Threads: {@link #engine()} and {@link #identity()} are touched by the follower thread only;
 * {@link #selfCheck()} runs on the writer thread and must build its own scratch engine.</p>
 */
public interface ReplicaEngine {

    /** {@code ShadowHeader.ENGINE_ASSETS} or {@code ENGINE_MATCH}. */
    int engineId();

    /** The live engine the replica applies the log to; restored or genesis-started once. */
    ShadowEngine engine();

    /** The live engine's replicated identity values, read between frames on the follower thread. */
    ShadowIdentitySource identity();

    /** The writer thread's round trip through a scratch engine. */
    ShadowSelfCheck selfCheck();

    /** The members' {@code ClusteredServiceContainer.Context.appVersion()} (members set none: the Aeron default). */
    int appVersion();

    /** The members' {@code appVersionValidator()}. */
    VersionValidator validator();

    /**
     * The engine's witness after a restore: null when it holds, else the failure text, for example
     * {@code shadow P_s witness failed: lastLogTime=<t> > header.clusterTime=<c>}. The replica halts on a non-null.
     */
    String postRestoreWitness(ShadowHeader header);

    /**
     * The genesis a member's full service snapshot image holds, decoded through a scratch engine (never
     * the live one). {@code image} is the reassembled enveloped payload a member's {@code onTakeSnapshot} wrote, the
     * bytes {@link ShadowEngine#restore} takes. Returns the ADOPTed genesisId, 0 before any ADOPT; throws when the
     * image does not decode (the replica then refuses to choose, loudly). A stub image never reaches this method: the
     * kit reads a stub's genesis from the stub itself.
     */
    long imageGenesisId(org.agrona.DirectBuffer image, int length);
}
