// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/**
 * The engine's replicated identity values a shadow header carries (a separate source, so
 * the {@code ShadowEngine} surface stays at nine methods). Read on the apply thread at the same instant as
 * {@code ShadowEngine.serialize}, between two frames; because that rests on the host, every load re-reads the source
 * from the RESTORED engine and compares ({@link ShadowIdentity#matches}).
 */
public interface ShadowIdentitySource {

    /** The engine's recorded genesis nonce; 0 = none recorded yet. A shadow needs {@code > 0} (H10). */
    long genesisId();

    /** The engine's REPLICATED journal arming, never a node-local sink switch. */
    boolean journalEnabled();

    /** The last journal seq the engine assigned (dense), 0 = none. */
    long journalSeq();

    /** Engine-defined payload layout, diagnostic only (3.1 offset 112); 0 allowed. */
    int payloadLayoutHint();
}
