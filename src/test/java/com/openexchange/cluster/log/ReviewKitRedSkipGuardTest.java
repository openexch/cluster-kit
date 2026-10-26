// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

import com.openexchange.cluster.testkit.HashChainEngine;
import io.aeron.cluster.AppVersionValidator;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Review (RCS S2 review-kit DEFECT-1, FIX-1). RED until fixed: the container knows its image position, so it must
 * refuse a skip that a correct SKIP window could never produce, and an engine apply of a frame the image already holds.
 */
public class ReviewKitRedSkipGuardTest {

    private static final int APP = SemanticVersion.compose(0, 0, 1);
    private static final long C = 1024;
    private static final long S = 2048;

    private static ReplayContainer skipLeg(final HashChainEngine engine) {
        final HashChainEngine source = new HashChainEngine();
        source.genesis();
        final ExpandableArrayBuffer img = new ExpandableArrayBuffer();
        final int length = source.serialize(img);
        final ReplayContainer c = new ReplayContainer(engine, APP, AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR,
                (p, t, ts, f) -> false);
        c.restore(new UnsafeBuffer(img.byteArray(), 0, length), length, S, new LogCursor(C, 5, 1, 0));
        return c;
    }

    private static void refused(final String what, final Runnable call) {
        try {
            call.run();
        } catch (final ReplayStoppedException expected) {
            return;
        }
        fail(what + " was accepted silently");
    }

    @Test
    public void aSkipOfAnEngineMutatingFrameAfterTheImageIsRefused() {
        final HashChainEngine engine = new HashChainEngine();
        final ReplayContainer c = skipLeg(engine);
        refused("onSkipped(SESSION_MESSAGE) at S + 64 (the engine never applies a frame it does not hold)",
                () -> c.onSkipped(FrameKind.SESSION_MESSAGE, S + 64, 9));
    }

    @Test
    public void aSkipAfterGenesisIsRefused() {
        final ReplayContainer c = new ReplayContainer(new HashChainEngine(), APP,
                AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR, (p, t, ts, f) -> false);
        c.genesis();
        refused("onSkipped(TIMER_EVENT) after genesis (image position 0)",
                () -> c.onSkipped(FrameKind.TIMER_EVENT, 64, 1));
    }

    @Test
    public void aSkipOfATermEventOrAnActionIsRefused() {
        final ReplayContainer c = skipLeg(new HashChainEngine());
        refused("onSkipped(NEW_LEADERSHIP_TERM): term and app-version check bypassed",
                () -> c.onSkipped(FrameKind.NEW_LEADERSHIP_TERM, C + 64, 9));
        final ReplayContainer d = skipLeg(new HashChainEngine());
        refused("onSkipped(CLUSTER_ACTION)", () -> d.onSkipped(FrameKind.CLUSTER_ACTION, S, 9));
    }

    @Test
    public void anEngineApplyOfAFrameTheImageAlreadyHoldsIsRefused() {
        // SKIP leg wired without its gate: frames in (c, s] would be applied a second time.
        final HashChainEngine engine = new HashChainEngine();
        final ReplayContainer c = skipLeg(engine);
        final long before = engine.count();
        refused("onTimerEvent at C + 64 <= S without a gate",
                () -> c.onTimerEvent(C + 64, 7, 9));
        assertEquals("the engine must not apply it", before, engine.count());
    }
}
