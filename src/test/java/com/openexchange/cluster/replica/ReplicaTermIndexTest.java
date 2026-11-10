// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import com.openexchange.cluster.shadow.ShadowHeader;
import com.openexchange.cluster.shadow.ShadowIdentity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Seed plus observed terms answer STRICT; an unseen term is HISTORY_UNVERIFIED. */
public class ReplicaTermIndexTest {

    static final int ENGINE = ShadowHeader.ENGINE_TEST;
    static final long GENESIS = 0x5eed;

    static ShadowHeader header(final long termId, final long termBase, final long position) {
        return new ShadowHeader(ENGINE, 0, 1, 1, 3, 1, GENESIS, position, termId, termBase, 1_000, 0, 0, 64, 0,
                4096, 0);
    }

    @Test
    public void seedAndObservedTermsAnswerStrict() {
        final ReplicaTermIndex index = new ReplicaTermIndex();
        index.seed(2, 4096);
        index.observe(3, 8192);
        index.observe(3, 8192); // re-seen after a re-attach: idempotent
        final ShadowIdentity.LocalIdentity local = index.local(ENGINE, 0, GENESIS);
        assertEquals("seeded term not OK", ShadowIdentity.Outcome.OK,
                ShadowIdentity.check(header(2, 4096, 6000), local));
        assertEquals("observed term not OK", ShadowIdentity.Outcome.OK,
                ShadowIdentity.check(header(3, 8192, 9000), local));
        assertTrue(ShadowIdentity.admits(header(3, 8192, 9000), local, ShadowIdentity.Policy.STRICT));
        assertEquals(2, index.size());
        assertEquals(2, index.seededTerm());
    }

    @Test
    public void anUnseenTermIsHistoryUnverified() {
        final ReplicaTermIndex index = new ReplicaTermIndex();
        index.seed(2, 4096);
        final ShadowIdentity.LocalIdentity local = index.local(ENGINE, 0, GENESIS);
        assertEquals("unseen term", ShadowIdentity.Outcome.HISTORY_UNVERIFIED,
                ShadowIdentity.check(header(1, 0, 100), local));
        assertEquals(-1, index.termBaseOf(1));
        assertEquals("another base for a known term", ShadowIdentity.Outcome.HISTORY_MISMATCH,
                ShadowIdentity.check(header(2, 8192, 9000), local));
    }

    @Test
    public void aTermWithTwoBasesIsRefusedLoudly() {
        final ReplicaTermIndex index = new ReplicaTermIndex();
        index.seed(2, 4096);
        try {
            index.observe(2, 8192);
            fail("a second base for term 2 was accepted");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("term 2 seen with term base 4096 and 8192"));
        }
        assertEquals(4096, index.termBaseOf(2));
    }
}
