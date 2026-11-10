// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Per-leadership-term timer ids, the core arithmetic in one kit helper. */
public class TermTimerIdTest {

    private static final long BASE = 1_000_000_000_000L;
    private static final long NEXT = 2_000_000_000_000_000L;

    @Test
    public void negativeTermIsBaseAndTheStrideIsAMillion() {
        assertEquals(BASE, TermTimerId.of(BASE, -1));
        assertEquals(BASE, TermTimerId.of(BASE, 0));
        assertEquals(BASE + 1_000_000L, TermTimerId.of(BASE, 1));
        assertEquals(BASE + 3_000_000L, TermTimerId.of(BASE, 3));
        assertEquals(1_000_000L, TermTimerId.TERM_STRIDE);
    }

    @Test
    public void kindIsARangeAndTheTermIsRecoverable() {
        final long id = TermTimerId.of(BASE, 42);
        assertTrue(TermTimerId.isKind(id, BASE));
        assertTrue(TermTimerId.isKind(id, BASE, NEXT));
        assertFalse(TermTimerId.isKind(BASE - 1, BASE));
        assertFalse(TermTimerId.isKind(NEXT, BASE, NEXT));
        assertFalse("a TimerManager counter id is never the flush kind", TermTimerId.isKind(12_345L, BASE));
        assertEquals(42, TermTimerId.termOf(id, BASE));
        assertEquals(0, TermTimerId.termOf(BASE, BASE));
        // CoreLogApplier's ids are exactly these (it computes them with this helper)
        assertEquals(BASE + 7 * 1_000_000L, TermTimerId.of(BASE, 7));
    }
}
