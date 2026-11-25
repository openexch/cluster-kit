// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The layouts one build carries: it reads the current layout and the one before it, refuses everything else by
 * name, and picks the layout it writes from the recorded behaviour version alone.
 */
public class SnapshotLayoutsTest {

    /** A build that carries one layout, v6, written from behaviour 1. */
    private static SnapshotLayouts<String> oneLayout() {
        return SnapshotLayouts.<String>of("balance snapshot")
                .current(6, -6L, 1L, "v6")
                .retired(5, -5L)
                .retired(4, -4L)
                .build();
    }

    /** The next build: v7 adopted by behaviour 5, v6 still carried. */
    private static SnapshotLayouts<String> twoLayouts() {
        return SnapshotLayouts.<String>of("balance snapshot")
                .current(7, -7L, 5L, "v7")
                .previous(6, -6L, 1L, "v6")
                .retired(5, -5L)
                .retired(4, -4L)
                .build();
    }

    /** The build after that: v6 is retired. */
    private static SnapshotLayouts<String> afterNext() {
        return SnapshotLayouts.<String>of("balance snapshot")
                .current(8, -8L, 6L, "v8")
                .previous(7, -7L, 5L, "v7")
                .retired(6, -6L)
                .build();
    }

    private static String refusal(final Runnable r) {
        return assertThrows(IllegalStateException.class, r::run).getMessage();
    }

    @Test
    public void theCurrentAndThePreviousLayoutAreRead() {
        final SnapshotLayouts<String> layouts = twoLayouts();
        assertEquals("v7", layouts.readerFor(-7L).codec());
        assertEquals("v6", layouts.readerFor(-6L).codec());
        assertSame(layouts.current(), layouts.readerFor(-7L));
        assertSame(layouts.previous(), layouts.readerFor(-6L));
        assertEquals(6, layouts.previous().version());
        assertNull("one layout: no previous", oneLayout().previous());
        assertEquals("v6", oneLayout().readerFor(-6L).codec());
    }

    @Test
    public void aLayoutOlderThanThePreviousOneIsRefusedByName() {
        final String twoBack = refusal(() -> afterNext().readerFor(-6L));
        assertTrue(twoBack, twoBack.contains("balance snapshot layout v6 (tag -6) refused: older than the previous layout"));
        assertTrue(twoBack, twoBack.contains("reads only v8 and v7"));
        final String single = refusal(() -> oneLayout().readerFor(-5L));
        assertTrue(single, single.contains("layout v5 (tag -5) refused: older than the previous layout"));
        assertTrue(single, single.contains("reads only v6."));
    }

    @Test
    public void anUnknownTagIsRefusedWithItsOwnText() {
        for (final long tag : new long[] {-7L, 0L, 6L, Long.MIN_VALUE, 0x4145534EL}) {
            final String text = refusal(() -> oneLayout().readerFor(tag));
            assertTrue(text, text.contains("balance snapshot layout tag " + tag + " refused: unknown to this build"));
            assertTrue(text, text.contains("reads only v6."));
            assertFalse("an unknown tag is not called old: " + text, text.contains("older than"));
        }
        // the build that retired v6 has never heard of v9
        final String newer = refusal(() -> afterNext().readerFor(-9L));
        assertTrue(newer, newer.contains("layout tag -9 refused: unknown to this build, which reads only v8 and v7"));
    }

    @Test
    public void theWrittenLayoutFollowsTheBehaviourVersionOnly() {
        final SnapshotLayouts<String> older = oneLayout();
        final SnapshotLayouts<String> newer = twoLayouts();
        // every behaviour the older build runs writes the same layout on both builds
        for (long behavior = 1; behavior <= 4; behavior++) {
            assertEquals(-6L, older.writtenBy(behavior).tag());
            assertEquals("behaviour " + behavior + ": the newer build keeps writing the previous layout",
                    older.writtenBy(behavior).tag(), newer.writtenBy(behavior).tag());
        }
        // the switch to behaviour 5 adopts the current layout, and every later behaviour keeps it
        assertEquals(-7L, newer.writtenBy(5L).tag());
        assertEquals(-7L, newer.writtenBy(6L).tag());
        assertEquals(7, newer.writtenBy(Long.MAX_VALUE).version());
        assertTrue(newer.writes(1L));
        assertTrue(newer.writes(5L));
    }

    @Test
    public void aBehaviourWhoseLayoutIsNoLongerCarriedIsRefused() {
        final SnapshotLayouts<String> layouts = afterNext();
        assertFalse(layouts.writes(4L));
        assertTrue(layouts.writes(5L));
        final String text = refusal(() -> layouts.writtenBy(4L));
        assertTrue(text, text.contains("behaviour 4 writes a layout this build no longer carries"));
        assertTrue(text, text.contains("v7 from behaviour 5, v8 from behaviour 6"));
        for (final long behavior : new long[] {0L, -1L, Long.MIN_VALUE}) {
            assertFalse(oneLayout().writes(behavior));
            final String low = refusal(() -> oneLayout().writtenBy(behavior));
            assertTrue(low, low.contains("behaviour " + behavior + " writes a layout this build no longer carries"));
        }
    }

    @Test
    public void anImageMustCarryTheLayoutItsOwnBehaviourWrites() {
        final SnapshotLayouts<String> layouts = twoLayouts();
        layouts.requireWrittenBy(-6L, 1L);
        layouts.requireWrittenBy(-6L, 4L);
        layouts.requireWrittenBy(-7L, 5L);
        final String oldTagNewBehaviour = refusal(() -> layouts.requireWrittenBy(-6L, 5L));
        assertTrue(oldTagNewBehaviour,
                oldTagNewBehaviour.contains("layout v6 does not belong to behaviour 5, which writes v7"));
        final String newTagOldBehaviour = refusal(() -> layouts.requireWrittenBy(-7L, 4L));
        assertTrue(newTagOldBehaviour,
                newTagOldBehaviour.contains("layout v7 does not belong to behaviour 4, which writes v6"));
        // the tag is checked first: an unreadable tag keeps its own refusal
        final String unknown = refusal(() -> layouts.requireWrittenBy(-9L, 5L));
        assertTrue(unknown, unknown.contains("unknown to this build"));
        final String retired = refusal(() -> layouts.requireWrittenBy(-5L, 5L));
        assertTrue(retired, retired.contains("older than the previous layout"));
    }

    @Test
    public void aBuildThatCannotBeRightIsRefusedWhenItIsDeclared() {
        final SnapshotLayouts.Builder<String> noCurrent = SnapshotLayouts.of("image");
        assertBad("a current layout is required", noCurrent::build);
        assertBad("the image name is required", () -> SnapshotLayouts.of(" "));
        assertBad("previous v7 must be older than current v7", () -> SnapshotLayouts.<String>of("image")
                .current(7, -7L, 5L, "a").previous(7, -6L, 1L, "b").build());
        // two layouts written from one behaviour: the written layout would not be a function of the behaviour
        assertBad("must be adopted by a later behaviour", () -> SnapshotLayouts.<String>of("image")
                .current(7, -7L, 1L, "a").previous(6, -6L, 1L, "b").build());
        assertBad("tag -6 names two layouts", () -> SnapshotLayouts.<String>of("image")
                .current(7, -6L, 5L, "a").previous(6, -6L, 1L, "b").build());
        assertBad("tag -7 names two layouts", () -> SnapshotLayouts.<String>of("image")
                .current(7, -7L, 5L, "a").retired(5, -7L).build());
        assertBad("retired v6 must be older than every layout this build carries", () -> SnapshotLayouts.<String>of("image")
                .current(7, -7L, 5L, "a").previous(6, -6L, 1L, "b").retired(6, -66L).build());
        assertBad("is listed twice", () -> SnapshotLayouts.<String>of("image")
                .current(7, -7L, 5L, "a").retired(5, -5L).retired(4, -5L).build());
        assertBad("a current layout is already set", () -> SnapshotLayouts.<String>of("image")
                .current(7, -7L, 5L, "a").current(8, -8L, 6L, "b"));
        assertBad("a previous layout is already set", () -> SnapshotLayouts.<String>of("image")
                .previous(6, -6L, 1L, "a").previous(5, -5L, 1L, "b"));
        assertBad("sinceBehavior 0 must be >= 1", () -> SnapshotLayouts.<String>of("image").current(7, -7L, 0L, "a"));
        assertBad("layout version 0 must be >= 1", () -> SnapshotLayouts.<String>of("image").current(0, -7L, 1L, "a"));
    }

    private static void assertBad(final String text, final Runnable r) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, r::run);
        assertTrue("expected '" + text + "', got: " + e.getMessage(), e.getMessage().contains(text));
    }
}
