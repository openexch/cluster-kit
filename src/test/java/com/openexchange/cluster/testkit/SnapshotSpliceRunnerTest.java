// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** RC1 S2: the splice runner must catch history-dependent state and pass state-only engines. */
public class SnapshotSpliceRunnerTest {

    /** A toy counter engine: ADD n, EMIT. Its snapshot is the counter; its events are the emitted values. */
    private static final class Counter implements Replica {
        long value;
        final List<String> events = new ArrayList<>();
        /** When true the snapshot leaks history (number of applies), which a restore cannot reproduce. */
        final boolean leaky;
        long applies;

        Counter(final boolean leaky) {
            this.leaky = leaky;
        }

        public void apply(final String line) {
            applies++;
            final String[] t = ScenarioText.tokens(line);
            switch (t[0]) {
                case "ADD": value += Long.parseLong(t[1]); break;
                case "EMIT": events.add("V=" + value); break;
                default: throw new IllegalArgumentException(line);
            }
        }

        public List<String> events() {
            return List.copyOf(events);
        }

        public byte[] snapshot() {
            return ByteBuffer.allocate(16).putLong(value).putLong(leaky ? applies : 0L).array();
        }

        public void restore(final byte[] bytes) {
            final ByteBuffer b = ByteBuffer.wrap(bytes);
            value = b.getLong();
            applies = 0; // a restored engine has no memory of how many lines produced its state
        }
    }

    private static final List<String> SCENARIO = List.of(
            "# toy", "ADD 1", "EMIT", "", "ADD 41  # forty-one", "EMIT", "ADD -2", "EMIT");

    @Test
    public void aStateOnlyEnginePassesAtEveryCut() {
        final SnapshotSpliceRunner.Report r = SnapshotSpliceRunner.run(() -> new Counter(false), SCENARIO);
        assertEquals(6, r.lines());
        assertEquals(7, r.cuts());
        assertEquals(3, r.baselineEvents());
        assertEquals(16, r.baselineSnapshotBytes());
    }

    @Test
    public void historyInTheSnapshotIsCaughtAndTheCutIsNamed() {
        final AssertionError e = assertThrows(AssertionError.class,
                () -> SnapshotSpliceRunner.run(() -> new Counter(true), SCENARIO));
        assertTrue(e.getMessage(), e.getMessage().contains("cut"));
    }
}
