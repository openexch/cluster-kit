// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * The A+B vs A+SNAPSHOT+B check at EVERY cut of a scenario: for each prefix length k, a fresh replica
 * applies lines [0,k), snapshots, restores from those bytes, applies [k,n), and must end with exactly
 * the events and exactly the final snapshot bytes of the unspliced run. It also requires the snapshot
 * taken at the cut to be reproducible (two serializations of the same state agree), which is the
 * "bytes are a function of state, not history" property.
 *
 * <p>JUnit-free: a divergence throws {@link AssertionError} naming the cut and the line.</p>
 */
public final class SnapshotSpliceRunner {

    private SnapshotSpliceRunner() {
    }

    /** Outcome of a full run, for callers that want numbers rather than an exception. */
    public record Report(int lines, int cuts, int baselineEvents, int baselineSnapshotBytes) { }

    public static Report run(final Supplier<? extends Replica> fresh, final List<String> rawLines) {
        final List<String> lines = ScenarioText.executableLines(rawLines);
        final Replica baseline = fresh.get();
        for (final String line : lines) {
            baseline.apply(line);
        }
        final List<String> expectedEvents = List.copyOf(baseline.events());
        final byte[] expectedBytes = baseline.snapshot();
        if (!Arrays.equals(expectedBytes, baseline.snapshot())) {
            throw new AssertionError("baseline snapshot is not reproducible: two serializations of the same state differ");
        }

        int cuts = 0;
        for (int k = 0; k <= lines.size(); k++) {
            final Replica r = fresh.get();
            for (int i = 0; i < k; i++) {
                r.apply(lines.get(i));
            }
            final byte[] atCut = r.snapshot();
            if (!Arrays.equals(atCut, r.snapshot())) {
                throw new AssertionError("snapshot at cut " + k + " is not reproducible (after line "
                        + (k == 0 ? "<none>" : lines.get(k - 1)) + ")");
            }
            r.restore(atCut);
            if (!Arrays.equals(atCut, r.snapshot())) {
                throw new AssertionError("restore at cut " + k + " does not reproduce the bytes it was restored from");
            }
            for (int i = k; i < lines.size(); i++) {
                r.apply(lines.get(i));
            }
            final List<String> got = r.events();
            if (!expectedEvents.equals(got)) {
                throw new AssertionError("events diverge with a snapshot spliced at cut " + k + " (before line "
                        + (k < lines.size() ? "'" + lines.get(k) + "'" : "<end>") + "):\n"
                        + GoldenAssert.diff(String.join("\n", expectedEvents), String.join("\n", got)));
            }
            if (!Arrays.equals(expectedBytes, r.snapshot())) {
                throw new AssertionError("final snapshot bytes diverge with a snapshot spliced at cut " + k
                        + " (before line " + (k < lines.size() ? "'" + lines.get(k) + "'" : "<end>") + ")");
            }
            cuts++;
        }
        return new Report(lines.size(), cuts, expectedEvents.size(), expectedBytes.length);
    }
}
