// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import java.util.List;

/**
 * What a determinism harness needs from one replica of an engine: apply a scenario line, expose the
 * rendered events so far, produce the snapshot bytes of the current state, and restart from snapshot
 * bytes (a fresh engine restored from them, the event log kept). Engine-specific runners implement it;
 * {@link SnapshotSpliceRunner} drives it.
 */
public interface Replica {

    void apply(String line);

    /** Rendered events so far, in emission order; never the live list of a running replica. */
    List<String> events();

    /** Snapshot bytes of the current state (payload only or enveloped: the replica decides, consistently). */
    byte[] snapshot();

    /** Replace the state with one restored from {@code bytes}; keep the events already rendered. */
    void restore(byte[] bytes);
}
