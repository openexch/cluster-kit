// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One position group (RCS S3 6.2 step 4): the content identity {@code (engineId, clusterId, genesisId, logPosition)}
 * with its copies. Producer, kind, seq and layout hint are attributes of a copy; retention and "newest" count
 * positions, never recordings (MF-9). {@code conflict} = the copies disagree on content: every copy is excluded from
 * every query and the host raises CRITICAL; there is never a winner (R Q-R11, MF-5).
 */
public record LogicalShadow(int engineId, int clusterId, long genesisId, long logPosition, List<ShadowEntry> copies,
                            boolean conflict) {

    public LogicalShadow {
        copies = List.copyOf(copies);
    }

    /** Copies grouped by producer ({@code R3}, {@code F1}), in first-seen order. D14: two producers agree when
     *  {@code producers().size() >= 2} on an agreeing VERIFIED group. */
    public Map<String, List<ShadowEntry>> producers() {
        final Map<String, List<ShadowEntry>> out = new LinkedHashMap<>();
        for (final ShadowEntry e : copies) {
            out.computeIfAbsent(e.producer(), k -> new ArrayList<>()).add(e);
        }
        return out;
    }

    /** At least one copy VERIFIED in this process. */
    public boolean verified() {
        return !conflict && copies.stream().anyMatch(e -> e.state() == ShadowEntry.State.VERIFIED);
    }

    /** The first VERIFIED copy, else the first copy: what a loader is offered. */
    public ShadowEntry representative() {
        for (final ShadowEntry e : copies) {
            if (e.state() == ShadowEntry.State.VERIFIED) {
                return e;
            }
        }
        return copies.get(0);
    }

    @Override
    public String toString() {
        return "LogicalShadow{P=" + logPosition + (conflict ? " CONFLICT" : "") + " copies=" + copies + "}";
    }
}
