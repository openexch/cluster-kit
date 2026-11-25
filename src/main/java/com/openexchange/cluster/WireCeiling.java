// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

/**
 * The wire ceiling rule, shared by both engines: which ingress header versions a member applies, and how the
 * recorded ceiling may change.
 *
 * <p>Each engine records a wire ceiling ({@code maxWireVersion}) in its replicated config. It is decided by a
 * config command in the log and rides the snapshot, so every member holds the same value at the same log
 * position. A frame whose header version is above the recorded ceiling is dropped from the header alone, before
 * the jar's own supported range is consulted. That is what lets two members on different jars do the same thing
 * with the same frame during a rolling upgrade: the later jar could decode the frame, but it drops it exactly like
 * the earlier jar until the ceiling is raised.</p>
 *
 * <p>The ceiling only rises. A member whose jar supports less than the recorded ceiling cannot follow the log and
 * stops itself; that is a node-local check, never a reject of the command.</p>
 *
 * <p>Pure functions: no I/O, no Aeron, no state. The schema id check stays in each engine.</p>
 */
public final class WireCeiling {

    /** Largest legal uint16 value; 65535 is the SBE uint16 null. */
    public static final int MAX_CEILING = 65_534;

    /** What a member does with one ingress frame, decided from its header. */
    public enum Verdict {
        /** Decode at the header's version and apply. */
        ACCEPT,
        /** Above the recorded ceiling: dropped by every member, whatever its jar. */
        DROP_ABOVE_CEILING,
        /** Outside this jar's supported range: the jar's own schema drop. */
        DROP_OUT_OF_RANGE
    }

    private WireCeiling() {
    }

    /**
     * The gate, in order: (1) a recorded ceiling exists and the header version is above it gives
     * {@link Verdict#DROP_ABOVE_CEILING}; (2) the header version is outside {@code [jarMin, jarMax]} gives
     * {@link Verdict#DROP_OUT_OF_RANGE}; (3) otherwise {@link Verdict#ACCEPT}. Step 1 never reads the jar range.
     *
     * @param headerVersion the version in the frame's message header
     * @param recordedMax   the recorded ceiling, 0 when nothing is recorded (the ceiling rule is then off)
     * @param jarMin        the lowest header version this jar accepts
     * @param jarMax        the highest header version this jar accepts
     */
    public static Verdict gate(final int headerVersion, final long recordedMax, final int jarMin, final int jarMax) {
        if (recordedMax > 0 && headerVersion > recordedMax) {
            return Verdict.DROP_ABOVE_CEILING;
        }
        if (headerVersion < jarMin || headerVersion > jarMax) {
            return Verdict.DROP_OUT_OF_RANGE;
        }
        return Verdict.ACCEPT;
    }

    /**
     * @param incomingMax the ceiling a config command carries
     * @param minMax      the lowest ceiling the engine may record
     * @return null when {@code incomingMax} is in {@code [minMax, 65534]}, else the refusal text
     */
    public static String invalidReason(final long incomingMax, final int minMax) {
        if (incomingMax < minMax || incomingMax > MAX_CEILING) {
            return "maxWireVersion=" + incomingMax + " must be in [" + minMax + ", " + MAX_CEILING + "]";
        }
        return null;
    }

    /** True when a config carrying {@code incomingMax} would lower the recorded ceiling. */
    public static boolean lowers(final long incomingMax, final long recordedMax) {
        return incomingMax < recordedMax;
    }

    /**
     * The node-local check: a jar whose highest supported version is below the recorded ceiling cannot follow
     * the log.
     *
     * @param recordedMax the recorded ceiling, 0 when nothing is recorded
     * @param jarMax      the highest header version this jar accepts
     * @param schemaName  the schema's name in the text, for example {@code "money-schema"}
     * @return null when this jar can follow the log, else the failure text
     */
    public static String jarBelow(final long recordedMax, final int jarMax, final String schemaName) {
        if (recordedMax > jarMax) {
            return "recorded maxWireVersion=" + recordedMax + " is above this jar's " + schemaName
                    + " version " + jarMax;
        }
        return null;
    }
}
