// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/**
 * A member's own verdict about the shadows its archive holds, published where every other member can read it over the
 * archive control channel alone: the alias of one small marker recording on {@link #STREAM_ID},
 * {@code oxf.v1.<engine>.c<clusterId>.g<genesisId>.m<memberId>.a<anchor>.w<witnesses>.v<margin + 1>.s<seq>}.
 *
 * <p>What it states, about the archive it is listed in, at the time it was written (from a scan that could answer:
 * no scan error, the startup pass done, so every verdict is a deep read of this process):</p>
 * <ul>
 *   <li>{@code a}: the newest position at which this archive holds VERIFIED ALIGNED copies, not in CONFLICT, from
 *       {@code w} distinct producers (two copies of one producer are one witness);</li>
 *   <li>{@code v}: the newest position below {@code a} with a VERIFIED copy (one producer is enough: the margin), or
 *       -1; written as {@code margin + 1}, so {@code v0} is "none".</li>
 * </ul>
 *
 * <p>Why a marker: a recording listing carries a shadow's alias, start and stop, never its header, its digest or the
 * deep verdict, and VERIFIED lives in the verifying process's memory only. A stopped shadow in a listing may be
 * CORRUPT, or one side of a CONFLICT (a kept pulled copy whose digest disagrees with the local one): two producers at
 * one position in a listing is exactly the poisoned case, so a listing of shadows can never stand in for the verdict.
 * The marker carries the verdict of the one process that read the bytes. It is a locator of a claim, not a proof: the
 * reader ({@link RemoteFloor}) applies its own anchor rule to the stated witnesses and trusts no more than the member
 * stated.</p>
 *
 * <p>One marker per archive: the writer ({@link FloorAttestor}) purges every other one, and retracts (purges) its own
 * when the stated facts stop holding. The marker recording holds no data; the alias is the whole content.</p>
 */
public record FloorAttestation(int engineId, int clusterId, long genesisId, int memberId, long anchor, int witnesses,
                               long margin, long seq) {

    /** The marker stream (the shadows are on 4301, their replays on 4302, the listing responses on 4304). */
    public static final int STREAM_ID = 4305;
    /** The response stream of the leader's attestation listing clients. */
    public static final int LISTING_RESPONSE_STREAM_ID = 4306;
    static final String HEAD = "oxf.v1.";
    /** The term length of the marker's (empty) publication: Aeron's minimum. */
    static final String TERM_LENGTH = "64k";

    public FloorAttestation {
        if (clusterId < 0 || genesisId <= 0 || memberId < 0 || anchor < 0 || witnesses < 1 || margin < -1
                || margin >= anchor && margin != -1 || seq < 1) {
            throw new IllegalArgumentException("attestation out of range: c" + clusterId + " g" + genesisId + " m"
                    + memberId + " a" + anchor + " w" + witnesses + " v" + margin + " s" + seq);
        }
    }

    /** {@code alias=oxf.v1.<engine>.}: every marker of the engine, whatever cluster, genesis or member. */
    public static String prefix(final int engineId) {
        return ShadowAlias.PARAM + HEAD + ShadowAlias.engineToken(engineId) + ".";
    }

    /** The recorded publication channel of this marker. */
    public String channel() {
        return "aeron:ipc?term-length=" + TERM_LENGTH + "|" + ShadowAlias.PARAM + format();
    }

    public String format() {
        return HEAD + ShadowAlias.engineToken(engineId) + ".c" + clusterId + ".g" + genesisId + ".m" + memberId + ".a"
                + anchor + ".w" + witnesses + ".v" + (margin + 1) + ".s" + seq;
    }

    /** The same facts (anchor, witnesses, margin), whatever the seq. */
    public boolean sameFacts(final FloorAttestation other) {
        return other != null && engineId == other.engineId && clusterId == other.clusterId
                && genesisId == other.genesisId && memberId == other.memberId && anchor == other.anchor
                && witnesses == other.witnesses && margin == other.margin;
    }

    /**
     * The marker of an original channel, or null when the channel carries no alias or one that is not exactly a
     * marker: a missing, reordered or extra token, a sign, a leading zero, a non-digit, an out-of-range number, a
     * genesis of 0, no witness, a margin at or above the anchor. Never throws.
     */
    public static FloorAttestation tryParse(final String originalChannel) {
        final String alias = ShadowAlias.aliasOfChannel(originalChannel);
        if (alias == null || alias.length() > ShadowAlias.MAX_LENGTH || !alias.startsWith(HEAD)) {
            return null;
        }
        final String[] t = alias.substring(HEAD.length()).split("\\.", -1);
        if (t.length != 8) {
            return null;
        }
        final int engine = ShadowAlias.engineOf(t[0]);
        final long cluster = number(t[1], 'c', Integer.MAX_VALUE);
        final long genesis = number(t[2], 'g', Long.MAX_VALUE);
        final long member = number(t[3], 'm', Integer.MAX_VALUE);
        final long anchor = number(t[4], 'a', Long.MAX_VALUE);
        final long witnesses = number(t[5], 'w', Integer.MAX_VALUE);
        final long marginPlusOne = number(t[6], 'v', Long.MAX_VALUE);
        final long seq = number(t[7], 's', Long.MAX_VALUE);
        if (engine < 0 || cluster < 0 || genesis < 0 || member < 0 || anchor < 0 || witnesses < 0 || marginPlusOne < 0
                || seq < 0) {
            return null;
        }
        try {
            return new FloorAttestation(engine, (int)cluster, genesis, (int)member, anchor, (int)witnesses,
                    marginPlusOne - 1, seq);
        } catch (final IllegalArgumentException outOfRange) {
            return null;
        }
    }

    /** {@code <letter><unsigned decimal, no leading zero>} at most {@code max}, or -1. */
    private static long number(final String token, final char letter, final long max) {
        if (token.length() < 2 || token.charAt(0) != letter || token.length() > 20
                || (token.length() > 2 && token.charAt(1) == '0')) {
            return -1;
        }
        long v = 0;
        for (int i = 1; i < token.length(); i++) {
            final int d = token.charAt(i) - '0';
            if (d < 0 || d > 9 || v > (max - d) / 10) {
                return -1;
            }
            v = v * 10 + d;
        }
        return v;
    }

    @Override
    public String toString() {
        return "m" + memberId + "{anchor=" + anchor + " witnesses=" + witnesses + " margin=" + margin + " seq=" + seq
                + "}";
    }
}
