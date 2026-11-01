// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/**
 * The archive alias of a shadow recording (RCS S3 section 4):
 * {@code oxs.v1.<engine>.c<clusterId>.g<genesisId>.p<P_s>.t<term>.r<R|F><producerId>.s<seq>.k<kind>}.
 * Unsigned decimal, no leading zeros, characters {@code [a-z0-9.RF]} only, never a URI separator. The alias is a
 * locator, never an authority (A1): the header is trusted and the alias must equal its fields. Discovery lists
 * {@code alias=oxs.v1.<engine>.} on {@link #STREAM_ID} (the leading {@code alias=} anchors the substring match of
 * {@code listRecordingsForUri} to the parameter, A0:LRU:61).
 */
public record ShadowAlias(int engineId, int clusterId, long genesisId, long logPosition, long leadershipTermId,
                          int producerKind, int producerId, long producerSeq, int kind) {

    /** The shadow recording stream: a constant, so every host finds every shadow with the kit alone (D-S3-8). */
    public static final int STREAM_ID = 4301;
    /** The stream every shadow replay is delivered on; subscriptions are session-scoped (8.1). */
    public static final int REPLAY_STREAM_ID = 4302;
    /** Every replay channel: no linger, sparse (6.6); a {@code term-length} would be ignored by the archive. */
    public static final String REPLAY_CHANNEL = "aeron:ipc?linger=0|sparse=true";
    /** Bound on the alias length; the longest legal alias is 125 characters (review FIX-10). */
    public static final int MAX_LENGTH = 128;
    public static final String PARAM = "alias=";
    static final String HEAD = "oxs.v1.";

    /** Thrown by {@link #parse}: the message names the refused token (rule A2). */
    public static final class InvalidAliasException extends IllegalArgumentException {
        private final String token;

        InvalidAliasException(final String token, final String message) {
            super(message);
            this.token = token;
        }

        public String token() {
            return token;
        }
    }

    public static String engineToken(final int engineId) {
        return switch (engineId) {
            case ShadowHeader.ENGINE_ASSETS -> "assets";
            case ShadowHeader.ENGINE_MATCH -> "match";
            case ShadowHeader.ENGINE_TEST -> "test99";
            default -> throw new IllegalArgumentException("no alias engine token for engineId " + engineId);
        };
    }

    static int engineOf(final String token) {
        return switch (token) {
            case "assets" -> ShadowHeader.ENGINE_ASSETS;
            case "match" -> ShadowHeader.ENGINE_MATCH;
            case "test99" -> ShadowHeader.ENGINE_TEST;
            default -> -1;
        };
    }

    /** {@code alias=oxs.v1.<engine>.}: what the catalog lists on every scan (review FIX-8). */
    public static String prefix(final int engineId) {
        return PARAM + HEAD + engineToken(engineId) + ".";
    }

    /** {@code alias=oxs.v1.<engine>.c<clusterId>.g<genesisId>.}: an operator narrowing. */
    public static String prefix(final int engineId, final int clusterId, final long genesisId) {
        return prefix(engineId) + "c" + clusterId + ".g" + genesisId + ".";
    }

    /** The recorded publication channel: IPC with the host's term length and this alias. */
    public static String channel(final String alias, final String ipcTermLength) {
        return "aeron:ipc?term-length=" + ipcTermLength + "|" + PARAM + alias;
    }

    /** Formats and self-checks (characters, length, and that it parses back to the same fields). */
    public String format() {
        final ProducerKind pk = ProducerKind.of(producerKind);
        if (pk == null) {
            throw new IllegalArgumentException("alias producerKind " + producerKind);
        }
        final String alias = format(engineToken(engineId), pk.letter(), true);
        final ShadowAlias back = parse(alias);
        if (!back.equals(this)) {
            throw new IllegalStateException("alias does not parse back: " + alias + " -> " + back);
        }
        return alias;
    }

    /** {@code checkCharacters=false} only for the NC-1 negative control (the character rule switched off). */
    String format(final String engineToken, final char producerLetter, final boolean checkCharacters) {
        final String alias = HEAD + engineToken + ".c" + clusterId + ".g" + genesisId + ".p" + logPosition + ".t"
                + leadershipTermId + ".r" + producerLetter + producerId + ".s" + producerSeq + ".k" + kind;
        if (checkCharacters) {
            for (int i = 0; i < alias.length(); i++) {
                final char ch = alias.charAt(i);
                if (!((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '.' || ch == 'R' || ch == 'F')) {
                    throw new IllegalArgumentException("alias contains a URI separator or foreign character '" + ch
                            + "': " + alias);
                }
            }
            if (alias.length() > MAX_LENGTH) {
                throw new IllegalArgumentException("alias longer than " + MAX_LENGTH + ": " + alias.length());
            }
        }
        return alias;
    }

    /** The value of the {@code alias=} parameter of an original channel, or null when it has none. */
    public static String aliasOfChannel(final String channel) {
        if (channel == null) {
            return null;
        }
        final int q = channel.indexOf('?');
        if (q < 0) {
            return null;
        }
        for (final String param : channel.substring(q + 1).split("\\|")) {
            if (param.startsWith(PARAM)) {
                return param.substring(PARAM.length());
            }
        }
        return null;
    }

    /**
     * Rule A2: refuses a missing, reordered, duplicated, signed or non-numeric token, a leading zero, an out-of-range
     * number, a {@code k} outside 1..4, a producer letter other than {@code R}/{@code F}, or a {@code g} of 0.
     *
     * @throws InvalidAliasException naming the token
     */
    public static ShadowAlias parse(final String alias) {
        if (alias == null || alias.isEmpty()) {
            throw invalid("alias", "empty alias");
        }
        if (alias.length() > MAX_LENGTH) {
            throw invalid("alias", "alias longer than " + MAX_LENGTH);
        }
        if (!alias.startsWith(HEAD)) {
            throw invalid("oxs.v1", "alias does not start with " + HEAD + ": " + alias);
        }
        final String[] t = alias.substring(HEAD.length()).split("\\.", -1);
        if (t.length != 8) {
            throw invalid("tokens", "alias has " + t.length + " tokens after " + HEAD + ", expected 8: " + alias);
        }
        final int engine = engineOf(t[0]);
        if (engine < 0) {
            throw invalid("engine", "unknown engine token '" + t[0] + "'");
        }
        final int clusterId = (int)number(t[1], 'c', Integer.MAX_VALUE, 0);
        final long genesisId = number(t[2], 'g', Long.MAX_VALUE, 1);
        final long logPosition = number(t[3], 'p', Long.MAX_VALUE, 0);
        final long term = number(t[4], 't', Long.MAX_VALUE, 0);
        if (t[5].length() < 3 || t[5].charAt(0) != 'r') {
            throw invalid("r", "producer token must be r<R|F><id>: '" + t[5] + "'");
        }
        final ProducerKind pk = ProducerKind.ofLetter(t[5].charAt(1));
        if (pk == null) {
            throw invalid("r", "producer letter must be R or F: '" + t[5] + "'");
        }
        final int producerId = (int)digits(t[5].substring(2), "r", Integer.MAX_VALUE, 0);
        final long seq = number(t[6], 's', Long.MAX_VALUE, 1);
        final int kind = (int)number(t[7], 'k', 4, 1);
        return new ShadowAlias(engine, clusterId, genesisId, logPosition, term, pk.code(), producerId, seq, kind);
    }

    /** {@link #parse} without the exception: null when refused. */
    public static ShadowAlias tryParse(final String alias) {
        try {
            return parse(alias);
        } catch (final InvalidAliasException e) {
            return null;
        }
    }

    private static long number(final String token, final char letter, final long max, final long min) {
        if (token.isEmpty() || token.charAt(0) != letter) {
            throw invalid(String.valueOf(letter), "expected token '" + letter + "<n>', got '" + token + "'");
        }
        return digits(token.substring(1), String.valueOf(letter), max, min);
    }

    private static long digits(final String s, final String token, final long max, final long min) {
        if (s.isEmpty()) {
            throw invalid(token, "token " + token + " has no digits");
        }
        if (s.length() > 1 && s.charAt(0) == '0') {
            throw invalid(token, "token " + token + " has a leading zero: '" + s + "'");
        }
        long v = 0;
        for (int i = 0; i < s.length(); i++) {
            final char ch = s.charAt(i);
            if (ch < '0' || ch > '9') {
                throw invalid(token, "token " + token + " is not unsigned decimal: '" + s + "'");
            }
            final int d = ch - '0';
            if (max - d < 0 || v > (max - d) / 10) { // v * 10 + d <= max, without overflow
                throw invalid(token, "token " + token + " out of range: '" + s + "'");
            }
            v = v * 10 + d;
        }
        if (v < min) {
            throw invalid(token, "token " + token + " below " + min + ": '" + s + "'");
        }
        return v;
    }

    private static InvalidAliasException invalid(final String token, final String message) {
        return new InvalidAliasException(token, message);
    }
}
