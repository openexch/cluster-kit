// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/** Who wrote a shadow (header {@code producerKind}); the alias letter is {@code R} or {@code F}. */
public enum ProducerKind {
    REPLICA(1, 'R'), FOLLOWER(2, 'F');

    private final int code;
    private final char letter;

    ProducerKind(final int code, final char letter) {
        this.code = code;
        this.letter = letter;
    }

    public int code() {
        return code;
    }

    public char letter() {
        return letter;
    }

    /** The kind for a wire code, or null. */
    public static ProducerKind of(final int code) {
        for (final ProducerKind k : values()) {
            if (k.code == code) {
                return k;
            }
        }
        return null;
    }

    /** The kind for an alias letter, or null. */
    public static ProducerKind ofLetter(final char letter) {
        for (final ProducerKind k : values()) {
            if (k.letter == letter) {
                return k;
            }
        }
        return null;
    }
}
