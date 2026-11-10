// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.DirectBuffer;

/**
 * The engine round trip on the in-memory buffer BEFORE the first chunk is offered, because a
 * recording is listable the moment it exists. The round trip (restore into a scratch engine, re-serialize, compare,
 * invariants) is the host's (it needs a second engine); the writer calls it, treats {@code false} or a throw as
 * {@code SELF_CHECK_FAILED}, and compares the SHA-256 payload digest before and after ({@code SELF_CHECK_MUTATED_BUFFER}).
 *
 * <p>{@link #UNCHECKED_TEST_ONLY} is refused by the writer unless the system property {@value #UNCHECKED_PROPERTY} is
 * {@code true}, which hosts never set: a production writer without a self-check is a configuration error. This class
 * is the one place in {@code cluster.shadow} that reads a system property (scan rule 9.1).</p>
 */
public interface ShadowSelfCheck {

    String UNCHECKED_PROPERTY = "oxs.shadow.unchecked";

    /** @return true when the payload round-trips through a scratch engine and its invariants hold */
    boolean verify(DirectBuffer payload, int length);

    /** Accepts everything; kit tests only, behind {@value #UNCHECKED_PROPERTY}. */
    ShadowSelfCheck UNCHECKED_TEST_ONLY = new ShadowSelfCheck() {
        @Override
        public boolean verify(final DirectBuffer payload, final int length) {
            return true;
        }

        @Override
        public String toString() {
            return "UNCHECKED_TEST_ONLY";
        }
    };

    /** Whether {@link #UNCHECKED_TEST_ONLY} may be used in this JVM. */
    static boolean uncheckedAllowed() {
        return Boolean.parseBoolean(System.getProperty(UNCHECKED_PROPERTY));
    }
}
