// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import java.util.function.Supplier;

/** Same input through two independent fresh engines must be byte-identical. JUnit-free. */
public final class RunTwiceAssert {

    private RunTwiceAssert() {
    }

    public static String assertIdentical(final String what, final Supplier<String> run) {
        final String a = run.get();
        final String b = run.get();
        if (!a.equals(b)) {
            throw new AssertionError("Run-twice divergence in '" + what + "':\n" + GoldenAssert.diff(a, b));
        }
        return a;
    }
}
