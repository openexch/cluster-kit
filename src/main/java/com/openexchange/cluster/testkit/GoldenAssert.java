// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Golden-master comparison for determinism scenarios. JUnit-free: it throws {@link AssertionError}
 * itself, so it can live in the kit and serve both engines' test suites.
 *
 * <p>Compares rendered output to a committed {@code .golden} file. Any divergence, any change that
 * alters the engine's output for a fixed input, fails loudly with a line diff. Regenerate
 * intentionally with {@code -Dupdate.golden=true} (then review and commit the diff). A missing golden
 * fails rather than silently auto-passing, so a scenario cannot ship without its expected output.</p>
 */
public final class GoldenAssert {

    public static final String UPDATE_PROPERTY = "update.golden";

    private final Path goldenDir;

    public GoldenAssert(final Path goldenDir) {
        this.goldenDir = goldenDir;
    }

    public void match(final String scenarioName, final String actual) {
        final Path golden = goldenDir.resolve(scenarioName + ".golden");
        final boolean update = Boolean.getBoolean(UPDATE_PROPERTY);
        try {
            if (update) {
                Files.createDirectories(goldenDir);
                Files.writeString(golden, actual, StandardCharsets.UTF_8);
                System.out.println("[GOLDEN] wrote " + golden);
                return;
            }
            if (!Files.exists(golden)) {
                throw new AssertionError("No golden for '" + scenarioName + "' at " + golden
                        + " (generate it with -D" + UPDATE_PROPERTY + "=true, then review and commit it)");
            }
            final String expected = Files.readString(golden, StandardCharsets.UTF_8);
            if (!expected.equals(actual)) {
                throw new AssertionError("Determinism golden mismatch for '" + scenarioName + "':\n"
                        + diff(expected, actual)
                        + "\nIf intentional, regenerate with -D" + UPDATE_PROPERTY + "=true and commit.");
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Minimal line-oriented diff: '-' expected, '+' actual, for the lines that differ ({@link TextDiff}). */
    public static String diff(final String expected, final String actual) {
        return TextDiff.diff(expected, actual);
    }
}
