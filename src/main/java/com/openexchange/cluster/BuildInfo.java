// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Which build a node runs: {@code Implementation-Version} and {@code Build-Sha} from the manifest of the jar an
 * engine class was loaded from. Both values are telemetry for {@code /health} and {@code /ready}; they decide
 * nothing.
 *
 * <p>Values are restricted to {@code [A-Za-z0-9._+-]} so they can never break the space-separated
 * {@code key=value} body or forge another token, and a missing or empty value reads {@code unknown}, so a token is
 * never empty. A dev build without the release stamp says {@code version=unknown}.</p>
 *
 * <p>The manifest is read from the class's own code source, not via {@code getResource("/META-INF/MANIFEST.MF")},
 * which returns whichever jar comes first on the class path.</p>
 */
public record BuildInfo(String version, String build) {

    public static final String UNKNOWN = "unknown";
    public static final String VERSION_ATTRIBUTE = "Implementation-Version";
    public static final String BUILD_ATTRIBUTE = "Build-Sha";

    private static final BuildInfo UNKNOWN_BUILD = new BuildInfo(UNKNOWN, UNKNOWN);

    public BuildInfo {
        version = sanitize(version);
        build = sanitize(build);
    }

    public static BuildInfo unknown() {
        return UNKNOWN_BUILD;
    }

    /** The build identity of the jar {@code type} was loaded from; {@link #unknown()} when it cannot be read. */
    public static BuildInfo of(final Class<?> type) {
        if (type == null) {
            return UNKNOWN_BUILD;
        }
        final CodeSource source;
        try {
            source = type.getProtectionDomain().getCodeSource();
        } catch (final SecurityException e) {
            return UNKNOWN_BUILD;
        }
        return source == null ? UNKNOWN_BUILD : fromCodeSource(source.getLocation());
    }

    /** A jar file or a classes directory; {@link #unknown()} when neither yields a manifest. */
    public static BuildInfo fromCodeSource(final URL location) {
        if (location == null) {
            return UNKNOWN_BUILD;
        }
        final File file;
        try {
            file = new File(location.toURI());
        } catch (final URISyntaxException | IllegalArgumentException e) {
            return UNKNOWN_BUILD;
        }
        try {
            if (file.isDirectory()) {
                final Path mf = file.toPath().resolve(JarFile.MANIFEST_NAME);
                if (!Files.isRegularFile(mf)) {
                    return UNKNOWN_BUILD;
                }
                try (InputStream in = Files.newInputStream(mf)) {
                    return fromManifest(new Manifest(in));
                }
            }
            if (!file.isFile()) {
                return UNKNOWN_BUILD;
            }
            try (JarFile jar = new JarFile(file, false)) {
                return fromManifest(jar.getManifest());
            }
        } catch (final IOException | SecurityException e) {
            return UNKNOWN_BUILD;
        }
    }

    public static BuildInfo fromManifest(final Manifest manifest) {
        if (manifest == null) {
            return UNKNOWN_BUILD;
        }
        return new BuildInfo(manifest.getMainAttributes().getValue(VERSION_ATTRIBUTE),
                manifest.getMainAttributes().getValue(BUILD_ATTRIBUTE));
    }

    /** Keep only {@code [A-Za-z0-9._+-]}; {@code unknown} when nothing is left. */
    public static String sanitize(final String value) {
        if (value == null) {
            return UNKNOWN;
        }
        final StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '+' || c == '-') {
                out.append(c);
            }
        }
        return out.length() == 0 ? UNKNOWN : out.toString();
    }

    /** The body suffix {@code " version=<v> build=<sha>"}, leading space included. */
    public String tokens() {
        return " version=" + version + " build=" + build;
    }
}
