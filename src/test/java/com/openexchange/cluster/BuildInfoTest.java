// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.Assert.assertEquals;

/** The build identity read from the manifest of the jar a class was loaded from. */
public class BuildInfoTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static Manifest manifest(final String version, final String sha) {
        final Manifest m = new Manifest();
        m.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (version != null) {
            m.getMainAttributes().putValue(BuildInfo.VERSION_ATTRIBUTE, version);
        }
        if (sha != null) {
            m.getMainAttributes().putValue(BuildInfo.BUILD_ATTRIBUTE, sha);
        }
        return m;
    }

    private File jar(final Manifest m) throws IOException {
        final File f = tmp.newFile("engine.jar");
        try (OutputStream out = new FileOutputStream(f); JarOutputStream jar = m == null
                ? new JarOutputStream(out) : new JarOutputStream(out, m)) {
            jar.flush();
        }
        return f;
    }

    @Test
    public void aJarManifestGivesVersionAndSha() throws IOException {
        final BuildInfo b = BuildInfo.fromCodeSource(jar(manifest("0.5.1", "9f3c2ab")).toURI().toURL());
        assertEquals("0.5.1", b.version());
        assertEquals("9f3c2ab", b.build());
    }

    @Test
    public void missingAttributesAreUnknown() throws IOException {
        final BuildInfo onlySha = BuildInfo.fromCodeSource(jar(manifest(null, "9f3c2ab")).toURI().toURL());
        assertEquals(BuildInfo.UNKNOWN, onlySha.version());
        assertEquals("9f3c2ab", onlySha.build());
    }

    @Test
    public void aJarWithoutAManifestIsUnknown() throws IOException {
        final BuildInfo b = BuildInfo.fromCodeSource(jar(null).toURI().toURL());
        assertEquals(BuildInfo.unknown(), b);
    }

    @Test
    public void aClassesDirectoryReadsItsOwnManifestFile() throws IOException {
        final File dir = tmp.newFolder("classes");
        Files.createDirectories(dir.toPath().resolve("META-INF"));
        try (OutputStream out = new FileOutputStream(dir.toPath().resolve("META-INF/MANIFEST.MF").toFile())) {
            manifest("2.0", "abcdef0").write(out);
        }
        assertEquals(new BuildInfo("2.0", "abcdef0"), BuildInfo.fromCodeSource(dir.toURI().toURL()));
        assertEquals(BuildInfo.unknown(), BuildInfo.fromCodeSource(tmp.newFolder("bare").toURI().toURL()));
    }

    @Test
    public void unreadableSourcesAreUnknownNotAThrow() throws IOException {
        assertEquals(BuildInfo.unknown(), BuildInfo.fromCodeSource(null));
        assertEquals(BuildInfo.unknown(), BuildInfo.fromCodeSource(new File(tmp.getRoot(), "gone.jar").toURI().toURL()));
        assertEquals("a bootstrap class has no code source", BuildInfo.unknown(), BuildInfo.of(String.class));
        assertEquals(BuildInfo.unknown(), BuildInfo.of(null));
    }

    @Test
    public void valuesAreSanitizedToTheTokenAlphabet() {
        assertEquals("an underscore is outside the alphabet", "ab", BuildInfo.sanitize("a_b"));
        assertEquals("1.0-rc.1+g9f3c2ab", BuildInfo.sanitize(" 1.0-rc.1+g9f3c2ab\r\n"));
        assertEquals("abc", BuildInfo.sanitize("a b=c;"));
        assertEquals(BuildInfo.UNKNOWN, BuildInfo.sanitize(null));
        assertEquals(BuildInfo.UNKNOWN, BuildInfo.sanitize(""));
        assertEquals(BuildInfo.UNKNOWN, BuildInfo.sanitize("= ;ç"));
        assertEquals(" version=unknown build=unknown", BuildInfo.unknown().tokens());
    }
}
