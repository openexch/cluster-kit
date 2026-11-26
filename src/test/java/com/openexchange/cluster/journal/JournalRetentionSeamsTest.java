// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The negative-control seams of {@link JournalRetention} are public statics (an engine's tests have to reach them),
 * so two things are held here: every seam is off on a fresh class load, and no main class of the kit writes one or,
 * outside {@code JournalRetention}, even names the holder. Surefire runs every test class in a fresh JVM, so the
 * default is read from a fresh class load.
 */
public class JournalRetentionSeamsTest {

    private static final String SEAMS = "com/openexchange/cluster/journal/JournalRetention$Seams";
    private static final String OWNER = "com/openexchange/cluster/journal/JournalRetention";

    @After
    public void allOff() {
        JournalRetention.Seams.reset();
    }

    private static List<Field> seams() {
        final List<Field> fields = new ArrayList<>();
        for (final Field f : JournalRetention.Seams.class.getDeclaredFields()) {
            if (!f.isSynthetic()) {
                fields.add(f);
            }
        }
        return fields;
    }

    @Test
    public void everySeamIsOffByDefaultAndResetSwitchesEveryOneOff() throws Exception {
        final List<Field> fields = seams();
        assertEquals("the eight seams: " + fields, 8, fields.size());
        for (final Field f : fields) {
            assertEquals(f.getName(), boolean.class, f.getType());
            assertTrue(f.getName() + " is static", Modifier.isStatic(f.getModifiers()));
            assertTrue(f.getName() + " is volatile", Modifier.isVolatile(f.getModifiers()));
            assertFalse("seam " + f.getName() + " is off on a fresh class load", f.getBoolean(null));
        }
        for (final Field f : fields) {
            f.setBoolean(null, true);
        }
        JournalRetention.Seams.reset();
        for (final Field f : fields) {
            assertFalse("reset switches " + f.getName() + " off", f.getBoolean(null));
        }
    }

    /** Constant pool indices of the field references of {@code bytes} whose owner is the seams holder. */
    private static Set<Integer> seamFieldRefs(final byte[] bytes, final int[] poolEnd) {
        final ByteBuffer b = ByteBuffer.wrap(bytes);
        b.position(8);
        final int count = b.getShort() & 0xFFFF;
        final Map<Integer, String> utf8 = new HashMap<>();
        final Map<Integer, Integer> className = new HashMap<>();
        final Map<Integer, Integer> fieldOwner = new HashMap<>();
        for (int i = 1; i < count; i++) {
            final int tag = b.get() & 0xFF;
            switch (tag) {
                case 1: {
                    final byte[] text = new byte[b.getShort() & 0xFFFF];
                    b.get(text);
                    utf8.put(i, new String(text, StandardCharsets.UTF_8));
                    break;
                }
                case 3: case 4: case 12: case 10: case 11: case 17: case 18:
                    b.position(b.position() + 4);
                    break;
                case 5: case 6:
                    b.position(b.position() + 8);
                    i++;
                    break;
                case 7:
                    className.put(i, b.getShort() & 0xFFFF);
                    break;
                case 9:
                    fieldOwner.put(i, b.getShort() & 0xFFFF);
                    b.position(b.position() + 2);
                    break;
                case 8: case 16: case 19: case 20:
                    b.position(b.position() + 2);
                    break;
                case 15:
                    b.position(b.position() + 3);
                    break;
                default:
                    throw new AssertionError("constant pool tag " + tag);
            }
        }
        poolEnd[0] = b.position();
        final Set<Integer> refs = new TreeSet<>();
        for (final Map.Entry<Integer, Integer> e : fieldOwner.entrySet()) {
            if (SEAMS.equals(utf8.get(className.get(e.getValue())))) {
                refs.add(e.getKey());
            }
        }
        return refs;
    }

    /** Whether the class holds a {@code putstatic} of a seam (opcode 0xB3 with a seam's field reference). */
    private static boolean writesASeam(final byte[] bytes, final Set<Integer> refs, final int from) {
        for (int i = from; i + 2 < bytes.length; i++) {
            if ((bytes[i] & 0xFF) == 0xB3 && refs.contains(((bytes[i + 1] & 0xFF) << 8) | (bytes[i + 2] & 0xFF))) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void noMainClassOfTheKitWritesASeamAndOnlyTheRetentionReadsThem() throws IOException, URISyntaxException {
        final Path main = Path.of(JournalRetention.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final Set<String> readers = new TreeSet<>();
        final Set<String> writers = new TreeSet<>();
        int scanned = 0;
        try (Stream<Path> files = Files.walk(main)) {
            for (final Path file : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".class"))::iterator) {
                final String name = main.relativize(file).toString().replace('\\', '/').replace(".class", "");
                final byte[] bytes = Files.readAllBytes(file);
                final int[] poolEnd = {0};
                final Set<Integer> refs = seamFieldRefs(bytes, poolEnd);
                scanned++;
                if (refs.isEmpty()) {
                    continue;
                }
                readers.add(name);
                if (writesASeam(bytes, refs, poolEnd[0])) {
                    writers.add(name);
                }
            }
        }
        assertTrue("scanned the kit's main classes: " + scanned, scanned >= 100);
        // the scan sees a write where there is one: the holder's own reset()
        assertEquals("only the holder's reset() writes a seam", Set.of(SEAMS), writers);
        final Set<String> outside = new HashSet<>();
        for (final String reader : readers) {
            if (!reader.equals(OWNER) && !reader.startsWith(OWNER + "$")) {
                outside.add(reader);
            }
        }
        assertEquals("a main class outside JournalRetention names a seam", Set.of(), outside);
        assertTrue("the retention reads its seams: " + readers, readers.contains(OWNER));
    }
}
