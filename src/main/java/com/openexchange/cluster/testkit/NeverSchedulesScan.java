// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * The RF-6 "never schedules" scanner: reads compiled class files (nested classes included) and reports every reference
 * to something that can schedule, publish, connect, read a clock or the environment.
 *
 * <p>Matching works on the constant pool. Every {@code CONSTANT_Utf8} is read (class constants, field and method
 * descriptors, {@code Signature} attributes, string constants used reflectively). A forbidden type matches exactly: as
 * a class constant / whole string ({@code io/aeron/cluster/service/Cluster} or its dotted form) or as a descriptor token
 * ({@code Lio/aeron/cluster/service/Cluster;} or {@code ...Cluster<}), never by prefix, because
 * {@code .../Cluster} is a prefix of {@code ClusterClock} and {@code ClusteredService}. A forbidden package
 * ({@link Rules#forbiddenPrefixes}) matches by prefix in the same positions. Members match as a
 * {@code Methodref}/{@code Fieldref}/{@code InterfaceMethodref} owner and name pair.</p>
 *
 * <p>Two inputs: {@link #scanDirectory} (every class under a class directory a predicate admits) and
 * {@link #scanClosure} (the transitive closure of classes reachable from a root inside one package prefix, with the
 * reference path reported on a hit). Test-only tooling: it reads files by design.</p>
 */
public final class NeverSchedulesScan {

    private NeverSchedulesScan() {
    }

    /** One forbidden reference. {@code path} is the chain of classes from the scan root to {@code className}. */
    public record Finding(String className, String rule, String reference, List<String> path) {
        @Override
        public String toString() {
            return String.join(" -> ", path) + ": " + rule + " " + reference;
        }
    }

    public record Report(int classesScanned, List<String> classes, List<Finding> findings) {
        public boolean clean() {
            return findings.isEmpty();
        }
    }

    /**
     * @param forbiddenTypes        internal names that must not be referenced
     * @param forbiddenPrefixes     internal-name prefixes (packages) that must not be referenced
     * @param forbiddenAnyOwner     member names forbidden on any owner
     * @param forbiddenMembers      owner -> member names forbidden on that owner
     * @param forbiddenOwnerPrefixMembers owner prefix -> member names forbidden on any owner under it
     * @param onlyAllowedMembers    owner -> the only member names allowed on that owner, each with its reason
     */
    public record Rules(Set<String> forbiddenTypes, List<String> forbiddenPrefixes, Set<String> forbiddenAnyOwner,
                        Map<String, Set<String>> forbiddenMembers, Map<String, Set<String>> forbiddenOwnerPrefixMembers,
                        Map<String, Map<String, String>> onlyAllowedMembers) {

        /** RF-6 list, used for every scan half: scheduling, publishing, connecting, clocks, environment, properties. */
        public static Rules rf6() {
            return new Rules(
                    Set.of("io/aeron/cluster/service/Cluster",
                            "io/aeron/cluster/service/ClientSession",
                            "com/openexchange/cluster/TimerPort",
                            "io/aeron/Aeron",
                            "io/aeron/Publication",
                            "io/aeron/ExclusivePublication",
                            "io/aeron/ConcurrentPublication",
                            "io/aeron/cluster/client/AeronCluster",
                            "io/aeron/archive/client/AeronArchive",
                            "io/aeron/cluster/service/ClusterClock",
                            "org/agrona/concurrent/EpochClock",
                            "org/agrona/concurrent/NanoClock",
                            "org/agrona/concurrent/SystemEpochClock",
                            "org/agrona/concurrent/SystemNanoClock",
                            "org/agrona/concurrent/EpochNanoClock",
                            "org/agrona/concurrent/SystemEpochNanoClock",
                            "org/agrona/concurrent/OffsetEpochNanoClock",
                            "org/agrona/concurrent/HighResolutionClock",
                            "java/util/Timer",
                            "java/util/TimerTask",
                            "java/util/concurrent/ScheduledExecutorService",
                            "java/util/concurrent/ScheduledThreadPoolExecutor",
                            "java/lang/Thread",
                            "java/time/Clock",
                            "java/time/InstantSource"),
                    List.of(),
                    Set.of("scheduleTimer", "cancelTimer", "newScheduledThreadPool",
                            "newSingleThreadScheduledExecutor"),
                    Map.of("java/lang/System", Set.of("currentTimeMillis", "nanoTime", "getenv", "getProperty",
                                    "getProperties", "setProperty"),
                            "java/lang/Long", Set.of("getLong"),
                            "java/lang/Integer", Set.of("getInteger"),
                            "java/lang/Boolean", Set.of("getBoolean")),
                    Map.of("java/time/", Set.of("now")),
                    Map.of());
        }

        /**
         * Kit half: RF-6, {@code java/lang/System} only for {@code arraycopy} (a pure copy), and no engine-specific
         * type.
         */
        public static Rules kit() {
            final Rules rf6 = rf6();
            return new Rules(rf6.forbiddenTypes(), List.of("com/openexchange/assets/", "com/match/"),
                    rf6.forbiddenAnyOwner(), rf6.forbiddenMembers(), rf6.forbiddenOwnerPrefixMembers(),
                    Map.of("java/lang/System", Map.of("arraycopy", "a pure memory copy: no clock, I/O or state")));
        }
    }

    // ------------------------------------------------------------------------------------------------------ entry

    /** Every class file under {@code classesRoot} whose internal name {@code include} admits. */
    public static Report scanDirectory(final Path classesRoot, final Predicate<String> include, final Rules rules) {
        final Map<String, byte[]> classes = new TreeMap<>();
        try (Stream<Path> files = Files.walk(classesRoot)) {
            for (final Path file : (Iterable<Path>)files.filter(f -> f.toString().endsWith(".class"))::iterator) {
                final String name = internalName(classesRoot, file);
                if (include.test(name)) {
                    classes.put(name, Files.readAllBytes(file));
                }
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final List<Finding> findings = new ArrayList<>();
        for (final Map.Entry<String, byte[]> e : classes.entrySet()) {
            findings.addAll(scan(e.getKey(), e.getValue(), rules, List.of(e.getKey())));
        }
        return new Report(classes.size(), List.copyOf(classes.keySet()), List.copyOf(findings));
    }

    /** {@link #scanClosure(List, String, String, Rules)} over one class root. */
    public static Report scanClosure(final Path classesRoot, final String rootInternalName, final String insidePrefix,
                                     final Rules rules) {
        return scanClosure(List.of(classesRoot), rootInternalName, insidePrefix, rules);
    }

    /**
     * The transitive closure from {@code rootInternalName}: every class whose name starts with {@code insidePrefix}
     * and is referenced (class constant or descriptor) from a class already in the closure, plus the nested classes of
     * each, resolved in {@code classRoots} in order (one root per module). Nothing outside the prefix is followed (JDK,
     * Agrona, Aeron). Fail closed: an in-prefix reference found in no root throws, so a missing module cannot shrink
     * the closure silently.
     *
     * @throws IllegalStateException naming the unresolved class and its reference path
     */
    public static Report scanClosure(final List<Path> classRoots, final String rootInternalName,
                                     final String insidePrefix, final Rules rules) {
        if (classRoots.isEmpty()) {
            throw new IllegalArgumentException("no class roots");
        }
        final Map<String, List<String>> pathTo = new LinkedHashMap<>();
        final Deque<String> queue = new ArrayDeque<>();
        pathTo.put(rootInternalName, List.of(rootInternalName));
        queue.add(rootInternalName);
        final List<Finding> findings = new ArrayList<>();
        while (!queue.isEmpty()) {
            final String name = queue.poll();
            final List<String> path = pathTo.get(name);
            final Path file = resolve(classRoots, name);
            if (file == null) {
                throw new IllegalStateException("unresolved in-prefix class " + name + " via "
                        + String.join(" -> ", path) + " (roots " + classRoots + ")");
            }
            final byte[] bytes;
            try {
                bytes = Files.readAllBytes(file);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            findings.addAll(scan(name, bytes, rules, path));
            final Set<String> next = new LinkedHashSet<>(referencedTypes(ConstantPool.read(bytes)));
            next.addAll(nestedOf(file, name));
            for (final String ref : next) {
                if (ref.startsWith(insidePrefix) && !pathTo.containsKey(ref)) {
                    final List<String> refPath = new ArrayList<>(path);
                    refPath.add(ref);
                    pathTo.put(ref, Collections.unmodifiableList(refPath));
                    queue.add(ref);
                }
            }
        }
        return new Report(pathTo.size(), List.copyOf(pathTo.keySet()), List.copyOf(findings));
    }

    private static Path resolve(final List<Path> classRoots, final String name) {
        for (final Path root : classRoots) {
            final Path file = root.resolve(name + ".class");
            if (Files.isRegularFile(file)) {
                return file;
            }
        }
        return null;
    }

    /** One class from its bytes (for example a class read from a dependency jar). */
    public static List<Finding> scanClass(final String internalName, final byte[] classBytes, final Rules rules) {
        return scan(internalName, classBytes, rules, List.of(internalName));
    }

    // ---------------------------------------------------------------------------------------------------- matching

    private static List<Finding> scan(final String name, final byte[] bytes, final Rules rules,
                                      final List<String> path) {
        final ConstantPool pool = ConstantPool.read(bytes);
        final List<Finding> findings = new ArrayList<>();
        final Set<String> seen = new LinkedHashSet<>();
        for (final String utf8 : pool.utf8s()) {
            for (final String type : rules.forbiddenTypes()) {
                if (matchesType(utf8, type) && seen.add("type " + type)) {
                    findings.add(new Finding(name, "forbidden type", type, path));
                }
            }
            for (final String prefix : rules.forbiddenPrefixes()) {
                if (matchesPrefix(utf8, prefix) && seen.add("prefix " + utf8)) {
                    findings.add(new Finding(name, "forbidden package", utf8, path));
                }
            }
        }
        for (final ConstantPool.MemberRef ref : pool.memberRefs()) {
            final String member = ref.owner() + "." + ref.name();
            if (rules.forbiddenAnyOwner().contains(ref.name()) && seen.add("member " + member)) {
                findings.add(new Finding(name, "forbidden member", member, path));
            }
            final Set<String> forbidden = rules.forbiddenMembers().get(ref.owner());
            if (forbidden != null && forbidden.contains(ref.name()) && seen.add("member " + member)) {
                findings.add(new Finding(name, "forbidden member", member, path));
            }
            for (final Map.Entry<String, Set<String>> e : rules.forbiddenOwnerPrefixMembers().entrySet()) {
                if (ref.owner() != null && ref.owner().startsWith(e.getKey()) && e.getValue().contains(ref.name())
                        && seen.add("member " + member)) {
                    findings.add(new Finding(name, "forbidden member", member, path));
                }
            }
            final Map<String, String> allowed = rules.onlyAllowedMembers().get(ref.owner());
            if (allowed != null && !allowed.containsKey(ref.name()) && seen.add("member " + member)) {
                findings.add(new Finding(name, "member outside the allowed set", member, path));
            }
        }
        return findings;
    }

    /** Exact: the whole string (slashed or dotted), or a descriptor token {@code L<type>;} / {@code L<type><}. */
    static boolean matchesType(final String utf8, final String type) {
        if (utf8.equals(type) || utf8.equals(type.replace('/', '.'))) {
            return true;
        }
        final String token = "L" + type;
        int from = 0;
        int at;
        while ((at = utf8.indexOf(token, from)) >= 0) {
            final int end = at + token.length();
            if (end < utf8.length() && (utf8.charAt(end) == ';' || utf8.charAt(end) == '<')) {
                return true;
            }
            from = at + 1;
        }
        return false;
    }

    /** Package prefix: the string starts with it (slashed or dotted), or a descriptor token {@code L<prefix>...}. */
    static boolean matchesPrefix(final String utf8, final String prefix) {
        return utf8.startsWith(prefix) || utf8.startsWith(prefix.replace('/', '.')) || utf8.contains("L" + prefix);
    }

    private static Set<String> referencedTypes(final ConstantPool pool) {
        final Set<String> types = new LinkedHashSet<>();
        for (final String className : pool.classNames()) {
            String n = className;
            while (n.startsWith("[")) {
                n = n.substring(1);
            }
            if (n.startsWith("L") && n.endsWith(";")) {
                n = n.substring(1, n.length() - 1);
            }
            types.add(n);
        }
        for (final String utf8 : pool.utf8s()) {
            int at = utf8.indexOf('L');
            while (at >= 0) {
                final int end = indexOfAny(utf8, at + 1);
                if (end > at + 1) {
                    types.add(utf8.substring(at + 1, end));
                }
                at = utf8.indexOf('L', at + 1);
            }
        }
        return types;
    }

    private static int indexOfAny(final String s, final int from) {
        for (int i = from; i < s.length(); i++) {
            final char ch = s.charAt(i);
            if (ch == ';' || ch == '<') {
                return i;
            }
            if (!(Character.isJavaIdentifierPart(ch) || ch == '/' || ch == '$')) {
                return -1;
            }
        }
        return -1;
    }

    private static List<String> nestedOf(final Path file, final String name) {
        final Path dir = file.getParent();
        final String simple = file.getFileName().toString().replace(".class", "");
        final List<String> nested = new ArrayList<>();
        try (Stream<Path> siblings = Files.list(dir)) {
            siblings.map(p -> p.getFileName().toString())
                    .filter(f -> f.startsWith(simple + "$") && f.endsWith(".class"))
                    .sorted()
                    .forEach(f -> nested.add(name.substring(0, name.length() - simple.length())
                            + f.substring(0, f.length() - ".class".length())));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return nested;
    }

    private static String internalName(final Path root, final Path file) {
        final String relative = root.relativize(file).toString().replace('\\', '/');
        return relative.substring(0, relative.length() - ".class".length());
    }

    // -------------------------------------------------------------------------------------------- constant pool

    /** The parts of a class file's constant pool the rules need (JVMS 4.4). */
    static final class ConstantPool {
        record MemberRef(String owner, String name) { }

        private final List<String> utf8s;
        private final List<String> classNames;
        private final List<MemberRef> memberRefs;

        private ConstantPool(final List<String> utf8s, final List<String> classNames,
                             final List<MemberRef> memberRefs) {
            this.utf8s = utf8s;
            this.classNames = classNames;
            this.memberRefs = memberRefs;
        }

        List<String> utf8s() {
            return utf8s;
        }

        List<String> classNames() {
            return classNames;
        }

        List<MemberRef> memberRefs() {
            return memberRefs;
        }

        static ConstantPool read(final byte[] bytes) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                if (in.readInt() != 0xCAFEBABE) {
                    throw new IllegalArgumentException("not a class file");
                }
                in.readUnsignedShort();
                in.readUnsignedShort();
                final int count = in.readUnsignedShort();
                final String[] utf8 = new String[count];
                final int[] classIndex = new int[count];
                final int[][] memberRef = new int[count][];
                final int[][] nameAndType = new int[count][];
                for (int i = 1; i < count; i++) {
                    final int tag = in.readUnsignedByte();
                    switch (tag) {
                        case 1 -> utf8[i] = in.readUTF();
                        case 3, 4 -> in.readInt();
                        case 5, 6 -> {
                            in.readLong();
                            i++;
                        }
                        case 7, 19, 20 -> classIndex[i] = tag == 7 ? in.readUnsignedShort() : -in.readUnsignedShort();
                        case 8, 16 -> in.readUnsignedShort();
                        case 9, 10, 11 -> memberRef[i] = new int[] {in.readUnsignedShort(), in.readUnsignedShort()};
                        case 12 -> nameAndType[i] = new int[] {in.readUnsignedShort(), in.readUnsignedShort()};
                        case 15 -> {
                            in.readUnsignedByte();
                            in.readUnsignedShort();
                        }
                        case 17, 18 -> {
                            in.readUnsignedShort();
                            in.readUnsignedShort();
                        }
                        default -> throw new IllegalArgumentException("unknown constant pool tag " + tag + " at " + i);
                    }
                }
                final List<String> utf8s = new ArrayList<>();
                for (final String s : utf8) {
                    if (s != null) {
                        utf8s.add(s);
                    }
                }
                final List<String> classNames = new ArrayList<>();
                final Map<Integer, String> classByIndex = new HashMap<>();
                for (int i = 1; i < count; i++) {
                    if (classIndex[i] > 0) {
                        classNames.add(utf8[classIndex[i]]);
                        classByIndex.put(i, utf8[classIndex[i]]);
                    }
                }
                final List<MemberRef> refs = new ArrayList<>();
                for (int i = 1; i < count; i++) {
                    if (memberRef[i] != null) {
                        final String owner = classByIndex.get(memberRef[i][0]);
                        final int[] nat = nameAndType[memberRef[i][1]];
                        refs.add(new MemberRef(owner, utf8[nat[0]]));
                    }
                }
                return new ConstantPool(List.copyOf(utf8s), List.copyOf(classNames), List.copyOf(refs));
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
