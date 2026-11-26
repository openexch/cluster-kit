// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.cluster.VersionValidator;
import org.agrona.SemanticVersion;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Which builds may share one cluster. A member version is {@code (generation, level)}: the generation changes only
 * when two builds can never share a cluster (a clean genesis), the level rises by exactly one in every release that
 * changes the build's capability manifest ({@link Manifest}). Aeron carries it as the {@code appVersion} of every
 * consensus module and service container ({@code SemanticVersion.compose(generation, level >>> 8, level & 0xFF)}),
 * stamps it on every leadership term and snapshot, and asks the validator on the receiving side.
 *
 * <p><b>The rule:</b> one generation, levels at most one apart. A cluster runs level N and N-1, in either direction
 * (an upgraded follower under an old leader, an old follower under a new leader). Every other pair is refused by the
 * member that receives it, with exit 78 ({@link EngineLifecycle#EXIT_CONFIG}): a restart cannot fix a build.</p>
 *
 * <p>Aeron asks only after a leader exists (canvass and vote messages carry no version), so a member two levels
 * ahead could win an election and stop every older follower. {@link #checkBeforeLaunch} stops that member first, from
 * one node-local file, {@code <clusterDir>/member-compat.seen}: the range of versions this node's validator has
 * accepted. The file is evidence about this node only and never feeds a replicated decision; a reseed or a wipe of the
 * cluster directory removes it with the rest.</p>
 *
 * <p>Each engine declares its own constant; the kit holds no engine's number. Instances are immutable.</p>
 */
public final class MemberCompat {

    /** The node-local file of {@link #checkBeforeLaunch}, in the consensus module's cluster directory. */
    public static final String SEEN_FILE = "member-compat.seen";
    public static final String SEEN_FORMAT = "member-compat-seen/2";
    /** The format before {@code last_own_level}: refused with its own text. */
    static final String SEEN_FORMAT_1 = "member-compat-seen/1";
    public static final int MIN_GENERATION = 1;
    public static final int MAX_GENERATION = 255;
    public static final int MAX_LEVEL = 65_535;
    /** How many levels apart two members of one generation may be. */
    public static final int LEVEL_WINDOW = 1;
    private static final int MAX_SEEN_BYTES = 1024;

    private final int generation;
    private final int level;

    private MemberCompat(final int generation, final int level) {
        this.generation = generation;
        this.level = level;
    }

    /**
     * A build's own version.
     *
     * @throws IllegalArgumentException for a generation outside 1..255 or a level outside 0..65535
     */
    public static MemberCompat of(final int generation, final int level) {
        if (generation < MIN_GENERATION || generation > MAX_GENERATION) {
            throw new IllegalArgumentException("member compat generation " + generation + " is outside "
                    + MIN_GENERATION + ".." + MAX_GENERATION);
        }
        if (level < 0 || level > MAX_LEVEL) {
            throw new IllegalArgumentException("member compat level " + level + " is outside 0.." + MAX_LEVEL);
        }
        return new MemberCompat(generation, level);
    }

    /**
     * The version another member, a log or a snapshot carries, decoded from an Aeron app version. Never refused:
     * a pre-S4 build carries Aeron's default {@code 0.0.1}, which decodes as {@code g0.1} (generation 0, never
     * compatible).
     */
    public static MemberCompat fromAppVersion(final int appVersion) {
        return new MemberCompat(SemanticVersion.major(appVersion),
                (SemanticVersion.minor(appVersion) << 8) | SemanticVersion.patch(appVersion));
    }

    public int generation() {
        return generation;
    }

    public int level() {
        return level;
    }

    /** The Aeron {@code appVersion} of this version. */
    public int appVersion() {
        return SemanticVersion.compose(generation, level >>> 8, level & 0xFF);
    }

    /** True when the two may share a cluster: one generation, levels at most {@link #LEVEL_WINDOW} apart. */
    public boolean compatibleWith(final MemberCompat other) {
        return generation == other.generation && Math.abs(level - other.level) <= LEVEL_WINDOW;
    }

    /** {@code g<generation>.<level>}. */
    @Override
    public String toString() {
        return "g" + generation + "." + level;
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof MemberCompat m && m.generation == generation && m.level == level;
    }

    @Override
    public int hashCode() {
        return generation * 65_536 + level;
    }

    // ------------------------------------------------------------------ refusal

    /**
     * What a node does when its validator refuses: called once, before the validator returns false. The validator
     * never throws into Aeron (a thrown validator is counted and dropped by the agent, and the node would neither stop
     * nor follow), so a fault of the {@code seen} file reaches the node through {@link #seenFileFault} too.
     */
    @FunctionalInterface
    public interface Refusal {
        /** {@code other} is outside the window of {@code own}. */
        void refuse(MemberCompat own, MemberCompat other);

        /** The {@code seen} file could not be read or written; {@code text} names the file. */
        default void seenFileFault(final String text) {
            System.err.println("member version check FATAL (exit " + EngineLifecycle.EXIT_CONFIG + "): " + text);
        }
    }

    /**
     * The one-line reason of a refusal by the validator. {@link EngineLifecycle#fatal} prefixes it with
     * {@code <name> FATAL (exit 78): }. Aeron passes the validator two integers, so the text cannot say whether a
     * leader, the log or a snapshot carried the other version, and names all three.
     */
    public static String refusalText(final MemberCompat own, final String buildSha, final MemberCompat other) {
        return "member version incompatible: this build is " + own + " (build " + buildSha
                + "), the cluster's leader, log or snapshot is " + other + "; " + RULE_AND_RECOVERY;
    }

    /** The reason of a refusal by {@link #checkBeforeLaunch} against the levels this member's validator accepted. */
    public static String lastSeenRefusalText(final MemberCompat own, final String buildSha, final MemberCompat seen) {
        return "member version incompatible: this build is " + own + " (build " + buildSha
                + "), the cluster's last seen level is " + seen + "; " + RULE_AND_RECOVERY;
    }

    /** The reason of a refusal by {@link #checkBeforeLaunch} against the level this member last launched. */
    public static String lastOwnRefusalText(final MemberCompat own, final String buildSha, final MemberCompat last) {
        return "member version incompatible: this build is " + own + " (build " + buildSha
                + "), this member last ran " + last + "; a member moves up one level at a time; " + RULE_AND_RECOVERY;
    }

    private static final String RULE_AND_RECOVERY = "a cluster runs level N and N-1 of one generation; install a"
            + " compatible build, a restart cannot fix this";

    /**
     * The production refusal: {@code lifecycle.fatal(EXIT_CONFIG, text)} for an incompatible version and for a fault of
     * the {@code seen} file. The first recorded stop wins, so the Aeron FATAL that follows does not turn the exit
     * into 1.
     */
    public static Refusal fatalOnRefusal(final EngineLifecycle lifecycle, final BuildInfo build) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        final String sha = (build == null ? BuildInfo.unknown() : build).build();
        return new Refusal() {
            @Override
            public void refuse(final MemberCompat own, final MemberCompat other) {
                lifecycle.fatal(EngineLifecycle.EXIT_CONFIG, refusalText(own, sha, other));
            }

            @Override
            public void seenFileFault(final String text) {
                lifecycle.fatal(EngineLifecycle.EXIT_CONFIG, text);
            }
        };
    }

    /**
     * A refusal for in-process test rigs, installed explicitly (there is no lifecycle to stop): it prints the text,
     * and the validator still returns false, so Aeron terminates the refusing component.
     */
    public static Refusal testRefusal(final String name) {
        return (own, other) -> System.err.println(name + " FATAL (exit " + EngineLifecycle.EXIT_CONFIG + "): "
                + refusalText(own, BuildInfo.unknown().build(), other));
    }

    /** The refusal of a member whose node never installed one: {@link #install} then refuses every version. */
    public static final Refusal NOT_INSTALLED = (own, other) -> { };

    static final String NOT_INSTALLED_TEXT = "member version check: no refusal was installed on this node"
            + " (MemberCompat.install without the lifecycle's refusal); every version is refused (fail closed)";

    // ------------------------------------------------------------------ validator

    /**
     * The validator for the consensus module, every service container and the replay processes (replica, shadow,
     * oracle), with no {@code seen} file (replay processes, tests).
     */
    public static VersionValidator validator(final MemberCompat own, final Refusal onRefusal) {
        return new Validator(own, onRefusal, null);
    }

    /**
     * The members' validator: as {@link #validator(MemberCompat, Refusal)}, and every accepted version outside the
     * range in {@code <clusterDir>/member-compat.seen} widens the file before the validator returns. One instance per
     * process, shared by the consensus module and the service containers (they call it from their own threads).
     */
    public static VersionValidator validator(final MemberCompat own, final Refusal onRefusal, final File clusterDir) {
        return new Validator(own, onRefusal, Objects.requireNonNull(clusterDir, "clusterDir"));
    }

    private static final class Validator implements VersionValidator {
        private final MemberCompat own;
        private final Refusal onRefusal;
        private final File clusterDir;
        /** The file as last read or written by this instance; null until first needed. */
        private Seen seen;

        Validator(final MemberCompat own, final Refusal onRefusal, final File clusterDir) {
            this.own = Objects.requireNonNull(own, "own");
            this.onRefusal = Objects.requireNonNull(onRefusal, "onRefusal");
            this.clusterDir = clusterDir;
        }

        @Override
        public boolean isVersionCompatible(final int contextAppVersion, final int appVersionUnderTest) {
            final MemberCompat other = fromAppVersion(appVersionUnderTest);
            if (contextAppVersion != own.appVersion() || !own.compatibleWith(other)) {
                // a context that carries another version than this validator's own is a wiring fault: refuse it
                onRefusal.refuse(own, other);
                return false;
            }
            if (clusterDir != null) {
                try {
                    record(other);
                } catch (final RuntimeException fault) {
                    // never into Aeron: an agent counts and drops it, and the node would neither stop nor follow
                    onRefusal.seenFileFault(fault.getMessage() == null ? String.valueOf(fault) : fault.getMessage());
                    return false;
                }
            }
            return true;
        }

        private synchronized void record(final MemberCompat other) {
            if (seen == null) {
                seen = readSeenOrNull(clusterDir);
                if (seen == null) {
                    // first acceptance with no file: the own version (as launched) and the accepted one
                    final Seen first = new Seen(own.generation, own.level, own.level, own.level).widen(other.level);
                    writeSeenUnchecked(clusterDir, first);
                    seen = first;
                    return;
                }
            }
            if (seen.generation() != other.generation) {
                // the file names another generation than the one just accepted (accepted means equal to own): a
                // file from another cluster generation was left in this directory; replace it with this one's
                final Seen replaced = new Seen(own.generation, Math.min(own.level, other.level),
                        Math.max(own.level, other.level), own.level);
                writeSeenUnchecked(clusterDir, replaced);
                seen = replaced;
                return;
            }
            final Seen widened = seen.widen(other.level);
            if (!widened.equals(seen)) {
                writeSeenUnchecked(clusterDir, widened);
                seen = widened;
            }
        }
    }

    // ------------------------------------------------------------------ the seen file

    /**
     * The range of levels of one generation this node's validator has accepted, and the level this node last
     * launched ({@link #checkBeforeLaunch} records it on every launch).
     */
    public record Seen(int generation, int levelMin, int levelMax, int lastOwnLevel) {
        public Seen {
            if (levelMin > levelMax) {
                throw new IllegalArgumentException("level_min " + levelMin + " > level_max " + levelMax);
            }
        }

        /** This range, widened to hold {@code level}. Never narrows; the last own level is kept. */
        public Seen widen(final int level) {
            return level >= levelMin && level <= levelMax ? this
                    : new Seen(generation, Math.min(levelMin, level), Math.max(levelMax, level), lastOwnLevel);
        }

        /** The file's bytes: the five lines, then {@code sha256=} of those bytes. */
        byte[] encode() {
            final String body = "format=" + SEEN_FORMAT + "\n" + "generation=" + generation + "\n"
                    + "level_min=" + levelMin + "\n" + "level_max=" + levelMax + "\n"
                    + "last_own_level=" + lastOwnLevel + "\n";
            final byte[] covered = body.getBytes(StandardCharsets.US_ASCII);
            return (body + "sha256=" + OffBoxWatermark.sha256Hex(covered, covered.length) + "\n")
                    .getBytes(StandardCharsets.US_ASCII);
        }
    }

    /** The file of {@code clusterDir}. */
    public static Path seenFile(final File clusterDir) {
        return clusterDir.toPath().resolve(SEEN_FILE);
    }

    /**
     * The file's content, or null when there is no file.
     *
     * @throws IllegalStateException naming the file when it exists but cannot be read or does not verify
     */
    public static Seen readSeenOrNull(final File clusterDir) {
        final Path file = seenFile(clusterDir);
        final byte[] bytes;
        try {
            if (Files.size(file) > MAX_SEEN_BYTES) {
                throw new IllegalStateException(unreadable(file, "larger than " + MAX_SEEN_BYTES + " bytes"));
            }
            bytes = Files.readAllBytes(file);
        } catch (final NoSuchFileException absent) {
            return null;
        } catch (final IOException e) {
            throw new IllegalStateException(unreadable(file, e.toString()), e);
        }
        return decodeSeen(file, bytes);
    }

    private static Seen decodeSeen(final Path file, final byte[] bytes) {
        final String text = new String(bytes, StandardCharsets.US_ASCII);
        if (text.startsWith("format=" + SEEN_FORMAT_1 + "\n")) {
            throw new IllegalStateException(unreadable(file, "format " + SEEN_FORMAT_1 + " was written by a"
                    + " pre-release build and does not record the level this member last ran"));
        }
        final String[] lines = text.split("\n", -1);
        if (lines.length != 7 || !lines[6].isEmpty()) {
            throw new IllegalStateException(unreadable(file, "expected 6 lines"));
        }
        final String[] keys = {"format", "generation", "level_min", "level_max", "last_own_level", "sha256"};
        final String[] values = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String prefix = keys[i] + "=";
            if (!lines[i].startsWith(prefix)) {
                throw new IllegalStateException(unreadable(file, "line " + (i + 1) + " is not " + prefix + "..."));
            }
            values[i] = lines[i].substring(prefix.length());
        }
        final int coveredLength = text.length() - lines[5].length() - 1;
        final String expected = OffBoxWatermark.sha256Hex(bytes, coveredLength);
        if (!expected.equals(values[5])) {
            throw new IllegalStateException(unreadable(file, "sha256 mismatch"));
        }
        if (!SEEN_FORMAT.equals(values[0])) {
            throw new IllegalStateException(unreadable(file, "format " + values[0] + " is not " + SEEN_FORMAT));
        }
        try {
            final int generation = Integer.parseInt(values[1]);
            final int min = Integer.parseInt(values[2]);
            final int max = Integer.parseInt(values[3]);
            final int lastOwn = Integer.parseInt(values[4]);
            if (generation < 0 || generation > MAX_GENERATION || min < 0 || max > MAX_LEVEL || min > max
                    || lastOwn < 0 || lastOwn > MAX_LEVEL) {
                throw new IllegalStateException(unreadable(file, "values out of range"));
            }
            return new Seen(generation, min, max, lastOwn);
        } catch (final NumberFormatException e) {
            throw new IllegalStateException(unreadable(file, "a value is not a number"));
        }
    }

    private static String unreadable(final Path file, final String why) {
        return "member version check: " + file + " is unreadable (" + why + "); delete it only together with a"
                + " reseed of this member";
    }

    /** Replace the file: temp file, fsync, atomic rename, fsync of the directory. */
    public static void writeSeen(final File clusterDir, final Seen seen) throws IOException {
        final Path file = seenFile(clusterDir);
        final Path dir = file.getParent();
        Files.createDirectories(dir);
        final Path temp = dir.resolve(SEEN_FILE + ".tmp");
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            final ByteBuffer bytes = ByteBuffer.wrap(seen.encode());
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        }
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void writeSeenUnchecked(final File clusterDir, final Seen seen) {
        try {
            writeSeen(clusterDir, seen);
        } catch (final IOException e) {
            throw new UncheckedIOException("member version check: cannot write " + seenFile(clusterDir) + " (" + e
                    + "); delete it only together with a reseed of this member", e);
        }
    }

    /**
     * Before the consensus module is launched: null when this build may start, else the refusal text (the caller
     * stops with exit 78). Refused: a stored generation other than {@code own}'s; an own level more than one away from
     * the stored {@code level_max} (the levels this member's validator accepted); an own level more than one above
     * {@code last_own_level} (the level this member last launched: a member moves up one level at a time); an
     * unreadable file, naming it. A missing file passes (a new or reseeded member has no log and cannot win an
     * election against members that have one). On a pass the own level is recorded as {@code last_own_level} (the
     * file is created when the cluster directory exists).
     */
    public static String checkBeforeLaunch(final File clusterDir, final MemberCompat own, final String buildSha) {
        final Seen seen;
        try {
            seen = readSeenOrNull(clusterDir);
        } catch (final IllegalStateException unreadable) {
            return unreadable.getMessage();
        }
        final Seen next;
        if (seen == null) {
            if (!clusterDir.isDirectory()) {
                return null;
            }
            next = new Seen(own.generation, own.level, own.level, own.level);
        } else {
            if (seen.generation() != own.generation || Math.abs(own.level - seen.levelMax()) > LEVEL_WINDOW) {
                return lastSeenRefusalText(own, buildSha, new MemberCompat(seen.generation(), seen.levelMax()));
            }
            if (own.level > seen.lastOwnLevel() + LEVEL_WINDOW) {
                return lastOwnRefusalText(own, buildSha, new MemberCompat(seen.generation(), seen.lastOwnLevel()));
            }
            next = new Seen(seen.generation(), seen.levelMin(), seen.levelMax(), own.level);
        }
        if (!next.equals(seen)) {
            try {
                writeSeen(clusterDir, next);
            } catch (final IOException e) {
                return "member version check: cannot write " + seenFile(clusterDir) + " (" + e + "); delete it only"
                        + " together with a reseed of this member";
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ the engines' glue

    /**
     * Test seam for process tests, set only by a test entry point before the node's {@code main}: another member
     * version, the pre-election check off, or Aeron's default validator in place of the kit's (the negative controls).
     * Both engines read it through {@link #effective}, {@link #install} and {@link #bootCheck}.
     */
    public static final class TestSeam {
        static volatile MemberCompat own;
        static volatile boolean skipBootCheck;
        static volatile boolean aeronValidator;

        private TestSeam() {
        }

        /**
         * {@code -Dtest.member.compat=<generation>.<level>}, {@code -Dtest.member.compat.prelaunch=off},
         * {@code -Dtest.member.compat.validator=aeron}; prints what it set.
         */
        public static void fromSystemProperties() {
            final String version = System.getProperty("test.member.compat");
            if (version != null) {
                final String[] parts = version.split("\\.");
                own = MemberCompat.of(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
            }
            skipBootCheck = "off".equals(System.getProperty("test.member.compat.prelaunch"));
            aeronValidator = "aeron".equals(System.getProperty("test.member.compat.validator"));
            System.out.println("[member-compat seam] own=" + own + " bootCheck=" + !skipBootCheck
                    + " aeronValidator=" + aeronValidator);
        }

        /** In-process: the same seam set directly (null leaves the version as declared). */
        public static void set(final MemberCompat ownOverride, final boolean skipCheck, final boolean aeron) {
            own = ownOverride;
            skipBootCheck = skipCheck;
            aeronValidator = aeron;
        }
    }

    /** The version this process runs: the engine's declared constant outside the process tests. */
    public static MemberCompat effective(final MemberCompat declared) {
        final MemberCompat forTest = TestSeam.own;
        return forTest == null ? Objects.requireNonNull(declared, "declared") : forTest;
    }

    /**
     * The members' wiring (R-A3), one copy for both engines: the consensus module and every service container carry
     * {@code own}'s app version and one validator whose {@code seen} file lives in the consensus module's cluster
     * directory. {@link #NOT_INSTALLED} fails closed: the validator refuses every version, so a node launched without
     * the lifecycle's refusal ({@link #fatalOnRefusal}) or a rig's explicit {@link #testRefusal} never forms a
     * cluster. Call again when the cluster directory changes.
     */
    public static VersionValidator install(final io.aeron.cluster.ConsensusModule.Context consensusModule,
                                           final java.util.List<io.aeron.cluster.service.ClusteredServiceContainer.Context>
                                                   services,
                                           final MemberCompat own, final Refusal onRefusal) {
        Objects.requireNonNull(own, "own");
        final VersionValidator validator;
        if (TestSeam.aeronValidator) {
            validator = io.aeron.cluster.AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
        } else if (onRefusal == NOT_INSTALLED) {
            validator = (contextAppVersion, other) -> {
                System.err.println(NOT_INSTALLED_TEXT);
                return false;
            };
        } else {
            validator = validator(own, onRefusal, consensusModule.clusterDir());
        }
        consensusModule.appVersion(own.appVersion()).appVersionValidator(validator);
        for (final io.aeron.cluster.service.ClusteredServiceContainer.Context ctx : services) {
            ctx.appVersion(own.appVersion()).appVersionValidator(validator);
        }
        return validator;
    }

    /**
     * The node's boot step before any Aeron component (R-A5), one copy for both engines: {@link #checkBeforeLaunch};
     * a refusal records {@code lifecycle.fatal(78, text)} and throws, so the launch never starts (the first recorded
     * stop wins over the startup failure that follows).
     */
    public static void bootCheck(final EngineLifecycle lifecycle, final File clusterDir, final MemberCompat own,
                                 final BuildInfo build) {
        if (TestSeam.skipBootCheck) {
            System.out.println("[member-compat seam] the pre-election check is off (negative control)");
            return;
        }
        final String refusal = checkBeforeLaunch(clusterDir, own,
                (build == null ? BuildInfo.unknown() : build).build());
        if (refusal != null) {
            lifecycle.fatal(EngineLifecycle.EXIT_CONFIG, refusal);
            throw new IllegalStateException(refusal);
        }
    }

    // ------------------------------------------------------------------ capabilities in readiness

    /**
     * The tokens {@code /health} and {@code /ready} append after {@code build=}:
     * {@code compat=g1.3 wire=10-10 behaviors=1,2,3,4 layouts=7 recorded_behavior=4 recorded_wire_max=10
     * recorded_genesis=1 driver=external}. The build's constants are formatted once; the recorded values are published
     * by the service thread ({@link #publish}) and read by the HTTP thread through one volatile reference. Before the
     * first publish they print 0.
     */
    public static final class Capabilities implements java.util.function.Supplier<String> {
        private final String constantTokens;
        private final String driverToken;
        private volatile Recorded recorded = Recorded.NONE;

        /** The replicated values a member has applied: 0 when nothing is recorded. */
        public record Recorded(long behavior, long wireMax, boolean genesis) {
            public static final Recorded NONE = new Recorded(0, 0, false);
        }

        /**
         * @param driver {@code embedded} or {@code external}
         */
        public Capabilities(final MemberCompat compat, final int wireFloor, final int wireTop, final int[] behaviors,
                            final int[] layouts, final String driver) {
            if (!"embedded".equals(driver) && !"external".equals(driver)) {
                throw new IllegalArgumentException("driver must be embedded or external, was " + driver);
            }
            this.constantTokens = " compat=" + compat + " wire=" + wireFloor + "-" + wireTop
                    + " behaviors=" + joinSorted(behaviors) + " layouts=" + joinSorted(layouts);
            this.driverToken = " driver=" + driver;
        }

        /** Service thread, after each apply that may change a recorded value; allocates only on a change. */
        public void publish(final long behavior, final long wireMax, final boolean genesis) {
            final Recorded current = recorded;
            if (current.behavior() != behavior || current.wireMax() != wireMax || current.genesis() != genesis) {
                recorded = new Recorded(behavior, wireMax, genesis);
            }
        }

        public Recorded recorded() {
            return recorded;
        }

        /** The tokens, leading space included. */
        @Override
        public String get() {
            final Recorded r = recorded;
            return constantTokens + " recorded_behavior=" + r.behavior() + " recorded_wire_max=" + r.wireMax()
                    + " recorded_genesis=" + (r.genesis() ? 1 : 0) + driverToken;
        }
    }

    static String joinSorted(final int[] values) {
        final int[] sorted = values.clone();
        Arrays.sort(sorted);
        final StringBuilder out = new StringBuilder();
        for (int i = 0; i < sorted.length; i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append(sorted[i]);
        }
        return out.toString();
    }

    /** The behaviour bitmask of the gauges ({@code <e>_supported_behavior_versions}): bit v-1 per version v. */
    public static long behaviorBitmask(final int[] behaviors) {
        long mask = 0;
        for (final int v : behaviors) {
            if (v >= 1 && v <= 64) {
                mask |= 1L << (v - 1);
            }
        }
        return mask;
    }

    // ------------------------------------------------------------------ capability manifest

    /**
     * The canonical text of what a build can do: wire floor and top, behaviour set, snapshot layouts with the
     * behaviour each is written from, and the engine's journal schemas. Each engine commits
     * {@code src/test/resources/compat/manifest-g<g>.<level>.txt} for its current level and keeps the previous
     * level's; {@link #manifestFailures} is the rule between them.
     */
    public record Manifest(int wireFloor, int wireTop, int[] behaviors, TreeMap<Integer, Long> layoutSince,
                           TreeMap<String, String> journals) {

        public static final String FORMAT = "member-compat-manifest/1";

        public static Builder builder() {
            return new Builder();
        }

        /** The highest behaviour this build runs. */
        public int maxBehavior() {
            return Arrays.stream(behaviors).max().orElse(0);
        }

        /** The layout version a member writes at {@code behavior}: the newest whose {@code since} is not above it. */
        public int layoutWrittenAt(final long behavior) {
            int version = -1;
            for (final var e : layoutSince.entrySet()) {
                if (e.getValue() <= behavior) {
                    version = e.getKey();
                }
            }
            return version;
        }

        public String text() {
            final StringBuilder out = new StringBuilder();
            out.append("format=").append(FORMAT).append('\n');
            out.append("wire_floor=").append(wireFloor).append('\n');
            out.append("wire_top=").append(wireTop).append('\n');
            out.append("behaviors=").append(joinSorted(behaviors)).append('\n');
            for (final var e : layoutSince.entrySet()) {
                out.append("layout=").append(e.getKey()).append(" since_behavior=").append(e.getValue()).append('\n');
            }
            for (final var e : journals.entrySet()) {
                out.append("journal=").append(e.getKey()).append(' ').append(e.getValue()).append('\n');
            }
            return out.toString();
        }

        /** Reads a committed manifest; the inverse of {@link #text}. */
        public static Manifest parse(final String text) {
            final Builder b = builder();
            int[] behaviors = null;
            boolean format = false;
            for (final String line : text.split("\n")) {
                if (line.isBlank()) {
                    continue;
                }
                final int eq = line.indexOf('=');
                if (eq < 0) {
                    throw new IllegalArgumentException("manifest line without '=': " + line);
                }
                final String key = line.substring(0, eq);
                final String value = line.substring(eq + 1);
                switch (key) {
                    case "format" -> {
                        if (!FORMAT.equals(value)) {
                            throw new IllegalArgumentException("manifest format " + value + " is not " + FORMAT);
                        }
                        format = true;
                    }
                    case "wire_floor" -> b.wireFloor = Integer.parseInt(value);
                    case "wire_top" -> b.wireTop = Integer.parseInt(value);
                    case "behaviors" -> behaviors = value.isEmpty() ? new int[0]
                            : Arrays.stream(value.split(",")).mapToInt(Integer::parseInt).toArray();
                    case "layout" -> {
                        final String[] parts = value.split(" since_behavior=");
                        b.layout(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
                    }
                    case "journal" -> {
                        final int sp = value.indexOf(' ');
                        b.journals.put(value.substring(0, sp), value.substring(sp + 1));
                    }
                    default -> throw new IllegalArgumentException("unknown manifest key: " + key);
                }
            }
            if (!format || behaviors == null) {
                throw new IllegalArgumentException("manifest without format= or behaviors=");
            }
            return b.behaviors(behaviors).build();
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof Manifest m && m.text().equals(text());
        }

        @Override
        public int hashCode() {
            return text().hashCode();
        }

        public static final class Builder {
            private int wireFloor = -1;
            private int wireTop = -1;
            private int[] behaviors;
            private final TreeMap<Integer, Long> layoutSince = new TreeMap<>();
            private final TreeMap<String, String> journals = new TreeMap<>();

            public Builder wire(final int floor, final int top) {
                this.wireFloor = floor;
                this.wireTop = top;
                return this;
            }

            public Builder behaviors(final int... versions) {
                this.behaviors = versions.clone();
                Arrays.sort(this.behaviors);
                return this;
            }

            /** One snapshot layout this build reads, and the recorded behaviour from which it is written. */
            public Builder layout(final int version, final long sinceBehavior) {
                if (layoutSince.put(version, sinceBehavior) != null) {
                    throw new IllegalArgumentException("layout " + version + " named twice");
                }
                return this;
            }

            /** The layouts a {@link SnapshotLayouts} reads (current and previous). */
            public Builder layouts(final SnapshotLayouts<?> layouts) {
                layout(layouts.current().version(), layouts.current().sinceBehavior());
                if (layouts.previous() != null) {
                    layout(layouts.previous().version(), layouts.previous().sinceBehavior());
                }
                return this;
            }

            /** One journal the engine writes: its SBE schema id and version. */
            public Builder journal(final String name, final int schemaId, final int schemaVersion) {
                if (name.isEmpty() || name.contains(" ") || name.contains("=")) {
                    throw new IllegalArgumentException("journal name must be one word: " + name);
                }
                journals.put(name, "schema=" + schemaId + " version=" + schemaVersion);
                return this;
            }

            public Manifest build() {
                if (wireFloor < 0 || wireTop < wireFloor || behaviors == null || behaviors.length == 0
                        || layoutSince.isEmpty()) {
                    throw new IllegalArgumentException("a manifest needs the wire range, the behaviours and a layout");
                }
                return new Manifest(wireFloor, wireTop, behaviors, new TreeMap<>(layoutSince), new TreeMap<>(journals));
            }
        }
    }

    /** The failure text of the level rule (tests and the T2 negative control assert it). */
    public static final String MANIFEST_CHANGED = "capability manifest changed without a level bump";

    /**
     * The manifest rules for a build at {@code own}: its manifest equals the committed file of its level, and against
     * the previous level's file (null for the genesis level: those checks are skipped) the build runs N-1's highest
     * behaviour, reads the layout N-1 writes at that behaviour, keeps the wire floor and does not lower the wire top.
     * Empty when every rule holds.
     *
     * @param committedOwn the committed text of {@code own}'s level, null when there is none
     */
    public static List<String> manifestFailures(final MemberCompat own, final Manifest build,
                                                final String committedOwn, final String committedPrevious) {
        final List<String> failures = new ArrayList<>();
        if (committedOwn == null) {
            failures.add("no committed manifest for " + own + ": commit manifest-" + own + ".txt");
        } else if (!build.text().equals(committedOwn)) {
            failures.add(MANIFEST_CHANGED + " (" + own + "): committed\n" + committedOwn + "build\n" + build.text());
        }
        if (committedPrevious != null) {
            final Manifest previous = Manifest.parse(committedPrevious);
            final int prevTop = previous.maxBehavior();
            if (Arrays.stream(build.behaviors()).noneMatch(v -> v == prevTop)) {
                failures.add("level rule: the build does not run behaviour " + prevTop
                        + ", the highest of the previous level");
            }
            final int layout = previous.layoutWrittenAt(prevTop);
            if (!build.layoutSince().containsKey(layout)) {
                failures.add("level rule: the build does not read layout " + layout
                        + ", the one the previous level writes at behaviour " + prevTop);
            }
            if (build.wireFloor() != previous.wireFloor()) {
                failures.add("level rule: the wire floor moved from " + previous.wireFloor() + " to "
                        + build.wireFloor() + " (the floor never moves within a generation)");
            }
            if (build.wireTop() < previous.wireTop()) {
                failures.add("level rule: the wire top fell from " + previous.wireTop() + " to " + build.wireTop());
            }
        }
        return failures;
    }

    /** {@code manifest-g<g>.<level>.txt}. */
    public static String manifestFileName(final MemberCompat version) {
        return "manifest-" + version + ".txt";
    }

    /** The level before this one in the same generation, or null at level 0. */
    public MemberCompat previousLevel() {
        return level == 0 ? null : new MemberCompat(generation, level - 1);
    }
}
