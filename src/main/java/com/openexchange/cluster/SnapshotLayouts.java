// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The snapshot layouts one build carries, shared by both engines: which layouts it reads and which layout it
 * writes. It exists so a cluster can be upgraded member by member across a layout change.
 *
 * <p><b>Read: the current layout and the one before it, nothing else.</b> {@link #readerFor} returns the layout
 * for a tag found in an image. A retired tag (older than the previous layout) and a tag this build has never
 * heard of are both refused, each with its own text, before a byte of the image is decoded. There is no "looks
 * like an old layout" path.</p>
 *
 * <p><b>Write: the layout is a function of the recorded behaviour version, never of the jar.</b> The behaviour
 * version is replicated state: it is decided by a command in the log and rides the snapshot. {@link #writtenBy}
 * maps it to a layout, so two members running different jars at one log position write identical snapshot bytes.
 * A newer jar keeps writing the previous layout until the behaviour switch that adopts the current one is
 * recorded. Nothing here reads a system property, an environment variable or a build constant.</p>
 *
 * <p>An image is consistent only when its tag is the one its own recorded behaviour writes;
 * {@link #requireWrittenBy} checks that after a decode.</p>
 *
 * <p>The tag is whatever value the engine stores to name the layout (a leading payload tag, an envelope
 * version); where it lives in the image is the engine's business. Instances are immutable and thread-safe.</p>
 *
 * @param <C> the engine's codec handle for one layout (its writer and reader)
 */
public final class SnapshotLayouts<C> {

    /**
     * One layout.
     *
     * @param version       the layout's number, as people name it (6 for "v6"); higher is newer
     * @param tag           the value an image of this layout carries
     * @param sinceBehavior the first behaviour version that writes this layout
     * @param codec         the engine's writer and reader for it
     */
    public record Layout<C>(int version, long tag, long sinceBehavior, C codec) {
        public Layout {
            Objects.requireNonNull(codec, "codec");
            if (version < 1) {
                throw new IllegalArgumentException("layout version " + version + " must be >= 1");
            }
            if (sinceBehavior < 1) {
                throw new IllegalArgumentException("layout v" + version + " sinceBehavior " + sinceBehavior + " must be >= 1");
            }
        }
    }

    private record Retired(int version, long tag) {
    }

    private final String image;
    private final Layout<C> current;
    private final Layout<C> previous;
    private final List<Retired> retired;

    private SnapshotLayouts(final String image, final Layout<C> current, final Layout<C> previous,
                            final List<Retired> retired) {
        this.image = image;
        this.current = current;
        this.previous = previous;
        this.retired = List.copyOf(retired);
    }

    /** @param image what the refusal texts call the image, for example {@code "balance snapshot"} */
    public static <C> Builder<C> of(final String image) {
        return new Builder<>(image);
    }

    /** Collects the layouts of one build; {@link #build} validates them. */
    public static final class Builder<C> {
        private final String image;
        private Layout<C> current;
        private Layout<C> previous;
        private final List<Retired> retired = new ArrayList<>();

        private Builder(final String image) {
            if (image == null || image.isBlank()) {
                throw new IllegalArgumentException("the image name is required");
            }
            this.image = image;
        }

        /** The newest layout this build carries. Required, once. */
        public Builder<C> current(final int version, final long tag, final long sinceBehavior, final C codec) {
            if (current != null) {
                throw new IllegalArgumentException(image + ": a current layout is already set (v" + current.version() + ")");
            }
            current = new Layout<>(version, tag, sinceBehavior, codec);
            return this;
        }

        /** The layout before the current one. Optional, once; without it the build reads and writes one layout. */
        public Builder<C> previous(final int version, final long tag, final long sinceBehavior, final C codec) {
            if (previous != null) {
                throw new IllegalArgumentException(image + ": a previous layout is already set (v" + previous.version() + ")");
            }
            previous = new Layout<>(version, tag, sinceBehavior, codec);
            return this;
        }

        /** A layout older than the previous one: known by name, so its refusal can say it is too old. */
        public Builder<C> retired(final int version, final long tag) {
            retired.add(new Retired(version, tag));
            return this;
        }

        public SnapshotLayouts<C> build() {
            if (current == null) {
                throw new IllegalArgumentException(image + ": a current layout is required");
            }
            final int oldestCarried;
            if (previous != null) {
                if (previous.version() >= current.version()) {
                    throw new IllegalArgumentException(image + ": previous v" + previous.version()
                            + " must be older than current v" + current.version());
                }
                if (previous.sinceBehavior() >= current.sinceBehavior()) {
                    throw new IllegalArgumentException(image + ": previous v" + previous.version() + " is written since behaviour "
                            + previous.sinceBehavior() + ", current v" + current.version() + " since "
                            + current.sinceBehavior() + ": the current layout must be adopted by a later behaviour");
                }
                if (previous.tag() == current.tag()) {
                    throw new IllegalArgumentException(image + ": tag " + current.tag() + " names two layouts");
                }
                oldestCarried = previous.version();
            } else {
                oldestCarried = current.version();
            }
            for (int i = 0; i < retired.size(); i++) {
                final Retired r = retired.get(i);
                if (r.version() < 1 || r.version() >= oldestCarried) {
                    throw new IllegalArgumentException(image + ": retired v" + r.version()
                            + " must be older than every layout this build carries");
                }
                if (r.tag() == current.tag() || (previous != null && r.tag() == previous.tag())) {
                    throw new IllegalArgumentException(image + ": tag " + r.tag() + " names two layouts");
                }
                for (int j = 0; j < i; j++) {
                    if (retired.get(j).tag() == r.tag() || retired.get(j).version() == r.version()) {
                        throw new IllegalArgumentException(image + ": retired v" + r.version() + " (tag " + r.tag()
                                + ") is listed twice");
                    }
                }
            }
            return new SnapshotLayouts<>(image, current, previous, retired);
        }
    }

    /** The newest layout this build carries. */
    public Layout<C> current() {
        return current;
    }

    /** The layout before the current one, or null when this build carries one layout. */
    public Layout<C> previous() {
        return previous;
    }

    /**
     * The layout for a tag found in an image.
     *
     * @throws IllegalStateException for a retired tag and for a tag unknown to this build
     */
    public Layout<C> readerFor(final long tag) {
        if (tag == current.tag()) {
            return current;
        }
        if (previous != null && tag == previous.tag()) {
            return previous;
        }
        for (final Retired r : retired) {
            if (r.tag() == tag) {
                throw new IllegalStateException(image + " layout v" + r.version() + " (tag " + tag
                        + ") refused: older than the previous layout; this build reads only " + readable()
                        + ". Restore it with a build that reads v" + r.version() + ", or reseed this member from a current one");
            }
        }
        throw new IllegalStateException(image + " layout tag " + tag + " refused: unknown to this build, which reads only "
                + readable() + ". A newer build wrote it, or the image is foreign");
    }

    /** True when this build writes the layout of {@code behaviorVersion}. */
    public boolean writes(final long behaviorVersion) {
        return behaviorVersion >= (previous != null ? previous.sinceBehavior() : current.sinceBehavior());
    }

    /**
     * The layout a member at the recorded {@code behaviorVersion} writes: the same answer on every build that
     * carries it.
     *
     * @throws IllegalStateException when that behaviour writes a layout this build does not carry
     */
    public Layout<C> writtenBy(final long behaviorVersion) {
        if (behaviorVersion >= current.sinceBehavior()) {
            return current;
        }
        if (previous != null && behaviorVersion >= previous.sinceBehavior()) {
            return previous;
        }
        throw new IllegalStateException(image + ": behaviour " + behaviorVersion
                + " writes a layout this build no longer carries (it writes " + writable() + ")");
    }

    /**
     * The consistency rule of an image: its tag must be the one its own recorded behaviour writes. Call it after
     * the decode, with the behaviour version the image restored.
     *
     * @throws IllegalStateException when the tag belongs to another behaviour, or to no layout this build reads
     */
    public void requireWrittenBy(final long tag, final long behaviorVersion) {
        final Layout<C> carried = readerFor(tag);
        final Layout<C> expected = writtenBy(behaviorVersion);
        if (carried != expected) {
            throw new IllegalStateException(image + " layout v" + carried.version() + " does not belong to behaviour "
                    + behaviorVersion + ", which writes v" + expected.version());
        }
    }

    private String readable() {
        return previous == null ? "v" + current.version() : "v" + current.version() + " and v" + previous.version();
    }

    private String writable() {
        return previous == null
                ? "v" + current.version() + " from behaviour " + current.sinceBehavior()
                : "v" + previous.version() + " from behaviour " + previous.sinceBehavior() + ", v" + current.version()
                + " from behaviour " + current.sinceBehavior();
    }

    @Override
    public String toString() {
        return image + " layouts: reads " + readable() + "; writes " + writable();
    }
}
