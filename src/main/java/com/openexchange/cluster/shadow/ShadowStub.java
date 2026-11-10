// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import com.openexchange.cluster.SnapshotEnvelope;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * The service snapshot a member writes in stub mode: {@value #LENGTH} bytes inside the kit
 * {@link SnapshotEnvelope}, so the envelope CRC covers it. It names where the engine state lives (a local shadow
 * plus the log), never the state itself.
 *
 * <pre>
 *   0 stubTag long = -100   8 stubVersion int = 1   12 engineId int   16 clusterId int   20 pad int = 0
 *  24 genesisId long   32 leadershipTermId long   40 logPosition (P_cm) long   48 hintPs long   56 hintRecordingId long
 * </pre>
 *
 * <p>Little-endian, like {@link ShadowHeader}. The tag is distinct from every engine payload layout tag (core
 * {@code LAYOUT_V4_TAG -4}, {@code LAYOUT_V5_TAG -5}), so a host dispatches on {@link #peekTag} after the envelope
 * opened. One rule table, S0-S8, serves the writer ({@link #encode}, rules S5-S8 on the values it is about to write)
 * and the reader ({@link #decode}, every rule, in order); a refusal names its rule (C7).</p>
 *
 * <p>Every field is a log-derived value the service already held (hard rule 12.11): nothing from a clock or the
 * environment enters a stub. {@code hintPs} and {@code hintRecordingId} are diagnostic only and never a selection
 * input.</p>
 */
public record ShadowStub(int engineId, int clusterId, long genesisId, long leadershipTermId, long logPosition,
                         long hintPs, long hintRecordingId) {

    /** The payload tag of a stub (distinct from -4 and -5). */
    public static final long STUB_TAG = -100L;
    public static final int VERSION = 1;
    /** A format constant, not a setting. */
    public static final int LENGTH = 64;
    /** {@link #LENGTH} inside the kit envelope. */
    public static final int ENVELOPED_LENGTH = SnapshotEnvelope.envelopedLength(LENGTH);

    static final int O_TAG = 0;
    static final int O_VERSION = 8;
    static final int O_ENGINE = 12;
    static final int O_CLUSTER = 16;
    static final int O_PAD = 20;
    static final int O_GENESIS = 24;
    static final int O_TERM = 32;
    static final int O_LOG_POSITION = 40;
    static final int O_HINT_PS = 48;
    static final int O_HINT_RECORDING = 56;

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    /** The decode rules, in evaluation order; each refusal names its rule id (S0-S8). */
    public enum Rule {
        /** S0: length == 64. */
        STUB_LENGTH("S0"),
        /** S1: tag == -100. */
        NOT_A_STUB("S1"),
        /** S2: version == 1. */
        STUB_VERSION_UNSUPPORTED("S2"),
        /** S3: engineId == the host's. */
        STUB_ENGINE("S3"),
        /** S4: clusterId == the host's. */
        STUB_CLUSTER("S4"),
        /** S5: P_cm >= 0. */
        STUB_POSITION("S5"),
        /** S6: leadershipTermId >= 0. */
        STUB_TERM("S6"),
        /** S7: genesisId >= 0 (0 = before ADOPT). */
        STUB_GENESIS("S7"),
        /** S8: each hint is -1 or >= 0. */
        STUB_HINT("S8");

        private final String id;

        Rule(final String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }
    }

    /** A refused rule and what the bytes said. */
    public record Refusal(Rule rule, String detail) {
        @Override
        public String toString() {
            return rule.id() + " " + rule.name() + ": " + detail;
        }
    }

    /** {@code stub} on success, else {@code refusal}. */
    public record Decoded(ShadowStub stub, Refusal refusal) {
        public boolean ok() {
            return refusal == null;
        }
    }

    /** Thrown by {@link #encode} for values the reader would refuse; the message is the refusal. */
    public static final class InvalidStubException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;
        private final transient Refusal refusal;

        InvalidStubException(final Refusal refusal) {
            super(refusal.toString());
            this.refusal = refusal;
        }

        public Refusal refusal() {
            return refusal;
        }
    }

    /**
     * Writes the 64 bytes at {@code offset}.
     *
     * @return {@value #LENGTH}
     * @throws InvalidStubException when a field breaks S5-S8 (the writer never writes what the reader refuses)
     */
    public int encode(final MutableDirectBuffer buffer, final int offset) {
        final Refusal r = validateFields(logPosition, leadershipTermId, genesisId, hintPs, hintRecordingId);
        if (r != null) {
            throw new InvalidStubException(r);
        }
        buffer.putLong(offset + O_TAG, STUB_TAG, LE);
        buffer.putInt(offset + O_VERSION, VERSION, LE);
        buffer.putInt(offset + O_ENGINE, engineId, LE);
        buffer.putInt(offset + O_CLUSTER, clusterId, LE);
        buffer.putInt(offset + O_PAD, 0, LE);
        buffer.putLong(offset + O_GENESIS, genesisId, LE);
        buffer.putLong(offset + O_TERM, leadershipTermId, LE);
        buffer.putLong(offset + O_LOG_POSITION, logPosition, LE);
        buffer.putLong(offset + O_HINT_PS, hintPs, LE);
        buffer.putLong(offset + O_HINT_RECORDING, hintRecordingId, LE);
        return LENGTH;
    }

    /**
     * Writes the stub at {@link SnapshotEnvelope#payloadOffset()} and seals the envelope over it.
     *
     * @return the envelope length to offer, {@link #ENVELOPED_LENGTH}
     */
    public int encodeEnveloped(final SnapshotEnvelope envelope, final MutableDirectBuffer buffer) {
        encode(buffer, SnapshotEnvelope.payloadOffset());
        return envelope.seal(buffer, LENGTH);
    }

    /** The first payload long, read the way a stub writes it (little-endian). */
    public static long peekTag(final DirectBuffer buffer, final int payloadOffset) {
        return buffer.getLong(payloadOffset, LE);
    }

    /** Whether {@code length >= 8} payload bytes at {@code offset} start with {@link #STUB_TAG}; a dispatch peek. */
    public static boolean isStubPayload(final DirectBuffer buffer, final int offset, final int length) {
        return length >= Long.BYTES && peekTag(buffer, offset) == STUB_TAG;
    }

    /** Rules S0-S8 over {@code length} payload bytes at {@code offset}, in order; never throws. */
    public static Decoded decode(final DirectBuffer buffer, final int offset, final int length,
                                 final int hostEngineId, final int hostClusterId) {
        if (length != LENGTH) {
            return refused(Rule.STUB_LENGTH, "length " + length + ", a stub is " + LENGTH);
        }
        final long tag = buffer.getLong(offset + O_TAG, LE);
        if (tag != STUB_TAG) {
            return refused(Rule.NOT_A_STUB, "tag " + tag + ", a stub is " + STUB_TAG);
        }
        final int version = buffer.getInt(offset + O_VERSION, LE);
        if (version != VERSION) {
            return refused(Rule.STUB_VERSION_UNSUPPORTED, "version " + version + ", this build reads " + VERSION);
        }
        final int engineId = buffer.getInt(offset + O_ENGINE, LE);
        if (engineId != hostEngineId) {
            return refused(Rule.STUB_ENGINE, "engineId " + engineId + ", host " + hostEngineId);
        }
        final int clusterId = buffer.getInt(offset + O_CLUSTER, LE);
        if (clusterId != hostClusterId) {
            return refused(Rule.STUB_CLUSTER, "clusterId " + clusterId + ", host " + hostClusterId);
        }
        final long genesisId = buffer.getLong(offset + O_GENESIS, LE);
        final long term = buffer.getLong(offset + O_TERM, LE);
        final long position = buffer.getLong(offset + O_LOG_POSITION, LE);
        final long hintPs = buffer.getLong(offset + O_HINT_PS, LE);
        final long hintRecordingId = buffer.getLong(offset + O_HINT_RECORDING, LE);
        final Refusal r = validateFields(position, term, genesisId, hintPs, hintRecordingId);
        if (r != null) {
            return new Decoded(null, r);
        }
        return new Decoded(new ShadowStub(engineId, clusterId, genesisId, term, position, hintPs, hintRecordingId),
                null);
    }

    /**
     * Opens the envelope (magic, version, length, CRC32C: {@link SnapshotEnvelope#open}) and decodes its payload.
     *
     * @throws IllegalStateException from {@link SnapshotEnvelope#open} on a torn or foreign image (never a stub)
     */
    public static Decoded openEnveloped(final SnapshotEnvelope envelope, final DirectBuffer buffer, final int length,
                                        final int hostEngineId, final int hostClusterId) {
        final int payloadLength = envelope.open(buffer, length);
        return decode(buffer, SnapshotEnvelope.payloadOffset(), payloadLength, hostEngineId, hostClusterId);
    }

    /** S5-S8: the rules over the values themselves (the writer's half of the table). */
    private static Refusal validateFields(final long position, final long term, final long genesisId,
                                          final long hintPs, final long hintRecordingId) {
        if (position < 0) {
            return new Refusal(Rule.STUB_POSITION, "logPosition " + position + " < 0");
        }
        if (term < 0) {
            return new Refusal(Rule.STUB_TERM, "leadershipTermId " + term + " < 0");
        }
        if (genesisId < 0) {
            return new Refusal(Rule.STUB_GENESIS, "genesisId " + genesisId + " < 0");
        }
        if (hintPs < -1) {
            return new Refusal(Rule.STUB_HINT, "hintPs " + hintPs + " (expected -1 or >= 0)");
        }
        if (hintRecordingId < -1) {
            return new Refusal(Rule.STUB_HINT, "hintRecordingId " + hintRecordingId + " (expected -1 or >= 0)");
        }
        return null;
    }

    private static Decoded refused(final Rule rule, final String detail) {
        return new Decoded(null, new Refusal(rule, detail));
    }
}
