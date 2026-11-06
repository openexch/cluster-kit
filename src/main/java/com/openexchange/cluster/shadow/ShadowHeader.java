// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * Message 0 of a shadow recording, fixed {@value #LENGTH} bytes, little-endian (RCS S3 3.1). One decode table, H0-H14
 * (3.4), serves the writer (on the values it is about to encode, {@link #validate}), the catalog and the loader (on
 * bytes read back, {@link #decode}). Enum fields are carried raw; no decoder throws on an unknown value.
 *
 * <p>Amendment Q0 (2026-11-01): {@code payloadDigest} (the contract's {@code payloadCrc32c}, widened to 8 bytes) is the
 * first 8 bytes of SHA-256 over the payload ({@code PayloadDigest}); {@code chunkLength} and {@code payloadLayoutHint}
 * move up 4 bytes each and {@code reserved1} shrinks to 4 bytes, so the header stays 128 bytes. The header CRC32C still
 * guards the header bytes.</p>
 *
 * <pre>
 *   0 magic "OXSH"  4 version 1  8 headerLength 128  12 engineId  16 clusterId  20 kind  24 producerKind
 *  28 producerId   32 producerSeq  40 genesisId  48 logPosition  56 leadershipTermId  64 termBaseLogPosition
 *  72 clusterTime  80 journalEnabled  84 reserved0  88 journalSeq  96 payloadLength  104 payloadDigest (int64)
 * 112 chunkLength 116 payloadLayoutHint  120 reserved1 (4 B)  124 headerCrc32c over [0, 124)
 * </pre>
 */
public record ShadowHeader(int engineId, int clusterId, int kind, int producerKind, int producerId, long producerSeq,
                           long genesisId, long logPosition, long leadershipTermId, long termBaseLogPosition,
                           long clusterTime, int journalEnabled, long journalSeq, long payloadLength,
                           long payloadDigest, int chunkLength, int payloadLayoutHint) {

    public static final int MAGIC = 0x4F585348;
    public static final int VERSION = 1;
    public static final int LENGTH = 128;
    /** One unfragmented frame: 32-byte data header + 128-byte body. */
    public static final int FRAME_LENGTH = 160;
    public static final int CRC_OFFSET = 124;
    public static final int ENGINE_ASSETS = 1;
    public static final int ENGINE_MATCH = 2;
    /** The kit's {@code HashChainEngine}: accepted only under {@link Options#allowTestEngine}. */
    public static final int ENGINE_TEST = 99;

    static final int O_MAGIC = 0;
    static final int O_VERSION = 4;
    static final int O_HEADER_LENGTH = 8;
    static final int O_ENGINE = 12;
    static final int O_CLUSTER = 16;
    static final int O_KIND = 20;
    static final int O_PRODUCER_KIND = 24;
    static final int O_PRODUCER_ID = 28;
    static final int O_PRODUCER_SEQ = 32;
    static final int O_GENESIS = 40;
    static final int O_LOG_POSITION = 48;
    static final int O_TERM_ID = 56;
    static final int O_TERM_BASE = 64;
    static final int O_CLUSTER_TIME = 72;
    static final int O_JOURNAL_ENABLED = 80;
    static final int O_RESERVED0 = 84;
    static final int O_JOURNAL_SEQ = 88;
    static final int O_PAYLOAD_LENGTH = 96;
    static final int O_PAYLOAD_DIGEST = 104;
    static final int O_CHUNK_LENGTH = 112;
    static final int O_LAYOUT_HINT = 116;
    static final int O_RESERVED1 = 120;

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    /** H14: the payload plus the header must fit an int-sized buffer. */
    public static final long MAX_PAYLOAD_LENGTH = Integer.MAX_VALUE - LENGTH;

    /** Decode refusals (3.4). */
    public enum Reason { HEADER_LENGTH_MISMATCH, HEADER_CORRUPT, HEADER_VERSION_UNSUPPORTED, HEADER_FIELD_INVALID,
        UNIDENTIFIED }

    /** A refused rule: the reason and, for {@code HEADER_FIELD_INVALID}, the field. */
    public record Problem(Reason reason, String field) {
        @Override
        public String toString() {
            return field == null ? reason.name() : reason + "(" + field + ")";
        }
    }

    /** {@code header} on success, else {@code problem}. */
    public record Decoded(ShadowHeader header, Problem problem) {
        public boolean ok() {
            return problem == null;
        }
    }

    /** Decode options: engine 99 is a kit-test engine and is refused unless allowed. */
    public record Options(boolean allowTestEngine) {
        public static final Options PRODUCTION = new Options(false);

        /** Engine 99 is allowed only for a catalog, loader or writer that is itself scoped to engine 99. */
        public static Options forEngine(final int engineId) {
            return new Options(engineId == ENGINE_TEST);
        }
    }

    public ShadowKind kindEnum() {
        return ShadowKind.of(kind);
    }

    public ProducerKind producerKindEnum() {
        return ProducerKind.of(producerKind);
    }

    public boolean journalEnabledFlag() {
        return journalEnabled == 1;
    }

    /** {@code ceil(payloadLength / chunkLength)} (T4). */
    public long chunkCount() {
        return chunkLength <= 0 ? -1 : (payloadLength + chunkLength - 1) / chunkLength;
    }

    /** The alias this header implies (section 4, rule A1: the alias must equal it). */
    public ShadowAlias alias() {
        return new ShadowAlias(engineId, clusterId, genesisId, logPosition, leadershipTermId, producerKind,
                producerId, producerSeq, kind);
    }

    /**
     * Writes all 128 bytes at {@code offset} (magic, version, length, reserved zeros, CRC). No rule is applied: the
     * writer calls {@link #validate} first; tests use this raw path to build refused headers.
     *
     * @return the header CRC32C
     */
    public int encode(final MutableDirectBuffer buffer, final int offset) {
        buffer.putInt(offset + O_MAGIC, MAGIC, LE);
        buffer.putInt(offset + O_VERSION, VERSION, LE);
        buffer.putInt(offset + O_HEADER_LENGTH, LENGTH, LE);
        buffer.putInt(offset + O_ENGINE, engineId, LE);
        buffer.putInt(offset + O_CLUSTER, clusterId, LE);
        buffer.putInt(offset + O_KIND, kind, LE);
        buffer.putInt(offset + O_PRODUCER_KIND, producerKind, LE);
        buffer.putInt(offset + O_PRODUCER_ID, producerId, LE);
        buffer.putLong(offset + O_PRODUCER_SEQ, producerSeq, LE);
        buffer.putLong(offset + O_GENESIS, genesisId, LE);
        buffer.putLong(offset + O_LOG_POSITION, logPosition, LE);
        buffer.putLong(offset + O_TERM_ID, leadershipTermId, LE);
        buffer.putLong(offset + O_TERM_BASE, termBaseLogPosition, LE);
        buffer.putLong(offset + O_CLUSTER_TIME, clusterTime, LE);
        buffer.putInt(offset + O_JOURNAL_ENABLED, journalEnabled, LE);
        buffer.putInt(offset + O_RESERVED0, 0, LE);
        buffer.putLong(offset + O_JOURNAL_SEQ, journalSeq, LE);
        buffer.putLong(offset + O_PAYLOAD_LENGTH, payloadLength, LE);
        buffer.putLong(offset + O_PAYLOAD_DIGEST, payloadDigest, LE);
        buffer.putInt(offset + O_CHUNK_LENGTH, chunkLength, LE);
        buffer.putInt(offset + O_LAYOUT_HINT, payloadLayoutHint, LE);
        buffer.putInt(offset + O_RESERVED1, 0, LE);
        return sealCrc(buffer, offset);
    }

    /** Recomputes the header CRC over {@code [offset, offset + 124)} and stores it at 124; returns it. */
    public static int sealCrc(final MutableDirectBuffer buffer, final int offset) {
        final int crc = Crc32c.of(buffer, offset, CRC_OFFSET);
        buffer.putInt(offset + CRC_OFFSET, crc, LE);
        return crc;
    }

    /** The CRC stored in an encoded header. */
    public static int storedCrc(final DirectBuffer buffer, final int offset) {
        return buffer.getInt(offset + CRC_OFFSET, LE);
    }

    /** H4-H14 on the values (the writer refuses rather than produce a header its own reader would refuse, 3.4). */
    public Problem validate(final Options options) {
        if (!(engineId == ENGINE_ASSETS || engineId == ENGINE_MATCH
                || (engineId == ENGINE_TEST && options.allowTestEngine()))) {
            return field("engineId");
        }
        if (clusterId < 0) {
            return field("clusterId");
        }
        if (ShadowKind.of(kind) == null) {
            return field("kind");
        }
        if (ProducerKind.of(producerKind) == null) {
            return field("producerKind");
        }
        if (producerId < 0) {
            return field("producerId");
        }
        if (producerSeq < 1) {
            return field("producerSeq");
        }
        if (genesisId <= 0) {
            return new Problem(Reason.UNIDENTIFIED, null);
        }
        if (logPosition < termBaseLogPosition) {
            return field("logPosition");
        }
        if (leadershipTermId < 0 || termBaseLogPosition < 0) {
            return field("term");
        }
        if (clusterTime < 0) {
            return field("clusterTime");
        }
        if (journalEnabled != 0 && journalEnabled != 1) {
            return field("journalEnabled");
        }
        if (journalSeq < 0) {
            return field("journalSeq");
        }
        if (payloadLength <= 0 || payloadLength > MAX_PAYLOAD_LENGTH) {
            return field("payloadLength");
        }
        if (chunkLength <= 0) {
            return field("chunkLength");
        }
        return null;
    }

    /** H0-H14 over {@code length} bytes read back at {@code offset}. */
    public static Decoded decode(final DirectBuffer buffer, final int offset, final int length, final Options options) {
        if (length != LENGTH) {
            return refused(new Problem(Reason.HEADER_LENGTH_MISMATCH, null));
        }
        if (buffer.getInt(offset + O_MAGIC, LE) != MAGIC
                || Crc32c.of(buffer, offset, CRC_OFFSET) != buffer.getInt(offset + CRC_OFFSET, LE)) {
            return refused(new Problem(Reason.HEADER_CORRUPT, null));
        }
        if (buffer.getInt(offset + O_VERSION, LE) != VERSION) {
            return refused(new Problem(Reason.HEADER_VERSION_UNSUPPORTED, null));
        }
        if (buffer.getInt(offset + O_HEADER_LENGTH, LE) != LENGTH) {
            return refused(new Problem(Reason.HEADER_CORRUPT, null));
        }
        final ShadowHeader h = new ShadowHeader(
                buffer.getInt(offset + O_ENGINE, LE),
                buffer.getInt(offset + O_CLUSTER, LE),
                buffer.getInt(offset + O_KIND, LE),
                buffer.getInt(offset + O_PRODUCER_KIND, LE),
                buffer.getInt(offset + O_PRODUCER_ID, LE),
                buffer.getLong(offset + O_PRODUCER_SEQ, LE),
                buffer.getLong(offset + O_GENESIS, LE),
                buffer.getLong(offset + O_LOG_POSITION, LE),
                buffer.getLong(offset + O_TERM_ID, LE),
                buffer.getLong(offset + O_TERM_BASE, LE),
                buffer.getLong(offset + O_CLUSTER_TIME, LE),
                buffer.getInt(offset + O_JOURNAL_ENABLED, LE),
                buffer.getLong(offset + O_JOURNAL_SEQ, LE),
                buffer.getLong(offset + O_PAYLOAD_LENGTH, LE),
                buffer.getLong(offset + O_PAYLOAD_DIGEST, LE),
                buffer.getInt(offset + O_CHUNK_LENGTH, LE),
                buffer.getInt(offset + O_LAYOUT_HINT, LE));
        Problem problem = h.validate(options);
        if (problem == null && (buffer.getInt(offset + O_RESERVED0, LE) != 0
                || buffer.getInt(offset + O_RESERVED1, LE) != 0)) {
            problem = field("reserved");
        }
        return problem == null ? new Decoded(h, null) : refused(problem);
    }

    private static Problem field(final String name) {
        return new Problem(Reason.HEADER_FIELD_INVALID, name);
    }

    private static Decoded refused(final Problem problem) {
        return new Decoded(null, problem);
    }
}
