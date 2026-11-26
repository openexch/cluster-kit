// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.Objects;

/**
 * What {@link JournalRetention} has to know about one journal to read a record's key out of a frame: where the
 * journal is recorded (stream id, channel fragment), which SBE schema its records carry, and the byte offsets, from
 * the start of the message (its SBE header included), of {@code journalSeq}, of the epoch (or {@link #NO_EPOCH}) and of
 * the log position (per template, and from which schema version on). {@code maxFrameLength} is the largest aligned
 * frame of one record, data header included; a record is one unfragmented frame.
 *
 * <p>An engine supplies one profile per journal. The offsets are format constants of the engine's codec; the
 * engine's own test holds them equal to the generated decoders.</p>
 */
public final class JournalProfile {

    /** {@code epochOffset} of a journal whose records carry no epoch. */
    public static final int NO_EPOCH = -1;
    /** A record that carries no log position (an older schema version, or a template the profile does not list). */
    public static final long NO_LOG_POSITION = -1;

    private static final int SBE_HEADER_LENGTH = 8;
    private static final int FRAME_ALIGNMENT = 32;
    private static final int DATA_HEADER_LENGTH = 32;

    private final String journal;
    private final int streamId;
    private final String channelFragment;
    private final int schemaId;
    private final int journalSeqOffset;
    private final int epochOffset;
    private final Map<Integer, Integer> logPositionOffsetByTemplate;
    private final int logPositionSinceVersion;
    private final int maxFrameLength;

    /**
     * @param journal                     the journal's name in settings, reader files and metrics ({@code money},
     *                                    {@code order})
     * @param streamId                    the stream the journal's publications are recorded on
     * @param channelFragment             the channel fragment the recordings are listed by ({@code aeron:ipc})
     * @param schemaId                    the SBE schema id of every record
     * @param journalSeqOffset            byte offset of {@code journalSeq} in the message
     * @param epochOffset                 byte offset of the epoch, or {@link #NO_EPOCH}
     * @param logPositionOffsetByTemplate byte offset of the log position in the message, by SBE template id
     * @param logPositionSinceVersion     the schema version from which a record carries its log position
     * @param maxFrameLength              the largest aligned frame of one record, a multiple of 32
     */
    public JournalProfile(final String journal, final int streamId, final String channelFragment, final int schemaId,
                          final int journalSeqOffset, final int epochOffset,
                          final Map<Integer, Integer> logPositionOffsetByTemplate, final int logPositionSinceVersion,
                          final int maxFrameLength) {
        this.journal = Objects.requireNonNull(journal, "journal");
        if (!JournalReaders.validName(journal)) {
            throw new IllegalArgumentException("journal name '" + journal + "' is not a name");
        }
        this.streamId = streamId;
        this.channelFragment = Objects.requireNonNull(channelFragment, "channelFragment");
        if (schemaId < 1 || schemaId > 0xFFFF) {
            throw new IllegalArgumentException("schemaId " + schemaId + " outside 1..65535");
        }
        this.schemaId = schemaId;
        if (journalSeqOffset < SBE_HEADER_LENGTH) {
            throw new IllegalArgumentException("journalSeqOffset " + journalSeqOffset + " inside the SBE header");
        }
        this.journalSeqOffset = journalSeqOffset;
        if (epochOffset != NO_EPOCH && epochOffset < SBE_HEADER_LENGTH) {
            throw new IllegalArgumentException("epochOffset " + epochOffset + " inside the SBE header");
        }
        this.epochOffset = epochOffset;
        this.logPositionOffsetByTemplate = Map.copyOf(logPositionOffsetByTemplate);
        for (final int offset : this.logPositionOffsetByTemplate.values()) {
            if (offset < SBE_HEADER_LENGTH) {
                throw new IllegalArgumentException("log position offset " + offset + " inside the SBE header");
            }
        }
        this.logPositionSinceVersion = logPositionSinceVersion;
        if (maxFrameLength < 2 * FRAME_ALIGNMENT || !BitUtil.isAligned(maxFrameLength, FRAME_ALIGNMENT)) {
            throw new IllegalArgumentException("maxFrameLength " + maxFrameLength + " is not a multiple of 32 above 32");
        }
        this.maxFrameLength = maxFrameLength;
    }

    public String journal() {
        return journal;
    }

    public int streamId() {
        return streamId;
    }

    public String channelFragment() {
        return channelFragment;
    }

    public int schemaId() {
        return schemaId;
    }

    public boolean hasEpoch() {
        return epochOffset != NO_EPOCH;
    }

    public int maxFrameLength() {
        return maxFrameLength;
    }

    /** The key of one record, as read from a frame. */
    public record Key(long journalSeq, long epoch, long logPosition, int templateId, int version) {
    }

    /** A message that is not a whole record of this journal; {@code why} says what is wrong with it. */
    public static final class NotARecord extends Exception {
        private static final long serialVersionUID = 1L;

        NotARecord(final String why) {
            super(why, null, false, false);
        }
    }

    /**
     * The key of the record in {@code buffer[offset, offset + length)} (one whole message, SBE header first).
     *
     * @param frameLength the frame length of the data header (header included)
     * @throws NotARecord the message is too short, is larger than the profile's frame, or carries another schema id
     */
    public Key key(final DirectBuffer buffer, final int offset, final int length, final int frameLength)
        throws NotARecord {
        if (frameLength != length + DATA_HEADER_LENGTH) {
            throw new NotARecord("the message is not one whole frame (frame length " + frameLength + ", message "
                + length + ")");
        }
        if (BitUtil.align(frameLength, FRAME_ALIGNMENT) > maxFrameLength) {
            throw new NotARecord("the frame of " + frameLength + " bytes is larger than the journal's largest record ("
                + maxFrameLength + ")");
        }
        if (length < SBE_HEADER_LENGTH) {
            throw new NotARecord("the message of " + length + " bytes has no SBE header");
        }
        final int blockLength = buffer.getShort(offset, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int templateId = buffer.getShort(offset + 2, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int schema = buffer.getShort(offset + 4, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int version = buffer.getShort(offset + 6, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        if (schema != schemaId) {
            throw new NotARecord("schema id " + schema + ", the journal's is " + schemaId);
        }
        final int end = Math.min(length, SBE_HEADER_LENGTH + blockLength);
        if (journalSeqOffset + Long.BYTES > end) {
            throw new NotARecord("the block of " + blockLength + " bytes ends before journalSeq");
        }
        final long seq = buffer.getLong(offset + journalSeqOffset, ByteOrder.LITTLE_ENDIAN);
        long epoch = 0;
        if (epochOffset != NO_EPOCH) {
            if (epochOffset + Long.BYTES > end) {
                throw new NotARecord("the block of " + blockLength + " bytes ends before the epoch");
            }
            epoch = buffer.getLong(offset + epochOffset, ByteOrder.LITTLE_ENDIAN);
        }
        long logPosition = NO_LOG_POSITION;
        final Integer at = logPositionOffsetByTemplate.get(templateId);
        if (at != null && version >= logPositionSinceVersion && at + Long.BYTES <= end) {
            logPosition = buffer.getLong(offset + at, ByteOrder.LITTLE_ENDIAN);
        }
        return new Key(seq, epoch, logPosition, templateId, version);
    }

    @Override
    public String toString() {
        return journal + " (stream " + streamId + ", schema " + schemaId + (hasEpoch() ? ", epoch" : ", no epoch") + ")";
    }
}
