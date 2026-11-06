// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

/**
 * One recording on the shadow stream and prefix, as one scan classified it (RCS S3 6.3). {@code header},
 * {@code headerBytes} and {@code trailer} are present from the point the scan read them; {@code code} is an archive
 * error code for SCAN_ERROR (or -1).
 */
public record ShadowEntry(long archiveId, long recordingId, long startPosition, long stopPosition,
                          String originalChannel, ShadowAlias alias, ShadowHeader header, byte[] headerBytes,
                          int headerCrc32c, ShadowTrailer trailer, State state, String reason, int code) {

    public enum State {
        ACTIVE, TOO_SHORT, NO_TRAILER, HEADER_INVALID, TRAILER_MISMATCH,
        /** A trailer of a newer format version (T2): fail closed, but not mistaken for a crashed write (C8). */
        TRAILER_VERSION_UNSUPPORTED, ALIAS_MISMATCH, ALIAS_UNPARSEABLE, SCAN_ERROR,
        FOREIGN, CONFLICT, COMPLETE, VERIFIED, CORRUPT;

        /** COMPLETE or VERIFIED: may be offered to a loader, which re-verifies. */
        public boolean usable() {
            return this == COMPLETE || this == VERIFIED;
        }
    }

    public ShadowVerdicts.Key key() {
        return new ShadowVerdicts.Key(archiveId, recordingId, startPosition, stopPosition, headerCrc32c);
    }

    ShadowEntry with(final State newState, final String newReason) {
        return new ShadowEntry(archiveId, recordingId, startPosition, stopPosition, originalChannel, alias, header,
                headerBytes, headerCrc32c, trailer, newState, newReason, code);
    }

    /** {@code R3} / {@code F1}, or {@code ?} without a header. */
    public String producer() {
        if (header == null) {
            return "?";
        }
        final ProducerKind pk = header.producerKindEnum();
        return (pk == null ? "?" : String.valueOf(pk.letter())) + header.producerId();
    }

    @Override
    public String toString() {
        return "ShadowEntry{rec=" + recordingId + " [" + startPosition + "," + stopPosition + ") " + state
                + (reason == null ? "" : "(" + reason + ")") + (header == null ? ""
                : " P=" + header.logPosition() + " " + producer() + " seq=" + header.producerSeq() + " kind="
                + header.kind() + String.format(" digest=%016x", header.payloadDigest())) + "}";
    }
}
