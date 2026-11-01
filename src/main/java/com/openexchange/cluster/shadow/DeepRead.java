// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.archive.client.ArchiveException;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The full read of one shadow recording (RCS S3 6.4 and 8.2 steps 2-3), shared by {@code verifyDeep} and the loader:
 * message 0 byte-equal to the header the scan read, messages {@code 1..chunkCount} the chunks (each exactly
 * {@code chunkLength} but the last), folded into the SHA-256 payload digest and optionally copied to {@code into} at 0, then exactly one
 * trailer that passes T1-T5 against the header, then nothing. Total length, chunk count and payload digest must equal the
 * header's. An archive refusal or timeout is ERROR (unknown is not corrupt).
 */
final class DeepRead {

    enum Kind { OK, CORRUPT, ERROR }

    record Outcome(Kind kind, String reason, int code) {
        static final Outcome OK = new Outcome(Kind.OK, null, -1);
    }

    private DeepRead() {
    }

    static Outcome read(final ShadowReplay replay, final ShadowEntry e, final MutableDirectBuffer into,
                        final long deadlineNs, final ShadowCounters counters) {
        final ShadowHeader h = e.header();
        final long chunkCount = h.chunkCount();
        final UnsafeBuffer expectedHeader = new UnsafeBuffer(e.headerBytes());
        final PayloadDigest digest = new PayloadDigest(); // amendment Q0: SHA-256, never the envelope's CRC32C
        final long[] payload = {0};
        final long[] message = {0};
        final boolean[] trailerSeen = {false};
        // a fragment handler must not throw (Aeron's subscriber error handler may swallow it): the first corruption
        // is kept here and ends the read early
        final String[] corrupt = {null};
        try {
            replay.readAll(e.recordingId(), e.startPosition(), e.stopPosition(),
                    (buffer, offset, length, header) -> {
                        if (corrupt[0] != null) {
                            return;
                        }
                        final long index = message[0]++;
                        if (index == 0) {
                            if (length != ShadowHeader.LENGTH || !equal(buffer, offset, expectedHeader)) {
                                { corrupt[0] = "message 0 differs from the header the scan read"; return; }
                            }
                        } else if (index <= chunkCount) {
                            final long expected = index < chunkCount ? h.chunkLength()
                                    : h.payloadLength() - (chunkCount - 1) * (long)h.chunkLength();
                            if (length != expected) {
                                { corrupt[0] = "chunk " + index + " is " + length + " bytes, expected " + expected; return; }
                            }
                            digest.update(buffer, offset, length);
                            if (into != null) {
                                into.putBytes((int)payload[0], buffer, offset, length);
                            }
                            payload[0] += length;
                        } else if (index == chunkCount + 1) {
                            final ShadowTrailer.Decoded t = ShadowTrailer.decode(buffer, offset, length);
                            if (!t.ok()) {
                                { corrupt[0] = "trailer " + t.problem(); return; }
                            }
                            final ShadowTrailer.Problem m = t.trailer().mismatch(h, e.headerCrc32c());
                            if (m != null) {
                                { corrupt[0] = "trailer " + m; return; }
                            }
                            trailerSeen[0] = true;
                        } else {
                            { corrupt[0] = "message " + index + " after the trailer"; return; }
                        }
                    }, () -> corrupt[0] != null, deadlineNs);
        } catch (final ArchiveException ae) {
            return new Outcome(Kind.ERROR, "archive refused the deep read: " + ae.getMessage(), ae.errorCode());
        } catch (final ShadowReplay.ReplayTimeoutException te) {
            counters.increment(ShadowCounters.Counter.REPLAY_TIMEOUTS_TOTAL);
            return new Outcome(Kind.ERROR, te.getMessage(), ShadowCatalog.SCAN_CODE_TIMEOUT);
        } catch (final ShadowReplay.ReplayEndedEarlyException early) {
            return new Outcome(Kind.ERROR, early.getMessage(), ShadowCatalog.SCAN_CODE_ENDED_EARLY);
        } catch (final RuntimeException re) {
            return new Outcome(Kind.ERROR, re.toString(), ShadowCatalog.SCAN_CODE_CLIENT);
        }
        if (corrupt[0] != null) {
            return new Outcome(Kind.CORRUPT, corrupt[0], -1);
        }
        if (!trailerSeen[0]) {
            return new Outcome(Kind.CORRUPT, "no trailer after " + message[0] + " message(s)", -1);
        }
        if (payload[0] != h.payloadLength()) {
            return new Outcome(Kind.CORRUPT, "payload " + payload[0] + " bytes, header says " + h.payloadLength(), -1);
        }
        final long actual = digest.value();
        if (actual != h.payloadDigest()) {
            return new Outcome(Kind.CORRUPT, String.format("payload digest %016x, header says %016x", actual,
                    h.payloadDigest()), -1);
        }
        return Outcome.OK;
    }

    private static boolean equal(final DirectBuffer buffer, final int offset, final DirectBuffer expected) {
        for (int i = 0; i < ShadowHeader.LENGTH; i++) {
            if (buffer.getByte(offset + i) != expected.getByte(i)) {
                return false;
            }
        }
        return true;
    }
}
