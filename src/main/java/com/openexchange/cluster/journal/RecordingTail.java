// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import com.openexchange.cluster.shadow.ShadowReplay;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.Objects;
import java.util.function.LongPredicate;

/**
 * The last whole message of a recording, read without ever asking the archive for a replay that starts inside a
 * frame. The archive refuses such a start and, besides answering the caller, raises the refusal on its own thread
 * into the node's error handler: a reader that probes for the last frame fills the error log of a healthy member.
 *
 * <h2>What is answered</h2>
 *
 * <p>For a recording {@code [start, stop)} with terms of {@code termLength} bytes, let
 * {@code F = max(start, the base of the term that holds stop - 1)}. The answer is the decoded last whole message
 * among the frames of {@code [F, stop)} that the decoder counts (it answers null for one it does not), or null when
 * that range holds none. Padding frames are not messages. The answer is a function of the recording's bytes and of
 * {@code (start, stop, termLength)} alone: what this object remembers changes how many bytes are replayed, never the
 * answer.</p>
 *
 * <h2>Where a replay starts</h2>
 *
 * <p>Only at one of two positions, both the start of a frame:</p>
 * <ul>
 *   <li>{@code F}: the recording's start position (a recording starts with a frame), or a term base above it (a
 *       publication never lets a frame, or a fragmented message, cross a term end: it pads the term, so every term
 *       base starts a frame);</li>
 *   <li>the position just after the last whole message this object delivered from the same recording (the end of a
 *       frame is the start of the next one).</li>
 * </ul>
 *
 * <p>The first look at a recording (a new object, a new recording, a recording whose range left the remembered
 * message behind: its end moved into a later term, or a purge moved its start past it, or its end moved back)
 * replays {@code [F, stop)}, at most one term. Every later look replays only the bytes recorded since the last
 * message, and nothing at all when the end has not moved.</p>
 *
 * <h2>Cost</h2>
 *
 * <p>A first look replays up to one term of the recording through a replay publication the archive creates with
 * the recording's term length: up to one term of bytes read from disk and copied through an IPC log buffer whose
 * three terms are mapped sparse (pages are touched as they are written, so about the replayed bytes, not three
 * terms). That is paid once per recording by a new object, and again whenever more than the rest of a term was
 * recorded between two looks (the end moved into a later term). {@link #replayBytes()} counts it.</p>
 *
 * <h2>What it relies on</h2>
 *
 * <p>The bytes of a recording below its end do not change: a recording is appended to, and purged from its start.
 * A recording that is truncated and extended again must not be read through an object that saw it before the
 * truncation (nothing in an engine member truncates a journal recording). If the archive still refuses a remembered
 * position, the entry is dropped and the same call reads from {@code F}.</p>
 *
 * <p>One owner thread. The decoder runs for every message of the replayed range and must not throw.</p>
 *
 * @param <T> what a message is decoded to
 */
public final class RecordingTail<T> {

    /**
     * Decodes one whole message, or answers null for a message that does not count (the tail is then the last one
     * before it that does). Never throws. The buffer is only valid during the call.
     */
    @FunctionalInterface
    public interface Decoder<T> {
        T decode(DirectBuffer buffer, int offset, int length, Header header);
    }

    private static final class Entry<T> {
        /** The end the recording had when it was last read. */
        long end;
        /** The position just after the last whole message delivered, counted or not: a frame start; -1 for none. */
        long resume = -1;
        /** The position just after the last message the decoder counted; -1 for none. */
        long valueEnd = -1;
        T value;
    }

    private final Decoder<T> decoder;
    private final Long2ObjectHashMap<Entry<T>> entries = new Long2ObjectHashMap<>();
    private long firstLooks;
    private long resumedReads;
    private long unchanged;
    private long replayBytes;

    public RecordingTail(final Decoder<T> decoder) {
        this.decoder = Objects.requireNonNull(decoder, "decoder");
    }

    /**
     * The decoded last counted message of {@code [F, stop)} (see the class comment), or null when there is none.
     *
     * @param replay      the replay to read with (session scoped, bounded)
     * @param recordingId the recording
     * @param start       the recording's start position as the archive lists it now
     * @param stop        the recording's stop position, or its recorded position while it is active
     * @param termLength  the recording's term length
     * @param deadlineNs  an absolute value of the replay's clock
     * @throws ArchiveException                        the archive refused the replay
     * @throws ShadowReplay.ReplayTimeoutException     past the deadline
     * @throws ShadowReplay.ReplayJoinedLateException  the replay image did not start where it was asked to
     * @throws ShadowReplay.ReplayEndedEarlyException  the replay ended before {@code stop}
     * @throws IllegalStateException                   the replay image closed before its end
     */
    public T last(final ShadowReplay replay, final long recordingId, final long start, final long stop,
                  final int termLength, final long deadlineNs) {
        if (stop <= start) {
            entries.remove(recordingId);
            return null;
        }
        final long floor = Math.max(start, (stop - 1) & -(long) termLength);
        final Entry<T> known = entries.get(recordingId);
        // what is remembered is usable when the recording did not shrink and the remembered position lies inside the
        // last term, above the floor: no message crosses a term base, and a start moved by a purge is a term base
        if (known != null && known.end <= stop && known.resume > floor) {
            if (known.end == stop) {
                unchanged++;
                return valueAbove(known, floor);
            }
            try {
                read(replay, recordingId, known.resume, stop, known, deadlineNs);
                resumedReads++;
                return valueAbove(known, floor);
            } catch (final ArchiveException refused) {
                if (refused.errorCode() != ArchiveException.INVALID_POSITION) {
                    throw refused;
                }
                // the remembered position is not a frame start any more: the recording is not the one that was
                // read before. Forget it and read from the floor, in this call.
                entries.remove(recordingId);
            }
        } else if (known != null) {
            entries.remove(recordingId);
        }
        final Entry<T> fresh = new Entry<>();
        firstLooks++;
        read(replay, recordingId, floor, stop, fresh, deadlineNs);
        if (fresh.resume < 0) {
            return null; // no whole message at all: nothing to resume from
        }
        entries.put(recordingId, fresh);
        return valueAbove(fresh, floor);
    }

    /** The remembered value when its message lies above the floor (inside the last term), else null. */
    private static <T> T valueAbove(final Entry<T> entry, final long floor) {
        return entry.valueEnd > floor ? entry.value : null;
    }

    /** Replays {@code [from, stop)} into {@code entry}. Only a replay that ran to its end changes the entry. */
    private void read(final ShadowReplay replay, final long recordingId, final long from, final long stop,
                      final Entry<T> entry, final long deadlineNs) {
        final Object[] last = {null};
        final long[] ends = {-1, -1};
        replayBytes += stop - from;
        replay.readRange(recordingId, from, stop - from, (buffer, offset, length, header) -> {
            ends[0] = header.position();
            final T decoded = decoder.decode(buffer, offset, length, header);
            if (decoded != null) {
                last[0] = decoded;
                ends[1] = header.position();
            }
        }, deadlineNs);
        entry.end = stop;
        if (ends[0] >= 0) {
            entry.resume = ends[0];
        }
        if (ends[1] >= 0) {
            @SuppressWarnings("unchecked")
            final T value = (T) last[0];
            entry.value = value;
            entry.valueEnd = ends[1];
        }
    }

    /** Forgets every recording {@code listed} does not accept (recordings the archive no longer lists). */
    public void retain(final LongPredicate listed) {
        entries.keySet().removeIf(id -> !listed.test(id));
    }

    /** Forgets everything: the next look at every recording is a first look. */
    public void clear() {
        entries.clear();
    }

    /** Looks that replayed from the floor {@code F}. */
    public long firstLooks() {
        return firstLooks;
    }

    /** Looks that replayed only the bytes after the remembered message. */
    public long resumedReads() {
        return resumedReads;
    }

    /** Looks answered without a replay: the recording's end had not moved. */
    public long unchanged() {
        return unchanged;
    }

    /**
     * The bytes this object asked the archive to replay, over all its looks: the length of every range it started
     * a replay for, whether the replay ran to its end or not.
     */
    public long replayBytes() {
        return replayBytes;
    }

    /** The position the next look at {@code recordingId} would resume from, or -1 when nothing is remembered. */
    public long resumePosition(final long recordingId) {
        final Entry<T> e = entries.get(recordingId);
        return e == null ? -1 : e.resume;
    }
}
