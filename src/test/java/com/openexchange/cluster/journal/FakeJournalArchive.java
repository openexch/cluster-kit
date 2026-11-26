// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import io.aeron.archive.client.ArchiveException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A member's archive in memory, for the rules that need no real replay: recordings of 256-byte records in segments of
 * {@link #SEG} bytes ({@link #PER_SEGMENT} records each), with the archive's own refusals of a purge. It also holds
 * recordings of other streams, which a listing of the journal's stream never returns.
 */
final class FakeJournalArchive implements JournalRetention.Archive {

    static final int FRAME = 256;
    static final int SEG = 4096;
    static final int PER_SEGMENT = SEG / FRAME;

    private static final class Rec {
        final long id;
        final boolean active;
        final boolean onStream;
        final List<JournalAudit.Rec> records = new ArrayList<>();
        long start;

        Rec(final long id, final boolean active, final boolean onStream) {
            this.id = id;
            this.active = active;
            this.onStream = onStream;
        }

        long end() {
            return (long) records.size() * FRAME;
        }
    }

    private final Map<Long, Rec> recordings = new LinkedHashMap<>();
    /** Ids every {@link #list} returned so far. */
    final Set<Long> listed = new TreeSet<>();
    /** Every purge call, as {@code segments <id> <position>} or {@code recording <id>}. */
    final List<String> purges = new ArrayList<>();
    /** Ids a purge call named. */
    final Set<Long> purgedIds = new TreeSet<>();
    /** Purge calls still to refuse as "replay in progress". */
    int refuseForReplay;
    /** Positions whose record cannot be read. */
    final Set<Long> unreadable = new TreeSet<>();
    int reads;
    /** The last record of every recording cannot be read. */
    boolean tailUnreadable;

    /** A recording on the journal's stream holding {@code first..last} of {@code epoch}. */
    FakeJournalArchive add(final long id, final boolean active, final long epoch, final long first, final long last) {
        final Rec r = new Rec(id, active, true);
        for (long seq = first; seq <= last; seq++) {
            r.records.add(new JournalAudit.Rec(r.end(), seq, epoch, JournalRig.logPositionOf(seq)));
        }
        recordings.put(id, r);
        return this;
    }

    /** Appends {@code first..last} to recording {@code id} (the member caught up, the journal grew). */
    FakeJournalArchive append(final long id, final long first, final long last) {
        final Rec r = recordings.get(id);
        final long epoch = r.records.get(0).epoch();
        for (long seq = first; seq <= last; seq++) {
            r.records.add(new JournalAudit.Rec(r.end(), seq, epoch, JournalRig.logPositionOf(seq)));
        }
        return this;
    }

    /** A recording of another stream in the same archive. */
    FakeJournalArchive addOtherStream(final long id, final int records) {
        final Rec r = new Rec(id, false, false);
        for (int i = 0; i < records; i++) {
            r.records.add(new JournalAudit.Rec(r.end(), i + 1, JournalRig.GENESIS, 0));
        }
        recordings.put(id, r);
        return this;
    }

    /** Replaces the sequence of the record at {@code position} of recording {@code id} (a recording that went wrong). */
    FakeJournalArchive overwrite(final long id, final long position, final long seq) {
        final Rec r = recordings.get(id);
        final int index = (int) (position / FRAME);
        final JournalAudit.Rec old = r.records.get(index);
        r.records.set(index, new JournalAudit.Rec(old.position(), seq, old.epoch(), old.logPosition()));
        return this;
    }

    boolean exists(final long id) {
        return recordings.containsKey(id);
    }

    long start(final long id) {
        return recordings.get(id).start;
    }

    JournalAudit.Snapshot snapshot() {
        final JournalAudit.Snapshot s = new JournalAudit.Snapshot();
        for (final Rec r : recordings.values()) {
            if (r.onStream) {
                final List<JournalAudit.Rec> left = new ArrayList<>();
                for (final JournalAudit.Rec rec : r.records) {
                    if (rec.position() >= r.start) {
                        left.add(rec);
                    }
                }
                s.recordings.put(r.id, new JournalAudit.Recording(r.id, r.start, r.end(), r.active, SEG, left));
            }
        }
        return s;
    }

    @Override
    public List<JournalRetention.Recording> list() {
        final List<JournalRetention.Recording> out = new ArrayList<>();
        for (final Rec r : recordings.values()) {
            if (r.onStream) {
                listed.add(r.id);
                out.add(new JournalRetention.Recording(r.id, r.start, r.end(), r.active, SEG, SEG));
            }
        }
        return out;
    }

    private static JournalRetention.Peek peek(final JournalAudit.Rec rec) {
        return JournalRetention.Peek.of(new JournalProfile.Key(rec.seq(), rec.epoch(), rec.logPosition(), 1, 1));
    }

    @Override
    public JournalRetention.Peek first(final JournalRetention.Recording recording, final long position,
                                       final long deadlineNs) {
        reads++;
        final Rec r = recordings.get(recording.id());
        if (r == null || position < r.start || position >= r.end() || position % FRAME != 0) {
            return JournalRetention.Peek.unreadable("invalid position " + position);
        }
        if (unreadable.contains(position)) {
            return JournalRetention.Peek.unreadable("schema id 9, the journal's is 5");
        }
        return peek(r.records.get((int) (position / FRAME)));
    }

    @Override
    public JournalRetention.Peek last(final JournalRetention.Recording recording, final long deadlineNs) {
        reads++;
        final Rec r = recordings.get(recording.id());
        if (r == null || r.end() <= r.start) {
            return JournalRetention.Peek.unreadable("no record");
        }
        if (tailUnreadable) {
            return JournalRetention.Peek.unreadable("REPLAY_TIMEOUT recording " + recording.id());
        }
        return peek(r.records.get(r.records.size() - 1));
    }

    @Override
    public void purgeSegments(final long recordingId, final long newStartPosition) {
        purges.add("segments " + recordingId + " " + newStartPosition);
        purgedIds.add(recordingId);
        final Rec r = recordings.get(recordingId);
        if (r == null) {
            throw new ArchiveException("unknown recording id: " + recordingId);
        }
        if (refuseForReplay > 0) {
            refuseForReplay--;
            throw new ArchiveException("invalid detach: replay in progress - newStartPosition=" + newStartPosition);
        }
        if (newStartPosition % SEG != 0) {
            throw new ArchiveException("invalid segment start: newStartPosition=" + newStartPosition);
        }
        if (newStartPosition < r.start + SEG) {
            throw new ArchiveException("invalid detach: newStartPosition=" + newStartPosition + " lowerBound="
                + (r.start + SEG));
        }
        if (newStartPosition > (r.end() & -(long) SEG)) {
            throw new ArchiveException("invalid detach: in use, newStartPosition=" + newStartPosition);
        }
        r.start = newStartPosition;
    }

    @Override
    public void purgeRecording(final long recordingId) {
        purges.add("recording " + recordingId);
        purgedIds.add(recordingId);
        final Rec r = recordings.get(recordingId);
        if (r == null) {
            throw new ArchiveException("unknown recording id: " + recordingId);
        }
        if (refuseForReplay > 0) {
            refuseForReplay--;
            throw new ArchiveException("cannot purge recording with active replay " + recordingId);
        }
        if (r.active) {
            throw new ArchiveException("cannot purge active recording " + recordingId);
        }
        recordings.remove(recordingId);
    }
}
