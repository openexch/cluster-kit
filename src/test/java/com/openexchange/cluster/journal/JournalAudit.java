// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.journal;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The tests' own check of what a pass deleted, from two snapshots of the recordings (before and after) and what the
 * test knows to be true: the readers' real cursors, the member's genesis, the off-box floor. Every rule of the
 * retention has one failure text here; a negative control switches the rule off inside the retention and must be
 * failed by this check with that text.
 */
final class JournalAudit {

    record Rec(long position, long seq, long epoch, long logPosition) {
    }

    record Recording(long id, long start, long end, boolean active, int segmentLength, List<Rec> records) {
        long lastSeq() {
            return records.isEmpty() ? -1 : records.get(records.size() - 1).seq();
        }
    }

    static final class Snapshot {
        final Map<Long, Recording> recordings = new LinkedHashMap<>();

        /** The member's tail as its restore rules read it: the largest last sequence over the genesis' recordings. */
        long tail(final long genesis, final boolean hasEpoch) {
            long max = -1;
            for (final Recording r : recordings.values()) {
                if (!r.records().isEmpty() && (!hasEpoch || r.records().get(r.records().size() - 1).epoch() == genesis)) {
                    max = Math.max(max, r.lastSeq());
                }
            }
            return max;
        }

        long retainedBytes() {
            long sum = 0;
            for (final Recording r : recordings.values()) {
                sum += r.end() - r.start();
            }
            return sum;
        }
    }

    /** What the test knows to be true. */
    static final class Truth {
        long genesis = JournalRig.GENESIS;
        boolean hasEpoch = true;
        /** Reader name to the sequence it really applied; an empty map is "no registered reader". */
        final Map<String, Long> cursors = new LinkedHashMap<>();
        /** The off-box floor in log position; {@code Long.MAX_VALUE} for none. */
        long logFloor = Long.MAX_VALUE;
        /** Epoch 0 records (written before the member had a genesis) are no other genesis'. */
        boolean epochZeroIsNoGenesis;

        Truth epochZeroIsNoGenesis() {
            epochZeroIsNoGenesis = true;
            return this;
        }

        Truth cursor(final String reader, final long applied) {
            cursors.put(reader, applied);
            return this;
        }

        Truth noEpoch() {
            hasEpoch = false;
            return this;
        }

        Truth logFloor(final long floor) {
            logFloor = floor;
            return this;
        }
    }

    private JournalAudit() {
    }

    /** Fails with the text of the first rule the pass broke; returns the number of records it deleted. */
    static long check(final Snapshot before, final Snapshot after, final Truth truth) {
        long purged = 0;
        final long tailBefore = before.tail(truth.genesis, truth.hasEpoch);
        for (final Recording b : before.recordings.values()) {
            final Recording a = after.recordings.get(b.id());
            final long newStart = a == null ? Long.MAX_VALUE : a.start();
            for (final Rec rec : b.records()) {
                if (rec.position() >= newStart) {
                    break;
                }
                purged++;
                if (truth.hasEpoch && rec.epoch() != truth.genesis
                    && !(truth.epochZeroIsNoGenesis && rec.epoch() == 0)) {
                    throw new AssertionError("purged a recording of genesis " + rec.epoch() + " (recording " + b.id()
                        + ", this member's genesis is " + truth.genesis + ")");
                }
                if (truth.cursors.isEmpty()) {
                    throw new AssertionError("purged with no registered reader (recording " + b.id() + ", journalSeq "
                        + rec.seq() + ")");
                }
                for (final Map.Entry<String, Long> c : truth.cursors.entrySet()) {
                    if (rec.seq() > c.getValue()) {
                        throw new AssertionError("purged a record above reader " + c.getKey() + "'s cursor (recording "
                            + b.id() + ", journalSeq " + rec.seq() + ", cursor " + c.getValue() + ")");
                    }
                }
                if (rec.logPosition() > truth.logFloor) {
                    throw new AssertionError("purged past the off-box position (recording " + b.id() + ", journalSeq "
                        + rec.seq() + ", log position " + rec.logPosition() + ", floor " + truth.logFloor + ")");
                }
            }
        }
        for (final Recording a : after.recordings.values()) {
            if (a.active()) {
                final long headBase = a.end() & -(long) a.segmentLength();
                final Recording b = before.recordings.get(a.id());
                if (b != null && a.start() > b.start() && headBase - a.start() < 2L * a.segmentLength()) {
                    throw new AssertionError("fewer than two segments below the head (recording " + a.id() + ": start "
                        + a.start() + ", head " + a.end() + ")");
                }
            }
        }
        final long tailAfter = after.tail(truth.genesis, truth.hasEpoch);
        if (tailAfter < tailBefore) {
            throw new AssertionError("journal tail fell from " + tailBefore + " to " + tailAfter);
        }
        return purged;
    }

    /**
     * A pass that purged anything while a reader file's bound was above the member's tail purged on a number that
     * cannot be a cursor of this journal.
     */
    static void checkBoundWithinTail(final long purged, final long largestReaderBound, final long tailBefore) {
        if (purged > 0 && largestReaderBound > tailBefore) {
            throw new AssertionError("purged records above the member's tail: the pass purged " + purged
                + " records by a reader bound of " + largestReaderBound + ", the tail is " + tailBefore);
        }
    }

    /** Every record of this genesis above {@code safeSeq} that was readable before is readable after. */
    static void checkKept(final Snapshot before, final Snapshot after, final Truth truth, final long safeSeq) {
        final TreeSet<Long> left = new TreeSet<>();
        for (final Recording a : after.recordings.values()) {
            for (final Rec rec : a.records()) {
                if (!truth.hasEpoch || rec.epoch() == truth.genesis) {
                    left.add(rec.seq());
                }
            }
        }
        for (final Recording b : before.recordings.values()) {
            for (final Rec rec : b.records()) {
                if ((!truth.hasEpoch || rec.epoch() == truth.genesis) && rec.seq() > safeSeq
                    && !left.contains(rec.seq())) {
                    throw new AssertionError("journalSeq " + rec.seq() + " above safeSeq " + safeSeq
                        + " does not replay any more (it was in recording " + b.id() + ")");
                }
            }
        }
    }

    /** A negative control: {@code body} must fail with an {@link AssertionError} containing {@code named}. */
    static String nc(final String id, final String named, final Runnable body) {
        try {
            body.run();
        } catch (final AssertionError e) {
            final String msg = String.valueOf(e.getMessage());
            if (!msg.contains(named)) {
                throw new AssertionError("negative control " + id + " failed for the wrong reason: <" + msg
                    + ">, expected <" + named + ">", e);
            }
            System.out.println("NC " + id + " -> " + msg);
            return msg;
        }
        throw new AssertionError("negative control " + id + " did not fail: the assertion cannot see its rule");
    }
}
