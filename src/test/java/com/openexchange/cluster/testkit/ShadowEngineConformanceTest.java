// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

import com.openexchange.cluster.log.ShadowEngine;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** RCS S2 3.6: a model engine passes R1-R7; each rule has an engine that breaks only it, and the rule catches it. */
public class ShadowEngineConformanceTest {

    enum Defect { NONE, GENESIS_TWICE, SERIALIZE_MUTATES, RESTORE_LOSES_SESSIONS, TERM_MUTATES, UNKNOWN_CLOSE_FOLDS,
        CLASH_ACCEPTED, NO_FAILURE_LATCH }

    /** HashChainEngine semantics plus a replicated session table (unknown close = no-op, clash refused). */
    static final class ModelEngine implements ShadowEngine {
        private static final int MAGIC = 0x4D4F444C;
        private final Defect defect;
        private final AtomicInteger events;
        private final TreeSet<Long> sessions = new TreeSet<>();
        private long chain = 17;
        private long serializations;
        private boolean pristine = true;
        private boolean startedByGenesis;
        private boolean failed;

        ModelEngine(final Defect defect, final AtomicInteger events) {
            this.defect = defect;
            this.events = events;
        }

        @Override
        public int engineId() {
            return 98;
        }

        @Override
        public void genesis() {
            ensure();
            if (!pristine && !(defect == Defect.GENESIS_TWICE && startedByGenesis)) {
                throw new IllegalStateException("not pristine");
            }
            pristine = false;
            startedByGenesis = true;
        }

        @Override
        public void restore(final DirectBuffer image, final int length) {
            ensure();
            if (!pristine) {
                throw new IllegalStateException("not pristine");
            }
            pristine = false;
            startedByGenesis = false;
            if (image.getInt(0, ByteOrder.LITTLE_ENDIAN) != MAGIC) {
                failed = true;
                throw new IllegalStateException("bad image");
            }
            chain = image.getLong(4, ByteOrder.LITTLE_ENDIAN);
            serializations = image.getLong(12, ByteOrder.LITTLE_ENDIAN);
            final int n = image.getInt(20, ByteOrder.LITTLE_ENDIAN);
            if (defect != Defect.RESTORE_LOSES_SESSIONS) {
                for (int i = 0; i < n; i++) {
                    sessions.add(image.getLong(24 + i * 8, ByteOrder.LITTLE_ENDIAN));
                }
            }
        }

        @Override
        public int serialize(final MutableDirectBuffer dst) {
            ensure();
            if (defect == Defect.SERIALIZE_MUTATES) {
                serializations++;
            }
            dst.putInt(0, MAGIC, ByteOrder.LITTLE_ENDIAN);
            dst.putLong(4, chain, ByteOrder.LITTLE_ENDIAN);
            dst.putLong(12, serializations, ByteOrder.LITTLE_ENDIAN);
            dst.putInt(20, sessions.size(), ByteOrder.LITTLE_ENDIAN);
            int at = 24;
            for (final long id : sessions) {
                dst.putLong(at, id, ByteOrder.LITTLE_ENDIAN);
                at += 8;
            }
            return at;
        }

        @Override
        public void onSessionOpen(final long id, final long p, final long ts, final byte[] principal) {
            enter();
            if (sessions.contains(id) && defect != Defect.CLASH_ACCEPTED) {
                failed = true;
                throw new IllegalStateException("clashing open " + id);
            }
            sessions.add(id);
            fold(1, id, p, ts);
            events.incrementAndGet();
        }

        @Override
        public void onSessionClose(final long id, final long p, final long ts, final CloseReason reason) {
            enter();
            if (!sessions.remove(id) && defect != Defect.UNKNOWN_CLOSE_FOLDS) {
                return;
            }
            fold(2, id, p, ts);
            events.incrementAndGet();
        }

        @Override
        public boolean onSessionMessage(final long id, final long p, final long ts, final DirectBuffer buffer,
                                        final int offset, final int length) {
            enter();
            fold(3, id, p, ts);
            for (int i = 0; i < length; i++) {
                chain = chain * 31 + buffer.getByte(offset + i);
            }
            events.incrementAndGet();
            return true;
        }

        @Override
        public int onTimerEvent(final long correlationId, final long p, final long ts) {
            enter();
            fold(4, correlationId, p, ts);
            events.incrementAndGet();
            return 0;
        }

        @Override
        public void onNewLeadershipTermEvent(final long term, final long p, final long ts, final TimeUnit unit) {
            enter();
            if (defect == Defect.TERM_MUTATES) {
                fold(5, term, p, ts);
            }
        }

        void poison() {
            if (defect != Defect.NO_FAILURE_LATCH) {
                failed = true;
            }
        }

        private void fold(final int kind, final long id, final long p, final long ts) {
            chain = ((chain * 31 + kind) * 31 + id) * 31 + p * 7 + ts;
        }

        private void enter() {
            ensure();
            pristine = false;
        }

        private void ensure() {
            if (failed) {
                throw new IllegalStateException("failed");
            }
        }
    }

    private static final RecordedLog SAMPLE = ShadowRestoreRunnerTest.syntheticLog();

    private static ShadowEngineConformance.Report run(final Defect defect) {
        final AtomicInteger events = new AtomicInteger();
        return ShadowEngineConformance.run(() -> new ModelEngine(defect, events), SAMPLE,
                new ShadowEngineConformance.Options<ModelEngine>(true, ModelEngine::poison, events::get));
    }

    @Test
    public void modelEnginePassesEveryRule() {
        final ShadowEngineConformance.Report report = run(Defect.NONE);
        assertEquals(600, report.frames());
        assertEquals(601, report.splices());
    }

    private static void assertRule(final Defect defect, final String rule) {
        try {
            run(defect);
        } catch (final AssertionError e) {
            assertTrue(defect + " must fail " + rule + ", got: " + e.getMessage(), e.getMessage().startsWith(rule));
            return;
        }
        fail("conformance blind to " + defect);
    }

    @Test
    public void eachRuleCatchesItsDefect() {
        assertRule(Defect.GENESIS_TWICE, "R1");
        assertRule(Defect.SERIALIZE_MUTATES, "R2");
        assertRule(Defect.RESTORE_LOSES_SESSIONS, "R3");
        assertRule(Defect.TERM_MUTATES, "R4");
        assertRule(Defect.UNKNOWN_CLOSE_FOLDS, "R5");
        assertRule(Defect.CLASH_ACCEPTED, "R6");
        assertRule(Defect.NO_FAILURE_LATCH, "R7");
    }

    @Test
    public void hashChainEngineIsNotAModelEngineItFailsR5() {
        try {
            ShadowEngineConformance.run(HashChainEngine::new, SAMPLE,
                    new ShadowEngineConformance.Options<HashChainEngine>(false, HashChainEngine::poison, () -> 0));
        } catch (final AssertionError e) {
            assertTrue(e.getMessage(), e.getMessage().startsWith("R5"));
            return;
        }
        fail("HashChainEngine folds a close of an unknown id; R5 must say so");
    }
}
