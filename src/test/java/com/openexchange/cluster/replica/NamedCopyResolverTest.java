// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

import io.aeron.cluster.service.ClusteredServiceContainer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Kit half: a retrieved copy whose envelope or decode refuses it joins the not-the-named-copy
 * set and is read once, not on every pass; an archive error is still retried. {@link GenesisGuard} over a
 * {@link NamedCopyResolver} wired as {@code ReplicaFollower} wires it, with a fake archive.
 */
public class NamedCopyResolverTest {

    static final long G1 = 111;
    static final int PASSES = 50;

    final long[] now = {1};
    final GenesisGuard.Named named = new GenesisGuard.Named(4096, 0, 0);
    final List<Long> candidates = new ArrayList<>();
    final Map<Long, ClusterIdentityCheck.Markers> markers = new HashMap<>();
    final Map<Long, Long> genesis = new HashMap<>();
    final Map<Long, RuntimeException> decodeFailure = new HashMap<>();
    final Map<Long, Integer> markerReads = new HashMap<>();
    final Map<Long, Integer> decodes = new HashMap<>();
    long namedByRecordingLog = -1;

    final NamedCopyResolver resolver = new NamedCopyResolver(new NamedCopyResolver.Source() {
        @Override
        public long retrievedCopy(final GenesisGuard.Named n) {
            return n.equals(named) ? namedByRecordingLog : -1;
        }

        @Override
        public List<Long> candidates() {
            return new ArrayList<>(candidates);
        }

        @Override
        public ClusterIdentityCheck.Markers markers(final long recordingId) {
            markerReads.merge(recordingId, 1, Integer::sum);
            return markers.get(recordingId);
        }

        @Override
        public long genesisOf(final long recordingId) {
            decodes.merge(recordingId, 1, Integer::sum);
            final RuntimeException failure = decodeFailure.get(recordingId);
            if (failure != null) {
                throw failure;
            }
            return genesis.get(recordingId);
        }
    });

    final GenesisGuard guard = new GenesisGuard(new GenesisGuard.Source() {
        @Override
        public GenesisGuard.Named newestNamed() {
            return named;
        }

        @Override
        public long localCopy(final GenesisGuard.Named n) {
            return resolver.resolve(n);
        }

        @Override
        public long genesisOf(final long recordingId) {
            return resolver.genesisOf(recordingId);
        }

        @Override
        public String termContradiction(final long recordingId) {
            return null;
        }
    }, () -> now[0], AlignmentTrust.CHECK_PACE_NS, false);

    void completeCopy(final long id) {
        candidates.add(id);
        markers.put(id, new ClusterIdentityCheck.Markers(ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID,
                named.position(), named.leadershipTermId(), true));
    }

    GenesisGuard.Outcome pass() {
        now[0] += AlignmentTrust.CHECK_PACE_NS + TimeUnit.MILLISECONDS.toNanos(1);
        return guard.check(G1);
    }

    static ClusterIdentityCheck.CopyRefused refused(final long id) {
        return new ClusterIdentityCheck.CopyRefused("retrieved snapshot recording " + id + " (64 payload bytes) did"
                + " not decode: java.lang.IllegalStateException: envelope CRC mismatch", null);
    }

    void assertReadOnce(final long id, final String path) {
        final int m = markerReads.getOrDefault(id, 0);
        final int d = decodes.getOrDefault(id, 0);
        if (d != 1 || m > 1) {
            throw new AssertionError("the refused copy rec=" + id + " (" + path + ") was read " + m
                    + " times (markers) and decoded " + d + " times in " + PASSES + " passes (expected once)");
        }
    }

    /** An archive-scan copy refused by its decode is read once; a later complete copy is then used. */
    @Test
    public void aCopyRefusedByItsDecodeIsReadOnce() {
        completeCopy(7);
        decodeFailure.put(7L, refused(7));
        for (int i = 0; i < PASSES; i++) {
            final GenesisGuard.Outcome o = pass();
            assertTrue(o == GenesisGuard.Outcome.UNREADABLE || o == GenesisGuard.Outcome.WAITING);
        }
        assertReadOnce(7, "archive scan");
        assertTrue(guard.namedCopyUnusable());
        completeCopy(9);
        genesis.put(9L, G1);
        assertEquals(GenesisGuard.Outcome.PASSED, pass());
        assertEquals(1, (int)decodes.get(7L));
        System.out.println("GREEN refused copy rec=7 read once in " + PASSES + " passes; rec=9 then PASSED");
    }

    /** A copy the backup's RecordingLog names, refused by its decode, is read once too. */
    @Test
    public void aRecordingLogNamedCopyRefusedByItsDecodeIsReadOnce() {
        namedByRecordingLog = 7;
        completeCopy(7);
        decodeFailure.put(7L, refused(7));
        for (int i = 0; i < PASSES; i++) {
            pass();
        }
        assertReadOnce(7, "recording log");
        System.out.println("GREEN RecordingLog-named refused copy rec=7 decoded once in " + PASSES + " passes");
    }

    /** An archive error is not a refusal; the copy is judged again (retried) at the next paced pass. */
    @Test
    public void anArchiveErrorIsRetried() {
        completeCopy(7);
        decodeFailure.put(7L, new IllegalStateException("retrieved snapshot recording 7 is not in the archive"));
        for (int i = 0; i < 5; i++) {
            assertEquals(GenesisGuard.Outcome.UNREADABLE, pass());
        }
        assertEquals("an archive error is retried on every paced pass", 5, (int)decodes.get(7L));
        decodeFailure.remove(7L);
        genesis.put(7L, G1);
        assertEquals(GenesisGuard.Outcome.PASSED, pass());
        assertTrue(resolver.notIt().isEmpty());
        System.out.println("GREEN archive error retried: 5 decodes then PASSED");
    }
}
