// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.archive.client.AeronArchive;

/** A positive control for the puller scan: a replay of a remote recording on UDP. Never run. */
final class ForbiddenRemoteReplayProbe {

    private ForbiddenRemoteReplayProbe() {
    }

    static long probe(final AeronArchive remote, final long recordingId) {
        return remote.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, "aeron:udp?endpoint=localhost:4302",
                ShadowAlias.REPLAY_STREAM_ID);
    }
}
