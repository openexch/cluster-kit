// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.replica;

/**
 * What a replica fence is, which decides how the process ends.
 *
 * <pre>
 * MISMATCH  the member copy at P opened, holds this replica's genesis, and its bytes differ from the held image: a
 *           same-history byte mismatch an operator must adjudicate. The replica HOLDS: it stops applying and
 *           writing, reads 503 {@code fenced:oracle mismatch at <P>: <detail>}, keeps serving /metrics and the
 *           mismatch dump, and exits only on operator stop, driver loss or REPLICA_MISMATCH_HOLD_MS.
 * HISTORY   evidence of another history: a member copy of another genesis (at the compare or at the genesis check),
 *           or a member snapshot at a position this replica applied past without a SNAPSHOT action there. Own
 *           shadows above the last EQUAL position or the restore point, whichever is higher, are purged (awaited,
 *           bounded by REPLICA_FENCE_PURGE_WAIT_MS; the ones at or below it are kept), then exit 1.
 * FAULT     everything else (apply failure, stall, writer, buffer, identity, restore): exit 1.
 * </pre>
 */
public enum FenceFamily {
    MISMATCH,
    HISTORY,
    FAULT
}
