// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

/**
 * Positive control for the never-schedules scan: its only forbidden reference is the DESCRIPTOR of an unused field
 * of type {@code io.aeron.cluster.service.Cluster} (no class constant, no member access), so the scan must read
 * descriptors to flag it.
 */
@SuppressWarnings("unused")
public final class ForbiddenFieldProbe {
    private io.aeron.cluster.service.Cluster cluster;
}
