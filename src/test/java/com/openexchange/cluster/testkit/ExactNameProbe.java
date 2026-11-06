// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.testkit;

/**
 * Negative control for the never-schedules scan: types whose names merely START with a forbidden name
 * ({@code ...service.Cluster} is a prefix of {@code ClusteredService} and {@code ClusterMarkFile}, {@code io.aeron.Aeron} of
 * {@code AeronCounters})
 * and a {@code System} member outside the clock/env list. Exact matching must leave this class clean under RF-6.
 */
@SuppressWarnings("unused")
public final class ExactNameProbe {
    private io.aeron.cluster.service.ClusteredService service;
    private io.aeron.cluster.service.ClusterMarkFile markFile;
    private io.aeron.AeronCounters counters;

    public static int hash(final Object o) {
        return System.identityHashCode(o);
    }
}
