// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import java.util.Objects;

/**
 * The producer's own identity and the cluster id, from host configuration (RCS S3 5.1, G3): a member passes
 * {@code cluster.context().clusterId()} and its member id, the replica its configured ids (D-S3-11).
 */
public record ShadowProducer(int clusterId, ProducerKind kind, int producerId) {

    public ShadowProducer {
        Objects.requireNonNull(kind, "kind");
        if (clusterId < 0) {
            throw new IllegalArgumentException("clusterId must be >= 0: " + clusterId);
        }
        if (producerId < 0) {
            throw new IllegalArgumentException("producerId must be >= 0: " + producerId);
        }
    }
}
