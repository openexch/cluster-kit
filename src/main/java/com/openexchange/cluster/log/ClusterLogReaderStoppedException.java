// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

/** Thrown by {@link ClusterLogReader#poll} (and {@code successor}) once the reader has stopped. */
public final class ClusterLogReaderStoppedException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    private final transient ReaderStop stop;

    public ClusterLogReaderStoppedException(final ReaderStop stop) {
        super("cluster log reader stopped: " + stop, stop.cause());
        this.stop = stop;
    }

    public ReaderStop stop() {
        return stop;
    }
}
