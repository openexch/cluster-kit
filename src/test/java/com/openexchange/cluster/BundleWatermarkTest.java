// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

/**
 * The staging watermark, whose safe default runs the OTHER way to housekeeping's.
 *
 * <p>{@link ArchiveHousekeeping} defaults an absent watermark to {@link
 * Long#MAX_VALUE} because it has a floor underneath it: it will never purge above
 * the latest valid snapshot, so the worst an absent flag can do is reclaim more
 * than some remote consumer wanted. The staged log has no such floor. It is a
 * private copy of the chain whose only protection is this number, so the same
 * default there would truncate the anchor the next bundle extends from.
 *
 * <p>Two defaults that look inconsistent side by side, both safe in their own
 * place. These tests exist so a later tidy-up does not "fix" the inconsistency.
 */
public class BundleWatermarkTest {

    private static final String[] POSITIONAL = {
            "/cluster", "aeron:udp?endpoint=localhost:9301", "/staging", "/bundles", "assets", "2",
    };

    private static long parse(final String... extra) throws Exception {
        final String[] args = new String[POSITIONAL.length + extra.length];
        System.arraycopy(POSITIONAL, 0, args, 0, POSITIONAL.length);
        System.arraycopy(extra, 0, args, POSITIONAL.length, extra.length);

        final Method m = BundleCapture.class.getDeclaredMethod("watermarkFrom", String[].class);
        m.setAccessible(true);
        return (long) m.invoke(null, (Object) args);
    }

    @Test
    public void namedFlagIsRead() throws Exception {
        assertEquals(1557078112L, parse("--watermark=1557078112"));
    }

    @Test
    public void absentFlagReclaimsNothing() throws Exception {
        // The load-bearing assertion. -1 means "no durable position known", and
        // reclaimStaging leaves the staged log whole. Defaulting the other way
        // would delete the chain anchor the first time a caller forgot the flag.
        assertEquals(-1L, parse());
    }

    @Test
    public void nodeIdIsNotMistakenForAWatermark() throws Exception {
        // args[5] is the node id and parses cleanly as a long. Scanning from index
        // 6 is what stops "capture from node 2" becoming "everything below 2 is
        // durable" — the same class of mistake the housekeeping flag was named for.
        assertEquals(-1L, parse());
    }

    @Test
    public void negativeWatermarkReclaimsNothing() throws Exception {
        // A caller bug. The safe response for staging is to keep everything,
        // which is the opposite of housekeeping's safe response.
        assertEquals(-1L, parse("--watermark=-5"));
    }

    @Test
    public void zeroIsALegitimateWatermark() throws Exception {
        // "nothing is durable in S3 yet" is a real state on a fresh bucket, and it
        // is distinct from "we do not know". Both reclaim nothing here, but zero
        // must survive as zero rather than collapsing into the unknown case.
        assertEquals(0L, parse("--watermark=0"));
    }

    @Test
    public void flagIsFoundAmongOtherTrailingArguments() throws Exception {
        assertEquals(99L, parse("--something-else", "--watermark=99"));
    }
}
