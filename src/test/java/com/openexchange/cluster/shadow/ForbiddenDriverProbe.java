// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.shadow;

import io.aeron.archive.ArchivingMediaDriver;

/** Positive control for {@code Rules.shadow()}: a field of a media driver type (C13: hosts own the driver). */
public final class ForbiddenDriverProbe {
    ArchivingMediaDriver driver;
}
