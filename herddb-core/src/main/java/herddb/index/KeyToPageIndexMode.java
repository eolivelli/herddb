/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.index;

import herddb.utils.SystemProperties;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Selects which {@link KeyToPageIndex} implementation is used for table
 * primary-key indexes.
 *
 * <p>The value is resolved once per JVM from the system property
 * {@value #PROPERTY_NAME}. Allowed values are {@code incremental} (default —
 * the new on-disk format whose checkpoint cost is proportional to the amount
 * of change since the previous checkpoint) and {@code legacy} (the original
 * {@code BLinkKeyToPageIndex} whose checkpoint rewrites a metadata entry for
 * every node in the tree).</p>
 *
 * <p>Both implementations expose the same {@link KeyToPageIndex} surface and
 * are interchangeable from the point of view of {@code TableManager}.
 * Selection happens inside the {@code DataStorageManager.createKeyToPageMap}
 * factory methods via {@link KeyToPageIndexFactory}.</p>
 *
 * @see KeyToPageIndexFactory
 */
public final class KeyToPageIndexMode {

    private static final Logger LOGGER = Logger.getLogger(KeyToPageIndexMode.class.getName());

    /**
     * System property name used to configure the PK index implementation.
     */
    public static final String PROPERTY_NAME = "herddb.index.pk.mode";

    /**
     * Available PK index implementations.
     */
    public enum Mode {
        /**
         * New incremental format. Default.
         */
        INCREMENTAL,
        /**
         * Legacy {@code BLinkKeyToPageIndex} format.
         */
        LEGACY
    }

    private static final Mode RESOLVED = resolve();

    private KeyToPageIndexMode() {
    }

    /**
     * Returns the mode configured for this JVM.
     */
    public static Mode getResolved() {
        return RESOLVED;
    }

    private static Mode resolve() {
        String raw = SystemProperties.getStringSystemProperty(PROPERTY_NAME, "incremental");
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "incremental":
                return Mode.INCREMENTAL;
            case "legacy":
                return Mode.LEGACY;
            default:
                LOGGER.log(Level.WARNING,
                        "unrecognised value {0}={1}; falling back to 'incremental'",
                        new Object[]{PROPERTY_NAME, raw});
                return Mode.INCREMENTAL;
        }
    }
}
