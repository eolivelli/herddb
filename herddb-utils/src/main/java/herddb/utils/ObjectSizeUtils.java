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

package herddb.utils;

import java.lang.management.ManagementFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Utility to detect JVM object layout parameters at runtime and provide
 * accurate object size estimates for memory accounting.
 * <p>
 * Please note that this is only used as an estimate of the actual memory used.
 * We are not computing an exact size: <ul>
 * <li>there is no way to do it in Java currently
 * <li>even if it was possible we should not take into account references to
 * objects that are already retained by the system, like refs to Table
 * definitions or cached constants
 * </ul>
 */
public final class ObjectSizeUtils {

    private static final Logger LOGGER = Logger.getLogger(ObjectSizeUtils.class.getName());

    /**
     * Whether compressed oops are enabled in this JVM.
     */
    public static final boolean COMPRESSED_OOPS;

    /**
     * Size of an object reference in bytes (4 with compressed oops, 8 without).
     */
    public static final int REFERENCE_SIZE;

    /**
     * Size of an object header in bytes (12 with compressed oops/class pointers, 16 without).
     */
    public static final int OBJECT_HEADER_SIZE;

    /**
     * Default overhead for a generic object (header size). Used for rough plan size estimates.
     */
    public static final int DEFAULT_OBJECT_SIZE_OVERHEAD = 16;

    /**
     * Size of a boolean field in an object (1 byte + alignment padding, typically 4 bytes).
     */
    public static final int BOOLEAN_FIELD_SIZE = 4;

    static {
        COMPRESSED_OOPS = detectCompressedOops();
        REFERENCE_SIZE = COMPRESSED_OOPS ? 4 : 8;
        OBJECT_HEADER_SIZE = COMPRESSED_OOPS ? 12 : 16;
        LOGGER.log(Level.INFO,
                "JVM object layout: compressedOops={0}, referenceSize={1}, objectHeaderSize={2}",
                new Object[]{COMPRESSED_OOPS, REFERENCE_SIZE, OBJECT_HEADER_SIZE});
    }

    private ObjectSizeUtils() {
    }

    public static int stringSize(String s) {
        return DEFAULT_OBJECT_SIZE_OVERHEAD + (s != null ? s.length() : 0);
    }

    private static boolean detectCompressedOops() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName mbean = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
            Object vmOption = server.invoke(mbean, "getVMOption",
                    new Object[]{"UseCompressedOops"},
                    new String[]{"java.lang.String"});
            // VMOption.getValue() returns "true" or "false"
            String value = (String) vmOption.getClass().getMethod("getValue").invoke(vmOption);
            return Boolean.parseBoolean(value);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Cannot detect UseCompressedOops, assuming enabled (conservative estimate)", e);
            // Default to true (smaller estimates) — this is the common case for heaps < 32GB
            return true;
        }
    }

    /**
     * Align a size to 8-byte boundary (JVM object alignment).
     */
    public static long align(long size) {
        return (size + 7L) & ~7L;
    }
}
