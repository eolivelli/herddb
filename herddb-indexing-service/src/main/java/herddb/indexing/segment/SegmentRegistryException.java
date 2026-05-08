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
package herddb.indexing.segment;

/**
 * Thrown when a {@link SegmentRegistryClient} operation fails.
 */
public class SegmentRegistryException extends Exception {

    public SegmentRegistryException(String message) {
        super(message);
    }

    public SegmentRegistryException(String message, Throwable cause) {
        super(message, cause);
    }

    public SegmentRegistryException(Throwable cause) {
        super(cause);
    }

    /** A segment with the same UUID already exists in the registry. */
    public static class SegmentAlreadyExists extends SegmentRegistryException {
        public SegmentAlreadyExists(String segmentUuid) {
            super("segment already exists: " + segmentUuid);
        }
    }

    /** A segment was not found in the registry. */
    public static class SegmentNotFound extends SegmentRegistryException {
        public SegmentNotFound(String segmentUuid) {
            super("segment not found: " + segmentUuid);
        }
    }

    /** A CAS (compare-and-swap) update failed because the expected version did not match. */
    public static class VersionMismatch extends SegmentRegistryException {
        public VersionMismatch(String segmentUuid, int expectedVersion) {
            super("segment " + segmentUuid + " version mismatch (expected " + expectedVersion + ")");
        }
    }
}
