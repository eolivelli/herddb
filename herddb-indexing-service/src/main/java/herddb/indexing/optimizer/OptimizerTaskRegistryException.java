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
package herddb.indexing.optimizer;

/**
 * Thrown when an {@link OptimizerTaskRegistry} operation fails. Mirrors the
 * hierarchy used by
 * {@link herddb.indexing.segment.SegmentRegistryException}.
 */
public class OptimizerTaskRegistryException extends Exception {

    public OptimizerTaskRegistryException(String message) {
        super(message);
    }

    public OptimizerTaskRegistryException(String message, Throwable cause) {
        super(message, cause);
    }

    public OptimizerTaskRegistryException(Throwable cause) {
        super(cause);
    }

    /** A task with the same id already exists in the registry. */
    public static class TaskAlreadyExists extends OptimizerTaskRegistryException {
        public TaskAlreadyExists(String taskId) {
            super("task already exists: " + taskId);
        }
    }

    /** A task was not found in the registry. */
    public static class TaskNotFound extends OptimizerTaskRegistryException {
        public TaskNotFound(String taskId) {
            super("task not found: " + taskId);
        }
    }

    /** A CAS update / delete failed because the expected version did not match. */
    public static class VersionMismatch extends OptimizerTaskRegistryException {
        public VersionMismatch(String taskId, int expectedVersion) {
            super("task " + taskId + " version mismatch (expected " + expectedVersion + ")");
        }
    }
}
