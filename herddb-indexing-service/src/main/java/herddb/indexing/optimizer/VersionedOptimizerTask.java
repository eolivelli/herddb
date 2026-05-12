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

import java.util.Objects;

/**
 * An {@link OptimizerTask} paired with the ZooKeeper version of the znode it
 * was read from. Used by callers to perform CAS updates via
 * {@link OptimizerTaskRegistry#casUpdateTask(VersionedOptimizerTask, OptimizerTask)}.
 */
public final class VersionedOptimizerTask {

    private final OptimizerTask task;
    private final int zkVersion;

    public VersionedOptimizerTask(OptimizerTask task, int zkVersion) {
        this.task = Objects.requireNonNull(task, "task");
        this.zkVersion = zkVersion;
    }

    public OptimizerTask task() {
        return task;
    }

    public int zkVersion() {
        return zkVersion;
    }

    @Override
    public String toString() {
        return "VersionedOptimizerTask{zkVersion=" + zkVersion + ", task=" + task + '}';
    }
}
