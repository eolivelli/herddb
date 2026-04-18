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

import herddb.core.MemoryManager;
import herddb.index.KeyToPageIndexMode.Mode;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.index.blink.IncrementalBLinkKeyToPageIndex;
import herddb.storage.DataStorageManager;

/**
 * Central factory for the table primary-key {@link KeyToPageIndex}. Invoked
 * from every {@code DataStorageManager.createKeyToPageMap} implementation
 * that persists its data (File, BookKeeper, RemoteFile, ReadReplica). The
 * in-memory {@code MemoryDataStorageManager} intentionally bypasses this
 * factory and keeps its simpler {@code ConcurrentMapKeyToPageIndex}.
 *
 * @see KeyToPageIndexMode
 */
public final class KeyToPageIndexFactory {

    private KeyToPageIndexFactory() {
    }

    /**
     * Returns the {@link KeyToPageIndex} implementation configured for this
     * JVM. Call sites must already have performed any on-disk format-mismatch
     * check (or will rely on the implementation's own validation in
     * {@link KeyToPageIndex#start}).
     */
    public static KeyToPageIndex create(String tableSpace, String tableName,
                                        MemoryManager memoryManager,
                                        DataStorageManager dataStorageManager) {
        Mode mode = KeyToPageIndexMode.getResolved();
        if (mode == Mode.INCREMENTAL) {
            return new IncrementalBLinkKeyToPageIndex(tableSpace, tableName,
                    memoryManager, dataStorageManager);
        }
        return new BLinkKeyToPageIndex(tableSpace, tableName, memoryManager, dataStorageManager);
    }
}
