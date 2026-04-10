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

package herddb.server;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Minimal view of the shared checkpoint metadata manager exposed to modules
 * that cannot take a compile-time dependency on
 * {@code herddb-remote-file-service}.
 */
public interface SharedCheckpointMetadata {

    /**
     * Downloads the checkpoint metadata files for the given tableSpace from
     * shared storage into {@code localMetadataDir}, so the local
     * {@code FileDataStorageManager} can read them as if they had always been
     * on disk.
     *
     * @return number of files hydrated
     */
    int hydrateLocalMetadataDir(Path localMetadataDir, String tableSpace) throws IOException;
}
