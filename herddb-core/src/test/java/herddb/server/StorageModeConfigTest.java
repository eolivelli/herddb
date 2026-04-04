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

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class StorageModeConfigTest {

    @Test
    public void testStorageModeConstants() {
        assertEquals("local", ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL);
        assertEquals("remote", ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
        assertEquals("bookkeeper", ServerConfiguration.PROPERTY_STORAGE_MODE_BOOKKEEPER);
        assertEquals("server.storage.mode", ServerConfiguration.PROPERTY_STORAGE_MODE);
    }

    @Test
    public void testDefaultStorageModeForStandalone() {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        String storageMode = config.getString(
            ServerConfiguration.PROPERTY_STORAGE_MODE,
            ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL);
        assertEquals(ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL, storageMode);
    }

    @Test
    public void testDefaultStorageModeForCluster() {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        String storageMode = config.getString(
            ServerConfiguration.PROPERTY_STORAGE_MODE,
            ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL);
        assertEquals(ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL, storageMode);
    }

    @Test
    public void testExplicitRemoteStorageMode() {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        config.set(ServerConfiguration.PROPERTY_STORAGE_MODE, ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE);
        String storageMode = config.getString(
            ServerConfiguration.PROPERTY_STORAGE_MODE,
            ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL);
        assertEquals(ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE, storageMode);
    }
}
