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

package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import herddb.file.FileMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that IndexingServiceEngine resolves the cluster metadata directory
 * correctly from the server.metadata.dir configuration property.
 */
public class MetadataDirectoryResolutionTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private IndexingServiceEngine makeEngine(IndexingServerConfiguration config) {
        Path logDir = tmp.getRoot().toPath().resolve("log");
        Path dataDir = tmp.getRoot().toPath().resolve("data");
        return new IndexingServiceEngine(logDir, dataDir, config);
    }

    private MetadataStorageManager invokeBuilderMetadataStorageManager(IndexingServiceEngine engine) throws Exception {
        Method method = IndexingServiceEngine.class.getDeclaredMethod("buildMetadataStorageManager");
        method.setAccessible(true);
        return (MetadataStorageManager) method.invoke(engine);
    }

    private Path getBaseDirectory(FileMetadataStorageManager manager) throws Exception {
        Field field = FileMetadataStorageManager.class.getDeclaredField("baseDirectory");
        field.setAccessible(true);
        return (Path) field.get(manager);
    }

    @Test
    public void testDefaultMetadataDir() throws Exception {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        // server.mode defaults to standalone; server.metadata.dir defaults to "metadata"
        IndexingServiceEngine engine = makeEngine(config);

        MetadataStorageManager manager = invokeBuilderMetadataStorageManager(engine);

        assertTrue("Expected FileMetadataStorageManager for standalone mode",
                manager instanceof FileMetadataStorageManager);
        Path resolvedDir = getBaseDirectory((FileMetadataStorageManager) manager);
        assertTrue("Metadata dir must be absolute", resolvedDir.isAbsolute());
        assertEquals("Default metadata dir must end with 'metadata'",
                "metadata", resolvedDir.getFileName().toString());
    }

    @Test
    public void testExplicitMetadataDir() throws Exception {
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_METADATA_DIR, "dbdata/metadata");

        IndexingServiceEngine engine = makeEngine(config);

        MetadataStorageManager manager = invokeBuilderMetadataStorageManager(engine);

        assertTrue("Expected FileMetadataStorageManager for standalone mode",
                manager instanceof FileMetadataStorageManager);
        Path resolvedDir = getBaseDirectory((FileMetadataStorageManager) manager);
        assertTrue("Metadata dir must be absolute", resolvedDir.isAbsolute());
        assertEquals("Metadata dir parent must be 'dbdata'",
                "dbdata", resolvedDir.getParent().getFileName().toString());
        assertEquals("Metadata dir must end with 'metadata'",
                "metadata", resolvedDir.getFileName().toString());
    }

    @Test
    public void testAbsoluteMetadataDir() throws Exception {
        Path absolutePath = Paths.get(System.getProperty("java.io.tmpdir"), "herddb-test-metadata");
        IndexingServerConfiguration config = new IndexingServerConfiguration();
        config.set(IndexingServerConfiguration.PROPERTY_METADATA_DIR, absolutePath.toString());

        IndexingServiceEngine engine = makeEngine(config);

        MetadataStorageManager manager = invokeBuilderMetadataStorageManager(engine);

        assertTrue("Expected FileMetadataStorageManager for standalone mode",
                manager instanceof FileMetadataStorageManager);
        Path resolvedDir = getBaseDirectory((FileMetadataStorageManager) manager);
        assertEquals("Absolute path must be preserved exactly", absolutePath, resolvedDir);
    }
}
