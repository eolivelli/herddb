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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that BLink directories for merged/discarded segments are properly
 * cleaned up from disk.
 */
public class PersistentVectorStoreBLinkCleanupTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String FIXED_UUID = "testidx_testtable_fixed";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", TABLE_SPACE,
                "vector_col", FIXED_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private List<Path> findBLinkDirectories(Path baseDir) throws IOException {
        Path tablespaceDir = baseDir.resolve(TABLE_SPACE + ".tablespace");
        List<Path> result = new ArrayList<>();
        if (!Files.exists(tablespaceDir)) {
            return result;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tablespaceDir,
                FIXED_UUID + "_seg*_pktonode.index")) {
            for (Path p : stream) {
                result.add(p);
            }
        }
        return result;
    }

    /**
     * After a FusedPQ checkpoint that merges old segments into new ones,
     * the BLink directories for the old (merged) segments must be deleted.
     */
    @Test
    public void testBLinkDirectoriesCleanedUpAfterMerge() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        int dim = 32;
        // Need >= 256 vectors for FusedPQ path which creates BLink per segment
        int vectorCount = 300;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            for (int i = 0; i < vectorCount; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();

            List<Path> blinkDirsAfterFirst = findBLinkDirectories(baseDir);
            assertFalse("Should have BLink directories after first checkpoint",
                    blinkDirsAfterFirst.isEmpty());

            // Record which BLink dirs exist now (these are the "old" segment dirs)
            List<String> oldDirNames = new ArrayList<>();
            for (Path p : blinkDirsAfterFirst) {
                oldDirNames.add(p.getFileName().toString());
            }

            // Add more vectors and checkpoint again -- old segments get merged
            for (int i = vectorCount; i < vectorCount * 2; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();

            List<Path> blinkDirsAfterSecond = findBLinkDirectories(baseDir);
            // New segments should have BLink dirs
            assertFalse("Should still have BLink directories for active segments",
                    blinkDirsAfterSecond.isEmpty());

            // Old segment BLink dirs should no longer exist
            for (String oldName : oldDirNames) {
                Path oldDir = baseDir.resolve(TABLE_SPACE + ".tablespace").resolve(oldName);
                assertFalse("Old segment BLink directory should have been cleaned up: " + oldName,
                        Files.exists(oldDir));
            }
        }
    }

    /**
     * When all vectors are deleted and a checkpoint runs, all segment BLink
     * directories must be removed.
     */
    @Test
    public void testBLinkDirectoriesCleanedUpWhenAllVectorsDeleted() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        int dim = 32;
        int vectorCount = 300;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            for (int i = 0; i < vectorCount; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();

            List<Path> blinkDirs = findBLinkDirectories(baseDir);
            assertFalse("Should have BLink directories after checkpoint", blinkDirs.isEmpty());

            // Delete all vectors
            for (int i = 0; i < vectorCount; i++) {
                store.removeVector(Bytes.from_int(i));
            }
            store.checkpoint();

            // All BLink directories should be gone
            List<Path> blinkDirsAfterDelete = findBLinkDirectories(baseDir);
            assertTrue("All BLink directories should be cleaned up after deleting all vectors, "
                    + "but found: " + blinkDirsAfterDelete,
                    blinkDirsAfterDelete.isEmpty());
        }
    }
}
