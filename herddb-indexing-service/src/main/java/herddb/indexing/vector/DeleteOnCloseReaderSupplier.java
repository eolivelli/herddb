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

package herddb.indexing.vector;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link ReaderSupplier} wrapper that deletes a temporary file when the supplier
 * is closed. Used by {@link SegmentPQReaderSupplier} to ensure that segment graph
 * files downloaded to local disk for bulk PQ-retraining vector extraction
 * (issue #599 Option B) are cleaned up after use.
 *
 * <p>Typical lifecycle:
 * <ol>
 *   <li>Download the segment graph file to {@code tempFile} via
 *       {@link herddb.storage.DataStorageManager#downloadMultipartIndexFile}.</li>
 *   <li>Wrap the mmap-backed {@link ReaderSupplier} with a
 *       {@code DeleteOnCloseReaderSupplier(delegate, tempFile)}.</li>
 *   <li>Pass the wrapper to {@link io.github.jbellis.jvector.graph.disk.PQRetrainer}
 *       via the reader-supplier factory.</li>
 *   <li>After all vectors for that source are extracted, {@code PQRetrainer} closes
 *       the view ({@link RandomAccessReader#close()}) and then calls
 *       {@link #close()} on this supplier, which closes the mmap and deletes
 *       {@code tempFile}.</li>
 * </ol>
 */
final class DeleteOnCloseReaderSupplier implements ReaderSupplier {

    private static final Logger LOGGER = Logger.getLogger(DeleteOnCloseReaderSupplier.class.getName());

    private final ReaderSupplier delegate;
    private final Path tempFile;

    /**
     * @param delegate the mmap-backed (or otherwise) supplier to delegate reads to
     * @param tempFile the temp file to delete when this supplier is closed
     */
    DeleteOnCloseReaderSupplier(ReaderSupplier delegate, Path tempFile) {
        this.delegate = delegate;
        this.tempFile = tempFile;
    }

    @Override
    public RandomAccessReader get() throws IOException {
        return delegate.get();
    }

    /**
     * Closes the backing {@link ReaderSupplier} (releasing any memory-mapped
     * resources) and then deletes {@link #tempFile}.
     *
     * <p>Any {@link IOException} from {@code delegate.close()} is logged at
     * {@code WARNING} level but does not suppress the file-deletion step.
     * Any {@link IOException} from {@link Files#deleteIfExists} is logged at
     * {@code WARNING} level; it is not re-thrown because by this point the
     * data has been fully consumed and a residual temp file, while wasteful,
     * is not a correctness error.
     */
    @Override
    public void close() throws IOException {
        IOException delegateException = null;
        try {
            delegate.close();
        } catch (IOException e) {
            delegateException = e;
            LOGGER.log(Level.WARNING, "Failed to close delegate reader supplier for temp file {0}", tempFile);
        }
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete temp segment file {0}", tempFile);
        }
        if (delegateException != null) {
            throw delegateException;
        }
    }
}
