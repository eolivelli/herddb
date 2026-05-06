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
package herddb.vectortesting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Issue #401: parse-time and runtime validation for the new
 * {@code --transaction-size} option and the joint invariants between
 * {@code --batch-size}, {@code --transaction-size} and {@code --ingest-max-ops}.
 */
class ConfigTransactionSizeTest {

    @Test
    void defaultEffectiveTransactionSizeEqualsBatchSize() throws ParseException {
        Config cfg = Config.parse(new String[]{});
        assertEquals(0, cfg.transactionSize, "transaction-size defaults to 0 (track batch-size)");
        assertEquals(cfg.batchSize, cfg.effectiveTransactionSize(),
                "effectiveTransactionSize must equal batch-size when transactionSize is 0");
    }

    @Test
    void parseAcceptsTransactionSizeAboveBatchSize() throws ParseException {
        Config cfg = Config.parse(new String[]{
                "--batch-size", "250",
                "--transaction-size", "1000"
        });
        assertEquals(250, cfg.batchSize);
        assertEquals(1000, cfg.transactionSize);
        assertEquals(1000, cfg.effectiveTransactionSize());
    }

    @Test
    void parseAcceptsTransactionSizeEqualToBatchSize() throws ParseException {
        Config cfg = Config.parse(new String[]{
                "--batch-size", "500",
                "--transaction-size", "500"
        });
        assertEquals(500, cfg.batchSize);
        assertEquals(500, cfg.transactionSize);
    }

    @Test
    void parseRejectsTransactionSizeBelowBatchSize() {
        ParseException ex = assertThrows(ParseException.class,
                () -> Config.parse(new String[]{
                        "--batch-size", "500",
                        "--transaction-size", "100"
                }));
        assertTrue(ex.getMessage().contains("transaction-size"),
                "diagnostic should name transaction-size; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("batch-size"),
                "diagnostic should name batch-size; got: " + ex.getMessage());
    }

    @Test
    void parseRejectsBatchSizeAboveIngestMaxOps() {
        ParseException ex = assertThrows(ParseException.class,
                () -> Config.parse(new String[]{
                        "--batch-size", "10000",
                        "--ingest-max-ops", "5000"
                }));
        assertTrue(ex.getMessage().contains("ingest-max-ops"),
                "diagnostic should name ingest-max-ops; got: " + ex.getMessage());
    }

    @Test
    void parseRejectsTransactionSizeAboveIngestMaxOps() {
        ParseException ex = assertThrows(ParseException.class,
                () -> Config.parse(new String[]{
                        "--batch-size", "100",
                        "--transaction-size", "10000",
                        "--ingest-max-ops", "5000"
                }));
        assertTrue(ex.getMessage().contains("ingest-max-ops"),
                "diagnostic should name ingest-max-ops; got: " + ex.getMessage());
    }

    @Test
    void parseAcceptsTransactionSizeWhenIngestMaxOpsUnlimited() throws ParseException {
        Config cfg = Config.parse(new String[]{
                "--batch-size", "100",
                "--transaction-size", "100000",
                "--ingest-max-ops", "0"
        });
        assertEquals(100, cfg.batchSize);
        assertEquals(100_000, cfg.transactionSize);
    }

    @Test
    void parseRejectsZeroBatchSize() {
        ParseException ex = assertThrows(ParseException.class,
                () -> Config.parse(new String[]{"--batch-size", "0"}));
        assertTrue(ex.getMessage().contains("batch-size"),
                "diagnostic should name batch-size; got: " + ex.getMessage());
    }

    @Test
    void propertiesFileTransactionSizeIsParsed(@TempDir Path tmp) throws IOException, ParseException {
        Path props = tmp.resolve("vb.properties");
        Files.writeString(props, "batch-size=200\ntransaction-size=2000\n");
        Config cfg = Config.parse(new String[]{"--config", props.toString()});
        assertEquals(200, cfg.batchSize);
        assertEquals(2000, cfg.transactionSize);
        assertEquals(2000, cfg.effectiveTransactionSize());
    }

    @Test
    void cliOverridesPropertiesFile(@TempDir Path tmp) throws IOException, ParseException {
        Path props = tmp.resolve("vb.properties");
        Files.writeString(props, "batch-size=200\ntransaction-size=2000\n");
        Config cfg = Config.parse(new String[]{
                "--config", props.toString(),
                "--transaction-size", "5000"
        });
        assertEquals(200, cfg.batchSize);
        assertEquals(5000, cfg.transactionSize);
    }
}
