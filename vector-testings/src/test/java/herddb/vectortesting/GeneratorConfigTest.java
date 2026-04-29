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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class GeneratorConfigTest {

    @Test
    void nullCsvDefaultsToTotalSingleton() {
        long[] result = GeneratorConfig.parseCheckpoints(null, 1_000_000, 1000);
        assertArrayEquals(new long[]{1_000_000L}, result);
    }

    @Test
    void emptyCsvDefaultsToTotalSingleton() {
        long[] result = GeneratorConfig.parseCheckpoints("   ", 1_000_000, 1000);
        assertArrayEquals(new long[]{1_000_000L}, result);
    }

    @Test
    void csvIsSortedAndAlwaysIncludesTotal() {
        long[] result = GeneratorConfig.parseCheckpoints("100000,5000,20000", 200_000, 1000);
        assertArrayEquals(new long[]{5000L, 20_000L, 100_000L, 200_000L}, result);
    }

    @Test
    void totalAlreadyInCsvIsNotDuplicated() {
        long[] result = GeneratorConfig.parseCheckpoints("10000,200000", 200_000, 1000);
        assertArrayEquals(new long[]{10_000L, 200_000L}, result);
    }

    @Test
    void rejectsCheckpointAtOrBelowNumQueries() {
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.parseCheckpoints("1000,5000", 100_000, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.parseCheckpoints("500", 100_000, 1000));
    }

    @Test
    void rejectsCheckpointAboveTotal() {
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.parseCheckpoints("200000", 100_000, 1000));
    }

    @Test
    void rejectsDuplicateCheckpoints() {
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.parseCheckpoints("5000,5000", 100_000, 1000));
    }

    @Test
    void rejectsNonNumericTokens() {
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorConfig.parseCheckpoints("5000,abc", 100_000, 1000));
    }

    @Test
    void emptyTokensSilentlyIgnored() {
        // Trailing/leading commas are common when scripts assemble the value programmatically;
        // be lenient about those rather than failing on a typo nobody will spot.
        long[] result = GeneratorConfig.parseCheckpoints(",5000,,10000,", 100_000, 1000);
        assertArrayEquals(new long[]{5000L, 10_000L, 100_000L}, result);
    }
}
