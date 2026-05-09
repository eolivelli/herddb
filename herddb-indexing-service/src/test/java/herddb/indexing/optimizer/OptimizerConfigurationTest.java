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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.util.Properties;
import org.junit.Test;

/**
 * Unit tests for {@link OptimizerConfiguration} — focusing on the {@code getLong}
 * and {@code getInt} methods that must handle both plain decimal integers and
 * scientific-notation strings emitted by the Go YAML parser / Helm for large
 * integer literals (e.g. {@code "2.68435456e+08"} for 268435456).
 *
 * <p>Issue #480: {@code Long.parseLong} and {@code Integer.parseInt} reject
 * scientific-notation inputs, crashing the optimizer pod at startup when
 * {@code minBytes}/{@code maxBytes} were rendered in that form by Helm.
 */
public class OptimizerConfigurationTest {

    // -------------------------------------------------------------------------
    // getLong
    // -------------------------------------------------------------------------

    @Test
    public void getLongReturnsDefaultWhenKeyAbsent() {
        OptimizerConfiguration cfg = new OptimizerConfiguration(new Properties());
        assertEquals(42L, cfg.getLong("missing.key", 42L));
    }

    @Test
    public void getLongParsesPlainDecimal() {
        Properties p = new Properties();
        p.setProperty("k", "268435456");
        assertEquals(268435456L, new OptimizerConfiguration(p).getLong("k", 0L));
    }

    @Test
    public void getLongParsesMiniBytes_scientificNotation() {
        // Helm emits 268435456 as "2.68435456e+08" when values.yaml carries an
        // unquoted YAML integer that Go's parser rounds to float64.
        Properties p = new Properties();
        p.setProperty("indexoptimizer.merge.min.bytes", "2.68435456e+08");
        assertEquals(268435456L,
                new OptimizerConfiguration(p).getLong(
                        OptimizerConfiguration.PROPERTY_MIN_BYTES, 0L));
    }

    @Test
    public void getLongParsesMaxBytes_scientificNotation() {
        Properties p = new Properties();
        p.setProperty("indexoptimizer.merge.max.bytes", "1.073741824e+09");
        assertEquals(1073741824L,
                new OptimizerConfiguration(p).getLong(
                        OptimizerConfiguration.PROPERTY_MAX_BYTES, 0L));
    }

    @Test
    public void getLongParsesLargeValue() {
        Properties p = new Properties();
        p.setProperty("k", "10737418240"); // 10 GiB
        assertEquals(10737418240L, new OptimizerConfiguration(p).getLong("k", 0L));
    }

    @Test
    public void getLongThrowsOnFractionalValue() {
        // A genuinely fractional value is malformed for a byte count.
        // BigDecimal.longValueExact() must reject it.
        // Note: "1.5e+08" equals 150000000 exactly (no fractional part) so use
        // "2.555e+02" = 255.5 which cannot be represented as a long.
        Properties p = new Properties();
        p.setProperty("k", "2.555e+02");
        try {
            new OptimizerConfiguration(p).getLong("k", 0L);
            fail("expected ArithmeticException for fractional value");
        } catch (ArithmeticException ok) {
            // expected
        }
    }

    // -------------------------------------------------------------------------
    // getInt
    // -------------------------------------------------------------------------

    @Test
    public void getIntReturnsDefaultWhenKeyAbsent() {
        assertEquals(7, new OptimizerConfiguration(new Properties()).getInt("missing", 7));
    }

    @Test
    public void getIntParsesPlainDecimal() {
        Properties p = new Properties();
        p.setProperty("k", "200");
        assertEquals(200, new OptimizerConfiguration(p).getInt("k", 0));
    }

    @Test
    public void getIntParsesScientificNotation() {
        // e.g. Helm could emit httpPort or count as scientific notation
        Properties p = new Properties();
        p.setProperty("k", "4e+01"); // 40
        assertEquals(40, new OptimizerConfiguration(p).getInt("k", 0));
    }

    @Test
    public void getIntThrowsOnFractionalValue() {
        Properties p = new Properties();
        p.setProperty("k", "4.5");
        try {
            new OptimizerConfiguration(p).getInt("k", 0);
            fail("expected ArithmeticException for fractional value");
        } catch (ArithmeticException ok) {
            // expected
        }
    }

    @Test
    public void getIntThrowsOnValueExceedingIntRange() {
        Properties p = new Properties();
        p.setProperty("k", "3000000000"); // > Integer.MAX_VALUE
        try {
            new OptimizerConfiguration(p).getInt("k", 0);
            fail("expected ArithmeticException for out-of-range value");
        } catch (ArithmeticException ok) {
            // expected
        }
    }
}
