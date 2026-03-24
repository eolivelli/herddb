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

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

public class MetricsCollector {

    private final LongAdder count = new LongAdder();
    private final ConcurrentLinkedQueue<Long> latenciesNanos = new ConcurrentLinkedQueue<>();

    public void record(long nanos) {
        count.increment();
        latenciesNanos.add(nanos);
    }

    public long getCount() {
        return count.sum();
    }

    public Stats computeStats() {
        long[] sorted = latenciesNanos.stream().mapToLong(Long::longValue).sorted().toArray();
        if (sorted.length == 0) {
            return new Stats(0, 0, 0, 0, 0, 0);
        }
        double mean = Arrays.stream(sorted).average().orElse(0);
        return new Stats(
                sorted.length,
                mean,
                sorted[percentileIndex(sorted.length, 50)],
                sorted[percentileIndex(sorted.length, 95)],
                sorted[percentileIndex(sorted.length, 99)],
                sorted[sorted.length - 1]
        );
    }

    private static int percentileIndex(int size, int percentile) {
        return Math.min(size - 1, (int) ((long) size * percentile / 100.0));
    }

    public record Stats(long count, double meanNanos, long p50Nanos, long p95Nanos, long p99Nanos, long maxNanos) {

        public void print(String label) {
            if (count == 0) {
                System.out.println("=== " + label + " ===");
                System.out.println("No data recorded.");
                return;
            }
            System.out.println("=== " + label + " ===");
            System.out.printf("Count: %d%n", count);
            System.out.printf("Latency mean: %.2f ms | p50: %.2f ms | p95: %.2f ms | p99: %.2f ms | max: %.2f ms%n",
                    meanNanos / 1_000_000.0,
                    p50Nanos / 1_000_000.0,
                    p95Nanos / 1_000_000.0,
                    p99Nanos / 1_000_000.0,
                    maxNanos / 1_000_000.0);
        }
    }
}
