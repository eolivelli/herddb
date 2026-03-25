package herddb.vectortesting;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class MetricsCollector {

    // Reservoir sampling: keep at most MAX_SAMPLES latency values so that memory
    // usage stays bounded even when ingesting hundreds of millions of rows.
    private static final int MAX_SAMPLES = 100_000;

    private final LongAdder count = new LongAdder();
    private final long[] reservoir = new long[MAX_SAMPLES];
    private final AtomicLong samplesStored = new AtomicLong(0);

    public void record(long nanos) {
        long n = count.longValue() + 1; // approximate position before increment
        count.increment();
        long stored = samplesStored.get();
        if (stored < MAX_SAMPLES) {
            // Fill reservoir sequentially until full
            long idx = samplesStored.getAndIncrement(); // may race, but bounded by MAX_SAMPLES
            if (idx < MAX_SAMPLES) {
                reservoir[(int) idx] = nanos;
            }
        } else {
            // Reservoir sampling: replace a random slot with probability MAX_SAMPLES/n
            long slot = ThreadLocalRandom.current().nextLong(n);
            if (slot < MAX_SAMPLES) {
                reservoir[(int) slot] = nanos;
            }
        }
    }

    public long getCount() {
        return count.sum();
    }

    public Stats computeStats() {
        int size = (int) Math.min(samplesStored.get(), MAX_SAMPLES);
        if (size == 0) {
            return new Stats(0, 0, 0, 0, 0, 0);
        }
        long[] sorted = Arrays.copyOf(reservoir, size);
        Arrays.sort(sorted);
        double mean = Arrays.stream(sorted).average().orElse(0);
        return new Stats(
                count.sum(),
                mean,
                sorted[percentileIndex(size, 50)],
                sorted[percentileIndex(size, 95)],
                sorted[percentileIndex(size, 99)],
                sorted[size - 1]
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
