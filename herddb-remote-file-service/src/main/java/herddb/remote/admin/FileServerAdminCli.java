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

package herddb.remote.admin;

import herddb.remote.proto.GetServerInfoResponse;
import herddb.remote.proto.ResizeDiskCacheResponse;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Diagnostic and management CLI for a HerdDB file-server instance (issue #336).
 *
 * <p>Usage:
 * <pre>
 *   fileserver-admin &lt;command&gt; [options]
 *
 *   Commands:
 *     server-info    Print identity, JVM, disk-cache and block-cache stats
 *     resize-cache   Dynamically resize the disk-cache LRU (non-persistent)
 * </pre>
 *
 * <p>All RPCs target one file-server pod selected via {@code --server host:port}.
 *
 * @author enrico.olivelli
 */
public final class FileServerAdminCli {

    static final String COMMAND_SERVER_INFO = "server-info";
    static final String COMMAND_RESIZE_CACHE = "resize-cache";

    private final PrintStream out;
    private final PrintStream err;

    public FileServerAdminCli(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    public static void main(String[] args) {
        int rc = new FileServerAdminCli(System.out, System.err).run(args);
        System.exit(rc);
    }

    /**
     * Entry point used by {@link #main(String[])} and by tests.
     * Returns 0 on success, 1 on command failure, 2 on bad usage.
     */
    public int run(String[] args) {
        if (args.length == 0 || "-h".equals(args[0]) || "--help".equals(args[0])) {
            printUsage();
            return args.length == 0 ? 2 : 0;
        }
        String command = args[0];
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        try {
            switch (command) {
                case COMMAND_SERVER_INFO:
                    return runServerInfo(rest);
                case COMMAND_RESIZE_CACHE:
                    return runResizeCache(rest);
                default:
                    err.println("Unknown command: " + command);
                    printUsage();
                    return 2;
            }
        } catch (ParseException e) {
            err.println("Invalid arguments: " + e.getMessage());
            return 2;
        } catch (Exception e) {
            // Broad catch: top-level CLI entry point; any unhandled exception must map to
            // a non-zero exit code rather than an unformatted stack trace on stderr.
            err.println("ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return 1;
        }
    }

    private void printUsage() {
        out.println("Usage: fileserver-admin <command> [options]");
        out.println();
        out.println("Commands:");
        out.println("  server-info    Print identity, JVM, disk-cache and block-cache stats");
        out.println("  resize-cache   Dynamically resize the disk-cache LRU (non-persistent)");
        out.println();
        out.println("Run 'fileserver-admin <command> --help' for command-specific flags.");
    }

    // ---------------------------------------------------------------
    // Command implementations
    // ---------------------------------------------------------------

    private int runServerInfo(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);

        CommandLine cli = parse(opts, args, COMMAND_SERVER_INFO);
        if (cli == null) {
            return 0;
        }
        try (FileServerAdminClient client = buildClient(cli)) {
            GetServerInfoResponse resp = client.getServerInfo();
            if (cli.hasOption("json")) {
                out.println(toJson(serverInfoToMap(resp)));
            } else {
                printServerInfoText(resp);
            }
        }
        return 0;
    }

    private int runResizeCache(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);
        opts.addOption(Option.builder().longOpt("max-bytes").hasArg().argName("BYTES")
                .desc("new maximum size for the disk-cache LRU in bytes (required)").required().build());

        CommandLine cli = parse(opts, args, COMMAND_RESIZE_CACHE);
        if (cli == null) {
            return 0;
        }
        long newMax = Long.parseLong(cli.getOptionValue("max-bytes"));
        try (FileServerAdminClient client = buildClient(cli)) {
            ResizeDiskCacheResponse resp = client.resizeDiskCache(newMax);
            if (cli.hasOption("json")) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("previous_max_bytes", resp.getPreviousMaxBytes());
                m.put("new_max_bytes", resp.getNewMaxBytes());
                out.println(toJson(m));
            } else {
                out.printf(Locale.ROOT,
                        "disk cache resized: previous=%d bytes (%d MiB), new=%d bytes (%d MiB)%n",
                        resp.getPreviousMaxBytes(), resp.getPreviousMaxBytes() / (1024 * 1024),
                        resp.getNewMaxBytes(), resp.getNewMaxBytes() / (1024 * 1024));
            }
        }
        return 0;
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private static void addCommonOptions(Options opts) {
        opts.addOption(Option.builder("h").longOpt("help")
                .desc("show help for this command").build());
        opts.addOption(Option.builder().longOpt("json")
                .desc("emit JSON instead of plain text").build());
        opts.addOption(Option.builder().longOpt("timeout-seconds").hasArg().argName("SECS")
                .desc("gRPC call deadline in seconds (default 30)").build());
    }

    private static void addServerOption(Options opts) {
        opts.addOption(Option.builder().longOpt("server").hasArg().argName("HOST:PORT")
                .desc("file-server gRPC endpoint (required)").required().build());
    }

    private CommandLine parse(Options opts, String[] args, String commandName) throws ParseException {
        if (args.length == 1 && ("-h".equals(args[0]) || "--help".equals(args[0]))) {
            java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.OutputStreamWriter(out, java.nio.charset.StandardCharsets.UTF_8), true);
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(pw, 100, "fileserver-admin " + commandName + " [options]",
                    "", opts, 2, 4, "", true);
            pw.flush();
            return null;
        }
        CommandLineParser parser = new DefaultParser();
        return parser.parse(opts, args);
    }

    private FileServerAdminClient buildClient(CommandLine cli) {
        long timeout = Long.parseLong(cli.getOptionValue("timeout-seconds", "30"));
        return new FileServerAdminClient(cli.getOptionValue("server"), timeout);
    }

    private static Map<String, Object> serverInfoToMap(GetServerInfoResponse r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("grpc_host", r.getGrpcHost());
        m.put("grpc_port", r.getGrpcPort());
        m.put("storage_mode", r.getStorageMode());
        m.put("jvm_heap_used_bytes", r.getJvmHeapUsedBytes());
        m.put("jvm_heap_max_bytes", r.getJvmHeapMaxBytes());
        // disk cache
        m.put("disk_cache_max_bytes", r.getDiskCacheMaxBytes());
        m.put("disk_cache_hit_count", r.getDiskCacheHitCount());
        m.put("disk_cache_miss_count", r.getDiskCacheMissCount());
        m.put("disk_cache_eviction_count", r.getDiskCacheEvictionCount());
        m.put("disk_cache_hit_bytes", r.getDiskCacheHitBytes());
        m.put("disk_cache_miss_bytes", r.getDiskCacheMissBytes());
        m.put("disk_cache_estimated_entries", r.getDiskCacheEstimatedEntries());
        // block cache
        m.put("block_cache_max_bytes", r.getBlockCacheMaxBytes());
        m.put("block_cache_estimated_bytes", r.getBlockCacheEstimatedBytes());
        m.put("block_cache_estimated_entries", r.getBlockCacheEstimatedEntries());
        m.put("block_cache_hits", r.getBlockCacheHits());
        m.put("block_cache_misses", r.getBlockCacheMisses());
        m.put("block_cache_evictions", r.getBlockCacheEvictions());
        return m;
    }

    private void printServerInfoText(GetServerInfoResponse r) {
        out.println("Server info:");
        out.printf(Locale.ROOT, "  grpc_host          = %s%n", r.getGrpcHost());
        out.printf(Locale.ROOT, "  grpc_port          = %d%n", r.getGrpcPort());
        out.printf(Locale.ROOT, "  storage_mode       = %s%n", r.getStorageMode());
        long heapMiB = r.getJvmHeapMaxBytes() / (1024 * 1024);
        long heapUsedMiB = r.getJvmHeapUsedBytes() / (1024 * 1024);
        out.printf(Locale.ROOT, "  jvm_heap           = %d / %d MiB%n", heapUsedMiB, heapMiB);
        out.println();
        out.println("Disk cache (s3 disk-cache LRU):");
        if (r.getDiskCacheMaxBytes() == 0) {
            out.println("  (not available — storage.mode is not s3)");
        } else {
            long maxMiB = r.getDiskCacheMaxBytes() / (1024 * 1024);
            long hitTotal = r.getDiskCacheHitCount() + r.getDiskCacheMissCount();
            double hitRatio = hitTotal > 0
                    ? (100.0 * r.getDiskCacheHitCount() / hitTotal)
                    : 0.0;
            out.printf(Locale.ROOT, "  max_bytes          = %d (%d MiB)%n",
                    r.getDiskCacheMaxBytes(), maxMiB);
            out.printf(Locale.ROOT, "  estimated_entries  = %d%n", r.getDiskCacheEstimatedEntries());
            out.printf(Locale.ROOT, "  hit_count          = %d%n", r.getDiskCacheHitCount());
            out.printf(Locale.ROOT, "  miss_count         = %d%n", r.getDiskCacheMissCount());
            out.printf(Locale.ROOT, "  hit_ratio          = %.1f%%%n", hitRatio);
            out.printf(Locale.ROOT, "  eviction_count     = %d%n", r.getDiskCacheEvictionCount());
            out.printf(Locale.ROOT, "  hit_bytes          = %d (%d MiB)%n",
                    r.getDiskCacheHitBytes(), r.getDiskCacheHitBytes() / (1024 * 1024));
            out.printf(Locale.ROOT, "  miss_bytes         = %d (%d MiB)%n",
                    r.getDiskCacheMissBytes(), r.getDiskCacheMissBytes() / (1024 * 1024));
        }
        out.println();
        out.println("Block cache (in-heap block LRU):");
        if (r.getBlockCacheMaxBytes() == 0) {
            out.println("  (not available — block.cache.enabled=false)");
        } else {
            long bcMaxMiB = r.getBlockCacheMaxBytes() / (1024 * 1024);
            long bcSizeMiB = r.getBlockCacheEstimatedBytes() / (1024 * 1024);
            long bcHitTotal = r.getBlockCacheHits() + r.getBlockCacheMisses();
            double bcHitRatio = bcHitTotal > 0
                    ? (100.0 * r.getBlockCacheHits() / bcHitTotal)
                    : 0.0;
            out.printf(Locale.ROOT, "  max_bytes          = %d (%d MiB)%n",
                    r.getBlockCacheMaxBytes(), bcMaxMiB);
            out.printf(Locale.ROOT, "  estimated_bytes    = %d (%d MiB)%n",
                    r.getBlockCacheEstimatedBytes(), bcSizeMiB);
            out.printf(Locale.ROOT, "  estimated_entries  = %d%n", r.getBlockCacheEstimatedEntries());
            out.printf(Locale.ROOT, "  hits               = %d%n", r.getBlockCacheHits());
            out.printf(Locale.ROOT, "  misses             = %d%n", r.getBlockCacheMisses());
            out.printf(Locale.ROOT, "  hit_ratio          = %.1f%%%n", bcHitRatio);
            out.printf(Locale.ROOT, "  evictions          = %d%n", r.getBlockCacheEvictions());
        }
    }

    // ---------------------------------------------------------------
    // Minimal JSON serialiser (no external deps)
    // ---------------------------------------------------------------

    /**
     * Converts a {@code Map<String, Object>} to a compact JSON string.
     * Supports Long, Integer, Double, Boolean, String, and null values.
     */
    static String toJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(escapeJson(entry.getKey())).append('"').append(':');
            appendJsonValue(sb, entry.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    private static void appendJsonValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String) {
            sb.append('"').append(escapeJson((String) value)).append('"');
        } else if (value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof Number) {
            sb.append(value);
        } else {
            sb.append('"').append(escapeJson(value.toString())).append('"');
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
