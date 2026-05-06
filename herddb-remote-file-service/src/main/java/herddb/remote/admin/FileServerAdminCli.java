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

import herddb.proto.PduCodec;
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
            PduCodec.GetServerInfoResponse.Info resp = client.getServerInfo();
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
            FileServerAdminClient.ResizeResult resp = client.resizeDiskCache(newMax);
            if (cli.hasOption("json")) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("previous_max_bytes", resp.previousMaxBytes);
                m.put("new_max_bytes", resp.newMaxBytes);
                out.println(toJson(m));
            } else {
                out.printf(Locale.ROOT,
                        "disk cache resized: previous=%d bytes (%d MiB), new=%d bytes (%d MiB)%n",
                        resp.previousMaxBytes, resp.previousMaxBytes / (1024 * 1024),
                        resp.newMaxBytes, resp.newMaxBytes / (1024 * 1024));
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
                .desc("call deadline in seconds (default 30)").build());
    }

    private static void addServerOption(Options opts) {
        opts.addOption(Option.builder().longOpt("server").hasArg().argName("HOST:PORT")
                .desc("file-server endpoint (required)").required().build());
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

    private static Map<String, Object> serverInfoToMap(PduCodec.GetServerInfoResponse.Info r) {
        Map<String, Object> m = new LinkedHashMap<>();
        // The JSON keys are kept identical to the gRPC era ("grpc_host" /
        // "grpc_port") so existing tooling (dashboards, scripts) consuming
        // the JSON output of `fileserver-admin server-info --json` keeps
        // working unchanged. The wire is no longer gRPC, but the field
        // names are part of the CLI's stable contract.
        m.put("grpc_host", r.host);
        m.put("grpc_port", r.port);
        m.put("storage_mode", r.storageMode);
        m.put("jvm_heap_used_bytes", r.jvmHeapUsedBytes);
        m.put("jvm_heap_max_bytes", r.jvmHeapMaxBytes);
        // disk cache
        m.put("disk_cache_max_bytes", r.diskCacheMaxBytes);
        m.put("disk_cache_hit_count", r.diskCacheHitCount);
        m.put("disk_cache_miss_count", r.diskCacheMissCount);
        m.put("disk_cache_eviction_count", r.diskCacheEvictionCount);
        m.put("disk_cache_hit_bytes", r.diskCacheHitBytes);
        m.put("disk_cache_miss_bytes", r.diskCacheMissBytes);
        m.put("disk_cache_estimated_entries", r.diskCacheEstimatedEntries);
        // block cache
        m.put("block_cache_max_bytes", r.blockCacheMaxBytes);
        m.put("block_cache_estimated_bytes", r.blockCacheEstimatedBytes);
        m.put("block_cache_estimated_entries", r.blockCacheEstimatedEntries);
        m.put("block_cache_hits", r.blockCacheHits);
        m.put("block_cache_misses", r.blockCacheMisses);
        m.put("block_cache_evictions", r.blockCacheEvictions);
        return m;
    }

    private void printServerInfoText(PduCodec.GetServerInfoResponse.Info r) {
        out.println("Server info:");
        out.printf(Locale.ROOT, "  host               = %s%n", r.host);
        out.printf(Locale.ROOT, "  port               = %d%n", r.port);
        out.printf(Locale.ROOT, "  storage_mode       = %s%n", r.storageMode);
        long heapMiB = r.jvmHeapMaxBytes / (1024 * 1024);
        long heapUsedMiB = r.jvmHeapUsedBytes / (1024 * 1024);
        out.printf(Locale.ROOT, "  jvm_heap           = %d / %d MiB%n", heapUsedMiB, heapMiB);
        out.println();
        out.println("Disk cache (s3 disk-cache LRU):");
        if (r.diskCacheMaxBytes == 0) {
            out.println("  (not available — storage.mode is not s3)");
        } else {
            long maxMiB = r.diskCacheMaxBytes / (1024 * 1024);
            long hitTotal = r.diskCacheHitCount + r.diskCacheMissCount;
            double hitRatio = hitTotal > 0
                    ? (100.0 * r.diskCacheHitCount / hitTotal)
                    : 0.0;
            out.printf(Locale.ROOT, "  max_bytes          = %d (%d MiB)%n",
                    r.diskCacheMaxBytes, maxMiB);
            out.printf(Locale.ROOT, "  estimated_entries  = %d%n", r.diskCacheEstimatedEntries);
            out.printf(Locale.ROOT, "  hit_count          = %d%n", r.diskCacheHitCount);
            out.printf(Locale.ROOT, "  miss_count         = %d%n", r.diskCacheMissCount);
            out.printf(Locale.ROOT, "  hit_ratio          = %.1f%%%n", hitRatio);
            out.printf(Locale.ROOT, "  eviction_count     = %d%n", r.diskCacheEvictionCount);
            out.printf(Locale.ROOT, "  hit_bytes          = %d (%d MiB)%n",
                    r.diskCacheHitBytes, r.diskCacheHitBytes / (1024 * 1024));
            out.printf(Locale.ROOT, "  miss_bytes         = %d (%d MiB)%n",
                    r.diskCacheMissBytes, r.diskCacheMissBytes / (1024 * 1024));
        }
        out.println();
        out.println("Block cache (in-heap block LRU):");
        if (r.blockCacheMaxBytes == 0) {
            out.println("  (not available — block.cache.enabled=false)");
        } else {
            long bcMaxMiB = r.blockCacheMaxBytes / (1024 * 1024);
            long bcSizeMiB = r.blockCacheEstimatedBytes / (1024 * 1024);
            long bcHitTotal = r.blockCacheHits + r.blockCacheMisses;
            double bcHitRatio = bcHitTotal > 0
                    ? (100.0 * r.blockCacheHits / bcHitTotal)
                    : 0.0;
            out.printf(Locale.ROOT, "  max_bytes          = %d (%d MiB)%n",
                    r.blockCacheMaxBytes, bcMaxMiB);
            out.printf(Locale.ROOT, "  estimated_bytes    = %d (%d MiB)%n",
                    r.blockCacheEstimatedBytes, bcSizeMiB);
            out.printf(Locale.ROOT, "  estimated_entries  = %d%n", r.blockCacheEstimatedEntries);
            out.printf(Locale.ROOT, "  hits               = %d%n", r.blockCacheHits);
            out.printf(Locale.ROOT, "  misses             = %d%n", r.blockCacheMisses);
            out.printf(Locale.ROOT, "  hit_ratio          = %.1f%%%n", bcHitRatio);
            out.printf(Locale.ROOT, "  evictions          = %d%n", r.blockCacheEvictions);
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
