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

package herddb.indexing.admin;

import com.google.protobuf.ByteString;
import herddb.indexing.proto.DescribeIndexResponse;
import herddb.indexing.proto.GetEngineStatsResponse;
import herddb.indexing.proto.GetIndexStatusResponse;
import herddb.indexing.proto.GetInstanceInfoResponse;
import herddb.indexing.proto.IndexDescriptor;
import herddb.indexing.proto.ListIndexesResponse;
import herddb.indexing.proto.PrimaryKeysChunk;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
 * Diagnostic CLI for a HerdDB indexing service instance.
 *
 * <p>Usage:
 * <pre>
 *   indexing-admin &lt;command&gt; [options]
 *
 *   Commands:
 *     list-instances   Read ZooKeeper and print registered indexing services
 *     list-indexes     Enumerate indexes loaded by one instance
 *     describe-index   Print detailed state of a single index
 *     status           Short single-line GetIndexStatus wrapper
 *     list-pks         Stream the list of primary keys for an index
 *     engine-stats     Tailer / apply / memory stats of one instance
 *     instance-info    Config/identity/heap snapshot of one instance
 * </pre>
 *
 * <p>All RPCs (except {@code list-instances}) target exactly one indexing
 * service instance selected via {@code --server host:port}. The
 * {@code list-instances} command reads ZooKeeper directly via
 * {@link ZkInstanceDiscovery}.
 */
public final class IndexingAdminCli {

    static final String COMMAND_LIST_INSTANCES = "list-instances";
    static final String COMMAND_LIST_INDEXES = "list-indexes";
    static final String COMMAND_DESCRIBE_INDEX = "describe-index";
    static final String COMMAND_STATUS = "status";
    static final String COMMAND_LIST_PKS = "list-pks";
    static final String COMMAND_ENGINE_STATS = "engine-stats";
    static final String COMMAND_INSTANCE_INFO = "instance-info";

    private final PrintStream out;
    private final PrintStream err;

    public IndexingAdminCli(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    public static void main(String[] args) {
        int rc = new IndexingAdminCli(System.out, System.err).run(args);
        System.exit(rc);
    }

    /**
     * Entry point used by {@link #main(String[])} and by tests. Returns a
     * process exit code: 0 = success, 1 = command failed, 2 = bad usage.
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
                case COMMAND_LIST_INSTANCES:
                    return runListInstances(rest);
                case COMMAND_LIST_INDEXES:
                    return runListIndexes(rest);
                case COMMAND_DESCRIBE_INDEX:
                    return runDescribeIndex(rest);
                case COMMAND_STATUS:
                    return runStatus(rest);
                case COMMAND_LIST_PKS:
                    return runListPks(rest);
                case COMMAND_ENGINE_STATS:
                    return runEngineStats(rest);
                case COMMAND_INSTANCE_INFO:
                    return runInstanceInfo(rest);
                default:
                    err.println("Unknown command: " + command);
                    printUsage();
                    return 2;
            }
        } catch (ParseException e) {
            err.println("Invalid arguments: " + e.getMessage());
            return 2;
        } catch (RuntimeException e) {
            err.println("ERROR: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err.println("ERROR: " + e.getMessage());
            return 1;
        }
    }

    private void printUsage() {
        out.println("Usage: indexing-admin <command> [options]");
        out.println();
        out.println("Commands:");
        out.println("  list-instances   Read ZooKeeper and print registered indexing services");
        out.println("  list-indexes     Enumerate indexes loaded by one instance");
        out.println("  describe-index   Print detailed state of a single index");
        out.println("  status           Short one-line GetIndexStatus wrapper");
        out.println("  list-pks         Stream the list of primary keys for an index");
        out.println("  engine-stats     Tailer / apply / memory stats of one instance");
        out.println("  instance-info    Config / identity / heap snapshot of one instance");
        out.println();
        out.println("Run 'indexing-admin <command> --help' for command-specific flags.");
    }

    // ---------------------------------------------------------------
    // Command implementations
    // ---------------------------------------------------------------

    private int runListInstances(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        opts.addOption(Option.builder().longOpt("zookeeper").hasArg().argName("HOST:PORT")
                .desc("ZooKeeper connect string (required)").required().build());
        opts.addOption(Option.builder().longOpt("zk-path").hasArg().argName("PATH")
                .desc("ZooKeeper base path (default /herddb)").build());
        opts.addOption(Option.builder().longOpt("zk-session-timeout-ms").hasArg().argName("MS")
                .desc("ZooKeeper session timeout (default 10000)").build());

        CommandLine cli = parse(opts, args, COMMAND_LIST_INSTANCES);
        if (cli == null) {
            return 0;
        }
        String zk = cli.getOptionValue("zookeeper");
        String zkPath = cli.getOptionValue("zk-path", "/herddb");
        int sessionTimeout = Integer.parseInt(cli.getOptionValue("zk-session-timeout-ms", "10000"));

        List<String> instances = ZkInstanceDiscovery.listInstances(zk, zkPath, sessionTimeout);

        if (cli.hasOption("json")) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("zookeeper", zk);
            payload.put("zk_path", zkPath);
            payload.put("instances", instances);
            out.println(JsonWriter.toJson(payload));
        } else {
            if (instances.isEmpty()) {
                out.println("(no indexing service instances registered at " + zkPath + ")");
            } else {
                out.println("Registered indexing services (" + instances.size() + "):");
                for (String inst : instances) {
                    out.println("  " + inst);
                }
            }
        }
        return 0;
    }

    private int runListIndexes(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);

        CommandLine cli = parse(opts, args, COMMAND_LIST_INDEXES);
        if (cli == null) {
            return 0;
        }
        try (IndexingAdminClient client = buildClient(cli)) {
            ListIndexesResponse response = client.listIndexes();
            if (cli.hasOption("json")) {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (IndexDescriptor d : response.getIndexesList()) {
                    rows.add(indexDescriptorToMap(d));
                }
                out.println(JsonWriter.toJson(rows));
            } else {
                if (response.getIndexesCount() == 0) {
                    out.println("(no indexes loaded on this instance)");
                } else {
                    out.printf(Locale.ROOT, "%-20s %-20s %-20s %12s %-10s%n",
                            "TABLESPACE", "TABLE", "INDEX", "VECTORS", "STATUS");
                    for (IndexDescriptor d : response.getIndexesList()) {
                        out.printf(Locale.ROOT, "%-20s %-20s %-20s %12d %-10s%n",
                                d.getTablespace(), d.getTable(), d.getIndex(),
                                d.getVectorCount(), d.getStatus());
                    }
                }
            }
        }
        return 0;
    }

    private int runDescribeIndex(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);
        addIndexOptions(opts);

        CommandLine cli = parse(opts, args, COMMAND_DESCRIBE_INDEX);
        if (cli == null) {
            return 0;
        }
        try (IndexingAdminClient client = buildClient(cli)) {
            DescribeIndexResponse response = client.describeIndex(
                    cli.getOptionValue("tablespace"),
                    cli.getOptionValue("table"),
                    cli.getOptionValue("index"));
            if (cli.hasOption("json")) {
                out.println(JsonWriter.toJson(describeIndexToMap(response)));
            } else {
                printDescribeIndexText(response);
            }
        }
        return 0;
    }

    private int runStatus(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);
        addIndexOptions(opts);

        CommandLine cli = parse(opts, args, COMMAND_STATUS);
        if (cli == null) {
            return 0;
        }
        try (IndexingAdminClient client = buildClient(cli)) {
            GetIndexStatusResponse response = client.getIndexStatus(
                    cli.getOptionValue("tablespace"),
                    cli.getOptionValue("table"),
                    cli.getOptionValue("index"));
            if (cli.hasOption("json")) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("vector_count", response.getVectorCount());
                m.put("segment_count", response.getSegmentCount());
                m.put("last_lsn_ledger", response.getLastLsnLedger());
                m.put("last_lsn_offset", response.getLastLsnOffset());
                m.put("status", response.getStatus());
                out.println(JsonWriter.toJson(m));
            } else {
                out.printf(Locale.ROOT,
                        "vectors=%d segments=%d lsn=%d/%d status=%s%n",
                        response.getVectorCount(),
                        response.getSegmentCount(),
                        response.getLastLsnLedger(),
                        response.getLastLsnOffset(),
                        response.getStatus());
            }
        }
        return 0;
    }

    private int runListPks(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);
        addIndexOptions(opts);
        opts.addOption(Option.builder().longOpt("limit").hasArg().argName("N")
                .desc("maximum number of PKs to return (default 0 = unlimited)").build());
        opts.addOption(Option.builder().longOpt("chunk-size").hasArg().argName("N")
                .desc("PKs per gRPC chunk (default 1000)").build());
        opts.addOption(Option.builder().longOpt("include-ondisk")
                .desc("also include PKs that live only in on-disk segments").build());
        opts.addOption(Option.builder().longOpt("format").hasArg().argName("FMT")
                .desc("output format: hex | base64 (default hex)").build());

        CommandLine cli = parse(opts, args, COMMAND_LIST_PKS);
        if (cli == null) {
            return 0;
        }
        long limit = Long.parseLong(cli.getOptionValue("limit", "0"));
        int chunkSize = Integer.parseInt(cli.getOptionValue("chunk-size", "1000"));
        boolean includeOnDisk = cli.hasOption("include-ondisk");
        String format = cli.getOptionValue("format", "hex").toLowerCase(Locale.ROOT);
        if (!"hex".equals(format) && !"base64".equals(format)) {
            err.println("--format must be hex or base64");
            return 2;
        }

        try (IndexingAdminClient client = buildClient(cli)) {
            Iterator<PrimaryKeysChunk> stream = client.listPrimaryKeys(
                    cli.getOptionValue("tablespace"),
                    cli.getOptionValue("table"),
                    cli.getOptionValue("index"),
                    chunkSize, includeOnDisk, limit);
            long total = 0;
            while (stream.hasNext()) {
                PrimaryKeysChunk chunk = stream.next();
                for (ByteString pk : chunk.getPrimaryKeysList()) {
                    total++;
                    byte[] arr = pk.toByteArray();
                    if ("base64".equals(format)) {
                        out.println(Base64.getEncoder().encodeToString(arr));
                    } else {
                        out.println(toHex(arr));
                    }
                }
                if (chunk.getLast()) {
                    break;
                }
            }
            err.println("# total: " + total);
        }
        return 0;
    }

    private int runEngineStats(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);

        CommandLine cli = parse(opts, args, COMMAND_ENGINE_STATS);
        if (cli == null) {
            return 0;
        }
        try (IndexingAdminClient client = buildClient(cli)) {
            GetEngineStatsResponse response = client.getEngineStats();
            if (cli.hasOption("json")) {
                out.println(JsonWriter.toJson(engineStatsToMap(response)));
            } else {
                out.println("Engine stats:");
                out.printf(Locale.ROOT, "  uptime_ms                   = %d%n", response.getUptimeMillis());
                out.printf(Locale.ROOT, "  tailer_running              = %s%n", response.getTailerRunning());
                out.printf(Locale.ROOT, "  tailer_watermark            = %d/%d%n",
                        response.getTailerWatermarkLedger(), response.getTailerWatermarkOffset());
                out.printf(Locale.ROOT, "  tailer_entries_processed    = %d%n", response.getTailerEntriesProcessed());
                out.printf(Locale.ROOT, "  apply_queue_size/capacity   = %d / %d%n",
                        response.getApplyQueueSize(), response.getApplyQueueCapacity());
                out.printf(Locale.ROOT, "  apply_parallelism           = %d%n", response.getApplyParallelism());
                out.printf(Locale.ROOT, "  loaded_index_count          = %d%n", response.getLoadedIndexCount());
                out.printf(Locale.ROOT, "  total_estimated_memory_bytes= %d%n", response.getTotalEstimatedMemoryBytes());
            }
        }
        return 0;
    }

    private int runInstanceInfo(String[] args) throws Exception {
        Options opts = new Options();
        addCommonOptions(opts);
        addServerOption(opts);

        CommandLine cli = parse(opts, args, COMMAND_INSTANCE_INFO);
        if (cli == null) {
            return 0;
        }
        try (IndexingAdminClient client = buildClient(cli)) {
            GetInstanceInfoResponse response = client.getInstanceInfo();
            if (cli.hasOption("json")) {
                out.println(JsonWriter.toJson(instanceInfoToMap(response)));
            } else {
                out.println("Instance info:");
                out.printf(Locale.ROOT, "  instance_id      = %s%n", response.getInstanceId());
                out.printf(Locale.ROOT, "  grpc_host:port   = %s:%d%n", response.getGrpcHost(), response.getGrpcPort());
                out.printf(Locale.ROOT, "  storage_type     = %s%n", response.getStorageType());
                out.printf(Locale.ROOT, "  data_dir         = %s%n", response.getDataDir());
                out.printf(Locale.ROOT, "  tablespace       = %s (uuid=%s)%n",
                        response.getTablespaceName(), response.getTablespaceUuid());
                out.printf(Locale.ROOT, "  ordinal          = %d / %d%n",
                        response.getInstanceOrdinal(), response.getNumInstances());
                out.printf(Locale.ROOT, "  jvm_max_heap     = %d bytes (%d MB)%n",
                        response.getJvmMaxHeapBytes(), response.getJvmMaxHeapBytes() / (1024 * 1024));
            }
        }
        return 0;
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private static void addCommonOptions(Options opts) {
        opts.addOption(Option.builder("h").longOpt("help").desc("show help for this command").build());
        opts.addOption(Option.builder().longOpt("json").desc("emit JSON instead of plain text").build());
        opts.addOption(Option.builder().longOpt("timeout-seconds").hasArg().argName("SECS")
                .desc("gRPC call deadline in seconds (default 30)").build());
    }

    private static void addServerOption(Options opts) {
        opts.addOption(Option.builder().longOpt("server").hasArg().argName("HOST:PORT")
                .desc("indexing service gRPC endpoint (required)").required().build());
    }

    private static void addIndexOptions(Options opts) {
        opts.addOption(Option.builder().longOpt("tablespace").hasArg().argName("NAME")
                .desc("tablespace name (optional; sent through to server)").build());
        opts.addOption(Option.builder().longOpt("table").hasArg().argName("NAME")
                .desc("table name (required)").required().build());
        opts.addOption(Option.builder().longOpt("index").hasArg().argName("NAME")
                .desc("index name (required)").required().build());
    }

    private CommandLine parse(Options opts, String[] args, String commandName) throws ParseException {
        if (args.length == 1 && ("-h".equals(args[0]) || "--help".equals(args[0]))) {
            java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.OutputStreamWriter(out, java.nio.charset.StandardCharsets.UTF_8), true);
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(pw, 100, "indexing-admin " + commandName + " [options]",
                    "", opts, 2, 4, "", true);
            pw.flush();
            return null;
        }
        CommandLineParser parser = new DefaultParser();
        return parser.parse(opts, args);
    }

    private IndexingAdminClient buildClient(CommandLine cli) {
        long timeout = Long.parseLong(cli.getOptionValue("timeout-seconds", "30"));
        return new IndexingAdminClient(cli.getOptionValue("server"), timeout);
    }

    private static Map<String, Object> indexDescriptorToMap(IndexDescriptor d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tablespace", d.getTablespace());
        m.put("table", d.getTable());
        m.put("index", d.getIndex());
        m.put("vector_count", d.getVectorCount());
        m.put("status", d.getStatus());
        return m;
    }

    private static Map<String, Object> describeIndexToMap(DescribeIndexResponse r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("basic", indexDescriptorToMap(r.getBasic()));
        m.put("dimension", r.getDimension());
        m.put("similarity", r.getSimilarity());
        m.put("live_node_count", r.getLiveNodeCount());
        m.put("ondisk_node_count", r.getOndiskNodeCount());
        m.put("segment_count", r.getSegmentCount());
        m.put("live_shard_count", r.getLiveShardCount());
        m.put("estimated_memory_bytes", r.getEstimatedMemoryBytes());
        m.put("live_vectors_memory_bytes", r.getLiveVectorsMemoryBytes());
        m.put("ondisk_size_bytes", r.getOndiskSizeBytes());
        m.put("dirty", r.getDirty());
        m.put("last_lsn_ledger", r.getLastLsnLedger());
        m.put("last_lsn_offset", r.getLastLsnOffset());
        m.put("fused_pq_enabled", r.getFusedPqEnabled());
        m.put("m", r.getM());
        m.put("beam_width", r.getBeamWidth());
        m.put("persistent", r.getPersistent());
        m.put("store_class", r.getStoreClass());
        return m;
    }

    private static Map<String, Object> engineStatsToMap(GetEngineStatsResponse r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("uptime_millis", r.getUptimeMillis());
        m.put("tailer_running", r.getTailerRunning());
        m.put("tailer_watermark_ledger", r.getTailerWatermarkLedger());
        m.put("tailer_watermark_offset", r.getTailerWatermarkOffset());
        m.put("tailer_entries_processed", r.getTailerEntriesProcessed());
        m.put("apply_queue_size", r.getApplyQueueSize());
        m.put("apply_queue_capacity", r.getApplyQueueCapacity());
        m.put("apply_parallelism", r.getApplyParallelism());
        m.put("loaded_index_count", r.getLoadedIndexCount());
        m.put("total_estimated_memory_bytes", r.getTotalEstimatedMemoryBytes());
        return m;
    }

    private static Map<String, Object> instanceInfoToMap(GetInstanceInfoResponse r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("instance_id", r.getInstanceId());
        m.put("grpc_host", r.getGrpcHost());
        m.put("grpc_port", r.getGrpcPort());
        m.put("storage_type", r.getStorageType());
        m.put("data_dir", r.getDataDir());
        m.put("tablespace_name", r.getTablespaceName());
        m.put("tablespace_uuid", r.getTablespaceUuid());
        m.put("instance_ordinal", r.getInstanceOrdinal());
        m.put("num_instances", r.getNumInstances());
        m.put("jvm_max_heap_bytes", r.getJvmMaxHeapBytes());
        return m;
    }

    private void printDescribeIndexText(DescribeIndexResponse r) {
        IndexDescriptor basic = r.getBasic();
        out.printf(Locale.ROOT, "Index %s.%s (tablespace=%s)%n",
                basic.getTable(), basic.getIndex(), basic.getTablespace());
        out.printf(Locale.ROOT, "  status                = %s%n", basic.getStatus());
        out.printf(Locale.ROOT, "  store_class           = %s (persistent=%s)%n",
                r.getStoreClass(), r.getPersistent());
        out.printf(Locale.ROOT, "  dimension             = %d%n", r.getDimension());
        out.printf(Locale.ROOT, "  similarity            = %s%n", r.getSimilarity());
        out.printf(Locale.ROOT, "  vector_count          = %d%n", basic.getVectorCount());
        out.printf(Locale.ROOT, "  live_node_count       = %d%n", r.getLiveNodeCount());
        out.printf(Locale.ROOT, "  ondisk_node_count     = %d%n", r.getOndiskNodeCount());
        out.printf(Locale.ROOT, "  segment_count         = %d%n", r.getSegmentCount());
        out.printf(Locale.ROOT, "  live_shard_count      = %d%n", r.getLiveShardCount());
        out.printf(Locale.ROOT, "  estimated_memory_bytes= %d%n", r.getEstimatedMemoryBytes());
        out.printf(Locale.ROOT, "  ondisk_size_bytes     = %d%n", r.getOndiskSizeBytes());
        out.printf(Locale.ROOT, "  dirty                 = %s%n", r.getDirty());
        out.printf(Locale.ROOT, "  fused_pq_enabled      = %s%n", r.getFusedPqEnabled());
        out.printf(Locale.ROOT, "  m / beam_width        = %d / %d%n", r.getM(), r.getBeamWidth());
        out.printf(Locale.ROOT, "  last_lsn              = %d/%d%n",
                r.getLastLsnLedger(), r.getLastLsnOffset());
    }

    static String toHex(byte[] arr) {
        char[] hex = "0123456789abcdef".toCharArray();
        char[] out = new char[arr.length * 2];
        for (int i = 0; i < arr.length; i++) {
            int b = arr[i] & 0xff;
            out[i * 2] = hex[b >>> 4];
            out[i * 2 + 1] = hex[b & 0x0f];
        }
        return new String(out);
    }
}
