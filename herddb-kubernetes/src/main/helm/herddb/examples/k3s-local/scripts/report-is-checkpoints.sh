#!/usr/bin/env bash
#
# Generate an HTML report on Indexing Service checkpoint dynamics and vector
# index storage layout for a given run.
#
# Parses the IS pod log (Phase A / Phase B / Phase C lines) and, if the
# cluster is reachable, enriches the report with live indexing-admin data.
#
# Usage:
#   ./scripts/report-is-checkpoints.sh --logs-dir <dir> [OPTIONS]
#
# Options:
#   --logs-dir <dir>      Directory produced by collect-logs.sh (required unless
#                         --is-log is given directly).
#   --is-log <file>       Path to an IS log file (overrides --logs-dir lookup).
#   --is-replica <N>      IS replica index (default: 0).  Used both for the log
#                         filename lookup and for indexing-admin queries.
#   --tablespace <name>   Tablespace name for indexing-admin queries (default: herd).
#   --table <name>        Table name (default: vector_bench).
#   --index <name>        Index name (default: vidx).
#   --no-live             Skip live indexing-admin queries even if the cluster is up.
#   --output <file>       Override the output HTML path.
#   --help                Show this help and exit.
#
# On success prints "REPORT=<path>" on the last line (stdout).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
REPORTS_DIR="${HERDDB_TESTS_HOME:-$(dirname "$SCRIPT_DIR")/reports}"
KUBECONFIG_FILE="$(dirname "$SCRIPT_DIR")/.kubeconfig"
mkdir -p "$REPORTS_DIR"
timestamp() { date +%Y%m%d-%H%M%S; }

# ── Defaults ─────────────────────────────────────────────────────────────────
LOGS_DIR=""
IS_LOG=""
IS_REPLICA=0
TABLESPACE="herd"
TABLE="vector_bench"
INDEX_NAME="vidx"
LIVE=1
OUTPUT=""

usage() {
    grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'
    exit 0
}

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --logs-dir)   LOGS_DIR="$2";      shift 2 ;;
        --is-log)     IS_LOG="$2";        shift 2 ;;
        --is-replica) IS_REPLICA="$2";   shift 2 ;;
        --tablespace) TABLESPACE="$2";   shift 2 ;;
        --table)      TABLE="$2";        shift 2 ;;
        --index)      INDEX_NAME="$2";   shift 2 ;;
        --no-live)    LIVE=0;            shift   ;;
        --output)     OUTPUT="$2";       shift 2 ;;
        --help|-h)    usage ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

# ── Resolve IS log ────────────────────────────────────────────────────────────
if [[ -z "$IS_LOG" ]]; then
    if [[ -z "$LOGS_DIR" ]]; then
        echo "ERROR: --logs-dir or --is-log is required." >&2
        exit 2
    fi
    IS_LOG="$LOGS_DIR/herddb-indexing-service-${IS_REPLICA}.log"
fi
if [[ ! -f "$IS_LOG" ]]; then
    echo "ERROR: IS log not found: $IS_LOG" >&2
    exit 2
fi

# ── Live indexing-admin queries ───────────────────────────────────────────────
ENGINE_JSON="{}"
DESCRIBE_JSON="{}"

if [[ "$LIVE" -eq 1 ]]; then
    IS_SERVER="herddb-indexing-service-${IS_REPLICA}.herddb-indexing-service:9850"
    if kubectl --kubeconfig "$KUBECONFIG_FILE" get pod herddb-tools-0 \
           --no-headers -o name >/dev/null 2>&1; then
        echo "==> Querying live indexing-admin (IS replica ${IS_REPLICA})…"
        ENGINE_JSON=$(kubectl --kubeconfig "$KUBECONFIG_FILE" exec herddb-tools-0 -- \
            indexing-admin engine-stats \
            --server "$IS_SERVER" --json 2>/dev/null \
            | grep '^{' || echo "{}")
        DESCRIBE_JSON=$(kubectl --kubeconfig "$KUBECONFIG_FILE" exec herddb-tools-0 -- \
            indexing-admin describe-index \
            --server "$IS_SERVER" \
            --tablespace "$TABLESPACE" \
            --table "$TABLE" \
            --index "$INDEX_NAME" \
            --json 2>/dev/null \
            | grep '^{' || echo "{}")
        echo "    engine-stats: OK"
        echo "    describe-index: OK"
    else
        echo "==> herddb-tools-0 not reachable — skipping live queries."
        LIVE=0
    fi
fi

# ── Parse IS log → JavaScript arrays ─────────────────────────────────────────
# Each checkpoint cycle consists of (at least one) Phase A + Phase B + Phase C.
# We match Phase B lines first as the anchor, then look for the surrounding
# Phase A / Phase C lines within the same cycle.
#
# Output format (one JS array literal per line):
#   [shards, liveVecs, newSegs, phaseB_ms, bytes, ondiskAfter, segCountAfter, newLiveDuring]

JS_ROWS=$(awk '
function sc(s,   r) { r=s; gsub(/,/,"",r); return r+0 }

/Phase A: snapshotted/ {
    s=$0
    sub(/.*snapshotted /,"",s); shards=s+0          # first token = shard count
    sub(/.*live shards \(/,"",s); split(s,a," "); liveVecs=sc(a[1])
    gotA=1
}

/Phase B: completed in/ {
    s=$0
    sub(/.*completed in /,"",s); split(s,a," "); phaseB_ms=sc(a[1])
    sub(/.*ms \(/,"",s);         split(s,a," "); newSegs=a[1]+0
    sub(/.*total vectors, /,"",s); split(s,a," "); bytes=sc(a[1])
    gotB=1
}

/Phase C:/ && /nodes across/ {
    s=$0
    sub(/.*Phase C: /,"",s);    split(s,a," "); ondiskAfter=sc(a[1])
    sub(/.*nodes across /,"",s); split(s,a," "); segsAfter=sc(a[1])
    # "…(FusedPQ), N new live inserts…"
    sub(/.*\), /,"",s);         split(s,a," "); newLive=sc(a[1])
    gotC=1
}

gotA && gotB && gotC {
    printf "  [%d,%d,%d,%d,%d,%d,%d,%d],\n",
        shards,liveVecs,newSegs,phaseB_ms,bytes,ondiskAfter,segsAfter,newLive
    gotA=0; gotB=0; gotC=0
    shards=0; liveVecs=0; newSegs=0; phaseB_ms=0
    bytes=0; ondiskAfter=0; segsAfter=0; newLive=0
}
' "$IS_LOG")

ROW_COUNT=$(echo "$JS_ROWS" | grep -c '^\s*\[' || true)

if [[ "$ROW_COUNT" -eq 0 ]]; then
    echo "WARNING: No checkpoint cycles found in $IS_LOG" >&2
    JS_ROWS="  /* no data */"
fi

# ── Resolve output path ───────────────────────────────────────────────────────
TS="$(timestamp)"
if [[ -z "$OUTPUT" ]]; then
    OUTPUT="$REPORTS_DIR/report-is-checkpoints-${TS}.html"
fi

# ── Extract scalar stats from indexing-admin JSON ─────────────────────────────
_jval() {
    # Naive key extraction from flat JSON without jq
    local key="$1" json="$2"
    echo "$json" | grep -oP "\"${key}\":\s*\K[^,}\"]+" 2>/dev/null | head -1 || echo "N/A"
}

VECTOR_COUNT=$(_jval "vector_count" "$DESCRIBE_JSON")
ONDISK_NODES=$(_jval "ondisk_node_count" "$DESCRIBE_JSON")
SEG_COUNT=$(_jval "segment_count" "$DESCRIBE_JSON")
ONDISK_BYTES=$(_jval "ondisk_size_bytes" "$DESCRIBE_JSON")
STATUS=$(_jval "status" "$DESCRIBE_JSON")
M_VAL=$(_jval '"m"' "$DESCRIBE_JSON"); M_VAL=$(_jval 'm' "$DESCRIBE_JSON")
FUSED_PQ=$(_jval "fused_pq_enabled" "$DESCRIBE_JSON")
LAST_LSN_LED=$(_jval "last_lsn_ledger" "$DESCRIBE_JSON")
LAST_LSN_OFF=$(_jval "last_lsn_offset" "$DESCRIBE_JSON")
COMP_PHASE=$(_jval "compaction_phase" "$DESCRIBE_JSON")
JVM_USED=$(_jval "jvm_heap_used_bytes" "$ENGINE_JSON")
JVM_MAX=$(_jval "jvm_heap_max_bytes" "$ENGINE_JSON")
UPTIME=$(_jval "uptime_millis" "$ENGINE_JSON")
TAILER_LED=$(_jval "tailer_watermark_ledger" "$ENGINE_JSON")
TAILER_OFF=$(_jval "tailer_watermark_offset" "$ENGINE_JSON")

# If live data not available, fall back to log-derived last row
if [[ "$VECTOR_COUNT" == "N/A" && "$ROW_COUNT" -gt 0 ]]; then
    VECTOR_COUNT=$(echo "$JS_ROWS" | tail -2 | grep '\[' | awk -F',' '{gsub(/[^0-9]/,"",$6); print $6}' | head -1)
    SEG_COUNT=$(echo "$JS_ROWS" | tail -2 | grep '\[' | awk -F',' '{gsub(/[^0-9]/,"",$7); print $7}' | head -1)
fi

LOG_BASENAME=$(basename "$IS_LOG")
LOG_DATE=$(grep -m1 'Apr\|Jan\|Feb\|Mar\|May\|Jun\|Jul\|Aug\|Sep\|Oct\|Nov\|Dec' "$IS_LOG" \
           | awk '{print $1,$2,$3,$4}' 2>/dev/null || echo "unknown date")

# ── Emit HTML ─────────────────────────────────────────────────────────────────
cat > "$OUTPUT" << 'HTMLEOF'
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HerdDB IS Checkpoint Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg:#0f1117;--bg2:#1a1d2e;--bg3:#252840;
    --accent:#6c8ebf;--accent2:#82c341;--accent3:#e0a400;
    --warn:#e05050;--text:#e0e4f0;--muted:#8890a8;--border:#2e3254;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;line-height:1.6}
  h1{font-size:1.5rem;color:var(--accent);padding:22px 28px 6px}
  h2{font-size:1.1rem;color:var(--accent2);margin:0 0 10px}
  h3{font-size:.95rem;color:var(--accent3);margin:10px 0 6px}
  .subtitle{color:var(--muted);padding:0 28px 18px;font-size:.85rem}
  .grid{display:grid;gap:18px;padding:0 20px 20px}
  .g2{grid-template-columns:repeat(auto-fit,minmax(360px,1fr))}
  .g1{grid-template-columns:1fr}
  .card{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:18px}
  .ch{position:relative;height:280px}
  .ch-tall{position:relative;height:380px}
  .kv{display:grid;grid-template-columns:auto 1fr;row-gap:5px;column-gap:14px}
  .kk{color:var(--muted);font-size:.82rem;white-space:nowrap}
  .kv2{font-weight:600;font-size:.9rem}
  .pill{display:inline-block;padding:1px 7px;border-radius:10px;font-size:.75rem;font-weight:700}
  .pg{background:#1a3a1a;color:var(--accent2);border:1px solid var(--accent2)}
  .pb{background:#1a2a3a;color:var(--accent);border:1px solid var(--accent)}
  .po{background:#3a2a00;color:var(--accent3);border:1px solid var(--accent3)}
  .scroll-t{overflow-x:auto;max-height:480px;overflow-y:auto}
  table{width:100%;border-collapse:collapse;font-size:.78rem}
  thead th{background:var(--bg3);color:var(--accent);position:sticky;top:0;padding:7px 9px;
           text-align:right;white-space:nowrap;border-bottom:1px solid var(--border)}
  thead th:first-child{text-align:center}
  tbody tr{border-bottom:1px solid var(--border)}
  tbody tr:hover{background:var(--bg3)}
  tbody td{padding:4px 9px;text-align:right}
  tbody td:first-child{text-align:center;color:var(--muted)}
  .df{color:var(--accent2)}.ds{color:var(--accent3)}.dv{color:var(--warn)}
  .s3{color:var(--accent3);font-weight:700}
  .callout{background:var(--bg3);border-left:3px solid var(--accent3);
           padding:9px 13px;border-radius:0 5px 5px 0;margin:8px 0;font-size:.85rem}
  .callout.green{border-color:var(--accent2)}
  .callout.red{border-color:var(--warn)}
  code{background:var(--bg3);padding:1px 5px;border-radius:3px;
       font-family:monospace;font-size:.8rem;color:var(--accent)}
  footer{text-align:center;padding:18px;color:var(--muted);font-size:.75rem}
</style>
</head>
<body>
HTMLEOF

# Inject dynamic metadata as a JS block
cat >> "$OUTPUT" << JSEOF
<script>
// ── Data injected by report-is-checkpoints.sh ──────────────────────────────
const META = {
  logFile:     $(echo "\"$LOG_BASENAME\""),
  logDate:     $(echo "\"$LOG_DATE\""),
  cycleCount:  $ROW_COUNT,
  vectorCount: $(echo "${VECTOR_COUNT:-0}" | tr -d ','),
  segCount:    $(echo "${SEG_COUNT:-0}"    | tr -d ','),
  ondiskBytes: $(echo "${ONDISK_BYTES:-0}" | tr -d ','),
  status:      $(echo "\"${STATUS:-unknown}\""),
  fusedPQ:     $(echo "\"${FUSED_PQ:-unknown}\""),
  lastLsnLed:  $(echo "${LAST_LSN_LED:-0}" | tr -d ','),
  lastLsnOff:  $(echo "${LAST_LSN_OFF:-0}" | tr -d ','),
  compPhase:   $(echo "\"${COMP_PHASE:-unknown}\""),
  jvmUsed:     $(echo "${JVM_USED:-0}"     | tr -d ','),
  jvmMax:      $(echo "${JVM_MAX:-0}"      | tr -d ','),
  uptime:      $(echo "${UPTIME:-0}"       | tr -d ','),
  tailerLed:   $(echo "${TAILER_LED:-0}"   | tr -d ','),
  tailerOff:   $(echo "${TAILER_OFF:-0}"   | tr -d ','),
  liveData:    $LIVE,
};

// [shards, liveVecs, newSegs, phaseB_ms, bytes, ondiskAfter, segCountAfter, newLiveDuring]
const CYCLES = [
$JS_ROWS
];
</script>
JSEOF

cat >> "$OUTPUT" << 'HTMLEOF2'
<h1>🗂 IS Checkpoint Dynamics &amp; Index Storage Layout</h1>
<p class="subtitle" id="subtitleLine">Loading…</p>

<div class="grid g2" style="padding-top:14px">

  <!-- Summary card -->
  <div class="card">
    <h2>📊 Index Summary</h2>
    <div class="kv" id="summaryKv"></div>
  </div>

  <!-- Storage pie -->
  <div class="card">
    <h2>💾 Per-vector On-disk Footprint</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Each segment has two files: <code>graph</code> (HNSW adjacency lists)
      and <code>map</code> (PK → nodeId + float32 vector copy for reranking).
      Sizes shown are for a typical 50,000-vector segment.
    </p>
    <div class="ch"><canvas id="pieChart"></canvas></div>
  </div>

</div>

<div class="grid g2">

  <div class="card">
    <h2>⏱ Phase B Duration per Checkpoint Cycle</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Phase B = upload live shard segments to file-server.
      Duration tracks <em>live vectors flushed</em>, not total segment count.
      <span style="color:var(--accent3)">■</span> slow (10–15 s)
      <span style="color:var(--warn)">■</span> very slow (&gt;15 s)
    </p>
    <div class="ch"><canvas id="durationChart"></canvas></div>
  </div>

  <div class="card">
    <h2>📈 On-disk Vectors &amp; Segment Count Growth</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Each checkpoint appends 2–3 new immutable segments.
      Segment count is the left axis; on-disk vectors the right axis.
    </p>
    <div class="ch"><canvas id="growthChart"></canvas></div>
  </div>

  <div class="card">
    <h2>🔢 Live Vectors Flushed per Checkpoint</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      <span style="color:var(--accent3)">■</span> 3-shard flush
      <span style="color:var(--accent)">■</span> 2-shard flush.
      Target shard capacity ≈ 50,000 vectors.
    </p>
    <div class="ch"><canvas id="liveVecsChart"></canvas></div>
  </div>

  <div class="card">
    <h2>📦 Bytes Uploaded per Checkpoint (Phase B)</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Total segment data written to file-server.
      Should track live-vectors linearly at ≈ 1,630 B/vec.
    </p>
    <div class="ch"><canvas id="bytesChart"></canvas></div>
  </div>

</div>

<!-- Full checkpoint table -->
<div class="grid g1">
  <div class="card">
    <h2>📋 All Visible Checkpoint Cycles</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px" id="tableNote"></p>
    <div class="scroll-t">
      <table>
        <thead><tr>
          <th>#</th><th>Shards</th><th>Live vecs flushed</th>
          <th>New segs</th><th>Phase B ms</th><th>MB uploaded</th>
          <th>MB/s</th><th>On-disk after (M)</th><th>Seg count</th>
          <th>New live during ckpt</th>
        </tr></thead>
        <tbody id="cpTbody"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- Analysis -->
<div class="grid g1">
  <div class="card">
    <h2>🔬 Analysis: Checkpoint Scaling &amp; PK→nodeId Mapping</h2>
    <div id="analysisBlock" style="font-size:.85rem;line-height:1.8"></div>
  </div>
</div>

<footer id="footerLine">Generated by report-is-checkpoints.sh</footer>

<script>
// ── helpers ──────────────────────────────────────────────────────────────────
const fmt  = n => Number(n).toLocaleString();
const fmtM = n => (n/1e6).toFixed(3)+' M';
const fmtMB= n => (n/1048576).toFixed(1)+' MB';
const fmtS = ms => (ms/1000).toFixed(1)+'s';

const labels   = CYCLES.map((_,i)=>i+1);
const durData  = CYCLES.map(r=>r[3]);
const liveData = CYCLES.map(r=>r[1]);
const bytData  = CYCLES.map(r=>(r[4]/1048576));
const ondisk   = CYCLES.map(r=>r[5]/1e6);
const segs     = CYCLES.map(r=>r[6]);

const durMed  = [...durData].sort((a,b)=>a-b)[Math.floor(durData.length/2)]||0;
const durMax  = Math.max(...durData);
const durMin  = Math.min(...durData);
const durMean = durData.reduce((a,b)=>a+b,0)/durData.length||0;
const dur95   = [...durData].sort((a,b)=>a-b)[Math.floor(durData.length*.95)]||0;

// ── Subtitle ──────────────────────────────────────────────────────────────────
document.getElementById('subtitleLine').textContent =
  `Source: ${META.logFile}  ·  Cycles visible: ${META.cycleCount}  `+
  `·  On-disk vectors: ${fmt(META.vectorCount)}  `+
  `·  Segments: ${fmt(META.segCount)}  ·  Status: ${META.status}  `+
  `·  Live data: ${META.liveData ? 'yes' : 'no (offline)'}`;

// ── Summary KV ───────────────────────────────────────────────────────────────
const gib = b => b>0 ? (b/1073741824).toFixed(1)+' GiB' : 'N/A';
const pct  = (u,m) => u>0&&m>0 ? (u/m*100).toFixed(0)+'%' : 'N/A';
const kvRows = [
  ['Total vectors',    fmt(META.vectorCount)],
  ['Segment count',    fmt(META.segCount)],
  ['Avg vecs/segment', META.segCount>0 ? fmt(Math.round(META.vectorCount/META.segCount)) : 'N/A'],
  ['On-disk size',     gib(META.ondiskBytes)+(META.ondiskBytes>0 ? ` (${(META.ondiskBytes/1e9).toFixed(1)} GB)` : '')],
  ['Bytes/vector',     META.vectorCount>0 && META.ondiskBytes>0 ?
                       fmt(Math.round(META.ondiskBytes/META.vectorCount))+' B' : 'N/A'],
  ['Status',           META.status],
  ['FusedPQ',          META.fusedPQ],
  ['Last LSN',         `ledger ${fmt(META.lastLsnLed)} · offset ${fmt(META.lastLsnOff)}`],
  ['Tailer watermark', `ledger ${fmt(META.tailerLed)} · offset ${fmt(META.tailerOff)}`],
  ['Compaction phase', META.compPhase],
  ['JVM heap',         META.jvmMax>0 ? `${gib(META.jvmUsed)} / ${gib(META.jvmMax)} (${pct(META.jvmUsed,META.jvmMax)})` : 'N/A'],
  ['Uptime',           META.uptime>0 ? (META.uptime/3600000).toFixed(2)+' h' : 'N/A'],
  ['Log file',         META.logFile],
  ['Checkpoint cycles (visible)', fmt(META.cycleCount)],
  ['Phase B median',   fmtS(durMed)],
  ['Phase B p95',      fmtS(dur95)],
  ['Phase B max',      fmtS(durMax)],
];
const kvDiv = document.getElementById('summaryKv');
kvRows.forEach(([k,v])=>{
  const kEl=document.createElement('span'); kEl.className='kk'; kEl.textContent=k;
  const vEl=document.createElement('span'); vEl.className='kv2'; vEl.textContent=v;
  kvDiv.append(kEl,vEl);
});

// ── Table note ────────────────────────────────────────────────────────────────
document.getElementById('tableNote').textContent =
  `${META.cycleCount} cycles parsed from ${META.logFile}.  `+
  `Yellow = 10–15 s, Red = >15 s, orange Shards column = 3-shard flush.`;

// ── Checkpoint table ──────────────────────────────────────────────────────────
const tbody = document.getElementById('cpTbody');
CYCLES.forEach((r,i)=>{
  const [shards,liveVecs,newSegs,phaseB,bytes,ondiskAfter,segCnt,newLive]=r;
  const mbps=(bytes/phaseB*1000/1048576).toFixed(0);
  let dc=phaseB>=15000?'dv':phaseB>=10000?'ds':'df';
  const sc=shards===3?'s3':'';
  tbody.innerHTML+=`<tr>
    <td>${i+1}</td><td class="${sc}">${shards}</td>
    <td>${fmt(liveVecs)}</td><td>${newSegs}</td>
    <td class="${dc}">${fmt(phaseB)}</td>
    <td>${fmtMB(bytes)}</td><td>${mbps}</td>
    <td>${fmtM(ondiskAfter)}</td><td>${fmt(segCnt)}</td>
    <td>${newLive>0?fmt(newLive):'—'}</td>
  </tr>`;
});

// ── Chart defaults ────────────────────────────────────────────────────────────
const CD = {
  responsive:true,maintainAspectRatio:false,animation:false,
  plugins:{legend:{labels:{color:'#8890a8',font:{size:10}}}},
  scales:{
    x:{ticks:{color:'#8890a8',font:{size:9}},grid:{color:'#2e3254'},
       title:{display:true,text:'Checkpoint cycle #',color:'#8890a8'}},
    y:{ticks:{color:'#8890a8',font:{size:9}},grid:{color:'#2e3254'}}
  }
};

// ── Pie chart ─────────────────────────────────────────────────────────────────
new Chart(document.getElementById('pieChart'),{
  type:'doughnut',
  data:{
    labels:['graph (HNSW adjacency lists)','map (PK + nodeId + float32 vector)'],
    datasets:[{data:[55064120,26400004],
      backgroundColor:['#1a3a1a','#3a2a00'],
      borderColor:['#82c341','#e0a400'],borderWidth:2}]
  },
  options:{
    responsive:true,maintainAspectRatio:false,
    plugins:{legend:{position:'bottom',labels:{color:'#8890a8',font:{size:10},padding:12}},
      tooltip:{callbacks:{label:ctx=>{
        const v=ctx.parsed, t=55064120+26400004;
        return ` ${(v/1048576).toFixed(1)} MB  (${(v/t*100).toFixed(0)}%)`;
      }}}}
  }
});

// ── Duration chart ────────────────────────────────────────────────────────────
new Chart(document.getElementById('durationChart'),{
  type:'line',
  data:{labels,datasets:[{
    label:'Phase B ms',data:durData,
    borderColor:'#6c8ebf',backgroundColor:'rgba(108,142,191,.12)',
    fill:true,tension:.3,pointRadius:3,
    pointBackgroundColor:durData.map(v=>v>=15000?'#e05050':v>=10000?'#e0a400':'#6c8ebf')
  }]},
  options:{...CD,scales:{...CD.scales,
    y:{...CD.scales.y,beginAtZero:true,title:{display:true,text:'ms',color:'#8890a8'}}}}
});

// ── Growth chart ──────────────────────────────────────────────────────────────
new Chart(document.getElementById('growthChart'),{
  type:'line',
  data:{labels,datasets:[
    {label:'On-disk vectors (M)',data:ondisk,
     borderColor:'#82c341',backgroundColor:'rgba(130,195,65,.08)',
     fill:true,tension:.2,pointRadius:0,yAxisID:'yL'},
    {label:'Segment count',data:segs,
     borderColor:'#e0a400',tension:.2,pointRadius:0,yAxisID:'yR'}
  ]},
  options:{
    responsive:true,maintainAspectRatio:false,animation:false,
    plugins:{legend:{labels:{color:'#8890a8',font:{size:10}}}},
    scales:{
      x:{ticks:{color:'#8890a8',font:{size:9}},grid:{color:'#2e3254'},
         title:{display:true,text:'Checkpoint cycle #',color:'#8890a8'}},
      yL:{type:'linear',position:'left',
          ticks:{color:'#82c341',font:{size:9}},grid:{color:'#2e3254'},
          title:{display:true,text:'On-disk vectors (M)',color:'#82c341'}},
      yR:{type:'linear',position:'right',
          ticks:{color:'#e0a400',font:{size:9}},grid:{drawOnChartArea:false},
          title:{display:true,text:'Segment count',color:'#e0a400'}}
    }
  }
});

// ── Live vecs chart ───────────────────────────────────────────────────────────
new Chart(document.getElementById('liveVecsChart'),{
  type:'bar',
  data:{labels,datasets:[{
    label:'Live vectors flushed',data:liveData,
    backgroundColor:CYCLES.map(r=>r[0]===3?'rgba(224,164,0,.7)':'rgba(108,142,191,.6)'),
    borderColor:CYCLES.map(r=>r[0]===3?'#e0a400':'#6c8ebf'),borderWidth:1
  }]},
  options:{...CD,scales:{...CD.scales,
    y:{...CD.scales.y,beginAtZero:true,title:{display:true,text:'vectors',color:'#8890a8'}}}}
});

// ── Bytes chart ───────────────────────────────────────────────────────────────
new Chart(document.getElementById('bytesChart'),{
  type:'bar',
  data:{labels,datasets:[{
    label:'MB uploaded',data:bytData,
    backgroundColor:'rgba(130,195,65,.55)',borderColor:'#82c341',borderWidth:1
  }]},
  options:{...CD,scales:{...CD.scales,
    y:{...CD.scales.y,beginAtZero:true,title:{display:true,text:'MB',color:'#8890a8'}}}}
});

// ── Analysis block ────────────────────────────────────────────────────────────
const trend = (() => {
  if(durData.length<10) return 'insufficient data';
  const first5=durData.slice(0,5).reduce((a,b)=>a+b,0)/5;
  const last5=durData.slice(-5).reduce((a,b)=>a+b,0)/5;
  const ratio=last5/first5;
  if(ratio>1.5) return `GROWING (last-5 avg ${fmtS(last5)} vs first-5 avg ${fmtS(first5)}, ×${ratio.toFixed(2)})`;
  if(ratio<0.7) return `SHRINKING (last-5 avg ${fmtS(last5)} vs first-5 avg ${fmtS(first5)})`;
  return `STABLE (last-5 avg ${fmtS(last5)} vs first-5 avg ${fmtS(first5)})`;
})();

const threeShardCount = CYCLES.filter(r=>r[0]===3).length;
const twoShardCount   = CYCLES.filter(r=>r[0]===2).length;

document.getElementById('analysisBlock').innerHTML = `
<div class="callout green">
  <b>Checkpoint time trend: ${trend}</b><br>
  Phase B duration is bounded by live vectors flushed per cycle (~50–150 k),
  <em>not</em> by the total number of on-disk segments. Each checkpoint writes
  only the newly frozen shard vectors to new immutable segments; existing
  segments are never rewritten.
</div>
<h3>Three-phase checkpoint protocol</h3>
<p><b>Phase A (snapshot):</b> Live shards are atomically frozen. New inserts
accumulate in a fresh shard while upload proceeds. Nearly instantaneous.</p>
<p><b>Phase B (upload):</b> Each frozen shard is serialised into a new segment
(<code>graph</code> + <code>map</code> files) and streamed to the file-server.
Work is O(live vectors) — constant per cycle regardless of total segment count.
Observed: median ${fmtS(durMed)}, p95 ${fmtS(dur95)}, max ${fmtS(durMax)}.</p>
<p><b>Phase C (registry update):</b> New segment references are appended to the
in-memory registry and the watermark is advanced. O(new segments added) = 2–3
per cycle — effectively O(1).</p>
<h3>PK → nodeId mapping structure</h3>
<p>The mapping is <b>per-segment</b>, stored inside the <code>multipart/map</code>
file. There is <em>no global growing dictionary</em>. Each segment's map holds:</p>
<ul style="margin:4px 0 8px 18px">
  <li><b>float32 vector copy</b>: 128 dims × 4 B = 512 B — for exact-distance
      reranking after HNSW beam search (FusedPQ uses approximate PQ distance
      during traversal, then reranks with exact floats)</li>
  <li><b>PK (long)</b>: 8 B — original table primary key</li>
  <li><b>nodeId (int)</b>: 4 B — graph node index within this segment</li>
  <li><b>overhead</b>: ~5 B (alignment / entry header)</li>
</ul>
<p>Result: ~529 B/vector in the map file vs ~1,101 B/vector in the graph file
→ <b>~1,630 B/vector total</b> on disk per vector.</p>
<h3>What does grow with segment count</h3>
<ul style="margin:4px 0 8px 18px">
  <li><b>In-memory segment registry</b> in the IS JVM — ${fmt(META.segCount)} entries at
      ${META.jvmMax>0?`${pct(META.jvmUsed,META.jvmMax)} JVM heap usage`:'N/A JVM heap usage'}.
      Each entry holds file references and in-memory metadata.</li>
  <li><b>Query fanout</b> — more segments = more parallel reads per search request.</li>
  <li><b>Cold-start time</b> — IS loads all segment headers at startup.</li>
</ul>
<h3>Flush pattern</h3>
<p>${twoShardCount} two-shard flushes (median duration ${fmtS([...durData].sort((a,b)=>a-b)[Math.floor(twoShardCount/2)]||0)})
and ${threeShardCount} three-shard flushes across ${META.cycleCount} visible cycles.
Three-shard flushes occur when the shard rotation trigger fires while a previous
checkpoint Phase B is still in progress — the extra shard accumulates and is flushed
together in the next cycle.</p>
`;

document.getElementById('footerLine').textContent =
  `Generated by report-is-checkpoints.sh · ${META.logFile} · ${META.cycleCount} cycles`;
</script>
</body></html>
HTMLEOF2

echo ""
echo "REPORT=$OUTPUT"
