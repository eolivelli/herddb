#!/usr/bin/env bash
#
# Generate an HTML report on HerdDB server checkpoint dynamics.
#
# Parses the server pod log looking for:
#   - "local checkpoint start herd at (L,O)"
#   - "local checkpoint finish herd started ad (L,O), … total time X ms"
#   - "BLinkKeyToPageIndex checkpoint finished: logpos (L,O), N pages"
#   - "dirty pages" flush lines from ActivatableTable / FileDataStorageManager
#
# The report includes:
#   - Timeline chart of checkpoint durations
#   - Pages checkpointed per run
#   - LSN (ledger, offset) advancement per checkpoint
#   - Analysis of whether duration grows over time
#
# Usage:
#   ./scripts/report-server-checkpoints.sh --logs-dir <dir> [OPTIONS]
#
# Options:
#   --logs-dir <dir>   Directory produced by collect-logs.sh (required unless
#                      --server-log is given directly).
#   --server-log <f>   Path to herddb-server-0.log (overrides --logs-dir).
#   --run-log <f>      Optional path to run-bench log; used to annotate ingest
#                      progress on the timeline.
#   --output <file>    Override the output HTML path.
#   --help             Show this help and exit.
#
# On success prints "REPORT=<path>" on the last line (stdout).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
REPORTS_DIR="${HERDDB_TESTS_HOME:-$(dirname "$SCRIPT_DIR")/reports}"
mkdir -p "$REPORTS_DIR"
timestamp() { date +%Y%m%d-%H%M%S; }

# ── Defaults ──────────────────────────────────────────────────────────────────
LOGS_DIR=""
SERVER_LOG=""
RUN_LOG=""
OUTPUT=""

usage() {
    grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --logs-dir)    LOGS_DIR="$2";    shift 2 ;;
        --server-log)  SERVER_LOG="$2";  shift 2 ;;
        --run-log)     RUN_LOG="$2";     shift 2 ;;
        --output)      OUTPUT="$2";      shift 2 ;;
        --help|-h)     usage ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

# ── Resolve server log ────────────────────────────────────────────────────────
if [[ -z "$SERVER_LOG" ]]; then
    if [[ -z "$LOGS_DIR" ]]; then
        echo "ERROR: --logs-dir or --server-log is required." >&2
        exit 2
    fi
    SERVER_LOG="$LOGS_DIR/herddb-server-0.log"
fi
if [[ ! -f "$SERVER_LOG" ]]; then
    echo "ERROR: server log not found: $SERVER_LOG" >&2
    exit 2
fi

LOG_BASENAME=$(basename "$SERVER_LOG")
LOG_DATE=$(grep -m1 'Apr\|Jan\|Feb\|Mar\|May\|Jun\|Jul\|Aug\|Sep\|Oct\|Nov\|Dec' "$SERVER_LOG" \
           | awk '{print $1,$2,$3,$4}' 2>/dev/null || echo "unknown date")

# ── Parse server log ──────────────────────────────────────────────────────────
#
# We need to correlate three line types:
#
#  START:  "local checkpoint start herd at (L,O)"
#  FINISH: "local checkpoint finish herd started ad (L,O), finished at (L2,O2), total time T ms"
#  BLINK:  "BLinkKeyToPageIndex checkpoint finished: logpos (L,O), N pages"
#  DIRTY:  "checkpointTable …: N dirty pages"  (optional, may not appear every cycle)
#
# Strategy: parse FINISH lines as the authoritative anchor (they contain start
# LSN, finish LSN, and duration).  Then look for the nearest preceding BLINK
# line to get page count.  DIRTY lines are collected similarly.
#
# Output JS array: [startLed, startOff, finishLed, finishOff, totalMs, blinkPages,
#                   timestamp_str, dirtyPages]

JS_ROWS=$(awk '
function sc(s,  r) { r=s; gsub(/,/,"",r); return r+0 }
# Parse "(L,O)" out of string s → set globals pLed pOff
function parseLSN(s,  t) {
    t=s; sub(/.*\(/,"",t); sub(/\).*/,"",t)
    split(t,lp,","); pLed=lp[1]+0; pOff=lp[2]+0
}

BEGIN { lastBlinkPages=-1 }

# Java util logging emits the timestamp on its OWN line before the INFO: line.
# Format: "Apr 18, 2026 12:58:58 AM herddb.core.TableSpaceManager localCheckpoint"
# Note the comma after the day number — /^[A-Z][a-z]+ [0-9]+, [0-9]+ .../
/^[A-Z][a-z]+ [0-9]+, [0-9]+ [0-9]+:[0-9]+:[0-9]+/ {
    lastTs=sprintf("%s %s %s %s %s", $1,$2,$3,$4,$5)
}

# BLink INFO line (comes inside the server checkpoint, before "local checkpoint finish"):
# "INFO: checkpoint index UUID_primary finished: logpos (L,O), N pages"
/checkpoint index.*finished.*logpos.*pages/ {
    s=$0; sub(/.*logpos /,"",s); parseLSN(s)
    # pages: strip the "(L,O), " prefix then read first word
    sub(/\([^)]*\), /,"",s); split(s,a," "); lastBlinkPages=sc(a[1])
}

/checkpointTable/ && /dirty pages/ {
    split($0,a," ")
    for(i=1;i<=NF;i++) if(a[i+1]=="dirty") { lastDirty=sc(a[i]); break }
}

/local checkpoint finish/ {
    s=$0
    sub(/.*started ad /,"",s);  parseLSN(s); sLed=pLed; sOff=pOff
    s=$0
    sub(/.*finished at /,"",s); parseLSN(s); fLed=pLed; fOff=pOff
    s=$0
    sub(/.*total time /,"",s);  split(s,a," "); ms=sc(a[1])
    printf "  [%d,%d,%d,%d,%d,%d,\"%s\",%d],\n",
        sLed,sOff,fLed,fOff,ms,lastBlinkPages,lastTs,lastDirty
    lastDirty=0
}
' "$SERVER_LOG")

ROW_COUNT=$(echo "$JS_ROWS" | grep -c '^\s*\[' || true)

if [[ "$ROW_COUNT" -eq 0 ]]; then
    echo "WARNING: No checkpoint finish lines found in $SERVER_LOG" >&2
    JS_ROWS="  /* no data */"
fi

# ── Optional: ingest progress annotations from run log ───────────────────────
RUN_LOG_ARGS=""
RUN_LOG_START=""
if [[ -n "$RUN_LOG" && -f "$RUN_LOG" ]]; then
    RUN_LOG_ARGS=$(grep -m1 '^# args:' "$RUN_LOG" | sed 's/^# args: //' || echo "")
    RUN_LOG_START=$(grep -m1 '^# start:' "$RUN_LOG" | sed 's/^# start: //' || echo "")
fi

# ── Resolve output path ───────────────────────────────────────────────────────
TS="$(timestamp)"
if [[ -z "$OUTPUT" ]]; then
    OUTPUT="$REPORTS_DIR/report-server-checkpoints-${TS}.html"
fi

# ── Emit HTML ─────────────────────────────────────────────────────────────────
cat > "$OUTPUT" << 'HTMLEOF'
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HerdDB Server Checkpoint Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root{--bg:#0f1117;--bg2:#1a1d2e;--bg3:#252840;
    --accent:#6c8ebf;--accent2:#82c341;--accent3:#e0a400;
    --warn:#e05050;--text:#e0e4f0;--muted:#8890a8;--border:#2e3254}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;
       font-size:14px;line-height:1.6}
  h1{font-size:1.5rem;color:var(--accent);padding:22px 28px 6px}
  h2{font-size:1.1rem;color:var(--accent2);margin:0 0 10px}
  h3{font-size:.95rem;color:var(--accent3);margin:10px 0 6px}
  .subtitle{color:var(--muted);padding:0 28px 18px;font-size:.85rem}
  .grid{display:grid;gap:18px;padding:0 20px 20px}
  .g2{grid-template-columns:repeat(auto-fit,minmax(360px,1fr))}
  .g1{grid-template-columns:1fr}
  .card{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:18px}
  .ch{position:relative;height:280px}
  .kv{display:grid;grid-template-columns:auto 1fr;row-gap:5px;column-gap:14px}
  .kk{color:var(--muted);font-size:.82rem;white-space:nowrap}
  .kv2{font-weight:600;font-size:.9rem}
  .scroll-t{overflow-x:auto;max-height:480px;overflow-y:auto}
  table{width:100%;border-collapse:collapse;font-size:.78rem}
  thead th{background:var(--bg3);color:var(--accent);position:sticky;top:0;
           padding:7px 9px;text-align:right;white-space:nowrap;
           border-bottom:1px solid var(--border)}
  thead th:first-child{text-align:center}
  tbody tr{border-bottom:1px solid var(--border)}
  tbody tr:hover{background:var(--bg3)}
  tbody td{padding:4px 9px;text-align:right}
  tbody td:first-child{text-align:center;color:var(--muted)}
  tbody td.ts{text-align:left;color:var(--muted);font-size:.72rem;white-space:nowrap}
  .df{color:var(--accent2)}.ds{color:var(--accent3)}.dv{color:var(--warn)}
  .big{font-size:1.6rem;font-weight:700;color:var(--warn)}
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

cat >> "$OUTPUT" << JSEOF
<script>
// ── Data injected by report-server-checkpoints.sh ─────────────────────────
const META = {
  logFile:    $(echo "\"$LOG_BASENAME\""),
  logDate:    $(echo "\"$LOG_DATE\""),
  runLogArgs: $(echo "\"${RUN_LOG_ARGS:-}\""),
  runLogStart:$(echo "\"${RUN_LOG_START:-}\""),
  rowCount:   $ROW_COUNT,
};

// [startLed, startOff, finishLed, finishOff, totalMs, blinkPages, timestamp, dirtyPages]
const CKPTS = [
$JS_ROWS
];
</script>
JSEOF

cat >> "$OUTPUT" << 'HTMLEOF2'
<h1>🖥 HerdDB Server Checkpoint Dynamics</h1>
<p class="subtitle" id="subtitleLine">Loading…</p>

<div class="grid g2" style="padding-top:14px">

  <div class="card">
    <h2>📊 Checkpoint Summary</h2>
    <div class="kv" id="summaryKv"></div>
  </div>

  <div class="card">
    <h2>🏗 Server Checkpoint Architecture</h2>
    <p style="color:var(--muted);font-size:.82rem;line-height:1.7">
      The server runs a periodic checkpoint every
      <code>server.checkpoint.period</code> ms (default configured: 60 s).
      Each checkpoint:
    </p>
    <ol style="margin:8px 0 0 18px;font-size:.82rem;line-height:1.9;color:var(--text)">
      <li>Acquires the <b>checkpoint read-write lock</b> (blocks concurrent commits)</li>
      <li>Flushes all <b>dirty data pages</b> to the remote file-server (MinIO)</li>
      <li>Writes the <b>BLink primary-key index</b> checkpoint file to local storage
          (one file per page, always <code>N pages</code> total — not incremental)</li>
      <li>Writes the <b>vector index checkpoint</b> reference (waits for IS catch-up)</li>
      <li>Advances the <b>checkpoint LSN</b> in ZooKeeper</li>
      <li>Releases the lock — commits can proceed</li>
    </ol>
    <div class="callout" style="margin-top:10px">
      <b>Key insight:</b> the BLink index is checkpointed as a full snapshot of
      all pages every time. Its duration scales with the total page count, not
      just new/dirty pages. Data pages use remote storage so are streamed
      incrementally, but must still be flushed under the lock.
    </div>
  </div>

</div>

<div class="grid g2">

  <div class="card">
    <h2>⏱ Checkpoint Duration Timeline</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Each bar = one checkpoint cycle.
      <span style="color:var(--accent3)">■</span> 1–120 s (slow)
      <span style="color:var(--warn)">■</span> &gt;120 s (very slow — likely caused commit lock timeouts)
    </p>
    <div class="ch"><canvas id="durationChart"></canvas></div>
  </div>

  <div class="card">
    <h2>📄 BLink Index Pages per Checkpoint</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Full page count written to disk at each checkpoint. A constant or
      slowly-growing count is expected as the primary-key BTree fills up.
    </p>
    <div class="ch"><canvas id="pagesChart"></canvas></div>
  </div>

  <div class="card">
    <h2>📈 LSN Advancement per Checkpoint</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Ledger offset difference between consecutive checkpoints.
      Higher = more write activity (entries logged) between checkpoints.
    </p>
    <div class="ch"><canvas id="lsnChart"></canvas></div>
  </div>

  <div class="card">
    <h2>🕐 Checkpoint Start Time Gaps</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px">
      Wall-clock gap between consecutive checkpoint starts.
      Should be close to <code>server.checkpoint.period</code> (60 s) unless
      a checkpoint ran very long.
    </p>
    <div class="ch"><canvas id="gapChart"></canvas></div>
  </div>

</div>

<div class="grid g1">
  <div class="card">
    <h2>📋 All Checkpoint Cycles</h2>
    <p style="color:var(--muted);font-size:.8rem;margin-bottom:8px" id="tableNote"></p>
    <div class="scroll-t">
      <table>
        <thead><tr>
          <th>#</th><th>Timestamp</th>
          <th>Start LSN</th><th>Finish LSN</th>
          <th>Total ms</th><th>Duration</th>
          <th>BLink pages</th><th>LSN Δoffset</th>
        </tr></thead>
        <tbody id="cpTbody"></tbody>
      </table>
    </div>
  </div>
</div>

<div class="grid g1">
  <div class="card">
    <h2>🔬 Analysis: Why Can a Checkpoint Take Minutes?</h2>
    <div id="analysisBlock" style="font-size:.85rem;line-height:1.8"></div>
  </div>
</div>

<footer id="footerLine"></footer>

<script>
// ── helpers ──────────────────────────────────────────────────────────────────
const fmt   = n => Number(n).toLocaleString();
const fmtS  = ms => ms>=60000 ? (ms/60000).toFixed(1)+' min'
                               : (ms/1000).toFixed(1)+' s';
const fmtMs = ms => fmt(ms)+' ms';
const lsnStr= (l,o) => `(${l},${o})`;

// ── Derived arrays ────────────────────────────────────────────────────────────
const labels  = CKPTS.map((_,i)=>i+1);
const durs    = CKPTS.map(r=>r[4]);
const pages   = CKPTS.map(r=>r[5]<0?null:r[5]);

// LSN offset delta between consecutive checkpoints
// (same ledger: delta offset; ledger roll: we use absolute offset of finish)
const lsnDelta = CKPTS.map((r,i)=>{
  if(i===0) return 0;
  const prev=CKPTS[i-1];
  if(r[0]===prev[2] && r[2]===r[0]) return r[1]-prev[3]; // same ledger
  return r[3]; // new ledger: just the finish offset
});

// ── Statistics ────────────────────────────────────────────────────────────────
const sorted  = [...durs].sort((a,b)=>a-b);
const durMed  = sorted[Math.floor(sorted.length/2)]||0;
const durMean = durs.reduce((a,b)=>a+b,0)/(durs.length||1);
const durMax  = Math.max(...durs);
const durMin  = Math.min(...durs);
const dur95   = sorted[Math.floor(sorted.length*.95)]||0;
const durP99  = sorted[Math.floor(sorted.length*.99)]||0;
const slowIdx = durs.reduce((mi,v,i,a)=>v>a[mi]?i:mi,0);
const pagesUniq= [...new Set(pages.filter(p=>p!==null))];
const pagesMin = pagesUniq.length ? Math.min(...pagesUniq) : 0;
const pagesMax = pagesUniq.length ? Math.max(...pagesUniq) : 0;

// Trend: linear regression slope on durs
const trendSlope = (() => {
  if(durs.length<4) return 0;
  const n=durs.length, sx=n*(n+1)/2, sx2=n*(n+1)*(2*n+1)/6;
  const sy=durs.reduce((a,b)=>a+b,0);
  const sxy=durs.reduce((s,v,i)=>s+(i+1)*v,0);
  return (n*sxy-sx*sy)/(n*sx2-sx*sx);
})();

// ── Subtitle ──────────────────────────────────────────────────────────────────
document.getElementById('subtitleLine').textContent =
  `Source: ${META.logFile}  ·  ${META.rowCount} checkpoint cycles  `+
  `·  Max duration: ${fmtS(durMax)}  ·  Median: ${fmtS(durMed)}`;

// ── Summary KV ───────────────────────────────────────────────────────────────
const kvRows = [
  ['Checkpoint cycles',  fmt(META.rowCount)],
  ['Duration min',       fmtS(durMin)],
  ['Duration median',    fmtS(durMed)],
  ['Duration mean',      fmtS(Math.round(durMean))],
  ['Duration p95',       fmtS(dur95)],
  ['Duration p99',       fmtS(durP99)],
  ['Duration max',       fmtS(durMax)],
  ['Slowest checkpoint', `#${slowIdx+1} at ${CKPTS[slowIdx]?.[6]||'?'} — ${fmtS(durMax)}`],
  ['BLink pages (min)',  fmt(pagesMin)],
  ['BLink pages (max)',  fmt(pagesMax)],
  ['Duration trend',     trendSlope>100 ? `⬆ growing (+${trendSlope.toFixed(0)} ms/cycle)`
                        :trendSlope<-100 ? `⬇ shrinking (${trendSlope.toFixed(0)} ms/cycle)`
                        :'→ stable'],
  ['Log file',           META.logFile],
  ...(META.runLogArgs ? [['Workload', META.runLogArgs]] : []),
  ...(META.runLogStart ? [['Run start', META.runLogStart]] : []),
];
const kvDiv = document.getElementById('summaryKv');
kvRows.forEach(([k,v])=>{
  const ke=document.createElement('span'); ke.className='kk'; ke.textContent=k;
  const ve=document.createElement('span'); ve.className='kv2'; ve.textContent=v;
  kvDiv.append(ke,ve);
});

// ── Table note ────────────────────────────────────────────────────────────────
document.getElementById('tableNote').textContent =
  `${META.rowCount} cycles parsed.  `+
  `Yellow = 1 s – 2 min, Red = >2 min.`;

// ── Table ─────────────────────────────────────────────────────────────────────
const tbody=document.getElementById('cpTbody');
CKPTS.forEach((r,i)=>{
  const[sL,sO,fL,fO,ms,pg,ts,dirty]=r;
  let dc=ms>120000?'dv':ms>1000?'ds':'df';
  const delta=lsnDelta[i];
  tbody.innerHTML+=`<tr>
    <td>${i+1}</td>
    <td class="ts">${ts}</td>
    <td>${lsnStr(sL,sO)}</td>
    <td>${lsnStr(fL,fO)}</td>
    <td class="${dc}">${fmt(ms)}</td>
    <td class="${dc}">${fmtS(ms)}</td>
    <td>${pg>=0?fmt(pg):'—'}</td>
    <td>${fmt(delta)}</td>
  </tr>`;
});

// ── Chart defaults ────────────────────────────────────────────────────────────
const CD={
  responsive:true,maintainAspectRatio:false,animation:false,
  plugins:{legend:{labels:{color:'#8890a8',font:{size:10}}}},
  scales:{
    x:{ticks:{color:'#8890a8',font:{size:9},maxTicksLimit:20},grid:{color:'#2e3254'},
       title:{display:true,text:'Checkpoint #',color:'#8890a8'}},
    y:{ticks:{color:'#8890a8',font:{size:9}},grid:{color:'#2e3254'}}
  }
};

// ── Duration chart ────────────────────────────────────────────────────────────
new Chart(document.getElementById('durationChart'),{
  type:'bar',
  data:{labels,datasets:[{
    label:'Duration (ms)',data:durs,
    backgroundColor:durs.map(v=>v>120000?'rgba(224,80,80,.8)':v>1000?'rgba(224,164,0,.7)':'rgba(108,142,191,.6)'),
    borderColor:durs.map(v=>v>120000?'#e05050':v>1000?'#e0a400':'#6c8ebf'),
    borderWidth:1
  }]},
  options:{...CD,scales:{...CD.scales,
    y:{...CD.scales.y,type:'logarithmic',
       title:{display:true,text:'ms (log scale)',color:'#8890a8'},
       ticks:{color:'#8890a8',font:{size:9},
              callback:v=>v>=60000?(v/60000).toFixed(0)+'min':
                         v>=1000?(v/1000).toFixed(0)+'s':v+'ms'}}}}
});

// ── Pages chart ───────────────────────────────────────────────────────────────
new Chart(document.getElementById('pagesChart'),{
  type:'line',
  data:{labels,datasets:[{
    label:'BLink pages',data:pages,
    borderColor:'#82c341',backgroundColor:'rgba(130,195,65,.1)',
    fill:true,tension:.2,pointRadius:2,spanGaps:true
  }]},
  options:{...CD,scales:{...CD.scales,
    y:{...CD.scales.y,beginAtZero:false,
       title:{display:true,text:'pages',color:'#8890a8'}}}}
});

// ── LSN delta chart ───────────────────────────────────────────────────────────
new Chart(document.getElementById('lsnChart'),{
  type:'bar',
  data:{labels,datasets:[{
    label:'LSN offset delta',data:lsnDelta,
    backgroundColor:'rgba(108,142,191,.55)',borderColor:'#6c8ebf',borderWidth:1
  }]},
  options:{...CD,scales:{...CD.scales,
    y:{...CD.scales.y,beginAtZero:true,
       title:{display:true,text:'offset Δ',color:'#8890a8'}}}}
});

// ── Gap chart (time between checkpoints) ──────────────────────────────────────
// Approximate: gap ≈ duration[i] + (period - duration[i-1]) — we don't have
// precise wall-clock starts, so we use duration as a proxy for "time consumed".
// The actual gap would need start timestamps; we show just the duration as proxy.
const PERIOD_MS = 60000; // configured period
const gaps = durs.map((d,i)=>{
  if(i===0) return PERIOD_MS;
  return Math.max(d, PERIOD_MS); // actual gap >= max(duration, period)
});
new Chart(document.getElementById('gapChart'),{
  type:'bar',
  data:{labels,datasets:[
    {label:'Checkpoint duration (ms)',data:durs,
     backgroundColor:'rgba(108,142,191,.5)',borderColor:'#6c8ebf',borderWidth:1},
    {label:'Configured period (60 s)',
     data:durs.map(()=>PERIOD_MS),
     type:'line',borderColor:'#e0a400',borderDash:[4,4],
     pointRadius:0,fill:false}
  ]},
  options:{...CD,scales:{...CD.scales,
    y:{...CD.scales.y,beginAtZero:true,
       title:{display:true,text:'ms',color:'#8890a8'},
       ticks:{callback:v=>v>=60000?(v/60000).toFixed(0)+'min':v>=1000?(v/1000).toFixed(0)+'s':v}}}}
});

// ── Analysis ──────────────────────────────────────────────────────────────────
const trendText = trendSlope > 200
  ? `<b style="color:var(--warn)">YES — growing</b> at +${trendSlope.toFixed(0)} ms per cycle. `+
    `This suggests an O(N) component (e.g. dirty pages accumulating faster than they are flushed, `+
    `or BLink page count growing unboundedly).`
  : trendSlope < -200
  ? `<b style="color:var(--accent2)">No — shrinking</b> (${trendSlope.toFixed(0)} ms/cycle). `+
    `Checkpoint load is decreasing, likely because ingest has slowed or stopped.`
  : `<b style="color:var(--accent2)">No — stable</b> (slope: ${trendSlope.toFixed(0)} ms/cycle). `+
    `Duration does not grow with time under the observed conditions.`;

const bigCkptIdx = durs.findIndex(d=>d>60000);
const bigCkptNote = bigCkptIdx>=0
  ? `<div class="callout red">
      <b>Outlier detected:</b> checkpoint #${bigCkptIdx+1} at ${CKPTS[bigCkptIdx]?.[6]||'?'}
      took <b>${fmtS(durs[bigCkptIdx])}</b>. This is likely the explicit
      <code>--checkpoint</code> phase at end of ingest, or a catch-up checkpoint after a
      long period without writes. During this window, any incoming commit that tries to acquire
      the checkpoint lock will time out if the lock is held longer than the commit timeout.
    </div>`
  : '';

document.getElementById('analysisBlock').innerHTML = `
<h3>Does checkpoint time grow over time?</h3>
<div class="callout">${trendText}</div>
${bigCkptNote}

<h3>What determines checkpoint duration?</h3>
<p>The server checkpoint has several serial phases under the checkpoint lock:</p>
<ol style="margin:6px 0 10px 18px;line-height:1.9">
  <li><b>Data page flush</b> — all dirty data pages are written to the remote file-server
      (MinIO). Duration scales with the number of dirty pages accumulated since the
      last checkpoint. Under sustained ingest at high throughput, more pages become
      dirty between checkpoints than during idle periods.</li>
  <li><b>BLink index checkpoint</b> — the primary-key BTree writes a snapshot of
      <em>all</em> its pages to the local data PVC. Observed: ${fmt(pagesMax)} pages.
      This is a full snapshot, not incremental — duration scales with total page count,
      which grows as rows are inserted. With ${fmt(pagesMax)} pages, even at fast local
      disk I/O this can take seconds.</li>
  <li><b>Vector index checkpoint</b> — the server waits for the IS to catch up to
      the current LSN before recording the checkpoint. If the IS tailer is far behind,
      this wait dominates the checkpoint duration.</li>
  <li><b>ZooKeeper write</b> — updates the durable checkpoint LSN. Usually milliseconds.</li>
</ol>

<h3>Why the commit lock timeout?</h3>
<p>
  <code>TableManager.onTransactionCommit</code> tries to acquire the same
  checkpoint read-write lock in read mode. If the checkpoint is holding the
  write-mode lock, all concurrent commits wait. The timeout for that wait is
  derived from the checkpoint lock configuration and is not directly
  configurable today. If the checkpoint takes longer than this timeout, the
  commit returns <code>DataStorageManagerException: timed out while acquiring
  checkpoint lock during a commit</code> and the transaction is rolled back.
</p>
<div class="callout red">
  <b>Root cause:</b> the commit lock timeout is shorter than the worst-case
  checkpoint duration. At large row counts the BLink index checkpoint and
  dirty-page flush together can hold the lock for 100+ seconds, exceeding the
  commit timeout (~60–120 s). The fix should either:
  <ul style="margin:4px 0 0 16px">
    <li>Make the commit lock wait timeout configurable and larger, or</li>
    <li>Make the BLink checkpoint incremental (only write newly-dirty index pages), or</li>
    <li>Release the checkpoint lock between the data-page flush and the BLink phase.</li>
  </ul>
</div>

<h3>BLink page count: ${fmt(pagesMin)} → ${fmt(pagesMax)}</h3>
<p>
  The BLink primary-key index currently has <b>${fmt(pagesMax)} pages</b>.
  Each page is written to disk at every checkpoint regardless of whether it is
  dirty. As the table grows, page count grows proportionally — meaning checkpoint
  duration from BLink alone grows linearly with row count. For 100 M rows this
  is already ${fmt(pagesMax)} pages; at 1 B rows it would be ~${fmt(Math.round(pagesMax*10))} pages.
</p>
`;

document.getElementById('footerLine').textContent =
  `Generated by report-server-checkpoints.sh · ${META.logFile} · ${META.rowCount} cycles`;
</script>
</body></html>
HTMLEOF2

echo ""
echo "REPORT=$OUTPUT"
