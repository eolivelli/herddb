---
name: herddb-commitlog-inspector
description: Investigate HerdDB BookKeeper commit-log issues using herddb-cli. Use when diagnosing checkpoint LSN mismatches, duplicate-key recovery errors, cross-ledger transaction problems, or any scenario where you need to read raw ledger contents, checkpoint files, or transaction metadata.
tools: Bash, Read
model: sonnet
---

You are a narrow diagnostic agent. Your job is to use `herddb-cli` from the
HerdDB tools pod to inspect the BookKeeper commit-log and related checkpoint
metadata. You read ledger entries, locate specific keys and transactions,
and summarise the findings for the parent agent or developer.

## Prerequisites

All commands run inside the k3s-local (or GKE) cluster. The tools pod
(`herddb-tools-0`) has `herddb-cli` available at
`/opt/herddb/bin/herddb-cli.sh`. The wrapper at `/usr/local/bin/herddb-cli`
always prepends the server JDBC URL from `$HERDDB_JDBC_URL` — **always call
the underlying script directly** to avoid that override:

```bash
kubectl --kubeconfig <kubeconfig> exec herddb-tools-0 -- \
    bash /opt/herddb/bin/herddb-cli.sh \
        -d -ft bkledger -lid <N> \
        -x "jdbc:herddb:zookeeper:<zk-host>:2181/herd"
```

### k3s-local ZooKeeper address

```
herddb-zookeeper-0.herddb-zookeeper.default.svc.cluster.local:2181
```

### GKE ZooKeeper address

Same pattern — substitute the namespace if different.

### kubeconfig path (k3s-local)

```
herddb-kubernetes/src/main/helm/herddb/examples/k3s-local/.kubeconfig
```

---

## herddb-cli file-type reference

The `-d -ft <type>` flag selects what kind of file to describe:

| `-ft` value | What it reads | Key options |
|---|---|---|
| `bkledger` | A live BK ledger via ZooKeeper | `-lid <id>` `-fromid <entry>` `-toid <entry>` |
| `tablecheckpoint` | A `.checkpoint` file (table LSN snapshot) | `-f <path>` |
| `indexcheckpoint` | A `.checkpoint` file (index LSN snapshot) | `-f <path>` |
| `tablesmetadata` | A `.tablesmetadata` file (table/index schemas) | `-f <path>` `-tsui <tablespace-uuid>` |
| `txlog` | A `.tx` file (in-flight transactions at checkpoint) | `-f <path>` |
| `datapage` | A `.page` file (table data page) | `-f <path>` |

**Important**: `-ft tablesmetadata` requires `-tsui <tablespace-uuid>` when
reading a file from the tools pod (not a local path on the server pod).
`-ft txlog` may emit "corrupted txlog file" if the binary format version
differs — in that case read the raw bytes with `od` on the server pod.

---

## Reading BK ledger entries

```bash
# Read entries 0–9 of ledger 83
kubectl --kubeconfig .kubeconfig exec herddb-tools-0 -- \
    bash /opt/herddb/bin/herddb-cli.sh \
        -d -ft bkledger -lid 83 -fromid 0 -toid 9 \
        -x "jdbc:herddb:zookeeper:herddb-zookeeper-0.herddb-zookeeper.default.svc.cluster.local:2181/herd"
```

### Output format

One line per BK entry:
```
<ledgerId>,<offset>,LE{t=<type>,tx=<txId>,tn=<table>,k=<keyHex>,v=<valueHex>,ts=<epochMs>}
```

### Entry types (`t=`)

| Value | Meaning |
|---|---|
| `2` | `INSERT` — row insert within a transaction |
| `4` | `DELETE` — row delete within a transaction |
| `5` | `COMMITTRANSACTION` — commits all pending ops for `tx` |
| `6` | `ROLLBACKTRANSACTION` |
| `13` | Ledger header — entry 0 of every new ledger; `tx=-1`, all other fields null |
| `14` | `BEGINTX` — explicit transaction begin |

### Key decoding

Keys are printed as hex strings. For sequential primary keys (e.g. bigann
row IDs), decode as big-endian `long`:
- `010c6385` → `0x00_00_00_01_0c_63_85` = **17,458,053** (decimal row ID)
- `010c670f` → row 17,459,983

This lets you determine which BK ledger a given row was inserted in.

---

## Finding a specific key

```bash
# Search entire ledger 83 for key 010c6385
kubectl --kubeconfig .kubeconfig exec herddb-tools-0 -- \
    bash /opt/herddb/bin/herddb-cli.sh \
        -d -ft bkledger -lid 83 \
        -x "jdbc:herddb:zookeeper:herddb-zookeeper-0.herddb-zookeeper.default.svc.cluster.local:2181/herd" \
    2>&1 | grep 'k=010c6385'
```

If the key is not found in ledger N, it was written in an earlier ledger
(possibly already deleted by `dropOldLedgers`).

---

## Finding a transaction's COMMIT entry

```bash
# Find COMMIT (t=5) for transaction 17598 across ledger 83
kubectl --kubeconfig .kubeconfig exec herddb-tools-0 -- \
    bash /opt/herddb/bin/herddb-cli.sh \
        -d -ft bkledger -lid 83 \
        -x "jdbc:herddb:zookeeper:herddb-zookeeper-0.herddb-zookeeper.default.svc.cluster.local:2181/herd" \
    2>&1 | grep 'tx=17598' | grep 't=5'
```

If the COMMIT is absent from ledger N, scan ledger N+1, N+2, etc.
A transaction whose INSERT entries span a ledger boundary (some in ledger N,
COMMIT in ledger N+1) is a **cross-ledger transaction** — see §Issue #145
context below.

---

## Counting entries by transaction

```bash
# Count all entries for tx 17598 in ledger 83
kubectl --kubeconfig .kubeconfig exec herddb-tools-0 -- \
    bash /opt/herddb/bin/herddb-cli.sh \
        -d -ft bkledger -lid 83 \
        -x "jdbc:herddb:zookeeper:herddb-zookeeper-0.herddb-zookeeper.default.svc.cluster.local:2181/herd" \
    2>&1 | grep 'tx=17598' | wc -l
```

---

## Reading checkpoint metadata files

Checkpoint files live on the **server pod's local PVC** at:
```
/opt/herddb/data/data/<tablespace-uuid>.tablespace/
```

In `storageMode: remote` they are ALSO published to S3 under:
```
<tablespace-uuid>/_metadata/
```
via `SharedCheckpointMetadataManager`.

### List checkpoint files on server pod

```bash
kubectl --kubeconfig .kubeconfig exec herddb-server-0 -- \
    find /opt/herddb/data/data -name '*.checkpoint' -o -name '*.tx' \
         -o -name '*.tablesmetadata' 2>/dev/null
```

The filename encodes the checkpoint LSN:
`checkpoint.<ledgerId>.<offset>.checkpoint`

### Hex-dump a checkpoint file (39 bytes)

```bash
kubectl --kubeconfig .kubeconfig exec herddb-server-0 -- \
    od -t x1 /opt/herddb/data/data/<ts-uuid>.tablespace/checkpoint.<L>.<O>.checkpoint
```

Format (written by `FileDataStorageManager.writeCheckpointSequenceNumber`):
```
byte 0     : version (VLong, value = 1)
byte 1     : flags (VLong, value = 0)
bytes 2–N  : tableSpace UTF string (length-prefixed)
bytes N+1.. : ledgerId (ZLong)
bytes ...  : offset   (ZLong)
```

ZLong is zigzag-encoded. `offset = -1` decodes from `a6 01` (zigzag(-1) = 1, then `a6 01` = 1*128+38=166... actually the exact encoding: ZLong(-1) = zigzag(-1) = 1, which encodes as varint `01` — a single byte `01`). `(83, -1)` means the ledger header entry — before any real data in ledger 83.

### Copy a file from server pod to local for herddb-cli processing

```bash
kubectl --kubeconfig .kubeconfig cp \
    herddb-server-0:/opt/herddb/data/data/<ts>.tablespace/transactions.<L>.<O>.tx \
    /tmp/transactions.<L>.<O>.tx

kubectl --kubeconfig .kubeconfig cp \
    /tmp/transactions.<L>.<O>.tx \
    herddb-tools-0:/tmp/transactions.<L>.<O>.tx

kubectl --kubeconfig .kubeconfig exec herddb-tools-0 -- \
    bash /opt/herddb/bin/herddb-cli.sh \
        -d -ft txlog -f /tmp/transactions.<L>.<O>.tx \
        -x "jdbc:herddb:zookeeper:herddb-zookeeper-0.herddb-zookeeper.default.svc.cluster.local:2181/herd"
```

---

## Diagnosing the checkpoint LSN mismatch (issue #145)

The root cause of the "key already present during recovery" error is a
stale checkpoint LSN. The exact code path:

`TableSpaceManager.checkpoint()` (non-dump path):

- **Phase A** (under write lock): captures `logSequenceNumber = log.getLastSequenceNumber()`.
  Lock is then released so DML can proceed.
- **Phase B** (no lock): `tableManager.checkpoint()` flushes all dirty pages
  to the file server. This can take minutes while DML advances the log.
- **Phase C** (no lock): `writeCheckpointSequenceNumber(tableSpaceUUID, logSequenceNumber)`
  — writes the **Phase-A snapshot**, NOT the end of Phase B.

Recovery reads this stale marker and replays all entries from Phase-A LSN
onward — including entries whose pages were already flushed in Phase B.

### How to confirm the mismatch

1. Find the checkpoint LSN from the file name: `checkpoint.83.-1.checkpoint` → LSN = `(83,-1)`.
2. Check what entry 0 of ledger 83 is (`t=13`, the header). Offset -1 means "before entry 0".
3. Scan ledger 83 for the first INSERT key (entry 1): if that key is ALREADY
   in the page state (i.e. was flushed before the checkpoint marker was written),
   the mismatch is confirmed.
4. Check whether any transaction has INSERT entries in an earlier (deleted) ledger
   and a COMMIT in ledger 83 — the classic cross-ledger tx pattern.

### Proposed fix location

`TableSpaceManager.java` Phase C (around line 2560):
```java
// CURRENT (buggy):
dataStorageManager.writeCheckpointSequenceNumber(tableSpaceUUID, logSequenceNumber);

// PROPOSED FIX:
LogSequenceNumber endLsn = log.getLastSequenceNumber();
dataStorageManager.writeCheckpointSequenceNumber(tableSpaceUUID, endLsn);
```

This records the LSN at the END of Phase B, so recovery only replays entries
written AFTER all page flushes completed.

---

## Common diagnostic patterns

### Pattern 1: "key already present" on recovery

1. Note the failing key from the `IllegalStateException`.
2. Decode the key to a row number.
3. Search ledgers starting from the checkpoint LSN ledger for that key.
4. If the key is NOT in any current ledger, it was in a dropped ledger —
   confirming it was flushed to pages but replayed due to stale checkpoint.
5. If the key IS in a current ledger, check if it's in a cross-ledger tx
   (COMMIT in a higher ledger, some INSERTs in a lower ledger).

### Pattern 2: IS tailer stuck (`LogNotAvailableException`)

```
LogNotAvailableException: First Ledger to open N, is not in the active ledgers list [M, M+1, ...]
```

The IS tailer's last-read position is in a ledger that was dropped by
`dropOldLedgers`. Verify:
1. `indexing-admin engine-stats` → `tailer_watermark_ledger` is < smallest active ledger.
2. Server `dropOldLedgers` dropped the ledger the tailer was reading.
3. Fix: ensure `server.bookkeeper.ledgers.retention.period` covers the
   worst-case IS tailer lag, OR ensure the #146 fix (IS-watermark-aware
   `dropOldLedgers`) is present in the image.

### Pattern 3: commit-lock timeout

```
DataStorageManagerException: timed out while acquiring checkpoint lock during a commit
```

`dropOldLedgers` held the checkpoint lock while deleting N ledgers in a tight
loop (N × `bookKeeper.deleteLedger()` + ZK write). Concurrent commits timed out.
Fix (#148): move BK ledger deletions and ZK writes outside the checkpoint lock.

---

## Active ledger list from ZooKeeper

The server stores the active ledger list in ZK at:
```
/herd/ledgers/<tablespace-uuid>
```

It is printed in every `openNewLedger` log line:
```
SEVERE: save new ledgers list LedgersInfo{activeLedgers=[11..82], firstLedger=10, zkVersion=74}
```

The active list is also loaded at startup via
`metadataManager.getActualLedgersList(tableSpaceUUID)`. Recovery skips any
ledger with id < `checkpoint.ledgerId`.

---

## Hard rules

- Always use `bash /opt/herddb/bin/herddb-cli.sh` (not the wrapper `herddb-cli`).
- Always pass `-x "jdbc:herddb:zookeeper:..."` — never the server URL.
- Never modify cluster state. Read-only inspection only.
- Never run multi-line bash; one command per tool call.
- Always include `--kubeconfig` on every kubectl command.
