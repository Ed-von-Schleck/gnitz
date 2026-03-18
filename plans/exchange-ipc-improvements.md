# Exchange & IPC Improvements

Merged and corrected from `exchange-optimizations.md` and `wal-block-improvements.md`.
Prerequisite: Unit 18 (IPC unification, WAL-block wire format) — complete.

---

## Background

The exchange mechanism is a hub-and-spoke IPC topology. For a push that triggers a
SHARD→REDUCE circuit:

```
1. fan_out_push: scatter B by PK → N sub-batches sent to workers (FLAG_PUSH)
2. Worker w: ingest + pre-plan seal (to_consolidated) + execute pre-plan
3. Worker w: do_exchange → sends pre_result to master (FLAG_EXCHANGE)
4. Master: collects all N → clones → merges → repartitions by shard_cols → sends N relays
5. Worker w: post-plan seal (to_consolidated) + execute post-plan (REDUCE)
6. Worker w: ACK
```

The source-optimizations seal (committed) calls `to_consolidated()` at every
`execute_epoch` entry. `op_reduce` also calls `ConsolidatedScope(delta_in)` internally.
For exchange relay batches (step 5), the seal pays O((M/N) log(M/N)) to sort by PK —
a redundant sort that `op_reduce` then discards when it re-sorts by group_by_cols.

### The routing invariant

Every routing decision in the system reduces to: "given a row, which worker owns it?"
This decision is made by two conceptually distinct operations that must agree:

- **Storage routing** — `_partition_for_key(pk_lo, pk_hi)`: which worker stores a row.
- **Computation routing** — `_extract_group_key` → `_partition_for_key`: which worker
  reduces a group.

The exchange round-trip exists because these two functions can disagree. Phases 0–4
progressively close this gap, culminating in the headline result: **for a trivial
pre-plan (INPUT → SHARD) with a consolidated input batch, zero extra sorts occur
anywhere in the system.** Only the unavoidable op_reduce group-by sort remains.

Phase 0's PK guard (item 0b) is also Phase 4's correctness proof: once
`hash_row_by_columns(batch, i, [pk_index])` ≡ `_partition_for_key(pk_lo, pk_hi)`
holds structurally, the co-partition condition `shard_cols == [pk_index]` is provably
equivalent to "push routing and shard routing produce the same sub-batches." Phase 4
relies on this identity without a separate argument.

### The zero-sort chain

The headline result flows through five links that must all be in place:

1. **Phase 0d** (`multi_scatter`): flag propagation marks PK-spec sub-batches
   `_consolidated` when the source is consolidated.
2. **Phase 2c** (IPC flags): `FLAG_BATCH_CONSOLIDATED` encodes and restores the flag
   across the wire for push sub-batches.
3. **Phase 2c** (worker send): workers set `FLAG_BATCH_CONSOLIDATED` on `FLAG_EXCHANGE`
   sends (pre-plan seal guarantees the pre-result is consolidated).
4. **Phase 3** (`repartition_batches_merged`): relay sub-batches are produced
   consolidated and transmitted with the flag.
5. **Post-plan seal** (executor): `to_consolidated()` on a batch with `_consolidated =
   True` short-circuits — O(1).

All five links must be in place before the redundant PK sort disappears.

---

## Phase 0: Routing unification — DONE (cfbde4b..918d014)

`exchange.py` is the single authority for all partition routing. GATHER deleted,
`hash_row_by_columns` PK bug fixed, `PartitionAssignment`/`worker_for_pk`/
`relay_scatter` moved in, `multi_scatter` added with PK-spec flag propagation.

---

## Phase 1: Mechanical efficiency — DONE (b1641ce)

- **1a** `repartition_batch` and `multi_scatter` pass `initial_capacity=batch.length()//num_workers` to avoid repeated buffer growth.
- **1b** `fan_out_push` builds poll lists once and uses backwards swap-remove on ACK. `fan_out_scan` uses the same poll pattern (latency O(max worker) vs O(sum)).
- **1c** `PartitionRouter` in `exchange.py` caches `(table_id, col_idx, key_lo, 0) → worker` for unique secondary indexes. `fan_out_push` records routing after repartitioning; `fan_out_seek_by_index` unicasts on hit, broadcasts on miss.

---

## Phase 2: WAL/IPC infrastructure — DONE (0cdf59d)

- **2a** Removed DDL ACK round-trip (`worker.py`); added `broadcast_batch` to
  `ipc.py` (encode-once via SCM_RIGHTS); `broadcast_ddl` is now a one-liner.
- **2b** Rewrote `WALReader` in `wal.py` to mmap the full file on open;
  `read_next_block` uses `decode_batch_from_ptr` into the live mmap — zero allocs,
  zero copies per block. Deleted `_has_rotated`. Added `test_zero_copy_wal_recovery`.
- **2c** Added `FLAG_BATCH_SORTED` (1<<50) and `FLAG_BATCH_CONSOLIDATED` (1<<51)
  to `ipc.py`; `serialize_to_memfd` sets them; `_recv_and_parse` restores them.
  Push sub-batch benefit deferred to Phase 4 (`multi_scatter`); `FLAG_EXCHANGE`
  path active immediately (pre-plan seal guarantees consolidated output).
  Added IPC roundtrip tests + `repartition_batch` non-propagation test.

---

## Phase 3: Exchange relay synthesis

Replaces the current clone→merge→repartition pipeline with a no-clone k-way merge
approach. Requires Phase 2c (`FLAG_BATCH_CONSOLIDATED` on `FLAG_EXCHANGE`).

### The redundant sort problem

With Phase 2c in place, the exchange path at the master carries full flag information:

1. Workers' pre-plan seal: `to_consolidated()` on push sub-batch → output consolidated.
2. Workers send `FLAG_EXCHANGE` with `FLAG_BATCH_CONSOLIDATED`.
3. Master has N consolidated sorted runs — each worker's PK partition is disjoint, so
   PKs across all N sources are globally unique.
4. Master currently: **clones** each source → **merges** all N → **scatter**s
   sequentially → sends unsorted relay with no flag.
5. Workers' post-plan seal: `to_consolidated()` on unsorted relay →
   **O((M/N) log(M/N)) PK sort** + O(M/N) consolidation pass.
6. `op_reduce` `ConsolidatedScope`: short-circuits (already consolidated).
7. `op_reduce` `_argsort_delta`: **O((M/N) log(M/N)) group-by sort** — unavoidable.

Step 5 is a redundant PK sort: it satisfies the consolidated invariant before REDUCE,
which immediately discards PK order to re-sort by group_by_cols.

### Architecture

**Key constraint:** to produce consolidated relay sub-batches, the master needs all N
source batches simultaneously to iterate them in merged PK order. This requires
deferring `payload.close()` until all N are collected — the mmap views must stay alive
during the merge. This is incompatible with streaming scatter (close-immediately after
each arrival). The choice is made here in favour of consolidated output.

**Collect loop** (`fan_out_push`, exchange arm):

Replace `exchange_buffers[vid][w] = payload.batch.clone()` + immediate
`payload.close()` with deferred storage:

```python
# exchange_payloads: view_id -> [IPCPayload | None] * num_workers
if vid not in exchange_payloads:
    exchange_payloads[vid] = [None] * self.num_workers
    exchange_counts[vid] = 0
exchange_payloads[vid][w] = payload        # store; do NOT close
exchange_counts[vid] += 1
# (schema, source_id bookkeeping unchanged)

if exchange_counts[vid] == self.num_workers:
    self._relay_exchange(vid, exchange_payloads[vid], ex_schema, src_id)
    del exchange_payloads[vid]
    del exchange_counts[vid]
    if vid in exchange_schemas: del exchange_schemas[vid]
    if vid in exchange_source_ids: del exchange_source_ids[vid]
    # payloads closed inside _relay_exchange
```

**`gnitz/dbsp/ops/exchange.py`** — two new functions (alongside `repartition_batch`):

```python
def repartition_batches(source_batches, shard_col_indices, num_workers, assignment):
    """Scatter N source batches into N dest batches without an intermediate merge.
    Sources are iterated sequentially. Output is NOT marked consolidated.
    Fallback when sources are not all consolidated."""
    ...

def repartition_batches_merged(source_batches, shard_col_indices, num_workers,
                                assignment):
    """Scatter N consolidated source batches into N dest batches in merged PK order.

    Requires all non-None source_batches to have _consolidated = True.
    Produces consolidated dest batches because:
      - Each source has unique PKs (pre-plan seal).
      - PK partition ranges are disjoint across workers.
      - Iterating in merged PK order within each dest sub-batch preserves monotonicity.

    Marks dest_batches[w]._consolidated = True before returning.

    For small fixed N (≤ 8): linear scan over N current-row pointers per output row —
    O(M × N) total. No heap required; RPython-safe.
    For larger N: tournament tree."""
    ...
```

**`_relay_exchange`** (`master.py`) — after Phase 0 and Phase 3, this becomes 5 lines
of orchestration with all routing logic in `relay_scatter` (exchange.py):

```python
def _relay_exchange(self, view_id, payloads, schema, source_id=0):
    shard_cols = []
    if source_id > 0:
        shard_cols = self.program_cache.get_join_shard_cols(view_id, source_id)
    if len(shard_cols) == 0:
        shard_cols = self.program_cache.get_shard_cols(view_id)
    sources = [p.batch if p is not None else None for p in payloads]
    dest_batches = relay_scatter(sources, shard_cols, self.num_workers, self.assignment)
    for p in payloads:
        if p is not None: p.close()
    for w in range(self.num_workers):
        ipc.send_batch(self.worker_fds[w], view_id, dest_batches[w], schema=schema)
        if dest_batches[w] is not None: dest_batches[w].free()
```

`shard_cols` is always non-empty here (GATHER deleted in Phase 0a; all exchange views
use SHARD). `relay_scatter` has no GATHER fallback.

### Memory profile

| Approach | Peak exchange memory |
|---|---|
| Current | O(3M): N clones + 1 merged batch + N dest batches |
| Phase 3 (this) | O(2M): N open mmap views + N dest batches |

### Performance effect on the exchange cycle

| Step | Before Phase 3 | After Phase 3 |
|---|---|---|
| Clone per exchange message | N allocs + N copies | 0 |
| Merge all N into merged batch | O(M) copy | 0 |
| Repartition | O(M) sequential scatter | O(M × N) merge-scatter ≈ O(4M) for N=4 |
| Relay batch flag | no flag | `FLAG_BATCH_CONSOLIDATED` |
| Post-plan seal per worker | O((M/N) log(M/N)) | O(1) short-circuit |
| `op_reduce` `ConsolidatedScope` | O(1) (already consolidated) | O(1) |

The master's repartition costs O(M × N) instead of O(M) — a factor of N overhead for
the merge scan. For N=4 this is O(4M). This replaces N parallel worker seals at
O((M/N) log(M/N)) wall-clock each. For M=10 000, N=4: master adds 4M ≈ 40 000 ops;
workers save 4 × 25 000 × log(25 000)/2 ≈ 730 000 parallel ops (wall-clock ≈ 182 500).
The merge overhead is recouped at M ≳ several hundred rows per push.

For very small batches (M < ~50 per push), sequential scatter (`repartition_batches`
without merge) is cheaper; `relay_scatter` provides the branch via the consolidated check.

---

## Phase 4: Exchange round-trip elimination

For `INPUT → SHARD → REDUCE` circuits (identity pre-plan), the entire exchange
round-trip is eliminable. Depends on Phase 3 infrastructure; Phase 2c flag propagation
is automatic after Phase 0d.

### The headline result

Phase 2c + Phase 4 compose to produce the maximum possible efficiency:

1. Client sends consolidated batch.
2. `multi_scatter` with `[pk_index]` as first spec: sub-batches inherit `_consolidated`
   (automatic in `multi_scatter`).
3. `multi_scatter` with `shard_cols`: preloaded exchange sub-batches produced.
4. Master sends `FLAG_PRELOADED_EXCHANGE` + `FLAG_PUSH` carrying consolidated flags.
5. Worker's post-plan seal sees `_consolidated = True` → O(1) short-circuit.
6. `op_reduce ConsolidatedScope` → O(1). `_argsort_delta` → O((M/N) log(M/N)) —
   unavoidable group-by sort, the only remaining work.

**For trivial pre-plan with consolidated input: zero extra sorts anywhere in the
system.** Only the group-by sort in op_reduce remains.

This result depends on Phase 0b's PK guard establishing `hash_row_by_columns(batch, i,
[pk_index])` ≡ `_partition_for_key(pk_lo, pk_hi)`. Without that structural identity,
the co-partition condition below would require a separate correctness argument.

### Co-partitioned views: zero messages

A view V is **co-partitioned** with its source table T when `shard_cols == [pk_index]`
(i.e., GROUP BY the primary key column). In this case PK routing and shard routing
produce identical sub-batches — the preloaded exchange for worker w equals the push
sub-batch for worker w.

Detect in `get_exchange_info`: return `is_co_partitioned` flag alongside `is_trivial`.
For co-partitioned views, master sends no `FLAG_PRELOADED_EXCHANGE`. Worker, on
receiving `FLAG_PUSH` for table T, auto-stashes its push batch for each co-partitioned
view depending on T:

```python
copart_views = self.program_cache.get_copartitioned_views(target_id)
for vid in copart_views:
    self.exchange_handler._stash[vid] = batch.clone()
```

When evaluate_dag calls `do_exchange` for that view, it returns the stash directly.
No IPC at all: N fewer messages per push for each co-partitioned view.

### Co-partitioned joins: same mechanism

`plan.join_shard_map[source_id]` gives join shard cols. If
`shard_cols == [source_schema.pk_index]`, the join rows are already on the correct
worker (they arrived via PK routing, which equals join-key routing). In executor.py:

```python
shard_cols = plan.join_shard_map[source_id]
if shard_cols == [incoming_delta._schema.pk_index]:
    # Co-partitioned join: no exchange needed
    out_delta = plan.execute_epoch(incoming_delta, source_id=source_id)
else:
    exchanged = exchange_handler.do_exchange(target_view_id, incoming_delta,
                                             source_id=source_id)
    out_delta = plan.execute_epoch(exchanged, source_id=source_id)
```

No new infrastructure. The same `shard_cols == [pk_index]` check handles both GROUP BY
and JOIN co-partitioning.

### Changes

**`gnitz/catalog/program_cache.py`** — extend `get_shard_cols` to
`get_exchange_info(view_id) → (shard_cols, is_trivial, is_co_partitioned)`:

```python
def get_exchange_info(self, view_id):
    if view_id in self._exchange_info_cache:
        return self._exchange_info_cache[view_id]
    # load nodes, edges from system tables
    # find SHARD node; collect shard_cols from params
    # is_trivial = True iff SHARD has exactly 1 incoming edge from a node with in_degree 0
    # is_co_partitioned = is_trivial and shard_cols == [source_pk_index]
    result = (shard_cols, is_trivial, is_co_partitioned)
    self._exchange_info_cache[view_id] = result
    return result

def get_shard_cols(self, view_id):
    shard_cols, _, _ = self.get_exchange_info(view_id)
    return shard_cols

def get_trivial_exchange_views(self, source_table_id, registry):
    """Returns (non_copartitioned, copartitioned) view lists."""
    dep_map = self.get_dep_map(registry)
    non_copart = []
    copart = []
    for vid in dep_map.get(source_table_id, []):
        shard_cols, is_trivial, is_copart = self.get_exchange_info(vid)
        if is_trivial and len(shard_cols) > 0:
            if is_copart:
                copart.append(vid)
            else:
                non_copart.append((vid, shard_cols))
    return non_copart, copart

def get_copartitioned_views(self, source_table_id, registry):
    _, copart = self.get_trivial_exchange_views(source_table_id, registry)
    return copart
```

Add `_exchange_info_cache = {}` to `__init__`; invalidate in `invalidate` /
`invalidate_all`.

**`gnitz/server/ipc.py`**:
```python
FLAG_PRELOADED_EXCHANGE = r_uint64(1 << 10)   # 1024
```

**`gnitz/server/master.py`** — `fan_out_push`: combined pass via `multi_scatter`:

```python
non_copart, copart = self.program_cache.get_trivial_exchange_views(target_id, registry)

col_specs = [[schema.pk_index]] + [sc for _, sc in non_copart]
all_batches = multi_scatter(batch, col_specs, self.num_workers, self.assignment)
pk_batches = all_batches[0]

# Send preloaded frames BEFORE FLAG_PUSH (FIFO ordering guarantees arrival order)
for w in range(self.num_workers):
    for si, (vid, sc) in enumerate(non_copart):
        ipc.send_batch(self.worker_fds[w], vid, all_batches[si + 1][w],
                       flags=ipc.FLAG_PRELOADED_EXCHANGE)
    ipc.send_batch(self.worker_fds[w], target_id, pk_batches[w],
                   flags=ipc.FLAG_PUSH)
```

Non-trivial views go through the Phase 3 relay path unchanged.

**`gnitz/server/worker.py`** — `WorkerExchangeHandler`:

```python
class WorkerExchangeHandler(object):
    def __init__(self, master_fd):
        self.master_fd = master_fd
        self._stash = {}   # view_id -> ArenaZSetBatch

    def stash_preloaded(self, view_id, batch):
        if batch is not None and batch.length() > 0:
            self._stash[view_id] = batch.clone()

    def do_exchange(self, view_id, batch, source_id=0):
        if view_id in self._stash:
            return self._stash.pop(view_id)
        # Normal path (unchanged)
        ...
```

`_handle_request`: drain all leading `FLAG_PRELOADED_EXCHANGE` frames before
dispatching `FLAG_PUSH`:

```python
while payload.flags & ipc.FLAG_PRELOADED_EXCHANGE:
    self.exchange_handler.stash_preloaded(intmask(payload.target_id), payload.batch)
    payload.close()
    payload = ipc.receive_payload(self.master_fd)
# continue with normal dispatch
```

### Impact

For `INPUT → SHARD → REDUCE` (every simple GROUP BY without WHERE or computed group
expressions):

| Metric | Before | After (non-copart) | After (co-partitioned) |
|---|---|---|---|
| IPC messages per push | 3N | N+1 (N preload + N push, pipelined) | N |
| Round-trips | 2N | N | N |
| Master work | O(2 × total_rows) | O(total_rows) one `multi_scatter` | O(total_rows) |
| `_relay_exchange` called | yes | no | no |
| Post-plan seal | O((M/N) log(M/N)) | O(1) short-circuit | O(1) short-circuit |
| Extra sorts | 1 (redundant PK sort) | 0 | 0 |

---

## Phase 5: Shared memory IPC

Long-range. Depends on Phase 3 (pointer-agnostic `decode_batch_from_ptr`). Phase 2b
exercises the same `decode_batch_from_ptr` zero-copy path in WAL recovery tests,
providing validation coverage before Phase 5 uses it in the production hot path.

### Shared slot arena

Before forking workers, master allocates one `MAP_SHARED | MAP_ANONYMOUS` region:

```
m2w_push[N]   (N × slot_size)  — master writes push sub-batches, workers read
w2m_exch[N]   (N × slot_size)  — workers write exchange batches, master reads
m2w_exch[N]   (N × slot_size)  — master writes relay batches, workers read
```

Workers inherit the mapping through `fork()`. Socket fds carry only 1-byte signals;
`sendmsg` / `recv_fd` (SCM_RIGHTS) eliminated from the hot path.

After Phase 0, all scatter goes through `repartition_batch` / `multi_scatter` in
`exchange.py`. Add `scatter_to_slots` that fuses routing and encoding into one pass,
writing rows directly into the pre-allocated shared memory slot for each destination
worker — no intermediate `ArenaZSetBatch` allocation:

```python
def scatter_to_slots(batch, col_indices, slot_ptrs, slot_sizes, schema, assignment):
    """Route and encode rows directly into shared memory slots in one pass."""
    for i in range(batch.length()):
        w = assignment.worker_for_partition(hash_row_by_columns(batch, i, col_indices))
        encode_row_append(batch, i, slot_ptrs[w], slot_sizes[w])
```

`scatter_to_slots` is `multi_scatter` at the memory level: same routing call
(`hash_row_by_columns` → `worker_for_partition`), encoding into pre-allocated shared
slots instead of `ArenaZSetBatch`. Phase 0's routing consolidation makes this a clean
optimization of an already-correct path.

Workers call `decode_batch_from_ptr(slot_ptrs[w], size, schema)` — the same call path
as the memfd IPC path.

Fallback for oversized batches: signal byte 0xFF triggers the existing memfd path.
For push sub-batches (already 1/N of the original), overflow is unlikely.

Syscall count per `fan_out_push`, N=4: 40 (memfd path) → 8 (shared slots: 4 writes +
4 reads).

### Exchange fault tolerance

Before sending an exchange batch, worker writes it to a local WAL (one file per
view_id in the worker's base_dir). After receiving the master's relay, truncate.
If the worker crashes before truncation, it replays the WAL on restart and resends.
Exactly-once exchange semantics from the format identity — no new infrastructure beyond
a per-worker WAL path with `truncate_before_lsn`.

---

## Implementation order

```
Phase 0 ─── prerequisite for all phases ────────────────────────────────
  [0a] Delete GATHER: OPCODE_EXCHANGE_GATHER, PARAM_GATHER_WORKER,
       CircuitBuilder.gather(), GATHER arm in program_cache,
       exchange_shard_cols from ExecutablePlan, shard_cols param from
       do_exchange, else:_split_batch_by_pk from _relay_exchange
       (opcodes.py, circuit_builder.py, program_cache.py, runtime.py,
        executor.py, worker.py, master.py, ~-40 lines net)
  [0b] Fix hash_row_by_columns + import _extract_group_key
       delete _mix64 / _read_col_or_pk / _read_col_u128_parts / _hash_string_col
       (exchange.py, linear.py, ~-50 lines net)
  [0c] Move PartitionAssignment, add worker_for_pk, add relay_scatter,
       delete _split_batch_by_pk, update all call sites
       (exchange.py, master.py, main.py, ~-30 lines net)
  [0d] Add multi_scatter              (exchange.py, ~30 lines)

Phase 1 ─── DONE (b1641ce) ──────────────────────────────────────────────
  [1a] Pre-allocation hints            DONE
  [1b] Poll loop uniformity            DONE
  [1c] Master-side index routing       DONE

Phase 2 ─── DONE (0cdf59d) ──────────────────────────────────────────────
  [2a] DDL ACK removal + broadcast_batch  DONE
  [2b] Zero-copy WAL recovery             DONE
  [2c] Batch property IPC flags           DONE
       ↓ enables Phase 3

Phase 3 ─── sequential within phase ────────────────────────────────────
  [3a] repartition_batches             (exchange.py, ~25 lines)
  [3b] repartition_batches_merged + update relay_scatter to dispatch between
       repartition_batches and repartition_batches_merged based on _consolidated
                                       (exchange.py, ~45 lines)
  [3c] Collect loop + _relay_exchange  (master.py, ~30 lines)
       ↓ enables Phase 4

Phase 4 ─── after Phase 3 ──────────────────────────────────────────────
  [4a] get_exchange_info + co-partitioned detection
                                       (program_cache.py, ~40 lines)
  [4b] multi_scatter fan_out_push      (master.py, ~25 lines)
  [4c] WorkerExchangeHandler stash +
       _handle_request drain loop      (worker.py, ~30 lines)
  [4d] Co-partitioned join elimination (executor.py, ~8 lines)

Phase 5 ─── long-range ─────────────────────────────────────────────────
  [5a] Shared slot arena + scatter_to_slots
                                       (exchange.py, master.py, worker.py, ~120 lines)
  [5b] Exchange fault tolerance        (worker.py, ~40 lines)
```

Each item is one commit. Run `make test` after each.

---

## Tests

All tests run with `GNITZ_WORKERS=4`.

**Phase 0:**
- `exchange_test` — `test_gather_deleted`: verify `OPCODE_EXCHANGE_GATHER` does not
  exist in `opcodes` module; verify `compile_from_graph` on a circuit graph containing
  opcode 21 raises `LayoutError` (or is skipped cleanly).
- `exchange_test` — `test_hash_row_pk_col`: create a batch, verify
  `hash_row_by_columns(batch, i, [schema.pk_index])` produces the same partition as
  `_partition_for_key(pk_lo, pk_hi)` for all rows including U128 PKs with non-zero hi.
- `exchange_test` — `test_repartition_batch_pk_equals_split`: verify
  `repartition_batch(batch, [schema.pk_index], ...)` is byte-for-byte identical to the
  former `_split_batch_by_pk` result (golden-value test against a known input).
- All existing exchange + multi-worker tests must continue to pass.

**Phase 1:** existing exchange + multi-worker tests cover correctness.

**Phase 2:** DONE — `master_worker_test`, `storage_comprehensive_test`
(`test_zero_copy_wal_recovery`), `exchange_test` (flag roundtrip + non-propagation).

**Phase 3:**
- `exchange_test` — `test_repartition_batches_merged`: N pre-sorted inputs with
  disjoint PK ranges; assert dest sub-batches are consolidated.
- `test_workers.py` — `test_exchange_relay_consolidated`: push rows to a GROUP BY
  view; assert relay sub-batches arrive at workers with `_consolidated = True`.

**Phase 4:**
- `test_workers.py` — `test_trivial_preplan_no_exchange`: create table T, view
  `SELECT group_col, COUNT(*) FROM T GROUP BY group_col`; push 100 rows across
  multiple transactions; assert correct COUNT per group; confirm `_relay_exchange`
  is never called for V (counter or log check).
- `test_workers.py` — `test_copartitioned_view_no_preload`: create table T with PK
  column `id`, view `SELECT id, COUNT(*) FROM T GROUP BY id`; verify no
  `FLAG_PRELOADED_EXCHANGE` is sent (counter check); assert correct results.
- `test_workers.py` — `test_copartitioned_join`: JOIN on PK column; verify
  `FLAG_EXCHANGE` is never sent; assert correct join results.
- `test_workers.py::test_push_scan_multiworker` — regression for routing correctness.
