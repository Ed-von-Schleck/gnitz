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

## Phase 3: Exchange relay synthesis — DONE

No-clone k-way merge relay. `repartition_batches` (sequential fallback) and
`repartition_batches_merged` (linear-scan merge, N≤8) added to `exchange.py`.
`relay_scatter` dispatches between them based on `_consolidated`. Collect loop in
`fan_out_push` defers `payload.close()` until all N arrive; `_relay_exchange` reduced
to 5-line orchestration calling `relay_scatter`. Peak exchange memory: O(2M) vs O(3M).
Post-plan seal on consolidated relay sub-batches: O(1) short-circuit.

---

## Phase 4: Exchange round-trip elimination

For `INPUT → SHARD → REDUCE` circuits (identity pre-plan), the entire exchange
round-trip is eliminable. Depends on Phase 3; Phase 2c flag propagation is automatic
after Phase 0d.

### Infrastructure already done

- **`multi_scatter`** (`exchange.py`) — scatters one batch by multiple col-specs in one
  pass; PK-spec sub-batches inherit `_consolidated`/`_sorted` from source. DONE.
- **`join_shard_map`** (`runtime.py`) — `ExecutablePlan` carries `source_id →
  [col_idx]` for join sharding, built in `compile_from_graph`. DONE.
- **Multi-worker join executor branch** (`executor.py`) — detects `join_shard_map` and
  exchanges before circuit execution. DONE.

### Still to implement

**4a — `get_exchange_info` + preloadable-view helper** (`program_cache.py`):

Extend `get_shard_cols` to `get_exchange_info(view_id) → (shard_cols, is_trivial,
is_co_partitioned)`. Add `_exchange_info_cache`; invalidate on `invalidate`/`invalidate_all`.
`is_trivial` = SHARD has exactly 1 incoming edge from an in-degree-0 node.
`is_co_partitioned` = `is_trivial and shard_cols == [source_pk_index]`.

Add `get_preloadable_views(source_table_id) → [(view_id, shard_cols)]` — views where
`is_trivial and not is_co_partitioned`. Co-partitioned views are excluded because their
exchange is eliminated entirely on the worker side (4d); no master-side preload is needed
or correct for them.

**4b — Combined `multi_scatter` `fan_out_push`** (`master.py`):

```python
preloadable = self.program_cache.get_preloadable_views(target_id)
col_specs = [[schema.pk_index]] + [sc for _, sc in preloadable]
all_batches = multi_scatter(batch, col_specs, self.num_workers, self.assignment)
pk_batches = all_batches[0]
# Send preloaded frames BEFORE FLAG_PUSH
for w in range(self.num_workers):
    for si, (vid, sc) in enumerate(preloadable):
        ipc.send_batch(self.worker_fds[w], vid, all_batches[si+1][w],
                       flags=ipc.FLAG_PRELOADED_EXCHANGE)
    ipc.send_batch(self.worker_fds[w], target_id, pk_batches[w], flags=ipc.FLAG_PUSH)
```

Add `FLAG_PRELOADED_EXCHANGE = 1024` (plain int, consistent with `FLAG_PUSH = 32`,
`FLAG_EXCHANGE = 16`, etc.) to `ipc.py`. Do NOT use `r_uint64(1 << 10)` — worker code
does `flags = intmask(payload.flags)` and compares with plain-int flags; mixing with
`r_uint64` would fail RPython annotation.
Non-trivial views continue through the Phase 3 relay path.

**4c — `WorkerExchangeHandler` stash + drain loop** (`worker.py`):

Add `_stash = {}` dict and `stash_preloaded(view_id, batch)` to handler.
`do_exchange`: check `_stash` first, pop and return if hit.
`_handle_request`: drain all leading `FLAG_PRELOADED_EXCHANGE` frames before
dispatching `FLAG_PUSH`.

**4d — Co-partitioned exchange elimination (compile-time)** (`runtime.py`,
`program_cache.py`, `executor.py`):

The co-partition condition is fully knowable at compile time. Encoding it on
`ExecutablePlan` keeps `evaluate_dag` O(1) and makes the plan self-describing.

*`runtime.py`* — add two fields to `ExecutablePlan`:

```python
_immutable_fields_ = [..., "skip_exchange", "co_partitioned_join_sources"]

def __init__(self, ..., skip_exchange=False, co_partitioned_join_sources=None):
    ...
    self.skip_exchange = skip_exchange
    self.co_partitioned_join_sources = co_partitioned_join_sources
```

*`program_cache.py` — `compile_from_graph`*, at the `OPCODE_EXCHANGE_SHARD` handler
(already has `in_reg_id_for_exchange`, `state[_ST_INPUT_DELTA_REG_ID]`, `shard_cols`,
`in_schema`):

```python
is_trivial    = (in_reg_id_for_exchange == state[_ST_INPUT_DELTA_REG_ID])
skip_exchange = (is_trivial and len(shard_cols) == 1
                 and shard_cols[0] == in_schema.pk_index)
pre_plan = runtime.ExecutablePlan(..., skip_exchange=skip_exchange)
```

At the `join_shard_map` build (already has `source_tid`, `reindex_col`):

```python
src_pk = self.registry.get_by_id(source_tid).schema.pk_index
if reindex_col == src_pk:
    co_partitioned_join_sources[source_tid] = True
```

Note: `shard_cols` does not exist in this scope — the local variable is `reindex_col`.
`shard_cols` only exists inside the `OPCODE_EXCHANGE_SHARD` handler below.

Pass `co_partitioned_join_sources` to the final `ExecutablePlan` constructor.

*`executor.py` — `evaluate_dag`*: check both flags.

For `exchange_post_plan` circuits — when `skip_exchange` is True, the pre-plan
output is already correctly owned by this worker (trivial pre-plan + PK routing
guarantee). No exchange is needed; use `pre_result` directly:

```python
if plan.exchange_post_plan is not None and exchange_handler is not None:
    pre_result = plan.execute_epoch(incoming_delta, source_id=source_id)
    incoming_delta.free()
    if pre_result is None:
        pre_result = ArenaZSetBatch(plan.out_schema)
    if plan.skip_exchange:
        out_delta = plan.exchange_post_plan.execute_epoch(pre_result)
        pre_result.free()
    else:
        exchanged = exchange_handler.do_exchange(target_view_id, pre_result)
        pre_result.free()
        out_delta = plan.exchange_post_plan.execute_epoch(exchanged)
        exchanged.free()
```

For join circuits — when `source_id` is in `co_partitioned_join_sources`, the
incoming delta is already routed to the correct worker by `fan_out_push`:

```python
elif (plan.join_shard_map is not None and source_id in plan.join_shard_map
      and exchange_handler is not None):
    copart = (plan.co_partitioned_join_sources is not None
              and source_id in plan.co_partitioned_join_sources)
    if copart:
        out_delta = plan.execute_epoch(incoming_delta, source_id=source_id)
        incoming_delta.free()
    else:
        exchanged = exchange_handler.do_exchange(
            target_view_id, incoming_delta, source_id=source_id)
        incoming_delta.free()
        out_delta = plan.execute_epoch(exchanged, source_id=source_id)
        exchanged.free()
```

When `skip_exchange=True` or a co-partitioned join source is detected, the worker
never sends `FLAG_EXCHANGE`. The master's poll loop in `fan_out_push` naturally
receives an ACK for that worker without ever waiting for a `FLAG_EXCHANGE` from it —
no master-side protocol change required.

### Impact (after all 4 sub-items)

| Metric | Before | After (non-copart) | After (co-partitioned) |
|---|---|---|---|
| IPC messages per push | 3N | 2N (N preload + N push) | N (push only) |
| `FLAG_PRELOADED_EXCHANGE` sent | no | yes | no |
| `FLAG_EXCHANGE` sent | yes | no (stash hit) | no (skip_exchange) |
| `_relay_exchange` called | yes | no | no |
| Post-plan seal | O((M/N) log(M/N)) | O(1) | O(1) |
| Extra sorts | 1 | 0 | 0 |

The N-message co-partitioned result requires 4d's compile-time skip. A stash-based
approach (sending `pk_batches[w]` as a preload for copart views) would be 2N — the
same data sent twice — and cannot achieve N.

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
Phase 0 ─── DONE (cfbde4b..918d014) ────────────────────────────────────
Phase 1 ─── DONE (b1641ce) ──────────────────────────────────────────────
Phase 2 ─── DONE (0cdf59d) ──────────────────────────────────────────────
Phase 3 ─── DONE ────────────────────────────────────────────────────────

Phase 4 ─── partial (multi_scatter + join_shard_map + executor branch done)
  [4a] get_exchange_info + get_preloadable_views
                                       (program_cache.py, ~30 lines)
  [4b] FLAG_PRELOADED_EXCHANGE + multi_scatter fan_out_push
                                       (ipc.py ~1 line, master.py ~20 lines)
  [4c] WorkerExchangeHandler stash +
       _handle_request drain loop      (worker.py, ~30 lines)
  [4d] Co-partitioned exchange elimination (compile-time skip_exchange +
       co_partitioned_join_sources)    (runtime.py ~5 lines,
                                        program_cache.py ~8 lines,
                                        executor.py ~15 lines)

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

**Phase 3:** DONE — `exchange_test` (`test_repartition_batches_merged`),
`test_workers.py` (`test_exchange_relay_consolidated`).

**Phase 4:**
- `test_workers.py` — `test_trivial_preplan_no_exchange`: create table T, view
  `SELECT group_col, COUNT(*) FROM T GROUP BY group_col`; push 100 rows across
  multiple transactions; assert correct COUNT per group; confirm `_relay_exchange`
  is never called for V (counter or log check).
- `test_workers.py` — `test_copartitioned_view_no_exchange`: create table T with PK
  column `id`, view `SELECT id, COUNT(*) FROM T GROUP BY id`; verify the compiled
  plan has `skip_exchange=True`; verify no `FLAG_EXCHANGE` is ever sent (counter
  check); assert correct results.
- `test_workers.py` — `test_copartitioned_join`: JOIN on PK column; verify
  `co_partitioned_join_sources` contains the source table id; verify `FLAG_EXCHANGE`
  is never sent; assert correct join results.
- `test_workers.py::test_push_scan_multiworker` — regression for routing correctness.
