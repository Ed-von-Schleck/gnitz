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

`exchange.py` sole routing authority. GATHER deleted, `hash_row_by_columns` PK bug fixed,
`PartitionAssignment`/`worker_for_pk`/`relay_scatter` moved in, `multi_scatter` added with
PK-spec flag propagation.

---

## Phase 1: Mechanical efficiency — DONE (b1641ce)

`initial_capacity` pre-sizing in scatter; poll-list swap-remove on ACK; `PartitionRouter`
secondary-index cache for unicast seeks.

---

## Phase 2: WAL/IPC infrastructure — DONE (0cdf59d)

DDL ACK round-trip removed; `broadcast_batch` encode-once via SCM_RIGHTS; `WALReader`
mmap zero-copy (`decode_batch_from_ptr`); `FLAG_BATCH_SORTED`/`FLAG_BATCH_CONSOLIDATED`
(bits 50/51) encode and restore across wire.

---

## Phase 3: Exchange relay synthesis — DONE

`repartition_batches_merged` linear-scan k-way merge; `relay_scatter` dispatches on
`_consolidated`; peak exchange memory O(2M); post-plan seal on consolidated relay O(1).

---

## Phase 4: Exchange round-trip elimination — DONE (353a43b)

For `INPUT → SHARD → REDUCE` (trivial pre-plan): co-partitioned circuits skip exchange
entirely (N IPC messages); non-co-partitioned trivial circuits use preloaded stash (2N).

- **4a** `get_exchange_info` + `get_preloadable_views` in `program_cache.py`.
- **4b** `FLAG_PRELOADED_EXCHANGE = 1024` in `ipc.py`; `fan_out_push` in `master.py`
  calls `multi_scatter` over all col-specs and sends preloaded frames before `FLAG_PUSH`.
- **4c** `WorkerExchangeHandler._stash`; `_handle_request` drains leading
  `FLAG_PRELOADED_EXCHANGE` frames; `do_exchange` returns stash hit directly.
- **4d** `ExecutablePlan.skip_exchange` + `co_partitioned_join_sources` (`runtime.py`);
  `compile_from_graph` sets both at compile time (`program_cache.py`); `evaluate_dag`
  skips `do_exchange` when either flag is set (`executor.py`).

| Metric | Before | After (non-copart) | After (co-partitioned) |
|---|---|---|---|
| IPC messages per push | 3N | 2N | N |
| `FLAG_EXCHANGE` sent | yes | no (stash hit) | no (skip_exchange) |
| Post-plan seal | O((M/N) log(M/N)) | O(1) | O(1) |

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

Phase 4 ─── DONE (353a43b) ──────────────────────────────────────────────

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

**Phase 4:** DONE — `test_workers.py` (`test_trivial_preplan_no_exchange`,
`test_copartitioned_view_no_exchange`, `test_copartitioned_join`,
`test_push_scan_multiworker`).
