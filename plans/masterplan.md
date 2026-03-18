# Master Implementation Plan

**Completed:** Units 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 18a/b/c/s5a (IPC + rust_client wire format).
**In progress:** Unit 18 Step 5b (rust_client design cleanup).

## Dependency Graph

```
Phase 1 ─── all unblocked ──────────────────────────────────────────────
  [6b] Join bug fixes    [9] Exchange Standalone    [10] Exchange IPC Flags
  (B1,B2,B3)                 (Opt 1,4,5,6)               (Opt 3)
                         [18s5b] rust_client cleanup
                              (Message flatten, field renames)
  [16] Push-time routing ─── depends on [9]; benefits from [10]
       (Opt 2)

Phase 2 ─── depends on [18a/b/c/s5a]✓ ────────────────────────────────
  [9] Exchange Standalone    [18s5] rust_client update
      (Opt 1,4,5,6)               (IPC wire format)

  [10] Exchange IPC Flags ─── unblocked ([4]✓ + [18a/b/c]✓)
       (Opt 3)

  [16] Push-time routing ─── depends on [9]; benefits from [10]
       (Opt 2)

Phase 4 ─── depends on [18a/b/c]✓ ─────────────────────────────────────
  [19] WAL-Block Improvements

Phase 3 ─── new features, all independent ──────────────────────────────
  [13] AVERAGE    [14] Semigroup    [15] LEFT JOIN    [17] Prefetch (J5)

Phase 4 ─── FLSM storage upgrade ───────────────────────────────────────
  [20] P1: Data Structures + Read Path
  [21] P2: L0→L1 Vertical Compaction           (depends on [20])
  [22] P3: Within-Guard Horizontal Compaction   (depends on [21])
  [23] P4: L1→L2 Vertical Compaction            (depends on [22])
```

---

## Join Bug Fixes (unblocked, ~15 lines)

Three correctness/cleanup gaps left by Units 6 + 8:

| Bug | File | Fix |
|---|---|---|
| B1 | `dbsp/ops/anti_join.py` | `op_semi_join_delta_trace`: add `delta_len > trace_len` branch calling `_semi_join_dt_swapped` (defined but never dispatched) |
| B2 | `dbsp/ops/join.py` | `_join_dt_normal`: restore `rows_since_consolidation` counter + `out_writer.consolidate()` every `CONSOLIDATE_INTERVAL=8192` rows |
| B3 | `dbsp/ops/join.py` | Remove unused `SortedScope` import |

**Tests:** Existing join + anti/semi-join tests cover B1. B2 covered by large-fan-out join test.

---

## Phase 1: Exchange + IPC rust_client

**[6b], [9], [10], [18s5] are all independent. [16] must follow [9].**

### Unit 9: Exchange Standalone
**Plan:** `exchange-optimizations.md` Opts 1, 4, 5, 6 | **~60 lines**

| Opt | File | Change |
|---|---|---|
| Opt 1 | `exchange.py` + `master.py` | `repartition_batches` multi-source; replace merge+repartition_batch in `_relay_exchange` with single-pass scatter (eliminates intermediate merge allocation) |
| Opt 4 | `exchange.py` + `master.py` | `initial_capacity = batch.length() // num_workers` hints in `repartition_batch`, `repartition_batches`, `_split_batch_by_pk` |
| Opt 5 | `master.py` | Build poll arrays once before loop; swap-remove on ACK |
| Opt 6 | `master.py` | Master-side `_index_routing` dict; populate in `fan_out_push` row scan; fast-path in `fan_out_seek_by_index` |

**Tests:** Existing exchange + multi-worker tests. Add assertion that sub-batch `_sorted` equals input `_sorted` in `test_repartition_batch`.

### Unit 10: Exchange IPC Flags
**Plan:** `exchange-optimizations.md` Opt 3 | **Unblocked** | **~20 lines**

Sorted/consolidated flags carried as new high bits in `CONTROL_SCHEMA.flags` (alongside `FLAG_HAS_SCHEMA` / `FLAG_HAS_DATA`).

| File | Change |
|---|---|
| `server/ipc.py` | `FLAG_BATCH_SORTED` / `FLAG_BATCH_CONSOLIDATED` high bits; set in `serialize_to_memfd`; read in `_recv_and_parse` |
| `dbsp/ops/exchange.py` | Propagate `_sorted`/`_consolidated` on sub-batches from decoded flags |
| `server/master.py` | Set flags in `_split_batch_by_pk` |

**Tests:** Existing tests cover; exchange batches skip re-sort in `execute_epoch`.

### Unit 16: Push-Time Routing for Trivial Pre-Plans
**Plan:** `exchange-optimizations.md` Opt 2 | **Depends on Unit 9; benefits from Unit 10** | **~100 lines**

For `INPUT → SHARD → REDUCE` circuits, master routes directly to shard-column partitions at push time, eliminating the PK-split → worker round-trip → merge → repartition sequence. Reduces IPC messages from 3N to N per push for all simple GROUP BY queries.

| File | Change |
|---|---|
| `catalog/program_cache.py` | `get_exchange_info(view_id) → (shard_cols, is_trivial)`; `get_trivial_exchange_views(source_table_id, registry)` |
| `server/ipc.py` | `FLAG_PRELOADED_EXCHANGE = 1024` |
| `server/master.py` | `fan_out_push` single combined pass: PK-partition + shard-partition simultaneously; send preloaded frames before FLAG_PUSH |
| `server/worker.py` | `WorkerExchangeHandler._stash`; `stash_preloaded`; `do_exchange` checks stash; `_handle_request` drains FLAG_PRELOADED_EXCHANGE before dispatch |

**Tests:** `test_trivial_preplan_no_exchange` (new, `GNITZ_WORKERS=4`); `test_push_scan_multiworker` regression.

### Unit 18 Step 5b: rust_client Design Cleanup
**Plan:** `ipc-unification.md` Step 5b | **In progress** | **Rust only**

Wire format correct (5a done, 27 unit tests pass). Remaining cosmetic cleanup:

| Item | File | Change |
|---|---|---|
| `TypeCode::String.wire_stride()` returns 8, should be 16 | `gnitz-protocol/src/types.rs` | Fix return value |
| `Message` nests `header: Header` | `gnitz-protocol/src/message.rs` | Flatten to top-level fields |
| `send_message` takes `Header` param | `gnitz-protocol/src/message.rs` | Rewrite to positional args |
| `msg.header.status/target_id/client_id` | `gnitz-core/src/connection.rs`, `gnitz-core/src/ops.rs` | Update to `msg.status/target_id/client_id` |

**Tests:** `cargo test`; `GNITZ_WORKERS=4 make e2e`.

---

## Phase 2: WAL-Block Improvements

### Unit 19: WAL-Block Improvements
**Plan:** `wal-block-improvements.md` | **Depends on Unit 18a/b/c ✓** | **~6 commits**

Each item is a separate commit; independent of each other once IPC unification is done.

| Item | Files | Change |
|---|---|---|
| Clone elimination | `master.py`, `exchange.py` | `repartition_into(src, shard_cols, dest_batches)` scatters rows directly from non-owning mmap view; no `clone()` needed (Unit 9 Opt 1 eliminated the merge; this eliminates the per-payload clone) |
| Parallel `fan_out_scan` | `master.py` | Poll loop; scan latency O(max worker) instead of O(sum) |
| DDL WAL fan-out | `wal.py`, `master.py` | `WALWriter.append_batch(subscriber_fds)` encodes once, N+1 destinations; deletes `broadcast_ddl`, `_handle_ddl_sync`, `FLAG_DDL_SYNC` |
| Zero-copy WAL recovery | `wal.py` | `WALReader` switches to mmap + `decode_batch_from_ptr`; 0 allocations per block |
| Shared slot arena | `master.py`, `worker.py` | Pre-forked `MAP_SHARED\|MAP_ANONYMOUS` slots; encode directly into shared memory; 1-byte signals replace SCM_RIGHTS; 8 syscalls per `fan_out_push` N=4 (down from 40) |
| Exchange fault tolerance | `worker.py` | Write exchange batch to local WAL before send; truncate after ACK; crash-safe replay |

**Tests:** Existing IPC + multi-worker tests; new `test_zero_copy_wal_recovery`; `GNITZ_WORKERS=4` E2E throughout.

---

## Phase 3: New Features (all independent)

| Unit | Plan | Description | Size |
|---|---|---|---|
| 13 | `reduce-opts` R6 | AVERAGE aggregate (sum+count pair) | ~80 lines, schema work |
| 14 | `reduce-opts` R7 | Semigroup `combine()` on accumulators | ~20 lines |
| 15 | `join-opts` J6 | LEFT OUTER JOIN (NullAccessor, new opcodes) | ~100 lines |
| 17 | `join-opts` J5 | Key-gather prefetch | ~60 lines, most invasive |

---

## Phase 4: FLSM Storage Upgrade

**Plan:** `flsm.md` | **Independent of all other work**

Upgrades storage to a Fragmented Log-Structured Merge-tree (PebbleDB style) with Lazy Leveling at Lmax. 3 levels: L0 flat pool + L1 guarded + L2 leveled (Z=1). Write amplification O(L+T)=6.

### Unit 20: FLSM Phase 1 — Data Structures + Read Path
**~350 lines** | **No functional change; all existing tests pass unchanged**

| File | Change |
|---|---|
| `storage/flsm.py` (new) | `LevelGuard`, `FLSMLevel`, `FLSMIndex`; backward-compat shims `.handles`/`.needs_compaction` |
| `storage/metadata.py` | Add `level`, `guard_key_lo`, `guard_key_hi` to `ManifestEntry` (default 0) |
| `storage/index.py` | `index_from_manifest` constructs `FLSMIndex`; all entries in L0 |
| `storage/ephemeral_table.py` | Replace `ShardIndex` with `FLSMIndex`; `_build_cursor` uses `all_handles_for_cursor()` |
| `storage/table.py` | Same + delegate manifest init to `index_from_manifest` |

**Tests:** `test_flsm_data_structures` + `test_flsm_guard_read_path` in `storage_comprehensive_test`.

### Unit 21: FLSM Phase 2 — L0→L1 Vertical Compaction
**Depends on Unit 20** | **~200 lines**

Tournament-tree merge of L0 shards routed into L1 guards. Guard boundaries from L0 shard `pk_min` on first compaction. Manifest VERSION=3 (ENTRY_SIZE=208).

| File | Change |
|---|---|
| `storage/compactor.py` | `_merge_and_route(input_files, guard_keys, ...)` + `compact_l0_to_l1(flsm_index, ...)` |
| `storage/flsm.py` | `FLSMIndex.commit_l0_to_l1(...)` |
| `storage/ephemeral_table.py` | Replace `_compact()` with L0→L1 routing version |
| `storage/table.py` | Replace `compact_if_needed()` with L0→L1 routing version |
| `storage/manifest.py` | Bump VERSION=3, ENTRY_SIZE=208; V2 legacy read path |
| `storage/index.py` | Route V3 entries to correct level/guard |

**Tests:** `test_flsm_l0_to_l1_compaction` + `test_flsm_manifest_persistence`.

### Unit 22: FLSM Phase 3 — Within-Guard Horizontal Compaction
**Depends on Unit 21** | **~80 lines**

Merge overfull guard files into one at the same level. L1 threshold=4; L2 threshold=1 (Lazy Leveling).

| File | Change |
|---|---|
| `storage/compactor.py` | `compact_guard_horizontal(guard, output_path, ...)` |
| `storage/flsm.py` | `FLSMIndex.compact_guards_if_needed(...)` + `_compact_one_guard(...)` |
| `storage/ephemeral_table.py` | `create_cursor` calls `compact_guards_if_needed` after L0 auto-compact |
| `storage/table.py` | `compact_if_needed` adds horizontal step; single manifest publish |

**Tests:** `test_flsm_horizontal_compaction` + `test_flsm_lmax_horizontal_threshold` + `test_flsm_horizontal_does_not_promote`.

### Unit 23: FLSM Phase 4 — L1→L2 Vertical Compaction
**Depends on Unit 22** | **~120 lines**

Picks overfull L1 guard, merges with overlapping L2 guards, enforces Lazy Leveling (Z=1) immediately.

| File | Change |
|---|---|
| `storage/compactor.py` | `compact_l1_to_l2(flsm_index, ...)` |
| `storage/flsm.py` | `FLSMIndex.needs_l1_compact`; `commit_l1_to_l2(...)` |
| `storage/ephemeral_table.py` | `create_cursor` checks `needs_l1_compact` |
| `storage/table.py` | `compact_if_needed` adds L1→L2 step |

**Tests:** `test_flsm_l1_to_l2_compaction` + `test_flsm_full_hierarchy`.

---

## Execution Strategy

**In progress:** Unit 18 Step 5b (rust_client cleanup).
**Next (all unblocked, do in parallel):** Join bug fixes (B1/B2/B3), Unit 9 (Exchange), Unit 10 (Exchange IPC Flags), Units 20+ (FLSM).
**Then:** Unit 16 (push-time routing, after Unit 9); Unit 19 (WAL-Block Improvements, after Unit 18 complete).
**Then:** Units 13-15, 17 (new features, any order).
**FLSM track:** Units 20→21→22→23 sequentially, independent of all other work.

Each unit is a single commit. Run `make test | tail -200` after each.
