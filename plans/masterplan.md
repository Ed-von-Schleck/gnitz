# Master Implementation Plan

## Dependency Graph

```
Phase 1 ─── all independent, do in parallel ───────────────────────────
  [1] Compaction ✓        [2] Reduce Bugfixes      [3] Distinct Cursor ✓
                               (R3,R4,R5)               (D1)

Phase 2 ─── foundational, unlocks many downstream ─────────────────────
  [4] Source Optimizations (_consolidated flag + seal)

Phase 3 ─── independent of each other, all benefit from Phase 2 ───────
  [5] Linear Opts         [6] Join Quick Wins      [7] Distinct Skip ✓
      (L1-L4)                 (J3,J4)                  (D2)

Phase 4 ─── depends on Phase 2 (_consolidated) ────────────────────────
  [8] Join Adaptive Swap + Merge-Walk (J1,J2)

Phase 5 ─── independent of each other ─────────────────────────────────
  [9] Exchange Standalone  [10] Exchange IPC Flags
      (E1,E4,E5,E6)             (E3, needs Phase 2)

Phase 6 ─── complex multi-file feature ────────────────────────────────
  [11] AggValueIndex (R1+R2)  →  [12] Group Index Extensions (G1)

Phase 7 ─── new features, all independent ─────────────────────────────
  [13] AVERAGE    [14] Semigroup    [15] LEFT JOIN
  [16] Push-time routing (E2)       [17] Prefetch (J5)
```

---

## Phase 1: Standalone Correctness & Infrastructure

### Unit 1: Automatic Compaction ✓ DONE
**Plan:** `compaction.md` | **Commit:** `8124594`

### Unit 2: Reduce Correctness Fixes
**Plan:** `reduce-optimizations.md` Opts 3, 4, 5 | **Impact:** Bug fix + correctness + perf | **~18 lines**

All changes in `dbsp/ops/reduce.py`:

| Opt | Change | Lines |
|---|---|---|
| R3 | Hoist `gi_cursor` creation before group loop; seek per group, close after loop | ~10 |
| R4 | Remove `has_old` guard on `is_linear()` — fix new-group correctness | ~5 |
| R5 | Guard `step()` call with `is_lin` — skip for non-linear aggregates | ~3 |

**Tests:** Existing reduce tests cover all three. No new tests required.

### Unit 3: Distinct Cursor Optimization ✓ DONE
**Commit:** `280546d`

---

## Phase 2: Foundational Infrastructure

### Unit 4: Source Optimizations (`_consolidated` flag + seal)
**Plan:** `source-optimizations.md` | **Impact:** Eliminates redundant sort/consolidation throughout pipeline | **~40 lines**

| Step | File | Change |
|---|---|---|
| 4a | `core/batch.py` | `_consolidated` field, `mark_consolidated()`, `clear()` update, `to_consolidated()` short-circuit |
| 4b | `core/batch.py` | `BatchWriter.mark_consolidated()` |
| 4c | `dbsp/ops/source.py` | `mark_consolidated(True)` after scan loop |
| 4d | `dbsp/ops/reduce.py` | Replace `mark_sorted` with `mark_consolidated` |
| 4e | `dbsp/ops/distinct.py` | Replace `mark_sorted(True)` with `mark_consolidated(True)` |
| 4f | `dbsp/ops/join.py` | Replace `mark_sorted(True)` with `mark_consolidated(True)` in delta-delta |
| 4g | `dbsp/ops/linear.py` | Propagate `_consolidated` in filter, negate, delay |
| 4h | `vm/runtime.py` | Seal at `execute_epoch`: `sealed = input_delta.to_consolidated()` |

**Tests:** Existing tests cover. One targeted test: push batch with duplicate PKs through DISTINCT, assert correct consolidation.

---

## Phase 3: Operator Optimizations (all independent, parallelizable)

### Unit 5: Linear Operator Optimizations
**Plan:** `linear-optimizations.md` Opts 1-4 | **~50 lines**

| Opt | File | Change |
|---|---|---|
| L1 | `linear.py` | `mark_sorted(batch_a._sorted)` when `batch_b is None` (1 line) |
| L2 | `interpreter.py` + `runtime.py` | DELAY: alias `reg_out.batch = reg_in.batch` instead of copy; add `out_reg.unbind()` in `execute_epoch` |
| L3 | `linear.py` | New `_op_union_merge` — two-pointer merge for two sorted inputs (~30 lines) |
| L4 | `runtime.py` | `execute_epoch` evict: take `_internal_batch` instead of cloning (~15 lines) |

**Note:** L4 modifies `execute_epoch` result-extraction block — coordinate with Unit 4's seal. The combined code is shown in `linear-optimizations.md` Opt 4.

**Tests:** Existing tests cover. One new test: `test_union_sorted_merge` in dbsp_comprehensive_test.

### Unit 6: Join Quick Wins
**Plan:** `join-optimizations.md` Opts 3, 4 | **~25 lines**

| Opt | File | Change |
|---|---|---|
| J3 | `join.py` | `ConsolidatedScope` instead of `SortedScope` in `op_join_delta_delta` (2 lines) |
| J4 | `batch.py` + `join.py` | `BatchWriter.consolidate()` method; periodic consolidation trigger in join loops (~20 lines) |

**Tests:** Existing join tests cover.

### Unit 7: Distinct Compile-Time Skip ✓ DONE
**Commit:** `280546d`

---

## Phase 4: Join Adaptive Swap + Merge-Walk

### Unit 8: Join Optimizations — Adaptive Swap + Merge-Walk
**Plan:** `join-optimizations.md` Opts 1, 2 | **Depends on Unit 4** | **~80 lines**

| Step | File | Change |
|---|---|---|
| 8a | `core/store.py` | `estimated_length()` + `seek_key_exact()` on AbstractCursor |
| 8b | `storage/cursor.py` | Implement `estimated_length()` on all concrete cursors |
| 8c | `dbsp/ops/join.py` | Restructure `op_join_delta_trace` into dispatch + `_join_dt_normal`, `_join_dt_swapped`, `_join_dt_merge_walk` |
| 8d | `dbsp/ops/anti_join.py` | Same adaptive swap + merge-walk for anti/semi-join |

**Tests:** Existing join tests; new test with asymmetric batch sizes to exercise swap path.

---

## Phase 5: Exchange Optimizations

### Unit 9: Exchange Standalone
**Plan:** `exchange-optimizations.md` Opts 1, 4, 5, 6 | **~60 lines**

| Opt | File | Change |
|---|---|---|
| E1 | `exchange.py` + `master.py` | `repartition_batches` multi-source; eliminate merge in `_relay_exchange` |
| E4 | `exchange.py` + `master.py` | `initial_capacity` hints in batch creation |
| E5 | `master.py` | Build poll arrays once; swap-remove on ACK |
| E6 | `master.py` | Route `fan_out_seek_by_index` to single owning worker |

**Tests:** Existing exchange + multi-worker tests cover.

### Unit 10: Exchange IPC Flags
**Plan:** `exchange-optimizations.md` Opt 3 | **Depends on Unit 4** | **~20 lines**

| File | Change |
|---|---|
| `server/ipc.py` | `FLAG_BATCH_SORTED` + `FLAG_BATCH_CONSOLIDATED` bits in header |
| `dbsp/ops/exchange.py` | Propagate `_sorted`/`_consolidated` on sub-batches |
| `server/master.py` | Propagate in `_split_batch_by_pk` |

**Tests:** Existing tests cover; exchange batches now skip re-sort in `execute_epoch`.

---

## Phase 6: AggValueIndex (Complex Feature)

### Unit 11: AggValueIndex for MIN/MAX
**Plan:** `reduce-optimizations.md` Opts 1+2 | **~120 lines**

| Step | File | Change |
|---|---|---|
| 11a | `dbsp/ops/group_index.py` | `make_agg_value_idx_schema`, encoding helpers, `AggValueIndex` class |
| 11b | `vm/interpreter.py` | INTEGRATE handler for AggValueIndex maintenance |
| 11c | `dbsp/ops/reduce.py` | `_read_agg_from_value_index` helper; AggValueIndex path in op_reduce |
| 11d | `catalog/program_cache.py` | Create AggValueIndex for eligible MIN/MAX; skip `tr_in_table` when AVI exists |

**Tests:** New tests: MIN/MAX query correctness with AggValueIndex; verify trace_in not created.

### Unit 12: Group Index Extensions
**Plan:** `group-index.md` | **Depends on Unit 11** | **~30 lines**

| File | Change |
|---|---|
| `dbsp/ops/group_index.py` | Move `_mix64`/`_extract_group_key` from reduce.py; add `_extract_gc_u64` |
| `dbsp/ops/reduce.py` | Import from group_index.py |
| `vm/interpreter.py` | Use `_extract_gc_u64` in INTEGRATE |
| `catalog/program_cache.py` | Remove `len(gcols)==1` gate |

**Tests:** `test_reduce_min_multi_col_group`, `test_reduce_max_string_group`.

---

## Phase 7: New Features (all independent, any order)

| Unit | Plan | Description | Size |
|---|---|---|---|
| 13 | `reduce-opts` R6 | AVERAGE aggregate (sum+count pair) | ~80 lines, schema work |
| 14 | `reduce-opts` R7 | Semigroup `combine()` on accumulators | ~20 lines |
| 15 | `join-opts` J6 | LEFT OUTER JOIN (NullAccessor, new opcodes) | ~100 lines |
| 16 | `exchange-opts` E2 | Push-time routing for trivial pre-plans | ~80 lines, protocol change |
| 17 | `join-opts` J5 | Key-gather prefetch | ~60 lines, most invasive |

---

## Recommended Execution Strategy

~~**Session 1:** Units 1+2+3 in parallel~~ — Units 1, 3, 7 done. **Next: Unit 2** (reduce R3/R4/R5, standalone).
**Next session:** Unit 4 (foundational — unlocks Phases 3-5)
**Then:** Units 5+6 in parallel (Unit 7 already done)
**Then:** Unit 8 (join adaptive/merge-walk)
**Then:** Units 9+10 in parallel
**Then:** Unit 11, then Unit 12
**Later:** Units 13-17 as needed

Each unit is a single commit. Run `make test | tail -200` after each to verify no regressions.
