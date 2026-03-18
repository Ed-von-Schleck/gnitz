# Plan: Join Operator Optimizations (Feldera-Inspired)

Optimizations ordered by expected impact. Based on Feldera's `JoinTrace` operator
(`crates/dbsp/src/operator/dynamic/join.rs`).

---

## Optimization 1: Adaptive Cursor Swapping — DONE

When `delta_len > trace_len`, iterate the trace and binary-search the consolidated
delta via `SortedBatchCursor.seek_key_exact()`. Applied to `op_join_delta_trace` and
`op_semi_join_delta_trace` (not anti-join — would miss unmatched delta keys).

---

## Optimization 2: Merge-Walk for Consolidated Delta — DONE

When `delta._consolidated` is True, replace N seeks with one seek at the first delta
key then O(N+M) monotone `advance()` calls. Applied to all three delta-trace operators.

Note: `_join_dt_merge_walk` marks `sorted(True)` (not consolidated) because the trace
can have multiple rows per PK; anti/semi merge-walks mark `consolidated(True)`.

---

## Optimization 3: ConsolidatedScope in op_join_delta_delta — DONE

Both inputs to all three `_delta_delta` operators wrapped in `ConsolidatedScope`.
Eliminates spurious cartesian expansion from delete+reinsert pairs within a tick.

---

## Optimization 4: Intermediate Output Consolidation — DONE

`BatchWriter.consolidate()` called in `op_join_delta_delta` every
`CONSOLIDATE_INTERVAL_DD = 16384` output rows. `_join_dt_normal` had its own
`CONSOLIDATE_INTERVAL = 8192` counter (deleted with that function in Opt 5).

---

## Optimization 5: Eliminate _normal Paths — Always-Consolidate Delta-Trace — DONE

Deleted `_join_dt_normal`, `_anti_join_dt_normal`, `_semi_join_dt_normal`, and
`CONSOLIDATE_INTERVAL`. All three delta-trace operators now wrap the delta in
`ConsolidatedScope` and call the merge-walk unconditionally (modulo the swap path in
join and semi-join). `to_consolidated()` returns `self` when already consolidated —
zero cost for the common path.

`_join_dt_merge_walk` re-seeks the trace when consecutive consolidated delta rows share
the same PK (multiset delta: multiple rows with same PK but different payload). This
preserves correct M:N join semantics at one extra comparison per iteration.

---

## Optimization 6: Single-Pass Left Outer Join — DONE

`OPCODE_JOIN_DELTA_TRACE_OUTER = 22` fuses inner-join + anti-join + null-fill into one
merge-walk, eliminating the 3-node decomposition previously emitted by the planner.

**Files changed:**
- `gnitz/core/opcodes.py`: `OPCODE_JOIN_DELTA_TRACE_OUTER = 22`
- `gnitz/core/comparator.py`: `NullAccessor` — right-side null-fill placeholder
- `gnitz/core/types.py`: `merge_schemas_for_join_outer` — nullable right-side columns
- `gnitz/dbsp/ops/join.py`: `op_join_delta_trace_outer` + `_join_dt_outer_merge_walk`
- `gnitz/vm/instructions.py`: `join_delta_trace_outer_op` builder
- `gnitz/vm/interpreter.py`: dispatch for opcode 22
- `gnitz/catalog/program_cache.py`: compile handler + trace-side-source detection
- `rust_client/gnitz-core/src/types.rs`: `OPCODE_JOIN_DELTA_TRACE_OUTER = 22`
- `rust_client/gnitz-core/src/circuit.rs`: `left_join` + `left_join_with_trace_node`
- `rust_client/gnitz-sql/src/planner.rs`: replaced 3-node subgraph with single node
- `rpython_tests/dbsp_comprehensive_test.py`: `test_outer_join_delta_trace`

---

## Implementation order

```
DONE
  3. ConsolidatedScope in op_join_delta_delta (and anti/semi dd variants)
  4. BatchWriter.consolidate() + triggers in _join_dt_normal and delta-delta
  1. estimated_length() + seek_key_exact() + swapped paths (join, semi-join)
  2. _join_dt_merge_walk + anti/semi merge-walk
  5. Eliminate _normal paths: always ConsolidatedScope + merge-walk
  6. Single-pass left outer join via OPCODE_JOIN_DELTA_TRACE_OUTER
```
