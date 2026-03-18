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

## Optimization 6: Left Outer Join

### What changes

#### `NullAccessor` — `gnitz/core/comparator.py`

```python
class NullAccessor(RowAccessor):
    """Returns NULL for every column. Used for outer-join right-side placeholder."""
    def is_null(self, col_idx):       return True
    def get_int(self, col_idx):       return r_uint64(0)
    def get_int_signed(self, col_idx): return r_int64(0)
    def get_float(self, col_idx):     return 0.0
    def get_u128(self, col_idx):      return r_uint128(0)
    def get_str_struct(self, col_idx): return (0, 0, NULL_PTR, NULL_PTR, "")
    def get_col_ptr(self, col_idx):   return NULL_PTR

NULL_ACCESSOR = NullAccessor()
```

#### `merge_schemas_for_join_outer` — `gnitz/core/types.py`

Same layout as `merge_schemas_for_join` but right-side non-PK columns marked nullable.

#### `op_join_delta_trace_outer` / `op_join_delta_delta_outer` — `gnitz/dbsp/ops/join.py`

Mirror of the inner-join operators. When no trace match is found for a delta row,
emit that row with `composite_acc.set_accessors(delta_acc, NULL_ACCESSOR)` and
`weight = w_delta`. `NullAccessor.is_null()` returns True for all columns so
`append_from_accessor` writes zeros with null bits set — correct SQL NULL encoding.

#### New opcodes — `gnitz/core/opcodes.py`

```python
OPCODE_LEFT_JOIN_DELTA_TRACE = 20
OPCODE_LEFT_JOIN_DELTA_DELTA = 21
```

Wire up in `gnitz/vm/interpreter.py` dispatch and add builder functions in
`gnitz/vm/instructions.py`.

#### Planner — `gnitz/catalog/program_cache.py`

`CircuitBuilder.left_join(left_node, right_node)` uses `merge_schemas_for_join_outer`
and emits the new opcodes. SQL planner emits `left_join(...)` for `LEFT JOIN` nodes.

---

## Implementation order

```
DONE
  3. ConsolidatedScope in op_join_delta_delta (and anti/semi dd variants)
  4. BatchWriter.consolidate() + triggers in _join_dt_normal and delta-delta
  1. estimated_length() + seek_key_exact() + swapped paths (join, semi-join)
  2. _join_dt_merge_walk + anti/semi merge-walk
  5. Eliminate _normal paths: always ConsolidatedScope + merge-walk

TODO
  6. NullAccessor + merge_schemas_for_join_outer
  7. op_join_delta_trace_outer + op_join_delta_delta_outer + opcodes + VM dispatch
  8. CircuitBuilder.left_join + SQL planner
```
