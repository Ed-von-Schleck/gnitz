# Plan: Join Operator Optimizations (Feldera-Inspired)

Optimizations ordered by expected impact. Based on Feldera's `JoinTrace` operator
(`crates/dbsp/src/operator/dynamic/join.rs`).

---

## Optimization 1: Adaptive Cursor Swapping — DONE

**Commit:** Unit 8.

`op_join_delta_trace` now compares `delta_batch.length()` vs
`trace_cursor.estimated_length()`. When `delta_len > trace_len`, iterates the trace
(smaller) and binary-searches the consolidated delta via `SortedBatchCursor.seek_key_exact()`.
Reduces cost from O(|Δ|·log|I|) to O(|I|·log|Δ|) when |Δ| ≫ |I|.

`estimated_length()` and `seek_key_exact()` added to `AbstractCursor` (store.py) and
all concrete cursors (cursor.py).

**Swap applies to:** `op_join_delta_trace`, `op_semi_join_delta_trace`.
**Swap does NOT apply to:** `op_anti_join_delta_trace` (would miss unmatched delta keys).

---

## Optimization 2: Merge-Walk for Consolidated Delta — DONE

**Commit:** Unit 8.

When `delta._consolidated` is True, replaces N `trace_cursor.seek()` calls with one
seek at the first delta key then O(N+M) monotone `advance()` calls. Applied to
`op_join_delta_trace`, `op_anti_join_delta_trace`, and `op_semi_join_delta_trace`.

**Deviation from plan doc:** `_join_dt_merge_walk` uses a `while` loop over matching
trace records (not a single `if`), and calls `mark_sorted(True)` instead of
`mark_consolidated(True)`. Reason: the trace can have multiple rows per PK (different
payloads). The plan doc assumed a relational trace but `execute_epoch` proves the
multi-row case is real. Anti/semi merge-walk correctly mark `consolidated(True)` because
their output has at most one row per PK (delta is unique-keyed after consolidation).

---

## Optimization 3: ConsolidatedScope in op_join_delta_delta — DONE

Both inputs to `op_join_delta_delta` are wrapped in `ConsolidatedScope` before the
sort-merge loop. Eliminates spurious cartesian expansion from delete+reinsert pairs
within the same tick. `op_anti_join_delta_delta` and `op_semi_join_delta_delta` follow
the same pattern.

---

## Optimization 4: Intermediate Output Consolidation — DONE

`BatchWriter.consolidate()` exists and is called in:
- `op_join_delta_delta` every `CONSOLIDATE_INTERVAL_DD = 16384` output rows.
- `_join_dt_normal` every `CONSOLIDATE_INTERVAL = 8192` output rows.

---

## Optimization 5: Eliminate _normal Paths — Always-Consolidate Delta-Trace — DONE

### Finding

The delta-delta operators (`op_join_delta_delta`, `op_anti_join_delta_delta`,
`op_semi_join_delta_delta`) always wrap both inputs in `ConsolidatedScope` and run a
merge-walk. No random seeks occur. This is the correct design.

The delta-trace operators have a `_normal` fallback for non-consolidated deltas:
`_join_dt_normal`, `_anti_join_dt_normal`, `_semi_join_dt_normal`. These do N
`trace_cursor.seek()` calls — N random binary searches into the mmap'd shard PK
column. On a cold shard each seek touches O(log S) scattered memory positions, causing
O(N log S) page faults.

### Why the previous gather proposal was wrong

The plan previously proposed `_gather_shard_rows`: scan the delta keys, binary-search
each into the shard, copy matching rows into an `ArenaZSetBatch`, then join against the
in-memory copy. This was described as "one sequential scan of the PK column." That
description was incorrect: the gather still performs N binary searches into the shard's
mmap'd `pk_lo_buf` — the same scattered access pattern, the same page faults. The
gather adds an extra allocation and a second data pass for no page-fault reduction.

### The solution already exists

The merge-walk functions already provide what the gather was trying to achieve.
`ShardCursor.advance()` (cursor.py:222) is:

```python
def advance(self):
    self.position += 1
    self._skip_ghosts()
```

`self.position += 1` is a sequential increment into the mmap'd `pk_lo_buf` array —
consecutive 8-byte reads in memory order. The merge-walk pattern (one `seek()` at the
first delta key, then pure `advance()` calls) **is** the sequential scan of the PK
column. No gather needed.

The only missing piece is that the `_normal` paths bypass consolidation, preventing
the merge-walk from being used when the delta is not already consolidated.

### Fix

Wrap the delta in `ConsolidatedScope` before dispatching to merge-walk in all three
delta-trace operators. This matches exactly what the delta-delta operators already do.
`ConsolidatedScope.__enter__` calls `to_consolidated()` which returns `self` immediately
when `_consolidated` is already True (batch.py:887) — zero cost for the common path.
The common path is the common path because `execute_epoch` (runtime.py:153) seals the
circuit input with `to_consolidated()` before the VM runs; only intermediate deltas
produced by UNION, MAP, or FILTER arrive non-consolidated.

**`op_join_delta_trace`** — replace `elif _consolidated / else _normal`:

```python
else:
    with ConsolidatedScope(delta_batch) as consolidated:
        _join_dt_merge_walk(consolidated, trace_cursor, out_writer, composite_acc)
```

**`op_anti_join_delta_trace`** — remove branch entirely:

```python
def op_anti_join_delta_trace(delta_batch, trace_cursor, out_writer, left_schema):
    with ConsolidatedScope(delta_batch) as consolidated:
        _anti_join_dt_merge_walk(consolidated, trace_cursor, out_writer)
```

**`op_semi_join_delta_trace`** — replace `elif _consolidated / else _normal`:

```python
else:
    with ConsolidatedScope(delta_batch) as consolidated:
        _semi_join_dt_merge_walk(consolidated, trace_cursor, out_writer)
```

Then delete `_join_dt_normal`, `_anti_join_dt_normal`, `_semi_join_dt_normal`, and
`CONSOLIDATE_INTERVAL` (which was only used by `_join_dt_normal`).

### Cost

`ConsolidatedScope` is O(N log N) for unsorted deltas, O(N) for sorted-but-not-
consolidated deltas, and O(1) (returns `self`) for already-consolidated deltas.
The mmap seek cost it replaces is O(N log S) with page-fault overhead where S is the
shard row count. Since mmap seeks involve TLB misses and potential minor faults,
and S ≫ N in the large-shard scenario, the in-memory sort is cheaper in wall time.

**Note:** `_join_dt_merge_walk` does not have the periodic `out_writer.consolidate()`
counter that `_join_dt_normal` had. For join output, `to_consolidated()` would be O(N)
work with zero row reduction (join rows share a PK but have distinct payloads from
different trace matches, so `compare_indices` returns non-zero for all of them). The
counter would waste time and is not ported.

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

TODO
  5. Eliminate _normal paths: always ConsolidatedScope + merge-walk
       - op_join_delta_trace: drop elif/else, add ConsolidatedScope else branch
       - op_anti_join_delta_trace: drop branch, always ConsolidatedScope
       - op_semi_join_delta_trace: drop elif/else, add ConsolidatedScope else branch
       - delete _join_dt_normal, _anti_join_dt_normal, _semi_join_dt_normal
       - delete CONSOLIDATE_INTERVAL constant
  6. NullAccessor + merge_schemas_for_join_outer
  7. op_join_delta_trace_outer + op_join_delta_delta_outer + opcodes + VM dispatch
  8. CircuitBuilder.left_join + SQL planner
```
