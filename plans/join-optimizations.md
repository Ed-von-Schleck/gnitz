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

### Known bug

`_semi_join_dt_swapped` is defined in `anti_join.py` but **never called**.
`op_semi_join_delta_trace` dispatch is missing the `delta_len > trace_len` branch —
the function skips straight to the `_consolidated` check. Fix: add the swap branch to
`op_semi_join_delta_trace` matching the pattern in `op_join_delta_trace`.

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
their output has at most one row per PK (delta is unique-keyed).

---

## Optimization 3: ConsolidatedScope in op_join_delta_delta — DONE

Both inputs to `op_join_delta_delta` are wrapped in `ConsolidatedScope` before the
sort-merge loop. Eliminates spurious cartesian expansion from delete+reinsert pairs
within the same tick.

---

## Optimization 4: Intermediate Output Consolidation — PARTIALLY DONE

`BatchWriter.consolidate()` exists and is called in `op_join_delta_delta` every
`CONSOLIDATE_INTERVAL_DD = 16384` output rows.

**Gap:** `CONSOLIDATE_INTERVAL = 8192` is defined but unused. The periodic consolidation
counter was present in the original `op_join_delta_trace` and was dropped when Unit 8
extracted `_join_dt_normal`. The normal delta-trace path (`_join_dt_normal`) no longer
triggers consolidation. For large fan-out joins (one delta key matching many trace rows),
the output batch can grow unboundedly.

**Fix:** add `rows_since_consolidation` counter + `out_writer.consolidate()` call to
`_join_dt_normal`, matching the existing `op_join_delta_delta` pattern.

---

## Optimization 5: Key-Gather Prefetch

### Motivation

`ShardCursor.seek()` calls `shard_view.find_lower_bound()` — a binary search over an
mmap'd PK column. For a large shard not in CPU cache, each seek faults in a 4 KB page
even if only one row is needed. With N delta rows, N random page faults occur.

A prefetch pass does one sequential scan of the PK column (narrow, dense, cache-friendly),
gathers all matching rows into a compact in-memory batch, and replaces all subsequent
seeks with O(1) `SortedBatchCursor` advances. Only worth doing for shard-backed
(persistent) traces; ephemeral/in-memory traces get no benefit.

### Design

#### `ZSetStore.create_filtered_cursor(sorted_keys_batch)` — `gnitz/core/store.py`

```python
def create_filtered_cursor(self, sorted_keys_batch):
    """
    Returns a cursor containing only entries whose PKs appear in sorted_keys_batch.
    sorted_keys_batch must be sorted. Default falls back to create_cursor() (no-op).
    """
    return self.create_cursor()
```

#### Override on `PersistentTable` — `gnitz/storage/table.py`

```python
def create_filtered_cursor(self, sorted_keys_batch):
    mt_cursor = MemTableCursor(self.memtable)
    cursors = [mt_cursor]
    for handle in self.index.handles:
        gathered = _gather_shard_rows(handle.view, sorted_keys_batch, self.schema)
        if gathered.length() > 0:
            cursors.append(SortedBatchCursor(gathered))
    return UnifiedCursor(self.schema, cursors)
```

#### `_gather_shard_rows(shard_view, sorted_keys_batch, schema)` — `gnitz/storage/shard_table.py`

Sequential scan of `sorted_keys_batch`; binary-search each key in `shard_view`; copy
matching rows into a new `ArenaZSetBatch` using `append_from_accessor`. Returns the
batch (may be empty — caller skips if so).

#### VM integration

`op_join_delta_trace` currently receives a pre-opened `trace_cursor`. To use prefetch,
the operator needs the store so it can call `create_filtered_cursor(sorted_delta)`.
This requires changing the trace-side register type from cursor to store.

**Phasing:** implement `_gather_shard_rows` + `create_filtered_cursor` first (no VM
changes). Add a new opcode variant `OPCODE_JOIN_DELTA_TRACE_PREFETCH = 20` for the
store-passing path so existing behaviour is not disturbed.

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

## Cleanup items

- `SortedScope` imported but unused in `join.py` — remove from import line.
- `CONSOLIDATE_INTERVAL = 8192` defined but unused — either restore the counter in
  `_join_dt_normal` (Opt 4 gap above) or delete the constant.

---

## Implementation order

```
DONE
  3. ConsolidatedScope in op_join_delta_delta
  4. BatchWriter.consolidate() + trigger in delta-delta
  1. estimated_length() + seek_key_exact() + _join_dt_swapped
  2. _join_dt_merge_walk + anti/semi merge-walk

TODO (bugs in done work)
  B1. Wire _semi_join_dt_swapped into op_semi_join_delta_trace dispatch
  B2. Restore consolidation counter in _join_dt_normal (or delete CONSOLIDATE_INTERVAL)
  B3. Remove unused SortedScope import

TODO (new work)
  5. _gather_shard_rows + create_filtered_cursor (no VM changes needed yet)
  6. VM opcode variant for prefetch (store-passing path)
  7. NullAccessor + merge_schemas_for_join_outer
  8. op_join_delta_trace_outer + op_join_delta_delta_outer + opcodes + VM dispatch
  9. CircuitBuilder.left_join + SQL planner
```
