# Plan: Reduce Operator Optimizations

This plan covers all identified correctness fixes and performance optimizations for the
`REDUCE` operator, ordered by expected impact. Each section maps precisely to the files
and lines that must change.

---

## Background

`op_reduce` (`gnitz/dbsp/ops/reduce.py`) implements incremental `GROUP BY + AGG` using
the DBSP formula:

```
δ_out = Agg(H + D) - Agg(H)
```

Where `H` is history (from `trace_in` and `trace_out`) and `D` is the current delta.
The operator has three execution paths, selected at runtime:

| Path | Condition | Cost |
|---|---|---|
| Linear shortcut | `is_linear() and has_old` | O(1) per group |
| ReduceGroupIndex | `not is_linear(), single ≤64-bit int group col` | O(log N + k) per group — but see bugs |
| Full scan fallback | everything else | O(N) per group |

The following optimizations attack the correctness gaps and the two non-linear paths.

---

## Optimization 1: AggValueIndex for MIN/MAX (highest impact)

### The problem

For MIN/MAX (`not is_linear()`), gnitz builds a `replay` batch per group, copies all
historical rows for the group into it, consolidates, then calls `agg_func.step()` for
every row. Even with the `ReduceGroupIndex`, this requires:

1. Allocating an `ArenaZSetBatch` per group per tick
2. Seeking `trace_in` once per historical row in the group (double-seek: group index →
   source PK → row data)
3. `to_consolidated()` on the replay batch (O(k log k))
4. Stepping through all k rows to find min/max

Feldera eliminates all of this: its trace is ordered by value within each group, so
`MIN = first entry with positive weight`, `MAX = last`. It terminates immediately in O(1)
best case.

### The insight

The current `ReduceGroupIndex` packs its U128 key as:

```
high 64 bits = group_col_value
low  64 bits = source_pk_lo         ← sorts within group by source PK (meaningless for agg)
```

Changing the low 64 bits to the **aggregate column value** instead gives a value-sorted
index where the first entry in a group is the minimum and the last is the maximum:

```
high 64 bits = group_col_value
low  64 bits = agg_col_value        ← sorts within group by the value being aggregated
```

Call this an `AggValueIndex`. All existing storage machinery (EphemeralTable, UnifiedCursor,
`seek()`, weight consolidation in `_find_next_non_ghost()`) works without any changes.

For **MAX**, the bits are complemented so that the largest value maps to the smallest key:

```
low 64 bits = ~agg_col_value        ← MAX index: largest value has smallest key
```

Seeking the group prefix then gives MAX from the first positive-weight entry.

Two index tables are maintained: one for MIN (forward encoding) and one for MAX (complement
encoding). A query using only MIN creates only the MIN index; a query using only MAX creates
only the MAX index. This avoids updating both indexes for single-aggregate queries.

### Type coverage

**Direct support (exact ordering):**

| Agg column type | Encoding |
|---|---|
| U8 / U16 / U32 / U64 | Zero-extend to 64 bits |
| I8 / I16 / I32 / I64 | `val + 2^63` (offset binary, preserves signed ordering) |
| F32 / F64 | IEEE order-preserving: `if sign=1: ~bits else: bits ^ (1<<63)` |

**Not supported (fall back to existing ReduceGroupIndex):**

| Agg column type | Reason |
|---|---|
| U128 | 128-bit value does not fit in the 64-bit low half |
| STRING | No order-preserving 64-bit encoding |

For unsupported agg types, the existing replay approach is retained unchanged.

The group column encoding (high 64 bits) is identical to `ReduceGroupIndex`'s existing
`promote_group_col_to_u64`. Multi-column group keys use `_extract_group_key`'s existing
hash (same as today). For multi-column groups the encoding is a 64-bit Murmur3 hash; key
collisions are astronomically unlikely but theoretically possible — document this clearly.

### New data structure

File: `gnitz/dbsp/ops/group_index.py`

```python
def make_agg_value_idx_schema():
    """Schema for the agg-value secondary index: just a U128 PK, no payload."""
    cols = newlist_hint(1)
    cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="ck"))
    return TableSchema(cols, pk_index=0)


def encode_agg_value_for_min(val_u64):
    """Order-preserving encoding for MIN index (no-op for unsigned)."""
    return val_u64  # caller has already done offset-binary for signed types


def encode_agg_value_for_max(val_u64):
    """Complement encoding so that largest value has smallest key."""
    return r_uint64(~intmask(val_u64))


class AggValueIndex(object):
    """
    Value-sorted secondary index for MIN/MAX aggregation.
    Key layout: (group_col_u64 << 64 | encoded_agg_col_u64).
    No payload — the aggregate value IS decodable from the key itself.

    for_max=False  → MIN index (direct encoding)
    for_max=True   → MAX index (complemented encoding)
    """
    _immutable_fields_ = ["table", "gc_col_idx", "gc_type",
                          "agg_col_idx", "agg_col_type", "for_max"]

    def __init__(self, table, gc_col_idx, gc_type,
                 agg_col_idx, agg_col_type, for_max):
        self.table = table
        self.gc_col_idx = gc_col_idx
        self.gc_type = gc_type
        self.agg_col_idx = agg_col_idx
        self.agg_col_type = agg_col_type
        self.for_max = for_max

    def create_cursor(self):
        return self.table.create_cursor()
```

Add a helper `promote_agg_col_to_u64_ordered(accessor, col_idx, col_type, for_max)` that
applies the encoding listed in the table above and then complements if `for_max=True`.

For F64/F32, the order-preserving encoding:
```python
def _ieee_order_bits(raw_bits):
    """Convert IEEE 754 bit pattern to order-preserving unsigned key."""
    if raw_bits >> 63:          # negative: flip all bits
        return r_uint64(~intmask(raw_bits))
    else:                       # non-negative: flip only sign bit
        return r_uint64(intmask(raw_bits) ^ intmask(r_uint64(1) << 63))
```

### INTEGRATE dispatch changes

File: `gnitz/vm/interpreter.py`, OPCODE_INTEGRATE handler (lines 167–194)

The existing handler maintains `ReduceGroupIndex` by packing `(gc_u64 << 64 | source_pk_lo)`.
Extend it to also handle `AggValueIndex`:

```python
if instr.agg_value_idx is not None:
    avi = instr.agg_value_idx
    for idx in range(n):
        b.bind_accessor(idx, acc)
        if acc.is_null(avi.agg_col_idx):
            continue
        gc_u64 = _gi_promote(acc, avi.gc_col_idx, avi.gc_type)
        av_u64 = promote_agg_col_to_u64_ordered(acc, avi.agg_col_idx,
                                                 avi.agg_col_type, avi.for_max)
        ck = (r_uint128(gc_u64) << 64) | r_uint128(av_u64)
        weight = b.get_weight(idx)
        try:
            avi.table.memtable.upsert_single(ck, weight, UNIT_ACCESSOR)
        except errors.MemTableFullError:
            avi.table.flush()
            avi.table.memtable.upsert_single(ck, weight, UNIT_ACCESSOR)
```

`UNIT_ACCESSOR` is a new zero-payload accessor (similar to `GroupIdxAccessor` but with
no columns at all — or a single dummy column if the schema requires it). Add it to
`group_index.py`.

The `integrate_op` instruction builder needs a new optional `agg_value_idx` field alongside
the existing `group_idx` field.

### op_reduce changes

File: `gnitz/dbsp/ops/reduce.py`

The `op_reduce` signature gains two new optional parameters:
```python
def op_reduce(
    delta_in, input_schema, trace_in_cursor, trace_out_cursor,
    out_writer, group_by_cols, agg_func, output_schema,
    trace_in_group_idx=None,   # existing ReduceGroupIndex
    agg_value_idx=None,        # NEW: AggValueIndex for MIN/MAX
):
```

In the per-group loop, after the retraction phase, replace the current `else:` replay block
for MIN/MAX with:

```python
if agg_func.is_linear() and has_old:
    agg_func.merge_accumulated(old_val_bits, r_int64(1))
elif agg_func.is_linear():
    pass  # step 3 accumulator already holds Agg(D) = Agg(H+D) for new group
elif agg_value_idx is not None:
    # O(log N + d) path: seek value-sorted index, read min/max from key directly
    av_u64 = _read_agg_from_value_index(agg_value_idx, gc_u64, agg_func)
    if av_u64 is not None:
        agg_func._acc = r_int64(intmask(av_u64))    # set result directly
        agg_func._has_value = True
else:
    # existing replay path (ReduceGroupIndex or full scan)
    ...
```

The helper `_read_agg_from_value_index(avi, gc_u64, agg_func)`:

```python
def _read_agg_from_value_index(avi, gc_u64, agg_func):
    gi_cursor = avi.create_cursor()
    gi_cursor.seek(r_uint128(gc_u64) << 64)
    result = None
    if gi_cursor.is_valid():
        k = gi_cursor.key()
        if r_uint64(intmask(k >> 64)) == gc_u64:
            # First positive-weight entry IS the min/max
            encoded_av = r_uint64(intmask(k))
            if avi.for_max:
                result = r_uint64(~intmask(encoded_av))   # un-complement
            else:
                result = encoded_av                        # direct (or undo offset-binary below)
    gi_cursor.close()
    return result
```

Note: "undo offset-binary" for signed types (subtract `2^63` if the agg column is signed).
This is a single mask operation, add a helper `decode_agg_value(encoded, col_type)`.

Also note: the `gi_cursor` is created once per group here. See Optimization 3 for the fix
to hoist this.

### program_cache.py changes

File: `gnitz/catalog/program_cache.py`, OPCODE_REDUCE section (lines 492–516)

Add logic to create an `AggValueIndex` when the aggregate is MIN or MAX and the agg column
type is eligible:

```python
agg_val_idx = None
if (tr_in_table is not None
        and agg_func_id in (functions.AGG_MIN, functions.AGG_MAX)
        and len(gcols) == 1):
    agg_col_type = in_reg.table_schema.columns[agg_col_idx].field_type
    gc_type = in_reg.table_schema.columns[gcols[0]].field_type
    if _agg_value_idx_eligible(agg_col_type) and _gi_eligible(gc_type):
        for_max = (agg_func_id == functions.AGG_MAX)
        av_table = EphemeralTable(
            tr_in_table.directory + "_avidx",
            "_avidx",
            group_index.make_agg_value_idx_schema(),
            table_id=0,
        )
        agg_val_idx = group_index.AggValueIndex(
            av_table, gcols[0], gc_type, agg_col_idx, agg_col_type, for_max
        )
```

Where:
```python
def _agg_value_idx_eligible(col_type):
    return col_type.code not in (TYPE_U128.code, TYPE_STRING.code)

def _gi_eligible(col_type):
    return col_type.code not in (TYPE_U128.code, TYPE_STRING.code,
                                 TYPE_F32.code, TYPE_F64.code)
```

Pass `agg_val_idx` to both `reduce_op(...)` and `integrate_op(...)`.

When `agg_val_idx` is set, the existing `grp_idx` (ReduceGroupIndex) is still created
because it may be needed if a future aggregate function requires full row access. However,
if `agg_val_idx` is set, `op_reduce` uses it and skips the ReduceGroupIndex path for
MIN/MAX. The ReduceGroupIndex INTEGRATE cost is paid regardless. See Optimization 2 for
eliminating `trace_in` + ReduceGroupIndex entirely for MIN/MAX.

### Impact summary

For `MIN`/`MAX` on an eligible column type:

| | Before | After |
|---|---|---|
| Allocations per group per tick | 1 (`replay` batch) | 0 |
| Seeks per group | O(k) (one per historical row) | 1 (seek to group prefix) |
| Consolidation per group | O(k log k) | 0 |
| Step() calls per group | O(k) | 0 |
| Time per group | O(k) | O(log N + d) |

where k = historical rows in group, d = deleted entries at group boundary (typically 0).

---

## Optimization 2: Eliminate `trace_in` for MIN/MAX with AggValueIndex

### The problem

When `AggValueIndex` is created (Optimization 1), `trace_in` still exists and is still
maintained (INTEGRATE writes to it). But `op_reduce` no longer reads from it for MIN/MAX.
The INTEGRATE cost for `trace_in` is still paid: one `ingest_batch` call per tick.

### The solution

When `agg_val_idx` is set, do not create `tr_in_table` at all. The AggValueIndex alone is
sufficient for MIN/MAX: it stores the aggregate answer directly in the key.

File: `gnitz/catalog/program_cache.py`, OPCODE_REDUCE (lines 476–506)

Change the condition that creates `tr_in_table`:

```python
# Before:
elif not agg_func.is_linear():
    tr_in_table = ...

# After:
elif not agg_func.is_linear() and not _will_use_agg_value_idx(agg_func_id, gcols, in_reg):
    tr_in_table = ...
```

Where `_will_use_agg_value_idx` checks the same eligibility conditions as in Optimization 1.

When `tr_in_table` is None and `agg_val_idx` is not None:
- No `integrate_op(in_reg, tr_in_table)` is emitted
- The INTEGRATE for the input delta only maintains the AggValueIndex

This removes the largest storage overhead for MIN/MAX queries: the full copy of the input
table in `_reduce_in_*`. For a 1M-row table, this saves 1M rows × row_size of storage and
one `ingest_batch` call per tick.

Note: the fallback path (no AggValueIndex, O(N) full scan or ReduceGroupIndex) still
creates `tr_in_table` as today.

---

## Optimization 3: Hoist `gi_cursor` Creation Out of the Group Loop (Bug Fix)

### The problem

In `op_reduce` (`reduce.py:442`), inside the `while idx < n` group loop:

```python
gi_cursor = trace_in_group_idx.create_cursor()   # ← called per group
gi_cursor.seek(target_prefix)
...
gi_cursor.close()
```

`create_cursor()` on an `EphemeralTable` triggers `MemTableCursor.__init__`, which merges
all sorted runs into a consolidated snapshot via `_merge_runs_to_consolidated()`. This is
O(m log m) per call where m = total entries in the group index. With G groups per tick,
this is O(G × m log m) — equivalent to re-sorting the entire group index for every group.

### The fix

Create the cursor **once** before the group loop and reuse it via `seek()` per group.
`seek()` just repositions the cursor with a binary search in the already-built snapshot.

In `op_reduce`, before the `while idx < n` loop:

```python
gi_cursor = None
avi_cursor = None
if trace_in_group_idx is not None:
    gi_cursor = trace_in_group_idx.create_cursor()
if agg_value_idx is not None:
    avi_cursor = agg_value_idx.create_cursor()
```

Then inside the loop, replace `gi_cursor = trace_in_group_idx.create_cursor()` with just:
```python
gi_cursor.seek(target_prefix)
```

And remove the `gi_cursor.close()` call from inside the loop, instead closing after the
`while idx < n` loop ends:

```python
if gi_cursor is not None:
    gi_cursor.close()
if avi_cursor is not None:
    avi_cursor.close()
```

**Impact:** For G groups per tick, reduces cursor creation cost from O(G × m log m) to
O(m log m) (one snapshot, G seeks). For G=100 groups and m=10K index entries, this is a
100× reduction in snapshot-merge work.

**Correctness:** The group index is only written by the INTEGRATE instruction, which runs
*after* REDUCE in the program. The snapshot created at the start of REDUCE reflects all
rows integrated in previous ticks, which is exactly the correct history. Safe to hoist.

---

## Optimization 4: Fix Linear Shortcut for New Groups (`has_old = False`)

### The problem (correctness + performance)

The linear shortcut condition is:

```python
if agg_func.is_linear() and has_old:   # ← has_old guard is wrong
    agg_func.merge_accumulated(old_val_bits, r_int64(1))
else:
    # Replay path — also used for: linear + new group
    replay = ArenaZSetBatch(...)
    ...
    agg_func.reset()
    for row in merged with w > 0:
        agg_func.step(row, w)
```

When `is_linear() and not has_old` (a group appearing for the first time), the code falls
through to the replay path. The replay path:
1. Builds a replay batch with only the delta rows (trace_in is empty for a new group)
2. Calls `to_consolidated()` on it
3. Calls `agg_func.reset()` — **discards the step 3 accumulation**
4. Re-aggregates with the `w > 0` filter — **ignores negative-weight delta rows**

This is both wasteful (redundant allocation and step loop) and **incorrect**: the replay
path's `w > 0` filter skips rows with negative weight, giving `Agg(D+)` instead of
`Agg(D)`, which violates the formula `δ_out = Agg(H+D) - Agg(H) = Agg(D)` when `H = ∅`.

The formula is correct only when all delta rows are insertions (w=+1), which holds for
standard INSERT-only workloads but not for complex DBSP circuits that may produce
retractions into new groups.

### The fix

Remove the `has_old` guard:

```python
# Before:
if agg_func.is_linear() and has_old:
    agg_func.merge_accumulated(old_val_bits, r_int64(1))
else:
    # replay ...

# After:
if agg_func.is_linear():
    if has_old:
        agg_func.merge_accumulated(old_val_bits, r_int64(1))
    # else: step 3 accumulator already holds Agg(D) which equals Agg(H+D) since H=∅
else:
    # replay for MIN/MAX only (or AggValueIndex path if available)
    ...
```

For `is_linear() and not has_old`, the step 3 accumulation in the delta scan is `Agg(D)`.
Since `H = ∅`, `Agg(H+D) = Agg(∅+D) = Agg(D)`. The accumulator already holds the answer.

**Impact:**
- Eliminates one `ArenaZSetBatch` allocation per new group per tick for SUM/COUNT
- Eliminates one `to_consolidated()` + re-aggregation loop per new group
- Fixes the correctness gap for mixed-sign deltas in new groups

---

## Optimization 5: Skip `step()` Calls for Non-Linear Aggregates in Delta Scan

### The problem

The delta accumulation loop (step 3, reduce.py:382–395) calls `agg_func.step()` for every
delta row regardless of whether the aggregate is linear:

```python
agg_func.reset()
while idx < n:
    curr_idx = sorted_indices[idx]
    b.bind_accessor(curr_idx, acc_in)
    if ...: break
    agg_func.step(acc_in, b.get_weight(curr_idx))   # called even for MIN/MAX
    idx += 1
```

For non-linear aggregates (MIN/MAX), step 5 immediately calls `agg_func.reset()` again,
discarding all of step 3's work. This is pure waste.

### The fix

Promote `is_linear()` before the group loop and guard the `step()` call:

```python
is_lin = jit.promote(agg_func.is_linear())
...
# Inside delta scan:
if is_lin:
    agg_func.step(acc_in, b.get_weight(curr_idx))
```

Because `is_linear()` uses `jit.promote(self.agg_op)` internally, the JIT already sees
the aggregate type as a compile-time constant when tracing. Promoting `is_lin` at the
outer scope guarantees the branch is compiled away in the trace for both the linear and
non-linear specializations.

**Impact:** For MIN/MAX with k delta rows per group, saves k calls to `step()` per group
per tick. Each `step()` call reads the aggregate column, does a comparison, and potentially
writes `_acc`. For large delta groups, this is measurable.

---

## Optimization 6: AVERAGE Aggregate

### The problem

gnitz supports COUNT (1), SUM (2), MIN (3), MAX (4). AVERAGE is missing.

AVERAGE cannot be maintained as a single scalar the way SUM is. The running average
changes when rows are inserted or deleted. The correct approach is to maintain the
(sum, count) pair and compute `sum / count` on read.

### Design

AVERAGE is a **linear** aggregate: `avg(H+D)` can be computed from `avg(H)` and `D` by
maintaining the sum and count separately. Specifically:

```
new_sum   = old_sum + sum(D)
new_count = old_count + count(D)
new_avg   = new_sum / new_count
```

#### Output schema change

The output schema currently has one aggregate column. For AVERAGE, we need two: the sum
and the count. This is the most invasive part.

Option A: two separate aggregate columns in the output schema — requires schema changes
across `_build_reduce_output_schema`, `ReduceAccessor`, and the IPC layer.

Option B: pack `(sum_i32, count_i32)` into a single I64 column (limited to small sums).

Option C: add a `TYPE_AVG_STATE` virtual type (a tagged U128 holding both values) and
encode `sum` in the high 64 bits and `count` in the low 32 bits.

**Recommended:** Option A with two aggregate columns, because it is transparent, debuggable,
and does not couple sum range to count range. The client receives both and computes the
final average. This matches how Feldera's `Avg` accumulator works (`(sum, count)` pair).

#### New accumulator fields

Add `AGG_AVG = 5` to `functions.py`.

`UniversalAccumulator` gains a second accumulator field `_count` (I64). For `AGG_AVG`:
- `step(accessor, weight)`: `_acc += val * weight` (sum); `_count += weight` (count)
- `merge_accumulated(old_sum_bits, old_count_bits, weight)`: `_acc += old_sum * weight`;
  `_count += old_count * weight`
- `is_linear()`: returns `True` — AVERAGE is linear in the (sum, count) sense
- `is_accumulator_zero()`: returns `_count == 0`

The `merge_accumulated` signature currently takes `(value_bits, weight)`. For AVERAGE, the
"old value" is two pieces: old sum and old count. This requires either:
- Packing both into a U128 and threading it through `ReduceAccessor` as a U128 field
- Or extending the interface to pass both `old_sum_bits` and `old_count_bits` separately

Simplest: pack `(sum_i32_or_i64, count_i32)` into the single existing U128 output column,
with the sum in the high bits and the count in the low bits. `ReduceAccessor.get_int()`
returns the appropriate half depending on which column is requested. The client decodes.

This avoids schema changes but requires a clear contract. Document the bit layout.

#### VM / compiler changes

- `_build_reduce_output_schema`: recognize `AGG_AVG`, add two I64 columns (or one U128)
- `ReduceAccessor`: add a mapping for the count column
- `program_cache.py`: `agg_func_id == AGG_AVG` follows the linear path (no `trace_in`)

---

## Optimization 7: Semigroup Combining for Distributed Aggregation

### The problem

When the multi-worker exchange operator (roadmap phase 3) partitions input rows across
workers, each worker holds a subset of the input table. For a `GROUP BY` query where one
group spans multiple partitions, each worker computes a **partial aggregate**. These
must be combined at a gather step.

gnitz has no `combine(partial_a, partial_b) → result` operation on aggregates. Without it,
distributed GROUP BY requires routing all rows for each group to the same worker (via hash
partitioning on the group key), which limits parallelism and causes data skew.

### Design

Add a `combine(other_bits)` method to `AggregateFunction`:

```python
class AggregateFunction(object):
    def combine(self, other_bits):
        """Merge another worker's partial aggregate into this accumulator."""
        pass
```

Implementations:

| Aggregate | `combine(other_bits)` |
|---|---|
| COUNT | `_acc += other_bits` (same as `merge_accumulated`) |
| SUM | `_acc += other_bits` (same as `merge_accumulated`) |
| MIN | `_acc = min(_acc, other_bits)` |
| MAX | `_acc = max(_acc, other_bits)` |

The gather worker's REDUCE instruction would:
1. Receive partial aggregate batches from each shard worker
2. For each group key: call `combine()` for each shard's partial result
3. Emit the final merged aggregate

This is exactly Feldera's `Semigroup::combine` pattern. The implementation is trivial for
all four existing aggregate types and does not require changes to storage or cursors.

The exchange operator wiring (how partial aggregates are routed and combined) is out of
scope for this plan; it belongs in the multi-core roadmap. This item only covers adding
`combine()` to `UniversalAccumulator`.

---

## Implementation order and dependencies

```
1. Hoist gi_cursor (Opt 3)              ← no deps, pure bugfix, 10-line change
2. Fix linear shortcut (Opt 4)          ← no deps, correctness + perf fix, 5-line change
3. Skip step() for non-linear (Opt 5)   ← no deps, 3-line change
4. AggValueIndex schema + encoding (Opt 1, step 1)  ← no deps
5. AggValueIndex INTEGRATE dispatch (Opt 1, step 2) ← depends on (4)
6. AggValueIndex op_reduce lookup (Opt 1, step 3)   ← depends on (4)
7. program_cache wiring (Opt 1, step 4) ← depends on (4-6)
8. Eliminate trace_in for MIN/MAX (Opt 2) ← depends on (7)
9. AVERAGE aggregate (Opt 6)            ← no deps on above, but schema work required
10. Semigroup combine (Opt 7)           ← no deps, prerequisite for multi-worker exchange
```

Items 1–3 are independent, low-risk, and should be done first. Items 4–8 are a single
cohesive feature (AggValueIndex). Items 9–10 are independent new features.

---

## Test approach

| Optimization | What to test |
|---|---|
| Opt 1+2 (AggValueIndex) | MIN/MAX query: assert correct result; assert `trace_in` table is not created; measure seeks vs. old path |
| Opt 3 (cursor hoist) | Same output correctness for ReduceGroupIndex path; no regression |
| Opt 4 (linear new group) | SUM query: delta contains both insertions AND a deletion for a new group in the same tick; assert output matches expected Agg(D) including the negative-weight contribution |
| Opt 5 (skip step for non-linear) | MIN/MAX correctness unchanged; verify via existing reduce tests |
| Opt 6 (AVERAGE) | `AVG(col) GROUP BY g`: assert (sum, count) columns correct after mixed inserts/deletes |
| Opt 7 (semigroup) | `combine()` unit tests for all four agg types; integration test deferred to exchange operator work |

All tests run with `GNITZ_WORKERS=4`.
