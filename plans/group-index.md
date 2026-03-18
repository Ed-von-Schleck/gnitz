# Group Index Optimizations

Two issues identified in the `ReduceGroupIndex` and `AggValueIndex` secondary index
infrastructure. Issue 1 is already planned in `reduce-optimizations.md` Optimization 3;
this document adds Issue 2 and documents a known correctness risk.

---

## Issue 1: Per-group cursor creation (already planned)

**Covered by `reduce-optimizations.md` Optimization 3.** Reproduced here for completeness.

`op_reduce` (`reduce.py:442`) calls `trace_in_group_idx.create_cursor()` inside the
per-group `while idx < n` loop. Each call allocates a `UnifiedCursor` — one sub-cursor
per shard, tournament-tree build, and a memtable snapshot merge. For a delta with G groups
and a group index with m total entries, this is O(G × m log m) re-work instead of the
O(m log m) once-per-tick that a hoisted cursor would cost.

The fix is to hoist both `gi_cursor` and (once AggValueIndex exists) `avi_cursor` to
before the `while idx < n` loop and seek them per group instead. See `reduce-optimizations.md`
Optimization 3 for the exact code.

---

## Issue 2: AggValueIndex restricted to single ≤64-bit integer group columns

### Problem

`reduce-optimizations.md` Optimization 1 introduces `AggValueIndex` for O(1) MIN/MAX
lookup. The planned eligibility condition (`program_cache.py` OPCODE_REDUCE block) gates
on two requirements:

1. `len(gcols) == 1` — single group column only
2. `_gi_eligible(gc_type)` — ≤64-bit integer type only (excludes U128, STRING, F32, F64)

Any MIN/MAX query outside this — `GROUP BY (dept_id, year)`, `GROUP BY string_col`,
`GROUP BY uuid_col` — still takes the O(N) full trace scan at `reduce.py:471–481` even
after `reduce-optimizations.md` is fully implemented. When `AggValueIndex` is not created,
`reduce-optimizations.md` Opt 2 still creates `tr_in_table` (the full input history copy),
so these queries also pay the per-tick `ingest_batch` overhead for maintaining a trace
that is only used in an O(N) scan.

### Root cause

The high 64 bits of the `AggValueIndex` composite key encode the group column value. For
single ≤64-bit integer columns, `promote_group_col_to_u64` gives an exact bit-pattern
with zero collision risk. For other types there is no exact 64-bit representation, so the
index was not attempted.

However, `_extract_group_key` (`reduce.py:286–319`) already computes a 128-bit group key
for all cases:

- Single U64: exact 64-bit value
- Single U128: exact 128-bit value
- Multi-column / STRING / FLOAT: 128-bit Murmur3 hash

Taking the **low 64 bits** of this result as `gc_u64` gives:

- Single ≤64-bit int (U8/U16/U32/U64/I8/I16/I32/I64): exact bit pattern via
  `promote_group_col_to_u64` — zero collision risk
- Everything else (U128, STRING, FLOAT, multi-column): 64-bit value derived from Murmur3
  hash — collision probability ~1/2^64 per group pair

A gc_u64 hash collision means the `AggValueIndex` prefix scan for one group bleeds into
an adjacent group's entries, potentially returning a wrong MIN/MAX. At 1/2^64 per pair
this is negligible — the same risk accepted for 128-bit PK hash collisions throughout
gnitz.

### Changes required

#### `gnitz/dbsp/ops/group_index.py`

**Step 1**: Move `_mix64` and `_extract_group_key` here from `reduce.py`. Both are
fundamentally about group key computation and belong in `group_index.py`. `reduce.py`
imports them from here afterwards. Add `intmask` to the `rpython.rlib.rarithmetic` import
and `xxh` from `gnitz.core` to support string hashing.

**Step 2**: Add `_extract_gc_u64`:

```python
@jit.unroll_safe
def _extract_gc_u64(accessor, schema, group_by_cols):
    """
    Returns the 64-bit group key used as the high 64 bits of a composite
    secondary index key.

    For a single ≤64-bit integer column: exact bit pattern via
    promote_group_col_to_u64 (zero collision risk).
    All other cases (multi-column, U128, STRING, FLOAT): low 64 bits of the
    128-bit _extract_group_key result (~1/2^64 collision probability per group pair).
    """
    if len(group_by_cols) == 1:
        col_type = schema.columns[group_by_cols[0]].field_type
        if (col_type.code != TYPE_U128.code
                and col_type.code != TYPE_STRING.code
                and col_type.code != TYPE_F32.code
                and col_type.code != TYPE_F64.code):
            return promote_group_col_to_u64(accessor, group_by_cols[0], col_type)
    return r_uint64(intmask(_extract_group_key(accessor, schema, group_by_cols)))
```

**Step 3**: Change `AggValueIndex` to store `group_by_cols` and `input_schema` instead
of `gc_col_idx` + `gc_type`:

```python
class AggValueIndex(object):
    _immutable_fields_ = ["table", "group_by_cols[*]", "input_schema",
                          "agg_col_idx", "agg_col_type", "for_max"]

    def __init__(self, table, group_by_cols, input_schema,
                 agg_col_idx, agg_col_type, for_max):
        self.table = table
        self.group_by_cols = group_by_cols    # list[int]
        self.input_schema = input_schema      # TableSchema
        self.agg_col_idx = agg_col_idx
        self.agg_col_type = agg_col_type
        self.for_max = for_max

    def create_cursor(self):
        return self.table.create_cursor()
```

#### `gnitz/vm/interpreter.py` — OPCODE_INTEGRATE handler

In the `if instr.agg_value_idx is not None:` block added by `reduce-optimizations.md`
Opt 1, replace the `_gi_promote(acc, avi.gc_col_idx, avi.gc_type)` call:

```python
gc_u64 = _extract_gc_u64(acc, avi.input_schema, avi.group_by_cols)
```

Add `_extract_gc_u64` to the import from `group_index`:

```python
from gnitz.dbsp.ops.group_index import (
    GroupIdxAccessor as _GroupIdxAccessor,
    promote_group_col_to_u64 as _gi_promote,
    _extract_gc_u64,
)
```

#### `gnitz/dbsp/ops/reduce.py`

In `_read_agg_from_value_index` (added by `reduce-optimizations.md` Opt 1), replace the
`_gi_promote` call that computes `gc_u64`:

```python
gc_u64 = _extract_gc_u64(acc_exemplar, avi.input_schema, avi.group_by_cols)
```

Since `_mix64` and `_extract_group_key` are moved to `group_index.py`, update the import:

```python
from gnitz.dbsp.ops.group_index import (
    promote_group_col_to_u64 as _gi_promote,
    _extract_group_key,
    _extract_gc_u64,
    ReduceGroupIndex,
)
```

#### `gnitz/catalog/program_cache.py` — OPCODE_REDUCE block

Remove the `len(gcols) == 1` and `_gi_eligible(gc_type)` gates from the `AggValueIndex`
creation condition. Before (as planned in `reduce-optimizations.md` Opt 1):

```python
if (tr_in_table is not None
        and agg_func_id in (functions.AGG_MIN, functions.AGG_MAX)
        and len(gcols) == 1):
    ...
    if _agg_value_idx_eligible(agg_col_type) and _gi_eligible(gc_type):
        ...
        agg_val_idx = group_index.AggValueIndex(
            av_table, gcols[0], gc_type, agg_col_idx, agg_col_type, for_max
        )
```

After:

```python
if (tr_in_table is not None
        and agg_func_id in (functions.AGG_MIN, functions.AGG_MAX)
        and _agg_value_idx_eligible(agg_col_type)):
    # No longer restricted to single ≤64-bit int group columns.
    # Multi-column, U128, STRING, FLOAT group columns use hash encoding
    # via _extract_gc_u64. Collision probability ~1/2^64 per group pair.
    av_table = EphemeralTable(...)
    agg_val_idx = group_index.AggValueIndex(
        av_table,
        gcols,                  # full list, not gcols[0]
        in_reg.table_schema,    # input_schema for _extract_gc_u64
        agg_col_idx, agg_col_type, for_max,
    )
```

Update the `_will_use_agg_value_idx` predicate (used in `reduce-optimizations.md` Opt 2
to decide whether to skip `tr_in_table` creation) to match — drop `len(gcols) == 1` and
`_gi_eligible(gc_type)` from it as well.

### Impact

| Group column | Agg column | Before | After |
|---|---|---|---|
| Single ≤64-bit int | Eligible (not U128/STRING) | O(1) via AggValueIndex (Opt 1) | O(1) — unchanged |
| Multi-column | Eligible | O(N) full scan + tr_in overhead | O(1) seek via hash gc_u64 |
| U128 | Eligible | O(N) full scan + tr_in overhead | O(1) seek via low-64-bit gc_u64 |
| STRING or FLOAT group | Eligible | O(N) full scan + tr_in overhead | O(1) seek via hash gc_u64 |
| Any group | U128 or STRING agg | O(N) or O(log N+k) via ReduceGroupIndex | unchanged — AggValueIndex ineligible |

"Eligible agg" = `_agg_value_idx_eligible`: not U128 and not STRING aggregate column.

For all newly-covered cases, `reduce-optimizations.md` Opt 2 also eliminates `tr_in_table`
(since `_will_use_agg_value_idx` now returns True for them), removing the per-tick
`ingest_batch` overhead for maintaining the full input history copy.

---

## Known limitation: source_pk_lo collision in ReduceGroupIndex

`ReduceGroupIndex` encodes the composite key as `(gc_u64 << 64 | source_pk_lo)` where
`source_pk_lo = r_uint64(intmask(source_pk))` (lower 64 bits of the U128 PK). The upper
64 bits (`spk_hi`) are stored as a payload column (`interpreter.py:186–188`).

If two source rows have the same `gc_u64` AND the same `source_pk_lo`, they produce the
same `ck`. `upsert_single` adds weights and overwrites `spk_hi` with the later writer.
The earlier row is then unrecoverable from the index: that group member is missed on
lookup.

For synthetic (Murmur3-hashed) PKs the probability per pair is ~1/2^64. For natural U64
PKs, `source_pk = r_uint128(pk_val)` so `source_pk_hi = 0` for all rows and uniqueness
is guaranteed by the primary key constraint. No collision possible.

This is accepted as negligible and consistent with the 128-bit PK hash collision risk
policy across gnitz. No fix planned.

Note: `ReduceGroupIndex` is only created when `AggValueIndex` is absent (`tr_in_table is
not None` is required, and `reduce-optimizations.md` Opt 2 eliminates `tr_in_table` when
`AggValueIndex` is created). So after the full `reduce-optimizations.md` implementation,
`ReduceGroupIndex` is only used for MIN/MAX with U128 or STRING aggregate columns. For all
other MIN/MAX cases, `AggValueIndex` is used and `ReduceGroupIndex` is never created.

---

## Dependencies

- Issue 2 depends on `reduce-optimizations.md` Optimizations 1 and 2 being implemented
  first, as it modifies the `AggValueIndex` structure introduced there.
- Issue 1 (cursor hoist) is independent and belongs to `reduce-optimizations.md` Opt 3.

## Implementation order

```
1. reduce-optimizations.md Opt 3 (cursor hoist)            — independent, do first
2. reduce-optimizations.md Opt 1+2 (AggValueIndex, single-col)  — prerequisite for below
3. Move _mix64 + _extract_group_key to group_index.py      — refactor only, ~5 lines moved
4. Add _extract_gc_u64 to group_index.py                   — ~12 lines
5. Change AggValueIndex fields: group_by_cols + input_schema replace gc_col_idx + gc_type
6. Update INTEGRATE handler to call _extract_gc_u64
7. Update _read_agg_from_value_index in reduce.py
8. Remove len(gcols)==1 and _gi_eligible gate in program_cache.py
9. Update _will_use_agg_value_idx predicate to match
```

Steps 3–9 are one cohesive change with no intermediate valid state; do them together.

## Test coverage

Add to `rpython_tests/dbsp_comprehensive_test.py`:

- `test_reduce_min_multi_col_group`: `MIN(value) GROUP BY (dept_id, year)` — exercises the
  hash-based `gc_u64` path; assert correct MIN after inserts and deletions across multiple
  ticks, including groups that appear and disappear
- `test_reduce_max_string_group`: `MAX(value) GROUP BY string_col` — exercises STRING group
  column with eligible agg col; assert correct MAX after mixed inserts/deletes

Both tests run with `GNITZ_WORKERS=4`.
