# Source Operator Optimizations

**Status: DONE** (committed f487d2f). Remaining: `join-optimizations.md` Opt 2 (merge-walk), which this plan unlocks.

---

## What was implemented

`_consolidated` flag on `ArenaZSetBatch` (sorted by PK, unique full-row keys, no zero weights, strictly stronger than `_sorted`) + seal at `execute_epoch` that enforces the invariant at every circuit-boundary crossing.

### `batch.py`
- `_consolidated = False` field; `mark_consolidated(value)` (sets `_sorted=True` when True); `clear()` resets it; `clone()` and `to_sorted()` preserve it.
- `to_consolidated()` short-circuits when `_consolidated` or `_count == 0` (returns `self`, zero allocation); marks result `_consolidated = True`.
- `BatchWriter.mark_consolidated` delegate.

### Production sites
| Site | File | Change |
|---|---|---|
| `op_scan_trace` | `source.py` | `mark_consolidated(True)` after loop |
| `op_distinct` | `distinct.py` | `mark_consolidated(True)` replacing `mark_sorted(True)` |
| `op_filter`, `op_negate`, `op_delay` | `linear.py` | propagate: `mark_consolidated(True)` if input is consolidated, else `mark_sorted(_sorted)` |
| `op_anti_join_delta_delta`, `op_semi_join_delta_delta` | `anti_join.py` | `mark_consolidated(True)` replacing `mark_sorted(True)` |

`op_map` and `op_union` not changed (map may remap PKs; union of two sorted batches is unsorted).

### Seal at `execute_epoch` (`runtime.py`)
```python
sealed = input_delta.to_consolidated()   # free if already consolidated
in_reg.bind(sealed)
...
in_reg.unbind()
if sealed is not input_delta:
    sealed.free()
```

### `op_reduce` — excluded (incorrect in original plan)
`op_reduce` emits retract+insert pairs: when the new aggregate equals the old, both rows share the same full-row key with cancelling weights. Marking output `_consolidated=True` would cause a downstream `ConsolidatedScope` to short-circuit and skip that cancellation → wrong output. Keep `mark_sorted(group_by_pk)`.

### `op_join_delta_delta` — excluded (incorrect in original plan)
`op_join_delta_delta` uses `SortedScope`, not `ConsolidatedScope`. Duplicate PKs in the input are valid (DBSP deltas); the cross-product loop emits multiple output rows per input PK. Output is sorted but not consolidated. Keep `mark_sorted(True)`.

### Bug fixed alongside
`ReduceAccessor.is_null` called `is_accumulator_zero()` even when `use_old_val=True`. For non-linear MIN/MAX the accumulator is never stepped before the retraction row is emitted, so the null bit was set and the old value zeroed. Fix: return `False` immediately when `use_old_val=True`.

---

## Remaining work

### `join-optimizations.md` Opt 2 — merge-walk for `op_join_delta_trace`

This optimization is now unblocked. It replaces the per-row cursor seek in `op_join_delta_trace` with a linear merge-walk when `delta_batch._consolidated` is True (guaranteed by the seal). See `join-optimizations.md` for the full spec.

#### Defects found in the plan (must fix during implementation)

**1. Empty-batch OOB in merge-walk (join and anti-join variants)**

Both `_join_dt_merge_walk` and `_anti_join_dt_merge_walk` as written in the plan call
`trace_cursor.seek(delta_batch.get_pk(0))` unconditionally. An empty batch can have
`_consolidated = True` because `mark_consolidated(True)` is called unconditionally at the
end of `op_anti_join_delta_delta` and `op_semi_join_delta_delta` regardless of output
size. That empty-consolidated batch can reach a downstream join as the delta input. At
`execute_epoch`, `to_consolidated()` short-circuits for an already-consolidated batch and
returns `self` without clearing the flag, so the empty batch arrives at the dispatch with
`_consolidated == True` and `length() == 0`. Calling `get_pk(0)` then crashes.

Fix: add an early-out at the top of each merge-walk helper:
```python
count = delta_batch.length()
if count == 0:
    return
trace_cursor.seek(delta_batch.get_pk(0))
```

**2. `UnifiedCursor.estimated_length()` generator expression is not valid RPython**

The plan proposes:
```python
return sum(c.estimated_length() for c in self.cursors)
```
RPython does not support generator expressions. Must be an explicit loop:
```python
def estimated_length(self):
    total = 0
    for c in self.cursors:
        total += c.estimated_length()
    return total
```

**3. Anti-join adaptive swap is incorrect (semi-join swap is valid)**

The plan's Step 5 claims anti-join can use the same swap as inner join: "iterate trace
keys and check for exact match in delta." This is wrong. Anti-join must emit delta rows
whose key has **no** trace match. If you iterate trace, you only visit keys that ARE in
the trace — you can identify rows to suppress but you can never identify the delta rows
with no trace counterpart, which are the ones that must be emitted. The swap as described
would always produce empty output.

No beneficial adaptive swap exists for anti-join: the operation is inherently
delta-driven and must visit every delta row. The masterplan's Step 8d ("same adaptive
swap + merge-walk for anti/semi-join") should be read as: merge-walk applies to both;
adaptive swap applies to semi-join only.

Semi-join swap IS valid: iterate trace, seek delta, emit delta rows found at each trace
key. Delta rows with no trace counterpart are naturally skipped, which is correct.

**4. Swapped path output sorted flag is under-reported (minor)**

After `_join_dt_swapped`, the plan writes `out_writer.mark_sorted(delta_batch._sorted)`.
But the swap iterates the trace in sorted key order, so the output is always sorted by PK
regardless of the original delta's `_sorted` flag. Should be `out_writer.mark_sorted(True)`.

---

## Dependencies

- `join-optimizations.md` Opt 2 depended on `_consolidated` being set on R0 → now satisfied.
- `join-optimizations.md` Opt 3 (`ConsolidatedScope` in delta-delta): still valuable as defence-in-depth for intra-circuit deltas not yet propagating `_consolidated`.
- `reduce-optimizations.md`: `op_reduce` output is sorted (not consolidated); downstream `op_distinct` still pays one `ConsolidatedScope` pass.
