# Plan: Reduce Operator Optimizations

This plan covers remaining performance optimizations and new features for the `REDUCE`
operator. Correctness fixes (Opts 1–4) and AggValueIndex (Opts 1–2) are already
implemented.

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
| AggValueIndex | `not is_linear(), MIN/MAX on eligible col` | O(log N) per group |
| Full scan fallback | everything else | O(N) per group |

---

## Optimization 5: Skip `step()` Calls for Non-Linear Aggregates in Delta Scan ✓ DONE

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

## Implementation order

```
1. Skip step() for non-linear (Opt 5)   ← ✓ DONE
2. AVERAGE aggregate (Opt 6)            ← no deps, schema work required
3. Semigroup combine (Opt 7)            ← no deps, prerequisite for multi-worker exchange
```

---

## Test approach

| Optimization | What to test |
|---|---|
| Opt 5 (skip step for non-linear) | ✓ DONE |
| Opt 6 (AVERAGE) | `AVG(col) GROUP BY g`: assert (sum, count) columns correct after mixed inserts/deletes |
| Opt 7 (semigroup) | `combine()` unit tests for all four agg types; integration test deferred to exchange operator work |

All tests run with `GNITZ_WORKERS=4`.
