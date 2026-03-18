# Plan: Wiring `combine` into Distributed Aggregation

## Context

`UniversalAccumulator.combine` was implemented in `gnitz/dbsp/functions.py`. No caller
exists yet. This plan describes what is needed to use it in query execution, validated
against the actual codebase.

---

## Validation of prior design claims

Before describing the work, prior claims are audited against the code.

### Exchange-before-aggregate already exists and works

**Claim (prior):** "currently no `op_reduce` with `exchange_post_plan` is generated"
**Status: WRONG.**

`CircuitBuilder.reduce()` (helpers/circuit_builder.py:121-122) auto-inserts
`OPCODE_EXCHANGE_SHARD` before every `OPCODE_REDUCE`:

```python
def reduce(self, input_handle, agg_func_id, group_by_cols, agg_col_idx=0):
    # Auto-insert exchange shard by group columns for multi-worker correctness
    sharded = self.shard(input_handle, group_by_cols)
    handle = self._alloc_node(OPCODE_REDUCE)
    self._connect(sharded, handle, PORT_IN_REDUCE)
```

`compile_from_graph` (program_cache.py:833-935) splits on `OPCODE_EXCHANGE_SHARD`:
pre-plan = everything before the exchange; post-plan = everything after, which runs
`op_reduce` on the repartitioned raw input rows. So `exchange_post_plan` is already set
on every reduce circuit, and distributed GROUP BY already works correctly today.

The current circuit topology is:

```
Input → [pre-plan: scan + filter/map] → EXCHANGE_SHARD (group key cols) → [post-plan: op_reduce]
```

Raw input rows are routed to the worker that owns each group key. Each worker's `op_reduce`
runs against a complete local history for its groups. This is correct for all aggregate types.

---

### The master does not need to run `combine`

**Claim (prior):** "master-side gather in `master.py`: collect all per-worker partial-aggregate
batches, run `combine` per group key, route final results to owning workers"
**Status: WRONG.**

`_relay_exchange` (master.py:134-156) calls `relay_scatter` to repartition exchange payloads
by shard columns and routes sub-batches to workers. This is already the correct mechanism for
the partial-agg + gather path:

1. Workers send partial aggregate rows via `FLAG_EXCHANGE`
2. Master calls `relay_scatter` — repartitions partial aggregate rows by group key column
3. Each worker receives all partial aggregates for its groups
4. Worker's post-plan runs `op_gather_reduce` (new), which calls `combine`

The master needs **no changes**. It already repartitions correctly regardless of whether
the exchange boundary contains raw input rows or partial aggregate rows.

---

### `ExecutablePlan` does not need an `agg_funcs` field

**Claim (prior):** "`compile_from_graph` needs to attach `agg_funcs` ... to `ExecutablePlan`
so the master can instantiate accumulators"
**Status: WRONG.**

The master never runs `combine`. The workers do, inside `op_gather_reduce`. The agg funcs
remain inside the compiled instruction (as they already do in `reduce_op`). `ExecutablePlan`'s
fields (program_cache.py / runtime.py:117-134) — program, reg_file, out_schema, in_reg_idx,
out_reg_idx, exchange_post_plan, source_reg_map, join_shard_map — are sufficient.

---

### `evaluate_dag` needs no changes

**Claim (prior):** "executor.py needs to distinguish combine-exchange from repartition-exchange"
**Status: WRONG.**

`evaluate_dag` (executor.py:121-133) already handles the exchange path uniformly:

```python
pre_result = plan.execute_epoch(incoming_delta, source_id=source_id)
exchanged = exchange_handler.do_exchange(target_view_id, pre_result)
out_delta = plan.exchange_post_plan.execute_epoch(exchanged)
```

This works for both topologies:
- Pre-plan outputs raw rows → exchange → post-plan runs `op_reduce` (current)
- Pre-plan outputs partial aggregates → exchange → post-plan runs `op_gather_reduce` (new)

No `is_combine_exchange` flag is needed on `ExecutablePlan`.

---

### MIN/MAX constraint is correctly stated but requires fuller explanation

**Claim (prior):** "MIN/MAX must use exchange-before-aggregate; COUNT/SUM can use
partial aggregation + combine"
**Status: CORRECT**, but the mechanism was not spelled out.

**Why the partial-agg + gather path is correct for COUNT/SUM/COUNT_NON_NULL:**

`op_gather_reduce` (see §1 below) needs a `trace_out` to look up `old_global` for each
group. With that in hand, the new global is:

```
new_global = old_global + Σ(+1 partial values) − Σ(−1 partial values)
```

This works because the operation is linear: the global delta equals the sum of per-worker
partial deltas. Workers with no changes send no rows; their contribution is zero, which
is correct. The formula holds regardless of how many workers sent deltas.

**Why the partial-agg + gather path is WRONG for MIN/MAX:**

The gather step receives only the partial deltas from workers that had local changes.
Workers with unchanged partials send nothing. Computing the correct new global MIN/MAX
requires:

```
new_global_MIN = min(current_partial_w for ALL workers w)
```

This depends on ALL workers' current partial values, not just the changed ones. The gather
step has no way to know `current_partial_B` if worker B sent no delta that tick. Even with
a `trace_out` for `old_global`, the formula `new_global = old_global + delta` does not
hold — there is no additive delta formulation for non-linear aggregates.

The only safe approach for MIN/MAX is exchange-before-aggregate: all rows for a group
land on one worker, so `op_reduce` always has complete history and `combine` is never
called across a group split. This is already what the current system does.

Note: the current system uses exchange-before-aggregate for ALL aggregates uniformly
(including COUNT/SUM/COUNT_NON_NULL), which is correct but less efficient for linear
aggregates on large input tables.

---

### `_relay_exchange` correctly stated as unconditional `relay_scatter`

**Status: CONFIRMED.** master.py:146 always calls `relay_scatter`. No changes needed.

---

## What is actually needed

The partial-agg + gather path requires three new pieces. Nothing else.

---

### 1. `op_gather_reduce` — new function in `reduce.py`

The post-plan for the partial-agg circuit receives rows in the **reduce output schema**
(group key + agg columns) with weights ±1. These are partial aggregate rows from multiple
workers for the same set of groups, after repartition by group key.

**This operator is stateful.** It must hold a `trace_out` (same schema and role as
`op_reduce`'s trace_out) tracking the current global aggregate per group. This is
necessary because the output must be expressed as `(−1, old_global), (+1, new_global)`,
which requires `old_global` to be read from the trace. Directly passing partial deltas
upstream would produce multiple rows per group in the view Z-set with conflicting weights,
corrupting the view.

The gather step algorithm for each group G in `partial_batch`:

1. Sort rows by group key (same strategy as `_argsort_delta` but on output schema)
2. Look up `old_global` for G from `trace_out` (or treat as zero/empty if absent)
3. Accumulate the global delta:
   - For each +1 row: `combine(row.agg_bits)` into a fresh accumulator (adds new partial)
   - For each −1 row: subtract old partial value from the accumulator explicitly
     (linear aggregates only — this is integer/float arithmetic, not a `combine` call)
4. `new_global = old_global + accumulated_delta`
5. If `new_global ≠ old_global`: emit `(−1, old_global)` and `(+1, new_global)`
6. Update `trace_out` with `new_global`

**Safety constraint:** `op_gather_reduce` is only correct for linear aggregates
(COUNT, SUM, COUNT_NON_NULL). See the linearity argument in the validation section above.
It must never be emitted for MIN/MAX agg functions.

Signature:
```python
def op_gather_reduce(partial_batch, trace_out_cursor, trace_out_table,
                     group_col_indices, agg_funcs, out_schema, out_writer):
```

Where `partial_batch` has schema = reduce output schema, `group_col_indices` are the
group column positions in the output schema, `trace_out_cursor`/`trace_out_table` track
the current global aggregate (same as `op_reduce`'s tr_out), and `agg_funcs` hold the
accumulators (reset between groups).

---

### 2. A new opcode `OPCODE_GATHER_REDUCE` in `gnitz/core/opcodes.py`

`op_gather_reduce` is emitted by a new opcode in `_emit_node`. It uses the same params
as `OPCODE_REDUCE` (group cols, agg funcs) but:
- Input register: reduce output schema (partial aggregates)
- One trace register: `trace_out` in reduce output schema (for reading/writing old_global)
- Output: merged aggregate delta in reduce output schema

The existing `OPCODE_EXCHANGE_SHARD` (unchanged) is placed after `OPCODE_REDUCE` and
before `OPCODE_GATHER_REDUCE` in the circuit graph.

---

### 3. `CircuitBuilder.reduce_partial()` — new method in circuit_builder.py

```python
def reduce_partial(self, input_handle, agg_func_id, group_by_cols, agg_col_idx=0):
    """Partial-agg + gather path. Only correct when agg is linear
    (COUNT, SUM, COUNT_NON_NULL). Must NOT be used with AGG_MIN or AGG_MAX.
    Inserts EXCHANGE_SHARD after REDUCE, followed by GATHER_REDUCE."""
    reduce_handle = self._alloc_node(OPCODE_REDUCE)
    self._connect(input_handle, reduce_handle, PORT_IN_REDUCE)
    self._params.append((reduce_handle.node_id, PARAM_AGG_FUNC_ID, agg_func_id))
    self._params.append((reduce_handle.node_id, PARAM_AGG_COL_IDX, agg_col_idx))
    for col_idx in group_by_cols:
        self._group_cols.append((reduce_handle.node_id, col_idx))
    # Exchange by PK of reduce output (group hash), col index 0
    sharded = self.shard(reduce_handle, [0])
    gather = self._alloc_node(OPCODE_GATHER_REDUCE)
    self._connect(sharded, gather, PORT_IN_REDUCE)
    self._params.append((gather.node_id, PARAM_AGG_FUNC_ID, agg_func_id))
    self._params.append((gather.node_id, PARAM_AGG_COL_IDX, agg_col_idx))
    for col_idx in group_by_cols:
        self._group_cols.append((gather.node_id, col_idx))
    return gather
```

The existing `reduce()` method (exchange-before-aggregate) is unchanged and remains the
default for all circuits. `reduce_partial()` is opt-in, and the caller is responsible
for only using it when all aggregates are linear.

---

## Circuit topology comparison

| Path | Circuit topology |
|---|---|
| Current (exchange-before-agg) | `Scan → SHARD(group cols) → REDUCE → Integrate` |
| New (partial-agg + gather) | `Scan → REDUCE → SHARD(pk col 0) → GATHER_REDUCE → Integrate` |

The pre-plan / post-plan split occurs at `OPCODE_EXCHANGE_SHARD` in both cases.
`compile_from_graph` handles this uniformly (existing code at program_cache.py:833).

---

## What does NOT need to change

| Component | Status |
|---|---|
| `master.py` `_relay_exchange` | No changes — `relay_scatter` handles both paths |
| `executor.py` `evaluate_dag` | No changes — pre/post exchange flow is topology-agnostic |
| `ExecutablePlan` fields | No changes — no `agg_funcs` or exchange-type flag needed |
| `ipc.py` / IPC flags | No changes — `FLAG_EXCHANGE` is sufficient for both paths |
| `OPCODE_EXCHANGE_SHARD` | No changes — reused unchanged for both paths |
| `get_shard_cols` | No changes — reads PARAM_SHARD_COL_BASE regardless of position in graph |

---

## Files to touch

| File | Change |
|---|---|
| `gnitz/core/opcodes.py` | Add `OPCODE_GATHER_REDUCE` constant |
| `gnitz/dbsp/ops/reduce.py` | Add `op_gather_reduce` function |
| `gnitz/vm/instructions.py` | Add `gather_reduce_op` instruction builder |
| `gnitz/vm/interpreter.py` | Dispatch `OPCODE_GATHER_REDUCE` to `op_gather_reduce` |
| `gnitz/catalog/program_cache.py` | Handle `OPCODE_GATHER_REDUCE` in `_emit_node`; count +1 extra reg (trace_out) in post-plan extra_regs loop (program_cache.py:864-875) |
| `helpers/circuit_builder.py` | Add `reduce_partial()` and `reduce_multi_partial()` |
| `rpython_tests/dbsp_comprehensive_test.py` | Tests for `op_gather_reduce` |

---

## Verification

Run: `make run-dbsp_comprehensive_test-c` (covers `op_gather_reduce` unit tests)
Run: `GNITZ_WORKERS=4 make run-server_test-c` (E2E with a linear-aggregate view using `reduce_partial`)

---

## Out of scope

- Using `reduce_partial` in `compile_from_graph` for auto-generated plans: requires a
  linearity check at compile time and is a follow-on query planner decision.
- Multi-agg partial path (`reduce_multi_partial`): same structure as `reduce_partial`
  but uses `PARAM_AGG_COUNT` / `PARAM_AGG_SPEC_BASE`; straightforward extension.
