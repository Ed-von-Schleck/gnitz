# Plan: Wire `combine` into the Partial-Agg + Gather Path

## Goal

`UniversalAccumulator.combine` exists in `functions.py` but has no caller.
This plan adds the partial-agg + gather circuit topology so linear aggregates
(COUNT, SUM, COUNT_NON_NULL) can be computed with a local pre-aggregate on
each worker before exchange, instead of sending raw input rows through the
exchange boundary.

The existing exchange-before-aggregate path (`CircuitBuilder.reduce`) is
unchanged and remains the default. This new path is opt-in.

---

## Background

Current topology (exchange-before-aggregate):
```
Scan → SHARD(group cols) → REDUCE → Integrate
```
Raw rows are repartitioned by group key. Each worker owns a disjoint set of
groups and runs `op_reduce` against complete local history. Correct for all
aggregate types including MIN/MAX.

New topology (partial-agg + gather):
```
Scan → REDUCE → SHARD(pk col 0) → GATHER_REDUCE → Integrate
```
Each worker runs `op_reduce` on its local input rows first, producing partial
aggregate rows. These partial rows are repartitioned by group key (their PK is
the group hash, col 0). Each worker then runs `op_gather_reduce`, which calls
`combine` to merge all partial values for each group it owns and emits the
correct global delta.

**Why this is correct only for linear aggregates:**
`op_gather_reduce` receives only partial deltas from workers that had local
changes that tick. Workers with no changes send nothing. For COUNT/SUM, the
global delta is the sum of partial deltas — missing workers contribute zero,
which is correct. For MIN/MAX this fails: the correct global minimum requires
knowing all workers' current partial values, not just the ones that changed.
MIN/MAX must use the exchange-before-aggregate path.

The master (`_relay_exchange`) already calls `relay_scatter` unconditionally,
which repartitions exchange payloads by shard columns regardless of whether
those payloads contain raw input rows or partial aggregate rows. No master
changes are needed.

---

## What to build

### 1. `OPCODE_GATHER_REDUCE` in `gnitz/core/opcodes.py`

Add the constant after the existing opcodes (21 is free):
```python
OPCODE_GATHER_REDUCE = 21
```

Also add the port constant (reuses the same port layout as REDUCE):
```python
# PORT_IN_REDUCE, PORT_TRACE_IN, PORT_TRACE_OUT are already defined and shared
```
No new port constants are needed — `op_gather_reduce` uses the same port
indices as `op_reduce`.

---

### 2. `op_gather_reduce` in `gnitz/dbsp/ops/reduce.py`

The function receives partial aggregate rows (in the reduce output schema)
and a `trace_out` (also in reduce output schema) tracking the current global
aggregate per group. It calls `combine` to merge partial values and emits
retraction/insertion pairs exactly as `op_reduce` does.

Signature:
```python
def op_gather_reduce(
    partial_batch,       # ArenaZSetBatch in reduce output schema
    partial_schema,      # Schema of partial_batch
    trace_out_cursor,    # cursor into trace_out (global agg state)
    trace_out_table,     # table backing trace_out (for writes)
    out_writer,          # output BatchWriter
    group_col_indices,   # column indices of the group key in partial_schema
    agg_funcs,           # list[AggregateFunction], one per agg column
    output_schema,       # = partial_schema (same schema for output)
):
```

Algorithm:
1. Sort `partial_batch` by group columns using `_argsort_delta` (reuse
   existing helper — it accepts any schema and column list).
2. For each group G:
   a. Look up `old_global` from `trace_out_cursor` (seek by group PK).
   b. Reset each `agg_func`.
   c. For each +1 partial row in `partial_batch` for G:
      - Call `agg_func.combine(row.agg_bits)` for each agg column.
   d. For each −1 partial row in `partial_batch` for G:
      - Subtract: `agg_func.merge_accumulated(old_partial_bits, weight=-1)`.
      - `old_partial_bits` is the agg column value read from that −1 row.
   e. If `old_global` exists: `agg_func.merge_accumulated(old_global_bits, 1)`.
      This adds the previous global value so that `new_global = old_global + delta`.
   f. Emit `(−1, old_global)` if `old_global` existed.
   g. Emit `(+1, new_global)` if `new_global` accumulator is non-zero.
   h. Update `trace_out` with `new_global`.

For output accessors, reuse `ReduceAccessor`. The group PK in the partial
schema is already the group hash (col 0), so the accessor can emit it directly.

---

### 3. `gather_reduce_op` builder in `gnitz/vm/instructions.py`

```python
def gather_reduce_op(reg_in, reg_trace_out, reg_out,
                     group_by_cols, agg_funcs, output_schema):
    i = Instruction(op.OPCODE_GATHER_REDUCE)
    i.reg_in = reg_in
    i.reg_trace_out = reg_trace_out
    i.reg_out = reg_out
    i.group_by_cols = group_by_cols
    i.agg_funcs = agg_funcs
    i.output_schema = output_schema
    return i
```

No new fields on `Instruction` are needed — `reg_trace_out`, `reg_out`,
`group_by_cols`, `agg_funcs`, and `output_schema` are all already declared in
`_immutable_fields_`.

---

### 4. Dispatch in `gnitz/vm/interpreter.py`

Add an `elif` branch for `OPCODE_GATHER_REDUCE` in the main dispatch loop,
alongside the existing `OPCODE_REDUCE` case. Call:
```python
op_gather_reduce(
    instr.reg_in.batch,
    instr.reg_in.table_schema,
    instr.reg_trace_out.cursor,
    instr.reg_trace_out.table,
    instr.reg_out.writer,
    instr.group_by_cols,
    instr.agg_funcs,
    instr.output_schema,
)
```

---

### 5. `_emit_node` + extra_regs in `gnitz/catalog/program_cache.py`

In `_emit_node`, add an `elif op == opcodes.OPCODE_GATHER_REDUCE:` branch.
It is simpler than OPCODE_REDUCE:
- No `trace_in` (input rows are already aggregated; full history replay is
  not needed).
- No AVI or group index (those are only for MIN/MAX, which never uses
  this path).
- Allocates one `TraceRegister` for `trace_out` (global agg state) and one
  `DeltaRegister` for the output delta.
- The `in_reg`'s schema is the reduce output schema (partial aggregates).
  Reconstruct `agg_funcs` from the same PARAM_AGG_FUNC_ID / PARAM_AGG_COL_IDX
  / PARAM_AGG_COUNT / PARAM_AGG_SPEC_BASE params as OPCODE_REDUCE — the
  parameters are identical. The `col_type` lookup uses `in_reg.table_schema`
  (the partial schema), targeting the agg column index within that schema.

In both extra_regs counting loops (pre-plan and post-plan, lines ~804-875),
add:
```python
elif pop == opcodes.OPCODE_GATHER_REDUCE:
    extra_regs += 1  # trace_out only (no trace_in, no separate out_delta needed)
```
Wait — need to verify. `op_reduce` allocates: 1 `TraceRegister` for `tr_out`
(at `reg_id`) and 1 extra `DeltaRegister` for `out_delta` (from
`state[_ST_NEXT_EXTRA_REG]`), plus optionally 1 more for `tr_in`. So the
"2" in `extra_regs += 2` covers `out_delta + auto trace_in` (the comment
says so). For `OPCODE_GATHER_REDUCE`, there is no `tr_in`, only `out_delta`:
```python
elif pop == opcodes.OPCODE_GATHER_REDUCE:
    extra_regs += 1  # out_delta register
```

---

### 6. `reduce_partial` / `reduce_multi_partial` in `rpython_tests/helpers/circuit_builder.py`

```python
def reduce_partial(self, input_handle, agg_func_id, group_by_cols, agg_col_idx=0):
    """Partial-agg + gather. Only for linear aggregates (COUNT, SUM,
    COUNT_NON_NULL). Must NOT be used with AGG_MIN or AGG_MAX."""
    reduce_handle = self._alloc_node(OPCODE_REDUCE)
    self._connect(input_handle, reduce_handle, PORT_IN_REDUCE)
    self._params.append((reduce_handle.node_id, PARAM_AGG_FUNC_ID, agg_func_id))
    self._params.append((reduce_handle.node_id, PARAM_AGG_COL_IDX, agg_col_idx))
    for col_idx in group_by_cols:
        self._group_cols.append((reduce_handle.node_id, col_idx))
    # Exchange partial aggregate rows by group PK (col 0 = group hash)
    sharded = self.shard(reduce_handle, [0])
    gather = self._alloc_node(OPCODE_GATHER_REDUCE)
    self._connect(sharded, gather, PORT_IN_REDUCE)
    self._params.append((gather.node_id, PARAM_AGG_FUNC_ID, agg_func_id))
    self._params.append((gather.node_id, PARAM_AGG_COL_IDX, agg_col_idx))
    for col_idx in group_by_cols:
        self._group_cols.append((gather.node_id, col_idx))
    return gather

def reduce_multi_partial(self, input_handle, agg_specs, group_by_cols):
    """Multi-agg variant. agg_specs: list of (agg_func_id, agg_col_idx).
    All agg functions must be linear."""
    reduce_handle = self._alloc_node(OPCODE_REDUCE)
    self._connect(input_handle, reduce_handle, PORT_IN_REDUCE)
    self._params.append((reduce_handle.node_id, PARAM_AGG_COUNT, len(agg_specs)))
    for i in range(len(agg_specs)):
        func_id, col_idx = agg_specs[i]
        self._params.append((reduce_handle.node_id, PARAM_AGG_SPEC_BASE + i,
                              (func_id << 32) | col_idx))
    for col_idx in group_by_cols:
        self._group_cols.append((reduce_handle.node_id, col_idx))
    sharded = self.shard(reduce_handle, [0])
    gather = self._alloc_node(OPCODE_GATHER_REDUCE)
    self._connect(sharded, gather, PORT_IN_REDUCE)
    self._params.append((gather.node_id, PARAM_AGG_COUNT, len(agg_specs)))
    for i in range(len(agg_specs)):
        func_id, col_idx = agg_specs[i]
        self._params.append((gather.node_id, PARAM_AGG_SPEC_BASE + i,
                              (func_id << 32) | col_idx))
    for col_idx in group_by_cols:
        self._group_cols.append((gather.node_id, col_idx))
    return gather
```

The group_by_cols recorded against OPCODE_GATHER_REDUCE are the original
input group columns. `_emit_node` for OPCODE_GATHER_REDUCE uses them only to
recover the agg schema; the actual sort key inside `op_gather_reduce` is
col 0 (the group PK hash), which is always the PK of the partial schema.

---

## Schema note

The partial schema (output of OPCODE_REDUCE feeding into OPCODE_GATHER_REDUCE)
is built by `_build_reduce_output_schema`. The global aggregate trace (`tr_out`
of OPCODE_GATHER_REDUCE) uses the same schema. `op_gather_reduce` reads and
writes group PK + agg columns from this schema, so no new schema builder is
needed.

---

## Files to touch

| File | Change |
|---|---|
| `gnitz/core/opcodes.py` | Add `OPCODE_GATHER_REDUCE = 21` |
| `gnitz/dbsp/ops/reduce.py` | Add `op_gather_reduce` |
| `gnitz/vm/instructions.py` | Add `gather_reduce_op` builder |
| `gnitz/vm/interpreter.py` | Dispatch `OPCODE_GATHER_REDUCE` |
| `gnitz/catalog/program_cache.py` | `_emit_node` branch + extra_regs counts (×2) |
| `rpython_tests/helpers/circuit_builder.py` | `reduce_partial`, `reduce_multi_partial` |
| `rpython_tests/dbsp_comprehensive_test.py` | Unit tests for `op_gather_reduce` |

---

## Testing

Unit tests (`make run-dbsp_comprehensive_test-c`):
- Single COUNT via `reduce_partial`: verify correct output on a 4-worker
  simulation with 2 groups, multiple ticks.
- Single SUM: same structure.
- Multi-agg (COUNT + SUM) via `reduce_multi_partial`.
- Retraction tick: insert rows, then delete some; verify output delta and trace.

E2E (`GNITZ_WORKERS=4 make run-server_test-c`):
- Add one test view using `reduce_partial` with COUNT or SUM.
- Verify results match a reference single-worker execution.

---

## Out of scope

- Auto-selecting `reduce_partial` in `compile_from_graph` for query-planner-
  generated circuits. This requires a linearity check at compile time and is a
  separate planner decision.
- Using `reduce_partial` for MIN/MAX. This is incorrect and must never be done.
