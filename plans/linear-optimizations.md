# Linear Operator Optimizations

Four optimizations across `gnitz/dbsp/ops/linear.py`, `gnitz/vm/interpreter.py`, and
`gnitz/vm/runtime.py`. Ordered from trivial to most invasive.

---

## Pre-write validation notes

Before writing this document, the following claims were verified against the code:

**Confirmed:**

- `prepare_for_tick()` (`runtime.py:82`) calls `reg.clear()` on every `DeltaRegister`.
  `DeltaRegister.clear()` (`runtime.py:39`) calls `self._internal_batch.clear()` — it clears
  only the **internal** batch, never `self.batch`. This means an alias set on `self.batch`
  survives `prepare_for_tick` intact (it is never dereferenced by clear).

- `ArenaZSetBatch.clear()` (`batch.py:438`) resets buffer `offset` fields to 0 but does NOT
  free the underlying `lltype.malloc`-ed memory. Capacity is fully retained between ticks.

- `DeltaRegister.batch` is NOT in `_immutable_fields_` of `BaseRegister` or `DeltaRegister`.
  The JIT treats it as a mutable field; assigning to it at interpreter dispatch time is legal.

- `Instruction._immutable_fields_` (`instructions.py:15`) includes `reg_in` and `reg_out`
  (constant pointers to register objects), not the registers' `.batch` field. The alias
  `reg_out.batch = reg_in.batch` mutates the register object, not the instruction. Legal.

- `DeltaRegister.unbind()` (`runtime.py:47`) unconditionally restores
  `self.batch = self._internal_batch`. Calling it on a non-aliased register is a no-op
  field assignment (`batch` is already `_internal_batch`). No cost.

- `execute_epoch` (`runtime.py:156`) always calls `out_reg.batch.clone()` regardless of
  operator content. This is one full memcpy + malloc per tick per circuit.

- `Buffer(initial_size=0)` calls `lltype.malloc(0, "raw")`. The first `ensure_capacity`
  check `if self.offset + needed_bytes > 0 * item_size` fires on any write, triggering
  doubling realloc. `if self.offset > 0: c_memmove(...)` is not taken (offset=0).
  `lltype.free(malloc(0))` is a C no-op if `malloc(0)` returns NULL. Safe.

- `from_buffers(initial_capacity=0)` already uses this pattern (allocates then immediately
  replaces buffers), confirming zero-capacity construction is supported.

**Invalidated:**

- The review's comparison of `op_delay` to Feldera's Z1 was wrong. Feldera's Z1 persists
  state across ticks. gnitz's `OPCODE_DELAY` does not: the delay output register is a plain
  `DeltaRegister` and is cleared by `prepare_for_tick` at the start of every epoch. The
  docstring ("forwards the current tick's batch to the **next** tick's input register") is
  incorrect. `op_delay` is a within-tick identity copy. There is no cross-tick state.

  Evidence: `DeltaRegister.clear()` is called for ALL delta registers unconditionally in
  `prepare_for_tick`. The delay output register is not special-cased. A call to
  `execute_epoch` in tick N+1 begins by clearing everything the tick N run wrote.

  Consequence: the comparison to Feldera's zero-copy `replace()` swap was comparing
  different things. Gnitz has no equivalent to Z1; the optimization discussed here is
  purely about reducing unnecessary copies within a single tick.

- `OPCODE_DELAY` does not appear in any SQL-compiled circuit. No path through the SQL
  compiler, `program_cache.py`, or `executor.py` emits it. It exists as a VM primitive
  but is unused in production circuits. All optimizations below still apply, but the
  impact is currently limited to circuits built directly via `CircuitBuilder.delay()`.

---

## Optimization 1: `op_union` — propagate `_sorted` for single-input case

### Problem

`append_batch` (`batch.py:742`) unconditionally sets `self._sorted = False` on the
destination batch. `op_union` calls no `mark_sorted` afterwards. When `batch_b is None`,
`op_union` is a pure identity copy of `batch_a`. If `batch_a._sorted` is True, the output
should be sorted — the ordering is the same. Currently it is always False.

```python
# linear.py:78 — current
def op_union(batch_a, batch_b, out_writer):
    out_writer.append_batch(batch_a)        # sets _sorted = False
    if batch_b is not None:
        out_writer.append_batch(batch_b)    # sets _sorted = False
    # no mark_sorted → output always unsorted
```

This causes unnecessary re-sort cost for any downstream operator that calls
`ConsolidatedScope`/`to_consolidated` on a UNION output whose only live input was sorted.

### Change

```python
def op_union(batch_a, batch_b, out_writer):
    out_writer.append_batch(batch_a)
    if batch_b is not None:
        out_writer.append_batch(batch_b)
    else:
        out_writer.mark_sorted(batch_a._sorted)    # ← add
```

`mark_sorted` is called after `append_batch`, so it correctly overrides the `False` set
inside `append_batch`. When both inputs are present the output is unsorted (potentially
overlapping PKs); `mark_sorted` is not called and `_sorted` remains False.

When combined with `source-optimizations.md` (the seal in `execute_epoch` ensures R0 is
consolidated at circuit entry), UNION nodes that receive exactly one live input — common
when one source table had no changes in this tick — propagate the consolidated flag
transitively instead of forcing a re-sort.

---

## Optimization 2: `op_delay` — register-level alias eliminates Copy 1

### Problem

For any circuit containing `OPCODE_DELAY`, the current execution pays two full batch
copies per tick:

```
prepare_for_tick()
    R1._internal_batch.clear()       # resets offsets; retains capacity

in_reg.bind(input_delta)             # R0.batch = input_delta (borrow, no copy)

DELAY handler:
    op_delay(R0.batch, BatchWriter(R1.batch))
        → R1._internal_batch.append_batch(input_delta)   # COPY 1: O(N) memcpy

HALT

result = out_reg.batch.clone()       # COPY 2: O(N) memcpy + malloc

in_reg.unbind()
```

Copy 1 is entirely wasteful: `op_delay` copies `input_delta` into R1's internal batch,
then `execute_epoch` clones R1's internal batch into `result`. Both copies carry identical
data. Copy 2 is addressed by Optimization 4.

### Alias approach

`op_delay` exists to make `out_reg.batch` readable as the same data that is in
`in_reg.batch`. This does not require copying: `out_reg.batch = in_reg.batch` is an alias
that costs a single pointer assignment. The alias is safe because:

1. No instruction in a valid DBSP circuit writes to `reg_out` after `DELAY` — the delay
   output register is only read by downstream consumers. An alias to `reg_in.batch` is
   read-only from the downstream perspective.
2. No operator modifies the source batch (all operators write to their designated output
   writer; `mark_sorted` is called only on the destination).
3. The dangling reference risk (alias pointing to a freed `input_delta` between ticks) is
   eliminated by adding `out_reg.unbind()` to `execute_epoch`'s cleanup, which restores
   `reg_out.batch = reg_out._internal_batch` before the caller frees `input_delta`.

### Changes required

#### `gnitz/vm/interpreter.py` — `OPCODE_DELAY` handler (~line 158)

```python
elif opcode == op.OPCODE_DELAY:
    reg_in = instr.reg_in
    reg_out = instr.reg_out
    assert reg_in is not None and reg_out is not None
    # Zero-copy alias: reg_out.batch borrows reg_in.batch for this tick.
    # Cleaned up by out_reg.unbind() in execute_epoch after result extraction.
    reg_out.batch = reg_in.batch
```

The `ops.op_delay(...)` call and `BatchWriter` construction are removed. `op_delay` in
`linear.py` is retained for direct use in unit tests; it is no longer called from here.

#### `gnitz/vm/runtime.py` — `execute_epoch` cleanup (~line 158)

```python
    # 5. Cleanup: Release borrowed references
    if out_reg.is_delta():
        out_reg.unbind()   # ← add: restores batch = _internal_batch
    in_reg.unbind()
```

`out_reg` is already retrieved at step 4 for result extraction. `unbind()` is
unconditionally safe on non-aliased registers: it assigns `batch = _internal_batch` which
is already the current value (no-op field write).

### Result

Tick execution for a DELAY circuit becomes:

```
prepare_for_tick()          # R1._internal_batch.clear() — fast offset resets

in_reg.bind(input_delta)    # borrow, no copy

DELAY handler:
    R1.batch = R0.batch     # alias, O(1)

HALT

result = R1.batch.clone()   # R1.batch IS input_delta → one copy instead of two
                             # (eliminated by Opt 4 for non-aliased cases)

R1.unbind()                 # R1.batch = R1._internal_batch
in_reg.unbind()
```

Copy 1 eliminated. One copy total (the `execute_epoch` clone, handled by Opt 4).

### `_sorted` propagation

With the alias, `out_reg.batch._sorted` IS `in_reg.batch._sorted` (same object).
`execute_epoch`'s `clone()` preserves `_sorted`. No explicit `mark_sorted` needed.

---

## Optimization 3: `op_union` — sorted merge for two sorted inputs

### Problem

When both `batch_a._sorted` and `batch_b._sorted` are True, `op_union`'s concatenation
produces an unsorted batch of `A+B` rows. Any downstream `ConsolidatedScope` call then
runs `to_consolidated()` which pays a full O((A+B) log(A+B)) sort before the O(A+B)
dedup pass.

A two-pointer merge of two sorted inputs is O(A+B) and produces a sorted (not
consolidated) output: `_sorted = True`, `_consolidated = False`. Downstream
`to_consolidated()` then only needs the O(A+B) dedup pass — the sort is skipped.

In DBSP circuits, UNION over two source table deltas is the common case for SQL UNION
queries. After `source-optimizations.md` is in place, both inputs to such a UNION are
consolidated at circuit entry. The merge path would be taken on every tick.

### Algorithm

```
i = 0, j = 0
while i < len(batch_a) and j < len(batch_b):
    pk_a = batch_a.get_pk(i)
    pk_b = batch_b.get_pk(j)
    if pk_a < pk_b:
        out_writer._direct_append_row(batch_a, i, batch_a.get_weight(i))
        i += 1
    elif pk_b < pk_a:
        out_writer._direct_append_row(batch_b, j, batch_b.get_weight(j))
        j += 1
    else:  # equal PKs: both rows appear (Z-set addition, not consolidation)
        out_writer._direct_append_row(batch_a, i, batch_a.get_weight(i))
        out_writer._direct_append_row(batch_b, j, batch_b.get_weight(j))
        i += 1
        j += 1
# flush remainder
while i < len(batch_a): ...
while j < len(batch_b): ...
out_writer.mark_sorted(True)
# _consolidated stays False: equal PKs were not summed
```

Equal-PK rows are both emitted because UNION is Z-set addition — the weights must both
appear for the downstream `to_consolidated` to sum them correctly. Emitting only one would
silently drop weight.

### Changes required

#### `gnitz/dbsp/ops/linear.py`

Add `_op_union_merge(batch_a, batch_b, out_writer)` using `_direct_append_row` for O(1)
single-row copies. Modify `op_union` to branch:

```python
def op_union(batch_a, batch_b, out_writer):
    if batch_b is None:
        out_writer.append_batch(batch_a)
        out_writer.mark_sorted(batch_a._sorted)    # from Opt 1
        return
    if batch_a._sorted and batch_b._sorted:
        _op_union_merge(batch_a, batch_b, out_writer)
        return
    out_writer.append_batch(batch_a)
    out_writer.append_batch(batch_b)
    # _sorted stays False (set by append_batch, no override needed)
```

The merge path calls `mark_sorted(True)` but not `mark_consolidated(True)` — the output
may have adjacent equal-PK rows.

### Dependency

`_direct_append_row` exists on `ArenaZSetBatch` (`batch.py:800`). It is used by
`repartition_batch` in exchange.py. No new infrastructure required.

### When this matters

- SQL `UNION ALL` over two source tables: both deltas are sealed consolidated at circuit
  entry (after `source-optimizations.md`). Merge is taken. Downstream DISTINCT's
  `ConsolidatedScope` skips sort.
- UNION where one source had no changes: the empty-batch `length() == 0` case falls into
  `append_batch` path (append of zero rows is a no-op). Both inputs can be empty and the
  merge still runs safely (both loops exit immediately, `mark_sorted(True)` is set on the
  empty output — correct).
- UNION inside a compound circuit (e.g., UNION feeding JOIN): the merge output's
  `_sorted = True` means JOIN's `ConsolidatedScope` over the UNION result skips its sort.

---

## Optimization 4 (last stage): `execute_epoch` — evict instead of clone

### Problem

`execute_epoch` always clones the output register's batch:

```python
# runtime.py:156
if out_reg.batch.length() > 0:
    result = out_reg.batch.clone()
```

`clone()` allocates a new `ArenaZSetBatch` at `initial_capacity = max(N, 8)`, which is
`(4 + num_payload_cols)` calls to `lltype.malloc` plus a full memcpy of every column
buffer. This cost is paid by **every circuit on every tick that produces output**, regardless
of operator content.

For a 4-column schema:
- 5 structural buffer mallocs (pk_lo, pk_hi, weight, null, blob)
- 3 payload column buffer mallocs
- 8 memcpys (one per column buffer)

This is the largest unavoidable allocation in the VM hot path.

### Root cause

`out_reg._internal_batch` is cleared by `prepare_for_tick` at the start of the next
epoch. The clone is the mechanism that allows the result to outlive that clearing. But
`clear()` only resets buffer offsets — it does **not** free the underlying C memory. The
`_internal_batch`'s buffers retain their allocated capacity across ticks.

The unnecessary work is: copying data out of a perfectly good batch that is about to be
cleared anyway, then clearing it.

### Evict approach

Instead of cloning the batch and then clearing it on the next tick, **evict it**: take the
populated `_internal_batch` out of the register, replace it with a fresh zero-capacity
batch, and return the evicted batch as the result. No memcpy. No realloc until the next
tick's first write.

```
Tick N:
    run_vm() fills out_reg._internal_batch with N rows.

    [Evict]:
        result = out_reg._internal_batch          ← take ownership
        fresh = ArenaZSetBatch(schema, initial_capacity=0)
        out_reg._internal_batch = fresh
        out_reg.batch = fresh

    return result                                 ← no copy

Tick N+1:
    prepare_for_tick():
        out_reg.clear() → fresh.clear()           ← O(1) no-op (offset already 0)

    run_vm() appends to fresh:
        fresh.ensure_capacity fires on first write → Buffer grows via doubling
        After first warm-up tick: fresh has full capacity, no more reallocs.
```

The fresh batch has zero capacity buffers (8 `lltype.malloc(0)` calls total). These are
immediately overwritten on the next append by `ensure_capacity`. After one tick of
warm-up, the batch reaches steady-state size and `ensure_capacity` no longer triggers.

### Interaction with the DELAY alias (Opt 2)

When DELAY aliases `reg_out.batch = reg_in.batch`, `reg_out.batch is not
reg_out._internal_batch`. The evict approach applies only to the non-aliased case. The
two cases are distinguished by the identity check:

```python
if out_reg.batch is out_reg._internal_batch:
    # Non-aliased (all non-DELAY circuits): evict
    if out_reg.batch.length() > 0:
        result = out_reg._internal_batch
        out_reg._internal_batch = ArenaZSetBatch(out_reg.table_schema,
                                                  initial_capacity=0)
        out_reg.batch = out_reg._internal_batch
else:
    # Aliased (DELAY): must clone, can't evict what we don't own
    if out_reg.batch.length() > 0:
        result = out_reg.batch.clone()
    out_reg.unbind()    # ← from Opt 2; placed here in the combined path
```

After this block, `in_reg.unbind()` follows as before.

### Changes required

#### `gnitz/vm/runtime.py` — `execute_epoch` (lines 150–161)

Full revised result-extraction block, replacing lines 151–159:

```python
    result = None
    if self.context.status == STATUS_HALTED:
        out_reg = self.reg_file.get_register(self.out_reg_idx)
        assert out_reg.is_delta()

        if out_reg.batch is out_reg._internal_batch:
            # Non-aliased path: evict to avoid clone.
            if out_reg.batch.length() > 0:
                result = out_reg._internal_batch
                out_reg._internal_batch = ArenaZSetBatch(
                    out_reg.table_schema, initial_capacity=0
                )
                out_reg.batch = out_reg._internal_batch
        else:
            # Aliased path (DELAY alias from Opt 2): clone the aliased source.
            if out_reg.batch.length() > 0:
                result = out_reg.batch.clone()
            out_reg.unbind()

    in_reg.unbind()
    return result
```

Note: when `out_reg.batch.length() == 0` in the non-aliased path, the evict is skipped
(`result` stays `None`) and `_internal_batch` is left in place. It will be cleared by
`prepare_for_tick` on the next tick as normal. The empty batch's allocated capacity is
reused — no allocation waste.

### RPython annotation concern

`out_reg._internal_batch` is not in `_immutable_fields_`. Mutating it at runtime is valid.
The type is `ArenaZSetBatch` in both cases (the replaced field and the fresh batch). Type
consistency is maintained. The RPython annotator sees a mutable `ArenaZSetBatch` field
assigned another `ArenaZSetBatch` — no union type introduced.

### Effect on caller

The caller (`executor.py`) currently does:

```python
out_delta = plan.execute_epoch(incoming_delta)
incoming_delta.free()
```

The returned `out_delta` is now the evicted `_internal_batch` (previously a clone). The
caller owns it and must call `out_delta.free()` when done — same contract as before. The
evicted batch carries the same data, same `_sorted`/`_consolidated` flags, same length.

The only visible difference: the returned batch's buffer capacity matches the previous
tick's steady-state size, not `max(N, 8)`. This is always `>= N` (it was just filled), so
no correctness issue.

### Cost model after all four optimizations

| Path | Before | After |
|---|---|---|
| DELAY circuit | 2× O(N) memcpy + 1 malloc(N) | 0× memcpy + `malloc(0)` per column |
| Non-DELAY circuit | 1× O(N) memcpy + 1 malloc(N) | 0× memcpy + `malloc(0)` per column |
| Empty output | 0 memcpy + 0 malloc | 0 memcpy + 0 malloc (no evict) |

---

## Correctness notes (no code changes)

### `func=None` in `op_filter` and `op_map`

`op_filter` with `func=None`: `func is not None` short-circuits to False for every row.
`range_start` never becomes non-negative. Output is always empty. A `None` predicate
intuitively means "no condition = pass all", but the current code treats it as "reject all".
`OPCODE_FILTER` is always compiled with a non-None predicate, so this is dead code in
production circuits. Document it and add an `assert func is not None` at entry to make the
contract explicit.

`op_map` with `func=None`: no `commit_row` is ever called; output is always empty. Same
dead path, same fix.

### `get_accessor` shared instance

`ArenaZSetBatch.get_accessor(i)` (`batch.py:541`) binds and returns `self._raw_accessor` —
a single pre-allocated `ColumnarBatchAccessor`. All operators call `get_accessor(i)` once
per row and use the accessor synchronously within the same iteration before the next call.
This invariant must be maintained: a caller must not hold onto an accessor reference across
two `get_accessor` calls on the same batch.

---

## Implementation order

```
1. op_union single-input _sorted propagation   (Opt 1 — 1 line, linear.py)
2. op_delay register-level alias               (Opt 2 — interpreter.py + runtime.py)
3. op_union sorted merge                       (Opt 3 — linear.py, new _op_union_merge)
4. execute_epoch evict                         (Opt 4 — runtime.py execute_epoch only)
```

Opts 1 and 2 are independent and can be done together. Opt 3 depends only on
`_direct_append_row` which already exists. Opt 4 must come last: it modifies the
result-extraction block of `execute_epoch` that Opt 2 also touches, and the combined
code in the plan above integrates both changes.

---

## Test coverage

All optimizations are transparent to observable behavior: same output, same weights, same
sorted/consolidated flags (or strictly better flags). Regressions surface as wrong output
counts or incorrect weights.

Existing tests that cover the affected paths:

- `dbsp_comprehensive_test.py` — exercises filter, negate, union, and delay directly
  via the operator functions. Catches regressions in `op_union` and any interaction with
  `mark_sorted`.
- `vm_comprehensive_test.py` `test_delay_op` — covers the `OPCODE_DELAY` interpreter
  path. After Opt 2, verify the result still equals the input (alias must produce the
  same output as the copy did).
- `compile_graph_test.py` `test_delay` — full compile+execute path for a DELAY circuit.
  After Opt 2 + Opt 4, verify multi-tick behavior: tick 1 output equals tick 1 input;
  tick 2 output equals tick 2 input (within-tick semantics are unchanged).
- `compile_graph_test.py` `test_union` — verify `_sorted` propagation after Opt 1 by
  pushing a sorted batch with one empty input and asserting the output's sorted flag.

No new test files required. For Opt 3, add a test to `dbsp_comprehensive_test.py`:

```
test_union_sorted_merge:
  batch_a: sorted (pk=1,2,3), batch_b: sorted (pk=2,4,5)
  op_union(batch_a, batch_b, out_writer)
  Assert: out._sorted == True
  Assert: out.length() == 7 (pk=1,2,2,3,4,5 — pk=2 appears twice)
  Assert: PKs in ascending order
  Run with GNITZ_WORKERS=4.
```

For Opt 4 (evict), add to `vm_comprehensive_test.py`:

```
test_execute_epoch_evict:
  Run the same plan for 3 consecutive ticks with different input batches.
  Assert output equals input on every tick (no cross-tick bleed from retained buffers).
  Assert result.free() does not crash (evicted batch is fully owned by caller).
  Run with GNITZ_WORKERS=4.
```
