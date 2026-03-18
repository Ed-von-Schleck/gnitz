# Distinct Operator Optimizations

Two targeted optimizations for `op_distinct`. They are independent and can be implemented
in either order.

---

## Optimization 1: Cursor-based history lookup

### Problem

`op_distinct` (`gnitz/dbsp/ops/distinct.py:51`) calls `history_table.get_weight(key, accessor)`
once per row in the consolidated delta batch. Each call does:

1. MemTable Bloom filter probe
2. `index.find_all_shards_and_indices(key)` — iterates **all K shard handles**, range-checks
   each, XOR8 filter probe, binary search into each candidate shard; **allocates a Python
   list** per call
3. For each shard hit: full `compare_rows(soa, accessor)` — O(num_columns) comparison to rule
   out PK hash collisions

With K shards and |Δ| delta keys this is O(|Δ| × K) shard-handle iterations with no
amortization across keys. The list allocation and `compare_rows` call add constant overhead
per key.

The fix is to use a cursor, exactly as `op_join_delta_trace` does (`join.py:124–162`): create
a `UnifiedCursor` over the history table once per tick, then seek it monotonically through the
sorted delta keys. The cursor gives the consolidated net weight directly — no shard iteration,
no list allocation, no row comparison.

### Why the cursor is already available

`program_cache.py:444` creates a `TraceRegister` for the history:

```python
hist_reg = runtime.TraceRegister(reg_id, hist_schema, history_table.create_cursor(), history_table)
```

`runtime.py:82–90` (`prepare_for_tick`) calls `reg.refresh()` on every `TraceRegister` before
each tick:

```python
def refresh(self):
    if self.cursor is not None:
        self.cursor.close()
    self.cursor = self.table.create_cursor()
```

So `reg_history.cursor` is a fresh `UnifiedCursor` snapping the current state of history at
the start of every tick. It is **not stale**. The sole problem is that `interpreter.py:128`
passes `reg_history.table` instead of `reg_history.cursor`:

```python
ops.op_distinct(
    reg_in.batch,
    reg_history.table,     # ← wrong: uses get_weight per key
    batch.BatchWriter(reg_out.batch)
)
```

### Changes required

#### `gnitz/dbsp/ops/distinct.py`

Change signature from `op_distinct(delta_batch, history_table, out_writer)` to
`op_distinct(delta_batch, hist_cursor, hist_table, out_writer)`.

Replace the per-key `get_weight` call with a cursor seek. The `accessor` variable is no
longer needed for the history lookup (the cursor gives consolidated weight directly). The
`hist_table` parameter is still needed for `ingest_batch` at the end.

```python
def op_distinct(delta_batch, hist_cursor, hist_table, out_writer):
    with ConsolidatedScope(delta_batch) as b:
        n = b.length()
        if n == 0:
            return

        for i in range(n):
            key = b.get_pk(i)
            w_delta = b.get_weight(i)
            accessor = b.get_accessor(i)

            hist_cursor.seek(key)
            w_old = r_int64(0)
            if hist_cursor.is_valid() and hist_cursor.key() == key:
                w_old = hist_cursor.weight()

            s_old = 0
            if w_old > r_int64(0):
                s_old = 1
            elif w_old < r_int64(0):
                s_old = -1

            w_new = r_int64(intmask(w_old + w_delta))

            s_new = 0
            if w_new > r_int64(0):
                s_new = 1
            elif w_new < r_int64(0):
                s_new = -1

            out_w = s_new - s_old
            if out_w != 0:
                out_writer.append_from_accessor(key, r_int64(out_w), accessor)

        out_writer.mark_sorted(True)
        hist_table.ingest_batch(b)
```

Note: `accessor` is still needed for `append_from_accessor` (writing the output row). It is
only removed from the history lookup path.

Note on hash collisions: `get_weight` called `compare_rows` to guard against 128-bit PK hash
collisions. The cursor-based approach returns the consolidated weight for the PK hash directly,
without a row-content check, consistent with how every other operator in gnitz (join,
anti-join, reduce) handles cursor lookups. The probability of a 128-bit collision is
negligible in practice.

#### `gnitz/vm/interpreter.py` — lines 126–130

```python
ops.op_distinct(
    reg_in.batch,
    reg_history.cursor,                      # was: reg_history.table
    reg_history.table,                       # new: still needed for ingest_batch
    batch.BatchWriter(reg_out.batch)
)
```

No changes to `instructions.py` or `program_cache.py` — `reg_history` is already the
`TraceRegister` that holds both `.cursor` and `.table`.

### Performance

The `UnifiedCursor` (tournament-tree merge of all K sub-cursors) seeks each sub-cursor once
in `seek(key)` and then advances monotonically. For |Δ| sorted delta keys over a history of
|H| rows, the total traversal is O((|Δ| + |H|) × log K) amortized, versus O(|Δ| × K) for
independent per-key probes. When the history table has accumulated many shards from many ticks
without compaction, this difference is significant.

---

## Optimization 2: Skip DISTINCT for already-distinct inputs

### Problem

`op_distinct` is a no-op when the input is provably already at set semantics (every row's
weight is +1 or -1, no two rows share the same PK in the same delta). In that case:

- `w_old ∈ {0, 1}` for every key (history has only positive-weight entries, at most one per
  key)
- `w_delta ∈ {-1, +1}` for every row in the delta
- `s_new - s_old = w_delta` always → `out_w = w_delta` → the operator is a pure passthrough
- The history table accumulates entries, grows with shards, and participates in cursor
  refreshes every tick — all for zero effect

The key case is **REDUCE output**: `op_reduce` emits at most one delta row per group per tick
(either +1 for a new/changed group or -1 for a deleted/changed group), with a synthetic U128
PK that is unique per group. This output is already at set semantics. A DISTINCT node wired
after REDUCE is a no-op.

Other operators whose output is provably already distinct:
- Another DISTINCT node (by definition)

### Approach

Add an `is_distinct` flag to `DeltaRegister` (compile-time only; not needed in RPython
interpreter). During circuit compilation in `program_cache.py`, set the flag on REDUCE and
DISTINCT outputs. When compiling an `OPCODE_DISTINCT` node whose input register has
`is_distinct = True`, skip the node entirely: no history table is created, no instruction is
emitted, and the output register ID is aliased to the input register ID.

This is a compile-time elimination. There is no runtime cost at all — the instruction simply
does not appear in the program.

### Changes required

#### `gnitz/vm/runtime.py` — `DeltaRegister.__init__`

Add `is_distinct = False`:

```python
class DeltaRegister(BaseRegister):
    def __init__(self, reg_id, table_schema):
        BaseRegister.__init__(self, reg_id, table_schema)
        self._internal_batch = ArenaZSetBatch(table_schema)
        self.batch = self._internal_batch
        self.is_distinct = False          # ← add this line
```

This is a compile-time property used only in `program_cache.py` (CPython). The RPython
annotator does not see this attribute in any compiled code path.

#### `gnitz/catalog/program_cache.py` — `OPCODE_REDUCE` block (~line 469)

After creating `out_delta_reg`, mark it as distinct:

```python
out_delta_reg = runtime.DeltaRegister(out_delta_id, reduce_out_schema)
out_delta_reg.is_distinct = True          # ← add this line
cur_reg_file.registers[out_delta_id] = out_delta_reg
```

#### `gnitz/catalog/program_cache.py` — `OPCODE_DISTINCT` block (~line 440)

Add the skip check before allocating the history table. When skipping:
- Do **not** create a history table or `TraceRegister`
- Do **not** allocate an `out_delta_reg` (do **not** increment `_ST_NEXT_EXTRA_REG`)
- Override `out_reg_of[nid]` to alias the input register — downstream nodes will read from the
  input register directly
- Return `None` so no instruction is appended (line 760: `if instr is not None:
  program.append(instr)` already handles this)

```python
elif op == opcodes.OPCODE_DISTINCT:
    in_reg = cur_reg_file.registers[in_regs[opcodes.PORT_IN_DISTINCT]]

    # ── Compile-time elimination for already-distinct inputs ──────────
    if in_reg.is_distinct:
        # No-op: alias output to input, emit nothing.
        out_reg_of[nid] = in_regs[opcodes.PORT_IN_DISTINCT]
        return None
    # ─────────────────────────────────────────────────────────────────

    hist_schema = in_reg.table_schema
    history_table = view_family.store.create_child("_hist_%d_%d" % (view_id, nid), hist_schema)
    hist_reg = runtime.TraceRegister(reg_id, hist_schema, history_table.create_cursor(), history_table)
    cur_reg_file.registers[reg_id] = hist_reg
    out_delta_id = state[_ST_NEXT_EXTRA_REG]
    state[_ST_NEXT_EXTRA_REG] += 1
    out_delta_reg = runtime.DeltaRegister(out_delta_id, in_reg.table_schema)
    out_delta_reg.is_distinct = True      # ← mark DISTINCT output as distinct
    cur_reg_file.registers[out_delta_id] = out_delta_reg
    out_reg_of[nid] = out_delta_id
    return instructions.distinct_op(in_reg, hist_reg, out_delta_reg)
```

### Register file sizing after skipping

`program_cache.py:622` pre-counts `extra_regs += 1` for every `OPCODE_DISTINCT` node before
allocating the `RegisterFile`. If a DISTINCT is skipped, that pre-allocated slot goes unused
(stays `None`). This is safe: the slot is never referenced since `out_reg_of[nid]` points to
the input register and `_ST_NEXT_EXTRA_REG` is not incremented for the skipped node. The same
applies to the post-plan counting at line 685 for exchange circuits.

There is no need to pre-scan for skippable DISTINCTs before sizing the register file; one or
two unused `None` slots are harmless.

### Correctness argument for REDUCE → DISTINCT skip

`op_reduce` produces exactly one output row per group per tick. The row's weight is +1
(new/changed group value) or -1 (retraction of old value). Two rows with the same group
cannot appear in the same tick's delta: the reduce output is already consolidated. Therefore:

- `H_old ∈ {0, 1}` for every group key (set semantics maintained across ticks)
- `w_delta ∈ {-1, +1}` always
- `sign(H + Δ) - sign(H) = Δ` for all combinations: `(0,+1)→+1`, `(1,-1)→-1`, `(0,-1)→0`
  (never occurs; group can't disappear before it existed), `(1,+1)→0` (never occurs; same
  group can't be emitted twice)
- DISTINCT is a passthrough and the history table accumulates changes it never needs to filter

The alias `out_reg_of[nid] = input_reg_id` correctly routes the reduce delta directly to
whatever node consumes the DISTINCT's output.

### What this does NOT cover

Operators where `is_distinct` propagation would also apply but is not implemented here for
simplicity:
- FILTER of a distinct input: output weights are inherited unchanged, so `is_distinct` could
  be propagated. Deferred.
- MAP of a distinct input: depends on whether the map is injective (PK-preserving). Not safe
  to assume in general.
- SEMI/ANTI-JOIN output: depends on the left input's distinctness. Deferred.

The REDUCE case is the primary one that arises in practice (GROUP BY → DISTINCT in SQL).

---

## Test coverage

Both optimizations should be covered by the existing test suite after implementation:

- `test_distinct_weight_accumulation` and `test_filter_then_distinct` in
  `py_client/tests/test_dbsp_ops.py` exercise the full distinct logic. These will catch any
  regression in the cursor-based path.
- A new test should be added to `py_client/tests/test_dbsp_ops.py` verifying that a
  REDUCE → DISTINCT circuit produces correct output (the skip path). This can be a simple
  GROUP BY with COUNT followed by an explicit `cb.distinct()` call in `build`.
- Run with `GNITZ_WORKERS=4` per project convention.
