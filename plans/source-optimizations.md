# Source Operator Optimizations

Two related optimizations for the source path: a new `_consolidated` flag on
`ArenaZSetBatch` and a seal at the circuit boundary. Together they establish a
**consolidated invariant** — every batch entering `execute_epoch` is sorted, deduplicated,
and zero-weight-free — that eliminates wasted work throughout the operator pipeline.

The two optimizations are a single cohesive change. Implement them together.

---

## Background: The gap vs. Feldera

Feldera enforces that every batch entering a circuit operator is consolidated (sorted +
deduplicated + zero-weight-free) via `batcher.seal()` in `Input::eval`. gnitz has no
equivalent. The primary input delta (R0) is whatever the user pushed — possibly unsorted,
possibly with duplicate PKs.

Consequence: operators that benefit from sorted input (join cursor seeks, distinct
consolidation) pay the full unsorted cost on every tick.

gnitz already has `_sorted` on `ArenaZSetBatch`. The gap is:
1. `_sorted` is never set by `op_scan_trace`, even though cursor iteration is always sorted.
2. Nothing enforces `_sorted = True` at the circuit boundary.
3. There is no stronger "consolidated" guarantee (sorted + no duplicate PKs + no zeros).

---

## Optimization 1: `_consolidated` flag on `ArenaZSetBatch`

### What it means

`_consolidated = True` asserts: the batch is sorted by PK, every PK appears at most once,
and all weights are non-zero. This is strictly stronger than `_sorted = True`.

`_consolidated` implies `_sorted`. Enforcing this in `mark_consolidated` means any code
path that checks `_sorted` automatically benefits when the batch is consolidated.

### Changes required

#### `gnitz/core/batch.py` — `ArenaZSetBatch`

Add `_consolidated = False` alongside `_sorted = False`:

```python
batch._sorted = is_sorted
batch._consolidated = False    # ← add
```

Add `mark_consolidated`:

```python
def mark_consolidated(self, value):
    self._consolidated = value
    if value:
        self._sorted = True    # consolidated ⇒ sorted, always
```

Update `clear()`:

```python
self._sorted = False
self._consolidated = False    # ← add
```

#### `gnitz/core/batch.py` — `to_consolidated()` short-circuit

`to_consolidated()` currently allocates a new batch unconditionally, even for already-clean
input. Add a fast exit:

```python
def to_consolidated(self):
    if self._count == 0 or self._consolidated:
        return self    # ← zero allocation, zero copy
    ...
    res._consolidated = True    # mark the result
    res._sorted = True          # implied
    return res
```

This makes the seal at `execute_epoch` (Optimization 2) free for already-clean batches,
which is the common case once production sites are wired up.

#### `gnitz/core/batch.py` — `BatchWriter`

Add `mark_consolidated` to `BatchWriter`:

```python
def mark_consolidated(self, value):
    self._batch._consolidated = value
    if value:
        self._batch._sorted = True
```

---

## Optimization 2: Seal at `execute_epoch`

### What it means

`execute_epoch` in `gnitz/vm/runtime.py` is the sole circuit-boundary crossing point for
every batch in gnitz — initial user pushes, view cascades, and multi-worker exchange
results all pass through it. Adding a single `to_consolidated()` call here enforces the
consolidated invariant universally without touching any call site.

This is the gnitz equivalent of Feldera's `batcher.seal()`.

### Changes required

#### `gnitz/vm/runtime.py` — `execute_epoch`

```python
def execute_epoch(self, input_delta):
    self.reg_file.prepare_for_tick()
    self.context.reset()

    # Seal: enforce consolidated invariant at circuit entry.
    # Returns self if already consolidated (no allocation); O(N log N) otherwise.
    sealed = input_delta.to_consolidated()

    in_reg = self.reg_file.get_register(self.in_reg_idx)
    assert in_reg.is_delta()
    in_reg.bind(sealed)

    run_vm(self.program, self.reg_file, self.context)

    result = None
    if self.context.status == STATUS_HALTED:
        out_reg = self.reg_file.get_register(self.out_reg_idx)
        assert out_reg.is_delta()
        if out_reg.batch.length() > 0:
            result = out_reg.batch.clone()

    in_reg.unbind()
    if sealed is not input_delta:
        sealed.free()

    return result
```

The only new lines are `sealed = input_delta.to_consolidated()`, `in_reg.bind(sealed)`
(was `in_reg.bind(input_delta)`), and `if sealed is not input_delta: sealed.free()`.

### Cost model

- If `input_delta._consolidated` is True: `to_consolidated()` returns `self` immediately.
  Cost: one field check. Zero allocation, zero copy.
- If `input_delta._sorted` is True but not consolidated: `to_consolidated()` does one
  O(N) merge pass (no sort needed). Rare in practice.
- If unsorted (raw user push): O(N log N) sort + O(N) merge pass. Paid once per push,
  amortized to zero for all cascades once production sites are wired.

---

## Production sites: where to set `_consolidated = True`

Setting `_consolidated` at every site that guarantees the invariant makes the seal
progressively cheaper as the flag propagates through the circuit.

### `op_scan_trace` — `gnitz/dbsp/ops/source.py`

`UnifiedCursor._find_next_non_ghost()` already sums weights across sub-cursors and skips
zero-weight groups. Cursor iteration is always in PK order. Scan output is consolidated
by construction.

```python
def op_scan_trace(cursor, out_writer, chunk_limit):
    ...
    while cursor.is_valid():
        ...
    out_writer.mark_consolidated(True)    # ← add; was missing entirely
    return scanned_count
```

This simultaneously fixes the "sorted flag missing" issue from the source review: since
`mark_consolidated(True)` also sets `_sorted = True`, downstream `SortedScope` and
`ConsolidatedScope` calls short-circuit correctly.

### `to_consolidated()` result

Already handled above — the result is marked `_consolidated = True` in the
`to_consolidated()` body.

### `op_reduce` output — `gnitz/dbsp/ops/reduce.py`

`op_reduce` emits at most one row per group per tick (a new value or a retraction, never
both). Output PKs are synthetic U128 hashes that are unique per group. The output is
consolidated.

```python
out_writer.mark_consolidated(group_by_pk)
# was: out_writer.mark_sorted(group_by_pk)
# group_by_pk is True when output is keyed by group — always True for op_reduce
```

Replace the existing `mark_sorted` call at `reduce.py:507` with `mark_consolidated`.

### `op_distinct` output — `gnitz/dbsp/ops/distinct.py`

`op_distinct` processes a `ConsolidatedScope` of its input, emitting at most ±1 per PK.
Output is consolidated.

```python
out_writer.mark_consolidated(True)
# was: out_writer.mark_sorted(True)
```

Replace the existing `mark_sorted(True)` at `distinct.py:75`.

### `op_join_delta_delta` output — `gnitz/dbsp/ops/join.py`

The sort-merge algorithm iterates both inputs in PK order and emits at most one output
row per matched key (since `ConsolidatedScope` eliminates duplicate PKs on both sides and
the trace has at most one entry per PK). Output is consolidated.

```python
out_writer.mark_consolidated(True)
# was: out_writer.mark_sorted(True)
```

Replace at `join.py:217`.

### Linear operators — `gnitz/dbsp/ops/linear.py`

`op_filter`, `op_negate`, and `op_delay` all produce a subset of their input (or a
weight-negated copy). A consolidated input remains consolidated after these operations:
the subset has no new duplicate PKs, no new zero weights.

```python
# op_filter, op_negate, op_delay — replace mark_sorted with mark_consolidated:
out_writer.mark_consolidated(in_batch._consolidated)
# was: out_writer.mark_sorted(in_batch._sorted)
```

`op_map` is excluded: it may remap PKs (the map function can alter the PK column), so
`_consolidated` cannot be safely propagated. Leave `mark_sorted(in_batch._sorted)`.

`op_union` is excluded: the union of two sorted batches with potentially overlapping PKs
is not sorted. Leave as-is (no `mark_sorted` call, `_sorted` stays False by default).

---

## Impact summary

| Before | After |
|---|---|
| R0 may be unsorted (raw user push) | R0 always consolidated (seal at `execute_epoch`) |
| Scan output `_sorted` never set | Scan output `_consolidated = True` |
| `op_distinct` re-consolidates on every call | No-op: input already consolidated |
| `op_join_delta_trace` uses N seeks | Unlocks merge-walk in `join-optimizations.md` Opt 2 |
| `to_consolidated()` always allocates | Short-circuits for clean batches |
| Cascade deltas pay consolidation cost | Free: already consolidated, seal is no-op |

---

## Dependencies and interactions

- **`join-optimizations.md` Optimization 2 (merge-walk)**: depends on `_consolidated`
  being set on R0. Without the seal, `delta_batch._consolidated` is always False and the
  merge-walk fast path is never taken.
- **`join-optimizations.md` Optimization 3 (ConsolidatedScope in delta-delta)**: with the
  seal in place, `ConsolidatedScope` on R0-derived inputs becomes a no-op. Opt 3 remains
  valuable as a defence-in-depth for delta-delta joins where both inputs come from
  intra-circuit operators that may not yet propagate `_consolidated`.
- **`join-optimizations.md` Optimization 4 (periodic consolidation)**: `mark_consolidated`
  on merge-walk output means `BatchWriter.consolidate()` short-circuits immediately —
  zero cost for the common path.
- **`reduce-optimizations.md`**: `mark_consolidated` on `op_reduce` output propagates
  through any downstream `op_distinct` (REDUCE → DISTINCT pattern from SQL GROUP BY),
  making the `distinct-optimizations.md` Opt 2 skip (already-distinct elimination) and
  the `ConsolidatedScope` in distinct both free.

---

## Implementation order

Steps 1–3 are a single cohesive change with no intermediate valid state — implement
together and test together.

```
1. Add _consolidated flag to ArenaZSetBatch
   — _consolidated field, mark_consolidated(), clear() update, to_consolidated() short-circuit,
     BatchWriter.mark_consolidated()

2. Set _consolidated at all production sites
   — op_scan_trace: mark_consolidated(True) after loop
   — to_consolidated() result: mark in body (part of step 1)
   — op_reduce: mark_consolidated(True) replacing mark_sorted
   — op_distinct: mark_consolidated(True) replacing mark_sorted
   — op_join_delta_delta: mark_consolidated(True) replacing mark_sorted
   — linear operators (filter, negate, delay): propagate _consolidated

3. Seal in execute_epoch
   — sealed = input_delta.to_consolidated()
   — bind sealed, free if different

4. join-optimizations.md Opt 2 (_join_dt_merge_walk)   ← now enabled
```

---

## Test coverage

No new test files required. The optimizations are transparent — they change performance
characteristics but not observable output.

- `dbsp_comprehensive_test.py` exercises distinct, reduce, and join operators end-to-end.
  Any regression in consolidation logic will manifest as wrong output counts or weights.
- `vm_comprehensive_test.py` covers `execute_epoch` directly. A test that pushes an
  unsorted batch (duplicate PKs with cancelling weights) and asserts correct output will
  verify the seal.
- `compile_graph_test.py` covers the full compile-to-execute path.
- All tests must run with `GNITZ_WORKERS=4` per project convention.

A targeted test to add to `dbsp_comprehensive_test.py`:

```
test_consolidated_invariant_through_circuit:
  Push a batch with duplicate PKs (PK=1 w=+2, PK=1 w=-1, PK=2 w=+1) to a source table
  with a downstream DISTINCT view. Assert: DISTINCT output contains exactly PK=1 w=+1
  and PK=2 w=+1 (the seal consolidates PK=1 to net w=+1 before DISTINCT sees it).
  Run with GNITZ_WORKERS=4.
```
