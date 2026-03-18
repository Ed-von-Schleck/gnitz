# Plan: Join Operator Optimizations (Feldera-Inspired)

This plan documents every performance optimization Feldera employs in its join operator
that gnitz does not yet implement, ordered from highest to lowest expected impact.
Each section maps the optimization precisely to gnitz's file/function topology.

---

## Background: What the comparison revealed

Feldera's `JoinTrace` operator (`crates/dbsp/src/operator/dynamic/join.rs`) applies five
optimizations on top of the same bilinear DBSP decomposition gnitz uses:

1. **Adaptive cursor swapping** — iterate the smaller side, seek in the larger
2. **ConsolidatedScope on delta-delta inputs** — eliminate cancelled rows before cartesian product
3. **Intermediate output consolidation** — collapse output duplicates before they accumulate
4. **Key-gather prefetch** — pull all matching trace entries once before the main loop
5. **Left outer join (SaturatingCursor)** — synthesize NULL rows for unmatched left keys

Items 1–4 are pure performance. Item 5 is a new feature (LEFT JOIN) that Feldera implements
as an extension to the same join infrastructure.

This plan adds a sixth optimization not present in Feldera:

6. **Merge-walk for consolidated delta** — replaces N `trace_cursor.seek()` calls with a
   single seek + O(N + M) monotone `advance()` calls, exploiting the `_consolidated`
   invariant introduced in `source-optimizations.md`. Complements Opt 1: the swap handles
   |Δ| ≫ |I|; the merge-walk handles |I| ≥ |Δ| with sorted/consolidated input.

---

## Optimization 1: Adaptive Cursor Swapping in `op_join_delta_trace`

### Motivation

`op_join_delta_trace` (`gnitz/dbsp/ops/join.py:124`) always iterates the delta batch and
calls `trace_cursor.seek()` for each row. Cost: `O(|Δ| · log |I|)`.

When a large delta arrives against a small trace — e.g., bulk-loading a table for the
first time, or a large batch update against a lightly-populated join partner — this is
backwards. It is cheaper to iterate the trace (smaller) and seek in the sorted delta
(larger). Feldera makes this swap at `join.rs:1596`:

```rust
JointKeyCursor::new(delta_cursor, trace_cursor,
    fetched.is_none() && (delta_len > trace_len));
```

When `swap=true`, Feldera iterates trace keys and calls `seek_key_exact()` into the delta,
reducing cost to `O(|I| · log |Δ|)`. For `|Δ| ≫ |I|` this is a large win.

### What needs to change

#### Step 1 — Add `estimated_length()` to `AbstractCursor`

File: `gnitz/core/store.py`

Add a method to `AbstractCursor`:

```python
def estimated_length(self):
    """Upper bound on the number of live (non-ghost) records. Used for join cardinality
    heuristics. Permitted to over-count; must not under-count."""
    return 0
```

Implement in each concrete cursor in `gnitz/storage/cursor.py`:

| Cursor class | Implementation |
|---|---|
| `SortedBatchCursor` | `return self._batch.length()` |
| `MemTableCursor` | `return self._snapshot.length()` |
| `ShardCursor` | `return self.view.count` (includes ghosts, slight over-count — acceptable) |
| `UnifiedCursor` | `sum(c.estimated_length() for c in self.cursors)` |

The `UnifiedCursor` sum double-counts cancelled pairs. That is fine — it is an upper bound
and the comparison only needs to be directionally correct.

#### Step 2 — Add `seek_key_exact()` to `AbstractCursor`

The swap path needs to know whether a seek landed on an exact match (not just `>= key`).
Add a default implementation to `AbstractCursor`:

```python
def seek_key_exact(self, key):
    """Seek to key. Returns True if the cursor is now positioned exactly on key."""
    self.seek(key)
    return self.is_valid() and self.key() == key
```

All cursor subclasses inherit this automatically. No per-class overrides needed because
every concrete `seek()` is already a lower-bound binary search.

#### Step 3 — Restructure `op_join_delta_trace` into two code paths

File: `gnitz/dbsp/ops/join.py`

Add import at top:
```python
from gnitz.storage.cursor import SortedBatchCursor
```

Replace `op_join_delta_trace` with a dispatch function and two helpers:

```python
ADAPTIVE_SWAP_THRESHOLD = 1   # swap whenever delta_len > trace_len

def op_join_delta_trace(delta_batch, trace_cursor, out_writer, d_schema, t_schema):
    composite_acc = CompositeAccessor(d_schema, t_schema)
    delta_len = delta_batch.length()
    trace_len = trace_cursor.estimated_length()

    if delta_len > trace_len * ADAPTIVE_SWAP_THRESHOLD:
        _join_dt_swapped(delta_batch, trace_cursor, out_writer, composite_acc)
        out_writer.mark_sorted(delta_batch._sorted)
    elif delta_batch._consolidated:
        _join_dt_merge_walk(delta_batch, trace_cursor, out_writer, composite_acc)
        # _join_dt_merge_walk calls mark_consolidated(True), which sets _sorted=True
    else:
        _join_dt_normal(delta_batch, trace_cursor, out_writer, composite_acc)
        out_writer.mark_sorted(delta_batch._sorted)
```

**Normal path** (existing logic, extracted into helper):

```python
def _join_dt_normal(delta_batch, trace_cursor, out_writer, composite_acc):
    count = delta_batch.length()
    for i in range(count):
        w_delta = delta_batch.get_weight(i)
        if w_delta == r_int64(0):
            continue
        key = delta_batch.get_pk(i)
        trace_cursor.seek(key)
        while trace_cursor.is_valid():
            if trace_cursor.key() != key:
                break
            w_trace = trace_cursor.weight()
            w_out = r_int64(intmask(w_delta * w_trace))
            if w_out != r_int64(0):
                composite_acc.set_accessors(
                    delta_batch.get_accessor(i),
                    trace_cursor.get_accessor(),
                )
                out_writer.append_from_accessor(key, w_out, composite_acc)
            trace_cursor.advance()
```

**Swapped path** (new, iterate trace / seek delta):

```python
def _join_dt_swapped(delta_batch, trace_cursor, out_writer, composite_acc):
    with ConsolidatedScope(delta_batch) as sorted_delta:
        delta_cursor = SortedBatchCursor(sorted_delta)
        while trace_cursor.is_valid():
            trace_key = trace_cursor.key()
            if not delta_cursor.seek_key_exact(trace_key):
                trace_cursor.advance()
                continue
            w_trace = trace_cursor.weight()
            # Emit all delta rows at this key
            while delta_cursor.is_valid() and delta_cursor.key() == trace_key:
                w_delta = delta_cursor.weight()
                w_out = r_int64(intmask(w_delta * w_trace))
                if w_out != r_int64(0):
                    composite_acc.set_accessors(
                        delta_cursor.get_accessor(),
                        trace_cursor.get_accessor(),
                    )
                    out_writer.append_from_accessor(trace_key, w_out, composite_acc)
                delta_cursor.advance()
            trace_cursor.advance()
```

Note: `composite_acc.set_accessors(delta_acc, trace_acc)` — argument order is unchanged.
The `CompositeAccessor` always maps `left = delta schema, right = trace schema`. In the
swapped path we still pass delta accessor as `left` and trace accessor as `right`. The
swap is in the *iteration order*, not in the schema role.

Note: `SortedBatchCursor` is created inside `_join_dt_swapped`. It holds a reference to
`sorted_delta` which is valid for the lifetime of the `SortedScope` context manager.
RPython's GC will keep `sorted_delta` alive through the `SortedScope.__exit__` call, so
there is no lifetime issue.

#### Step 4 — RPython annotation considerations

`_join_dt_normal` and `_join_dt_swapped` take `delta_batch` (always `ArenaZSetBatch`) and
`trace_cursor` (always `AbstractCursor`, concretely `UnifiedCursor`). The RPython
annotator will see two calls to `CompositeAccessor.set_accessors`: one with a
`ColumnarBatchAccessor` for delta and a `SoAAccessor` for trace (normal), and one with a
`ColumnarBatchAccessor` for delta (from `SortedBatchCursor`) and a `SoAAccessor` for trace
(swapped). Both combinations are identical accessor types, so annotation is unambiguous.

The JIT will trace the two helpers as separate code paths (different call sites), which
is correct and enables independent JIT specialization.

#### Step 5 — Anti/semi-join adaptive swap

The same swap applies to `op_anti_join_delta_trace` and `op_semi_join_delta_trace`
(`gnitz/dbsp/ops/anti_join.py`).

For anti-join, the swap logic is simpler: in the swapped version, iterate trace keys and
check for exact match in delta. If the key exists in both, the row is excluded from anti-join
output. If a trace key has no matching delta key, it produces no output (anti-join output
schema is left-only). So the outer loop iteration order flip still gives the same
`O(min · log max)` cost. Implementation mirrors the join case above.

---

## Optimization 2: Merge-Walk for Consolidated Delta in `op_join_delta_trace`

### Motivation

`UnifiedCursor.seek()` (`gnitz/storage/cursor.py:321`) does two expensive things for every
call: it binary-searches all K sub-cursors from position 0 (O(K × log(N/K))), then calls
`self.tree.rebuild()` — a full tournament-tree reconstruction costing O(K log K). The
current `_join_dt_normal` calls `seek()` once per delta row, so the total cost is
O(N × (K × log(N/K) + K log K)).

`UnifiedCursor.advance()` (`cursor.py:328`) is fundamentally cheaper: it calls
`advance_cursor_by_index` for only the cursor(s) at the current minimum, then runs a
single heapify pass — O(log K). No sub-cursor binary searches, no full tree rebuild.

When the delta is **consolidated** (sorted, no duplicate PKs, no zero weights — see
`source-optimizations.md`), the delta keys are strictly increasing. The trace cursor
therefore only ever needs to move forward: a single `seek()` to anchor at the first delta
key, then pure `advance()` calls the rest of the way. This converts N seeks into
(N + M) advances, reducing the tournament-tree cost from O(N × K log K) to
O((N + M) × log K). The O(K log K) rebuild is paid exactly once.

This is the gnitz equivalent of a classical sort-merge join, made possible by the
consolidated invariant at the circuit boundary.

| Case | Cost | When |
|---|---|---|
| `_join_dt_normal` (current) | O(N × K × log(N/K) + N × K log K) | delta unsorted |
| `_join_dt_swapped` (Opt 1) | O(M × log N) | N ≫ M |
| `_join_dt_merge_walk` (this) | O(K log K + (N+M) × log K) | N ≤ M, delta consolidated |

### Dependency

Requires `_consolidated` flag on `ArenaZSetBatch` and the seal at `execute_epoch` —
both introduced in `source-optimizations.md`. Without `_consolidated`, the merge-walk
cannot be triggered safely (it would produce incorrect output if delta contains duplicate
PKs or is unsorted).

### What needs to change

#### Step 1 — Add `_join_dt_merge_walk` helper

File: `gnitz/dbsp/ops/join.py`

```python
def _join_dt_merge_walk(delta_batch, trace_cursor, out_writer, composite_acc):
    """
    Merge-walk join for consolidated, sorted delta.

    Pre-condition: delta_batch._consolidated is True (sorted, no duplicate PKs,
    no zero weights). The caller (op_join_delta_trace) checks this before dispatch.

    Replaces N trace seeks with 1 seek + O(N+M) monotone advances.
    Output is consolidated: at most one row per key (trace has at most one entry per PK),
    all non-zero weights.
    """
    count = delta_batch.length()
    # One seek to anchor the trace cursor at the first delta key.
    trace_cursor.seek(delta_batch.get_pk(0))

    for i in range(count):
        d_key = delta_batch.get_pk(i)
        w_delta = delta_batch.get_weight(i)
        # w_delta is non-zero by _consolidated invariant — no check needed.

        # Advance trace forward to d_key. Since delta is sorted and trace is
        # already past previous delta keys, this is always a forward-only move.
        while trace_cursor.is_valid() and trace_cursor.key() < d_key:
            trace_cursor.advance()

        if trace_cursor.is_valid() and trace_cursor.key() == d_key:
            w_trace = trace_cursor.weight()
            w_out = r_int64(intmask(w_delta * w_trace))
            if w_out != r_int64(0):
                composite_acc.set_accessors(
                    delta_batch.get_accessor(i),
                    trace_cursor.get_accessor(),
                )
                out_writer.append_from_accessor(d_key, w_out, composite_acc)
            # Advance past this key. Next delta key is strictly greater (consolidated),
            # so trace_cursor is already positioned correctly for the next iteration.
            trace_cursor.advance()

    out_writer.mark_consolidated(True)   # requires source-optimizations.md Step 1
```

The `mark_consolidated(True)` call requires `BatchWriter.mark_consolidated` added in
`source-optimizations.md`. Until that lands, use `out_writer.mark_sorted(True)` as a
placeholder — output is sorted even without the full `_consolidated` semantics.

#### Step 2 — Dispatch in `op_join_delta_trace`

Already shown in Optimization 1's updated dispatch code: the `elif delta_batch._consolidated`
branch routes to `_join_dt_merge_walk`.

#### Step 3 — Anti/semi-join merge-walk

File: `gnitz/dbsp/ops/anti_join.py`

The same merge-walk applies to `op_anti_join_delta_trace` and `op_semi_join_delta_trace`.
For anti-join, "no match in trace" = emit the delta row; for semi-join, "match in trace"
= emit the delta row. Both become:

```python
def _anti_join_dt_merge_walk(delta_batch, trace_cursor, out_writer):
    count = delta_batch.length()
    trace_cursor.seek(delta_batch.get_pk(0))
    for i in range(count):
        d_key = delta_batch.get_pk(i)
        while trace_cursor.is_valid() and trace_cursor.key() < d_key:
            trace_cursor.advance()
        in_trace = trace_cursor.is_valid() and trace_cursor.key() == d_key
        if not in_trace:   # anti-join: emit if NOT in trace
            out_writer.append_from_accessor(d_key, delta_batch.get_weight(i),
                                            delta_batch.get_accessor(i))
        if in_trace:
            trace_cursor.advance()
    out_writer.mark_consolidated(True)
```

`_semi_join_dt_merge_walk` is identical with the condition flipped (`if in_trace`).

### Interaction with Optimization 5 (key-gather prefetch)

After prefetch (`create_filtered_cursor`), the trace cursor only contains rows matching
delta keys. The merge-walk then processes those rows in O(N × log K_filtered) where
K_filtered ≈ 1 (mostly `SortedBatchCursor`s over gathered batches). The two optimizations
compose multiplicatively.

### Interaction with Optimization 4 (periodic consolidation)

`_join_dt_merge_walk` produces sorted output. The `to_consolidated()` inside
`BatchWriter.consolidate()` (Opt 4) short-circuits to an O(N) pass on sorted input
instead of O(N log N). Combined with the `_consolidated` short-circuit from
`source-optimizations.md`, `BatchWriter.consolidate()` becomes a no-op for merge-walk
output if the output batch already has `_consolidated = True`.

---

## Optimization 3: `ConsolidatedScope` in `op_join_delta_delta`

### Motivation

`op_join_delta_delta` (`gnitz/dbsp/ops/join.py:164`) currently uses `SortedScope` on both
inputs. `SortedScope` only sorts; it does not merge rows with the same (PK, payload).

If a delta batch contains multiple rows for the same PK — which happens whenever two
operations on the same key arrive in the same tick (e.g., a delete followed by a re-insert)
— `SortedScope` leaves both rows. The cartesian product then emits multiple output rows for
that key, all of which need to be consolidated later by the caller.

`to_consolidated()` is only marginally more expensive than `to_sorted()` (sort is shared;
consolidation adds one O(N) merge pass). The savings are in output size: cancelled pairs
(net weight = 0 after merging) are eliminated before the cartesian product, and the
remaining rows have their weights correctly summed so the product is exact.

Feldera's equivalent is that its trace is always consolidated (the spine background-merges
batches) and its delta batches are pushed through a `Batcher` before being passed to the
join, which consolidates them.

### What changes

File: `gnitz/dbsp/ops/join.py`

One-line change in `op_join_delta_delta`:

```python
# Before:
with SortedScope(batch_a) as b_a:
    with SortedScope(batch_b) as b_b:

# After:
with ConsolidatedScope(batch_a) as b_a:
    with ConsolidatedScope(batch_b) as b_b:
```

Add `ConsolidatedScope` to the import:
```python
from gnitz.core.batch import ConsolidatedScope, SortedScope, BatchWriter
```

`SortedScope` is no longer needed in `join.py` after this change (it is still used
elsewhere).

### Impact

For typical OLTP deltas where each PK appears at most once per tick, `to_consolidated()`
and `to_sorted()` produce identical results and have the same cost. For update-heavy
workloads (delete + re-insert on same PK), this eliminates the spurious cartesian product
expansion and reduces output size by up to 2×.

---

## Optimization 4: Intermediate Output Consolidation

### Motivation

Feldera consolidates output *during* the join loop using a `Batcher`:

```rust
if output_tuples.len() >= chunk_size / 3 {
    batcher.push_batch(&mut output_tuples);  // consolidates here
    if batcher.tuples() >= chunk_size {
        yield (batch, false, position);
        batcher = new Batcher();
    }
}
```

This keeps peak output batch memory bounded even for joins with high per-key fan-out, and
eliminates zero-weight pairs early (e.g., when the same output key is produced by multiple
input combinations and the weights cancel).

Gnitz accumulates all output rows in the output `ArenaZSetBatch` without any intermediate
consolidation. For joins that produce large intermediate outputs (e.g., a many-to-many join
between two large deltas), this inflates peak memory unnecessarily.

Gnitz's synchronous RPython model cannot `yield` mid-operator the way Feldera's async
stream can. However, the *consolidation* benefit is still fully achievable within the
synchronous model by periodically calling `to_consolidated()` on the output batch.

### What changes

#### Step 1 — Add `consolidate()` to `BatchWriter`

File: `gnitz/core/batch.py`

`BatchWriter` currently wraps `self._batch` (the output `ArenaZSetBatch`) as write-only.
Add a consolidation method:

```python
class BatchWriter(object):
    # ... existing methods ...

    def consolidate(self):
        """Consolidate the output batch in place, merging duplicate (PK, payload) pairs.
        Frees the pre-consolidation batch if a new one was allocated."""
        old = self._batch
        new = old.to_consolidated()
        if new is not old:
            # to_consolidated() returned a new batch; replace and free the old one.
            self._batch = new
            old.free()
```

This is safe because `BatchWriter` is the exclusive writer to its underlying batch, and
`to_consolidated()` is a functional operation that returns either `self` (if already
consolidated) or a fresh batch. The `old.free()` call is safe because nothing else holds
a reference to the pre-consolidation batch at the point `BatchWriter.consolidate()` is
called (the `out_writer` is the sole owner).

**RPython annotation**: `self._batch` is typed as `ArenaZSetBatch` throughout. The
reassignment `self._batch = new` preserves that type. RPython's annotator handles this
correctly because both `old` and `new` are `ArenaZSetBatch` instances.

#### Step 2 — Add periodic consolidation to `op_join_delta_trace`

File: `gnitz/dbsp/ops/join.py`

Add a consolidation trigger in `_join_dt_normal` (and `_join_dt_swapped`):

```python
CONSOLIDATE_INTERVAL = 8192   # rows written before consolidation

def _join_dt_normal(delta_batch, trace_cursor, out_writer, composite_acc):
    count = delta_batch.length()
    rows_since_consolidation = 0
    for i in range(count):
        # ... existing loop body ...
        if w_out != r_int64(0):
            out_writer.append_from_accessor(key, w_out, composite_acc)
            rows_since_consolidation += 1
            if rows_since_consolidation >= CONSOLIDATE_INTERVAL:
                out_writer.consolidate()
                rows_since_consolidation = 0
```

`CONSOLIDATE_INTERVAL = 8192` is a tunable constant. The cost is one `to_consolidated()`
call (O(N) if already sorted by PK, O(N log N) if not) per 8K output rows. For joins that
produce millions of output rows this is a net win; for joins that produce a few dozen rows
the interval is never reached and there is zero overhead.

#### Step 3 — Add consolidation in `op_join_delta_delta`

The same `consolidate()` call applies after the cartesian product inner loop. However,
because `op_join_delta_delta` with `ConsolidatedScope` already minimizes duplicates in the
inputs, the trigger should be less frequent:

```python
CONSOLIDATE_INTERVAL_DD = 16384
```

---

## Optimization 5: Key-Gather Prefetch

### Motivation

Feldera prefetches all trace entries whose keys exist in the delta before the main join
loop (`join.rs:1572`):

```rust
let fetched = if dev_tweaks.fetch_join {
    trace.fetch(&delta).await
} else { None };
```

`fetch()` iterates the delta's keys and builds a compact cursor that only contains matching
trace rows. For disk-backed storage this avoids repeated random seeks (each `seek()` on an
unmapped page faults in a 4 KB page even if only one row is needed).

Gnitz's shards are mmap'd (`gnitz/storage/shard_table.py`). A `seek()` into a shard that
is not in CPU TLB causes a page fault. With N delta rows each seeking into a large shard,
N random page faults occur. A prefetch pass converts these into a sequential scan of the
PK column (narrow, dense, cache-friendly), then caches the result in a compact batch.

This optimization matters most when:
- The trace (shard) is large (> a few MB, spills out of L3 cache)
- The delta has many distinct keys that hit different pages of the shard

It is a no-op for small in-memory workloads and adds one O(|Δ| · log |I|) pass before the
join, so it only pays off when the join itself is the bottleneck.

### Design

#### New method: `ZSetStore.create_filtered_cursor(keys_batch)`

File: `gnitz/core/store.py`

```python
def create_filtered_cursor(self, sorted_keys_batch):
    """
    Returns a cursor that contains only entries whose PKs appear in sorted_keys_batch.
    sorted_keys_batch must be sorted by PK.
    Default implementation falls back to create_cursor() (no gathering).
    """
    return self.create_cursor()
```

#### New method on `TableFamily` / `EphemeralTable` / `PersistentTable`

Each ZSetStore implementation overrides `create_filtered_cursor`:

**For `EphemeralTable` (in-memory only)**:
The default fallback is fine — the memtable is already in RAM and seeks are O(log N) with
no I/O, so gathering is not worth the overhead.

**For `PersistentTable` (shards + memtable)**:

```python
def create_filtered_cursor(self, sorted_keys_batch):
    # 1. Create a consolidated memtable cursor (already in RAM — no gather needed).
    mt_cursor = self._memtable.create_cursor()   # existing path

    # 2. For each shard, extract matching rows into a fresh ArenaZSetBatch.
    gathered_batches = []
    for shard_view in self._shard_views:
        gathered = _gather_shard_rows(shard_view, sorted_keys_batch, self.schema)
        if gathered.length() > 0:
            gathered_batches.append(gathered)

    # 3. Wrap in cursors and return a UnifiedCursor over all of them.
    cursors = [mt_cursor]
    for gb in gathered_batches:
        cursors.append(SortedBatchCursor(gb))  # gb is already sorted

    return UnifiedCursor(self.schema, cursors)
```

#### New helper: `_gather_shard_rows(shard_view, sorted_keys_batch, schema)`

File: `gnitz/storage/shard_table.py` (new helper function)

```python
def _gather_shard_rows(shard_view, sorted_keys_batch, schema):
    """
    Scan sorted_keys_batch and binary-search each key in shard_view.
    Copy matching rows (weight != 0) into a new ArenaZSetBatch.
    Returns the batch (may be empty).
    """
    count = sorted_keys_batch.length()
    out = ArenaZSetBatch(schema, initial_capacity=count)
    for i in range(count):
        key = sorted_keys_batch.get_pk(i)
        pos = shard_view.find_lower_bound(key)
        if pos >= shard_view.count:
            continue
        row_key = shard_view.get_pk_u128(pos) if is_u128 else r_uint128(shard_view.get_pk_u64(pos))
        if row_key != key:
            continue
        if shard_view.get_weight(pos) == 0:
            continue
        # Direct row copy from shard into batch
        accessor = SoAAccessor(schema)
        accessor.set_row(shard_view, pos)
        row_builder.begin(key, shard_view.get_weight(pos))
        # ... write all columns from accessor ...
        row_builder.commit(out)
    return out
```

The actual column-copy loop reuses the existing `append_from_accessor` infrastructure
that the join operator already calls. No new copy path is needed.

#### Integration into `op_join_delta_trace`

File: `gnitz/dbsp/ops/join.py`

The VM passes a `trace_cursor` to `op_join_delta_trace`. To use prefetch, the VM (or the
executor) would need to pass the `ZSetStore` instead, so the operator can call
`create_filtered_cursor()` itself:

```python
def op_join_delta_trace(delta_batch, trace_store, out_writer, d_schema, t_schema):
    with SortedScope(delta_batch) as sorted_delta:
        trace_cursor = trace_store.create_filtered_cursor(sorted_delta)
        try:
            # ... existing join body using trace_cursor ...
        finally:
            trace_cursor.close()
```

**VM change** (`gnitz/vm/interpreter.py`): the `OPCODE_JOIN_DELTA_TRACE` dispatch currently
passes a pre-opened cursor. After this change it would pass the store directly. The
register type for trace-side inputs changes from `cursor` to `store`. This is consistent
with how the executor already holds references to stores.

**Phasing**: because this requires a VM/register-type change, it is the most invasive of
the optimizations. Implement last, behind a feature flag or as a separate opcode variant
(`OPCODE_JOIN_DELTA_TRACE_PREFETCH = 20`) so existing behavior is not disturbed during
development.

---

## Optimization 6: Left Outer Join (SaturatingCursor)

### Motivation

Feldera implements `LEFT JOIN` via `SaturatingCursor` (`trace/cursor/saturating_cursor.rs`):
when the trace cursor is seeked to a key and the key is not found, the saturating cursor
synthesizes a virtual row with `weight=1` and all-NULL payload. The join then produces an
output row with NULLs in the right-side columns for every unmatched left key. The rest of
the join operator is identical to inner join.

Gnitz has no `LEFT JOIN` support. This section plans the full addition.

### What changes

#### Step 1 — `NullAccessor`

File: `gnitz/core/comparator.py` (new class)

```python
class NullAccessor(RowAccessor):
    """
    RowAccessor that returns NULL for every column.
    Used for outer-join right-side placeholder rows.
    """
    def is_null(self, col_idx):
        return True
    def get_int(self, col_idx):
        return r_uint64(0)
    def get_int_signed(self, col_idx):
        return r_int64(0)
    def get_float(self, col_idx):
        return 0.0
    def get_u128(self, col_idx):
        return r_uint128(0)
    def get_str_struct(self, col_idx):
        return (0, 0, NULL_PTR, NULL_PTR, "")
    def get_col_ptr(self, col_idx):
        return NULL_PTR
```

A single module-level singleton is enough:
```python
NULL_ACCESSOR = NullAccessor()
```

#### Step 2 — `merge_schemas_for_join_outer`

File: `gnitz/core/types.py`

```python
def merge_schemas_for_join_outer(left_schema, right_schema):
    """
    Same layout as merge_schemas_for_join but right-side non-PK columns
    are marked nullable (field_type.nullable = True).
    """
```

This produces the output schema for a LEFT JOIN. The SQL planner must use this variant
when the right side of a `LEFT JOIN` is the trace.

#### Step 3 — `op_join_delta_trace_outer`

File: `gnitz/dbsp/ops/join.py` (new function)

```python
def op_join_delta_trace_outer(delta_batch, trace_cursor, out_writer, d_schema, t_schema):
    """
    Delta-Trace Left Outer Join.
    For each delta row, if no matching trace row exists, emit a row with
    the right-side columns set to NULL (weight = w_delta).
    """
    composite_acc = CompositeAccessor(d_schema, t_schema)
    count = delta_batch.length()
    for i in range(count):
        w_delta = delta_batch.get_weight(i)
        if w_delta == r_int64(0):
            continue
        key = delta_batch.get_pk(i)
        trace_cursor.seek(key)

        found_match = False
        while trace_cursor.is_valid():
            if trace_cursor.key() != key:
                break
            w_trace = trace_cursor.weight()
            w_out = r_int64(intmask(w_delta * w_trace))
            if w_out != r_int64(0):
                composite_acc.set_accessors(
                    delta_batch.get_accessor(i),
                    trace_cursor.get_accessor(),
                )
                out_writer.append_from_accessor(key, w_out, composite_acc)
                found_match = True
            trace_cursor.advance()

        if not found_match:
            # Emit the left row with NULLs on the right side
            composite_acc.set_accessors(
                delta_batch.get_accessor(i),
                NULL_ACCESSOR,
            )
            out_writer.append_from_accessor(key, w_delta, composite_acc)

    out_writer.mark_sorted(delta_batch._sorted)
```

#### Step 4 — `op_join_delta_delta_outer`

File: `gnitz/dbsp/ops/join.py` (new function)

Mirror of `op_join_delta_delta` but unmatched left key groups are emitted with NULL right
columns at weight = sum(wa for that group).

#### Step 5 — New opcodes

File: `gnitz/core/opcodes.py`

```python
OPCODE_LEFT_JOIN_DELTA_TRACE = 20
OPCODE_LEFT_JOIN_DELTA_DELTA = 21
```

File: `gnitz/vm/interpreter.py` — add dispatch cases.
File: `gnitz/vm/instructions.py` — add builder functions `left_join_delta_trace_op()`, etc.

#### Step 6 — Schema and planner

File: `gnitz/catalog/program_cache.py`

The `CircuitBuilder` in `gnitz-core` needs a `left_join(left_node, right_node)` method
that uses `merge_schemas_for_join_outer` and emits `OPCODE_LEFT_JOIN_DELTA_TRACE` /
`OPCODE_LEFT_JOIN_DELTA_DELTA` opcodes. The SQL planner (`gnitz-sql/src/planner.rs`) emits
`CircuitBuilder::left_join(...)` for `LEFT JOIN` SQL nodes.

#### Nullable columns and `ArenaZSetBatch`

`append_from_accessor` already handles the null bitmap (`null_buf`):
```python
if accessor.is_null(col_idx):
    null_word |= (1 << payload_bit_idx)
    # column buffer gets a zero value
```

`NullAccessor.is_null()` always returns `True`, so all right-side column values will be
zero with the null bit set. This is exactly the correct encoding for SQL NULLs.

---

## Non-goals and deferred items

### Async / streaming yield (not applicable)

Feldera yields partial output batches mid-join via `async_stream::stream!`. This enables
operator-level pipelining in Feldera's multi-threaded runtime. Gnitz's RPython runtime is
synchronous and single-threaded per worker. The equivalent benefit (bounded peak memory)
is fully covered by Optimization 4 (periodic consolidation). The streaming machinery
itself is not worth porting.

### Background spine merging (storage-layer concern)

Feldera maintains a 9-level LSM-style spine that background-merges batches using a
dedicated thread. Gnitz's storage layer has a two-level structure (memtable + immutable
shards). Background compaction of shards is a storage-layer optimization orthogonal to
the join operators and is tracked in the multi-core roadmap. It would reduce
`UnifiedCursor` merge fan-out (fewer shards = cheaper N-way heap merge in the join's trace
seeks) but requires the multi-process worker model to be in place first.

### Fetch/prefetch for ephemeral tables

Optimization 5 is only impactful for persistent (shard-backed) traces. Ephemeral tables
used for DBSP operator state (e.g., the trace register in the VM) are already fully
in-memory. Attempting to gather-prefetch an in-memory batch adds overhead with no benefit.
The `create_filtered_cursor` default implementation (returns `create_cursor()`) handles
this correctly — ephemeral tables simply inherit the no-op default.

---

## Implementation order and dependencies

```
# Prerequisites (from source-optimizations.md — implement first)
P1. _consolidated flag + to_consolidated() short-circuit   ← source-optimizations.md Step 1
P2. mark_consolidated() at all production sites            ← source-optimizations.md Step 2
P3. Seal in execute_epoch                                  ← source-optimizations.md Step 3

# Join-specific, ordered by impact and dependency
1. ConsolidatedScope in op_join_delta_delta     ← no join dependencies, 2-line change
2. estimated_length() + seek_key_exact()        ← preconditions for (3)
3. Adaptive cursor swapping + ConsolidatedScope in _join_dt_swapped  ← depends on (2)
4. _join_dt_merge_walk + anti/semi-join walk    ← depends on P1–P3
5. BatchWriter.consolidate()                    ← no dependencies
6. Periodic consolidation in join operators     ← depends on (5)
7. NullAccessor + merge_schemas_outer           ← no dependencies
8. op_join_delta_trace_outer + opcodes          ← depends on (7)
9. _gather_shard_rows + create_filtered_cursor  ← no dependencies for helper
10. VM register-type change for prefetch        ← depends on (9), most invasive
```

Items 1–6 are pure performance improvements to existing operators. Items 7–8 are a new
feature (LEFT JOIN). Items 9–10 are the prefetch infrastructure, highest engineering cost,
deferred until traces grow large enough to make seeks I/O-bound.

---

## Test approach

Each optimization has a natural regression target:

| Optimization | Test file | What to add |
|---|---|---|
| ConsolidatedScope (delta-delta) | `rust_client/gnitz-py/tests/test_workers.py` | Join with update batch (delete+reinsert same PK same tick): verify output has correct single consolidated row, not doubled |
| Adaptive cursor swap | same | Join where delta.length > trace estimated_length: verify same output as normal path |
| Merge-walk | same | Join with sorted/consolidated delta against larger trace: verify same output as normal path; run with GNITZ_WORKERS=4 |
| BatchWriter.consolidate | `ipc_comprehensive_test` or new `join_consolidation_test.py` | Inject large fan-out join, assert peak batch size stays bounded |
| Left outer join | new E2E test | LEFT JOIN with unmatched left rows: verify NULL right columns appear in output |
| Prefetch | `partitioned_table_test` or new test | Join against large shard (>L3 cache): verify correctness; benchmark seek count |

All tests must be run with `GNITZ_WORKERS=4` per project convention.
