# Trace Memtable: Inline Consolidation + Small-Batch Accumulator Routing

No external dependencies. Implementation in `crates/gnitz-engine/src/memtable.rs`.

---

## Problem

### 1. Redundant retraction/insertion pairs (write amplification)

REDUCE, DISTINCT, and other stateful DBSP operators maintain trace tables. Each tick, the
operator writes a small delta batch via INTEGRATE → `upsert_batch()`. For REDUCE, each tick's
delta contains per changed group:

```
[(group_key, old_aggregate_value, weight = -1),
 (group_key, new_aggregate_value, weight = +1)]
```

If group G changes every tick for 100 ticks before a memtable flush:
- 100 `OwnedBatch` runs in `self.runs`, each containing 2 rows for G
- 200 total rows across all runs
- After consolidation: the retraction/insertion pairs cancel, leaving at most 2 rows

For M groups each changing every tick, the memtable holds `M × ticks × 2` rows of
intermediate state. The net state is only `M` rows.

### 2. Many tiny sorted runs (per-call overhead)

Each `upsert_batch()` call:
1. Calls `sort_batch()` → allocates a new `OwnedBatch`, copies rows sorted by PK
2. Pushes the batch to `self.runs`
3. Invalidates `cached_consolidated`

For a 2-row batch from INTEGRATE:
- `sort_batch()` allocates ~250 bytes (7 `Vec`s with ~80 bytes of actual data)
- The run is a separate allocation with its own Vec bookkeeping

After 100 ticks: 100 `OwnedBatch` objects in `self.runs`.

### 3. Costs compound on read paths

**`get_snapshot()`** with 100 runs:
- Creates 100 `MappedShard` views via `as_shard()`
- Calls `merge_to_writer()` — builds a tournament tree of 100 nodes
- O(200 × log(100)) ≈ 1300 full-row comparisons
- Converts `ShardWriter` to `OwnedBatch` via `writer_to_owned()` (clones all Vecs)

This runs every time the consolidation cache is invalidated — which is every `upsert_batch()`
call. For operators that read their trace every tick (DISTINCT, REDUCE), this means a full
N-way merge every tick.

**`lookup_pk()`** with 100 runs:
- Iterates all runs: `for run in &self.runs { run.as_shard(); view.find_lower_bound(); }`
- 100 MappedShard constructions + 100 binary searches per point lookup
- This is the hot path for DISTINCT (every input row seeks the history table)

**`should_flush()`** triggers prematurely:
- `total_bytes()` counts all intermediate rows, not net state
- A trace table with 1000 groups changing every tick fills the 1 MB memtable in ~16 ticks
  (1000 × 16 × 2 × 32 = 1 MB), even though the net state is only 32 KB

---

## Solution

Two complementary changes to `RustMemTable`:

### A. Route small batches through the accumulator

`upsert_batch()` currently always creates a new sorted run. But `self.acc` (the accumulator)
already exists for single-row appends via `append_row()` — it stages rows unsorted until
`ACCUMULATOR_THRESHOLD` (64 rows), then sorts and promotes to a run.

For small batches, route their rows through the accumulator instead of creating a new run.
No new threshold constant needed — use the existing `ACCUMULATOR_THRESHOLD` as the boundary.

```rust
pub fn upsert_batch(&mut self, src: &MappedShard) -> i32 {
    if src.count == 0 { return 0; }
    if self.total_bytes() > self.max_bytes { return ERR_MEMTABLE_FULL; }

    for i in 0..src.count {
        self.bloom.add(src.get_pk_lo(i), src.get_pk_hi(i));
    }

    // If the batch fits in the accumulator alongside existing rows, append there.
    // This batches many small INTEGRATE calls into one sorted run.
    if src.count + self.acc.count <= ACCUMULATOR_THRESHOLD {
        self.acc.append_from_shard(src, &self.schema);
        self.total_row_count += src.count;
        return 0;
    }

    // Accumulator would overflow — flush it first.
    if self.acc.count > 0 {
        self.flush_accumulator();
    }

    // If batch alone fits in the (now-empty) accumulator, put it there.
    if src.count <= ACCUMULATOR_THRESHOLD {
        self.acc.append_from_shard(src, &self.schema);
        self.total_row_count += src.count;
        return 0;
    }

    // Large batch: sort and add as run (existing behavior)
    let sorted = sort_batch(src, &self.schema);
    self.total_row_count += sorted.count;
    self.runs_bytes += sorted.bytes();
    self.runs.push(sorted);
    self.invalidate_cache();
    0
}
```

The new `OwnedBatch::append_from_shard()` method copies rows from a `MappedShard` into the
batch's Vecs. For non-string columns, the source data is contiguous in the MappedShard —
one `extend_from_slice` per column replaces N per-row copies:

```rust
impl OwnedBatch {
    fn append_from_shard(&mut self, src: &MappedShard, schema: &SchemaDescriptor) {
        let n = src.count;
        if n == 0 { return; }
        let pk_index = schema.pk_index as usize;

        // Structural columns: bulk copy (contiguous in MappedShard)
        for i in 0..n {
            self.pk_lo.extend_from_slice(&src.get_pk_lo(i).to_le_bytes());
            self.pk_hi.extend_from_slice(&src.get_pk_hi(i).to_le_bytes());
            self.weight.extend_from_slice(&src.get_weight(i).to_le_bytes());
            self.null_bm.extend_from_slice(&src.get_null_word(i).to_le_bytes());
        }

        // Payload columns
        let mut payload_idx = 0;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col = &schema.columns[ci];
            let col_size = col.size as usize;

            if col.type_code == 11 { // STRING: per-row (blob relocation)
                let src_blob = src.blob_slice();
                for i in 0..n {
                    let s = src.get_col_ptr(i, payload_idx, 16);
                    relocate_string_into(&mut self.cols[ci], &mut self.blob, s, src_blob);
                }
            } else {
                // Non-string: per-row from MappedShard accessors.
                // (If MappedShard exposes contiguous column slices, this can
                // become a single extend_from_slice per column.)
                for i in 0..n {
                    let data = src.get_col_ptr(i, payload_idx, col_size);
                    self.cols[ci].extend_from_slice(data);
                }
            }
            payload_idx += 1;
        }

        self.count += n;
    }
}
```

**Effect**: 32 two-row INTEGRATE calls → rows accumulate in `self.acc` → 1 sorted run of 64
rows when the accumulator flushes (instead of 32 separate runs).

### B. Periodic inline consolidation

After every K runs accumulate, merge them into one consolidated run. Weight cancellations
happen early.

```rust
const INLINE_CONSOLIDATION_THRESHOLD: usize = 16;

fn maybe_inline_consolidate(&mut self) {
    if self.runs.len() < INLINE_CONSOLIDATION_THRESHOLD {
        return;
    }
    let views: Vec<MappedShard> = self.runs.iter()
        .map(|r| r.as_shard(&self.schema))
        .collect();
    let refs: Vec<ShardRef> = views.iter()
        .map(|v| ShardRef::Borrowed(v as *const MappedShard))
        .collect();
    let writer = merge_to_writer(&refs, &self.schema);
    let merged = writer_to_owned(&writer, &self.schema);

    self.runs.clear();
    self.runs_bytes = merged.bytes();
    self.total_row_count = merged.count + self.acc.count;
    self.runs.push(merged);
    self.invalidate_cache();
}
```

Called at the end of `flush_accumulator()`:

```rust
fn flush_accumulator(&mut self) {
    if self.acc.count == 0 { return; }
    let shard_view = self.acc.as_shard(&self.schema);
    let sorted = sort_batch(&shard_view, &self.schema);
    self.runs_bytes += sorted.bytes();
    self.runs.push(sorted);
    self.acc.clear();
    self.invalidate_cache();

    self.maybe_inline_consolidate();  // NEW
}
```

**Effect**: Run count bounded at 16. After inline consolidation, weight-cancelled rows are
eliminated — the memtable shrinks to net state size.

### Combined effect

For a trace table receiving 2-row INTEGRATE batches every tick:

| Stage | What happens |
|---|---|
| Ticks 1-32 | Rows append to accumulator (no runs created) |
| Tick 32 | Accumulator reaches 64 rows → `flush_accumulator()` → 1 sorted run |
| Tick 32 | 1 run < 16 → no inline consolidation yet |
| Ticks 33-64 | Another 32 ticks → accumulator fills again → 2 runs |
| ... | |
| Tick ~512 | 16 runs → `maybe_inline_consolidate()` → merge + weight-cancel → 1 run |

After inline consolidation of 16 runs × 64 rows = 1024 total rows:
- If 90% cancel (same group retracted+reinserted): 1 run of ~100 rows
- `total_bytes()` reflects net state, not accumulated deltas
- `should_flush()` stays below threshold much longer

Without either optimization: 256 runs after 512 ticks.
With both: 1-16 runs, much smaller.

---

## Existing Bug in `get_snapshot()` (fix alongside this work)

`memtable.rs` lines 323-326 contain dead code with pointers to dropped temporaries:

```rust
// Lines 323-326: DEAD CODE — refs shadows this at line 331
let refs: Vec<ShardRef> = self.runs.iter()
    .map(|r| ShardRef::Borrowed(&r.as_shard(&self.schema) as *const MappedShard))
    .collect();
// ^^^ r.as_shard() creates a temporary MappedShard inside the closure.
// &temp as *const MappedShard takes a pointer to it. The temporary is dropped
// at the end of the closure body. refs now holds dangling pointers.

// Lines 328-333: THE CORRECT VERSION (keeps views alive)
let views: Vec<MappedShard> = self.runs.iter()
    .map(|r| r.as_shard(&self.schema))
    .collect();
let refs: Vec<ShardRef> = views.iter()   // shadows the first `refs`
    .map(|v| ShardRef::Borrowed(v as *const MappedShard))
    .collect();
```

The first `refs` is never dereferenced (shadowed immediately), so this is not UB in practice.
But it is dead code holding dangling raw pointers — should be deleted. Lines 324-326 should
be removed entirely; the comment at line 327 is the explanation for why the two-step
(views + refs) pattern exists.

---

## Impact

### Memory

For a trace table with 1000 active groups changing every tick (stride=32):

| Metric | Current | With both optimizations |
|---|---|---|
| Rows in memtable after 100 ticks | 200,000 | ~2,000 |
| Memtable bytes | 6.4 MB | ~64 KB |
| Runs in `self.runs` | 100 | 1-2 |
| Flushes triggered (1 MB limit) | ~6 | 0 |

### Read paths

| Operation | Current (100 runs) | Optimized (1-2 runs) |
|---|---|---|
| `get_snapshot()` | 100-way merge, O(200K × log(100)) | 1-2 way merge (or cached) |
| `lookup_pk()` | 100 binary searches | 1-2 binary searches |
| `find_weight_for_row()` | 100 binary searches + row compare | 1-2 |

### Flush frequency

Fewer flushes → fewer L0 shards → less compaction → fewer `fsync()` calls.

---

## Correctness

### Z-set algebraic identity

```
consolidate(consolidate(A) ∪ B) = consolidate(A ∪ B)
```

Inline consolidation produces the same result as deferred consolidation. The merge via
`merge_to_writer()` groups by (PK, full payload) and sums weights, eliminating zero-weight
rows. This is the same operation used at flush time.

### Accumulator routing

Appending batch rows to the accumulator instead of creating sorted runs does not change
semantics — the rows enter the memtable either way. The accumulator is unsorted, but
`flush_accumulator()` sorts it before promoting to a run. `get_snapshot()` sorts and merges
the accumulator if needed.

### Bloom filter

The bloom filter remains additive. Accumulator-routed rows are added to the bloom at
`upsert_batch()` time (before the routing decision). Inline consolidation does not rebuild
the bloom — cancelled keys remain as false positives. This is acceptable (inherent FPR,
washes out at next memtable reset).

### Cache invalidation

The accumulator routing paths do NOT call `invalidate_cache()`. This is correct:
`cached_consolidated` represents the merged state of `self.runs` only. `get_snapshot()`
always checks `self.acc.count > 0` and merges the cache with the sorted accumulator when
needed (line 338). `lookup_pk()` and `find_weight_for_row()` separately scan the accumulator
(lines 434-445, 488-500). This is consistent with the existing `append_row()` behavior,
which also does not invalidate the cache.

`flush_accumulator()` calls `invalidate_cache()` (because it adds a run to `self.runs`).
`maybe_inline_consolidate()` also calls `invalidate_cache()` (because it replaces all runs
with one merged run). The next `get_snapshot()` sees the updated runs. No stale state.

### `total_row_count` accuracy

After inline consolidation, `total_row_count` is updated to reflect surviving rows +
accumulator rows. After accumulator routing, `total_row_count` is incremented per row
appended. Both are consistent with `should_flush()` and `is_empty()`.

---

## Implementation Steps

### Step 1: Fix existing bug in `get_snapshot()`

Delete dead code at lines 324-326 (dangling raw pointers to dropped temporaries, shadowed
at line 331). See "Existing Bug" section above.

### Step 2: Add `OwnedBatch::append_from_shard()`

Add the method to `OwnedBatch` that copies rows from a `MappedShard` into the batch's Vecs.
Handle all column types including string relocation (reuse the existing
`relocate_string_into` function). Use bulk `extend_from_slice` for non-string columns where
the MappedShard data is contiguous.

### Step 3: Route small batches through accumulator in `upsert_batch()`

Add the `src.count + self.acc.count <= ACCUMULATOR_THRESHOLD` check. Small batches go
through `self.acc.append_from_shard()`. Large batches use the existing `sort_batch()` + push
path. Ensure `total_row_count` is updated in both accumulator routing branches.

### Step 4: Add `maybe_inline_consolidate()`

Add the inline consolidation method. Call it at the end of `flush_accumulator()`.

### Step 5: Tests

1. **Accumulator routing**: `upsert_batch()` with 2-row batches. Verify rows go to
   accumulator (`mt.acc.count` increases), runs stay empty until accumulator threshold.

2. **`total_row_count` and `is_empty()` correctness**: After accumulator-routed upserts,
   verify `total_rows()` returns the correct count and `is_empty()` returns false. This
   specifically tests the bug fix (missing `total_row_count` update in routing paths).

3. **Accumulator flush + inline consolidation**: Upsert enough small batches to trigger
   accumulator flush, then enough runs to trigger inline consolidation. Verify run count
   stays bounded.

4. **Weight cancellation**: Upsert 100 two-row batches with retraction/insertion pairs for
   the same group. Verify memtable row count decreases after inline consolidation. Verify
   `get_snapshot()` returns the net state.

5. **Snapshot correctness**: After mixed upsert_batch + append_row calls, verify
   `get_snapshot()` returns the same rows as a baseline without optimizations.

6. **lookup_pk correctness**: After inline consolidation, verify `lookup_pk()` returns
   correct summed weight for keys that survived cancellation AND for keys that were
   fully cancelled (should return weight 0, found None).

7. **Large batch bypass**: Upsert a 1000-row batch. Verify it goes directly to runs
   (not through accumulator). Verify `total_row_count` is correct.

8. **Accumulator-only snapshot**: Upsert a few small batches (no runs created yet). Call
   `get_snapshot()`. Verify it returns the correct consolidated rows. (Tests the
   `get_snapshot()` "only accumulator" path that becomes more frequent with routing.)

7. **Flush correctness**: After inline consolidation, flush to shard. Read shard back.
   Verify contents match `get_snapshot()`.

8. **Existing tests pass**: All tests in `mod tests` at the bottom of `memtable.rs` must
   continue to pass unchanged.

### Step 6: Measure

Benchmark with a synthetic REDUCE trace workload:
- 1000 groups, each changing every tick, 500 ticks
- Measure: peak memtable bytes, number of runs, flush count, lookup_pk latency,
  get_snapshot latency
- Compare against baseline (no accumulator routing, no inline consolidation)

---

## Threshold Tuning

Only two thresholds, both already present in the codebase:

### ACCUMULATOR_THRESHOLD (existing: 64)

Controls when the accumulator flushes to a sorted run. Also now controls the small-batch
routing boundary — if `src.count + self.acc.count <= ACCUMULATOR_THRESHOLD`, the batch goes
into the accumulator.

For REDUCE trace tables (2 rows per tick): 32 ticks to fill the accumulator → 1 run per 32
ticks. This captures the common case without a separate threshold constant.

For base table inserts (100-10K rows per batch): exceeds the threshold → goes directly to a
sorted run (existing behavior, unchanged).

### INLINE_CONSOLIDATION_THRESHOLD (new: 16)

Inline consolidation runs when `self.runs.len() >= 16`. With accumulator routing, each run
contains ~64 rows. So inline consolidation processes ~16 × 64 = 1024 rows in a 16-way merge.

If set too high: run accumulation continues too long, hurting `lookup_pk()` and
`get_snapshot()` between consolidations.

If set too low: inline consolidation runs too frequently. Each consolidation call builds a
tournament tree, performs a full merge, and allocates a new OwnedBatch. For 2 runs of 64
rows, the overhead is small but non-zero.

16 is a reasonable default. The cost of a 16-way merge of ~1024 rows is sub-millisecond in
Rust. The benefit is early weight cancellation + bounded run count.

---

## What this plan does NOT cover

- **Merge into last run**: Instead of accumulator routing, merge the incoming batch directly
  into `runs.last()` via a 2-way merge. This would keep run count at 1 at all times but
  requires a new `OwnedBatch` allocation per merge. Consider if inline consolidation proves
  insufficient.

- **Sorted accumulator / B-tree memtable**: Replace the unsorted accumulator + sorted runs
  with a single sorted mutable structure (B-tree, skip list). Eliminates consolidation
  entirely but changes the memtable fundamentally. Much higher implementation complexity.

- **Per-table threshold tuning**: Different thresholds for trace tables vs base tables.
  Start with constants; tune if measurement shows overhead for base tables.

- **Bloom filter compaction**: After inline consolidation, the bloom contains false positives
  for cancelled keys. Rebuilding it would eliminate these but costs O(N) hash operations.
  Defer unless bloom FPR becomes a measured problem.

- **Cached MappedShard views per run**: `lookup_pk()` calls `run.as_shard(&self.schema)` for
  every run on every call, constructing a `MappedShard` each time (including `col_regions`
  Vec allocation). With fewer runs (from our optimizations) this matters less, but for
  DISTINCT's per-row history seeks it's still significant. Could cache the `MappedShard`
  alongside each run and invalidate on mutation.

- **`get_snapshot()` single-batch fast path** (known regression from this plan): When only
  the accumulator has data (no runs, no cache), `get_snapshot()` sorts the accumulator,
  wraps it as a `MappedShard`, calls `merge_to_writer` (builds a tournament tree of 1 node),
  then converts back. For a single sorted batch, this is overkill — a direct sort +
  consolidate (without the tournament tree) would be cheaper. **Accumulator routing makes
  this path more frequent**: before, `upsert_batch()` always created a run, so `self.runs`
  was non-empty after the first call. With routing, the first ~31 ticks (2-row batches) put
  data only in the accumulator with no runs. `get_snapshot()` hits this path ~31 times per
  accumulator cycle. This should be addressed as a follow-up (direct consolidation of a
  single sorted batch, bypassing merge_to_writer).

- **`scatter_into` refactor**: Extract the population logic from `scatter_copy` into
  `scatter_into(out: &mut OwnedBatch, ...)` so both `sort_batch` and `append_from_shard`
  share the column-copy dispatch. Not required for correctness but reduces code duplication.
