# Refactor: shared k-way merge driver across compact, merge, and read_cursor

## Background

The same outer-group / inner-fold loop appears in three places:

- `storage/compact.rs:177–233` (`open_and_merge_inner`) — folds `ShardCursor`s
  over `MappedShard` sources; emits via a free `emit(key, weight, shard, row)`
  callback.
- `storage/merge.rs:448–501` (`merge_batches_inner`) — folds `MemBatchCursor`s
  over `SortedMemBatch` sources; emits via `writer.write_row(batch, row, weight)`.
- `storage/read_cursor.rs:513–581` (`drive_with`) — folds `ReadCursorEntry`s
  over `CursorSource`s; emits by writing to `self.current_*` and returning to
  the caller after each non-ghost group.

The loop body is structurally identical in all three:

1. Open a group from the heap root: save `(group_src, group_row, group_key)`.
2. Inner fold: while the heap root ties on `(key, payload)` with the saved
   exemplar, accumulate `net_weight`, advance the cursor, and call
   `replace_top` (cursor still valid) or `pop_top` (cursor exhausted).
3. After the inner loop, if `net_weight != 0`, emit the group.

All three rebuild the `less` closure inside the inner loop, with an identical
comment explaining the cause:

```
// HeapNode comparator is rebuilt at each tree op so its capture of
// `cursors` stays scoped to a single call, leaving `cursors` free to
// be mutated in between.
```

The `less` closure captures `cursors[src].position` (immutable read) while
`advance()` takes `&mut cursors[src]`.  These cannot coexist as simultaneously
live borrows in Rust, so the closure is dropped and recreated each iteration.

### Why eliminating the rebuild requires a HeapNode change

The cleanest way to break the conflict is to store the current row index
directly in `HeapNode`:

```rust
pub struct HeapNode {
    pub key: u128,
    pub source_idx: usize,
    pub row: usize,         // current row position for this source
}
```

With `row` embedded in the node, `less` can compare `(key, payload)` using
`a.row` and `b.row` from the nodes themselves — no cursor access needed.
Cursor access is then confined exclusively to the `advance` closure, which
holds the only mutable borrow.  The conflict disappears, and the `less`
closure no longer needs to be recreated per tree operation.

This requires updating `LoserTree::build` (init_fn returns `Option<(u128,
usize)>`), replacing `replace_top_key` with `replace_top(new_key, new_row,
&less)`, and touching the three existing call sites plus the heap unit tests.

## The change

### Step 0 — Extend `HeapNode` and update `LoserTree` API in `heap.rs`

```rust
#[derive(Clone, Copy)]
pub struct HeapNode {
    pub key: u128,
    pub source_idx: usize,
    pub row: usize,
}

const SENTINEL_NODE: HeapNode = HeapNode { key: 0, source_idx: SENTINEL, row: 0 };
```

`build` signature and leaf-init change:

```rust
pub fn build(
    n: usize,
    init_fn: impl Fn(usize) -> Option<(u128, usize)>,  // (key, row) or None
    less: impl Fn(&HeapNode, &HeapNode) -> bool,
) -> Self {
    // ...
    for i in 0..n {
        if let Some((key, row)) = init_fn(i) {
            winners[n_pad + i] = HeapNode { key, source_idx: i, row };
        }
    }
    // ...
}
```

Replace `replace_top_key` with, and update `pop_top` to take `less` by
reference (matching the existing `walk_up` signature):

```rust
/// Overwrite the champion's key and row, then walk up.
#[inline]
pub fn replace_top(
    &mut self,
    new_key: u128,
    new_row: usize,
    less: &impl Fn(&HeapNode, &HeapNode) -> bool,
)

#[inline]
pub fn pop_top(
    &mut self,
    less: &impl Fn(&HeapNode, &HeapNode) -> bool,
)
```

`walk_up` already takes `&impl Fn(...)`; its bound is unchanged.  All real
`less` closures capture only immutable data (`&batches`, `&shards`, `&schema`)
so they implement `Fn`, not merely `FnMut`.  Using `&impl Fn` avoids
unnecessary `FnMut` gymnastics.

Heap unit tests: change `build(keys: &[Option<u128>])` helper to pass
`|i| keys[i].map(|k| (k, i))` as `init_fn`, change `replace_top_key(v, less)`
calls to `replace_top(v, 0, &less)`, and `pop_top(less)` calls to
`pop_top(&less)`.  The test `less` function already ignores `row`, so no
test logic changes beyond the API update.

### Step 1 — Add `drive_merge` to `heap.rs`

```rust
use std::ops::ControlFlow;

/// Drive an N-way merge to completion.
///
/// `less` — compare two HeapNodes; uses `a.row` / `b.row` for payload
///           tie-breaks; must NOT capture cursor state directly.
/// `advance(src) -> Option<(key, row)>` — advance source `src`; returns
///           the cursor's new (key, row) or None when exhausted.
/// `eq_payload(a_src, a_row, b_src, b_row) -> bool` — true when both
///           positions carry the same payload.
/// `weight(src, row) -> i64` — weight at (src, row).
/// `emit(group_src, group_row, group_key, net_weight) -> ControlFlow<()>` —
///           called for each non-ghost group; Break returns immediately.
///
/// `#[inline(always)]`: the compact/merge emit closures return a constant
/// `ControlFlow::Continue(())`, so forced inlining lets LLVM evaluate
/// the branch at compile time and DCE it.
#[inline(always)]
pub fn drive_merge<ADV, EQ, W, EM>(
    heap: &mut LoserTree,
    less: impl Fn(&HeapNode, &HeapNode) -> bool,
    mut advance: ADV,
    mut eq_payload: EQ,
    mut weight: W,
    mut emit: EM,
)
where
    ADV: FnMut(usize) -> Option<(u128, usize)>,
    EQ: FnMut(usize, usize, usize, usize) -> bool,
    W: FnMut(usize, usize) -> i64,
    EM: FnMut(usize, usize, u128, i64) -> ControlFlow<()>,
{
    loop {
        if heap.is_empty() { return; }

        let group_src = heap.peek().source_idx;
        let group_row = heap.peek().row;
        let group_key = heap.peek().key;
        let mut net_weight: i64 = 0;

        loop {
            let cur_src = heap.peek().source_idx;
            let cur_key = heap.peek().key;
            let cur_row = heap.peek().row;

            if cur_key != group_key
                || !eq_payload(group_src, group_row, cur_src, cur_row)
            {
                break;
            }

            net_weight += weight(cur_src, cur_row);

            if let Some((new_key, new_row)) = advance(cur_src) {
                heap.replace_top(new_key, new_row, &less);
            } else {
                heap.pop_top(&less);
                if heap.is_empty() { break; }
            }
        }

        if net_weight != 0 {
            if emit(group_src, group_row, group_key, net_weight).is_break() {
                return;
            }
        }
    }
}
```

`less`, `eq_payload`, and `weight` capture only immutable data
(`batches`/`shards`/`sources`).  Only `advance` captures a mutable cursor
slice.  No closure conflict.

### Step 2 — Port `merge_batches_inner` in `merge.rs`

With `row` in `HeapNode`, the `less` closure no longer reads
`cursors[src].position` — it uses `a.row`/`b.row` directly.  The
`cursors_ref` scope block is not needed; closures pass directly to
`LoserTree::build`.

Drop the `batch_idx` field from `MemBatchCursor`.  `batch_idx == source_idx`
always: `MemBatchCursor::new(i, …)` is always called with `i` matching its
slot in the `cursors` slice.  After removal every `cursors[src].batch_idx`
reference becomes `src`, and `MemBatchCursor` shrinks from 3×usize to 2×usize
(24 → 16 bytes).

Update the constructor: `MemBatchCursor::new(batch_idx, count)` →
`MemBatchCursor::new(count)`.

Update `peek_key`: since `batch_idx` is gone, change the signature from
`peek_key(&self, batches: &[SortedMemBatch])` to
`peek_key(&self, batch: &SortedMemBatch)` (symmetric with how
`ShardCursor::peek_key` already works).  Call sites become
`cursors[i].peek_key(&batches[i])`.

```rust
fn merge_batches_inner<RowCmp>(
    cursors: &mut [MemBatchCursor],
    batches: &[SortedMemBatch],
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    row_cmp: RowCmp,
) where RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy
{
    let less = |a: &HeapNode, b: &HeapNode| -> bool {
        if a.key != b.key { return a.key < b.key; }
        // source_idx == batch slot always in merge_batches_inner
        row_cmp(schema, &batches[a.source_idx].0, a.row,
                        &batches[b.source_idx].0, b.row) == Ordering::Less
    };
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| {
            if cursors[i].is_valid() {
                Some((cursors[i].peek_key(&batches[i]), cursors[i].position))
            } else {
                None
            }
        },
        &less,
    );
    drive_merge(
        &mut tree,
        less,
        |src| {
            cursors[src].advance();
            if cursors[src].is_valid() {
                Some((cursors[src].peek_key(&batches[src]), cursors[src].position))
            } else {
                None
            }
        },
        |a_src, a_row, b_src, b_row| {
            row_cmp(schema, &batches[a_src].0, a_row,
                            &batches[b_src].0, b_row) == Ordering::Equal
        },
        |src, row| batches[src].get_weight(row),
        |group_src, group_row, _key, w| {
            writer.write_row(&batches[group_src], group_row, w);
            ControlFlow::Continue(())
        },
    );
}
```

`less` captures only `schema` (Copy), `batches` (immutable ref), and `row_cmp`
(Copy fn item).  `advance` captures only `cursors` (mutable ref).  No conflict.

Verify with `merge_consolidate` and `merge_batches` micro-benches — must be
within noise.

### Step 3 — Port `open_and_merge_inner` in `compact.rs`

Identical shape to Step 2.  `shard_idx == source_idx` always (cursors are
built as `ShardCursor::new(i, &shards[i])`), so `&shards[a.source_idx]`
replaces `&shards[cursors[a.source_idx].shard_idx]` in `less`.  The
`cursors_ref` scope block is dropped here as well.

Drop the `shard_idx` field from `ShardCursor` for the same reason as
`batch_idx` in Step 2.  `ShardCursor` shrinks from 3×usize to 2×usize.
Update the constructor: `ShardCursor::new(shard_idx, shard)` →
`ShardCursor::new(shard)`.

`ShardCursor::peek_key` already takes `&MappedShard` directly; no change
needed there.

`advance` calls `cursors[src].advance(&shards[src])` (mutable) and returns
`Some((cursors[src].peek_key(&shards[src]), cursors[src].position))`.

`emit` returns `ControlFlow::Continue(())` always — compact is a full drain.

### Step 4 — Port `drive_with` in `read_cursor.rs`

Replace `ReadCursorEntry` with a struct-of-arrays layout inside `ReadCursor`.
`source` is never mutated after construction; `position` and `count` form the
mutable cursor state.  Splitting them into separate `Vec`s lets the borrow
checker see `&sources` and `&mut states` as disjoint fields — no per-call
allocation and no `EmitSlot`.

**Delete `ReadCursorEntry` entirely** (struct and both constructors
`new_batch` / `new_shard`).

```rust
struct CursorState {
    position: usize,
    count: usize,   // keep inline: is_valid() must not dispatch through CursorSource
}

impl CursorState {
    fn is_valid(&self) -> bool { self.position < self.count }

    fn advance(&mut self, src: &CursorSource) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts(src);
        }
    }

    fn seek(&mut self, src: &CursorSource, key: u128) {
        self.position = src.find_lower_bound(key);
        self.skip_ghosts(src);
    }

    fn skip_ghosts(&mut self, src: &CursorSource) {
        if let CursorSource::Shard(s) = src {
            self.position = s.next_non_ghost(self.position);
        }
    }
}

pub struct ReadCursor {
    sources: Vec<CursorSource>,  // immutable after construction
    states: Vec<CursorState>,    // cursor positions
    unified_sources: OnceCell<Vec<UnifiedSource>>,
    mode: SourceMode,
    schema: SchemaDescriptor,
    is_fast: bool,
    pub valid: bool,
    pub current_key: u128,
    pub current_weight: i64,
    pub current_null_word: u64,
    current_entry_idx: usize,
    current_row: usize,
}
```

Update `create_read_cursor` to populate `sources` and `states` directly
instead of building `Vec<ReadCursorEntry>`.  `ReadCursor::new` takes
`(sources, states, schema)` or is inlined into `create_read_cursor`.

**Unit test migration**: every test in `read_cursor.rs` that constructs
`ReadCursorEntry::new_batch(...)` or `ReadCursorEntry::new_shard(...)` and
calls `ReadCursor::new(entries, schema)` must be rewritten to use the
`create_read_cursor(&[batch], &[], schema)` helper.  The helper is already
tested by the existing suite; using it directly makes the test setup cleaner.

Update all methods that previously accessed `self.entries`:

- **`advance_with` single-source fast path**: `self.entries[0].advance()` →
  `self.states[0].advance(&self.sources[0])`.

- **`seek`**: `for e in &mut self.entries { e.seek(key); }` →
  ```rust
  for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
      state.seek(src, key);
  }
  ```
  The `build_tree_with` call that follows also changes to pass `&self.sources`
  and `&self.states`.

- **`estimated_length`**: `self.entries.iter().map(|e| e.count.saturating_sub(e.position)).sum()`
  → `self.states.iter().map(|s| s.count.saturating_sub(s.position)).sum()`.

- **`col_ptr`**: `self.entries[self.current_entry_idx].source` →
  `self.sources[self.current_entry_idx]`; `entry.position` → `self.current_row`
  (already held in the field).

- **`blob_ptr`**: `self.entries[self.current_entry_idx].source.blob_ptr()` →
  `self.sources[self.current_entry_idx].blob_ptr()`.

- **`blob_len`**: `self.entries[self.current_entry_idx].source.blob_slice().len()` →
  `self.sources[self.current_entry_idx].blob_slice().len()`.

- **`drain_single_source`**: `self.entries.len()`, `self.entries[0].position`,
  `self.entries[0].source` → `self.sources.len()`, `self.states[0].position`,
  `self.sources[0]`.  Update the position advance to `self.states[0].position`.

- **`single_mem_batch`**: `self.entries.len()`, `self.entries[0].source`,
  `self.entries[0].position` → `self.sources.len()`, `self.sources[0]`,
  `self.states[0].position`.

- **`total_blob_len`**: `self.entries.iter().map(|e| e.source_blob_len()).sum()` →
  `self.sources.iter().map(|s| s.source_blob_len()).sum()`.

- **`materialize`**: `self.entries[0].position`, `self.entries[0].source` →
  `self.states[0].position`, `self.sources[0]`.

- **`scatter_drained_into`** lazy-init of `unified_sources`:
  ```rust
  let unified = self.unified_sources.get_or_init(|| {
      self.sources.iter().map(|s| s.to_unified(&self.schema)).collect()
  });
  ```

- **`drive_single`**: reads `sources[0]` and `states[0]` directly.

In `drive_with`, unpack disjoint field borrows before calling `drive_merge`
so `emit` can write `self.current_*` fields without conflicting with
`advance`'s `&mut self.states`:

```rust
fn drive_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
    let heap = match &mut self.mode {
        SourceMode::Empty  => { self.valid = false; return; }
        SourceMode::Single => { self.drive_single(); return; }
        SourceMode::Multi(h) => h,
    };
    let schema   = &self.schema;
    let sources  = &self.sources;
    let states   = &mut self.states;
    let c_key    = &mut self.current_key;
    let c_weight = &mut self.current_weight;
    let c_idx    = &mut self.current_entry_idx;
    let c_row    = &mut self.current_row;
    let c_null   = &mut self.current_null_word;
    let mut found = false;

    let less = |a: &HeapNode, b: &HeapNode| -> bool {
        if a.key != b.key { return a.key < b.key; }
        row_cmp(schema, &sources[a.source_idx], a.row,
                        &sources[b.source_idx], b.row) == Ordering::Less
    };
    drive_merge(
        heap, less,
        |src| {
            states[src].advance(&sources[src]);
            if states[src].is_valid() {
                Some((sources[src].get_pk(states[src].position), states[src].position))
            } else {
                None
            }
        },
        |a_src, a_row, b_src, b_row| {
            row_cmp(schema, &sources[a_src], a_row,
                            &sources[b_src], b_row) == Ordering::Equal
        },
        |src, row| sources[src].get_weight(row),
        |gs, gr, gk, nw| {
            *c_key    = gk;
            *c_weight = nw;
            *c_idx    = gs;
            *c_row    = gr;
            *c_null   = sources[gs].get_null_word(gr);
            found = true;
            ControlFlow::Break(())
        },
    );
    self.valid = found;
}
```

`less`, `eq_payload`, and `weight` capture `&sources` (immutable).
`advance` captures `(&sources, &mut states)` — two immutable borrows of
`sources` coexist fine; `&mut states` is a disjoint field.
`emit` captures `&sources` (immutable) and six `&mut self.*` refs, all
disjoint from `states`.  No allocations; no intermediate slot.

### Step 5 — Verify and clean up

- Correctness: existing `storage::compact::tests`, `storage::merge::tests`,
  `storage::read_cursor::tests` cover the contract without change.
- Performance: `benchmarks/sal_merge.rs` and `benchmarks/read_cursor_scan.rs`
  (if present) must be within ±2% of the pre-refactor baseline.  The
  monomorphization chain — `drive_merge<RowCmp=compare_rows_int_nonnull>` vs
  `drive_merge<RowCmp=compare_rows>` — still produces two specializations per
  site; LLVM must inline through the closure wrappers as before.
- Net LOC: target ~80 lines deleted across the three sites, ~60 lines added
  in `heap.rs`, net ~20 lines down.

## What this does NOT do

- Does not change the loser-tree algorithm in `LoserTree`.
- Does not unify the `build_tree` step — each site builds its heap from a
  different cursor type; the build closures stay site-local.
- Does not change the comparator monomorphization discipline.  Each of the
  three call sites still wraps `drive_merge` in two outer functions
  (`*_int_nonnull` / generic), so six specialised copies are emitted as today.

## Risks

- **`HeapNode` API churn.** `replace_top_key` is called at six points (two
  specializations × three files) plus the heap unit tests.  All must be
  updated atomically; a partial update silently compiles with the old call
  since the old function can be kept temporarily.
- **`HeapNode` size.** Adding `row: usize` changes `HeapNode` from 24 bytes
  to 32 bytes.  At 32 bytes two nodes fit exactly per 64-byte cache line —
  better alignment than the previous 24 bytes which could span lines.  The
  loser tree `Vec` is not larger in element count, so this is a net cache
  improvement.
- **Closure inlining.** `drive_merge` takes five generic parameters.
  LLVM monomorphizes through them when the function is `#[inline(always)]`, but
  the chain must not be broken by an intermediate non-inline wrapper.  Confirm
  with a before/after perf comparison on the merge micro-bench before
  declaring the refactor done.

## Acceptance

- `make test` green.
- Storage micro-benchmarks within ±2% of pre-refactor baseline on the
  merge / compact / scan paths.
- `git diff --stat` net negative on `crates/gnitz-engine/src/storage/`.
