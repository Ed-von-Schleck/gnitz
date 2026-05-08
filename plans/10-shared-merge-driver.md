# Refactor: shared k-way merge driver across compact, merge, and read_cursor

## Background

After the post-Plan-3 simplification pass, the same outer-group / inner-fold
loop now exists in three near-identical copies:

- `storage/compact.rs::open_and_merge_inner` — folds `ShardCursor`s over
  `MappedShard` sources; emits via a free `emit(key, weight, shard, row)`
  callback.
- `storage/merge.rs::merge_batches_inner` — folds `MemBatchCursor`s over
  `SortedMemBatch` sources; emits via `writer.write_row(batch, row, weight)`.
- `storage/read_cursor.rs::drive_with` — folds `ReadCursorEntry`s over
  `CursorSource`s; emits by writing to `self.current_*` and returning to the
  caller after each non-ghost group.

The loop body is structurally identical in all three:

1. Open a group from the heap root: read `peek().source_idx`, save
   `(group_src, group_row, group_key)`.
2. Inner fold: pop tied entries by repeatedly comparing
   `(cur_key, row_cmp)` against the saved exemplar; on match,
   accumulate weight, advance the cursor, and either `replace_top_key`
   (cursor still valid) or `pop_top` (cursor exhausted).
3. After the inner loop, if `net_weight != 0`, emit the group.

The `less` closure is rebuilt verbatim at each heap operation in all
three sites — same shape, same captures (cursor slice, schema, row_cmp).

### Why this wasn't done in /simplify

The three sites differ on three axes:

- **Cursor type** (`ShardCursor` / `MemBatchCursor` / `ReadCursorEntry`).
- **Source array** (`&[MappedShard]` / `&[SortedMemBatch]` / inlined in
  `ReadCursorEntry`).
- **Emit shape** (free callback / writer method / `current_*` write +
  early-return after one group).

Items 1 and 2 are easy generics. Item 3 is the structural blocker:
read_cursor returns to the caller after every non-ghost group, so its
"emit" is `self.current_* = ...; return;` rather than a callback. A
naive `FnMut(...)` extraction would force read_cursor to either pay a
callback indirection on the hottest scan path or use a sentinel return
value to short-circuit.

The driver also has to preserve the existing comparator monomorphization
discipline (`compare_rows_int_nonnull` vs `compare_rows`, dispatched at
the outer wrapper). Each of the three sites currently lives behind two
specialised inner functions — six total. A shared driver must continue
to produce two specialisations per emit shape (or accept one extra
indirection).

## The change

### Phase 1 — define the driver in `storage/heap.rs`

Add a single generic loop that takes everything site-specific as
arguments. The driver itself never references concrete types.

```rust
/// Drive an N-way merge to completion, folding rows tied on
/// `(key, payload)` into one logical group per emit.
///
/// `peek_key(src)` reads the current key from cursor `src`.
/// `advance(src) -> Option<u128>` advances cursor `src` and returns
///   the cursor's new peek key, or `None` when exhausted.
/// `eq_payload(a_src, b_src) -> bool` returns whether two cursor
///   positions tie under payload comparison.
/// `weight(src) -> i64` reads the current weight at cursor `src`.
/// `emit(group_src, group_row, group_key, net_weight) -> Flow` is
///   called once per group with non-zero net weight; `Flow::Stop`
///   makes the driver return immediately (used by read_cursor).
pub enum Flow { Continue, Stop }

#[inline]
pub fn drive_merge<PK, ADV, EQ, W, EM>(
    heap: &mut MergeHeap,
    less: impl Fn(&HeapNode, &HeapNode) -> bool + Copy,
    mut peek_key: PK,
    mut advance: ADV,
    mut eq_payload: EQ,
    mut weight: W,
    mut emit: EM,
) where
    PK: FnMut(usize) -> u128,
    ADV: FnMut(usize) -> Option<u128>,
    EQ: FnMut(usize, usize) -> bool,
    W: FnMut(usize) -> i64,
    EM: FnMut(usize, usize, u128, i64) -> Flow,
{ ... }
```

The closure params keep all per-site state (cursor slices, source
arrays, schema, row_cmp) inside the closures, so the driver stays
generic only over their function types.

The `less` closure that goes into `replace_top_key` / `pop_top` is the
same closure the driver already requires for the heap operations, so
there is no per-iteration rebuild — the comment chain about
"closure-rebuilt-each-iteration-because-borrow-conflict" goes away.
The conflict was specific to having `entries: &mut Vec<_>` and a
`less` closure capturing `&entries` simultaneously; with the driver
owning the closures, the cursor mutation moves into the `advance`
callback, which the driver invokes between heap ops.

### Phase 2 — port `merge_batches`

Smallest of the three sites; least state to thread.

```rust
fn merge_batches_inner<RowCmp>(
    cursors: &mut [MemBatchCursor],
    batches: &[SortedMemBatch],
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    row_cmp: RowCmp,
) where RowCmp: Fn(...) -> Ordering + Copy {
    let mut tree = build_tree(cursors, batches, schema, row_cmp);
    drive_merge(
        &mut tree,
        |a, b| less(a, b, cursors, batches, schema, row_cmp),
        |src| cursors[src].peek_key(batches),
        |src| { cursors[src].advance();
                cursors[src].is_valid().then(|| cursors[src].peek_key(batches)) },
        |a, b| row_cmp(schema, &batches[cursors[a].batch_idx].0, cursors[a].position,
                       &batches[cursors[b].batch_idx].0, cursors[b].position) == Ordering::Equal,
        |src| batches[cursors[src].batch_idx].get_weight(cursors[src].position),
        |group_src, group_row, _key, w| {
            writer.write_row(&batches[cursors[group_src].batch_idx], group_row, w);
            Flow::Continue
        },
    );
}
```

The `cursors` slice is mut-borrowed inside the `advance` closure but
read-only inside the others. Rust 2021 disjoint-closure-captures handles
this: each closure captures only what it needs.

Verify no perf regression with `merge_consolidate` micro-bench
(`benchmarks/merge_consolidate.rs`) — must match within noise.

### Phase 3 — port `compact.rs`

Same shape, but `cursors[src]` mutation calls `cursor.advance(shard)`
which needs both `&mut cursors[src]` and `&shards`. Should reduce to
roughly the same closure forms.

The `emit` callback returns `Flow::Continue` always — compact is a
full drain.

### Phase 4 — port `ReadCursor::drive_with`

The only site whose emit isn't `Continue`. After committing a non-ghost
group to `self.current_*`, return `Flow::Stop` so the driver returns,
matching the existing "emit one group then yield to caller" semantics.

This is the awkward case: the emit closure needs `&mut self` to write
the `current_*` fields, while the heap (`self.mode → Multi(heap)`) and
entries (`self.entries`) are simultaneously borrowed by the other
closures. Two options:

1. **Refactor `ReadCursor` so the emit closure writes to a local
   `EmitSlot`, not `self.current_*`.** After the driver returns,
   commit the slot to `self`. Cleanest but adds a copy.
2. **Use raw pointers / split_borrow.** The fields are disjoint, so
   `unsafe { &mut *(self as *mut ReadCursor).field }` would compile,
   but degrades the safety story.

Prefer option 1; the slot is 4×8 bytes (key, weight, null_word, idx,
row), copy is negligible vs the per-group work.

### Phase 5 — verify and clean up

- Tests: existing `storage::compact::tests`, `storage::merge::tests`,
  `storage::read_cursor::tests` cover the contract.
- Bench: `benchmarks/sal_merge.rs`, `benchmarks/read_cursor_scan.rs`,
  `benchmarks/compact.rs` (if present) — within noise.
- Net LOC: target ~90 lines deleted across the three sites,
  ~50 lines added in `heap.rs`, net ~40 lines down.

## What this does NOT do

- Does not change `MergeHeap` itself (Plan 3 already shipped).
- Does not change comparator monomorphization. Each of the three
  call sites still wraps the driver in two outer functions
  (`*_int_nonnull` / generic), so we still emit two specialised
  copies per site (six total). The body is the driver, but each copy
  inlines it under its own `row_cmp` type.
- Does not unify the `build_tree` step. Each site builds its heap
  from a different cursor type; the build closures stay site-local.

## Risks

- **Closure overhead.** Five closures passed by value to a generic
  function; LLVM should inline them but this is unverified in our
  setup. If profiling shows extra branches, fall back to a trait-based
  interface (one trait, three impls).
- **Read-cursor borrow choreography.** Phase 4's `EmitSlot` adds a
  copy; if benchmarks regress, revert to inline-only and accept the
  duplication on this one site.
- **Comparator inlining.** Each driver instantiation must monomorphize
  through the closures. If the `compare_rows_int_nonnull` fast path
  stops inlining (`#[inline]` chain breaks), perf regresses globally.
  Regression-test with `compare_rows_int_nonnull` benchmark before
  declaring done.

## Acceptance

- `make test` green.
- Storage micro-benchmarks within ±2% of pre-refactor baseline on
  the merge / compact / scan paths.
- `git diff --stat` net negative on `crates/gnitz-engine/src/storage/`.
