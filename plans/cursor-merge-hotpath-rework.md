# Cursor / merge hot-path rework

Three independent changes to the N-way merge + cursor machinery, each justified
by a committed microbenchmark (`gnitz-engine/src/bench_cursor.rs`). Run the
baseline with:

```text
cargo test -p gnitz-engine --release bench_cursor:: -- --ignored --nocapture --test-threads=1
```

Baseline (U64 PK, 500k rows, min-of-9):

| Signal | Number | Reads as |
|---|---|---|
| scan ns/row by k | k1 **8.3** → k2 **19.5** → k4 20.0 → k8 21.1 → k16 21.6 | k1→k2 is a 2.3× cliff; flat in k after ⇒ **not compare-bound**, fixed heap cost |
| scan ns/row by width | U64 **19.4** vs U128 **19.6** | the u128 key is free in cycles today |
| forward seek gallop vs rebuild | 10.3× @100% sel, 4.3× @25%, 3.0× @6%, 2.5× @1.6% | the landed in-place forward seek; no further work |
| cogroup membership vs cursor-free floor | **4.3–4.9×**, flat across fan-out | per-row `advance()` overhead in the group walk |

The three phases are independent (different code regions) and may land in any
order. Recommended sequence by risk: **A** (isolated, ~4.5× on cogroup) → **C**
(isolated, ~2× on the dominant k=2 shape) → **B** (cross-cutting simplification,
perf-neutral by construction).

## Invariants (all phases must preserve)

- **OPK order.** Every PK region at rest holds order-preserving big-endian bytes
  (`columnar.rs:137`): `compare_pk_bytes(a,b) == a.cmp(b)` equals typed PK order
  at every width, signedness, and arity. The heap never needs to know column
  types — byte order *is* the order.
- **Consolidation.** A merge emits one row per distinct `(PK, payload)` with
  summed weight; net-zero groups are dropped (`heap.rs:282`). `eq_payload` must
  keep folding only exact `(PK-bytes, payload)` matches.
- **Oracle.** `assert_advance_to_matches_seek_oracle` (`read_cursor.rs` tests)
  pins every `advance_to` landing — including emitted weight — against a
  from-scratch `seek_bytes`. Any heap/cursor change must keep it green, plus the
  `merge.rs` / `compact.rs` property tests against their sorted references.

---

## Phase A — `walk_pk_group`: monomorphized equal-PK group walk

### Problem

The cogroup callbacks walk a trace match-group one `ReadCursor::advance()` at a
time. `group_has_positive` (`read_cursor.rs:748`):

```rust
while self.valid && self.current_pk_eq(key) {
    if self.current_weight > 0 { present = true; }
    self.advance();
}
```

and the inner-join cartesian (`join.rs:151`):

```rust
while m.valid && m.current_pk_eq(key) {
    let w_trace = m.current_weight;
    for i in range.clone() { /* write_join_row */ }
    m.advance();
}
```

Per trace row this pays: the `is_pk_unique` + `with_payload_cmp!` dispatch
(re-selected every call), a full `drive_single`/`drive_*` that commits all five
`current_*` fields (`current_key` via `widen_pk_be`, `current_null_word`,
`current_entry_idx`, `current_row`, `current_weight`), and a `current_pk_eq`
byte compare. A membership test reads only `current_weight`'s sign — the
`current_key` recompute and four other commits are dead. Benchmark
`bench_cogroup_group_walk`: **4.3–4.9× over a cursor-free floor**, flat across
fan-out (so the overhead is per-row, not per-group).

### Design

A generic group walk that dispatches **once** and emits only the weight (plus,
for the cartesian variant, the live position so the callback can read columns),
skipping the per-row `current_*` materialization.

```rust
// read_cursor.rs, on ReadCursor.

/// Walk the equal-`key` PK group from the current position to its end, invoking
/// `f` with each row's net weight. Dispatches the comparator once; the inner
/// loop is the monomorphized drive with a lean emit (no `current_*` commit).
/// On return the cursor sits at the first row past the group (same postcondition
/// as the old `while current_pk_eq { advance }`).
fn walk_pk_group<F: FnMut(i64)>(&mut self, key: &[u8], f: F);

/// Membership specialization, replacing the body of `group_has_positive`.
pub fn group_has_positive(&mut self, key: &[u8]) -> bool {
    let mut present = false;
    self.walk_pk_group(key, |w| present |= w > 0);
    present
}
```

`walk_pk_group` dispatches like `drive` does, then loops the *monomorphized*
advance without leaving the dispatch:

```rust
fn walk_pk_group<F: FnMut(i64)>(&mut self, key: &[u8], mut f: F) {
    if self.is_pk_unique {
        self.walk_pk_group_pk_unique(key, &mut f);
    } else {
        with_payload_cmp!(self.schema, Self::walk_pk_group_with, self, key, &mut f);
    }
}
```

For `SourceMode::Single` the inner loop is a tight position scan reading only
`get_weight`/`get_pk_bytes` (the floor's cost). For `Multi` it is `drive_merge`
with an `emit` that records the net weight and breaks the *group*, not the whole
cursor — i.e. the existing `drive_inner` loop with a lean emit and a
"`while same PK`" outer bound instead of a single-group break.

For the **cartesian** consumer the callback needs column access per trace row,
so add a sibling that yields the live cursor between advances:

```rust
/// Like `walk_pk_group` but invokes `f(&*self)` at each row so the callback can
/// read the current trace row's columns (e.g. `write_join_row`). `f` must not
/// re-enter the cursor.
fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], f: F);
```

`f(&*self)` borrows `self` immutably for the call, which ends before the
internal advance re-borrows `&mut self` — sequential, no conflict. `current_*`
is still committed in this variant (the callback reads columns through it), but
the dispatch is hoisted out of the loop.

### Consumers to convert

- `op_semi_join_delta_trace` (`join.rs:100`) — `group_has_positive` (already
  routed through the new body).
- `op_anti_join` / the left-semi at `join.rs:50` — `!group_has_positive`.
- `op_join_delta_trace` (`join.rs:151`) — replace the `while … advance` with
  `for_each_pk_group_row`.

### Non-goals

`distinct.rs:42` and the reduce drivers (`op_reduce.rs`, `op_gather.rs`) do **not**
walk for membership — they *interleave* the cursor group against delta rows by
payload (`compare_cursor_payload_to_batch_row`, selective `advance`). That needs
random access into the group (a materialized group view), not a forward fold;
it is out of scope here and not converted.

### Steps

1. Add `walk_pk_group` + `walk_pk_group_pk_unique` + `walk_pk_group_with` and
   reduce `group_has_positive` to call it. The `Multi` path reuses `drive_inner`
   with a lean `emit` (record weight, `ControlFlow::Continue` until PK changes).
2. Add `for_each_pk_group_row`; convert `op_join_delta_trace`.
3. Confirm `write_join_row` takes `&ReadCursor` (read-only). If it takes
   `&mut`, narrow it — it only reads columns.

### Validation

- All existing join/semi/anti/distinct tests green.
- `bench_cogroup_group_walk` `current` column drops toward the `floor` column
  (target: ≤ 2× rather than 4.5×).

---

## Phase B — eliminate the universal u128 heap key

### Problem

`HeapNode { key: u128, source_idx: usize, row: usize }` (`heap.rs:28`) is 32
bytes; `key` is `pack_pk_be(pk_bytes)` (`merge.rs:391`) — the first ≤16 OPK
bytes loaded big-endian — cached so a heap compare is a register `cmp` instead
of the runtime-length `compare_pk_bytes` (`= a.cmp(b)`, a memcmp,
`columnar.rs:146`). It is odd on four counts:

1. **Redundant** with the OPK bytes already stored in each source's PK region.
2. **Oversized** — fixed 16 bytes when the dominant PK is 8; u128's 16-byte
   alignment pads `HeapNode` to 32 regardless of the int fields.
3. **Recomputed** every advance (`get_pk_prefix`/`peek_key` → `pack_pk_be`:
   copy + bswap), re-reading the bytes it caches.
4. **Incomplete** — for stride > 16 it is only a *prefix*, so the comparators
   fall back to `compare_pk_bytes` anyway. It induces the entire narrow/wide
   apparatus (`pk_cmp_after_key_tie`, `pk_is_wide()` branches, the dual
   `current_key_from_bytes`).

The benchmark shows it is **free in cycles** (U64 == U128, flat in k ⇒ not
compare-bound). So this phase is a **simplification with a perf-neutrality
constraint**, not a speedup: delete the key and the width matrix without
regressing `bench_merge_scan`.

### Design: keyless node + stride-dispatched OPK comparator

```rust
// heap.rs
#[derive(Clone, Copy)]
pub struct HeapNode {
    pub source_idx: u32,   // was usize; sources are u32-bounded (drain already casts)
    pub row: u32,          // was usize; rows-per-source < 2^32
}
// 8 bytes. Sentinel: source_idx == u32::MAX.
```

The node carries no key. The comparator reads OPK bytes through the sources it
already captures, and — to keep compares register-cheap where the cached key
did — is selected by stride **once** per drive, mirroring `with_payload_cmp!`:

```rust
// PK ordering primitive, chosen by stride at drive entry. The 8/16 arms load
// the OPK bytes big-endian into a register (what the key cached); other strides
// (compound / >16) use compare_pk_bytes, which they already did on the wide
// path. `lt` and `eq` share the same dispatch.
match schema.pk_stride() {
    8  => /* u64::from_be_bytes  cmp/eq */,
    16 => /* u128::from_be_bytes cmp/eq */,
    _  => /* compare_pk_bytes (dynamic memcmp) */,
}
```

Because OPK byte order is total, **one comparator shape** serves every width:
`pk_lt(a,b)` then, on PK tie, the payload `row_cmp` (non-PkUnique) or nothing
(PkUnique). `pk_cmp_after_key_tie` and every `pk_is_wide()` branch *inside the
comparators* disappear — the stride arm subsumes them.

### `drive_merge` change (`heap.rs:221`)

`drive_merge` uses `top.key` for the group boundary (`cur_key != group_key`,
`heap.rs:273`) and passes `group_key` to `emit`. Replace both:

- Add a `same_pk(a_src, a_row, b_src, b_row) -> bool` closure param (the
  stride-dispatched PK-byte equality); the boundary becomes
  `!same_pk(group_src, group_row, cur_src, cur_row) || !eq_payload(...)`. Keep
  `eq_payload` for the payload term (or narrow it to payload-only since
  `same_pk` now gates the PK term).
- Drop `group_key` from the `emit` signature. Audit the three emit closures:
  `read_cursor.rs` `drive_inner` ignores it (`|gs, gr, _gk, nw|`); confirm
  `merge_batches_inner` and `open_and_merge_inner` derive their output PK from
  `(src, row)` (via `get_pk_bytes`) rather than the passed key, and adjust.

`walk_up`/`replace_top`/`pop_top` lose the `new_key` parameter — they shuffle
`{src,row}` only; comparisons go through the caller's `less`.

### The three drivers

All build a `LoserTree` and call `drive_merge` with a `less` of the same
u128-key shape; all are converted identically:

- `read_cursor.rs`: `heap_less_with`, `drive_inner`, `drive_with`,
  `drive_pk_unique`, `build_tree_with`, and `seek_phase`/`gallop_heap_forward`.
  `seek_phase` (`read_cursor.rs:647`) currently packs the target with
  `pack_pk_be(key)` and compares `top.cmp(&target)`; keyless, it compares the
  root's OPK bytes to `key` via the stride arm (`pk_cmp(get_pk_bytes(root), key)`),
  which also *removes* the `is_wide` re-check in that loop.
- `merge.rs`: `merge_run`'s `less` (`merge.rs:460`) and `merge_batches_inner`
  (`merge.rs:492`); delete `MemBatchCursor::peek_key`.
- `compact.rs`: `open_and_merge_inner` (`compact.rs:229`) and its two `less`
  closures (`compact.rs:171,201`).

### Deleted vs kept

- **Deleted:** `HeapNode.key`; `pack_pk_be`, `pk_sort_key` (`merge.rs:391,404`);
  `get_pk_prefix` (`read_cursor.rs:191`); `peek_key`; `pk_cmp_after_key_tie`;
  the `node.key` plumbing in `walk_up`/`replace_top`/`pop_top`/`build`.
- **Kept:** the public `current_key` field and `current_key_from_bytes`
  (`read_cursor.rs:773`) — consumer-facing (catalog readers do `current_key as
  u64`), unrelated to the heap key. The `pk_is_wide()` calls *outside* the
  merge (catalog, exchange, reduce) are untouched.

### Why keyless over a generic `LoserTree<K>`

A right-sized `HeapNode<K>` (K = u32/u64/u128 by stride) also shrinks the node
and drops the tie-break for the common case, but the tree is a **stored field**
(`SourceMode::Multi(LoserTree)`), so a runtime-chosen `K` forces an
`enum { U32(LoserTree<u32>), U64(…), U128(…) }` and a per-drive variant match —
it *relocates* the width split into the stored type. Keyless keeps the stored
tree monomorphic (`Vec<HeapNode>`, 8-byte nodes) and stride-specializes only the
**transient** comparator at drive entry, exactly where `with_payload_cmp!`
already specializes. It is a net deletion, not a relocation.

### Steps

1. `heap.rs`: drop `key` from `HeapNode` (→ `{u32,u32}`); update `SENTINEL_NODE`,
   `build`, `walk_up`, `replace_top` (drop `new_key`), `pop_top`, `peek`. Update
   the `heap_node_is_32_bytes` guard test to `_is_8_bytes`.
2. `heap.rs`: `drive_merge` — add `same_pk` param, drop `group_key` from `emit`,
   boundary via `same_pk`.
3. Add the stride-dispatched `pk_lt`/`pk_eq` selection (a small macro or a
   `match pk_stride` helper returning the two closures, 8/16 fast arms + dynamic).
4. Convert the three drivers (read_cursor, merge, compact) and `seek_phase`.
5. Delete `pack_pk_be`/`pk_sort_key`/`get_pk_prefix`/`peek_key`/
   `pk_cmp_after_key_tie`.

### Validation

- Oracle + `merge.rs`/`compact.rs` property tests green; full suite green.
- **Neutrality gate:** `bench_merge_scan_by_k` and `_pk_width` must not regress
  vs the committed baseline (k2 ≈ 19.5, U128 ≈ 19.6). If the dynamic-stride arm
  shows up for 8/16, the fast arms are mis-wired.
- `heap_node_is_8_bytes` asserts the node shrank.

---

## Phase C — Pair (k = 2) heap bypass

### Problem

`bench_merge_scan_by_k`: k1 **8.3** → k2 **19.5** ns/row — a 2.3× cliff when the
loser tree engages, then *flat* to k16. The ~11 ns/row is the heap's fixed
per-emit cost (build, `walk_up`, `replace_top`/`pop_top`), independent of k. The
DBSP hot shape is one delta against a compacted trace of very few segments, so
**k=2 is the common multi-source case** and pays full heap price for a merge a
single branch can do.

### Design

A `SourceMode::Pair` bypassing the tree, analogous to the existing `Single`
bypass (`read_cursor.rs:924`):

```rust
enum SourceMode { Empty, Single, Pair, Multi(LoserTree) }
```

`new()` (`read_cursor.rs:487`) selects `Pair` for `sources.len() == 2`. The
Pair driver holds two head positions and merges by one PK compare (the same
stride-dispatched `pk_lt`/payload `row_cmp` as the heap, post-Phase B), folding
equal `(PK,payload)` and skipping ghosts — no `walk_up`, no allocation. Forward
`advance_to` on a Pair gallops both heads directly (no tree to maintain),
replacing the `gallop_heap_forward` path for k=2.

`drive`/`advance`/`advance_to`/`drain_*` gain a `Pair` arm beside `Single` and
`Multi`. The comparator is selected once (Phase B's stride dispatch), so the
Pair loop is branch-free per row.

### Steps

1. Add the `Pair` variant and `new()` selection.
2. `drive_pair_with` / `drive_pair_pk_unique`: 2-head merge + consolidate +
   ghost-skip, committing `current_*` like `drive_single`.
3. `advance_to` Pair arm: gallop both heads via `CursorState::advance_to`,
   then `drive_pair`.
4. Route `drain_*` and `estimated_length` through the new arm.

### Validation

- Oracle tests already cover k=2 (`advance_to_*` use two batches); extend the
  scan tests to assert Pair and Multi(k=2) emit identically.
- `bench_merge_scan_by_k` k2 drops from ~19.5 toward k1 (~8–12 ns/row target).

---

## Risks

- **Phase B neutrality (primary risk).** Removing the cached key shifts the PK
  compare from a register op to a per-compare byte load; the 8/16 stride arms
  must keep it register-cheap or the not-compare-bound property breaks. The
  neutrality gate above catches a regression before merge.
- **Phase B blast radius.** Three drivers + the heap change together; land it as
  one change behind the property/oracle tests rather than per-driver (the heap
  signature change forces all three at once).
- **`emit` group_key audit.** `merge_batches_inner`/`open_and_merge_inner` must
  not depend on the passed `group_key`; verify before dropping it.
- **Phase C dispatch creep.** A fourth `SourceMode` arm touches every dispatch
  site; keep the Pair driver a faithful 2-way of the Multi comparator so the
  oracle equivalence holds by construction.
