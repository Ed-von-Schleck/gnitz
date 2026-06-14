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
(isolated, ~2× on the frequent k=2 shape) → **B** (cross-cutting simplification,
perf-neutral *on the heap path*).

The ordering interacts with B's neutrality measurement: once **C** lands, k=2
no longer touches the loser tree (it takes the `Pair` bypass), so a post-C
`bench_merge_scan_by_k` k2 number measures the Pair path, not the heap. B's
neutrality gate must therefore read the **k≥4** rows (which always exercise the
tree) regardless of merge order; `bench_merge_scan_pk_width` already runs at k=4.

The committed bench's doc comments still use the pre-rename labels
(`fold_pk_group`, "Rung 1/2/3"); align them to `walk_pk_group` / "Phase A/B/C"
when Phase A lands.

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

**Fold granularity (correctness).** A single PK group may hold several payload
sub-groups (same PK, different payloads), each consolidated separately. The
`while current_pk_eq { advance }` loops being replaced iterate *emitted*
(consolidated, non-ghost) rows — so they observe one row per `(PK, payload)`
sub-group with that sub-group's net weight, and `group_has_positive` ORs `w > 0`
across them. `walk_pk_group` must reproduce exactly this: invoke `f` once per
non-ghost `(PK, payload)` sub-group with its net weight (the `drive_merge` fold),
**not** once per raw row and **not** with the whole PK group summed into one
number. Summing the PK group is a silent bug: payload A at net `+1` and payload B
at net `−1` sum to 0 (→ `group_has_positive` false) when A is in fact present.
The `Multi` lean fold therefore reuses `drive_merge`'s per-`(PK,payload)`
boundary, breaking the *outer* walk only when the PK changes.

### Exit postcondition

Both walks must leave the cursor exactly where the `while current_pk_eq { advance }`
loop they replace did: sitting on the first row past the group with **every**
`current_*` field committed (`valid`, `current_key`, `current_weight`,
`current_null_word`, `current_entry_idx`, `current_row`), or `valid == false` at
end of source. Downstream reads it directly — `cogroup`'s `key()` reads `valid` +
`current_pk_bytes()` (i.e. `current_entry_idx`/`current_row`) before its next
`advance_to`, and the next group's loop guard re-reads `current_pk_eq`. A walk
that skips this commit corrupts the following group.

Get the postcondition by making the walk's **terminating step an ordinary
committing drive**, not a hand-rolled re-peek: in `Single` the scan ends by
calling `drive_single` once the position leaves the group; in `Multi` the lean
fold loop, on reaching a root whose PK ≠ `key`, performs one standard
`drive_inner` (fold + commit) for that boundary-crossing group and returns. That
group is folded exactly as the old final `advance()` folded it, so
`current_weight` holds the group's *consolidated net weight*.

Do **not** materialize the exit state by peeking the heap root and reading its
weight: the root is only the next group's *exemplar*, and its true
`current_weight` requires the full `drive_merge` tie-fold over the group's rows —
a raw peek yields the wrong weight for any multi-row group.

### Consumers to convert

Every site that walks an equal-PK trace group forward via
`while m.valid && m.current_pk_eq(key) { … m.advance() }` (or its
`group_has_positive` shorthand). The cogroup callbacks are monomorphized to a
concrete `&mut ReadCursor` (the `SortedKeyStream` trait carries only
`key`/`advance_to`), so the new methods are plain `ReadCursor` methods — no trait
change, and `write_join_row` already takes `&ReadCursor` (read-only).

- `op_semi_join_delta_trace` (`join.rs:100`) — `group_has_positive` (already
  routed through the new body).
- `op_anti_join` / the left-semi at `join.rs:50` — `!group_has_positive`.
- `op_join_delta_trace` (`join.rs:151`) — replace the `while … advance` with
  `for_each_pk_group_row`.
- `op_join_delta_trace_outer` (`join.rs:205`) — the **same** trace-major
  cartesian walk as the inner join, plus a `matched` boolean. Convert to
  `for_each_pk_group_row` with the callback folding `matched |= w_trace > 0`
  alongside the product (`w_trace` read from the yielded `&ReadCursor`). Omitting
  it leaves the LEFT-join hot path on the slow per-row walk — it is in scope.
- `op_reduce.rs` MIN/MAX history reads (`while … current_pk_eq { push_current_row;
  advance }` at `op_reduce.rs:510` *group_by_pk* and `:533` *GI-driven*) — these
  push the whole equal-PK group with no payload interleave, so they fit
  `for_each_pk_group_row` (`|c| c.push_current_row(&mut trace_rows)`). These are
  in scope (convert) or an explicit deferral — they are *not* in the Non-goals
  below, which covers only the genuinely-interleaved reduce paths.

### Non-goals

These are **not** forward equal-PK folds and stay as-is:

- `distinct.rs:45` — *interleaves* the cursor group against delta rows by payload
  (`compare_cursor_payload_to_batch_row`, selective `Less`/`Equal`/`Greater`
  advance). Needs random access into the group (a materialized view), not a
  forward fold.
- `op_gather.rs:78` and `op_reduce.rs:401,449` — `current_pk_eq` is a **point**
  predicate (`has_old = valid && current_pk_eq`), not a loop.
- `op_reduce.rs:308` — a full `rewind`-then-`while valid` trace scan routing each
  row to a group by hash; not equal-PK-bounded.
- `op_join_delta_trace_range` (`join.rs:325,393`) — walks ordered `[start, end)`
  *ranges*, not equal-PK groups; a different access pattern (`range_per_row_seek`
  / `range_merge_walk`), out of scope for the equal-PK `walk_pk_group`.

### Steps

1. Add `walk_pk_group` + `walk_pk_group_pk_unique` + `walk_pk_group_with` and
   reduce `group_has_positive` to call it. The `Multi` path reuses `drive_inner`
   with a lean `emit` (record weight, `ControlFlow::Continue` until PK changes);
   the boundary-crossing group is folded by one ordinary committing `drive_inner`
   so the exit postcondition above holds.
2. Add `for_each_pk_group_row`; convert `op_join_delta_trace` and
   `op_join_delta_trace_outer` (the latter folds `matched` in its callback).
3. `write_join_row` is already `&ReadCursor` (`join.rs:488`) — no change.
4. Convert (or explicitly defer) the `op_reduce.rs:510,533` history reads.
5. Rename the `bench_cursor.rs` doc comments off `fold_pk_group`/"Rung".

### Validation

- All existing join/semi/anti/outer/distinct tests green.
- Assert that after `group_has_positive(key)` (and after a `for_each_pk_group_row`
  walk) the cursor's `current_*` equal a from-scratch `seek` to the first row with
  PK ≠ `key` — this is what catches a wrong exit weight from a naive root-peek.
- Add a same-PK / multi-payload case (one payload nets positive, another nets
  negative) so a "sum the whole PK group" regression in `walk_pk_group` fails.
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
// use compare_pk_bytes (dynamic memcmp). `lt` and `eq` share the same dispatch.
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

Two arms outside `{8,16}` carry a cost the neutrality gate (U64/U128 only) will
not see, both acceptable but worth stating:

- **Stride 1/2/4** (U8/U16/U32 single-scalar PKs) falls to the dynamic `_` arm —
  a length-checked `&[u8]::cmp` rather than today's cached `pack_pk_be` register
  compare. Short slices, rare types; if it ever matters, add a `<=8`→u64
  left-pad arm. It is a (small) regression, not a wiring bug.
- **Stride > 16** (compound ≥3 columns) loses the cached 16-byte prefix
  fast-reject: every compare is now a full-width `compare_pk_bytes`, where today
  most compares settle on the u128 prefix and only prefix-collisions pay the full
  walk. Wide PKs are uncommon; accept it.

The compared bytes come from `get_pk_bytes(src, row)` — an inlined two-arm
`CursorSource` match, the same path today's wide tie-break already takes — read
directly from each source's PK region. `UnifiedSource` is **not** involved: it is
the column-scatter accessor, orthogonal to ordering, and stays a lazily-built
`OnceCell` (forcing it unconditional would add `to_unified` cost to every
seek-only cursor).

**The cost the neutrality claim rides on.** A compare was "read a `u128` field
already resident in the (cache-hot) node"; it becomes "two `CursorSource`
matches + two slice constructions + two integer loads", evaluated once per
`walk_up` level (≈`log k`) plus at each `drive_merge` group boundary. The
keyless **advance** closure conversely *saves* one `get_pk_prefix` (the key
recompute: `get_pk_bytes` + `pack_pk_be`) per row. Net is empirical — the
benchmark's "not compare-bound, flat in k" is the reason to expect it to wash —
but the path most exposed is narrow **PkUnique** (`drive_pk_unique`'s narrow arm,
today the cheapest possible `a.key < b.key`), which is the base-table read path.
That arm, at k≥3, is where a neutrality regression would first show; treat a
failure there as possibly inherent, not necessarily "fast arms mis-wired".

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
- `merge_and_route` (the `open_and_merge` consumer in `compact.rs`) routes each
  row to a shard via `find_guard_for_key(guard_keys, group_key)`. With
  `group_key` gone from `emit`, recompute the routing key inside the closure from
  the row bytes it already fetches:
  `find_guard_for_key(guard_keys, pack_pk_be(pk_bytes))`. `guard_keys: &[u128]`
  is unchanged — this is one of the reasons `pack_pk_be` is kept (below).

`walk_up`/`replace_top`/`pop_top` lose the `new_key` parameter — they shuffle
`{src,row}` only; comparisons go through the caller's `less`.

### The three drivers

All build a `LoserTree` and call `drive_merge` with a `less` of the same
u128-key shape; all are converted identically. Because one stride-dispatched
comparator now serves every width, the existing **narrow-vs-wide split collapses
in two of the drivers** — extra deletions, not extra work:

- `read_cursor.rs`: `heap_less_with`, `drive_inner`, `drive_with`,
  `build_tree_with`, and `seek_phase`/`gallop_heap_forward`. **`drive_pk_unique`
  (`read_cursor.rs:867`) loses its `if schema.pk_is_wide()` fork** (`:877`): its
  wide `less`+`eq_payload` (`:879`) and narrow `less` (`:902`, `a.key < b.key`)
  merge into the single stride-dispatched pair, so `gallop_heap_forward`'s
  `is_wide` plumbing (`:629`) and `pk_cmp_after_key_tie` (`:407`) go too.
  `seek_phase` (`:647`) currently packs the target with `pack_pk_be(key)` and
  compares `top.cmp(&target)`; keyless, it compares the root's OPK bytes to `key`
  via the stride arm (`pk_cmp(get_pk_bytes(root), key)`), removing the `is_wide`
  re-check (`:666–671`) in that loop.
- `merge.rs`: `merge_run`'s `less` (`merge.rs:460`) and `merge_batches_inner`
  (`merge.rs:492`); delete `MemBatchCursor::peek_key` (`merge.rs:267`). Already a
  single width-agnostic closure set, so no fork to collapse here.
- `compact.rs`: `open_and_merge_inner` (`compact.rs:229`); `ShardCursor::peek_key`
  (`compact.rs:94`). **`open_and_merge`'s `if schema.pk_is_wide()` selector
  (`compact.rs:147`) collapses**: `open_and_merge_narrow_with` (`:160`) and
  `open_and_merge_wide_with` (`:193`) — today two near-identical closure builders
  — merge into one stride-dispatched builder, deleting one of the two functions
  and the selector branch.

After this, the only surviving `pk_is_wide()` calls are *outside* the merge
(catalog, exchange, reduce/sort, gather, master, compact's width-independent
paths) — those are untouched.

**Lower-churn alternative for the routing key.** Rather than threading the
re-derivation into `merge_and_route`'s callback (and so changing
`open_and_merge`'s public `FnMut(u128, …)` signature, which also touches
`compact_shards`'s `|_key, …|`), recompute the `u128` *inside*
`open_and_merge_inner`'s emit and keep the public callback byte-for-byte:
`emit(pack_pk_be(shard.get_pk_bytes(group_row)), w, shard, group_row)`.
`compact_shards` ignores it as before; `merge_and_route` keeps
`find_guard_for_key(guard_keys, key)`. Either form is correct — the routing key
is `pack_pk_be` of the OPK bytes = today's `group_key`; pick the one with the
smaller diff.

**Defensive cleanup (`merge_and_route`).** While in this function, tighten its
write-loop error path: on a write failure it removes every prior
`out_filenames[..i]`, but a file exists only for guards with `batches[j].count >
0` (the loop `continue`s on empty shards). Today the stray `remove_file`s are
swallowed by `let _ =`, so this is latent, not live corruption — but it can
`unlink` an unrelated file that happens to share a never-written path. Guard it:
  ```rust
  if let Err(e) = batches[i].write_as_shard_with_flags(&cpath, table_id, flags) {
      for (j, fname) in out_filenames.iter().enumerate().take(i) {
          if batches[j].count > 0 {
              let _ = std::fs::remove_file(fname);
          }
      }
      return Err(e);
  }
  ```

### Deleted vs kept

- **Deleted:** `HeapNode.key`; the *heap* call-sites of `pack_pk_be`/`pk_sort_key`
  — `peek_key` (`merge.rs:267`, `compact.rs:94`), `get_pk_prefix`
  (`read_cursor.rs:191`), and `seek_phase`'s `pack_pk_be(key)`;
  `pk_cmp_after_key_tie` (`read_cursor.rs:401`); the in-comparator `pk_is_wide()`
  forks (`read_cursor.rs:407,629,877`; `compact.rs:147`) and the now-redundant
  builder `open_and_merge_{narrow,wide}_with` (one of the two); the `node.key`
  plumbing in `walk_up`/`replace_top`/`pop_top`/`build` (`init_fn` returns the
  row only).
- **Kept:** the `pack_pk_be`/`pk_sort_key` *function definitions* — they have
  non-heap callers and must **not** be deleted: bloom hashing (`memtable.rs`,
  `shard_file.rs` `PkUniqueChecker`), guard routing (`shard_index.rs:301,617` and
  `merge_and_route`'s `find_guard_for_key`), the wide arm of
  `current_key_from_bytes` (`read_cursor.rs:777`), the single-batch sort
  (`SortEntry` in `columnar.rs`, `sort_and_consolidate` `merge.rs:567`), the
  reduce/gather argsort (`ops/reduce/sort.rs`), and the exchange order-cache
  (`ops/exchange.rs`). Phase B removes only their heap usages.
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

1. `heap.rs`: drop `key` from `HeapNode` (→ `{source_idx: u32, row: u32}`, 8
   bytes, sentinel `source_idx == u32::MAX`); update `SENTINEL_NODE`, `build`
   (its `init_fn` returns `Option<u32>` row, no key), `walk_up`, `replace_top`
   (drop `new_key`), `pop_top`, `peek`. Update the `heap_node_is_32_bytes` guard
   test to `_is_8_bytes`. The `u32` fields index `usize`-typed arrays, so every
   `sources[src]`/`states[src]`/`get_pk_bytes(row)` site gains an `as usize`
   (`seek_phase`'s `HeapNode { source_idx: src, row }` destructure at
   `read_cursor.rs:661`, the `less`/`eq_payload` closures in all three drivers).
   `usize`-fields (16 bytes) avoid the casts but only halve the node;
   `u32` (8 bytes, single-register swap in `walk_up`) is worth the casts. Add a
   `debug_assert!(count <= u32::MAX as usize)` at cursor/tree build so the
   rows-per-source < 2³² assumption fails loudly if ever violated.
2. `heap.rs`: `drive_merge` — add `same_pk` param, drop `group_key` from `emit`,
   boundary via `same_pk`.
3. Add the stride-dispatched `pk_lt`/`pk_eq` selection (a small macro or a
   `match pk_stride` helper returning the two closures, 8/16 fast arms + dynamic).
4. Convert the three drivers (read_cursor, merge, compact) and `seek_phase`,
   collapsing the narrow/wide forks (above).
5. Delete the heap-only `get_pk_prefix`/`peek_key`/`pk_cmp_after_key_tie` and the
   `seek_phase` `pack_pk_be(key)` call. **Keep** the `pack_pk_be`/`pk_sort_key`
   *function definitions* — their non-heap callers (bloom, guard routing,
   sort/reduce/exchange, the `current_key_from_bytes` wide arm) remain.

### Validation

- Oracle + `merge.rs`/`compact.rs` property tests green; full suite green.
- **Neutrality gate:** measure on the rows that actually exercise the tree —
  `bench_merge_scan_by_k` **k4/k8/k16** (≈20.0 / 21.1 / 21.6) and `_pk_width`
  (U64 19.4, U128 19.6, both at k4). Do **not** gate on k2: once Phase C lands,
  k2 takes the `Pair` bypass and no longer measures the heap. A regression
  confined to k≥4 narrow-PkUnique is the path to scrutinise; the dynamic-stride
  arm showing up for 8/16 means the fast arms are mis-wired, but a uniform small
  slowdown across k≥4 may be the `get_pk_bytes`-in-comparator cost itself — judge
  against the gate, don't assume mis-wiring.
- `heap_node_is_8_bytes` asserts the node shrank.

---

## Phase C — Pair (k = 2) heap bypass

### Problem

`bench_merge_scan_by_k`: k1 **8.3** → k2 **19.5** ns/row — a 2.3× cliff when the
loser tree engages, then *flat* to k16. The ~11 ns/row is the heap's fixed
per-emit cost (build, `walk_up`, `replace_top`/`pop_top`), independent of k. The
DBSP hot shape — one delta against a well-compacted trace of few segments — makes
**k=2 a frequent multi-source case** that pays full heap price for a merge a
single branch can do. (The bench establishes the per-k *cost* cliff, not the
production *frequency*: the cogroup bench runs a single-source trace, and a trace
cursor's k is `memtable runs + shards`, so k=2 is the compacted ideal, not a
guarantee — the win still lands on every actual 2-source merge.)

### Design

A `SourceMode::Pair` bypassing the tree, analogous to the existing `Single`
bypass (`read_cursor.rs:924`):

```rust
enum SourceMode { Empty, Single, Pair, Multi(LoserTree) }
```

`new()` (`read_cursor.rs:493`) selects `Pair` for `sources.len() == 2`. The Pair
driver reads the two heads **directly from `states[0]`/`states[1]`** (like
`drive_single` reads `states[0]`) — no separately-cached head positions — and
merges by one PK compare (the same stride-dispatched `pk_lt`/payload `row_cmp` as
the heap, post-Phase B), folding equal `(PK,payload)` and skipping ghosts — no
`walk_up`, no allocation. Reading `states` directly is what lets the seek/rewind
paths (`rebuild_and_drive`, which only rebuilds a `Multi` tree) re-drive a Pair
with no special handling. Forward `advance_to` on a Pair gallops both heads via
`CursorState::advance_to` (no tree to maintain), replacing `gallop_heap_forward`
for k=2.

The comparator is selected once (Phase B's stride dispatch), hoisting the
width branch out of the per-row loop; the loop itself still branches on
head-selection, ghost-skip, and the payload fold — "one compare per row", not
literally branch-free.

### Steps

A fourth `SourceMode` arm touches every `match self.mode` / `matches!(self.mode,
…)` site. The full surface (all in `read_cursor.rs`):

1. Add the `Pair` variant and the `new()` selection (`:493`).
2. `drive_pair_with` / `drive_pair_pk_unique`: 2-head merge + consolidate +
   ghost-skip, committing `current_*` like `drive_single`. Wire into the
   `drive_pk_unique` (`:868`) and `drive_with` (`:992`) dispatch matches.
3. `advance` dispatch (`advance_pk_unique` `:832`, `advance_with` `:846`): Pair,
   like `Multi`, does **not** pre-step `states[0]` (the `drive_pair` fold already
   consumed the emitted group) — keep the `if Single { prestep }` guard and let
   Pair fall through to `drive_pair`. Getting this wrong double-advances or stalls.
4. `advance_to` Pair arm (`:585` dispatch): gallop both heads via
   `CursorState::advance_to`, then `drive_pair` — mirror the `Multi` in-place
   fast path's "forward-only, else rebuild" guard.
5. `rebuild_and_drive` (`:521`) needs **no** Pair branch *iff* the driver reads
   `states` directly (above): the `if let Multi` rebuild is correctly skipped and
   the trailing `drive()` re-drives the Pair. Confirm this holds; it is the one
   easy-to-miss dispatch site.
6. `drain_sorted_into*` and `estimated_length` need **no** direct change — they
   route through `advance()`/`drive()` and `states`, not a `mode` match.

### Validation

- The existing `assert_advance_to_matches_seek_oracle` (`read_cursor.rs`) is
  Pair/Multi-agnostic — it pins each landing against a from-scratch `seek_bytes`
  — and its k=2 cases (`advance_to_*` use two batches) now exercise the Pair
  path. Keep them green.
- To assert Pair ≡ Multi *directly*, add a test-only constructor that forces
  `SourceMode::Multi` at `len == 2` (the production selector always picks Pair, so
  there is otherwise no Multi(k=2) to compare against) and diff the full scan +
  `advance_to` output of the two on the same sources, incl. ghost-folding and
  same-PK/multi-payload rows.
- `bench_merge_scan_by_k` k2 drops from ~19.5 toward k1 (~8–12 ns/row target).

---

## Risks

- **Phase B neutrality (primary risk).** Removing the cached key shifts each PK
  compare from "read a resident `u128` node field" to "`CursorSource` dispatch +
  slice build + integer load", run ≈`log k` times per row in `walk_up` plus at
  each group boundary; the keyless advance only partly offsets it by dropping the
  per-row key recompute. The 8/16 stride arms keep the load register-cheap, and
  "not compare-bound" is the reason to expect a wash, but a uniform k≥4 slowdown
  (worst on the narrow-PkUnique base-table path) may be **inherent**, not a
  wiring bug. Gate on k≥4 (not k2, which Phase C bypasses).
- **Phase B blast radius.** Three drivers + the heap change + two narrow/wide
  forks collapsing together; land it as one change behind the property/oracle
  tests rather than per-driver (the heap signature change forces all at once).
- **`emit` group_key audit.** `merge_batches_inner` and `compact_shards` derive
  their output PK from `(src, row)` (both ignore the passed key); only
  `merge_and_route` reads it (shard routing). Re-derive its routing `u128` as
  `pack_pk_be(pk_bytes)` — locally in `open_and_merge_inner` (lower churn) or in
  the callback — don't just confirm it's unused.
- **Phase A scope (membership-walk completeness).** The conversion must cover
  *every* forward equal-PK walk, not just semi/anti/inner: `op_join_delta_trace_outer`
  (LEFT join, with its `matched` fold) and the `op_reduce.rs:510,533` MIN/MAX
  history reads are the same shape and easy to miss. Genuinely-interleaved sites
  (`distinct`, the GI/point/full-scan reduce reads) stay out — see Non-goals.
- **Phase A fold granularity.** `walk_pk_group` must fold per-`(PK,payload)`
  (reusing `drive_merge`), not sum the whole PK group — else a multi-payload
  group with offsetting weights mis-reports membership. Pinned by the
  multi-payload validation case.
- **Phase C dispatch creep.** A fourth `SourceMode` arm touches every `mode`
  match; `rebuild_and_drive` is the one site that needs *no* change but is easy
  to break (it must keep skipping non-`Multi`). Keep the Pair driver a faithful
  2-way of the Multi comparator (reading `states` directly) so the oracle
  equivalence holds by construction.
