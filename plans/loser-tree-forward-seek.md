# Maintain the multi-source cursor's loser tree across forward seeks

## Thesis

A **multi-source** `ReadCursor` (`SourceMode::Multi`) is driven almost entirely
by **monotone forward `advance_to` sweeps**, yet today every `advance_to`
**throws the loser tree away and rebuilds it from scratch** (two `Vec`
allocations + `Θ(num_sources)` comparisons per call). That is the loser tree's
*construction* cost paid on every *step* — the opposite of how a loser tree is
meant to be used.

The tree does not need rebuilding on a forward seek. `LoserTree::replace_top`
already overwrites the champion's key with an arbitrarily larger (or smaller)
value and restores the tree in one `Θ(log num_sources)` walk-up — it carries
**no monotonicity precondition** (`storage/heap.rs:148-149`). So a forward
`advance_to(key)` can be served by **galloping the laggard root source straight
to `key` and `replace_top`-ing it**, repeated until the merged head reaches
`key` — at most `num_sources` walk-ups, **no rebuild and no allocation**, reusing
the battle-tested `replace_top`/`pop_top` that already drive the merge.

The full rebuild then survives only where the tree genuinely changes at many
leaves at once — construction, `rewind`, and a *backward* (non-monotone) seek —
which are rare relative to the per-group forward seeks. `LoserTree::build` is
left **untouched**: no reusable-scratch field, no in-place-rebuild variant, no
new sentinel-fill footgun. The win comes from rebuilding *less often*, not from
making the rebuild cheaper.

This is simpler (the hot path stops touching `heap.rs` at all), more robust (it
reuses `replace_top` instead of a new in-place build with a subtle leaf-reset
invariant), more elegant (the loser tree is *maintained*, as designed), and more
performant (the per-group rebuild + its allocation pair both disappear).

## The access pattern (why forward seeks dominate)

Every cited driver repositions a `Multi` cursor with `advance_to` in **ascending
key order**, then walks a few rows forward with `advance`:

- **Co-group merges** (`ops/cogroup.rs`): `cogroup_left` calls `m.advance_to(dk)`
  for **every** delta group (`cogroup.rs:162`); `cogroup_intersection` calls it
  once up front plus once per `Greater` transition (`cogroup.rs:120,127`). Delta
  groups are visited in ascending order, so the targets are monotone. Backs
  anti / outer / distinct / reduce (`cogroup_left`) and semi / inner
  (`cogroup_intersection`).
- **Range / band joins** (`ops/join.rs:324,391`): one `advance_to(start)` per
  delta row / eq-group; the start cut is "globally monotone non-decreasing across
  rows" except the `Lt`/`Le` `n_eq == 0` reset, which the code already flags as
  the non-monotone case.
- **Natural-PK reduce / gather** (`ops/reduce/op_reduce.rs:444,509`;
  `ops/reduce/op_gather.rs:77`): `advance_to(group_pk)` per group; the comments
  state the probe "visits groups in ascending output-PK order, so the probe is
  monotone."
- **Catalog resolve** (`catalog/store.rs:893,1008`): `advance_to_exact_live(pk)`
  per PK; the comments note the "(common) ascending-`pks` caller" gets "one
  monotone forward sweep."

`advance_to` is documented as backward-capable (`read_cursor.rs:546-559`): a
non-monotone target "forfeits the speedup … but never correctness." That is
exactly the contract this plan keeps — the speedup is now "no rebuild," and the
forfeit is "fall back to a rebuild."

### Why the cursor is `Multi` (and stays that way)

Trace cursors are owned and **rebuilt every epoch** by `refresh_owned_cursors`
(`vm.rs:566-612`) via `create_cursor_compacting` (`table.rs:684`), which compacts
the **disk tier only** and then `open_cursor`s (`table.rs:665`) — exposing every
memtable run, every `in_memory_l0` run, and every disk shard as a separate
`CursorSource`. Memtable runs fold only at `INLINE_CONSOLIDATE_THRESHOLD = 16`
(`memtable.rs:18`) or a flush, so `num_sources` sits at ~2–15 in steady state.
Forcing the trace to a single run would make every probe a `Single` gallop, but
that is eager full consolidation **per epoch** — `O(trace)` write amplification
on a relation that is also written every epoch — so it is correctly avoided. The
cursor *will* be `Multi`; the fix is to seek a `Multi` cursor without rebuilding.

`SourceMode::Single` / `Empty` already bypass the tree (`read_cursor.rs:704,718`,
`drive_single`), so this plan changes only the `Multi` path.

## Key invariant the fix relies on

In a **monotone forward** seek (`key ≥ the current merged head`), every row a
source has already consumed has PK `< key`. Proof: the current merged head is the
minimum unconsumed PK across all sources, and consumption is in non-decreasing PK
order, so every consumed PK is `< current_head ≤ key`. Therefore:

- A source whose head is **already `≥ key`** has *no* unconsumed row in
  `[old_head, key)` (its rows are sorted and its unconsumed run starts at its
  head), so it already contributes the correct first-row-`≥ key`. **Leave it
  untouched** — the rebuild needlessly re-reads it.
- A source whose head is **`< key`** is a laggard: gallop it to its own
  `lower_bound(key)` and the tree is restored by a single `replace_top`/`pop_top`
  (one leaf changed). Each source crosses `key` at most once, so at most
  `num_sources` laggards are touched.

The dispatch condition `key ≥ current_head` (`current_pk_cmp_bytes(key) !=
Greater`) is exactly what makes this sound: if it fails (backward target, or the
cursor is exhausted so there is no head to compare), a previously-consumed row
could be `≥ key` and galloping forward would miss it — so that case takes the
absolute-reposition + rebuild path.

## Change — galloping forward merge in `advance_to` (`read_cursor.rs`)

`advance_to` dispatches on the monotonicity condition. Forward on a `Multi`
cursor runs the gallop-the-laggard loop; everything else (backward, exhausted,
`Single`, `Empty`) keeps today's per-source seek + `rebuild_and_drive`.

```rust
pub fn advance_to(&mut self, key: &[u8]) {
    // Forward seek on a live multi-source cursor: maintain the tree in place.
    // `current_pk_cmp_bytes(key) != Greater` ⇒ key ≥ current head ⇒ monotone
    // (see "Key invariant"). Single/Empty have no tree; an exhausted or backward
    // cursor needs the absolute reposition below.
    if self.valid
        && matches!(self.mode, SourceMode::Multi(_))
        && self.current_pk_cmp_bytes(key) != Ordering::Greater
    {
        self.seek_forward_multi(key);
        return;
    }
    for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
        state.advance_to(src, key);
    }
    self.rebuild_and_drive();
}
```

`seek_forward_multi` gallops the laggard root source to `key` and `replace_top`s
(or `pop_top`s) until the head reaches `key`, then drives one live group — the
same group-fold/ghost-skip `drive` already performs. It is monomorphized on the
PK-width comparator set exactly like `drive_with`/`drive_pk_unique`, so it reuses
their `less` closure (no new comparator code):

```rust
// inside the with_payload_cmp! / pk_unique dispatch, holding `less`:
fn seek_phase(
    heap: &mut LoserTree,
    sources: &[CursorSource],
    states: &mut [CursorState],
    key: &[u8],
    less: &impl Fn(&HeapNode, &HeapNode) -> bool,
) {
    while !heap.is_empty() {
        let src = heap.peek().source_idx;
        // Head already ≥ key ⇒ merged head ≥ key ⇒ positioned. (peek() is the
        // global min, so if it is ≥ key every source head is.)
        if sources[src].get_pk_bytes(states[src].position).cmp /* OPK */ (key) != Ordering::Less {
            // compare via current_pk_bytes semantics; see note
            break;
        }
        states[src].advance_to(&sources[src], key);     // gallop this laggard
        if states[src].is_valid() {
            heap.replace_top(sources[src].get_pk_prefix(states[src].position),
                             states[src].position, less);
        } else {
            heap.pop_top(less);
        }
    }
}
```

After `seek_phase` the root is the first row `≥ key` across all sources; the
existing `drive` then folds that group, skipping ghosts to the first live row —
byte-identical to what `rebuild_and_drive`'s `drive` produces. `seek_bytes`,
`rewind`, and `seek_group` are unchanged (they are not forward-monotone in
general and keep the rebuild path).

> Implementation note: compare the root's head against `key` in **OPK byte
> order** (the order the heap is keyed on), via `get_pk_bytes(...).cmp(key)` /
> the wide-PK `compare_pk_bytes` tie-break — not the `u128` `current_key`, which
> is non-authoritative for wide PKs. The dispatch guard's `current_pk_cmp_bytes`
> is already byte-order; `seek_phase`'s loop guard must match it.

`heap.rs` is **not modified.** `LoserTree::build` / `replace_top` / `pop_top` /
`walk_up` are used as-is.

## Correctness

- **Forward path = a sequence of single-leaf updates.** Each gallop moves exactly
  one source; `replace_top`/`pop_top` is the existing, tested restore for a
  single changed leaf. `replace_top` has no monotonicity precondition
  (`heap.rs:148-149`), so a galloping jump of the key is as valid as a one-row
  advance — this is the same operation `drive_merge`'s `step` already performs,
  only with a larger stride.
- **Lands identically to `seek_bytes`.** Both reach "first live row `≥ key`": the
  rebuild path re-seeks all sources then folds; the forward path seeks only the
  laggards (the rest are already `≥ key` with nothing skippable in between, per
  the Key invariant) then folds the same group. The existing
  `advance_to_lands_like_seek_bytes_monotone` test asserts exactly this equality
  (forward `advance_to` vs from-scratch `seek_bytes`) for single- and
  multi-source cursors and now exercises the new path directly.
- **Backward / exhausted / non-monotone** falls through to the unchanged
  per-source `advance_to` + `rebuild_and_drive`, so those keep today's behavior
  bit-for-bit.
- **Ghost elimination** is unchanged: the seek phase only positions; `drive`
  performs the net-weight group fold afterward, as it does after a rebuild.

## Cost

Let `N = num_sources`, and per forward seek let `m ≤ N` be the laggards (heads
`< key`) and `g_i` each laggard's gallop distance.

| case | today (rebuild) | this plan (forward) |
|---|---|---|
| any forward seek | gallop **all** `N` + rebuild `Θ(N)` + **2 malloc/free** | gallop **`m`** + `m·Θ(log N)` walk-up, **0 alloc** |
| single-source-dominated dense (laggard in ~1 run) | `Θ(N)` + alloc per group | `Θ(log N)`, no alloc — **big win** |
| sparse (large gaps) | `Θ(N·log gap)` + alloc | `Θ(m·log gap + m·log N)`, no alloc — skips ahead sources |
| `N`-way-balanced dense (every run contributes each group) | `Θ(N)` + alloc | `Θ(N·log N)` walk-ups, no alloc |

The forward path never allocates and **skips sources already past `key`** (the
rebuild always re-reads all `N`). The one case where its raw comparison count is
higher is `N`-way-balanced dense (`N·log N` walk-up compares vs `N` rebuild
compares) — but those are cheap `u128` compares, dwarfed by the eliminated
`malloc`/`free` pair, and that shape is atypical (a trace is usually one large
compacted shard plus a few small memtable runs, i.e. single-source-dominated).
The `u128`-key compares avoid payload work except on exact key ties.

## Residual rebuilds

Rebuilds remain only at construction (once per cursor per epoch), `rewind`, and
backward/non-monotone seeks. These are `O(num cursors + num passes)` per epoch,
not `O(num groups)`, so leaving `LoserTree::build` allocating is fine — the
allocation pressure this plan removes is the per-group churn.

- **`rewind` is the one residual that fires per op-pass.** `op_join_delta_trace`
  / range join call `cursor.rewind()` before each pass purely so
  `estimated_length()` sees the full trace for the size selector
  (`join.rs:281-285`). It can be dropped by adding a position-independent
  `total_length()` (`Σ states[i].count`) for the selector and letting the first
  forward `advance_to(delta[0])` self-position (which `cogroup_intersection`
  already relies on, `cogroup.rs:96-101`). The `join_dt_swapped` path, which
  iterates the trace from row 0, is the only consumer that still needs an
  explicit reset; scope that to its own follow-up if it complicates this change —
  this plan stands without it, since even keeping `rewind` drops allocs from
  `O(groups)` to `O(passes)`.

## Scope / non-goals

- **`heap.rs` is unchanged.** No reusable `winners` field, no `empty()` shell, no
  in-place `rebuild`. The earlier "allocation-free in-place rebuild" framing
  optimized the rebuild; this plan removes it from the hot path instead, so that
  machinery — and its leaf-reset correctness hazard — is unnecessary.
- **No materialize / single-run trace.** Folding the trace to one source would
  make probes `Single` but costs `O(trace)` write amplification per epoch on a
  per-epoch-written relation. Out of scope (and likely never worth it).
- **No incremental update for the *backward* / multi-leaf case.** A backward or
  `rewind` reposition changes many leaves with no shared-ancestor-safe partial
  update (the loser tree stores only losers, not subtree winners), so it stays a
  full rebuild.
- **Seek-from-`Single` is already optimal** and untouched.

## Testing

- Full engine suite (`make test`) green — every co-group / range / reduce /
  catalog seek path now exercises the forward gallop path.
- `heap.rs` property tests (`property_kway_merge_random`,
  `k_way_merge_against_reference`, `drain_emits_sorted`,
  `loser_tree_wide_pk_prefix_collision`, `u128_max_key_is_a_real_value`) pass
  unchanged (`heap.rs` is untouched; they still pin `replace_top`/`pop_top`,
  which the forward path now leans on harder).
- Extend `advance_to_lands_like_seek_bytes_monotone`: it already drives one reused
  multi-source cursor through an ascending sweep against a from-scratch
  `seek_bytes` oracle — post-change that *is* the forward-path test. Add (a) a run
  where one source's head starts past the first probe key (so the loop must skip
  an ahead source) and (b) an interleaved forward/backward sweep asserting the
  backward steps still match the oracle (exercising the rebuild fallback on the
  same cursor).
- New multi-source test: large gaps with a ghost group straddling the boundary
  (PK nets to 0 across two runs at the seek target) — the forward seek must land
  on the first *live* row past the ghost, equal to `seek_bytes`. This pins the
  seek-phase / ghost-fold handoff.
- New test: a source that **exhausts** mid-sweep (its `lower_bound(key)` is its
  end) so the loop takes the `pop_top` branch, with the remaining sources still
  producing the correct merged order.

## Measurement / acceptance

Micro-benchmark (no server): a `Multi` trace `ReadCursor` (`num_sources` = 2, 4,
8, 16) driven by `cogroup_intersection` / `cogroup_left` over both a dense
(small-gap) and a sparse (large-gap) delta. Count allocations with a passthrough
counting `#[global_allocator]` (delegates to `System`, bumps an atomic), reading
the delta around an allocation-free probe loop.

- **Acceptance:** allocations across the forward probe loop drop from
  `2 ×` (number of forward seeks) to **zero**; landings bit-identical to a
  from-scratch `seek_bytes` oracle; wall-clock improves on dense (rebuild gone)
  and does not regress on sparse (ahead sources skipped, no alloc). Per
  "measure, don't assume": confirm the `N`-way-balanced dense corner does not
  regress despite its higher walk-up count; if a real workload ever shows it
  does, that corner — not the common case — can revisit a cheap rebuild, with its
  own evidence.
- End-to-end signal: `benchmarks/micro/test_join.py` and
  `benchmarks/micro/test_view_incr.py` must not regress.
