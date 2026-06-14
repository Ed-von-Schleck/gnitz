# Maintain the multi-source cursor's loser tree across forward seeks

## Thesis

A **multi-source** `ReadCursor` (`SourceMode::Multi`) is driven almost entirely
by **monotone forward `advance_to` sweeps**, yet today every `advance_to`
**throws the loser tree away and rebuilds it from scratch** (the build's retained
`tree` Vec + a transient `winners` Vec + `Θ(num_sources)` comparisons per call).
That is the loser tree's *construction* cost paid on every *step* — the opposite
of how a loser tree is meant to be used.

The tree does not need rebuilding on a forward seek. `LoserTree::replace_top`
already overwrites the champion's key with an arbitrarily larger (or smaller)
value and restores the tree in one `Θ(log num_sources)` walk-up — it carries
**no monotonicity precondition** (`heap.rs:147-149`). So a strictly-forward
`advance_to(key)` can be served by **galloping the laggard root source straight
to `key` and `replace_top`-ing it**, repeated until the merged head reaches
`key` — at most `num_sources` walk-ups, **no rebuild and no allocation**, reusing
the battle-tested `replace_top`/`pop_top` that already drive the merge.

The full rebuild then survives only where the tree genuinely changes at many
leaves at once — construction, `rewind`, and a *non-forward* seek (backward,
exhausted, or re-seek of the current key) — which are rare relative to the
per-group forward seeks. `LoserTree::build` is left **untouched**: no
reusable-scratch field, no in-place-rebuild variant, no new sentinel-fill
footgun. The win comes from rebuilding *less often*, not from making the rebuild
cheaper.

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
- **Range / band joins** (`ops/join.rs`): one `advance_to(start)` per delta row /
  eq-group; the start cut is "globally monotone non-decreasing across rows"
  except the `Lt`/`Le` `n_eq == 0` reset, which the code already flags as the
  non-monotone case.
- **Natural-PK reduce / gather** (`ops/reduce/op_reduce.rs`,
  `ops/reduce/op_gather.rs`): `advance_to(group_pk)` per group; the probe "visits
  groups in ascending output-PK order, so the probe is monotone."
- **Catalog resolve** (`catalog/store.rs`): `advance_to_exact_live(pk)` per PK;
  the "(common) ascending-`pks` caller" gets "one monotone forward sweep."

`advance_to` is documented as backward-capable (`read_cursor.rs:543-553`): a
non-monotone target "forfeits the speedup … but never correctness." That is
exactly the contract this plan keeps — the speedup is now "no rebuild," and the
forfeit is "fall back to a rebuild."

### Why the cursor is `Multi` (and stays that way)

Trace cursors are owned and **rebuilt every epoch** by `refresh_owned_cursors`
via `create_cursor_compacting`, which compacts the **disk tier only** and then
`open_cursor`s — exposing every memtable run, every `in_memory_l0` run, and every
disk shard as a separate `CursorSource`. Memtable runs fold only at
`INLINE_CONSOLIDATE_THRESHOLD = 16` or a flush, so `num_sources` sits at ~2–15 in
steady state. Forcing the trace to a single run would make every probe a `Single`
gallop, but that is eager full consolidation **per epoch** — `O(trace)` write
amplification on a relation that is also written every epoch — so it is correctly
avoided. The cursor *will* be `Multi`; the fix is to seek a `Multi` cursor
without rebuilding.

`SourceMode::Single` / `Empty` already bypass the tree (`drive_single`), so this
plan changes only the `Multi` path.

## Key invariant the fix relies on

The fast path is taken **only** when `key` is *strictly greater* than the current
emitted PK (`current_pk_cmp_bytes(key) == Ordering::Less`). Under that condition
every row any source has already consumed has PK `< key`.

**Proof.** The cursor emits consolidated groups in ascending `(PK, payload)`
order, and `drive` steps each source past exactly the emitted group's rows and
everything ordered before them. So the largest consumed PK equals the current
emitted PK, which is `< key`. Every consumed row therefore has PK `< key`, and
every *un*consumed row (the loser-tree heads and what follows each) has PK `≥`
the current emitted PK. Galloping each head forward to its own `lower_bound(key)`
thus cannot skip a row in `[·, key)`: nothing below `key` was consumed, and each
source's unconsumed run is sorted. The seek reduces to single-leaf updates.

**Why the boundary is strict.** At `key == current_pk`, `lower_bound(key)` is the
*first* row with PK `== key`, and that row may already be consumed — the current
group can be a *later* payload at the same PK (the cursor emits `(key, p0)`
before `(key, p1)`). A forward gallop starts from the heads, which sit at
`(key, p1)` or beyond, and can never reach the consumed `(key, p0)`. Routing
`key == current_pk` to the fast path would silently emit the *next* group and
drop the re-seek's target — a correctness bug. So `Equal`, every backward target,
and an exhausted cursor (no head to compare) all take the absolute-reposition +
rebuild path, whose per-source `advance_to` is backward-capable and re-finds
`lower_bound(key)` exactly. The dispatch condition `== Ordering::Less` is
precisely this safety boundary, not a heuristic.

## Change — galloping forward merge in `advance_to` (`read_cursor.rs`)

`advance_to` dispatches on the strict-forward condition. Forward on a live
`Multi` cursor runs the gallop-the-laggard loop; everything else (re-seek of the
current key, backward, exhausted, `Single`, `Empty`) keeps today's per-source
seek + `rebuild_and_drive`.

```rust
pub fn advance_to(&mut self, key: &[u8]) {
    // Strictly-forward seek on a live multi-source cursor: maintain the loser
    // tree in place instead of rebuilding it. The boundary is strict — `key`
    // must be > the current emitted PK. At `key == current_pk` the lower bound
    // of `key` can be a row this cursor already consumed (an earlier payload at
    // the same PK), which a forward gallop cannot reach; that case — and every
    // backward / exhausted / Single / Empty case — takes the absolute reposition.
    // `self.valid` is checked first so `current_pk_cmp_bytes` never reads an
    // unpositioned cursor.
    if self.valid
        && matches!(self.mode, SourceMode::Multi(_))
        && self.current_pk_cmp_bytes(key) == Ordering::Less
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

`seek_forward_multi` mirrors `drive`'s comparator dispatch exactly — PkUnique
skips the payload tiebreak, otherwise `with_payload_cmp!` selects the live
payload comparator:

```rust
/// Gallop the `Multi` loser tree forward to the first head `>= key`, then drive
/// the first live group. Precondition: `matches!(self.mode, SourceMode::Multi)`
/// and `key > current emitted PK` (enforced by `advance_to`'s dispatch).
fn seek_forward_multi(&mut self, key: &[u8]) {
    if self.is_pk_unique {
        self.seek_forward_multi_pk_unique(key);
    } else {
        with_payload_cmp!(self.schema, Self::seek_forward_multi_with, self, key);
    }
}
```

The two monomorphized wrappers build the same `less` closures `drive_pk_unique`
/ `drive_with` use, run the seek phase, then hand off to the matching `drive`.
The borrows of `schema` / `sources` / `states` / `mode` are scoped to an inner
block so they end before the `drive_*` re-borrow of `self`:

```rust
/// All-PkUnique forward seek: the heap key is authoritative (narrow) or
/// prefix-tied via `compare_pk_bytes` (wide); no payload tiebreak.
#[inline]
fn seek_forward_multi_pk_unique(&mut self, key: &[u8]) {
    {
        let schema = &self.schema;
        let sources = &self.sources;
        let states = &mut self.states;
        let heap = match &mut self.mode {
            SourceMode::Multi(h) => h,
            _ => unreachable!("seek_forward_multi_* requires SourceMode::Multi"),
        };
        if schema.pk_is_wide() {
            let less = |a: &HeapNode, b: &HeapNode| -> bool {
                match a.key.cmp(&b.key) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => columnar::compare_pk_bytes(
                        sources[a.source_idx].get_pk_bytes(a.row),
                        sources[b.source_idx].get_pk_bytes(b.row),
                    ) == Ordering::Less,
                }
            };
            Self::seek_phase(heap, sources, states, key, true, &less);
        } else {
            let less = |a: &HeapNode, b: &HeapNode| -> bool { a.key < b.key };
            Self::seek_phase(heap, sources, states, key, false, &less);
        }
    }
    self.drive_pk_unique();
}

/// Non-PkUnique forward seek: full `less` with prefix tie-break then payload
/// `row_cmp`, identical to `drive_with`'s comparator.
#[inline]
fn seek_forward_multi_with<RowCmp: RowComparator>(&mut self, key: &[u8], row_cmp: RowCmp) {
    {
        let schema = &self.schema;
        let sources = &self.sources;
        let states = &mut self.states;
        let heap = match &mut self.mode {
            SourceMode::Multi(h) => h,
            _ => unreachable!("seek_forward_multi_* requires SourceMode::Multi"),
        };
        let less = |a: &HeapNode, b: &HeapNode| -> bool {
            match a.key.cmp(&b.key) {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => match pk_cmp_after_key_tie(schema, sources, a, b) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => row_cmp(schema, &sources[a.source_idx], a.row,
                                               &sources[b.source_idx], b.row) == Ordering::Less,
                },
            }
        };
        Self::seek_phase(heap, sources, states, key, schema.pk_is_wide(), &less);
    }
    self.drive_with(row_cmp);
}
```

`seek_phase` gallops each laggard root source to `key` and `replace_top`s (or
`pop_top`s) until the head reaches `key`. Only the current root is ever a laggard
— it is the global min, so if it is `>= key` every head is — and each gallop
moves that source to `>= key`, so the loop touches at most `num_sources` leaves
and always terminates. The loop guard compares the heap's cached **OPK
`pk_sort_key`** (`= pack_pk_be`) against `pack_pk_be(key)`: a register `u128`
test for narrow PKs (the heap node already carries the key — no `get_pk_bytes`
load, no `memcmp`), with a full byte compare only on a wide-prefix tie.

```rust
/// Advance every laggard head (OPK `< key`) to its own `lower_bound(key)`,
/// restoring the loser tree with one `replace_top`/`pop_top` per gallop. On
/// return every head is `>= key`, leaving the tree positioned exactly as a
/// from-scratch rebuild at `key` would — `drive` then folds the first live
/// group. `key` is exactly `pk_stride` OPK bytes; `is_wide` is `schema.pk_is_wide()`.
fn seek_phase(
    heap: &mut LoserTree,
    sources: &[CursorSource],
    states: &mut [CursorState],
    key: &[u8],
    is_wide: bool,
    less: &impl Fn(&HeapNode, &HeapNode) -> bool,
) {
    // The heap key is the order-preserving `pk_sort_key` (`pack_pk_be`): the
    // exact whole PK for narrow (`stride <= 16`), the leading-16 OPK prefix for
    // wide. `target` packs `key` the same way, so `top.cmp(&target)` is the OPK
    // byte order without a memory load on the common narrow path.
    let target = pack_pk_be(key);
    while !heap.is_empty() {
        let HeapNode { key: top, source_idx: src, row } = *heap.peek();
        match top.cmp(&target) {
            // Root >= key ⇒ merged head >= key ⇒ positioned (root is the min).
            Ordering::Greater => break,
            Ordering::Equal => {
                // Narrow: equal packed key ⇒ equal PK ⇒ positioned. Wide: an
                // equal prefix can still hide `full < key`, so byte-compare.
                if !is_wide
                    || sources[src].get_pk_bytes(row).cmp(key) != Ordering::Less
                {
                    break;
                }
            }
            // Root < key (narrow), or wide prefix tie with full bytes < key.
            Ordering::Less => {}
        }
        states[src].advance_to(&sources[src], key); // gallop this laggard
        if states[src].is_valid() {
            heap.replace_top(sources[src].get_pk_prefix(states[src].position),
                             states[src].position, less);
        } else {
            heap.pop_top(less);
        }
    }
}
```

After `seek_phase` the root is the first head `>= key` across all sources; the
existing `drive` then folds that group, skipping ghosts to the first live row —
byte-identical to what `rebuild_and_drive`'s `drive` produces. `seek_bytes`,
`rewind`, and `seek_group` are unchanged (they are not forward-monotone in
general and keep the rebuild path).

`heap.rs` is **not modified.** `LoserTree::build` / `replace_top` / `pop_top` /
`walk_up` are used as-is. `pack_pk_be`, `HeapNode`, and `Ordering` are already in
scope in `read_cursor.rs`.

## Correctness

- **Forward path = a sequence of single-leaf updates.** Each gallop moves exactly
  one source; `replace_top`/`pop_top` is the existing, tested restore for a
  single changed leaf. `replace_top` has no monotonicity precondition
  (`heap.rs:147-149`), so a galloping jump of the key is as valid as a one-row
  advance — the same operation `drive_merge`'s `step` already performs, with a
  larger stride.
- **Lands identically to `seek_bytes`.** Both reach "first live row `>= key`": the
  rebuild path re-seeks all sources then folds; the forward path seeks only the
  laggards (the rest are already `>= key` with nothing skippable in between, per
  the Key invariant) then folds the same group. `advance_to_lands_like_seek_bytes_monotone`
  pins this equality on one reused multi-source cursor against a from-scratch
  `seek_bytes` oracle — and its probe sweep already interleaves strict-forward
  steps (the fast path) with current-key re-seeks (the `Equal` fallback, e.g.
  probe `20` after landing on `20`), so it fails fast if the boundary is widened
  past `== Less`.
- **`Equal` / backward / exhausted** fall through to the unchanged per-source
  `advance_to` + `rebuild_and_drive`, re-finding `lower_bound(key)` even when it
  precedes the current position — so those keep today's behavior bit-for-bit.
  This is load-bearing: `cogroup_intersection`'s opening `m.advance_to(delta[0])`
  on a multi-source trace already positioned at `delta[0]` is an `Equal` re-seek
  that must re-emit that group (pinned by `intersection_skips_ghost_group_multi_source`
  and `intersection_self_positions_stale_cursor`).
- **Ghost elimination** is unchanged: `seek_phase` only positions; `drive`
  performs the net-weight group fold afterward, as it does after a rebuild.

## Cost

Let `N = num_sources`, and per forward seek let `m ≤ N` be the laggards (heads
`< key`) and `g_i` each laggard's gallop distance.

| case | today (rebuild) | this plan (forward) |
|---|---|---|
| any strict-forward seek | gallop **all** `N` + rebuild `Θ(N)` + **2 allocs** | gallop **`m`** + `m·Θ(log N)` walk-up, **0 alloc** |
| single-source-dominated dense (laggard in ~1 run) | `Θ(N)` + alloc per group | `Θ(log N)`, no alloc — **big win** |
| sparse (large gaps) | `Θ(N·log gap)` + alloc | `Θ(m·log gap + m·log N)`, no alloc — skips ahead sources |
| `N`-way-balanced dense (every run contributes each group) | `Θ(N)` + alloc | `Θ(N·log N)` walk-ups, no alloc |

The forward path never allocates and **skips sources already past `key`** (the
rebuild always re-reads all `N`). Its loop guard is a register `u128` compare of
the heap's cached key — no `get_pk_bytes` load, no `memcmp` — except on a
wide-prefix tie. The one case where its raw comparison count is higher is
`N`-way-balanced dense (`N·log N` walk-up compares vs `N` rebuild compares), but
those are cheap `u128` compares dwarfed by the eliminated allocations, and that
shape is atypical (a trace is usually one large compacted shard plus a few small
memtable runs, i.e. single-source-dominated).

**What the fast path does *not* capture:** an `Equal` re-seek of the current key
still rebuilds. This is the dominant case only for a **dense-aligned**
`cogroup_left` (every delta key is the immediately-next trace key), where the
gallop distance would be ~0 anyway — so only the allocation is lost, not a skip.
The incremental hot path the plan targets — a **sparse** delta over a dense trace
— makes each per-group `advance_to` strictly forward (skip many trace keys to the
next delta key), landing on the fast path. `cogroup_intersection` only ever calls
`advance_to` in its `Greater` arm (strict forward) plus the one-time opening
self-position, so it is fast-path throughout.

## Residual rebuilds

Rebuilds remain at construction (once per cursor per epoch), `rewind`, and every
non-forward seek (`Equal` re-seek, backward, exhausted). These are
`O(num cursors + num passes)` per epoch, not `O(num groups)`, so leaving
`LoserTree::build` allocating is fine — the allocation pressure this plan removes
is the per-group churn on the strict-forward sweeps.

## Scope / non-goals

- **`heap.rs` is unchanged.** No reusable `winners` field, no `empty()` shell, no
  in-place `rebuild`. This plan removes the rebuild from the hot path instead of
  optimizing it, so that machinery — and its leaf-reset correctness hazard — is
  unnecessary.
- **The `Equal` re-seek stays a rebuild, by design.** Making it a no-op is
  unsound (the current group can be a later payload at the same PK, so the lower
  bound is a *consumed* row), and cheaply distinguishing that from the
  first-payload case is not possible without redoing the per-source seek.
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
- `advance_to_lands_like_seek_bytes_monotone` is the core regression: it drives
  one reused multi-source cursor through an ascending sweep (fast path) inter-
  leaved with current-key re-seeks (`Equal` fallback) against a from-scratch
  `seek_bytes` oracle. Extend it with (a) a run where one source's head starts
  past the first probe key (so the loop must skip an ahead source while galloping
  another) and (b) an interleaved forward/backward sweep asserting the backward
  steps still match the oracle (exercising the rebuild fallback on the same
  cursor).
- The `cogroup.rs` multi-source tests (`intersection_skips_ghost_group_multi_source`,
  `intersection_self_positions_stale_cursor`) drive the new path through the
  `SortedKeyStream` impl and pin the `Equal`-fallback at the opening
  self-position; they must stay green.
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

- **Acceptance:** allocations across the strict-forward probe loop drop from
  `2 ×` (number of forward seeks) to **zero**; landings bit-identical to a
  from-scratch `seek_bytes` oracle; wall-clock improves on dense (rebuild gone)
  and does not regress on sparse (ahead sources skipped, no alloc). Per
  "measure, don't assume": confirm the `N`-way-balanced dense corner does not
  regress despite its higher walk-up count; if a real workload ever shows it
  does, that corner — not the common case — can revisit a cheap rebuild, with its
  own evidence.
- End-to-end signal: `benchmarks/micro/test_join.py` and
  `benchmarks/micro/test_view_incr.py` must not regress.
</content>
</invoke>
