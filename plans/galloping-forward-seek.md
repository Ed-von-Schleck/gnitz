# Sorted-stream co-group merge

## Problem

The engine merges two sorted OPK-byte streams — drive one, match the other by
key — in roughly a dozen places across three stream-pair shapes. Each hand-rolls
the merge, and the "match the other side" step appears in several incompatible
spellings; the equi-join additionally hand-picks between two of them by size.

**Shape A — delta `&Batch` ↔ trace `&mut ReadCursor`:**

| site | drive (sorted) | match (sorted) | "match the other side" today |
|------|----------------|----------------|------------------------------|
| `op_distinct` (`ops/distinct.rs:16`) | delta batch | trace cursor | from-scratch `seek_bytes` + inner `(PK,payload)` walk |
| `op_join_delta_trace` (inner, `ops/join.rs:226`) | delta or trace | the other | size selector → `join_dt_swapped` / `join_dt_merge_walk` |
| `join_dt_swapped` (`ops/join.rs:326`) | trace cursor | delta **batch** | from-scratch `find_lower_bound_bytes` per trace row |
| `join_dt_merge_walk` (`ops/join.rs:262`) | delta batch | trace cursor | **linear** `while cur < k { advance }` + backward re-seek |
| `op_join_delta_trace_outer` (`ops/join.rs:378`) | delta batch | trace cursor | "outer always merge-walks" special case |
| `op_semi_join_delta_trace` (`ops/join.rs:112`) | delta or trace | the other | size selector → `dt_emit_indices` / `semi_join_dt_swapped` |
| `op_anti_join_delta_trace` (`ops/join.rs:73`) | delta batch | trace cursor | `dt_emit_indices` (`ops/join.rs:35`): `seek_bytes` + linear group walk |
| `op_reduce` history (natural-PK, `ops/reduce/op_reduce.rs:497`) | group heads | trace_in cursor | from-scratch `seek_group` per group |
| `op_reduce` retraction (`ops/reduce/op_reduce.rs:438`) | group heads | trace_out cursor | from-scratch `seek_group` per group |
| `op_gather_reduce` (`ops/reduce/op_gather.rs:75`) | group heads | trace_out cursor | from-scratch `seek_group` per group |
| `range_merge_walk` / `range_per_row_seek` (`ops/join.rs:507`, `452`) | trace or delta | the other | linear pointer advance / per-row `seek_bytes` |

**Shape B — slice `&[PkBuf]` ↔ base `&mut ReadCursor`:**

| site | drive (sorted) | match (sorted) | "match the other side" today |
|------|----------------|----------------|------------------------------|
| `resolve_source_pks` (`catalog/store.rs:989`) | ascending PK slice | base cursor | from-scratch `seek_exact_live` per PK |
| `gather_family_bytes` (`catalog/store.rs:877`) | PK slice (order not required) | base cursor | from-scratch `seek_exact_live` per PK |

**Shape C — delta `&Batch` ↔ delta `&Batch`:**

| site | drive (sorted) | match (sorted) | "match the other side" today |
|------|----------------|----------------|------------------------------|
| `op_join_delta_delta` (`ops/join.rs:908`) | batch A | batch B | hand-rolled `Less`/`Greater`/`Equal` linear co-group |
| `filter_join_dd` (semi/anti, `ops/join.rs:842`) | batch A | batch B | linear advance of B past each A key |
| `filter_join_dd_with_payload` (anti, `ops/join.rs:739`) | batch A | batch B | linear advance with `(PK,payload)` sub-merge |

All of these are one operation: **co-group two sorted byte-key streams** — walk
both, group rows by equal key, hand each `(key, left-group, right-group)` to the
operator. The absence of that primitive is the single root cause of everything
below:

- **Several spellings of one skip.** `Batch::find_lower_bound_bytes`
  (`storage/batch.rs:989`) and `MappedShard::find_lower_bound_bytes`
  (`storage/shard_reader.rs:515`) are byte-identical from-scratch `[0, count)`
  binary searches; `ReadCursor::seek_bytes` (`storage/read_cursor.rs:518`)
  descends to the same per source; and the merge-walks instead linear-scan, while
  the DD joins linear-scan both sides. None reuses the position the stream already
  holds, so K ascending probes over an N-row source cost `O(K · log N)` (or
  `O(K + N)` linear) when the boundary only moves forward.
- **A monotonicity question at every site.** Whether a loop's probe keys ascend,
  and whether the live position is a valid lower-bound hint, has to be re-derived
  by reading each loop — a per-site audit with no type-level answer.
- **Backward re-seeks.** `join_dt_merge_walk`'s re-seek (`ops/join.rs:287`) exists
  only because a row-at-a-time loop advanced past a key group it must revisit when
  the next delta row shares the PK. A co-group presents the whole group at once and
  never advances past it unprocessed. (This is distinct from the GI scatter-gather
  re-seek at `op_reduce.rs:520`, which is genuinely non-monotone — see Boundaries.)
- **A size selector and a special case.** `op_join_delta_trace` (`ops/join.rs:226`)
  does `cursor.rewind()` + `estimated_length()` + `if n > trace_len` to choose
  `join_dt_swapped` (drive the small trace, binary-search the big delta) vs
  `join_dt_merge_walk` (drive the small delta, linear-advance the big trace); the
  semi-join entry does the same; and `op_join_delta_trace_outer` (`ops/join.rs:378`)
  carries a separate "outer always merge-walks" rule. All three approximate what a
  symmetric galloping merge does automatically and optimally.

A co-group merge whose skip step galloping-seeks from the live position dissolves
all four. The galloping seek is not a feature to add at a dozen call sites — it is
the merge's internal *skip*.

## Design

Three layers, smallest to largest.

### 1. The skip: `advance_to` (galloping, position-seeded)

A lower-bound search seeded at the live position. The shared body is a pure
byte-search utility next to `compare_pk_bytes` (`storage/columnar.rs:146`):

```rust
// storage/columnar.rs

/// Lower bound (first index with `get(i) >= key`) over `[lo, hi)`. `get(i)`
/// yields row `i`'s OPK PK bytes; memcmp order equals typed order at every PK
/// width. The named `'a` is load-bearing: a bare `Fn(usize) -> &[u8]` desugars
/// to a higher-ranked bound the `|i| self.get_pk_bytes(i)` closures (result
/// borrows `self`) cannot satisfy.
#[inline]
pub fn binary_lower_bound<'a>(
    mut lo: usize, mut hi: usize, key: &[u8], get: &impl Fn(usize) -> &'a [u8],
) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if get(mid) < key { lo = mid + 1; } else { hi = mid; }
    }
    lo
}

/// Lower bound over `[0, count)`, seeded at `hint`. Galloping forward when the
/// boundary is after the hint (`O(log gap)`), `O(1)` when the boundary IS the
/// hint (consecutive keys in one inter-row gap, or a run past the source end with
/// `hint == count`), and a bounded `[0, hint)` search when the boundary is before
/// it. Correct for ANY hint, and since `[0, hint] ⊆ [0, count)` it is **never
/// asymptotically worse** than `binary_lower_bound(0, count, …)` — a backward or
/// stale hint forfeits only the speedup, at the cost of at most two extra
/// comparisons.
#[inline]
pub fn gallop_lower_bound_bytes<'a>(
    count: usize, key: &[u8], hint: usize, get: impl Fn(usize) -> &'a [u8],
) -> usize {
    let h = hint.min(count);
    if h < count && get(h) < key {           // boundary strictly after the hint
        let mut lo = h;                       // invariant: get(lo) < key
        let mut step = 1usize;
        while lo + step < count && get(lo + step) < key { lo += step; step *= 2; }
        let hi = (lo + step).min(count);      // get(hi) >= key, or hi == count
        return binary_lower_bound(lo + 1, hi, key, &get);
    }
    if h == 0 || get(h - 1) < key { return h; } // boundary is exactly h (incl. h == count)
    binary_lower_bound(0, h, key, &get)         // genuine overshoot: bounded [0, h)
}
```

This body is correct for every `(key, hint, count)` — verified by case analysis:
the gallop branch keeps the invariant `get(lo) < key` and brackets the boundary in
`(lo, hi]`; the `O(1)` branch returns `h` exactly when `get(h-1) < key ≤ get(h)`
(including `h == 0` and `h == count`); the bounded branch runs only when
`get(h-1) ≥ key`, where the boundary is provably `< h`. `count == 0` returns `0`.

`Batch` and `MappedShard` keep the stateless `find_lower_bound_bytes` (now a thin
`binary_lower_bound(0, count, …)` wrapper) and gain the galloping form:

```rust
// storage/batch.rs (MappedShard identical, swapping count / get_pk_bytes)
pub fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
    columnar::binary_lower_bound(0, self.count, key, &|i| self.get_pk_bytes(i))
}
pub fn advance_to(&self, key: &[u8], hint: usize) -> usize {
    columnar::gallop_lower_bound_bytes(self.count, key, hint, |i| self.get_pk_bytes(i))
}
```

`ReadCursor::advance_to(key)` seeds each source's search at that source's live
`position` — there is **no hint parameter**, because the stream owns its position:

```rust
// storage/read_cursor.rs — CursorState (per source)
fn advance_to(&mut self, src: &CursorSource, key: &[u8]) {
    self.position = src.advance_to(key, self.position);  // dispatch to Batch / Shard galloping
    self.skip_ghosts(src);
}
// ReadCursor
pub fn advance_to(&mut self, key: &[u8]) {
    for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
        state.advance_to(src, key);
    }
    self.rebuild_and_drive();
}
```

`advance_to` is forward-only and owns its hint, so a stale or non-monotone hint is
unrepresentable — the merge below is its only consumer, and the monotonicity
question never reaches a call site. The from-scratch `seek_bytes` / absolute
`find_lower_bound_bytes` remain for the callers that hold no position or probe at
genuinely random keys (point lookups, scan positioning, prefix walks — see
Boundaries).

> **Multi-source cost.** `ReadCursor::advance_to` calls `rebuild_and_drive`, which
> rebuilds the loser tree (`O(num_sources)`) whenever the cursor is `Multi`. This
> is the same cost the existing `seek_bytes` pays, so `advance_to` is no regression
> against any seek-based path. But the linear `join_dt_merge_walk` it replaces
> stepped the big trace with `advance()` (a single `O(log num_sources)` heap pop),
> not a rebuild. The galloping merge fires `advance_to` once per *skip gap*
> (≤ distinct delta keys), so its asymptotic win over linear-advance holds, but for
> a **multi-source** trace only marginally larger than the delta the per-gap rebuild
> is a real constant factor. If benchmarks show it, re-sift only the sources that
> actually moved instead of a full `build_tree` — single-source traces
> (`SourceMode::Single`) already pay nothing.

### 2. The merge: two co-group skeletons over a generic match stream

The match side is abstracted so **one** skeleton serves every stream-pair shape —
the delta-trace joins, the slice-vs-base resolves, *and* the delta-delta joins —
instead of three hand-rolled families. A `SortedKeyStream` exposes the current key,
the galloping skip, and the single-step in-group walk:

```rust
/// A sorted OPK-byte stream with a galloping forward skip. `&mut ReadCursor`
/// implements it via `advance_to`; a `BatchCursor { batch, pos }` newtype
/// implements it via `gallop_lower_bound_bytes` over the batch rows — so the
/// delta-delta joins co-group through the same skeleton as the delta-trace joins.
trait SortedKeyStream {
    fn key(&self) -> Option<&[u8]>;       // current row's OPK bytes; None when exhausted
    fn advance_to(&mut self, key: &[u8]); // galloping forward lower-bound skip
    fn advance(&mut self);                // single step, for the in-group walk
}
```

Both skeletons drive a consolidated `delta: &Batch` (sorted) by group and present
each key's two groups to a callback. The callback walks the match group itself
(`while m.key() == Some(key) { … m.advance() }`) so it keeps its existing
column-first / `scatter_copy` / `write_join_row` emission — the skeleton owns only
the control flow, never the row copy.

```rust
/// Intersection co-group: emit only at keys present on both sides. Both pointers
/// galloping-skip to catch up, so the cost is O((matches + skips) · log gap),
/// optimal whichever side is larger — this replaces the size selector AND both
/// `join_dt_swapped` / `join_dt_merge_walk`. Inner join, semi join, inner DD join.
fn cogroup_intersection<M: SortedKeyStream>(
    delta: &Batch, m: &mut M,
    mut on_match: impl FnMut(&[u8], Range<usize>, &mut M),
) {
    let n = delta.count;
    let mut i = 0;
    while i < n {
        let Some(mk) = m.key() else { break };
        let dk = delta.get_pk_bytes(i);
        match dk.cmp(mk) {
            Ordering::Less    => i = delta.advance_to(mk, i), // skip delta
            Ordering::Greater => m.advance_to(dk),            // skip match side
            Ordering::Equal => {
                let mut j = i + 1;
                while j < n && delta.get_pk_bytes(j) == dk { j += 1; }   // delta group
                on_match(dk, i..j, m);                                   // walks match group
                i = j;
            }
        }
    }
}

/// Left co-group: visit EVERY delta group (the match side galloping-skips to it),
/// because the operator emits per delta key whether or not the match group exists.
/// Outer join (null-fill on an empty group), anti join, distinct, reduce, anti DD.
fn cogroup_left<M: SortedKeyStream>(
    delta: &Batch, m: &mut M,
    mut on_group: impl FnMut(&[u8], Range<usize>, &mut M),
) {
    let n = delta.count;
    let mut i = 0;
    while i < n {
        let dk = delta.get_pk_bytes(i);
        let mut j = i + 1;
        while j < n && delta.get_pk_bytes(j) == dk { j += 1; }
        m.advance_to(dk);                                  // galloping skip; group may be empty
        on_group(dk, i..j, m);
        i = j;
    }
}
```

`cogroup_intersection` is symmetric — it drives neither side, advancing whichever
pointer is behind. With galloping skips it is `O(min-work)` automatically: huge
delta + tiny match skips the delta down to each match key (the old `swapped` cost);
tiny delta + huge match skips the match (the old `merge_walk` cost), and *better
than* the old linear merge_walk, which scanned the whole big side. No `rewind`, no
`estimated_length`, no `n > trace_len`. `cogroup_left` is the delta-must-be-visited
case, `O(delta + galloping match)` — inherent and identical to today's "always
merge-walk."

> **Callback contract.** The callback MUST advance the match side past the matched
> group before returning (the `while m.key() == Some(key)` walk does this). The
> intersection loop self-corrects a forgotten walk on the next `Greater` skip; the
> left loop does not, so a left callback that emits without consuming an existing
> group would re-see it. State this in the skeleton doc-comment.

> **Group identity is two levels for `(PK, payload)` operators.** The skeleton keys
> on **PK only**. Operators whose Z-Set identity is `(PK, payload)` — distinct, and
> the payload-aware anti DD — must run a *secondary* merge by payload **inside** the
> callback, walking the delta sub-range `[i, j)` and the match group in lockstep by
> payload (both are `(PK, payload)`-sorted). Folding the whole match group's weight
> into one number is **wrong** for a PK carrying multiple payloads (intermediate
> batches are not PK-unique — `foundations.md` §1). Joins are pure key co-groups
> (payload flows into the output, never into the match test) and need no inner merge.

### 3. Operators as callbacks

Each operator becomes its emit logic over `(key, delta-range, match-group)`:

- **`op_distinct`** — `cogroup_left`; per delta group run the `(PK, payload)`
  sub-merge: for each `(PK, payload)` element fold the byte-equal match row's
  weight, compute `signum(w_old + w_delta) − signum(w_old)`, push the emit
  index/weight. Deletes the per-row `seek_bytes`; **keeps**
  `compare_cursor_payload_to_batch_row` as the payload comparator inside the
  sub-merge.
- **`op_join_delta_trace` (inner) and `op_semi_join_delta_trace`** —
  `cogroup_intersection`; emit the product (inner) or the delta rows (semi, gated on
  a positive-weight match row). Deletes both selectors, `join_dt_swapped`,
  `join_dt_merge_walk`, and `semi_join_dt_swapped`.
- **`op_join_delta_trace_outer`** — `cogroup_left`; emit the product when the match
  group has a positive-weight row, else the null-filled left row. Deletes the
  "outer always merge-walks" special case.
- **`op_anti_join_delta_trace`** — `cogroup_left`; emit the delta group iff the
  match group has **no** positive-weight row. Deletes the `dt_emit_indices`
  merge-walk (the last remaining caller once semi-join moves off it).
- **`op_reduce` / `op_gather_reduce` (natural-PK / `group_set_eq_pk`)** —
  `cogroup_left` against `trace_in` (history); per group fold the trace group into
  the accumulators. The **retraction read** from `trace_out` (`op_reduce.rs:438`,
  the only stream `op_gather_reduce` reads, `op_gather.rs:75`) is a *second* monotone
  per-group seek over a different cursor; switch it to `advance_to` as well — the
  groups are processed in output-PK order on the natural-PK path, so the trace_out
  probe is monotone. Deletes the per-group `seek_group` on **both** cursors.
- **`resolve_source_pks` / `gather_family_bytes`** — the degenerate co-group: the
  driver is a sorted `&[PkBuf]` with no payload to product, so it is a plain
  `for pk in pks { exact_live_advance_to(cursor, pk) }` loop, where the helper is
  `advance_to` followed by the `valid && current_pk_eq && current_weight > 0` check
  that `seek_exact_live` bundles today. `resolve_source_pks` asserts ascending PKs,
  so its hint is monotone; `gather_family_bytes` does **not** require sorted input
  (its doc says order is not required), so `advance_to` there is correct but speeds
  up only when the caller happens to pass ascending PKs — no regression either way.
  `seek_exact_live` itself stays for the random point-lookup callers
  (`seek_family`, the unique-upsert probe, single-row retract, FK validation).
- **`op_join_delta_delta` / `filter_join_dd` / `filter_join_dd_with_payload`** —
  pass a `BatchCursor` over batch B as the match stream. Inner DD →
  `cogroup_intersection` + cartesian callback; semi DD → `cogroup_intersection`;
  anti DD (PK-only) → `cogroup_left`; anti DD (payload-aware) → `cogroup_left` with
  the `(PK, payload)` sub-merge. This collapses three more hand-rolled co-groups and
  gives the skewed DD case (tiny ΔB ⋈ huge ΔA) the same galloping speedup the
  delta-trace joins get, instead of today's unconditional `O(n_a + n_b)` scan.

## What this deletes

- `op_join_delta_trace`'s and `op_semi_join_delta_trace`'s `rewind` +
  `estimated_length` + `n > trace_len` selectors, `join_dt_swapped`,
  `join_dt_merge_walk`, `semi_join_dt_swapped`, and `dt_emit_indices`.
- `op_join_delta_trace_outer`'s "outer always merge-walks" special case.
- The backward re-seek `ops/join.rs:287` (co-grouping presents the whole group at
  once, so a multiset delta never revisits a key).
- The hand-rolled `Less`/`Greater`/`Equal` co-groups in `op_join_delta_delta`,
  `filter_join_dd`, and `filter_join_dd_with_payload` (now the same skeleton).
- The per-group from-scratch `seek_group` in the natural-PK `op_reduce` history read,
  **both** its `trace_out` retraction read **and** `op_gather_reduce`'s `trace_out`
  read (now `advance_to`).
- The per-site monotonicity audit: forward-only `advance_to` cannot be misused, and
  the few genuinely-random probes are visibly outside the merge framework.

`ReadCursor::seek_bytes` / `seek_group` are **not** deleted — they remain the
primitive for scan positioning (`vm.rs:874` `SeekTrace`, the catalog range scans at
`catalog/store.rs:394`/`1118`/`1587`, `catalog/utils.rs:378`), for point lookups
(`seek_exact_live` is `seek_bytes` + a predicate), and inside the prefix-walk family
(`seek_first_positive_with_prefix` seeks then walks). Only the per-row *operator*
probes inside the co-groups switch to `advance_to`.

## Boundaries (out by nature, not deferral)

- **Point lookups** (`storage/memtable.rs:30` `run_exact_match_start_bytes`,
  `storage/shard_index.rs:123` `probe_pk_bytes`): single random probes with no
  position context. They keep the stateless `find_lower_bound_bytes`.
- **Prefix-walk family** (`seek_first_positive_with_prefix` /
  `walk_to_positive_with_prefix`: `compiler.rs` circuit load, `catalog/validation.rs`,
  `runtime/worker.rs:1663`, `ops/reduce/agg.rs:413` AVI, the `seek_by_index` walk):
  a *prefix* scan — seek once to a fixed prefix, then walk forward — not an equal-key
  co-group over two streams. It uses `seek_bytes` internally and stays. A galloping
  *prefix* variant built on `gallop_lower_bound_bytes` is a possible later
  optimization for any caller that drives a sorted sequence of prefixes, but no
  current caller does (each seeks one fixed prefix), so it is out of scope here.
- **`op_reduce` payload-`GROUP BY` via the group index** (`op_reduce.rs:504-529`):
  the trace is gathered through the GI in GI order, **not** trace order, so the
  per-source-PK re-seek at `op_reduce.rs:520` is genuinely non-monotone (the comment
  there says so) and cannot become a forward `advance_to`. This path keeps its
  bespoke scatter-gather; it is not a two-stream co-group.
- **Range / band join**: matches by inequality on the range slot within an eq-group,
  a different rule than equal-key co-group. It is the sibling primitive `merge_band`
  (`range_merge_walk`, already landed), which switches its linear pointer advance and
  the per-row `range_per_row_seek` to `advance_to` (note: the `range_per_row_seek`
  doc-comment at `ops/join.rs:450` anticipates a `seek_bytes_forward` name — update
  it to `advance_to`, the form this plan actually introduces). The selector there
  (`op_join_delta_trace_range`, `ops/join.rs:410`) is a genuine fan-out decision, not
  the equi-join's redundant one, so it stays.

## Illegal states and the type system

Monotonicity does not earn a marker type: forward-only `advance_to` makes a
non-monotone probe unrepresentable already, and it is never-worse even when misused,
so there is nothing left to forbid. The illegal state actually worth a type is the
one the comments guard by convention (the double-sign-flip on a re-encoded signed PK
in the catalog seek paths; the 16-byte truncation of two wide PKs sharing a prefix in
`storage/shard_reader.rs`): **the match key must be the probed source's OPK bytes at
its exact `pk_stride`.** A stride/encoding-tagged `PkKey` minted only by the OPK
encoder would make a wrong-width or wrong-encoding probe a compile error. That is a
larger, separate change, but the co-group design is its natural substrate — one key
type flows through `advance_to` and both skeletons.

## Why this is correct

- **Match-set identity.** `cogroup_intersection` emits at exactly the keys present on
  both sides; `cogroup_left` at every delta key with its (possibly empty) match
  group. Both present the full delta group `[i, j)` and the full match group
  (contiguous, since both streams are sorted), so the per-key product / fold /
  sign-change the callback computes is the same multiset the row-at-a-time loops
  produce, including multiset duplicates (adjacent equal-key rows). For `(PK,
  payload)`-identity operators the callback's inner payload sub-merge reproduces the
  exact element-level matching the current `compare_cursor_payload_to_batch_row` /
  payload-aware DD loops do.
- **`advance_to` equals the from-scratch lower bound.** `gallop_lower_bound_bytes`
  returns `binary_lower_bound(0, count, key)` for every `(key, hint)` (case analysis
  above), so the skeletons land on the same rows regardless of which path ran.
- **Ghosts and multi-source drive unchanged.** `ReadCursor::advance_to` reuses
  `skip_ghosts` and `rebuild_and_drive` per source; only the per-source lower-bound
  index differs, and it equals the from-scratch result. A ghost run between two keys
  overshoots the hint, the bounded `[0, hint)` fallback runs (still correct), and
  `skip_ghosts` re-advances to the same live row.
- **Uncompacted-trace weights.** The match group may carry weight ≤ 0; the callback
  applies the operator's existing weight rule (product with `w_out != 0` suppression
  for joins; positive-weight presence for semi/anti/outer; fold for reduce;
  sign-change for distinct) — unchanged.
- **Adaptivity is automatic, not heuristic.** `cogroup_intersection`'s cost is
  bounded by the smaller side's matches plus galloping skips, so it never does the
  work the old selector tried to avoid, and never mispredicts.

## Tests

- **Helper.** `gallop_lower_bound_bytes(count, key, hint, get) ==
  binary_lower_bound(0, count, key, get)` over a small sorted array, sweeping `hint`
  across `0..=count` and `key` across present / absent-between / below-min / above-max
  / duplicate values; cover the gallop branch, the `O(1)` boundary-at-hint return,
  `hint == count` run-off, the overshoot fallback, and `count == 0`. Reuse the
  existing `find_lower_bound_bytes` oracle tests for the `binary_lower_bound`
  extraction.
- **Skeletons.** `cogroup_intersection` and `cogroup_left` over random sorted
  delta/match pairs visit exactly the expected `(key, delta-range, match-group)`
  triples vs a naive reference; cover empty delta, empty match, no shared keys, all
  shared, duplicate keys on each side, a match group absent for a delta key (left),
  a multi-source ghosted trace, and — for both `M = ReadCursor` and `M = BatchCursor`
  — that the same triples come out (the generic skeleton must not behave differently
  per match-stream impl).
- **Operators (differential vs the pre-merge implementations on a recorded oracle).**
  `op_distinct` (including same-PK / different-payload groups), inner / outer / semi /
  **anti** join, the three **delta-delta** joins, and natural-PK `op_reduce` /
  `op_gather_reduce` produce byte-identical consolidated output — same `(PK, payload,
  weight)` multiset — over inputs with: huge delta + tiny match and the reverse (both
  former selector regimes), multiset deltas, multi-payload groups, outer/anti rows
  with no match, and positive/negative net weights. Add a `op_reduce` case whose
  `trace_out` retraction read straddles many groups, confirming the `advance_to`
  retraction path matches the `seek_group` one. The GI reduce path and the point
  lookups keep their existing tests unchanged.
- **E2E.** `make e2e` (`GNITZ_WORKERS=4`) across the join / distinct / aggregate
  suites stays green; add one epoch per affected operator whose delta straddles the
  old size boundary, confirming the single co-group path matches the Python reference
  in both regimes.

## Migration order

1. `binary_lower_bound` / `gallop_lower_bound_bytes` in `columnar`, `advance_to` on
   `Batch` / `MappedShard` / `ReadCursor`, with the helper and cursor unit tests.
   Self-contained; no caller changes yet.
2. `SortedKeyStream` + `BatchCursor`, `cogroup_intersection` / `cogroup_left` with
   skeleton tests (both match-stream impls).
3. Port the inner / semi / outer / **anti** delta-trace joins to the skeletons;
   delete the selectors, `join_dt_swapped`, `join_dt_merge_walk`, `semi_join_dt_swapped`,
   `dt_emit_indices`, and the outer special case. Differential tests + the join E2E.
4. Port `op_distinct` (with the payload sub-merge), then `op_reduce` /
   `op_gather_reduce` (natural-PK history **and** the `trace_out` retraction read).
   Their differential tests + E2E.
5. Port the three delta-delta joins to the skeleton via `BatchCursor`. Differential
   tests + E2E.
6. Port `resolve_source_pks` / `gather_family_bytes` to the `advance_to` exact-live
   loop.
7. Switch `range_merge_walk` / `range_per_row_seek` to `advance_to` and fix the
   `seek_bytes_forward` comment (`plans/range-join-probe-loop.md`, Strategy 1),
   retiring the last from-scratch probe in a sorted-stream merge.
