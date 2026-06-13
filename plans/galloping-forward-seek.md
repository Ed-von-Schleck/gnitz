# Forward-galloping seek for monotone ascending PK sweeps

## Problem

Several paths resolve a sorted, ascending list of source PKs by driving one
cursor (or binary-searching one batch) with a sequence of lower-bound probes,
one per PK:

- `CatalogEngine::resolve_source_pks` (`catalog/store.rs:989`) — the index-seek
  resolution sweep, the single chokepoint for both `seek_by_index` (`:971`) and
  `seek_by_index_range` (`:1136`). Its callers sort the collected PKs
  (`pks.sort_unstable()` at `:969` / `:1135`) and it asserts ascending order
  (`debug_assert!` at `:996`), then loops `seek_exact_live` over them.
- `CatalogEngine::gather_family_bytes` (`catalog/store.rs:877`) — batched FK
  point lookups, fed a master-sorted ascending `&[PkBuf]` (`worker.rs:921`:
  scatter preserves the master's global PK sort). Loops `seek_exact_live`.
- `semi_join_dt_swapped` (`ops/join.rs:160`, probe at `:193`) and
  `join_dt_swapped` (`:326`, probe at `:347`) — the trace cursor walks ascending
  and each step binary-searches the *other* side (`consolidated` / `delta`,
  both PK-sorted batches) by the monotone non-decreasing trace key via
  `Batch::find_lower_bound_bytes`.

`resolve_source_pks` and `gather_family_bytes` reach the binary search through
`ReadCursor::seek_exact_live` (`storage/read_cursor.rs:533`), which calls
`seek_bytes` (`:518`) → `CursorState::seek_bytes` (`:324`) →
`CursorSource::find_lower_bound_bytes` (`:160`) →
`Batch::find_lower_bound_bytes` (`storage/batch.rs:989`) /
`MappedShard::find_lower_bound_bytes` (`storage/shard_reader.rs:515`). The two
join probes call `Batch::find_lower_bound_bytes` directly. Every variant
re-binary-searches the **full** `[0, count)` of its source:

```rust
let mut lo = 0usize;
let mut hi = self.count;          // ← every probe restarts at the whole source
while lo < hi {
    let mid = lo + (hi - lo) / 2;
    if self.get_pk_bytes(mid) < key { lo = mid + 1; } else { hi = mid; }
}
lo
```

The cursor's per-source `position` (and the join probe's previous result)
already sits at the prior lower bound, but the search ignores it. For K
ascending probes over a source of N rows the cost is `O(K · log N)` even though
the boundary only ever moves forward; restarting at the midpoint of the whole
source also defeats page locality on a large mmap'd shard.

A hint-seeded galloping (exponential) search bounds each probe to the distance
actually advanced: `O(K · log(N/K))` per source, and probes stay near the live
position so shard pages and merge state stay hot. When consecutive probe keys
land in the same inter-row gap, or the sweep has run past the end of the source,
the hint is *exactly* the answer and the probe is `O(1)`.

## Design

Add a hint-seeded lower-bound variant alongside the existing one (the existing
`find_lower_bound_bytes` stays a from-scratch `[0, count)` search — it has
genuine random-access point-lookup callers in `storage/memtable.rs`
(`run_exact_match_start_bytes:30`) and `storage/shard_index.rs` (`:123`) that
must not change). Thread it up through a new `ReadCursor::seek_bytes_forward`
and `seek_exact_live_forward`, point the two catalog sweeps at the latter, and
point the two join probes at the Batch variant directly.

The variant is correct for **any** hint: if the hint sits at or past the
boundary it answers from `[0, hint]` (in `O(1)` when the boundary *is* the
hint). So misuse on a non-monotone caller forfeits only the speedup, never
correctness — which keeps `gather_family_bytes`'s "ascending is a perf hint, not
a correctness requirement" contract intact.

### Shared search helper

The two `find_lower_bound_bytes` bodies (`batch.rs:989`, `shard_reader.rs:515`)
are byte-identical; both collapse onto a shared `binary_lower_bound`, and the
galloping variant builds on it. Place both free fns in `storage/columnar.rs`
next to `compare_pk_bytes` (`:146`) — pure byte-search utilities with no schema
dependency.

```rust
// storage/columnar.rs

/// Lower bound (first index with `pk_bytes >= key`) over the half-open
/// `[lo, hi)` window. `get(i)` yields row `i`'s OPK PK bytes (exactly
/// `pk_stride` wide); memcmp order equals typed order at every PK width. The
/// named `'a` is load-bearing: `Fn(usize) -> &[u8]` would desugar to a
/// higher-ranked `for<'r>` bound the `|i| self.get_pk_bytes(i)` closures (whose
/// result borrows `self`) cannot satisfy.
#[inline]
pub fn binary_lower_bound<'a>(
    mut lo: usize,
    mut hi: usize,
    key: &[u8],
    get: &impl Fn(usize) -> &'a [u8],
) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if get(mid) < key { lo = mid + 1; } else { hi = mid; }
    }
    lo
}

/// Lower bound over `[0, count)`, optimized for a `hint` at or before the
/// boundary. Seeking monotonically ascending keys and passing the previous
/// result as `hint` turns each probe into `O(log gap)` (`gap` = rows advanced),
/// or `O(1)` when the boundary is exactly the hint — including a sweep that has
/// run past the end (`hint == count`) or consecutive keys that fall in one
/// inter-row gap. Correct for ANY hint: an overshooting hint (boundary strictly
/// before it) falls back to a bounded `[0, hint)` search, so a non-monotone
/// caller loses only the speedup.
#[inline]
pub fn gallop_lower_bound_bytes<'a>(
    count: usize,
    key: &[u8],
    hint: usize,
    get: impl Fn(usize) -> &'a [u8],
) -> usize {
    let h = hint.min(count);
    // `get(h) < key`: boundary strictly after the hint. Gallop forward,
    // doubling the step, to bracket it in `(lo, hi]` with `O(log gap)` probes.
    if h < count && get(h) < key {
        let mut lo = h;                 // invariant: get(lo) < key
        let mut step = 1usize;
        while lo + step < count && get(lo + step) < key {
            lo += step;
            step *= 2;
        }
        let hi = (lo + step).min(count); // get(hi) >= key, or hi == count
        return binary_lower_bound(lo + 1, hi, key, &get);
    }
    // Boundary at or before the hint (incl. `h == count`: the sweep ran past the
    // source). One probe back resolves the common monotone case — boundary
    // exactly `h` — in O(1); a genuine overshoot pays a bounded `[0, h)` search.
    if h == 0 || get(h - 1) < key {
        return h;
    }
    binary_lower_bound(0, h, key, &get)
}
```

Both source methods become thin wrappers (existing `find_lower_bound_bytes`
delegates to the shared binary search; the new `_from` adds the hint):

```rust
// storage/batch.rs  (and identically MappedShard in storage/shard_reader.rs,
// swapping `self.count` / `self.get_pk_bytes`)
pub fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
    columnar::binary_lower_bound(0, self.count, key, &|i| self.get_pk_bytes(i))
}

pub fn find_lower_bound_bytes_from(&self, key: &[u8], hint: usize) -> usize {
    columnar::gallop_lower_bound_bytes(self.count, key, hint, |i| self.get_pk_bytes(i))
}
```

### Cursor plumbing

Mirror the existing `find_lower_bound_bytes` dispatch and `seek_bytes`,
forwarding the per-source `position` as the hint. Both catalog sweeps reach the
seek through `seek_exact_live`, which gates the result on exact-PK match and
positive weight — so the forward path needs its own `seek_exact_live_forward`
wrapper; a bare `seek_bytes_forward` at those call sites would drop the gate and
copy the wrong row (or a tombstone) on an absent PK.

```rust
// storage/read_cursor.rs — CursorSource, beside find_lower_bound_bytes (:160)
fn find_lower_bound_bytes_from(&self, key: &[u8], hint: usize) -> usize {
    match self {
        CursorSource::Batch(b) => b.find_lower_bound_bytes_from(key, hint),
        CursorSource::Shard(s) => s.find_lower_bound_bytes_from(key, hint),
    }
}

// CursorState, beside seek_bytes (:324) — seed the search at the live position.
fn seek_bytes_forward(&mut self, src: &CursorSource, key: &[u8]) {
    self.position = src.find_lower_bound_bytes_from(key, self.position);
    self.skip_ghosts(src);
}

// ReadCursor, beside seek_bytes (:518).
/// Like [`seek_bytes`], but seeds each source's lower-bound search at its
/// current position. Amortizes to `O(log gap)` per source (or `O(1)` when the
/// boundary is unchanged) under monotonically non-decreasing keys; falls back to
/// a bounded search (still correct) on any key that lands before a source's
/// position.
pub fn seek_bytes_forward(&mut self, key: &[u8]) {
    for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
        state.seek_bytes_forward(src, key);
    }
    self.rebuild_and_drive();
}

/// Forward-seeking sibling of [`seek_exact_live`] (:533): same exact-PK +
/// positive-weight gate, seeded at the live position. The gate reads the current
/// row, not `position`, so it is unaffected by which lower-bound path ran.
pub fn seek_exact_live_forward(&mut self, key: &[u8]) -> bool {
    self.seek_bytes_forward(key);
    self.valid && self.current_pk_eq(key) && self.current_weight > 0
}
```

### Call sites

Cursor sweeps swap `seek_exact_live` → `seek_exact_live_forward`:

```rust
// resolve_source_pks (store.rs:1003): pks are caller-sorted ascending (asserted).
if src_cursor.cursor.seek_exact_live_forward(pk.pk_bytes()) { … }

// gather_family_bytes (store.rs:890): pks arrive master-sorted ascending.
if cursor.cursor.seek_exact_live_forward(pk.pk_bytes()) { … }
```

The two join probes use the Batch primitive directly — no cursor plumbing.
The trace cursor walks ascending, so the probe key is monotone non-decreasing;
thread the previous result as the hint:

```rust
// semi_join_dt_swapped (join.rs:193) and join_dt_swapped (join.rs:347):
// `hint` starts at 0, updated to `pos` after each probe.
let pos = consolidated.find_lower_bound_bytes_from(t_pk_bytes, hint);
hint = pos;
```

`join_dt_swapped` admits duplicate trace PKs (no dedup), so consecutive probe
keys can be equal; `hint == pos` then hits the helper's `O(1)` boundary-at-hint
path. `semi_join_dt_swapped` dedups, so its keys are strictly increasing.

A further consumer lives outside this plan: the range / band join probe
(`op_join_delta_trace_range`, `ops/join.rs:403`, cursor seek at `:445`) seeks
the trace by a monotone non-decreasing `start`. `plans/range-join-probe-loop.md`
swaps that cursor seek to `seek_bytes_forward` once this primitive lands.
`join_dt_merge_walk`'s seek (`:287`) is deliberately *not* a candidate: it
re-seeks *backward* to a duplicate delta PK after the cursor advanced past the
group, so its hint would always overshoot.

## Why this is correct

- **Forward case (ghost-free source — every batch; a compacted shard).** After
  a probe for `k`, `position = lower_bound(k)` (no ghosts to skip). The next key
  `k' ≥ k`, and `lower_bound` is monotone in the key, so
  `lower_bound(k') ≥ position`. The helper's `get(h) < key` gallop branch runs
  when `lower_bound(k') > position`, and the `O(1)` boundary-at-hint return runs
  when `lower_bound(k') == position`; both yield the same index a full search
  would.
- **Forward case (shard with ghosts).** `skip_ghosts` advances `position` to
  `next_non_ghost(lower_bound(k))`, which may sit *past* `lower_bound(k')` when a
  ghost run separates `k` and `k'`. The hint then overshoots, the bounded
  `[0, hint)` fallback runs (still correct), and `skip_ghosts` re-advances to the
  same live row. Galloping never changes *which* row is found, only how fast —
  and the overshoot is bounded by one back-probe plus a `[0, hint)` search.
- **Overshoot / non-monotone case.** If `get(hint) ≥ key` the boundary is in
  `[0, hint]`: the helper returns `hint` directly when `get(hint-1) < key`
  (boundary is exactly `hint`), else `binary_lower_bound(0, hint, …)`. When
  `hint ≥ count` the same logic over `h = count` returns `count` iff every row
  is `< key`. So the result equals `find_lower_bound_bytes(key)` for every input.
- **Exact-live gate unchanged.** `seek_exact_live_forward` reuses the
  `valid && current_pk_eq && current_weight > 0` predicate verbatim; it reads the
  current row, not `position`, so it is identical regardless of which lower-bound
  path ran. An absent PK leaves `position` at `lower_bound(k)` (the first row
  `> k`), a valid hint for the next `k' ≥ k`.
- **Ghosts and multi-source drive unchanged.** `seek_bytes_forward` reuses
  `skip_ghosts` and `rebuild_and_drive` verbatim, per source; only the
  per-source lower-bound index differs, and it equals the from-scratch result.
- **Existing `find_lower_bound_bytes` untouched semantically.** It still searches
  `[0, count)`; the only change is delegating to `binary_lower_bound`, a
  byte-for-byte-equivalent extraction of its current body.

## Tests

- Unit (helper): `gallop_lower_bound_bytes(count, key, hint, get)` equals
  `binary_lower_bound(0, count, key, get)` for every `(key, hint)` over a small
  sorted byte array — sweep `hint` across `0..=count` and `key` across present,
  absent-between, below-min, and above-max values. Must cover the gallop branch,
  the `O(1)` boundary-at-hint return (`hint == lower_bound(key)`), the
  `hint == count` run-off-end (every key past it returns `count`), the genuine
  overshoot fallback (`hint > lower_bound(key)`), repeated/duplicate keys, and
  `count == 0`.
- Unit (cursor): on a multi-shard table (including a shard with ghost rows), a
  sequence of ascending `seek_bytes_forward` calls lands on the same rows
  (`current_pk_bytes`, `current_weight`) as the same sequence of plain
  `seek_bytes`; and `seek_exact_live_forward` matches `seek_exact_live` over a
  mix of present and absent PKs.
- Unit (anti-regression): a *descending* / random sequence of
  `seek_bytes_forward` calls still matches `seek_bytes` (exercises the overshoot
  fallback).
- Unit (join probes): `semi_join_dt_swapped` and `join_dt_swapped` produce
  byte-identical output with the threaded hint as they did with the
  from-scratch probe, over inputs with duplicate trace PKs and trace keys that
  miss the probed batch entirely (both sides of the boundary-at-hint path).
- Reuse the existing `find_lower_bound_bytes` oracle tests (`storage/batch.rs`,
  `storage/shard_reader.rs`) unchanged — they now cover the `binary_lower_bound`
  extraction.
- Perf: a ≥ 1M-row index range scan (`resolve_source_pks`) and a large-K
  `gather_family_bytes` on a multi-shard table, before/after — expect a
  wall-clock win from bounded probes and page locality; confirm no regression
  when matches are sparse (the boundary-at-hint `O(1)` path covers the
  many-misses case, and gallop degrades to a bounded search otherwise).

## Precondition

The ascending order the gallop branch relies on already holds in the tree, with
no pending dependency: `seek_by_index` / `seek_by_index_range` sort the
collected PKs before `resolve_source_pks` (which asserts it), `gather_family_bytes`
is fed the master's global PK sort, and both join probes are driven by a
PK-ascending trace cursor.
