# Forward-galloping seek for monotone ascending PK sweeps

## Problem

Two paths resolve a sorted, ascending list of source PKs by driving one cursor
with a sequence of `seek_bytes` calls, one per PK:

- `CatalogEngine::resolve_collected_pks_into` (`catalog/store.rs`) — the
  index-seek resolution sweep (see `index-range-sorted-pk-resolution.md`, whose
  sort produces the ascending key order this optimization needs).
- `CatalogEngine::gather_family_bytes` (`catalog/store.rs:885`) — batched FK
  point lookups, fed a master-sorted ascending `&[PkBuf]`.

Each `seek_bytes` re-binary-searches the **full** `[0, count)` of every source.
`ReadCursor::seek_bytes` (`storage/read_cursor.rs:518`) loops the sources and
calls `CursorState::seek_bytes` (`:323`), which calls
`CursorSource::find_lower_bound_bytes` (`:160`) → `Batch::find_lower_bound_bytes`
(`storage/batch.rs:989`) / `MappedShard::find_lower_bound_bytes`
(`storage/shard_reader.rs:515`):

```rust
let mut lo = 0usize;
let mut hi = self.count;          // ← every seek restarts at the whole source
while lo < hi {
    let mid = lo + (hi - lo) / 2;
    if self.get_pk_bytes(mid) < key { lo = mid + 1; } else { hi = mid; }
}
lo
```

`CursorState.position` already sits at the previous seek's result, but
`find_lower_bound_bytes` ignores it. For K ascending seeks over a source of N
rows the cost is `O(K · log N)` even though the probes only ever move forward;
restarting at the midpoint of the whole source also defeats page locality on a
large mmap'd shard.

A hint-seeded galloping (exponential) search bounds each probe to the distance
actually advanced: `O(K · log(N/K))` per source, and probes stay near the live
position so shard pages and merge state stay hot.

## Design

Add a hint-seeded lower-bound variant alongside the existing one (the existing
`find_lower_bound_bytes` stays a from-scratch `[0, count)` search — it has
random-access callers in `ops/join.rs`, `storage/memtable.rs`, and
`storage/shard_index.rs` that must not change). Thread it up through a new
`ReadCursor::seek_bytes_forward`, and point the two ascending sweeps at it.

The variant is correct for **any** hint: if the hint overshoots the key (the
boundary lies before it), it falls back to a bounded `[0, hint]` search. So
misuse on a non-monotone caller forfeits only the speedup, never correctness —
which keeps `gather_family_bytes`'s existing "ascending is a perf hint, not a
correctness requirement" contract intact.

### Shared search helper

The two existing `find_lower_bound_bytes` bodies are byte-identical; both
collapse onto a shared `binary_lower_bound`, and the galloping variant builds on
it. Place both free fns in `storage/columnar.rs` next to `compare_pk_bytes`
(`:146`) — pure byte-search utilities with no schema dependency.

```rust
// storage/columnar.rs

/// Lower bound (first index with `pk_bytes >= key`) over the half-open
/// `[lo, hi)` window. `get(i)` yields row `i`'s OPK PK bytes (exactly
/// `pk_stride` wide); memcmp order equals typed order at every PK width.
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
/// result as `hint` turns each seek from `O(log count)` into `O(log gap)`,
/// `gap` = rows advanced. Correct for ANY hint: an overshooting hint (boundary
/// before it) falls back to a bounded `[0, hint]` search, so a non-monotone
/// caller loses only the speedup.
#[inline]
pub fn gallop_lower_bound_bytes<'a>(
    count: usize,
    key: &[u8],
    hint: usize,
    get: impl Fn(usize) -> &'a [u8],
) -> usize {
    let h = hint.min(count);
    // Boundary at or before the hint (or empty tail): answer is in `[0, h]`.
    // `get(h) >= key` ⇒ first-`>=` is `<= h`; search the bounded prefix.
    if h == count || get(h) >= key {
        return binary_lower_bound(0, h, key, &get);
    }
    // `get(h) < key`: gallop forward, doubling the step, to bracket the
    // boundary in `(lo, hi]` with `O(log gap)` probes.
    let mut lo = h;                 // invariant: get(lo) < key
    let mut step = 1usize;
    while lo + step < count && get(lo + step) < key {
        lo += step;
        step *= 2;
    }
    let hi = (lo + step).min(count); // get(hi) >= key, or hi == count
    binary_lower_bound(lo + 1, hi, key, &get)
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
forwarding the per-source `position` as the hint:

```rust
// storage/read_cursor.rs — CursorSource, beside find_lower_bound_bytes (:160)
fn find_lower_bound_bytes_from(&self, key: &[u8], hint: usize) -> usize {
    match self {
        CursorSource::Batch(b) => b.find_lower_bound_bytes_from(key, hint),
        CursorSource::Shard(s) => s.find_lower_bound_bytes_from(key, hint),
    }
}

// CursorState, beside seek_bytes (:323) — seed the search at the live position.
fn seek_bytes_forward(&mut self, src: &CursorSource, key: &[u8]) {
    self.position = src.find_lower_bound_bytes_from(key, self.position);
    self.skip_ghosts(src);
}

// ReadCursor, beside seek_bytes (:518).
/// Like [`seek_bytes`], but seeds each source's lower-bound search at its
/// current position. Amortizes to `O(log gap)` per source when called with
/// monotonically non-decreasing keys; falls back to a bounded search (still
/// correct) on any key that lands before a source's position.
pub fn seek_bytes_forward(&mut self, key: &[u8]) {
    for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
        state.seek_bytes_forward(src, key);
    }
    self.rebuild_and_drive();
}
```

### Call sites

Both sweeps swap `seek_bytes` → `seek_bytes_forward`; nothing else changes.

```rust
// resolve_collected_pks_into: pks are caller-sorted ascending.
src_cursor.cursor.seek_bytes_forward(bytes);

// gather_family_bytes: pks arrive master-sorted ascending.
cursor.cursor.seek_bytes_forward(bytes);
```

## Why this is correct

- **Forward case (monotone keys).** After `seek_bytes_forward(k)`,
  `position = next_non_ghost(lower_bound(k))`. The next key `k' ≥ k`, and
  `lower_bound` is monotone in the key, so `lower_bound(k') ≥ lower_bound(k)`.
  The hint is therefore at or before the new boundary, the `get(h) < key` branch
  runs, and galloping returns the same index a full search would.
- **Overshoot / non-monotone case.** If `get(hint) ≥ key`, the boundary is in
  `[0, hint]` and the bounded `binary_lower_bound(0, hint, …)` returns it
  (returns `hint` when every earlier row is `< key`, which is correct because
  `get(hint) ≥ key`). So the result equals `find_lower_bound_bytes(key)` for
  every input — galloping never changes *which* row is found, only how fast.
- **Ghosts and multi-source drive unchanged.** `seek_bytes_forward` reuses
  `skip_ghosts` and `rebuild_and_drive` verbatim; only the per-source
  lower-bound index differs, and it is identical to the from-scratch result.
- **Existing `find_lower_bound_bytes` untouched semantically.** It still
  searches `[0, count)`; the only change is delegating to `binary_lower_bound`,
  a byte-for-byte-equivalent extraction of its current body.

## Tests

- Unit (helper): `gallop_lower_bound_bytes(count, key, hint, get)` equals
  `binary_lower_bound(0, count, key, get)` for every `(key, hint)` over a small
  sorted byte array — sweep `hint` across `0..=count` and `key` across present,
  absent-between, below-min, and above-max values (covers the gallop branch, the
  overshoot fallback, `hint == count`, and `count == 0`).
- Unit (cursor): on a multi-shard table, a sequence of ascending
  `seek_bytes_forward` calls lands on the same rows (`current_pk_bytes`,
  `current_weight`) as the same sequence of plain `seek_bytes` calls.
- Unit (anti-regression): a *descending* / random sequence of
  `seek_bytes_forward` calls still matches `seek_bytes` (exercises the overshoot
  fallback).
- Reuse the existing `find_lower_bound_bytes` oracle tests
  (`storage/batch.rs`, `storage/shard_reader.rs`) unchanged — they now cover the
  `binary_lower_bound` extraction.
- Perf: a ≥ 1M-row index range scan (`resolve_collected_pks_into`) and a
  large-K `gather_family_bytes` on a multi-shard table, before/after — expect a
  wall-clock win from bounded probes and page locality; confirm no regression
  when matches are sparse (gallop degrades to a full search via the fallback).

## Dependency

Builds on the sorted ascending sweep from
`index-range-sorted-pk-resolution.md`: that plan's sort is what makes the index
path's keys monotone, the precondition for the gallop branch to engage.
`gather_family_bytes` is already fed ascending by its master-side sort.
