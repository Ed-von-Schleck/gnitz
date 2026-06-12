# Normalize the index range walk to a half-open key interval

## Goal

Remove the lo/hi inclusivity semantics from the engine's range walk by
normalizing both bounds **once**, at the top of
`CatalogEngine::seek_by_index_range`, into a half-open byte-key interval
`[start, end)` over the full index-PK space. After normalization the walk is a
single uniform iterator — seek to `start`, advance while `key < end` — with no
per-row inclusivity classification, no sentinel reasoning, and no defensive
boundary trims. The lo/hi *distinction* (an interval has two ends) survives only
where it is information-bearing: the SQL operator (`>` vs `<`), the wire
descriptor, and the two `Bound<u128>` parameters. The lo/hi *machinery* —
`lo_excl`/`hi_excl`, the dual prefix buffers, the 0xFF seed trick, and the two
per-row trim branches — is deleted.

This is the same normalization every LSM iterator API converges on
(RocksDB `iterate_lower_bound`/`iterate_upper_bound`): inclusivity is a
property of how a cut point is *computed*, not of how the scan *runs*.

A second, smaller unification falls out of the same review: the planner's two
mirror-image bound-saturation blocks in `collect_index_range_candidates`
collapse into one side-aware helper.

## Background — current state (verified against code)

`CatalogEngine::seek_by_index_range` (`crates/gnitz-engine/src/catalog/store.rs:1018-1136`)
takes `eq_natives: &[u128]`, `lo: Bound<u128>`, `hi: Bound<u128>` and currently:

1. Builds **two** prefix buffers of `prefix_len = eq_size + slot_size` bytes:
   `lo_prefix` (eq OPK ‖ lower-bound OPK slot, `0x00` slot when `Unbounded`) and
   `hi_prefix` (clone of the eq part ‖ upper-bound OPK slot, `0xFF` slot when
   `Unbounded`) — store.rs:1062-1083.
2. Extracts `lo_excl`/`hi_excl` flags from the `Bound` shapes (store.rs:1049-1050).
3. Seeds the seek key as `lo_prefix ‖ 0x00*suffix`, flipping the suffix to
   `0xFF` for an exclusive lower bound so the binary search lands past the
   boundary duplicate group in O(log N) (store.rs:1100-1104).
4. Walks with **two three-way compares per row** (store.rs:1106-1127):
   - upper trim: `cur > hi_prefix → break`, `cur == hi_prefix && hi_excl → break`;
   - lower trim: `cur < lo_prefix → skip` (provably dead — `seek_bytes` is a
     lower-bound seek, so it lands at `key ≥ seek_key`, hence
     `prefix(key) ≥ lo_prefix`), and `cur == lo_prefix && lo_excl → skip`,
     whose only live case is the boundary-group entry whose **source-PK suffix
     is itself all-`0xFF`** (e.g. a U64 source PK of `u64::MAX`) — the one key
     the `0xFF`-suffixed seed cannot get past. The 13-line comment block at
     store.rs:1115-1121 exists solely to justify keeping both.

The correctness substrate is the post-`cff7c58` OPK invariant: the index PK
region is `[promoted leading-key OPK ‖ source-PK OPK]`, and memcmp order on
those bytes equals typed order (signed and composite included). Everything
below builds only on that.

Three facts make the half-open normalization exact:

- **Lower-bound seek.** `cursor.seek_bytes(k)` positions at the first entry
  with `key ≥ k` (read_cursor.rs:518; every source does a lower-bound seek,
  then `rebuild_and_drive` picks the minimum).
- **Group containment.** For any `prefix_len`-byte group key `p`, every full
  key `k` with `k[..prefix_len] == p` satisfies
  `p‖0x00…  ≤  k  <  succ(p)‖0x00…`, where `succ(p)` is the byte-string
  successor of `p` at fixed width (`p + 1` with carry). When `p` is all-`0xFF`
  the successor does not exist and the group is the last one in the table.
- **Order extension.** Full-key memcmp order refines group-prefix order, so
  cutting the key space at `p‖0x00…` / `succ(p)‖0x00…` includes or excludes
  whole groups with no per-row prefix slicing.

## Design

### Bound → cut-point mapping

Let `eq_key` = the `eq_size`-byte OPK of the equality prefix (empty when
`n_eq == 0`), `group(v)` = `eq_key ‖ opk_slot(v)` (`prefix_len` bytes, via
`index_opk_prefix` exactly as today), `pad(p)` = `p` zero-extended to
`idx_pk_stride` bytes, and `succ(p)` = fixed-width byte successor
(`None` when `p` is all-`0xFF`; for the empty string, `None`).

| bound           | cut point                                                            |
|-----------------|----------------------------------------------------------------------|
| lo `Unbounded`  | `start = pad(eq_key)`                                                |
| lo `Included(v)`| `start = pad(group(v))`                                              |
| lo `Excluded(v)`| `start = pad(succ(group(v)))`; `succ` fails → **return `Ok(None)`** (excluding the maximal group leaves nothing above it) |
| hi `Excluded(v)`| `end = Some(pad(group(v)))` (the group's first key is already out)   |
| hi `Included(v)`| `end = succ(group(v)).map(pad)`; `succ` fails → `None` = scan to table end (the group is the last representable one) |
| hi `Unbounded`  | `end = succ(eq_key).map(pad)`; `n_eq == 0` ⇒ empty `eq_key` ⇒ `None` = scan to table end |

The walk becomes:

```rust
idx_cursor.cursor.seek_bytes(&start);
while idx_cursor.cursor.valid
    && end.as_deref().is_none_or(|e| idx_cursor.cursor.current_pk_bytes() < e)
{
    if idx_cursor.cursor.current_weight > 0 {
        Self::resolve_index_entry_into(
            idx_cursor.cursor.current_pk_bytes(), idx_key_size, src_pk_stride,
            &mut src_cursor, &mut acc);
    }
    idx_cursor.cursor.advance();
}
```

One full-key slice compare per row (`idx_pk_stride` bytes, vs. today's two
three-way compares over `prefix_len` bytes); zero compares when `end` is
`None`.

### Why each deleted piece is subsumed

- **`lo_excl`/`hi_excl` + both trim branches**: inclusivity is consumed by the
  cut-point mapping. The exclusive-lower live case (boundary-group entry with
  all-`0xFF` source-PK suffix) is impossible by construction —
  `start = succ(group(v))‖0x00…` is strictly greater than *every* key of the
  boundary group, including the all-`0xFF`-suffix one. The dead
  `cur < lo_prefix` branch and its justification comment go with it.
- **`hi_prefix` and the `0xFF` slot sentinel for `Unbounded`**: the
  equality-group termination and the upper bound become the *same* mechanism
  (the `end` key). `hi = Unbounded` with `n_eq > 0` no longer fakes an
  inclusive maximal slot; it cuts at `succ(eq_key)` — the first key of the
  next equality group — directly.
- **The `0xFF` seed trick** (store.rs:1091-1104 incl. the 9-line comment): the
  O(log N) duplicate-group skip is preserved and sharpened. The seed used to
  land *at or before* the first in-range key (possibly on the boundary
  group's last entry, then trimmed); `start` now *is* the first in-range key
  position.
- **Forced-inclusivity at sentinels**: already deleted in the `Bound`
  migration; the mapping keeps it impossible — `Unbounded` has no inclusivity
  bit to mis-handle, and a value whose OPK slot is exactly `0x00`/`0xFF` is an
  ordinary group key here.

### Ripple-into-eq-prefix is handled for free

`succ` runs at full `prefix_len` (or `eq_size`) width, so a carry out of the
range slot ripples into the equality bytes. Example: index `(a, b)`, query
`a = 7 AND b > u64::MAX`. `group(u64::MAX) = opk(7) ‖ 0xFF*8`, so
`start = opk(8) ‖ 0x00…` — and `end = succ(opk(7)) ‖ 0x00… = opk(8) ‖ 0x00…`.
`start == end`, the `<` condition fails on the first visited key, empty
result, no special case. Only a carry out of the **entire** prefix (all bytes
`0xFF`) needs handling, and the table above pins both occurrences: early
`Ok(None)` on the lower side, `end = None` on the upper side.

### The `succ` helper

```rust
/// Fixed-width byte-string successor: `p + 1` with carry, in place. Returns
/// false when `p` is all-0xFF (or empty) — no successor exists at this width.
fn increment_key_in_place(p: &mut [u8]) -> bool {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_add(1);
        if *b != 0 { return true; }
    }
    false
}
```

Private to `store.rs` (single consumer). The empty-slice case returning
`false` is load-bearing: it is exactly the `n_eq == 0, hi == Unbounded` →
scan-to-end row of the mapping table.

### Planner: one saturation helper instead of two mirror blocks

`collect_index_range_candidates` (`crates/gnitz-sql/src/dml.rs:1192-1211`)
saturates the two chosen ends with two 8-line `match` blocks that are mirror
images — `BelowMin`/`AboveMax` swap roles between the lower and the upper
block. The rule is direction-generic: *a literal past the type range on the
side pointing **away** from the interval widens to `Unbounded`; one pointing
**across** the interval proves it empty.* `RangeEnd` already carries its
`side`, so:

```rust
/// Map a chosen range end to its scan bound, saturating an out-of-type-range
/// literal: past the type range away from the interval (below a lower end,
/// above an upper end) → Unbounded; across it → None (provably empty range).
fn saturated_bound(end: &RangeEnd) -> Option<Bound<u128>> {
    match (end.bound, end.side) {
        (RangeLit::In(v), _) =>
            Some(if end.incl { Bound::Included(v) } else { Bound::Excluded(v) }),
        (RangeLit::BelowMin, RangeSide::Lower)
        | (RangeLit::AboveMax, RangeSide::Upper) => Some(Bound::Unbounded),
        _ => None,
    }
}
```

Call site replaces both blocks:

```rust
let mut empty = false;
let mut resolve = |idx: Option<usize>| match idx {
    None    => Bound::Unbounded,
    Some(i) => saturated_bound(&ends[i].end)
        .unwrap_or_else(|| { empty = true; Bound::Unbounded }),
};
let lo = resolve(lower_idx);
let hi = resolve(upper_idx);
```

(`empty = true` leaves the bound value irrelevant — `execute_select`
short-circuits on `cand.empty` before reading `lo`/`hi`.)

## The change, by file

### 1. `crates/gnitz-engine/src/catalog/store.rs`

In `seek_by_index_range` (currently :1018-1136), unchanged signature:

- Keep: the table/index lookup, the `n_eq >= col_indices.len()` self-guard,
  the stride/size derivation block, the `IndexKeySpec::seek_prefix` eq-encode
  (with its `n_eq == 0` skip), the cursor setup, `resolve_index_entry_into`,
  and the `Ok((acc.count > 0).then_some(acc))` return.
- Replace the `lo_prefix`/`hi_prefix` construction, `lo_excl`/`hi_excl`, the
  seed block, and the trim-laden loop with:
  - `eq_key: Vec<u8>` (eq_size bytes) from `seek_prefix`;
  - a `group(v)` closure: `eq_key ‖ index_opk_prefix(v, src_type, idx_key_type)[..slot_size]`;
  - the six-row mapping table above producing `start: Vec<u8>` and
    `end: Option<Vec<u8>>` (early `return Ok(None)` on lower-`succ` failure);
  - the four-line walk.
- Add `increment_key_in_place` as a private fn.
- Doc comment: replace the sentinel-inclusivity paragraph with the mapping
  table and the group-containment argument; the OPK-invariant paragraph stays.
- The `use std::cmp` import added for the three-way trims becomes unused —
  remove it (slice `<` suffices).

Net: −2 buffers, −2 flags, −2 per-row branches, −~25 comment lines justifying
the trims/seed; +~10-line helper, +~15-line normalization.

### 2. `crates/gnitz-sql/src/dml.rs`

Add `saturated_bound` next to `RangeEnd`; replace dml.rs:1192-1211 with the
`resolve` call site. The `empty` flag, candidate struct, and everything
downstream are unchanged.

### 3. Untouched, deliberately

- **`gnitz_wire::RangeDescriptor`** and the §2 wire bytes: the descriptor must
  ship *native* bound values — the worker is the sole OPK encoder (trust
  boundary), and cut-point computation requires the OPK encode plus the index
  stride layout, which only the engine has. Inclusivity therefore must cross
  the wire; `Bound<u128>` is its minimal representation.
- **Client/connection/worker signatures**: `(eq_vals, lo, hi)` stays. Grouping
  `lo`/`hi` into one `(Bound<u128>, Bound<u128>)` parameter (it already
  implements `RangeBounds<u128>`) renames the split without removing it;
  revisit only if a shared interval type emerges for the range join.
- **The planner's `RangeSide` / `lower_idx` / `upper_idx`**: the lower/upper
  distinction *is* the information content of `>` vs `<`; it cannot be
  normalized away above the engine.

## Correctness invariants to preserve

- **OPK ordering** (`cff7c58`): the mapping table is valid only because
  memcmp order == typed order on the leading-key region. No new encoder is
  introduced; `index_opk_prefix` and `IndexKeySpec::seek_prefix` remain the
  only encode paths.
- **Worker is the sole OPK encoder**: normalization happens inside the engine
  method, after descriptor decode — nothing about the trust boundary moves.
- **Z-Set multiplicity**: `resolve_index_entry_into` (net `current_weight`,
  never a hardcoded 1) is untouched; the positive-weight gate on index entries
  stays in the walk.
- **NULL exclusion**: unchanged — NULL rows are absent from the index, and the
  interval cut points never synthesize keys for them.
- **Broadcast/merge distribution**: untouched; this is engine-local.
- **`n_eq` self-guard**: keeps `prefix_len ≤ idx_key_size < idx_pk_stride`, so
  `pad` always extends and `group(v)` never exceeds the key.

## Testing

Existing guards that must pass unchanged (they pin behavior, not mechanism):
the nine `test_seek_by_index_range_*` engine tests — in particular
`..._exclusive_lower_large_dup_group`, whose all-`0xFF`-source-PK row is the
defensive trim's only live case today and must now be excluded by the `start`
cut alone — plus the 110 gnitz-sql unit tests, `TestIndexRangeSql` (9 tests,
gnitz-py), and the three `@_NEEDS_MULTI` worker broadcast tests.

New engine tests (`catalog/tests/index_tests.rs`), written **before** the
refactor — all four also pass against the current implementation, so they
bisect cleanly:

1. **Exclusive lower at the type maximum** (lower-`succ` overflow): U64 `x`
   with rows at `x = u64::MAX`; `(&[], Excluded(u64::MAX), Unbounded)` → empty;
   `(&[], Included(u64::MAX), Unbounded)` → exactly the `MAX` rows.
2. **Inclusive upper at the type maximum** (upper-`succ` overflow → scan to
   end): `(&[], Unbounded, Included(u64::MAX))` → every row, including
   `x = u64::MAX`.
3. **Carry ripple into the equality prefix** (`start == end` degenerate
   interval): index `(a, b)`, rows `(7, u64::MAX)` and `(8, 0)`;
   `(&[7], Excluded(u64::MAX), Unbounded)` → empty — the `(8, 0)` row must not
   leak in; `(&[7], Unbounded, Included(u64::MAX))` → the `(7, u64::MAX)` row
   only.
4. **`increment_key_in_place` unit coverage** (direct): carry chain
   (`[0x00, 0xFF] → [0x01, 0x00]`), no-carry, all-`0xFF` → `false`,
   empty → `false`.

Planner: the existing `range_candidate_saturates_out_of_range` covers both
saturation directions through the new helper; add one `saturated_bound` direct
test for the four `(RangeLit, RangeSide)` corners.

## Migration order

1. Add the four new engine tests + the `succ` unit test; run against the
   current walk — green.
2. Replace the walk with the normalization (file change 1); all engine tests
   green, comment deltas included in the same change.
3. Planner helper (file change 2); gnitz-sql tests green.
4. Full sweep: `cargo clippy --workspace --all-targets`,
   `cargo test --workspace`, `make pyext && pytest -m "not slow"`.

## Out of scope

- Changing the wire descriptor, client API shape, or SAL/flag plumbing.
- A general engine-level interval/iterator type. If the range join
  (`wide-pk-incremental-views.md` §1 item 1) wants a reusable
  `[start, end)` index iterator, it should be extracted from this walk *then*,
  with the join's probe loop as the second consumer.
- Bound normalization above the engine (planner emitting cut points): blocked
  by the worker-encodes-OPK trust boundary, not by code shape.
