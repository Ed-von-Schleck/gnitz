# Sort matched source PKs before base-row resolution in index range/prefix seeks

## Problem

`seek_by_index_range` (`catalog/store.rs:1032`) and the prefix mode of
`seek_by_index` (`catalog/store.rs:919`) resolve every positive index entry
immediately via `resolve_index_entry_into` (`catalog/store.rs:982`): one
`src_cursor.cursor.seek_bytes(src_pk)` per entry. Each `seek_bytes` is a full
lower-bound binary search per cursor source plus a merge re-sync
(`storage/read_cursor.rs:518` / `:324`) — there is no positional reuse between
consecutive seeks.

The index walk emits entries in index-PK order
`[leading-key OPK ‖ source-PK OPK]`. Within one duplicate group the source-PK
suffix makes resolution ascending, but a range or prefix scan spans many
groups, so the resolved source PKs are effectively random across the base
table: K matches cost K unclustered seeks — `O(K · S · log N)` (S = sources in
the merge cursor) with hostile cache behavior on large K.

Single-group lookups (full-arity equality seeks, unique seeks) are already
source-PK-ascending and are not the target; they share the helper but gain
only the (cheap, nearly-sorted) sort.

Two further inefficiencies on these paths are independent of the sort but
fixed in the same edit: both methods open the base-table cursor and allocate
the accumulator *before* the walk, so a zero-match scan pays for an
`open_cursor()` (Rc-snapshot clones + tournament-tree build,
`storage/table.rs:665`) and a `Batch` allocation it never uses; and the
accumulator is built with capacity `0`, so it grows row by row during
resolution even though the final row count is known once the walk finishes.

## Design

Split the walk into collect-then-resolve, shared by both methods. Collecting
source PKs first lets the resolve phase sort them; it also lets both seeks
skip base-cursor and accumulator construction entirely when the walk found
nothing, and size the accumulator exactly when it did.

### Shared resolve helper

`resolve_index_entry_into` is deleted; both callers move to this collected
form. Because the source PKs are now owned `PkBuf`s (the index cursor has
advanced past them by resolve time), the helper drops the old `MAX_PK_BYTES`
scratch-copy: `PkBuf` already stores the exact `src_pk_stride` bytes inline
(up to `MAX_PK_BYTES`), so wide (`src_pk_stride > 16`) sources stay correct.

```rust
// catalog/store.rs — associated fn on CatalogEngine, replacing
// `resolve_index_entry_into`. Called by both `seek_by_index` and
// `seek_by_index_range`.
/// Resolve collected source PKs against the base table in ascending PK order.
/// Sorting first turns K random point-seeks into a monotone forward sweep:
/// each `seek_bytes` lower-bounds at or past the previous position's key, so
/// shard pages and the merge state stay hot. The collected PKs are the index
/// entries' source-PK OPK suffixes, whose memcmp order equals the base table's
/// storage order, so the byte sort *is* the seek order. Resolution semantics
/// are identical to the previous per-entry path: present, live, exact-PK rows
/// are appended at their net `current_weight` (never a hardcoded 1, so Z-Set
/// multiplicity is preserved).
fn resolve_collected_pks_into(
    pks: &mut [crate::storage::PkBuf],
    src_cursor: &mut CursorHandle,
    acc: &mut Batch,
) {
    pks.sort_unstable_by(|a, b| a.pk_bytes().cmp(b.pk_bytes()));
    for pk in pks.iter() {
        let bytes = pk.pk_bytes();
        src_cursor.cursor.seek_bytes(bytes);
        if src_cursor.cursor.valid
            && src_cursor.cursor.current_weight > 0
            && src_cursor.cursor.current_pk_bytes() == bytes
        {
            let w = src_cursor.cursor.current_weight;
            src_cursor.cursor.copy_current_row_into(acc, w);
        }
    }
}
```

### Range seeks (`seek_by_index_range`)

Everything up to and including the `start`/`end` cut computation is unchanged.
The cursor/accumulator setup and the walk tail become:

```rust
// One index cursor for the walk; collect each positive match's source PK.
let mut idx_cursor = ic.table_mut().open_cursor();
let mut pks: Vec<crate::storage::PkBuf> = Vec::new();

idx_cursor.cursor.seek_bytes(&start[..idx_pk_stride]);
while idx_cursor.cursor.valid {
    let cur_pk = idx_cursor.cursor.current_pk_bytes();
    if end.is_some_and(|e| cur_pk >= e) { break; }
    if idx_cursor.cursor.current_weight > 0 {
        pks.push(crate::storage::PkBuf::from_bytes(
            &cur_pk[idx_key_size..idx_key_size + src_pk_stride]));
    }
    idx_cursor.cursor.advance();
}

// Zero matches: skip base-cursor construction (Rc-snapshot clones +
// tournament-tree build in `open_cursor`) and the accumulator allocation.
if pks.is_empty() {
    return Ok(None);
}

// Base table holds unique PKs and each row carries one indexed value, so the
// collected PKs are distinct and `pks.len()` is a tight upper bound on the
// result row count — size `acc` once instead of growing it row by row.
let mut src_cursor = entry.handle.open_cursor();
let mut acc = Batch::with_schema(src_schema, pks.len());
Self::resolve_collected_pks_into(&mut pks, &mut src_cursor, &mut acc);

Ok((acc.count > 0).then_some(acc))
```

### Prefix seeks (`seek_by_index`)

Identical collect-then-resolve shape over the
`seek_first_positive_with_prefix` / `walk_to_positive_with_prefix` walk:

```rust
let mut idx_cursor = ic.table_mut().open_cursor();
let mut pks: Vec<crate::storage::PkBuf> = Vec::new();

let mut hit = idx_cursor.cursor.seek_first_positive_with_prefix(opk_prefix);
while hit {
    let cur_pk = idx_cursor.cursor.current_pk_bytes();
    pks.push(crate::storage::PkBuf::from_bytes(
        &cur_pk[idx_key_size..idx_key_size + src_pk_stride]));
    idx_cursor.cursor.advance();
    hit = idx_cursor.cursor.walk_to_positive_with_prefix(opk_prefix);
}

if pks.is_empty() {
    return Ok(None);
}

let mut src_cursor = entry.handle.open_cursor();
let mut acc = Batch::with_schema(src_schema, pks.len());
Self::resolve_collected_pks_into(&mut pks, &mut src_cursor, &mut acc);

Ok((acc.count > 0).then_some(acc))
```

(`src_pk_stride` is already bound in `seek_by_index`; the walk no longer needs
`idx_pk_stride`.)

### Why this is semantics-preserving

- **Result order.** The accumulator becomes source-PK-ordered instead of
  index-key-ordered. The result is an unordered Z-set: the master merges
  per-worker batches by appending (`fan_out_index_collect_common`), so global
  order was never index-ordered to begin with. No consumer contract changes.
- **Weights.** Identical predicate and `copy_current_row_into(acc, w)` at the
  source row's net weight.
- **Duplicates.** The merge cursor consolidates same-PK entries across
  sources, and a row holds one indexed value, so one positive
  `(key ‖ src_pk)` entry exists per matching row; even if an entry were
  duplicated, resolving it twice is exactly what the current code does.
- **Empty short-circuit.** Returning `Ok(None)` on `pks.is_empty()` is
  identical to the old tail: with no collected PKs the accumulator would stay
  at `count == 0` and `then_some` already yields `None`. The early return only
  elides the unused cursor and `Batch`.
- **Capacity hint.** `Batch::with_schema`'s capacity argument pre-sizes the
  columnar buffers; it never bounds the row count (appends still grow the
  buffer if exceeded, which cannot happen here). Results are unaffected.

### Memory

The transient `pks` vector costs `K × size_of::<PkBuf>()` (`PkBuf` is a fixed
`[u8; MAX_PK_BYTES]` + `len`, ~81 bytes, independent of the actual PK width).
For wide rows this is well under the accumulator's K full rows that already
exist on this path; for very narrow rows it can exceed them, so peak memory on
a large-K scan rises by roughly that amount. It is a single bounded allocation
freed right after resolution, traded for the seek-locality win — acceptable
for the large-K range scans this targets. If worker-side incremental result
production is ever added, sort per emitted chunk instead of globally: the
helper takes `&mut [PkBuf]`, so call it per collected slice. (A tighter
representation — one flat `K × src_pk_stride` byte buffer sorted via an index
permutation — would shrink the per-entry overhead but adds sorting complexity;
defer it until a profile shows memory pressure.)

### Follow-on (separate, optional)

A forward-galloping `seek_bytes` variant (lower-bound restricted to
`[current position ..)` with exponential probe) would make the sorted sweep
amortized `O(K · log(N/K))` per source instead of `O(K · log N)`. The sort
above is its prerequisite; measure before adding it.

## Tests

- Unit: prefix seek and range scan across ≥ 2 duplicate groups return the same
  multiset (PK + payload + weight) as a scan-and-filter reference; retraction
  in one group reflected at net weight.
- Unit: a range/prefix predicate matching nothing returns `Ok(None)` (exercises
  the `pks.is_empty()` short-circuit).
- Unit: a table with `src_pk_stride > 16` (wide / composite PK) round-trips
  through collect → sort → resolve, confirming `PkBuf` carries the full-width
  source PK (regression guard for dropping the `MAX_PK_BYTES` scratch-copy).
- Existing index-range e2e suites cover end-to-end semantics; they compare
  order-insensitively (master merge order is already worker-interleaved).
- Perf: time a range scan matching ≥ 1M rows on a multi-shard table before and
  after; expect a wall-clock win from seek locality, and no regression for
  single-group point seeks (sort of an already-sorted vector).
