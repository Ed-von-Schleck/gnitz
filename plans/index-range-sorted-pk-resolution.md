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
source-PK-ascending: an equality prefix that pins every indexed column
(`natives.len() == col_indices.len()`) leaves only the trailing source-PK
suffix to order the entries, so the walk yields them ascending and distinct.
They share the helper but skip the sort entirely, so the point-lookup path pays
nothing for this change.

Two further inefficiencies on these paths are independent of the sort but
fixed in the same edit: both methods open the base-table cursor and allocate
the accumulator *before* the walk, so a zero-match scan pays for an
`open_cursor()` (Rc-snapshot clones + tournament-tree build,
`storage/table.rs:665`) and a `Batch` allocation it never uses; and the
accumulator is built with capacity `0`, so it grows row by row during
resolution even though the final row count is known once the walk finishes.

## Design

Split the walk into collect-then-resolve, shared by both methods. Collecting
source PKs first lets the caller sort them before the resolve sweep; it also
lets both seeks skip base-cursor and accumulator construction entirely when the
walk found nothing, and size the accumulator exactly when it did. The walk's
index cursor is dropped the moment collection finishes, so it never coexists
with the base cursor.

### Shared resolve helper

`resolve_index_entry_into` is deleted; both callers move to this collected
form. Because the source PKs are now owned `PkBuf`s (the index cursor has
advanced past them by resolve time), the helper drops the old `MAX_PK_BYTES`
scratch-copy: `PkBuf` already stores the exact `src_pk_stride` bytes inline
(up to `MAX_PK_BYTES`), so wide (`src_pk_stride > 16`) sources stay correct.

The helper resolves in the order it is given; sorting is the caller's
responsibility (it owns the arity knowledge that decides whether a sort is even
needed — see below). Mirrors `gather_family_bytes`, whose contract is likewise
"pass PKs ascending for monotone probes; correctness does not require it."

```rust
// catalog/store.rs — associated fn on CatalogEngine, replacing
// `resolve_index_entry_into`. Called by both `seek_by_index` and
// `seek_by_index_range`.
/// Resolve already-collected source PKs against the base table, appending each
/// present, live, exact-PK row to `acc` at its net `current_weight` (never a
/// hardcoded 1, so Z-Set multiplicity is preserved). Callers pass `pks`
/// ascending: each `seek_bytes` then lower-bounds at or past the previous key,
/// turning K scattered point-seeks into a monotone forward sweep that keeps
/// shard pages and merge state hot. The PKs are the index entries' source-PK
/// OPK suffixes, whose memcmp order equals the base table's storage order, so a
/// byte sort *is* the seek order. `PkBuf` carries the exact `src_pk_stride`
/// bytes inline (up to `MAX_PK_BYTES`), so wide sources resolve with no widen.
fn resolve_collected_pks_into(
    pks: &[crate::storage::PkBuf],
    src_cursor: &mut CursorHandle,
    acc: &mut Batch,
) {
    for pk in pks.iter() {
        let bytes = pk.pk_bytes();
        src_cursor.cursor.seek_bytes(bytes);
        // `current_weight > 0` is kept deliberately, not as redundancy: the
        // single-source cursor path (`drive_single`) does not consolidate, so
        // it can surface a tombstone an uncompacted source still holds. One
        // predictable branch, dwarfed by the seek, keeps this correct against
        // any cursor and matches every other point-seek here (`seek_family`,
        // `gather_family_bytes`).
        if src_cursor.cursor.valid
            && src_cursor.cursor.current_pk_bytes() == bytes
            && src_cursor.cursor.current_weight > 0
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
// Collection is done; free the index merge tree and its shard snapshots before
// opening the base cursor (`open_cursor` returns an owned, Rc/Arc-snapshot
// handle), so the two never coexist and obsolete shards can be reclaimed.
drop(idx_cursor);

// Zero matches: skip base-cursor construction (Rc-snapshot clones +
// tournament-tree build in `open_cursor`) and the accumulator allocation.
if pks.is_empty() {
    return Ok(None);
}

// A range spans many duplicate groups, so the collected source PKs interleave
// across the base table — sort to recover the ascending sweep. `PkBuf: Ord`
// delegates to `compare_pk_bytes` (memcmp), the base table's storage order.
pks.sort_unstable();

// Base table holds unique PKs and each row carries one indexed value, so the
// collected PKs are distinct and `pks.len()` is a tight upper bound on the
// result row count — size `acc` once instead of growing it row by row.
let mut src_cursor = entry.handle.open_cursor();
let mut acc = Batch::with_schema(src_schema, pks.len());
Self::resolve_collected_pks_into(&pks, &mut src_cursor, &mut acc);

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
drop(idx_cursor);

if pks.is_empty() {
    return Ok(None);
}

// Full-arity equality (`natives.len() == col_indices.len()`) pins every indexed
// column, so the entries vary only in their trailing source-PK suffix: the
// collected PKs are already ascending and distinct — skip the sort. A leading-
// prefix seek leaves trailing indexed columns free, interleaving source PKs
// across groups, so it must sort.
if natives.len() < col_indices.len() {
    pks.sort_unstable();
}

let mut src_cursor = entry.handle.open_cursor();
let mut acc = Batch::with_schema(src_schema, pks.len());
Self::resolve_collected_pks_into(&pks, &mut src_cursor, &mut acc);

Ok((acc.count > 0).then_some(acc))
```

(`src_pk_stride` and the `natives` / `col_indices` slices are already bound in
`seek_by_index`; the walk no longer needs `idx_pk_stride`.)

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
for the large-K range scans this targets.

## Tests

- Unit: prefix seek and range scan across ≥ 2 duplicate groups return the same
  multiset (PK + payload + weight) as a scan-and-filter reference; retraction
  in one group reflected at net weight.
- Unit: a range/prefix predicate matching nothing returns `Ok(None)` (exercises
  the `pks.is_empty()` short-circuit).
- Unit: a table with `src_pk_stride > 16` (wide / composite PK) round-trips
  through collect → sort → resolve, confirming `PkBuf` carries the full-width
  source PK (regression guard for dropping the `MAX_PK_BYTES` scratch-copy).
- Unit: a full-arity equality seek on a **non-unique** index
  (`natives.len() == col_indices.len()`, multiple matching rows whose source PKs
  are *not* the index emission order) returns the correct multiset — exercises
  the sort-skip branch and asserts the entries were already source-PK-ascending.
- Existing index-range e2e suites cover end-to-end semantics; they compare
  order-insensitively (master merge order is already worker-interleaved).
- Perf: time a range scan matching ≥ 1M rows on a multi-shard table before and
  after; expect a wall-clock win from seek locality, and no regression for
  single-group point seeks (sort of an already-sorted vector).
