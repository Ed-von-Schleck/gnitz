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

## Design

Split the walk into collect-then-resolve, shared by both methods:

```rust
// catalog/store.rs
/// Resolve collected source PKs against the base table in ascending PK order.
/// Sorting first turns K random point-seeks into a monotone sweep: each
/// `seek_bytes` lower-bounds forward of the previous position's key, so shard
/// pages and the merge state stay hot. Resolution semantics are identical to
/// the previous per-entry path (`resolve_index_entry_into`): present, live,
/// exact-PK rows are appended at their net weight.
fn resolve_collected_pks_into(
    pks: &mut Vec<crate::storage::PkBuf>,
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

The walk loops collect instead of resolving inline — in `seek_by_index_range`:

```rust
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
resolve_collected_pks_into(&mut pks, &mut src_cursor, &mut acc);
```

and equivalently in `seek_by_index`'s
`seek_first_positive_with_prefix` / `walk_to_positive_with_prefix` loop.
`resolve_index_entry_into` is deleted (both callers move to the collected
form).

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

### Memory

`K × size_of::<PkBuf>()` transient — strictly smaller than the result
accumulator's K full rows that already exist on this path. If worker-side
incremental result production lands (non-goal of
`plans/chunked-seek-replies.md`), sort per emitted chunk instead of globally;
the helper signature already supports that (call it per collected slice).

### Follow-on (separate, optional)

A forward-galloping `seek_bytes` variant (lower-bound restricted to
`[current position ..)` with exponential probe) would make the sorted sweep
amortized `O(K · log(N/K))` per source instead of `O(K · log N)`. The sort
above is its prerequisite; measure before adding it.

## Tests

- Unit: prefix seek and range scan across ≥ 2 duplicate groups return the same
  multiset (PK + payload + weight) as a scan-and-filter reference; retraction
  in one group reflected at net weight.
- Existing index-range e2e suites cover end-to-end semantics; they compare
  order-insensitively (master merge order is already worker-interleaved).
- Perf: time a range scan matching ≥ 1M rows on a multi-shard table before and
  after; expect a wall-clock win from seek locality, and no regression for
  single-group point seeks (sort of an already-sorted vector).
