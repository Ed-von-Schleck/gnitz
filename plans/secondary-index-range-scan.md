# Ordered range scans over secondary indexes

## Goal

Serve a **range predicate** on a secondary-indexed column from the index by an
ordered range scan, instead of failing (direct SELECT) or full-scanning. Concretely:

```sql
CREATE INDEX ON t (x);
SELECT * FROM t WHERE x > 100;            -- today: hard error; after: index range scan
SELECT * FROM t WHERE x BETWEEN -5 AND 5; -- inclusive both ends, signed-correct
SELECT * FROM t WHERE a = 7 AND b < 50;   -- composite (a,b): a-equality prefix + b range
```

This builds the **ordered range-scan access primitive** over a secondary index
and wires one non-join consumer (direct SELECT range predicates) end-to-end across
workers. It is the first consumer of the order-preserving signed index encoding
landed in `cff7c58`, whose commit message states the encoding is needed for "any
future range or ordered index scan" — this is that scan.

## Relationship to the parent plan

This is the next self-contained slice of `wide-pk-incremental-views.md` §1 item 1
("Non-equi join predicates"). That item names two prerequisites and an operator:

- **(a) ordered index keys at rest** — DONE (`cff7c58`, order-preserving signed
  encoding; `index_key_type` promotes signed ≤8-byte ints to `I64`, every encoder
  sign-extends from the source width through `encode_pk_column_promoted`).
- **(b) a range-aware distributed exchange** (or broadcast) — needed only by the
  *join*, where the inner index is probed by another relation's delta and the
  equality scatter no longer routes a single key. **Out of scope here.**
- **the range-scan-per-delta join operator + non-equi `ON`-clause planning** —
  **out of scope here.**

A single-table range SELECT needs neither (b) nor the join operator: it
**broadcasts** the bounds to every worker and merges, exactly as the existing
non-unique point seek does (`fan_out_seek_by_index_collect_async`). The index is
partitioned by **source PK**, so one value's — and one range's — matching entries
scatter across all workers; broadcast-and-merge is already the correct, in-tree
pattern for that. This slice therefore delivers the reusable range-scan substrate
(storage walk + bound encoding + broadcast/merge IPC + client API) that the future
range-join will consume, while shipping a complete, testable user-visible feature
on its own.

## Background — current state (verified against code)

The secondary index is a child table whose PK region is
`[promoted leading-key columns…  ‖  source-PK columns…]`, OPK at rest, zero
payload (`dag.rs::batch_project_index`, ~1876). After `cff7c58` the leading-key
OPK is order-preserving for signed columns too (`gnitz_wire::index_key_type`
promotes `I8..I64 → I64`; `wire_stride(I64) == wire_stride(U64) == 8` so record
arity/stride are unchanged).

**Access today is equality/prefix-equality only:**

- `CatalogStore::seek_by_index(table_id, col_indices, natives)`
  (`catalog/store.rs:919`) builds the OPK leading-key prefix via
  `IndexKeySpec::seek_prefix`, calls
  `cursor.seek_first_positive_with_prefix(opk_prefix)` then loops with
  `walk_to_positive_with_prefix` after each consumed entry, reconstructs the
  source PK from the index PK suffix (`current_pk_bytes()[idx_key_size..]`), and
  resolves rows via `seek_family_bytes`. It returns **one worker's** partial set;
  the master merges across workers.
- `seek_first_positive_with_prefix` / `walk_to_positive_with_prefix`
  (`storage/read_cursor.rs:577`, `:595`) zero-pad the prefix to `pk_stride`
  (0x00 is the OPK minimum for every PK type), `seek_bytes`, then walk while the
  current PK `starts_with(prefix)`, skipping non-positive weights.
- Wire: client `Connection::seek_by_index` (`gnitz-core/connection.rs:302`) packs
  `K` native u128 values as 16-byte LE slots into a `PkTuple`; `split_wire`
  routes slot 0 → `seek_pk`, slots 1..K → the `seek_pk_extra` BLOB.
  `seek_col_idx = pack_pk_cols(col_indices)`. Flag `FLAG_SEEK_BY_INDEX (256)`.
  `PkTuple` is `MAX_PK_BYTES = MAX_PK_COLUMNS·16 = 80` bytes, so `split_wire`
  caps `seek_pk_extra` at `80 − 16 = 64` bytes — `encode_message`/`send_message`
  have **no** explicit `seek_pk_extra` argument; the only way to populate it is
  through a `PkTuple`. The master→worker leg (`write_group_with_req_ids`) and the
  control-block BLOB itself are arbitrary-length; the 64-byte ceiling is purely
  the client encode (see §2/§6).
- Flag spaces are split. The SAL group-header flag is a **`u32`**
  (`sal::write_group(… sal_flags: u32 …)`); the worker dispatches on
  `SalMessageKind`, derived by `sal::classify(flags: u32)`. The full `u64` wire
  flags ride separately in the per-worker control block. `FLAG_SEEK_BY_INDEX`
  (256, bit 8) is free in both spaces, so the same constant serves as the client
  request flag *and* the SAL dispatch flag. SAL flags are allocated up through
  bit 17 (`FLAG_UNIQUE_PREFLIGHT = 1<<17`); wire bits 16–47 are packed fields
  (conflict mode / schema version / index version), so **no single bit is free in
  both spaces** anymore — a new dual-leg operation needs one flag per leg (§1).
- `executor.rs:704` routes `FLAG_SEEK_BY_INDEX` → `handle_seek_by_index`.
- `worker.rs:859` decodes: `cols = unpack_pk_cols(seek_col_idx)`, validates the
  column list against the schema, reassembles `K = 1 + seek_pk_extra.len()/16`
  natives (rejecting a non-16-multiple `extra`), guards `K ≤ cols.len()`, calls
  `store.seek_by_index`.
- `master.rs:935` `fan_out_seek_by_index_collect_async` broadcasts the verbatim
  frame to all workers via `dispatch_scan_fanout` (`write_group_with_req_ids …
  FLAG_SEEK_BY_INDEX …`) and accumulates the returned batch slots. The master
  forwards `seek_pk_extra` **verbatim** — the worker is the sole OPK encoder.

**SQL planner (`gnitz-sql/dml.rs`):**

- `collect_index_seek_candidates(expr, schema, fetch_indexes)` (`:868`) flattens
  the WHERE `AND`-tree and collects every `col = literal` equality
  (`try_col_eq_literal`, `:840`, → `(col_idx, key: u128)` via
  `parse_pk_literal_packed`). For each index it walks the declared columns,
  consuming equality conjuncts as a **leading prefix** (stops at the first
  uncovered column), rejects a prefix whose uncovered trailing columns are
  nullable, and emits `(idx_cols, vals, residual)`.
- Direct SELECT (`:683`–`:741`): try `try_extract_pk_seek_residual` (PK
  equality), then each `collect_index_seek_candidates` candidate via
  `client.seek_by_index`, applying `apply_residual_filter` after a hit. **If no
  index serves, direct SELECT raises** `Unsupported("WHERE on non-indexed column
  not supported in direct SELECT; …")`. **Range conjuncts (`>`,`>=`,`<`,`<=`,
  `BETWEEN`) are never extracted**, so today every range SELECT hits that error.
  (UPDATE/DELETE have a predicate full-scan fallback, so they already *work* on
  ranges, just unindexed.)

**Cursor primitives already sufficient for a range walk** (no new cursor method
required): `seek_bytes(&[u8])`, `current_pk_bytes() -> &[u8]`, `current_weight`,
`advance()`, `valid`, and (if wanted) `current_pk_cmp_bytes(key) -> Ordering`
(storage-order compare against an OPK key). The range walk lives in `store.rs`
(like `seek_by_index`) because resolving each match needs `&mut self`.

## Design

### The range, as OPK byte bounds

A range query covers a contiguous OPK key interval of the index's leading
columns: `E` equality-pinned leading columns (E ≥ 0) followed by **one** range
column at position `E`. Let `prefix_len = index_schema.leading_key_size(E + 1)`
(bytes covering the equality columns + the range column).

Build two byte prefixes of length `prefix_len`, identical in the equality slots,
differing only in the range slot:

- `lo_prefix` = `OPK(eq…)` ‖ `range_slot`, where `range_slot` =
  `OPK(lo)` if the lower bound is finite, else all-`0x00` (the OPK minimum of the
  column's type — correct for signed too: `INT_MIN` sign-flips to all-zeros).
- `hi_prefix` = `OPK(eq…)` ‖ `range_slot`, where `range_slot` =
  `OPK(hi)` if the upper bound is finite, else all-`0xFF` (the OPK maximum).

`OPK(v)` is produced through the **same** encoder the equality seek and the
projector use — `schema::index_opk_prefix(native, src_type, idx_key_type)` (single
column; the caller slices the returned buffer to the index column's `slot_size`) /
`IndexKeySpec::seek_prefix` (the equality prefix) — so the bound bytes are
byte-identical to stored entries and signed sign-extension from the source column
width is handled by `encode_pk_column_promoted`. No new encoding logic.

### The walk (engine, `CatalogStore::seek_by_index_range`)

```
// inclusive/unbounded lower → 0x00 suffix (OPK min); exclusive lower → 0xFF
// suffix (OPK max), so the binary search skips the whole lower-boundary
// duplicate group in O(log N) instead of walking it entry by entry.
seek_bytes( lo_prefix ‖ (lo_incl ? 0x00 : 0xFF) * (pk_stride - prefix_len) )
while cursor.valid {
    let cur = current_pk_bytes()[..prefix_len];
    // upper bound terminates the scan (entries are ordered by leading OPK):
    if cur > hi_prefix                  { break; }
    if cur == hi_prefix && !hi_incl     { break; }
    // lower bound trims the boundary group (the zero-padded seek can land on it):
    if cur < lo_prefix                  { advance(); continue; }   // defensive
    if cur == lo_prefix && !lo_incl     { advance(); continue; }
    if current_weight > 0 {
        // reconstruct source PK from current_pk_bytes()[idx_key_size..], then
        // resolve through a SINGLE primary cursor reused across the whole walk:
        //   primary.seek_bytes(src_pk); if valid && weight>0 && pk matches
        //       primary.copy_current_row_into(&mut acc, weight);
        // copy_current_row_into appends straight into the reused accumulator
        // (German-string aware), so the walk allocates no per-row Batch.
    }
    advance();
}
```

The resolve uses one persistent `CursorHandle` on the source table opened
*before* the loop, not a fresh `seek_family_bytes` per match (which opens a
cursor and heap-allocates a one-row `Batch` every row). `copy_current_row_into`
(`storage/read_cursor.rs`) copies the current row — out-of-line strings
included — directly into the accumulator. The existing `seek_by_index`
(`store.rs:919`) is refactored to call the same shared resolve helper, so the
point seek loses its per-row allocation too.

Correctness rests on the OPK ordering invariant: because the leading-key OPK is
order-preserving (`memcmp` == typed order, including signed and the equality
prefix), a raw byte compare of `current_pk_bytes()[..prefix_len]` against
`lo_prefix`/`hi_prefix` **is** the typed lexicographic compare. The walk visits
exactly the entries whose leading columns fall in the requested interval, then
stops — it does not run to the end of the index (unless the upper bound is
unbounded). For a composite `a = 7 AND b < 50`, `hi_prefix = OPK(a=7) ‖ 0xFF`
stops the scan before `a = 8` because `OPK(a=8)‖… > OPK(a=7)‖0xFF`.

This generalizes `seek_first_positive_with_prefix` (which is the special case
`lo_prefix == hi_prefix`, both inclusive). Factor the shared zero-pad-and-seek
into a private helper if convenient, but it is not required.

### NULL semantics are already correct

Rows with NULL in any indexed column are absent from the index
(`batch_project_index` skips them). SQL range semantics exclude NULL operands
(`NULL > 5` is unknown → row excluded), so "absent from the index" is exactly
right — the range scan needs no NULL special-casing for the *covered* columns.
The existing leading-prefix nullability guard in `collect_index_seek_candidates`
(`dml.rs:915`) still applies to *uncovered trailing* index columns and is reused
unchanged.

### Distribution: broadcast + merge, no range exchange

`seek_by_index_range` returns one worker's partial set. The master fans the range
request out to **all** workers and merges, identical to
`fan_out_seek_by_index_collect_async`. The index is partitioned by source PK, so a
range's matches are spread across workers; broadcasting the bounds and merging the
per-worker partials is the existing, correct mechanism — **and is precisely why
this slice needs no range-aware exchange.**

## The change, by file

### 1. Two flags: a wire request flag and a SAL dispatch flag

A range seek crosses **two** legs that no longer share a free bit (see
Background), so it needs one flag per leg:

- **Client→master request flag** — `crates/gnitz-wire/src/flags.rs`:
  `pub const FLAG_SEEK_BY_INDEX_RANGE: u64 = 1 << 55;` (the next free high request
  bit, above the bit-16–47 packed fields, alongside `FLAG_GET_INDICES = 1<<54`).
  The master's `executor.rs` routes on this `u64` wire flag; it is **never**
  written to the SAL. Re-export it from `gnitz-core`
  (`protocol/header.rs`'s `pub use gnitz_wire::{…}` block) and add it to the
  compile-time non-collision guard in `flags.rs`.
- **Master→worker SAL dispatch flag** — `crates/gnitz-engine/src/runtime/sal.rs`:
  `pub const FLAG_SEEK_BY_INDEX_RANGE_SAL: u32 = 1 << 18;` (the next free SAL
  group-header bit, above `FLAG_UNIQUE_PREFLIGHT = 1<<17`). Add a
  `SalMessageKind::SeekByIndexRange` variant and a `classify` arm
  (`if flags & FLAG_SEEK_BY_INDEX_RANGE_SAL != 0 { return …SeekByIndexRange; }`),
  matching the enum-exhaustiveness convention the file documents.

Why not one flag, modeled on `FLAG_GET_INDICES`? `GET_INDICES` is answered
**master-locally** from catalog metadata and never fans out to workers, so its
high bit is never classified by `sal::classify` (a `u32`). A range seek's matches
are partitioned by source PK and **must** fan out; the worker therefore must
classify it, which requires a `u32`-range SAL flag with its own `SalMessageKind`.
A single dual-leg flag is impossible: the wire request flag must avoid the
bit-16–47 packed fields, and the SAL flag must fit a `u32` and miss the
bits-2–17 SAL run — those two free regions are disjoint.

Follow the dev-guide IPC-flag checklist: add the SAL flag in `sal.rs`, classify
it, handle it in the `worker.rs` dispatch loop (§4), and write it as the
`sal_flags` arg from the master fan-out (§5). No new control-block column is
added — the range descriptor reuses the existing `seek_pk_extra` BLOB channel
(§2).

### 2. Wire payload: a self-describing range descriptor in `seek_pk_extra`

Encode the whole request as a compact byte blob carried in the existing
`seek_pk_extra` BLOB (the same channel `FLAG_SEEK_BY_INDEX` uses for key slots).
`seek_col_idx = pack_pk_cols(col_indices)` as before; `seek_pk` is unused (0).

Descriptor layout (all integers fixed-width LE):

```
byte 0 : n_eq         // equality-pinned leading columns, 0..=K-1
byte 1 : flags        // bit0 lo_inclusive, bit1 hi_inclusive,
                      // bit2 lo_unbounded, bit3 hi_unbounded
bytes 2 .. 2+16*n_eq        : eq natives           (16-byte LE each)
next 16 (unless lo_unbounded): lo native           (16-byte LE)
next 16 (unless hi_unbounded): hi native           (16-byte LE)
```

The worker validates `n_eq + 1 ≤ cols.len()` and the exact descriptor length
against `n_eq`/the unbounded bits at the trust boundary (mirroring the existing
`seek_pk_extra.len() % 16 == 0` guard), rejecting a malformed frame rather than
mis-decoding it.

**Size and channel.** A full descriptor is `2 + 16·n_eq + 16 + 16` bytes; at the
maximum index arity (`PK_LIST_MAX_COLS = 4` ⇒ `n_eq ≤ 3`) that is **82 bytes**,
larger than `MAX_PK_BYTES = 80`. The descriptor therefore **cannot** be packed
into a `PkTuple` — `PkTuple::from_bytes` hard-asserts `len ≤ 80`, and `split_wire`
caps `seek_pk_extra` at 64 bytes regardless. The client must send the descriptor
as an **explicit, arbitrary-length `seek_pk_extra` blob** (§6); the control-block
BLOB column and the master→worker `write_group_with_req_ids` blob argument are
already arbitrary-length, so only the client encode path needs the new
explicit-blob entry point. `seek_pk` is unused (0); `seek_col_idx =
pack_pk_cols(col_indices)` as before.

(Alternative considered: add a typed upper-bound control column + inclusivity
fields alongside `seek_pk_extra`. Rejected — it touches the control-block schema
in `gnitz-wire/control` and every encode/decode site for no benefit over a
self-describing blob under a dedicated flag.)

### 3. Engine: `CatalogStore::seek_by_index_range` (`catalog/store.rs`)

New method next to `seek_by_index` (`:919`):

```rust
pub fn seek_by_index_range(
    &mut self,
    table_id: i64,
    col_indices: &[u32],
    n_eq: usize,
    eq_natives: &[u128],            // len == n_eq
    lo: Option<u128>, lo_incl: bool,
    hi: Option<u128>, hi_incl: bool,
) -> Result<Option<Batch>, String>
```

Reference implementation:

```rust
pub fn seek_by_index_range(
    &mut self,
    table_id: i64,
    col_indices: &[u32],
    n_eq: usize,
    eq_natives: &[u128],            // len == n_eq
    lo: Option<u128>, lo_incl: bool,
    hi: Option<u128>, hi_incl: bool,
) -> Result<Option<Batch>, String> {
    let entry = self.dag.tables.get(&table_id)
        .ok_or_else(|| format!("Unknown table_id {table_id}"))?;
    let ic = entry.index_circuits.iter()
        .find(|ic| ic.col_indices.as_slice() == col_indices)
        .ok_or_else(|| format!("No index on cols {col_indices:?} for table {table_id}"))?;

    // Precondition: the range column sits at position `n_eq` among the index's
    // leading columns, so `n_eq + 1` of them must exist. The worker validates
    // this at the trust boundary, but `seek_by_index_range` is a `pub` method
    // reachable directly from unit tests — guard here too, *before* any
    // `columns[n_eq]` / `leading_key_size(n_eq + 1)` indexing below, which would
    // otherwise panic out of bounds. `n_eq >= len` (not `n_eq + 1 > len`) avoids
    // overflowing the `+ 1` on an adversarial descriptor byte. This also makes
    // `prefix_len < idx_pk_stride` strict, so the exclusive-lower `seek_key
    // [prefix_len..]` suffix below is always a non-empty slice.
    if n_eq >= col_indices.len() {
        return Err(format!(
            "seek_by_index_range: n_eq {n_eq} has no range column within index \
             arity {} on cols {col_indices:?}", col_indices.len()));
    }
    debug_assert_eq!(eq_natives.len(), n_eq, "eq_natives must hold exactly n_eq values");

    let src_schema    = entry.schema;
    let src_pk_stride = src_schema.pk_stride() as usize;
    let idx_pk_stride = ic.index_schema.pk_stride() as usize;       // leading + source PK
    let idx_key_size  = ic.index_schema.leading_key_size(col_indices.len());
    let eq_size       = ic.index_schema.leading_key_size(n_eq);     // equality prefix bytes
    let slot_size     = ic.index_schema.columns[n_eq].size() as usize;
    let prefix_len    = eq_size + slot_size;                        // == leading_key_size(n_eq + 1)
    let src_type      = src_schema.columns[col_indices[n_eq] as usize].type_code;
    let idx_key_type  = ic.index_schema.columns[n_eq].type_code;

    // Two byte prefixes: identical equality slots, range slot differs.
    let mut lo_prefix = vec![0u8; prefix_len];
    let mut hi_prefix = vec![0u8; prefix_len];
    if n_eq > 0 {
        // n_eq == 0 (pure range, e.g. `x > 10`) must NOT reach IndexKeySpec::new:
        // an empty `cols` slice trips its `debug_assert!(!cols.is_empty())`. The
        // empty equality prefix is already all-zero, so skip the encode.
        let spec = crate::schema::IndexKeySpec::new(
            &col_indices[..n_eq], &src_schema, &ic.index_schema);
        let (opk, plen) = spec.seek_prefix(eq_natives);            // plen == eq_size
        lo_prefix[..plen].copy_from_slice(&opk[..plen]);
        hi_prefix[..plen].copy_from_slice(&opk[..plen]);
    }
    if let Some(v) = lo {
        lo_prefix[eq_size..].copy_from_slice(
            &crate::schema::index_opk_prefix(v, src_type, idx_key_type)[..slot_size]);
    } // else 0x00 * slot_size == OPK minimum (signed INT_MIN sign-flips to all-zero)
    match hi {
        Some(v) => hi_prefix[eq_size..].copy_from_slice(
            &crate::schema::index_opk_prefix(v, src_type, idx_key_type)[..slot_size]),
        None    => hi_prefix[eq_size..].fill(0xFF),                // OPK maximum
    }

    // One index cursor + one reused source cursor (both raw-pointer-backed, like
    // `seek_by_index`); copy each match straight into the accumulator.
    let mut idx_cursor = ic.table_mut().open_cursor();
    let mut src_cursor = entry.handle.open_cursor();
    let mut acc = Batch::with_schema(src_schema, 0);

    // Seed the seek key with the lower-bound prefix. For an *exclusive* finite
    // lower bound, pad the source-PK suffix with 0xFF (the OPK maximum) so the
    // binary search lands at the first entry strictly past the boundary group
    // in O(log N). With 0x00 the seek lands on the first member of the boundary
    // group, and the loop below would then skip every duplicate of the lower
    // value one entry at a time — O(N) on a low-cardinality column with a large
    // duplicate group. `suffix_len = idx_pk_stride - prefix_len ≥ src_pk_stride
    // > 0` because the range column is a leading column, so the suffix always
    // exists. (Inclusive or unbounded-below keeps 0x00, the OPK minimum.)
    let mut seek_key = vec![0u8; idx_pk_stride];
    seek_key[..prefix_len].copy_from_slice(&lo_prefix);
    if lo.is_some() && !lo_incl {
        seek_key[prefix_len..].fill(0xFF);
    }
    idx_cursor.cursor.seek_bytes(&seek_key);
    while idx_cursor.cursor.valid {
        {
            let cur = &idx_cursor.cursor.current_pk_bytes()[..prefix_len];
            if cur > hi_prefix.as_slice() { break; }
            if cur == hi_prefix.as_slice() && !hi_incl { break; }
            // Defensive lower trim. The 0xFF-suffixed exclusive seek above lands
            // past the boundary group, so `cur == lo_prefix && !lo_incl` only
            // fires for the astronomically-rare entry whose source PK is itself
            // all-0xFF (e.g. a U64 source PK of u64::MAX); the inclusive seek
            // lands at `cur >= lo_prefix`, so `cur < lo_prefix` never fires.
            // Both are kept so the walk stays correct independent of the seed.
            if cur < lo_prefix.as_slice()
                || (cur == lo_prefix.as_slice() && !lo_incl)
            { idx_cursor.cursor.advance(); continue; }
        }
        if idx_cursor.cursor.current_weight > 0 {
            let mut src_pk = [0u8; crate::schema::MAX_PK_BYTES];
            let full = idx_cursor.cursor.current_pk_bytes();
            src_pk[..src_pk_stride].copy_from_slice(
                &full[idx_key_size..idx_key_size + src_pk_stride]);
            src_cursor.cursor.seek_bytes(&src_pk[..src_pk_stride]);
            if src_cursor.cursor.valid
                && src_cursor.cursor.current_weight > 0
                && src_cursor.cursor.current_pk_bytes() == &src_pk[..src_pk_stride]
            {
                // Copy the row at its true Z-Set multiplicity — the net
                // `current_weight` — never a hardcoded 1. This mirrors what the
                // point seek does today (`seek_family_bytes` →
                // `copy_cursor_row_with_weight(.., current_weight)`), so folding
                // `seek_by_index` onto the shared resolve helper below cannot
                // regress its weights, and the `(ℤ[D], +, 0, -)` group structure
                // any downstream COUNT/SUM relies on stays intact. (For a
                // unique-PK base table the net is 1, so this is observationally
                // identical there — but the primitive must not assume it.)
                let w = src_cursor.cursor.current_weight;
                src_cursor.cursor.copy_current_row_into(&mut acc, w);
            }
        }
        idx_cursor.cursor.advance();
    }
    Ok((acc.count > 0).then_some(acc))
}
```

`index_opk_prefix(native, src_type, idx_key_type)` (`schema.rs:914`) is the
single-column scalar sibling of `seek_prefix`; it sign-extends the signed source
from its native width before OPK-encoding at the promoted index column, so the
bound bytes are byte-identical to stored entries. Factor the source-PK-reconstruct
+ `copy_current_row_into` block into a private helper — copying the cursor's net
`current_weight`, exactly as `copy_cursor_row_with_weight` does — and call it from
`seek_by_index`'s loop too, so the point seek drops its per-row `seek_family_bytes`
`Batch` allocation while keeping its existing weight-preserving behavior.

### 4. Worker dispatch (`runtime/worker.rs`) + executor routing (`runtime/executor.rs`)

- `executor.rs` (~`:704`, master-side client dispatch on the `u64` wire flags):
  add an arm `if flags & FLAG_SEEK_BY_INDEX_RANGE != 0` →
  `handle_seek_by_index_range(shared, fd, client_id, target_id, seek_col_idx,
  &seek_pk_extra, client_version)`, under the catalog read lock, placed next to
  the `FLAG_SEEK_BY_INDEX` arm. `handle_seek_by_index_range` calls the §5
  fan-out and replies exactly like `handle_seek_by_index`.
- `worker.rs` (`dispatch_inner`, ~`:856`, dispatch on `SalMessageKind`): add a
  `SalMessageKind::SeekByIndexRange` arm (and list it in the `run_via_dispatch_inner`
  passthrough match alongside `SeekByIndex`). It `unpack_pk_cols(seek_col_idx)`,
  validates the column list against the schema (reuse the existing check), decodes
  the **range descriptor** from `seek_pk_extra` (§2), validates `n_eq + 1 ≤
  cols.len()` and the exact descriptor length, calls `store.seek_by_index_range`,
  and replies via `send_response` — identical reply plumbing to the
  `SalMessageKind::SeekByIndex` arm. The worker classifies this kind from the
  `u32` SAL flag `FLAG_SEEK_BY_INDEX_RANGE_SAL` (§1), not the high `u64` wire bit.

### 5. Master fan-out (`runtime/master.rs`)

`fan_out_seek_by_index_range_collect_async` — a near-clone of
`fan_out_seek_by_index_collect_async` (`:935`). Inside the `dispatch_scan_fanout`
closure, call `write_group_with_req_ids` with the **SAL** flag
`FLAG_SEEK_BY_INDEX_RANGE_SAL` as the `sal_flags` argument (where the point-seek
clone passes `FLAG_SEEK_BY_INDEX`), `seek_col_idx`, `seek_pk = 0`, and the
descriptor blob as the trailing `seek_pk_extra: &[u8]` argument (arbitrary length
— this leg is not `PkTuple`-bound). Then accumulate the per-worker batch slots
into one merged batch exactly as the point-seek collect does. The descriptor is
forwarded verbatim (the worker is the sole OPK encoder, as for SEEK_BY_INDEX).

### 6. Core client (`gnitz-core`)

- `protocol/message.rs`: add an explicit-blob send entry point, because
  `encode_message`/`send_message` derive `seek_pk_extra` solely from
  `seek_pk.split_wire()` (≤64 bytes) and the range descriptor can be 82 bytes.
  Add `encode_message_with_extra(target_id, client_id, flags, seek_col_idx,
  seek_pk_extra: &[u8]) -> Vec<u8>` (sets `seek_pk = 0`, passes the blob straight
  to `encode_control_block(&hdr, "", seek_pk_extra)` — which already accepts an
  arbitrary-length BLOB) and the matching `send_message_with_extra`. No churn to
  existing call sites; the seek/push/scan paths keep `send_message`.
- `connection.rs` (next to `seek_by_index`, `:302`): `seek_by_index_range(...)`
  builds the descriptor blob (§2) and calls `send_message_with_extra` with
  `seek_col_idx = pack_pk_cols(col_indices)`, `flags =
  wire_flags_set_schema_version(FLAG_SEEK_BY_INDEX_RANGE, cached_version)`, and the
  descriptor as the explicit `seek_pk_extra` argument; decode the response exactly
  like `seek_by_index`.
- `client.rs` (next to `seek_by_index`, `:190`): public
  `seek_by_index_range(table_id, col_indices, n_eq, eq_vals, lo, lo_incl, hi,
  hi_incl) -> ScanResult`. This is the single choke point for arity validation —
  reuse `validate_pk_col_list`, require `eq_vals.len() == n_eq`,
  `n_eq + 1 ≤ col_indices.len()`, and at least one finite bound. Returns
  `(schema, batch, view_lsn)` like the other seeks.

### 7. SQL planner (`gnitz-sql/dml.rs`) — extract range conjuncts, serve direct SELECT

**Bound out-of-range literals (correctness, shared with the equality path).**
`parse_pk_literal_packed` parses a signed literal to `i64` then casts (`v as i32`
…) with no range check, and the unsigned arm widens to `u128` and lets the OPK
encode truncate to the column width. Both silently wrap an out-of-range literal
(`x = 3000000000` on an `I32` column today seeks `x = -1294967296`). Add a
range check so the function declines instead of wrapping:

```rust
TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 => {
    let v = s.parse::<i64>().ok()?;
    let (min, max) = match tc {
        TypeCode::I8  => (i8::MIN  as i64, i8::MAX  as i64),
        TypeCode::I16 => (i16::MIN as i64, i16::MAX as i64),
        TypeCode::I32 => (i32::MIN as i64, i32::MAX as i64),
        _             => (i64::MIN,        i64::MAX),
    };
    if v < min || v > max { return None; }            // out of range → decline
    Some(match tc {
        TypeCode::I8  => (v as i8  as u8)  as u128,
        TypeCode::I16 => (v as i16 as u16) as u128,
        TypeCode::I32 => (v as i32 as u32) as u128,
        _             => (v as u64)        as u128,
    })
}
// unsigned ≤8B arm: parse u64, then `if v > type_max { return None; }`
```

`None` makes `try_col_eq_literal` decline the out-of-range equality (a clean
"non-indexed"/empty outcome instead of silent wrong rows). Verify the other two
callers — `extract_pk_value` and `try_extract_pk_in` — treat `None` as
"unroutable literal" (they already do for non-numeric inputs); an out-of-range PK
literal in an `INSERT`/`IN` is correctly rejected rather than wrapped.

**Range extraction.**

- `parse_range_bound(tc, n_str, negated) -> Option<Bound>` where
  `enum Bound { In(u128), BelowMin, AboveMax }`. `In` is the in-range native;
  `BelowMin`/`AboveMax` report a literal past the column type's range (parsed as
  `i128`, compared against the type min/max) so the range planner can saturate
  instead of declining.
- `try_col_range_literal(expr, schema) -> Option<(usize /*col*/, RangeEnd)>`
  recognizing `col > lit`, `col >= lit`, `col < lit`, `col <= lit` and their
  flipped forms (`lit < col` ≡ `col > lit`, …), mapping each to a `RangeEnd`
  `{ side: Lower|Upper, incl, bound: Bound }`.
- **`BETWEEN` is a single AST node**, not two conjuncts: `flatten_conjuncts`
  (`dml.rs:758`) descends only `AND`/`Nested` and pushes everything else —
  including `Expr::Between { expr, low, high, negated }` — as an opaque leaf.
  Desugar it explicitly in the range collector: a non-negated `Expr::Between`
  contributes a lower end (`expr >= low`) **and** an upper end (`expr <= high`) on
  the same column. A **negated** `BETWEEN` is `expr < low OR expr > high` — not a
  contiguous interval — so leave it as a residual conjunct (not index-served).
- **Teach the binder `Expr::Between` so residual `BETWEEN` filters instead of
  erroring.** `binder.bind_expr` (`binder.rs:144`) has no `Expr::Between` arm —
  it falls to the catch-all `_ => Unsupported`. So a `BETWEEN`/`NOT BETWEEN` on a
  **non-covered** column (e.g. `WHERE x > 5 AND y BETWEEN 1 AND 9`, index on `x`)
  reaches `bind_residuals` as an opaque `Expr::Between` and the whole query fails
  to bind — a pre-existing gap that the range path now routes traffic into. Add a
  desugaring arm so any residual `BETWEEN` binds to the equivalent comparison tree
  (this is independent of the range collector's own desugar, which extracts an
  index bound; this one makes the *residual* filterable):

  ```rust
  Expr::Between { expr, negated, low, high } => {
      // `e BETWEEN lo AND hi` ≡ `e >= lo AND e <= hi`; NOT BETWEEN is its
      // negation. Bind `e` twice rather than require BoundExpr: Clone. NULL
      // semantics are SQL-correct: a NULL operand makes the AND NULL, and
      // NOT(NULL) is NULL → the row is excluded under either form.
      let ge = BoundExpr::BinOp(
          Box::new(self.bind_expr(expr, schema)?), BinOp::Ge,
          Box::new(self.bind_expr(low,  schema)?));
      let le = BoundExpr::BinOp(
          Box::new(self.bind_expr(expr, schema)?), BinOp::Le,
          Box::new(self.bind_expr(high, schema)?));
      let between = BoundExpr::BinOp(Box::new(ge), BinOp::And, Box::new(le));
      Ok(if *negated {
          BoundExpr::UnaryOp(UnaryOp::Not, Box::new(between))
      } else {
          between
      })
  }
  ```
- `collect_index_range_candidates(expr, schema, fetch_indexes)` — mirror
  `collect_index_seek_candidates`: collect equality conjuncts (for the leading
  prefix) **and** range ends (incl. desugared `BETWEEN`); for each index, consume
  equality conjuncts as the leading `E` columns, then require column `E` to be
  covered by ≥1 range end. **At most one lower end and one upper end become the
  scan bounds**: take the *first* `>`/`>=` end on column `E` as `(lo, lo_incl)`
  and the *first* `<`/`<=` end as `(hi, hi_incl)`. Do **not** compare or pick the
  most-restrictive among multiple same-side bounds — that would require a
  signed-aware compare of packed native `u128`s in the planner
  (`parse_pk_literal_packed` returns native LE, where a negative value's high bit
  sorts it above positives), the exact `cff7c58`-class trap one layer up. A
  redundant same-side predicate (`x > 5 AND x > 10`) keeps only its first end as
  the bound; the other stays a residual conjunct that `apply_residual_filter`
  trims with negligible overhead. Saturate out-of-range bounds:
  - lower `AboveMax` or upper `BelowMin` ⇒ the range is **provably empty** —
    serve an empty result directly, no server round-trip;
  - lower `BelowMin` ⇒ drop to unbounded-below; upper `AboveMax` ⇒ unbounded-above.

  Emit `(idx_cols, n_eq, eq_vals, lo, lo_incl, hi, hi_incl, residual)`. Reuse the
  **uncovered-trailing-nullable** rejection (`:915`) unchanged. Only the *one*
  lower end and *one* upper end that became the bounds are *consumed* (removed
  from the residual): the OPK-ordered bound is exact for them. Every other
  conjunct — non-range predicates, redundant same-side range ends, a range on a
  non-covered column — stays in the residual and flows through
  `apply_residual_filter` (which binds each via `binder.bind_expr`, so the
  leftover ends must be expressions the binder accepts; see the `BETWEEN` bullet).
- Direct SELECT path (`:683`–`:717`): **try range candidates before the
  equality-prefix candidates.** A pure-equality leading-prefix seek (e.g. `a = 5`
  on index `(a, b)`) otherwise hits first and relegates `b > 10` to an in-memory
  residual filter, defeating the range scan this slice exists to provide. Range
  candidates are never *less* selective than the equality prefix they extend (a
  full all-columns-equality match has no range column, so it produces no range
  candidate and is never shadowed), so range-first is strictly correct.
  Order: **PK equality → range index → equality index → error**. Apply
  `apply_residual_filter` after any hit; fall to the existing "non-indexed" error
  only if nothing serves.

UPDATE/DELETE already work on ranges via their full-scan fallback; routing them
through the same range candidate is a pure optimization and is **out of scope**
(follow-on; same extractor, swap the scan for `seek_by_index_range`).

### 8. Python binding parity (optional)

`gnitz-py` exposes `seek_by_index`; add `seek_by_index_range` for symmetry
(`crates/gnitz-py/src/lib.rs` + `python/gnitz/_client.py`). Not required for the
SQL feature; include only if Python-level range index access is wanted now.

## Correctness invariants to preserve

- **OPK ordering is the whole correctness argument.** The byte compares
  `cur ⋛ lo_prefix/hi_prefix` are valid *only because* the leading-key OPK is
  order-preserving (post-`cff7c58`) — including signed columns (sign-flip) and the
  equality prefix. Never compare the un-encoded native or a non-promoted image.
- **Encode bounds through the existing index encoder.** Build `OPK(lo)`/`OPK(hi)`
  via `index_opk_prefix` / `IndexKeySpec::seek_prefix` with the **source** column
  type, so bound bytes are byte-identical to stored entries
  (`encode_pk_column_promoted` does the source-width sign-extension). A hand-rolled
  bound encode would re-introduce exactly the bug `cff7c58` fixed.
- **Unbounded sentinels are type-correct.** `0x00 * slot` is the OPK minimum and
  `0xFF * slot` the OPK maximum for *every* PK type (the sign flip maps `INT_MIN`
  → all-zeros, `INT_MAX` → all-ones), so an open-ended range is encoded without a
  per-type branch.
- **Source-PK partitioning ⇒ broadcast.** Do not attempt to route a range to a
  single worker; entries scatter by source PK. Broadcast + merge, like the
  non-unique point seek.
- **Worker is the sole OPK encoder.** The client/master ship native bound values
  (and the descriptor) verbatim; only the worker OPK-encodes. Preserves the
  existing trust boundary.
- **Preserve source multiplicity.** Resolve each match with the source row's net
  `current_weight`, never a hardcoded `1`. The point seek already does this
  (`copy_cursor_row_with_weight(.., current_weight)`); the shared resolve helper
  must too, or routing `seek_by_index` through it silently truncates weights and
  breaks the Z-Set group structure (`foundations.md` §1) that downstream
  COUNT/SUM depend on.
- **The engine method self-guards its arity.** `seek_by_index_range` returns
  `Err` when `n_eq >= col_indices.len()` (no range column within the index),
  *before* indexing `columns[n_eq]` / `leading_key_size(n_eq + 1)`. The worker
  enforces the same bound at the trust boundary, but the `pub` method is also
  reachable from tests, and the guard keeps `prefix_len < idx_pk_stride` strict
  so the exclusive-lower `seek_key[prefix_len..]` suffix is never an empty slice.
- **Trust-boundary validation.** Validate the descriptor length and `n_eq` arity
  in the worker before decoding, mirroring the `seek_pk_extra.len() % 16` guard;
  never index past a malformed frame.
- **One flag per leg.** The client request flag (`FLAG_SEEK_BY_INDEX_RANGE`, high
  `u64` wire bit) and the SAL dispatch flag (`FLAG_SEEK_BY_INDEX_RANGE_SAL`, low
  `u32` bit with its own `SalMessageKind`) are distinct. A high-only flag cannot
  be classified by the worker (`classify` reads a `u32`); a low-only flag would
  collide with the wire packed fields. The master writes the SAL flag explicitly
  as `sal_flags`; it is never derived from the client's wire flags.
- **`n_eq == 0` skips the equality spec.** A pure-range predicate passes no
  equality columns; do **not** call `IndexKeySpec::new` with an empty slice (it
  asserts `!cols.is_empty()`). The empty equality prefix is already all-zero.
- **Out-of-range bounds saturate, never wrap.** A literal past the column type's
  range must widen the bound to unbounded or mark the range provably-empty — never
  silently truncate (the `cff7c58`-class bug, one layer up in the planner).
- **The planner never compares two packed native bounds.** Pick the first lower
  and first upper end as the scan bounds and push the rest to the residual. A
  signed-aware compare of `parse_pk_literal_packed`'s native-LE `u128`s (where a
  negative's high bit sorts above positives) would re-introduce the encoding bug
  `cff7c58` fixed — there is no reason to do it: `apply_residual_filter` removes
  the slack exactly.
- **Exclusive lower bounds seek past the boundary group.** Seed the index seek
  key with a `0xFF` (OPK-max) source-PK suffix when the lower bound is finite and
  exclusive, so the binary search lands at the first entry strictly greater than
  the boundary prefix in O(log N) instead of seeking into the duplicate group and
  skipping it one entry at a time (O(N) on a low-cardinality column). The loop's
  `cur == lo_prefix && !lo_incl` trim stays — it still catches the lone entry
  whose source PK is itself all-`0xFF`.

## Migration order

1. **Flags** (§1: the `u64` wire request flag + the `u32` SAL dispatch flag with
   its `SalMessageKind` variant and `classify` arm) + **core client** (§6:
   `message.rs` explicit-blob send entry point, `connection.rs` encode,
   `client.rs` choke point). Compiles standalone; no server consumer yet.
2. **Engine primitive** (§3, `seek_by_index_range`) — unit-testable directly
   against a `CatalogStore` with a seeded index, no IPC.
3. **Worker/executor/master** (§4, §5) — the `SalMessageKind::SeekByIndexRange`
   dispatch arm, the `executor.rs` wire-flag route, and the master fan-out that
   writes `FLAG_SEEK_BY_INDEX_RANGE_SAL` as `sal_flags`. End-to-end reachable via
   the core client; testable from a Rust integration test or the Python binding.
4. **SQL planner** (§7) — direct SELECT range predicates start resolving.
5. **Python parity** (§8, optional).

Each step is independently shippable; after step 3 a low-level client can range-
scan; step 4 makes the SQL surface use it.

## Out of scope

- **The range-join operator and non-equi `ON`-clause planning** — the rest of
  `wide-pk-incremental-views.md` §1 item 1. This slice is the access primitive
  they will reuse, not the join.
- **The range-aware distributed exchange (prerequisite b)** — needed only by the
  join (probe a partitioned index by another relation's delta). Single-table range
  SELECT broadcasts instead, so it is unneeded here.
- **Two-sided ranges on *different* columns** (`a > 1 AND b < 9` with no index
  ordering them together) — not a contiguous index interval; not served. One range
  column (optionally behind equality-pinned leading columns) per scan.
- **ORDER BY / LIMIT pushdown** — the scan yields rows in index order, but the
  SELECT result is an unordered set today; emitting ordered/limited results from
  the range scan is a follow-on.
- **UPDATE/DELETE range-index optimization** — already correct via full-scan
  fallback; indexing them is a trivial follow-on using the same extractor.
- **Streaming/chunked seek replies for large result sets.** Like the existing
  non-streaming point seek-by-index, each worker answers in a single
  `send_response` frame, capped at `MAX_W2M_MSG` (256 MiB) by the `send_encoded`
  assert; the master then accumulates every worker's batch into one merged
  `Batch`. A range matching more than ~256 MiB of rows on one worker therefore
  trips the assert. This is an inherited limitation, not introduced here; chunking
  the seek reply train (as `Scan` already does via `FLAG_CONTINUATION`) is the fix.
  See `plans/chunked-seek-replies.md`.

## Testing

Rust (`gnitz-engine`, `seek_by_index_range` unit tests against a seeded index):

- **Unsigned leading key**: `x` indexed, rows `x ∈ {0,10,20,30}`. Assert each of
  `x > 10`, `x >= 10`, `x < 20`, `x <= 20`, `10 < x < 30`, `10 <= x <= 20` returns
  exactly the right rows; verify inclusive vs exclusive boundary handling at both
  ends. (These are `n_eq == 0` pure-range scans — they also guard against the
  `IndexKeySpec::new` empty-slice assertion firing on no equality prefix.)
- **Out-of-range bound**: `x: I32` indexed. `x > 3000000000` returns empty
  (lower bound above the type max), `x < 3000000000` returns all rows (upper bound
  saturates to unbounded); the equality `x = 3000000000` returns empty, **not**
  the rows holding `-1294967296` — the regression guard for the truncation fix.
- **Signed leading key** (the `cff7c58` payoff): `x: I32` indexed, rows including
  negatives. Assert `x BETWEEN -5 AND 5` returns the contiguous signed interval
  (and that `OPK(-5) < OPK(5)` so the walk does not invert) — this is the test
  that would fail under the old unsigned promotion.
- **Composite `(a,b)`**: `a = 7 AND b < 50` returns only `a==7` rows with `b<50`,
  and the scan stops before `a==8` (assert via a row at `(8, 0)` that must be
  absent).
- **Open-ended**: `x > L` (unbounded above, scans to the index max) and `x < H`
  (unbounded below, starts at the index min) both correct.
- **Exclusive lower over a large duplicate group**: `x` indexed, many rows share
  `x == 10` (distinct source PKs). Assert `x > 10` returns every `x > 10` row and
  **none** of the `x == 10` group, and that an `x == 10` row whose source PK is
  all-`0xFF` (e.g. a `U64` source PK of `u64::MAX`) is still excluded — exercises
  the `0xFF`-suffixed exclusive seed and the surviving boundary trim.
- **Retraction**: insert rows in range, retract some, assert the range scan
  reflects net positive weight (no ghost entries) — the mandatory retraction test
  for any new index access path (dev-guide IPC checklist).
- **Multiplicity preserved** (regression guard for the weight fix): seed the
  index plus a source row at net weight 2 by ingesting a batch directly into the
  `CatalogStore` (bypassing DML unique-PK enforcement, which would otherwise pin
  every base row to weight 1); assert the range scan returns that row at weight 2,
  not 1 — proving the resolve copies `current_weight` rather than a hardcoded
  constant.
- **NULL exclusion**: a row with NULL in the indexed column is absent from results
  of any range predicate.

Multi-worker E2E (`make e2e`, `GNITZ_WORKERS=4`; new test in `test_workers.py`):

- Insert rows whose indexed values scatter across partitions; a range SELECT
  returns the **merged** set from all workers (proves broadcast+merge), and equals
  a full-scan-and-filter reference over the same data.
- Boundary inclusivity and the signed range survive the wire round-trip
  (descriptor encode/decode, master verbatim forward, worker OPK encode).
- **Max-arity descriptor**: a composite index at `PK_LIST_MAX_COLS = 4`
  (`a = .. AND b = .. AND c = .. AND d < ..`, `n_eq = 3`) round-trips. Its
  descriptor is 82 bytes — over the 64-byte `seek_pk_extra`/`PkTuple` cap — so this
  exercises the explicit-blob send path (§6) end-to-end.

SQL (`gnitz-sql`): `SELECT * FROM t WHERE x > k` (and `BETWEEN`, composite
`a = .. AND b < ..`) returns correct rows where it previously raised
`Unsupported`; a range on a **non-indexed** column still raises the clean
"non-indexed" error (no silent wrong-rows); a residual non-range conjunct
(`x > 5 AND y = 'q'`) is applied after the scan via `apply_residual_filter`.
Additionally:

- **Composite served by the range path**: on index `(a, b)`, `a = 5 AND b > 10`
  is served by the composite range scan (range-first ordering), not a bare `a = 5`
  prefix seek + in-memory `b > 10` residual. Assert via a row at `(5, 0)` that must
  be **absent** from the result (it would be present if only `a = 5` were seeked
  and the residual filter were the thing excluding it — distinguish by checking
  the path, e.g. that `(8, 11)` rows never enter the candidate at all).
- **`BETWEEN` desugaring**: `x BETWEEN 10 AND 20` resolves via the range scan;
  `x NOT BETWEEN 10 AND 20` (non-contiguous) falls through to the residual/error
  path rather than being mis-served as a single interval.
- **Residual `BETWEEN` binds**: `x > 5 AND y BETWEEN 1 AND 9` (only `x` indexed)
  is served by the `x > 5` range scan with `y BETWEEN 1 AND 9` as a residual that
  now **binds and filters** (regression guard for the new `Expr::Between` binder
  arm), instead of failing with `Unsupported`. `x > 5 AND y NOT BETWEEN 1 AND 9`
  likewise filters correctly.
- **Redundant same-side bound**: `x > 5 AND x > 10` (index on `x`) returns exactly
  the `x > 10` rows — the scan takes `x > 5` as the bound and the residual
  `x > 10` trims the `(5, 10]` slack — and `x > 10 AND x > 5` returns the same set
  regardless of conjunct order (proves the planner never compares the two packed
  natives to pick the tighter one).
