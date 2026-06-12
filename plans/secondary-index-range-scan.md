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
projector use — `schema::index_opk_prefix(native, src_type, idx_key_type,
slot_size)` (single column) / `IndexKeySpec::seek_prefix` (the equality prefix) —
so the bound bytes are byte-identical to stored entries and signed sign-extension
from the source column width is handled by `encode_pk_column_promoted`. No new
encoding logic.

### The walk (engine, `CatalogStore::seek_by_index_range`)

```
seek_bytes( lo_prefix ‖ 0x00 * (pk_stride - prefix_len) )   // first entry ≥ lo
while cursor.valid {
    let cur = current_pk_bytes()[..prefix_len];
    // upper bound terminates the scan (entries are ordered by leading OPK):
    if cur > hi_prefix                  { break; }
    if cur == hi_prefix && !hi_incl     { break; }
    // lower bound trims the boundary group (the zero-padded seek can land on it):
    if cur < lo_prefix                  { advance(); continue; }   // defensive
    if cur == lo_prefix && !lo_incl     { advance(); continue; }
    if current_weight > 0 {
        // reconstruct source PK from current_pk_bytes()[idx_key_size..], resolve
        // via seek_family_bytes, append to the accumulator — identical to
        // seek_by_index's resolve block.
    }
    advance();
}
```

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

### 1. Wire: new flag `FLAG_SEEK_BY_INDEX_RANGE`

`crates/gnitz-wire/src/flags.rs` — add `pub const FLAG_SEEK_BY_INDEX_RANGE: u64`
at the next free bit. Like `FLAG_GET_INDICES`, this is a **client-only** request
flag (never written to the SAL), so it sits above the SAL-mirror run (bits 0–15),
not inside it. Re-export it from `gnitz-core` (`protocol/header.rs`'s
`pub use gnitz_wire::{…}` block).

Follow the dev-guide IPC-flag checklist: add to the flag module, handle in
`executor.rs`/`worker.rs` dispatch, and (n/a here) `master.rs` for master-side
logic. No new control-block column is added — the range descriptor reuses the
existing `seek_pk_extra` BLOB channel (below).

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

- Resolve the circuit by exact `col_indices` (as `seek_by_index`); compute
  `src_pk_stride`, `idx_key_size = leading_key_size(col_indices.len())`,
  `prefix_len = leading_key_size(n_eq + 1)`, the range slot's offset
  (`leading_key_size(n_eq)`) and size (`index_schema.columns[n_eq].size()`).
- Build `lo_prefix`/`hi_prefix` per **Design**: encode the equality prefix via
  `IndexKeySpec::new(&col_indices[..n_eq], …).seek_prefix(eq_natives)`, then fill
  the range slot with `index_opk_prefix(bound, src_type, idx_key_type, slot)` or
  `0x00`/`0xFF`. `src_type = entry.schema.columns[col_indices[n_eq]].type_code`.
- Run the walk in **Design** §"The walk", reusing `seek_by_index`'s
  source-PK-reconstruction + `seek_family_bytes` resolve block verbatim.

### 4. Worker dispatch (`runtime/worker.rs`) + executor routing (`runtime/executor.rs`)

- `executor.rs` (~`:704`): add an arm `if flags & FLAG_SEEK_BY_INDEX_RANGE != 0`
  → `handle_seek_by_index_range(shared, fd, client_id, target_id, seek_col_idx,
  &seek_pk_extra, client_version)`, under the catalog read lock like the others.
- `worker.rs` (~`:859`): new dispatch arm that `unpack_pk_cols(seek_col_idx)`,
  validates the column list against the schema (reuse the existing check), decodes
  the **range descriptor** from `seek_pk_extra` (§2), validates `n_eq + 1 ≤
  cols.len()`, and calls `store.seek_by_index_range`. Reply-frame plumbing is
  identical to the `FLAG_SEEK_BY_INDEX` arm.

### 5. Master fan-out (`runtime/master.rs`)

`fan_out_seek_by_index_range_collect_async` — a near-clone of
`fan_out_seek_by_index_collect_async` (`:935`): `dispatch_scan_fanout` broadcasts
`FLAG_SEEK_BY_INDEX_RANGE` with `seek_col_idx` and the verbatim descriptor blob,
then accumulates the per-worker batch slots into one merged batch. The descriptor
is forwarded verbatim (the worker is the sole OPK encoder, as for SEEK_BY_INDEX).

### 6. Core client (`gnitz-core`)

- `connection.rs` (next to `seek_by_index`, `:302`): `seek_by_index_range(...)`
  builds the descriptor blob (§2), sends with `pk = &PkTuple::EMPTY`,
  `seek_col_idx = pack_pk_cols(col_indices)`, `flags =
  wire_flags_set_schema_version(FLAG_SEEK_BY_INDEX_RANGE, cached_version)`, and the
  descriptor as the explicit `seek_pk_extra` blob argument; decode the response
  exactly like `seek_by_index`.
- `client.rs` (next to `seek_by_index`, `:190`): public
  `seek_by_index_range(table_id, col_indices, n_eq, eq_vals, lo, lo_incl, hi,
  hi_incl) -> ScanResult`. This is the single choke point for arity validation —
  reuse `validate_pk_col_list`, require `eq_vals.len() == n_eq`,
  `n_eq + 1 ≤ col_indices.len()`, and at least one finite bound. Returns
  `(schema, batch, view_lsn)` like the other seeks.

### 7. SQL planner (`gnitz-sql/dml.rs`) — extract range conjuncts, serve direct SELECT

- `try_col_range_literal(expr, schema) -> Option<(usize /*col*/, RangeEnd)>`
  recognizing `col > lit`, `col >= lit`, `col < lit`, `col <= lit` (and their
  flipped forms `lit < col`, …), reusing `parse_pk_literal_packed` for the literal
  → native u128. `BETWEEN` desugars to `col >= lo AND col <= hi` (two conjuncts,
  already produced by `flatten_conjuncts`, so no special case needed beyond
  merging two ends on the same column).
- `collect_index_range_candidates(expr, schema, fetch_indexes)` — mirror
  `collect_index_seek_candidates`: collect equality conjuncts (for the leading
  prefix) **and** range ends; for each index, consume equality conjuncts as the
  leading `E` columns, then require column `E` to be covered by ≥1 range end (a
  `<`/`<=` and/or a `>`/`>=` on the same column merge into `(lo,lo_incl,hi,
  hi_incl)`). Emit `(idx_cols, n_eq, eq_vals, lo, lo_incl, hi, hi_incl,
  residual)`. Reuse the **uncovered-trailing-nullable** rejection (`:915`)
  unchanged. The range conjunct(s) are *consumed* (removed from the residual): the
  OPK-ordered bound is exact, so re-filtering is unnecessary — but the remaining
  non-range conjuncts still flow through `apply_residual_filter`.
- Direct SELECT path (`:683`–`:741`): after the PK-seek and equality-index
  attempts, try range candidates via `client.seek_by_index_range(...)`, applying
  `apply_residual_filter` after a hit; only fall to the existing "non-indexed"
  error if no equality **or** range candidate serves. Order: PK equality →
  equality index → range index → error.

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
- **Trust-boundary validation.** Validate the descriptor length and `n_eq` arity
  in the worker before decoding, mirroring the `seek_pk_extra.len() % 16` guard;
  never index past a malformed frame.

## Migration order

1. **Wire flag** (§1) + **core client** (§6, `connection.rs` encode +
   `client.rs` choke point). Compiles standalone; no server consumer yet.
2. **Engine primitive** (§3, `seek_by_index_range`) — unit-testable directly
   against a `CatalogStore` with a seeded index, no IPC.
3. **Worker/executor/master** (§4, §5) — end-to-end reachable via the core client;
   testable from a Rust integration test or the Python binding.
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

## Testing

Rust (`gnitz-engine`, `seek_by_index_range` unit tests against a seeded index):

- **Unsigned leading key**: `x` indexed, rows `x ∈ {0,10,20,30}`. Assert each of
  `x > 10`, `x >= 10`, `x < 20`, `x <= 20`, `10 < x < 30`, `10 <= x <= 20` returns
  exactly the right rows; verify inclusive vs exclusive boundary handling at both
  ends.
- **Signed leading key** (the `cff7c58` payoff): `x: I32` indexed, rows including
  negatives. Assert `x BETWEEN -5 AND 5` returns the contiguous signed interval
  (and that `OPK(-5) < OPK(5)` so the walk does not invert) — this is the test
  that would fail under the old unsigned promotion.
- **Composite `(a,b)`**: `a = 7 AND b < 50` returns only `a==7` rows with `b<50`,
  and the scan stops before `a==8` (assert via a row at `(8, 0)` that must be
  absent).
- **Open-ended**: `x > L` (unbounded above, scans to the index max) and `x < H`
  (unbounded below, starts at the index min) both correct.
- **Retraction**: insert rows in range, retract some, assert the range scan
  reflects net positive weight (no ghost entries) — the mandatory retraction test
  for any new index access path (dev-guide IPC checklist).
- **NULL exclusion**: a row with NULL in the indexed column is absent from results
  of any range predicate.

Multi-worker E2E (`make e2e`, `GNITZ_WORKERS=4`; new test in `test_workers.py`):

- Insert rows whose indexed values scatter across partitions; a range SELECT
  returns the **merged** set from all workers (proves broadcast+merge), and equals
  a full-scan-and-filter reference over the same data.
- Boundary inclusivity and the signed range survive the wire round-trip
  (descriptor encode/decode, master verbatim forward, worker OPK encode).

SQL (`gnitz-sql`): `SELECT * FROM t WHERE x > k` (and `BETWEEN`, composite
`a = .. AND b < ..`) returns correct rows where it previously raised
`Unsupported`; a range on a **non-indexed** column still raises the clean
"non-indexed" error (no silent wrong-rows); a residual non-range conjunct
(`x > 5 AND y = 'q'`) is applied after the scan via `apply_residual_filter`.
