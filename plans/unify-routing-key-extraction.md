# Routing-key extraction: single-home the last two drift hazards

## Verdict up front

**The routing-key encodings are already single-homed; the three "duplicate"
key functions are three thin policies whose differences are load-bearing,
documented, and test-pinned. Do not merge them.** What remains — and what this
plan implements — are the two genuine drift hazards the research found, plus a
correctness cleanup that falls out of colocating the second pair:

- **A.** The relay-scatter PK-routing gate is duplicated verbatim (with its
  load-bearing comment) at two sites. Single-home it in `router.rs`.
- **B.** The master-side index-routing cache (`PartitionRouter` +
  `extract_col_key`) lives in `ops/exchange/router.rs`, three layers away from
  `index_route_key` (`runtime/orchestration/master/mod.rs:677`) — the function
  it must byte-agree with. Both are master-only code. Relocate them into one
  master module so the coherence pair and its pinning tests are adjacent, and in
  doing so **drop STRING/BLOB from the cache**: their seeks always broadcast
  (`index_route_key` returns `None`), so every string entry the record path
  hashes in is dead weight no seek can ever probe. Excluding them at the record
  site makes `extract_col_key` integer-only and collapses its documented
  string-image divergence entirely.

## Ground truth: what is already single-homed

Every byte-level key encoding has exactly one definition; the "three copies"
named in the commissioning finding are policy selectors over these:

| Shared encoding | Home | Used by |
|---|---|---|
| OPK encode / widen (`encode_pk_column`, `widen_pk_be`) | `gnitz-wire` | every key path, incl. `index_route_key` |
| `pk_route_key` / `payload_route_key` (canonical sign-flipped route image) | `schema` | group-key fold, single-col fast paths, `ColumnLocator::route_key` |
| `partition_for_pk_bytes` / `partition_for_key` | `schema::key` (via storage facade) | every partition decision |
| `german_string_promote_key` (128-bit string `_join_pk` image) | `ops/reindex.rs:79` | `PkPromoter::read_string` (trace side) AND `route_partition_key` (scatter side) |
| `hash_german_string_content` (length-prefixed string fold input) | `ops/util.rs:292` | group-key fold AND `reindex_hash_row` |
| The group-key fold itself | `ops/util.rs:408` `extract_group_key_row`, one body over batch + cursor via `GroupKeyRow` | `extract_group_key`, `extract_group_key_cursor`; pinned by `extract_group_key_cursor_matches_batch` (`ops/reduce/tests.rs:5108`) |
| Pack-or-hash scatter decision | `ops/exchange/router.rs:263-310` `key_is_promoted` / `compound_join_packer` / `route_nonpk_row` | both scatter implementations |

The three policy functions and why each divergence must stay:

- `extract_group_key` (`ops/util.rs:379`): NULL hashes **distinct** — it also
  mints GROUP BY output PKs in `op_reduce`, where a NULL group and a 0 group
  must not collide on one PK (`router.rs:199-203`).
- `route_partition_key` (`ops/exchange/router.rs:204`): a NULL single int
  column routes by its **raw zero bytes**, and a string by
  `german_string_promote_key` — both must match the `_join_pk` bytes
  `PkPromoter::promote_into` writes, or a LEFT-join NULL-key bypass row lands
  on a worker that does not own its `_join_pk` partition and the view scan
  misses it (`router.rs:193-203`). `RouteMode` exists precisely to select
  between this and `extract_group_key`.
- `extract_col_key` (the index-routing-cache image): **integer/U128/UUID only**
  — NULL → 0, OPK-encode-and-widen otherwise. STRING/BLOB never reach it (part B
  excludes them at the record site), so it carries no string image. Its sole
  counterpart is `index_route_key`, which rebuilds the identical key from a
  native seek value.

### Rejected: one policy-parameterized key function

Merging the three into `routing_key(…, policy)` centralizes nothing that is
not already central (the encodings) and turns three thin functions, each
carrying its own correctness comment, into one function with a NULL-policy ×
string-policy branch matrix — including combinations no caller may ever use.
Net lines ≈ 0; readability and the documented divergences lose.

### Rejected: unifying the two string-key images

`german_string_promote_key`'s xxh128 image and `hash_german_string_content`'s
length-prefixed stream are each one half of a different coherence pair
(`_join_pk` writer ↔ scatter router; row-identity hash ↔ group fold). Each pair
already shares one function; collapsing across pairs couples consumers the code
documents as independent and changes hash images for zero behavioral gain.
(`extract_col_key` no longer holds a third string image — it is integer-only.)

## A. Single-home the relay-scatter PK-routing gate

The identical expression + comment pair lives at
`ops/exchange/relay.rs:130` (`op_repartition_batches_mode`) and
`relay.rs:325` (`relay_scatter_merge_walk`):

```rust
let is_pk_routing = col_indices == schema.pk_indices() && !key_is_promoted(target_tcs);
```

Add to `ops/exchange/router.rs`, next to `key_is_promoted` (`:263`):

```rust
/// One home for the relay-scatter "route by native PK bytes" gate: strict
/// sequence equality with the schema's PK list (set equality would route a
/// permuted compound PK differently from `partition_for_pk_bytes`, which
/// hashes OPK bytes in schema order) AND no cross-width promotion (a promoted
/// key packs at the wider `T`; its narrow source PK bytes must not route
/// natively). Shared by `op_repartition_batches_mode` and
/// `relay_scatter_merge_walk` so the two scatter paths cannot drift.
///
/// Deliberately NOT used by `fill_worker_indices`: the write-path scatter
/// routes by the table's distribution prefix (`schema.partition_for_pk`), a
/// different hash domain with no promotion concept.
#[inline]
pub(super) fn scatter_is_pk_routed(col_indices: &[u32], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    col_indices == schema.pk_indices() && !key_is_promoted(target_tcs)
}
```

At both relay call sites, replace the local computation with
`let is_pk_routing = scatter_is_pk_routed(col_indices, target_tcs, schema);`
and trim each site's duplicated rationale block down to one line pointing at
the shared gate (keep the site-specific sentences: the `partition_for_pk_bytes`
hoist note at the first site, the co-partition note at the second).

A third, weaker copy of the gate lives in the `#[cfg(test)]`
`op_repartition_batch` helper at `relay.rs:59` (`col_indices ==
schema.pk_indices()`, no promotion term because the helper takes no
`target_tcs`). It gates layout propagation there, not routing (the helper
routes via `compute_worker_indices`); route it through the shared gate as
`scatter_is_pk_routed(col_indices, &[], schema)` — identical when
`target_tcs` is empty (`key_is_promoted(&[]) == false`) — so the test helper
cannot drift from production either.

Import fix: with both gate sites now calling `scatter_is_pk_routed`,
`key_is_promoted` is no longer named directly in `relay.rs`. Change its
`use super::router::{…}` (`relay.rs:14`) to drop `key_is_promoted` and add
`scatter_is_pk_routed`; leaving `key_is_promoted` would be an unused import and
fail `make verify`'s clippy-as-errors gate.

Signatures validated: `pk_indices() -> &[u32]`
(`crates/gnitz-engine/src/schema.rs:365`),
`key_is_promoted(&[u8]) -> bool` (`router.rs:263`), both call sites have
`col_indices: &[u32]`, `target_tcs: &[u8]`, `schema: &SchemaDescriptor` in
scope.

## B. Colocate and simplify the index-routing coherence pair

`PartitionRouter` is master-only state: constructed at
`runtime/orchestration/master/dispatch.rs:38`, written at `dispatch.rs:783`
(`record_routing_from_source`, from the commit path), read at
`dispatch.rs:908` (`worker_for_index_key`, seek fan-out), field at
`master/mod.rs:294`. Nothing in `ops`, `query`, `catalog`, or the worker uses
it. Its key image (`extract_col_key`) must byte-agree with `index_route_key`
(`master/mod.rs:677-690`) — a pair currently spread across two layers and
kept honest by comments on both ends plus a master-side test.

### B.1 Exclude STRING/BLOB from the cache

`index_route_key` returns `None` for STRING/BLOB, so a unique-index seek on a
string column always falls through to the broadcast path — it never probes
`worker_for_index_key`. But `record_index_routing` records **every**
single-column unique circuit unconditionally, so it hashes each string value
via `extract_col_key` and inserts a `(table, col, hash) → worker` entry that no
seek can ever read. Those entries are pure waste: an unbounded map growing with
the table's distinct-string cardinality, plus an `xxh` per string per commit,
for a routing decision the seek side structurally cannot use (and the DELETE
path, carrying filler payload, cannot even retract them correctly).

Skip them at the record site. In `record_index_routing`
(`dispatch.rs:770-793`), after resolving the single unique column:

```rust
for ci in 0..n_idx {
    // The routing cache is keyed (table, col, u128); only SINGLE-COLUMN
    // unique circuits populate it. A composite unique seek broadcasts-and-
    // merges instead, so its routing is never recorded.
    let col_idx = match cat.unique_index_circuit_cols(target_id, ci) {
        Some(c) if c.len() == 1 => c[0],
        _ => continue,
    };
    // STRING/BLOB unique seeks broadcast (`index_route_key` returns None), so a
    // cached (table, col, hash) → worker entry is never probed. Recording it
    // only grows the map with every distinct string value and hashes on every
    // commit, for a routing decision no seek can use. Skip it — this also keeps
    // `extract_col_key` integer-only (B.2).
    if gnitz_wire::is_german_string(schema.columns[col_idx as usize].type_code) {
        continue;
    }
    for (w, wi) in worker_indices[..self.num_workers].iter().enumerate() {
        if !wi.is_empty() {
            self.router.record_routing_from_source(
                source_batch,
                wi,
                schema,
                target_id as u32,
                col_idx,
                w as u32,
            );
        }
    }
}
```

`is_german_string(u8) -> bool` (`gnitz-wire`) covers both STRING and BLOB;
`col_idx` here is a valid unique-circuit column (`< num_columns() ≤ MAX_COLUMNS`),
so `schema.columns[col_idx as usize]` is in bounds. This changes nothing a seek
observes — string seeks already broadcast and find their rows — it only stops
the cache from accreting entries it can never serve.

### B.2 `extract_col_key` becomes integer-only

With B.1 filtering STRING/BLOB out of the sole caller, `extract_col_key`'s
content-hash branch and its NULL-string wild-heap guard are dead production
code. Drop them; the function reduces to the integer/U128/UUID arm plus a NULL
guard for nullable integer columns:

```rust
/// Integer/U128/UUID index-routing-cache key for `col_idx` at `row`: a PK column
/// widens its OPK bytes; an integer payload column OPK-encodes then widens
/// (sign-flipping signed columns), so equal logical values route to the same
/// partition as `partition_for_pk_bytes` and the key is rebuilt bit-for-bit by
/// `index_route_key` from a native seek value. A NULL (nullable integer column)
/// shares key 0. STRING/BLOB columns never reach here — `record_index_routing`
/// excludes them (their seeks broadcast, so a cache entry could never be probed).
fn extract_col_key(mb: &MemBatch<'_>, row: usize, col_idx: usize, schema: &SchemaDescriptor) -> u128 {
    let loc = schema.locate(col_idx);
    debug_assert!(
        !gnitz_wire::is_german_string(loc.type_code()),
        "extract_col_key: STRING/BLOB excluded from the routing cache upstream"
    );
    // NULL routes to key 0. PK columns are never null, so this only fires for a
    // nullable payload column.
    if loc.is_null(mb, row) {
        return 0u128;
    }
    loc.route_key(mb, row)
}
```

`route_key` returns a raw low-8-byte image (never panics) for a STRING/BLOB
column, so the `debug_assert` is a documented tripwire, not a crash guard — the
key it would produce is simply never stored. `record_routing_from_source` (the
insert/remove loop) is unchanged and moves verbatim.

### B.3 Relocation

Create `crates/gnitz-engine/src/runtime/orchestration/master/index_router.rs`:

```rust
//! Master-side unique-index routing cache: `PartitionRouter` maps
//! (table_id, col_idx, key) → worker, recorded on commit and read to unicast
//! integer unique-index seeks instead of broadcasting. `extract_col_key`
//! (batch-row side) and `index_route_key` (native-seek side) are the two halves
//! of one key image and MUST byte-agree — both reduce to
//! `widen_pk_be(encode_pk_column(native))` for integer columns. STRING/BLOB are
//! excluded from the cache on both sides (`record_index_routing` skips them;
//! `index_route_key` returns `None`), so no string image lives here. Keeping the
//! pair and its pinning tests in one module is what stops them drifting apart.
```

Move, with the signature changes above:

1. `extract_col_key` (integer-only per B.2) — private to the new module.
2. `PartitionRouter` + impl (`router.rs:112-187`, including the section header
   and doc). Visibility drops from `pub` to `pub(super)` (only `master/mod.rs`
   and `master/dispatch.rs` touch it). The `#[cfg(test)] record_routing`
   batch-scan helper is **deleted**, not moved — it is a test-only near-duplicate
   of `record_routing_from_source`; its callers switch to the production method
   (B.4).
3. `index_route_key` (`master/mod.rs:670-690`, doc comment included) — becomes
   `pub(super)`.

### B.4 Tests

- **Delete** the two STRING/BLOB tests (`extract_col_key_handles_blob_column`,
  `test_partition_router_string_col`) and their sole builders
  (`make_schema_u64_string`, `make_batch_str`, `router.rs:441-493`): they exercise
  a code path that no longer exists (the record path skips strings; the extractor
  is integer-only). Leaving them dead would fail clippy under `make verify`.
- **Move and rewrite** `partition_router_basic` and
  `index_route_key_hits_signed_routing_cache` (from `master/mod.rs:916`) into the
  new module's `tests`, calling the production `record_routing_from_source`
  directly (the deleted helper's job) with a one-element index slice:

```rust
#[test]
fn partition_router_basic() {
    let schema = make_schema_u64_i64();
    let mut router = PartitionRouter::new();

    let b = make_batch(&schema, &[(42, 1, 0)]);
    router.record_routing_from_source(&b, &[0], &schema, 1, 0, 3);
    assert_eq!(router.worker_for_index_key(1, 0, 42), 3);
    assert_eq!(router.worker_for_index_key(1, 0, 99), -1);

    // Retract: negative weight removes the entry.
    let b2 = make_batch(&schema, &[(42, -1, 0)]);
    router.record_routing_from_source(&b2, &[0], &schema, 1, 0, 3);
    assert_eq!(router.worker_for_index_key(1, 0, 42), -1);
}

#[test]
fn index_route_key_hits_signed_routing_cache() {
    // The cache stores `extract_col_key` (OPK-widened) keys; a seek arrives with
    // the native value. For a signed column the two differ, so a raw native
    // query misses and `index_route_key` must transform it to hit.
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::I64, 0)], &[0]);
    let mut opk = [0u8; 8];
    gnitz_wire::encode_pk_column(&(-7i64).to_le_bytes(), type_code::I64, &mut opk);

    // Single I64 PK column, no payload: one row is PK bytes + weight + null word.
    // (Inlined here — `build_check_batch_pkbuf` is private to `master/mod.rs`.)
    let mut batch = Batch::with_schema(schema, 1);
    batch.extend_pk_bytes(&opk);
    batch.extend_weight(&1i64.to_le_bytes());
    batch.extend_null_bmp(&0u64.to_le_bytes());
    batch.count += 1;

    let mut router = PartitionRouter::new();
    router.record_routing_from_source(&batch, &[0], &schema, 1, 0, 2);

    let native = (-7i64) as u64 as u128;
    // Raw native query misses: native != OPK-widened for a signed column.
    assert_eq!(router.worker_for_index_key(1, 0, native), -1);
    // Transformed query hits.
    let rk = index_route_key(&schema, 0, native).expect("integer column has a route key");
    assert_eq!(router.worker_for_index_key(1, 0, rk), 2);
}
```

`make_schema_u64_i64` and `make_batch` are **copied** into the new module's
`tests` (the originals stay — `router.rs`'s partition-filter tests and
`test_partition_routing_invariance_narrow_pk` still use them). The new `tests`
module needs:

```rust
use super::*;
use crate::schema::{type_code, SchemaColumn};
use crate::storage::Layout; // make_batch certifies its layout
```

(`SchemaDescriptor`, `Batch`, and `MemBatch` come in via `super::*` from the
module body's own imports.)

### B.5 Reference updates

- `runtime/orchestration/master/mod.rs`: add `mod index_router;`; change the
  `router` field type path (`:294`) and drop the now-moved `index_route_key`
  definition/doc; drop `PartitionRouter` from the `ops` import (`:16`) —
  `RouteMode` and the rest stay (genuinely shared scatter policy).
- `runtime/orchestration/master/dispatch.rs`: import `index_route_key` from
  `super::index_router` (`:907`); `record_routing_from_source` and
  `worker_for_index_key` are `self.router.…` method calls needing no new import,
  and `PartitionRouter::new()` at `:38` resolves through `master/mod.rs`'s import.
- `ops/exchange/mod.rs:11` and `ops/mod.rs:21`: drop the `PartitionRouter`
  re-export.
- `ops/exchange/router.rs`: drop the moved code (`extract_col_key`,
  `PartitionRouter`, the three moved/deleted tests, the two string builders) and
  the now-unused `use rustc_hash::FxHashMap;` (`:6`, sole user was
  `PartitionRouter`). `extract_group_key` import stays (used by
  `route_partition_key`).

Dependency check: the new module needs `SchemaDescriptor`, `ColumnLocator`
(via `schema.locate`), `foundation::xxh` is **no longer needed** (integer-only),
`MemBatch`/`Batch` — all below `runtime` in the layering, so no new
wrong-direction edge. `ops/exchange/router.rs` loses its only master-serving
code and keeps the scatter-side policies (`RouteMode`, `route_partition_key`,
`hash_row_for_partition`, `route_nonpk_row`, `compound_join_packer`,
`key_is_promoted`, `scatter_is_pk_routed`, `fill_worker_indices`,
`op_partition_filter`).

## Not in scope

- **Untrusted `col_idx` bounds.** The client seek path validates every column
  index against `num_columns()` in `validated_index_cols` (`executor.rs:975`)
  before dispatch, the internal `seek_unique_holder` path uses catalog-defined
  columns, and `num_columns() ≤ MAX_COLUMNS` is asserted at schema creation — so
  `index_route_key`'s `columns[col_idx]` can never index out of bounds. No guard
  is added; one would duplicate the deliberate single validation point.
- **Retract-then-insert cache eviction.** The master's `record_index_routing`
  never sees a batch carrying both a `+1` and a `-1` for the same real key:
  UPDATE and ON CONFLICT DO UPDATE emit only `+1` rows (the old-value retraction
  is synthesized downstream by the worker's `enforce_unique_pk`), and DELETE
  emits only `-1` rows with filler payload (`client.rs:530`). No reordering fix
  is needed.

## Verification

- `make verify` — fmt, clippy-as-errors, full unit tests. The moved pinning
  tests (`partition_router_basic`, `index_route_key_hits_signed_routing_cache`)
  and the untouched `extract_group_key_cursor_matches_batch` must pass — no hash
  image changes on any surviving path.
- `make e2e` (multi-worker, `GNITZ_WORKERS=4`) — item A touches the exchange
  scatter gate (join/group-by/set-op suites exercise both scatter paths); the
  unique-index suites, including a string/BLOB unique index, confirm B.1 leaves
  seek results unchanged (string seeks still broadcast and find their rows).
