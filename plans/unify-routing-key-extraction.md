# Routing-key extraction: single-home the last two drift hazards

## Verdict up front

**The routing-key encodings are already single-homed; the three "duplicate"
key functions are three thin policies whose differences are load-bearing,
documented, and test-pinned. Do not merge them.** What remains — and what this
plan implements — are the two genuine drift hazards the research found:

- **A.** The relay-scatter PK-routing gate is duplicated verbatim (with its
  load-bearing comment) at two sites. Single-home it in `router.rs`.
- **B.** The master-side index-routing cache (`PartitionRouter` +
  `extract_col_key`) lives in `ops/exchange/router.rs`, three layers away from
  `index_route_key` (`runtime/orchestration/master/mod.rs:676`) — the function
  it must byte-agree with. Both are master-only code. Relocate them into one
  master module so the coherence pair and its pinning tests are adjacent.

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
- `extract_col_key` (`ops/exchange/router.rs:22`): the index-routing-cache
  image — NULL → 0, string → 64-bit content hash — documented at
  `router.rs:31-37` as deliberately **not** matching the 128-bit join-scatter
  image, because its only counterpart is `index_route_key` (string seeks fall
  back to broadcast).

### Rejected: one policy-parameterized key function

Merging the three into `routing_key(…, policy)` centralizes nothing that is
not already central (the encodings) and turns three ~25-line functions, each
carrying its own correctness comment, into one function with a three-way
NULL-policy × string-policy branch matrix — including combinations no caller
may ever use. Net lines ≈ 0; readability and the documented divergences lose.

### Rejected: unifying the three string-key images

`extract_col_key`'s xxh64 image, `german_string_promote_key`'s xxh128 image,
and `hash_german_string_content`'s length-prefixed stream are each one half of
a different coherence pair (index cache ↔ `index_route_key`; `_join_pk` writer
↔ scatter router; row-identity hash ↔ group fold). Each pair already shares
one function; collapsing across pairs couples consumers that the code
explicitly documents as independent (`router.rs:31-37`) and changes hash
images for zero behavioral gain.

## A. Single-home the relay-scatter PK-routing gate

The identical expression + comment pair lives at
`ops/exchange/relay.rs:122-130` (`op_repartition_batches_mode`) and
`relay.rs:319-325` (`relay_scatter_merge_walk`):

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
`target_tcs` is empty — so the test helper cannot drift from production
either.

Signatures validated: `pk_indices() -> &[u32]`
(`crates/gnitz-engine/src/schema.rs:365`),
`key_is_promoted(&[u8]) -> bool` (`router.rs:263`), both call sites have
`col_indices: &[u32]`, `target_tcs: &[u8]`, `schema: &SchemaDescriptor` in
scope.

## B. Colocate the index-routing coherence pair on the master side

`PartitionRouter` is master-only state: constructed at
`runtime/orchestration/master/dispatch.rs:38`, written at `dispatch.rs:783`
(`record_routing_from_source`, from the commit path), read at
`dispatch.rs:908` (`worker_for_index_key`, seek fan-out), field at
`master/mod.rs:293`. Nothing in `ops`, `query`, `catalog`, or the worker uses
it. Its key image (`extract_col_key`) must byte-agree with `index_route_key`
(`master/mod.rs:677-690`) — a pair currently spread across two layers and
kept honest by comments on both ends plus a master-side test.

Create `crates/gnitz-engine/src/runtime/orchestration/master/index_router.rs`:

```rust
//! Master-side unique-index routing cache: `PartitionRouter` maps
//! (table_id, col_idx, key) → worker, recorded on commit and read to unicast
//! index seeks instead of broadcasting. `extract_col_key` (batch-row side)
//! and `index_route_key` (native-seek side) are the two halves of one key
//! image and MUST byte-agree — both reduce to
//! `widen_pk_be(encode_pk_column(native))` for integer columns; STRING/BLOB
//! keys are a 64-bit content hash on the record side and `None` on the seek
//! side (string seeks broadcast). They live in this one module, with the
//! pinning tests, so the pair cannot drift apart.
```

Move, verbatim (no signature changes):

1. `extract_col_key` (`ops/exchange/router.rs:20-51`) — becomes private to
   the new module.
2. `PartitionRouter` + impl (`router.rs:108-187`, including the section
   header and doc), including the `#[cfg(test)] record_routing` helper.
   Visibility drops from `pub` to `pub(super)` (only `master/mod.rs` and
   `master/dispatch.rs` touch it), and `record_routing`'s test-only
   `pub(crate)` (`router.rs:167`) drops to private — its sole test caller
   becomes same-module.
3. `index_route_key` (`master/mod.rs:669-690`, doc comment included) —
   becomes `pub(super)`.
4. Tests: `extract_col_key_handles_blob_column`, `partition_router_basic`,
   `test_partition_router_string_col` (from `router.rs`'s test mod) and
   `index_route_key_hits_signed_routing_cache` (from `master/mod.rs:916`).
   Test builders split by their remaining users: `make_schema_u64_string` and
   `make_batch_str` **move** (their only users are the two moved string
   tests; leaving them behind is dead code and a clippy error under
   `make verify`); `make_schema_u64_i64` and `make_batch` are **copied**
   (the originals stay — the partition-filter tests and
   `test_partition_routing_invariance_narrow_pk` at `router.rs:606` still use
   them).

Reference updates:

- `runtime/orchestration/master/mod.rs`: add `mod index_router;`, change the
  field type path (`:293`), drop `PartitionRouter` from the `ops` import
  (`:16`; `RouteMode` and the rest stay — they are genuinely shared scatter
  policy).
- `runtime/orchestration/master/dispatch.rs:38, 783, 908`: import from
  `super::index_router`.
- `ops/exchange/mod.rs:11` and `ops/mod.rs:21`: drop the `PartitionRouter`
  re-export.
- `ops/exchange/router.rs`: drop the now-unused `FxHashMap` import and the
  moved code; `extract_group_key` import stays (used by
  `route_partition_key`).

Dependency check: the new module needs `SchemaDescriptor`, `ColumnLocator`
(via `schema.locate`), `german_string_content`, `foundation::xxh::checksum`,
`MemBatch`/`Batch` — all below `runtime` in the layering, so no new
wrong-direction edge. `ops/exchange/router.rs` loses its only master-serving
code and keeps the scatter-side policies (`RouteMode`,
`route_partition_key`, `hash_row_for_partition`, `route_nonpk_row`,
`compound_join_packer`, `key_is_promoted`, `fill_worker_indices`,
`op_partition_filter`).

## Verification

- `make verify` — fmt, clippy-as-errors, full unit tests. The moved pinning
  tests (`partition_router_basic`, `test_partition_router_string_col`,
  `extract_col_key_handles_blob_column`,
  `index_route_key_hits_signed_routing_cache`) and the untouched
  `extract_group_key_cursor_matches_batch` must pass unmodified — no hash
  image changes anywhere in this plan.
- `make e2e` (multi-worker, `GNITZ_WORKERS=4`) — item A touches the exchange
  scatter gate; the join/group-by/set-op suites exercise both scatter paths
  and would catch a co-partition regression.
