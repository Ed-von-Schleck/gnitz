# Key-, routing-, and merge-walk hot-path fixes

A set of independent, self-contained fixes across the engine's PK-key packing,
exchange routing, and merge-walk hot paths. Each preserves observable behaviour
unless noted, and each is gated by an existing or moved test. The sections are
orthogonal and may land in any order.

## 1. `pack_pk_be`: register-load the wide and common-narrow widths

`crates/gnitz-engine/src/schema/key.rs`. The wildcard arm packs every width other
than 8/16 through a 16-byte stack buffer and a runtime-length `copy_from_slice` —
including all `> 16` keys (whose leading 16 bytes are a plain register load) and
the common `u16`/`u32` single-column PKs. `pack_pk_be` is the packer behind
`pk_sort_key` (relay `order_cache`, `sort_consolidate_inner` sort keys) and
`compare_pk_ordering` / `pk_bytes_eq` (every N-way merge comparison), so the stack
copy sits on the hot comparator path. Fold the `16` arm and the `> 16` case into
one `len >= 16` register load (which also drops the wildcard's now-dead
`len.min(16)`), and add `2`/`4` arms for the narrow scalars:

```rust
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
        // 1/3/5/6/7/9..=15: odd narrow widths — pad-and-copy (len < 16 here, so
        // the whole slice is copied and the old `len.min(16)` is unnecessary).
        len => {
            let mut buf = [0u8; 16];
            buf[..len].copy_from_slice(pk_bytes);
            u128::from_be_bytes(buf)
        }
    }
}
```

Update the doc comment's "`{8, 16}` arms" wording to "`{2, 4, 8, ≥16}` arms".
`pack_pk_be_specialization_matches_naive` (widths 1/2/4/8/16/24/80) already pins
every arm value-identical to the pad-and-copy. `u128::from_opk` is `pack_pk_be`, so
it inherits the `≥16` register load for free.

## 2. `PkSortKey::from_opk`: register-load the dominant seek widths

`crates/gnitz-engine/src/schema/key.rs`. `from_opk` is the comparator body of the
OPK seek fast path: `lower_bound_opk` / `gallop_opk`
(`storage/repr/columnar.rs`) call it once per binary-search / gallop step. The
`u64` and `[u128; 2]` impls pad-copy through a stack buffer even at the exact
dominant stride (8 / 32 bytes). Add the exact-width register load:

```rust
impl PkSortKey for u64 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u64 {
        // Dispatched only for strides ≤ 8; the `== 8` arm loads the dominant
        // U64/I64 key straight into a register (no memcpy). The narrower strides
        // left-align at the MSB end so a raw `u64` compare is the OPK byte order.
        if opk.len() == 8 {
            u64::from_be_bytes(opk.try_into().unwrap())
        } else {
            let mut x = [0u8; 8];
            x[..opk.len()].copy_from_slice(opk);
            u64::from_be_bytes(x)
        }
    }
}

impl PkSortKey for [u128; 2] {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> [u128; 2] {
        // Dispatched only for 17..=32-byte strides: hi = the full leading 16 bytes
        // (always a register load), lo = the trailing 1..=16 left-aligned. Array
        // `Ord` is lexicographic, so the low limb settles a leading-16-byte tie a
        // bare `u128` prefix would tie on.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
            let mut lo = [0u8; 16];
            lo[..opk.len() - 16].copy_from_slice(&opk[16..]);
            [hi, u128::from_be_bytes(lo)]
        }
    }
}
```

`lower_bound_opk_matches_byte_search` (strides 4/8/12/16/24/32/40) drives
`from_opk` through both arms against the byte oracle and pins them. The `u128`
impl needs no change — it delegates to `pack_pk_be`.

## 3. Single-home the relay-scatter PK-routing gate

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

## 4. Colocate and simplify the index-routing coherence pair

`PartitionRouter` is master-only state: constructed at
`runtime/orchestration/master/dispatch.rs:38`, written at `dispatch.rs:783`
(`record_routing_from_source`, from the commit path), read at
`dispatch.rs:908` (`worker_for_index_key`, seek fan-out), field at
`master/mod.rs:294`. Nothing in `ops`, `query`, `catalog`, or the worker uses
it. Its key image (`extract_col_key`) must byte-agree with `index_route_key`
(`master/mod.rs:677-690`) — a pair currently spread across two layers and
kept honest by comments on both ends plus a master-side test. Colocate them into
one master module so the coherence pair and its pinning tests are adjacent, and
in doing so drop STRING/BLOB from the cache: their seeks always broadcast
(`index_route_key` returns `None`), so every string entry the record path hashes
in is dead weight no seek can ever probe.

### 4.1 Exclude STRING/BLOB from the cache

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
    // `extract_col_key` integer-only (§4.2).
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

### 4.2 `extract_col_key` becomes integer-only

With §4.1 filtering STRING/BLOB out of the sole caller, `extract_col_key`'s
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

### 4.3 Relocation

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

1. `extract_col_key` (integer-only per §4.2) — private to the new module.
2. `PartitionRouter` + impl (`router.rs:112-187`, including the section header
   and doc). Visibility drops from `pub` to `pub(super)` (only `master/mod.rs`
   and `master/dispatch.rs` touch it). The `#[cfg(test)] record_routing`
   batch-scan helper is **deleted**, not moved — it is a test-only near-duplicate
   of `record_routing_from_source`; its callers switch to the production method
   (§4.4).
3. `index_route_key` (`master/mod.rs:670-690`, doc comment included) — becomes
   `pub(super)`.

### 4.4 Tests

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

### 4.5 Reference updates

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
  `route_partition_key`). Keeps the scatter-side policies (`RouteMode`,
  `route_partition_key`, `hash_row_for_partition`, `route_nonpk_row`,
  `compound_join_packer`, `key_is_promoted`, `scatter_is_pk_routed`,
  `fill_worker_indices`, `op_partition_filter`).

Dependency check: the new module needs `SchemaDescriptor`, `ColumnLocator`
(via `schema.locate`), `MemBatch`/`Batch` — all below `runtime` in the layering,
so no new wrong-direction edge. `foundation::xxh` is no longer needed
(integer-only).

## 5. `PreflightKeyStream`: drop the view before its backing slot

`crates/gnitz-engine/src/runtime/orchestration/master/preflight.rs`. `mb` is a
lifetime-erased zero-copy view into `slot`'s W2M ring bytes; the struct doc and
`attach_frame` both state "the view must die before its backing slot". Rust
drops fields in declaration order, but `slot` is declared before `mb`, so the
struct's own drop glue releases the ring slot *before* the view — the reverse of
the invariant. `MemBatch` has no `Drop` today, so this is latent rather than a
live use-after-free, but the ordering must be correct by construction. Declare
`mb` before `slot`:

```rust
struct PreflightKeyStream {
    /// Worker index (error attribution) and scan request id (frame pulls).
    w: usize,
    req_id: u64,
    /// Zero-copy view of the current frame's key batch (`None` for an empty
    /// terminal frame or after a decode error). Declared before `slot` so it
    /// drops first — it borrows `slot`'s ring bytes with its lifetime erased.
    mb: Option<crate::storage::MemBatch<'static>>,
    /// Ring slot backing `mb`. Holding it parks the frame's ring bytes.
    slot: Option<W2mSlot>,
    /// Cursor into `mb`.
    row: usize,
    /// Current frame is non-terminal: status 0 and no FLAG_SCAN_LAST.
    has_more: bool,
}
```

`new` uses named-field init, so only the declaration order changes.

## 6. Ownership-doc repair

Six module docs record the ownership rules for the merge / exec hot paths but
cite a section ("§8 cluster N") of a document that no longer exists, so the
contract text is unreachable. Replace each dangling reference with the
self-contained rule. Exact edits:

1. `crates/gnitz-engine/src/query/vm/exec.rs:1-2`

   ```rust
   //! Epoch execution: `execute_epoch` / `execute_epoch_multi` and the opcode
   //! dispatch loop — kept whole: boxing per-opcode handlers or splitting the
   //! match arms would break monomorphization of the dispatch loop.
   ```

2. `crates/gnitz-engine/src/storage/lsm/compact/merge.rs:7-9` (the
   parenthetical spans three lines) — delete the dead pointer, keep the rule:

   ```rust
   //! [`run_merge`](super::super::merge::run_merge) (the sole pending-group
   //! drain owner; re-extracting a local drain loop would fork the
   //! (PK, payload) total order); this module only drives it and materializes
   ```

3. `crates/gnitz-engine/src/ops/join/rowwrite.rs:1-3`

   ```rust
   //! Shared inner-join row writer. Kept `#[inline]` so the per-row column-copy
   //! loops fold into their callers across the join split — the "no per-row
   //! cross-file call boundary" guarantee.
   ```

4. `crates/gnitz-engine/src/ops/distinct.rs:543`

   ```rust
   // Wide-PK distinct tests
   ```

5. `crates/gnitz-engine/src/catalog/write_path.rs:6-7` — the invariant is
   already stated in full by the surrounding sentence; drop only the dead
   parenthetical:

   ```rust
   //! `sys_tables.rs` / `apply_context.rs`. No second ingest entry point may
   //! skip this precheck/hooks path.
   ```

6. `crates/gnitz-engine/src/runtime/orchestration/worker/exchange.rs:1-3` —
   drop the dead sentence; the module doc already names the machinery:

   ```rust
   //! Worker exchange-wait re-entry: the defer-then-replay machinery
   //! (`do_exchange_wait` inline dispatch loop + `dispatch_deferred` /
   //! `replay_deferred_ticks`).
   ```

## Verification

- `make verify` — fmt, clippy-as-errors, full unit tests. Pins for each section:
  - §1 `pack_pk_be_specialization_matches_naive` and the `opk_proptest` module
    (`schema/key.rs`).
  - §2 `lower_bound_opk_matches_byte_search` (`storage/repr/columnar.rs`).
  - §3/§4 the moved `partition_router_basic` /
    `index_route_key_hits_signed_routing_cache` and the untouched
    `extract_group_key_cursor_matches_batch` — no hash image changes on any
    surviving path.
  - §5 is a compile-time reorder over a `Drop`-free `MemBatch`; §6 is
    comment-only.
- `make e2e` (multi-worker, `GNITZ_WORKERS=4`) — §3 touches the exchange scatter
  gate (join / group-by / set-op suites exercise both scatter paths); the
  unique-index suites, including a string/BLOB unique index, confirm §4.1 leaves
  seek results unchanged (string seeks still broadcast and find their rows).
