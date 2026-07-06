# Key-kernel unification: one packer, one fold, one row-hash

## Problem

The engine derives "this row's key bytes" through overlapping kernels whose
byte-for-byte agreement is enforced only by comments and tests:

1. `ReindexPacker` / `ColPromoter` (`ops/reindex.rs:311-401`, `:251-303`) —
   OPK-packs a column list into the PK region. Used by the trace-side reindex
   `Map` (`ops/linear.rs::op_map`) and by the exchange scatter
   (`router.rs::compound_join_packer`), which must co-partition byte-for-byte.
2. `route_partition_key` (`router.rs:88-109`) — a hand-rolled single-column
   special case of the same packing (routable-int via `route_key`, string via
   `german_string_promote_key`), reachable only for a non-promoted single-column
   `JoinPromote` scatter. It re-states `ColPromoter`'s per-type rules in a
   second place; the doc comment ("This must match `PkPromoter::promote_into`")
   is the only thing keeping them aligned.
3. `extract_group_key` (`ops/util.rs:380-541`) — the null-distinct 128-bit
   group fold, used by `RouteMode::GroupKey` scatters and by `op_reduce`'s
   synthetic output PK.
4. `reindex_hash_row` (`ops/reindex.rs:28-69`) — the branch-salted full-payload
   content hash for set-op/DISTINCT identity, a third `op_map` mode.
5. `PkPromoter` (`ops/reindex.rs:97-245`) — in production only a per-column
   classifier (`new().kind` consumed at `reindex.rs:346`) plus two read
   helpers; its `promote`/`promote_into` are already `#[cfg(test)]` oracles.

The seams: `op_map` forks three ways on opcode fields
(`linear.rs:72-148`); the router forks per-row across four functions
(`route_partition_key`, `hash_row_for_partition`, `compound_join_packer`,
`route_nonpk_row`, `router.rs:88-210`) where picking the wrong key silently
misroutes rows to a worker that does not own them.

## Committed design

Three kernels remain, each with exactly one implementation and one home:

- **Pack** — `ReindexPacker` (OPK column packing; strings → `german_string_promote_key`).
- **Fold** — `extract_group_key` (null-distinct group identity).
- **Row-hash** — `reindex_hash_row` (branch-salted full-payload identity).

These are deliberately NOT merged into one hash: pack is null-blind and
value-preserving (LEFT-join NULL-key bypass rows must route to the `_join_pk 0`
partition, `router.rs:73-87`), fold is null-distinct (a NULL group and a 0
group must not share an output PK), and row-hash covers all payload columns
with a branch salt. Conflating any two re-keys existing traces and breaks a
documented invariant.

What is deleted is every *duplicated dispatch over* these kernels:

### 1. `PkPromoter` struct dissolves into free functions (`ops/reindex.rs`)

`PkPromoter::new` becomes `fn classify_promote(schema: &SchemaDescriptor,
col_idx: usize) -> PromoteKind` (body unchanged — the `match` on
`schema.locate(col_idx)` at `reindex.rs:123-155`). `read_wide`/`read_string`
become free `fn`s. The `#[cfg(test)]` arity-1 oracle (`promote`,
`promote_into`, `reindex.rs:173-244`) moves into the tests module as a
test-local `PkPromoter { kind }` wrapper over `classify_promote`, so its call
sites stay unchanged. It stays golden for
`test_reindex_packer_arity1_byte_identity` (a single-column encoder
cross-check of `promote_into`) and the `test_pk_promoter_*` tests, whose
per-row `promote()` derives expected values from the *routing* helpers
(`pk_route_key`/`payload_route_key`), independent of the packer's encoders.
(`test_reindex_packer_copartition_contract*` compare packer output against the
independent ingest-side `opk_pk` oracle and don't use `PkPromoter` at all.)
`ColPromoter` calls `classify_promote` at `ReindexPacker::new`
(`reindex.rs:346`). No byte changes; pure structure.

### 2. Router: one per-scatter key object (`ops/exchange/router.rs`)

`RouteMode` (`router.rs:117-121`) stays as the structural selector — it is
chosen from circuit metadata, not per-query data (`master/dispatch.rs:626-678`:
`get_join_shard_cols` non-empty → `JoinPromote`, else `GroupKey`), and that
hard-to-misuse selection is worth keeping. Everything downstream of the enum
collapses into one type constructed once per scatter:

```rust
/// Per-scatter routing key, built once and applied per row. A JoinPromote
/// scatter packs the SAME OPK bytes the downstream reindex Map stamps as the
/// `_join_pk` (so delta scatter and reindexed trace co-partition
/// byte-for-byte); a GroupKey scatter routes by the null-distinct group fold,
/// which op_reduce also uses for the group's output PK.
pub(super) enum ScatterKey {
    Packed(ReindexPacker),
    Fold,
}

impl ScatterKey {
    pub(super) fn new(mode: RouteMode, cols: &[u32], tcs: &[u8], schema: &SchemaDescriptor) -> Self {
        match mode {
            RouteMode::JoinPromote => ScatterKey::Packed(ReindexPacker::new(schema, cols, tcs)),
            RouteMode::GroupKey => ScatterKey::Fold,
        }
    }

    /// Route one non-PK-keyed row. Caller hoists `pack_buf` (a `MAX_PK_BYTES`
    /// scratch; `pack_into` fully overwrites the `out_stride` prefix it reads).
    #[inline]
    pub(super) fn partition(&self, mb: &MemBatch, row: usize, cols: &[u32],
                            schema: &SchemaDescriptor, pack_buf: &mut [u8]) -> usize {
        match self {
            ScatterKey::Packed(p) => {
                p.pack_into(&mut pack_buf[..p.out_stride], mb, row);
                partition_for_pk_bytes(&pack_buf[..p.out_stride])
            }
            ScatterKey::Fold => partition_for_key(extract_group_key(mb, row, schema, cols)),
        }
    }
}
```

Deleted outright: `route_partition_key` (`router.rs:88-109`),
`hash_row_for_partition` (`:123-139`), `compound_join_packer` (`:176-184`),
`route_nonpk_row` (`:195-210`). `key_is_promoted` and `scatter_is_pk_routed`
(the PK-bytes fast-path gate, `:147-165`) are untouched — rows whose scatter
cols equal `pk_indices()` (non-promoted) still route via
`partition_for_pk_bytes` on the existing PK region with no key object.
`fill_worker_indices`' non-PK arm (`:243-247`) calls
`partition_for_key(extract_group_key(..))` directly (it is always a
group/seek-style scatter; no mode parameter reaches it).

The scatter entry points (`op_repartition_batches_mode`,
`op_relay_scatter_consolidated_mode`, `relay.rs`) keep their signatures
(`mode: RouteMode`); internally each builds one `ScatterKey` where it today
builds `compound_join_packer` and threads `mode` per row. The `#[cfg(test)]`
helper `op_multi_scatter` (`relay.rs:502`) is `hash_row_for_partition`'s third
caller (`relay.rs:534`, always `GroupKey`); its non-PK arm inlines to
`partition_for_key(extract_group_key(&mb, i, schema, spec))`. Dropping
`route_partition_key` also removes `router.rs`'s only use of
`german_string_promote_key` — delete it from the `router.rs:9` import
(clippy-as-errors).

**Byte-destination proof for the collapsed single-column JoinPromote path**
(the only path whose code changes; compound/promoted keys already take the
packer):

- Non-null int, and NULL int (null-blind by design: both old and new read the
  same canonically-zeroed column slot): old key = `loc.route_key(mb, row)` =
  OPK-widened native value; new bytes = the packer's `Pk`/`Narrow` arm =
  `OPK(value)` at width `cs`. For any `len <= 16`,
  `partition_for_pk_bytes(bytes) == partition_for_key(widen_pk_be(bytes))`
  (`schema/key.rs:125,138`), and `widen_pk_be(OPK(value)) == route_key(value)`
  — the exact identity `PromoteKind`'s docs assert (`reindex.rs:103-119`).
  Same partition. (A single PK *sub-column* key takes `route_partition_key`'s
  fall-through to `extract_group_key`'s canonical `Pk` arm today —
  `pk_route_key` — the same value again.)
- String: old key = `german_string_promote_key(..)`; new bytes = the packer's
  `String` arm = `to_be_bytes()` of the same hash; `widen_pk_be(BE(h)) == h`.
  Same partition.

So no row moves workers; the LEFT-join NULL-key bypass invariant
(`router.rs:73-87`) is preserved because `Packed` remains null-blind.

### 3. `op_map`: one stamp object (`ops/linear.rs`, `ops/reindex.rs`)

The three-way fork in `op_map` (`linear.rs:91-147`) becomes construction of
one stamp before the (unchanged) `evaluate_map_batch` call:

```rust
/// What op_map writes into the output PK region after the payload map:
/// nothing (plain projection — the bulk PK copy in expr/plan.rs stands),
/// a packed reindex key, or the branch-salted full-row content hash.
pub(crate) enum MapStamp {
    None,
    Cols(ReindexPacker),
    HashRow { branch_id: u8 },
}

impl MapStamp {
    pub(crate) fn new(in_schema: &SchemaDescriptor, reindex_cols: &[u32],
                      target_tcs: &[u8], reindex_hash: bool, branch_id: u8) -> Self {
        debug_assert!(!reindex_hash || reindex_cols.is_empty());
        if reindex_hash {
            MapStamp::HashRow { branch_id }
        } else if reindex_cols.is_empty() {
            MapStamp::None
        } else {
            MapStamp::Cols(ReindexPacker::new(in_schema, reindex_cols, target_tcs))
        }
    }

    pub(crate) fn apply(&self, in_mb: &MemBatch, out_schema: &SchemaDescriptor, output: &mut Batch) {
        match self {
            MapStamp::None => {}
            MapStamp::Cols(p) => p.promote_into(in_mb, output),
            MapStamp::HashRow { branch_id } => reindex_hash_row(out_schema, output, *branch_id),
        }
    }
}
```

`op_map`'s body becomes: evaluate, `debug_assert_eq!` on row count (once),
`stamp.apply(..)`, return — layout stays `Raw` in all arms (a stamp rewrite
scrambles PK order; `MapStamp::None` keeps today's `Raw` for the
payload-reordering-projection reason documented at `linear.rs:114-117`).

`MapStamp` and `ScatterKey` are deliberately two types, not one enum: no
circuit shape routes by `HashRow` (set-op exchanges happen *after* the hash
map stamped the PK, so they are PK-bytes scatters over `shard(x, &[0])`), and
no map stamps a `Fold` today. A shared enum would carry documented-unreachable
arms in both consumers. The unification is at the kernel level.

## Not in scope

- `op_reduce`'s output-PK derivation (`op_reduce.rs:483-495`) and
  `build_reduce_output_schema` are untouched; they already consume
  `extract_group_key`/verbatim PK bytes and change under a separate effort.
- The AVI composite key (`ops/index.rs`, `group_cols ‖ ordinal ‖ av`) is an
  order-preserving raw-byte *seek prefix*, structurally not a partition/identity
  key — folding it into any hash kernel would destroy ordered seeks. Untouched.

## Performance

The only hot-path codegen change is the single-column non-promoted JoinPromote
scatter: previously "load column → `route_key` (sign-flip + widen in
registers) → mix", now "arity-1 `pack_into` (one ≤8-byte OPK store to a stack
buffer) → `widen_pk_be` (one load) → mix". The design is committed on
kernel-count grounds regardless; to pin the expected parity, add an `#[ignore]`
release-mode micro-bench alongside the relay tests (per the dev-guide pattern):
`scatter_route_bench` timing `op_relay_scatter_consolidated_mode` over 1M rows
with a single-column I64 join key, run before and after the router commit.

## Sequencing

- [ ] `reindex.rs`: dissolve `PkPromoter` into `classify_promote` +
      free read helpers; move the arity-1 oracle into the tests module;
      adjust test imports. `make verify` green, zero behavior change.
- [ ] `router.rs` + `relay.rs`: add `scatter_route_bench` (`#[ignore]`),
      record baseline in the commit message; introduce `ScatterKey`; delete
      `route_partition_key` / `hash_row_for_partition` /
      `compound_join_packer` / `route_nonpk_row`; re-run bench.
- [ ] `linear.rs`/`reindex.rs`: introduce `MapStamp`; collapse `op_map`'s
      three-way fork.

## Testing

- Existing byte-identity and copartition tests must pass **unchanged in
  substance**: `test_reindex_packer_copartition_contract*`,
  `arity1_byte_identity`, `promoted_columns_match_encoder`,
  `test_pk_promoter_*` (`reindex.rs` tests), and the relay scatter tests
  (`relay.rs` tests module) — imports/paths of deleted helpers move, and
  `op_multi_scatter`'s body inlines its routing call (above); no assertion
  changes.
- New router test: for each of {non-null I64, NULL I64 (nullable col), string,
  NULL string, promoted I32→I64, compound (u64,i32)} single-scatter inputs,
  assert the `ScatterKey::Packed` partition equals the pre-collapse
  `route_partition_key`/`route_nonpk_row` result (compute the old value inline
  in the test from `route_key`/`german_string_promote_key` — the helpers are
  deleted, the arithmetic is three lines).
- `make e2e` at `GNITZ_WORKERS=4` (join, set-op, group-by, null-join-key
  suites are the tripwires for misrouting).
