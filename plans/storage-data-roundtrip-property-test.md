# Storage-layer data round-trip property test

The schema codec has its own round-trip property test (see
`plans/schema-roundtrip-property-test.md`). The **data** layer — the
end-to-end path from `Batch` → `MemTable` → shard file →
`MappedShard` / `ReadCursor` — has no analogous coverage. Every existing
storage unit test fixes a single concrete schema (almost always
`(U64 PK)` or `(U64 PK, U64)`) and exercises one code path at a time.
The result is that PK-width and PK-arity assumptions hard-coded across
unrelated files stay quiet until a single change routes traffic through
them.

The compound-PK secondary-index work turned up storage-layer bugs of
this exact shape, all rooted in PK-width assumptions that hold for the
`(U64 PK)` fixtures every existing test uses:

- `shard_reader::open` hard-codes `num_non_pk = num_cols - 1`. Zero-payload
  schemas (`pk_count == num_cols`) compute `num_regions` one short and
  return `InvalidShard` on every read.
- `shard_file::detect_encoding` writes `value[..element_width]` into a
  `[u8; 16]` Constant buffer; `element_width = 24` overruns and panics.
- `Batch::get_pk` / `MappedShard::get_pk` route every PK through
  `widen_pk_le`, which only accepts `pk_stride ∈ {1, 2, 4, 8, 16}` and
  panics on every other width. A compound index PK whose total stride is
  not a power of two — e.g. a secondary index `(U64 indexed-key, U32
  source-PK)` = 12 bytes, or `(U16, U16, U16)` = 6 bytes — panics the
  moment its key is widened: in `MemTable::upsert_sorted_batch`'s bloom
  loop on first ingest, and in `ReadCursor::drive_single` on first scan.

The wide-PK case (`pk_stride > 16`) is partly papered over today: the
bloom loop is guarded by `if !self.schema.pk_is_wide()` and the storage
sort/merge uses `merge::pack_pk_le` (which prefix-truncates instead of
panicking). But `ReadCursor` still widens every PK to a `u128` key
(`drive_single`, `build_tree`, `MappedShard::get_pk` all call
`widen_pk_le`), so any `full_scan()` over a wide-PK table panics during
cursor construction — the `u128` key model cannot represent a >16-byte
PK at all.

Two fixes fall out, both landed alongside the proptest below:

1. **Generalise `widen_pk_le` to zero-extend any `stride ≤ 16`.** A
   compound PK ≤ 16 bytes packs losslessly into a `u128`, and
   `make_slow_pk_cmp` already recovers the exact bytes via
   `a.to_le_bytes()[..stride]` before handing them to
   `compare_pk_bytes`. This is the same prefix-pack `pack_pk_le` performs
   on the comparator path; aligning the two removes the panic for every
   narrow compound PK while leaving the `> 16` arm a panic (a >16-byte
   key genuinely cannot fit a `u128`).
2. **Read wide-PK (`> 16`) tables through the byte-addressed shard APIs,
   never `ReadCursor`.** `MappedShard::to_owned_batch` /
   `slice_to_owned_batch` and `get_pk_bytes` operate on raw bytes and are
   correct for any width.

These are storage-internal, touch neither operators nor planner, and are
each independently reproducible once the test fixture iterates over
`(type_code, num_pk_cols)` instead of fixing them. Add one property test
that does.

## Storage fix: narrow compound PK widening

`crates/gnitz-engine/src/storage/batch.rs`, `widen_pk_le`. Add a fallback
arm for any `stride ≤ 16`; keep the `> 16` arm a panic.

```rust
/// LE-widen a single- or compound-PK region slice to its u128 key.
/// Widths 1/2/4/8/16 hit a specialised arm; any other `stride ≤ 16`
/// (a non-power-of-two compound region) zero-extends, which is lossless
/// and round-trips through `make_slow_pk_cmp`'s `to_le_bytes()[..stride]`.
/// `stride > 16` cannot fit a u128 and is a routing bug — callers must use
/// `get_pk_bytes`/`pk_bytes` — so it panics to surface the misroute.
///
/// `stride` is taken explicitly rather than from `src.len()` because the
/// fixed-array callers (`PkBuf::as_u128_single_pk`) pass a constant-length
/// backing array whose length carries no width information.
/// Contract: `src.len() >= stride`.
#[inline(always)]
pub(super) fn widen_pk_le(src: &[u8], stride: usize) -> u128 {
    match stride {
        1  => src[0] as u128,
        2  => u16::from_le_bytes(src[..2].try_into().unwrap()) as u128,
        4  => u32::from_le_bytes(src[..4].try_into().unwrap()) as u128,
        8  => u64::from_le_bytes(src[..8].try_into().unwrap()) as u128,
        16 => u128::from_le_bytes(src[..16].try_into().unwrap()),
        n if n <= 16 => {
            let mut bytes = [0u8; 16];
            bytes[..n].copy_from_slice(&src[..n]);
            u128::from_le_bytes(bytes)
        }
        n => panic!("widen_pk_le: wide PK region (stride {n}); caller must use get_pk_bytes/pk_bytes instead"),
    }
}
```

A single-column PK always has a stride in `{1,2,4,8,16}` (it is one
column's `size()`, UUID at most), so the new arm fires only for compound
PKs — the only legitimate source of strides like 6 or 12 — and the
existing single-PK misroute guard (`> 16` panics) is unchanged. With this
in place the `MemTable` bloom guard (`if !self.schema.pk_is_wide()`) stays
correct: it skips only the genuinely-unpackable `> 16` case, and every
narrow compound PK now feeds the bloom (and `lookup_pk`, `find_lower_bound`,
the cursor) without panicking.

## Scope

In:

- A single proptest at the storage-layer boundary that, for an
  arbitrary valid schema:
  1. Generates `n` random rows for that schema (PKs distinct, payload
     values arbitrary in-type, null bitmaps randomised).
  2. Ingests them into a `Table` (durable=false to keep the test in
     `~/git/gnitz/tmp/`).
  3. Forces a flush so the data exits the memtable into a shard file.
  4. Re-opens the table directory (`Table::new`) and scans.
  5. Asserts the scanned set equals the ingested set (as Z-Sets,
     i.e. multiset equality on (PK, payload) tuples).
- A second proptest covering point lookups: for each generated row,
  `Table::has_pk(pk)` returns `true` for a single-PK schema; for
  compound schemas, a prefix-scan via cursor returns the row. The
  bloom-filter and PK-hash paths are width-sensitive and load-bearing
  for `validate_unique_indices` / `validate_fk_parent_restrict`.
- A third proptest covering retraction: ingest `n` rows, retract a
  random half, scan, assert the surviving set equals the un-retracted
  half. Exercises `retract_pk`'s memtable / shard reconciliation,
  which has its own width assumptions.
- Schema generation respects the catalog's `pk_stride ∈ {1, 2, 4, 8, 16}`
  rule (enforced in `catalog/sys_tables.rs`) for user-table-shaped
  schemas. **Crucially**, separately generate *index-schema-shaped*
  schemas with arbitrary total strides, including non-power-of-two narrow
  strides (6, 12 — the case the `widen_pk_le` fix addresses) and wide
  strides `> 16` (UUID + companion = 24). These bypass the catalog rule
  and exist legitimately for index circuits and other internal tables.
- The index-schema round-trip splits on width: strides `≤ 16` read back
  through `Table::full_scan()` (the cursor path, now correct after the
  `widen_pk_le` fix); strides `> 16` cannot use the cursor at all and
  read back through the single flushed shard's `to_owned_batch`. Both
  assert the same Z-Set equality.

Out:

- Proptests on the SQL planner, the join / reduce operators, or the
  multi-worker exchange path. Each is its own surface; the storage
  layer is the right place to start because its bugs propagate
  silently into every layer above.
- Fuzzing the binary shard format against random bytes — that's
  malformed-input coverage, a separate scope.
- Replacing the existing concrete-schema tests. They document specific
  behaviours (e.g. negative-i64 encoding, German-string blob layout)
  that random generation won't reliably hit. The proptest adds a
  width-axis sweep on top of, not in place of, the existing fixtures.

## Touchpoints

`crates/gnitz-engine/src/storage/tests/data_roundtrip_proptest.rs` (new):

```rust
use std::collections::HashMap;
use proptest::prelude::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::storage::{Batch, Table};

fn arb_pk_type() -> impl Strategy<Value = u8> {
    prop_oneof![
        Just(type_code::U8),  Just(type_code::I8),
        Just(type_code::U16), Just(type_code::I16),
        Just(type_code::U32), Just(type_code::I32),
        Just(type_code::U64), Just(type_code::I64),
        Just(type_code::U128), Just(type_code::UUID),
    ]
}

fn arb_payload_type() -> impl Strategy<Value = u8> {
    prop_oneof![
        arb_pk_type(),
        Just(type_code::F32), Just(type_code::F64),
        Just(type_code::STRING),
    ]
}

/// Build a schema from PK type codes + payload (type, nullable) pairs.
/// PK columns come first (non-nullable); payloads follow.
fn build_schema(pk_types: &[u8], payloads: &[(u8, u8)]) -> SchemaDescriptor {
    let mut cols = Vec::with_capacity(pk_types.len() + payloads.len());
    let mut pk_indices = Vec::with_capacity(pk_types.len());
    for (i, &tc) in pk_types.iter().enumerate() {
        cols.push(SchemaColumn::new(tc, 0));
        pk_indices.push(i as u32);
    }
    for &(tc, nullable) in payloads {
        cols.push(SchemaColumn::new(tc, nullable));
    }
    SchemaDescriptor::new(&cols, &pk_indices)
}

fn pk_stride_of(types: &[u8]) -> usize {
    types.iter().map(|&t| SchemaColumn::new(t, 0).size() as usize).sum()
}

/// User-table-shaped: total PK stride ∈ {1, 2, 4, 8, 16} (catalog rule).
fn arb_user_schema() -> impl Strategy<Value = SchemaDescriptor> {
    let pk = prop::collection::vec(arb_pk_type(), 1..=4)
        .prop_filter("pk_stride must be 1/2/4/8/16",
            |t| matches!(pk_stride_of(t), 1 | 2 | 4 | 8 | 16));
    let payload = prop::collection::vec((arb_payload_type(), 0u8..=1), 0..=4);
    (pk, payload).prop_map(|(pk, pl)| build_schema(&pk, &pl))
}

/// Index-schema-shaped: arbitrary total stride (incl. non-power-of-two
/// narrow and `> 16` wide), zero payload columns — matches the layout
/// `make_index_schema` produces. PK arity is capped at `MAX_PK_COLUMNS`.
fn arb_index_schema() -> impl Strategy<Value = SchemaDescriptor> {
    prop::collection::vec(arb_pk_type(), 1..=crate::schema::MAX_PK_COLUMNS)
        .prop_map(|pk| build_schema(&pk, &[]))
}

proptest! {
    /// Ingest → flush → scan must round-trip the multiset of rows.
    #[test]
    fn batch_roundtrip_user_schema(schema in arb_user_schema(), rows in 1usize..=64) {
        let dir = TestDir::new();
        let mut table = Table::new(dir.path(), "t", schema, 1, 1 << 20, false).unwrap();
        let original = arb_batch(&schema, rows);
        table.ingest_owned_batch(original.clone_batch()).unwrap();
        table.flush().unwrap();
        let scanned = table.full_scan();
        prop_assert_eq!(
            zset_of(&original, &schema),
            zset_of(scanned.as_ref(), &schema),
            "scan does not match ingest under schema {:?}",
            schema_summary(&schema),
        );
    }

    /// Same property for wide / zero-payload index schemas. Catches the
    /// `shard_reader num_cols-1` undercount and the `detect_encoding`
    /// overrun, and — after the `widen_pk_le` fix — exercises every
    /// non-power-of-two narrow compound stride through the bloom/cursor.
    /// Strides `> 16` cannot use the cursor; read them back through the
    /// flushed shard directly.
    #[test]
    fn batch_roundtrip_index_schema(schema in arb_index_schema(), rows in 1usize..=64) {
        let dir = TestDir::new();
        let mut table = Table::new(dir.path(), "t", schema, 1, 1 << 20, false).unwrap();
        let original = arb_batch(&schema, rows);
        table.ingest_owned_batch(original.clone_batch()).unwrap();
        table.flush().unwrap();

        let expected = zset_of(&original, &schema);
        if schema.pk_stride() as usize > 16 {
            // u128-keyed ReadCursor cannot represent a >16-byte PK; read
            // the single flushed shard through the byte-addressed path.
            let shards = table.all_shard_arcs();
            prop_assert_eq!(shards.len(), 1);
            let scanned = shards[0].to_owned_batch(&schema);
            prop_assert_eq!(expected, zset_of(&scanned, &schema));
        } else {
            let scanned = table.full_scan();
            prop_assert_eq!(expected, zset_of(scanned.as_ref(), &schema));
        }
    }

    /// has_pk and prefix-scan return `true` for every ingested row.
    #[test]
    fn point_lookup_after_flush(schema in arb_user_schema(), rows in 1usize..=64) {
        // Ingest + flush, then iterate rows and assert the table can
        // re-find each one. Narrow-PK path uses has_pk(u128); compound-PK
        // path uses seek_first_positive_with_prefix on the cursor.
    }

    /// Retraction reconciles memtable and shard.
    #[test]
    fn retract_then_scan(schema in arb_user_schema(), rows in 2usize..=64) {
        // Ingest n rows, retract a deterministic half, flush, scan,
        // assert the surviving set matches expectation.
    }
}
```

### Helpers

`arb_batch(schema, n)` walks `schema.columns`, generates random in-range
values per type, and builds the batch via `Batch::extend_pk_bytes`
(correct for any width) + per-column fills, keeping the `n` PKs distinct.
The helper IS the test — its correctness over arbitrary width is what
makes the proptest interesting.

`zset_of` reduces a batch to a multiset of `(pk_bytes, null_word,
logical_payload_values) → weight`. STRING/BLOB columns must be **decoded**
before comparison: the on-disk 16-byte German-string struct embeds a blob
offset for long (`> 12` byte) strings, so two batches holding the same
logical string carry different struct bytes. Comparing raw struct bytes
would yield false inequalities; decode to the logical value instead.

```rust
type RowKey = (Vec<u8>, u64, Vec<Vec<u8>>);

fn zset_of(batch: &Batch, schema: &SchemaDescriptor) -> HashMap<RowKey, i64> {
    let mut z: HashMap<RowKey, i64> = HashMap::new();
    for row in 0..batch.count {
        let pk = batch.get_pk_bytes(row).to_vec();   // byte-addressed: any width
        let nw = batch.get_null_word(row);
        let mut vals = Vec::new();
        for (pi, _ci, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            if (nw >> pi) & 1 != 0 {
                vals.push(Vec::new());               // null
                continue;
            }
            let raw = batch.get_col_ptr(row, pi, cs);
            let logical = if col.type_code == type_code::STRING
                || col.type_code == type_code::BLOB
            {
                let st: [u8; 16] = raw.try_into().unwrap();
                crate::schema::decode_german_string(&st, &batch.blob)
            } else {
                raw.to_vec()
            };
            vals.push(logical);
        }
        *z.entry((pk, nw, vals)).or_insert(0) += batch.get_weight(row);
    }
    z.retain(|_, w| *w != 0);
    z
}
```

`TestDir` is a RAII guard that creates a unique directory **under
`~/git/gnitz/tmp/`** (not the system `/tmp`, per project convention) and
unlinks it on drop. With 256 cases × ~5 tests each generating a fresh
non-durable table, leaking directories would exhaust inodes during a
`PROPTEST_CASES=4096` sweep, so cleanup must be automatic rather than
relying on `Table`'s in-directory stale-shard erase. Implement it with
`tempfile::Builder::new().tempdir_in(gnitz_tmp_root())` so the existing
`tempfile` dev-dependency does the unlinking.

`Cargo.toml` (`gnitz-engine`, `[dev-dependencies]`): add `proptest` if
not already present. (Check via `cargo tree -p gnitz-engine`; the
schema codec proptest plan may have already pulled it in.)

## Implementation sketch — what the failures look like

Pre-fix, running on a randomly-generated 2-column compound-PK schema
with `pk_indices = [0, 1]`:

```text
test batch_roundtrip_user_schema failed:
  schema: pk_stride=16 pk_count=2 payload_cols=0
  shrunk to minimal:
    schema { columns: [U64, U64], pk_indices: [0, 1] }
    rows:   1
  message: scan returned Err(InvalidShard)
```

That single line names `shard_reader::num_cols - 1`. The
`detect_encoding` overrun surfaces the same way (any stride > 16 panics
inside the constant-encoding detector), as does the narrow-compound
`widen_pk_le` panic (a stride-12 index schema crashes inside
`MemTable::upsert_sorted_batch`'s bloom loop on first ingest). Each
reduces to a one-line proptest failure with the shrunk schema named.

Today, those bugs surface only when the secondary-index path on a
compound-PK source happens to construct an index table with the
exact wrong shape, two file boundaries and several call layers away.

## Migration order

1. Add `proptest` to `gnitz-engine` dev-dependencies (no-op if already
   present).
2. Land the `widen_pk_le` fix above. On its own it stops every
   narrow-compound index table from panicking on ingest/scan; the
   proptest below is what proves it and guards against regression.
3. Land the `arb_user_schema` generator and the first round-trip
   property test. This commit alone catches the `shard_reader num_cols-1`
   regression for any zero-payload user schema.
4. Add `arb_index_schema` and the index-shape proptest. Exercises every
   compound stride (including the non-power-of-two narrow strides the
   `widen_pk_le` fix unblocks) and the `> 16` shard read-back path, and
   catches the `detect_encoding` overrun.
5. Add the point-lookup and retraction proptests. These mostly buy
   coverage for paths that *aren't* covered by ingest→scan alone:
   `has_pk`, `retract_pk`, memtable/shard reconciliation.

Each step is independently mergeable. Step 2 fixes a live panic; step 4
is the highest-leverage test (the `shard_reader`, `detect_encoding`, and
compound-stride paths in one sweep).

## Out of scope

- **Concurrency / multi-worker proptests.** The storage layer is
  single-threaded; the exchange and partition routing are tested
  elsewhere. The proptest stays in-process.
- **Generated SQL DDL.** The schemas come from direct
  `SchemaDescriptor::new` calls; we deliberately bypass planner
  admission to exercise legitimate-but-not-yet-admitted shapes (wide
  PK, compound PK with non-canonical column order).
- **Variable-length payload value generation.** STRING and BLOB
  generation can stay minimal (empty + a few short literals); the
  bugs this plan targets are PK-shape bugs, not blob-encoding bugs.
  A separate proptest can extend payload-value coverage if a
  blob-encoding regression appears.

## Testing

- `cargo test -p gnitz-engine data_roundtrip` (or whatever the module
  name resolves to) runs the proptests. Default 256 cases per test;
  with ~5 tests this costs a few seconds on a debug build.
- The proptest seed must be CI-visible on failure: proptest's default
  output already prints the shrunk input on the failing assertion,
  including the schema. Capture that in the PR description when
  reporting a new failure.
- Once green: treat the proptest as the gate for any new storage-layer
  change that touches PK arithmetic, region counting, or encoding
  selection. A future PR that adds a new width-dependent code path
  (e.g. a vectorised partition router for wide PKs) must run this
  proptest with `PROPTEST_CASES=4096` once as part of the review.
- Future-proofing: when wide-PK schemas become user-visible (the
  `wide-pk-trace-cursor.md` work + downstream planner gate lift), the
  index-shape proptest is the regression guard that prevents that
  lift from re-exposing the bugs this plan just closed.
