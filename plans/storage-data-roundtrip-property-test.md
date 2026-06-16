# Storage-layer data round-trip property test

The schema codec already has a round-trip property test (`arb_schema` +
`schema_roundtrip_engine_codec` / `schema_roundtrip_client_codec` in
`crates/gnitz-engine/src/runtime/wire.rs`). The **data** layer — the
end-to-end path from `Batch` → `MemTable` → shard file →
`MappedShard` / `ReadCursor` — has no analogous coverage. Every existing
storage unit test fixes a single concrete schema (almost always
`(U64 PK)` or `(U64 PK, I64)`) and exercises one code path at a time.
PK-width and PK-arity assumptions hard-coded across unrelated files stay
quiet until a single change routes traffic through them; the storage
layer is the right place to catch them because its bugs propagate
silently into every layer above.

PK width is the load-bearing axis. A PK region is stored as
order-preserving bytes (OPK, §6). On the **bloom** build/probe, strides `≤ 16`
widen to a `u128` fast key via `gnitz_wire::widen_pk_be` (in `gnitz-wire`,
`pk.rs`), which zero-extends **any** width — including the non-power-of-two
compound strides (6, 10, 12) a multi-column or secondary-index key produces —
while strides `> 16` cannot fit a `u128` and collapse to a `u128` via xxh3
(`xxh::checksum`). Both arms feed the same XOR8 filter, and build
(`shard_file.rs`) and probe (`shard_index.rs`) branch identically. The **ordered**
paths (merge, sort, seek, retract) never use the `u128` key: they compare raw
OPK bytes through `compare_pk_bytes` (a plain unsigned `memcmp`, width-agnostic),
and `full_scan` / `has_pk_bytes` / `retract_pk_bytes` are all byte-path and
correct at every width. The only `pk_stride ≤ 16` hard assumptions
(`widen_pk_be`'s `debug_assert`, the `u128`-keyed `opk_key`, the narrow
`extend_pk` / `has_pk(u128)` / `retract_pk(u128)` convenience wrappers) sit on
encode/reindex helpers the wide byte path bypasses — so a wide PK never trips
the read round-trip, but no test sweeps width across all the arms at once.

The catalog admits any PK whose total stride is `1..=MAX_PK_BYTES`
(`MAX_PK_BYTES == MAX_PK_COLUMNS * 16 == 80`), enforced in
`catalog/sys_tables.rs` (`validate_pk_cols`) — there is **no** power-of-two
stride restriction. So a user table can already carry a 12-byte `(U64, U32)`
PK or a 24-byte `(UUID, U64)` PK (24 > 16, the wide arm). Index circuits and
other internal tables construct `SchemaDescriptor`s directly and can pack
zero-payload, arbitrary-stride keys. A single proptest that iterates over
`(type codes, pk arity, payload shape)` instead of fixing them exercises
every width arm in one sweep.

## Scope

In:

- A round-trip proptest at the storage-layer boundary that, for an
  arbitrary valid schema and either persistence mode:
  1. Generates `n` random rows for that schema (PKs distinct, payload
     values arbitrary in-type, null bitmaps randomised on nullable columns).
  2. Ingests them into a `Table` and forces a flush.
  3. Scans the table back and asserts the scanned set equals the ingested
     set as Z-Sets — multiset equality on `(PK bytes, null word,
     decoded-payload)` tuples.
- The round-trip checks **both read paths at every width**, rather than
  splitting paths by width (`full_scan` is width-agnostic — it merges via
  `compare_pk_bytes` over raw OPK bytes — so it reads wide PKs correctly too):
  - `Table::full_scan()` (the `ReadCursor` / `UnifiedCursor` byte-merge path)
    is asserted in **both** persistence arms. With `Persistence::Durable` it
    reads from the on-disk shard; with `Persistence::Ephemeral` it reads the
    `in_memory_l0` runs. This sweeps the cursor's N-way merge over both
    backing stores across the full width axis.
  - With `Persistence::Durable`, the single flushed shard is **also** read
    back directly through `MappedShard::to_owned_batch`, exercising the
    on-disk region layout, the wide-PK `RegionView::Raw` guard
    (`shard_reader.rs`), and the shard decode independent of the cursor.
  - Schema generation covers narrow (`≤ 16`), non-power-of-two narrow
    (6, 10, 12), and wide (`> 16`) strides, plus zero-payload (index-shaped)
    and payload-bearing (user-shaped) layouts — so one proptest drives the
    narrow-widen, wide-byte-path, and zero-payload region-count arms together.
- A point-lookup proptest (`Persistence::Durable`): `Table::has_pk_bytes`
  re-finds every ingested row **both before and after flush** (so the
  pre-flush memtable bloom and the post-flush on-disk XOR8 bloom — `xxh3`
  collapse for stride `> 16`, `widen_pk_be` for `≤ 16` — are both probed),
  and a synthesized **absent** PK returns `false` both times (exercising
  bloom discrimination and the run-scan resolution of a bloom false
  positive). `has_pk_bytes` is byte-keyed and uniform across all widths;
  the narrow `has_pk(u128)` wrapper (test-only, `opk_key` `debug_assert`s
  `stride ≤ 16`) is not used. The generator deliberately produces
  **prefix-twin** wide PKs (sharing the leading 16 OPK bytes, differing in
  the tail) to stress the memtable bloom, which keys on a 16-byte prefix
  (`pack_pk_be`) for all widths and therefore collides for wide twins — a
  tolerated false positive that must fall through to the scan. These paths
  are load-bearing for `validate_unique_indices` / `validate_fk_parent_restrict`.
- A retraction proptest (`Persistence::Durable`, unit-weight rows): ingest
  `n` rows, flush (→ shard), retract a deterministic half via
  `retract_pk_bytes` (asserting `(weight, found)` and that post-retract
  `has_pk_bytes` is `false`), then `full_scan` — which reconciles
  shard `(+1)` against the memtable retraction `(−1)` to net zero, no second
  flush needed — and assert the surviving set equals the un-retracted half.
  `retract_pk_bytes` is **base-table-only**: it scans memtable + disk shards
  and `debug_assert`s `in_memory_l0.is_empty()`, so this sub-test **must** be
  `Durable` (an `Ephemeral` flush parks rows in `in_memory_l0`, tripping the
  assert and bypassing the reconciliation). It is the width-swept
  generalisation of the existing `table_wide_pk_has_and_retract_bytes`.

Out:

- Proptests on the SQL planner, the join / reduce operators, or the
  multi-worker exchange path. Each is its own surface.
- Fuzzing the binary shard format against random bytes — that's
  malformed-input coverage, a separate scope.
- Replacing the existing concrete-schema tests (e.g. `to_owned_batch_*`
  in `shard_reader.rs`, the mixed-tier and overflow tests in `table.rs`).
  They document specific behaviours — negative-i64 encoding, German-string
  blob layout, cross-tier retraction — that random generation won't
  reliably hit. The proptest adds a width-axis sweep on top of, not in
  place of, the existing fixtures.
- The ephemeral **spill-to-shard** path (`spill_in_memory_to_disk` past
  `EPHEMERAL_INMEM_CEILING == 4 MiB`). Proptest-sized data (`≤ 64` rows)
  never breaches the ceiling, so the `Ephemeral` arm stays in `in_memory_l0`.
  Forcing a spill needs `set_inmem_ceiling_for_test`; it is covered by the
  existing `nondurable_ceiling_spill_to_disk` fixture and out of this sweep.
- Concurrency / multi-worker proptests. The storage `Table` is
  single-threaded; the exchange and partition routing are tested
  elsewhere. The proptest stays in-process.
- Generated SQL DDL. Schemas come from direct `SchemaDescriptor::new`
  calls; the proptest deliberately bypasses planner admission so it can
  exercise legitimate-but-internal shapes (wide PK, zero-payload index
  keys, the 5th PK-column slot, non-canonical PK column order).
- PK-column value **decoding** (`extract_pk_value`). The round-trip keys
  on raw OPK bytes (the canonical key); decoding PK columns back to native
  values is a separate concern.

## Touchpoints

New module `crates/gnitz-engine/src/storage/data_roundtrip_proptest.rs`,
declared `#[cfg(test)] mod data_roundtrip_proptest;` from `storage/mod.rs`.
This is the **first** dedicated test-only module under `storage/` (every
other storage test is an in-file `mod tests`); the sweep spans `Table`,
`Batch`, `MappedShard`, and the shard reader, so a shared module is cleaner
than wedging it into one file's `mod tests`. The module **must** be in-crate:
`MappedShard::to_owned_batch` is `pub(crate)` (and `MappedShard` is not
re-exported from `storage/mod.rs`), and the helper relies on the
`#[cfg(test)]` `Batch::extend_pk_opk`.

```rust
use std::collections::HashMap;
use proptest::prelude::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::storage::{Batch, Persistence, Table};

fn arb_pk_type() -> impl Strategy<Value = u8> {
    // The exact set admitted by `TypeCode::is_pk_eligible`: fixed-width
    // signed/unsigned ints, U128, I128, UUID. STRING / BLOB / float are
    // PK-ineligible (and STRING/BLOB/float-as-PK panics in SchemaDescriptor::new).
    prop_oneof![
        Just(type_code::U8),   Just(type_code::I8),
        Just(type_code::U16),  Just(type_code::I16),
        Just(type_code::U32),  Just(type_code::I32),
        Just(type_code::U64),  Just(type_code::I64),
        Just(type_code::U128), Just(type_code::I128),
        Just(type_code::UUID),
    ]
}

fn arb_payload_type() -> impl Strategy<Value = u8> {
    // All 15 type codes are valid payloads. BLOB shares STRING's 16-byte
    // German-string layout and must be covered too (region-count + blob arena).
    prop_oneof![
        arb_pk_type(),
        Just(type_code::F32), Just(type_code::F64),
        Just(type_code::STRING), Just(type_code::BLOB),
    ]
}

/// Build a schema from PK type codes + payload (type, nullable) pairs.
/// PK columns come first (non-nullable); payloads follow. `nullable` is a
/// `u8` (0 / 1), matching `SchemaColumn::new`'s parameter type.
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

/// Arbitrary valid schema. PK arity is 1..=MAX_PK_COLUMNS (== 5 — this is the
/// in-memory descriptor cap = 4 user columns + 1 index-prefix slot; the
/// user-facing SQL cap, PK_LIST_MAX_COLS == 4, is intentionally exceeded
/// because the proptest exercises internal index-shaped schemas). The total
/// stride is constrained to <= MAX_PK_BYTES; this filter is a safety net
/// (currently always true, since MAX_PK_COLUMNS * 16 == MAX_PK_BYTES, but it
/// auto-corrects if either constant changes). The generator naturally
/// produces narrow (<=16), non-power-of-two narrow (6, 10, 12), and wide
/// (>16) strides — the three width arms — plus zero-payload (index-shaped)
/// and payload-bearing (user-shaped) layouts.
fn arb_schema() -> impl Strategy<Value = SchemaDescriptor> {
    let pk = prop::collection::vec(arb_pk_type(), 1..=crate::schema::MAX_PK_COLUMNS)
        .prop_filter("pk_stride must fit MAX_PK_BYTES",
            |t| pk_stride_of(t) <= crate::schema::MAX_PK_BYTES);
    let payload = prop::collection::vec((arb_payload_type(), 0u8..=1), 0..=4);
    (pk, payload).prop_map(|(pk, pl)| build_schema(&pk, &pl))
}

proptest! {
    /// Ingest -> flush -> scan must round-trip the multiset of rows, for both
    /// persistence modes and at every PK width. `SchemaDescriptor` is `Copy`,
    /// so passing `schema` to `Table::new` by value leaves it usable below.
    #[test]
    fn batch_roundtrip(
        schema in arb_schema(),
        rows in 1usize..=64,
        durable in any::<bool>(),
    ) {
        let dir = tempfile::tempdir().unwrap();   // RAII: unlinked on drop
        let tdir = dir.path().join("rt");
        let persistence = if durable { Persistence::Durable } else { Persistence::Ephemeral };
        let mut table = Table::new(
            tdir.to_str().unwrap(), "t", schema, 1, 1 << 20, persistence,
        ).unwrap();
        let original = arb_batch(&schema, rows);
        table.ingest_owned_batch(original.clone_batch()).unwrap();
        table.flush().unwrap();

        let expected = zset_of(&original, &schema);

        // Cursor / byte-merge path — width-agnostic, exercised for both stores.
        let scanned = table.full_scan();
        prop_assert_eq!(
            &expected, &zset_of(scanned.as_ref(), &schema),
            "full_scan does not match ingest under stride {}", schema.pk_stride(),
        );

        let shards = table.all_shard_arcs();
        if durable {
            // A durable flush synchronously commits exactly one on-disk shard.
            prop_assert_eq!(shards.len(), 1);
            // Direct shard decode: on-disk region layout + wide-PK Raw guard.
            let owned = shards[0].to_owned_batch(&schema);
            prop_assert_eq!(&expected, &zset_of(&owned, &schema));
        } else {
            // A sub-ceiling ephemeral flush writes no shard; rows live in
            // in_memory_l0 and are served by full_scan (asserted above).
            prop_assert!(shards.is_empty());
        }
        table.close();
    }

    /// has_pk_bytes re-finds every ingested row before and after flush; an
    /// absent PK is rejected both times. Durable so the post-flush XOR8 bloom
    /// (xxh3 collapse for stride > 16, widen_pk_be for <= 16) is probed.
    #[test]
    fn point_lookup_after_flush(schema in arb_schema(), rows in 1usize..=64) {
        // Durable table. Build rows with prefix-twin wide PKs (see arb_batch),
        // ingest. For each row: prop_assert!(table.has_pk_bytes(&pk)) (memtable).
        // Synthesize an absent PK and prop_assert!(!has_pk_bytes(absent)).
        // flush(), then repeat both assertions against the on-disk shard.
    }

    /// Retraction reconciles memtable and shard. Durable, unit-weight rows.
    #[test]
    fn retract_then_scan(schema in arb_schema(), rows in 2usize..=64) {
        // Durable table; ingest n unit-weight rows; flush (-> shard). For a
        // deterministic half, (w, found) = retract_pk_bytes(&pk):
        // prop_assert_eq!((w, found), (1, true)) then
        // prop_assert!(!has_pk_bytes(&pk)). full_scan reconciles +1/-1 to zero
        // for the retracted half; assert zset_of(full_scan) == the un-retracted
        // half. No second flush: the cursor merges shard + memtable runs.
    }
}
```

### Helpers

`arb_batch(schema, n)` builds the batch row-by-row via
`Batch::with_schema(*schema, n)` + the public `extend_*` appenders, bumping
`batch.count` per row (the appenders clear `sorted`/`consolidated`, so the
batch is correctly marked unsorted and `ingest_owned_batch` sorts and
consolidates it). It walks `schema.payload_columns()` — yielding
`(payload_idx, col_idx, &SchemaColumn)` — and fills per type. **The helper
IS the test**: its correctness over arbitrary width is what makes the
proptest interesting. Per row:

- **PK**: generate one native `u128` per PK column (masked to the column's
  `size()`), then `batch.extend_pk_opk(schema, &native_pk_vals)`. This
  OPK-encodes (big-endian, sign-flip for signed columns) via
  `encode_order_preserving_pk` — the same path real data takes — so the test
  exercises the OPK encoder, not just opaque bytes. (Do **not** write raw LE
  bytes via `extend_pk_bytes`; that round-trips only because storage treats
  PKs as opaque, and never invokes the sign-flip / typed-order encoder.)
  Keep PKs distinct by fixing the **trailing** PK column to the row ordinal
  while the leading columns vary; when leading columns repeat this naturally
  yields the **prefix-twin** wide PKs (shared 16-byte OPK prefix, distinct
  tail) the point-lookup test wants.
- **weight**: `extend_weight(&w.to_le_bytes())` — a positive `i64` (base-table
  positivity, §1); the retraction test fixes `w == 1`.
- **null word**: `extend_null_bmp(&nw.to_le_bytes())`. Set bit `pi` only for
  **nullable** payload columns (`col.nullable != 0`); non-nullable columns
  must stay `0`.
- **payload columns** (`extend_col(pi, bytes)`, `bytes.len() == col.size()`):
  - null cell → `fill_col_zero(pi, col.size() as usize)` (zeros; `zset_of`
    won't read them).
  - fixed-width → the value's little-endian bytes.
  - STRING / BLOB → `let gs = gnitz_wire::encode_german_string(&value,
    &mut batch.blob); batch.extend_col(pi, &gs);`. `encode_german_string`
    returns the 16-byte struct and, for `len > 12`
    (`SHORT_STRING_THRESHOLD`), appends to `batch.blob` and writes the
    correct offset. Generate empty, short (`≤ 12`), and at least one
    long (`> 12`) value so the inline-vs-blob split and the blob arena /
    offset path are both exercised.

`zset_of` reduces a batch to a multiset of `(pk_bytes, null_word,
logical_payload_values) → weight`. STRING / BLOB columns are **decoded**
before comparison: the 16-byte German-string struct embeds a blob offset for
long strings, so two batches holding the same logical string carry different
struct bytes. `get_col_ptr`'s second argument is the **payload index** (not
the schema column index), matching `payload_columns()`'s `pi`.

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

Temp directories use plain `tempfile::tempdir()` (a `tempfile::TempDir` RAII
guard that unlinks its tree on drop), matching the universal storage-test
precedent — pass `dir.path().join(name).to_str().unwrap()` (a `&str`) as
`Table::new`'s first argument. Each proptest case binds and drops its own
`TempDir`, so there is no accumulation even under a `PROPTEST_CASES=4096`
sweep; no custom guard or pinned tmp root is needed. (The
`~/git/gnitz/tmp/` location in dev-guide is the Python E2E log dir, a
different directory from the repo root and not the Rust storage-test
convention, which is `tempfile::tempdir()` under the system temp.)

`proptest` and `tempfile` are already `gnitz-engine` dev-dependencies; no
Cargo change is needed.

## What a failure looks like

Proptest prints the shrunk input on the failing assertion, including the
schema. A region-count or width bug reduces to a one-line failure naming the
minimal schema, e.g.:

```text
test batch_roundtrip failed:
  schema { columns: [U64, U64], pk_indices: [0, 1] }   (pk_stride=16, 0 payload)
  rows: 1, durable: true
  message: full_scan does not match ingest under stride 16
```

A zero-payload schema (`pk_count == num_cols`) stresses region counting in
the shard reader; a `> 16`-stride schema stresses the byte-path read-back and
the `to_owned_batch` `RegionView::Raw` guard; a non-power-of-two narrow stride
(6, 10, 12) stresses `widen_pk_be`'s zero-extend and the bloom that consumes
the widened key; a prefix-twin wide pair stresses the 16-byte-prefix memtable
bloom.

## Testing

- `cargo test -p gnitz-engine data_roundtrip` runs the proptests. Default
  256 cases per test; three tests cost a few seconds on a debug build (the
  `Durable` arms do real shard I/O into a temp dir).
- On failure, proptest's default output prints the shrunk input (schema +
  row count + persistence flag) on the failing assertion. Capture that in
  the PR description.
- Treat the proptest as the gate for any new storage-layer change that
  touches PK arithmetic, region counting, or encoding selection: run it once
  with `PROPTEST_CASES=4096` as part of reviewing such a change.
