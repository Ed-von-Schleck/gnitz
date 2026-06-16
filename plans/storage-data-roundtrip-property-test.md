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
zero-payload, arbitrary-stride keys, and PK columns at non-prefix positions.
`SchemaDescriptor::new` (`schema.rs`) imposes no PK-prefix or PK-ascending
constraint: it only rejects STRING/BLOB/float/nullable PK columns and duplicate
PK indices. A single proptest that iterates over `(type codes, pk arity,
payload shape, column interleaving)` instead of fixing them exercises every
width arm and the non-prefix payload-index renumbering in one sweep.

## Scope

In:

- A round-trip proptest at the storage-layer boundary that, for an
  arbitrary valid schema and either persistence mode:
  1. Generates `n` random rows for that schema (PKs distinct, payload
     values arbitrary in-type, null bitmaps randomised on nullable columns),
     from a proptest-supplied `u64` seed so failures replay and shrink.
  2. Ingests them into a `Table` and forces a flush.
  3. Scans the table back and asserts the scanned set equals the ingested
     set as Z-Sets — multiset equality on `(PK bytes, logical-payload)` tuples.
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
    (6, 10, 12), and wide (`> 16`) strides; zero-payload (index-shaped) and
    payload-bearing (user-shaped) layouts; and **interleaved** PK/payload
    column order (PK columns not a prefix) so the non-closed-form
    `payload_mapping` renumbering (§6) is exercised, not just the
    `payload_idx == ci - pk_count` easy case.
- A point-lookup proptest (`Persistence::Durable`): `Table::has_pk_bytes`
  re-finds every ingested row **both before and after flush** (so the
  pre-flush memtable bloom and the post-flush on-disk XOR8 bloom — `xxh3`
  collapse for stride `> 16`, `widen_pk_be` for `≤ 16` — are both probed),
  and a synthesized **absent** PK returns `false` both times (exercising
  bloom discrimination and the run-scan resolution of a bloom false
  positive). The generator fixes the leading PK columns and varies only the
  trailing column (= row ordinal), so wide PKs share a `≥ 16`-byte OPK prefix:
  the **prefix-twins** the 16-byte-prefix memtable bloom (`pack_pk_be`)
  collides for, plus a synthesized absent key with the **same** leading
  columns (a tolerated false positive that must fall through to the scan).
  These paths are load-bearing for `validate_unique_indices` /
  `validate_fk_parent_restrict`.
- A retraction proptest (`Persistence::Durable`): ingest `n` rows, flush
  (→ shard), then for a deterministic half probe the live row with
  `retract_pk_bytes` (asserting `(net_weight, found)`) and physically retract
  it by ingesting the same rows **negated** (weight `−w`) into the memtable —
  `retract_pk_bytes` is a **read-only** probe and does not remove anything.
  `full_scan` reconciles the shard `(+w)` against the memtable retraction
  `(−w)` to net zero, no second flush needed; assert the surviving set equals
  the un-retracted half and that `has_pk_bytes` is `false` for the retracted
  half. `retract_pk_bytes` `debug_assert`s `in_memory_l0.is_empty()`, so this
  sub-test **must** be `Durable` (an `Ephemeral` flush parks rows in
  `in_memory_l0`, tripping the assert). It is the width-swept generalisation
  of the existing `table_wide_pk_has_and_retract_bytes`.
- A compaction proptest (`Persistence::Durable`): ingest the rows in
  `> L0_COMPACT_THRESHOLD` (== 4) flushed waves so the L0 shard count crosses
  the compaction trigger, then `Table::compact_if_needed()` runs the k-way
  shard merge (`compact_shards`, the §4c path: guard-key routing, loser tree,
  pending-group drain). `full_scan` after compaction must still equal the
  ingested set. This is the only sub-test that exercises shard→shard merge
  over random schemas.

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
  keys, the 5th PK-column slot, interleaved PK/payload column order).
- PK-column value **decoding** (`extract_pk_value`). The round-trip keys
  on raw OPK bytes (the canonical key); decoding PK columns back to native
  values is a separate concern.

## Touchpoints

1. **`crates/gnitz-engine/src/schema.rs`** — add a manual `Debug` impl for
   `SchemaDescriptor` (the type derives `Clone, Copy` but not `Debug`, and
   `proptest` requires `Value: Debug` to print/shrink a failing case). A
   derive would dump the fixed `[_; MAX_COLUMNS]` / `[_; MAX_PK_COLUMNS]`
   arrays; the manual impl prints only the live columns and PK indices and is
   broadly useful for every schema assertion/log in the crate. `TypeCode` is
   already imported (`pub(crate) use gnitz_wire::TypeCode`, schema.rs:12) and
   derives `Debug`; `is_pk_col`, `num_columns`, `pk_indices`, and the public
   `columns` field are all in hand.

   ```rust
   impl std::fmt::Debug for SchemaDescriptor {
       fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
           write!(f, "SchemaDescriptor {{ columns: [")?;
           for ci in 0..self.num_columns() {
               if ci > 0 { write!(f, ", ")?; }
               let col = self.columns[ci];
               match TypeCode::try_from_u8(col.type_code) {
                   Some(t) => write!(f, "{t:?}")?,
                   None => write!(f, "type({})", col.type_code)?,
               }
               if col.nullable != 0 { write!(f, "?")?; }
               if self.is_pk_col(ci) { write!(f, " pk")?; }
           }
           write!(f, "], pk_indices: {:?} }}", self.pk_indices())
       }
   }
   ```

2. **`crates/gnitz-engine/src/storage/data_roundtrip_proptest.rs`** (new),
   declared `#[cfg(test)] mod data_roundtrip_proptest;` from `storage/mod.rs`.
   This is the **first** dedicated test-only module under `storage/` (every
   other storage test is an in-file `mod tests`); the sweep spans `Table`,
   `Batch`, `MappedShard`, and the shard reader, so a shared module is cleaner
   than wedging it into one file's `mod tests`. The module **must** be
   in-crate: `MappedShard::to_owned_batch` is `pub(crate)`, and the helpers
   rely on the `#[cfg(test)]` `Batch::extend_pk_opk`, the `#[cfg(test)]`
   `crate::test_support::opk_pk`, and the shared `#[cfg(test)]`
   `crate::test_rng::Rng`.

`proptest` and `tempfile` are already `gnitz-engine` dev-dependencies, and
`crate::test_rng::Rng` is the crate's shared deterministic PRNG — no Cargo
change and no new RNG implementation are needed.

### Strategies

```rust
use std::collections::HashMap;
use proptest::prelude::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_PK_BYTES, MAX_PK_COLUMNS};
use crate::storage::{Batch, Persistence, Table};

fn arb_pk_type() -> impl Strategy<Value = u8> {
    // Exactly TypeCode::is_pk_eligible: fixed-width signed/unsigned ints, U128,
    // I128, UUID. STRING / BLOB / float are PK-ineligible and panic in
    // SchemaDescriptor::new.
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

/// Arbitrary valid schema. PK arity is 1..=MAX_PK_COLUMNS (== 5 — the in-memory
/// descriptor cap = 4 user columns + 1 index-prefix slot; the user-facing SQL
/// cap PK_LIST_MAX_COLS == 4 is intentionally exceeded to reach internal
/// index-shaped schemas). Columns are shuffled so PK columns are not always a
/// prefix — that exercises `compute_mappings`' payload renumbering around every
/// PK position (§6), where the closed form `payload_idx = ci - pk_count` breaks.
/// PK columns are non-nullable (SchemaDescriptor::new rejects nullable PKs). The
/// stride filter is a safety net (currently always true: MAX_PK_COLUMNS * 16 ==
/// MAX_PK_BYTES) that auto-corrects if either constant changes.
fn arb_schema() -> impl Strategy<Value = SchemaDescriptor> {
    let pk = prop::collection::vec(arb_pk_type(), 1..=MAX_PK_COLUMNS);
    let payload = prop::collection::vec((arb_payload_type(), 0u8..=1), 0..=4);
    (pk, payload)
        // (type_code, nullable, is_pk); PK columns are non-nullable.
        .prop_map(|(pk_types, payloads)| {
            let mut specs: Vec<(u8, u8, bool)> =
                pk_types.into_iter().map(|tc| (tc, 0u8, true)).collect();
            specs.extend(payloads.into_iter().map(|(tc, n)| (tc, n, false)));
            specs
        })
        .prop_shuffle() // interleave PK and payload columns
        .prop_filter("pk_stride must fit MAX_PK_BYTES", |specs| {
            specs.iter()
                .filter(|&&(_, _, is_pk)| is_pk)
                .map(|&(tc, _, _)| SchemaColumn::new(tc, 0).size() as usize)
                .sum::<usize>()
                <= MAX_PK_BYTES
        })
        .prop_map(|specs| {
            let mut cols = Vec::with_capacity(specs.len());
            let mut pk_indices = Vec::new();
            for (i, (tc, nullable, is_pk)) in specs.into_iter().enumerate() {
                cols.push(SchemaColumn::new(tc, nullable));
                if is_pk {
                    pk_indices.push(i as u32);
                }
            }
            SchemaDescriptor::new(&cols, &pk_indices)
        })
}
```

### Row generation

`arb_batch` builds the batch row-by-row via `Batch::with_schema(*schema, n)`
and the public `extend_*` appenders. **The helper IS the test**: its
correctness over arbitrary width/interleaving is what makes the proptest
interesting. Two facts drive its shape:

- `Batch::with_schema` defaults `sorted = true, consolidated = true`, and the
  low-level `extend_*` appenders (unlike the bulk `append_*` methods) do
  **not** clear those flags. `into_consolidated` fast-paths on
  `consolidated == true` and returns the batch as-is with **no** defensive
  re-sort, so the helper **must** clear both flags at the end — otherwise the
  unsorted, randomly-keyed batch is ingested mislabeled and the N-way merge /
  shard write silently corrupt.
- The leading PK columns are fixed once and reused; only the trailing column
  varies (= row ordinal). That keeps PKs distinct (the ordinal `< n ≤ 64 <
  256 ≤ 2^(8·width)` of the narrowest column, so no truncation collision) and,
  for wide PKs, makes every row share a `≥ 16`-byte OPK prefix — the
  prefix-twins the point-lookup test needs. `opk_pk` truncates each native
  value to its column width, so the raw `u128` values pass through unmasked.

```rust
/// Returns the batch plus the fixed leading PK column values (empty for a
/// single-column PK), so a caller can synthesize an absent prefix-twin key.
fn arb_batch(schema: &SchemaDescriptor, n: usize, seed: u64) -> (Batch, Vec<u128>) {
    let mut rng = crate::test_rng::Rng::new(seed);
    let mut batch = Batch::with_schema(*schema, n);

    let pk_count = schema.pk_columns().count();
    let leading: Vec<u128> =
        (0..pk_count.saturating_sub(1)).map(|_| rng.gen_u128()).collect();

    for i in 0..n {
        // PK: fixed leading columns + trailing ordinal. extend_pk_opk OPK-encodes
        // (big-endian, sign-flip for signed columns) via the production encoder.
        let mut pk_vals = leading.clone();
        pk_vals.push(i as u128);
        batch.extend_pk_opk(schema, &pk_vals);

        // Positive weight (base-table positivity, §1).
        let w = 1 + rng.gen_range(4) as i64;
        batch.extend_weight(&w.to_le_bytes());

        // Null bitmap: a null bit only where the column is nullable.
        let mut nw: u64 = 0;
        for (pi, _ci, col) in schema.payload_columns() {
            if col.nullable != 0 && rng.gen_range(2) == 0 {
                nw |= 1 << pi;
            }
        }
        batch.extend_null_bmp(&nw.to_le_bytes());

        // Payload columns, in payload (not schema-column) index order.
        for (pi, _ci, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            if (nw >> pi) & 1 != 0 {
                batch.fill_col_zero(pi, cs); // null cell; zset_of won't read it
            } else if gnitz_wire::is_german_string(col.type_code) {
                let val = arb_string(&mut rng);
                let gs = gnitz_wire::encode_german_string(&val, &mut batch.blob);
                batch.extend_col(pi, &gs);
            } else {
                let v = rng.gen_u128();
                batch.extend_col(pi, &v.to_le_bytes()[..cs]);
            }
        }
        batch.count += 1;
    }

    // extend_* did not touch the flags and with_schema defaults them true; mark
    // unsorted so ingest_owned_batch actually sorts + consolidates.
    batch.sorted = false;
    batch.consolidated = false;
    (batch, leading)
}

/// Empty / inline (<=12) / blob (>12) lengths exercise the inline German-string
/// struct, the blob arena, and the offset path. Bytes need not be valid UTF-8:
/// the storage layer stores raw bytes and decode returns them verbatim.
fn arb_string(rng: &mut crate::test_rng::Rng) -> Vec<u8> {
    let len = match rng.gen_range(4) {
        0 => 0,
        1 => 1 + rng.gen_range(12) as usize,   // 1..=12, inline
        2 => 13 + rng.gen_range(24) as usize,  // 13..=36, blob
        _ => rng.gen_range(40) as usize,       // 0..=39, mixed
    };
    (0..len).map(|_| rng.next_u64() as u8).collect()
}
```

### Z-Set reduction

`row_key` reduces a row to its **logical** identity. Null is `None`, distinct
from an empty string `Some(vec![])` — keeping the raw null word out of the key,
so its non-load-bearing unused bits cannot cause a spurious mismatch, while
every meaningful null bit is still reflected by a `None` entry. STRING / BLOB
cells are **decoded** before comparison (the 16-byte German-string struct
embeds a blob offset, so two batches holding the same logical string carry
different struct bytes). `get_col_ptr`'s second argument is the **payload
index** (not the schema column index), matching `payload_columns()`'s `pi`.

```rust
type RowKey = (Vec<u8>, Vec<Option<Vec<u8>>>);

fn row_key(batch: &Batch, schema: &SchemaDescriptor, row: usize) -> RowKey {
    let pk = batch.get_pk_bytes(row).to_vec(); // byte-addressed: any width
    let nw = batch.get_null_word(row);
    let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(schema.num_payload_cols());
    for (pi, _ci, col) in schema.payload_columns() {
        if (nw >> pi) & 1 != 0 {
            vals.push(None);
            continue;
        }
        let cs = col.size() as usize;
        let raw = batch.get_col_ptr(row, pi, cs);
        let logical = if gnitz_wire::is_german_string(col.type_code) {
            let st: [u8; 16] = raw.try_into().unwrap();
            crate::schema::decode_german_string(&st, &batch.blob)
        } else {
            raw.to_vec()
        };
        vals.push(Some(logical));
    }
    (pk, vals)
}

fn zset_of(batch: &Batch, schema: &SchemaDescriptor) -> HashMap<RowKey, i64> {
    let mut z: HashMap<RowKey, i64> = HashMap::new();
    for row in 0..batch.count {
        *z.entry(row_key(batch, schema, row)).or_insert(0) += batch.get_weight(row);
    }
    z.retain(|_, w| *w != 0);
    z
}
```

### Tests

`tempfile::tempdir()` is the universal storage-test convention (a
`tempfile::TempDir` RAII guard that unlinks its tree on drop). Each proptest
case binds and drops its own `TempDir`, so there is no accumulation even under
`PROPTEST_CASES=4096`. `SchemaDescriptor` is `Copy`, so passing `schema` by
value to `new_table` leaves it usable below.

```rust
fn new_table(dir: &std::path::Path, schema: SchemaDescriptor, durable: bool) -> Table {
    let p = if durable { Persistence::Durable } else { Persistence::Ephemeral };
    Table::new(dir.to_str().unwrap(), "t", schema, 1, 1 << 20, p).unwrap()
}

proptest! {
    /// Ingest -> flush -> scan round-trips the multiset, for both persistence
    /// modes at every PK width and column interleaving.
    #[test]
    fn batch_roundtrip(
        schema in arb_schema(),
        rows in 1usize..=64,
        durable in any::<bool>(),
        seed in any::<u64>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("rt"), schema, durable);
        let (original, _) = arb_batch(&schema, rows, seed);

        table.ingest_owned_batch(original.clone_batch()).unwrap();
        table.flush().unwrap();

        let expected = zset_of(&original, &schema);

        // Cursor / byte-merge path — width-agnostic, exercised for both stores.
        prop_assert_eq!(
            &expected, &zset_of(table.full_scan().as_ref(), &schema),
            "full_scan != ingest at pk_stride {}", schema.pk_stride(),
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
    /// absent prefix-twin is rejected both times. Durable so the post-flush
    /// XOR8 bloom is probed.
    #[test]
    fn point_lookup_after_flush(schema in arb_schema(), rows in 1usize..=64, seed in any::<u64>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("pl"), schema, true);
        let (original, leading) = arb_batch(&schema, rows, seed);

        // Absent prefix-twin: same leading PK columns, trailing ordinal == rows
        // (never used by a real row). For wide PKs it shares the 16-byte bloom
        // prefix, so the bloom may report a false positive the scan must reject.
        let mut absent_vals = leading.clone();
        absent_vals.push(rows as u128);
        let absent = crate::test_support::opk_pk(&schema, &absent_vals);

        table.ingest_owned_batch(original.clone_batch()).unwrap();

        for i in 0..rows {
            prop_assert!(table.has_pk_bytes(original.get_pk_bytes(i)));
        }
        prop_assert!(!table.has_pk_bytes(&absent));

        table.flush().unwrap();

        for i in 0..rows {
            prop_assert!(table.has_pk_bytes(original.get_pk_bytes(i)));
        }
        prop_assert!(!table.has_pk_bytes(&absent));
        table.close();
    }

    /// retract_pk_bytes is a read-only probe; physical retraction ingests a
    /// negated batch. full_scan nets shard (+w) against memtable (-w) to zero.
    #[test]
    fn retract_then_scan(schema in arb_schema(), rows in 2usize..=64, seed in any::<u64>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("rx"), schema, true);
        let (original, _) = arb_batch(&schema, rows, seed);

        table.ingest_owned_batch(original.clone_batch()).unwrap();
        table.flush().unwrap(); // rows now live in one on-disk shard

        let half = rows / 2; // retract rows [0, half)

        // Read-only probe of the live shard rows.
        for i in 0..half {
            let probe = table.retract_pk_bytes(original.get_pk_bytes(i));
            prop_assert_eq!(probe, (original.get_weight(i), true));
        }

        // Physical retraction: ingest the same rows negated into the memtable.
        let mut neg = Batch::with_schema(schema, half.max(1));
        neg.append_batch_negated(&original, 0, half);
        table.ingest_owned_batch(neg).unwrap();

        for i in 0..half {
            prop_assert!(!table.has_pk_bytes(original.get_pk_bytes(i)));
        }

        // Surviving set == the un-retracted half [half, rows).
        let mut expected = zset_of(&original, &schema);
        for i in 0..half {
            expected.remove(&row_key(&original, &schema, i));
        }
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));
        table.close();
    }

    /// Flush in > L0_COMPACT_THRESHOLD (== 4) waves, then compact: the k-way
    /// shard merge must preserve the multiset over a random schema.
    #[test]
    fn compaction_roundtrip(schema in arb_schema(), rows in 5usize..=64, seed in any::<u64>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("cp"), schema, true);
        let (original, _) = arb_batch(&schema, rows, seed);

        // 6 waves over rows >= 5 yields 5..=6 non-empty L0 shards (> the
        // threshold of 4). append_batch relocates string blobs (the batch has a
        // schema), so each wave is self-contained.
        let waves = 6usize;
        let chunk = rows.div_ceil(waves);
        let mut start = 0;
        while start < rows {
            let end = (start + chunk).min(rows);
            let mut wave = Batch::with_schema(schema, end - start);
            wave.append_batch(&original, start, end);
            table.ingest_owned_batch(wave).unwrap();
            table.flush().unwrap();
            start = end;
        }
        prop_assert!(table.all_shard_arcs().len() > 4, "need > L0_COMPACT_THRESHOLD shards");

        table.compact_if_needed().unwrap();

        let expected = zset_of(&original, &schema);
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));
        table.close();
    }
}
```

## What a failure looks like

Proptest prints the shrunk input on the failing assertion, including the schema
(via the new `Debug` impl) and the seed (so the row data replays exactly):

```text
test batch_roundtrip failed:
  schema: SchemaDescriptor { columns: [U64 pk, U64 pk], pk_indices: [0, 1] }
  rows: 1, durable: true, seed: 0
  message: full_scan != ingest at pk_stride 16
```

A zero-payload schema (`pk_count == num_cols`) stresses region counting in the
shard reader; a `> 16`-stride schema stresses the byte-path read-back and the
`to_owned_batch` `RegionView::Raw` guard; a non-power-of-two narrow stride
(6, 10, 12) stresses `widen_pk_be`'s zero-extend and the bloom that consumes
the widened key; an interleaved (non-prefix) PK layout stresses the
`payload_mapping` renumbering; a prefix-twin wide pair stresses the
16-byte-prefix memtable bloom; the compaction sub-test stresses the §4c k-way
shard merge.

## Testing

- `cargo test -p gnitz-engine data_roundtrip` runs the four proptests. Default
  256 cases each; the `Durable` arms do real shard I/O into a temp dir, so a
  full run costs a few seconds on a debug build.
- On failure, proptest's default output prints the shrunk input (schema + row
  count + persistence flag + seed) on the failing assertion. Capture that in
  the PR description.
- Treat the proptest as the gate for any new storage-layer change that touches
  PK arithmetic, region counting, payload-index mapping, or encoding selection:
  run it once with `PROPTEST_CASES=4096` as part of reviewing such a change.
