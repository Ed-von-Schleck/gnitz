//! Storage-layer data round-trip property tests.
//!
//! The schema codec has its own round-trip proptest; this is the analogous
//! coverage for the **data** layer — the path from `Batch` → run set →
//! shard file → compaction → `MappedShard` / `ReadCursor` — over random `(type
//! codes, PK arity, payload shape, column interleaving)`: every PK-width arm,
//! both read paths (the `full_scan` merge cursor and the direct
//! `slice_to_owned_batch` shard decode), and the non-prefix payload-slot
//! renumbering.

use proptest::prelude::*;

use gnitz_wire::MAX_PK_COLUMNS;

use crate::storage::{flush_barrier, RecoverySource, Table, DEFAULT_RAM_TIER_BYTES};
use crate::test_support::{self, arb_schema, row_key, zset_of, zset_sum};

use gnitz_wire::RowSource;
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::SchemaDescriptor;

// ---------------------------------------------------------------------------
// Row generation
// ---------------------------------------------------------------------------

/// Build a batch row by row through [`BatchBuilder`].
///
/// Returns the batch plus the fixed leading PK column values (empty for a
/// single-column PK), so a caller can synthesize an absent prefix-twin key.
fn arb_batch(schema: &SchemaDescriptor, n: usize, seed: u64) -> (Batch, Vec<u128>) {
    let mut rng = crate::test_support::Rng::new(seed);
    let mut batch = BatchBuilder::new(schema);

    let pk_widths: Vec<usize> = schema
        .pk_cols()
        .iter()
        .map(|&c| schema.columns()[c as usize].size())
        .collect();
    // The leading PK columns are fixed once; only the trailing column varies
    // (= row ordinal). That keeps PKs distinct (the ordinal `< n ≤ 64 < 256 ≤
    // 2^(8·width)` of the narrowest column, so no truncation collision) and,
    // for wide PKs, makes every row share a `≥ 16`-byte OPK prefix.
    let leading: Vec<u128> = pk_widths[..pk_widths.len() - 1]
        .iter()
        .map(|&w| rng.gen_u128() & gnitz_wire::image_mask(w))
        .collect();

    for i in 0..n {
        // PK: fixed leading columns + trailing ordinal, OPK-encoded by the
        // production encoder. Positive weight (base-table positivity, §1).
        let mut pk_vals = leading.clone();
        pk_vals.push(i as u128);
        let w = 1 + rng.gen_range(4) as i64;
        batch.begin_row_bytes(schema.opk_key_cols(&pk_vals).pk_bytes(), w);

        // Payload columns, in payload (not schema-column) index order; a NULL
        // only where the column is nullable.
        for (_, col) in schema.payload_columns() {
            if col.nullable && rng.gen_range(2) == 0 {
                batch.put_null();
            } else if col.type_code.is_german_string() {
                batch.put_blob(&arb_string(&mut rng));
            } else {
                batch.put_int(rng.gen_u128() & gnitz_wire::image_mask(col.size()));
            }
        }
        batch.end_row();
    }

    // Raw, so ingest sorts and consolidates it.
    (batch.finish(), leading)
}

/// Empty / inline (<=12) / blob (>12) lengths exercise the inline German-string
/// struct, the blob arena, and the offset path. Bytes need not be valid UTF-8:
/// the storage layer stores raw bytes and decode returns them verbatim.
fn arb_string(rng: &mut crate::test_support::Rng) -> Vec<u8> {
    let len = match rng.gen_range(3) {
        0 => 0,
        1 => 1 + rng.gen_range(12) as usize,  // 1..=12, inline
        _ => 13 + rng.gen_range(24) as usize, // 13..=36, blob
    };
    (0..len).map(|_| rng.next_u64() as u8).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Ingests, each flushed: one more than the L0 shards a store holds before it
/// owes a compaction.
const WAVES: usize = 5;

proptest! {
    /// Ingested in waves, each flushed (durable) or folded into the RAM tier
    /// (ephemeral): `has_pk_bytes` finds exactly the rows ingested so far and
    /// never an absent prefix twin the PK filter may pass; both read paths
    /// serve the multiset, a durable store's past the L0 compaction its waves
    /// owe; and a negated ingest of half the rows leaves the other half.
    #[test]
    fn data_roundtrip(
        schema in arb_schema(MAX_PK_COLUMNS),
        rows in WAVES..=64,
        durable in any::<bool>(),
        seed in any::<u64>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let mut table = match durable {
            true => test_support::new_table(&path, schema, RecoverySource::SalReplay, DEFAULT_RAM_TIER_BYTES),
            false => test_support::scratch_table(&path, schema),
        };
        let (original, mut absent) = arb_batch(&schema, rows, seed);
        // Same leading PK columns, trailing ordinal `rows`: never a real row.
        absent.push(rows as u128);
        let absent = test_support::opk_pk(&schema, &absent);
        let finds = |table: &Table, live: std::ops::Range<usize>| {
            !table.has_pk_bytes(&absent)
                && (0..rows).all(|i| table.has_pk_bytes(original.get_pk_bytes(i)) == live.contains(&i))
        };

        for k in 0..WAVES {
            let end = (k + 1) * rows / WAVES;
            // Each wave non-empty, since `rows >= WAVES`.
            table.ingest(Batch::from_ranges(&original, &[(k * rows / WAVES, end)], 0)).unwrap();
            prop_assert!(finds(&table, 0..end));
            if durable { flush_barrier([&mut table], 0) } else { table.fold_to_ram() }.unwrap();
            prop_assert!(finds(&table, 0..end));
        }

        let mut expected = zset_of(&original, &schema);
        if durable {
            // One L0 shard per wave crosses `l0.len() > 4`, so the store owes
            // the fold into guards below L0.
            table.settle().unwrap();
            prop_assert!(table.level_shape().1.iter().sum::<usize>() > 0, "no L0->L1 compaction");
            // Direct shard decode of the on-disk region layout.
            let owned: Vec<Batch> = test_support::table_shards(&table)
                .iter()
                .map(|s| s.slice_to_owned_batch(0, s.row_count()))
                .collect();
            prop_assert_eq!(&expected, &zset_sum(&owned, &schema));
        } else {
            // A sub-ceiling ephemeral store writes no shard.
            prop_assert!(test_support::table_shards(&table).is_empty());
        }
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));

        let half = rows / 2;
        for i in 0..half {
            let (w, found) = table.live_row_at(original.get_pk_bytes(i));
            prop_assert_eq!(w, original.get_weight(i));
            prop_assert!(found.is_some());
        }
        table.ingest(Batch::from_ranges(&original, &[(0, half)], 0).negated()).unwrap();
        prop_assert!(finds(&table, half..rows));
        for i in 0..half {
            expected.remove(&row_key(&original, &schema, i));
        }
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));
    }
}
