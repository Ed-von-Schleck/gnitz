//! Storage-layer data round-trip property tests.
//!
//! The schema codec has its own round-trip proptest; this is the analogous
//! coverage for the **data** layer — the path from `Batch` → run set →
//! shard file → `MappedShard` / `ReadCursor` — over random `(type codes, PK
//! arity, payload shape, column interleaving)`: every PK-width arm, both read
//! paths (the `full_scan` merge cursor and the direct `slice_to_owned_batch`
//! shard decode), and the non-prefix payload-slot renumbering.

use proptest::prelude::*;

use gnitz_wire::MAX_PK_COLUMNS;

use crate::storage::{RecoverySource, StoreBudgets, Table};
use crate::test_support::{arb_schema, row_key, zset_of};
use gnitz_expr::RowSource;
use gnitz_expr::{ColumnTable, SchemaFacts};
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
        .map(|&c| schema.columns[c as usize].size() as usize)
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
                batch.put_int(rng.gen_u128() & gnitz_wire::image_mask(col.size() as usize));
            }
        }
        batch.end_row();
    }

    // Raw, so ingest_owned_batch sorts and consolidates it.
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

fn new_table(dir: &std::path::Path, schema: SchemaDescriptor, durable: bool) -> Table {
    let p = if durable {
        RecoverySource::SalReplay
    } else {
        RecoverySource::Rederive { resume_at: None }
    };
    Table::new(dir.to_str().unwrap(), schema, p, StoreBudgets::default()).unwrap()
}

proptest! {
    /// Ingest -> flush -> scan round-trips the multiset, for both persistence
    /// modes at every PK width and column interleaving.
    #[test]
    fn batch_roundtrip(
        schema in arb_schema(MAX_PK_COLUMNS),
        rows in 1usize..=64,
        durable in any::<bool>(),
        seed in any::<u64>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("rt"), schema, durable);
        let (original, _) = arb_batch(&schema, rows, seed);

        table.ingest_owned_batch(Batch::clone(&original)).unwrap();
        if durable { table.flush() } else { table.fold_to_ram() }.unwrap();

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
            // Direct shard decode of the on-disk region layout.
            let owned = shards[0].slice_to_owned_batch(0, shards[0].row_count());
            prop_assert_eq!(&expected, &zset_of(&owned, &schema));
        } else {
            // A sub-ceiling ephemeral flush writes no shard; rows live in
            // the RAM tier and are served by full_scan (asserted above).
            prop_assert!(shards.is_empty());
        }
    }

    /// has_pk_bytes re-finds every ingested row before and after flush; an
    /// absent prefix-twin is rejected both times. Durable so the post-flush
    /// shard PK filter is probed.
    #[test]
    fn point_lookup_after_flush(schema in arb_schema(MAX_PK_COLUMNS), rows in 1usize..=64, seed in any::<u64>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("pl"), schema, true);
        let (original, leading) = arb_batch(&schema, rows, seed);

        // Absent prefix-twin: same leading PK columns, trailing ordinal == rows
        // (never used by a real row): the PK filter may report it as a false
        // positive, which the scan must reject.
        let mut absent_vals = leading.clone();
        absent_vals.push(rows as u128);
        let absent = crate::test_support::opk_pk(&schema, &absent_vals);

        table.ingest_owned_batch(Batch::clone(&original)).unwrap();

        for i in 0..rows {
            prop_assert!(table.has_pk_bytes(original.get_pk_bytes(i)));
        }
        prop_assert!(!table.has_pk_bytes(&absent));

        table.flush().unwrap();

        for i in 0..rows {
            prop_assert!(table.has_pk_bytes(original.get_pk_bytes(i)));
        }
        prop_assert!(!table.has_pk_bytes(&absent));
    }

    /// live_row_at is a read-only probe; physical retraction ingests a
    /// negated batch. full_scan nets shard (+w) against memtable (-w) to zero.
    #[test]
    fn retract_then_scan(schema in arb_schema(MAX_PK_COLUMNS), rows in 2usize..=64, seed in any::<u64>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("rx"), schema, true);
        let (original, _) = arb_batch(&schema, rows, seed);

        table.ingest_owned_batch(Batch::clone(&original)).unwrap();
        table.flush().unwrap(); // rows now live in one on-disk shard

        let half = rows / 2; // retract rows [0, half)

        // Read-only probe of the live shard rows.
        for i in 0..half {
            let (w, found) = table.live_row_at(original.get_pk_bytes(i));
            prop_assert_eq!(w, original.get_weight(i));
            prop_assert!(found.is_some());
        }

        // Physical retraction: ingest the same rows negated into the memtable.
        let neg = Batch::from_ranges(&original, &[(0, half)], 0);
        table.ingest_owned_batch(neg.negated()).unwrap();

        for i in 0..half {
            prop_assert!(!table.has_pk_bytes(original.get_pk_bytes(i)));
        }

        // Surviving set == the un-retracted half [half, rows).
        let mut expected = zset_of(&original, &schema);
        for i in 0..half {
            expected.remove(&row_key(&original, &schema, i));
        }
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));
    }

    /// Flush in 5 waves (> L0_COMPACT_THRESHOLD == 4), then compact: the k-way
    /// shard merge must preserve the multiset over a random schema.
    #[test]
    fn compaction_roundtrip(schema in arb_schema(MAX_PK_COLUMNS), rows in 5usize..=64, seed in any::<u64>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("cp"), schema, true);
        let (original, _) = arb_batch(&schema, rows, seed);

        // Exactly WAVES non-empty flushes -> WAVES L0 shards. WAVES == 5 is the
        // minimum that crosses the strictly-greater trigger (l0.len() > 4). The
        // boundaries k*rows/WAVES distribute rows evenly; rows >= WAVES makes
        // every wave non-empty (each gets >= floor(rows/WAVES) >= 1 rows).
        // append_batch relocates string blobs (the batch has a schema), so each
        // wave is self-contained.
        const WAVES: usize = 5;
        for k in 0..WAVES {
            let start = k * rows / WAVES;
            let end = (k + 1) * rows / WAVES;
            let wave = Batch::from_ranges(&original, &[(start, end)], 0);
            table.ingest_owned_batch(wave).unwrap();
            table.flush().unwrap();
        }
        // Registering the WAVES'th shard crosses `l0.len() > 4`, so the flush
        // loop itself compacted into guards below L0.
        prop_assert!(
            table.level_shape().1.iter().sum::<usize>() > 0,
            "WAVES flushes must have driven an L0->L1 compaction"
        );

        let expected = zset_of(&original, &schema);
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));
    }
}
