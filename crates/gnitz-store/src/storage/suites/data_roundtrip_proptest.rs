//! Storage-layer data round-trip property tests.
//!
//! The schema codec has its own round-trip proptest; this is the analogous
//! coverage for the **data** layer — the path from `Batch` → run set →
//! shard file → `MappedShard` / `ReadCursor`. Every existing storage unit test
//! fixes one concrete schema (almost always `(U64 PK)` or `(U64 PK, I64)`) and
//! exercises one code path; PK-width and PK-arity assumptions hard-coded across
//! unrelated files stay quiet until a single change routes traffic through them.
//!
//! A single proptest that iterates over `(type codes, PK arity, payload shape,
//! column interleaving)` instead of fixing them sweeps every PK-width arm
//! (narrow `≤ 16`, non-power-of-two narrow `6/10/12`, wide `> 16`), both read
//! paths (the `full_scan` byte-merge cursor and the direct `slice_to_owned_batch`
//! shard decode), and the non-prefix payload-slot renumbering in one place.

use proptest::prelude::*;

use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_PK_BYTES, MAX_PK_COLUMNS};
use crate::storage::{Batch, RamBudgets, RecoverySource, Table};
use crate::test_support::{arb_pk_type, arb_type_code, row_key, zset_of};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Arbitrary valid schema. PK arity is 1..=MAX_PK_COLUMNS (== 5 — the in-memory
/// descriptor cap = 4 user columns + 1 index-prefix slot; the user-facing SQL
/// cap PK_LIST_MAX_COLS == 4 is intentionally exceeded to reach internal
/// index-shaped schemas). Columns are shuffled so PK columns are not always a
/// prefix — that exercises `compute_mappings`' payload renumbering around every
/// PK position, where the closed form `payload_idx = ci - pk_count` breaks.
/// PK columns are non-nullable (SchemaDescriptor::new rejects nullable PKs). The
/// stride filter is a safety net (currently always true: MAX_PK_COLUMNS * 16 ==
/// MAX_PK_BYTES) that auto-corrects if either constant changes.
fn arb_schema() -> impl Strategy<Value = SchemaDescriptor> {
    let pk = prop::collection::vec(arb_pk_type(), 1..=MAX_PK_COLUMNS);
    // Any of the 15 type codes is a valid payload column (incl. F32/F64 and the
    // German-string STRING/BLOB), so payloads draw from the full type set.
    let payload = prop::collection::vec((arb_type_code(), 0u8..=1), 0..=4);
    (pk, payload)
        // (type_code, nullable, is_pk); PK columns are non-nullable.
        .prop_map(|(pk_types, payloads)| {
            let mut specs: Vec<(u8, u8, bool)> = pk_types.into_iter().map(|tc| (tc, 0u8, true)).collect();
            specs.extend(payloads.into_iter().map(|(tc, n)| (tc, n, false)));
            specs
        })
        .prop_shuffle() // interleave PK and payload columns
        .prop_filter("pk_stride must fit MAX_PK_BYTES", |specs| {
            specs
                .iter()
                .filter(|&&(_, _, is_pk)| is_pk)
                .map(|&(tc, _, _)| gnitz_wire::wire_stride(tc))
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

// ---------------------------------------------------------------------------
// Row generation
// ---------------------------------------------------------------------------

/// Build a batch row-by-row via `Batch::with_capacity` and the public `extend_*`
/// appenders. The helper IS the test: its correctness over arbitrary
/// width/interleaving is what makes the proptest interesting.
///
/// Returns the batch plus the fixed leading PK column values (empty for a
/// single-column PK), so a caller can synthesize an absent prefix-twin key.
fn arb_batch(schema: &SchemaDescriptor, n: usize, seed: u64) -> (Batch, Vec<u128>) {
    let mut rng = crate::test_rng::Rng::new(seed);
    let mut batch = Batch::with_capacity(*schema, n);

    let pk_count = schema.pk_columns().count();
    // The leading PK columns are fixed once; only the trailing column varies
    // (= row ordinal). That keeps PKs distinct (the ordinal `< n ≤ 64 < 256 ≤
    // 2^(8·width)` of the narrowest column, so no truncation collision) and,
    // for wide PKs, makes every row share a `≥ 16`-byte OPK prefix.
    let leading: Vec<u128> = (0..pk_count.saturating_sub(1)).map(|_| rng.gen_u128()).collect();

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
        for (pi, col) in schema.payload_columns() {
            if col.nullable != 0 && rng.gen_range(2) == 0 {
                gnitz_wire::null_word_set(&mut nw, pi, true);
            }
        }
        // Payload columns, in payload (not schema-column) index order.
        for (pi, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            if gnitz_wire::null_word_get(nw, pi) {
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
        batch.commit_row(nw);
    }

    // extend_* did not touch the flags and the constructor defaults to Raw; a
    // fresh batch is already Raw so ingest_owned_batch will sort + consolidate.
    (batch, leading)
}

/// Empty / inline (<=12) / blob (>12) lengths exercise the inline German-string
/// struct, the blob arena, and the offset path. Bytes need not be valid UTF-8:
/// the storage layer stores raw bytes and decode returns them verbatim.
fn arb_string(rng: &mut crate::test_rng::Rng) -> Vec<u8> {
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

// Table::with_budgets(dir, schema, table_id, recovery, ram). The two numerics
// are table_id = 1 and a 1 MiB memtable budget — large enough that no test
// here folds — not a row capacity. The constructor creates the not-yet-
// existing sub-dir. all_shard_arcs returns `Vec<Rc<MappedShard>>`
// (single-threaded — Rc, not Arc).
fn new_table(dir: &std::path::Path, schema: SchemaDescriptor, durable: bool) -> Table {
    let p = if durable {
        RecoverySource::SalReplay
    } else {
        RecoverySource::Rederive { resume_at: None }
    };
    let ram = RamBudgets {
        memtable_bytes: 1 << 20,
        ..Default::default()
    };
    Table::with_budgets(dir.to_str().unwrap(), schema, 1, p, ram).unwrap()
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
            let owned = shards[0].slice_to_owned_batch(0, shards[0].count, &schema);
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
    }

    /// live_row_at is a read-only probe; physical retraction ingests a
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
            let (w, found) = table.live_row_at(original.get_pk_bytes(i));
            prop_assert_eq!(w, original.get_weight(i));
            prop_assert!(found.is_some());
        }

        // Physical retraction: ingest the same rows negated into the memtable.
        let mut neg = Batch::with_capacity(schema, half.max(1));
        neg.append_batch(&original, 0, half);
        neg.map_weights(|w| -w);
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
    }

    /// Flush in 5 waves (> L0_COMPACT_THRESHOLD == 4), then compact: the k-way
    /// shard merge must preserve the multiset over a random schema.
    #[test]
    fn compaction_roundtrip(schema in arb_schema(), rows in 5usize..=64, seed in any::<u64>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut table = new_table(&dir.path().join("cp"), schema, true);
        let (original, _) = arb_batch(&schema, rows, seed);

        // Exactly WAVES non-empty flushes -> WAVES L0 shards. WAVES == 5 is the
        // minimum that crosses the strictly-greater trigger (l0.len() > 4). The
        // boundaries k*rows/WAVES distribute rows evenly; rows >= WAVES makes
        // every wave non-empty (each gets >= floor(rows/WAVES) >= 1 rows). A
        // fixed chunk = rows.div_ceil(6) is WRONG: at rows in {7, 8} it forms
        // only 4 waves (chunk 2, ceil(7/2) == 4), so `len() > 4` would fail.
        // append_batch relocates string blobs (the batch has a schema), so each
        // wave is self-contained.
        const WAVES: usize = 5;
        for k in 0..WAVES {
            let start = k * rows / WAVES;
            let end = (k + 1) * rows / WAVES;
            let mut wave = Batch::with_capacity(schema, end - start);
            wave.append_batch(&original, start, end);
            table.ingest_owned_batch(wave).unwrap();
            table.flush().unwrap();
        }
        // Registering the WAVES'th shard crosses `l0.len() > 4`, so the flush
        // loop itself compacted. A compaction output carries the `_L` level
        // marker; the count is not fixed (L0 -> L1 emits one shard per guard).
        let compacted = std::fs::read_dir(dir.path().join("cp")).unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains("_L"))
            .count();
        prop_assert!(compacted > 0, "WAVES flushes must have driven an L0->L1 compaction");

        let expected = zset_of(&original, &schema);
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));
    }
}
