//! Storage-layer data round-trip property tests.
//!
//! The schema codec has its own round-trip proptest; this is the analogous
//! coverage for the **data** layer — the path from `Batch` → `MemTable` →
//! shard file → `MappedShard` / `ReadCursor`. Every existing storage unit test
//! fixes one concrete schema (almost always `(U64 PK)` or `(U64 PK, I64)`) and
//! exercises one code path; PK-width and PK-arity assumptions hard-coded across
//! unrelated files stay quiet until a single change routes traffic through them.
//!
//! A single proptest that iterates over `(type codes, PK arity, payload shape,
//! column interleaving)` instead of fixing them sweeps every PK-width arm
//! (narrow `≤ 16`, non-power-of-two narrow `6/10/12`, wide `> 16`), both read
//! paths (the `full_scan` byte-merge cursor and the direct `to_owned_batch`
//! shard decode), and the non-prefix `payload_mapping` renumbering in one place.

use std::collections::HashMap;

use proptest::prelude::*;

use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_PK_BYTES, MAX_PK_COLUMNS};
use crate::storage::{Batch, Persistence, Table};
use crate::test_support::{arb_pk_type, arb_type_code};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Row generation
// ---------------------------------------------------------------------------

/// Build a batch row-by-row via `Batch::with_schema` and the public `extend_*`
/// appenders. The helper IS the test: its correctness over arbitrary
/// width/interleaving is what makes the proptest interesting.
///
/// Returns the batch plus the fixed leading PK column values (empty for a
/// single-column PK), so a caller can synthesize an absent prefix-twin key.
fn arb_batch(schema: &SchemaDescriptor, n: usize, seed: u64) -> (Batch, Vec<u128>) {
    let mut rng = crate::test_rng::Rng::new(seed);
    let mut batch = Batch::with_schema(*schema, n);

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

    // extend_* did not touch the flags and with_schema defaults to Raw; a fresh
    // batch is already Raw so ingest_owned_batch will sort + consolidate.
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
// Z-Set reduction
// ---------------------------------------------------------------------------

type RowKey = (Vec<u8>, Vec<Option<Vec<u8>>>);

/// Reduce a row to its **logical** identity. Null is `None`, distinct from an
/// empty string `Some(vec![])` — keeping the raw null word out of the key, so
/// its non-load-bearing unused bits cannot cause a spurious mismatch, while
/// every meaningful null bit is still reflected by a `None` entry. STRING /
/// BLOB cells are **decoded** before comparison (the 16-byte German-string
/// struct embeds a blob offset, so two batches holding the same logical string
/// carry different struct bytes).
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
        // get_col_ptr's second argument is the payload index, matching `pi`.
        let raw = batch.get_col_ptr(row, pi, cs);
        let logical = if gnitz_wire::is_german_string(col.type_code) {
            let st: [u8; 16] = raw.try_into().unwrap();
            crate::schema::try_decode_german_string(&st, &batch.blob).unwrap()
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Table::new(dir, name, schema, table_id, arena_size, persistence). The two
// numerics are table_id = 1 and arena_size = 1 MiB (the memtable arena byte
// budget), not a row capacity. Table::new calls ensure_dir, so the not-yet-
// existing sub-dir is created here. full_scan / has_pk_bytes / retract_pk_bytes
// all take `&mut self` (full_scan memoizes into a per-table cache, invalidated
// on ingest) and all_shard_arcs returns `Vec<Rc<MappedShard>>` (single-threaded
// — Rc, not Arc), so every test binds `let mut table`.
fn new_table(dir: &std::path::Path, schema: SchemaDescriptor, durable: bool) -> Table {
    let p = if durable {
        Persistence::Durable
    } else {
        Persistence::Ephemeral
    };
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
            let mut wave = Batch::with_schema(schema, end - start);
            wave.append_batch(&original, start, end);
            table.ingest_owned_batch(wave).unwrap();
            table.flush().unwrap();
        }
        // flush() does not auto-compact, so all 5 shards are live here.
        prop_assert_eq!(
            table.all_shard_arcs().len(), WAVES,
            "expected WAVES live L0 shards (> L0_COMPACT_THRESHOLD == 4)",
        );

        table.compact_if_needed().unwrap();

        let expected = zset_of(&original, &schema);
        prop_assert_eq!(&expected, &zset_of(table.full_scan().as_ref(), &schema));
        table.close();
    }
}
