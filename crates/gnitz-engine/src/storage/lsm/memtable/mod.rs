//! MemTable: manages sorted runs, Bloom filter, PK lookups, and shard flush.
//!
//! Split into sorted-run management ([`runs`]) and point lookup + Bloom probe
//! ([`lookup`]); the `MemTable` struct, its constructor, the inline-consolidate
//! threshold, and the found-row state accessor live here so both sub-modules
//! read the (private) fields directly.

use std::rc::Rc;

use super::batch::Batch;
use super::bloom::BloomFilter;
use crate::schema::SchemaDescriptor;

mod lookup;
mod runs;

pub(crate) use lookup::run_pk_match_rows;
pub(crate) use runs::{bloom_add_batch, consolidate_runs};

/// Maximum sorted runs before inline consolidation merges them into one.
const INLINE_CONSOLIDATE_THRESHOLD: usize = 16;

pub struct MemTable {
    runs: Vec<Rc<Batch>>,
    bloom: BloomFilter,
    schema: SchemaDescriptor,
    max_bytes: usize,
    total_row_count: usize,
    runs_bytes: usize,
    // Last lookup result (set by lookup_pk, read by found_entry); None = unarmed.
    found: Option<(usize, usize)>,
    // Reused (run, row, weight) scratch for `find_positive_payload_row_bytes`;
    // cleared per call and kept across calls so that path never allocates.
    cand_scratch: Vec<(usize, usize, i64)>,
}

impl MemTable {
    pub fn new(schema: SchemaDescriptor, max_bytes: usize) -> Self {
        let capacity = (max_bytes / 40).max(16); // rough estimate
        MemTable {
            runs: Vec::with_capacity(INLINE_CONSOLIDATE_THRESHOLD),
            bloom: BloomFilter::new(capacity as u32),
            schema,
            max_bytes,
            total_row_count: 0,
            runs_bytes: 0,
            found: None,
            cand_scratch: Vec::new(),
        }
    }

    /// The row most recently located by `find_positive_payload_row*` / the
    /// retract probe, as `(run, row)`, or `None` when nothing is armed. The
    /// `Table` layer wraps this in a `FoundRow` `ColumnarSource` view.
    pub(super) fn found_entry(&self) -> Option<(&Batch, usize)> {
        self.found.map(|(r, row)| {
            // The indices address `runs`; every runs-shrinking path clears
            // `found` to `None`, so a stale index is a bug, not a valid state.
            debug_assert!(r < self.runs.len(), "found index must address a live run");
            (self.runs[r].as_ref(), row)
        })
    }
}

#[cfg(test)]
// 3.14 / 2.718 are deliberate test-fixture values exercising f32/f64 column
// round-trip; they're not approximations of PI/E meant to be replaced with
// std::f*::consts.
#[allow(clippy::approx_constant)]
mod tests {
    use super::super::batch::relocate_string_cell;
    use super::super::error::StorageError;
    use super::super::merge::SortedMemBatch;
    use super::runs::consolidate_batches;
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Layout;

    fn make_u64_i64_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    /// Build a consolidated Batch from (pk, weight, payload) triples.
    /// Assumes triples are already sorted by pk with no duplicate (pk, payload) pairs.
    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }

        b.into_consolidated(schema)
    }

    #[test]
    fn owned_batch_roundtrip() {
        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        assert_eq!(batch.count, 2);
        assert_eq!(batch.get_pk(0), 10);
        assert_eq!(batch.get_pk(1), 20);

        let mb = batch.as_mem_batch();
        assert_eq!(mb.count, 2);
        assert_eq!(mb.get_pk(0), 10);
        assert_eq!(mb.get_weight(1), 1);
    }

    #[test]
    fn memtable_upsert_and_snapshot() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        assert!(mt.is_empty());

        // Upsert two runs
        let b1 = make_batch(&schema, &[(10, 1, 100), (30, 1, 300)]);
        let b2 = make_batch(&schema, &[(20, 1, 200), (30, -1, 300)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();
        assert_eq!(mt.total_row_count(), 4);

        // consolidate_for_flush should fold the runs: PK 30 has +1 -1 = 0 (ghost)
        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 2); // PK 10 and 20 survive
        assert_eq!(snap.get_pk(0), 10);
        assert_eq!(snap.get_pk(1), 20);
    }

    /// Singleton fast path: when only one run is present, `consolidate_for_flush`
    /// must return an `Rc::clone` of the existing run (no rewrite).  Multi-run
    /// path produces a fresh allocation.
    #[test]
    fn consolidate_for_flush_singleton_returns_existing_run() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();
        let original = Rc::clone(&mt.snapshot_runs()[0]);

        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 2);
        assert!(
            Rc::ptr_eq(&snap, &original),
            "singleton path must Rc::clone the existing run"
        );

        // Multi-run path: add a second run, drop the singleton handle, then
        // consolidate. The merged batch must be a fresh allocation distinct
        // from either pre-existing run.
        drop(snap);
        let b2 = make_batch(&schema, &[(30, 1, 300)]);
        mt.upsert_sorted_batch(b2).unwrap();
        let runs_pre: Vec<Rc<Batch>> = mt.snapshot_runs().to_vec();
        assert_eq!(runs_pre.len(), 2);
        let snap2 = mt.consolidate_for_flush();
        assert_eq!(snap2.count, 3);
        assert!(!Rc::ptr_eq(&snap2, &runs_pre[0]));
        assert!(!Rc::ptr_eq(&snap2, &runs_pre[1]));
    }

    /// A 2-row batch with descending PKs — not `(PK, payload)`-sorted. Default
    /// flags (`with_schema` sets both true); callers adjust them per scenario.
    fn desc_two_row_batch(schema: &SchemaDescriptor) -> Batch {
        let mut b = Batch::with_schema(*schema, 2);
        for &(pk, val) in &[(20u128, 200i64), (10, 100)] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    /// A run that lies about being consolidated is rejected on the way in.
    /// `set_layout_unchecked` sets the tag without verifying data, so a descending
    /// batch flagged Consolidated is built without complaint. But the consumer that
    /// trusts the tag — `upsert_sorted_batch`, which skips a re-fold — calls
    /// `consolidated_verified`, which checks the flag against the data (debug) and
    /// panics. The `handle_message` strip clears the layout at ingress so this run
    /// never reaches the MemTable in production.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "flagged consolidated")]
    fn memtable_lying_sorted_run_rejected_at_flush() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        mt.upsert_sorted_batch(make_batch(&schema, &[(5, 1, 50)])).unwrap();

        // The lie: descending data flagged Consolidated without verification.
        // `set_layout_unchecked` trusts the caller, so the batch is built — but
        // `upsert_sorted_batch` verifies the tag and rejects the run.
        let mut bad = desc_two_row_batch(&schema);
        bad.set_layout_unchecked(Layout::Consolidated);
        mt.upsert_sorted_batch(bad).unwrap();
    }

    /// Cleared path: the same unsorted rows with both flags stripped (as the
    /// ingress strip leaves every client batch) sort+consolidate without panic.
    #[test]
    fn memtable_cleared_flags_unsorted_run_consolidates_ok() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        mt.upsert_sorted_batch(make_batch(&schema, &[(5, 1, 50)])).unwrap();

        let clean = desc_two_row_batch(&schema);
        mt.upsert_sorted_batch(clean.into_consolidated(&schema)).unwrap();

        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 3, "PK 5, 10, 20 all survive");
        assert_eq!(snap.get_pk(0), 5);
        assert_eq!(snap.get_pk(1), 10);
        assert_eq!(snap.get_pk(2), 20);
    }

    #[test]
    fn memtable_lookup_pk() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
        let b2 = make_batch(&schema, &[(20, 2, 200)]); // PK 20 appears in two runs
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        // OPK bytes for a U64 PK are the value's big-endian bytes.
        let (w, found, _) = mt.lookup_pk_bytes(&20u64.to_be_bytes());
        assert_eq!(w, 3); // 1 + 2
        assert!(found);

        let (w, found, _) = mt.lookup_pk_bytes(&99u64.to_be_bytes());
        assert_eq!(w, 0);
        assert!(!found);
    }

    #[test]
    fn memtable_bloom() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();

        assert!(mt.may_contain_pk(&10u64.to_be_bytes()));
        assert!(mt.may_contain_pk(&20u64.to_be_bytes()));
        // 99 might be a false positive, but definitely not in data
    }

    #[test]
    fn memtable_reset() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();
        assert!(!mt.is_empty());

        mt.reset();
        assert!(mt.is_empty());
        assert_eq!(mt.total_row_count(), 0);

        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 0);
    }

    /// An `Rc<Batch>` obtained from `snapshot_runs()` must keep the data
    /// alive across `reset()`.  This is the cursor-lifetime contract.
    #[test]
    fn snapshot_survives_reset() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let runs = mt.snapshot_runs();
        assert_eq!(runs.len(), 1);
        let snap = Rc::clone(&runs[0]);
        assert_eq!(snap.count, 1);

        mt.reset();
        // Snapshot still valid (Rc keeps data alive)
        assert_eq!(snap.count, 1);
        assert_eq!(snap.get_pk(0), 10);
    }

    #[test]
    fn memtable_capacity_error() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 64); // tiny capacity

        // First upsert fills past max_bytes (4 rows × 40 bytes = 160 > 64)
        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300), (40, 1, 400)]);
        mt.upsert_sorted_batch(b1).unwrap(); // OK — check fires before adding

        // Second upsert fails: runs_bytes (160) > max_bytes (64)
        let b2 = make_batch(&schema, &[(50, 1, 500)]);
        let rc = mt.upsert_sorted_batch(b2);
        assert_eq!(rc, Err(StorageError::Capacity));
    }

    #[test]
    fn memtable_find_weight_for_row() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // PK 10 with payload 100
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        // PK 10 with payload 200 (different row identity)
        let b2 = make_batch(&schema, &[(10, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        // OPK bytes for a U64 PK are the value's big-endian bytes.
        let pk10 = 10u64.to_be_bytes();

        // Search for PK 10, payload 100 — should find weight 1
        let ref_batch = make_batch(&schema, &[(10, 1, 100)]);
        let w = mt.find_weight_for_row_bytes(&pk10, &ref_batch, 0);
        assert_eq!(w, 1);

        // Search for PK 10, payload 200 — should find weight 1
        let ref_batch2 = make_batch(&schema, &[(10, 1, 200)]);
        let w2 = mt.find_weight_for_row_bytes(&pk10, &ref_batch2, 0);
        assert_eq!(w2, 1);

        // Search for PK 10, payload 999 — should find weight 0
        let ref_batch3 = make_batch(&schema, &[(10, 1, 999)]);
        let w3 = mt.find_weight_for_row_bytes(&pk10, &ref_batch3, 0);
        assert_eq!(w3, 0);
    }

    #[test]
    fn batch_append_batch() {
        let schema = make_u64_i64_schema();
        let src = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
        let mut dst = Batch::with_schema(schema, 8);

        // Append all rows
        dst.append_batch(&src, 0, 3);
        assert_eq!(dst.count, 3);
        assert_eq!(dst.get_pk(0), 10);
        assert_eq!(dst.get_pk(2), 30);
        assert_eq!(dst.get_weight(1), 1);

        // Append subset
        dst.clear();
        dst.append_batch(&src, 1, 2);
        assert_eq!(dst.count, 1);
        assert_eq!(dst.get_pk(0), 20);
    }

    #[test]
    fn batch_append_batch_negated() {
        let schema = make_u64_i64_schema();
        let src = make_batch(&schema, &[(10, 1, 100), (20, 2, 200)]);
        let mut dst = Batch::with_schema(schema, 8);

        dst.append_batch_negated(&src, 0, 2);
        assert_eq!(dst.count, 2);
        assert_eq!(dst.get_weight(0), -1);
        assert_eq!(dst.get_weight(1), -2);
        assert_eq!(dst.get_pk(0), 10);
    }

    #[test]
    fn batch_append_batch_from_empty_exceeds_initial_capacity() {
        // Regression: bulk append into an empty_with_schema() batch must not
        // spin when n >> initial capacity (which is 0).
        let schema = make_u64_i64_schema();
        let rows: Vec<(u64, i64, i64)> = (1u64..=200).map(|i| (i, 1i64, (i * 10) as i64)).collect();
        let src = make_batch(&schema, &rows);

        let mut dst = Batch::empty_with_schema(&schema);

        dst.append_batch(&src, 0, src.count);

        assert_eq!(dst.count, 200);
        for i in 0..200usize {
            assert_eq!(dst.get_pk(i), (i + 1) as u128);
        }
    }

    #[test]
    fn batch_region_access() {
        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(10, 1, 100)]);

        // Schema has 2 columns: PK (U64) + payload (I64)
        // Regions: pk(0), weight(1), null(2), col0(3), blob(4)
        assert_eq!(batch.num_regions_total(), 5);
        assert_eq!(batch.region_size(0), 8); // pk: 1 row * 8 bytes (U64 PK)
        assert_eq!(batch.region_size(3), 8); // col0: 1 row * 8 bytes
        assert!(!batch.region_ptr(0).is_null());
    }

    #[test]
    fn batch_clear() {
        let schema = make_u64_i64_schema();
        let mut batch = make_batch(&schema, &[(10, 1, 100)]);
        assert_eq!(batch.count, 1);

        batch.clear();
        assert_eq!(batch.count, 0);
        assert!(batch.is_sorted());
        assert!(batch.is_consolidated());
    }

    /// Schema matching reduce output: U128 PK (pk_index=0) + I64 group_val + I64 agg_val
    fn make_reduce_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0), // U128 PK
                SchemaColumn::new(type_code::I64, 0),  // I64 group_val
                SchemaColumn::new(type_code::I64, 0),  // I64 agg_val
            ],
            &[0],
        )
    }

    /// Build a 3-column Batch from (pk, weight, group_val, agg_val) tuples.
    /// PK is a single U128 column.
    fn make_reduce_batch(rows: &[(u128, i64, i64, i64)]) -> Batch {
        let schema = make_reduce_schema();
        let n = rows.len();
        let mut b = Batch::with_schema(schema, n.max(1));

        for &(pk, w, gv, av) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &gv.to_le_bytes());
            b.extend_col(1, &av.to_le_bytes());
            b.count += 1;
        }

        b.certify_layout(Layout::Sorted, &schema);
        b
    }

    /// Reproduce the reduce output pattern: insertion + retraction across ticks.
    /// After consolidation, only the LAST tick's aggregate should survive.
    #[test]
    fn test_reduce_output_consolidation_3col() {
        let schema = make_reduce_schema();

        // Tick 1: insert (PK=0, group=0, sum=5000, w=+1)
        let run1 = make_reduce_batch(&[(0, 1, 0, 5000)]);
        // Tick 2: retract old, insert new
        let run2 = make_reduce_batch(&[
            (0, -1, 0, 5000), // retract sum=5000
            (0, 1, 0, 10000), // insert sum=10000
        ]);
        // Tick 3: retract old, insert new
        let run3 = make_reduce_batch(&[
            (0, -1, 0, 10000), // retract sum=10000
            (0, 1, 0, 15000),  // insert sum=15000
        ]);

        let batches: Vec<SortedMemBatch> = vec![
            run1.as_sorted_mem_batch(&schema).expect("test batch is sorted"),
            run2.as_sorted_mem_batch(&schema).expect("test batch is sorted"),
            run3.as_sorted_mem_batch(&schema).expect("test batch is sorted"),
        ];

        let consolidated = consolidate_batches(&batches, &schema);

        // After consolidation: only (PK=0, group=0, sum=15000, w=+1) should remain
        assert_eq!(
            consolidated.count, 1,
            "expected 1 row after consolidation, got {}",
            consolidated.count
        );
        assert_eq!(consolidated.get_pk(0), 0);
        assert_eq!(consolidated.get_weight(0), 1);
        // Check agg_val (payload column 1) = 15000
        let agg_bytes = consolidated.get_col_ptr(0, 1, 8);
        let agg_val = i64::from_le_bytes(agg_bytes.try_into().unwrap());
        assert_eq!(agg_val, 15000, "expected agg_val=15000, got {agg_val}");
    }

    // ── append_row_simple tests ──────────────────────────────────────────

    fn make_schema_cols(cols: &[(u8, u8)], pk_index: u32) -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
        for (i, &(tc, nullable)) in cols.iter().enumerate() {
            columns[i] = SchemaColumn::new(tc, nullable);
        }
        SchemaDescriptor::new(&columns[..cols.len()], &[pk_index])
    }

    fn decode_str(batch: &Batch, row: usize, payload_col: usize) -> Vec<u8> {
        let raw = batch.get_col_ptr(row, payload_col, 16);
        let st: [u8; 16] = raw.try_into().unwrap();
        crate::schema::try_decode_german_string(&st, &batch.blob).unwrap()
    }

    #[test]
    fn test_append_row_simple_nullable_string() {
        // Schema: U64(pk=0), STRING(nullable)
        let schema = make_schema_cols(&[(type_code::U64, 0), (type_code::STRING, 1)], 0);
        let mut batch = Batch::with_schema(schema, 4);

        // Row 0: non-null string "not null"
        let s = b"not null";
        let lo = [0i64]; // not used for STRING
        let hi = [0u64];
        let ptrs = [s.as_ptr()];
        let lens = [s.len() as u32];
        unsafe {
            batch.append_row_simple(1, 1, 0, &lo, &hi, &ptrs, &lens);
        }

        // Row 1: null string (null_word bit 0 set)
        let null_ptr: *const u8 = std::ptr::null();
        let ptrs2 = [null_ptr];
        let lens2 = [0u32];
        unsafe {
            batch.append_row_simple(2, 1, 1, &[0i64], &[0u64], &ptrs2, &lens2);
        }

        assert_eq!(batch.count, 2);
        // Row 0: string decodes correctly
        assert_eq!(decode_str(&batch, 0, 0), b"not null");
        // Row 0: not null
        assert_eq!(batch.get_null_word(0) & 1, 0);
        // Row 1: null bit set
        assert_eq!(batch.get_null_word(1) & 1, 1);
        // Row 1: col data is zeroed
        let raw = batch.get_col_ptr(1, 0, 16);
        assert!(raw.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_append_row_simple_multi_string() {
        // Schema: U64(pk=0), STRING(name), STRING(desc)
        let schema = make_schema_cols(
            &[(type_code::U64, 0), (type_code::STRING, 0), (type_code::STRING, 0)],
            0,
        );
        let mut batch = Batch::with_schema(schema, 4);

        let cases: &[(&[u8], &[u8])] = &[
            (b"Alice", b"short"),
            (b"Bob has a long name!", b"Also quite a long description"),
            (b"", b"nonempty"),
            (b"mix", b"another long one for blob storage"),
        ];

        for (pk, (name, desc)) in cases.iter().enumerate() {
            let lo = [0i64, 0i64];
            let hi = [0u64, 0u64];
            let ptrs = [name.as_ptr(), desc.as_ptr()];
            let lens = [name.len() as u32, desc.len() as u32];
            unsafe {
                batch.append_row_simple(pk as u128, 1, 0, &lo, &hi, &ptrs, &lens);
            }
        }

        assert_eq!(batch.count, 4);
        for (i, (name, desc)) in cases.iter().enumerate() {
            assert_eq!(decode_str(&batch, i, 0), *name, "row {i} name mismatch");
            assert_eq!(decode_str(&batch, i, 1), *desc, "row {i} desc mismatch");
        }
    }

    #[test]
    fn test_append_row_simple_all_types() {
        // Schema: U64(pk=0), then one of each remaining type
        // Payload cols (pi): U8(0), I8(1), U16(2), I16(3), U32(4), I32(5),
        //                     F32(6), U64(7), I64(8), F64(9), STRING(10), U128(11)
        let schema = make_schema_cols(
            &[
                (type_code::U64, 0),    // pk
                (type_code::U8, 0),     // pi 0
                (type_code::I8, 0),     // pi 1
                (type_code::U16, 0),    // pi 2
                (type_code::I16, 0),    // pi 3
                (type_code::U32, 0),    // pi 4
                (type_code::I32, 0),    // pi 5
                (type_code::F32, 0),    // pi 6
                (type_code::U64, 0),    // pi 7
                (type_code::I64, 0),    // pi 8
                (type_code::F64, 0),    // pi 9
                (type_code::STRING, 0), // pi 10
                (type_code::U128, 0),   // pi 11
            ],
            0,
        );
        let mut batch = Batch::with_schema(schema, 1);

        let n = 12;
        let mut lo = vec![0i64; n];
        let mut hi = vec![0u64; n];
        let mut ptrs = vec![std::ptr::null::<u8>(); n];
        let mut lens = vec![0u32; n];

        lo[0] = 42; // U8: 42
        lo[1] = -7; // I8: -7
        lo[2] = 1000; // U16: 1000
        lo[3] = -500; // I16: -500
        lo[4] = 70000; // U32: 70000
        lo[5] = -12345; // I32: -12345
                        // F32: 3.14 → store as f64 bit pattern (float2longlong convention)
        lo[6] = f64::to_bits(3.14f64) as i64;
        lo[7] = 0x1234_5678_9ABC_DEF0u64 as i64; // U64
        lo[8] = -99999; // I64
                        // F64: 2.718281828 → store as bit pattern
        lo[9] = f64::to_bits(2.718281828f64) as i64;
        // STRING: "hello world!"
        let s = b"hello world!";
        ptrs[10] = s.as_ptr();
        lens[10] = s.len() as u32;
        // U128: lo=0xDEADBEEF, hi=0xCAFEBABE
        lo[11] = 0xDEADBEEFu64 as i64;
        hi[11] = 0xCAFEBABE;

        unsafe {
            batch.append_row_simple(100, 1, 0, &lo, &hi, &ptrs, &lens);
        }

        assert_eq!(batch.count, 1);

        // U8
        assert_eq!(batch.get_col_ptr(0, 0, 1), &[42]);
        // I8
        assert_eq!(batch.get_col_ptr(0, 1, 1), &[(-7i8) as u8]);
        // U16
        assert_eq!(batch.get_col_ptr(0, 2, 2), &1000u16.to_le_bytes());
        // I16
        assert_eq!(batch.get_col_ptr(0, 3, 2), &(-500i16).to_le_bytes());
        // U32
        assert_eq!(batch.get_col_ptr(0, 4, 4), &70000u32.to_le_bytes());
        // I32
        assert_eq!(batch.get_col_ptr(0, 5, 4), &(-12345i32).to_le_bytes());
        // F32: 3.14f64 as f32
        let f32_bytes = batch.get_col_ptr(0, 6, 4);
        let f32_val = f32::from_le_bytes(f32_bytes.try_into().unwrap());
        assert!((f32_val - 3.14f32).abs() < 1e-5, "f32: {f32_val}");
        // U64
        assert_eq!(batch.get_col_ptr(0, 7, 8), &0x1234_5678_9ABC_DEF0u64.to_le_bytes());
        // I64
        assert_eq!(batch.get_col_ptr(0, 8, 8), &(-99999i64).to_le_bytes());
        // F64
        let f64_bytes = batch.get_col_ptr(0, 9, 8);
        let f64_val = f64::from_le_bytes(f64_bytes.try_into().unwrap());
        assert!((f64_val - 2.718281828).abs() < 1e-9, "f64: {f64_val}");
        // STRING
        assert_eq!(decode_str(&batch, 0, 10), b"hello world!");
        // U128
        let u128_bytes = batch.get_col_ptr(0, 11, 16);
        let u128_lo = u64::from_le_bytes(u128_bytes[0..8].try_into().unwrap());
        let u128_hi = u64::from_le_bytes(u128_bytes[8..16].try_into().unwrap());
        assert_eq!(u128_lo, 0xDEADBEEF);
        assert_eq!(u128_hi, 0xCAFEBABE);
    }

    /// An I128 payload column (a cross-sign `_join_pk` surfaced into a payload
    /// slot) round-trips through the lo/hi split in `append_row_simple`. Guards
    /// against the pre-fix `_` arm, which sliced `[u8;8][..16]` (OOB panic).
    #[test]
    fn test_append_row_simple_i128_payload() {
        let schema = make_schema_cols(
            &[
                (type_code::U64, 0),  // pk
                (type_code::I128, 0), // pi 0: I128 payload
            ],
            0,
        );
        let mut batch = Batch::with_schema(schema, 1);
        // A negative value with bits in both halves: bit 127 set. The I128 column
        // is the first (and only) payload column, so lo/hi are indexed at pi = 0.
        let v: i128 = -0x0123_4567_89AB_CDEF_1122_3344_5566_7788;
        let bits = v as u128;
        let lo = vec![(bits as u64) as i64];
        let hi = vec![(bits >> 64) as u64];
        let ptrs = vec![std::ptr::null::<u8>(); 1];
        let lens = vec![0u32; 1];
        unsafe {
            batch.append_row_simple(7, 1, 0, &lo, &hi, &ptrs, &lens);
        }
        assert_eq!(batch.count, 1);
        let stored = batch.get_col_ptr(0, 0, 16);
        let got = i128::from_le_bytes(stored.try_into().unwrap());
        assert_eq!(got, v, "I128 payload must round-trip through the lo/hi split");
    }

    /// Verify that `append_row` substitutes an empty string when the
    /// declared length would read past the end of `blob_src`. This matches
    /// `relocate_string_cell` and prevents silent corruption from emitting
    /// unrelated bytes from the start of the blob.
    #[test]
    fn test_append_row_blob_length_header() {
        let schema = make_schema_cols(
            &[
                (type_code::U64, 0),    // PK
                (type_code::STRING, 0), // STRING payload
            ],
            0,
        );
        let mut batch = Batch::with_schema(schema, 1);

        // Build a German String struct with length=20 but only 5 blob bytes available.
        // This triggers the malformed-wire-data fallback branch in append_row.
        let mut gs_struct = [0u8; 16];
        gs_struct[0..4].copy_from_slice(&20u32.to_le_bytes()); // declared length = 20
        gs_struct[4..8].copy_from_slice(&0u32.to_le_bytes()); // prefix bytes = 0
        gs_struct[8..16].copy_from_slice(&0u64.to_le_bytes()); // blob offset = 0

        let blob_src = b"hello"; // only 5 bytes — out of bounds for length=20

        let col_ptrs = [gs_struct.as_ptr()];
        let col_sizes = [16u32];
        unsafe {
            batch.append_row(1, 1, 0, &col_ptrs, &col_sizes, blob_src);
        }

        // The stored German String header must have length=0 (empty-string
        // fallback), not the declared length=20 nor a truncated length=5.
        // Emitting an empty string is the only safe response to malformed
        // wire data; anything else silently corrupts the row.
        let stored_gs = batch.get_col_ptr(0, 0, 16);
        let stored_len = u32::from_le_bytes(stored_gs[0..4].try_into().unwrap());
        assert_eq!(stored_len, 0, "malformed long-string should fall back to empty");

        // No bytes copied into the blob arena.
        assert!(batch.blob.is_empty());
    }

    /// Verify that `find_positive_payload_row` locates the row with positive
    /// net weight, skipping rows whose (PK, payload) is cancelled out.
    #[test]
    fn test_find_positive_payload_row() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Run 0: INSERT (PK=10, val=100, weight=+1)
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        // Run 1: UPDATE delta — retract val=100, insert val=200
        let b2 = make_batch(&schema, &[(10, -1, 100), (10, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        // Net weights: val=100 → 1-1=0, val=200 → 1 (positive)
        // OPK bytes for a U64 PK are the value's big-endian bytes.
        let found = mt.find_positive_payload_row_bytes(&10u64.to_be_bytes());
        assert!(found, "should find a live row");
        assert!(mt.found.is_some());

        // The found row should have payload = 200
        let (run, row) = mt.found_entry().expect("a live row was found");
        let val = i64::from_le_bytes(run.get_col_ptr(row, 0, 8).try_into().unwrap());
        assert_eq!(
            val, 200,
            "found row should be the live val=200 row, not the retracted val=100"
        );

        // PK with no rows at all → not found
        let found2 = mt.find_positive_payload_row_bytes(&99u64.to_be_bytes());
        assert!(!found2);
        assert!(mt.found.is_none());
    }

    /// PK matches exist but every payload group nets to zero across runs.
    /// Distinct from the "PK absent" case: we enter the iteration and call
    /// find_weight_for_row, but every candidate's net weight is 0.
    #[test]
    fn test_find_positive_payload_row_all_cancelled() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        mt.upsert_sorted_batch(make_batch(&schema, &[(42, 1, 100)])).unwrap();
        mt.upsert_sorted_batch(make_batch(&schema, &[(42, 1, 200)])).unwrap();
        mt.upsert_sorted_batch(make_batch(&schema, &[(42, -1, 100)])).unwrap();
        mt.upsert_sorted_batch(make_batch(&schema, &[(42, -1, 200)])).unwrap();

        assert!(!mt.find_positive_payload_row_bytes(&42u64.to_be_bytes()));
        assert!(mt.found.is_none());
    }

    /// One payload for a PK spread across three runs as (+1), (-1), (+1) —
    /// net +1 — interleaved with filler PKs. Exercises the single-pass
    /// cross-run grouping: the live group's members live in non-adjacent runs,
    /// and the middle retraction must be summed into the group, not treated as
    /// a separate cancelled payload. Cross-checks the per-group net against the
    /// `lookup_pk_bytes` PK aggregate (equal here, since PK 10 has one payload).
    #[test]
    fn test_find_positive_payload_row_cross_run_grouping() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Run 0: filler PK 5, then PK 10 payload 100 (+1)
        mt.upsert_sorted_batch(make_batch(&schema, &[(5, 1, 50), (10, 1, 100)]))
            .unwrap();
        // Run 1: PK 10 payload 100 (-1), filler PK 20
        mt.upsert_sorted_batch(make_batch(&schema, &[(10, -1, 100), (20, 1, 200)]))
            .unwrap();
        // Run 2: PK 10 payload 100 (+1), filler PK 30
        mt.upsert_sorted_batch(make_batch(&schema, &[(10, 1, 100), (30, 1, 300)]))
            .unwrap();
        assert_eq!(mt.runs.len(), 3, "three un-consolidated runs");

        let pk10 = 10u64.to_be_bytes();
        let found = mt.find_positive_payload_row_bytes(&pk10);
        assert!(found, "payload 100 nets +1 across the three runs");
        assert!(mt.found.is_some());

        // Found row decodes the live payload (100).
        let (run, row) = mt.found_entry().expect("a live row was found");
        let val = i64::from_le_bytes(run.get_col_ptr(row, 0, 8).try_into().unwrap());
        assert_eq!(val, 100, "found row should decode the live payload");

        // Cross-check: PK 10 has a single payload, so its PK aggregate equals
        // the winning group's net weight (+1). (Call after the found-row reads,
        // since lookup_pk_bytes also mutates found.)
        let (w, agg_found, row_count) = mt.lookup_pk_bytes(&pk10);
        assert_eq!(w, 1, "PK aggregate matches the winning group's net (+1)");
        assert!(agg_found);
        assert_eq!(row_count, 3, "three rows across runs match PK 10");
    }

    /// Found-index contract: after a hit, the `found` (run, row) indices
    /// must address the row of the winning group — even when an *earlier* run
    /// holds a fully-cancelled payload, so the winning group's first member is
    /// in a later run (found.0 != 0). Guards that `found_entry` addresses the
    /// live row, not the cancelled one.
    #[test]
    fn test_find_positive_payload_row_found_index_validity() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Run 0: PK 10 payload 100 (+1)  ── cancelled by run 1
        mt.upsert_sorted_batch(make_batch(&schema, &[(10, 1, 100)])).unwrap();
        // Run 1: PK 10 payload 100 (-1)  ── group {100} nets 0
        mt.upsert_sorted_batch(make_batch(&schema, &[(10, -1, 100)])).unwrap();
        // Run 2: PK 10 payload 200 (+1)  ── the lone live group
        mt.upsert_sorted_batch(make_batch(&schema, &[(10, 1, 200)])).unwrap();

        let pk10 = 10u64.to_be_bytes();
        assert!(mt.find_positive_payload_row_bytes(&pk10));
        assert_eq!(
            mt.found,
            Some((2, 0)),
            "winning group's first member is in run 2 at row 0"
        );

        // Payload column is non-null in the fixture, so the null word is 0 and
        // the decoded value is the live payload (200), not the cancelled 100.
        let (run, row) = mt.found_entry().expect("a live row was found");
        assert_eq!(run.get_null_word(row), 0);
        let val = i64::from_le_bytes(run.get_col_ptr(row, 0, 8).try_into().unwrap());
        assert_eq!(val, 200);
    }

    /// Defensive structural guard (synthetic): two *distinct* payloads for one
    /// PK that both net positive, seeded directly via `upsert_sorted_batch`.
    /// This state is unreachable through the unique-PK DML path (which keeps at
    /// most one live payload per PK), so this is NOT a regression guard for a
    /// real divergence — it only pins that the rewrite returns *a* positive
    /// group's payload and is internally consistent (the returned payload's own
    /// net weight is strictly positive).
    #[test]
    fn test_find_positive_payload_row_multi_positive_structural() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Two distinct payloads for PK 10, each net +1.
        mt.upsert_sorted_batch(make_batch(&schema, &[(10, 1, 100)])).unwrap();
        mt.upsert_sorted_batch(make_batch(&schema, &[(10, 1, 200)])).unwrap();

        let pk10 = 10u64.to_be_bytes();
        assert!(mt.find_positive_payload_row_bytes(&pk10));
        assert!(mt.found.is_some());

        // Returned row decodes one of the two positive payloads...
        let (run, row) = mt.found_entry().expect("a live row was found");
        let val = i64::from_le_bytes(run.get_col_ptr(row, 0, 8).try_into().unwrap());
        assert!(val == 100 || val == 200, "found a positive group's payload");

        // ...and that payload's group genuinely nets strictly positive (internal
        // consistency: the found row is not a cancelled or zero-net group).
        let ref_batch = make_batch(&schema, &[(10, 1, val)]);
        assert!(mt.find_weight_for_row_bytes(&pk10, &ref_batch, 0) > 0);
    }

    /// Build a run from `(pk, weight, payload)` triples in any order, sorting by
    /// PK first so the run's binary-search invariant holds.
    fn make_run(schema: &SchemaDescriptor, mut rows: Vec<(u64, i64, i64)>) -> Batch {
        rows.sort_by_key(|&(pk, _, _)| pk);
        make_batch(schema, &rows)
    }

    /// Construct a memtable with `r` runs in which one hot PK has `c` candidate
    /// rows spread as `c/2` cancelling `(+1,-1)` pairs across the runs (so every
    /// payload group nets 0 and a lookup scans all `c` candidates — the
    /// full-scan worst case). Each run also carries `filler` distinct PKs below
    /// the hot PK plus a sentinel above it, so the per-run binary search for the
    /// hot PK runs fully even in runs holding no hot row (modelling the re-scan
    /// visiting every run). `c` must be even and `c ≤ 2r`; `r` stays below the
    /// inline-consolidate threshold so the runs are not merged.
    fn build_bench_memtable(r: usize, c: usize, filler: usize) -> MemTable {
        assert!(c.is_multiple_of(2), "bench uses cancelling pairs; C must be even");
        assert!(c <= 2 * r, "each batch adds at most a +/- pair per PK");
        assert!(
            (2..INLINE_CONSOLIDATE_THRESHOLD).contains(&r),
            "R below merge threshold"
        );
        const HOT: u64 = 1_000_000;
        const SENTINEL_HI: u64 = HOT + 1; // keeps HOT inside every run's [min,max]

        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 16 << 20);

        let mut run_rows: Vec<Vec<(u64, i64, i64)>> = (0..r)
            .map(|ri| {
                let mut v: Vec<(u64, i64, i64)> = (0..filler as u64)
                    .map(|f| (f, 1i64, ri as i64 * 100 + f as i64))
                    .collect();
                v.push((SENTINEL_HI, 1, 7));
                v
            })
            .collect();

        // Distinct payload per pair → each pair is one zero-net group; the two
        // halves land in different runs, so no run holds a duplicate (PK,payload).
        for j in 0..c / 2 {
            let payload = 5000 + j as i64;
            run_rows[(2 * j) % r].push((HOT, 1, payload));
            run_rows[(2 * j + 1) % r].push((HOT, -1, payload));
        }

        for rows in run_rows {
            mt.upsert_sorted_batch(make_run(&schema, rows)).unwrap();
        }
        assert_eq!(mt.runs.len(), r, "runs must not inline-consolidate below threshold");
        mt
    }

    /// Micro-benchmark: pre-rewrite per-candidate re-scan vs single-pass
    /// grouping. Ignored by default; run with:
    ///
    /// ```text
    /// cargo test -p gnitz-engine --release memtable_find_positive_payload_row_bench \
    ///     -- --ignored --nocapture --test-threads=1
    /// ```
    ///
    /// Sweeps `C` independently of `R`. The headline `C = 2` is one
    /// un-consolidated UPDATE delta (a retract+insert pair) over
    /// `R ∈ {2,4,8,15}` — the gap there tracks `(1+C)` in re-scan count. The
    /// `C ≈ R` points stress a key re-updated on nearly every un-consolidated
    /// run; `INLINE_CONSOLIDATE_THRESHOLD` caps `R` at 15, so `C` cannot exceed
    /// `2R` (~30) before a merge folds the cancelling pairs.
    #[test]
    #[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
    fn memtable_find_positive_payload_row_bench() {
        use std::time::Instant;
        const ITERS: usize = 1_000_000;
        const FILLER: usize = 64;
        let hot = 1_000_000u64.to_be_bytes();

        let points: &[(usize, usize, &str)] = &[
            (2, 2, "headline C=2"),
            (4, 2, "headline C=2"),
            (8, 2, "headline C=2"),
            (15, 2, "headline C=2"),
            (8, 8, "stress C≈R"),
            (15, 14, "stress C≈R"),
        ];

        println!("\nfind_positive_payload_row_bytes — re-scan baseline vs single-pass");
        println!("  (all-cancelled full-scan worst case; {FILLER} filler PKs/run, {ITERS} iters)");
        println!(
            "  {:>3} {:>3}  {:>13}  {:>13}  {:>8}  point",
            "R", "C", "baseline ns", "single ns", "speedup"
        );
        for &(r, c, label) in points {
            let mut mt = build_bench_memtable(r, c, FILLER);

            // Both algorithms select the same result (false here); pin it once.
            assert_eq!(
                mt.find_positive_payload_row_rescan_baseline(&hot),
                mt.find_positive_payload_row_bytes(&hot),
                "baseline and single-pass must agree (R={r}, C={c})",
            );

            for _ in 0..2_000 {
                std::hint::black_box(mt.find_positive_payload_row_rescan_baseline(&hot));
                std::hint::black_box(mt.find_positive_payload_row_bytes(&hot));
            }

            let t0 = Instant::now();
            for _ in 0..ITERS {
                std::hint::black_box(mt.find_positive_payload_row_rescan_baseline(&hot));
            }
            let base_ns = t0.elapsed().as_nanos() as f64 / ITERS as f64;

            let t1 = Instant::now();
            for _ in 0..ITERS {
                std::hint::black_box(mt.find_positive_payload_row_bytes(&hot));
            }
            let single_ns = t1.elapsed().as_nanos() as f64 / ITERS as f64;

            println!(
                "  {r:>3} {c:>3}  {base_ns:>13.2}  {single_ns:>13.2}  {:>7.2}x  {label}",
                base_ns / single_ns,
            );
        }
    }

    #[test]
    fn test_inline_consolidate_basic() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Push 15 batches — no consolidation yet
        for i in 0..15u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert_eq!(mt.runs.len(), 15);

        // 16th batch triggers consolidation
        let b16 = make_batch(&schema, &[(16, 1, 1600)]);
        mt.upsert_sorted_batch(b16).unwrap();
        assert_eq!(mt.runs.len(), 1);

        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 16);
        assert_eq!(snap.get_pk(0), 1);
        assert_eq!(snap.get_pk(15), 16);
    }

    #[test]
    fn test_inline_consolidate_ghost_elimination() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // 14 distinct insertions
        for i in 0..14u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        // Retract PK 1
        let retract = make_batch(&schema, &[(1, -1, 100)]);
        mt.upsert_sorted_batch(retract).unwrap();
        // 16th batch — triggers consolidation
        let b16 = make_batch(&schema, &[(15, 1, 1500)]);
        mt.upsert_sorted_batch(b16).unwrap();

        assert_eq!(mt.runs.len(), 1);
        let snap = mt.consolidate_for_flush();
        // PK 1 cancelled out: 14 - 1 + 1 new = 14 survive
        assert_eq!(snap.count, 14);
        // PK 1 should be gone, first row is PK 2
        assert_eq!(snap.get_pk(0), 2);
    }

    #[test]
    fn test_inline_consolidate_all_cancelled() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // 8 insertions + 8 matching retractions = 16 batches
        for i in 0..8u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        for i in 0..8u64 {
            let b = make_batch(&schema, &[(i + 1, -1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }

        // All cancelled: runs should be empty
        assert_eq!(mt.runs.len(), 0);
        assert!(mt.is_empty());
        assert_eq!(mt.total_row_count(), 0);

        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 0);
    }

    #[test]
    fn test_inline_consolidate_below_threshold() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        for i in 0..15u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        // 15 < 16, no consolidation
        assert_eq!(mt.runs.len(), 15);

        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 15);
    }

    #[test]
    fn test_inline_consolidate_repeated() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // First 16 → consolidation fires → 1 run
        for i in 0..16u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert_eq!(mt.runs.len(), 1);

        // 14 more → 1 + 14 = 15 runs (no consolidation yet)
        for i in 16..30u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert_eq!(mt.runs.len(), 15);

        // One more → 16 runs → consolidation fires again → 1 run
        let b31 = make_batch(&schema, &[(31, 1, 3100)]);
        mt.upsert_sorted_batch(b31).unwrap();
        assert_eq!(mt.runs.len(), 1);

        let snap = mt.consolidate_for_flush();
        assert_eq!(snap.count, 31);
    }

    #[test]
    fn test_inline_consolidate_should_flush() {
        let schema = make_u64_i64_schema();
        // Each 1-row batch = 32 bytes (U64 PK=8, weight=8, null=8, col=8).
        // max_bytes = 560 → threshold = 420.
        // After 14 insertions: runs_bytes = 448 > 420 → should_flush true.
        // After 2 retractions (total 16 batches): consolidation fires,
        // net 12 rows = 384 < 420 → should_flush false.
        let mut mt = MemTable::new(schema, 560);

        // 8 insertions
        for i in 0..8u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }

        // 6 more insertions → 14 runs, runs_bytes = 448 > 420
        for i in 8..14u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert!(mt.should_flush(), "gross bytes should exceed threshold");

        // Retract PKs 1-2 → 16 batches total, triggers consolidation
        // Net: 12 rows survive (PKs 3-14)
        let r1 = make_batch(&schema, &[(1, -1, 100)]);
        let r2 = make_batch(&schema, &[(2, -1, 200)]);
        mt.upsert_sorted_batch(r1).unwrap();
        mt.upsert_sorted_batch(r2).unwrap();

        // After consolidation: net 12 rows = 384 < 420
        assert!(!mt.should_flush(), "net state should be under threshold");
    }

    #[test]
    fn test_inline_consolidate_found_cleared() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        for i in 0..15u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        // lookup_pk_bytes sets found
        let (w, found, _) = mt.lookup_pk_bytes(&5u64.to_be_bytes());
        assert_eq!(w, 1);
        assert!(found);
        assert!(mt.found.is_some());

        // 16th batch triggers consolidation → found cleared
        let b16 = make_batch(&schema, &[(16, 1, 1600)]);
        mt.upsert_sorted_batch(b16).unwrap();
        assert!(mt.found.is_none());
    }

    /// Bug 5: relocate_string_cell must not panic when blob offset is past end.
    #[test]
    fn test_relocate_string_cell_out_of_bounds() {
        // Build a 16-byte German string struct for a long string (length > 12)
        // with an out-of-bounds blob offset.
        let length: u32 = 20;
        let prefix = [b'A'; 4];
        let bad_offset: u64 = 9999; // way past any blob
        let mut src_struct = [0u8; 16];
        src_struct[0..4].copy_from_slice(&length.to_le_bytes());
        src_struct[4..8].copy_from_slice(&prefix);
        src_struct[8..16].copy_from_slice(&bad_offset.to_le_bytes());

        let src_blob: &[u8] = &[0u8; 10]; // only 10 bytes, offset 9999 is OOB
        let mut dst = [0u8; 16];
        let mut dst_blob = Vec::new();

        // Must not panic
        relocate_string_cell(&src_struct, src_blob, &mut dst, &mut dst_blob);

        // The corrupted string should have length set to 0
        let out_len = u32::from_le_bytes(dst[0..4].try_into().unwrap());
        assert_eq!(out_len, 0, "corrupted string should be zero-length");
        assert!(dst_blob.is_empty(), "no blob data should have been copied");
    }

    /// Bug 5: append_row_from_source must not panic when blob offset is invalid.
    #[test]
    fn test_append_row_from_source_corrupted_blob() {
        use crate::schema::{type_code, BlobCache, SchemaColumn};

        // Schema: col0 = PK (U64), col1 = STRING
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );

        // Build a source Batch with a STRING column containing a bad blob offset.
        let mut src = Batch::with_schema(schema, 1);

        // German string struct: length=20, prefix="ABCD", offset=9999 (OOB)
        let length: u32 = 20;
        let prefix = [b'A', b'B', b'C', b'D'];
        let bad_offset: u64 = 9999;
        let mut str_struct = [0u8; 16];
        str_struct[0..4].copy_from_slice(&length.to_le_bytes());
        str_struct[4..8].copy_from_slice(&prefix);
        str_struct[8..16].copy_from_slice(&bad_offset.to_le_bytes());

        src.ensure_row_capacity();
        src.extend_pk(42u128);
        src.extend_weight(&1i64.to_le_bytes());
        src.extend_null_bmp(&0u64.to_le_bytes());
        src.extend_col(0, &str_struct);
        src.blob = vec![0u8; 10]; // only 10 bytes
        src.count = 1;

        // Build destination batch and call append_row_from_source
        let mut dst = Batch::with_schema(schema, 1);
        let mut blob_cache: BlobCache = BlobCache::default();

        // Must not panic
        dst.append_row_from_source(42u128, 1, &src, 0, Some(&mut blob_cache));

        assert_eq!(dst.count, 1);
        // The corrupted string should have length 0
        let out_len = u32::from_le_bytes(dst.col_data(0)[0..4].try_into().unwrap());
        assert_eq!(out_len, 0, "corrupted blob reference should produce zero-length string");
    }

    #[test]
    fn drop_recycles_buffers() {
        use crate::storage::batch_pool::{acquire_buf, recycle_buf};
        // Drain pool first.
        while acquire_buf().capacity() > 0 {}

        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let data_cap = batch.data_capacity();
        assert!(data_cap > 0);
        drop(batch);

        // Pool should now have at least one warm buffer (data buf).
        // May also have blob buf (if non-zero cap). Drain and check.
        let mut found = false;
        let mut drained = Vec::new();
        loop {
            let buf = acquire_buf();
            if buf.capacity() == 0 {
                break;
            }
            if buf.capacity() >= data_cap {
                found = true;
            }
            drained.push(buf);
        }
        assert!(found, "pool should contain the recycled data buffer");
        for buf in drained {
            recycle_buf(buf);
        }
    }

    #[test]
    fn clone_drops_independently() {
        use crate::storage::batch_pool::acquire_buf;
        while acquire_buf().capacity() > 0 {}

        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(1, 1, 10)]);
        let cloned = batch.clone();
        drop(batch);
        drop(cloned);

        // Both data + blob buffers from two batches should be in pool.
        // Original had data + blob, clone has data + blob.
        // Some may be zero-cap (empty blob), so just verify at least 2 buffers.
        let mut count = 0;
        while acquire_buf().capacity() > 0 {
            count += 1;
        }
        assert!(count >= 2, "expected at least 2 recycled buffers, got {count}");
    }

    #[test]
    fn empty_batch_drop_is_noop() {
        use crate::storage::batch_pool::acquire_buf;
        while acquire_buf().capacity() > 0 {}

        let batch = Batch::empty(2, 16);
        assert_eq!(batch.data_capacity(), 0);
        drop(batch);

        // Zero-capacity buffers should NOT be pooled.
        assert_eq!(acquire_buf().capacity(), 0, "empty batch should not pollute pool");
    }
}
