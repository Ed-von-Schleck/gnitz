//! MemTable: manages sorted runs, Bloom filter, consolidation cache,
//! PK lookups, and shard flush.

use std::cmp::Ordering;
use std::ffi::CStr;
use std::sync::Arc;

use super::batch::{Batch, write_to_batch, ConsolidatedBatch};
use super::bloom::BloomFilter;
use super::columnar;
use super::error::StorageError;
use crate::schema::SchemaDescriptor;
use super::merge::{self, SortedMemBatch};
use super::shard_file;

// Accessible to the tests submodule (private items are visible to descendants).
#[cfg(test)]
use super::batch::relocate_string_cell;

/// Maximum sorted runs before inline consolidation merges them into one.
const INLINE_CONSOLIDATE_THRESHOLD: usize = 16;

/// Merge N sorted MemBatch views into a single consolidated Batch.
fn consolidate_batches(
    batches: &[SortedMemBatch],
    schema: &SchemaDescriptor,
) -> Batch {
    if batches.is_empty() {
        return Batch::empty_with_schema(schema);
    }

    let total_rows: usize = batches.iter().map(|b| b.count).sum();
    let total_blob: usize = batches.iter().map(|b| b.blob.len()).sum();
    if total_rows == 0 {
        return Batch::empty_with_schema(schema);
    }

    let mut result = write_to_batch(schema, total_rows, total_blob, |writer| {
        merge::merge_batches(batches, schema, writer);
    });
    result.sorted = true;
    result.consolidated = true;
    result
}


pub struct MemTable {
    runs: Vec<Batch>,
    bloom: BloomFilter,
    schema: SchemaDescriptor,
    max_bytes: usize,
    total_row_count: usize,
    runs_bytes: usize,
    cached_consolidated: Option<Arc<Batch>>,
    // Last lookup result (set by lookup_pk, read by found_* accessors)
    found_run: usize,
    found_row: usize,
    has_found: bool,
}

impl MemTable {
    pub fn new(schema: SchemaDescriptor, max_bytes: usize) -> Self {
        let capacity = (max_bytes / 40).max(16); // rough estimate
        MemTable {
            runs: Vec::with_capacity(8),
            bloom: BloomFilter::new(capacity as u32),
            schema,
            max_bytes,
            total_row_count: 0,
            runs_bytes: 0,
            cached_consolidated: None,
            found_run: 0,
            found_row: 0,
            has_found: false,
        }
    }

    /// Append a consolidated batch as a new run.
    pub fn upsert_sorted_batch(&mut self, batch: ConsolidatedBatch) -> Result<(), StorageError> {
        let batch = batch.into_inner();
        if batch.count == 0 {
            return Ok(());
        }
        self.check_capacity()?;
        for i in 0..batch.count {
            self.bloom.add(batch.get_pk(i));
        }
        self.total_row_count += batch.count;
        self.runs_bytes += batch.total_bytes();
        self.runs.push(batch);
        self.invalidate_cache();
        self.maybe_inline_consolidate();
        Ok(())
    }

    pub fn may_contain_pk(&self, key: u128) -> bool {
        self.bloom.may_contain(key)
    }

    pub fn should_flush(&self) -> bool {
        self.runs_bytes > self.max_bytes * 3 / 4
    }

    pub fn is_empty(&self) -> bool {
        self.total_row_count == 0
    }

    #[cfg(test)]
    pub fn total_row_count(&self) -> usize {
        self.total_row_count
    }

    fn runs_as_sorted(&self) -> Vec<SortedMemBatch<'_>> {
        self.runs.iter()
            .map(|r| r.as_sorted_mem_batch().expect("MemTable runs are always sorted"))
            .collect()
    }

    /// Get a consolidated snapshot.  Caches the merged result of all runs.
    /// Returns an `Arc<Batch>` — cheap to clone for multiple consumers.
    pub fn get_snapshot(&mut self) -> Arc<Batch> {
        if self.cached_consolidated.is_none() && !self.runs.is_empty() {
            let batches = self.runs_as_sorted();
            let consolidated = consolidate_batches(&batches, &self.schema);
            self.cached_consolidated = Some(Arc::new(consolidated));
        }

        match &self.cached_consolidated {
            Some(arc) => Arc::clone(arc),
            None => Arc::new(Batch::empty_with_schema(&self.schema)),
        }
    }

    /// Look up a PK across all sorted runs.
    ///
    /// Returns `(net_weight, Some((run_idx, row_idx)))` if found, else
    /// `(0, None)`.
    pub fn lookup_pk(&mut self, key: u128) -> (i64, bool) {
        let mut total_w: i64 = 0;
        self.has_found = false;

        for (ri, run) in self.runs.iter().enumerate() {
            if run.count == 0 || key < run.get_pk(0) || key > run.get_pk(run.count - 1) {
                continue;
            }
            let mut lo = run.find_lower_bound(key);
            while lo < run.count && run.get_pk(lo) == key {
                total_w += run.get_weight(lo);
                if !self.has_found {
                    self.found_run = ri;
                    self.found_row = lo;
                    self.has_found = true;
                }
                lo += 1;
            }
        }

        (total_w, self.has_found)
    }

    fn found_entry(&self) -> Option<(&Batch, usize)> {
        if self.has_found && self.found_run < self.runs.len() {
            Some((&self.runs[self.found_run], self.found_row))
        } else {
            None
        }
    }

    pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
        match self.found_entry() {
            Some((run, row)) => run.get_col_ptr(row, payload_col, col_size).as_ptr(),
            None => std::ptr::null(),
        }
    }

    pub fn found_null_word(&self) -> u64 {
        match self.found_entry() {
            Some((run, row)) => run.get_null_word(row),
            None => 0,
        }
    }

    pub fn found_blob_ptr(&self) -> *const u8 {
        match self.found_entry() {
            Some((run, _)) => run.blob.as_ptr(),
            None => std::ptr::null(),
        }
    }

    /// Find the net weight for rows matching both PK and full payload.
    pub fn find_weight_for_row<S: super::columnar::ColumnarSource>(
        &self,
        key: u128,
        ref_source: &S,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        for run in &self.runs {
            if run.count == 0 || key < run.get_pk(0) || key > run.get_pk(run.count - 1) {
                continue;
            }
            let mut lo = run.find_lower_bound(key);
            while lo < run.count && run.get_pk(lo) == key {
                let ord = columnar::compare_rows(
                    &self.schema, run, lo, ref_source, ref_row,
                );
                if ord == Ordering::Equal {
                    total_w += run.get_weight(lo);
                }
                lo += 1;
            }
        }

        total_w
    }

    /// Find the first memtable row whose (PK, payload) has positive net weight
    /// across all runs.  Sets `found_run`/`found_row`/`has_found` on success.
    /// Returns true if such a row was found, false otherwise.
    ///
    /// Used by `Table::retract_pk` to locate the live row for an UPDATE+DELETE
    /// sequence where the old payload has been cancelled but the new payload
    /// is still positive.
    pub fn find_positive_payload_row(&mut self, key: u128) -> bool {
        // Single pass: for each PK-matching row, compute its net weight immediately.
        // find_weight_for_row takes &self, so it can overlap with the iterator's
        // immutable borrow of self.runs. Mutable writes to self.found_* touch
        // disjoint fields and are fine under NLL.
        for (ri, run) in self.runs.iter().enumerate() {
            if run.count == 0 || key < run.get_pk(0) || key > run.get_pk(run.count - 1) {
                continue;
            }
            let mut lo = run.find_lower_bound(key);
            while lo < run.count && run.get_pk(lo) == key {
                let net_w = self.find_weight_for_row(key, &run.as_mem_batch(), lo);
                if net_w > 0 {
                    self.found_run = ri;
                    self.found_row = lo;
                    self.has_found = true;
                    return true;
                }
                lo += 1;
            }
        }
        self.has_found = false;
        false
    }

    /// Consolidate all runs and write to a shard file.
    ///
    /// Returns `Ok(true)` on success, `Ok(false)` when there is nothing to
    /// flush (no file written), or `Err(_)` on shard-write failure.
    pub fn flush(
        &mut self,
        dirfd: libc::c_int,
        basename: &CStr,
        table_id: u32,
        durable: bool,
    ) -> Result<bool, StorageError> {
        let snapshot = self.get_snapshot();
        if snapshot.count == 0 {
            return Ok(false);
        }

        let regions = snapshot.regions();
        let image = shard_file::build_shard_image(
            table_id,
            snapshot.count as u32,
            &regions,
        );
        shard_file::write_shard_at(dirfd, basename, &image, durable)?;
        Ok(true)
    }

    /// Clear all runs, bloom filter, and cache.  Ready for reuse.
    pub fn reset(&mut self) {
        self.runs.clear();
        self.runs_bytes = 0;
        self.total_row_count = 0;
        self.cached_consolidated = None;
        self.has_found = false;
        self.bloom.reset();
    }


    fn invalidate_cache(&mut self) {
        self.cached_consolidated = None;
    }

    /// If the run count has reached the threshold, merge all runs into a
    /// single consolidated run.  Bounds the cost of `get_snapshot()` and
    /// `lookup_pk()`, and eliminates weight-cancelled rows early.
    fn maybe_inline_consolidate(&mut self) {
        if self.runs.len() < INLINE_CONSOLIDATE_THRESHOLD {
            return;
        }
        let batches = self.runs_as_sorted();
        let merged = consolidate_batches(&batches, &self.schema);
        self.runs.clear();
        self.runs_bytes = merged.total_bytes();
        self.total_row_count = merged.count;
        if merged.count > 0 {
            self.runs.push(merged);
        }
        self.has_found = false;
        self.invalidate_cache();
    }

    fn check_capacity(&self) -> Result<(), StorageError> {
        if self.runs_bytes > self.max_bytes {
            return Err(StorageError::Capacity);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

    fn make_u64_i64_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        // Col 0: PK (U64)
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        // Col 1: payload (I64)
        columns[1] = SchemaColumn::new(type_code::I64, 0);
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    /// Build a consolidated Batch from (pk, weight, payload) triples.
    /// Assumes triples are already sorted by pk with no duplicate (pk, payload) pairs.
    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }

        b.sorted = true;
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

        // Snapshot should consolidate: PK 30 has +1 -1 = 0 (ghost)
        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 2); // PK 10 and 20 survive
        assert_eq!(snap.get_pk(0), 10);
        assert_eq!(snap.get_pk(1), 20);
    }

    #[test]
    fn memtable_snapshot_caching() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let s1 = mt.get_snapshot();
        let s2 = mt.get_snapshot();
        // Should be the same Arc (cached)
        assert!(Arc::ptr_eq(&s1, &s2));

        // After new upsert, cache is invalidated
        let b2 = make_batch(&schema, &[(20, 1, 200)]);
        mt.upsert_sorted_batch(b2).unwrap();
        let s3 = mt.get_snapshot();
        assert!(!Arc::ptr_eq(&s1, &s3));
        assert_eq!(s3.count, 2);
    }

    #[test]
    fn memtable_lookup_pk() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
        let b2 = make_batch(&schema, &[(20, 2, 200)]); // PK 20 appears in two runs
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        let (w, found) = mt.lookup_pk(20);
        assert_eq!(w, 3); // 1 + 2
        assert!(found);

        let (w, found) = mt.lookup_pk(99);
        assert_eq!(w, 0);
        assert!(!found);
    }

    #[test]
    fn memtable_bloom() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();

        assert!(mt.may_contain_pk(10));
        assert!(mt.may_contain_pk(20));
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

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 0);
    }

    #[test]
    fn snapshot_survives_reset() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 1);

        mt.reset();
        // Snapshot still valid (Arc keeps data alive)
        assert_eq!(snap.count, 1);
        assert_eq!(snap.get_pk(0), 10);
    }

    #[test]
    fn memtable_capacity_error() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 64); // tiny capacity

        // First upsert fills past max_bytes (4 rows × 40 bytes = 160 > 64)
        let b1 = make_batch(
            &schema,
            &[(10, 1, 100), (20, 1, 200), (30, 1, 300), (40, 1, 400)],
        );
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

        // Search for PK 10, payload 100 — should find weight 1
        let ref_batch = make_batch(&schema, &[(10, 1, 100)]);
        let ref_mb = ref_batch.as_mem_batch();
        let w = mt.find_weight_for_row(10, &ref_mb, 0);
        assert_eq!(w, 1);

        // Search for PK 10, payload 200 — should find weight 1
        let ref_batch2 = make_batch(&schema, &[(10, 1, 200)]);
        let ref_mb2 = ref_batch2.as_mem_batch();
        let w2 = mt.find_weight_for_row(10, &ref_mb2, 0);
        assert_eq!(w2, 1);

        // Search for PK 10, payload 999 — should find weight 0
        let ref_batch3 = make_batch(&schema, &[(10, 1, 999)]);
        let ref_mb3 = ref_batch3.as_mem_batch();
        let w3 = mt.find_weight_for_row(10, &ref_mb3, 0);
        assert_eq!(w3, 0);
    }

    #[test]
    fn memtable_flush_to_shard() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let dirfd = unsafe {
            libc::open(
                dir.path().as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
                libc::O_RDONLY | libc::O_DIRECTORY,
            )
        };
        assert!(dirfd >= 0);

        let name = std::ffi::CString::new("test_shard.db").unwrap();
        let wrote = mt.flush(dirfd, &name, 42, false).unwrap();
        assert!(wrote);

        // Verify shard file exists
        let shard_path = dir.path().join("test_shard.db");
        assert!(shard_path.exists());
        assert!(std::fs::metadata(&shard_path).unwrap().len() > 0);

        unsafe { libc::close(dirfd); }
    }

    #[test]
    fn memtable_flush_empty_returns_false() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let dir = tempfile::tempdir().unwrap();
        let dirfd = unsafe {
            libc::open(
                dir.path().as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
                libc::O_RDONLY | libc::O_DIRECTORY,
            )
        };
        assert!(dirfd >= 0);

        let name = std::ffi::CString::new("empty.db").unwrap();
        let wrote = mt.flush(dirfd, &name, 42, false).unwrap();
        assert!(!wrote);
        unsafe { libc::close(dirfd); }
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
        assert_eq!(batch.region_size(0), 8);  // pk: 1 row * 8 bytes (U64 PK)
        assert_eq!(batch.region_size(3), 8);  // col0: 1 row * 8 bytes
        assert!(!batch.region_ptr(0).is_null());
    }

    #[test]
    fn batch_clear() {
        let schema = make_u64_i64_schema();
        let mut batch = make_batch(&schema, &[(10, 1, 100)]).into_inner();
        assert_eq!(batch.count, 1);

        batch.clear();
        assert_eq!(batch.count, 0);
        assert!(batch.sorted);
        assert!(batch.consolidated);
    }

    /// Schema matching reduce output: U128 PK (pk_index=0) + I64 group_val + I64 agg_val
    fn make_reduce_schema() -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn::new(0, 0); 64],
        };
        sd.columns[0] = SchemaColumn::new(type_code::U128, 0); // U128 PK
        sd.columns[1] = SchemaColumn::new(type_code::I64, 0);  // I64 group_val
        sd.columns[2] = SchemaColumn::new(type_code::I64, 0);  // I64 agg_val
        sd
    }

    /// Build a 3-column Batch from (pk_lo, pk_hi, weight, group_val, agg_val) tuples.
    fn make_reduce_batch(rows: &[(u64, u64, i64, i64, i64)]) -> Batch {
        let schema = make_reduce_schema();
        let n = rows.len();
        let mut b = Batch::with_schema(schema, n.max(1));

        for &(plo, phi, w, gv, av) in rows {
            b.extend_pk(crate::util::make_pk(plo, phi));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &gv.to_le_bytes());
            b.extend_col(1, &av.to_le_bytes());
            b.count += 1;
        }

        b.sorted = true;
        b.consolidated = false;
        b
    }

    /// Reproduce the reduce output pattern: insertion + retraction across ticks.
    /// After consolidation, only the LAST tick's aggregate should survive.
    #[test]
    fn test_reduce_output_consolidation_3col() {
        let schema = make_reduce_schema();

        // Tick 1: insert (PK=0, group=0, sum=5000, w=+1)
        let run1 = make_reduce_batch(&[(0, 0, 1, 0, 5000)]);
        // Tick 2: retract old, insert new
        let run2 = make_reduce_batch(&[
            (0, 0, -1, 0, 5000),   // retract sum=5000
            (0, 0,  1, 0, 10000),  // insert sum=10000
        ]);
        // Tick 3: retract old, insert new
        let run3 = make_reduce_batch(&[
            (0, 0, -1, 0, 10000),  // retract sum=10000
            (0, 0,  1, 0, 15000),  // insert sum=15000
        ]);

        let batches: Vec<SortedMemBatch> = vec![
            run1.as_sorted_mem_batch().expect("test batch is sorted"),
            run2.as_sorted_mem_batch().expect("test batch is sorted"),
            run3.as_sorted_mem_batch().expect("test batch is sorted"),
        ];

        let consolidated = consolidate_batches(&batches, &schema);

        // After consolidation: only (PK=0, group=0, sum=15000, w=+1) should remain
        assert_eq!(consolidated.count, 1,
            "expected 1 row after consolidation, got {}", consolidated.count);
        assert_eq!(consolidated.get_pk(0), 0);
        assert_eq!(consolidated.get_weight(0), 1);
        // Check agg_val (payload column 1) = 15000
        let agg_bytes = consolidated.get_col_ptr(0, 1, 8);
        let agg_val = i64::from_le_bytes(agg_bytes.try_into().unwrap());
        assert_eq!(agg_val, 15000, "expected agg_val=15000, got {}", agg_val);
    }

    // ── append_row_simple tests ──────────────────────────────────────────

    fn make_schema_cols(cols: &[(u8, u8)], pk_index: u32) -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        for (i, &(tc, nullable)) in cols.iter().enumerate() {
            columns[i] = SchemaColumn::new(tc, nullable);
        }
        SchemaDescriptor { num_columns: cols.len() as u32, pk_index, columns }
    }

    fn decode_str(batch: &Batch, row: usize, payload_col: usize) -> Vec<u8> {
        let raw = batch.get_col_ptr(row, payload_col, 16);
        let st: [u8; 16] = raw.try_into().unwrap();
        crate::schema::decode_german_string(&st, &batch.blob)
    }

    #[test]
    fn test_append_row_simple_nullable_string() {
        // Schema: U64(pk=0), STRING(nullable)
        let schema = make_schema_cols(&[
            (type_code::U64, 0),
            (type_code::STRING, 1),
        ], 0);
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
        let schema = make_schema_cols(&[
            (type_code::U64, 0),
            (type_code::STRING, 0),
            (type_code::STRING, 0),
        ], 0);
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
        let schema = make_schema_cols(&[
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
        ], 0);
        let mut batch = Batch::with_schema(schema, 1);

        let n = 12;
        let mut lo = vec![0i64; n];
        let mut hi = vec![0u64; n];
        let mut ptrs = vec![std::ptr::null::<u8>(); n];
        let mut lens = vec![0u32; n];

        lo[0] = 42;        // U8: 42
        lo[1] = -7;        // I8: -7
        lo[2] = 1000;      // U16: 1000
        lo[3] = -500;      // I16: -500
        lo[4] = 70000;     // U32: 70000
        lo[5] = -12345;    // I32: -12345
        // F32: 3.14 → store as f64 bit pattern (float2longlong convention)
        lo[6] = f64::to_bits(3.14f64) as i64;
        lo[7] = 0x1234_5678_9ABC_DEF0u64 as i64;  // U64
        lo[8] = -99999;    // I64
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

    /// Verify that `append_row` substitutes an empty string when the
    /// declared length would read past the end of `blob_src`. This matches
    /// `relocate_string_cell` and prevents silent corruption from emitting
    /// unrelated bytes from the start of the blob.
    #[test]
    fn test_append_row_blob_length_header() {
        let schema = make_schema_cols(&[
            (type_code::U64, 0),    // PK
            (type_code::STRING, 0), // STRING payload
        ], 0);
        let mut batch = Batch::with_schema(schema, 1);

        // Build a German String struct with length=20 but only 5 blob bytes available.
        // This triggers the malformed-wire-data fallback branch in append_row.
        let mut gs_struct = [0u8; 16];
        gs_struct[0..4].copy_from_slice(&20u32.to_le_bytes()); // declared length = 20
        gs_struct[4..8].copy_from_slice(&0u32.to_le_bytes());  // prefix bytes = 0
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
        let found = mt.find_positive_payload_row(10);
        assert!(found, "should find a live row");
        assert!(mt.has_found);

        // The found row should have payload = 200
        let col_ptr = mt.found_col_ptr(0, 8);
        assert!(!col_ptr.is_null());
        let val = i64::from_le_bytes(
            unsafe { std::slice::from_raw_parts(col_ptr, 8) }.try_into().unwrap(),
        );
        assert_eq!(val, 200, "found row should be the live val=200 row, not the retracted val=100");

        // PK with no rows at all → not found
        let found2 = mt.find_positive_payload_row(99);
        assert!(!found2);
        assert!(!mt.has_found);
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

        let snap = mt.get_snapshot();
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
        let snap = mt.get_snapshot();
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

        let snap = mt.get_snapshot();
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

        let snap = mt.get_snapshot();
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

        let snap = mt.get_snapshot();
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

        // 6 more insertions → 14 runs, runs_bytes ≈ 560 > 525
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

        // After consolidation: net 12 rows ≈ 480 < 525
        assert!(!mt.should_flush(), "net state should be under threshold");
    }

    #[test]
    fn test_inline_consolidate_has_found_cleared() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        for i in 0..15u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        // lookup_pk sets has_found
        let (w, found) = mt.lookup_pk(5);
        assert_eq!(w, 1);
        assert!(found);
        assert!(mt.has_found);

        // 16th batch triggers consolidation → has_found cleared
        let b16 = make_batch(&schema, &[(16, 1, 1600)]);
        mt.upsert_sorted_batch(b16).unwrap();
        assert!(!mt.has_found);
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
        super::relocate_string_cell(&src_struct, src_blob, &mut dst, &mut dst_blob);

        // The corrupted string should have length set to 0
        let out_len = u32::from_le_bytes(dst[0..4].try_into().unwrap());
        assert_eq!(out_len, 0, "corrupted string should be zero-length");
        assert!(dst_blob.is_empty(), "no blob data should have been copied");
    }

    /// Bug 5: append_row_from_source must not panic when blob offset is invalid.
    #[test]
    fn test_append_row_from_source_corrupted_blob() {
        use crate::schema::{SchemaColumn, type_code};
        use std::collections::HashMap;

        // Schema: col0 = PK (U64), col1 = STRING
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
        let schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns };

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
        let mut blob_cache: HashMap<(u64, usize), usize> = HashMap::new();

        // Must not panic
        dst.append_row_from_source(42u128, 1, &src, 0, &mut blob_cache);

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
            if buf.capacity() == 0 { break; }
            if buf.capacity() >= data_cap { found = true; }
            drained.push(buf);
        }
        assert!(found, "pool should contain the recycled data buffer");
        for buf in drained { recycle_buf(buf); }
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
        while acquire_buf().capacity() > 0 { count += 1; }
        assert!(count >= 2, "expected at least 2 recycled buffers, got {}", count);
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
