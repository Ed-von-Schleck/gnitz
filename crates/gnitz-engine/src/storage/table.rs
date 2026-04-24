//! Unified Table: owns MemTable + ShardIndex.
//!
//! Ephemeral tables skip durability; persistent tables publish manifests on flush.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::sync::Arc;

use super::columnar;
use super::error::StorageError;
use crate::schema::SchemaDescriptor;
use super::memtable::MemTable;
use super::batch::Batch;
#[cfg(test)]
use super::batch::ConsolidatedBatch;
use super::read_cursor::{self, CursorHandle};
use super::shard_index::ShardIndex;
use super::shard_reader::MappedShard;


// ---------------------------------------------------------------------------
// FoundSource — tracks where retract_pk found its row
// ---------------------------------------------------------------------------

enum FoundSource {
    None,
    MemTable,
    Shard(Arc<MappedShard>, usize),
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

pub struct Table {
    memtable: MemTable,
    shard_index: ShardIndex,
    schema: SchemaDescriptor,
    table_id: u32,
    directory: String,
    dirfd: libc::c_int,

    durable: bool,
    manifest_path: Option<String>,

    flush_seq: u32,
    pub current_lsn: u64,

    found_source: FoundSource,
}

impl Table {
    /// Create a new table.  `durable=true` enables manifest (persistent).
    pub fn new(
        dir: &str,
        _name: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        arena_size: u64,
        durable: bool,
    ) -> Result<Self, StorageError> {
        let dir_c = ensure_dir(dir)?;

        // Try to set NOCOW (btrfs; silently ignored on other fs)
        set_nocow_dir(&dir_c);

        // Open dirfd
        let dirfd = unsafe {
            libc::open(dir_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY)
        };
        if dirfd < 0 {
            return Err(StorageError::Io);
        }

        // Erase stale ephemeral shards (prefix-matched)
        if !durable {
            erase_stale_shards(dir, table_id);
        }

        let memtable = MemTable::new(schema, arena_size as usize);
        let shard_index = ShardIndex::new(table_id, dir, schema);

        let mut table = Table {
            memtable,
            shard_index,
            schema,
            table_id,
            directory: dir.to_string(),
            dirfd,
            durable,
            manifest_path: None,
            flush_seq: 0,
            current_lsn: 1,
            found_source: FoundSource::None,
        };

        if durable {
            let manifest_path = format!("{}/manifest.bin", dir);
            let _ = table.shard_index.load_manifest(&manifest_path);
            table.current_lsn = table.shard_index.max_lsn() + 1;
            if table.current_lsn == 0 {
                table.current_lsn = 1;
            }
            table.manifest_path = Some(manifest_path);
        }

        Ok(table)
    }

    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch into the memtable.
    /// Used by PartitionedTable after hash-routing.
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        let consolidated = batch.into_consolidated(&self.schema);
        self.found_source = FoundSource::None;

        if self.durable {
            self.current_lsn += 1;
        }

        if self.memtable.should_flush() {
            self.flush()?;
        }
        // The should_flush() pre-check above ensures runs_bytes is either 0
        // (post-flush) or <= 75% of max_bytes, so check_capacity() inside
        // upsert_sorted_batch (which fires at 100%) cannot return ERR_CAPACITY.
        self.memtable.upsert_sorted_batch(consolidated)?;
        if self.memtable.should_flush() {
            self.flush()?;
        }
        Ok(())
    }

    /// Ingest a pre-sorted batch into the memtable.
    pub fn ingest_batch_from_regions(
        &mut self,
        ptrs: &[*const u8],
        sizes: &[u32],
        count: u32,
        num_payload_cols: usize,
    ) -> Result<(), StorageError> {
        if count == 0 {
            return Ok(());
        }
        self.found_source = FoundSource::None;

        if self.durable {
            self.current_lsn += 1;
        }

        self.upsert_and_maybe_flush(ptrs, sizes, count, num_payload_cols, false)
    }

    /// Ingest without WAL (worker DDL sync path).  Uses ephemeral flush on overflow.
    pub fn ingest_batch_memonly_from_regions(
        &mut self,
        ptrs: &[*const u8],
        sizes: &[u32],
        count: u32,
        num_payload_cols: usize,
    ) -> Result<(), StorageError> {
        if count == 0 {
            return Ok(());
        }
        self.found_source = FoundSource::None;
        self.upsert_and_maybe_flush(ptrs, sizes, count, num_payload_cols, true)
    }

    fn upsert_and_maybe_flush(
        &mut self,
        ptrs: &[*const u8],
        sizes: &[u32],
        count: u32,
        num_payload_cols: usize,
        force_ephemeral: bool,
    ) -> Result<(), StorageError> {
        let batch = unsafe {
            Batch::from_regions(ptrs, sizes, count as usize, num_payload_cols)
        };
        let consolidated = batch.into_consolidated(&self.schema);
        let durable = !force_ephemeral && self.durable;
        match self.memtable.upsert_sorted_batch(consolidated) {
            Ok(()) => {
                if self.memtable.should_flush() {
                    self.flush_inner(durable)?;
                }
                Ok(())
            }
            Err(StorageError::Capacity) => {
                self.flush_inner(durable)?;
                let batch2 = unsafe {
                    Batch::from_regions(ptrs, sizes, count as usize, num_payload_cols)
                };
                let consolidated2 = batch2.into_consolidated(&self.schema);
                self.memtable.upsert_sorted_batch(consolidated2)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------------
    // Flush
    // ------------------------------------------------------------------

    /// Flush memtable to shard.  Persistent tables also update manifest.
    pub fn flush(&mut self) -> Result<bool, StorageError> {
        self.flush_inner(self.durable)
    }

    /// Flush with durable shard naming and manifest update, regardless of
    /// WAL state.  Used by checkpoint: system tables disable WAL (SAL
    /// provides durability) but still need manifest-tracked shards so
    /// the data survives restart.
    pub fn flush_durable(&mut self) -> Result<bool, StorageError> {
        self.flush_inner(true)
    }

    fn flush_inner(&mut self, durable: bool) -> Result<bool, StorageError> {
        self.found_source = FoundSource::None;
        if self.memtable.is_empty() {
            return Ok(false);
        }
        let (shard_name, lsn_max) = if durable {
            (
                format!("shard_{}_{}.db", self.table_id, self.current_lsn),
                self.current_lsn.saturating_sub(1),
            )
        } else {
            self.flush_seq += 1;
            let pid = unsafe { libc::getpid() };
            (
                format!(
                    "eph_shard_{}_{}_{}_{}.db",
                    self.table_id, pid, self.flush_seq, self.current_lsn
                ),
                0,
            )
        };

        let shard_path = format!("{}/{}", self.directory, shard_name);
        let name_c = CString::new(shard_name.as_str()).map_err(|_| StorageError::InvalidPath)?;

        let wrote = self.memtable.flush(self.dirfd, &name_c, self.table_id, durable)?;
        if wrote {
            self.shard_index.add_shard(&shard_path, 0, lsn_max)?;
        } else {
            let _ = unlinkat(self.dirfd, &name_c);
        }

        if durable {
            if let Some(ref path) = self.manifest_path {
                self.shard_index.publish_manifest(path)?;
            }
            if unsafe { libc::fsync(self.dirfd) } < 0 {
                return Err(StorageError::Io);
            }
        }

        self.memtable.reset();
        Ok(wrote)
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    /// Create a cursor over all data (memtable snapshot + shards).
    /// Runs compaction if needed.  Returns an opaque CursorHandle.
    pub fn create_cursor(&mut self) -> Result<CursorHandle, StorageError> {
        self.compact_if_needed()?;
        let snapshot = self.memtable.get_snapshot();
        let shard_arcs = self.shard_index.all_shard_arcs();
        let snapshots = vec![snapshot];
        let handle = read_cursor::create_cursor_from_snapshots(&snapshots, &shard_arcs, self.schema);
        Ok(handle)
    }

    /// Get a memtable snapshot handle (for PartitionedTable cursor gathering).
    pub fn get_snapshot(&mut self) -> Arc<Batch> {
        self.memtable.get_snapshot()
    }

    /// Get all shard Arcs (for PartitionedTable cursor gathering).
    pub fn all_shard_arcs(&self) -> Vec<Arc<MappedShard>> {
        self.shard_index.all_shard_arcs()
    }

    /// Test helper: returns true when the memtable has no rows.
    #[cfg(test)]
    pub fn memtable_is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Test helper: upsert a consolidated batch directly into the memtable (no WAL).
    #[cfg(test)]
    pub fn memtable_upsert_sorted_batch(&mut self, batch: ConsolidatedBatch) -> Result<(), StorageError> {
        self.memtable.upsert_sorted_batch(batch)
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    /// Check if a PK exists with positive net weight.
    pub fn has_pk(&mut self, key: u128) -> bool {
        let mut total_w: i64 = 0;
        if self.memtable.may_contain_pk(key) {
            let (w, _) = self.memtable.lookup_pk(key);
            total_w = w;
        }
        let (shard_w, _) = self.scan_shards_for_pk(key);
        total_w += shard_w;
        total_w > 0
    }

    /// Look up a PK for retraction.  Returns (net_weight, found).
    /// If found, sets `found_source` so `found_*` accessors can read the row.
    ///
    /// Uses `find_positive_payload_row` so that after an UPDATE (which leaves
    /// old-payload rows with net weight 0 in the memtable), the found row is
    /// the live (PK, new_payload) entry, not the cancelled old one.
    pub fn retract_pk(&mut self, key: u128) -> (i64, bool) {
        let mut total_w: i64 = 0;
        self.found_source = FoundSource::None;

        // Step 1: compute total weight (PK-only sum is correct for existence check).
        if self.memtable.may_contain_pk(key) {
            let (w, _) = self.memtable.lookup_pk(key);
            total_w = w;
        }
        let (shard_w, shard_candidates) = self.scan_shards_for_pk(key);
        total_w += shard_w;

        if total_w <= 0 {
            return (total_w, false);
        }

        // Step 2: find the specific live (PK, payload) row by checking net weight
        // per distinct payload — skips cancelled rows whose net weight is 0.
        let mt_live = self.memtable.find_positive_payload_row(key);
        if mt_live {
            self.found_source = FoundSource::MemTable;
        } else {
            // Payload-aware shard fallback: check net weight per (PK, payload)
            for (arc, idx) in &shard_candidates {
                let net_w = self.get_weight_for_row(key, arc.as_ref(), *idx);
                if net_w > 0 {
                    self.found_source = FoundSource::Shard(Arc::clone(arc), *idx);
                    break;
                }
            }
        }

        (total_w, true)
    }

    /// Get net weight for a specific (PK, payload) row.
    pub fn get_weight_for_row<S: columnar::ColumnarSource>(
        &mut self,
        key: u128,
        ref_source: &S,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        // MemTable
        if self.memtable.may_contain_pk(key) {
            total_w += self.memtable.find_weight_for_row(key, ref_source, ref_row);
        }

        // Shards
        self.shard_index.find_pk(key, &mut |shard_arc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_arc.count {
                if shard_arc.get_pk(idx) != key {
                    break;
                }
                let ord = columnar::compare_rows(
                    &self.schema, shard_arc.as_ref(), idx, ref_source, ref_row,
                );
                if ord == Ordering::Equal {
                    total_w += shard_arc.get_weight(idx);
                }
                idx += 1;
            }
        });

        total_w
    }

    // ------------------------------------------------------------------
    // Found-row accessors (after retract_pk)
    // ------------------------------------------------------------------

    pub fn found_null_word(&self) -> u64 {
        match &self.found_source {
            FoundSource::MemTable => self.memtable.found_null_word(),
            FoundSource::Shard(arc, idx) => arc.get_null_word(*idx),
            FoundSource::None => 0,
        }
    }

    pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
        match &self.found_source {
            FoundSource::MemTable => self.memtable.found_col_ptr(payload_col, col_size),
            FoundSource::Shard(arc, idx) => arc.get_col_ptr(*idx, payload_col, col_size).as_ptr(),
            FoundSource::None => std::ptr::null(),
        }
    }

    pub fn found_blob_ptr(&self) -> *const u8 {
        match &self.found_source {
            FoundSource::MemTable => self.memtable.found_blob_ptr(),
            FoundSource::Shard(arc, _) => arc.blob_slice().as_ptr(),
            FoundSource::None => std::ptr::null(),
        }
    }

    /// Read the found row's payload column as a u128 (little-endian, zero-padded for col_size < 16).
    /// Returns None if no row was found.
    pub fn read_found_u128(&self, payload_col: usize, col_size: usize) -> Option<u128> {
        let ptr = self.found_col_ptr(payload_col, col_size);
        if ptr.is_null() { return None; }
        let val = if col_size == 16 {
            let lo = u64::from_le_bytes(unsafe { std::slice::from_raw_parts(ptr, 8) }.try_into().unwrap());
            let hi = u64::from_le_bytes(unsafe { std::slice::from_raw_parts(ptr.add(8), 8) }.try_into().unwrap());
            ((hi as u128) << 64) | lo as u128
        } else {
            let mut buf = [0u8; 8];
            unsafe { std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), col_size.min(8)) };
            u64::from_le_bytes(buf) as u128
        };
        Some(val)
    }

    // ------------------------------------------------------------------
    // Compaction
    // ------------------------------------------------------------------

    pub fn compact_if_needed(&mut self) -> Result<(), StorageError> {
        if !self.shard_index.should_compact() {
            return Ok(());
        }
        self.found_source = FoundSource::None;
        self.shard_index.run_compact()?;
        if let Some(ref path) = self.manifest_path {
            self.shard_index.publish_manifest(path)?;
            if unsafe { libc::fsync(self.dirfd) } < 0 {
                return Err(StorageError::Io);
            }
        }
        self.shard_index.try_cleanup();
        Ok(())
    }

    // ------------------------------------------------------------------
    // Close
    // ------------------------------------------------------------------

    pub fn close(&mut self) {
        // ShardIndex + MemTable dropped when Table is dropped
        if self.dirfd >= 0 {
            unsafe { libc::close(self.dirfd); }
            self.dirfd = -1;
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn scan_shards_for_pk(
        &self,
        key: u128,
    ) -> (i64, Vec<(Arc<MappedShard>, usize)>) {
        let mut total_w: i64 = 0;
        let mut candidates: Vec<(Arc<MappedShard>, usize)> = Vec::new();

        self.shard_index.find_pk(key, &mut |shard_arc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_arc.count {
                if shard_arc.get_pk(idx) != key {
                    break;
                }
                total_w += shard_arc.get_weight(idx);
                candidates.push((Arc::clone(&shard_arc), idx));
                idx += 1;
            }
        });

        (total_w, candidates)
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.close();
    }
}

// ---------------------------------------------------------------------------
// OS helpers
// ---------------------------------------------------------------------------

pub(crate) fn ensure_dir(dir: &str) -> Result<CString, StorageError> {
    let dir_c = CString::new(dir).map_err(|_| StorageError::InvalidPath)?;
    match std::fs::create_dir(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(StorageError::Io),
    }
    Ok(dir_c)
}

fn set_nocow_dir(dir_c: &CStr) {
    unsafe {
        let fd = libc::open(dir_c.as_ptr(), libc::O_RDONLY);
        if fd >= 0 {
            // FS_IOC_SETFLAGS = 0x40086602, FS_NOCOW_FL = 0x00800000
            let mut flags: libc::c_ulong = 0;
            libc::ioctl(fd, 0x80086601, &mut flags); // FS_IOC_GETFLAGS
            flags |= 0x00800000; // FS_NOCOW_FL
            libc::ioctl(fd, 0x40086602, &flags); // FS_IOC_SETFLAGS
            libc::close(fd);
        }
    }
}

fn unlinkat(dirfd: libc::c_int, name: &CStr) -> i32 {
    unsafe { libc::unlinkat(dirfd, name.as_ptr(), 0) }
}

fn erase_stale_shards(dir: &str, table_id: u32) {
    // Remove all shard files belonging to this ephemeral table, including
    // compaction outputs (shard_* and hcomp_*) that may have been left by a
    // previous process.  All three patterns include table_id, so only this
    // table's files are touched even when tables share the same directory.
    let eph_prefix   = format!("eph_shard_{}_",  table_id);
    let shard_prefix = format!("shard_{}_",      table_id);
    let hcomp_prefix = format!("hcomp_{}_",      table_id);
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&eph_prefix)
                    || name.starts_with(&shard_prefix)
                    || name.starts_with(&hcomp_prefix)
                {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

    fn make_u64_i64_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::I64, 0);
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_regions(rows: &[(u64, i64, i64)]) -> (Vec<*const u8>, Vec<u32>, u32) {
        let n = rows.len();
        let pk_lo: Vec<u8> = rows.iter().flat_map(|r| r.0.to_le_bytes()).collect();
        let pk_hi: Vec<u8> = vec![0u8; n * 8];
        let weight: Vec<u8> = rows.iter().flat_map(|r| r.1.to_le_bytes()).collect();
        let null_bmp: Vec<u8> = vec![0u8; n * 8];
        let col0: Vec<u8> = rows.iter().flat_map(|r| r.2.to_le_bytes()).collect();
        let blob: Vec<u8> = Vec::new();

        // Leak the vecs so pointers remain valid for the test
        let pk_lo = pk_lo.into_boxed_slice(); let pk_lo_ptr = pk_lo.as_ptr(); std::mem::forget(pk_lo);
        let pk_hi = pk_hi.into_boxed_slice(); let pk_hi_ptr = pk_hi.as_ptr(); std::mem::forget(pk_hi);
        let weight = weight.into_boxed_slice(); let weight_ptr = weight.as_ptr(); std::mem::forget(weight);
        let null_bmp = null_bmp.into_boxed_slice(); let null_bmp_ptr = null_bmp.as_ptr(); std::mem::forget(null_bmp);
        let col0 = col0.into_boxed_slice(); let col0_ptr = col0.as_ptr(); std::mem::forget(col0);
        let blob = blob.into_boxed_slice(); let blob_ptr = blob.as_ptr(); std::mem::forget(blob);

        let ptrs = vec![pk_lo_ptr, pk_hi_ptr, weight_ptr, null_bmp_ptr, col0_ptr, blob_ptr];
        let sizes = vec![
            (n * 8) as u32, (n * 8) as u32, (n * 8) as u32, (n * 8) as u32,
            (n * 8) as u32, 0u32,
        ];
        (ptrs, sizes, n as u32)
    }

    #[test]
    fn table_ephemeral_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("eph_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();

        assert!(t.memtable_is_empty());

        let (ptrs, sizes, count) = make_regions(&[(10, 1, 100), (20, 1, 200)]);
        t.ingest_batch_from_regions(&ptrs, &sizes, count, 1).unwrap();

        assert!(t.has_pk(10));
        assert!(t.has_pk(20));
        assert!(!t.has_pk(99));

        t.flush().unwrap();

        // After flush, data is in shards
        assert!(t.has_pk(10));
        assert!(t.has_pk(20));

        t.close();
    }

    #[test]
    fn table_persistent_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("pers_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 200, 1 << 20, true,
        ).unwrap();

        let (ptrs, sizes, count) = make_regions(&[(10, 1, 100), (20, 1, 200)]);
        t.ingest_batch_from_regions(&ptrs, &sizes, count, 1).unwrap();
        t.flush().unwrap();

        assert!(t.has_pk(10));
        t.close();

        // Re-open and recover
        let mut t2 = Table::new(
            tdir.to_str().unwrap(), "test", schema, 200, 1 << 20, true,
        ).unwrap();

        // Data should be in shards via manifest
        assert!(t2.has_pk(10));
        assert!(t2.has_pk(20));
        t2.close();
    }

    #[test]
    fn table_cursor_iteration() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cursor_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 300, 1 << 20, false,
        ).unwrap();

        let (ptrs, sizes, count) = make_regions(&[(30, 1, 300), (10, 1, 100), (20, 1, 200)]);
        t.ingest_batch_from_regions(&ptrs, &sizes, count, 1).unwrap();

        let cursor = t.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key_lo(), 10);
        // Don't need to iterate further — cursor creation works
    }

    #[test]
    fn table_retract_pk() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("retract_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 400, 1 << 20, false,
        ).unwrap();

        let (ptrs, sizes, count) = make_regions(&[(10, 1, 100), (20, 1, 200)]);
        t.ingest_batch_from_regions(&ptrs, &sizes, count, 1).unwrap();

        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found);
        assert_ne!(t.found_null_word(), u64::MAX); // should return valid null word
        assert!(!t.found_col_ptr(0, 8).is_null()); // payload column accessible

        let (w, found) = t.retract_pk(99);
        assert_eq!(w, 0);
        assert!(!found);
    }

    #[test]
    fn table_compact() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("compact_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 500, 256, false,
        ).unwrap();

        // Create enough flushes to trigger compaction
        for i in 0..6u64 {
            let (ptrs, sizes, count) = make_regions(&[(i * 10, 1, (i * 100) as i64)]);
            t.ingest_batch_from_regions(&ptrs, &sizes, count, 1).unwrap();
            t.flush().unwrap();
        }

        t.compact_if_needed().unwrap();

        // All data should still be accessible
        for i in 0..6u64 {
            assert!(t.has_pk((i * 10) as u128));
        }
    }

    /// After INSERT then UPDATE (which adds a retraction for the old payload and
    /// an insertion for the new payload), `retract_pk` must return the NEW payload,
    /// not the cancelled old one.
    #[test]
    fn test_retract_pk_after_update() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("retract_update_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 600, 1 << 20, false,
        ).unwrap();

        // Batch 1: INSERT (PK=10, weight=+1, val=100)
        let (ptrs1, sizes1, count1) = make_regions(&[(10, 1, 100)]);
        t.ingest_batch_from_regions(&ptrs1, &sizes1, count1, 1).unwrap();

        // Batch 2: UPDATE delta — retract val=100, insert val=200
        // Rows sorted by (PK, payload): (-1 for val=100) before (+1 for val=200)
        let (ptrs2, sizes2, count2) = make_regions(&[(10, -1, 100), (10, 1, 200)]);
        t.ingest_batch_from_regions(&ptrs2, &sizes2, count2, 1).unwrap();

        // Net state: val=100 has weight 0 (cancelled), val=200 has weight 1
        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found);

        // The found row must be val=200, not the cancelled val=100
        let col_ptr = t.found_col_ptr(0, 8);
        assert!(!col_ptr.is_null());
        let val = i64::from_le_bytes(
            unsafe { std::slice::from_raw_parts(col_ptr, 8) }.try_into().unwrap(),
        );
        assert_eq!(val, 200, "retract_pk must return the live (val=200) row, not the retracted val=100");
    }

    /// `ingest_owned_batch` must sort the batch before WAL write and memtable
    /// insert, even when the incoming Batch has sorted=false (reverse order).
    #[test]
    fn test_ingest_owned_batch_unsorted() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("ingest_owned_unsorted_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 700, 1 << 20, false,
        ).unwrap();

        // Build a reverse-sorted batch (PK order: 30, 20, 10)
        let (ptrs, sizes, count) = make_regions(&[(30, 1, 300), (20, 1, 200), (10, 1, 100)]);
        let batch = unsafe { Batch::from_regions(&ptrs, &sizes, count as usize, 1) };
        // from_regions sets sorted=false
        assert!(!batch.sorted);

        t.ingest_owned_batch(batch).unwrap();

        // Cursor must yield rows in ascending PK order
        let cursor = t.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key_lo(), 10, "cursor should start at PK=10 (smallest)");
    }

    /// When upsert_and_maybe_flush hits ERR_CAPACITY and rebuilds batch2 from
    /// raw regions, it must sort batch2 before the retry upsert.
    #[test]
    fn test_upsert_capacity_retry_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cap_retry_test");
        let schema = make_u64_i64_schema();

        // Very small arena: 40 bytes.  A 3-row batch (~120 bytes) will exceed it.
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 900, 40, false,
        ).unwrap();

        // Directly fill memtable past max_bytes using memtable_upsert_sorted_batch
        // (bypasses auto-flush so runs_bytes exceeds max_bytes).
        let (fill_ptrs, fill_sizes, fill_count) =
            make_regions(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let fill_batch = unsafe {
            Batch::from_regions(&fill_ptrs, &fill_sizes, fill_count as usize, 1)
        };
        let fill_batch = fill_batch.into_consolidated(&schema);
        t.memtable_upsert_sorted_batch(fill_batch).unwrap();
        // runs_bytes (~120) > max_bytes (40) — next upsert will hit ERR_CAPACITY.

        // Ingest a REVERSE-sorted batch.  This hits ERR_CAPACITY → flush → retry.
        // Fix 1d: the retry batch (batch2 from raw regions) must be sorted.
        let (ptrs, sizes, count) = make_regions(&[(50, 1, 500), (40, 1, 400), (30, 1, 300)]);
        t.ingest_batch_from_regions(&ptrs, &sizes, count, 1).unwrap();

        // fill batch (1,2,3) is now in shard; sorted (30,40,50) is in memtable.
        let cursor = t.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key_lo(), 1, "cursor should start at PK=1 from flushed shard");
    }

    /// Bug 2: INSERT (PK=10, val=100) → flush → UPDATE delta → flush → retract_pk.
    /// The shard fallback must pick the live payload (val=200), not the cancelled one.
    #[test]
    fn test_retract_pk_shard_fallback_multiple_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("retract_shard_fallback");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 1000, 1 << 20, false,
        ).unwrap();

        // Batch 1: INSERT (PK=10, weight=+1, val=100)
        let (ptrs1, sizes1, count1) = make_regions(&[(10, 1, 100)]);
        t.ingest_batch_from_regions(&ptrs1, &sizes1, count1, 1).unwrap();
        t.flush().unwrap();

        // Batch 2: UPDATE delta — retract val=100, insert val=200
        let (ptrs2, sizes2, count2) = make_regions(&[(10, -1, 100), (10, 1, 200)]);
        t.ingest_batch_from_regions(&ptrs2, &sizes2, count2, 1).unwrap();
        t.flush().unwrap();

        // Both batches are now in shards, memtable is empty.
        // retract_pk must find val=200 (net weight 1), not val=100 (net weight 0).
        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found);

        let col_ptr = t.found_col_ptr(0, 8);
        assert!(!col_ptr.is_null());
        let val = i64::from_le_bytes(
            unsafe { std::slice::from_raw_parts(col_ptr, 8) }.try_into().unwrap(),
        );
        assert_eq!(val, 200, "shard fallback must pick live payload (val=200), not cancelled (val=100)");
    }
}
