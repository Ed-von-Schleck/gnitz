//! Unified Rust Table: owns MemTable + ShardIndex + optional WAL writer.
//!
//! Replaces RPython's EphemeralTable and PersistentTable with a single opaque
//! handle.  Ephemeral tables have `wal_writer = None`; persistent tables have
//! a WAL writer and publish manifests on flush.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::sync::Arc;

use crate::columnar;
use crate::compact::SchemaDescriptor;
use crate::memtable::{self, MemTable, OwnedBatch};
use crate::merge::MemBatch;
use crate::read_cursor::{self, CursorHandle};
use crate::shard_index::ShardIndex;
use crate::shard_reader::MappedShard;
use crate::wal::{WalReader, WalWriter};

const MAX_REGIONS: usize = 70; // 4 core + up to 63 payload cols + 1 blob

// ---------------------------------------------------------------------------
// FoundSource — tracks where retract_pk found its row
// ---------------------------------------------------------------------------

enum FoundSource {
    None,
    MemTable,
    Shard(*const MappedShard, usize),
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
    name: String,
    dirfd: libc::c_int,

    wal_writer: Option<WalWriter>,
    manifest_path: Option<String>,

    flush_seq: u32,
    pub current_lsn: u64,

    found_source: FoundSource,
}

impl Table {
    /// Create a new table.  `durable=true` enables WAL + manifest (persistent).
    pub fn new(
        dir: &str,
        name: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        arena_size: u64,
        durable: bool,
    ) -> Result<Self, i32> {
        let dir_c = ensure_dir(dir)?;

        // Try to set NOCOW (btrfs; silently ignored on other fs)
        set_nocow_dir(&dir_c);

        // Open dirfd
        let dirfd = unsafe {
            libc::open(dir_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY)
        };
        if dirfd < 0 {
            return Err(-3);
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
            name: name.to_string(),
            dirfd,
            wal_writer: None,
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

            let wal_path = format!("{}/{}.wal", dir, name);
            let wal_c = CString::new(wal_path.as_str()).map_err(|_| -1)?;
            table.wal_writer = Some(WalWriter::open(&wal_c)?);
            table.recover_from_wal()?;
        }

        Ok(table)
    }

    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Enable or disable WAL writes.  Disabling drops the WAL writer.
    pub fn set_has_wal(&mut self, flag: bool) {
        if !flag {
            self.wal_writer = None;
        }
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed OwnedBatch.  Writes WAL (if durable),
    /// then memtable.  Used by PartitionedTable after hash-routing.
    pub fn ingest_owned_batch(&mut self, batch: OwnedBatch) -> Result<(), i32> {
        if batch.count == 0 {
            return Ok(());
        }
        self.found_source = FoundSource::None;

        if let Some(ref mut wal) = self.wal_writer {
            let regions = batch.regions();
            let ptrs: Vec<*const u8> = regions.iter().map(|&(p, _)| p).collect();
            let sizes: Vec<u32> = regions.iter().map(|&(_, s)| s as u32).collect();
            let blob_size = *sizes.last().unwrap_or(&0) as u64;
            let rc = wal.append_batch(
                self.current_lsn, self.table_id, batch.count as u32,
                &ptrs, &sizes, blob_size,
            );
            if rc < 0 {
                return Err(rc);
            }
            self.current_lsn += 1;
        }

        if self.memtable.should_flush() {
            self.flush()?;
        }
        self.memtable.upsert_sorted_batch(batch)?;
        if self.memtable.should_flush() {
            self.flush()?;
        }
        Ok(())
    }

    /// Ingest a pre-sorted batch.  Writes WAL first (if durable), then memtable.
    pub fn ingest_batch_from_regions(
        &mut self,
        ptrs: &[*const u8],
        sizes: &[u32],
        count: u32,
        num_payload_cols: usize,
    ) -> Result<(), i32> {
        if count == 0 {
            return Ok(());
        }
        self.found_source = FoundSource::None;

        if let Some(ref mut wal) = self.wal_writer {
            let blob_idx = 4 + num_payload_cols;
            let blob_size = if blob_idx < sizes.len() { sizes[blob_idx] as u64 } else { 0 };
            let rc = wal.append_batch(
                self.current_lsn, self.table_id, count, ptrs, sizes, blob_size,
            );
            if rc < 0 {
                return Err(rc);
            }
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
    ) -> Result<(), i32> {
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
    ) -> Result<(), i32> {
        let mut batch = unsafe {
            OwnedBatch::from_regions(ptrs, sizes, count as usize, num_payload_cols)
        };
        // Defensive sort: from_regions always has sorted=false.  If the
        // batch has >1 row, re-sort by (PK, payload) so that memtable
        // runs are always in canonical order for merge_batches.
        if !batch.sorted && batch.count > 1 {
            let mb = batch.as_mem_batch();
            let mut sorted = memtable::write_to_owned_batch(
                &self.schema, batch.count, batch.blob.len().max(1),
                |w| crate::merge::sort_only(&mb, &self.schema, w),
            );
            sorted.sorted = true;
            sorted.schema = Some(self.schema);
            batch = sorted;
        }
        let durable = !force_ephemeral && self.wal_writer.is_some();
        match self.memtable.upsert_sorted_batch(batch) {
            Ok(()) => {
                if self.memtable.should_flush() {
                    self.flush_inner(durable)?;
                }
                Ok(())
            }
            Err(code) if code == memtable::ERR_CAPACITY => {
                self.flush_inner(durable)?;
                let batch2 = unsafe {
                    OwnedBatch::from_regions(ptrs, sizes, count as usize, num_payload_cols)
                };
                self.memtable.upsert_sorted_batch(batch2)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------------
    // Flush
    // ------------------------------------------------------------------

    /// Flush memtable to shard.  Persistent tables also update manifest and
    /// truncate WAL.
    pub fn flush(&mut self) -> Result<bool, i32> {
        self.flush_inner(self.wal_writer.is_some())
    }

    fn flush_inner(&mut self, durable: bool) -> Result<bool, i32> {
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
        let name_c = CString::new(shard_name.as_str()).map_err(|_| -1)?;

        let rc = self.memtable.flush(self.dirfd, &name_c, self.table_id, durable);
        if rc < 0 && rc != -1 {
            return Err(rc);
        }

        let wrote = rc >= 0;
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
                return Err(-3);
            }
            if let Some(ref mut wal) = self.wal_writer {
                wal.truncate();
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
    pub fn create_cursor(&mut self) -> Result<CursorHandle<'static>, i32> {
        self.compact_if_needed()?;
        let snapshot = self.memtable.get_snapshot();
        let shard_ptrs = self.shard_index.all_shard_ptrs();
        let snapshots = vec![snapshot];
        let handle = unsafe {
            read_cursor::create_cursor_from_snapshots(&snapshots, &shard_ptrs, self.schema)
        };
        Ok(handle)
    }

    /// Get a memtable snapshot handle (for PartitionedTable cursor gathering).
    pub fn get_snapshot(&mut self) -> Arc<OwnedBatch> {
        self.memtable.get_snapshot()
    }

    /// Get all shard pointers (for PartitionedTable cursor gathering).
    pub fn all_shard_ptrs(&self) -> Vec<*const MappedShard> {
        self.shard_index.all_shard_ptrs()
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    /// Check if a PK exists with positive net weight.
    pub fn has_pk(&mut self, key_lo: u64, key_hi: u64) -> bool {
        let mut total_w: i64 = 0;
        if self.memtable.may_contain_pk(key_lo, key_hi) {
            let (w, _) = self.memtable.lookup_pk(key_lo, key_hi);
            total_w = w;
        }
        let (shard_w, _) = self.scan_shards_for_pk(key_lo, key_hi);
        total_w += shard_w;
        total_w > 0
    }

    /// Look up a PK for retraction.  Returns (net_weight, found).
    /// If found, sets `found_source` so `found_*` accessors can read the row.
    pub fn retract_pk(&mut self, key_lo: u64, key_hi: u64) -> (i64, bool) {
        let mut total_w: i64 = 0;
        self.found_source = FoundSource::None;

        let mut mt_found = false;
        if self.memtable.may_contain_pk(key_lo, key_hi) {
            let (w, found) = self.memtable.lookup_pk(key_lo, key_hi);
            total_w = w;
            mt_found = found;
        }

        let (shard_w, shard_found) = self.scan_shards_for_pk(key_lo, key_hi);
        total_w += shard_w;

        if total_w <= 0 {
            return (total_w, false);
        }

        if mt_found {
            self.found_source = FoundSource::MemTable;
        } else if let Some((ptr, idx)) = shard_found {
            self.found_source = FoundSource::Shard(ptr, idx);
        }

        (total_w, true)
    }

    /// Get net weight for a specific (PK, payload) row.
    pub fn get_weight_for_row(
        &mut self,
        key_lo: u64,
        key_hi: u64,
        ref_batch: &MemBatch,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        // MemTable
        if self.memtable.may_contain_pk(key_lo, key_hi) {
            total_w += self.memtable.find_weight_for_row(key_lo, key_hi, ref_batch, ref_row);
        }

        // Shards
        self.shard_index.find_pk(key_lo, key_hi, &mut |shard_ptr, start_idx| {
            let shard = unsafe { &*shard_ptr };
            let mut idx = start_idx;
            while idx < shard.count {
                if shard.get_pk_lo(idx) != key_lo || shard.get_pk_hi(idx) != key_hi {
                    break;
                }
                let ord = columnar::compare_rows(
                    &self.schema, shard, idx, ref_batch, ref_row,
                );
                if ord == Ordering::Equal {
                    total_w += shard.get_weight(idx);
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
        match self.found_source {
            FoundSource::MemTable => self.memtable.found_null_word(),
            FoundSource::Shard(ptr, idx) => {
                let shard = unsafe { &*ptr };
                shard.get_null_word(idx)
            }
            FoundSource::None => 0,
        }
    }

    pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
        match self.found_source {
            FoundSource::MemTable => self.memtable.found_col_ptr(payload_col, col_size),
            FoundSource::Shard(ptr, idx) => {
                let shard = unsafe { &*ptr };
                shard.get_col_ptr(idx, payload_col, col_size).as_ptr()
            }
            FoundSource::None => std::ptr::null(),
        }
    }

    pub fn found_blob_ptr(&self) -> *const u8 {
        match self.found_source {
            FoundSource::MemTable => self.memtable.found_blob_ptr(),
            FoundSource::Shard(ptr, _) => {
                let shard = unsafe { &*ptr };
                shard.blob_slice().as_ptr()
            }
            FoundSource::None => std::ptr::null(),
        }
    }

    // ------------------------------------------------------------------
    // Compaction
    // ------------------------------------------------------------------

    pub fn compact_if_needed(&mut self) -> Result<(), i32> {
        if !self.shard_index.needs_compaction {
            return Ok(());
        }
        self.found_source = FoundSource::None;
        self.shard_index.run_compact()?;
        if let Some(ref path) = self.manifest_path {
            self.shard_index.publish_manifest(path)?;
            if unsafe { libc::fsync(self.dirfd) } < 0 {
                return Err(-3);
            }
        }
        self.shard_index.try_cleanup();
        Ok(())
    }

    // ------------------------------------------------------------------
    // WAL recovery
    // ------------------------------------------------------------------

    pub fn recover_from_wal(&mut self) -> Result<(), i32> {
        let wal_path = format!("{}/{}.wal", self.directory, self.name);
        let wal_c = match CString::new(wal_path.as_str()) {
            Ok(c) => c,
            Err(_) => return Err(-1),
        };

        let mut reader = match WalReader::open(&wal_c) {
            Ok(r) => r,
            Err(_) => return Ok(()), // No WAL file = nothing to recover
        };

        let boundary = self.current_lsn;
        let num_payload_cols = self.schema.num_columns as usize - 1;

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = vec![0u32; MAX_REGIONS];
        let mut sizes = vec![0u32; MAX_REGIONS];

        loop {
            let rc = reader.read_next_block(
                &mut lsn, &mut tid, &mut count,
                &mut num_regions, &mut blob_size,
                &mut offsets, &mut sizes, MAX_REGIONS as u32,
            );
            if rc == 1 {
                break; // EOF
            }
            if rc < 0 {
                break; // Corrupt block — stop recovery
            }

            if tid != self.table_id {
                continue;
            }
            if lsn < boundary {
                continue;
            }

            // Build OwnedBatch from WAL block regions (mmap'd data)
            let nr = num_regions as usize;
            let base = reader.base_ptr();
            let mut ptrs = Vec::with_capacity(nr);
            let mut szs = Vec::with_capacity(nr);
            for i in 0..nr {
                let ptr = unsafe { base.add(offsets[i] as usize) };
                ptrs.push(ptr);
                szs.push(sizes[i]);
            }

            let batch = unsafe {
                OwnedBatch::from_regions(&ptrs, &szs, count as usize, num_payload_cols)
            };
            let _ = self.memtable.upsert_sorted_batch(batch);

            if lsn >= self.current_lsn {
                self.current_lsn = lsn + 1;
            }
        }

        // reader dropped here (closes mmap + fd)
        Ok(())
    }

    // ------------------------------------------------------------------
    // Accumulator support (delegated to MemTable)
    // ------------------------------------------------------------------

    pub fn bloom_add(&mut self, key_lo: u64, key_hi: u64) {
        self.memtable.bloom_add(key_lo, key_hi);
    }

    pub fn memtable_should_flush(&self) -> bool {
        self.memtable.should_flush()
    }

    pub fn memtable_is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Upsert a pre-sorted batch directly into the memtable (no WAL).
    /// Used by the RPython accumulator flush path.
    pub fn memtable_upsert_sorted_batch(&mut self, batch: OwnedBatch) -> Result<(), i32> {
        self.memtable.upsert_sorted_batch(batch)
    }

    // ------------------------------------------------------------------
    // Child table creation
    // ------------------------------------------------------------------

    pub fn create_child(
        &self,
        child_name: &str,
        child_schema: SchemaDescriptor,
    ) -> Result<Table, i32> {
        let child_dir = format!("{}/scratch_{}", self.directory, child_name);
        Table::new(
            &child_dir,
            child_name,
            child_schema,
            self.table_id,
            // Use same arena size — rough estimate from schema stride
            self.memtable.max_bytes() as u64,
            false, // children are always ephemeral
        )
    }

    // ------------------------------------------------------------------
    // Close
    // ------------------------------------------------------------------

    pub fn close(&mut self) {
        // WAL first (flush pending writes)
        self.wal_writer = None; // Drop closes WAL fd + lock

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
        key_lo: u64,
        key_hi: u64,
    ) -> (i64, Option<(*const MappedShard, usize)>) {
        let mut total_w: i64 = 0;
        let mut first_found: Option<(*const MappedShard, usize)> = None;

        self.shard_index.find_pk(key_lo, key_hi, &mut |shard_ptr, start_idx| {
            let shard = unsafe { &*shard_ptr };
            let mut idx = start_idx;
            while idx < shard.count {
                if shard.get_pk_lo(idx) != key_lo || shard.get_pk_hi(idx) != key_hi {
                    break;
                }
                total_w += shard.get_weight(idx);
                if first_found.is_none() {
                    first_found = Some((shard_ptr, idx));
                }
                idx += 1;
            }
        });

        (total_w, first_found)
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

pub(crate) fn ensure_dir(dir: &str) -> Result<CString, i32> {
    let dir_c = CString::new(dir).map_err(|_| -1)?;
    unsafe {
        let rc = libc::mkdir(dir_c.as_ptr(), 0o755);
        if rc < 0 && *libc::__errno_location() != libc::EEXIST {
            return Err(-3);
        }
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
    let prefix = format!("eph_shard_{}_", table_id);
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&prefix) {
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
    use crate::compact::{SchemaColumn, SchemaDescriptor};

    fn make_u64_i64_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn { type_code: 3, size: 8, nullable: 0, _pad: 0 }; // U64 PK
        columns[1] = SchemaColumn { type_code: 7, size: 8, nullable: 0, _pad: 0 }; // I64
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

        assert!(t.has_pk(10, 0));
        assert!(t.has_pk(20, 0));
        assert!(!t.has_pk(99, 0));

        t.flush().unwrap();

        // After flush, data is in shards
        assert!(t.has_pk(10, 0));
        assert!(t.has_pk(20, 0));

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

        assert!(t.has_pk(10, 0));
        t.close();

        // Re-open and recover
        let mut t2 = Table::new(
            tdir.to_str().unwrap(), "test", schema, 200, 1 << 20, true,
        ).unwrap();

        // Data should be in shards via manifest
        assert!(t2.has_pk(10, 0));
        assert!(t2.has_pk(20, 0));
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
        assert_eq!(cursor.cursor.current_key_lo, 10);
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

        let (w, found) = t.retract_pk(10, 0);
        assert_eq!(w, 1);
        assert!(found);
        assert_ne!(t.found_null_word(), u64::MAX); // should return valid null word
        assert!(!t.found_col_ptr(0, 8).is_null()); // payload column accessible

        let (w, found) = t.retract_pk(99, 0);
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
            assert!(t.has_pk(i * 10, 0));
        }
    }
}
