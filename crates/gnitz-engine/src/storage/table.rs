//! Unified Table: owns MemTable + ShardIndex.
//!
//! Ephemeral tables skip durability; persistent tables publish manifests on flush.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::rc::Rc;

use super::columnar;
use super::error::StorageError;
use crate::schema::SchemaDescriptor;
use super::memtable::MemTable;
use super::batch::Batch;
#[cfg(test)]
use super::batch::ConsolidatedBatch;
use super::manifest::PreparedManifest;
use super::read_cursor::{self, CursorHandle};
use super::shard_file;
use super::shard_index::{PendingShard, ShardIndex};
use super::shard_reader::MappedShard;

// ---------------------------------------------------------------------------
// Two-phase flush API
// ---------------------------------------------------------------------------

/// Outcome of `Table::flush_prepare`.
pub enum FlushOutcome {
    /// memtable empty or consolidated count == 0 — nothing written, memtable reset.
    Empty,
    /// Non-durable shard written and committed inline; no deferred work.
    DoneInline,
    /// Durable: .tmp files written, fdatasync + rename pending.
    Pending(FlushWork),
}

/// Open file descriptors and rename targets pending for one durable flush.
/// Owned by the worker between `flush_prepare` and `flush_commit`. Drop
/// unlinks any remaining `.tmp` files so a partial failure leaves no debris.
pub struct FlushWork {
    shard_fd: Option<libc::c_int>,
    shard_rename: Option<ShardRename>,
    manifest: Option<PreparedManifest>,
    pending_shard: Option<PendingShard>,
}

pub struct ShardRename {
    dirfd: libc::c_int,
    tmp_name: CString,
    final_name: CString,
}

impl FlushWork {
    /// Close any still-open fds. Called by the worker after the io_uring
    /// fdatasync batch completes and before renames start.
    pub fn close_fds(&mut self) {
        if let Some(fd) = self.shard_fd.take() {
            unsafe { libc::close(fd); }
        }
        if let Some(m) = &mut self.manifest {
            if let Some(fd) = m.fd.take() {
                unsafe { libc::close(fd); }
            }
        }
    }

    pub fn shard_fd(&self) -> Option<libc::c_int> { self.shard_fd }
    pub fn manifest_fd(&self) -> Option<libc::c_int> {
        self.manifest.as_ref().and_then(|m| m.fd)
    }
}

impl Drop for FlushWork {
    fn drop(&mut self) {
        self.close_fds();
        if let Some(r) = self.shard_rename.take() {
            unsafe { libc::unlinkat(r.dirfd, r.tmp_name.as_ptr(), 0); }
        }
        if let Some(m) = self.manifest.take() {
            unsafe { libc::unlink(m.tmp_path.as_ptr()); }
        }
    }
}


// ---------------------------------------------------------------------------
// FoundSource — tracks where retract_pk found its row
// ---------------------------------------------------------------------------

enum FoundSource {
    None,
    MemTable,
    Shard(Rc<MappedShard>, usize),
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

    cached_full_scan: Option<Rc<Batch>>,
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
            cached_full_scan: None,
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

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch into the memtable.
    /// Used by PartitionedTable after hash-routing.
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.upsert_owned_and_maybe_flush(batch, false)
    }

    /// Ingest an already-constructed Batch without WAL (worker DDL sync /
    /// memonly index population). Forces ephemeral flushes on overflow.
    pub fn ingest_owned_batch_memonly(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.upsert_owned_and_maybe_flush(batch, true)
    }

    fn upsert_owned_and_maybe_flush(
        &mut self,
        batch: Batch,
        force_ephemeral: bool,
    ) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        self.found_source = FoundSource::None;
        self.cached_full_scan = None;

        let durable = !force_ephemeral && self.durable;
        if durable {
            self.current_lsn += 1;
        }

        let consolidated = batch.into_consolidated(&self.schema);
        if self.memtable.should_flush() {
            self.flush_inner(durable, true)?;
        }
        // The should_flush() pre-check above ensures runs_bytes is either 0
        // (post-flush) or <= 75% of max_bytes, so check_capacity() inside
        // upsert_sorted_batch (which fires at 100%) cannot return ERR_CAPACITY.
        self.memtable.upsert_sorted_batch(consolidated)?;
        if self.memtable.should_flush() {
            self.flush_inner(durable, true)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Flush
    // ------------------------------------------------------------------

    /// Flush memtable to shard.  Persistent tables also update manifest.
    pub fn flush(&mut self) -> Result<bool, StorageError> {
        self.flush_inner(self.durable, true)
    }

    /// Flush with durable shard naming and manifest update, regardless of
    /// WAL state.  Used by checkpoint: system tables disable WAL (SAL
    /// provides durability) but still need manifest-tracked shards so
    /// the data survives restart.
    pub fn flush_durable(&mut self) -> Result<bool, StorageError> {
        self.flush_inner(true, true)
    }

    /// Phase 1 of two-phase flush.
    ///
    /// `durable=true` and a non-empty consolidated snapshot:
    ///   write shard `.tmp` (no fdatasync), open MappedShard against it for
    ///   metadata, write manifest `.tmp`, return `Pending(FlushWork)`. The
    ///   memtable is NOT reset.
    ///
    /// `durable=false` (index circuits, ephemeral overflow flushes) with a
    /// non-empty consolidated snapshot:
    ///   write the shard inline via `write_shard_streaming(durable=false)` —
    ///   no fdatasync, no manifest. Memtable IS reset and `DoneInline` is
    ///   returned.
    ///
    /// Empty memtable or `consolidated.count == 0`:
    ///   reset memtable and return `Empty`. The cancelled rows are still
    ///   recoverable from the SAL.
    pub fn flush_prepare(&mut self, durable: bool) -> Result<FlushOutcome, StorageError> {
        self.found_source = FoundSource::None;
        if self.memtable.is_empty() {
            return Ok(FlushOutcome::Empty);
        }

        let snapshot = self.memtable.get_snapshot();
        if snapshot.count == 0 {
            self.memtable.reset();
            return Ok(FlushOutcome::Empty);
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

        let name_c = CString::new(shard_name.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let regions = snapshot.regions();

        if !durable {
            shard_file::write_shard_streaming(
                self.dirfd, &name_c, self.table_id, snapshot.count as u32,
                &regions, false,
            )?;
            let final_full = format!("{}/{}", self.directory, shard_name);
            self.shard_index.add_shard(&final_full, 0, 0)?;
            self.memtable.reset();
            return Ok(FlushOutcome::DoneInline);
        }

        let prepared = shard_file::write_shard_streaming_prepare(
            self.dirfd, &name_c, self.table_id, snapshot.count as u32, &regions,
        )?;
        let shard_fd = prepared.fd;
        let tmp_name = prepared.tmp_name;

        let mut work = FlushWork {
            shard_fd: Some(shard_fd),
            shard_rename: Some(ShardRename {
                dirfd: self.dirfd,
                tmp_name: tmp_name.clone(),
                final_name: name_c.clone(),
            }),
            manifest: None,
            pending_shard: None,
        };

        let tmp_full_c = CString::new(format!(
            "{}/{}", self.directory,
            tmp_name.to_str().map_err(|_| StorageError::InvalidPath)?,
        )).map_err(|_| StorageError::InvalidPath)?;
        let final_full = format!("{}/{}", self.directory, shard_name);

        let pending = self.shard_index.open_shard_for_pending(
            &tmp_full_c, final_full, 0, lsn_max,
        )?;

        let manifest_path = self.manifest_path.as_ref()
            .expect("durable table must have manifest_path");
        let manifest_c = CString::new(manifest_path.as_str())
            .map_err(|_| StorageError::InvalidPath)?;
        work.manifest = Some(self.shard_index
            .prepare_manifest_with_pending(&manifest_c, &pending)?);
        work.pending_shard = Some(pending);

        Ok(FlushOutcome::Pending(work))
    }

    /// Phase 3: rename shard then manifest into final names; insert
    /// pending_shard into shard_index; reset the memtable. Returns
    /// `Some(dirfd)` to fsync after all renames in the worker batch.
    pub fn flush_commit(&mut self, mut work: FlushWork) -> Result<Option<libc::c_int>, StorageError> {
        // Rename shard .tmp → final.
        if let Some(rename) = work.shard_rename.take() {
            let rc = unsafe {
                libc::renameat(
                    rename.dirfd, rename.tmp_name.as_ptr(),
                    rename.dirfd, rename.final_name.as_ptr(),
                )
            };
            if rc < 0 {
                // Restore so Drop unlinks the shard .tmp.
                work.shard_rename = Some(rename);
                return Err(StorageError::Io);
            }
        }

        // Rename manifest .tmp → final. fd already closed by close_fds.
        if let Some(m) = work.manifest.take() {
            let rc = unsafe { libc::rename(m.tmp_path.as_ptr(), m.final_path.as_ptr()) };
            if rc < 0 {
                // Restore so Drop unlinks the manifest .tmp. The shard is
                // already at its final path and will be collected by orphan GC.
                work.manifest = Some(m);
                return Err(StorageError::Io);
            }
        }

        // Publish the pending shard into the index.
        if let Some(pending) = work.pending_shard.take() {
            self.shard_index.add_opened_shard(pending)?;
        }

        self.memtable.reset();
        Ok(Some(self.dirfd))
    }

    fn flush_inner(&mut self, durable: bool, sync_dir: bool) -> Result<bool, StorageError> {
        match self.flush_prepare(durable)? {
            FlushOutcome::Empty => Ok(false),
            FlushOutcome::DoneInline => Ok(true),
            FlushOutcome::Pending(mut work) => {
                if let Some(fd) = work.shard_fd() {
                    if unsafe { libc::fdatasync(fd) } < 0 {
                        return Err(StorageError::Io);
                    }
                }
                if let Some(fd) = work.manifest_fd() {
                    if unsafe { libc::fdatasync(fd) } < 0 {
                        return Err(StorageError::Io);
                    }
                }
                work.close_fds();
                let dirfd = self.flush_commit(work)?;
                if sync_dir {
                    if let Some(fd) = dirfd {
                        if unsafe { libc::fsync(fd) } < 0 {
                            return Err(StorageError::Io);
                        }
                    }
                }
                Ok(true)
            }
        }
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

    /// Return the fully consolidated batch of all live rows, caching the result.
    /// The cache is invalidated on any logical write (upsert or test-helper upsert).
    /// Cheap on repeated calls: returns `Rc::clone` of the cached batch.
    pub fn full_scan(&mut self) -> Result<Rc<Batch>, StorageError> {
        if let Some(ref rc) = self.cached_full_scan {
            return Ok(Rc::clone(rc));
        }
        let handle = self.create_cursor()?;
        let rc = handle.cursor.materialize();
        self.cached_full_scan = Some(Rc::clone(&rc));
        Ok(rc)
    }

    /// Get a memtable snapshot handle (for PartitionedTable cursor gathering).
    pub fn get_snapshot(&mut self) -> Rc<Batch> {
        self.memtable.get_snapshot()
    }

    /// Get all shard Rcs (for PartitionedTable cursor gathering).
    pub fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
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
        self.cached_full_scan = None;
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
            for (rc, idx) in &shard_candidates {
                let net_w = self.get_weight_for_row(key, rc.as_ref(), *idx);
                if net_w > 0 {
                    self.found_source = FoundSource::Shard(Rc::clone(rc), *idx);
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
        self.shard_index.find_pk(key, &mut |shard_rc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_rc.count {
                if shard_rc.get_pk(idx) != key {
                    break;
                }
                let ord = columnar::compare_rows(
                    &self.schema, shard_rc.as_ref(), idx, ref_source, ref_row,
                );
                if ord == Ordering::Equal {
                    total_w += shard_rc.get_weight(idx);
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
    ) -> (i64, Vec<(Rc<MappedShard>, usize)>) {
        let mut total_w: i64 = 0;
        let mut candidates: Vec<(Rc<MappedShard>, usize)> = Vec::new();

        self.shard_index.find_pk(key, &mut |shard_rc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_rc.count {
                if shard_rc.get_pk(idx) != key {
                    break;
                }
                total_w += shard_rc.get_weight(idx);
                candidates.push((Rc::clone(&shard_rc), idx));
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
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::I64, 0);
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    /// Build an unsorted owned `Batch` of (pk, weight, val_i64) rows for the
    /// 2-column `make_u64_i64_schema` (U64 PK + I64 payload). Rows land in
    /// the order given; `sorted` and `consolidated` are cleared so the
    /// ingest path runs the canonical sort+fold.
    fn make_batch(rows: &[(u64, i64, i64)]) -> Batch {
        let schema = make_u64_i64_schema();
        let mut batch = Batch::with_schema(schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            batch.extend_pk(pk as u128);
            batch.extend_weight(&w.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(0, &val.to_le_bytes());
            batch.count += 1;
        }
        if !rows.is_empty() {
            batch.sorted = false;
            batch.consolidated = false;
        }
        batch
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

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

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

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
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

        t.ingest_owned_batch(make_batch(&[(30, 1, 300), (10, 1, 100), (20, 1, 200)])).unwrap();

        let cursor = t.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key as u64, 10);
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

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

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
            t.ingest_owned_batch(make_batch(&[(i * 10, 1, (i * 100) as i64)])).unwrap();
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
        t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();

        // Batch 2: UPDATE delta — retract val=100, insert val=200
        // Rows sorted by (PK, payload): (-1 for val=100) before (+1 for val=200)
        t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)])).unwrap();

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

    /// `ingest_owned_batch` must sort the batch before memtable insert, even
    /// when the incoming Batch has `sorted=false` (reverse order).
    #[test]
    fn test_ingest_owned_batch_unsorted() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("ingest_owned_unsorted_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 700, 1 << 20, false,
        ).unwrap();

        // Build a reverse-sorted batch (PK order: 30, 20, 10).
        let batch = make_batch(&[(30, 1, 300), (20, 1, 200), (10, 1, 100)]);
        // make_batch produces an unsorted batch (sorted=false default).
        assert!(!batch.sorted);

        t.ingest_owned_batch(batch).unwrap();

        // Cursor must yield rows in ascending PK order
        let cursor = t.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key as u64, 10, "cursor should start at PK=10 (smallest)");
    }

    /// `ingest_owned_batch` must pre-flush when the memtable is already
    /// over its size budget, then accept the new batch (which itself may be
    /// unsorted). Pre-fill the memtable past `max_bytes` directly, then
    /// ingest a reverse-sorted batch and verify rows from both batches are
    /// retrievable in ascending PK order.
    #[test]
    fn test_ingest_owned_batch_pre_flushes_when_overflowing() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("pre_flush_test");
        let schema = make_u64_i64_schema();

        // Very small arena: 40 bytes. A 3-row batch (~120 bytes) will exceed it.
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 900, 40, false,
        ).unwrap();

        // Directly fill memtable past max_bytes using memtable_upsert_sorted_batch
        // (bypasses auto-flush so runs_bytes exceeds max_bytes).
        let fill_batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let fill_batch = fill_batch.into_consolidated(&schema);
        t.memtable_upsert_sorted_batch(fill_batch).unwrap();
        // runs_bytes (~120) > max_bytes (40) — next ingest must pre-flush
        // before upsert. The new owned-batch path checks should_flush()
        // before upserting, so the pre-fill goes to a shard cleanly.

        // Ingest a REVERSE-sorted batch. ingest_owned_batch must sort it
        // (via into_consolidated) before insert.
        t.ingest_owned_batch(make_batch(&[(50, 1, 500), (40, 1, 400), (30, 1, 300)])).unwrap();

        // fill batch (1,2,3) is now in shard; sorted (30,40,50) is in memtable.
        let cursor = t.create_cursor().unwrap();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key as u64, 1, "cursor should start at PK=1 from flushed shard");
    }

    /// Two ingest calls that cumulatively cross the 75% threshold must produce
    /// an L0 shard and an empty memtable without any explicit flush() call.
    #[test]
    fn test_memtable_overflow_auto_flush() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("overflow_auto_flush");
        let schema = make_u64_i64_schema();

        // arena = 128 bytes → should_flush threshold = 96.
        // Each row is 32 bytes (PK 8 + weight 8 + null_bmp 8 + col 8).
        // First call: 2 rows = 64 bytes, below threshold → no flush.
        // Second call: pre-check 64 < 96 → no pre-flush; upsert → 128 > 96 → post-flush.
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 1200, 128, false,
        ).unwrap();

        t.ingest_owned_batch(make_batch(&[(1, 1, 10), (2, 1, 20)])).unwrap();
        assert!(!t.memtable_is_empty(), "two rows must not yet trigger overflow");

        t.ingest_owned_batch(make_batch(&[(3, 1, 30), (4, 1, 40)])).unwrap();

        assert!(t.memtable_is_empty(), "overflow post-check must auto-flush to shard");
        assert!(
            !t.all_shard_arcs().is_empty(),
            "at least one L0 shard must exist after overflow flush",
        );
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
        t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();
        t.flush().unwrap();

        // Batch 2: UPDATE delta — retract val=100, insert val=200
        t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)])).unwrap();
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

    /// Dropping a `Pending` FlushWork without committing must unlink the
    /// shard `.tmp` and the manifest `.tmp`, leaving the directory clean
    /// for a future retry.
    #[test]
    fn flush_prepare_drop_cleans_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("drop_clean_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 1100, 1 << 20, true,
        ).unwrap();

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

        match t.flush_prepare(true).unwrap() {
            FlushOutcome::Pending(work) => {
                let dir_entries: Vec<String> = std::fs::read_dir(&tdir).unwrap()
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect();
                assert!(dir_entries.iter().any(|n| n.ends_with(".tmp")),
                    ".tmp files must exist before Drop, got {:?}", dir_entries);
                drop(work);
            }
            other => panic!("expected Pending, got non-pending outcome: {}",
                match other { FlushOutcome::Empty => "Empty",
                              FlushOutcome::DoneInline => "DoneInline",
                              _ => "?" }),
        }

        let leftover_tmp: Vec<String> = std::fs::read_dir(&tdir).unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftover_tmp.is_empty(),
            "Drop must unlink all .tmp files, found: {:?}", leftover_tmp);
    }

    /// Non-durable `flush_prepare` performs the inline write itself and
    /// returns `DoneInline`; the memtable is reset and the shard is visible
    /// to subsequent reads via the in-memory shard index.
    #[test]
    fn flush_prepare_non_durable_done_inline() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("done_inline_test");
        let schema = make_u64_i64_schema();

        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 1200, 1 << 20, false,
        ).unwrap();

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
        match t.flush_prepare(false).unwrap() {
            FlushOutcome::DoneInline => {}
            FlushOutcome::Empty => panic!("expected DoneInline, got Empty"),
            FlushOutcome::Pending(_) => panic!("expected DoneInline, got Pending"),
        }
        assert!(t.memtable_is_empty(), "memtable must be reset after non-durable flush_prepare");
        assert!(t.has_pk(10));
        assert!(t.has_pk(20));
    }
}
