//! Unified Table: owns MemTable + ShardIndex.
//!
//! Ephemeral tables skip durability; persistent tables publish manifests on flush.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::rc::Rc;

use super::columnar;
use super::error::StorageError;
use crate::schema::SchemaDescriptor;
use crate::util::{fdatasync_eintr, fsync_eintr};
use super::memtable::{self, MemTable};
use super::batch::Batch;
#[cfg(test)]
use super::batch::ConsolidatedBatch;
use super::manifest::PreparedManifest;
use super::read_cursor::{self, CursorHandle};
use super::shard_file::{self, PkUniqueChecker};
use super::shard_index::{PendingShard, ShardIndex};
use super::shard_reader::MappedShard;

/// Fold `in_memory_l0` once it exceeds this many runs. Mirrors the disk
/// `L0_COMPACT_THRESHOLD`; keeps cursor source count and per-lookup run scans
/// small, and folds cross-flush retraction churn down to net state.
const INMEM_COMPACT_THRESHOLD: usize = 4;

/// Hard per-table (per child `Table` = per partition) heap ceiling for
/// `in_memory_l0`. A flush that would exceed it folds first; if the folded net
/// state still exceeds it, the run set spills to a real `eph_shard`. Bounds heap
/// at this value per table at all times.
const EPHEMERAL_INMEM_CEILING: usize = 4 * 1024 * 1024;

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
    /// Boxed because `FlushWork` is ~300B and dwarfs the unit variants.
    Pending(Box<FlushWork>),
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
            // `dirfd` is owned by this in-flight flush (opened in
            // `flush_prepare`); unlink the orphaned .tmp through it, then close
            // it. `flush_commit` `take()`s `shard_rename` on success, so the
            // committed fd is returned to the caller, never reaching here.
            unsafe {
                libc::unlinkat(r.dirfd, r.tmp_name.as_ptr(), 0);
                libc::close(r.dirfd);
            }
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

    durable: bool,
    manifest_path: Option<String>,

    flush_seq: u32,
    pub current_lsn: u64,

    found_source: FoundSource,

    cached_full_scan: Option<Rc<Batch>>,
    /// When true, flushed and compacted shards may be tagged `SHARD_FLAG_PK_UNIQUE`
    /// if `PkUniqueChecker` confirms the invariant. Must only be set for base tables
    /// with a user-defined PK constraint enforced by the DML layer.
    can_tag_pk_unique: bool,

    /// Flushed runs of a non-durable table, held in heap instead of on disk.
    /// Populated only when `flush_prepare` is called with `durable == false`.
    /// Empty for every durable table (base tables, master system tables): those
    /// always flush through the disk branch.
    in_memory_l0: Vec<Rc<Batch>>,

    /// Test-only override for `EPHEMERAL_INMEM_CEILING`, so spill paths can be
    /// exercised without ingesting megabytes. `None` in production.
    #[cfg(test)]
    inmem_ceiling_override: Option<usize>,
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
            durable,
            manifest_path: None,
            flush_seq: 0,
            current_lsn: 1,
            found_source: FoundSource::None,
            cached_full_scan: None,
            can_tag_pk_unique: false,
            in_memory_l0: Vec::new(),
            #[cfg(test)]
            inmem_ceiling_override: None,
        };

        if durable {
            let manifest_path = format!("{}/manifest.bin", dir);
            table.shard_index.load_manifest(&manifest_path)?;
            table.shard_index.gc_orphans();
            table.current_lsn = table.shard_index.max_lsn() + 1;
            if table.current_lsn == 0 {
                table.current_lsn = 1;
            }
            table.manifest_path = Some(manifest_path);
        }

        Ok(table)
    }

    /// Enable `SHARD_FLAG_PK_UNIQUE` tagging for flushed and compacted shards.
    /// Only call this for base tables with a user-defined PK constraint enforced
    /// by the DML layer. Idempotent.
    pub fn enable_pk_unique_tagging(&mut self) {
        self.can_tag_pk_unique = true;
        self.shard_index.enable_pk_unique_tagging();
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
    /// memonly index population). Forces ephemeral flushes on overflow; that
    /// ephemeral overflow now lands in `in_memory_l0` (heap), not a shard file,
    /// unless it breaches `EPHEMERAL_INMEM_CEILING` and spills.
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

    /// Whether this table flushes durably (manifest-tracked shards). Base and
    /// master system tables are durable; view child tables and index circuits
    /// are not. Lets callers route each table through `flush_prepare(durable)`
    /// with its own durability instead of a hardcoded `true`.
    pub fn is_durable(&self) -> bool {
        self.durable
    }

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

    /// Open the partition directory fd on demand (`O_RDONLY|O_DIRECTORY`).
    /// Opened per flush/compaction rather than held for the table's lifetime,
    /// so a 256-partition table pins 0 directory fds at rest instead of 256
    /// (which exhausted the default `ulimit -n` after a handful of tables).
    /// The caller owns the returned fd and must close it.
    fn open_dirfd(&self) -> Result<libc::c_int, StorageError> {
        let dir_c = CString::new(self.directory.as_str())
            .map_err(|_| StorageError::InvalidPath)?;
        let fd = unsafe {
            libc::open(dir_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY)
        };
        if fd < 0 {
            return Err(StorageError::Io);
        }
        Ok(fd)
    }

    /// Phase 1 of two-phase flush.
    ///
    /// `durable == false` (index circuits, views, worker memonly overflow) with
    /// a non-empty consolidated snapshot:
    ///   append the snapshot to `in_memory_l0` — no file I/O, no manifest. The
    ///   memtable IS reset. The run set is folded once it passes
    ///   `INMEM_COMPACT_THRESHOLD`, and spilled to a real `eph_shard` only when
    ///   `in_memory_bytes()` exceeds `EPHEMERAL_INMEM_CEILING` (the spill path
    ///   restores page-cache reclaimability for the rare oversized table).
    ///   `DoneInline` is returned.
    ///
    /// `durable == true` and a non-empty consolidated snapshot:
    ///   write shard `.tmp` (no fdatasync), open MappedShard against it for
    ///   metadata, write manifest `.tmp`, return `Pending(FlushWork)`. The
    ///   memtable is NOT reset.
    ///
    /// Empty memtable or `consolidated.count == 0`:
    ///   reset memtable and return `Empty`. The cancelled rows are still
    ///   recoverable from the SAL.
    pub fn flush_prepare(&mut self, durable: bool) -> Result<FlushOutcome, StorageError> {
        self.found_source = FoundSource::None;
        if self.memtable.is_empty() {
            return Ok(FlushOutcome::Empty);
        }

        let snapshot = self.memtable.consolidate_for_flush();
        if snapshot.count == 0 {
            self.memtable.reset();
            return Ok(FlushOutcome::Empty);
        }

        if !durable {
            // In-memory ephemeral flush: no file I/O. `snapshot` is the
            // (PK,payload)-consolidated run; the memtable references it until
            // reset(), after which `in_memory_l0` keeps it alive.
            self.cached_full_scan = None;
            self.in_memory_l0.push(snapshot);
            self.memtable.reset();
            self.enforce_inmem_bound()?;
            return Ok(FlushOutcome::DoneInline);
        }

        let shard_name = format!("shard_{}_{}.db", self.table_id, self.current_lsn);
        let lsn_max = self.current_lsn.saturating_sub(1);

        let name_c = CString::new(shard_name.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let regions = snapshot.regions();

        // Compute PkUnique flag by observing all rows in sorted order.
        // Only run the checker for base tables; memtable runs are always ZSet.
        let flush_flags = if self.can_tag_pk_unique {
            let mut checker = PkUniqueChecker::new();
            for i in 0..snapshot.count {
                checker.observe(snapshot.get_pk_bytes(i), snapshot.get_weight(i));
            }
            checker.flags_if(true)
        } else {
            0
        };

        // Owned for this flush only. The durable branch moves ownership into
        // `ShardRename` (whose Drop closes it on a rollback) and hands it to the
        // committer.
        let dirfd = self.open_dirfd()?;

        let prepared = match shard_file::write_shard_streaming_prepare(
            dirfd, &name_c, self.table_id, snapshot.count as u32, &regions, flush_flags,
        ) {
            Ok(p) => p,
            Err(e) => {
                unsafe { libc::close(dirfd); }
                return Err(e);
            }
        };
        let shard_fd = prepared.fd;
        let tmp_name = prepared.tmp_name;

        let mut work = FlushWork {
            shard_fd: Some(shard_fd),
            shard_rename: Some(ShardRename {
                dirfd,
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

        Ok(FlushOutcome::Pending(Box::new(work)))
    }

    /// Phase 3: rename shard then manifest into final names; insert
    /// pending_shard into shard_index; reset the memtable. Returns the
    /// per-flush directory fd (opened in `flush_prepare`, owned by the
    /// returned `Some`) for the caller to `fsync` after all renames in the
    /// worker batch and then **close**. On any error path the fd is closed
    /// here (or by `FlushWork`'s Drop, when `shard_rename` is restored) so a
    /// failed commit does not leak it.
    pub fn flush_commit(&mut self, mut work: FlushWork) -> Result<Option<libc::c_int>, StorageError> {
        // Rename shard .tmp → final.
        let dirfd = if let Some(rename) = work.shard_rename.take() {
            let rc = unsafe {
                libc::renameat(
                    rename.dirfd, rename.tmp_name.as_ptr(),
                    rename.dirfd, rename.final_name.as_ptr(),
                )
            };
            if rc < 0 {
                // Restore so Drop unlinks the shard .tmp and closes the fd.
                work.shard_rename = Some(rename);
                return Err(StorageError::Io);
            }
            // Rename succeeded: the fd is no longer owned by `ShardRename`
            // (consumed by `take()`), so ownership moves to the returned value.
            Some(rename.dirfd)
        } else {
            None
        };

        // Rename manifest .tmp → final. fd already closed by close_fds.
        if let Some(m) = work.manifest.take() {
            let rc = unsafe { libc::rename(m.tmp_path.as_ptr(), m.final_path.as_ptr()) };
            if rc < 0 {
                // Restore so Drop unlinks the manifest .tmp. The shard is
                // already at its final path and will be collected by orphan GC.
                work.manifest = Some(m);
                if let Some(fd) = dirfd { unsafe { libc::close(fd); } }
                return Err(StorageError::Io);
            }
        }

        // Publish the pending shard into the index.
        if let Some(pending) = work.pending_shard.take() {
            if let Err(e) = self.shard_index.add_opened_shard(pending) {
                if let Some(fd) = dirfd { unsafe { libc::close(fd); } }
                return Err(e);
            }
        }

        self.memtable.reset();
        Ok(dirfd)
    }

    fn flush_inner(&mut self, durable: bool, sync_dir: bool) -> Result<bool, StorageError> {
        match self.flush_prepare(durable)? {
            FlushOutcome::Empty => Ok(false),
            FlushOutcome::DoneInline => Ok(true),
            FlushOutcome::Pending(mut work) => {
                if let Some(fd) = work.shard_fd() {
                    if fdatasync_eintr(fd).is_err() {
                        return Err(StorageError::Io);
                    }
                }
                if let Some(fd) = work.manifest_fd() {
                    if fdatasync_eintr(fd).is_err() {
                        return Err(StorageError::Io);
                    }
                }
                work.close_fds();
                // `flush_commit` returns the owned per-flush dir fd; fsync it
                // (when durability is requested) then close it — it is not held
                // for the table's lifetime.
                let dirfd = self.flush_commit(*work)?;
                if let Some(fd) = dirfd {
                    let ok = if sync_dir {
                        fsync_eintr(fd).is_ok()
                    } else {
                        true
                    };
                    unsafe { libc::close(fd); }
                    if !ok {
                        return Err(StorageError::Io);
                    }
                }
                Ok(true)
            }
        }
    }

    // ------------------------------------------------------------------
    // In-memory ephemeral run set (non-durable flushes)
    // ------------------------------------------------------------------

    /// The effective per-table heap ceiling — `EPHEMERAL_INMEM_CEILING` in
    /// production, or the test override when one is set.
    fn inmem_ceiling(&self) -> usize {
        #[cfg(test)]
        {
            if let Some(c) = self.inmem_ceiling_override {
                return c;
            }
        }
        EPHEMERAL_INMEM_CEILING
    }

    /// Heap footprint of `in_memory_l0` — the byte total that drives the spill
    /// ceiling. Recomputed on demand rather than cached: the run set is bounded
    /// by `INMEM_COMPACT_THRESHOLD`, so this is a handful of `total_bytes()`
    /// arithmetic calls, with no cached field for every mutation of
    /// `in_memory_l0` to keep in sync.
    fn in_memory_bytes(&self) -> usize {
        self.in_memory_l0.iter().map(|r| r.total_bytes()).sum()
    }

    /// Keep `in_memory_l0` bounded after a non-durable flush. Called by
    /// `flush_prepare`'s in-memory branch (the only site that grows the set),
    /// so on return `in_memory_l0.len() <= INMEM_COMPACT_THRESHOLD` and
    /// `in_memory_bytes() <= EPHEMERAL_INMEM_CEILING` always hold.
    fn enforce_inmem_bound(&mut self) -> Result<(), StorageError> {
        if self.in_memory_l0.len() > INMEM_COMPACT_THRESHOLD {
            self.compact_in_memory();
        }
        if self.in_memory_bytes() > self.inmem_ceiling() {
            self.spill_in_memory_to_disk()?;
        }
        Ok(())
    }

    /// Fold `in_memory_l0` to a single net-state run. Reuses the memtable's
    /// N-way consolidation primitive; drops net-zero (PK,payload) rows.
    fn compact_in_memory(&mut self) {
        if self.in_memory_l0.len() <= 1 {
            return;
        }
        let merged = memtable::consolidate_runs(&self.in_memory_l0, &self.schema);
        self.in_memory_l0.clear();
        if merged.count > 0 {
            self.in_memory_l0.push(merged);
        }
    }

    /// Ceiling breach: fold to net state first. Folding cancels cross-flush
    /// churn (an insert in flush N against its retraction in N+1) and can
    /// reclaim enough to fall back under the ceiling — in which case this
    /// returns without touching disk. Otherwise write the folded run to a real
    /// `eph_shard` (the historical non-durable disk path), register it, drop it
    /// from heap, then compact the disk tier.
    ///
    /// After a spill the table carries disk runs + future heap runs; cross-tier
    /// churn does NOT fold (neither `compact_in_memory`, which is heap-only, nor
    /// `run_compact`, which is disk-only, sees both). This is bounded: it only
    /// occurs for tables that breached the ceiling, affects disk footprint not
    /// heap (heap stays <= ceiling by construction), and the disk tier still
    /// self-compacts. Eliminating it would need a unified disk+heap compaction —
    /// out of scope.
    ///
    /// Transactional. The consolidated run stays in `in_memory_l0` until the
    /// shard is both written and registered; heap is dropped only on the commit
    /// path. A `write_shard_streaming` failure leaves the run in heap for retry
    /// with nothing on disk to clean up; an `add_shard` failure unlinks the
    /// just-written shard before returning. No error can lose the integrated
    /// state or strand an orphan file.
    fn spill_in_memory_to_disk(&mut self) -> Result<(), StorageError> {
        self.compact_in_memory();
        // Folding may have dropped the net state back under the ceiling (heavy
        // churn cancels to near-nothing); if so there is nothing to spill.
        if self.in_memory_bytes() <= self.inmem_ceiling() {
            return Ok(());
        }
        // Above the ceiling the byte total is > 0, so `compact_in_memory` left
        // exactly one net-state run (count > 0) in heap — `flush_prepare` never
        // enqueues an empty snapshot and `compact_in_memory` only re-pushes
        // `merged.count > 0`. Borrow it — do not remove — so a write/register
        // failure below leaves heap intact for retry.
        let run = Rc::clone(
            self.in_memory_l0
                .first()
                .expect("compaction leaves one net-state run when above the spill ceiling"),
        );

        self.flush_seq += 1;
        let pid = unsafe { libc::getpid() };
        let shard_name = format!(
            "eph_shard_{}_{}_{}_{}.db",
            self.table_id, pid, self.flush_seq, self.current_lsn
        );
        let name_c = CString::new(shard_name.as_str()).map_err(|_| StorageError::InvalidPath)?;

        let dirfd = self.open_dirfd()?;
        let res = shard_file::write_shard_streaming(
            dirfd, &name_c, self.table_id, run.count as u32, &run.regions(), false, 0,
        );
        unsafe { libc::close(dirfd); }
        res?; // Write failed: heap still owns `run`; no on-disk residue.

        let final_full = format!("{}/{}", self.directory, shard_name);
        if let Err(e) = self.shard_index.add_shard(&final_full, 0, 0) {
            // Registration failed: unlink the shard we wrote, keep heap intact.
            let _ = std::fs::remove_file(&final_full);
            return Err(e);
        }

        // Commit: the run is on disk and registered — safe to drop from heap.
        self.in_memory_l0.clear();

        // Bound the spilled disk L0. A repeatedly-spilling table is read only
        // via the non-compacting `open_cursor`, so without this its `eph_shard`s
        // accumulate unbounded and every cursor merges them all.
        // `compact_if_needed` is a no-op until the disk tier crosses its own
        // threshold and skips the manifest (None) for this non-durable table.
        self.compact_if_needed()?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    /// Open a read-only cursor over (memtable runs + shards).
    /// Does NOT mutate the table — compaction is a maintenance operation,
    /// not part of the read path. Cheap and infallible.
    ///
    /// This is the recommended default. Use `create_cursor_compacting` only
    /// when the caller explicitly wants L0→L1 to run synchronously (manual
    /// FLUSH, foreground checkpoint, etc.). The snapshot-driven cursor itself
    /// doesn't care whether L0 has accumulated past the compact threshold.
    pub fn open_cursor(&self) -> CursorHandle {
        let mt = self.memtable.snapshot_runs();
        let shard_arcs = self.shard_index.all_shard_arcs();
        if self.in_memory_l0.is_empty() {
            return read_cursor::create_cursor_from_snapshots(mt, &shard_arcs, self.schema);
        }
        let mut snaps: Vec<Rc<Batch>> = Vec::with_capacity(mt.len() + self.in_memory_l0.len());
        snaps.extend(mt.iter().cloned());
        snaps.extend(self.in_memory_l0.iter().cloned());
        read_cursor::create_cursor_from_snapshots(&snaps, &shard_arcs, self.schema)
    }

    /// Open a cursor after running `compact_if_needed`. Intended for
    /// maintenance / flush paths that want an up-to-date L1.
    ///
    /// **Do not use from validators.** A compaction `Err` returned here is a
    /// correctness hazard if the caller treats cursor absence as "no match":
    /// a transient Io turns "value exists" into "value absent", silently
    /// corrupting unique-index / FK checks. Use `open_cursor` instead.
    pub fn create_cursor_compacting(&mut self) -> Result<CursorHandle, StorageError> {
        self.compact_if_needed()?;
        Ok(self.open_cursor())
    }

    /// Return the fully consolidated batch of all live rows, caching the result.
    /// The cache is invalidated on any logical write (upsert or test-helper upsert).
    /// Cheap on repeated calls: returns `Rc::clone` of the cached batch.
    /// Infallible: delegates to `open_cursor`.
    pub fn full_scan(&mut self) -> Rc<Batch> {
        if let Some(ref rc) = self.cached_full_scan {
            return Rc::clone(rc);
        }
        let rc = self.open_cursor().cursor.materialize();
        self.cached_full_scan = Some(Rc::clone(&rc));
        rc
    }

    /// Borrow the current memtable runs (for PartitionedTable cursor
    /// gathering).  See `MemTable::snapshot_runs` for lifetime rules.
    pub fn snapshot_runs(&self) -> &[Rc<Batch>] {
        self.memtable.snapshot_runs()
    }

    /// Borrow the non-durable in-memory flush runs (for PartitionedTable cursor
    /// gathering). Empty for every durable table.
    pub fn in_memory_runs(&self) -> &[Rc<Batch>] {
        &self.in_memory_l0
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

    /// Test helper: shrink the per-table heap ceiling so spill paths can be
    /// exercised without ingesting megabytes.
    #[cfg(test)]
    pub fn set_inmem_ceiling_for_test(&mut self, bytes: usize) {
        self.inmem_ceiling_override = Some(bytes);
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
        let (opk, n) = columnar::opk_key(&self.schema, key);
        self.has_pk_bytes(&opk[..n])
    }

    /// Byte-keyed PK existence check. The OPK-keyed memtable bloom gates the
    /// sorted-run lookup at every PK width (a wide-PK probe matches the 16-byte
    /// prefix the insert side hashed, so there are no false negatives).
    pub fn has_pk_bytes(&mut self, key: &[u8]) -> bool {
        let mut w: i64 = 0;
        if self.memtable.may_contain_pk(key) {
            let (mt_w, _, _) = self.memtable.lookup_pk_bytes(key);
            w += mt_w;
        }
        let (shard_w, _) = self.scan_shards_for_pk_bytes(key, false);
        w += shard_w;
        // A non-durable table can hold positive-weight rows only in
        // `in_memory_l0` (after a flush, before any spill), so it must be
        // scanned too. No-op for durable tables (`in_memory_l0` empty).
        w += self.scan_inmem_weight(key);
        w > 0
    }

    /// Net weight for `key` across `in_memory_l0`. Empty (returns 0) for every
    /// durable table.
    fn scan_inmem_weight(&self, key: &[u8]) -> i64 {
        let mut w = 0;
        for run in &self.in_memory_l0 {
            for lo in memtable::run_pk_match_rows(run, key) {
                w += run.get_weight(lo);
            }
        }
        w
    }

    /// Look up a PK for retraction.  Returns (net_weight, found).
    /// If found, sets `found_source` so `found_*` accessors can read the row.
    ///
    /// Takes a **native** `u128` and `opk_key`s it; the DML retraction path now
    /// keys on verbatim OPK bytes via `retract_pk_bytes`, so this native entry
    /// point has no production caller and is retained only for unit tests.
    #[cfg(test)]
    pub fn retract_pk(&mut self, key: u128) -> (i64, bool) {
        let (opk, n) = columnar::opk_key(&self.schema, key);
        self.retract_pk_bytes(&opk[..n])
    }

    /// Look up a PK for retraction by its OPK `key` bytes. Returns
    /// `(net_weight, found)`; on a hit sets `found_source` so the `found_*`
    /// accessors expose the live (PK, payload) row. The OPK-keyed memtable
    /// bloom gates the run lookup at every PK width.
    pub fn retract_pk_bytes(&mut self, key: &[u8]) -> (i64, bool) {
        // Reached only on `unique_pk` base tables (via
        // `enforce_unique_pk_partitioned`), which are always durable and never
        // acquire `in_memory_l0`. `get_weight_for_row_bytes` /
        // `scan_shards_for_pk_bytes` (the found-row path) likewise scan only the
        // memtable and disk shards; a future caller on an in-memory-bearing
        // table must extend them and add a `FoundSource::InMemRun` variant.
        debug_assert!(
            self.in_memory_l0.is_empty(),
            "retract_pk_bytes is base-table-only; base tables never flush in-memory",
        );
        self.found_source = FoundSource::None;

        let (mt_w, _, mt_row_count) =
            if self.memtable.may_contain_pk(key) {
                self.memtable.lookup_pk_bytes(key)
            } else {
                (0, false, 0)
            };

        let need_candidates = !(mt_row_count == 1 && mt_w > 0);
        let mut total_w = mt_w;
        let (shard_w, shard_candidates) = self.scan_shards_for_pk_bytes(key, need_candidates);
        total_w += shard_w;

        if total_w <= 0 {
            return (total_w, false);
        }

        let in_memtable = if mt_row_count == 1 && mt_w > 0 {
            true
        } else if mt_row_count > 1 {
            self.memtable.find_positive_payload_row_bytes(key)
        } else {
            false
        };

        if in_memtable {
            self.found_source = FoundSource::MemTable;
        } else if shard_candidates.len() == 1 {
            let (rc, idx) = &shard_candidates[0];
            self.found_source = FoundSource::Shard(Rc::clone(rc), *idx);
        } else {
            for (rc, idx) in &shard_candidates {
                let net_w = self.get_weight_for_row_bytes(key, rc.as_ref(), *idx);
                if net_w > 0 {
                    self.found_source = FoundSource::Shard(Rc::clone(rc), *idx);
                    break;
                }
            }
        }

        (total_w, true)
    }

    /// Net weight for a specific (PK, payload) row, keyed by OPK `key` bytes.
    /// The OPK-keyed memtable bloom gates the run scan at every PK width.
    pub fn get_weight_for_row_bytes<S: columnar::ColumnarSource>(
        &mut self,
        key: &[u8],
        ref_source: &S,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        if self.memtable.may_contain_pk(key) {
            total_w += self.memtable.find_weight_for_row_bytes(key, ref_source, ref_row);
        }

        self.shard_index.find_pk_bytes(key, &mut |shard_rc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_rc.count && shard_rc.get_pk_bytes(idx) == key {
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

    pub fn found_blob_slice(&self) -> &[u8] {
        match &self.found_source {
            FoundSource::MemTable      => self.memtable.found_blob_slice(),
            FoundSource::Shard(arc, _) => arc.blob_slice(),
            FoundSource::None          => &[],
        }
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
            // Open the dir fd just for this fsync, then close it — not held
            // for the table's lifetime.
            let dirfd = self.open_dirfd()?;
            let ok = fsync_eintr(dirfd).is_ok();
            unsafe { libc::close(dirfd); }
            if !ok {
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
        // ShardIndex + MemTable drop with the Table. Directory fds are opened
        // per flush/compaction and closed there, so nothing is held to close.
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Scan shards for a PK by its OPK `key` bytes.
    fn scan_shards_for_pk_bytes(
        &self,
        key: &[u8],
        need_candidates: bool,
    ) -> (i64, Vec<(Rc<MappedShard>, usize)>) {
        let mut total_w: i64 = 0;
        let mut candidates: Vec<(Rc<MappedShard>, usize)> = Vec::new();

        self.shard_index.find_pk_bytes(key, &mut |shard_rc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_rc.count && shard_rc.get_pk_bytes(idx) == key {
                total_w += shard_rc.get_weight(idx);
                if need_candidates {
                    candidates.push((Rc::clone(&shard_rc), idx));
                }
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
    //
    // In steady state a non-durable table writes no shard files at all — its
    // flushes land in `in_memory_l0` (heap). This sweep reclaims (a) spill-path
    // `eph_shard_*` left by a crashed prior process, (b) pre-upgrade
    // `eph_shard_*`/`shard_*` from before in-memory flushes existed, and
    // (c) `hcomp_*` from disk compaction of spilled data — not routine flushes.
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
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
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

    /// Count files named `eph_shard_*` directly in `dir`.
    fn count_eph_shards(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.flatten()
                    .filter(|e| e.file_name().to_string_lossy().starts_with("eph_shard_"))
                    .count()
            })
            .unwrap_or(0)
    }

    fn no_eph_shard(dir: &std::path::Path) -> bool {
        count_eph_shards(dir) == 0
    }

    /// Materialize `open_cursor` into a (pk -> net_weight) map.
    fn materialize_weights(t: &Table) -> std::collections::HashMap<u64, i64> {
        let mut out = std::collections::HashMap::new();
        let batch = t.open_cursor().cursor.materialize();
        for i in 0..batch.count {
            out.insert(batch.get_pk(i) as u64, batch.get_weight(i));
        }
        out
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

        let cursor = t.open_cursor();
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

        // Durable: each flush writes a real `shard_*` so `run_compact` (L0→L1)
        // is exercised. Under non-durable flush these tiny rows would stay in
        // `in_memory_l0` and never reach the disk compaction path.
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 500, 256, true,
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
        let cursor = t.open_cursor();
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
        let cursor = t.open_cursor();
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

        assert!(t.memtable_is_empty(), "overflow post-check must auto-flush");
        // Non-durable flushes land in the in-memory run set, not disk shards.
        assert!(
            !t.in_memory_runs().is_empty(),
            "at least one in-memory L0 run must exist after overflow flush",
        );
        assert!(
            t.all_shard_arcs().is_empty(),
            "sub-ceiling ephemeral flush must not write a disk shard",
        );
        // Data from both batches must still be readable.
        for pk in [1u128, 2, 3, 4] {
            assert!(t.has_pk(pk), "PK {pk} must survive the in-memory flush");
        }
    }

    /// Bug 2: INSERT (PK=10, val=100) → flush → UPDATE delta → flush → retract_pk.
    /// The shard fallback must pick the live payload (val=200), not the cancelled one.
    #[test]
    fn test_retract_pk_shard_fallback_multiple_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("retract_shard_fallback");
        let schema = make_u64_i64_schema();

        // Durable: `retract_pk` is base-table-only (base tables are durable), so
        // the flushed rows must land in a real shard for the shard-fallback path
        // under test — not `in_memory_l0` (which a non-durable flush would use).
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 1000, 1 << 20, true,
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

    /// Table::new on a corrupted manifest must return Err and must not run
    /// gc_orphans — any stray shard files must survive untouched.
    #[test]
    fn table_new_corrupted_manifest_preserves_stray_shard() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("corrupted_manifest_test");
        std::fs::create_dir_all(&tdir).unwrap();
        let schema = make_u64_i64_schema();

        // Write a corrupted manifest (wrong magic).
        let manifest_path = tdir.join("manifest.bin");
        std::fs::write(&manifest_path, b"not a valid manifest").unwrap();

        // Drop a stray shard file.
        let stray = tdir.join("shard_200_1.db");
        std::fs::write(&stray, b"orphan").unwrap();

        let result = Table::new(
            tdir.to_str().unwrap(), "test", schema, 200, 1 << 20, true,
        );
        assert!(result.is_err(), "Table::new must fail on corrupted manifest");
        assert!(stray.exists(), "stray shard must survive when gc_orphans did not run");
    }

    /// Non-durable `flush_prepare` consolidates the snapshot into the in-memory
    /// run set and returns `DoneInline`; the memtable is reset, no shard file is
    /// written, and the rows stay visible to subsequent reads.
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
        assert!(!t.in_memory_runs().is_empty(), "snapshot must land in in_memory_l0");
        assert!(no_eph_shard(&tdir), "non-durable flush must not write an eph_shard file");
        assert!(t.has_pk(10));
        assert!(t.has_pk(20));
    }

    /// Wide (`pk_stride = 24`) PK: `has_pk_bytes`/`retract_pk_bytes` must work
    /// across both the active memtable runs and flushed shards, and prefix-twins
    /// (sharing the OPK 16-byte prefix, differing in the trailing column) must be
    /// independently tracked.
    #[test]
    fn table_wide_pk_has_and_retract_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("wide_pk_test");
        // (U64, U64, U64) PK + I64 payload.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        );
        assert_eq!(schema.pk_stride(), 24);

        let pk3 = |a: u64, b: u64, c: u64| {
            let mut v = a.to_le_bytes().to_vec();
            v.extend_from_slice(&b.to_le_bytes());
            v.extend_from_slice(&c.to_le_bytes());
            v
        };
        let wide_batch = |rows: &[(Vec<u8>, i64, i64)]| -> Batch {
            let mut b = Batch::with_schema(schema, rows.len().max(1));
            for (pk, w, val) in rows {
                b.extend_pk_bytes(pk);
                b.extend_weight(&w.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &val.to_le_bytes());
                b.count += 1;
            }
            if !rows.is_empty() {
                b.sorted = false;
                b.consolidated = false;
            }
            b
        };

        // Durable: this exercises `retract_pk_bytes` over flushed data, which is
        // base-table-only (base tables are durable). A durable flush writes a
        // real shard so the shard scan path is what's tested; a non-durable
        // flush would route the rows into `in_memory_l0` instead.
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 4242, 1 << 20, true,
        ).unwrap();

        // Two prefix-twins (1,1,100)/(1,1,200) and a distinct (2,0,0).
        t.ingest_owned_batch(wide_batch(&[
            (pk3(1, 1, 100), 1, 10),
            (pk3(1, 1, 200), 1, 20),
            (pk3(2, 0, 0), 1, 30),
        ])).unwrap();

        // Memtable lookups.
        assert!(t.has_pk_bytes(&pk3(1, 1, 100)));
        assert!(t.has_pk_bytes(&pk3(1, 1, 200)));
        assert!(t.has_pk_bytes(&pk3(2, 0, 0)));
        assert!(!t.has_pk_bytes(&pk3(1, 1, 300)), "absent prefix-twin must not be found");
        assert!(!t.has_pk_bytes(&pk3(9, 9, 9)));

        // Flush to a shard, then re-check the same keys from disk.
        t.flush().unwrap();
        assert!(t.has_pk_bytes(&pk3(1, 1, 100)));
        assert!(t.has_pk_bytes(&pk3(1, 1, 200)));
        assert!(t.has_pk_bytes(&pk3(2, 0, 0)));
        assert!(!t.has_pk_bytes(&pk3(1, 1, 300)));

        // Retract one prefix-twin (DBSP -1 in the memtable); the other survives.
        t.ingest_owned_batch(wide_batch(&[(pk3(1, 1, 100), -1, 10)])).unwrap();
        assert!(!t.has_pk_bytes(&pk3(1, 1, 100)), "retracted twin must be gone");
        assert!(t.has_pk_bytes(&pk3(1, 1, 200)), "the other twin must survive");

        // retract_pk_bytes reports the live (PK, payload) row.
        let (w, found) = t.retract_pk_bytes(&pk3(1, 1, 200));
        assert_eq!(w, 1);
        assert!(found);
        let (w2, found2) = t.retract_pk_bytes(&pk3(1, 1, 100));
        assert_eq!(w2, 0);
        assert!(!found2);

        t.close();
    }

    // ── In-memory ephemeral flush (durable=false) ────────────────────────

    /// A sub-ceiling non-durable flush writes no file at all; rows are served
    /// from `in_memory_l0` via the cursor.
    #[test]
    fn nondurable_flush_writes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("no_file_test");
        let schema = make_u64_i64_schema();
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200), (30, 1, 300)])).unwrap();
        assert!(matches!(t.flush_prepare(false).unwrap(), FlushOutcome::DoneInline));

        let shard_files: Vec<String> = std::fs::read_dir(&tdir).unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("eph_shard_") || n.starts_with("shard_"))
            .collect();
        assert!(shard_files.is_empty(), "non-durable flush wrote files: {shard_files:?}");
        assert!(t.all_shard_arcs().is_empty());
        assert!(!t.in_memory_runs().is_empty());

        let w = materialize_weights(&t);
        assert_eq!(w.get(&10), Some(&1));
        assert_eq!(w.get(&20), Some(&1));
        assert_eq!(w.get(&30), Some(&1));
    }

    /// Cross-flush churn (insert in one flush, retraction in another) folds at
    /// `INMEM_COMPACT_THRESHOLD` to net state — the cancelled key is gone, not
    /// accumulated.
    #[test]
    fn nondurable_cross_flush_fold_nets_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cross_fold_test");
        let schema = make_u64_i64_schema();
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();

        // 6 alternating +1 / -1 flushes on (k=7, v=70): each lands in its own
        // run until the threshold folds them. Net weight is 0.
        for i in 0..6 {
            let w = if i % 2 == 0 { 1 } else { -1 };
            t.ingest_owned_batch(make_batch(&[(7, w, 70)])).unwrap();
            assert!(t.flush().unwrap());
        }
        assert!(!t.has_pk(7), "net-zero key must not be present");
        let weights = materialize_weights(&t);
        assert!(!weights.contains_key(&7), "net-zero key folds away (0 rows)");
        assert!(
            t.in_memory_runs().len() <= INMEM_COMPACT_THRESHOLD,
            "run set must stay folded",
        );
        assert!(no_eph_shard(&tdir), "churn must not spill (tiny, sub-ceiling)");
    }

    /// More than `INMEM_COMPACT_THRESHOLD` distinct-key flushes keep the run
    /// count bounded by folding, and every key survives.
    #[test]
    fn nondurable_run_count_stays_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("run_bound_test");
        let schema = make_u64_i64_schema();
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();

        let n = INMEM_COMPACT_THRESHOLD as u64 + 4;
        for k in 0..n {
            t.ingest_owned_batch(make_batch(&[(k, 1, (k * 10) as i64)])).unwrap();
            assert!(t.flush().unwrap());
            assert!(
                t.in_memory_runs().len() <= INMEM_COMPACT_THRESHOLD,
                "run count exceeded threshold after flush {k}",
            );
        }
        for k in 0..n {
            assert!(t.has_pk(k as u128), "key {k} must survive folding");
        }
    }

    /// A flush past `EPHEMERAL_INMEM_CEILING` (shrunk via the test seam) spills
    /// the folded run to an `eph_shard`, drains heap, and keeps rows readable.
    #[test]
    fn nondurable_ceiling_spill_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("ceiling_spill_test");
        let schema = make_u64_i64_schema();
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();
        t.set_inmem_ceiling_for_test(100); // < one flush (~10 rows × 32 B)

        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert!(t.flush().unwrap());

        assert!(count_eph_shards(&tdir) > 0, "ceiling breach must spill to an eph_shard");
        assert_eq!(t.in_memory_bytes(), 0, "heap drained after spill");
        assert!(t.in_memory_runs().is_empty());
        assert!(!t.all_shard_arcs().is_empty());
        for k in 0..10u128 {
            assert!(t.has_pk(k), "row {k} must remain readable from disk after spill");
        }
    }

    /// Repeated over-ceiling flushes keep the on-disk eph_shard count bounded
    /// (the spill path's `compact_if_needed` folds L0→L1 and cleans up), and all
    /// rows across rounds stay correct.
    #[test]
    fn nondurable_repeated_spill_stays_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("repeated_spill_test");
        let schema = make_u64_i64_schema();
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();
        t.set_inmem_ceiling_for_test(100);

        const ROUNDS: u64 = 20;
        const PER: u64 = 10;
        for r in 0..ROUNDS {
            let rows: Vec<(u64, i64, i64)> = (0..PER)
                .map(|j| (r * PER + j, 1, ((r * PER + j) * 10) as i64))
                .collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            assert!(t.flush().unwrap());
            assert_eq!(t.in_memory_bytes(), 0, "round {r}: heap drained after spill");
            // Raw eph_shards do not accumulate with rounds — disk L0 self-folds.
            assert!(
                count_eph_shards(&tdir) <= 5,
                "round {r}: {} eph_shards accumulated", count_eph_shards(&tdir),
            );
        }

        let weights = materialize_weights(&t);
        assert_eq!(weights.len() as u64, ROUNDS * PER, "all rows present");
        assert!(weights.values().all(|&w| w == 1), "every row nets +1");
    }

    /// `has_pk` reflects net weight held only in `in_memory_l0`: true after an
    /// insert flush, false after the net-zero retraction flush.
    #[test]
    fn nondurable_has_pk_over_in_memory_runs() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("has_pk_inmem_test");
        let schema = make_u64_i64_schema();
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();

        t.ingest_owned_batch(make_batch(&[(5, 1, 50)])).unwrap();
        assert!(t.flush().unwrap());
        assert!(t.memtable_is_empty());
        assert!(t.has_pk(5), "in-memory positive-weight row must be found");

        t.ingest_owned_batch(make_batch(&[(5, -1, 50)])).unwrap();
        assert!(t.flush().unwrap());
        assert!(!t.has_pk(5), "net-zero key across in-memory runs must be absent");
    }

    /// After a spill, fresh heap runs coexist with disk shards; `open_cursor`
    /// returns their union with correct net weights, including a cross-tier
    /// retraction (heap retraction cancelling a disk insert).
    #[test]
    fn nondurable_mixed_disk_and_heap_read() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("mixed_read_test");
        let schema = make_u64_i64_schema();
        let mut t = Table::new(
            tdir.to_str().unwrap(), "test", schema, 100, 1 << 20, false,
        ).unwrap();

        // Force keys 0..10 to disk.
        t.set_inmem_ceiling_for_test(100);
        let disk_rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&disk_rows)).unwrap();
        assert!(t.flush().unwrap());
        assert!(!t.all_shard_arcs().is_empty(), "first flush must spill to disk");
        assert_eq!(t.in_memory_bytes(), 0);

        // Raise ceiling so subsequent flushes stay in heap.
        t.set_inmem_ceiling_for_test(usize::MAX);
        let heap_rows: Vec<(u64, i64, i64)> = (100..105).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&heap_rows)).unwrap();
        assert!(t.flush().unwrap());
        assert!(!t.in_memory_runs().is_empty(), "second flush stays in heap");

        // Cross-tier retraction: cancel disk key 3 (payload 30) from heap.
        t.ingest_owned_batch(make_batch(&[(3, -1, 30)])).unwrap();
        assert!(t.flush().unwrap());

        assert!(!t.has_pk(3), "disk insert + heap retraction nets zero");
        let weights = materialize_weights(&t);
        for k in 0..10u64 {
            if k == 3 {
                assert!(!weights.contains_key(&3), "retracted disk key must be gone");
            } else {
                assert_eq!(weights.get(&k), Some(&1), "disk key {k}");
            }
        }
        for k in 100..105u64 {
            assert_eq!(weights.get(&k), Some(&1), "heap key {k}");
        }
    }
}
