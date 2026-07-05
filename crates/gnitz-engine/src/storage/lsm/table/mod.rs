//! Unified Table: owns MemTable + ShardIndex.
//!
//! Between checkpoints every table keeps its ingest overflow in the RAM tier
//! (`in_memory_l0`); the checkpoint barrier folds that tier into one durable
//! shard for `SalReplay` tables. `Rederive` tables never publish a manifest —
//! they are erased at open and rebuilt from their sources.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use super::batch::Batch;
use super::bloom::BloomFilter;
use super::columnar;
use super::error::StorageError;
use super::manifest::PreparedManifest;
use super::memtable::{self, MemTable};
use super::merge::pack_pk_be;
use super::read_cursor::{self, CursorHandle};
use super::shard_index::{ShardEntry, ShardIndex};
use super::shard_reader::MappedShard;
use crate::foundation::posix_io::fsync_eintr;
use crate::schema::SchemaDescriptor;

/// Fold `in_memory_l0` once it exceeds this many runs. Mirrors the disk
/// `L0_COMPACT_THRESHOLD`; keeps cursor source count and per-lookup run scans
/// small, and folds cross-flush retraction churn down to net state.
const INMEM_COMPACT_THRESHOLD: usize = 4;

/// Hard per-table (per child `Table` = per partition) heap ceiling for
/// `in_memory_l0`. A flush that would exceed it folds first; if the folded net
/// state still exceeds it, the run set spills to a shard file. Bounds heap at
/// this value per table at all times. The aggregate un-spilled RAM across the
/// cluster is bounded by the un-checkpointed SAL tail: every ingested byte
/// flows through the fsynced SAL, and a spill frees the RAM.
const INMEM_CEILING: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// RecoverySource
// ---------------------------------------------------------------------------

/// How a relation's tail is recovered across a restart. The single fact that
/// `Table::new` and the boot rebuild read — they can no longer disagree the
/// way a positional `durable: bool` let them. This controls recovery, not the
/// flush path: between checkpoints every table keeps its overflow in the RAM
/// tier, and the checkpoint barrier folds it into a durable shard only for
/// `SalReplay` tables.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoverySource {
    /// The tail is recovered by replaying the fsynced SAL over the shards loaded
    /// from the manifest at open. Base tables and master system tables.
    SalReplay,
    /// Storage is erased at open and the relation is rebuilt (rederived) from
    /// its sources. Views and index circuits.
    Rederive,
}

// ---------------------------------------------------------------------------
// Two-phase flush API
// ---------------------------------------------------------------------------

/// Outcome of `Table::flush_prepare`.
pub enum FlushOutcome {
    /// `SalReplay` table with memtable and RAM tier net-empty after the fold —
    /// nothing to publish; the SAL covers it.
    Empty,
    /// `Rederive` table folded into the RAM tier; no file I/O, no deferred work.
    DoneInline,
    /// `SalReplay`: .tmp files written, fdatasync + rename pending.
    /// Boxed because `FlushWork` is ~300B and dwarfs the unit variants.
    Pending(Box<FlushWork>),
}

/// Open file descriptors and rename targets pending for one durable flush.
/// Owned by the worker between `flush_prepare` and `flush_commit`. Drop
/// unlinks any remaining `.tmp` files so a partial failure leaves no debris.
pub struct FlushWork {
    shard_fd: Option<OwnedFd>,
    shard_rename: Option<ShardRename>,
    manifest: Option<PreparedManifest>,
    /// The flush's shard, written and mmap'd at its `.tmp` path but not yet
    /// inserted into the index — held here from `flush_prepare` until
    /// `flush_commit` renames the `.tmp` into place and inserts it.
    pending_shard: Option<ShardEntry>,
}

pub struct ShardRename {
    dirfd: OwnedFd,
    tmp_name: CString,
    final_name: CString,
}

impl FlushWork {
    /// Close any still-open fds. Called by the worker after the io_uring
    /// fdatasync batch completes and before renames start.
    pub fn close_fds(&mut self) {
        self.shard_fd = None;
        if let Some(m) = &mut self.manifest {
            m.fd = None;
        }
    }

    pub fn shard_fd(&self) -> Option<libc::c_int> {
        self.shard_fd.as_ref().map(|fd| fd.as_raw_fd())
    }
    pub fn manifest_fd(&self) -> Option<libc::c_int> {
        self.manifest
            .as_ref()
            .and_then(|m| m.fd.as_ref())
            .map(|fd| fd.as_raw_fd())
    }
}

impl Drop for FlushWork {
    fn drop(&mut self) {
        if let Some(r) = self.shard_rename.take() {
            // Unlink the orphaned shard .tmp through the per-flush dir fd.
            // `flush_commit` `take()`s `shard_rename` on success, so a committed
            // flush never reaches here. All fds close via their `OwnedFd` drops.
            unsafe {
                libc::unlinkat(r.dirfd.as_raw_fd(), r.tmp_name.as_ptr(), 0);
            }
        }
        if let Some(m) = self.manifest.take() {
            unsafe {
                libc::unlink(m.tmp_path.as_ptr());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FoundSource — tracks where retract_pk found its row
// ---------------------------------------------------------------------------

/// One in-RAM L0 run plus a PK bloom over its own rows, so a point probe skips
/// the sorted-run search on a miss. The bloom is keyed exactly like the memtable
/// bloom (`pack_pk_be` of the OPK bytes) so probes never false-negative at any
/// PK width. Built fresh over the run (one hashing pass) at flush-push and at
/// each fold — correctness never needs an XOR8 or a PK-unique certificate here.
struct InMemRun {
    batch: Rc<Batch>,
    bloom: BloomFilter,
}

impl InMemRun {
    fn from_batch(batch: Rc<Batch>) -> Self {
        let mut bloom = BloomFilter::new((batch.count as u32).max(16));
        memtable::bloom_add_batch(&mut bloom, &batch);
        Self { batch, bloom }
    }
    #[inline]
    fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(pack_pk_be(key))
    }
}

enum FoundSource {
    None,
    MemTable,
    Shard(Rc<MappedShard>, usize),
    InMemRun(Rc<Batch>, usize),
}

/// A [`ColumnarSource`](super::columnar::ColumnarSource) view over the row a
/// `retract_pk*` call most recently located (the "found row"). Both arms wrap a
/// type that already implements `ColumnarSource` — a memtable run (`Batch`) or a
/// `MappedShard` — so the found row flows as the canonical columnar abstraction
/// rather than a raw-pointer triple, and the stored (PK, payload) can be copied
/// into a batch through `Batch::append_row_from_source_bytes`. A `FoundRow` is
/// pinned to its single located row, so the trait's `row` argument is ignored.
pub(crate) enum FoundRow<'a> {
    Mem(&'a Batch, usize),
    Shard(&'a MappedShard, usize),
}

impl columnar::ColumnarSource for FoundRow<'_> {
    fn get_pk_bytes(&self, _row: usize) -> &[u8] {
        match *self {
            FoundRow::Mem(b, r) => columnar::ColumnarSource::get_pk_bytes(b, r),
            FoundRow::Shard(s, r) => columnar::ColumnarSource::get_pk_bytes(s, r),
        }
    }
    fn get_weight(&self, _row: usize) -> i64 {
        match *self {
            FoundRow::Mem(b, r) => columnar::ColumnarSource::get_weight(b, r),
            FoundRow::Shard(s, r) => columnar::ColumnarSource::get_weight(s, r),
        }
    }
    fn get_null_word(&self, _row: usize) -> u64 {
        match *self {
            FoundRow::Mem(b, r) => columnar::ColumnarSource::get_null_word(b, r),
            FoundRow::Shard(s, r) => columnar::ColumnarSource::get_null_word(s, r),
        }
    }
    fn get_col_ptr(&self, _row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        match *self {
            FoundRow::Mem(b, r) => columnar::ColumnarSource::get_col_ptr(b, r, payload_col, col_size),
            FoundRow::Shard(s, r) => columnar::ColumnarSource::get_col_ptr(s, r, payload_col, col_size),
        }
    }
    fn blob_slice(&self) -> &[u8] {
        match *self {
            FoundRow::Mem(b, _) => columnar::ColumnarSource::blob_slice(b),
            FoundRow::Shard(s, _) => columnar::ColumnarSource::blob_slice(s),
        }
    }
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

    recovery_source: RecoverySource,
    manifest_path: Option<String>,

    pub current_lsn: u64,

    found_source: FoundSource,

    cached_full_scan: Option<Rc<Batch>>,
    /// When true, flushed and compacted shards may be tagged `SHARD_FLAG_PK_UNIQUE`
    /// if `PkUniqueChecker` confirms the invariant. Must only be set for base tables
    /// with a user-defined PK constraint enforced by the DML layer.
    can_tag_pk_unique: bool,

    /// Flushed runs held in heap instead of on disk, each paired with a per-run
    /// PK bloom for point-probe gating. Populated for **every** table on ingest
    /// overflow (`flush_to_ram`), bounded by `INMEM_CEILING` (spill). The
    /// checkpoint barrier folds this tier into one durable shard for `SalReplay`
    /// tables.
    in_memory_l0: Vec<InMemRun>,

    /// Test-only override for `INMEM_CEILING`, so spill paths can be
    /// exercised without ingesting megabytes. `None` in production.
    #[cfg(test)]
    inmem_ceiling_override: Option<usize>,
}

mod flush;

impl Table {
    /// Create a new table.  `RecoverySource::SalReplay` loads the manifest at
    /// open; `RecoverySource::Rederive` erases stale storage and starts empty.
    pub fn new(
        dir: &str,
        _name: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        arena_size: u64,
        recovery_source: RecoverySource,
    ) -> Result<Self, StorageError> {
        let dir_c = ensure_dir(dir)?;

        // Try to set NOCOW (btrfs; silently ignored on other fs)
        set_nocow_dir(&dir_c);

        // Rederive tables erase stale shards (prefix-matched) and start empty.
        if recovery_source == RecoverySource::Rederive {
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
            recovery_source,
            manifest_path: None,
            current_lsn: 1,
            found_source: FoundSource::None,
            cached_full_scan: None,
            can_tag_pk_unique: false,
            in_memory_l0: Vec::new(),
            #[cfg(test)]
            inmem_ceiling_override: None,
        };

        if recovery_source == RecoverySource::SalReplay {
            let manifest_path = format!("{dir}/manifest.bin");
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
        self.upsert_owned_and_maybe_flush(batch)
    }

    /// Ingest a borrowed Batch, copying it exactly once: an unconsolidated
    /// batch is consolidated straight into the owned copy the memtable keeps
    /// (the consolidation pass IS the copy), an already-consolidated batch is
    /// cloned verbatim. The borrow-based twin of [`Self::ingest_owned_batch`]
    /// for callers that keep reading `batch` afterwards — `clone_batch()` +
    /// `ingest_owned_batch()` would copy an unconsolidated batch twice.
    pub fn ingest_borrowed_batch(&mut self, batch: &Batch) -> Result<(), StorageError> {
        self.upsert_borrowed_and_maybe_flush(batch)
    }

    fn upsert_borrowed_and_maybe_flush(&mut self, batch: &Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        let owned = Batch::consolidate_if_needed(batch, &self.schema).unwrap_or_else(|| batch.clone_batch());
        self.upsert_owned_and_maybe_flush(owned)
    }

    fn upsert_owned_and_maybe_flush(&mut self, batch: Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        self.found_source = FoundSource::None;
        self.cached_full_scan = None;

        // Ingest overflow always folds into the RAM tier; durability lives in the
        // fsynced SAL, and the checkpoint barrier writes the durable shard. Bump
        // `current_lsn` unconditionally so spill/barrier shard naming is
        // collision-free (it feeds only the master's zone-LSN allocator floor).
        self.current_lsn += 1;

        let consolidated = batch.into_consolidated(&self.schema);
        if self.memtable.should_flush() {
            self.flush_to_ram()?;
        }
        // The should_flush() pre-check above ensures runs_bytes is either 0
        // (post-flush) or <= 75% of max_bytes, so check_capacity() inside
        // upsert_sorted_batch (which fires at 100%) cannot return ERR_CAPACITY.
        self.memtable.upsert_sorted_batch(consolidated)?;
        if self.memtable.should_flush() {
            self.flush_to_ram()?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Flush
    // ------------------------------------------------------------------

    /// How this table's tail is recovered. Base and master system tables are
    /// `SalReplay`; view child tables and index circuits are `Rederive`.
    pub fn recovery_source(&self) -> RecoverySource {
        self.recovery_source
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
        snaps.extend(self.in_memory_runs());
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
    pub(crate) fn snapshot_runs(&self) -> &[Rc<Batch>] {
        self.memtable.snapshot_runs()
    }

    /// The RAM-tier runs (for cursor gathering), as owned batch handles —
    /// yielded lazily so callers `extend` without an intermediate `Vec`.
    pub(crate) fn in_memory_runs(&self) -> impl Iterator<Item = Rc<Batch>> + '_ {
        self.in_memory_l0.iter().map(|r| Rc::clone(&r.batch))
    }

    /// Get all shard Rcs (for PartitionedTable cursor gathering).
    pub(crate) fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.shard_index.all_shard_arcs()
    }

    /// Test helper: returns true when the memtable has no rows.
    #[cfg(test)]
    pub(crate) fn memtable_is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Test helper: shrink the per-table heap ceiling so spill paths can be
    /// exercised without ingesting megabytes.
    #[cfg(test)]
    pub(crate) fn set_inmem_ceiling_for_test(&mut self, bytes: usize) {
        self.inmem_ceiling_override = Some(bytes);
    }

    /// Test helper: upsert a consolidated batch directly into the memtable (no WAL).
    #[cfg(test)]
    pub(crate) fn memtable_upsert_sorted_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        self.cached_full_scan = None;
        self.memtable.upsert_sorted_batch(batch)
    }

    /// Test helper: the highest LSN registered in the shard index (0 when no
    /// shard is registered). Used to check spill/barrier LSN registration.
    #[cfg(test)]
    pub(crate) fn shard_index_max_lsn(&self) -> u64 {
        self.shard_index.max_lsn()
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    /// Check if a PK exists with positive net weight.
    #[cfg(test)] // production existence checks go through has_pk_bytes
    pub fn has_pk(&mut self, key: u128) -> bool {
        let (opk, n) = columnar::opk_key(&self.schema, &key.to_le_bytes());
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
        // Between checkpoints any table can hold live rows in `in_memory_l0`
        // (ingest overflow), so the RAM tier is scanned too.
        w += self.scan_inmem(key, false).0;
        w > 0
    }

    /// Net weight and (optionally) the matching rows for `key` across
    /// `in_memory_l0`, bloom-gated per run. Mirrors `scan_shards_for_pk_bytes`.
    fn scan_inmem(&self, key: &[u8], need_candidates: bool) -> (i64, Vec<(Rc<Batch>, usize)>) {
        let mut total_w: i64 = 0;
        let mut candidates: Vec<(Rc<Batch>, usize)> = Vec::new();
        for run in &self.in_memory_l0 {
            if !run.may_contain(key) {
                continue;
            }
            for lo in memtable::run_pk_match_rows(&run.batch, key) {
                total_w += run.batch.get_weight(lo);
                if need_candidates {
                    candidates.push((Rc::clone(&run.batch), lo));
                }
            }
        }
        (total_w, candidates)
    }

    /// Look up a PK for retraction.  Returns (net_weight, found).
    /// If found, sets `found_source` so `found_*` accessors can read the row.
    ///
    /// Takes a **native** `u128` and `opk_key`s it; the DML retraction path now
    /// keys on verbatim OPK bytes via `retract_pk_bytes`, so this native entry
    /// point has no production caller and is retained only for unit tests.
    #[cfg(test)]
    pub(crate) fn retract_pk(&mut self, key: u128) -> (i64, bool) {
        let (opk, n) = columnar::opk_key(&self.schema, &key.to_le_bytes());
        self.retract_pk_bytes(&opk[..n])
    }

    /// Look up a PK for retraction by its OPK `key` bytes. Returns
    /// `(net_weight, found)`; on a hit sets `found_source` so the `found_*`
    /// accessors expose the live (PK, payload) row. The OPK-keyed memtable
    /// bloom gates the run lookup at every PK width.
    pub fn retract_pk_bytes(&mut self, key: &[u8]) -> (i64, bool) {
        self.found_source = FoundSource::None;

        let (mt_w, _, mt_row_count) = if self.memtable.may_contain_pk(key) {
            self.memtable.lookup_pk_bytes(key)
        } else {
            (0, false, 0)
        };

        let need_candidates = !(mt_row_count == 1 && mt_w > 0);
        let (inmem_w, inmem_candidates) = self.scan_inmem(key, need_candidates);
        let (shard_w, shard_candidates) = self.scan_shards_for_pk_bytes(key, need_candidates);
        let total_w = mt_w + inmem_w + shard_w;

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

        // Tier-agnostic candidate list, RAM runs before shards. Each candidate
        // is already a `FoundSource`, so arming the winner is a move.
        let mut candidates: Vec<FoundSource> = inmem_candidates
            .into_iter()
            .map(|(rc, idx)| FoundSource::InMemRun(rc, idx))
            .chain(
                shard_candidates
                    .into_iter()
                    .map(|(rc, idx)| FoundSource::Shard(rc, idx)),
            )
            .collect();

        self.found_source = if in_memtable {
            FoundSource::MemTable
        } else if candidates.len() == 1 {
            // Single candidate + total_w > 0 + not in memtable ⇒ it is the live row.
            candidates.pop().expect("len checked")
        } else {
            // Multi-candidate: arm the first candidate whose global net weight is
            // positive — `candidate_weight` is the sole liveness oracle across
            // all three tiers.
            let mut winner = FoundSource::None;
            for cand in candidates {
                if self.candidate_weight(key, &cand) > 0 {
                    winner = cand;
                    break;
                }
            }
            winner
        };

        (total_w, true)
    }

    /// Global net weight of a located candidate row, dispatching on its tier.
    fn candidate_weight(&mut self, key: &[u8], cand: &FoundSource) -> i64 {
        match cand {
            FoundSource::InMemRun(rc, idx) => self.get_weight_for_row_bytes(key, rc.as_ref(), *idx),
            FoundSource::Shard(rc, idx) => self.get_weight_for_row_bytes(key, rc.as_ref(), *idx),
            FoundSource::None | FoundSource::MemTable => 0,
        }
    }

    /// Net weight for a specific (PK, payload) row, keyed by OPK `key` bytes.
    /// The OPK-keyed memtable bloom gates the run scan at every PK width.
    fn get_weight_for_row_bytes<S: columnar::ColumnarSource>(
        &mut self,
        key: &[u8],
        ref_source: &S,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        if self.memtable.may_contain_pk(key) {
            total_w += self.memtable.find_weight_for_row_bytes(key, ref_source, ref_row);
        }

        // RAM tier: bloom-gated and payload-aware, mirroring
        // `MemTable::find_weight_for_row_bytes`. This immutable borrow of
        // `in_memory_l0` ends before the shard split-borrow below.
        for run in &self.in_memory_l0 {
            if !run.may_contain(key) {
                continue;
            }
            for lo in memtable::run_pk_match_rows(&run.batch, key) {
                if columnar::compare_rows(&self.schema, run.batch.as_ref(), lo, ref_source, ref_row) == Ordering::Equal
                {
                    total_w += run.batch.get_weight(lo);
                }
            }
        }

        self.shard_index.find_pk_bytes(key, &mut |shard_rc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_rc.count && columnar::pk_bytes_eq(shard_rc.get_pk_bytes(idx), key) {
                let ord = columnar::compare_rows(&self.schema, shard_rc.as_ref(), idx, ref_source, ref_row);
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

    /// The row most recently located by `retract_pk*`, as a `ColumnarSource`
    /// view, or `None` when the last probe missed. The memtable arm resolves
    /// through `MemTable::found_entry`; the shard arm borrows the mapped shard
    /// held live by `FoundSource::Shard`.
    pub(crate) fn found_row(&self) -> Option<FoundRow<'_>> {
        match &self.found_source {
            FoundSource::None => None,
            FoundSource::MemTable => self.memtable.found_entry().map(|(b, r)| FoundRow::Mem(b, r)),
            FoundSource::Shard(arc, idx) => Some(FoundRow::Shard(arc, *idx)),
            FoundSource::InMemRun(rc, idx) => Some(FoundRow::Mem(rc.as_ref(), *idx)),
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
            // Open the dir fd just for this fsync; the `OwnedFd` closes it on
            // drop — not held for the table's lifetime.
            let dirfd = self.open_dirfd()?;
            let ok = fsync_eintr(dirfd.as_raw_fd()).is_ok();
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
    fn scan_shards_for_pk_bytes(&self, key: &[u8], need_candidates: bool) -> (i64, Vec<(Rc<MappedShard>, usize)>) {
        let mut total_w: i64 = 0;
        let mut candidates: Vec<(Rc<MappedShard>, usize)> = Vec::new();

        self.shard_index.find_pk_bytes(key, &mut |shard_rc, start_idx| {
            let mut idx = start_idx;
            while idx < shard_rc.count && columnar::pk_bytes_eq(shard_rc.get_pk_bytes(idx), key) {
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
    // Recursive: a table may home into a nested dir whose parents don't exist
    // yet (a worker's per-rank index subdir under a fresh index dir).
    std::fs::create_dir_all(dir).map_err(|_| StorageError::Io)?;
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
    // Remove all shard files belonging to this Rederive table: spill shards
    // (`shard_*`) and compaction outputs (`hcomp_*`) left by a previous
    // process. Both patterns include table_id, so only this table's files are
    // touched even when tables share the same directory.
    //
    // In steady state a Rederive table writes no shard files at all — its
    // flushes land in `in_memory_l0` (heap); files appear only past the
    // `INMEM_CEILING` spill and its disk compaction.
    let shard_prefix = format!("shard_{table_id}_");
    let hcomp_prefix = format!("hcomp_{table_id}_");
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&shard_prefix) || name.starts_with(&hcomp_prefix) {
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
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::{opk_pk, wide_pk_3xu64_schema, wide_row};

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
    /// the order given; a freshly-built batch is `Raw` so the ingest path
    /// runs the canonical sort+fold.
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
        batch
    }

    /// `Table::new(...)` with the per-test boilerplate folded away.
    fn new_table(dir: &std::path::Path, schema: SchemaDescriptor, id: u32, arena: u64, rs: RecoverySource) -> Table {
        Table::new(dir.to_str().unwrap(), "test", schema, id, arena, rs).unwrap()
    }

    /// Names of the "flat" `shard_{table_id}_{lsn}.db` files directly in `dir` —
    /// the unified spill/barrier naming. Excludes L1+ compaction outputs
    /// (`shard_{tid}_{seq}_L{n}_G{guard}.db`, distinguished by the `_L` level
    /// marker).
    fn shard_db_files(dir: &std::path::Path, table_id: u32) -> Vec<String> {
        let prefix = format!("shard_{table_id}_");
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.starts_with(&prefix) && n.ends_with(".db") && !n.contains("_L"))
                    .collect()
            })
            .unwrap_or_default()
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

        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

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

        let mut t = new_table(&tdir, schema, 200, 1 << 20, RecoverySource::SalReplay);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
        t.flush().unwrap();

        assert!(t.has_pk(10));
        t.close();

        // Re-open and recover
        let mut t2 = new_table(&tdir, schema, 200, 1 << 20, RecoverySource::SalReplay);

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

        let mut t = new_table(&tdir, schema, 300, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(30, 1, 300), (10, 1, 100), (20, 1, 200)]))
            .unwrap();

        let cursor = t.open_cursor();
        assert!(cursor.cursor.valid);
        assert_eq!(cursor.cursor.current_key_narrow() as u64, 10);
        // Don't need to iterate further — cursor creation works
    }

    #[test]
    fn table_retract_pk() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("retract_test");
        let schema = make_u64_i64_schema();

        let mut t = new_table(&tdir, schema, 400, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found);
        // The retracted row is the found row: a valid null word and an
        // accessible payload column, read through the ColumnarSource view.
        let fr = t.found_row().expect("retracted row is the found row");
        assert_ne!(columnar::ColumnarSource::get_null_word(&fr, 0), u64::MAX);
        assert_eq!(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).len(), 8);

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
        let mut t = new_table(&tdir, schema, 500, 256, RecoverySource::SalReplay);

        // Create enough flushes to trigger compaction
        for i in 0..6u64 {
            t.ingest_owned_batch(make_batch(&[(i * 10, 1, (i * 100) as i64)]))
                .unwrap();
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

        let mut t = new_table(&tdir, schema, 600, 1 << 20, RecoverySource::Rederive);

        // Batch 1: INSERT (PK=10, weight=+1, val=100)
        t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();

        // Batch 2: UPDATE delta — retract val=100, insert val=200
        // Rows sorted by (PK, payload): (-1 for val=100) before (+1 for val=200)
        t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)]))
            .unwrap();

        // Net state: val=100 has weight 0 (cancelled), val=200 has weight 1
        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found);

        // The found row must be val=200, not the cancelled val=100
        let fr = t.found_row().expect("retracted row is the found row");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(
            val, 200,
            "retract_pk must return the live (val=200) row, not the retracted val=100"
        );
    }

    /// `ingest_owned_batch` must sort the batch before memtable insert, even
    /// when the incoming Batch has `sorted=false` (reverse order).
    #[test]
    fn test_ingest_owned_batch_unsorted() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("ingest_owned_unsorted_test");
        let schema = make_u64_i64_schema();

        let mut t = new_table(&tdir, schema, 700, 1 << 20, RecoverySource::Rederive);

        // Build a reverse-sorted batch (PK order: 30, 20, 10).
        let batch = make_batch(&[(30, 1, 300), (20, 1, 200), (10, 1, 100)]);
        // make_batch produces a Raw (unsorted) batch.
        assert!(!batch.is_sorted());

        t.ingest_owned_batch(batch).unwrap();

        // Cursor must yield rows in ascending PK order
        let cursor = t.open_cursor();
        assert!(cursor.cursor.valid);
        assert_eq!(
            cursor.cursor.current_key_narrow() as u64,
            10,
            "cursor should start at PK=10 (smallest)"
        );
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
        let mut t = new_table(&tdir, schema, 900, 40, RecoverySource::Rederive);

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
        t.ingest_owned_batch(make_batch(&[(50, 1, 500), (40, 1, 400), (30, 1, 300)]))
            .unwrap();

        // fill batch (1,2,3) is now in shard; sorted (30,40,50) is in memtable.
        let cursor = t.open_cursor();
        assert!(cursor.cursor.valid);
        assert_eq!(
            cursor.cursor.current_key_narrow() as u64,
            1,
            "cursor should start at PK=1 from flushed shard"
        );
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
        let mut t = new_table(&tdir, schema, 1200, 128, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(1, 1, 10), (2, 1, 20)])).unwrap();
        assert!(!t.memtable_is_empty(), "two rows must not yet trigger overflow");

        t.ingest_owned_batch(make_batch(&[(3, 1, 30), (4, 1, 40)])).unwrap();

        assert!(t.memtable_is_empty(), "overflow post-check must auto-flush");
        // Non-durable flushes land in the in-memory run set, not disk shards.
        assert!(
            t.in_memory_runs().count() > 0,
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
        let mut t = new_table(&tdir, schema, 1000, 1 << 20, RecoverySource::SalReplay);

        // Batch 1: INSERT (PK=10, weight=+1, val=100)
        t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();
        t.flush().unwrap();

        // Batch 2: UPDATE delta — retract val=100, insert val=200
        t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)]))
            .unwrap();
        t.flush().unwrap();

        // Both batches are now in shards, memtable is empty.
        // retract_pk must find val=200 (net weight 1), not val=100 (net weight 0).
        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found);

        let fr = t.found_row().expect("retracted row is the found row");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(
            val, 200,
            "shard fallback must pick live payload (val=200), not cancelled (val=100)"
        );
    }

    /// Dropping a `Pending` FlushWork without committing must unlink the
    /// shard `.tmp` and the manifest `.tmp`, leaving the directory clean
    /// for a future retry.
    #[test]
    fn flush_prepare_drop_cleans_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("drop_clean_test");
        let schema = make_u64_i64_schema();

        let mut t = new_table(&tdir, schema, 1100, 1 << 20, RecoverySource::SalReplay);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

        match t.flush_prepare().unwrap() {
            FlushOutcome::Pending(work) => {
                let dir_entries: Vec<String> = std::fs::read_dir(&tdir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect();
                assert!(
                    dir_entries.iter().any(|n| n.ends_with(".tmp")),
                    ".tmp files must exist before Drop, got {dir_entries:?}"
                );
                drop(work);
            }
            other => panic!(
                "expected Pending, got non-pending outcome: {}",
                match other {
                    FlushOutcome::Empty => "Empty",
                    FlushOutcome::DoneInline => "DoneInline",
                    _ => "?",
                }
            ),
        }

        let leftover_tmp: Vec<String> = std::fs::read_dir(&tdir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(
            leftover_tmp.is_empty(),
            "Drop must unlink all .tmp files, found: {leftover_tmp:?}"
        );
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
            tdir.to_str().unwrap(),
            "test",
            schema,
            200,
            1 << 20,
            RecoverySource::SalReplay,
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

        let mut t = new_table(&tdir, schema, 1200, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
        match t.flush_prepare().unwrap() {
            FlushOutcome::DoneInline => {}
            FlushOutcome::Empty => panic!("expected DoneInline, got Empty"),
            FlushOutcome::Pending(_) => panic!("expected DoneInline, got Pending"),
        }
        assert!(
            t.memtable_is_empty(),
            "memtable must be reset after non-durable flush_prepare"
        );
        assert!(t.in_memory_runs().count() > 0, "snapshot must land in in_memory_l0");
        assert!(
            shard_db_files(&tdir, 1200).is_empty(),
            "Rederive flush must not write a shard file"
        );
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
        let schema = wide_pk_3xu64_schema();
        assert_eq!(schema.pk_stride(), 24);

        let pk3 = |a: u64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);
        let wide_batch = |rows: &[(Vec<u8>, i64, i64)]| -> Batch {
            let mut b = Batch::with_schema(schema, rows.len().max(1));
            for (pk, w, val) in rows {
                b.extend_pk_bytes(pk);
                b.extend_weight(&w.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &val.to_le_bytes());
                b.count += 1;
            }
            b
        };

        // Durable: this exercises `retract_pk_bytes` over flushed data, which is
        // base-table-only (base tables are durable). A durable flush writes a
        // real shard so the shard scan path is what's tested; a non-durable
        // flush would route the rows into `in_memory_l0` instead.
        let mut t = new_table(&tdir, schema, 4242, 1 << 20, RecoverySource::SalReplay);

        // Two prefix-twins (1,1,100)/(1,1,200) and a distinct (2,0,0).
        t.ingest_owned_batch(wide_batch(&[
            (pk3(1, 1, 100), 1, 10),
            (pk3(1, 1, 200), 1, 20),
            (pk3(2, 0, 0), 1, 30),
        ]))
        .unwrap();

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

    /// Wide (`pk_stride = 24`) PK with a *signed* leading column: OPK must order a
    /// negative leading value before a positive one, end-to-end through storage
    /// (compare, scan, membership, retract). This is the case where hand-written
    /// big-endian/little-endian bytes are wrong and only the real encoder's
    /// sign-flip is correct.
    #[test]
    fn table_wide_signed_compound_pk_opk_order() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("wide_signed_pk_test");
        // (I64, U64, U64) PK [stride 24, wide] + I64 payload used as an order marker.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        );
        assert_eq!(schema.pk_stride(), 24);

        let key = |a: i64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);

        // Direct sign-flip property: a negative leading column sorts before a
        // positive one in OPK byte order. Plain big-endian (no flip) would place
        // 0xFF.. (negatives) after 0x00.. (positives) and fail this.
        assert_eq!(
            columnar::compare_pk_bytes(&key(-1, 0, 0), &key(1, 0, 0)),
            std::cmp::Ordering::Less,
            "OPK must order a negative signed PK column before a positive one",
        );

        let mut t = new_table(&tdir, schema, 4243, 1 << 20, RecoverySource::SalReplay);

        // Payload marker == the signed leading value, so scan order is read back
        // without decoding the PK. `w` lets the same builder emit the DBSP -1
        // retraction row below.
        let row = |a: i64, w: i64| wide_row(&schema, &key(a, 0, 0), w, a);
        for a in [3i64, -5, 0, -1] {
            // scrambled insertion order
            t.ingest_owned_batch(row(a, 1)).unwrap();
        }
        t.flush().unwrap(); // Durable: one on-disk shard

        // Membership at signed width, byte-keyed (both must survive the flush).
        assert!(t.has_pk_bytes(&key(-5, 0, 0)));
        assert!(t.has_pk_bytes(&key(3, 0, 0)));
        assert!(!t.has_pk_bytes(&key(-2, 0, 0)), "absent signed key must not be found");

        // full_scan returns rows in OPK (= typed signed) order: -5, -1, 0, 3.
        // A missing sign-flip would scan back as 0, 3, -5, -1 and fail here.
        let scanned = t.full_scan();
        let payloads: Vec<i64> = (0..scanned.count)
            .map(|r| i64::from_le_bytes(scanned.get_col_ptr(r, 0, 8).try_into().unwrap()))
            .collect();
        assert_eq!(
            payloads,
            vec![-5, -1, 0, 3],
            "wide signed compound PK must scan back in typed (sign-flipped) order",
        );

        // `retract_pk_bytes` reports the live (weight, found) for the signed OPK
        // key at wide width — a read-only probe; the row is removed by a DBSP -1
        // ingest, whose memtable weight nets the shard's +1 to zero.
        let (w, found) = t.retract_pk_bytes(&key(-5, 0, 0));
        assert_eq!((w, found), (1, true));
        t.ingest_owned_batch(row(-5, -1)).unwrap();
        assert!(!t.has_pk_bytes(&key(-5, 0, 0)), "retracted signed key is gone");

        t.close();
    }

    // ── In-memory Rederive flush (RAM tier, no file I/O) ─────────────────

    /// A sub-ceiling Rederive flush writes no file at all; rows are served
    /// from `in_memory_l0` via the cursor.
    #[test]
    fn nondurable_flush_writes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("no_file_test");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200), (30, 1, 300)]))
            .unwrap();
        assert!(matches!(t.flush_prepare().unwrap(), FlushOutcome::DoneInline));

        let shard_files: Vec<String> = std::fs::read_dir(&tdir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("shard_"))
            .collect();
        assert!(shard_files.is_empty(), "non-durable flush wrote files: {shard_files:?}");
        assert!(t.all_shard_arcs().is_empty());
        assert!(t.in_memory_runs().count() > 0);

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
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

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
            t.in_memory_runs().count() <= INMEM_COMPACT_THRESHOLD,
            "run set must stay folded",
        );
        assert!(
            shard_db_files(&tdir, 100).is_empty(),
            "churn must not spill (tiny, sub-ceiling)"
        );
    }

    /// More than `INMEM_COMPACT_THRESHOLD` distinct-key flushes keep the run
    /// count bounded by folding, and every key survives.
    #[test]
    fn nondurable_run_count_stays_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("run_bound_test");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

        let n = INMEM_COMPACT_THRESHOLD as u64 + 4;
        for k in 0..n {
            t.ingest_owned_batch(make_batch(&[(k, 1, (k * 10) as i64)])).unwrap();
            assert!(t.flush().unwrap());
            assert!(
                t.in_memory_runs().count() <= INMEM_COMPACT_THRESHOLD,
                "run count exceeded threshold after flush {k}",
            );
        }
        for k in 0..n {
            assert!(t.has_pk(k as u128), "key {k} must survive folding");
        }
    }

    /// A flush past `INMEM_CEILING` (shrunk via the test seam) spills the
    /// folded run to a `shard_{tid}_{lsn}` file, drains heap, and keeps rows
    /// readable.
    #[test]
    fn nondurable_ceiling_spill_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("ceiling_spill_test");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);
        t.set_inmem_ceiling_for_test(100); // < one flush (~10 rows × 32 B)

        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert!(t.flush().unwrap());

        assert!(
            !shard_db_files(&tdir, 100).is_empty(),
            "ceiling breach must spill to a shard file"
        );
        assert_eq!(t.in_memory_bytes(), 0, "heap drained after spill");
        assert!(t.in_memory_runs().count() == 0);
        assert!(!t.all_shard_arcs().is_empty());
        for k in 0..10u128 {
            assert!(t.has_pk(k), "row {k} must remain readable from disk after spill");
        }
    }

    /// Repeated over-ceiling flushes keep the on-disk shard count bounded
    /// (the spill path's `compact_if_needed` folds L0→L1 and cleans up), and all
    /// rows across rounds stay correct.
    #[test]
    fn nondurable_repeated_spill_stays_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("repeated_spill_test");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);
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
            // Raw spill shards do not accumulate with rounds — disk L0 self-folds
            // into L1 and the consumed raw shards are unlinked.
            assert!(
                shard_db_files(&tdir, 100).len() <= 5,
                "round {r}: {} raw shards accumulated",
                shard_db_files(&tdir, 100).len(),
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
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

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
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

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
        assert!(t.in_memory_runs().count() > 0, "second flush stays in heap");

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

    // ── RAM-tier (`in_memory_l0`) found-row path ─────────────────────────
    //
    // Twins of the durable retract tests above, but the rows stay in
    // `in_memory_l0` (non-durable flush, sub-ceiling) instead of on disk, so the
    // `retract_pk*` / `has_pk_bytes` / `get_weight_for_row_bytes` RAM-tier code
    // is what's exercised. `retract_pk*` is production-invoked only on durable
    // base tables, but the machinery is tier-agnostic and these drive it directly.

    /// Twin of `test_retract_pk_shard_fallback_multiple_payloads`, in RAM: an
    /// UPDATE delta split across two in-memory runs. The multi-candidate
    /// global-net loop over `in_memory_l0` must pick the live payload (200), not
    /// the cancelled one (100).
    #[test]
    fn inmem_retract_multiple_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_retract_payloads");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 5001, 1 << 20, RecoverySource::Rederive);

        // Run 1: INSERT (PK=10, +1, val=100).
        t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();
        assert!(t.flush().unwrap());
        // Run 2: UPDATE delta — retract val=100, insert val=200.
        t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)]))
            .unwrap();
        assert!(t.flush().unwrap());

        assert!(t.memtable_is_empty());
        assert_eq!(t.in_memory_runs().count(), 2, "two sub-ceiling flushes → two L0 runs");

        let (w, found) = t.retract_pk(10);
        assert_eq!((w, found), (1, true), "net weight 1 across the RAM runs");
        let fr = t.found_row().expect("retracted row is the found row");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(
            val, 200,
            "RAM-tier global-net must pick live payload 200, not cancelled 100"
        );
    }

    /// Twin of `table_wide_pk_has_and_retract_bytes`, in RAM: wide (`pk_stride =
    /// 24`) prefix-twins `(1,1,100)`/`(1,1,200)` held in `in_memory_l0`. Retract
    /// the live twin; the other survives; the retracted one reports absent.
    #[test]
    fn inmem_retract_wide_pk() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_retract_wide_pk");
        let schema = wide_pk_3xu64_schema();
        assert_eq!(schema.pk_stride(), 24);
        let mut t = new_table(&tdir, schema, 5002, 1 << 20, RecoverySource::Rederive);

        let pk3 = |a: u64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);
        let wide_batch = |rows: &[(Vec<u8>, i64, i64)]| -> Batch {
            let mut b = Batch::with_schema(schema, rows.len().max(1));
            for (pk, w, val) in rows {
                b.extend_pk_bytes(pk);
                b.extend_weight(&w.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &val.to_le_bytes());
                b.count += 1;
            }
            b
        };

        // Two prefix-twins + a distinct key, flushed into `in_memory_l0`.
        t.ingest_owned_batch(wide_batch(&[
            (pk3(1, 1, 100), 1, 10),
            (pk3(1, 1, 200), 1, 20),
            (pk3(2, 0, 0), 1, 30),
        ]))
        .unwrap();
        assert!(t.flush().unwrap());
        assert!(t.has_pk_bytes(&pk3(1, 1, 100)));
        assert!(t.has_pk_bytes(&pk3(1, 1, 200)));
        assert!(t.has_pk_bytes(&pk3(2, 0, 0)));
        assert!(!t.has_pk_bytes(&pk3(1, 1, 300)), "absent prefix-twin must not be found");

        // Retract one twin via a second in-memory run; the other must survive.
        t.ingest_owned_batch(wide_batch(&[(pk3(1, 1, 100), -1, 10)])).unwrap();
        assert!(t.flush().unwrap());
        assert!(!t.has_pk_bytes(&pk3(1, 1, 100)), "retracted twin gone");
        assert!(t.has_pk_bytes(&pk3(1, 1, 200)), "the other twin survives");

        let (w, found) = t.retract_pk_bytes(&pk3(1, 1, 200));
        assert_eq!((w, found), (1, true));
        let fr = t.found_row().expect("live twin is the found row");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(val, 20, "found row must be the surviving twin's payload");

        let (w2, found2) = t.retract_pk_bytes(&pk3(1, 1, 100));
        assert_eq!((w2, found2), (0, false), "net-zero twin reports absent");
        t.close();
    }

    /// Twin of `table_wide_signed_compound_pk_opk_order`, in RAM: a signed
    /// leading PK column, rows in `in_memory_l0`. OPK order is read back via
    /// `full_scan` (over the heap runs), then the signed key is retracted.
    #[test]
    fn inmem_retract_signed_compound_pk() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_signed_pk");
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        );
        assert_eq!(schema.pk_stride(), 24);
        let mut t = new_table(&tdir, schema, 5003, 1 << 20, RecoverySource::Rederive);

        let key = |a: i64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);
        // Payload marker == the signed leading value, read back in scan order.
        let row = |a: i64, w: i64| wide_row(&schema, &key(a, 0, 0), w, a);
        for a in [3i64, -5, 0, -1] {
            t.ingest_owned_batch(row(a, 1)).unwrap();
        }
        assert!(t.flush().unwrap()); // one in-memory L0 run
        assert!(t.in_memory_runs().count() > 0, "rows land in in_memory_l0");

        assert!(t.has_pk_bytes(&key(-5, 0, 0)));
        assert!(t.has_pk_bytes(&key(3, 0, 0)));
        assert!(!t.has_pk_bytes(&key(-2, 0, 0)), "absent signed key must not be found");

        // full_scan over the heap run returns OPK (typed signed) order: -5,-1,0,3.
        let scanned = t.full_scan();
        let payloads: Vec<i64> = (0..scanned.count)
            .map(|r| i64::from_le_bytes(scanned.get_col_ptr(r, 0, 8).try_into().unwrap()))
            .collect();
        assert_eq!(
            payloads,
            vec![-5, -1, 0, 3],
            "wide signed compound PK scans back in sign-flipped order"
        );

        let (w, found) = t.retract_pk_bytes(&key(-5, 0, 0));
        assert_eq!((w, found), (1, true));
        let fr = t.found_row().expect("signed key is the found row");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(val, -5, "found row payload marks the retracted signed key");

        // Retraction across a second run nets the signed key to zero.
        t.ingest_owned_batch(row(-5, -1)).unwrap();
        assert!(t.flush().unwrap());
        assert!(!t.has_pk_bytes(&key(-5, 0, 0)), "retracted signed key is gone");
        t.close();
    }

    /// The live row sits in RAM while the memtable holds a *negative*-weight
    /// entry for the same PK. `get_weight_for_row_bytes` must net memtable + RAM
    /// per candidate (killing the payload that cancels to zero) and arm the
    /// globally-live payload from the RAM run.
    #[test]
    fn inmem_cross_tier_netting() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_cross_tier");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 5004, 1 << 20, RecoverySource::Rederive);

        // RAM run: two payloads for PK=5 (val=50, val=60), each +1.
        t.ingest_owned_batch(make_batch(&[(5, 1, 50), (5, 1, 60)])).unwrap();
        assert!(t.flush().unwrap());
        // Memtable (unflushed): retract val=50, leaving (5,60) globally live.
        t.ingest_owned_batch(make_batch(&[(5, -1, 50)])).unwrap();
        assert!(!t.memtable_is_empty(), "retraction stays in the memtable");

        assert!(t.has_pk(5), "PK 5 nets +1 across RAM (+2) and memtable (-1)");

        let (w, found) = t.retract_pk(5);
        assert_eq!((w, found), (1, true), "global net weight is 1");
        let fr = t.found_row().expect("live row found");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(
            val, 60,
            "global oracle rejects the cancelled val=50, arms live val=60 from RAM"
        );
    }

    /// More than `INMEM_COMPACT_THRESHOLD` runs force `compact_in_memory`, which
    /// rebuilds the folded run's PK bloom via `InMemRun::from_batch`. After the
    /// fold, live rows must still be found (no false negative from the rebuilt
    /// bloom) and an all-runs-absent PK must report absent (bloom-miss path
    /// equals the linear result).
    #[test]
    fn inmem_fold_rebuilds_run_bloom() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_fold_bloom");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 5005, 1 << 20, RecoverySource::Rederive);

        // One key per flush past the fold threshold; the folded run's bloom is
        // rebuilt over the merged batch.
        let n = INMEM_COMPACT_THRESHOLD as u64 + 2;
        for k in 0..n {
            t.ingest_owned_batch(make_batch(&[(k, 1, 100 + k as i64)])).unwrap();
            assert!(t.flush().unwrap());
        }
        assert!(
            t.in_memory_runs().count() <= INMEM_COMPACT_THRESHOLD,
            "fold kept the run count bounded"
        );

        // Every live key survives folding and is found through the rebuilt bloom.
        for k in 0..n {
            assert!(t.has_pk(k as u128), "key {k} must survive fold + bloom rebuild");
        }
        let (w, found) = t.retract_pk(0);
        assert_eq!((w, found), (1, true));
        let fr = t.found_row().expect("folded key is the found row");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(val, 100, "found row payload survives the fold");

        // A PK absent from every run: bloom-miss path must equal the linear miss.
        assert!(!t.has_pk(9999), "absent PK reports absent (bloom miss)");
        assert_eq!(t.retract_pk(9999), (0, false), "absent PK retract nets zero, not found");
    }

    // ── Unified checkpointing: RAM-tier lifecycle for SalReplay tables ────

    /// The fold-first data-loss guard: a `SalReplay` table whose ingest overflow
    /// left the memtable empty and `in_memory_l0` populated must barrier-flush to
    /// `Pending` (NOT `Empty`) — else the RAM-tier data is silently dropped.
    /// After commit + reopen the full contents survive.
    #[test]
    fn barrier_flush_folds_populated_l0_not_empty() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("barrier_fold_l0");
        let schema = make_u64_i64_schema();
        // Small arena (128 B, threshold 96): an 8-row batch (256 B) overflows the
        // memtable into in_memory_l0, leaving the memtable empty.
        let mut t = new_table(&tdir, schema, 7100, 128, RecoverySource::SalReplay);

        let rows: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert!(t.memtable_is_empty(), "ingest must overflow the memtable into L0");
        assert!(t.in_memory_runs().count() > 0, "in_memory_l0 must be populated");
        assert!(
            t.all_shard_arcs().is_empty(),
            "sub-ceiling overflow writes no disk shard"
        );

        // `flush()` returns false iff `flush_prepare` said `Empty` — which here
        // would mean the fold-first gate dropped a populated `in_memory_l0`.
        assert!(
            t.flush().unwrap(),
            "fold-first gate dropped populated in_memory_l0 (data loss)"
        );

        assert!(t.in_memory_runs().count() == 0, "flush_commit clears the RAM tier");
        assert!(!t.all_shard_arcs().is_empty(), "barrier wrote a durable shard");
        t.close();

        // Reopen (SalReplay loads the manifest) → every row survives.
        let mut t2 = new_table(&tdir, schema, 7100, 128, RecoverySource::SalReplay);
        for k in 0..8u128 {
            assert!(t2.has_pk(k), "row {k} must survive barrier flush + reopen");
        }
        t2.close();
    }

    /// Spill unification (SalReplay): a ceiling breach spills to
    /// `shard_{tid}_{lsn}.db` (the unified naming), registered with
    /// `max_lsn == current_lsn - 1`. Two spills land at distinct `current_lsn`
    /// → two distinct filenames. After a manifest-publishing barrier flush,
    /// reopen seeds `current_lsn = max_lsn + 1` and every row survives.
    #[test]
    fn salreplay_spill_unified_naming_and_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("salreplay_spill");
        let schema = make_u64_i64_schema();
        // Small arena forces memtable overflow → flush_to_ram; tiny ceiling forces
        // the folded L0 to spill.
        let mut t = new_table(&tdir, schema, 7200, 128, RecoverySource::SalReplay);
        t.set_inmem_ceiling_for_test(100);

        // First spill: 10 rows (320 B) overflow, fold to L0 (320 B > 100) → spill.
        let b1: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&b1)).unwrap();
        assert_eq!(t.in_memory_bytes(), 0, "spill drains the RAM tier");
        let lsn1 = t.current_lsn;
        assert_eq!(
            t.shard_index_max_lsn(),
            lsn1 - 1,
            "spill registers with real LSNs (max_lsn == current_lsn - 1)"
        );
        let files1 = shard_db_files(&tdir, 7200);
        assert_eq!(files1, vec![format!("shard_7200_{lsn1}.db")], "unified spill naming");

        // Second spill at a distinct current_lsn → distinct filename, no collision.
        let b2: Vec<(u64, i64, i64)> = (100..110).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&b2)).unwrap();
        assert_eq!(t.in_memory_bytes(), 0, "second spill drains the RAM tier");
        let lsn2 = t.current_lsn;
        assert!(lsn2 > lsn1, "current_lsn strictly increased between spills");
        let mut files2 = shard_db_files(&tdir, 7200);
        files2.sort();
        assert_eq!(
            files2,
            vec![format!("shard_7200_{lsn1}.db"), format!("shard_7200_{lsn2}.db")],
            "two spills → two distinct shard files"
        );

        // Publish a manifest so reopen sees the spilled shards: a small in-memtable
        // batch + barrier flush references the whole index.
        t.ingest_owned_batch(make_batch(&[(500, 1, 5000)])).unwrap();
        assert!(t.flush().unwrap(), "barrier flush must publish a manifest");
        t.close();

        let mut t2 = new_table(&tdir, schema, 7200, 128, RecoverySource::SalReplay);
        assert_eq!(
            t2.current_lsn,
            t2.shard_index_max_lsn() + 1,
            "reopen seeds current_lsn = max_lsn + 1 from the registered shard LSNs"
        );
        assert!(t2.current_lsn > 1, "reopen recovered a non-trivial LSN");
        for k in 0..10u128 {
            assert!(t2.has_pk(k), "first-spill row {k} survives reopen");
        }
        for k in 100..110u128 {
            assert!(t2.has_pk(k), "second-spill row {k} survives reopen");
        }
        assert!(t2.has_pk(500), "barrier-flushed row survives reopen");
        t2.close();
    }

    /// A `can_tag_pk_unique` base table's barrier shard carries
    /// `SHARD_FLAG_PK_UNIQUE` and an XOR8 filter; the same holds for a SalReplay
    /// spill.
    #[test]
    fn salreplay_barrier_and_spill_carry_pk_unique_tag() {
        let schema = make_u64_i64_schema();

        // Barrier shard: live memtable + populated L0, base-table tagging on.
        {
            let dir = tempfile::tempdir().unwrap();
            let tdir = dir.path().join("barrier_tag");
            let mut t = new_table(&tdir, schema, 7300, 128, RecoverySource::SalReplay);
            t.enable_pk_unique_tagging();

            // Overflow 8 rows into L0, then leave 2 rows live in the memtable.
            let over: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
            t.ingest_owned_batch(make_batch(&over)).unwrap();
            assert!(t.in_memory_runs().count() > 0);
            t.ingest_owned_batch(make_batch(&[(100, 1, 1000), (101, 1, 1010)]))
                .unwrap();
            assert!(!t.memtable_is_empty(), "small second batch stays live in the memtable");

            assert!(t.flush().unwrap(), "barrier flush must write the shard");
            let shards = t.all_shard_arcs();
            assert_eq!(shards.len(), 1, "barrier folds memtable + L0 into one shard");
            assert!(shards[0].is_pk_unique, "barrier shard must carry SHARD_FLAG_PK_UNIQUE");
            assert!(shards[0].has_xor8(), "barrier shard must carry the XOR8 filter");
            assert!(
                shards[0].xor8_may_contain(100),
                "XOR8 must contain a folded memtable PK"
            );
        }

        // SalReplay spill: durable + tagged.
        {
            let dir = tempfile::tempdir().unwrap();
            let tdir = dir.path().join("spill_tag");
            let mut t = new_table(&tdir, schema, 7301, 128, RecoverySource::SalReplay);
            t.enable_pk_unique_tagging();
            t.set_inmem_ceiling_for_test(100);

            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            assert_eq!(t.in_memory_bytes(), 0, "ceiling breach spilled to disk");
            let shards = t.all_shard_arcs();
            assert_eq!(shards.len(), 1, "one spilled shard");
            assert!(
                shards[0].is_pk_unique,
                "SalReplay spill must carry SHARD_FLAG_PK_UNIQUE"
            );
            assert!(shards[0].has_xor8(), "SalReplay spill must carry the XOR8 filter");
        }
    }

    /// `current_lsn` bumps on every ingest, including a `Rederive` table (which
    /// previously pinned it at 1 because ephemeral flushes never advanced it).
    #[test]
    fn current_lsn_bumps_on_every_ingest_including_rederive() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("lsn_bump_rederive");
        let schema = make_u64_i64_schema();
        let mut t = new_table(&tdir, schema, 7400, 1 << 20, RecoverySource::Rederive);

        assert_eq!(t.current_lsn, 1, "fresh Rederive table starts at LSN 1");
        t.ingest_owned_batch(make_batch(&[(1, 1, 10)])).unwrap();
        assert_eq!(t.current_lsn, 2, "first ingest bumps current_lsn");
        t.ingest_owned_batch(make_batch(&[(2, 1, 20)])).unwrap();
        assert_eq!(t.current_lsn, 3, "second ingest bumps current_lsn again");
    }

    /// Twin of the RAM-tier retract tests on a real `SalReplay` table: ingest
    /// overflow lands in `in_memory_l0` naturally (no artificial Rederive), and
    /// `has_pk`/`retract_pk` resolve the live row across the RAM tier.
    #[test]
    fn salreplay_overflow_into_l0_retract() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("salreplay_l0_retract");
        let schema = make_u64_i64_schema();
        // Small arena so an 8-row ingest overflows the memtable into in_memory_l0.
        let mut t = new_table(&tdir, schema, 7500, 128, RecoverySource::SalReplay);
        t.enable_pk_unique_tagging();

        let rows: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert!(t.memtable_is_empty(), "overflow emptied the memtable");
        assert!(
            t.in_memory_runs().count() > 0,
            "SalReplay overflow lands in in_memory_l0"
        );
        assert!(
            t.all_shard_arcs().is_empty(),
            "sub-ceiling overflow writes no disk shard"
        );

        for k in 0..8u128 {
            assert!(t.has_pk(k), "row {k} readable from the RAM tier");
        }
        let (w, found) = t.retract_pk(3);
        assert_eq!((w, found), (1, true), "retract resolves the live row in in_memory_l0");
        let fr = t.found_row().expect("found row from RAM tier");
        let val = i64::from_le_bytes(columnar::ColumnarSource::get_col_ptr(&fr, 0, 0, 8).try_into().unwrap());
        assert_eq!(val, 30, "found-row payload matches the RAM-tier row");
    }
}
