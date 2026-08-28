//! Unified Table: two RAM-tier [`RunSet`]s over a `ShardIndex`.
//!
//! Ingest lands in the `memtable` run set and folds into the `ram_tier` at 3/4
//! of the arena; the RAM tier spills to a shard past [`INMEM_CEILING`], and the
//! checkpoint barrier folds it into one durable shard — on the base round for
//! `SalReplay` tables, on the ephemeral round for `Rederive` ones.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::os::fd::OwnedFd;
use std::rc::Rc;

use super::batch::Batch;
use super::columnar;
use super::error::StorageError;
use super::manifest::PreparedManifest;
use super::read_cursor::{self, ReadCursor};
use super::run::{pk_match_rows_from, Run, StoredRow};
use super::run_set::RunSet;
use super::shard_index::ShardIndex;
#[cfg(test)]
use super::shard_reader::MappedShard;
use crate::schema::key::probe_key;
use crate::schema::SchemaDescriptor;

/// Hard per-`Table` (= per relation per worker) heap ceiling for the RAM tier. A
/// flush that would exceed it folds first; if the folded net state still exceeds
/// it, the tier spills to a shard file. Bounds heap at this value per table at
/// all times. The aggregate un-spilled RAM across the cluster is bounded by the
/// un-checkpointed SAL tail: every ingested byte flows through the fsynced SAL,
/// and a spill frees the RAM.
///
/// Swept at the **production** checkpoint cadence (`GNITZ_SAL_BYTES` at its 1 GiB
/// default, threshold 75% of it), 4M rows, W=4, btrfs: at 4 MiB a worker's single
/// store wrote 98.7 MB of RAM-tier spill per 4M rows (~25 B/row); at 32 MiB that
/// spill is gone, for +45 MB of cluster RSS. An earlier sweep found this constant
/// flat from 4 to 128 MiB, but ran at a 4 MiB checkpoint threshold — 192× more
/// frequent than production — which drains the tier continuously and is exactly
/// the regime where the ceiling cannot bind.
///
/// Spilling past the ceiling stays the intended safety valve; 32 MiB is where
/// ordinary ingest stops reaching it, not a promise that nothing will.
const INMEM_CEILING: usize = 32 * 1024 * 1024;

/// [`INMEM_CEILING`] with its `GNITZ_RAM_TIER_BYTES` override applied, read once
/// per process rather than per store — `Table::new` runs for every system table,
/// every user relation, and every view's per-node scratch. Shrinking it is how an
/// E2E test reaches the disk regime (spills, compaction, the capacity sweep) on
/// small data. Being process-wide is why the per-table
/// `set_inmem_ceiling_for_test` stays: the Rust units share one process and must
/// not fight over a global.
fn inmem_ceiling() -> usize {
    static CEILING: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CEILING.get_or_init(|| crate::foundation::env::env_num("GNITZ_RAM_TIER_BYTES", INMEM_CEILING))
}

/// Memtable arena of every store [`Table::new`] opens — every system table,
/// every user relation, every view's operator scratch. Measured on btrfs, W=4,
/// 4 views, 200k rows, interleaved ×3: 256 KiB and 1 MiB are indistinguishable
/// in total stall (545/543/540 ms against 544/563/550 ms), so the smaller value
/// stands. A separate non-interleaved sweep appeared to show 256 KiB winning by
/// 1.5×; interleaving dissolved it.
const DEFAULT_ARENA: u64 = 256 << 10;

// ---------------------------------------------------------------------------
// RecoverySource
// ---------------------------------------------------------------------------

/// How a relation's tail is recovered across a restart — the one fact both
/// `Table::new` and the boot rebuild read, so they cannot disagree.
///
/// This controls recovery, not the flush path: between checkpoints every table
/// keeps its overflow in the RAM tier, and the checkpoint barrier folds it into a
/// durable shard only for `SalReplay` tables.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoverySource {
    /// The tail is recovered by replaying the fsynced SAL over the shards loaded
    /// from the manifest at open. Base tables and master system tables.
    SalReplay,
    /// The relation is rebuilt from its sources, and the ephemeral checkpoint
    /// round force-persists it with a generation-stamped manifest: view
    /// operator-trace tables, view output stores, secondary indexes.
    Rederive {
        /// The generation a manifest must carry for the open to resume from it
        /// instead of erasing it. `None` is how a caller whose verdict has more
        /// to it than the generation — a topology change invalidates every
        /// rederived relation — says "never resume" without inventing a
        /// generation no manifest can hold.
        resume_at: Option<u64>,
    },
}

// ---------------------------------------------------------------------------
// Two-phase flush API
// ---------------------------------------------------------------------------

/// Outcome of `Table::flush_prepare`.
pub(in crate::storage) enum FlushOutcome {
    /// Nothing to publish: a rederived table on the base round, which folded into
    /// the RAM tier with no file I/O.
    Done,
    /// `SalReplay`: the folded shard is written at its final name (unsynced) and
    /// a manifest `.tmp` is staged; the barrier fdatasyncs `sync_paths` then
    /// renames the manifest.
    Pending(FlushWork),
}

/// The deferred half of one barrier flush, owned by the worker between
/// `flush_prepare` and `flush_commit`. Carries the full paths of every file
/// written unsynced since the last publish (prior spills + this barrier's own
/// folded shard) and the staged manifest `.tmp`. The barrier fdatasyncs each
/// `sync_paths` entry (opening it O_RDONLY, in bounded fd sub-chunks),
/// fdatasyncs the manifest `.tmp`, then renames it. Dropping a `FlushWork`
/// before commit unlinks only the manifest `.tmp` (via `PreparedManifest`'s own
/// `Drop`); the shard — already at its final name and registered in the index —
/// is an unreferenced orphan reclaimed by `gc_orphans` at the next open.
pub(in crate::storage) struct FlushWork {
    sync_paths: Vec<CString>,
    manifest: PreparedManifest,
}

impl FlushWork {
    /// Full paths the barrier must fdatasync (each opened O_RDONLY) before the
    /// manifest rename — the files this publish makes reachable that are not yet
    /// durable.
    pub(in crate::storage) fn sync_paths(&self) -> &[CString] {
        &self.sync_paths
    }

    /// The staged manifest `.tmp`'s fd, open from `prepare_file` until
    /// `flush_commit` consumes the work (it closes when the `PreparedManifest`
    /// drops after the rename).
    pub(in crate::storage) fn manifest_fd(&self) -> libc::c_int {
        self.manifest.fd()
    }
}

/// Index of the first candidate in `pool` (pool order) whose payload group nets
/// strictly positive — the live row for the PK the pool was gathered for.
/// Candidates equal to an earlier one are skipped rather than re-summed, so each
/// group is judged once, at its first member.
fn first_live_payload_group(schema: &SchemaDescriptor, pool: &[StoredRow]) -> Option<usize> {
    let same_payload =
        |a: &StoredRow, b: &StoredRow| columnar::compare_rows(schema, &a.run, a.row, &b.run, b.row) == Ordering::Equal;
    (0..pool.len()).find(|&i| {
        if pool[..i].iter().any(|prev| same_payload(prev, &pool[i])) {
            return false; // this payload was already judged at its first member
        }
        let net: i64 = pool[i..]
            .iter()
            .filter(|c| same_payload(&pool[i], c))
            .map(StoredRow::weight)
            .sum();
        net > 0
    })
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

pub struct Table {
    /// Ingest runs, folded into `ram_tier` once they pass 3/4 of the arena.
    memtable: RunSet,
    /// Flushed runs held in heap instead of on disk. Populated for **every**
    /// table on ingest overflow (`flush_to_ram`), bounded by `INMEM_CEILING`
    /// (spill). The checkpoint barrier folds this tier into one durable shard
    /// for `SalReplay` tables.
    ram_tier: RunSet,
    shard_index: ShardIndex,
    schema: SchemaDescriptor,
    table_id: u32,
    directory: String,

    recovery_source: RecoverySource,

    current_lsn: u64,

    /// This child set's layout sequence. Loaded at open and re-stamped by every
    /// publish, so it survives checkpoints; the boot relayout stamps a target set
    /// one above its source's, which is what decides the live set when two
    /// complete sets survive a crash.
    layout_seq: u64,

    /// True when this open reloaded a generation-matching checkpointed manifest
    /// instead of starting empty. The boot index rebuild skips a table that
    /// reports true. Emptiness is not a substitute: a row with NULL in an
    /// indexed column is not indexed at all, so an all-NULL slice yields a
    /// legitimately empty index over a large owner.
    resumed_from_checkpoint: bool,

    /// Reused candidate pool for `retract_pk_bytes`' grouping pass; cleared per
    /// call (dropping its `Rc`s) with capacity retained, so the path stops
    /// allocating once warmed up.
    retract_scratch: Vec<StoredRow>,

    cached_full_scan: Option<Rc<Batch>>,
}

mod flush;
mod unique_pk;

pub(crate) use unique_pk::enforce_unique_pk;

#[cfg(test)]
mod bench_flush;

#[cfg(test)]
mod bench_ingest;

impl Table {
    /// Create a new table. The `RecoverySource` decides what the open does with
    /// whatever is already on disk.
    pub fn new(
        dir: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        recovery_source: RecoverySource,
    ) -> Result<Self, StorageError> {
        Self::with_arena(dir, schema, table_id, DEFAULT_ARENA, recovery_source)
    }

    /// [`Table::new`] with an explicit memtable arena. Every production store
    /// takes `DEFAULT_ARENA`; this exists for the tests that drive spill
    /// pressure by shrinking it.
    pub(crate) fn with_arena(
        dir: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        arena_size: u64,
        recovery_source: RecoverySource,
    ) -> Result<Self, StorageError> {
        // The directory is created before either arm decides anything, so an
        // unusable one fails here — a client-visible rejection on the master's
        // CREATE VIEW pre-flight, where a first-flush failure would instead be a
        // worker abort with no client left to tell.
        // The fd is dropped: a relation pins none at rest.
        open_table_dirfd(dir)?;
        let load_shards = match recovery_source {
            RecoverySource::SalReplay => true,
            // Resume only from the generation the caller named; otherwise erase
            // the shards *and* the manifest, so a later re-open cannot re-peek it.
            RecoverySource::Rederive { resume_at } => {
                let cpath = super::super::cstr(super::manifest::path(dir))?;
                let resumes =
                    resume_at.is_some() && super::manifest::peek_header(&cpath)?.map(|h| h.checkpoint_gen) == resume_at;
                if !resumes {
                    erase_stale_shards(dir, table_id);
                }
                resumes
            }
        };

        let mut table = Table {
            // Fold at 3/4 of the arena so the next ingest batch always fits.
            memtable: RunSet::new(arena_size as usize * 3 / 4),
            ram_tier: RunSet::new(inmem_ceiling()),
            // Only a `SalReplay` store is point-probed by PK, so only it needs
            // the PK filters its shards would otherwise all carry.
            shard_index: ShardIndex::new(
                table_id,
                dir,
                schema,
                matches!(recovery_source, RecoverySource::Rederive { .. }),
            ),
            schema,
            table_id,
            directory: dir.to_string(),
            recovery_source,
            current_lsn: 1,
            layout_seq: 0,
            resumed_from_checkpoint: load_shards && matches!(recovery_source, RecoverySource::Rederive { .. }),
            retract_scratch: Vec::new(),
            cached_full_scan: None,
        };

        if load_shards {
            let header = table.shard_index.load_manifest(&table.manifest_full_path())?;
            table.shard_index.gc_orphans();
            table.current_lsn = table.shard_index.max_lsn() + 1;
            if table.current_lsn == 0 {
                table.current_lsn = 1;
            }
            table.layout_seq = header.map_or(0, |h| h.layout_seq);
        }

        Ok(table)
    }

    /// The directory this store's shards live in — the child it is homed at.
    pub fn directory(&self) -> &str {
        &self.directory
    }

    /// Bound this store's registered shard bytes — the sweep itself lives on the
    /// shard index, which owns every quantity it touches. Called once, by
    /// `build_relation_store`, so a store is bounded from birth.
    pub fn set_capacity(&mut self, capacity_bytes: Option<u64>) {
        self.shard_index.set_capacity(capacity_bytes);
    }

    /// Configure this store as a fed view's **delta store**: bounded by `budget`
    /// registered shard bytes, evicting by dropping its victim outright, and
    /// unlinking every compaction's superseded inputs at once because it
    /// publishes no manifest. Called once, by `build_relation_store`, exactly as
    /// [`Self::set_capacity`] is.
    pub fn set_delta_budget(&mut self, budget: u64) {
        self.shard_index.set_delta_budget(budget);
    }

    /// The highest tick round this delta store's capacity sweep has dropped. A
    /// read at `after_tick > dropped_through` asks only for rounds above it, and
    /// no such round was ever dropped; a read at or below it is refused with
    /// `STATUS_DELTA_EXPIRED`. `0` for every store that has dropped nothing.
    pub fn dropped_through(&self) -> u64 {
        self.shard_index.dropped_through()
    }

    /// Whether a read of this store can meet a skeleton row it has to hydrate.
    /// See [`ShardIndex::has_skeleton_shard`].
    pub fn has_skeleton_rows(&self) -> bool {
        self.shard_index.has_skeleton_shard()
    }

    /// Full path of this table's manifest — a pure function of the directory,
    /// carrying no state worth caching.
    fn manifest_full_path(&self) -> String {
        super::manifest::path(&self.directory)
    }

    /// True when this store is rebuilt from its sources at open. It is the whole
    /// difference between the two checkpoint rounds: the base round publishes
    /// the others and folds these to RAM, the ephemeral round publishes exactly
    /// these.
    pub(crate) fn is_rederived(&self) -> bool {
        matches!(self.recovery_source, RecoverySource::Rederive { .. })
    }

    /// The policy this open accepted its on-disk state under — a frozen fact of
    /// this open, not the engine's current verdict, which every later checkpoint
    /// moves.
    pub(crate) fn recovery_source(&self) -> RecoverySource {
        self.recovery_source
    }

    /// True when the base round would publish this store at a cut newer than its
    /// last manifest: that round publishes it at all, and it holds rows in a heap
    /// tier or shards a spill wrote that no manifest references yet. False right
    /// after a publish, which folds both tiers into one synced shard and
    /// re-stamps the manifest over them.
    pub(crate) fn base_round_advances_publish(&self) -> bool {
        !self.is_rederived()
            && (self.ram_tiers().iter().any(|s| s.row_count() > 0)
                || self.shard_index.unsynced_paths().next().is_some())
    }

    /// Whether this open reloaded checkpointed state rather than starting empty.
    pub(crate) fn resumed_from_checkpoint(&self) -> bool {
        self.resumed_from_checkpoint
    }

    /// Unlink this store's manifest, so the next `Rederive` open
    /// peeks `None` and erases the shards instead of reloading them — how a
    /// caller rejects state it must not resume from.
    pub fn unlink_manifest(&self) {
        let _ = std::fs::remove_file(self.manifest_full_path());
    }

    /// Publish `schema` across this store (any column ALTER): re-open every
    /// registered shard under it, then install the result.
    ///
    /// The re-open runs first and mutates nothing, so a failure leaves the store
    /// entirely on the old schema, never half-widened where a NULL could
    /// consolidate against a real `0`. Widening the resident tiers reads
    /// `self.schema` as the runs' input schema, so it must precede the
    /// reassignment. `cached_full_scan` was materialized under the old column
    /// set, so it is dropped either way.
    pub fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        let staged = self.shard_index.reopen_all(&schema)?;
        self.memtable.widen_runs(&self.schema, &schema);
        self.ram_tier.widen_runs(&self.schema, &schema);
        self.shard_index.install_reopened(staged, schema);
        self.schema = schema;
        self.cached_full_scan = None;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch into the memtable.
    /// The relation-store ingest entry point.
    pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        self.cached_full_scan = None;

        // Ingest overflow always folds into the RAM tier; durability lives in the
        // fsynced SAL, and the checkpoint barrier writes the durable shard. Bump
        // `current_lsn` unconditionally so spill/barrier shard naming is
        // collision-free (it feeds only the master's zone-LSN allocator floor).
        self.current_lsn += 1;

        let consolidated = batch.into_consolidated(&self.schema);
        self.memtable.push(Rc::new(consolidated), &self.schema);
        if self.memtable.is_full() {
            self.flush_to_ram()?;
        }
        Ok(())
    }

    /// Ingest a borrowed Batch, copying it exactly once: an unconsolidated
    /// batch is consolidated straight into the owned copy the memtable keeps
    /// (the consolidation pass IS the copy), an already-consolidated batch is
    /// cloned verbatim. The borrow-based twin of [`Self::ingest_owned_batch`]
    /// for callers that keep reading `batch` afterwards — `clone_batch()` +
    /// `ingest_owned_batch()` would copy an unconsolidated batch twice.
    pub fn ingest_borrowed_batch(&mut self, batch: &Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        let owned = Batch::consolidate_if_needed(batch, &self.schema).unwrap_or_else(|| batch.clone_batch());
        self.ingest_owned_batch(owned)
    }

    // ------------------------------------------------------------------
    // Flush
    // ------------------------------------------------------------------

    /// The next-shard LSN counter. Seeded `max_lsn + 1` at open, bumped on every
    /// ingest; feeds spill/barrier shard naming and the master's zone-LSN
    /// allocator floor.
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Pin the LSN counter to a recovery watermark, monotonically: the counter
    /// only ever moves forward. Idempotent SAL re-replay legitimately presents
    /// an older zone LSN (the dedupe filter under-dedupes by design), and
    /// regressing the counter would let a later spill reuse a live shard name.
    pub fn pin_lsn(&mut self, lsn: std::num::NonZeroU64) {
        self.current_lsn = self.current_lsn.max(lsn.get());
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    /// The heap-resident tiers, newest first. The one enumeration behind every
    /// walk that spans them — cursor opens, PK probes, row counts — so a tier
    /// cannot be visible to one and invisible to another. The shard tier is not
    /// here: each walk reaches it differently (gated binary search for a probe, a
    /// bare count for the estimate).
    fn ram_tiers(&self) -> [&RunSet; 2] {
        [&self.memtable, &self.ram_tier]
    }

    /// The heap-resident runs, newest tier first. No guard partitions them, so
    /// every walk takes them whole however narrow its key bound.
    fn mem_runs(&self) -> impl Iterator<Item = Run> + '_ {
        self.ram_tiers()
            .into_iter()
            .flat_map(|set| set.runs().iter().cloned().map(Run::Mem))
    }

    /// Every run this table reads through, newest tier first.
    pub(crate) fn runs(&self) -> impl Iterator<Item = Run> + '_ {
        self.mem_runs()
            .chain(self.shard_index.all_shard_arcs_iter().map(Run::Shard))
    }

    /// Open a read-only cursor over every tier. Does NOT mutate the table —
    /// compaction is a maintenance operation, not part of the read path. Cheap
    /// and infallible. Maintenance paths that want an up-to-date L1 call
    /// `compact_if_needed` first.
    ///
    /// Opening is Θ(sources), so a read that knows its key bound beforehand
    /// should take [`Self::open_cursor_in_range`].
    pub fn open_cursor(&self) -> ReadCursor {
        read_cursor::from_runs(self.runs(), self.schema)
    }

    /// A cursor that can answer only about keys in `[start, end]`, `end` `None`
    /// meaning the top of the key space. Unpositioned, like
    /// [`Self::open_cursor`] — the caller seeks or probes within the bound it
    /// named.
    ///
    /// The gather over-approximates (a whole guard comes in for one key), so a
    /// half-open `end` is safe to pass.
    pub fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> ReadCursor {
        let runs = self
            .mem_runs()
            .chain(self.shard_index.shard_arcs_in_range(start, end).map(Run::Shard));
        read_cursor::from_runs(runs, self.schema)
    }

    /// Return the fully consolidated batch of all live rows, caching the result.
    /// The cache is invalidated on any logical write (upsert or test-helper upsert).
    /// Cheap on repeated calls: returns `Rc::clone` of the cached batch.
    /// Infallible: delegates to `open_cursor`.
    pub fn full_scan(&mut self) -> Rc<Batch> {
        if let Some(ref rc) = self.cached_full_scan {
            return Rc::clone(rc);
        }
        let rc = self.open_cursor().materialize();
        self.cached_full_scan = Some(Rc::clone(&rc));
        rc
    }

    /// Raw rows across this table's three read tiers — memtable runs, RAM tier,
    /// shards — summed by arithmetic alone: no `Rc` is cloned, no merge tree is
    /// built, nothing is repositioned. Raw, so cross-run duplicates and ghosts
    /// are counted; that is an upper bound on what a walk would emit, which is
    /// what the index selectivity gate compares its measured range size against.
    pub(crate) fn estimated_rows(&self) -> usize {
        self.ram_tiers().iter().map(|s| s.row_count()).sum::<usize>() + self.shard_index.total_rows()
    }

    /// Test helper: returns true when the memtable has no rows.
    #[cfg(test)]
    pub(crate) fn memtable_is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Test helper: RAM-tier run count.
    #[cfg(test)]
    pub(crate) fn ram_run_count(&self) -> usize {
        self.ram_tier.len()
    }

    /// Test helper: RAM-tier row count.
    #[cfg(test)]
    pub(crate) fn ram_row_count(&self) -> usize {
        self.ram_tier.row_count()
    }

    /// Test helper: RAM-tier heap footprint.
    #[cfg(test)]
    pub(crate) fn ram_bytes(&self) -> usize {
        self.ram_tier.bytes()
    }

    /// Test helper: shard Rcs (production reads go through `runs`).
    #[cfg(test)]
    pub(crate) fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.shard_index.all_shard_arcs()
    }

    /// Test helper: the FLSM tree's shape, for the amplification bench.
    #[cfg(test)]
    pub(crate) fn tree_report(&self) -> String {
        self.shard_index.tree_report()
    }

    /// Test helper: shrink the per-table heap ceiling so spill paths can be
    /// exercised without ingesting megabytes.
    #[cfg(test)]
    pub(crate) fn set_inmem_ceiling_for_test(&mut self, bytes: usize) {
        self.ram_tier.set_budget(bytes);
    }

    /// Test helper: stop this store's writes from building a PK filter, so a
    /// benchmark can price one against the same ingest run without it.
    #[cfg(test)]
    pub(crate) fn set_skip_pk_filter_for_test(&mut self, skip: bool) {
        self.shard_index.set_skip_pk_filter_for_test(skip);
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
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        self.has_pk_bytes(opk.pk_bytes())
    }

    /// Byte-keyed PK existence check: the net weight across every tier is
    /// positive.
    #[inline]
    pub fn has_pk_bytes(&self, key: &[u8]) -> bool {
        let mut w: i64 = 0;
        self.for_each_pk_candidate(key, |row| w += row.weight());
        w > 0
    }

    /// Visit every row whose PK equals `key`, in tier order — memtable, RAM
    /// tier, then shards. Each RAM tier gates on its own bloom and each shard on
    /// its PK range plus PK filter, so a miss costs a few loads and no search.
    ///
    /// The one PK walk of the table. Both point-lookup entry points read through
    /// it, so no tier can be visible to one and invisible to the other — a live
    /// row parked in the RAM tier between checkpoints is found exactly like a
    /// memtable row.
    fn for_each_pk_candidate(&self, key: &[u8], mut f: impl FnMut(StoredRow)) {
        // One derivation for every filter this walk consults — both RAM-tier
        // blooms and each shard's PK filter.
        let fingerprint = probe_key(key);
        for set in self.ram_tiers() {
            if !set.may_contain(fingerprint) {
                continue;
            }
            for batch in set.runs() {
                let run = Run::Mem(Rc::clone(batch));
                for row in run.pk_match_rows(key) {
                    f(StoredRow { run: run.clone(), row });
                }
            }
        }
        // The shard index's gated binary search already lands on the first
        // match, so the scan resumes from there rather than re-searching.
        self.shard_index.find_pk_bytes(key, fingerprint, &mut |shard, start| {
            let count = shard.count;
            for row in pk_match_rows_from(&*shard, count, start, key) {
                f(StoredRow {
                    run: Run::Shard(Rc::clone(&shard)),
                    row,
                });
            }
        });
    }

    /// Look up a PK for retraction.  Returns (net_weight, located row).
    ///
    /// Takes a **native** `u128` and `opk_key`s it; the DML retraction path
    /// keys on verbatim OPK bytes via `retract_pk_bytes`, so this native entry
    /// point has no production caller and is retained only for unit tests.
    #[cfg(test)]
    pub(crate) fn retract_pk(&mut self, key: u128) -> (i64, Option<StoredRow>) {
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        self.retract_pk_bytes(opk.pk_bytes())
    }

    /// Look up a PK for retraction by its OPK `key` bytes. Returns the net
    /// weight and, when it is positive, the live (PK, payload) row.
    ///
    /// Winner selection is one grouping pass over the cross-tier candidate pool
    /// in tier order (memtable first — the newest history). Groups form by
    /// payload equality; the first group whose weights net strictly positive
    /// holds the live row. Grouping over the whole pool rather than trusting any
    /// single row's own weight is what makes the answer right: a candidate whose
    /// weight is positive but whose full group nets ≤ 0 has been retracted by a
    /// later tier and must not be returned.
    pub(in crate::storage) fn retract_pk_bytes(&mut self, key: &[u8]) -> (i64, Option<StoredRow>) {
        let mut pool = std::mem::take(&mut self.retract_scratch);
        debug_assert!(pool.is_empty());

        let mut total_w: i64 = 0;
        self.for_each_pk_candidate(key, |row| {
            total_w += row.weight();
            pool.push(row);
        });

        let row = (total_w > 0)
            .then(|| {
                let winner = first_live_payload_group(&self.schema, &pool);
                debug_assert!(
                    winner.is_some(),
                    "positive net PK weight implies a positive payload group"
                );
                winner.map(|i| pool.swap_remove(i))
            })
            .flatten();

        pool.clear();
        self.retract_scratch = pool;
        (total_w, row)
    }

    // ------------------------------------------------------------------
    // Compaction
    // ------------------------------------------------------------------

    /// Run L0→L1+ compaction if the disk tier crossed its threshold. No manifest
    /// publish and no dir fsync happen here — the barrier is the sole
    /// manifest-publish point, and it fsyncs everything the new manifest
    /// references. Compaction only swaps the in-memory index and appends the
    /// superseded inputs to `pending_deletions`.
    ///
    /// The superseded inputs are not unlinked here for a store that publishes a
    /// manifest: unlinking mid-epoch would strand the last-published one over
    /// deleted files, so `flush_barrier` drains them once it has republished over
    /// the compacted index. A fed view's delta store publishes none and is in
    /// neither checkpoint round, so it unlinks at the end of each compaction
    /// instead — see `ShardIndex::unlink_superseded`.
    pub fn compact_if_needed(&mut self) -> Result<(), StorageError> {
        if !self.shard_index.should_compact() {
            return Ok(());
        }
        self.shard_index.run_compact()
    }

    /// Unlink compaction-superseded shard files, once no surviving manifest can
    /// reference them — post-publish. Best-effort: a still-present file is
    /// retried on the next drain.
    pub(in crate::storage) fn drain_deletions(&mut self) {
        self.shard_index.try_cleanup();
    }
}

// ---------------------------------------------------------------------------
// OS helpers
// ---------------------------------------------------------------------------

pub(crate) fn ensure_dir(dir: &str) -> Result<CString, StorageError> {
    let dir_c = super::super::cstr(dir)?;
    // Recursive: a table may home into a nested dir whose parents don't exist
    // yet (a worker's per-rank index subdir under a fresh index dir).
    std::fs::create_dir_all(dir)?;
    Ok(dir_c)
}

/// Open a table's directory, creating it if absent — the one way in, for both
/// the construction check and every file write. NOCOW (btrfs; ignored elsewhere)
/// is applied to every opened fd, so it also covers directories the catalog's
/// layout staging pre-created; `try_set_nocow` skips the write when the flag is
/// already set, and the re-application is a btrfs transaction commit.
pub(super) fn open_table_dirfd(dir: &str) -> Result<OwnedFd, StorageError> {
    use std::os::fd::AsRawFd;
    let open = |c: &CStr| crate::foundation::posix_io::open_owned(c, libc::O_RDONLY | libc::O_DIRECTORY);
    let fd = match open(&super::super::cstr(dir)?) {
        Ok(fd) => fd,
        Err(_) => open(&ensure_dir(dir)?)?,
    };
    crate::foundation::posix_io::try_set_nocow(fd.as_raw_fd());
    Ok(fd)
}

/// Drop a rederived table's on-disk state: this table's shard files (the
/// grammar includes `table_id`, so a shared directory keeps its other tables)
/// and its manifest. The manifest goes too, or a later open could accept a
/// generation whose shards are gone.
fn erase_stale_shards(dir: &str, table_id: u32) {
    super::naming::remove_shard_files(dir, table_id, &std::collections::HashSet::new());
    let _ = std::fs::remove_file(super::manifest::path(dir));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/table.rs"]
mod tests;
