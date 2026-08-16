//! Unified Table: two RAM-tier [`RunSet`]s over a `ShardIndex`.
//!
//! Ingest lands in the `memtable` run set and folds into the `ram_tier` at 3/4
//! of the arena; the RAM tier spills to a shard past [`INMEM_CEILING`], and the
//! checkpoint barrier folds it into one durable shard for `SalReplay` tables.
//! `Rederive` tables never publish a manifest — they are erased at open and
//! rebuilt from their sources.

use std::cmp::Ordering;
use std::ffi::{CStr, CString};
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
/// store wrote 98.7 MB of RAM-tier spill per 4M rows (~25 B/row) that the old
/// 256-child-per-relation layout did not; at 32 MiB that spill is gone and
/// throughput sits 23–28% above that layout, for +45 MB of cluster RSS. An
/// earlier sweep found this constant flat from 4 to 128 MiB, but ran at a 4 MiB
/// checkpoint threshold — 192× more frequent than production — which drains the
/// tier continuously and is exactly the regime where the ceiling cannot bind.
///
/// Spilling past the ceiling stays the intended safety valve; 32 MiB is where
/// ordinary ingest stops reaching it, not a promise that nothing will.
const INMEM_CEILING: usize = 32 * 1024 * 1024;

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
    /// Storage is erased at open and the relation is rebuilt (rederived) from
    /// its sources. Index circuits and non-checkpointed operator scratch.
    Rederive,
    /// Rederived like `Rederive`, but the ephemeral checkpoint round
    /// force-persists this table with a generation-stamped manifest (view
    /// operator-trace tables and output stores), and the open conditionally
    /// reloads it: the checkpointed shards load iff the manifest's generation
    /// equals `committed` (the caller's committed checkpoint generation);
    /// otherwise the state is erased and rebuilt. Compaction cleanup defers to
    /// the next publish like `SalReplay` — an immediate drain would unlink a
    /// compacted-away shard the last-published manifest still references
    /// (stranded at reload).
    RederiveCheckpointed {
        /// The committed checkpoint generation the reload gate compares against.
        committed: u64,
    },
}

impl RecoverySource {
    /// The checkpointed-rederive policy sampled at the current committed
    /// checkpoint generation — the one constructor for a view's output store
    /// and its operator-trace tables, so the reload gate's `committed` sample
    /// can never drift between the two.
    #[inline]
    pub fn rederive_checkpointed_now() -> RecoverySource {
        RecoverySource::RederiveCheckpointed {
            committed: crate::foundation::worker_ctx::committed_generation(),
        }
    }
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

    /// Reused candidate pool for `retract_pk_bytes`' grouping pass; cleared per
    /// call (dropping its `Rc`s) with capacity retained, so the path stops
    /// allocating once warmed up.
    retract_scratch: Vec<StoredRow>,

    cached_full_scan: Option<Rc<Batch>>,
}

mod flush;

#[cfg(test)]
mod bench_flush;

impl Table {
    /// Create a new table.  `RecoverySource::SalReplay` loads the manifest at
    /// open; `RecoverySource::Rederive` erases stale storage and starts empty.
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
        // Decide the open action per recovery source:
        //   SalReplay            → always load the manifest.
        //   Rederive             → always erase stale shards and start empty.
        //     DIRLESS: the directory is not created here. A Rederive table
        //     starts empty, its steady-state flushes land in the RAM tier, and
        //     it never publishes a manifest — so in the common case its dir
        //     would hold nothing, ever. `open_dirfd` (the single choke point
        //     every file write goes through) creates it lazily on the first
        //     spill, making a Rederive open free of filesystem work (this is
        //     what keeps an index circuit's per-worker scratch tables
        //     syscall-free). A dir left by a previous boot still gets
        //     its stale shards erased (a missing dir erases nothing).
        //   RederiveCheckpointed → load only when the manifest's checkpoint
        //     generation equals the caller's committed generation; otherwise
        //     erase the shards *and* unlink the manifest so a later re-open
        //     cannot re-peek a stale `== g` manifest.
        //
        // NOTE for future tests: a `RederiveCheckpointed` table force-flushed at
        // generation 0, then re-opened with `committed: 0`, will *load* (peek
        // `Some(0)` == committed `0`). Table unit tests that want a
        // guaranteed-empty open use `Rederive`, not `RederiveCheckpointed`.
        let load_shards = match recovery_source {
            RecoverySource::SalReplay => {
                // Try to set NOCOW (btrfs; silently ignored on other fs).
                set_nocow_dir(&ensure_dir(dir)?);
                true
            }
            RecoverySource::Rederive => {
                erase_stale_shards(dir, table_id);
                false
            }
            RecoverySource::RederiveCheckpointed { committed } => {
                // Eager, unlike the DIRLESS `Rederive` arm: creating the dir here
                // is what makes an unusable view directory fail the compile rather
                // than the first flush — a client-visible rejection on the master's
                // CREATE VIEW pre-flight, where a first-flush failure would be a
                // worker abort with no client left to tell.
                set_nocow_dir(&ensure_dir(dir)?);
                let cpath = super::super::cstr(super::manifest::path(dir))?;
                let generation_matches =
                    super::manifest::peek_header(&cpath)?.map(|h| h.checkpoint_gen) == Some(committed);
                if !generation_matches {
                    erase_stale_shards(dir, table_id);
                }
                generation_matches
            }
        };

        let mut table = Table {
            // Fold at 3/4 of the arena so the next ingest batch always fits.
            memtable: RunSet::new(arena_size as usize * 3 / 4),
            ram_tier: RunSet::new(INMEM_CEILING),
            shard_index: ShardIndex::new(table_id, dir, schema),
            schema,
            table_id,
            directory: dir.to_string(),
            recovery_source,
            current_lsn: 1,
            layout_seq: 0,
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

    /// Full path of this table's manifest — a pure function of the directory.
    /// Only `SalReplay` tables publish one (`Rederive` state is erased at open),
    /// but the path itself carries no state worth caching.
    fn manifest_full_path(&self) -> String {
        super::manifest::path(&self.directory)
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

    /// How this table's tail is recovered. Base and master system tables are
    /// `SalReplay`; view child tables and index circuits are `Rederive`.
    pub fn recovery_source(&self) -> RecoverySource {
        self.recovery_source
    }

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

    /// Every run this table reads through, newest tier first.
    pub(crate) fn runs(&self) -> impl Iterator<Item = Run> + '_ {
        self.ram_tiers()
            .into_iter()
            .flat_map(|set| set.runs().iter().cloned().map(Run::Mem))
            .chain(self.shard_index.all_shard_arcs_iter().map(Run::Shard))
    }

    /// Open a read-only cursor over every tier. Does NOT mutate the table —
    /// compaction is a maintenance operation, not part of the read path. Cheap
    /// and infallible. Maintenance paths that want an up-to-date L1 call
    /// `compact_if_needed` first.
    pub fn open_cursor(&self) -> ReadCursor {
        read_cursor::from_runs(self.runs(), self.schema)
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

    /// Test helper: shrink the per-table heap ceiling so spill paths can be
    /// exercised without ingesting megabytes.
    #[cfg(test)]
    pub(crate) fn set_inmem_ceiling_for_test(&mut self, bytes: usize) {
        self.ram_tier.set_budget(bytes);
    }

    /// Test helper: push a consolidated batch straight into the memtable,
    /// skipping the ingest path's flush check.
    #[cfg(test)]
    pub(crate) fn memtable_upsert_sorted_batch(&mut self, batch: Batch) {
        self.cached_full_scan = None;
        self.memtable.push(Rc::new(batch), &self.schema);
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
    pub fn has_pk_bytes(&self, key: &[u8]) -> bool {
        let mut w: i64 = 0;
        self.for_each_pk_candidate(key, |row| w += row.weight());
        w > 0
    }

    /// Visit every row whose PK equals `key`, in tier order — memtable, RAM
    /// tier, then shards. Each RAM tier gates on its own bloom and each shard on
    /// its PK range plus XOR8 filter, so a miss costs a few loads and no search.
    ///
    /// The one PK walk of the table. Both point-lookup entry points read through
    /// it, so no tier can be visible to one and invisible to the other — a live
    /// row parked in the RAM tier between checkpoints is found exactly like a
    /// memtable row.
    fn for_each_pk_candidate(&self, key: &[u8], mut f: impl FnMut(StoredRow)) {
        // One derivation for every filter this walk consults — both RAM-tier
        // blooms and each shard's XOR8.
        let probe_key = super::xor8::probe_key(key);
        for set in self.ram_tiers() {
            if !set.may_contain(probe_key) {
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
        self.shard_index.find_pk_bytes(key, probe_key, &mut |shard, start| {
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
    pub fn retract_pk_bytes(&mut self, key: &[u8]) -> (i64, Option<StoredRow>) {
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
    /// Cleanup timing splits on recovery source. A `Rederive` table publishes
    /// no manifest referencing the superseded inputs, so it drains them
    /// immediately — deferring would leak them until boot-time
    /// `erase_stale_shards`. A `SalReplay` or `RederiveCheckpointed` table
    /// defers both publish and cleanup to the next barrier, which republishes
    /// over the compacted index before unlinking the inputs; unlinking
    /// mid-epoch would strand the last-published manifest over deleted files
    /// on the next boot.
    pub fn compact_if_needed(&mut self) -> Result<(), StorageError> {
        if !self.shard_index.should_compact() {
            return Ok(());
        }
        self.shard_index.run_compact()?;
        if self.recovery_source == RecoverySource::Rederive {
            self.drain_deletions();
        }
        Ok(())
    }

    /// Unlink compaction-superseded shard files, once no surviving manifest can
    /// reference them: post-publish for `SalReplay` tables (the worker barrier
    /// per family, the synchronous `flush()` inline), immediately after
    /// `run_compact` for `Rederive` tables. Best-effort — a still-present file
    /// is retried on the next drain.
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
    std::fs::create_dir_all(dir).map_err(|_| StorageError::Io)?;
    Ok(dir_c)
}

fn set_nocow_dir(dir_c: &CStr) {
    use std::os::fd::AsRawFd;
    if let Some(fd) = crate::foundation::posix_io::open_owned(dir_c, libc::O_RDONLY) {
        crate::foundation::posix_io::try_set_nocow(fd.as_raw_fd());
    }
}

/// Drop a rederived table's on-disk state: this table's shard files (the
/// grammar includes `table_id`, so a shared directory keeps its other tables)
/// and its manifest. The manifest goes too, or a later open could accept a
/// generation whose shards are gone.
///
/// Creates nothing, which is what keeps a `Rederive` open dirless.
fn erase_stale_shards(dir: &str, table_id: u32) {
    super::naming::remove_shard_files(dir, table_id, &std::collections::HashSet::new());
    let _ = std::fs::remove_file(super::manifest::path(dir));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::flush_barrier::FlushRound;
    use super::super::run_set::FOLD_THRESHOLD;
    use gnitz_expr::RowSource;

    /// Payload column 0 (an 8-byte integer) of a located row — the one read
    /// every `retract_pk` assertion makes.
    fn row_val(fr: &StoredRow) -> i64 {
        i64::from_le_bytes(RowSource::get_col_ptr(&fr.run, fr.row, 0, 8).try_into().unwrap())
    }
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::{make_batch_raw, make_schema_u64_i64, opk_pk, wide_pk_3xu64_schema, wide_row};

    /// Unsorted `Raw` rows for the U64+I64 schema; the ingest path runs the
    /// canonical sort+fold.
    fn make_batch(rows: &[(u64, i64, i64)]) -> Batch {
        make_batch_raw(&make_schema_u64_i64(), rows)
    }

    /// `Table::with_arena(...)` with the per-test boilerplate folded away.
    fn new_table(dir: &std::path::Path, schema: SchemaDescriptor, id: u32, arena: u64, rs: RecoverySource) -> Table {
        Table::with_arena(dir.to_str().unwrap(), schema, id, arena, rs).unwrap()
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

    /// Count files directly in `dir` whose basename satisfies `pred`.
    fn count_files(dir: &std::path::Path, pred: impl Fn(&str) -> bool) -> usize {
        std::fs::read_dir(dir)
            .map(|rd| rd.flatten().filter(|e| pred(&e.file_name().to_string_lossy())).count())
            .unwrap_or(0)
    }

    /// Count compaction-output files for `table_id` — named
    /// `shard_{tid}_{seq}_L{n}_G{gk}.db`; the `_L` marker distinguishes them
    /// from flat spill/barrier shards. Presence proves a compaction ran.
    fn compaction_output_count(dir: &std::path::Path, table_id: u32) -> usize {
        let shard = format!("shard_{table_id}_");
        count_files(dir, |n| n.starts_with(&shard) && n.contains("_L"))
    }

    /// Count every on-disk shard/compaction-output file for `table_id`.
    fn all_shard_file_count(dir: &std::path::Path, table_id: u32) -> usize {
        let shard = format!("shard_{table_id}_");
        count_files(dir, |n| n.starts_with(&shard))
    }

    /// Run a full synchronous force-publish flush stamped at `generation` —
    /// the ephemeral checkpoint round's prepare + commit, minus the fsyncs the
    /// tests don't observe.
    fn flush_ephemeral_at(t: &mut Table, generation: u64) {
        match t.flush_prepare(FlushRound::Ephemeral(generation)).unwrap() {
            FlushOutcome::Pending(work) => {
                let _ = t.flush_commit(work).unwrap();
                t.drain_deletions();
            }
            FlushOutcome::Done => {}
        }
    }

    /// Materialize `open_cursor` into a (pk -> net_weight) map.
    fn materialize_weights(t: &Table) -> std::collections::HashMap<u64, i64> {
        let mut out = std::collections::HashMap::new();
        let batch = t.open_cursor().materialize();
        for i in 0..batch.count {
            out.insert(batch.get_pk(i) as u64, batch.get_weight(i));
        }
        out
    }

    #[test]
    fn table_ephemeral_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("eph_test");
        let schema = make_schema_u64_i64();

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
    }

    #[test]
    fn table_persistent_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("pers_test");
        let schema = make_schema_u64_i64();

        let mut t = new_table(&tdir, schema, 200, 1 << 20, RecoverySource::SalReplay);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
        t.flush().unwrap();

        assert!(t.has_pk(10));

        // Re-open and recover
        let mut t2 = new_table(&tdir, schema, 200, 1 << 20, RecoverySource::SalReplay);

        // Data should be in shards via manifest
        assert!(t2.has_pk(10));
        assert!(t2.has_pk(20));
    }

    #[test]
    fn table_cursor_iteration() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cursor_test");
        let schema = make_schema_u64_i64();

        let mut t = new_table(&tdir, schema, 300, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(30, 1, 300), (10, 1, 100), (20, 1, 200)]))
            .unwrap();

        let cursor = t.open_cursor();
        assert!(cursor.valid);
        assert_eq!(cursor.current_key_narrow() as u64, 10);
        // Don't need to iterate further — cursor creation works
    }

    #[test]
    fn table_retract_pk() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("retract_test");
        let schema = make_schema_u64_i64();

        let mut t = new_table(&tdir, schema, 400, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1);
        assert!(found.is_some());
        // The retracted row is the found row: a valid null word and an
        // accessible payload column, read through the RowSource view.
        let fr = found.expect("retracted row is the found row");
        assert_ne!(RowSource::get_null_word(&fr.run, fr.row), u64::MAX);
        assert_eq!(row_val(&fr), 100);

        let (w, found) = t.retract_pk(99);
        assert_eq!(w, 0);
        assert!(found.is_none());
    }

    #[test]
    fn table_compact() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("compact_test");
        let schema = make_schema_u64_i64();

        // Durable: each flush writes a real `shard_*` so `run_compact` (L0→L1)
        // is exercised. Under non-durable flush these tiny rows would stay in
        // the RAM tier and never reach the disk compaction path.
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
        let schema = make_schema_u64_i64();

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
        assert!(found.is_some());

        // The found row must be val=200, not the cancelled val=100
        let fr = found.expect("retracted row is the found row");
        let val = row_val(&fr);
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
        let schema = make_schema_u64_i64();

        let mut t = new_table(&tdir, schema, 700, 1 << 20, RecoverySource::Rederive);

        // Build a reverse-sorted batch (PK order: 30, 20, 10).
        let batch = make_batch(&[(30, 1, 300), (20, 1, 200), (10, 1, 100)]);
        // make_batch produces a Raw (unsorted) batch.
        assert!(!batch.is_sorted());

        t.ingest_owned_batch(batch).unwrap();

        // Cursor must yield rows in ascending PK order
        let cursor = t.open_cursor();
        assert!(cursor.valid);
        assert_eq!(
            cursor.current_key_narrow() as u64,
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
        let schema = make_schema_u64_i64();

        // Very small arena: 40 bytes. A 3-row batch (~120 bytes) will exceed it.
        let mut t = new_table(&tdir, schema, 900, 40, RecoverySource::Rederive);

        // Directly fill memtable past max_bytes using memtable_upsert_sorted_batch
        // (bypasses auto-flush so runs_bytes exceeds max_bytes).
        let fill_batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let fill_batch = fill_batch.into_consolidated(&schema);
        t.memtable_upsert_sorted_batch(fill_batch);
        // runs_bytes (~120) > max_bytes (40) — next ingest must pre-flush
        // before upsert. The new owned-batch path checks should_flush()
        // before upserting, so the pre-fill goes to a shard cleanly.

        // Ingest a REVERSE-sorted batch. ingest_owned_batch must sort it
        // (via into_consolidated) before insert.
        t.ingest_owned_batch(make_batch(&[(50, 1, 500), (40, 1, 400), (30, 1, 300)]))
            .unwrap();

        // fill batch (1,2,3) is now in shard; sorted (30,40,50) is in memtable.
        let cursor = t.open_cursor();
        assert!(cursor.valid);
        assert_eq!(
            cursor.current_key_narrow() as u64,
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
        let schema = make_schema_u64_i64();

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
            t.ram_run_count() > 0,
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
        let schema = make_schema_u64_i64();

        // Durable: `retract_pk` is base-table-only (base tables are durable), so
        // the flushed rows must land in a real shard for the shard-fallback path
        // under test — not the RAM tier (which a non-durable flush would use).
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
        assert!(found.is_some());

        let fr = found.expect("retracted row is the found row");
        let val = row_val(&fr);
        assert_eq!(
            val, 200,
            "shard fallback must pick live payload (val=200), not cancelled (val=100)"
        );
    }

    /// Dropping a `Pending` FlushWork without committing must unlink the staged
    /// manifest `.tmp`, leaving the directory clean for a future retry. The
    /// folded shard was written at its final name (not a `.tmp`) and registered
    /// in the index by `flush_prepare`, so it survives as an orphan the next
    /// open's `gc_orphans` reclaims — no `.tmp` residue either way.
    #[test]
    fn flush_prepare_drop_cleans_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("drop_clean_test");
        let schema = make_schema_u64_i64();

        let mut t = new_table(&tdir, schema, 1100, 1 << 20, RecoverySource::SalReplay);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

        match t.flush_prepare(FlushRound::Base).unwrap() {
            FlushOutcome::Pending(work) => {
                let dir_entries: Vec<String> = std::fs::read_dir(&tdir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect();
                assert!(
                    dir_entries.iter().any(|n| n == "manifest.bin.tmp"),
                    "the staged manifest .tmp must exist before Drop, got {dir_entries:?}"
                );
                drop(work);
            }
            FlushOutcome::Done => panic!("expected Pending, got Done"),
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

    /// A table's layout sequence is loaded at open and re-stamped by every
    /// publish, so it survives an arbitrary number of checkpoints. That
    /// persistence is what lets the boot relayout resolve two complete sets after
    /// a crash by picking the newer one — a publish that reset it to 0 would make
    /// a stale set win.
    #[test]
    fn base_publish_preserves_the_layout_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("layout_seq");
        let schema = make_schema_u64_i64();
        let cpath = crate::storage::cstr(super::super::manifest::path(tdir.to_str().unwrap())).unwrap();
        let peek_layout_seq =
            |p: &std::ffi::CStr| super::super::manifest::peek_header(p).unwrap().map(|h| h.layout_seq);

        // Stamp a set at sequence 7, as the relayout's target write would.
        {
            let mut t = new_table(&tdir, schema, 88, 1 << 20, RecoverySource::SalReplay);
            t.ingest_owned_batch(make_batch(&[(1, 1, 10)])).unwrap();
            t.flush().unwrap();
            let (entries, header) = super::super::manifest::read_file(&cpath).unwrap().unwrap();
            super::super::manifest::prepare_file(
                &cpath,
                &entries,
                super::super::manifest::ManifestHeader {
                    layout_seq: 7,
                    ..header
                },
            )
            .unwrap()
            .commit()
            .unwrap();
        }

        // Two further checkpoints, one with new rows and one without, must both
        // re-stamp 7 rather than reset it.
        let mut t = new_table(&tdir, schema, 88, 1 << 20, RecoverySource::SalReplay);
        t.ingest_owned_batch(make_batch(&[(2, 1, 20)])).unwrap();
        t.flush().unwrap();
        assert_eq!(peek_layout_seq(&cpath), Some(7));
        t.flush().unwrap();
        assert_eq!(
            peek_layout_seq(&cpath),
            Some(7),
            "an unchanged publish must not reset the layout sequence"
        );
    }

    /// Table::new on a corrupted manifest must return Err and must not run
    /// gc_orphans — any stray shard files must survive untouched.
    #[test]
    fn table_new_corrupted_manifest_preserves_stray_shard() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("corrupted_manifest_test");
        std::fs::create_dir_all(&tdir).unwrap();
        let schema = make_schema_u64_i64();

        // Write a corrupted manifest (wrong magic).
        let manifest_path = tdir.join("manifest.bin");
        std::fs::write(&manifest_path, b"not a valid manifest").unwrap();

        // Drop a stray shard file.
        let stray = tdir.join("shard_200_1.db");
        std::fs::write(&stray, b"orphan").unwrap();

        let result = Table::with_arena(tdir.to_str().unwrap(), schema, 200, 1 << 20, RecoverySource::SalReplay);
        assert!(result.is_err(), "Table::new must fail on corrupted manifest");
        assert!(stray.exists(), "stray shard must survive when gc_orphans did not run");
    }

    /// Non-durable `flush_prepare` consolidates the snapshot into the in-memory
    /// run set and returns `Done`; the memtable is reset, no shard file is
    /// written, and the rows stay visible to subsequent reads.
    #[test]
    fn flush_prepare_non_durable_done_inline() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("done_inline_test");
        let schema = make_schema_u64_i64();

        let mut t = new_table(&tdir, schema, 1200, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
        match t.flush_prepare(FlushRound::Base).unwrap() {
            FlushOutcome::Done => {}
            FlushOutcome::Pending(_) => panic!("expected Done, got Pending"),
        }
        assert!(
            t.memtable_is_empty(),
            "memtable must be reset after non-durable flush_prepare"
        );
        assert!(t.ram_run_count() > 0, "snapshot must land in the RAM tier");
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
            let mut b = Batch::with_capacity(schema, rows.len().max(1));
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
        // flush would route the rows into the RAM tier instead.
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
        assert!(found.is_some());
        let (w2, found2) = t.retract_pk_bytes(&pk3(1, 1, 100));
        assert_eq!(w2, 0);
        assert!(found2.is_none());
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
            crate::schema::key::compare_pk_bytes(&key(-1, 0, 0), &key(1, 0, 0)),
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
        assert_eq!(w, 1);
        assert!(found.is_some());
        t.ingest_owned_batch(row(-5, -1)).unwrap();
        assert!(!t.has_pk_bytes(&key(-5, 0, 0)), "retracted signed key is gone");
    }

    // ── In-memory Rederive flush (RAM tier, no file I/O) ─────────────────

    /// A sub-ceiling Rederive flush writes no file at all; rows are served
    /// from the RAM tier via the cursor.
    #[test]
    fn nondurable_flush_writes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("no_file_test");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200), (30, 1, 300)]))
            .unwrap();
        assert!(matches!(t.flush_prepare(FlushRound::Base).unwrap(), FlushOutcome::Done));

        // A Rederive table opens dirless (created lazily on first spill), so a
        // sub-ceiling flush leaves not even the directory behind — the
        // strongest form of "no file".
        let shard_files: Vec<String> = match std::fs::read_dir(&tdir) {
            Err(_) => Vec::new(),
            Ok(rd) => rd
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("shard_"))
                .collect(),
        };
        assert!(shard_files.is_empty(), "non-durable flush wrote files: {shard_files:?}");
        assert!(t.all_shard_arcs().is_empty());
        assert!(t.ram_run_count() > 0);

        let w = materialize_weights(&t);
        assert_eq!(w.get(&10), Some(&1));
        assert_eq!(w.get(&20), Some(&1));
        assert_eq!(w.get(&30), Some(&1));
    }

    /// Cross-flush churn (insert in one flush, retraction in another) folds at
    /// `FOLD_THRESHOLD` to net state — the cancelled key is gone, not
    /// accumulated.
    #[test]
    fn nondurable_cross_flush_fold_nets_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cross_fold_test");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

        // 6 alternating +1 / -1 flushes on (k=7, v=70): each lands in its own
        // run until the threshold folds them. Net weight is 0.
        for i in 0..6 {
            let w = if i % 2 == 0 { 1 } else { -1 };
            t.ingest_owned_batch(make_batch(&[(7, w, 70)])).unwrap();
            t.flush().unwrap();
        }
        assert!(!t.has_pk(7), "net-zero key must not be present");
        let weights = materialize_weights(&t);
        assert!(!weights.contains_key(&7), "net-zero key folds away (0 rows)");
        assert!(t.ram_run_count() <= FOLD_THRESHOLD, "run set must stay folded",);
        assert!(
            shard_db_files(&tdir, 100).is_empty(),
            "churn must not spill (tiny, sub-ceiling)"
        );
    }

    /// More than `FOLD_THRESHOLD` distinct-key flushes keep the run
    /// count bounded by folding, and every key survives.
    #[test]
    fn nondurable_run_count_stays_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("run_bound_test");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

        let n = FOLD_THRESHOLD as u64 + 4;
        for k in 0..n {
            t.ingest_owned_batch(make_batch(&[(k, 1, (k * 10) as i64)])).unwrap();
            t.flush().unwrap();
            assert!(
                t.ram_run_count() <= FOLD_THRESHOLD,
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
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);
        t.set_inmem_ceiling_for_test(100); // < one flush (~10 rows × 32 B)

        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        t.flush().unwrap();

        assert!(
            !shard_db_files(&tdir, 100).is_empty(),
            "ceiling breach must spill to a shard file"
        );
        assert_eq!(t.ram_bytes(), 0, "heap drained after spill");
        assert!(t.ram_run_count() == 0);
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
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);
        t.set_inmem_ceiling_for_test(100);

        const ROUNDS: u64 = 20;
        const PER: u64 = 10;
        for r in 0..ROUNDS {
            let rows: Vec<(u64, i64, i64)> = (0..PER)
                .map(|j| (r * PER + j, 1, ((r * PER + j) * 10) as i64))
                .collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            t.flush().unwrap();
            assert_eq!(t.ram_bytes(), 0, "round {r}: heap drained after spill");
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

    /// `has_pk` reflects net weight held only in the RAM tier: true after an
    /// insert flush, false after the net-zero retraction flush.
    #[test]
    fn nondurable_has_pk_over_in_memory_runs() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("has_pk_inmem_test");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

        t.ingest_owned_batch(make_batch(&[(5, 1, 50)])).unwrap();
        t.flush().unwrap();
        assert!(t.memtable_is_empty());
        assert!(t.has_pk(5), "in-memory positive-weight row must be found");

        t.ingest_owned_batch(make_batch(&[(5, -1, 50)])).unwrap();
        t.flush().unwrap();
        assert!(!t.has_pk(5), "net-zero key across in-memory runs must be absent");
    }

    /// After a spill, fresh heap runs coexist with disk shards; `open_cursor`
    /// returns their union with correct net weights, including a cross-tier
    /// retraction (heap retraction cancelling a disk insert).
    #[test]
    fn nondurable_mixed_disk_and_heap_read() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("mixed_read_test");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::Rederive);

        // Force keys 0..10 to disk.
        t.set_inmem_ceiling_for_test(100);
        let disk_rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&disk_rows)).unwrap();
        t.flush().unwrap();
        assert!(!t.all_shard_arcs().is_empty(), "first flush must spill to disk");
        assert_eq!(t.ram_bytes(), 0);

        // Raise ceiling so subsequent flushes stay in heap.
        t.set_inmem_ceiling_for_test(usize::MAX);
        let heap_rows: Vec<(u64, i64, i64)> = (100..105).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&heap_rows)).unwrap();
        t.flush().unwrap();
        assert!(t.ram_run_count() > 0, "second flush stays in heap");

        // Cross-tier retraction: cancel disk key 3 (payload 30) from heap.
        t.ingest_owned_batch(make_batch(&[(3, -1, 30)])).unwrap();
        t.flush().unwrap();

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

    // ── RAM-tier (the RAM tier) found-row path ─────────────────────────
    //
    // Twins of the durable retract tests above, but the rows stay in
    // the RAM tier (non-durable flush, sub-ceiling) instead of on disk, so the
    // `retract_pk*` / `has_pk_bytes` / `get_weight_for_row_bytes` RAM-tier code
    // is what's exercised. `retract_pk*` is production-invoked only on durable
    // base tables, but the machinery is tier-agnostic and these drive it directly.

    /// Twin of `test_retract_pk_shard_fallback_multiple_payloads`, in RAM: an
    /// UPDATE delta split across two in-memory runs. The multi-candidate
    /// global-net loop over the RAM tier must pick the live payload (200), not
    /// the cancelled one (100).
    #[test]
    fn inmem_retract_multiple_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_retract_payloads");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 5001, 1 << 20, RecoverySource::Rederive);

        // Run 1: INSERT (PK=10, +1, val=100).
        t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();
        t.flush().unwrap();
        // Run 2: UPDATE delta — retract val=100, insert val=200.
        t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)]))
            .unwrap();
        t.flush().unwrap();

        assert!(t.memtable_is_empty());
        assert_eq!(t.ram_run_count(), 2, "two sub-ceiling flushes → two L0 runs");

        let (w, found) = t.retract_pk(10);
        assert_eq!(w, 1, "net weight 1 across the RAM runs");
        assert!(found.is_some());
        let fr = found.expect("retracted row is the found row");
        let val = row_val(&fr);
        assert_eq!(
            val, 200,
            "RAM-tier global-net must pick live payload 200, not cancelled 100"
        );
    }

    /// Twin of `table_wide_pk_has_and_retract_bytes`, in RAM: wide (`pk_stride =
    /// 24`) prefix-twins `(1,1,100)`/`(1,1,200)` held in the RAM tier. Retract
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
            let mut b = Batch::with_capacity(schema, rows.len().max(1));
            for (pk, w, val) in rows {
                b.extend_pk_bytes(pk);
                b.extend_weight(&w.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &val.to_le_bytes());
                b.count += 1;
            }
            b
        };

        // Two prefix-twins + a distinct key, flushed into the RAM tier.
        t.ingest_owned_batch(wide_batch(&[
            (pk3(1, 1, 100), 1, 10),
            (pk3(1, 1, 200), 1, 20),
            (pk3(2, 0, 0), 1, 30),
        ]))
        .unwrap();
        t.flush().unwrap();
        assert!(t.has_pk_bytes(&pk3(1, 1, 100)));
        assert!(t.has_pk_bytes(&pk3(1, 1, 200)));
        assert!(t.has_pk_bytes(&pk3(2, 0, 0)));
        assert!(!t.has_pk_bytes(&pk3(1, 1, 300)), "absent prefix-twin must not be found");

        // Retract one twin via a second in-memory run; the other must survive.
        t.ingest_owned_batch(wide_batch(&[(pk3(1, 1, 100), -1, 10)])).unwrap();
        t.flush().unwrap();
        assert!(!t.has_pk_bytes(&pk3(1, 1, 100)), "retracted twin gone");
        assert!(t.has_pk_bytes(&pk3(1, 1, 200)), "the other twin survives");

        let (w, found) = t.retract_pk_bytes(&pk3(1, 1, 200));
        assert_eq!(w, 1);
        assert!(found.is_some());
        let fr = found.expect("live twin is the found row");
        let val = row_val(&fr);
        assert_eq!(val, 20, "found row must be the surviving twin's payload");

        let (w2, found2) = t.retract_pk_bytes(&pk3(1, 1, 100));
        assert_eq!(w2, 0, "net-zero twin reports absent");
        assert!(found2.is_none());
    }

    /// Twin of `table_wide_signed_compound_pk_opk_order`, in RAM: a signed
    /// leading PK column, rows in the RAM tier. OPK order is read back via
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
        t.flush().unwrap(); // one in-memory L0 run
        assert!(t.ram_run_count() > 0, "rows land in the RAM tier");

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
        assert_eq!(w, 1);
        assert!(found.is_some());
        let fr = found.expect("signed key is the found row");
        let val = row_val(&fr);
        assert_eq!(val, -5, "found row payload marks the retracted signed key");

        // Retraction across a second run nets the signed key to zero.
        t.ingest_owned_batch(row(-5, -1)).unwrap();
        t.flush().unwrap();
        assert!(!t.has_pk_bytes(&key(-5, 0, 0)), "retracted signed key is gone");
    }

    /// The live row sits in RAM while the memtable holds a *negative*-weight
    /// entry for the same PK. `get_weight_for_row_bytes` must net memtable + RAM
    /// per candidate (killing the payload that cancels to zero) and arm the
    /// globally-live payload from the RAM run.
    #[test]
    fn inmem_cross_tier_netting() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_cross_tier");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 5004, 1 << 20, RecoverySource::Rederive);

        // RAM run: two payloads for PK=5 (val=50, val=60), each +1.
        t.ingest_owned_batch(make_batch(&[(5, 1, 50), (5, 1, 60)])).unwrap();
        t.flush().unwrap();
        // Memtable (unflushed): retract val=50, leaving (5,60) globally live.
        t.ingest_owned_batch(make_batch(&[(5, -1, 50)])).unwrap();
        assert!(!t.memtable_is_empty(), "retraction stays in the memtable");

        assert!(t.has_pk(5), "PK 5 nets +1 across RAM (+2) and memtable (-1)");

        let (w, found) = t.retract_pk(5);
        assert_eq!(w, 1, "global net weight is 1");
        assert!(found.is_some());
        let fr = found.expect("live row found");
        let val = row_val(&fr);
        assert_eq!(
            val, 60,
            "global oracle rejects the cancelled val=50, arms live val=60 from RAM"
        );
    }

    /// Three-tier retract grouping: one PK with distinct payloads split across
    /// a durable shard, the RAM tier, and the memtable. The grouping pass must
    /// net each payload across ALL tiers — the shard payload cancelled by a
    /// memtable retraction must not be armed even though its shard row alone
    /// carries +1; the RAM payload is the only globally-live group.
    #[test]
    fn retract_groups_across_all_three_tiers() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("retract_three_tier");
        let schema = make_schema_u64_i64();
        // Durable: shard tier participation requires SalReplay persistence.
        let mut t = new_table(&tdir, schema, 5014, 1 << 20, RecoverySource::SalReplay);

        // Shard: (7, val=70, +1) — flushed durably.
        t.ingest_owned_batch(make_batch(&[(7, 1, 70)])).unwrap();
        t.flush().unwrap();
        assert!(t.shard_index_max_lsn() > 0, "row landed in a durable shard");

        // RAM tier: (7, val=80, +1) — folded into the RAM tier, not durable.
        t.ingest_owned_batch(make_batch(&[(7, 1, 80)])).unwrap();
        t.flush_to_ram().unwrap();
        assert!(t.ram_run_count() > 0, "val=80 sits in the RAM tier");

        // Memtable: retract the SHARD payload (7, val=70, -1) — unflushed.
        t.ingest_owned_batch(make_batch(&[(7, -1, 70)])).unwrap();
        assert!(!t.memtable_is_empty(), "retraction stays in the memtable");

        // Global nets: val=70 → shard +1, memtable −1 = 0 (dead);
        //              val=80 → RAM +1 (live). Total = +1.
        let (w, found) = t.retract_pk(7);
        assert_eq!(w, 1, "total net weight across all three tiers");
        let fr = found.expect("live row found");
        let val = row_val(&fr);
        assert_eq!(
            val, 80,
            "grouping must net val=70 across shard+memtable to zero and arm the RAM-tier val=80"
        );
    }

    /// More than `FOLD_THRESHOLD` runs force a RAM-tier fold, which
    /// rebuilds the folded run's PK bloom via `InMemRun::from_batch`. After the
    /// fold, live rows must still be found (no false negative from the rebuilt
    /// bloom) and an all-runs-absent PK must report absent (bloom-miss path
    /// equals the linear result).
    #[test]
    fn inmem_fold_rebuilds_run_bloom() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("inmem_fold_bloom");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 5005, 1 << 20, RecoverySource::Rederive);

        // One key per flush past the fold threshold; the folded run's bloom is
        // rebuilt over the merged batch.
        let n = FOLD_THRESHOLD as u64 + 2;
        for k in 0..n {
            t.ingest_owned_batch(make_batch(&[(k, 1, 100 + k as i64)])).unwrap();
            t.flush().unwrap();
        }
        assert!(t.ram_run_count() <= FOLD_THRESHOLD, "fold kept the run count bounded");

        // Every live key survives folding and is found through the rebuilt bloom.
        for k in 0..n {
            assert!(t.has_pk(k as u128), "key {k} must survive fold + bloom rebuild");
        }
        let (w, found) = t.retract_pk(0);
        assert_eq!(w, 1);
        assert!(found.is_some());
        let fr = found.expect("folded key is the found row");
        let val = row_val(&fr);
        assert_eq!(val, 100, "found row payload survives the fold");

        // A PK absent from every run: bloom-miss path must equal the linear miss.
        assert!(!t.has_pk(9999), "absent PK reports absent (bloom miss)");
        let (w_absent, found_absent) = t.retract_pk(9999);
        assert_eq!(w_absent, 0, "absent PK retract nets zero");
        assert!(found_absent.is_none(), "absent PK retract finds no row");
    }

    // ── Unified checkpointing: RAM-tier lifecycle for SalReplay tables ────

    /// The fold-first data-loss guard: a `SalReplay` table whose ingest overflow
    /// left the memtable empty and the RAM tier populated must barrier-flush to
    /// `Pending` (NOT `Empty`) — else the RAM-tier data is silently dropped.
    /// After commit + reopen the full contents survive.
    #[test]
    fn barrier_flush_folds_populated_l0_not_empty() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("barrier_fold_l0");
        let schema = make_schema_u64_i64();
        // Small arena (128 B, threshold 96): an 8-row batch (256 B) overflows the
        // memtable into the RAM tier, leaving the memtable empty.
        let mut t = new_table(&tdir, schema, 7100, 128, RecoverySource::SalReplay);

        let rows: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert!(t.memtable_is_empty(), "ingest must overflow the memtable into L0");
        assert!(t.ram_run_count() > 0, "the RAM tier must be populated");
        assert!(
            t.all_shard_arcs().is_empty(),
            "sub-ceiling overflow writes no disk shard"
        );

        // `flush()` returns false iff `flush_prepare` said `Empty` — which here
        // would mean the fold-first gate dropped a populated the RAM tier.
        t.flush().unwrap();

        assert!(t.ram_run_count() == 0, "flush_commit clears the RAM tier");
        assert!(!t.all_shard_arcs().is_empty(), "barrier wrote a durable shard");

        // Reopen (SalReplay loads the manifest) → every row survives.
        let mut t2 = new_table(&tdir, schema, 7100, 128, RecoverySource::SalReplay);
        for k in 0..8u128 {
            assert!(t2.has_pk(k), "row {k} must survive barrier flush + reopen");
        }
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
        let schema = make_schema_u64_i64();
        // Small arena forces memtable overflow → flush_to_ram; tiny ceiling forces
        // the folded L0 to spill.
        let mut t = new_table(&tdir, schema, 7200, 128, RecoverySource::SalReplay);
        t.set_inmem_ceiling_for_test(100);

        // First spill: 10 rows (320 B) overflow, fold to L0 (320 B > 100) → spill.
        let b1: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&b1)).unwrap();
        assert_eq!(t.ram_bytes(), 0, "spill drains the RAM tier");
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
        assert_eq!(t.ram_bytes(), 0, "second spill drains the RAM tier");
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
        t.flush().unwrap();

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
    }

    /// A barrier folds the live memtable and L0 into one shard, and a ceiling
    /// breach spills to one shard. Both outputs go through the filter-building
    /// writer, so each carries an XOR8 — and the barrier's covers a PK that was
    /// only ever live in the memtable, which is what pins that the fold happened
    /// before the write.
    #[test]
    fn salreplay_barrier_folds_memtable_and_l0_then_spill_writes_one_shard() {
        let schema = make_schema_u64_i64();

        // Barrier shard: live memtable + populated L0.
        {
            let dir = tempfile::tempdir().unwrap();
            let tdir = dir.path().join("barrier_xor8");
            let mut t = new_table(&tdir, schema, 7300, 128, RecoverySource::SalReplay);

            // Overflow 8 rows into L0, then leave 2 rows live in the memtable.
            let over: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
            t.ingest_owned_batch(make_batch(&over)).unwrap();
            assert!(t.ram_run_count() > 0);
            t.ingest_owned_batch(make_batch(&[(100, 1, 1000), (101, 1, 1010)]))
                .unwrap();
            assert!(!t.memtable_is_empty(), "small second batch stays live in the memtable");

            t.flush().unwrap();
            let shards = t.all_shard_arcs();
            assert_eq!(shards.len(), 1, "barrier folds memtable + L0 into one shard");
            assert!(shards[0].has_xor8(), "barrier shard must carry the XOR8 filter");
            assert!(
                shards[0].xor8_may_contain(super::super::xor8::probe_key(&100u64.to_be_bytes())),
                "XOR8 must contain a folded memtable PK"
            );
        }

        // Ceiling-breach spill.
        {
            let dir = tempfile::tempdir().unwrap();
            let tdir = dir.path().join("spill_xor8");
            let mut t = new_table(&tdir, schema, 7301, 128, RecoverySource::SalReplay);
            t.set_inmem_ceiling_for_test(100);

            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            assert_eq!(t.ram_bytes(), 0, "ceiling breach spilled to disk");
            let shards = t.all_shard_arcs();
            assert_eq!(shards.len(), 1, "one spilled shard");
            assert!(shards[0].has_xor8(), "SalReplay spill must carry the XOR8 filter");
        }
    }

    /// `current_lsn` bumps on every ingest, including a `Rederive` table (which
    /// previously pinned it at 1 because ephemeral flushes never advanced it).
    #[test]
    fn current_lsn_bumps_on_every_ingest_including_rederive() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("lsn_bump_rederive");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 7400, 1 << 20, RecoverySource::Rederive);

        assert_eq!(t.current_lsn, 1, "fresh Rederive table starts at LSN 1");
        t.ingest_owned_batch(make_batch(&[(1, 1, 10)])).unwrap();
        assert_eq!(t.current_lsn, 2, "first ingest bumps current_lsn");
        t.ingest_owned_batch(make_batch(&[(2, 1, 20)])).unwrap();
        assert_eq!(t.current_lsn, 3, "second ingest bumps current_lsn again");
    }

    /// Twin of the RAM-tier retract tests on a real `SalReplay` table: ingest
    /// overflow lands in the RAM tier naturally (no artificial Rederive), and
    /// `has_pk`/`retract_pk` resolve the live row across the RAM tier.
    #[test]
    fn salreplay_overflow_into_l0_retract() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("salreplay_l0_retract");
        let schema = make_schema_u64_i64();
        // Small arena so an 8-row ingest overflows the memtable into the RAM tier.
        let mut t = new_table(&tdir, schema, 7500, 128, RecoverySource::SalReplay);

        let rows: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert!(t.memtable_is_empty(), "overflow emptied the memtable");
        assert!(t.ram_run_count() > 0, "SalReplay overflow lands in the RAM tier");
        assert!(
            t.all_shard_arcs().is_empty(),
            "sub-ceiling overflow writes no disk shard"
        );

        for k in 0..8u128 {
            assert!(t.has_pk(k), "row {k} readable from the RAM tier");
        }
        let (w, found) = t.retract_pk(3);
        assert_eq!(w, 1, "retract resolves the live row in the RAM tier");
        assert!(found.is_some());
        let fr = found.expect("found row from RAM tier");
        let val = row_val(&fr);
        assert_eq!(val, 30, "found-row payload matches the RAM-tier row");
    }

    // ── Barrier-only durability: unsynced spills, deferred cleanup ────────

    /// F1 regression. A `SalReplay` table whose only post-checkpoint write
    /// spilled — clearing the RAM tier, one lone L0 shard, no compaction, so no
    /// manifest was published — must still barrier-flush to `Pending` (via the
    /// every barrier publishes) and durably capture the spill. Without the
    /// disjunct the barrier returns `Empty`, the spill is never manifested, and a
    /// reopen's `gc_orphans` deletes it — acknowledged rows lost.
    #[test]
    fn lone_spill_survives_checkpoint_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("lone_spill_ckpt");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 7600, 128, RecoverySource::SalReplay);
        t.set_inmem_ceiling_for_test(100);

        // One over-ceiling ingest → one spill shard (1 < L0 compaction threshold,
        // so no compaction, no publish).
        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert_eq!(t.ram_bytes(), 0, "ceiling breach spilled to disk");
        assert_eq!(t.all_shard_arcs().len(), 1, "exactly one lone spill shard");

        // F1 regression: the barrier must publish the lone unpublished spill
        // (verified below via the manifest/shard the reopen sees).
        t.flush().unwrap();

        let mut t2 = new_table(&tdir, schema, 7600, 128, RecoverySource::SalReplay);
        assert!(
            !t2.all_shard_arcs().is_empty(),
            "manifest must reference the spill shard"
        );
        for k in 0..10u128 {
            assert!(t2.has_pk(k), "spilled row {k} must survive checkpoint + reopen");
        }
    }

    /// Deferred-cleanup crash simulation. After a mid-epoch compaction that did
    /// not republish (SalReplay defers), a crash (drop without a barrier) reopens
    /// from the *old* manifest: the cut state is intact, and the unreferenced
    /// compaction outputs + post-cut spills are orphans reclaimed by `gc_orphans`.
    #[test]
    fn deferred_cleanup_crash_sim_reopens_from_old_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("deferred_cleanup_crash");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 7700, 128, RecoverySource::SalReplay);
        t.set_inmem_ceiling_for_test(100);

        // Cut A: one row, published by a barrier (manifest M0 references its shard).
        t.ingest_owned_batch(make_batch(&[(1000, 1, 1)])).unwrap();
        t.flush().unwrap();

        // Post-cut churn: spill > L0_COMPACT_THRESHOLD shards so run_compact fires,
        // swapping the index and deferring cleanup (no mid-epoch publish).
        for r in 0..6u64 {
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
        }
        assert!(
            compaction_output_count(&tdir, 7700) > 0,
            "post-cut compaction must have produced deferred outputs"
        );
        // Crash: drop without a barrier flush → reopen from the old manifest M0.
        drop(t);

        let mut t2 = new_table(&tdir, schema, 7700, 128, RecoverySource::SalReplay);
        assert!(t2.has_pk(1000), "cut A row survives the crash (loaded from M0)");
        assert!(!t2.has_pk(0), "post-cut orphaned data must not resurrect");
        assert_eq!(
            compaction_output_count(&tdir, 7700),
            0,
            "deferred compaction outputs (never in M0) must be gc'd at open"
        );
    }

    /// Compact-then-quiet. A table that compacts mid-epoch and then goes quiet
    /// (empty memtable and RAM tier) must still publish at the barrier so the
    /// manifest reflects the compacted index; a reopen finds every referenced
    /// shard and every row.
    #[test]
    fn compact_then_quiet_barrier_publishes_compacted_index() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("compact_then_quiet");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 7800, 128, RecoverySource::SalReplay);
        t.set_inmem_ceiling_for_test(100);

        let mut all_keys = Vec::new();
        for r in 0..6u64 {
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
            for &(pk, _, _) in &rows {
                all_keys.push(pk);
            }
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
        }
        assert_eq!(t.ram_bytes(), 0, "spilled: RAM tier empty");
        assert!(t.memtable_is_empty(), "no live memtable rows");
        assert!(compaction_output_count(&tdir, 7800) > 0, "compaction ran");

        t.flush().unwrap();

        let mut t2 = new_table(&tdir, schema, 7800, 128, RecoverySource::SalReplay);
        for pk in &all_keys {
            assert!(
                t2.has_pk(*pk as u128),
                "row {pk} present after compact + barrier + reopen"
            );
        }
    }

    /// The manifest generation is stamped from the value the publish path
    /// passes explicitly (`flush_prepare_ephemeral(generation)`), so a
    /// compaction republish carries the caller's generation, and a later
    /// republish re-stamps a newer one.
    #[test]
    fn generation_preserved_by_compaction_republish() {
        use super::super::manifest::read_file;

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("gen_republish");
        let schema = make_schema_u64_i64();
        let manifest_path = std::ffi::CString::new(tdir.join("manifest.bin").to_str().unwrap()).unwrap();
        let read_generation = |path: &std::ffi::CStr| -> u64 { read_file(path).unwrap().unwrap().1.checkpoint_gen };

        // Publish at generation G1 with a compaction pending.
        let mut t = new_table(&tdir, schema, 7900, 128, RecoverySource::SalReplay);
        t.set_inmem_ceiling_for_test(100);
        for r in 0..6u64 {
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            flush_ephemeral_at(&mut t, 0x1111);
        }
        assert!(compaction_output_count(&tdir, 7900) > 0, "compaction ran");
        flush_ephemeral_at(&mut t, 0x1111);
        assert_eq!(
            read_generation(&manifest_path),
            0x1111,
            "republished manifest carries the passed generation",
        );

        // A later publish at a higher generation re-stamps the manifest.
        t.ingest_owned_batch(make_batch(&[(9999, 1, 1)])).unwrap();
        flush_ephemeral_at(&mut t, 0x2222);
        assert_eq!(
            read_generation(&manifest_path),
            0x2222,
            "later republish re-stamps the newer generation",
        );
    }

    /// `Table::new`'s `RederiveCheckpointed` open decision: a manifest whose
    /// generation matches the caller's `committed` loads its shards; a mismatch
    /// (or absence) erases the shards and unlinks the manifest.
    #[test]
    fn rederive_checkpointed_conditional_load() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("cond_load");
        let schema = make_schema_u64_i64();
        let manifest_path = tdir.join("manifest.bin");

        // Publish a durable shard + manifest stamped at generation 7.
        {
            let mut t = new_table(&tdir, schema, 7910, 128, RecoverySource::SalReplay);
            t.ingest_owned_batch(make_batch(&[(1, 1, 100), (2, 1, 200)])).unwrap();
            flush_ephemeral_at(&mut t, 7);
        }
        assert!(manifest_path.exists(), "manifest published at generation 7");

        // Matching generation ⇒ shards load, rows present.
        {
            let t = new_table(
                &tdir,
                schema,
                7910,
                128,
                RecoverySource::RederiveCheckpointed { committed: 7 },
            );
            assert!(
                t.has_pk_bytes(&1u64.to_be_bytes()) && t.has_pk_bytes(&2u64.to_be_bytes()),
                "matching-generation reopen must load the checkpointed shards"
            );
        }
        assert!(manifest_path.exists(), "matching reopen leaves the manifest in place");

        // Mismatched generation ⇒ shards erased, manifest unlinked.
        {
            let t = new_table(
                &tdir,
                schema,
                7910,
                128,
                RecoverySource::RederiveCheckpointed { committed: 8 },
            );
            assert!(
                !t.has_pk_bytes(&1u64.to_be_bytes()) && !t.has_pk_bytes(&2u64.to_be_bytes()),
                "mismatched-generation reopen must erase the stale shards"
            );
        }
        assert!(
            !manifest_path.exists(),
            "mismatched-generation reopen must unlink manifest.bin so a re-open cannot re-peek it"
        );
    }

    /// A damaged manifest names no checkpoint generation, so the
    /// `RederiveCheckpointed` arm rebuilds rather than failing the boot. A failed
    /// read is not evidence of staleness: it propagates and erases nothing.
    #[test]
    fn rederive_checkpointed_rebuilds_on_a_damaged_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let table_id = 7911;

        // A checkpointed table with one published shard, and the path to the
        // manifest that names it.
        let checkpointed = |name: &str| -> (std::path::PathBuf, std::path::PathBuf) {
            let tdir = dir.path().join(name);
            let mut t = new_table(&tdir, schema, table_id, 128, RecoverySource::SalReplay);
            t.ingest_owned_batch(make_batch(&[(1, 1, 100)])).unwrap();
            flush_ephemeral_at(&mut t, 7);
            assert_eq!(shard_db_files(&tdir, table_id).len(), 1, "{name}: shard published");
            let manifest = std::path::PathBuf::from(crate::storage::lsm::manifest::path(tdir.to_str().unwrap()));
            (tdir, manifest)
        };
        let reopen = |tdir: &std::path::Path| {
            Table::with_arena(
                tdir.to_str().unwrap(),
                schema,
                table_id,
                128,
                RecoverySource::RederiveCheckpointed { committed: 7 },
            )
        };

        type Damage = fn(&mut Vec<u8>);
        let damages: [(&str, Damage); 3] = [
            ("truncated", |b| b.truncate(20)),
            ("bad_magic", |b| b[0] ^= 0xFF),
            // Inside the first entry's filename, which only the digest checks.
            ("checksum", |b| b[60] ^= 0x01),
        ];
        for (name, damage) in damages {
            let (tdir, manifest) = checkpointed(name);
            let mut buf = std::fs::read(&manifest).unwrap();
            damage(&mut buf);
            std::fs::write(&manifest, &buf).unwrap();

            let t = reopen(&tdir)
                .unwrap_or_else(|e| panic!("{name}: a damaged manifest must rebuild, not fail the boot: {e:?}"));
            assert!(
                !t.has_pk_bytes(&1u64.to_be_bytes()),
                "{name}: the rebuild verdict opens empty"
            );
            assert!(
                shard_db_files(&tdir, table_id).is_empty(),
                "{name}: stale shards erased"
            );
        }

        // Replacing the manifest with a directory makes `std::fs::read` fail
        // with something other than NotFound.
        let (tdir, manifest) = checkpointed("io_err");
        std::fs::remove_file(&manifest).unwrap();
        std::fs::create_dir(&manifest).unwrap();
        assert_eq!(reopen(&tdir).err(), Some(StorageError::Io));
        assert_eq!(
            shard_db_files(&tdir, table_id).len(),
            1,
            "a failed read must not erase the shards"
        );
    }

    /// A `SalReplay` table publishes on every barrier, whatever its tier holds:
    /// an unchanged one re-stamps its manifest (which is what makes "every
    /// `w{k}of{n}` has a manifest" a decidable completeness test for the boot
    /// relayout), a lone unsynced spill lands in the sweep list, and compaction
    /// outputs are swept while the inputs they superseded are not.
    #[test]
    fn barrier_gate_matrix() {
        let schema = make_schema_u64_i64();

        // Arm 1 — an unchanged tier still publishes, with nothing to sweep and no
        // new shard.
        {
            let dir = tempfile::tempdir().unwrap();
            let tdir = dir.path().join("gate_empty");
            let mut t = new_table(&tdir, schema, 7900, 1 << 20, RecoverySource::SalReplay);
            t.ingest_owned_batch(make_batch(&[(1, 1, 1)])).unwrap();
            t.flush().unwrap();
            let shards_before = shard_db_files(&tdir, 7900).len();
            match t.flush_prepare(FlushRound::Base).unwrap() {
                FlushOutcome::Pending(w) => assert!(
                    w.sync_paths().is_empty(),
                    "an unchanged tier has nothing left to fdatasync"
                ),
                FlushOutcome::Done => panic!("a SalReplay table must publish even when unchanged"),
            }
            assert_eq!(
                shard_db_files(&tdir, 7900).len(),
                shards_before,
                "an empty RAM tier publishes no new shard"
            );
        }

        // Arm 2 — a lone unsynced spill gates to Pending with the spill swept.
        {
            let dir = tempfile::tempdir().unwrap();
            let tdir = dir.path().join("gate_unsynced");
            let mut t = new_table(&tdir, schema, 7901, 128, RecoverySource::SalReplay);
            t.set_inmem_ceiling_for_test(100);
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, 1)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            assert_eq!(t.ram_bytes(), 0, "spilled");
            match t.flush_prepare(FlushRound::Base).unwrap() {
                FlushOutcome::Pending(w) => {
                    assert!(
                        !w.sync_paths().is_empty(),
                        "the unsynced spill must be in the sweep list"
                    )
                }
                _ => panic!("an unsynced spill must gate to Pending"),
            }
        }

        // Arm 3 — a spill-driven compaction gates to Pending and sweeps its own
        // outputs: compaction writes them unsynced like any other shard, so the
        // publish that makes them reachable is what makes them durable.
        {
            let dir = tempfile::tempdir().unwrap();
            let tdir = dir.path().join("gate_pending");
            let mut t = new_table(&tdir, schema, 7902, 128, RecoverySource::SalReplay);
            t.set_inmem_ceiling_for_test(100);
            // Five spills put L0 over the threshold, and the fifth registration
            // compacts — so the last thing to touch `unsynced` is that compaction.
            for r in 0..5u64 {
                let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
                t.ingest_owned_batch(make_batch(&rows)).unwrap();
                assert_eq!(t.ram_bytes(), 0, "round {r} spilled");
            }
            assert!(
                compaction_output_count(&tdir, 7902) > 0,
                "the spills drove a compaction"
            );

            let shards_before = shard_db_files(&tdir, 7902).len();
            match t.flush_prepare(FlushRound::Base).unwrap() {
                FlushOutcome::Pending(w) => {
                    let swept: Vec<String> = w
                        .sync_paths()
                        .iter()
                        .map(|c| c.to_string_lossy().into_owned())
                        .collect();
                    assert!(!swept.is_empty(), "the compaction outputs must be swept");
                    assert!(
                        swept.iter().all(|p| p.contains("_L")),
                        "the superseded inputs must have left the sweep list, got {swept:?}",
                    );
                }
                _ => panic!("unsynced compaction outputs must gate to Pending"),
            }
            assert_eq!(
                shard_db_files(&tdir, 7902).len(),
                shards_before,
                "an empty RAM tier publishes no new shard"
            );
        }
    }

    /// F5. A `Rederive` table that spills and compacts drains the superseded
    /// inputs inline in `compact_if_needed` (it publishes no manifest that could
    /// strand them), so raw spill shards stay bounded and nothing leaks.
    #[test]
    fn rederive_compaction_drains_deletions_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("rederive_immediate_drain");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 8000, 128, RecoverySource::Rederive);
        t.set_inmem_ceiling_for_test(100);

        for r in 0..8u64 {
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            assert_eq!(t.ram_bytes(), 0, "round {r} spilled");
        }
        assert!(compaction_output_count(&tdir, 8000) > 0, "compaction must have run");
        assert!(
            shard_db_files(&tdir, 8000).len() <= 5,
            "compacted inputs drained inline: {} raw spills remain",
            shard_db_files(&tdir, 8000).len(),
        );
        for r in 0..8u64 {
            for k in 0..10u64 {
                assert!(t.has_pk((r * 100 + k) as u128), "row survives compaction");
            }
        }
    }

    /// F-sys. A `SalReplay` table stands in for a master `_sys` table:
    /// `compact_if_needed` defers cleanup (a manifest could strand the inputs),
    /// and the synchronous `flush()` republishes over the compacted index and
    /// drains the deferred inputs — no intra-session leak.
    #[test]
    fn salreplay_flush_drains_deferred_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("salreplay_flush_drain");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, 8100, 128, RecoverySource::SalReplay);
        t.set_inmem_ceiling_for_test(100);

        for r in 0..6u64 {
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
        }
        assert!(compaction_output_count(&tdir, 8100) > 0, "compaction ran");
        let files_before = all_shard_file_count(&tdir, 8100);

        t.flush().unwrap();
        assert!(
            all_shard_file_count(&tdir, 8100) < files_before,
            "flush must drain the deferred compaction inputs (no intra-session leak)"
        );

        for r in 0..6u64 {
            for k in 0..10u64 {
                assert!(t.has_pk((r * 100 + k) as u128), "row present after flush-drain");
            }
        }

        let mut t2 = new_table(&tdir, schema, 8100, 128, RecoverySource::SalReplay);
        for r in 0..6u64 {
            for k in 0..10u64 {
                assert!(t2.has_pk((r * 100 + k) as u128), "row survives flush-drain + reopen");
            }
        }
    }
}
