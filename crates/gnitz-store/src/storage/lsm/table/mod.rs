//! Unified Table: two RAM-tier [`RunSet`]s over a `ShardIndex`.
//!
//! Ingest lands in the `memtable` run set and folds into the `ram_tier` once it
//! passes its byte budget; the RAM tier spills to a shard past its own
//! ([`StoreBudgets`]), and the checkpoint barrier folds it into one durable shard
//! — on the base round for `SalReplay` tables, on the ephemeral round for
//! `Rederive` ones.

use std::cell::Cell;
use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::os::fd::OwnedFd;
use std::rc::Rc;

use super::batch::Batch;
use super::columnar::{pk_group_end, with_payload_cmp, ColumnarSource};
use super::error::StorageError;
use super::merge::RowComparator;
use super::read_cursor::{self, ReadCursor};
use super::run::{Run, StoredRow};
use super::run_set::RunSet;
use super::shard_index::{ShardBudget, ShardIndex};
#[cfg(test)]
use super::shard_reader::MappedShard;
use super::StagedFile;
use crate::schema::key::{pk_bytes_eq, pk_in_range, pk_ranges_overlap, probe_key, PkBuf};
use crate::schema::SchemaDescriptor;

/// Ingest runs fold into the RAM tier once they pass this. 192 KiB and 768 KiB
/// are indistinguishable in total stall (btrfs, W=4, 4 views, 200k rows, ×3:
/// 545/543/540 ms against 544/563/550 ms).
const MEMTABLE_BYTES: usize = 192 << 10;

/// The RAM-tier ceiling every store opens with: it bounds that one tier, not the
/// table's heap. At the production checkpoint cadence (4M rows, W=4, btrfs) a
/// 4 MiB ceiling wrote 98.7 MB of spill per 4M rows and this 32 MiB none, for
/// +45 MB of cluster RSS; shrinking it is how a test reaches the disk regime on
/// small data.
pub(crate) const DEFAULT_RAM_TIER_BYTES: usize = 32 << 20;

/// What one `Table` opens with: its RAM-tier ceiling, and what bounds its
/// registered on-disk shard bytes. Outside this crate only [`Self::new`] is
/// reachable, so no external caller can mint a store that evicts.
#[derive(Clone, Copy)]
pub(crate) struct StoreBudgets {
    ram_tier_bytes: usize,
    shard: ShardBudget,
}

impl Default for StoreBudgets {
    fn default() -> Self {
        StoreBudgets::new(DEFAULT_RAM_TIER_BYTES)
    }
}

impl StoreBudgets {
    /// An unbounded store at `ram_tier_bytes`.
    pub(crate) fn new(ram_tier_bytes: usize) -> Self {
        StoreBudgets {
            ram_tier_bytes,
            shard: ShardBudget::Unbounded,
        }
    }

    /// `CREATE VIEW … WITH (capacity = …)` — see [`ShardBudget::Dehydrate`].
    /// `None` is an unbounded view.
    pub(crate) fn bounded(self, capacity_bytes: Option<u64>) -> Self {
        StoreBudgets {
            shard: capacity_bytes.map_or(ShardBudget::Unbounded, ShardBudget::Dehydrate),
            ..self
        }
    }

    /// A fed view's **delta store** — see [`ShardBudget::Drop`].
    pub(crate) fn delta(self, budget: u64) -> Self {
        StoreBudgets { shard: ShardBudget::Drop(budget), ..self }
    }
}

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
pub(crate) enum RecoverySource {
    /// The tail is recovered by replaying the fsynced SAL over the shards loaded
    /// from the manifest at open. Base tables and master system tables.
    SalReplay,
    /// Derived state, force-persisted by the ephemeral checkpoint round with a
    /// generation-stamped manifest: view operator-trace tables, view output
    /// stores, secondary indexes.
    Rederive {
        /// The generation a manifest must carry for the open to resume from it
        /// instead of erasing it. `None` says "never resume", for a caller whose
        /// verdict turns on more than the generation.
        resume_at: Option<u64>,
    },
}

// ---------------------------------------------------------------------------
// Two-phase flush API
// ---------------------------------------------------------------------------

/// The deferred half of one barrier flush, owned by the worker between
/// `flush_prepare` and `flush_commit`.
pub(in crate::storage) struct FlushWork {
    /// Full paths of every file written unsynced since the last publish — prior
    /// spills plus this barrier's own folded shard.
    pub(in crate::storage) sync_paths: Vec<CString>,
    /// The staged manifest `.tmp`, its fd open from `prepare_file` until
    /// `flush_commit` consumes the work.
    pub(in crate::storage) manifest: StagedFile,
}

/// Index of the first candidate in `pool` (pool order) whose payload group nets
/// strictly positive — the live row for the PK the pool was gathered for. `cmp`
/// is the payload comparator the merge seats pick between, so this groups the way
/// every other (PK, payload) grouping in storage does.
///
/// "First" is a performance choice: every production caller holds at most one
/// positive group at a PK — a base table by `enforce_unique_pk`, a system table
/// by the catalog precheck's per-PK CAS and `0..=1` net bound. *Which member* of
/// the group comes back is unspecified.
fn first_live_payload_group(
    schema: &SchemaDescriptor,
    pool: &[StoredRow],
    cmp: impl RowComparator<Run>,
) -> Option<usize> {
    (0..pool.len()).find(|&i| {
        let net: i64 = pool
            .iter()
            .filter(|c| cmp(schema, &pool[i].run, pool[i].row, &c.run, c.row) == Ordering::Equal)
            .map(StoredRow::weight)
            .sum();
        net > 0
    })
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

pub(crate) struct Table {
    /// Ingest runs, folded into `ram_tier` once they pass its byte budget.
    memtable: RunSet,
    /// Flushed runs held in heap instead of on disk. Populated for **every**
    /// table on ingest overflow (`flush_to_ram`), bounded by
    /// [`StoreBudgets`]'s RAM-tier ceiling (spill). The checkpoint barrier folds
    /// this tier into one durable shard for `SalReplay` tables.
    ram_tier: RunSet,
    /// The disk tier, and the one owner of this store's schema, directory and
    /// table id.
    shard_index: ShardIndex,

    recovery_source: RecoverySource,

    current_lsn: u64,

    /// True when this open reloaded a generation-matching checkpointed manifest
    /// instead of starting empty. The boot index rebuild skips a table that
    /// reports true. Emptiness is not a substitute: a row with NULL in an
    /// indexed column is not indexed at all, so an all-NULL slice yields a
    /// legitimately empty index over a large owner.
    resumed_from_checkpoint: bool,

    /// Reused candidate pool for `live_row_at`'s grouping pass; taken out and
    /// handed back per call (dropping its `Rc`s) with capacity retained, so the
    /// path stops allocating once warmed up. In a `Cell` so the probe that fills
    /// it is a read.
    retract_scratch: Cell<Vec<StoredRow>>,

    /// Last `full_scan` result, held until the row set moves. In a `Cell` so
    /// `full_scan` stays `&self` — a read reborrowed as `&mut` would widen its
    /// callers' aliasing obligation to "no reference at all live".
    cached_full_scan: Cell<Option<Rc<Batch>>>,
}

mod flush;

#[cfg(test)]
mod bench_flush;

#[cfg(test)]
mod bench_ingest;

impl Table {
    /// Open a table at `dir`. `recovery_source` decides what this does with
    /// whatever is already on disk; `budgets` binds for the store's whole life.
    pub(crate) fn new(
        dir: &str,
        schema: SchemaDescriptor,
        table_id: u32,
        recovery_source: RecoverySource,
        budgets: StoreBudgets,
    ) -> Result<Self, StorageError> {
        // First, so an unusable directory fails the open rather than the first
        // flush: this is the master's CREATE VIEW pre-flight, where a client is
        // still waiting. The fd is dropped — a relation pins none at rest.
        open_table_dirfd(dir)?;

        // `skip_pk_filter` is exactly "is rederived": only a `SalReplay` store is
        // point-probed by PK, so only it needs the filters its shards would
        // otherwise all carry.
        let rederived = matches!(recovery_source, RecoverySource::Rederive { .. });
        let mut table = Table {
            memtable: RunSet::new(MEMTABLE_BYTES),
            ram_tier: RunSet::new(budgets.ram_tier_bytes),
            shard_index: ShardIndex::new(table_id, dir, schema, budgets.shard, rederived),
            recovery_source,
            current_lsn: 1,
            resumed_from_checkpoint: false,
            retract_scratch: Cell::new(Vec::new()),
            cached_full_scan: Cell::new(None),
        };

        let path = table.manifest_full_path();
        let loaded = match recovery_source {
            // Erased at open, so it reads nothing: an I/O error on the file it is
            // about to unlink must not abort it.
            RecoverySource::Rederive { resume_at: None } => None,
            // Rebuilt from its sources, so damage is erased rather than fatal.
            RecoverySource::Rederive { resume_at: Some(want) } => match super::manifest::read_file(&path) {
                Ok(v) => v.filter(|(_, h)| h.checkpoint_gen == want),
                Err(e @ StorageError::Io(_)) => return Err(e),
                Err(_) => None,
            },
            // Its shards are its only copy, so damage stops the open.
            RecoverySource::SalReplay => super::manifest::read_file(&path)?,
        };

        if let Some((entries, header)) = &loaded {
            table.shard_index.install_manifest(entries, header)?;
            table.current_lsn = table.shard_index.max_lsn() + 1;
            table.resumed_from_checkpoint = rederived;
        } else if rederived {
            // So a later re-open cannot re-peek what this one rejected.
            table.unlink_manifest()?;
        }
        table.shard_index.gc_orphans();

        Ok(table)
    }

    /// Shrink the memtable budget so a test outside `lsm` can drive the drain
    /// path without ingesting megabytes.
    #[cfg(test)]
    pub(crate) fn set_memtable_budget(&mut self, budget: usize) {
        self.memtable.set_budget(budget);
    }

    /// The highest key this store's capacity sweep has dropped.
    /// See [`ShardIndex::dropped_max`].
    pub(crate) fn dropped_max(&self) -> PkBuf {
        self.shard_index.dropped_max()
    }

    /// Whether a read of this store can meet a skeleton row it has to hydrate.
    /// See [`ShardIndex::has_skeleton_shard`].
    pub(crate) fn has_skeleton_rows(&self) -> bool {
        self.shard_index.has_skeleton_shard()
    }

    /// Full path of this table's manifest — a pure function of the directory,
    /// carrying no state worth caching.
    fn manifest_full_path(&self) -> String {
        super::manifest::path(&self.shard_index.output_dir)
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

    /// Durably unlink this store's manifest, so the next `Rederive` open erases
    /// its shards instead of reloading them.
    pub(crate) fn unlink_manifest(&self) -> Result<(), StorageError> {
        super::manifest::unlink(&self.shard_index.output_dir)
    }

    /// See [`ShardIndex::append_terminal_run`].
    pub(crate) fn append_terminal_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        self.cached_full_scan.set(None);
        self.shard_index.append_terminal_run(run)
    }

    /// Publish `schema` across this store (any column ALTER). All-or-nothing:
    /// only the shard index's swap can fail, and it runs first.
    pub(crate) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        self.shard_index.swap_schema(schema)?;
        self.memtable.widen_runs(&schema);
        self.ram_tier.widen_runs(&schema);
        self.cached_full_scan.set(None);
        Ok(())
    }

    /// Never spill again: a process that holds this store as a read replica of
    /// another process's directory must not write into it.
    pub(crate) fn hold_in_ram(&mut self) {
        self.ram_tier.set_budget(usize::MAX);
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch into the memtable.
    /// The relation-store ingest entry point.
    pub(crate) fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        self.cached_full_scan.set(None);

        // Ingest overflow always folds into the RAM tier; durability lives in the
        // fsynced SAL, and the checkpoint barrier writes the durable shard. Bump
        // `current_lsn` unconditionally so spill/barrier shard naming is
        // collision-free.
        self.current_lsn += 1;

        let consolidated = batch.into_consolidated(&self.shard_index.schema);
        self.memtable.push(Rc::new(consolidated), &self.shard_index.schema);
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
    pub(crate) fn ingest_borrowed_batch(&mut self, batch: &Batch) -> Result<(), StorageError> {
        if batch.count == 0 {
            return Ok(());
        }
        let owned =
            Batch::consolidate_if_needed(batch, &self.shard_index.schema).unwrap_or_else(|| batch.clone_batch());
        self.ingest_owned_batch(owned)
    }

    // ------------------------------------------------------------------
    // Flush
    // ------------------------------------------------------------------

    /// The next-shard LSN counter: seeded `max_lsn + 1` at open, bumped on every
    /// ingest, raised by [`Self::pin_lsn`].
    pub(crate) fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Raise the LSN counter to `lsn`. Never lowers it, which would reuse a live
    /// shard name.
    pub(crate) fn pin_lsn(&mut self, lsn: u64) {
        self.current_lsn = self.current_lsn.max(lsn);
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

    /// The heap-resident runs that can hold a key in `bound` — `None` taking
    /// them all, an inclusive `[lo, hi]` pruning by each run's own PK range.
    ///
    /// No guard partitions these runs, so what the prune is worth is the data's
    /// to say: a monotone key (a delta store's `_tick`-led PK, a `SERIAL`) drops
    /// most runs, a hashed one drops none.
    fn mem_runs(&self, bound: Option<(PkBuf, PkBuf)>) -> impl Iterator<Item = Run> + '_ {
        let [memtable, ram_tier] = self.ram_tiers();
        memtable
            .runs()
            .iter()
            .chain(ram_tier.runs().iter())
            // `RunSet` never stores an empty run, so row 0 and `count - 1` are
            // this run's min and max.
            .filter(move |run| match bound {
                None => true,
                Some((lo, hi)) => pk_ranges_overlap(
                    run.get_pk_bytes(0),
                    run.get_pk_bytes(run.count - 1),
                    lo.pk_bytes(),
                    hi.pk_bytes(),
                ),
            })
            .cloned()
            .map(Run::Mem)
    }

    /// How many cursor sources [`Self::mem_runs`] can yield.
    fn mem_run_count(&self) -> usize {
        self.ram_tiers().iter().map(|s| s.len()).sum()
    }

    /// Every run this table reads through, newest tier first.
    pub(crate) fn runs(&self) -> impl Iterator<Item = Run> + '_ {
        self.mem_runs(None)
            .chain(self.shard_index.all_shard_arcs_iter().map(Run::Shard))
    }

    /// Open a read-only cursor over every tier. Does NOT mutate the table —
    /// compaction is a maintenance operation, not part of the read path. Cheap
    /// and infallible. Maintenance paths that want an up-to-date L1 call
    /// `compact_if_needed` first.
    ///
    /// Opening is Θ(sources), so a read that knows its key bound beforehand
    /// should take [`Self::open_cursor_in_range`].
    pub(crate) fn open_cursor(&self) -> ReadCursor {
        let cap = self.mem_run_count() + self.shard_index.shard_count();
        read_cursor::from_runs(self.runs(), self.shard_index.schema, cap)
    }

    /// A cursor that can answer only about keys in `[start, end]`, `end` `None`
    /// meaning the top of the key space, and **unpositioned** — every caller
    /// seeks or probes within the bound it named.
    ///
    /// Both bounds are exactly `pk_stride` OPK bytes. The gather over-approximates
    /// on both tiers, so a half-open `end` is safe to pass.
    pub(crate) fn open_cursor_in_range(&self, start: &[u8], end: Option<&[u8]>) -> ReadCursor {
        let stride = self.shard_index.schema.pk_stride();
        debug_assert_eq!(start.len(), stride, "open_cursor_in_range: start is not pk_stride wide");
        debug_assert!(
            end.is_none_or(|e| e.len() == stride),
            "open_cursor_in_range: end is not pk_stride wide",
        );
        let (lo, hi) = (
            PkBuf::from_bytes(start),
            end.map_or_else(|| PkBuf::max(stride), PkBuf::from_bytes),
        );
        let runs = self
            .mem_runs(Some((lo, hi)))
            .chain(self.shard_index.shard_arcs_in_range(lo, hi).map(Run::Shard));
        read_cursor::from_runs_unpositioned(runs, self.shard_index.schema, self.mem_run_count())
    }

    /// Return the fully consolidated batch of all live rows, caching the result.
    /// The cache is invalidated wherever the row set can move: `ingest_owned_batch`,
    /// `swap_schema`, the RAM-tier fold and the spill commit in `flush.rs`.
    /// Cheap on repeated calls: returns `Rc::clone` of the cached batch.
    /// Infallible: delegates to `open_cursor`.
    pub(crate) fn full_scan(&self) -> Rc<Batch> {
        let rc = self
            .cached_full_scan
            .take()
            .unwrap_or_else(|| self.open_cursor().materialize());
        self.cached_full_scan.set(Some(Rc::clone(&rc)));
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

    /// `(registered shards, how many carry a PK filter)`. What an assertion asks
    /// this store about its shard set, in counts — so neither the shard type nor
    /// the index behind it has to be reachable to ask it.
    #[cfg(test)]
    pub(crate) fn pk_filter_census(&self) -> (usize, usize) {
        self.shard_index.all_shard_arcs_iter().fold((0, 0), |(n, filtered), s| {
            (n + 1, filtered + usize::from(s.has_shard_filter()))
        })
    }

    /// Test helper: shard Rcs (production reads go through `runs`). Reached from
    /// outside `lsm`, where the shard index itself is not visible.
    #[cfg(test)]
    pub(crate) fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.shard_index.all_shard_arcs_iter().collect()
    }

    /// Test helper: `(L0 shard count, per-level guard count)`. Same reach as
    /// [`Self::all_shard_arcs`].
    #[cfg(test)]
    pub(crate) fn level_shape(&self) -> (usize, [usize; super::shard_index::FLSM_LEVELS]) {
        self.shard_index.level_shape()
    }

    // ------------------------------------------------------------------
    // PK lookups
    // ------------------------------------------------------------------

    /// Byte-keyed PK existence check: the net weight across every tier is
    /// positive.
    #[inline]
    pub(crate) fn has_pk_bytes(&self, key: &[u8]) -> bool {
        let mut w: i64 = 0;
        self.for_each_pk_candidate(key, |run, row| w += run.get_weight(row));
        w > 0
    }

    /// Visit every row whose PK equals `key`, newest first: memtable before RAM
    /// tier before shards, and newest run first inside each tier — the order both
    /// callers' grouping passes read it as.
    ///
    /// The one PK walk of the table, so no tier can be visible to one
    /// point-lookup entry point and invisible to the other. The visitor borrows
    /// its run because the bloom gates a whole `RunSet`: a hit walks all ≤16 of
    /// its runs, and only the ones that hold the key clone an `Rc`.
    fn for_each_pk_candidate(&self, key: &[u8], mut f: impl FnMut(&Run, usize)) {
        // One derivation for every filter this walk consults — both RAM-tier
        // blooms and each shard's PK filter.
        let fingerprint = probe_key(key);
        for set in self.ram_tiers() {
            if !set.may_contain(fingerprint) {
                continue;
            }
            // `runs()` is push-ordered (oldest first) and `ram_tiers()` is already
            // newest-tier-first, so reversing here makes the whole walk newest-first.
            for batch in set.runs().iter().rev() {
                let Some(start) = pk_match_start(batch, key) else {
                    continue;
                };
                let run = Run::Mem(Rc::clone(batch));
                for row in start..pk_group_end(&run, start) {
                    f(&run, row);
                }
            }
        }
        // The shard index's gated binary search already lands on the first
        // match, so the scan resumes from there rather than re-searching.
        self.shard_index.find_pk_bytes(key, fingerprint, &mut |shard, start| {
            let run = Run::Shard(shard);
            for row in start..pk_group_end(&run, start) {
                f(&run, row);
            }
        });
    }

    /// The net weight at OPK `key` and, when it is positive, the live
    /// (PK, payload) row. The DML retraction probe and the catalog's CAS read
    /// through the same entry point.
    ///
    /// Winner selection is one grouping pass over the cross-tier candidate pool
    /// in tier order (memtable first — the newest history). Groups form by
    /// payload equality; the first group whose weights net strictly positive
    /// holds the live row. Grouping over the whole pool rather than trusting any
    /// single row's own weight is what makes the answer right: a candidate whose
    /// weight is positive but whose full group nets ≤ 0 has been retracted by a
    /// later tier and must not be returned.
    pub(crate) fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        let mut pool = self.retract_scratch.take();

        let mut total_w: i64 = 0;
        self.for_each_pk_candidate(key, |run, row| {
            total_w += run.get_weight(row);
            pool.push(StoredRow { run: run.clone(), row });
        });

        let mut row = None;
        if total_w > 0 {
            let schema = &self.shard_index.schema;
            let winner = with_payload_cmp!(schema, first_live_payload_group, schema, &pool);
            debug_assert!(
                winner.is_some(),
                "positive net PK weight implies a positive payload group"
            );
            row = winner.map(|i| pool.swap_remove(i));
        }

        pool.clear();
        self.retract_scratch.set(pool);
        (total_w, row)
    }

    // ------------------------------------------------------------------
    // Compaction
    // ------------------------------------------------------------------

    /// Run L0→L1+ compaction if the disk tier crossed its threshold. Publishes no
    /// manifest: the barrier is the sole publish point.
    pub(crate) fn compact_if_needed(&mut self) -> Result<(), StorageError> {
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

/// Where `key`'s matching rows begin in a PK-sorted RAM-tier run, or `None` when
/// it holds none — the run's own PK range rejects most keys before the binary
/// search runs, and the equality test rejects the rest. A `RunSet` never stores
/// an empty run.
fn pk_match_start(run: &Batch, key: &[u8]) -> Option<usize> {
    let count = run.count;
    if !pk_in_range(run.get_pk_bytes(0), run.get_pk_bytes(count - 1), key) {
        return None;
    }
    let start = run.find_lower_bound_bytes(key);
    (start < count && pk_bytes_eq(run.get_pk_bytes(start), key)).then_some(start)
}

// ---------------------------------------------------------------------------
// OS helpers
// ---------------------------------------------------------------------------

/// Open a table's directory, creating it (and any missing parent) if absent —
/// the one way in, and the only thing that re-creates an absent one: the shard
/// write goes by path and `ENOENT`s instead.
///
/// Every opened fd gets the NOCOW hint (btrfs; ignored elsewhere), so it also
/// covers directories the catalog's layout staging pre-created.
pub(super) fn open_table_dirfd(dir: &str) -> Result<OwnedFd, StorageError> {
    use std::os::fd::AsRawFd;
    let open = |c: &CStr| gnitz_foundation::posix_io::open_owned(c, libc::O_RDONLY | libc::O_DIRECTORY);
    let dir_c = super::super::cstr(dir)?;
    let fd = match open(&dir_c) {
        Ok(fd) => fd,
        Err(_) => {
            std::fs::create_dir_all(dir)?;
            open(&dir_c)?
        }
    };
    gnitz_foundation::posix_io::try_set_nocow(fd.as_raw_fd());
    Ok(fd)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/table.rs"]
mod tests;
