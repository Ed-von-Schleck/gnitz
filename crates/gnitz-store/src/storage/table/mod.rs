//! Unified Table: three [`RunSet`]s in RAM over a `ShardIndex`.
//!
//! Ingest lands in the `memtable` run set and folds into the `ram_tier` once it
//! passes its byte budget; the RAM tier spills to a shard past its own ceiling,
//! and a barrier folds it into one durable shard.
//!
//! A store's runs are its history in order, so a read may stop short of the
//! newest: a pending ingest lands in the `pending` run set, above the store's
//! [`Cut`], and [`Table::seal`] moves the cut past it. A barrier that comes
//! first writes the pending rows to shards the disk tier holds apart, which a
//! reader at the cut leaves out.

use std::cell::Cell;
use std::fs;
use std::io;
use std::rc::Rc;

use gnitz_wire::PkKeys;

use super::manifest::Manifest;
use super::run_set::RunSet;
use super::shard_index::{ShardBudget, ShardIndex};
use gnitz_wire::PkBuf;
use gnitz_zset::repr::pk_group_end;
use gnitz_zset::repr::Batch;
#[cfg(test)]
use gnitz_zset::repr::MappedShard;
use gnitz_zset::repr::StorageError;
use gnitz_zset::repr::{empty_cursor, from_runs, from_runs_in_band, PkSetGather, ReadCursor};
use gnitz_zset::repr::{first_live_payload_group, Run, StoredRow};
use gnitz_zset::schema::key::{key_range_between_cuts, probe_key, KeyCut};
use gnitz_zset::schema::SchemaDescriptor;

/// Ingest runs fold into the RAM tier once they pass this.
pub(super) const MEMTABLE_BYTES: usize = 192 << 10;

/// Memtable rows per row of a seal's delta past which the seal leaves the
/// memtable unfolded: beyond it a fold is mostly the copy of rows the delta
/// does not touch.
const SEAL_FOLD_RATIO: usize = 64;

/// Fold input bytes an ingest pays for per byte it ingests. A chosen factor, not
/// a derived one: it has to exceed what a store's folds read per ingested byte,
/// or what the store owes grows with it, and that ratio is a property of the
/// workload.
const UPKEEP_LEVY: u64 = 64;

/// The RAM-tier ceiling every store opens with: it bounds that one tier, not the
/// table's heap. The ceiling trades spill writes against RSS; shrinking it is how
/// a test reaches the disk regime on small data.
pub(crate) const DEFAULT_RAM_TIER_BYTES: usize = 32 << 20;

// ---------------------------------------------------------------------------
// RecoverySource
// ---------------------------------------------------------------------------

/// How a relation's tail is recovered across a restart.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RecoverySource {
    /// The tail is recovered by replaying the fsynced SAL over the shards loaded
    /// from the manifest at open.
    SalReplay,
    /// Derived state, persisted under a generation-stamped manifest by each
    /// ephemeral round that picks its relation.
    Rederive {
        /// The generation a manifest must carry for the open to resume from it
        /// instead of erasing it; `None` erases it.
        resume_at: Option<u64>,
    },
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

/// Which time-prefix of a store's ingests a read sees. A store's runs are its
/// history in order, so a prefix of them is the Z-set the store held then.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cut {
    /// Every ingest.
    Now,
    /// Every ingest up to the last [`Table::seal`]: the rows ingested as
    /// pending since are left out.
    Sealed,
}

pub(crate) struct Table {
    /// Runs ingested above the cut, folded among themselves only: [`Self::seal`]
    /// moves them into `memtable` as one run.
    pending: RunSet,
    /// Ingest runs, folded into `ram_tier` once they pass its byte budget.
    memtable: RunSet,
    /// Folded memtable runs, spilled to a shard past the RAM-tier ceiling.
    ram_tier: RunSet,
    /// The disk tier, and the one owner of this store's schema and directory.
    shard_index: ShardIndex,

    /// Whether this store is rebuilt from its sources at open.
    rederived: bool,

    /// The checkpoint mark of the manifest this open loaded.
    loaded_mark: Option<u64>,

    /// What the next publish writes as the manifest's caller record.
    caller_record: Vec<u8>,

    /// Set by [`Self::hold_in_ram`]: the RAM tier grows past its budget and never
    /// persists.
    held_in_ram: bool,
    /// Fold input bytes [`Self::upkeep`] merged past what its calls so far paid
    /// for: a fold stops at a destination, not at a byte.
    upkeep_overdraft: u64,

    /// `live_row_at`'s candidate pool, kept between calls for its capacity.
    live_row_scratch: Cell<Vec<StoredRow>>,

    /// Last `full_scan` result, held until the row set moves.
    cached_full_scan: Cell<Option<Rc<Batch>>>,

    /// The bytes of the manifest in place: the one this open read, or the one
    /// this process's last barrier renamed. `None` for a store that has none,
    /// and from the staging of another until its barrier completes.
    manifest_in_place: Option<Vec<u8>>,
    /// Whether a barrier of this process synced the directories behind
    /// `manifest_in_place`. The rename of a manifest read at open may predate a
    /// kill that came before them.
    manifest_synced: bool,
}

mod flush;

pub(crate) use flush::flush_barrier;

impl Table {
    /// Open a table at `dir`. `recovery_source` decides what this does with
    /// whatever is already on disk; the RAM-tier ceiling and `shard`, which
    /// bounds the registered on-disk shard bytes, bind for the store's whole life.
    pub(crate) fn new(
        dir: &str,
        schema: SchemaDescriptor,
        recovery_source: RecoverySource,
        ram_tier_bytes: usize,
        shard: ShardBudget,
    ) -> Result<Self, StorageError> {
        // First, so an unusable directory fails the open rather than the first
        // flush. `created` is `mkdir`'s own verdict, never a `stat`'s: a store
        // read as freshly created skips its manifest, and the shard index then
        // unlinks every shard in the directory.
        let created = match fs::create_dir(dir) {
            Ok(()) => true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(dir)?;
                true
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(e.into()),
        };

        let rederived = matches!(recovery_source, RecoverySource::Rederive { .. });
        let loaded = match recovery_source {
            _ if created => None,
            // Erased at open, so it reads nothing: an I/O error on the file it is
            // about to unlink must not abort it.
            RecoverySource::Rederive { resume_at: None } => None,
            // Rebuilt from its sources, so damage is erased rather than fatal.
            RecoverySource::Rederive { resume_at: Some(want) } => super::manifest::read_at(dir, want)?,
            // Its shards are its only copy, so damage stops the open.
            RecoverySource::SalReplay => super::manifest::read(dir)?,
        };
        if loaded.is_none() && rederived && !created {
            // So a later re-open cannot reload what this one rejected.
            super::manifest::unlink(dir)?;
        }
        let loaded_mark = loaded.as_ref().map(|m| m.checkpoint_mark);
        let manifest_in_place = loaded.as_ref().map(super::manifest::encode);
        let Manifest { caller_record, shards, .. } = loaded.unwrap_or_default();
        // A rederived store's shards carry no PK filter.
        let skip_pk_filter = rederived;
        Ok(Table {
            // Never full: the budget only sizes its PK filter, to a tick's worth of rows.
            pending: RunSet::new(MEMTABLE_BYTES),
            memtable: RunSet::new(MEMTABLE_BYTES),
            ram_tier: RunSet::new(ram_tier_bytes),
            shard_index: ShardIndex::open(dir, schema, shard, skip_pk_filter, &shards)?,
            rederived,
            loaded_mark,
            caller_record,
            held_in_ram: false,
            upkeep_overdraft: 0,
            live_row_scratch: Cell::new(Vec::new()),
            cached_full_scan: Cell::new(None),
            manifest_in_place,
            manifest_synced: false,
        })
    }

    /// The schema this store's rows are read in.
    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        &self.shard_index.schema
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

    /// True when this store is rebuilt from its sources at open.
    pub(crate) fn is_rederived(&self) -> bool {
        self.rederived
    }

    /// The checkpoint mark of the manifest this open loaded; `None` without one.
    pub(crate) fn loaded_mark(&self) -> Option<u64> {
        self.loaded_mark
    }

    /// See [`ShardIndex::append_terminal_run`].
    pub(crate) fn append_terminal_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        self.cached_full_scan.set(None);
        self.shard_index.append_terminal_run(run)
    }

    /// Verify every shard's body.
    pub(crate) fn verify_shards(&self) -> Result<(), StorageError> {
        self.shard_index.verify_shards()
    }

    /// Publish `schema` across this store (any column ALTER). All-or-nothing:
    /// only the shard index's swap can fail, and it runs first.
    pub(crate) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        self.shard_index.swap_schema(schema)?;
        self.pending.widen_runs(&schema);
        self.memtable.widen_runs(&schema);
        self.ram_tier.widen_runs(&schema);
        self.cached_full_scan.set(None);
        Ok(())
    }

    /// Never spill again: a process that holds this store as a read replica of
    /// another process's directory must not write into it.
    pub(crate) fn hold_in_ram(&mut self) {
        self.held_in_ram = true;
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    /// Ingest an already-constructed Batch into the memtable.
    /// The relation-store ingest entry point.
    ///
    /// `#[inline]`: it takes a `Batch` by value, and every stateful operator
    /// calls it once per epoch.
    #[inline]
    pub(crate) fn ingest(&mut self, batch: Batch) -> Result<(), StorageError> {
        let batch = batch.into_consolidated();
        if batch.is_empty() {
            return Ok(());
        }
        self.cached_full_scan.set(None);
        let ingested = batch.total_bytes();
        self.memtable.push(batch, &self.shard_index.schema);
        if self.memtable.is_full() {
            self.fold_to_ram()?;
        }
        self.upkeep(ingested)
    }

    /// Drop every row this store holds, in RAM and on disk. The next barrier
    /// publishes the empty store.
    pub(crate) fn clear(&mut self) {
        self.cached_full_scan.set(None);
        self.pending.clear();
        self.memtable.clear();
        self.ram_tier.clear();
        self.shard_index.clear();
    }

    /// [`Self::ingest`] above the cut: every reader sees the rows
    /// but one at [`Cut::Sealed`], until [`Self::seal`].
    pub(crate) fn ingest_pending(&mut self, batch: Batch) {
        let batch = batch.into_consolidated();
        if batch.is_empty() {
            return;
        }
        self.cached_full_scan.set(None);
        self.pending.push(batch, &self.shard_index.schema);
    }

    /// Whether any row sits above the cut.
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.len() > 0 || self.shard_index.pending_arcs().next().is_some()
    }

    /// Move the cut past every pending row and answer them, consolidated: the
    /// store's delta since the last seal. `None` when there is none. The row set
    /// a [`Cut::Now`] reader sees does not move.
    pub(crate) fn seal(&mut self) -> Result<Option<Batch>, StorageError> {
        let schema = self.shard_index.schema;
        let run = self.pending.drain_into(&mut self.memtable, &schema);
        // Left as a run of its own, a seal's delta lengthens every PK probe and
        // keeps the rows it cancels held until the memtable next folds. A fold
        // rewrites the whole memtable, though, so a delta under a
        // `SEAL_FOLD_RATIO`th of it waits for the memtable's own trigger.
        if run
            .as_ref()
            .is_some_and(|run| run.len() * SEAL_FOLD_RATIO >= self.memtable.row_count())
        {
            self.memtable.fold(&schema);
        }
        let shards: Vec<Run> = self.shard_index.pending_arcs().map(Run::Shard).collect();
        let delta = match shards.is_empty() {
            true => run,
            // A barrier since the last seal left part of the delta in shards.
            false => {
                let cap = shards.len() + 1;
                let mem = run.into_iter().map(Run::Mem);
                Some(from_runs(mem.chain(shards), schema, cap).materialize())
            }
        };
        self.shard_index.seal_pending();
        if self.memtable.is_full() {
            self.fold_to_ram()?;
        }
        self.upkeep(delta.as_ref().map_or(0, |delta| delta.total_bytes()))?;
        // Copied only while the memtable still holds the run.
        Ok(delta.map(Rc::unwrap_or_clone).filter(|delta| !delta.is_empty()))
    }

    /// Pay the disk tier's upkeep for `ingested` bytes: [`UPKEEP_LEVY`] bytes of
    /// fold input for each, less what earlier calls overdrew. A store nothing
    /// is ingested into keeps what it owes.
    fn upkeep(&mut self, ingested: usize) -> Result<(), StorageError> {
        // A read replica of another process's directory must not write into it.
        if self.held_in_ram || !self.shard_index.owed() {
            return Ok(());
        }
        let levy = ingested as u64 * UPKEEP_LEVY;
        let budget = levy.saturating_sub(self.upkeep_overdraft);
        self.upkeep_overdraft = self.upkeep_overdraft.saturating_sub(levy);
        let read = self.shard_index.maintain(budget)?;
        self.upkeep_overdraft += read.saturating_sub(budget);
        Ok(())
    }

    /// Every fold the disk tier owes, in one call: for a caller with no ingest
    /// to pay for them out of.
    pub(crate) fn settle(&mut self) -> Result<(), StorageError> {
        if self.held_in_ram {
            return Ok(());
        }
        self.shard_index.maintain(u64::MAX).map(drop)
    }

    /// The bytes this store's next published manifest carries for its owner.
    pub(crate) fn set_caller_record(&mut self, record: Vec<u8>) {
        self.caller_record = record;
    }

    // ------------------------------------------------------------------
    // Cursor
    // ------------------------------------------------------------------

    /// The heap-resident tiers `cut` reads, newest first.
    fn ram_tiers(&self, cut: Cut) -> impl Iterator<Item = &RunSet> + Clone {
        [&self.pending, &self.memtable, &self.ram_tier]
            .into_iter()
            .skip(usize::from(cut == Cut::Sealed))
    }

    /// The heap-resident runs whose PK extent meets the inclusive `bound`; `None`
    /// takes them all.
    fn mem_runs(&self, bound: Option<(PkBuf, PkBuf)>, cut: Cut) -> impl Iterator<Item = Run> + '_ {
        self.ram_tiers(cut)
            .flat_map(move |set| set.runs_overlapping(bound))
            .cloned()
            .map(Run::Mem)
    }

    /// How many cursor sources [`Self::mem_runs`] can yield.
    fn mem_run_count(&self) -> usize {
        self.ram_tiers(Cut::Now).map(|s| s.len()).sum()
    }

    /// Every run `cut` reads.
    pub(crate) fn runs(&self, cut: Cut) -> impl Iterator<Item = Run> + '_ {
        self.mem_runs(None, cut)
            .chain(self.shard_index.shard_arcs(cut == Cut::Now).map(Run::Shard))
    }

    /// Open a read-only cursor over the rows `cut` reads.
    pub(crate) fn open_cursor(&self, cut: Cut) -> ReadCursor {
        let cap = self.mem_run_count() + self.shard_index.shard_count();
        from_runs(self.runs(cut), self.shard_index.schema, cap)
    }

    /// A cursor for probing at the keys in `[first, last]` — whole PKs, or the
    /// same leading bytes of one — positioned on the band they span, over the
    /// rows `cut` reads.
    pub(crate) fn cursor_between(&self, first: &[u8], last: &[u8], cut: Cut) -> ReadCursor {
        // The zero-width prefix is every key: no run to rule out, no key to seek.
        if first.is_empty() {
            return self.open_cursor(cut);
        }
        let stride = self.shard_index.schema.pk_stride();
        let band = key_range_between_cuts(KeyCut::min_of(first), KeyCut::above(last), stride)
            .expect("`first <= last`, so the band from one's group to the other's holds a key");
        self.range_cursor(Some(band), cut)
    }

    /// A cursor positioned on the OPK key band `[start, end)` of the rows `cut`
    /// reads (`None` for `end`: the top of the key space), over the runs that can
    /// hold a key of it; `None` opens empty.
    pub(crate) fn range_cursor(&self, range: Option<(PkBuf, Option<PkBuf>)>, cut: Cut) -> ReadCursor {
        let schema = self.shard_index.schema;
        let Some((start, end)) = range else {
            return empty_cursor(schema);
        };
        let stride = schema.pk_stride();
        let hi = end.unwrap_or_else(|| PkBuf::max(stride));
        debug_assert!(
            start.pk_bytes().len() == stride && hi.pk_bytes().len() == stride,
            "range_cursor: a bound is not pk_stride wide",
        );
        let runs = self.mem_runs(Some((start, hi)), cut).chain(
            self.shard_index
                .shard_arcs_in_range(start, hi, cut == Cut::Now)
                .map(Run::Shard),
        );
        let cap = self.mem_run_count() + self.shard_index.narrow_range_shards();
        let end = end.as_ref().map(PkBuf::pk_bytes);
        from_runs_in_band(runs, schema, cap, start.pk_bytes(), end)
    }

    /// Every live row of `keys` — whole PKs, or the same leading columns of
    /// one — over the runs `cut` reads that the span they cover can reach.
    pub(crate) fn gather(&self, keys: PkKeys, cut: Cut) -> PkSetGather {
        let cursor = match keys.bounds() {
            Some((first, last)) => self.cursor_between(first, last, cut),
            None => empty_cursor(self.shard_index.schema),
        };
        PkSetGather::over(cursor, keys)
    }

    /// The consolidated batch of all live rows, cached until the row set moves.
    pub(crate) fn full_scan(&self) -> Rc<Batch> {
        let rc = self
            .cached_full_scan
            .take()
            .unwrap_or_else(|| self.open_cursor(Cut::Now).materialize());
        // A sweep that drops rows moves the row set with no ingest to say so.
        if !self.shard_index.drops_rows() {
            self.cached_full_scan.set(Some(Rc::clone(&rc)));
        }
        rc
    }

    /// Raw rows across every tier, cross-run duplicates and ghosts included.
    pub(crate) fn estimated_rows(&self) -> usize {
        self.ram_tiers(Cut::Now).map(|s| s.row_count()).sum::<usize>() + self.shard_index.total_rows()
    }

    /// Every registered shard.
    #[cfg(test)]
    pub(crate) fn all_shard_arcs(&self) -> Vec<Rc<MappedShard>> {
        self.shard_index.shard_arcs(true).collect()
    }

    /// `(L0 shard count, L1 and terminal guard counts)`.
    #[cfg(test)]
    pub(crate) fn level_shape(&self) -> (usize, [usize; 2]) {
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

    /// Visit every row whose PK equals `key`, the heap tiers' newest first: that
    /// puts `live_row_at`'s live row ahead of the retracted history it would
    /// otherwise scan past.
    fn for_each_pk_candidate(&self, key: &[u8], mut f: impl FnMut(&Run, usize)) {
        let mut visit = |run: Run, start: usize| {
            for row in start..pk_group_end(&run, start) {
                f(&run, row);
            }
        };
        let fingerprint = probe_key(key);
        for set in self.ram_tiers(Cut::Now) {
            set.find_pk_bytes(key, fingerprint, |batch, start| {
                visit(Run::Mem(Rc::clone(batch)), start)
            });
        }
        self.shard_index.find_pk_bytes(key, fingerprint, |shard, start| {
            visit(Run::Shard(Rc::clone(shard)), start)
        });
    }

    /// The net weight at OPK `key` and, when it is positive, a live
    /// (PK, payload) row: one whose payload group across every tier nets
    /// positive.
    pub(crate) fn live_row_at(&self, key: &[u8]) -> (i64, Option<StoredRow>) {
        let mut pool = self.live_row_scratch.take();

        let mut total_w: i64 = 0;
        self.for_each_pk_candidate(key, |run, row| {
            total_w += run.get_weight(row);
            pool.push(StoredRow::new(run.clone(), row));
        });

        let mut row = None;
        if total_w > 0 {
            let schema = &self.shard_index.schema;
            let winner = first_live_payload_group(schema, &pool);
            debug_assert!(
                winner.is_some(),
                "positive net PK weight implies a positive payload group"
            );
            row = winner.map(|i| pool.swap_remove(i));
        }

        pool.clear();
        self.live_row_scratch.set(pool);
        (total_w, row)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/table.rs"]
mod bench;
