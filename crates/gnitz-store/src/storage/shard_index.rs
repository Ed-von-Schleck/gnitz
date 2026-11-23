//! FLSM Shard Index: the tree a store's shards form — L0, levels of guards and
//! the shard entries in them, the compaction policy's constants and its learned
//! cancel yield — opened from the shard set of its directory's manifest; the one
//! shard writer and the appends through it, the PK probes and shard listings a
//! cursor opens over, and the upkeep (`maintain`) with the folds it plans and
//! runs — the L0→L1 fold, the byte targets every level's guard partition is held
//! at, and the vertical drain into the terminal level. The files, and the
//! publish that makes a tree durable, are the [`StoreDir`]'s.

use std::ops::Range;
use std::rc::Rc;

use super::manifest::{FlushWork, ManifestEntry, RecoverySource, ShardSet, StoreDir};
use super::{Cut, pk_ranges_overlap};
use gnitz_trace::runs::StorageError;
use gnitz_trace::runs::{Codebook, ShardWriteOpts};
use gnitz_trace::runs::{GuardMerge, MappedShard, guard_slot, write_shard};
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

/// Which trigger a compaction is serving. Only [`Dehydrate`](Self::Dehydrate)
/// changes what is written. The index learns its run size from an
/// [`L0Fold`](Self::L0Fold) and its cancel yield from a
/// [`GuardSplit`](Self::GuardSplit), the one fold that reads a whole guard in
/// place; the rest only label the byte accounting of `ShardIndex::stats`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum CompactionKind {
    L0Fold,
    GuardSplit,
    /// The newest shards of an L1 guard over its file threshold, folded into one.
    TierFold,
    /// An L1 guard cut at the terminal guard keys its extent spans, each band
    /// sunk into the terminal guard it meets.
    BandCut,
    /// A terminal guard holding more than one shard, folded whole.
    Vertical,
    GuardMerge,
    Dehydrate,
}

/// One trigger's byte accounting over the folds a [`ShardIndex`] installed.
#[cfg(test)]
#[derive(Default)]
struct Phase {
    n: usize,
    in_bytes: u64,
    max_in: u64,
    out_bytes: u64,
    in_files: usize,
}

#[cfg(test)]
impl Phase {
    /// A fold that read `inb` bytes over `inf` files and wrote `outb`.
    fn add(&mut self, inb: u64, outb: u64, inf: usize) {
        self.n += 1;
        self.in_bytes += inb;
        self.max_in = self.max_in.max(inb);
        self.out_bytes += outb;
        self.in_files += inf;
    }
}

/// The tree's guarded levels, top down, under L0. Every one is a guard partition
/// of the key line.
const LEVELS: usize = 2;
/// Guards of overlapping shards, each folding its newest ones in place past
/// its [`GUARD_FILE_THRESHOLD`] and drained into the terminal level.
const L1: usize = 0;
/// The deepest level: one shard per guard at rest, each guard keyed at its
/// least row, and the only level a sweep dehydrates.
const TERMINAL: usize = 1;
/// L0 shards past this count trigger the fold into L1.
pub(super) const L0_COMPACT_THRESHOLD: usize = 4;
/// Files one guard of each level holds at rest; a guard past it folds its
/// newest shards into one.
const GUARD_FILE_THRESHOLD: [usize; LEVELS] = [4, 1];
/// The share of its rows a guard, or of the store's rows L0, may hold that a
/// fold is expected to cancel before it folds for that alone: the rows a
/// retraction and the row it retracts take up until one merge reads both.
const CANCEL_PERCENT: usize = 25;
/// Floor under every guard byte target. It bounds the guard *count*: a store
/// holds `resident / target` guards, so a target derived from a tiny budget
/// would otherwise shatter one fold into shards of a few hundred bytes.
const MIN_GUARD_BYTES: u64 = 64 * 1024;
/// How many eviction steps a budgeted store's capacity is cut into. One step is
/// the terminal guard target, and so the granularity the capacity sweep evicts
/// at; L1 may park two, leaving the sweep the rest.
const SWEEP_STEPS: u64 = 8;
/// Keys sampled per guard when deriving its split points.
const SPLIT_SAMPLES: usize = 1024;
/// Most destination buckets one fold may write — one output shard each.
const MAX_PARTS: u64 = 64;
/// Fold input bytes an ingest pays for per byte it ingests. A chosen factor, not
/// a derived one: it has to exceed what a store's folds read per ingested byte,
/// or what the store owes grows with it, and that ratio is a property of the
/// workload.
const UPKEEP_LEVY: u64 = 64;

struct ShardEntry {
    shard: Rc<MappedShard>,
    /// The seq that names this shard in its store's directory.
    seq: u64,
    /// The highest seq whose rows this shard holds: its write recency.
    newest: u64,
    pk_min: PkBuf,
    pk_max: PkBuf,
}

impl ShardEntry {
    /// Open the shard drawn at `seq`, at `path`.
    fn open(path: &str, seq: u64, schema: &SchemaDescriptor, newest: u64) -> Result<Self, StorageError> {
        let shard = Rc::new(MappedShard::open(path, schema)?);
        let (pk_min, pk_max) = (shard.pk_at(0), shard.pk_at(shard.row_count() - 1));
        Ok(ShardEntry { shard, seq, newest, pk_min, pk_max })
    }
}

struct LevelGuard {
    guard_key: PkBuf,
    entries: Vec<ShardEntry>,
}

impl LevelGuard {
    /// Whether this guard holds skeleton shards — the state a capacity sweep
    /// leaves behind, read off the shard headers.
    fn dehydrated(&self) -> bool {
        self.entries.iter().any(|e| e.shard.is_skeleton())
    }

    /// Total registered bytes of this guard's entries.
    fn bytes(&self) -> u64 {
        self.entries.iter().map(|e| e.shard.file_len()).sum()
    }

    /// Whether this guard's entries can hold a key of `[lo, hi]`.
    fn overlaps(&self, lo: PkBuf, hi: PkBuf) -> bool {
        let (min, max) = self.key_extent();
        pk_ranges_overlap(min.pk_bytes(), max.pk_bytes(), lo.pk_bytes(), hi.pk_bytes())
    }

    /// The key span this guard's entries actually cover. The guard *key* is only
    /// the span's lower fence, so a fold out of this guard must route by this
    /// instead.
    fn key_extent(&self) -> (PkBuf, PkBuf) {
        extent(self.entries.iter()).expect("a guard holds at least one shard")
    }
}

/// The key span `entries` cover: their least `pk_min` and greatest `pk_max`,
/// or `None` for no entries.
fn extent<'a>(entries: impl Iterator<Item = &'a ShardEntry>) -> Option<(PkBuf, PkBuf)> {
    entries.fold(None, |span, e| match span {
        None => Some((e.pk_min, e.pk_max)),
        Some((lo, hi)) => Some((lo.min(e.pk_min), hi.max(e.pk_max))),
    })
}

/// The rows of a guard's `shards`, oldest first, the ones that cancel included,
/// and the retractions a fold of them alone can cancel: those of every shard but
/// the oldest, which has no older one there to retract from.
fn rows_and_retractions<'a>(shards: impl Iterator<Item = &'a MappedShard> + Clone) -> (usize, usize) {
    (
        shards.clone().map(MappedShard::row_count).sum(),
        shards.skip(1).map(MappedShard::retraction_rows).sum(),
    )
}

/// The destination keys a fold of `entries` into the guard at `key` routes into
/// — sorted and distinct, as each fold step's window ends must ascend. `key`
/// alone until the entries hold more than `target` bytes, then row quantiles of
/// a bounded sample as well.
///
/// Row quantiles stand in for byte quantiles: a skewed sample only makes the
/// split uneven, and the oversized half crosses the target again next fold.
/// A quantile below `key` arises only for guard 0, which owns the tail below its
/// own key; minting a guard down there is what makes that tail addressable.
///
/// Entries holding one distinct key cannot be cut, and say so from their extent
/// rather than from a sample — they stay over target forever, so a sampling
/// pass would repeat on them indefinitely. That is the one limit of `target` as
/// a bound, and it is inherent: a guard boundary *is* a key, so rows sharing a
/// PK cannot be split across one.
///
/// Entries mixing skeleton and hydrated shards fold to `key` alone: the output
/// is a skeleton, one row per key, so a part count derived from the hydrated
/// bytes would shatter it.
fn fold_destinations<'a>(key: PkBuf, entries: impl Iterator<Item = &'a ShardEntry> + Clone, target: u64) -> Vec<PkBuf> {
    let mut keys = vec![key];
    let bytes: u64 = entries.clone().map(|e| e.shard.file_len()).sum();
    let parts = bytes.div_ceil(target).min(MAX_PARTS) as usize;
    let skeleton = |s: bool| entries.clone().any(|e| e.shard.is_skeleton() == s);
    if parts >= 2 && !(skeleton(true) && skeleton(false)) && extent(entries.clone()).is_some_and(|(lo, hi)| lo < hi) {
        // About `SPLIT_SAMPLES` row keys: each shard is sorted and its PK region
        // fixed-stride, so a key is one O(1) read.
        let rows: usize = entries.clone().map(|e| e.shard.row_count()).sum();
        let step = (rows / SPLIT_SAMPLES).max(1);
        let mut sample: Vec<PkBuf> = entries
            .flat_map(|e| (0..e.shard.row_count()).step_by(step).map(|r| e.shard.pk_at(r)))
            .collect();
        sample.sort_unstable();
        sample.dedup();
        let parts = parts.min(sample.len());
        keys.extend((1..parts).map(|i| sample[i * sample.len() / parts]));
        keys.sort_unstable();
        keys.dedup();
    }
    keys
}

#[derive(Default)]
struct FLSMLevel {
    guards: Vec<LevelGuard>,
}

impl FLSMLevel {
    /// The guard owning `key`: the last one `≤ key`, or 0 below the first — and
    /// 0 on an empty level, which callers bound against `guards.len()`.
    fn slot(&self, key: &[u8]) -> usize {
        // A lone guard owns the whole key line, whatever its key: no compare.
        if self.guards.len() <= 1 {
            return 0;
        }
        guard_slot(&self.guards, key, |g| g.guard_key.pk_bytes())
    }

    /// The guards overlapping `[range_min, range_max]`: a contiguous run,
    /// because guards partition the key line.
    fn find_guards_for_range(&self, range_min: &[u8], range_max: &[u8]) -> Range<usize> {
        self.slot(range_min)..(self.slot(range_max) + 1).min(self.guards.len())
    }

    /// Every shard in this level, in guard order.
    #[cfg(test)]
    fn entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.guards.iter().flat_map(|g| g.entries.iter())
    }

    /// Total registered bytes of every guard in this level.
    fn bytes(&self) -> u64 {
        self.guards.iter().map(LevelGuard::bytes).sum()
    }

    fn get_or_create_guard(&mut self, gk: PkBuf) -> &mut LevelGuard {
        let pos = match self.guards.binary_search_by(|g| g.guard_key.cmp(&gk)) {
            Ok(pos) => pos,
            Err(pos) => {
                self.guards
                    .insert(pos, LevelGuard { guard_key: gk, entries: Vec::new() });
                pos
            }
        };
        &mut self.guards[pos]
    }
}

pub(super) struct ShardIndex {
    /// The directory the shards are drawn in and published through.
    pub(super) dir: StoreDir,
    pub(super) schema: SchemaDescriptor,

    /// L0: shards as they arrive, oldest first, whose key spans overlap. All
    /// but the last [`Self::pending`] are under the store's cut and fold down
    /// into L1 whole, never in place.
    l0: Vec<ShardEntry>,
    /// How many of `l0`'s last shards a flush wrote of rows above the cut:
    /// read by every reader but one at the cut and folded with nothing, until
    /// [`Self::seal_pending`] moves the cut past them. A reopen finds every
    /// shard of L0 under it.
    pending: usize,
    levels: [FLSMLevel; LEVELS],

    /// `R`, the unit every byte target is stated in: the running max of the
    /// registered L0 bytes one L0 fold consumed, of one shard it wrote and of
    /// one terminal run, floored at [`MIN_GUARD_BYTES`] and persisted in the
    /// manifest header.
    l0_run_bytes: u64,
    /// `CREATE VIEW … WITH (capacity = …)`: the registered shard bytes past
    /// which a sweep leaves skeleton rows behind. `None` for every store but a
    /// capacity-bounded view's output store.
    capacity: Option<u64>,
    /// Passed to every shard write.
    skip_pk_filter: bool,
    /// `WITH (compress = true)`: the store codes its shards' heaps under a
    /// symbol table where that pays.
    compress: bool,
    /// The table a compressing store codes under: the one its newest coded
    /// shard was written under, until a spill its codes drift from trains
    /// another.
    codebook: Option<Rc<Codebook>>,
    /// What the in-place guard folds so far cancelled per retraction they read.
    cancel_yield: CancelYield,
    /// Whether the tree may owe a fold: set by every shard that enters it and
    /// cleared by the [`Self::maintain`] that finds none left.
    owed: bool,
    /// Fold input bytes [`Self::upkeep`] merged past what its calls so far paid
    /// for: a fold stops at a destination, not at a byte.
    upkeep_overdraft: u64,
    /// The fold under way, between two calls of [`Self::maintain`].
    running: Option<Running>,
    /// Per-trigger byte accounting of the folds this index installed.
    #[cfg(test)]
    stats: std::collections::BTreeMap<CompactionKind, Phase>,
}

/// How many rows a retraction is expected to cancel when a fold brings it
/// together with the shards under it, learned from the folds that did. Two where
/// each retraction meets the row it retracts, none in a store whose retractions
/// are its content — the trace of a subtracted operand, a delta feed.
///
/// Both terms halve at every observation, so the estimate follows a store whose
/// input changes character. Not persisted: a reopened store starts at two and
/// pays at most one fold a guard to learn otherwise.
#[derive(Clone, Copy)]
struct CancelYield {
    cancelled: usize,
    retractions: usize,
}

impl CancelYield {
    const FRESH: Self = CancelYield { cancelled: 2, retractions: 1 };

    /// The rows a fold over `retractions` is expected to cancel.
    fn expect(self, retractions: usize) -> usize {
        (retractions as u128 * self.cancelled as u128 / self.retractions as u128) as usize
    }

    /// A fold that read `retractions` wrote `cancelled` rows fewer than it read.
    fn observe(&mut self, retractions: usize, cancelled: usize) {
        if retractions > 0 {
            self.cancelled = self.cancelled / 2 + cancelled.min(2 * retractions);
            self.retractions = self.retractions / 2 + retractions;
        }
    }
}

/// One fold the tree owes: the shards it merges and where the merge goes.
struct Fold {
    kind: CompactionKind,
    /// The inputs, in merge order.
    shards: Vec<Rc<MappedShard>>,
    /// The newest stamp over the inputs, which every output inherits.
    newest: u64,
    dest: usize,
    /// The destination guards, sorted and distinct.
    keys: Vec<PkBuf>,
}

impl Fold {
    /// The fold of `inputs`, in merge order, into the guards keyed `keys` at
    /// level `dest`.
    fn new<'a>(
        kind: CompactionKind,
        dest: usize,
        keys: Vec<PkBuf>,
        inputs: impl IntoIterator<Item = &'a ShardEntry>,
    ) -> Self {
        let (mut shards, mut newest) = (Vec::new(), 0);
        for e in inputs {
            shards.push(Rc::clone(&e.shard));
            newest = newest.max(e.newest);
        }
        Fold { kind, shards, newest, dest, keys }
    }
}

/// A fold under way: its inputs stay registered and read, its outputs are
/// written and registered nowhere, until the last destination is written and
/// the two are exchanged.
struct Running {
    fold: Fold,
    merge: GuardMerge,
    /// The outputs written so far, one per destination of `fold.keys`; `None`
    /// where every row cancelled.
    opened: Vec<Option<ShardEntry>>,
}

impl ShardIndex {
    /// Open the store at `output_dir`, creating the directory if it is missing.
    /// `recovery` decides which manifest, if any, the open loads; every shard
    /// that manifest does not name, and the staging file, are unlinked.
    pub(super) fn open(
        output_dir: &str,
        schema: SchemaDescriptor,
        recovery: RecoverySource,
        capacity: Option<u64>,
        compress: bool,
    ) -> Result<Self, StorageError> {
        let dir = StoreDir::open(output_dir, recovery)?;
        let shards = dir.in_place().cloned().unwrap_or_default();
        let mut idx = ShardIndex {
            dir,
            schema,
            l0: Vec::new(),
            pending: 0,
            levels: Default::default(),
            l0_run_bytes: shards.run_bytes.max(MIN_GUARD_BYTES),
            capacity,
            // A rederived store's shards carry no PK filter: nothing probes it by PK.
            skip_pk_filter: matches!(recovery, RecoverySource::Rederive { .. }),
            compress,
            codebook: None,
            cancel_yield: CancelYield::FRESH,
            // A reopened tree is asked once whether it owes anything.
            owed: !shards.entries.is_empty(),
            upkeep_overdraft: 0,
            running: None,
            #[cfg(test)]
            stats: Default::default(),
        };
        for e in &shards.entries {
            let entry = ShardEntry::open(&idx.dir.shard_path(e.seq), e.seq, &idx.schema, e.newest)?;
            match e.level {
                // L0's manifest entries, pending ones included, carry no guard.
                0 => idx.l0.push(entry),
                level if level as usize <= LEVELS => idx.levels[level as usize - 1]
                    .get_or_create_guard(e.guard_key)
                    .entries
                    .push(entry),
                _ => return Err(StorageError::Corrupt("manifest level")),
            }
        }
        if compress {
            let newest_coded = idx
                .all_entries()
                .filter(|e| e.shard.heap().plain().is_none())
                .max_by_key(|e| e.seq);
            idx.codebook = newest_coded.and_then(|e| e.shard.codebook()).map(Rc::new);
        }
        Ok(idx)
    }

    /// The shard set this index publishes: L0 at level 0 under no guard, then
    /// each guarded level below it, pending shards included.
    pub(super) fn shard_set(&self) -> ShardSet {
        let entry = |level, guard_key, e: &ShardEntry| ManifestEntry {
            seq: e.seq,
            newest: e.newest,
            level,
            guard_key,
        };
        let whole = PkBuf::zeroed(self.schema.pk_stride());
        let l0 = self.l0.iter().map(|e| entry(0, whole, e));
        let guarded = (1u64..).zip(&self.levels).flat_map(|(level, l)| {
            (l.guards.iter()).flat_map(move |g| g.entries.iter().map(move |e| entry(level, g.guard_key, e)))
        });
        ShardSet {
            run_bytes: self.l0_run_bytes,
            entries: l0.chain(guarded).collect(),
        }
    }

    /// Publish `schema`, rebinding every registered shard to it. A failure on any
    /// shard leaves the index on the old schema.
    pub(super) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        if schema != self.schema {
            self.abandon_fold();
            // Every rebind first, so a failure leaves each entry as it was.
            let guarded = (self.levels.iter_mut()).flat_map(|l| l.guards.iter_mut().flat_map(|g| &mut g.entries));
            let mut entries: Vec<&mut ShardEntry> = self.l0.iter_mut().chain(guarded).collect();
            let rebound: Vec<Rc<MappedShard>> = (entries.iter())
                .map(|e| e.shard.rebind(&schema).map(Rc::new))
                .collect::<Result<_, _>>()?;
            for (e, shard) in entries.iter_mut().zip(rebound) {
                e.shard = shard;
            }
        }
        self.schema = schema;
        Ok(())
    }

    /// Verify every registered shard's body.
    pub(super) fn verify_shards(&self) -> Result<(), StorageError> {
        self.all_entries().try_for_each(|e| e.shard.verify_body())
    }

    /// Every shard `cut` reads whose PK extent meets the inclusive `bound`, the
    /// pending ones only at [`Cut::Now`]; `None` takes them all.
    ///
    /// Newest first: L0's shards, pending ones above the rest, then each
    /// level's routed guards, each newest first. Every fold keeps that order, as
    /// its outputs hold no row older than one left above them, and a key lies in
    /// one guard per level, so of the shards holding a key each is newer than
    /// every later one.
    fn entries(&self, bound: Option<(&[u8], &[u8])>, cut: Cut) -> impl Iterator<Item = &ShardEntry> {
        let levels = self.levels.iter().flat_map(move |level| {
            let guards = match bound {
                Some((lo, hi)) => level.find_guards_for_range(lo, hi),
                None => 0..level.guards.len(),
            };
            level.guards[guards].iter().flat_map(|g| g.entries.iter().rev())
        });
        let l0 = match cut {
            Cut::Now => &self.l0[..],
            Cut::Sealed => self.sealed_l0(),
        };
        (l0.iter().rev()).chain(levels).filter(move |e| {
            bound.is_none_or(|(lo, hi)| pk_ranges_overlap(e.pk_min.pk_bytes(), e.pk_max.pk_bytes(), lo, hi))
        })
    }

    fn all_entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.entries(None, Cut::Now)
    }

    /// L0's shards under the cut, the ones its folds read.
    fn sealed_l0(&self) -> &[ShardEntry] {
        &self.l0[..self.l0.len() - self.pending]
    }

    /// L0's shards above the cut.
    fn pending_l0(&self) -> &[ShardEntry] {
        &self.l0[self.l0.len() - self.pending..]
    }

    /// Write `run`, a run spilled into the store, as an unpublished shard.
    fn write_run(&mut self, run: &Batch) -> Result<ShardEntry, StorageError> {
        self.write_with(Some(run), None, |path, opts| write_shard(run, path, opts))
    }

    /// Write a shard named by a fresh seq through `write`, handed its path and
    /// the store's options, and open it; `newest` defaults to that seq. A
    /// compressing store codes `plain`, rows whose heap holds their values
    /// verbatim, under the codebook [`Codebook::for_batch`] picks for them, and
    /// adopts it as its own; none for a run too small to code.
    fn write_with(
        &mut self,
        plain: Option<&Batch>,
        newest: Option<u64>,
        write: impl FnOnce(&str, ShardWriteOpts<'_>) -> Result<(), StorageError>,
    ) -> Result<ShardEntry, StorageError> {
        let codebook =
            (plain.filter(|_| self.compress)).and_then(|run| Codebook::for_batch(self.codebook.as_ref(), run));
        if codebook.is_some() {
            self.codebook.clone_from(&codebook);
        }
        let (seq, path) = self.dir.draw();
        let opts = ShardWriteOpts {
            skip_pk_filter: self.skip_pk_filter,
            codebook: codebook.as_deref(),
        };
        write(&path, opts)
            .and_then(|()| ShardEntry::open(&path, seq, &self.schema, newest.unwrap_or(seq)))
            // Drawn after the last publish, so retired is unlinked.
            .inspect_err(|_| self.dir.retire(seq))
    }

    /// Append `run` to L0 as one unpublished shard under the cut: the spill.
    pub(super) fn append_l0_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        let entry = self.write_run(run)?;
        self.l0.insert(self.l0.len() - self.pending, entry);
        self.owed = true;
        Ok(())
    }

    /// Append `run`, rows above the store's cut, to L0 as one unpublished shard
    /// above it.
    pub(super) fn append_pending_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        let entry = self.write_run(run)?;
        self.l0.push(entry);
        self.pending += 1;
        Ok(())
    }

    /// Whether any shard sits above the cut.
    pub(super) fn has_pending(&self) -> bool {
        self.pending > 0
    }

    /// The least key a shard above the cut holds.
    pub(super) fn pending_least(&self) -> Option<&[u8]> {
        self.pending_l0().iter().map(|e| e.pk_min.pk_bytes()).min()
    }

    /// Move the cut past every pending shard. Answers their shards.
    pub(super) fn seal_pending(&mut self) -> Vec<Rc<MappedShard>> {
        let shards: Vec<_> = self.pending_l0().iter().map(|e| Rc::clone(&e.shard)).collect();
        if !shards.is_empty() {
            self.pending = 0;
            self.owed = true;
        }
        shards
    }

    /// The tree's shape as counts: L0 shards, then L1's and the terminal level's
    /// guards.
    #[cfg(test)]
    pub(super) fn level_shape(&self) -> (usize, [usize; 2]) {
        (
            self.sealed_l0().len(),
            [L1, TERMINAL].map(|l| self.levels[l].guards.len()),
        )
    }

    /// Whether the tree may owe a fold.
    #[cfg(test)]
    pub(super) fn owed(&self) -> bool {
        self.owed
    }

    /// Pay the upkeep for `ingested` bytes: [`UPKEEP_LEVY`] bytes of fold input
    /// for each, less what earlier calls overdrew. A store nothing is ingested
    /// into keeps what it owes.
    pub(super) fn upkeep(&mut self, ingested: usize) -> Result<(), StorageError> {
        if !self.owed {
            return Ok(());
        }
        let levy = ingested as u64 * UPKEEP_LEVY;
        let read = self.maintain(levy.saturating_sub(self.upkeep_overdraft))?;
        self.upkeep_overdraft = (self.upkeep_overdraft + read).saturating_sub(levy);
        Ok(())
    }

    /// The disk tier's upkeep: the folds the tree owes, one destination at a
    /// time, until none is left or `budget` input bytes are merged. A fold is
    /// resumed where the last call left it, so a call is held for one
    /// destination past its budget at most. Answers the input bytes merged.
    pub(super) fn maintain(&mut self, budget: u64) -> Result<u64, StorageError> {
        let mut read = 0;
        while self.owed && read < budget {
            if self.running.is_none() {
                match self.plan() {
                    Some(fold) => self.begin(fold)?,
                    None => self.owed = false,
                }
                continue;
            }
            read += self.step()?;
        }
        Ok(read)
    }

    /// Finish the fold under way, if any: a manifest names a tree, and a
    /// half-written fold's outputs are in none.
    fn finish_fold(&mut self) -> Result<(), StorageError> {
        while self.running.is_some() {
            self.step()?;
        }
        Ok(())
    }

    /// Give up the fold under way and what it wrote; its inputs were never
    /// touched.
    fn abandon_fold(&mut self) {
        if let Some(run) = self.running.take() {
            self.retire(run.opened.into_iter().flatten());
        }
    }

    /// Whether `retractions` are expected to cancel [`CANCEL_PERCENT`] of `rows`.
    fn cancels(&self, retractions: usize, rows: usize) -> bool {
        let cancelled = self.cancel_yield.expect(retractions);
        cancelled > 0 && cancelled * 100 >= rows * CANCEL_PERCENT
    }

    /// Write `run` as one unpublished shard at the terminal level, under a new
    /// guard keyed by its first PK.
    pub(super) fn append_terminal_run(&mut self, run: &Batch) -> Result<(), StorageError> {
        assert!(
            !run.is_empty() && run.is_consolidated(),
            "a terminal run is consolidated"
        );
        let first = PkBuf::from_bytes(run.get_pk_bytes(0));
        let terminal = &self.levels[TERMINAL];
        assert!(
            terminal.guards.last().is_none_or(|g| g.key_extent().1 < first),
            "a terminal run ascends past every key its level holds"
        );
        debug_assert!(
            self.running.is_none(),
            "a terminal run enters a store with no fold under way"
        );
        debug_assert!(
            self.l0.is_empty() && self.levels[L1].guards.is_empty(),
            "a terminal run lands under no newer shard"
        );
        let entry = self.write_run(run)?;
        self.l0_run_bytes = self.l0_run_bytes.max(entry.shard.file_len());
        self.sink(vec![entry]);
        Ok(())
    }

    /// Stage the manifest of the current tree, carrying `checkpoint_mark`; see
    /// [`StoreDir::stage`].
    pub(super) fn stage(&mut self, checkpoint_mark: u64) -> Result<Option<FlushWork<'_>>, StorageError> {
        self.finish_fold()?;
        let shards = self.shard_set();
        self.dir.stage(checkpoint_mark, shards)
    }

    /// The shards of [`Self::entries`].
    pub(super) fn shard_arcs(&self, bound: Option<(&[u8], &[u8])>, cut: Cut) -> impl Iterator<Item = Rc<MappedShard>> {
        self.entries(bound, cut).map(|e| Rc::clone(&e.shard))
    }

    /// Registered shards across every tier.
    #[cfg(test)]
    pub(super) fn shard_count(&self) -> usize {
        self.all_entries().count()
    }

    /// Raw rows across every live shard, summed without touching an `Rc`. Raw:
    /// cross-shard duplicates and ghosts are counted, so it is an upper bound on
    /// the live rows a walk would emit — the shape the selectivity gate wants.
    pub(super) fn total_rows(&self) -> usize {
        self.all_entries().map(|e| e.shard.row_count()).sum()
    }

    /// Whether a read of this store can meet a `(PK, coarse weight)` row it has to
    /// hydrate. Only a budgeted view's sweep writes one, and only into the
    /// terminal level.
    pub(super) fn has_skeleton_shard(&self) -> bool {
        self.capacity.is_some() && self.levels[TERMINAL].guards.iter().any(LevelGuard::dehydrated)
    }

    /// Each shard holding OPK `key`, in [`Self::entries`]' newest-first order,
    /// with its rows of that key; `filter_key` is `key`'s `probe_key`. Each
    /// level routes by the whole key against its guard partition, once the
    /// walk reaches it.
    #[inline]
    pub(super) fn holding<'s, 'k>(&'s self, key: &'k [u8], filter_key: u64) -> Holding<'s, 'k> {
        Holding {
            entries: &self.l0,
            levels: &self.levels,
            key,
            filter_key,
        }
    }

    /// Start `fold`: pin its inputs and verify them.
    fn begin(&mut self, fold: Fold) -> Result<(), StorageError> {
        let dehydrate = fold.kind == CompactionKind::Dehydrate;
        let merge = GuardMerge::begin(fold.shards.clone(), fold.keys[1..].to_vec(), dehydrate)?;
        let opened = Vec::with_capacity(fold.keys.len());
        self.running = Some(Running { fold, merge, opened });
        Ok(())
    }

    /// Merge the next destination of the fold under way and write its shard;
    /// after the last, exchange the fold's inputs for its outputs.
    ///
    /// A failure registers nothing and unlinks every output the fold wrote.
    /// Answers the input bytes it merged.
    fn step(&mut self) -> Result<u64, StorageError> {
        let mut run = self.running.take().expect("a fold is under way");
        let before = run.merge.bytes_merged();
        let codebook = self.codebook.clone();
        let merged = run.merge.next(codebook.as_deref());
        // +1: a destination whose rows all cancelled still costs a step.
        let read = run.merge.bytes_merged() - before + 1;
        // A run the merge left plain is coded under a codebook of its own.
        let written = merged.map(|merged| {
            self.write_with(merged.plain(), Some(run.fold.newest), |path, opts| {
                merged.write_as_shard(path, opts)
            })
        });
        match written.transpose() {
            Ok(entry) => run.opened.push(entry),
            // The fold is out of `running` and so given up: unlink what it wrote.
            Err(e) => {
                self.retire(run.opened.into_iter().flatten());
                return Err(e);
            }
        }
        if run.merge.done() {
            self.install(run);
        } else {
            self.running = Some(run);
        }
        Ok(read)
    }

    /// Retire the inputs of a finished fold and register its outputs: in L1
    /// under the guards of its keys, at the terminal level sunk where their
    /// rows lie.
    fn install(&mut self, run: Running) {
        let Running {
            fold: Fold { kind, shards, dest, keys, .. },
            opened,
            ..
        } = run;
        let opened: Vec<(PkBuf, ShardEntry)> = (keys.into_iter())
            .zip(opened)
            .filter_map(|(key, entry)| Some((key, entry?)))
            .collect();
        if kind == CompactionKind::L0Fold {
            // A run that lands in one guard is one shard whose encodings span
            // the whole run, and can outweigh the spills it was written from.
            // Unobserved, every such guard would be over target as written and
            // be rewritten at once to be cut in two.
            let largest = opened.iter().map(|(_, e)| e.shard.file_len()).max();
            self.l0_run_bytes = self.l0_run_bytes.max(largest.unwrap_or(0));
        }
        if kind == CompactionKind::GuardSplit {
            // The one fold that reads a guard's whole history at its level, in
            // order, and so every row its retractions can cancel there.
            let wrote: usize = opened.iter().map(|(_, e)| e.shard.row_count()).sum();
            let (read, retractions) = rows_and_retractions(shards.iter().map(|s| &**s));
            self.cancel_yield.observe(retractions, read.saturating_sub(wrote));
        }
        #[cfg(test)]
        self.stats.entry(kind).or_default().add(
            shards.iter().map(|s| s.file_len()).sum(),
            opened.iter().map(|(_, e)| e.shard.file_len()).sum(),
            shards.len(),
        );

        // The inputs are found by their `Rc`s: each registered entry holds its
        // own, and only `ShardIndex::swap_schema` replaces one, abandoning the
        // fold first.
        let is_input = |e: &ShardEntry| shards.iter().any(|s| Rc::ptr_eq(s, &e.shard));
        let mut superseded = Vec::with_capacity(shards.len());
        // No fold reads a pending shard, so its inputs lie under the cut.
        let sealed = self.l0.len() - self.pending;
        superseded.extend(self.l0.extract_if(..sealed, |e| is_input(e)));
        for level in &mut self.levels {
            for guard in &mut level.guards {
                superseded.extend(guard.entries.extract_if(.., |e| is_input(e)));
            }
            level.guards.retain(|g| !g.entries.is_empty());
        }
        debug_assert_eq!(superseded.len(), shards.len(), "a fold's inputs are registered entries");
        self.retire(superseded);
        for (key, entry) in opened {
            if dest == TERMINAL {
                self.sink(vec![entry]);
            } else {
                self.levels[dest].get_or_create_guard(key).entries.push(entry);
            }
        }
    }

    /// Register `entries`, whose keys lie in one terminal slot, on top of the
    /// terminal guard whose shards they meet, or under a guard of their own at
    /// their least key. A terminal guard is keyed at its least row: only guard
    /// 0 can be met below its key, and lowering the first key keeps the guards
    /// sorted.
    fn sink(&mut self, entries: Vec<ShardEntry>) {
        let (lo, hi) = extent(entries.iter()).expect("something to sink");
        let terminal = &mut self.levels[TERMINAL];
        debug_assert!(
            terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes()).len() <= 1,
            "what sinks lies in one terminal slot"
        );
        let slot = terminal.slot(lo.pk_bytes());
        let guard = if terminal.guards.get(slot).is_some_and(|g| g.overlaps(lo, hi)) {
            let guard = &mut terminal.guards[slot];
            guard.guard_key = guard.guard_key.min(lo);
            guard
        } else {
            terminal.get_or_create_guard(lo)
        };
        guard.entries.extend(entries);
        self.owed = true;
    }

    fn retire(&mut self, entries: impl IntoIterator<Item = ShardEntry>) {
        for e in entries {
            self.dir.retire(e.seq);
        }
    }

    /// Retire every shard, leaving a store of no rows.
    pub(super) fn clear(&mut self) {
        self.abandon_fold();
        let l0 = std::mem::take(&mut self.l0);
        self.pending = 0;
        let levels = std::mem::take(&mut self.levels);
        let settled = levels.into_iter().flat_map(|l| l.guards).flat_map(|g| g.entries);
        self.retire(l0.into_iter().chain(settled));
    }

    /// The next fold the tree owes, or `None` when it owes none. What costs no
    /// merge — an L1 guard sunk into the terminal level as it stands — is done
    /// here.
    ///
    /// In order: every guard over its byte target or file threshold, the
    /// terminal guards bands sank into among them, before the L0 fold lands
    /// more on it; the L0 fold; the underfull runs, which must see the sizes a
    /// split left; L1 down to its byte target; and the store down to its
    /// capacity.
    fn plan(&mut self) -> Option<Fold> {
        loop {
            // The highest guard of a level that owes a split.
            let split = [L1, TERMINAL].into_iter().find_map(|level| {
                (self.levels[level].guards.iter().rev()).find_map(|guard| self.split_of(level, guard))
            });
            if split.is_some() {
                return split;
            }
            // L0's retractions retract from the levels below, so only the fold
            // down brings them to the guards whose own folds then cancel them:
            // they are judged against the whole store.
            let l0_retractions = self.sealed_l0().iter().map(|e| e.shard.retraction_rows()).sum();
            if self.sealed_l0().len() > L0_COMPACT_THRESHOLD
                || (l0_retractions > 0
                    && self.cancels(
                        l0_retractions,
                        self.entries(None, Cut::Sealed).map(|e| e.shard.row_count()).sum(),
                    ))
            {
                return self.plan_l0_fold();
            }
            let merge = [L1, TERMINAL].into_iter().find_map(|level| self.plan_merge(level));
            if merge.is_some() {
                return merge;
            }
            if self.levels[L1].bytes() <= self.l1_target_bytes() {
                let cap = self.capacity.filter(|&cap| self.resident_bytes() > cap)?;
                // The oldest-written hydrated terminal guard: the only recency
                // signal the tree carries, since nothing records that a row was read.
                let victim = self.levels[TERMINAL]
                    .guards
                    .iter()
                    .enumerate()
                    .filter(|(_, g)| !g.dehydrated())
                    .min_by_key(|(_, g)| g.entries.iter().map(|e| e.newest).max())
                    .map(|(gi, _)| gi);
                if let Some(gi) = victim {
                    return Some(self.plan_dehydration(gi));
                }
                // Every terminal guard a skeleton, and that floor alone at or over
                // the capacity: no drain brings the store under it.
                if self.levels[TERMINAL].bytes() >= cap {
                    return None;
                }
                // Nothing left to evict where it sits: push a level's worth of
                // data down to make one, L1 before L0.
            }
            match self.cheapest_l1_guard_to_drain() {
                Some(gi) => {
                    if let Some(fold) = self.plan_drain(gi) {
                        return Some(fold);
                    }
                }
                // L1 empty, so only the capacity sweep gets here, over a floor
                // under the capacity: L0 folds down to be drained next. With L0
                // under the cut empty too, pending shards hold the store over
                // its capacity, and no fold reaches them.
                None => return self.plan_l0_fold(),
            }
        }
    }

    /// Total registered bytes of L0 under the cut.
    fn l0_bytes(&self) -> u64 {
        self.sealed_l0().iter().map(|e| e.shard.file_len()).sum()
    }

    /// All of L0 under the cut into L1's guards; `None` while that is empty.
    /// Observing `R` first is what makes the targets reflect the fold this plans.
    fn plan_l0_fold(&mut self) -> Option<Fold> {
        if self.sealed_l0().is_empty() {
            return None;
        }
        self.l0_run_bytes = self.l0_run_bytes.max(self.l0_bytes());
        Some(Fold::new(
            CompactionKind::L0Fold,
            L1,
            self.l1_guard_keys(),
            self.sealed_l0(),
        ))
    }

    /// The largest a guard of `level_idx` is allowed to get: `R`, so no
    /// compaction's input grows with the dataset — every fold reads at most two
    /// guards' worth.
    ///
    /// A budgeted store's terminal level takes one sweep step instead, since that
    /// is the granularity the capacity sweep evicts at. The clamp keeps a very
    /// large or very small `capacity` from naming a target outside
    /// `[MIN_GUARD_BYTES, R]`.
    fn guard_target_bytes(&self, level_idx: usize) -> u64 {
        match self.capacity {
            Some(cap) if level_idx == TERMINAL => (cap / SWEEP_STEPS).clamp(MIN_GUARD_BYTES, self.l0_run_bytes),
            _ => self.l0_run_bytes,
        }
    }

    /// Bytes L1 is drained to. Capped at two sweep steps for a budgeted store:
    /// the capacity sweep cannot evict from L1, so bytes parked there come out of
    /// what the user asked for.
    fn l1_target_bytes(&self) -> u64 {
        let target = Self::balanced_l1_target(self.levels[TERMINAL].bytes(), self.l0_run_bytes);
        match self.capacity {
            Some(cap) => target.min(2 * (cap / SWEEP_STEPS)),
            None => target,
        }
    }

    /// `2√(|L2|·R)`, a chosen size and not a derived optimum: a larger L1 saves
    /// vertical rewrites and holds more shards for a scan to merge. The `16 R`
    /// floor under it is chosen too: it keeps a small store from draining L1 on
    /// every spill.
    ///
    /// `l2_bytes × r` overflows a `u64` at the design point, hence the `u128`;
    /// the `2` stays outside the root so the extremes saturate rather than
    /// overflow it in turn.
    fn balanced_l1_target(l2_bytes: u64, r: u64) -> u64 {
        let balanced = 2 * (u128::from(l2_bytes) * u128::from(r)).isqrt();
        u64::try_from(balanced).unwrap_or(u64::MAX).max(16u64.saturating_mul(r))
    }

    /// The L1 guard whose fold rewrites the fewest terminal bytes per byte it
    /// drains: those of the terminal guards its key extent meets.
    fn cheapest_l1_guard_to_drain(&self) -> Option<usize> {
        let guards = &self.levels[L1].guards;
        let terminal = &self.levels[TERMINAL];
        let target = self.guard_target_bytes(TERMINAL);
        let cost = |g: &LevelGuard| {
            let (lo, hi) = g.key_extent();
            let run = terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
            let rewritten: u64 = terminal.guards[run]
                .iter()
                .filter(|t| t.overlaps(lo, hi))
                // A fold into a dehydrated guard evicts what it folds.
                .map(|t| if t.dehydrated() { target } else { t.bytes() })
                .sum();
            (u128::from(rewritten), u128::from(g.bytes()))
        };
        (guards.iter().map(cost).enumerate())
            .min_by(|(_, (ra, da)), (_, (rb, db))| (ra * db).cmp(&(rb * da)))
            .map(|(gi, _)| gi)
    }

    /// The keys an L0 fold routes into: L1's guard keys. A guard key is a whole
    /// OPK key, compared as one byte string, so the partition is exact at every
    /// PK width.
    ///
    /// L0 rows above everything L1 holds get a guard of their own once they are
    /// worth half of one, so an ascending stream lands in fresh guards and not on
    /// the last one, which it would only push over target. With L1 empty every
    /// L0 row is above it, and L0 gets one guard keyed at its least key whatever
    /// its size.
    fn l1_guard_keys(&self) -> Vec<PkBuf> {
        let guards = &self.levels[L1].guards;
        let top = guards.last().map(|g| g.key_extent().1.max(g.guard_key));
        let (mut above, mut rows, mut fence) = (0, 0, None::<PkBuf>);
        for e in self.sealed_l0() {
            let n = e.shard.row_count();
            let first = top.map_or(0, |t| e.shard.pk_rows(t.pk_bytes()).end);
            rows += n;
            if first < n {
                above += n - first;
                let key = e.shard.pk_at(first);
                fence = Some(fence.map_or(key, |f| f.min(key)));
            }
        }
        // Row share stands in for byte share.
        let above_bytes = u128::from(self.l0_bytes()) * above as u128 / rows.max(1) as u128;
        let fence = fence.filter(|_| guards.is_empty() || above_bytes >= u128::from(self.guard_target_bytes(L1) / 2));
        guards.iter().map(|g| g.guard_key).chain(fence).collect()
    }

    /// The fold `guard` of `level_idx` owes when it is over its byte target, or
    /// its retractions are expected to cancel [`CANCEL_PERCENT`] of its rows —
    /// cut at its own key quantiles, or folded to one shard in place — or else
    /// over its level's file threshold, when its newest shards fold into one:
    /// the fewest that bring it back to the threshold, then each older one no
    /// larger than the shards already taken — at a threshold of 1, the whole
    /// guard. A shard is rewritten once the ones above it have grown to its
    /// size, so a byte is rewritten a logarithmic number of times over the
    /// guard's growth; folding the whole guard each time would rewrite its
    /// oldest shard at every fold.
    fn split_of(&self, level_idx: usize, guard: &LevelGuard) -> Option<Fold> {
        let target = self.guard_target_bytes(level_idx);
        let keys = fold_destinations(guard.guard_key, guard.entries.iter(), target);
        let whole = keys.len() > 1 || {
            let (rows, retractions) = rows_and_retractions(guard.entries.iter().map(|e| &*e.shard));
            self.cancels(retractions, rows)
        };
        let mut start = 0;
        if !whole {
            if guard.entries.len() <= GUARD_FILE_THRESHOLD[level_idx] {
                return None;
            }
            let bytes = |e: &ShardEntry| e.shard.file_len();
            start = GUARD_FILE_THRESHOLD[level_idx] - 1;
            let mut taken: u64 = guard.entries[start..].iter().map(bytes).sum();
            while start > 0 && bytes(&guard.entries[start - 1]) <= taken {
                start -= 1;
                taken += bytes(&guard.entries[start]);
            }
        }
        let kind = if level_idx == TERMINAL && guard.entries.len() > 1 {
            CompactionKind::Vertical
        } else if start == 0 {
            CompactionKind::GuardSplit
        } else {
            CompactionKind::TierFold
        };
        Some(Fold::new(kind, level_idx, keys, &guard.entries[start..]))
    }

    /// The highest underfull run of `level_idx`, folded into its lowest key, so
    /// the guard count follows the level's bytes down as well as up — the
    /// capacity sweep can shrink a guard by an order of magnitude in one rewrite.
    ///
    /// The runs are the maximal runs of two or more adjacent guards whose
    /// combined bytes fit half the target. A run also breaks at a change of
    /// representation, because folding a hydrated guard together with a
    /// dehydrated one would evict it (see [`GuardMerge::begin`]).
    ///
    /// Half the target is the hysteresis: a merged run no larger than its inputs
    /// is under the split trigger.
    fn plan_merge(&self, level_idx: usize) -> Option<Fold> {
        let bound = self.guard_target_bytes(level_idx) / 2;
        let guards = &self.levels[level_idx].guards;
        let (mut start, mut acc, mut highest) = (0usize, 0u64, None);
        for (i, g) in guards.iter().enumerate() {
            let bytes = g.bytes();
            if i > start && (acc + bytes > bound || g.dehydrated() != guards[start].dehydrated()) {
                if i - start > 1 {
                    highest = Some(start..i);
                }
                (start, acc) = (i, 0);
            }
            acc += bytes;
        }
        if guards.len() - start > 1 {
            highest = Some(start..guards.len());
        }
        let guards = &guards[highest?];
        Some(Fold::new(
            CompactionKind::GuardMerge,
            level_idx,
            vec![guards[0].guard_key],
            guards.iter().flat_map(|g| &g.entries),
        ))
    }

    /// The sweep's first dehydration of a hydrated terminal guard: one skeleton
    /// shard in place, never split — the output is one row per key, so a part
    /// count derived from the hydrated input would shatter it.
    fn plan_dehydration(&self, guard_idx: usize) -> Fold {
        let guard = &self.levels[TERMINAL].guards[guard_idx];
        Fold::new(
            CompactionKind::Dehydrate,
            TERMINAL,
            vec![guard.guard_key],
            &guard.entries,
        )
    }

    /// Start L1 guard `src` on its way down into the terminal level, banded so
    /// no single merge reads more than one band plus one terminal guard. A
    /// guard whose key extent meets one terminal guard at most is a band
    /// already: it sinks into the terminal level as it stands, and the answer
    /// is `None`. Any other is cut at the low end of its key extent and at every
    /// terminal guard key inside the extent, and each band sinks into the
    /// terminal guard it meets.
    ///
    /// The split pass then folds each guard a band sank into as a fold of its
    /// own, so a failure in one leaves that band registered on top of its guard
    /// and every intermediate state a valid partition.
    fn plan_drain(&mut self, src: usize) -> Option<Fold> {
        let guard = &self.levels[L1].guards[src];
        let (lo, hi) = guard.key_extent();
        let terminal = &self.levels[TERMINAL];
        let run = terminal.find_guards_for_range(lo.pk_bytes(), hi.pk_bytes());
        if run.len() <= 1 {
            let band = self.levels[L1].guards.remove(src);
            self.sink(band.entries);
            return None;
        }
        // `skip(1)`: the run's first guard owns `lo`, and every later key is above it.
        let keys = std::iter::once(lo)
            .chain(terminal.guards[run].iter().skip(1).map(|g| g.guard_key))
            .collect();
        Some(Fold::new(CompactionKind::BandCut, TERMINAL, keys, &guard.entries))
    }

    /// Sum of every registered shard's file size — the quantity a
    /// capacity-bounded store is held under. The published superseded files
    /// awaiting the barrier's drain are on disk but not counted.
    fn resident_bytes(&self) -> u64 {
        self.all_entries().map(|e| e.shard.file_len()).sum()
    }
}

/// [`ShardIndex::holding`]'s walk: a loop of its own rather than an adaptor
/// chain, which the per-key probes do not inline.
pub(super) struct Holding<'s, 'k> {
    /// The tier being walked, oldest first; its newest unwalked entry is last.
    /// L0, pending shards included, is the first.
    entries: &'s [ShardEntry],
    /// The levels below, each routed once the walk reaches it.
    levels: &'s [FLSMLevel],
    key: &'k [u8],
    filter_key: u64,
}

impl<'s> Iterator for Holding<'s, '_> {
    type Item = (&'s Rc<MappedShard>, Range<usize>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while let Some((e, rest)) = self.entries.split_last() {
                self.entries = rest;
                if pk_ranges_overlap(e.pk_min.pk_bytes(), e.pk_max.pk_bytes(), self.key, self.key)
                    && e.shard.pk_filter_may_contain(self.filter_key)
                {
                    let rows = e.shard.pk_rows(self.key);
                    if !rows.is_empty() {
                        return Some((&e.shard, rows));
                    }
                }
            }
            let (level, rest) = self.levels.split_first()?;
            self.levels = rest;
            self.entries = level.guards.get(level.slot(self.key)).map_or(&[], |g| &g.entries);
        }
    }
}

#[cfg(test)]
#[path = "tests/shard_index.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/shard_index.rs"]
mod bench;
