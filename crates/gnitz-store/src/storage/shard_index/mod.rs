//! FLSM Shard Index: the tree a store's shards form — L0, levels of guards and
//! the shard entries in them, the compaction policy's constants and its learned
//! cancel yield — opened from the shard set of its directory's manifest. The
//! writer, probes and upkeep over the tree are [`index`]'s; the files, and the
//! publish that makes a tree durable, are the [`StoreDir`]'s.

use std::ops::Range;
use std::rc::Rc;

use super::manifest::{ManifestEntry, RecoverySource, ShardSet, StoreDir};
use super::pk_ranges_overlap;
use gnitz_trace::runs::StorageError;
use gnitz_trace::runs::{MappedShard, guard_slot};
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;
use gnitz_zset::repr::Codebook;
use gnitz_zset::schema::SchemaDescriptor;

mod index;

/// Which trigger a compaction is serving. Only [`Dehydrate`](Self::Dehydrate)
/// changes what is written. The index learns its run size from an
/// [`L0Fold`](Self::L0Fold) and its cancel yield from a
/// [`GuardSplit`](Self::GuardSplit), the one fold that reads a whole guard in
/// place; the rest only label the byte accounting of `ShardIndex::stats`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum CompactionKind {
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

#[cfg(test)]
impl ShardIndex {
    /// The tree's shape as counts: L0 shards, then L1's and the terminal level's
    /// guards.
    pub(crate) fn level_shape(&self) -> (usize, [usize; 2]) {
        (self.l0.len(), [L1, TERMINAL].map(|l| self.levels[l].guards.len()))
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

pub(super) struct ShardEntry {
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

    /// This shard's rows of OPK `key` (exactly `pk_stride` wide), where it has
    /// any. `filter_key` is `probe_key(key)` — the caller hoists it because it is
    /// the same value for every shard in one sweep.
    #[inline]
    fn probe_pk_bytes(&self, key: &[u8], filter_key: u64) -> Option<Range<usize>> {
        if !pk_ranges_overlap(self.pk_min.pk_bytes(), self.pk_max.pk_bytes(), key, key) {
            return None;
        }
        if !self.shard.pk_filter_may_contain(filter_key) {
            return None;
        }
        Some(self.shard.pk_rows(key)).filter(|rows| !rows.is_empty())
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

    /// Total rows of this guard's entries, the ones that cancel included.
    fn rows(&self) -> usize {
        self.entries.iter().map(|e| e.shard.row_count()).sum()
    }

    /// The retractions a fold of this guard alone can cancel: those of every
    /// shard but the oldest, which has no older one here to retract from.
    fn retractions(&self) -> usize {
        self.entries.iter().skip(1).map(|e| e.shard.retraction_rows()).sum()
    }

    /// Whether this guard's entries can hold a key of `[lo, hi]`.
    fn overlaps(&self, lo: PkBuf, hi: PkBuf) -> bool {
        let (min, max) = self.key_extent();
        min <= hi && max >= lo
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
        // A lone guard owns the whole key line, whatever its key.
        if self.guards.len() <= 1 {
            return 0;
        }
        guard_slot(&self.guards, key, |g| g.guard_key.pk_bytes())
    }

    /// The guards overlapping `[range_min, range_max]`: a contiguous run,
    /// because guards partition the key line.
    fn find_guards_for_range(&self, range_min: &[u8], range_max: &[u8]) -> std::ops::Range<usize> {
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

    /// L0: spills as they arrive, oldest first, whose key spans overlap. Folded
    /// down into L1 whole, never in place.
    l0: Vec<ShardEntry>,
    levels: [FLSMLevel; LEVELS],
    /// Shards a flush wrote of rows above the store's cut: read by every reader
    /// but one at the cut, compacted with nothing, and entered into L0 by
    /// [`Self::seal_pending`]. A reopen finds them in L0.
    pending: Vec<ShardEntry>,

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
    running: Option<index::Running>,
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
            levels: Default::default(),
            pending: Vec::new(),
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
    /// each guarded level below it. Pending shards go in at level 0, so a
    /// reopen finds them in L0.
    pub(super) fn shard_set(&self) -> ShardSet {
        let entry = |level, guard_key, e: &ShardEntry| ManifestEntry {
            seq: e.seq,
            newest: e.newest,
            level,
            guard_key,
        };
        let whole = PkBuf::zeroed(self.schema.pk_stride());
        let l0 = (self.l0.iter()).chain(&self.pending).map(|e| entry(0, whole, e));
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
            let mut entries: Vec<&mut ShardEntry> = self.all_entries_mut().collect();
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
    pub(crate) fn verify_shards(&self) -> Result<(), StorageError> {
        self.all_entries().try_for_each(|e| e.shard.verify_body())
    }
}
