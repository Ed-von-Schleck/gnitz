//! FLSM Shard Index: the tree a store's shards form — levels of guards and the
//! shard entries in them, the compaction policy's constants and its learned
//! cancel yield — and the manifest it is opened from and stages and publishes.
//! The writer, probes and upkeep over the tree are [`index`]'s.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::ops::Range;
use std::rc::Rc;

use super::manifest::{self, Manifest, ManifestEntry, ShardSet};
use super::pk_ranges_overlap;
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;
use gnitz_zset::repr::StorageError;
use gnitz_zset::repr::{guard_slot, Codebook, MappedShard};
use gnitz_zset::schema::SchemaDescriptor;

mod index;

/// Which trigger a compaction is serving. Only [`Dehydrate`](Self::Dehydrate)
/// changes what is written. The index learns its run size from an
/// [`L0Fold`](Self::L0Fold) and its cancel yield from a
/// [`GuardSplit`](Self::GuardSplit), the one fold that reads a whole guard in
/// place, and a [`BandCut`](Self::BandCut) leaves its outputs as the bands the
/// plan folds down next; the rest only label the byte accounting of [`cstats`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum CompactionKind {
    L0Fold,
    GuardSplit,
    /// The newest shards of a guard over the file threshold, folded into one.
    TierFold,
    /// An L1 guard cut into bands before a vertical.
    BandCut,
    Vertical,
    GuardMerge,
    Dehydrate,
}

/// Per-trigger byte accounting over the folds [`ShardIndex::maintain`] runs.
#[cfg(test)]
pub(crate) mod cstats {
    use super::CompactionKind;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Default, Clone)]
    pub(crate) struct Phase {
        pub(crate) n: usize,
        pub(crate) in_bytes: u64,
        pub(crate) max_in: u64,
        pub(crate) out_bytes: u64,
        pub(crate) in_files: usize,
    }

    thread_local! {
        static STATS: RefCell<BTreeMap<CompactionKind, Phase>> = const { RefCell::new(BTreeMap::new()) };
    }

    pub(super) fn record(kind: CompactionKind, inb: u64, outb: u64, inf: usize) {
        STATS.with(|stats| {
            let mut stats = stats.borrow_mut();
            let p = stats.entry(kind).or_default();
            p.n += 1;
            p.in_bytes += inb;
            p.max_in = p.max_in.max(inb);
            p.out_bytes += outb;
            p.in_files += inf;
        });
    }

    pub(crate) fn reset() {
        STATS.with(|stats| stats.borrow_mut().clear());
    }

    /// Every trigger that ran since the last [`reset`].
    pub(crate) fn dump() -> BTreeMap<CompactionKind, Phase> {
        STATS.with(|stats| stats.borrow().clone())
    }
}

#[cfg(test)]
impl ShardIndex {
    /// The tree's shape as counts: L0 shards, then L1's and the terminal level's
    /// guards.
    pub(crate) fn level_shape(&self) -> (usize, [usize; 2]) {
        (
            self.levels[L0].entries().count(),
            [L1, TERMINAL].map(|l| self.levels[l].guards.len()),
        )
    }
}

/// The tree's levels, top down. Every level is a guard partition of the key line.
const LEVELS: usize = 3;
/// Spills as they arrive: at most one guard, keyed zero and so owning every key,
/// whose shards overlap. Folded down into L1, never in place.
const L0: usize = 0;
/// Guards of overlapping shards, each folding its newest ones in place past
/// [`GUARD_FILE_THRESHOLD`] and drained into the terminal level.
const L1: usize = 1;
/// The deepest level: one shard per guard, and the only one a sweep dehydrates.
const TERMINAL: usize = 2;
/// L0 shards past this count trigger the fold into L1.
pub(super) const L0_COMPACT_THRESHOLD: usize = 4;
/// Files one guard holds at rest.
const GUARD_FILE_THRESHOLD: usize = 4;
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
    /// Open the shard drawn at `seq` in the store at `dir`.
    fn open(dir: &str, seq: u64, schema: &SchemaDescriptor, newest: u64) -> Result<Self, StorageError> {
        let shard = Rc::new(MappedShard::open(&manifest::shard_path(dir, seq), schema)?);
        let (pk_min, pk_max) = (shard.pk_at(0), shard.pk_at(shard.row_count() - 1));
        Ok(ShardEntry { shard, seq, newest, pk_min, pk_max })
    }

    /// This shard's rows of OPK `key` (exactly `pk_stride` wide), where it has
    /// any. `filter_key` is `probe_key(key)` — the caller hoists it because it is
    /// the same value for every shard in one sweep.
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
fn fold_destinations<'a>(key: PkBuf, entries: impl Iterator<Item = &'a ShardEntry> + Clone, target: u64) -> Vec<PkBuf> {
    let mut keys = vec![key];
    let bytes: u64 = entries.clone().map(|e| e.shard.file_len()).sum();
    let parts = bytes.div_ceil(target).min(MAX_PARTS) as usize;
    if parts >= 2 && extent(entries.clone()).is_some_and(|(lo, hi)| lo < hi) {
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
    output_dir: String,
    pub(super) schema: SchemaDescriptor,

    levels: [FLSMLevel; LEVELS],
    /// Shards a flush wrote of rows above the store's cut: read by every reader
    /// but one at the cut, compacted with nothing, and entered into L0 by
    /// [`Self::seal_pending`]. A reopen finds them in L0.
    pending: Vec<ShardEntry>,

    /// The last seq drawn; a shard is named by its seq.
    shard_seq: u64,
    /// The last seq drawn when the most recently renamed manifest was built; see
    /// [`Self::published`].
    published_through: u64,
    /// Published shards the index has dropped, unlinked by
    /// [`Self::unlink_retired`].
    retired: Vec<u64>,
    /// The manifest in place: the one this open read, or the one this
    /// process's last barrier renamed. `None` for a store that has none, and
    /// from the staging of another until its barrier completes.
    manifest_in_place: Option<Manifest>,
    /// Whether a barrier of this process synced the directories behind
    /// `manifest_in_place`. The rename of a manifest read at open may predate a
    /// kill that came before them.
    manifest_synced: bool,
    /// The checkpoint mark of the manifest this open loaded.
    pub(super) loaded_mark: Option<u64>,
    /// What the next publish writes as the manifest's caller record.
    pub(super) caller_record: Vec<u8>,
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
    /// The L1 guards a band cut left to fold down, by guard key, the lowest
    /// last. While any is left nothing else reshapes L1 or the terminal level:
    /// the bands were cut against the terminal partition as it stood.
    bands: Vec<PkBuf>,
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

/// A publish [`ShardIndex::stage`] staged and [`ShardIndex::publish`] completes.
pub(super) struct FlushWork {
    /// The manifest staged for the rename. Without one the manifest in place
    /// is already current, and only the directory syncs are owed.
    staged: Option<Manifest>,
    /// Whether no publish of this store is known durable, so the entries that
    /// reach its directory are owed a sync beside the directory itself.
    first_sync: bool,
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
        // First, so an unusable directory fails the open rather than the first
        // flush. `created` is `mkdir`'s own verdict, never a `stat`'s: a store
        // read as freshly created skips its manifest, and so unlinks every
        // shard in the directory.
        let created = match fs::create_dir(output_dir) {
            Ok(()) => true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(output_dir)?;
                true
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(e.into()),
        };
        let rederived = matches!(recovery, RecoverySource::Rederive { .. });
        let loaded = match recovery {
            _ if created => None,
            // Erased at open, so it reads nothing: an I/O error on the file it is
            // about to unlink must not abort it.
            RecoverySource::Rederive { resume_at: None } => None,
            // Rebuilt from its sources, so damage is erased rather than fatal.
            RecoverySource::Rederive { resume_at: Some(want) } => manifest::read_at(output_dir, want)?,
            // Its shards are its only copy, so damage stops the open.
            RecoverySource::SalReplay => manifest::read(output_dir)?,
        };
        if loaded.is_none() && rederived && !created {
            // So a later re-open cannot reload what this one rejected.
            manifest::unlink(&[output_dir])?;
        }
        // A rederived store's shards carry no PK filter: nothing probes it by PK.
        let skip_pk_filter = rederived;
        let none = ShardSet::default();
        let shards = loaded.as_ref().map_or(&none, |m| &m.shards);
        let live: HashSet<u64> = shards.entries.iter().map(|e| e.seq).collect();
        let shard_seq = live.iter().copied().max().unwrap_or(0);
        let mut idx = ShardIndex {
            output_dir: output_dir.to_string(),
            schema,
            levels: Default::default(),
            pending: Vec::new(),
            shard_seq,
            published_through: shard_seq,
            retired: Vec::new(),
            manifest_in_place: None,
            manifest_synced: false,
            loaded_mark: loaded.as_ref().map(|m| m.checkpoint_mark),
            caller_record: loaded.as_ref().map(|m| m.caller_record.clone()).unwrap_or_default(),
            l0_run_bytes: shards.run_bytes.max(MIN_GUARD_BYTES),
            capacity,
            skip_pk_filter,
            compress,
            codebook: None,
            cancel_yield: CancelYield::FRESH,
            // A reopened tree is asked once whether it owes anything.
            owed: !shards.entries.is_empty(),
            upkeep_overdraft: 0,
            running: None,
            bands: Vec::new(),
        };
        for e in &shards.entries {
            let entry = ShardEntry::open(output_dir, e.seq, &idx.schema, e.newest)?;
            let level = idx
                .levels
                .get_mut(e.level as usize)
                .ok_or(StorageError::Corrupt("manifest level"))?;
            level.get_or_create_guard(e.guard_key).entries.push(entry);
        }
        if compress {
            let newest_coded = idx
                .all_entries()
                .filter(|e| e.shard.heap().plain().is_none())
                .max_by_key(|e| e.seq);
            idx.codebook = newest_coded.and_then(|e| Codebook::of_shard(&e.shard)).map(Rc::new);
        }
        manifest::remove_stale_files(output_dir, |seq| live.contains(&seq))?;
        idx.manifest_in_place = loaded;
        Ok(idx)
    }

    /// The shard set this index publishes.
    pub(super) fn shard_set(&self) -> ShardSet {
        let whole = PkBuf::zeroed(self.schema.pk_stride());
        let entries = (0u64..)
            .zip(&self.levels)
            .flat_map(|(level, l)| {
                l.guards.iter().flat_map(move |g| {
                    g.entries.iter().map(move |e| ManifestEntry {
                        seq: e.seq,
                        newest: e.newest,
                        level,
                        guard_key: g.guard_key,
                    })
                })
            })
            .chain(self.pending.iter().map(|e| ManifestEntry {
                seq: e.seq,
                newest: e.newest,
                level: L0 as u64,
                guard_key: whole,
            }))
            .collect();
        ShardSet { run_bytes: self.l0_run_bytes, entries }
    }

    /// Publish `schema`, rebinding every registered shard to it. A failure on any
    /// shard leaves the index on the old schema.
    pub(super) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        if schema != self.schema {
            self.abandon_fold();
            let rebound: Vec<Rc<MappedShard>> = self
                .all_entries()
                .map(|e| e.shard.rebind(&schema).map(Rc::new))
                .collect::<Result<_, _>>()?;
            for (e, shard) in self.all_entries_mut().zip(rebound) {
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
