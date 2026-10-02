//! FLSM Shard Index: the one shard writer — shard lifecycle, the PK filter
//! policy, compaction ([`index`]), and the shard set a manifest publishes.

use std::collections::HashSet;
use std::rc::Rc;

use super::manifest::{self, ManifestEntry, ShardSet};
use gnitz_expr::RowSource;
use gnitz_zset::repr::StorageError;
use gnitz_zset::repr::{guard_slot, pk_group_end, MappedShard};
use gnitz_zset::schema::key::PkBuf;
use gnitz_zset::schema::key::{pk_bytes_eq, pk_in_range};
use gnitz_zset::schema::SchemaDescriptor;

mod index;

/// Which trigger a compaction is serving. Only [`Dehydrate`](Self::Dehydrate)
/// changes what is written. The index learns its run size from an
/// [`L0Fold`](Self::L0Fold) and its cancel yield from a
/// [`GuardSplit`](Self::GuardSplit); the rest label the byte accounting the
/// amplification benchmark reads.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum CompactionKind {
    L0Fold,
    GuardSplit,
    /// An L1 guard cut into bands before a vertical.
    BandCut,
    Vertical,
    GuardMerge,
    Dehydrate,
}

/// Per-trigger byte accounting over [`ShardIndex::compact`], for the
/// amplification benchmark.
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
    /// One line of tree shape for the amplification bench: the observed `R`, the
    /// target it derives, and each level's bytes, guards and shards.
    pub(super) fn tree_report(&self) -> String {
        let levels: Vec<String> = self
            .levels
            .iter()
            .enumerate()
            .map(|(li, l)| format!("L{li}={}B/{}g/{}f", l.bytes(), l.guards.len(), l.entries().count()))
            .collect();
        format!(
            "R={} l1_target={} {}",
            self.l0_run_bytes,
            self.l1_target_bytes(),
            levels.join(" ")
        )
    }

    /// The tree's shape as counts: L0 shards, then L1's and the terminal level's
    /// guards. What a test asserts a placement against, where `tree_report` is
    /// for reading.
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
pub(super) const L0: usize = 0;
/// Guards of overlapping shards, each folded in place past
/// [`GUARD_FILE_THRESHOLD`] and drained into the terminal level.
pub(super) const L1: usize = 1;
/// The deepest level: one shard per guard, and the only one a sweep dehydrates.
pub(super) const TERMINAL: usize = 2;
/// L0 shards past this count trigger the fold into L1.
pub(super) const L0_COMPACT_THRESHOLD: usize = 4;
/// Files one guard holds before it folds.
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
/// the terminal guard target, and so the granularity `enforce_capacity` evicts
/// at; L1 may park two, leaving the sweep the rest.
const SWEEP_STEPS: u64 = 8;
/// Keys sampled per guard when deriving its split points.
const SPLIT_SAMPLES: usize = 1024;
/// Most destination buckets one fold may write — one output shard each.
const MAX_PARTS: u64 = 64;

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
    pub(crate) fn open(dir: &str, seq: u64, schema: &SchemaDescriptor, newest: u64) -> Result<Self, StorageError> {
        let shard = Rc::new(MappedShard::open(&manifest::shard_path(dir, seq), schema)?);
        let pk_min = PkBuf::from_bytes(shard.get_pk_bytes(0));
        let pk_max = PkBuf::from_bytes(shard.get_pk_bytes(shard.row_count() - 1));
        Ok(ShardEntry { shard, seq, newest, pk_min, pk_max })
    }

    /// The first row above `key`, or the row count when there is none.
    fn first_row_above(&self, key: &PkBuf) -> usize {
        let row = self.shard.find_lower_bound_bytes(key.pk_bytes());
        match row < self.shard.row_count() && pk_bytes_eq(self.shard.get_pk_bytes(row), key.pk_bytes()) {
            true => pk_group_end(&*self.shard, row),
            false => row,
        }
    }

    /// The row this shard's matches of OPK `key` (exactly `pk_stride` wide) start
    /// at. `filter_key` is `probe_key(key)` — the caller hoists it because it is
    /// the same value for every shard in one sweep.
    fn probe_pk_bytes(&self, key: &[u8], filter_key: u64) -> Option<usize> {
        if !pk_in_range(self.pk_min.pk_bytes(), self.pk_max.pk_bytes(), key) {
            return None;
        }
        if !self.shard.shard_filter_may_contain(filter_key) {
            return None;
        }
        let idx = self.shard.find_lower_bound_bytes(key);
        (idx < self.shard.row_count() && pk_bytes_eq(self.shard.get_pk_bytes(idx), key)).then_some(idx)
    }
}

struct LevelGuard {
    guard_key: PkBuf,
    entries: Vec<ShardEntry>,
}

impl LevelGuard {
    /// Whether this guard holds skeleton shards — the state a capacity sweep
    /// leaves behind, read off the shard headers. Only a terminal guard is ever
    /// dehydrated, and it holds one shard.
    fn dehydrated(&self) -> bool {
        self.entries.iter().all(|e| e.shard.is_skeleton())
    }

    /// When this guard was last written, as the newest stamp over its entries.
    /// The only recency signal the tree carries; the capacity sweep orders its
    /// victims by it.
    fn newest(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| e.newest)
            .max()
            .expect("a guard holds at least one shard")
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
    /// instead — see [`ShardIndex::vertical_fold`].
    fn key_extent(&self) -> (PkBuf, PkBuf) {
        let lo = self.entries.iter().map(|e| e.pk_min).min();
        let hi = self.entries.iter().map(|e| e.pk_max).max();
        lo.zip(hi).expect("a guard holds at least one shard")
    }
}

/// The destination keys a fold of `entries` into the guard at `key` routes into
/// — sorted and distinct, as `merge_and_route` requires. `key` alone until the
/// entries hold more than `target` bytes, then row quantiles of a bounded sample
/// as well.
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
    let lo = entries.clone().map(|e| e.pk_min).min();
    let hi = entries.clone().map(|e| e.pk_max).max();
    if parts >= 2 && lo < hi {
        // About `SPLIT_SAMPLES` row keys: each shard is sorted and its PK region
        // fixed-stride, so a key is one O(1) read.
        let rows: usize = entries.clone().map(|e| e.shard.row_count()).sum();
        let step = (rows / SPLIT_SAMPLES).max(1);
        let mut sample: Vec<PkBuf> = entries
            .flat_map(|e| {
                (0..e.shard.row_count())
                    .step_by(step)
                    .map(|r| PkBuf::from_bytes(e.shard.get_pk_bytes(r)))
            })
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

    /// The guards overlapping `[range_min, range_max]` — a contiguous run,
    /// because guards partition the key line, so callers may `drain` it.
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

/// What bounds this store's registered shard bytes, and what a sweep does to
/// its victim.
#[derive(Clone, Copy)]
pub(crate) enum ShardBudget {
    /// Every store but a capacity-bounded view's output store and a delta store.
    Unbounded,
    /// `CREATE VIEW … WITH (capacity = …)`: evict by leaving skeleton rows behind.
    Dehydrate(u64),
    /// A view's delta store: evict by unlinking the guard outright — its rows are
    /// the change itself, so there is no summed weight worth a stub of.
    Drop(u64),
}

impl ShardBudget {
    /// The ceiling, for the targets that read the number and not the eviction it
    /// implies.
    fn cap(self) -> Option<u64> {
        match self {
            ShardBudget::Unbounded => None,
            ShardBudget::Dehydrate(cap) | ShardBudget::Drop(cap) => Some(cap),
        }
    }
}

pub(super) struct ShardIndex {
    pub(super) output_dir: String,
    pub schema: SchemaDescriptor,

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
    /// `R`, the unit every byte target is stated in: the running max of the
    /// registered L0 bytes one `run_compact` consumed, of one shard its fold
    /// wrote and of one terminal run, floored at [`MIN_GUARD_BYTES`] and
    /// persisted in the manifest header.
    l0_run_bytes: u64,
    /// What bounds this store's registered on-disk shard bytes, and how a sweep
    /// evicts. Unbounded for every store but a capacity-bounded view's output
    /// store and a view's delta store.
    budget: ShardBudget,
    /// The highest key any drop removed. Zero until the first drop.
    dropped_max: PkBuf,
    /// Passed to every shard write.
    skip_pk_filter: bool,
    /// What the in-place guard folds so far cancelled per retraction they read.
    cancel_yield: CancelYield,
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
    /// Open the store at `output_dir` holding `shards`, unlinking the staging file
    /// and every shard `shards` does not name. `skip_pk_filter`: nothing
    /// probes this store by PK.
    pub(super) fn open(
        output_dir: &str,
        schema: SchemaDescriptor,
        budget: ShardBudget,
        skip_pk_filter: bool,
        shards: &ShardSet,
    ) -> Result<Self, StorageError> {
        let mut idx = ShardIndex {
            output_dir: output_dir.to_string(),
            schema,
            levels: Default::default(),
            pending: Vec::new(),
            shard_seq: 0,
            published_through: 0,
            retired: Vec::new(),
            l0_run_bytes: shards.run_bytes.max(MIN_GUARD_BYTES),
            budget,
            dropped_max: PkBuf::zeroed(schema.pk_stride()),
            skip_pk_filter,
            cancel_yield: CancelYield::FRESH,
        };
        for e in &shards.entries {
            let entry = ShardEntry::open(output_dir, e.seq, &idx.schema, e.newest)?;
            let level = idx
                .levels
                .get_mut(e.level as usize)
                .ok_or(StorageError::Corrupt("manifest level"))?;
            level.get_or_create_guard(e.guard_key).entries.push(entry);
        }
        idx.shard_seq = idx.all_entries().map(|e| e.seq).max().unwrap_or(0);
        idx.published_through = idx.shard_seq;
        let live: HashSet<u64> = idx.all_entries().map(|e| e.seq).collect();
        manifest::remove_stale_files(output_dir, |seq| live.contains(&seq))?;
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

    /// The highest key any drop removed; see [`Self::drop_guard`].
    pub(super) fn dropped_max(&self) -> PkBuf {
        self.dropped_max
    }

    /// Publish `schema`, rebinding every registered shard to it. A failure on any
    /// shard leaves the index on the old schema.
    pub(super) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        if schema != self.schema {
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
