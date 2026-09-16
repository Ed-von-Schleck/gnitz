//! FLSM Shard Index: manages shard lifecycle, compaction, and manifest I/O.
//!
//! Split into the in-memory index + compaction trigger ([`index`]) and the
//! manifest serialize/load/recover path ([`persist`]). The shared types
//! (`ShardEntry`, `LevelGuard`, `FLSMLevel`, `ShardIndex`), the guard-routing
//! helper, the level constants, and the constructor live here so both
//! sub-modules read the (private) fields and helpers directly.

use std::rc::Rc;

use super::columnar::ColumnarSource;
use super::error::StorageError;
use super::shard_reader::MappedShard;
use crate::schema::key::PkBuf;
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq, pk_in_range};
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;

mod index;
mod persist;

/// Which trigger a compaction is serving. Only [`Dehydrate`](Self::Dehydrate)
/// changes what is written; the rest buckets the byte accounting the
/// amplification benchmark reads.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CompactionKind {
    L0Fold,
    GuardSplit,
    Vertical,
    GuardMerge,
    Dehydrate,
}

impl CompactionKind {
    /// This phase's slot in [`cstats`] — exhaustive, so a new variant fails to
    /// compile here rather than indexing past `PHASE_NAMES`.
    #[cfg(test)]
    fn slot(self) -> usize {
        match self {
            Self::L0Fold => 0,
            Self::GuardSplit => 1,
            Self::Vertical => 2,
            Self::GuardMerge => 3,
            Self::Dehydrate => 4,
        }
    }
}

/// Per-phase byte accounting over [`ShardIndex::compact_into`], for the
/// amplification benchmark — which states its acceptances in bytes because
/// wall-clock on the development machine varies 3.6x on identical compaction
/// work. Outside a test build `record` does nothing.
pub(crate) mod cstats {
    #[cfg(not(test))]
    #[inline]
    pub(super) fn record(_kind: super::CompactionKind, _inb: u64, _outb: u64, _inf: usize) {}

    #[cfg(test)]
    use std::cell::RefCell;

    #[cfg(test)]
    #[derive(Default, Clone)]
    pub(crate) struct Phase {
        pub(crate) n: usize,
        pub(crate) in_bytes: u64,
        pub(crate) max_in: u64,
        pub(crate) out_bytes: u64,
        pub(crate) in_files: usize,
    }

    #[cfg(test)]
    pub(crate) const PHASE_NAMES: [&str; 5] = ["l0_fold", "guard_split", "vertical", "guard_merge", "dehydrate"];

    #[cfg(test)]
    thread_local! {
        static STATS: RefCell<[Phase; PHASE_NAMES.len()]> =
            RefCell::new(std::array::from_fn(|_| Phase::default()));
    }

    #[cfg(test)]
    pub(crate) fn record(kind: super::CompactionKind, inb: u64, outb: u64, inf: usize) {
        STATS.with(|ph| {
            let p = &mut ph.borrow_mut()[kind.slot()];
            p.n += 1;
            p.in_bytes += inb;
            p.max_in = p.max_in.max(inb);
            p.out_bytes += outb;
            p.in_files += inf;
        });
    }

    #[cfg(test)]
    pub(crate) fn reset() {
        STATS.with(|ph| *ph.borrow_mut() = std::array::from_fn(|_| Phase::default()));
    }

    #[cfg(test)]
    pub(crate) fn dump() -> [Phase; PHASE_NAMES.len()] {
        STATS.with(|ph| ph.borrow().clone())
    }
}

#[cfg(test)]
impl ShardIndex {
    /// One line of tree shape for the amplification bench: the observed `R`, the
    /// target it derives, and each level's bytes and guard count.
    pub(super) fn tree_report(&self) -> String {
        let levels: Vec<String> = (0..FLSM_LEVELS)
            .map(|li| {
                format!(
                    "L{}={}B/{}g",
                    Self::level_num(li),
                    self.levels[li].bytes(),
                    self.levels[li].guards.len()
                )
            })
            .collect();
        format!(
            "R={} l1_target={} L0={}f {}",
            self.l0_run_bytes,
            self.l1_target_bytes(),
            self.l0.len(),
            levels.join(" ")
        )
    }

    /// The tree's shape as counts: L0 shards, then each level's guards. What a
    /// test asserts a placement against, where `tree_report` is for reading.
    pub(crate) fn level_shape(&self) -> (usize, [usize; FLSM_LEVELS]) {
        (self.l0.len(), std::array::from_fn(|i| self.levels[i].guards.len()))
    }
}

/// Guarded levels below L0 — L1 and L2.
pub(super) const FLSM_LEVELS: usize = 2;
/// Index of the deepest guarded level (L2). The one level whose guards fold to a
/// single file, and the only one whose guards may be dehydrated.
pub(super) const TERMINAL_LEVEL_IDX: usize = FLSM_LEVELS - 1;
/// L0 shards past this count trigger the fold into L1.
pub(super) const L0_COMPACT_THRESHOLD: usize = 4;
/// Files one guard holds before it folds.
const GUARD_FILE_THRESHOLD: usize = 4;
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

/// Slot owning `key` in a sorted guard list: the last guard `≤ key`, saturating
/// to slot 0 for keys below the first guard. `compact::merge_and_route` derives
/// the same slots independently, and `compact`'s differential oracle checks the
/// two — which is why this stays a named function.
pub(super) fn guard_slot<T>(guards: &[T], key: &[u8], gk: impl Fn(&T) -> &[u8]) -> usize {
    guards
        .partition_point(|g| compare_pk_ordering(gk(g), key).is_le())
        .saturating_sub(1)
}

/// One compaction's input set, as [`ShardIndex::compaction_inputs`] gathers it.
#[derive(Default)]
pub(super) struct CompactionInputs {
    files: Vec<String>,
    max_lsn: u64,
    /// Registered bytes of the inputs — the amplification bench's read side,
    /// taken from the entries rather than re-`stat`ed off the paths.
    bytes: u64,
}

pub(super) struct ShardEntry {
    shard: Rc<MappedShard>,
    filename: String,
    max_lsn: u64,
    pk_min: PkBuf,
    pk_max: PkBuf,
    /// Named by a published manifest, which the barrier fdatasyncs a file before
    /// — so an unpublished file is exactly one that owes the sweep.
    published: bool,
}

impl ShardEntry {
    /// The one mapping call: [`open`](Self::open) builds a fresh entry around it,
    /// [`ShardIndex::swap_schema`] swaps it into an existing one.
    fn map(path: &str, schema: &SchemaDescriptor) -> Result<Rc<MappedShard>, StorageError> {
        Ok(Rc::new(MappedShard::open(&super::super::cstr(path)?, schema, false)?))
    }

    pub(crate) fn open(
        path: &str,
        schema: &SchemaDescriptor,
        max_lsn: u64,
        published: bool,
    ) -> Result<Self, StorageError> {
        let shard = Self::map(path, schema)?;
        // Every writer skips an empty output, so a zero-row file is damage.
        if shard.count == 0 {
            return Err(StorageError::InvalidShard);
        }
        let pk_min = PkBuf::from_bytes(shard.get_pk_bytes(0));
        let pk_max = PkBuf::from_bytes(shard.get_pk_bytes(shard.count - 1));
        Ok(ShardEntry {
            shard,
            filename: path.to_string(),
            max_lsn,
            pk_min,
            pk_max,
            published,
        })
    }

    /// Probe this shard for a PK by its OPK `key` bytes (exactly `pk_stride`
    /// wide). `filter_key` is `probe_key(key)` — the caller hoists it because
    /// it is the same value for every shard in one sweep.
    fn probe_pk_bytes(&self, key: &[u8], filter_key: u64) -> Option<(Rc<MappedShard>, usize)> {
        if !pk_in_range(self.pk_min.pk_bytes(), self.pk_max.pk_bytes(), key) {
            return None;
        }
        if !self.shard.shard_filter_may_contain(filter_key) {
            return None;
        }
        let idx = self.shard.find_lower_bound_bytes(key);
        if idx < self.shard.count && pk_bytes_eq(self.shard.get_pk_bytes(idx), key) {
            return Some((Rc::clone(&self.shard), idx));
        }
        None
    }
}

struct LevelGuard {
    guard_key: PkBuf,
    entries: Vec<ShardEntry>,
}

impl LevelGuard {
    pub(crate) fn new(gk: PkBuf) -> Self {
        LevelGuard { guard_key: gk, entries: Vec::new() }
    }

    /// Whether this guard holds skeleton shards — the state a capacity sweep
    /// leaves behind, read off the shard headers. Only a terminal guard is ever
    /// dehydrated, and it holds one shard.
    fn dehydrated(&self) -> bool {
        self.entries.iter().all(|e| e.shard.is_skeleton())
    }

    /// When this guard was last written, as the newest LSN over its entries. The
    /// only recency signal the tree carries; the capacity sweep orders its
    /// victims by it.
    fn newest_lsn(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| e.max_lsn)
            .max()
            .expect("a guard holds at least one shard")
    }

    /// Total registered bytes of this guard's entries.
    fn bytes(&self) -> u64 {
        self.entries.iter().map(|e| e.shard.file_len()).sum()
    }

    /// The destination keys a fold of this guard routes into — sorted and
    /// distinct, as `merge_and_route` requires. Its own key alone until it holds
    /// more than `target` bytes, then row quantiles of a bounded sample as well.
    ///
    /// Row quantiles stand in for byte quantiles: a skewed sample only makes the
    /// split uneven, and the oversized half crosses the target again next fold.
    /// A quantile below `guard_key` arises only for guard 0, which owns the tail
    /// below its own key; minting a guard down there is what makes that tail
    /// addressable.
    ///
    /// A guard holding one distinct key cannot be cut, and says so from its
    /// extent rather than from a sample — it stays over target forever, so a
    /// sampling pass would repeat on it indefinitely. That is the one limit of
    /// `target` as a bound, and it is inherent: a guard boundary *is* a key, so
    /// rows sharing a PK cannot be split across one.
    fn fold_destinations(&self, target: u64) -> Vec<PkBuf> {
        let mut keys = vec![self.guard_key];
        let parts = self.bytes().div_ceil(target).min(MAX_PARTS) as usize;
        let (lo, hi) = self.key_extent();
        if parts >= 2 && lo < hi {
            let sample = self.sample_keys();
            let parts = parts.min(sample.len());
            keys.extend((1..parts).map(|i| sample[i * sample.len() / parts]));
            keys.sort_unstable();
            keys.dedup();
        }
        keys
    }

    /// About [`SPLIT_SAMPLES`] of this guard's row keys, sorted and deduped. Each
    /// shard is already sorted and its PK region is fixed-stride, so a key is one
    /// O(1) read.
    fn sample_keys(&self) -> Vec<PkBuf> {
        let rows: usize = self.entries.iter().map(|e| e.shard.count).sum();
        let step = (rows / SPLIT_SAMPLES).max(1);
        let mut sample: Vec<PkBuf> = Vec::with_capacity(SPLIT_SAMPLES + self.entries.len());
        for e in &self.entries {
            sample.extend(
                (0..e.shard.count)
                    .step_by(step)
                    .map(|r| PkBuf::from_bytes(e.shard.get_pk_bytes(r))),
            );
        }
        sample.sort_unstable();
        sample.dedup();
        sample
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

struct FLSMLevel {
    guards: Vec<LevelGuard>,
}

impl FLSMLevel {
    pub(crate) fn new() -> Self {
        FLSMLevel { guards: Vec::new() }
    }

    /// The guard owning `key`: the last one `≤ key`, or 0 below the first — and
    /// 0 on an empty level, which callers bound against `guards.len()`.
    fn slot(&self, key: &[u8]) -> usize {
        guard_slot(&self.guards, key, |g| g.guard_key.pk_bytes())
    }

    /// The guards overlapping `[range_min, range_max]` — a contiguous run,
    /// because guards partition the key line, so callers may `drain` it.
    fn find_guards_for_range(&self, range_min: &[u8], range_max: &[u8]) -> std::ops::Range<usize> {
        self.slot(range_min)..(self.slot(range_max) + 1).min(self.guards.len())
    }

    /// Total registered bytes of every guard in this level.
    fn bytes(&self) -> u64 {
        self.guards.iter().map(LevelGuard::bytes).sum()
    }

    fn get_or_create_guard(&mut self, gk: PkBuf) -> &mut LevelGuard {
        let pos = match self.guards.binary_search_by(|g| g.guard_key.cmp(&gk)) {
            Ok(pos) => pos,
            Err(pos) => {
                self.guards.insert(pos, LevelGuard::new(gk));
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
    pub(super) table_id: u32,
    pub(super) output_dir: String,
    pub schema: SchemaDescriptor,

    l0: Vec<ShardEntry>,
    levels: [FLSMLevel; FLSM_LEVELS],

    /// Source of every output shard's basename. Each output-emitting compaction
    /// draws a fresh value, and the manifest header carries it across a restart,
    /// so no two of this table's output shards ever share a basename. Unique, not
    /// dense: a compaction that fails after drawing one burns it.
    compact_seq: u64,
    pending_deletions: Vec<String>,
    /// `R`, the unit every byte target is stated in: the running max of the
    /// registered L0 bytes one `run_compact` consumed, floored at
    /// [`MIN_GUARD_BYTES`] and persisted in the manifest header.
    l0_run_bytes: u64,
    /// What bounds this store's registered on-disk shard bytes, and how a sweep
    /// evicts. Unbounded for every store but a capacity-bounded view's output
    /// store and a view's delta store.
    budget: ShardBudget,
    /// The highest key any drop removed. Zero until the first drop.
    dropped_max: PkBuf,
    /// Passed to every compaction's write. Held rather than derived from the
    /// input shards: a derivation would let one filterless input turn the filter
    /// off for this table's whole descendant line, permanently and invisibly.
    skip_pk_filter: bool,
}

impl ShardIndex {
    /// The 1-based level *number* of a 0-based tier index — used only by the two
    /// serde boundaries that carry it, the shard filename and the manifest field.
    pub(super) fn level_num(level_idx: usize) -> usize {
        level_idx + 1
    }

    /// `skip_pk_filter` declares that nothing point-probes this store by PK, so
    /// its shards need no PK filter.
    pub(super) fn new(
        table_id: u32,
        output_dir: &str,
        schema: SchemaDescriptor,
        budget: ShardBudget,
        skip_pk_filter: bool,
    ) -> Self {
        ShardIndex {
            table_id,
            output_dir: output_dir.to_string(),
            schema,
            l0: Vec::new(),
            levels: std::array::from_fn(|_| FLSMLevel::new()),
            compact_seq: 0,
            pending_deletions: Vec::new(),
            l0_run_bytes: MIN_GUARD_BYTES,
            budget,
            dropped_max: PkBuf::zeroed(schema.pk_stride()),
            skip_pk_filter,
        }
    }

    /// Whether this store's writes skip the PK filter. Every write reads it
    /// here — the spill path and compaction must agree, or a store's shards
    /// disagree about whether a probe can trust them.
    pub(super) fn skip_pk_filter(&self) -> bool {
        self.skip_pk_filter
    }

    /// Test helper: flip the filter off for a store that would otherwise build
    /// one, so a benchmark can price what the filter costs.
    #[cfg(test)]
    pub(super) fn set_skip_pk_filter_for_test(&mut self, skip: bool) {
        self.skip_pk_filter = skip;
    }

    /// The highest key any drop removed; see [`Self::drop_guard`].
    pub(super) fn dropped_max(&self) -> PkBuf {
        self.dropped_max
    }

    /// Publish `schema`, re-mapping every registered shard when its payload arity
    /// changed — a mapping's column regions are fixed at open. A failure on any
    /// file leaves the index on the old schema.
    pub(super) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        if schema.num_payload_cols() != self.schema.num_payload_cols() {
            let remapped: Vec<Rc<MappedShard>> = self
                .all_entries()
                .map(|e| ShardEntry::map(&e.filename, &schema))
                .collect::<Result<_, _>>()?;
            for (e, shard) in self.all_entries_mut().zip(remapped) {
                e.shard = shard;
            }
        }
        self.schema = schema;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/shard_index.rs"]
mod tests;
