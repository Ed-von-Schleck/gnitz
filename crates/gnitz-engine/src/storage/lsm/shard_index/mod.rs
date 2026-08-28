//! FLSM Shard Index: manages shard lifecycle, compaction, and manifest I/O.
//!
//! Split into the in-memory index + compaction trigger ([`index`]) and the
//! manifest serialize/load/recover path ([`persist`]). The shared types
//! (`ShardEntry`, `LevelGuard`, `FLSMLevel`, `ShardIndex`), the
//! range/cstring helpers, the level constants, and the constructor live here so
//! both sub-modules read the (private) fields and helpers directly.

use std::ffi::CString;
use std::rc::Rc;

use super::error::StorageError;
use super::shard_reader::MappedShard;
use crate::schema::key::PkBuf;
use crate::schema::key::{pk_bytes_eq, pk_in_range};
use crate::schema::SchemaDescriptor;

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
    pub(crate) use enabled::*;

    #[cfg(test)]
    mod enabled {
        use std::cell::RefCell;

        #[derive(Default, Clone)]
        pub(crate) struct Phase {
            pub(crate) n: usize,
            pub(crate) in_bytes: u64,
            pub(crate) max_in: u64,
            pub(crate) out_bytes: u64,
            pub(crate) in_files: usize,
        }

        pub(crate) const PHASE_NAMES: [&str; 5] = ["l0_fold", "guard_split", "vertical", "guard_merge", "dehydrate"];

        thread_local! {
            static STATS: RefCell<Vec<Phase>> = RefCell::new(vec![Phase::default(); PHASE_NAMES.len()]);
        }

        pub(crate) fn record(kind: crate::storage::lsm::shard_index::CompactionKind, inb: u64, outb: u64, inf: usize) {
            STATS.with(|ph| {
                let p = &mut ph.borrow_mut()[kind.slot()];
                p.n += 1;
                p.in_bytes += inb;
                p.max_in = p.max_in.max(inb);
                p.out_bytes += outb;
                p.in_files += inf;
            });
        }

        pub(crate) fn reset() {
            STATS.with(|ph| *ph.borrow_mut() = vec![Phase::default(); PHASE_NAMES.len()]);
        }

        pub(crate) fn dump() -> Vec<Phase> {
            STATS.with(|ph| ph.borrow().clone())
        }
    }
}

#[cfg(test)]
impl ShardIndex {
    /// One line of tree shape for the amplification bench: the observed `R`, the
    /// target it derives, and each level's bytes and guard count.
    pub(super) fn tree_report(&self) -> String {
        let levels: Vec<String> = (0..self.levels.len())
            .map(|li| {
                format!(
                    "L{}={}B/{}g",
                    Self::level_num(li),
                    self.level_bytes(li),
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
}

/// Serialized level bound: level numbers run 0 (L0) ..= `FLSM_LEVELS`, and
/// `load_manifest` rejects anything at or above `MAX_LEVELS`.
const MAX_LEVELS: usize = 3;
/// Guarded levels below L0 — L1 and L2.
const FLSM_LEVELS: usize = MAX_LEVELS - 1;
/// Index of the deepest guarded level (L2). The one level whose guards fold to a
/// single file, and the only one whose guards may be dehydrated.
const TERMINAL_LEVEL_IDX: usize = FLSM_LEVELS - 1;
/// L0 shards past this count trigger the fold into L1.
pub(super) const L0_COMPACT_THRESHOLD: usize = 4;
const GUARD_FILE_THRESHOLD: usize = 4;
const LMAX_FILE_THRESHOLD: usize = 1;
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

/// One compaction's input set, as [`ShardIndex::compaction_inputs`] gathers it.
#[derive(Default)]
pub(super) struct CompactionInputs {
    files: Vec<String>,
    max_lsn: u64,
    /// Registered bytes of the inputs — the amplification bench's read side,
    /// taken from the entries rather than re-`stat`ed off the paths.
    bytes: u64,
}

/// Path strings as `CString`s — the compaction input list (a `Vec<String>`) and
/// the barrier's by-path fdatasync sweep list (borrowed `&str`s off the live
/// entries) take the same conversion.
pub(super) fn to_cstrings<S: AsRef<str>>(paths: impl IntoIterator<Item = S>) -> Result<Vec<CString>, StorageError> {
    paths.into_iter().map(|f| super::super::cstr(f.as_ref())).collect()
}

pub(super) struct ShardEntry {
    shard: Rc<MappedShard>,
    filename: String,
    max_lsn: u64,
    pk_min: PkBuf,
    pk_max: PkBuf,
    /// Has an `fdatasync` reached this file? False for a shard this session
    /// wrote, true for one a published manifest named. Carried on the entry so a
    /// superseded shard takes its own flag out of the index with it — nothing
    /// can be left behind naming a deleted file.
    synced: bool,
}

impl ShardEntry {
    // An empty shard must fail every range check. A min > max sentinel cannot
    // express that under `compare_pk_bytes` (it holds only for unsigned
    // byte-lex), so probe/sort short-circuit on the row count instead.
    #[inline]
    fn is_empty(&self) -> bool {
        self.shard.count == 0
    }

    /// The one mapping call: [`open`](Self::open) builds a fresh entry around it,
    /// [`reopen`](Self::reopen) swaps it into an existing one.
    fn map(path: &str, schema: &SchemaDescriptor) -> Result<Rc<MappedShard>, StorageError> {
        Ok(Rc::new(MappedShard::open(&super::super::cstr(path)?, schema, false)?))
    }

    fn open(path: &str, schema: &SchemaDescriptor, max_lsn: u64, synced: bool) -> Result<Self, StorageError> {
        let shard = Self::map(path, schema)?;
        let (pk_min, pk_max) = shard.pk_bounds();
        Ok(ShardEntry {
            shard,
            filename: path.to_string(),
            max_lsn,
            pk_min,
            pk_max,
            synced,
        })
    }

    /// Re-`mmap` this entry's own file under `schema`, yielding **only** the new
    /// mapping: a payload widen rewrites `col_regions` and `null_pad_mask` and
    /// nothing else, so every other field is already right. Exchanging just the
    /// `Rc` is what keeps a reopen from dropping a field it does not know about.
    fn reopen(&self, schema: &SchemaDescriptor) -> Result<Rc<MappedShard>, StorageError> {
        Self::map(&self.filename, schema)
    }

    /// Probe this shard for a PK by its OPK `key` bytes (exactly `pk_stride`
    /// wide). `filter_key` is `probe_key(key)` — the caller hoists it because
    /// it is the same value for every shard in one sweep.
    fn probe_pk_bytes(&self, key: &[u8], filter_key: u64) -> Option<(Rc<MappedShard>, usize)> {
        if self.is_empty() {
            return None;
        }
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
    guard_key: u128,
    entries: Vec<ShardEntry>,
}

impl LevelGuard {
    fn new(gk: u128) -> Self {
        LevelGuard {
            guard_key: gk,
            entries: Vec::new(),
        }
    }

    /// Whether this guard holds only skeleton shards — the state a capacity
    /// sweep leaves behind. Derived from the shard headers rather than persisted,
    /// so it survives any manifest reload with no new manifest field to keep in
    /// step. An empty guard is not dehydrated: there is nothing to say it of, and
    /// answering `true` would make the next fold into it skeletonize data the
    /// sweep never chose to evict.
    ///
    /// Only terminal-level guards are ever dehydrated (L0 and L1 always hold
    /// full-width shards), and a terminal guard holds exactly one entry
    /// (`LMAX_FILE_THRESHOLD == 1`), so "uniformly skeleton or uniformly
    /// hydrated" holds by construction.
    fn dehydrated(&self) -> bool {
        !self.entries.is_empty() && self.entries.iter().all(|e| e.shard.is_skeleton())
    }

    /// When this guard was last written, as the newest LSN over its entries —
    /// `None` for an empty guard, which was never written. The only recency signal
    /// the tree carries; the capacity sweep orders its victims by it.
    fn newest_lsn(&self) -> Option<u64> {
        self.entries.iter().map(|e| e.max_lsn).max()
    }

    /// The highest value the leading eight OPK bytes of this guard's rows take —
    /// for a delta store, whose key is `_tick ‖ view PK`, the newest round it
    /// holds. `None` for a guard holding no rows.
    ///
    /// Taken from the *highest* round because guard boundaries are key ranges, not
    /// round boundaries: a drop can leave the tail of its highest round behind, and
    /// taking that round refuses exactly the cursors that would have needed the
    /// part that went.
    fn highest_leading_u64(&self) -> Option<u64> {
        self.key_extent().map(|(_, max)| (max >> 64) as u64)
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
    /// A guard whose rows share one `pack_pk_be` key cannot be cut, and says so
    /// from its extent rather than from a sample — it stays over target forever,
    /// so a sampling pass would repeat on it indefinitely. That is also the limit
    /// of `target` as a bound: PKs differing only past their leading 16 bytes
    /// share a route key, so their group never splits.
    fn fold_destinations(&self, target: u64) -> Vec<u128> {
        let mut keys = vec![self.guard_key];
        let parts = self.bytes().div_ceil(target).min(MAX_PARTS) as usize;
        let cuttable = self.key_extent().is_some_and(|(lo, hi)| lo < hi);
        if parts >= 2 && cuttable {
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
    fn sample_keys(&self) -> Vec<u128> {
        let rows: usize = self.entries.iter().map(|e| e.shard.count).sum();
        let step = (rows / SPLIT_SAMPLES).max(1);
        let mut sample: Vec<u128> = Vec::with_capacity(SPLIT_SAMPLES + self.entries.len());
        for e in self.entries.iter().filter(|e| !e.is_empty()) {
            sample.extend(
                (0..e.shard.count)
                    .step_by(step)
                    .map(|r| crate::schema::key::pack_pk_be(e.shard.get_pk_bytes(r))),
            );
        }
        sample.sort_unstable();
        sample.dedup();
        sample
    }

    /// The `pack_pk_be` key span this guard's entries actually cover, or `None` for
    /// a guard holding no rows. The guard *key* is only the span's lower fence, so
    /// a fold out of this guard must route by this instead — see
    /// [`ShardIndex::vertical_fold`].
    fn key_extent(&self) -> Option<(u128, u128)> {
        let live = || self.entries.iter().filter(|e| !e.is_empty());
        let opk = |k: &crate::schema::key::PkBuf| crate::schema::key::pack_pk_be(k.pk_bytes());
        Some((
            live().map(|e| opk(&e.pk_min)).min()?,
            live().map(|e| opk(&e.pk_max)).max()?,
        ))
    }
}

struct FLSMLevel {
    guards: Vec<LevelGuard>,
}

impl FLSMLevel {
    fn new() -> Self {
        FLSMLevel { guards: Vec::new() }
    }

    fn find_guard_idx(&self, key: u128) -> Option<usize> {
        // Empty level → `None` (skip this level).
        (!self.guards.is_empty()).then(|| super::guard_slot(&self.guards, key, |g| g.guard_key))
    }

    /// The guards overlapping `[range_min, range_max]`. Guards partition the key
    /// line, so the overlap is a contiguous run — callers may `drain` it.
    ///
    /// Never empty while a guard exists: guard 0 owns the tail below its own key,
    /// so it answers a range that falls entirely under the partition.
    fn find_guards_for_range(&self, range_min: u128, range_max: u128) -> std::ops::Range<usize> {
        // `find_guard_idx` is `None` exactly for an empty level.
        let Some(start) = self.find_guard_idx(range_min) else {
            return 0..0;
        };
        let end = self.guards.partition_point(|g| g.guard_key <= range_max);
        start..end.max(start + 1)
    }

    /// Total registered bytes of every guard in this level.
    fn bytes(&self) -> u64 {
        self.guards.iter().map(LevelGuard::bytes).sum()
    }

    /// The guard keyed exactly `gk` — a guard's identity wherever a fold may have
    /// moved indices under the caller. [`Self::find_guard_idx`] answers the
    /// routing question instead.
    fn find_exact_guard(&self, gk: u128) -> Option<usize> {
        self.guards.binary_search_by_key(&gk, |g| g.guard_key).ok()
    }

    fn get_or_create_guard(&mut self, gk: u128) -> &mut LevelGuard {
        let pos = match self.guards.binary_search_by_key(&gk, |g| g.guard_key) {
            Ok(pos) => pos,
            Err(pos) => {
                self.guards.insert(pos, LevelGuard::new(gk));
                pos
            }
        };
        &mut self.guards[pos]
    }
}

pub(super) struct ShardIndex {
    table_id: u32,
    output_dir: String,
    schema: SchemaDescriptor,

    l0: Vec<ShardEntry>,
    levels: Vec<FLSMLevel>,

    compact_seq: u64,
    pending_deletions: Vec<String>,
    /// Running **max** over the registered L0 bytes each `run_compact` consumed —
    /// `R`, the unit every byte target is stated in. Observed rather than taken
    /// from the RAM-tier ceiling, which budgets heap bytes and bounds a spill from
    /// below rather than above.
    ///
    /// Never below [`MIN_GUARD_BYTES`], and `load_manifest` raises it to the
    /// largest guard it reloads, so a resumed store does not shatter guards built
    /// under a larger `R`.
    l0_run_bytes: u64,
    /// Ceiling on this store's **registered on-disk shard bytes**, from
    /// `CREATE VIEW … WITH (capacity = …)`. `None` for every store but a
    /// capacity-bounded view's output store, which pays nothing.
    capacity_bytes: Option<u64>,
    /// This store evicts by **dropping** its victim rather than by dehydrating
    /// it, and unlinks each compaction's superseded inputs at once rather than
    /// deferring them to the checkpoint barrier. `false` for every store but a
    /// view's delta store.
    ///
    /// One field for both because one fact decides both: a delta store's rows
    /// are the change itself, so there is no summed weight worth a skeleton stub
    /// of, and it publishes no manifest, so the post-publish drain it would
    /// otherwise defer to never runs — deferring there would leak every dropped
    /// *and* every compacted-away shard for the life of the process, invisibly to
    /// `resident_bytes`, which counts registered entries only.
    evict_by_drop: bool,
    /// Running **max** over the leading eight OPK bytes — the `_tick` — of every row
    /// this store has ever dropped. A delta read at `after_tick > dropped_through`
    /// asks only for rows above it, and no such row was ever dropped; a read at or
    /// below it is refused. Zero until the first drop, and always zero for a store
    /// whose eviction residue is not `Nothing`.
    dropped_through: u64,
    /// Passed to every compaction's write. Held rather than derived from the
    /// input shards: a derivation would let one filterless input turn the filter
    /// off for this table's whole descendant line, permanently and invisibly.
    skip_pk_filter: bool,
}

impl ShardIndex {
    /// `skip_pk_filter` declares that nothing point-probes this store by PK, so
    /// its shards need no PK filter.
    pub(super) fn new(table_id: u32, output_dir: &str, schema: SchemaDescriptor, skip_pk_filter: bool) -> Self {
        ShardIndex {
            table_id,
            output_dir: output_dir.to_string(),
            schema,
            l0: Vec::new(),
            levels: Vec::new(),
            compact_seq: 0,
            pending_deletions: Vec::new(),
            l0_run_bytes: MIN_GUARD_BYTES,
            capacity_bytes: None,
            evict_by_drop: false,
            dropped_through: 0,
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

    /// Bound this store's registered shard bytes, once, at construction.
    pub(super) fn set_capacity(&mut self, capacity_bytes: Option<u64>) {
        self.capacity_bytes = capacity_bytes;
    }

    /// Configure this index as a view's **delta store**: bounded by `budget`,
    /// and evicting by dropping rather than by dehydrating (see
    /// [`Self::evict_by_drop`]).
    ///
    /// The budget is bytes and never a subscriber's cursor: the sweep drops the
    /// oldest-written guard whether or not someone is still reading it, so a slow
    /// reader falls off the window alone — it is refused at
    /// [`Self::dropped_through`] and re-reads from scratch — and cannot hold
    /// bytes against a healthy one.
    ///
    /// A drop is destructive, so the guard is the residual granularity: a budget
    /// under [`MIN_GUARD_BYTES`] retains nothing rather than settling above
    /// itself. That costs every subscriber a re-read and costs correctness
    /// nothing.
    pub(super) fn set_delta_budget(&mut self, budget: u64) {
        self.capacity_bytes = Some(budget);
        self.evict_by_drop = true;
    }

    /// The highest round this store has dropped; see [`Self::dropped_through`].
    pub(super) fn dropped_through(&self) -> u64 {
        self.dropped_through
    }

    /// Fallible half of a schema swap: re-open every registered shard under
    /// `new_schema` without touching `self`, so a failure on any one file leaves
    /// the index exactly as it was.
    ///
    /// Empty at an unchanged payload arity: the equal-region ALTERs (RENAME
    /// COLUMN, DROP COLUMN, DROP NOT NULL) change only how existing bytes are
    /// compared. Only a widen (ADD COLUMN) re-opens, because `MappedShard` is
    /// `Rc`-shared with no interior mutability, so its `col_regions` and
    /// `null_pad_mask` cannot be retrofitted. Old `Rc`s held by in-flight
    /// consumers stay valid and drop naturally.
    ///
    /// Re-opening re-reads each shard's PK filter, so this costs one extra
    /// filter allocation per shard until the commit drops the old handles.
    pub(super) fn reopen_all(&self, new_schema: &SchemaDescriptor) -> Result<Vec<Rc<MappedShard>>, StorageError> {
        if new_schema.num_payload_cols() == self.schema.num_payload_cols() {
            return Ok(Vec::new());
        }
        self.all_entries().map(|e| e.reopen(new_schema)).collect()
    }

    /// Infallible half: install the mappings [`reopen_all`](Self::reopen_all)
    /// staged, in the same (deterministic) `all_entries` order, and publish
    /// `new_schema`. `staged` is empty for an equal-region swap, in which case
    /// only the comparator schema moves — `compact_shards` / `ShardEntry::open`
    /// read `&self.schema` per call, so every subsequent compaction uses the new
    /// comparator.
    pub(super) fn install_reopened(&mut self, staged: Vec<Rc<MappedShard>>, new_schema: SchemaDescriptor) {
        debug_assert!(
            staged.is_empty() || staged.len() == self.all_entries().count(),
            "install_reopened: staged count must match the index it was prepared from",
        );
        for (slot, shard) in self.all_entries_mut().zip(staged) {
            slot.shard = shard;
        }
        self.schema = new_schema;
    }
}

#[cfg(test)]
#[path = "../tests/shard_index.rs"]
mod tests;
