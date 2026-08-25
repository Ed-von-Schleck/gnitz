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
const L0_COMPACT_THRESHOLD: usize = 4;
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
    /// wide). `xor8_key` is `probe_key(key)` — the caller hoists it because
    /// it is the same value for every shard in one sweep.
    fn probe_pk_bytes(&self, key: &[u8], xor8_key: u64) -> Option<(Rc<MappedShard>, usize)> {
        if self.is_empty() {
            return None;
        }
        if !pk_in_range(self.pk_min.pk_bytes(), self.pk_max.pk_bytes(), key) {
            return None;
        }
        if !self.shard.xor8_may_contain(xor8_key) {
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
    /// its shards need no XOR8 filter.
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
    /// Re-opening re-reads each shard's XOR8 filter, so this costs one extra
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
mod tests {
    use super::super::shard_file;
    use super::*;
    use crate::schema::key::probe_key;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::make_schema_u64_i64;

    /// Derives the filter key the way the production sweep does, so no assertion
    /// hand-spells a second version of it.
    fn probe(e: &ShardEntry, key: &[u8]) -> Option<(Rc<MappedShard>, usize)> {
        e.probe_pk_bytes(key, probe_key(key))
    }

    /// Synthetic 2-column compound PK schema: (U64, U64) PK + I64
    /// payload. 16-byte PK region, but the column-aware comparison
    /// differs from a u128 numerical compare of the concatenation.
    fn compound_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // col 0 = PK
                SchemaColumn::new(type_code::U64, 0), // col 1 = PK
                SchemaColumn::new(type_code::I64, 0), // col 2 = payload
            ],
            &[0, 1],
        )
    }

    /// LE concatenation of a (U64, U64) compound key, as the u128 the
    /// probe_pk entry point carries. This is the *native* tuple value used
    /// to drive a test; the on-disk / probe-key form is OPK (see `opk2`).
    fn pack2(a: u64, b: u64) -> u128 {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&a.to_le_bytes());
        buf[8..].copy_from_slice(&b.to_le_bytes());
        u128::from_le_bytes(buf)
    }

    /// OPK (order-preserving) encoding of a (U64, U64) compound key: each
    /// column big-endian, concatenated in pk-list order. memcmp of these
    /// bytes equals the typed (col0, col1) comparison. This is what the PK
    /// region stores and what `probe_pk_bytes`/`pk_in_range` expect.
    fn opk2(a: u64, b: u64) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&a.to_be_bytes());
        buf[8..].copy_from_slice(&b.to_be_bytes());
        buf
    }

    /// Write a shard whose PK region is the OPK concatenation of two U64
    /// columns (16 bytes/row), with one I64 payload column. Rows must be passed
    /// in compound-sorted order.
    fn write_compound_shard(dir: &std::path::Path, name: &str, pks: &[(u64, u64)], values: &[i64]) -> String {
        let rows: Vec<(Vec<u8>, i64, i64)> = pks
            .iter()
            .zip(values)
            .map(|(&(a, b), &v)| ([a.to_be_bytes(), b.to_be_bytes()].concat(), 1, v))
            .collect();
        let path = dir.join(name);
        shard_file::write_test_shard(&path, &compound_schema(), &rows, shard_file::ShardWriteOpts::default());
        path.to_str().unwrap().to_string()
    }

    /// Guard key for a native u64 PK value, in the order-preserving
    /// `pack_pk_be` space (the same key the read router and the compaction
    /// merge use), so multi-guard levels route data keys correctly.
    fn gk(v: u64) -> u128 {
        crate::schema::key::pack_pk_be(&v.to_be_bytes())
    }

    /// Build and write a `(U64 PK | I64 payload)` shard at weight 1.
    fn write_test_shard(dir: &std::path::Path, name: &str, pks: &[u64], values: &[i64]) -> String {
        let rows: Vec<(Vec<u8>, i64, i64)> = pks
            .iter()
            .zip(values)
            .map(|(&p, &v)| (p.to_be_bytes().to_vec(), 1, v))
            .collect();
        let path = dir.join(name);
        shard_file::write_test_shard(
            &path,
            &make_schema_u64_i64(),
            &rows,
            shard_file::ShardWriteOpts::default(),
        );
        path.to_str().unwrap().to_string()
    }

    /// A `(U64 PK | I64)` shard of `n` dense keys from `base`, payload = key.
    fn write_dense_shard(dir: &std::path::Path, name: &str, base: u64, n: u64) -> String {
        let pks: Vec<u64> = (base..base + n).collect();
        let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
        write_test_shard(dir, name, &pks, &vals)
    }

    /// A dense shard the guard rebalance will neither split nor merge — what a
    /// test needs when the partition itself is what it asserts about. The
    /// assertion pins it inside the window an unfolded store leaves alone, so a
    /// change to the row width or the constants fails here rather than silently
    /// re-partitioning the test.
    fn write_stable_shard(dir: &std::path::Path, name: &str, base: u64) -> (String, Vec<u64>) {
        let n = 2800;
        let path = write_dense_shard(dir, name, base, n);
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(
            (MIN_GUARD_BYTES / 2..MIN_GUARD_BYTES).contains(&len),
            "a stable shard must sit between the merge and split thresholds, got {len} B",
        );
        (path, (base..base + n).collect())
    }

    /// Register `path` as an entry of `level_idx`'s guard `key`, creating it.
    fn seed_guard(idx: &mut ShardIndex, level_idx: usize, key: u128, path: &str, lsn: u64) {
        let schema = idx.schema;
        let entry = ShardEntry::open(path, &schema, lsn, true).unwrap();
        idx.ensure_level(level_idx);
        idx.levels[level_idx].get_or_create_guard(key).entries.push(entry);
    }

    /// Every key still routes to a shard that holds it — the property a guard
    /// partition has to keep across every fold.
    fn assert_all_found(idx: &ShardIndex, keys: impl IntoIterator<Item = u64>) {
        for k in keys {
            let mut found = false;
            idx.find_pk(k as u128, &mut |_, _| found = true);
            assert!(found, "key {k} is not reachable through the guard partition");
        }
    }

    /// Publish the index's manifest at `path` for reload tests: stage the
    /// `.tmp` via the production `prepare_manifest`, then rename it into place
    /// (the barrier's `flush_commit` step, minus the fsyncs the round-trip
    /// doesn't observe).
    fn publish_manifest(idx: &ShardIndex, path: &std::path::Path) {
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        let m = idx.prepare_manifest(&cpath, 0, 0).unwrap();
        m.commit().unwrap();
    }

    #[test]
    fn test_add_unsynced_shard_and_find_pk() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        let path1 = write_test_shard(dir.path(), "s1.db", &[10, 20, 30], &[100, 200, 300]);
        let path2 = write_test_shard(dir.path(), "s2.db", &[25, 35, 40], &[250, 350, 400]);

        idx.add_unsynced_shard(&path1, 10).unwrap();
        idx.add_unsynced_shard(&path2, 20).unwrap();

        // Find existing keys
        let mut hits = Vec::new();
        idx.find_pk(10, &mut |ptr, row| hits.push((ptr, row)));
        assert_eq!(hits.len(), 1);

        hits.clear();
        idx.find_pk(25, &mut |ptr, row| hits.push((ptr, row)));
        assert_eq!(hits.len(), 1);

        // Missing key returns nothing
        hits.clear();
        idx.find_pk(99, &mut |ptr, row| hits.push((ptr, row)));
        assert!(hits.is_empty());
    }

    #[test]
    fn test_manifest_roundtrip_with_levels() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // Add enough shards to trigger compaction to L1
        for i in 0..5u64 {
            let name = format!("s{i}.db");
            let pk = i * 10 + 1;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 100]);
            idx.add_unsynced_shard(&path, i + 1).unwrap();
        }
        idx.run_compact().unwrap();

        // Publish manifest
        let manifest_path = dir.path().join("MANIFEST");
        publish_manifest(&idx, &manifest_path);

        // Load into a fresh index
        let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();

        // Verify all keys are findable in the new index
        for i in 0..5u64 {
            let pk = (i * 10 + 1) as u128;
            let mut found = false;
            idx2.find_pk(pk, &mut |_, _| found = true);
            assert!(found, "key {pk} not found after manifest roundtrip");
        }

        assert_eq!(idx.max_lsn(), idx2.max_lsn());

        // The compaction counter is persisted and restored, so a post-restart
        // compaction never reuses a sequence value baked into a live shard name.
        assert!(idx.compact_seq > 0, "run_compact must have advanced compact_seq");
        assert_eq!(
            idx.compact_seq, idx2.compact_seq,
            "compact_seq must survive publish/load"
        );
    }

    #[test]
    fn test_run_compact_l0_to_l1() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // Add > L0_COMPACT_THRESHOLD shards
        let mut all_pks = Vec::new();
        for i in 0..5u64 {
            let name = format!("s{i}.db");
            let pk = (i + 1) * 10;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64]);
            idx.add_unsynced_shard(&path, i + 1).unwrap();
            all_pks.push(pk);
        }

        assert!(idx.should_compact());
        idx.run_compact().unwrap();

        // L0 should be empty after compaction
        assert!(idx.l0.is_empty());
        // L1 should have entries
        assert!(!idx.levels.is_empty());
        assert!(idx.levels[0].bytes() > 0);

        // All keys still findable
        assert_all_found(&idx, all_pks.iter().copied());
    }

    #[test]
    fn the_rebalance_holds_every_level_at_its_targets() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // Manually populate L1 with > GUARD_FILE_THRESHOLD entries in one guard
        idx.ensure_level(0); // L1
        let guard = idx.levels[0].get_or_create_guard(0);
        let mut all_pks = Vec::new();
        for i in 0..6u64 {
            let name = format!("guard_s{i}.db");
            let pk = i + 1;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 10]);
            let entry = ShardEntry::open(&path, &schema, 100, true).unwrap();
            guard.entries.push(entry);
            all_pks.push(pk);
        }
        assert!(idx.levels[0].guards[0].entries.len() > GUARD_FILE_THRESHOLD);

        idx.rebalance_guards().unwrap();

        // After compaction the guard should have 1 file
        assert_eq!(idx.levels[0].guards[0].entries.len(), 1);

        // All keys still findable
        assert_all_found(&idx, all_pks.iter().copied());
    }

    /// A vertical is atomic **per band**, not per call: a failure in band `k`
    /// leaves bands `0..k` folded, the failing band's own source and destination
    /// untouched, and every key still reachable. No manifest is published inside
    /// the sequence, so every intermediate state is a valid partition and the next
    /// spill redoes the rest.
    #[test]
    fn a_failing_vertical_band_leaves_the_bands_before_it_folded() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx.ensure_level(1); // L2

        // Two destination guards over disjoint key bands, each large enough that
        // the trailing rebalance would neither merge nor split them.
        let mut dest_pks = Vec::new();
        for (name, base, key) in [("d_lo.db", 200u64, gk(100)), ("d_hi.db", 100_100, gk(100_000))] {
            let (p, pks) = write_stable_shard(dir.path(), name, base);
            idx.levels[1]
                .get_or_create_guard(key)
                .entries
                .push(ShardEntry::open(&p, &schema, 80, true).unwrap());
            dest_pks.extend(pks);
        }
        // One L1 guard spanning both of them.
        let src_pks: Vec<u64> = vec![100, 150, 100_500, 100_550];
        for (i, &pk) in src_pks.iter().enumerate() {
            let p = write_test_shard(dir.path(), &format!("src_{i}.db"), &[pk], &[pk as i64]);
            idx.levels[0]
                .get_or_create_guard(gk(100))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }

        // compact_seq 1 splits the source into its two bands, 2 folds the first
        // band down, 3 folds the second — which is the one blocked here.
        let blocker = dir.path().join(format!("shard_42_3_L2_G{}.db", gk(100_000)));
        std::fs::create_dir_all(&blocker).unwrap();

        let hi_dest_file = idx.levels[1].guards[1].entries[0].filename.clone();
        assert!(idx.vertical_fold(0).is_err(), "the second band cannot write");

        assert_eq!(idx.levels[0].guards.len(), 1, "only the failed band is left in L1");
        assert_eq!(
            idx.levels[0].guards[0].guard_key,
            gk(100_000),
            "the failed band keeps its own source guard",
        );
        assert_eq!(
            idx.levels[1].guards[1].entries[0].filename, hi_dest_file,
            "the failed band's destination guard is untouched",
        );
        assert_all_found(&idx, src_pks.iter().chain(&dest_pks).copied());
    }

    #[test]
    fn test_l1_guard_routing_gap_key_below_first_guard() {
        // Regression for the find_guard_idx/find_guard_for_key mismatch: a key
        // inserted below L1's first guard key (100) must remain findable after
        // an L0→L1 compaction.
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // L1 already has a guard at key 100 (keys 100, 200).
        idx.ensure_level(0); // L1
        let path = write_test_shard(dir.path(), "l1_g100.db", &[100, 200], &[1000, 2000]);
        let entry = ShardEntry::open(&path, &schema, 1, true).unwrap();
        idx.levels[0].get_or_create_guard(100).entries.push(entry);

        // Insert 5 L0 shards (> L0_COMPACT_THRESHOLD) with keys all below 100.
        let low_keys = [50u64, 60, 70, 80, 90];
        for (i, &k) in low_keys.iter().enumerate() {
            let name = format!("l0_{i}.db");
            let p = write_test_shard(dir.path(), &name, &[k], &[k as i64 * 10]);
            idx.add_unsynced_shard(&p, (i + 2) as u64).unwrap();
        }
        assert!(idx.should_compact());
        idx.run_compact().unwrap();

        // Every below-first-guard key must be findable, plus the original L1 keys.
        assert_all_found(&idx, low_keys.iter().copied().chain([100u64, 200]));
    }

    #[test]
    fn test_find_guards_for_range() {
        let mut level = FLSMLevel::new();
        // Guards at keys 0, 100, 200, 300
        for gk in [0u64, 100, 200, 300] {
            level.guards.push(LevelGuard::new(gk as u128));
        }

        // Range entirely within guard 0
        assert_eq!(level.find_guards_for_range(10, 50), 0..1);

        // Range spanning guards 1 and 2
        assert_eq!(level.find_guards_for_range(100, 250), 1..3);

        // Range spanning all guards
        assert_eq!(level.find_guards_for_range(0, 999), 0..4);

        // Point query at exact guard boundary
        assert_eq!(level.find_guards_for_range(200, 200), 2..3);

        // Range below all guards still hits guard 0 (partition_point - 1)
        assert_eq!(level.find_guards_for_range(0, 0), 0..1);

        // A range entirely below the first guard KEY still names guard 0, which
        // owns the tail below it. An empty run here would let a caller mint a
        // second guard down there without rewriting the rows already in it.
        let mut above_zero = FLSMLevel::new();
        for gk in [100u64, 200] {
            above_zero.guards.push(LevelGuard::new(gk as u128));
        }
        assert_eq!(above_zero.find_guards_for_range(10, 50), 0..1);

        // No guards at all
        let empty = FLSMLevel::new();
        assert!(empty.find_guards_for_range(0, 100).is_empty());
    }

    #[test]
    fn test_try_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // Create real files
        let path1 = write_test_shard(dir.path(), "cleanup1.db", &[1], &[10]);
        let path2 = write_test_shard(dir.path(), "cleanup2.db", &[2], &[20]);
        assert!(std::path::Path::new(&path1).exists());
        assert!(std::path::Path::new(&path2).exists());

        // Add real + nonexistent to pending deletions
        idx.pending_deletions.push(path1.clone());
        idx.pending_deletions.push(path2.clone());
        idx.pending_deletions
            .push(dir.path().join("nonexistent.db").to_str().unwrap().to_string());

        let deleted = idx.try_cleanup();
        // All 3 should count as deleted (2 real + 1 NotFound)
        assert_eq!(deleted, 3);
        assert!(idx.pending_deletions.is_empty());
        assert!(!std::path::Path::new(&path1).exists());
        assert!(!std::path::Path::new(&path2).exists());
    }

    /// Every shard the index registers is unsynced until a barrier sweeps it:
    /// spills on the way in, compaction outputs on the way out, and the inputs a
    /// compaction consumed drop out (they move to `pending_deletions`).
    #[test]
    fn test_unsynced_tracking_register_prune_clear() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        assert!(idx.unsynced_paths().next().is_none());
        let mut spills = Vec::new();
        for i in 0..5u64 {
            let pk = (i + 1) * 10;
            let p = write_test_shard(dir.path(), &format!("shard_42_{i}.db"), &[pk], &[pk as i64]);
            idx.add_unsynced_shard(&p, i + 1).unwrap();
            spills.push(p);
        }
        assert_eq!(idx.unsynced_paths().count(), 5, "registration marks, on its own");

        assert!(idx.should_compact());
        idx.run_compact().unwrap();
        assert!(
            !idx.pending_deletions.is_empty(),
            "compacted inputs queued for deletion"
        );
        for p in &spills {
            assert!(
                !idx.unsynced_paths().any(|q| q == p),
                "consumed input {p} must leave the unsynced set"
            );
        }
        assert!(
            idx.unsynced_paths().next().is_some(),
            "the compaction outputs are themselves unsynced until a barrier sweeps them"
        );

        idx.clear_unsynced();
        assert!(idx.unsynced_paths().next().is_none());
    }

    /// The two ways an entry reaches the index with no write behind it: a
    /// manifest reload, and the re-`mmap` a widening ALTER stages. Neither owes
    /// a sweep, and both are questions about how the entry was built.
    #[test]
    fn reload_and_widen_owe_no_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        for i in 0..3u64 {
            let p = write_test_shard(dir.path(), &format!("shard_42_{i}.db"), &[i * 10 + 1], &[i as i64]);
            idx.add_unsynced_shard(&p, i + 1).unwrap();
        }
        let manifest_path = dir.path().join("MANIFEST");
        publish_manifest(&idx, &manifest_path);

        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx.load_manifest(manifest_path.to_str().unwrap()).unwrap();
        assert!(idx.unsynced_paths().next().is_none(), "a manifest names durable files");

        let wide = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let staged = idx.reopen_all(&wide).unwrap();
        assert_eq!(staged.len(), 3, "a payload widen re-opens every shard");
        idx.install_reopened(staged, wide);
        assert!(idx.unsynced_paths().next().is_none(), "a re-mmap moves no durability");
    }

    #[test]
    fn test_max_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        assert_eq!(idx.max_lsn(), 0);

        let path1 = write_test_shard(dir.path(), "lsn1.db", &[10], &[100]);
        idx.add_unsynced_shard(&path1, 50).unwrap();
        assert_eq!(idx.max_lsn(), 50);

        let path2 = write_test_shard(dir.path(), "lsn2.db", &[20], &[200]);
        idx.add_unsynced_shard(&path2, 200).unwrap();
        assert_eq!(idx.max_lsn(), 200);

        let path3 = write_test_shard(dir.path(), "lsn3.db", &[30], &[300]);
        idx.add_unsynced_shard(&path3, 75).unwrap();
        // max_lsn should still be 200 (from second shard)
        assert_eq!(idx.max_lsn(), 200);
    }

    /// A compaction that cannot write its output leaves L0 exactly as it was, so
    /// the next trigger retries against intact inputs.
    #[test]
    fn test_run_compact_failure_leaves_l0_intact() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();

        // Output dir that does not exist: the finalizing write fails.
        let missing_out_dir = dir.path().join("no_such_dir");
        let mut idx = ShardIndex::new(42, missing_out_dir.to_str().unwrap(), schema, false);

        // Add L0_COMPACT_THRESHOLD + 1 shards (triggers compaction).
        let mut all_pks = Vec::new();
        for i in 0..5u64 {
            let pk = (i + 1) * 10;
            let path = write_test_shard(dir.path(), &format!("s{i}.db"), &[pk], &[pk as i64]);
            idx.add_unsynced_shard(&path, i + 1).unwrap();
            all_pks.push(pk);
        }

        assert!(idx.should_compact());
        let l0_before = idx.l0.len();

        let result = idx.run_compact();
        assert!(result.is_err(), "expected Err when the output shard cannot be written");

        assert_eq!(idx.l0.len(), l0_before, "L0 must not be modified on failure");

        // A failed run_compact leaves l0 intact, so should_compact still holds.
        assert!(idx.should_compact(), "should_compact must remain true after failure");

        // All original keys must still be findable via L0.
        assert_all_found(&idx, all_pks.iter().copied());
    }

    /// An L1 guard at key=100 folded into an L2 that starts at key=200 must
    /// route the keys below 200 — the source range's lower bound — or 100..199
    /// become unfindable.
    #[test]
    fn a_vertical_does_not_lose_keys_below_the_destination_guard() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // Build L1 (levels[0]) and L2 (levels[1])
        idx.ensure_level(1); // L2

        // L1 guard at key=100: 5 shards (> GUARD_FILE_THRESHOLD=4) with keys in [100, 199]
        let src_pks: Vec<u64> = vec![100, 120, 140, 160, 180];
        for (i, &pk) in src_pks.iter().enumerate() {
            let name = format!("src_{i}.db");
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 10]);
            let entry = ShardEntry::open(&path, &schema, 100, true).unwrap();
            idx.levels[0].get_or_create_guard(100).entries.push(entry);
        }

        // L1 guard at key=500: 1 shard (so worst_guard picks key=100)
        {
            let path = write_test_shard(dir.path(), "high.db", &[500], &[5000]);
            let entry = ShardEntry::open(&path, &schema, 50, true).unwrap();
            idx.levels[0].get_or_create_guard(500).entries.push(entry);
        }

        // L2 guard at key=200: 1 shard with key=250
        {
            let path = write_test_shard(dir.path(), "dest.db", &[250], &[2500]);
            let entry = ShardEntry::open(&path, &schema, 80, true).unwrap();
            idx.levels[1].get_or_create_guard(200).entries.push(entry);
        }

        // Compact L1 → L2
        idx.vertical_fold(0).unwrap();

        // All source keys (100-180) must be findable — they should not be lost
        // to the routing gap below L2's guard at 200.
        assert_all_found(&idx, src_pks.iter().copied());

        // The destination key 250 must also still be present
        let mut found_250 = false;
        idx.find_pk(250, &mut |_, _| found_250 = true);
        assert!(found_250, "destination key 250 lost after vertical compaction");
    }

    /// Bucket 0 receives everything below the first guard key, so a terminal guard
    /// routinely holds rows below its own. A vertical whose source sits in that
    /// tail must fold into it rather than mint a second guard underneath — two
    /// guards claiming the same keys leaves the router reaching only one.
    #[test]
    fn a_vertical_into_a_guards_lower_tail_does_not_shadow_it() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // L2 guard at key 200, holding keys on both sides of it.
        let dest_pks = [50u64, 150, 250];
        let vals: Vec<i64> = dest_pks.iter().map(|&p| p as i64).collect();
        let p = write_test_shard(dir.path(), "dest.db", &dest_pks, &vals);
        seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(200), &p, 80);

        // L1 guard whose whole extent is below the destination's key.
        let src_pks = [100u64, 180];
        let vals: Vec<i64> = src_pks.iter().map(|&p| p as i64).collect();
        let p = write_test_shard(dir.path(), "src.db", &src_pks, &vals);
        seed_guard(&mut idx, 0, gk(100), &p, 100);

        idx.vertical_fold(0).unwrap();

        assert_eq!(
            idx.levels[TERMINAL_LEVEL_IDX].guards.len(),
            1,
            "no second guard was minted"
        );
        assert_all_found(&idx, dest_pks.into_iter().chain(src_pks));
    }

    /// A source guard spanning several terminal guards is cut at the destination
    /// partition first, so each band's merge reads one band plus the single
    /// terminal guard it lands in — never the whole overlapped run at once.
    #[test]
    fn a_vertical_bands_its_source_at_the_destination_partition() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        let mut dest_pks = Vec::new();
        for (name, base, key) in [("d_lo.db", 200u64, gk(100)), ("d_hi.db", 100_100, gk(100_000))] {
            let (p, pks) = write_stable_shard(dir.path(), name, base);
            seed_guard(&mut idx, TERMINAL_LEVEL_IDX, key, &p, 80);
            dest_pks.extend(pks);
        }
        let before: Vec<(u128, String)> = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .map(|g| (g.guard_key, g.entries[0].filename.clone()))
            .collect();

        let src_pks = [100u64, 150, 100_500, 100_550];
        let vals: Vec<i64> = src_pks.iter().map(|&p| p as i64).collect();
        let p = write_test_shard(dir.path(), "src.db", &src_pks, &vals);
        seed_guard(&mut idx, 0, gk(100), &p, 100);

        idx.vertical_fold(0).unwrap();

        assert!(idx.levels[0].guards.is_empty(), "every band went down");
        let after: Vec<(u128, String)> = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .map(|g| (g.guard_key, g.entries[0].filename.clone()))
            .collect();
        assert_eq!(
            after.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            before.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            "the destination partition is unchanged",
        );
        assert!(
            after.iter().zip(&before).all(|(a, b)| a.1 != b.1),
            "each band rewrote exactly its own destination",
        );
        assert_all_found(&idx, src_pks.into_iter().chain(dest_pks));
    }

    /// Regression: two vertical compactions that select different worst guards
    /// routing to *disjoint* destination guards, both topping at the same
    /// `max_lsn`, must emit distinct output basenames. Without the per-call
    /// `compact_seq` + destination-guard-key naming, both calls would emit the
    /// same name (shared lsn tag + positional loop index), the second rename
    /// would clobber the first call's live shard, and a reload would lose that
    /// guard's key range.
    #[test]
    fn test_vertical_disjoint_guards_no_name_collision() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx.ensure_level(1); // L2

        // Key 250 routes to the gk(100) bucket; 6000 to the gk(5000) bucket.
        // L1 guard gk(100): two entries (keys 100, 110).
        for (i, &k) in [100u64, 110].iter().enumerate() {
            let p = write_test_shard(dir.path(), &format!("l1a_{i}.db"), &[k], &[k as i64]);
            idx.levels[0]
                .get_or_create_guard(gk(100))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }
        // L1 guard gk(5000): two entries (keys 5000, 5010).
        for (i, &k) in [5000u64, 5010].iter().enumerate() {
            let p = write_test_shard(dir.path(), &format!("l1b_{i}.db"), &[k], &[k as i64]);
            idx.levels[0]
                .get_or_create_guard(gk(5000))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }
        // L2 pre-seed: guard gk(100) (keys 250…) and guard gk(5000) (keys 6000…)
        // at the same max_lsn, so both vertical calls compute the identical
        // `vert_max_lsn` — the pre-fix collision tag. Stable-sized, so the
        // trailing rebalance keeps them two guards.
        let mut l2_pks = Vec::new();
        for (name, base, key) in [("l2a.db", 250u64, gk(100)), ("l2b.db", 6000, gk(5000))] {
            let (p, pks) = write_stable_shard(dir.path(), name, base);
            idx.levels[1]
                .get_or_create_guard(key)
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
            l2_pks.push(pks);
        }

        // Call 1 folds L1 guard 5000 → L2 guard 5000; call 2 folds L1 guard 100 →
        // L2 guard 100 (disjoint destinations, both topping at max_lsn=100).
        idx.vertical_fold(1).unwrap();
        idx.vertical_fold(0).unwrap();

        // Both destination guards reference distinct, existing files.
        let files: Vec<String> = idx.levels[1]
            .guards
            .iter()
            .flat_map(|g| g.entries.iter().map(|e| e.filename.clone()))
            .collect();
        assert_eq!(files.len(), 2, "two L2 guards, one entry each");
        assert_ne!(files[0], files[1], "disjoint-guard outputs must not share a name");
        for f in &files {
            assert!(std::path::Path::new(f).exists(), "output shard {f} missing");
        }

        // Publish + reload into a fresh index: every key must survive.
        let manifest_path = dir.path().join("MANIFEST");
        publish_manifest(&idx, &manifest_path);
        let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();
        let l2_ends = l2_pks.iter().flat_map(|pks| [pks[0], *pks.last().unwrap()]);
        for k in [100u64, 110, 5000, 5010].into_iter().chain(l2_ends) {
            let mut found = false;
            idx2.find_pk(k as u128, &mut |_, _| found = true);
            assert!(found, "key {k} lost after disjoint-guard vertical compactions + reload");
        }
    }

    /// Regression: re-compacting the *same* destination guard twice must not let
    /// `try_cleanup` delete the live shard. If both calls emitted the same output
    /// name, the second would overwrite the first's file in place and then queue
    /// that very name for deletion — `try_cleanup` would unlink the live shard
    /// and the table would fail to reopen. The per-call `compact_seq` keeps the
    /// outputs distinct, so only the genuinely-superseded input is deleted.
    #[test]
    fn test_vertical_same_guard_recompaction_try_cleanup_keeps_live() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx.ensure_level(1); // L2

        // L2 guard gk(100) pre-seeded with key 250.
        {
            let p = write_test_shard(dir.path(), "l2.db", &[250], &[2500]);
            idx.levels[1]
                .get_or_create_guard(gk(100))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }
        // L1 guard gk(100): two entries (keys 100, 110).
        for (i, &k) in [100u64, 110].iter().enumerate() {
            let p = write_test_shard(dir.path(), &format!("l1a_{i}.db"), &[k], &[k as i64]);
            idx.levels[0]
                .get_or_create_guard(gk(100))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }
        idx.vertical_fold(0).unwrap();

        // Re-add L1 guard gk(100) with two more entries (keys 120, 130) and
        // re-compact into the same destination guard.
        for (i, &k) in [120u64, 130].iter().enumerate() {
            let p = write_test_shard(dir.path(), &format!("l1b_{i}.db"), &[k], &[k as i64]);
            idx.levels[0]
                .get_or_create_guard(gk(100))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }
        idx.vertical_fold(0).unwrap();

        // Flush the queued deletions (the consumed inputs). The live L2 shard must
        // NOT be among them.
        idx.try_cleanup();
        let live = idx.levels[1].guards[0].entries[0].filename.clone();
        assert!(
            std::path::Path::new(&live).exists(),
            "try_cleanup deleted the live L2 shard {live}",
        );

        // Publish + reload: every key survives.
        let manifest_path = dir.path().join("MANIFEST");
        publish_manifest(&idx, &manifest_path);
        let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();
        for k in [100u64, 110, 120, 130, 250] {
            let mut found = false;
            idx2.find_pk(k as u128, &mut |_, _| found = true);
            assert!(
                found,
                "key {k} lost after same-guard re-compaction + try_cleanup + reload"
            );
        }
    }

    #[test]
    fn test_gc_orphans_removes_stale_shard() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // Write a live shard and add it to the index.
        let live_path = write_test_shard(dir.path(), "shard_42_1.db", &[10], &[100]);
        idx.add_unsynced_shard(&live_path, 1).unwrap();

        // Drop an orphan shard that the manifest never referenced.
        let orphan_path = dir.path().join("shard_42_99.db");
        std::fs::write(&orphan_path, b"garbage").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 1, "expected 1 file removed");
        assert!(!orphan_path.exists(), "orphan shard must be deleted");
        assert!(std::path::Path::new(&live_path).exists(), "live shard must survive");
    }

    #[test]
    fn test_gc_orphans_ignores_other_table_id() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        // Files belonging to a different table must not be touched.
        let other_path = dir.path().join("shard_99_1.db");
        std::fs::write(&other_path, b"data").unwrap();
        let other_compact = dir.path().join("shard_99_1_L1_G0.db");
        std::fs::write(&other_compact, b"data").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 0);
        assert!(other_path.exists(), "other-table shard must not be removed");
        assert!(
            other_compact.exists(),
            "other-table compaction output must not be removed"
        );
    }

    #[test]
    fn test_gc_orphans_removes_manifest_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        let tmp_path = dir.path().join("manifest.bin.tmp");
        std::fs::write(&tmp_path, b"stray").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 1);
        assert!(!tmp_path.exists(), "manifest.bin.tmp must be removed");
    }

    #[test]
    fn test_gc_orphans_removes_tmp_suffix_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        let shard_tmp = dir.path().join("shard_42_5.db.tmp");
        std::fs::write(&shard_tmp, b"half-written").unwrap();
        let compact_tmp = dir.path().join("shard_42_3_L1_G0.db.tmp");
        std::fs::write(&compact_tmp, b"half-written").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 2);
        assert!(!shard_tmp.exists(), "shard .tmp must be removed");
        assert!(!compact_tmp.exists(), "compaction-output .tmp must be removed");
    }

    #[test]
    fn test_gc_orphans_empty_index_removes_stray() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        // Empty index — no load_manifest call.
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);

        let stray = dir.path().join("shard_42_7.db");
        std::fs::write(&stray, b"orphan").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 1);
        assert!(!stray.exists(), "stray shard must be removed when index is empty");
    }

    /// Golden values for the single-PK probe range gate and the L0 sort order.
    #[test]
    fn test_single_pk_probe_and_sort_golden() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();

        let p_lo = write_test_shard(dir.path(), "lo.db", &[10, 20], &[1, 2]);
        let p_hi = write_test_shard(dir.path(), "hi.db", &[30, 40], &[3, 4]);
        let e_lo = ShardEntry::open(&p_lo, &schema, 1, true).unwrap();
        let e_hi = ShardEntry::open(&p_hi, &schema, 1, true).unwrap();

        // Range gate: in-range key passes (and resolves), out-of-range
        // key is pruned. OPK for a U64 PK is the value's big-endian bytes.
        assert!(probe(&e_lo, &10u64.to_be_bytes()).is_some());
        assert!(probe(&e_lo, &20u64.to_be_bytes()).is_some());
        assert!(probe(&e_lo, &25u64.to_be_bytes()).is_none(), "25 outside [10,20]");
        assert!(probe(&e_hi, &5u64.to_be_bytes()).is_none(), "5 below [30,40]");

        // L0 sort orders by pk_min, empty entries last (golden order).
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        let p_empty = write_test_shard(dir.path(), "empty.db", &[], &[]);
        idx.add_unsynced_shard(&p_hi, 1).unwrap();
        idx.add_unsynced_shard(&p_lo, 1).unwrap();
        idx.add_unsynced_shard(&p_empty, 1).unwrap();
        let order: Vec<bool> = idx.l0.iter().map(|e| e.is_empty()).collect();
        // pk_min holds OPK bytes; widen_pk_be recovers the native U64 value.
        let pk_min_val = |e: &ShardEntry| {
            let b = e.pk_min.pk_bytes();
            gnitz_wire::widen_pk_be(b, b.len())
        };
        assert_eq!(pk_min_val(&idx.l0[0]), 10, "lowest pk_min sorts first");
        assert_eq!(pk_min_val(&idx.l0[1]), 30);
        assert_eq!(order, vec![false, false, true], "empty entry sinks last");
    }

    /// Empty-shard sentinel: is_empty fails every range check under both
    /// a single-PK and a synthetic compound schema, without ever calling
    /// get_pk_bytes on a count == 0 shard.
    #[test]
    fn test_empty_shard_sentinel() {
        let dir = tempfile::tempdir().unwrap();

        let single = make_schema_u64_i64();
        let p = write_test_shard(dir.path(), "e_single.db", &[], &[]);
        let e = ShardEntry::open(&p, &single, 0, true).unwrap();
        assert!(e.is_empty());
        assert!(probe(&e, &0u64.to_be_bytes()).is_none());
        assert!(probe(&e, &u64::MAX.to_be_bytes()).is_none());

        let compound = compound_schema();
        let pc = write_compound_shard(dir.path(), "e_compound.db", &[], &[]);
        let ec = ShardEntry::open(&pc, &compound, 0, true).unwrap();
        assert!(ec.is_empty());
        assert_eq!(ec.pk_min.len, compound.pk_stride());
        // Short-circuits before the stride assert / pk_in_range.
        assert!(probe(&ec, &opk2(1, 1)).is_none());
    }

    /// Compound range-prune correctness: pk_min is numerically greater
    /// than pk_max as a u128 (a naive concatenation compare is wrong),
    /// but correctly ordered under compare_pk_bytes. Proves the compound
    /// path is wired into the range-prune predicate.
    #[test]
    fn test_compound_range_prune() {
        let schema = compound_schema();
        // Rows in compound order: (1,5) < (1,9) < (2,3). pk_min/pk_max and the
        // probe key are all OPK (per-column big-endian) bytes; pk_in_range is a
        // raw memcmp over them.
        let min = PkBuf::from_bytes(&opk2(1, 5));
        let max = PkBuf::from_bytes(&opk2(2, 3));

        // Why OPK is needed: a naive u128 compare of the *LE* concatenation is
        // inverted here — pack2(1,5) = 5·2^64 + 1 > pack2(2,3) = 3·2^64 + 2 —
        // so memcmp must operate on OPK bytes, not the native concatenation.
        assert!(
            pack2(1, 5) > pack2(2, 3),
            "test premise: u128 order of LE concat is inverted vs compound order",
        );

        let inside = opk2(1, 9); // (1,9): >= (1,5), <= (2,3)
        let below = opk2(1, 1); // (1,1): col0 == min, col1 < 5
        let above = opk2(3, 0); // (3,0): col0 > 2

        assert!(
            pk_in_range(min.pk_bytes(), max.pk_bytes(), &inside),
            "key inside the true compound range must not be pruned",
        );
        assert!(
            !pk_in_range(min.pk_bytes(), max.pk_bytes(), &below),
            "key below the true compound range must be pruned",
        );
        assert!(
            !pk_in_range(min.pk_bytes(), max.pk_bytes(), &above),
            "key above the true compound range must be pruned",
        );

        // probe_pk_bytes's compound arm prunes an out-of-range key (exercises
        // the stride assert + pk_in_range wiring).
        let dir = tempfile::tempdir().unwrap();
        let p = write_compound_shard(dir.path(), "compound.db", &[(1, 5), (1, 9), (2, 3)], &[10, 20, 30]);
        let entry = ShardEntry::open(&p, &schema, 1, true).unwrap();
        assert_eq!(entry.pk_min.pk_bytes(), &opk2(1, 5));
        assert_eq!(entry.pk_max.pk_bytes(), &opk2(2, 3));
        assert!(
            probe(&entry, &opk2(3, 0)).is_none(),
            "out-of-range compound key must be pruned by probe_pk_bytes",
        );
    }

    /// Wide (`pk_stride > 16`) 3×U64 schema. Guard keys are derived from the
    /// OPK pk_min bytes via `pack_pk_be`, so this width is handled uniformly.
    fn wide_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        )
    }

    /// An empty wide-PK table has no L0 shards, so `l1_guard_keys` returns the
    /// anchor guard `vec![0]` for every PK width, wide included.
    #[test]
    fn test_l1_guard_keys_wide_bypass() {
        let dir = tempfile::tempdir().unwrap();
        let schema = wide_schema();
        assert_eq!(schema.pk_stride(), 24);
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        assert_eq!(idx.l1_guard_keys(), vec![0]);
    }

    // -----------------------------------------------------------------------
    // Byte targets: guard split and merge
    // -----------------------------------------------------------------------

    /// Rows enough to put one dense shard over `MIN_GUARD_BYTES` — the guard
    /// target an unbounded store that has folded nothing yet carries.
    const OVER_TARGET_ROWS: u64 = 5000;

    /// A guard over its byte target is folded into several destination keys at
    /// its own key quantiles, and every row stays reachable through the
    /// partition the split leaves behind.
    #[test]
    fn an_overfull_guard_splits_at_its_own_key_quantiles() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        let p = write_dense_shard(tmp.path(), "big.db", 1, OVER_TARGET_ROWS);
        seed_guard(&mut idx, 0, gk(1), &p, 1);

        let target = idx.guard_target_bytes(0);
        let before = idx.levels[0].guards[0].bytes();
        assert!(before > target, "premise: {before} B is not over the {target} B target");

        idx.split_overfull_guards(0).unwrap();
        assert_eq!(idx.levels[0].guards.len(), before.div_ceil(target) as usize);
        assert!(
            idx.levels[0].guards.iter().all(|g| g.bytes() <= target),
            "every part fits"
        );
        assert_all_found(&idx, 1..=OVER_TARGET_ROWS);
    }

    /// Guard 0 owns the key line below its own key, so a quantile down there is
    /// how that tail becomes addressable — the one place a split mints a key
    /// below the guard it splits.
    #[test]
    fn splitting_guard_zero_mints_keys_below_its_own() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        // Guard 0 keyed above most of what it holds — the shape a spill below the
        // partition's floor leaves behind.
        let p = write_dense_shard(tmp.path(), "tail.db", 1, OVER_TARGET_ROWS);
        let key = gk(OVER_TARGET_ROWS * 4 / 5);
        seed_guard(&mut idx, 0, key, &p, 1);

        idx.split_overfull_guards(0).unwrap();
        assert!(idx.levels[0].guards.len() > 1, "the tail split");
        assert!(
            idx.levels[0].guards[0].guard_key < key,
            "the new lowest guard sits below the key it was split off",
        );
        assert_all_found(&idx, 1..=OVER_TARGET_ROWS);
    }

    /// A guard whose keys all share one route key cannot be cut, so it must not
    /// report itself overfull either — the fold would rewrite it to the same size
    /// forever.
    #[test]
    fn a_guard_behind_one_route_key_neither_splits_nor_refolds() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        // 9000 rows of one PK at distinct payloads — a legal intermediate batch
        // shape, and past the target even with the PK region as compressible as
        // it gets.
        let pks = vec![7u64; 9000];
        let vals: Vec<i64> = (0..9000).collect();
        let p = write_test_shard(tmp.path(), "one_pk.db", &pks, &vals);
        seed_guard(&mut idx, 0, gk(7), &p, 1);

        let target = idx.guard_target_bytes(0);
        assert!(idx.levels[0].guards[0].bytes() > target, "premise: over target");
        assert_eq!(idx.levels[0].guards[0].fold_destinations(target), vec![gk(7)]);

        let before = idx.levels[0].guards[0].entries[0].filename.clone();
        idx.split_overfull_guards(0).unwrap();
        idx.split_overfull_guards(0).unwrap();
        assert_eq!(idx.levels[0].guards.len(), 1);
        assert_eq!(
            idx.levels[0].guards[0].entries[0].filename, before,
            "an uncuttable guard is not rewritten at all",
        );
    }

    /// The same limit stated for a wide PK: `pack_pk_be` keeps 16 bytes, so keys
    /// that differ only past them share a route key and the byte target stops
    /// being a bound.
    #[test]
    fn a_wide_pk_sharing_its_leading_sixteen_bytes_does_not_split() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        );
        assert_eq!(schema.pk_stride(), 24);
        let rows: Vec<(Vec<u8>, i64, i64)> = (0..9000u64)
            .map(|i| {
                let pk = [1u64.to_be_bytes(), 1u64.to_be_bytes(), i.to_be_bytes()].concat();
                (pk, 1, i as i64)
            })
            .collect();
        let path = tmp.path().join("wide.db");
        shard_file::write_test_shard(&path, &schema, &rows, shard_file::ShardWriteOpts::default());

        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, false);
        seed_guard(&mut idx, 0, 0, path.to_str().unwrap(), 1);
        let target = idx.guard_target_bytes(0);
        assert!(idx.levels[0].guards[0].bytes() > target, "premise: over target");
        assert_eq!(idx.levels[0].guards[0].fold_destinations(target), vec![0]);
    }

    /// One fold writes at most `MAX_PARTS` shards however far over target the
    /// guard is; repeated folds converge instead of one merge writing hundreds of
    /// files.
    #[test]
    fn a_guard_far_over_target_splits_in_bounded_steps() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        let rows = 300_000u64;
        let p = write_dense_shard(tmp.path(), "huge.db", 1, rows);
        seed_guard(&mut idx, 0, gk(1), &p, 1);
        let target = idx.guard_target_bytes(0);
        assert!(
            idx.levels[0].guards[0].bytes() > MAX_PARTS * target,
            "premise: the guard must want more parts than one fold may write",
        );

        idx.split_overfull_guards(0).unwrap();
        assert_eq!(idx.levels[0].guards.len(), MAX_PARTS as usize);

        for _ in 0..4 {
            idx.split_overfull_guards(0).unwrap();
        }
        assert!(
            idx.levels[0].guards.iter().all(|g| g.bytes() <= target),
            "repeated folds converge to guards at the target",
        );
        assert_all_found(&idx, (1..=rows).step_by(997));
    }

    /// A fold whose source bucket comes out empty removes the guard rather than
    /// leaving an entry-less slot in the router's search space.
    #[test]
    fn a_guard_whose_rows_all_cancel_is_removed() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, false);
        // Five entries, so the file threshold fires: two insert/retract pairs and
        // one more insert that its own pair cancels.
        for i in 0..5u64 {
            let w = if i % 2 == 0 { 1 } else { -1 };
            let rows: Vec<(Vec<u8>, i64, i64)> = (0..4u64).map(|k| (k.to_be_bytes().to_vec(), w, k as i64)).collect();
            let path = tmp.path().join(format!("cancel_{i}.db"));
            shard_file::write_test_shard(&path, &schema, &rows, shard_file::ShardWriteOpts::default());
            seed_guard(&mut idx, 0, 0, path.to_str().unwrap(), i + 1);
        }
        // Weights sum to +1 per key over five entries, so one more retraction
        // takes every key to zero.
        let rows: Vec<(Vec<u8>, i64, i64)> = (0..4u64).map(|k| (k.to_be_bytes().to_vec(), -1, k as i64)).collect();
        let path = tmp.path().join("cancel_last.db");
        shard_file::write_test_shard(&path, &schema, &rows, shard_file::ShardWriteOpts::default());
        seed_guard(&mut idx, 0, 0, path.to_str().unwrap(), 6);

        idx.split_overfull_guards(0).unwrap();
        assert!(
            idx.levels[0].guards.is_empty(),
            "an emptied guard leaves no slot behind"
        );
    }

    /// Adjacent guards whose combined bytes fit half the target fold into the
    /// run's lowest key, and the merged guard is under the split trigger by
    /// construction — the hysteresis that stops the two passes trading the same
    /// bytes back and forth.
    #[test]
    fn underfull_neighbours_merge_into_the_runs_lowest_key() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        for i in 0..4u64 {
            let base = 1 + i * 1000;
            let p = write_dense_shard(tmp.path(), &format!("g{i}.db"), base, 200);
            seed_guard(&mut idx, 0, gk(base), &p, i + 1);
        }
        assert_eq!(idx.levels[0].guards.len(), 4);

        idx.merge_underfull_guards(0).unwrap();
        assert_eq!(idx.levels[0].guards.len(), 1, "one run, one guard");
        assert_eq!(idx.levels[0].guards[0].guard_key, gk(1), "keyed by the run's lowest");
        assert_all_found(
            &idx,
            (0..4).flat_map(|i| {
                let b = 1 + i * 1000;
                b..b + 200
            }),
        );

        let after = idx.levels[0].guards[0].entries[0].filename.clone();
        idx.split_overfull_guards(0).unwrap();
        assert_eq!(
            idx.levels[0].guards[0].entries[0].filename, after,
            "the merged guard is below the split trigger",
        );
    }

    /// A run breaks at a change of representation: folding a hydrated guard
    /// together with a dehydrated one would evict it.
    #[test]
    fn a_merge_run_does_not_cross_a_representation_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        for (i, base) in [1u64, 1000].into_iter().enumerate() {
            let p = write_dense_shard(tmp.path(), &format!("t{i}.db"), base, 200);
            seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(base), &p, i as u64 + 1);
        }
        idx.set_capacity(Some(1));
        idx.dehydrate_guard(0).unwrap();
        let (dehy, hyd) = terminal_split(&idx);
        assert_eq!((dehy.as_slice(), hyd.as_slice()), (&[0][..], &[1][..]));

        idx.merge_underfull_guards(TERMINAL_LEVEL_IDX).unwrap();
        assert_eq!(
            idx.levels[TERMINAL_LEVEL_IDX].guards.len(),
            2,
            "the run broke at the boundary"
        );
        assert_eq!(
            terminal_split(&idx),
            (vec![0], vec![1]),
            "neither guard changed representation"
        );
    }

    /// The sweep's first dehydration writes exactly one skeleton shard whatever
    /// the guard's size: its output is one row per key, so a part count derived
    /// from the hydrated input would shatter it into hundreds of tiny guards.
    #[test]
    fn a_first_dehydration_never_splits() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        let p = write_dense_shard(tmp.path(), "hot.db", 1, OVER_TARGET_ROWS);
        seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(1), &p, 1);
        idx.set_capacity(Some(1));

        idx.dehydrate_guard(0).unwrap();
        assert_eq!(idx.levels[TERMINAL_LEVEL_IDX].guards.len(), 1);
        assert_eq!(idx.levels[TERMINAL_LEVEL_IDX].guards[0].entries.len(), 1);
        assert!(idx.has_skeleton_shard());
        assert_all_found(&idx, 1..=OVER_TARGET_ROWS);
    }

    /// An already-dehydrated guard splits like any other, and every part stays
    /// skeleton. Excluding it would leave the one unbounded unit in the tree — a
    /// dehydrated guard absorbs every later vertical over its band.
    #[test]
    fn a_dehydrated_guard_over_target_splits_and_stays_skeleton() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        let rows = 40_000u64;
        let p = write_dense_shard(tmp.path(), "wide_band.db", 1, rows);
        seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(1), &p, 1);
        idx.set_capacity(Some(1));
        idx.dehydrate_guard(0).unwrap();

        let target = idx.guard_target_bytes(TERMINAL_LEVEL_IDX);
        assert!(
            idx.levels[TERMINAL_LEVEL_IDX].guards[0].bytes() > target,
            "premise: the skeleton itself is over target",
        );

        idx.split_overfull_guards(TERMINAL_LEVEL_IDX).unwrap();
        let level = &idx.levels[TERMINAL_LEVEL_IDX];
        assert!(level.guards.len() > 1, "the skeleton split");
        assert!(
            level.guards.iter().all(|g| g.dehydrated()),
            "a split must not re-hydrate what the sweep evicted",
        );
        assert!(idx.has_skeleton_shard(), "reads still route through hydration");
        assert_all_found(&idx, (1..=rows).step_by(101));

        let names: Vec<String> = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .map(|g| g.entries[0].filename.clone())
            .collect();
        idx.split_overfull_guards(TERMINAL_LEVEL_IDX).unwrap();
        let after: Vec<String> = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .map(|g| g.entries[0].filename.clone())
            .collect();
        assert_eq!(names, after, "the trigger cleared");
    }

    /// The guard count tracks the level's bytes in both directions — splitting
    /// alone would make it a high-water mark.
    #[test]
    fn the_guard_count_comes_back_down_after_the_bytes_do() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        // Eight rows per key: dehydration folds each key to one `(PK, Σweight)`
        // row, which is the order-of-magnitude shrink the merge pass exists for.
        let keys = 5_000u64;
        let pks: Vec<u64> = (1..=keys).flat_map(|k| std::iter::repeat_n(k, 8)).collect();
        let vals: Vec<i64> = (0..pks.len() as i64).collect();
        let p = write_test_shard(tmp.path(), "band.db", &pks, &vals);
        seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(1), &p, 1);
        idx.set_capacity(Some(1));

        idx.split_overfull_guards(TERMINAL_LEVEL_IDX).unwrap();
        let split_count = idx.levels[TERMINAL_LEVEL_IDX].guards.len();
        assert!(split_count > 4, "the hydrated level really is finely partitioned");

        // The sweep shrinks every one of them by an order of magnitude.
        while let Some(gi) = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .position(|g| !g.dehydrated())
        {
            idx.dehydrate_guard(gi).unwrap();
        }
        idx.merge_underfull_guards(TERMINAL_LEVEL_IDX).unwrap();

        let target = idx.guard_target_bytes(TERMINAL_LEVEL_IDX);
        let count = idx.levels[TERMINAL_LEVEL_IDX].guards.len();
        assert!(
            count < split_count,
            "the count followed the bytes down: {split_count} -> {count}"
        );
        assert!(
            count <= (idx.level_bytes(TERMINAL_LEVEL_IDX).div_ceil(target / 2) + 1) as usize,
            "{count} guards for {} B at a {target} B target",
            idx.level_bytes(TERMINAL_LEVEL_IDX),
        );
        assert_all_found(&idx, (1..=keys).step_by(101));
    }

    /// A read whose key bound is known before the open reaches only the guards
    /// that can own it — where a whole-index gather is Θ(shards) per open.
    #[test]
    fn a_range_gather_visits_only_the_guards_that_can_own_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        for i in 0..4u64 {
            let base = 1 + i * 1000;
            let p = write_dense_shard(tmp.path(), &format!("g{i}.db"), base, 200);
            seed_guard(&mut idx, 0, gk(base), &p, i + 1);
        }
        let count = |lo: u64, hi: Option<u64>| {
            let hi = hi.map(u64::to_be_bytes);
            idx.shard_arcs_in_range(&lo.to_be_bytes(), hi.as_ref().map(|b| &b[..]))
                .count()
        };

        assert_eq!(idx.all_shard_arcs().len(), 4);
        assert_eq!(count(1500, Some(1500)), 1, "a point read routes to one guard");
        assert_eq!(count(1500, Some(2500)), 2, "a range takes the run it spans");
        assert_eq!(count(0, None), 4, "an open end takes the rest of the key space");
        assert_eq!(count(0, Some(0)), 1, "below every guard key, guard 0 owns the tail");
    }

    // -----------------------------------------------------------------------
    // Byte targets: the derived values
    // -----------------------------------------------------------------------

    /// `R` is observed, not injected: a running max over the registered L0 bytes
    /// each fold consumed, floored so an unfolded store still names a reachable
    /// target.
    #[test]
    fn the_guard_target_tracks_the_l0_folds_the_store_has_seen() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        assert_eq!(idx.l0_run_bytes, MIN_GUARD_BYTES, "before any fold");

        let mut folded = 0u64;
        for i in 0..5u64 {
            let (p, _) = write_stable_shard(tmp.path(), &format!("big_{i}.db"), 1 + i * 10_000);
            folded += std::fs::metadata(&p).unwrap().len();
            idx.add_unsynced_shard(&p, i + 1).unwrap();
        }
        idx.run_compact().unwrap();
        assert_eq!(idx.l0_run_bytes, folded, "the fold this store actually performed");
        assert_eq!(idx.guard_target_bytes(0), folded, "every unevicted level takes R");

        for i in 0..5u64 {
            let p = write_dense_shard(tmp.path(), &format!("small_{i}.db"), 500_000 + i * 100, 10);
            idx.add_unsynced_shard(&p, 100 + i).unwrap();
        }
        idx.run_compact().unwrap();
        assert_eq!(
            idx.l0_run_bytes, folded,
            "a running max never shrinks under a small fold"
        );
    }

    /// The terminal level of a budgeted store takes one sweep step, clamped into
    /// `[MIN_GUARD_BYTES, R]` because `parse_size` admits any positive `u64`.
    #[test]
    fn a_budgeted_terminal_target_is_an_eighth_of_the_budget_within_the_clamp() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        for i in 0..5u64 {
            let (p, _) = write_stable_shard(tmp.path(), &format!("big_{i}.db"), 1 + i * 10_000);
            idx.add_unsynced_shard(&p, i + 1).unwrap();
        }
        idx.run_compact().unwrap();
        let r = idx.l0_run_bytes;
        assert!(r > 2 * MIN_GUARD_BYTES, "premise: R leaves room inside the clamp");
        let mid = (MIN_GUARD_BYTES + r) / 2;

        for (cap, want) in [(mid * 8, mid), (1024, MIN_GUARD_BYTES), (8 * 1024 * 1024 * 1024, r)] {
            idx.set_capacity(Some(cap));
            assert_eq!(idx.guard_target_bytes(TERMINAL_LEVEL_IDX), want, "capacity {cap}");
            assert_eq!(idx.guard_target_bytes(0), r, "only the evicted level reads the budget");
        }
    }

    /// The `u128` intermediate is not optional: at the design point `|L2| × R`
    /// runs past `u64::MAX`. Pure arithmetic, because reaching that product
    /// through `level_bytes` would need a terabyte of mapped shard files.
    #[test]
    fn the_balanced_l1_target_computes_its_product_in_u128() {
        let r = 160 * 1024 * 1024u64;
        let l2 = 1024 * 1024 * 1024 * 1024u64;
        assert!(l2.checked_mul(r).is_none(), "premise: the product overflows a u64");
        let want = 2 * (u128::from(l2) * u128::from(r)).isqrt() as u64;
        assert_eq!(ShardIndex::balanced_l1_target(l2, r), want);

        assert_eq!(
            ShardIndex::balanced_l1_target(0, r),
            16 * r,
            "the floor, for an empty L2"
        );
        assert_eq!(ShardIndex::balanced_l1_target(u64::MAX, u64::MAX), u64::MAX);
    }

    // -----------------------------------------------------------------------
    // Capacity sweep
    // -----------------------------------------------------------------------

    /// A `(U64 PK | I64)` shard of `pks`, all weight 1, payload = pk.
    /// An index holding `n` L0 shards of 40 distinct keys each, spill-stamped
    /// with ascending LSNs so write-recency victim ordering is observable.
    fn index_with_l0(dir: &std::path::Path, n: u64) -> ShardIndex {
        let mut idx = ShardIndex::new(1, dir.to_str().unwrap(), make_schema_u64_i64(), false);
        for s in 0..n {
            let pks: Vec<u64> = (0..40).map(|i| s * 1000 + i + 1).collect();
            let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
            let p = write_test_shard(dir, &format!("l0_{s}.db"), &pks, &vals);
            idx.add_unsynced_shard(&p, s + 1).unwrap();
        }
        idx
    }

    /// Guard indices of the terminal level, split by representation.
    fn terminal_split(idx: &ShardIndex) -> (Vec<usize>, Vec<usize>) {
        let mut dehy = Vec::new();
        let mut hyd = Vec::new();
        let Some(l) = idx.levels.get(TERMINAL_LEVEL_IDX) else {
            return (dehy, hyd);
        };
        for (gi, g) in l.guards.iter().enumerate() {
            if g.entries.is_empty() {
            } else if g.dehydrated() {
                dehy.push(gi);
            } else {
                hyd.push(gi);
            }
        }
        (dehy, hyd)
    }

    /// A capacity above the store's size never dehydrates anything and never
    /// pushes data down: the sweep's first test fails and it returns at once.
    #[test]
    fn a_slack_capacity_leaves_the_store_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = index_with_l0(tmp.path(), 3);
        let before = idx.resident_bytes();
        assert!(before > 0);

        idx.set_capacity(Some(before * 4));
        idx.enforce_capacity().unwrap();

        assert_eq!(idx.resident_bytes(), before, "no compaction ran");
        assert_eq!(idx.l0.len(), 3, "L0 was not pushed down");
        assert!(idx.levels.is_empty(), "no level was created");
        assert!(idx.all_entries().all(|e| !e.shard.is_skeleton()));
    }

    /// A capacity under the skeleton floor converges to it — everything at the
    /// terminal level, everything skeleton, the cap still unmet — and the floor is
    /// a fixpoint. Each call pushes at most one level's worth down; dehydration
    /// above that is unbudgeted.
    #[test]
    fn the_sweep_converges_to_the_skeleton_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = index_with_l0(tmp.path(), 4);
        idx.set_capacity(Some(1));

        // Bounded — the loop would not terminate if a call could make no progress.
        for _ in 0..8 {
            idx.enforce_capacity().unwrap();
        }
        let (dehy, hyd) = terminal_split(&idx);
        assert!(idx.l0.is_empty() && idx.levels[0].guards.is_empty(), "everything sank");
        assert!(!dehy.is_empty() && hyd.is_empty(), "the floor is fully dehydrated");
        assert!(idx.all_entries().all(|e| e.shard.is_skeleton()));

        // At the floor the sweep is a no-op even though the cap is still unmet.
        let floor = idx.resident_bytes();
        assert!(floor > 1);
        idx.enforce_capacity().unwrap();
        assert_eq!(idx.resident_bytes(), floor, "the floor is the fixpoint");
    }

    // -----------------------------------------------------------------------
    // Delta-store retention (evict_by_drop)
    // -----------------------------------------------------------------------

    /// Every `.db` file physically present in `dir` — what a leak shows up in and
    /// `resident_bytes` cannot see, since it counts registered entries only.
    fn on_disk_shards(dir: &std::path::Path) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".db"))
            .collect();
        out.sort();
        out
    }

    /// A delta store's sweep **drops** its victim rather than dehydrating it: the
    /// guard is removed, its file unlinked at once, and the highest round it held
    /// becomes the retention floor. A read at `after_tick > dropped_through` asks
    /// only for rounds above it, and no such round was ever dropped.
    #[test]
    fn a_delta_budget_drops_its_victim_and_raises_the_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = index_with_l0(tmp.path(), 4);
        idx.set_delta_budget(1);
        assert_eq!(idx.dropped_through(), 0, "nothing dropped yet");

        // Same shape as the skeleton floor's convergence: one push-down per call,
        // dehydration — here, dropping — unbudgeted above it.
        for _ in 0..12 {
            idx.enforce_capacity().unwrap();
        }

        assert!(idx.l0.is_empty(), "L0 sank");
        assert!(idx.levels[0].guards.is_empty(), "L1 sank");
        assert!(
            idx.levels[TERMINAL_LEVEL_IDX].guards.is_empty(),
            "a drop REMOVES its guard; clearing it would leave one entry-less guard \
             behind every drop, forever, in the binary-search space"
        );
        assert_eq!(idx.resident_bytes(), 0, "a delta store has no floor to stop above");
        // The last key `index_with_l0` writes is `3 * 1000 + 40`.
        assert_eq!(
            idx.dropped_through(),
            3_040,
            "the watermark is the HIGHEST round dropped, taken from the victim's pk_max"
        );
        assert!(on_disk_shards(tmp.path()).is_empty(), "every dropped shard is unlinked");
        assert!(idx.pending_deletions.is_empty());
    }

    /// The residue is a **per-call** parameter, and only the sweep's own eviction
    /// step may pass anything but `Derived`. A store-scoped residue would be the
    /// shorter change and a silent data-loss bug: a delta store runs every
    /// ordinary compaction too, and those would then delete live, un-evicted rows
    /// and raise the floor past rounds nothing asked to evict — with no error and
    /// no row-set difference.
    #[test]
    fn ordinary_compaction_of_a_delta_store_keeps_its_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = index_with_l0(tmp.path(), 4);
        // A budget the store already meets, so the sweep's own step never runs.
        idx.set_delta_budget(idx.resident_bytes() * 8);
        let before = idx.resident_bytes();

        idx.run_compact().unwrap(); // L0 fold
        for gi in (0..idx.levels[0].guards.len()).rev() {
            idx.vertical_fold(gi).unwrap(); // push-down
        }
        idx.enforce_capacity().unwrap();

        assert_eq!(idx.dropped_through(), 0, "ordinary compaction drops nothing");
        assert!(idx.resident_bytes() > 0, "the rows survived");
        assert!(
            idx.resident_bytes() <= before,
            "a fold never grows the store: {} -> {}",
            before,
            idx.resident_bytes()
        );
        let live: usize = idx.all_entries().map(|e| e.shard.count).sum();
        assert_eq!(live, 4 * 40, "every row survived the folds");
    }

    /// A delta store publishes no manifest and is in neither checkpoint round, so
    /// the post-publish drain every other store defers to never runs at all.
    /// Deferring there would leak every dropped **and every compacted-away** shard
    /// for the life of the process — invisibly to `resident_bytes`, which counts
    /// registered entries.
    ///
    /// The trigger is the **store**, not the residue: an ordinary compaction is
    /// what a delta store runs most often, so draining only on a drop would unlink
    /// the sweep's victims and leak everything else.
    #[test]
    fn a_delta_stores_superseded_shards_unlink_at_once() {
        let tmp = tempfile::tempdir().unwrap();
        let plain_dir = tmp.path().join("plain");
        std::fs::create_dir_all(&plain_dir).unwrap();
        let mut plain = index_with_l0(&plain_dir, 4);
        plain.run_compact().unwrap();
        assert!(
            !plain.pending_deletions.is_empty(),
            "every other store defers its superseded inputs to the checkpoint barrier",
        );

        let dir = tmp.path().join("delta");
        std::fs::create_dir_all(&dir).unwrap();
        let mut idx = index_with_l0(&dir, 4);
        let inputs = on_disk_shards(&dir);
        idx.set_delta_budget(idx.resident_bytes() * 8);
        idx.run_compact().unwrap();

        assert!(
            idx.pending_deletions.is_empty(),
            "unlinked at the end of the compaction"
        );
        let after = on_disk_shards(&dir);
        assert!(
            after.iter().all(|f| !inputs.contains(f)),
            "the compaction's inputs are gone: {inputs:?} -> {after:?}",
        );
        assert!(!after.is_empty(), "its output is not");
    }

    /// A store fed one spill's worth per round, swept every round — the shape a
    /// live delta store takes. Its footprint must plateau: what a leak, or a sweep
    /// that cannot keep pace, shows up as is a footprint that tracks everything
    /// ever written.
    #[test]
    fn a_swept_delta_store_plateaus_under_a_steady_write_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        idx.set_delta_budget(1);

        let mut early = 0u64;
        for round in 0..60u64 {
            // Ascending keys, as a `_tick`-led delta store's always are.
            let pks: Vec<u64> = (0..40).map(|i| round * 1000 + i + 1).collect();
            let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
            let p = write_test_shard(tmp.path(), &format!("spill_{round}.db"), &pks, &vals);
            idx.add_unsynced_shard(&p, round + 1).unwrap();
            if idx.should_compact() {
                idx.run_compact().unwrap();
            }
            idx.enforce_capacity().unwrap();
            if round == 9 {
                early = idx.resident_bytes();
            }
        }

        let late = idx.resident_bytes();
        assert!(
            late <= early.max(1) * 2,
            "footprint grew {early} -> {late} bytes over 50 further rounds of the same \
             write rate — the sweep is not keeping pace",
        );
        assert!(idx.dropped_through() > 0, "the sweep dropped something");
        // Nothing left behind on disk beyond what the index still registers.
        let registered: usize = idx.all_entries().count();
        assert_eq!(on_disk_shards(tmp.path()).len(), registered, "no orphan shard files");
    }

    /// The contract `open_delta_cursor`'s refusal is derived from: a drop removes
    /// only rows **at or below** the floor it raises, so every round above the
    /// floor survives it whole.
    ///
    /// That is what lets a cursor sitting exactly *at* the floor be served rather
    /// than refused — it asks for `(floor, cut]`, and nothing in that span was
    /// ever dropped. Refusing it as well would strand a bootstrap whose watermark
    /// landed on the floor: it would re-read at 0, be handed the same round, and
    /// be refused again until a later tick moved it.
    #[test]
    fn a_drop_removes_nothing_above_the_floor_it_raises() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        let mut written: Vec<u64> = Vec::new();
        let add = |idx: &mut ShardIndex, written: &mut Vec<u64>, round: u64| {
            // Ascending and distinct, as a `_tick`-led delta store's keys are, so
            // nothing cancels in a fold and a row count is a faithful census.
            let pks: Vec<u64> = (0..40).map(|i| round * 1000 + i + 1).collect();
            let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
            let p = write_test_shard(tmp.path(), &format!("spill_{round}.db"), &pks, &vals);
            idx.add_unsynced_shard(&p, round + 1).unwrap();
            written.extend_from_slice(&pks);
        };

        // Fill unbudgeted first, so the budget below is a size the store has
        // actually reached rather than a guess.
        for round in 0..6u64 {
            add(&mut idx, &mut written, round);
        }
        idx.run_compact().unwrap();
        idx.set_delta_budget(idx.resident_bytes());

        for round in 6..24u64 {
            add(&mut idx, &mut written, round);
            if idx.should_compact() {
                idx.run_compact().unwrap();
            }
            idx.enforce_capacity().unwrap();
        }

        let floor = idx.dropped_through();
        let retained: usize = idx.all_entries().map(|e| e.shard.count).sum();
        assert!(floor > 0, "the sweep dropped nothing — nothing is being tested");
        assert!(retained > 0, "the sweep emptied the store — nothing is being tested");

        let above = written.iter().filter(|&&k| k > floor).count();
        assert_eq!(
            retained, above,
            "floor {floor}: every one of the {above} rows above it must survive, and \
             every row at or below it must be gone — {retained} retained",
        );
    }

    /// Dehydration picks its victim by **write recency**: the terminal guard
    /// whose newest entry carries the smallest `max_lsn` goes first. The row
    /// content survives — a skeleton row keeps its key and its summed weight.
    #[test]
    fn dehydration_takes_the_oldest_written_terminal_guard_first() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, false);
        idx.ensure_level(TERMINAL_LEVEL_IDX);
        // Three hydrated terminal guards at distinct write recencies, each too
        // big for the rebalance to merge into its neighbour.
        for (i, base) in [1u64, 10_000, 20_000].into_iter().enumerate() {
            let (p, _) = write_stable_shard(tmp.path(), &format!("t{i}.db"), base);
            idx.levels[TERMINAL_LEVEL_IDX]
                .get_or_create_guard(gk(base))
                .entries
                .push(ShardEntry::open(&p, &schema, 30 - i as u64, true).unwrap());
        }
        let (dehy, hyd) = terminal_split(&idx);
        assert!(dehy.is_empty() && hyd.len() >= 2, "several hydrated terminal guards");

        let lsn_of = |idx: &ShardIndex, gi: usize| {
            idx.levels[TERMINAL_LEVEL_IDX].guards[gi]
                .entries
                .iter()
                .map(|e| e.max_lsn)
                .max()
                .unwrap()
        };
        let oldest = *hyd.iter().min_by_key(|&&gi| lsn_of(&idx, gi)).unwrap();
        let oldest_key = idx.levels[TERMINAL_LEVEL_IDX].guards[oldest].guard_key;
        let live_before: Vec<(u128, i64)> = {
            let g = &idx.levels[TERMINAL_LEVEL_IDX].guards[oldest];
            let s = &g.entries[0].shard;
            (0..s.count).map(|i| (s.get_pk(i), s.get_weight(i))).collect()
        };

        // A capacity just under the current size dehydrates exactly one guard,
        // and it is that one.
        idx.set_capacity(Some(idx.resident_bytes() - 1));
        idx.enforce_capacity().unwrap();
        let (dehy, _) = terminal_split(&idx);
        assert_eq!(dehy.len(), 1, "dehydration stops as soon as the cap is met");
        assert_eq!(
            idx.levels[TERMINAL_LEVEL_IDX].guards[dehy[0]].guard_key, oldest_key,
            "write-recency victim",
        );
        let g = &idx.levels[TERMINAL_LEVEL_IDX].guards[dehy[0]];
        let s = &g.entries[0].shard;
        let live_after: Vec<(u128, i64)> = (0..s.count).map(|i| (s.get_pk(i), s.get_weight(i))).collect();
        assert_eq!(live_after, live_before, "keys and coarse weights survive dehydration");
    }

    /// Ordinary compaction never re-hydrates: a vertical folding hydrated L1 data
    /// into an already-dehydrated terminal guard emits skeleton, so the sweep's
    /// work is not undone.
    #[test]
    fn a_dehydrated_guard_stays_dehydrated_under_ordinary_compaction() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        // One key band, so every later fold routes back into the same guard.
        for s in 0..2u64 {
            let pks: Vec<u64> = (0..20).map(|i| i + 1).collect();
            let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
            let p = write_test_shard(tmp.path(), &format!("a{s}.db"), &pks, &vals);
            idx.add_unsynced_shard(&p, s + 1).unwrap();
        }
        idx.run_compact().unwrap();
        idx.vertical_fold(0).unwrap();
        idx.set_capacity(Some(1));
        idx.enforce_capacity().unwrap();
        assert!(terminal_split(&idx).0.contains(&0), "guard 0 is dehydrated");

        // New hydrated data over the same keys, folded down by the ordinary path:
        // under a capacity this tight `run_compact` drains L1 to the terminal
        // level itself.
        let pks: Vec<u64> = (0..20).map(|i| i + 1).collect();
        let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
        let p = write_test_shard(tmp.path(), "b.db", &pks, &vals);
        idx.add_unsynced_shard(&p, 99).unwrap();
        idx.run_compact().unwrap();
        assert!(idx.levels[0].guards.is_empty(), "the drain emptied L1");

        let (dehy, hyd) = terminal_split(&idx);
        assert!(hyd.is_empty() && !dehy.is_empty(), "the derived rule kept it skeleton");
        assert!(idx.all_entries().all(|e| !e.shard.is_skeleton() || e.shard.count > 0));
    }

    /// `vertical_fold` bounds its destination range by the source guard's true
    /// key extent, not by the gap to the next L1 guard key — which is
    /// `u128::MAX` for the last (or only) guard and would rewrite the whole
    /// terminal level on every spill.
    #[test]
    fn vertical_fold_touches_only_the_guards_its_extent_overlaps() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, false);
        idx.ensure_level(TERMINAL_LEVEL_IDX);
        // Three well-separated terminal bands, each too big for the rebalance to
        // merge into its neighbour or to split.
        for (i, base) in [1u64, 10_000, 20_000].into_iter().enumerate() {
            let (p, _) = write_stable_shard(tmp.path(), &format!("t{i}.db"), base);
            idx.levels[TERMINAL_LEVEL_IDX]
                .get_or_create_guard(gk(base))
                .entries
                .push(ShardEntry::open(&p, &schema, 10, true).unwrap());
        }
        let names: Vec<String> = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .map(|g| g.entries[0].filename.clone())
            .collect();

        // The only L1 guard, so a destination range derived from the gap to the
        // next L1 guard key would be `u128::MAX` and rewrite all three.
        let p = write_test_shard(tmp.path(), "late.db", &[2, 3], &[2, 3]);
        idx.levels[0]
            .get_or_create_guard(gk(2))
            .entries
            .push(ShardEntry::open(&p, &schema, 50, true).unwrap());
        idx.vertical_fold(0).unwrap();

        let after: Vec<String> = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .map(|g| g.entries[0].filename.clone())
            .collect();
        assert_eq!(after.len(), 3);
        assert_ne!(after[0], names[0], "the overlapped guard was rewritten");
        assert_eq!(&after[1..], &names[1..], "the untouched guards kept their files");
    }
}
