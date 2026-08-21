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
const L1_TARGET_FILES: usize = 16;

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
    /// line, so the overlap is always a contiguous run — callers may `drain` it.
    fn find_guards_for_range(&self, range_min: u128, range_max: u128) -> std::ops::Range<usize> {
        let start = self.find_guard_idx(range_min).unwrap_or(0);
        let end = self.guards.partition_point(|g| g.guard_key <= range_max);
        start..end.max(start)
    }

    fn total_file_count(&self) -> usize {
        self.guards.iter().map(|g| g.entries.len()).sum()
    }

    fn get_or_create_guard(&mut self, gk: u128) -> &mut LevelGuard {
        let pos = self.guards.partition_point(|g| g.guard_key < gk);
        if pos < self.guards.len() && self.guards[pos].guard_key == gk {
            return &mut self.guards[pos];
        }
        self.guards.insert(pos, LevelGuard::new(gk));
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
        assert!(idx.levels[0].total_file_count() > 0);

        // All keys still findable
        for pk in &all_pks {
            let mut found = false;
            idx.find_pk(*pk as u128, &mut |_, _| found = true);
            assert!(found, "key {pk} lost after compaction");
        }
    }

    #[test]
    fn test_compact_guards_if_needed() {
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

        idx.compact_guards_if_needed().unwrap();

        // After compaction the guard should have 1 file
        assert_eq!(idx.levels[0].guards[0].entries.len(), 1);

        // All keys still findable
        for pk in &all_pks {
            let mut found = false;
            idx.find_pk(*pk as u128, &mut |_, _| found = true);
            assert!(found, "key {pk} lost after guard compaction");
        }
    }

    #[test]
    fn test_compact_guard_vertical_failure_leaves_index_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_u64_i64();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, false);
        idx.ensure_level(0); // L1

        // Build a guard at key 0 with 3 entries.
        for i in 0..3u64 {
            let path = write_test_shard(dir.path(), &format!("src_{i}.db"), &[i + 1], &[(i as i64 + 1) * 10]);
            let e = ShardEntry::open(&path, &schema, 100, true).unwrap();
            idx.levels[0].get_or_create_guard(0).entries.push(e);
        }
        // Second guard so worst_count > 1 condition is met in compact_guard_vertical
        {
            let path = write_test_shard(dir.path(), "other.db", &[9999], &[42]);
            let e = ShardEntry::open(&path, &schema, 50, true).unwrap();
            idx.levels[0].get_or_create_guard(5000).entries.push(e);
        }

        // Block the output path so it fails to finalize. The output is named by
        // this call's `compact_seq` (fresh index → increments to 1) and the
        // fallback destination guard key 0 (L2 is empty), at level L2.
        let blocker = dir.path().join("shard_42_1_L2_G0.db");
        std::fs::create_dir_all(&blocker).unwrap();

        let pre_guard_count = idx.levels[0].guards.len();
        let pre_entries = idx.levels[0].guards[0].entries.len();

        let result = idx.compact_guard_vertical();
        assert!(result.is_err(), "expected Err when output path is blocked");
        assert_eq!(
            idx.pending_deletions.len(),
            0,
            "no input files should be queued on failure"
        );
        assert_eq!(
            idx.levels[0].guards.len(),
            pre_guard_count,
            "src guards must be unchanged on failure"
        );
        assert_eq!(
            idx.levels[0].guards[0].entries.len(),
            pre_entries,
            "src entries must be unchanged on failure"
        );
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
        for k in low_keys.iter().chain([100u64, 200].iter()) {
            let mut found = false;
            idx.find_pk(*k as u128, &mut |_, _| found = true);
            assert!(found, "key {k} not found after compaction (guard routing gap)");
        }
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
        for pk in &all_pks {
            let mut found = false;
            idx.find_pk(*pk as u128, &mut |_, _| found = true);
            assert!(found, "pk {pk} lost from L0 after failed run_compact");
        }
    }

    /// An L1 guard at key=100 folded into an L2 that starts at key=200 must
    /// route the keys below 200 — the source range's lower bound — or 100..199
    /// become unfindable.
    #[test]
    fn test_compact_guard_vertical_routing_gap() {
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
        idx.compact_guard_vertical().unwrap();

        // All source keys (100-180) must be findable — they should not be lost
        // to the routing gap below L2's guard at 200.
        for &pk in &src_pks {
            let mut found = false;
            idx.find_pk(pk as u128, &mut |_, _| found = true);
            assert!(found, "key {pk} lost after vertical compaction (routing gap bug)");
        }

        // The destination key 250 must also still be present
        let mut found_250 = false;
        idx.find_pk(250, &mut |_, _| found_250 = true);
        assert!(found_250, "destination key 250 lost after vertical compaction");
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
        // L2 pre-seed: guard gk(100) (key 250) and guard gk(5000) (key 6000) at the
        // same max_lsn, so both vertical calls compute the identical `vert_max_lsn`
        // — the pre-fix collision tag.
        {
            let p = write_test_shard(dir.path(), "l2a.db", &[250], &[2500]);
            idx.levels[1]
                .get_or_create_guard(gk(100))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
            let p = write_test_shard(dir.path(), "l2b.db", &[6000], &[60000]);
            idx.levels[1]
                .get_or_create_guard(gk(5000))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }

        // Call 1 folds L1 guard 100 → L2 guard 100; call 2 folds L1 guard 5000 →
        // L2 guard 5000 (disjoint destinations, both topping at max_lsn=100).
        idx.compact_guard_vertical().unwrap();
        idx.compact_guard_vertical().unwrap();

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
        for k in [100u64, 110, 5000, 5010, 250, 6000] {
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
        idx.compact_guard_vertical().unwrap();

        // Re-add L1 guard gk(100) with two more entries (keys 120, 130) and
        // re-compact into the same destination guard.
        for (i, &k) in [120u64, 130].iter().enumerate() {
            let p = write_test_shard(dir.path(), &format!("l1b_{i}.db"), &[k], &[k as i64]);
            idx.levels[0]
                .get_or_create_guard(gk(100))
                .entries
                .push(ShardEntry::open(&p, &schema, 100, true).unwrap());
        }
        idx.compact_guard_vertical().unwrap();

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

    /// A capacity under the skeleton floor converges across calls rather than
    /// running the whole level down inside one trigger: each call pushes at most
    /// once, and dehydration itself is unbudgeted. It stops at the floor —
    /// everything terminal, everything skeleton — with the cap still unmet.
    #[test]
    fn the_sweep_converges_to_the_skeleton_floor_one_push_down_per_call() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = index_with_l0(tmp.path(), 4);

        // Call 1: nothing terminal to dehydrate, so one push-down (L0 → L1) and
        // then stop — L1 data cannot be dehydrated where it sits.
        idx.set_capacity(Some(1));
        idx.enforce_capacity().unwrap();
        assert!(idx.l0.is_empty(), "L0 folded into L1");
        assert!(terminal_split(&idx).0.is_empty(), "nothing dehydrated yet");

        // Further calls: one vertical each, then unbudgeted dehydration of every
        // terminal guard it exposed. Bounded — the loop below would not
        // terminate if a call could make no progress.
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

    /// Dehydration picks its victim by **write recency**: the terminal guard
    /// whose newest entry carries the smallest `max_lsn` goes first. The row
    /// content survives — a skeleton row keeps its key and its summed weight.
    #[test]
    fn dehydration_takes_the_oldest_written_terminal_guard_first() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = index_with_l0(tmp.path(), 3);
        // Sink everything to the terminal level, still hydrated.
        idx.run_compact().unwrap();
        for gi in (0..idx.levels[0].guards.len()).rev() {
            idx.vertical_fold(gi).unwrap();
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

        // New hydrated data over the same keys, folded down by the ordinary path.
        let pks: Vec<u64> = (0..20).map(|i| i + 1).collect();
        let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
        let p = write_test_shard(tmp.path(), "b.db", &pks, &vals);
        idx.add_unsynced_shard(&p, 99).unwrap();
        idx.run_compact().unwrap();
        idx.vertical_fold(0).unwrap();

        let (dehy, hyd) = terminal_split(&idx);
        assert!(hyd.is_empty() && !dehy.is_empty(), "the derived rule kept it skeleton");
        assert!(idx.all_entries().all(|e| !e.shard.is_skeleton() || e.shard.count > 0));
    }

    /// `vertical_fold` bounds its destination range by the source guard's true
    /// key extent, not by the gap to the next L1 guard key — which is
    /// `u128::MAX` for the last (or only) guard and would rewrite the whole
    /// terminal level on every spill. `compact_guard_vertical` declines a
    /// single-entry L1 guard; `vertical_fold` does not, which is what lets the
    /// sweep drain L1 at all.
    #[test]
    fn vertical_fold_touches_only_the_guards_its_extent_overlaps() {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), make_schema_u64_i64(), false);
        // Three well-separated key bands → three terminal guards.
        for (s, base) in [(0u64, 1u64), (1, 10_000), (2, 20_000)] {
            let pks: Vec<u64> = (0..10).map(|i| base + i).collect();
            let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
            let p = write_test_shard(tmp.path(), &format!("s{s}.db"), &pks, &vals);
            idx.add_unsynced_shard(&p, s + 1).unwrap();
        }
        idx.run_compact().unwrap();
        for gi in (0..idx.levels[0].guards.len()).rev() {
            idx.vertical_fold(gi).unwrap();
        }
        assert_eq!(idx.levels[TERMINAL_LEVEL_IDX].guards.len(), 3);
        let names: Vec<String> = idx.levels[TERMINAL_LEVEL_IDX]
            .guards
            .iter()
            .map(|g| g.entries[0].filename.clone())
            .collect();

        // A new lowest-band spill: the only L1 guard, so the old spelling would
        // route into every terminal guard at or above its key — all three.
        let p = write_test_shard(tmp.path(), "late.db", &[2, 3], &[2, 3]);
        idx.add_unsynced_shard(&p, 50).unwrap();
        idx.run_compact().unwrap();
        assert_eq!(idx.levels[0].guards.len(), 1, "one L1 guard");
        assert!(
            idx.compact_guard_vertical().is_ok() && idx.levels[0].guards.len() == 1,
            "compact_guard_vertical declines a single-entry L1 guard",
        );
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
