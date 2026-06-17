//! FLSM Shard Index: manages shard lifecycle, compaction, and manifest I/O.
//!
//! Split into the in-memory index + compaction trigger ([`index`]) and the
//! manifest serialize/load/recover path ([`persist`]). The shared types
//! (`ShardEntry`, `LevelGuard`, `FLSMLevel`, `PendingShard`, `ShardIndex`), the
//! range/cstring helpers, the level constants, and the constructor live here so
//! both sub-modules read the (private) fields and helpers directly.

use std::cmp::Ordering;
use std::ffi::CString;
use std::rc::Rc;

use super::compare_pk_bytes;
use super::error::StorageError;
use crate::schema::SchemaDescriptor;
use super::manifest::PkBuf;
use super::shard_reader::MappedShard;
use super::xor8;

mod index;
mod persist;

/// PK range check over OPK bytes: `min <= key <= max`. After the OPK-at-rest
/// flip the stored `pk_min`/`pk_max` and `key` are all order-preserving big-
/// endian, so this is a raw `memcmp` at every PK width — no schema. `&[u8]` key
/// so the `sort_by`/probe closures never copy the 81-byte `PkBuf` by value.
#[inline]
fn pk_in_range(min: &PkBuf, max: &PkBuf, key: &[u8]) -> bool {
    compare_pk_bytes(min.pk_bytes(), key) != Ordering::Greater
        && compare_pk_bytes(key, max.pk_bytes()) != Ordering::Greater
}

/// A shard that has been written and mmap'd at its `.tmp` path but not yet
/// inserted into the index. Held by `Table::flush_prepare` until the worker
/// completes Phase 2 and `flush_commit` renames the .tmp into place.
pub struct PendingShard {
    pub(crate) mapped: Rc<MappedShard>,
    pub(crate) final_path: String,
    pub(crate) pk_min: PkBuf,
    pub(crate) pk_max: PkBuf,
    pub(crate) min_lsn: u64,
    pub(crate) max_lsn: u64,
}

const MAX_LEVELS: usize = 3;
const L0_COMPACT_THRESHOLD: usize = 4;
const GUARD_FILE_THRESHOLD: usize = 4;
const LMAX_FILE_THRESHOLD: usize = 1;
const L1_TARGET_FILES: usize = 16;

fn to_cstrings(strings: &[String]) -> Result<Vec<CString>, StorageError> {
    strings
        .iter()
        .map(|f| CString::new(f.as_str()).map_err(|_| StorageError::InvalidPath))
        .collect()
}

struct ShardEntry {
    shard: Rc<MappedShard>,
    filename: String,
    min_lsn: u64,
    max_lsn: u64,
    pk_min: PkBuf,
    pk_max: PkBuf,
}

impl ShardEntry {
    // Derived from shard.count, never serialized. An empty shard must
    // fail every range check; the old "min > max" (u128::MAX, 0)
    // sentinel only worked for unsigned byte-lex and breaks under
    // compare_pk_bytes for signed columns, so probe/sort short-circuit
    // on this instead.
    #[inline]
    fn is_empty(&self) -> bool {
        self.shard.count == 0
    }

    fn open(
        path: &str,
        schema: &SchemaDescriptor,
        min_lsn: u64,
        max_lsn: u64,
    ) -> Result<Self, StorageError> {
        let cpath = CString::new(path).map_err(|_| StorageError::InvalidPath)?;
        let shard = Rc::new(MappedShard::open(&cpath, schema, false)?);
        let is_empty = shard.count == 0;
        let (pk_min, pk_max) = if !is_empty {
            (
                PkBuf::from_bytes(shard.get_pk_bytes(0)),
                PkBuf::from_bytes(shard.get_pk_bytes(shard.count - 1)),
            )
        } else {
            // Valid in-bounds zero key; get_pk_bytes must not be called
            // on a count == 0 shard.
            let e = PkBuf::empty(schema.pk_stride());
            (e, e)
        };
        Ok(ShardEntry {
            shard,
            filename: path.to_string(),
            min_lsn,
            max_lsn,
            pk_min,
            pk_max,
        })
    }

    /// Probe this shard for a PK by its OPK `key` bytes (exactly `pk_stride`
    /// wide). Universal across all PK widths after the OPK-at-rest flip: range
    /// check, XOR8 fingerprint, and the binary search are all raw byte ops. The
    /// `xor8::fingerprint` call is the same derivation the build side
    /// (`build_xor8_from_pk_region`) used, so the probe matches what was inserted.
    fn probe_pk_bytes(&self, key: &[u8]) -> Option<(Rc<MappedShard>, usize)> {
        if self.is_empty() {
            return None;
        }
        if !pk_in_range(&self.pk_min, &self.pk_max, key) {
            return None;
        }
        if self.shard.has_xor8() && !self.shard.xor8_may_contain(xor8::fingerprint(key)) {
            return None;
        }
        let idx = self.shard.find_lower_bound_bytes(key);
        if idx < self.shard.count && self.shard.get_pk_bytes(idx) == key {
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
}

struct FLSMLevel {
    guards: Vec<LevelGuard>,
}

impl FLSMLevel {
    fn new() -> Self {
        FLSMLevel { guards: Vec::new() }
    }

    fn find_guard_idx(&self, key: u128) -> Option<usize> {
        match self.guards.partition_point(|g| g.guard_key <= key) {
            0 => None,
            n => Some(n - 1),
        }
    }

    fn find_guards_for_range(&self, range_min: u128, range_max: u128) -> Vec<usize> {
        let start = match self.guards.partition_point(|g| g.guard_key <= range_min) {
            0 => 0,
            n => n - 1,
        };
        let mut result = Vec::new();
        for i in start..self.guards.len() {
            let gk = self.guards[i].guard_key;
            if gk > range_max {
                break;
            }
            let next_gk = if i + 1 < self.guards.len() {
                self.guards[i + 1].guard_key
            } else {
                u128::MAX
            };
            if next_gk > range_min {
                result.push(i);
            }
        }
        result
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

pub struct ShardIndex {
    table_id: u32,
    output_dir: String,
    schema: SchemaDescriptor,

    l0: Vec<ShardEntry>,
    levels: Vec<FLSMLevel>,

    needs_compaction: bool,
    compact_seq: u64,
    pending_deletions: Vec<String>,
    /// Propagated from `Table::can_tag_pk_unique`; passed through to
    /// `compact_shards` / `merge_and_route` so compacted output shards
    /// are tagged correctly. Defaults to `false` (conservative).
    can_tag_pk_unique: bool,
}

impl ShardIndex {
    pub fn new(table_id: u32, output_dir: &str, schema: SchemaDescriptor) -> Self {
        ShardIndex {
            table_id,
            output_dir: output_dir.to_string(),
            schema,
            l0: Vec::new(),
            levels: Vec::new(),
            needs_compaction: false,
            compact_seq: 0,
            pending_deletions: Vec::new(),
            can_tag_pk_unique: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::posix_io::raise_fd_limit_for_tests;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use super::super::shard_file;

    fn test_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                // col 0 = PK (U64)
                SchemaColumn::new(type_code::U64, 0),
                // col 1 = payload (I64)
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
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

    /// Write a shard whose PK region is the OPK concatenation of two
    /// U64 columns (16 bytes/row), with one I64 payload column. Rows
    /// must be passed in compound-sorted order.
    fn write_compound_shard(
        dir: &std::path::Path,
        name: &str,
        pks: &[(u64, u64)],
        values: &[i64],
    ) -> String {
        let n = pks.len();
        // PK region holds OPK (per-column big-endian) bytes at rest.
        let mut pk_bytes: Vec<u8> = Vec::with_capacity(n * 16);
        for &(a, b) in pks {
            pk_bytes.extend_from_slice(&a.to_be_bytes());
            pk_bytes.extend_from_slice(&b.to_be_bytes());
        }
        let weights = vec![1i64; n];
        let nulls = vec![0u64; n];
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, n * 8),
            (nulls.as_ptr() as *const u8, n * 8),
            (values.as_ptr() as *const u8, n * 8),
            (blob.as_ptr(), 0),
        ];

        let image = shard_file::build_shard_image(42, n as u32, &regions);
        let path = dir.join(name);
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        shard_file::write_shard_at(libc::AT_FDCWD, &cpath, &image, false).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Build and write a shard file with the given PK/value pairs.
    fn write_test_shard(
        dir: &std::path::Path,
        name: &str,
        pks: &[u64],
        values: &[i64],
    ) -> String {
        let n = pks.len();
        // PK region holds OPK (order-preserving big-endian) bytes at rest.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|&p| p.to_be_bytes()).collect();
        let weights = vec![1i64; n];
        let nulls = vec![0u64; n];
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, n * 8),
            (nulls.as_ptr() as *const u8, n * 8),
            (values.as_ptr() as *const u8, n * 8),
            (blob.as_ptr(), 0),
        ];

        let image = shard_file::build_shard_image(42, n as u32, &regions);
        let path = dir.join(name);
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        shard_file::write_shard_at(libc::AT_FDCWD, &cpath, &image, false).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn test_add_shard_and_find_pk() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        let path1 = write_test_shard(dir.path(), "s1.db", &[10, 20, 30], &[100, 200, 300]);
        let path2 = write_test_shard(dir.path(), "s2.db", &[25, 35, 40], &[250, 350, 400]);

        idx.add_shard(&path1, 1, 10).unwrap();
        idx.add_shard(&path2, 11, 20).unwrap();

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
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Add enough shards to trigger compaction to L1
        for i in 0..5u64 {
            let name = format!("s{i}.db");
            let pk = i * 10 + 1;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 100]);
            idx.add_shard(&path, i, i + 1).unwrap();
        }
        idx.run_compact().unwrap();

        // Publish manifest
        let manifest_path = dir.path().join("MANIFEST");
        idx.publish_manifest(manifest_path.to_str().unwrap()).unwrap();

        // Load into a fresh index
        let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);
        idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();

        // Verify all keys are findable in the new index
        for i in 0..5u64 {
            let pk = (i * 10 + 1) as u128;
            let mut found = false;
            idx2.find_pk(pk, &mut |_, _| found = true);
            assert!(found, "key {pk} not found after manifest roundtrip");
        }

        assert_eq!(idx.max_lsn(), idx2.max_lsn());
    }

    #[test]
    fn test_run_compact_l0_to_l1() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Add > L0_COMPACT_THRESHOLD shards
        let mut all_pks = Vec::new();
        for i in 0..5u64 {
            let name = format!("s{i}.db");
            let pk = (i + 1) * 10;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64]);
            idx.add_shard(&path, i, i + 1).unwrap();
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
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Manually populate L1 with > GUARD_FILE_THRESHOLD entries in one guard
        idx.ensure_level(1);
        let guard = idx.levels[0].get_or_create_guard(0);
        let mut all_pks = Vec::new();
        for i in 0..6u64 {
            let name = format!("guard_s{i}.db");
            let pk = i + 1;
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 10]);
            let entry = ShardEntry::open(&path, &schema, 0, 100).unwrap();
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
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);
        idx.ensure_level(1);

        // Build a guard at key 0 with 3 entries (max_lsn=100 → lsn_tag=100)
        for i in 0..3u64 {
            let path = write_test_shard(
                dir.path(),
                &format!("src_{i}.db"),
                &[i + 1],
                &[(i as i64 + 1) * 10],
            );
            let e = ShardEntry::open(&path, &schema, 0, 100).unwrap();
            idx.levels[0].get_or_create_guard(0).entries.push(e);
        }
        // Second guard so worst_count > 1 condition is met in compact_guard_vertical
        {
            let path = write_test_shard(dir.path(), "other.db", &[9999], &[42]);
            let e = ShardEntry::open(&path, &schema, 0, 50).unwrap();
            idx.levels[0].get_or_create_guard(5000).entries.push(e);
        }

        // Block the output path: shard_42_100_L2_G0.db must fail to finalize
        let blocker = dir.path().join("shard_42_100_L2_G0.db");
        std::fs::create_dir_all(&blocker).unwrap();

        let pre_guard_count = idx.levels[0].guards.len();
        let pre_entries = idx.levels[0].guards[0].entries.len();

        let result = idx.compact_guard_vertical(1);
        assert!(result.is_err(), "expected Err when output path is blocked");
        assert_eq!(idx.pending_deletions.len(), 0, "no input files should be queued on failure");
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
    fn test_l1_guard_keys_anchored_at_zero() {
        // Regression: when L1's first guard key is > 0, l1_guard_keys must
        // still anchor the guard space at 0 so keys below the first guard are
        // routable and readable.
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Establish L1 with a single guard whose key is 100.
        idx.ensure_level(1);
        let path = write_test_shard(dir.path(), "l1_g100.db", &[100, 200], &[1000, 2000]);
        let entry = ShardEntry::open(&path, &schema, 0, 100).unwrap();
        idx.levels[0].get_or_create_guard(100).entries.push(entry);

        let keys = idx.l1_guard_keys();
        assert_eq!(keys.first().copied(), Some(0), "guard space must start at 0, got {keys:?}");
        assert!(keys.contains(&100), "existing guard key 100 must be preserved");
    }

    #[test]
    fn test_l1_guard_routing_gap_key_below_first_guard() {
        // Regression for the find_guard_idx/find_guard_for_key mismatch: a key
        // inserted below L1's first guard key (100) must remain findable after
        // an L0→L1 compaction.
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // L1 already has a guard at key 100 (keys 100, 200).
        idx.ensure_level(1);
        let path = write_test_shard(dir.path(), "l1_g100.db", &[100, 200], &[1000, 2000]);
        let entry = ShardEntry::open(&path, &schema, 0, 1).unwrap();
        idx.levels[0].get_or_create_guard(100).entries.push(entry);

        // Insert 5 L0 shards (> L0_COMPACT_THRESHOLD) with keys all below 100.
        let low_keys = [50u64, 60, 70, 80, 90];
        for (i, &k) in low_keys.iter().enumerate() {
            let name = format!("l0_{i}.db");
            let p = write_test_shard(dir.path(), &name, &[k], &[k as i64 * 10]);
            idx.add_shard(&p, (i + 2) as u64, (i + 2) as u64).unwrap();
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
        let r = level.find_guards_for_range(10, 50);
        assert_eq!(r, vec![0]);

        // Range spanning guards 1 and 2
        let r = level.find_guards_for_range(100, 250);
        assert_eq!(r, vec![1, 2]);

        // Range spanning all guards
        let r = level.find_guards_for_range(0, 999);
        assert_eq!(r, vec![0, 1, 2, 3]);

        // Point query at exact guard boundary
        let r = level.find_guards_for_range(200, 200);
        assert_eq!(r, vec![2]);

        // Range below all guards still hits guard 0 (partition_point - 1)
        let r = level.find_guards_for_range(0, 0);
        assert_eq!(r, vec![0]);

        // No guards at all
        let empty = FLSMLevel::new();
        let r = empty.find_guards_for_range(0, 100);
        assert!(r.is_empty());
    }

    #[test]
    fn test_try_cleanup() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

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

    #[test]
    fn test_max_lsn() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        assert_eq!(idx.max_lsn(), 0);

        let path1 = write_test_shard(dir.path(), "lsn1.db", &[10], &[100]);
        idx.add_shard(&path1, 5, 50).unwrap();
        assert_eq!(idx.max_lsn(), 50);

        let path2 = write_test_shard(dir.path(), "lsn2.db", &[20], &[200]);
        idx.add_shard(&path2, 100, 200).unwrap();
        assert_eq!(idx.max_lsn(), 200);

        let path3 = write_test_shard(dir.path(), "lsn3.db", &[30], &[300]);
        idx.add_shard(&path3, 10, 75).unwrap();
        // max_lsn should still be 200 (from second shard)
        assert_eq!(idx.max_lsn(), 200);
    }

    #[test]
    fn test_run_compact_fails_on_long_path_l0_intact() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();

        // Build an output dir long enough that output filenames exceed 255 bytes.
        // Filename overhead: "/shard_42_N_L1_GN.db" ≈ 20 bytes, so out_dir >= 236 bytes.
        // A 240-char subdir is within NAME_MAX (255) and guarantees the total overflows.
        let long_subdir = "a".repeat(240);
        let long_out_dir = dir.path().join(&long_subdir);
        std::fs::create_dir_all(&long_out_dir).unwrap();
        let long_out_str = long_out_dir.to_str().unwrap();
        assert!(
            long_out_str.len() >= 236,
            "test setup: out_dir too short ({})",
            long_out_str.len()
        );

        let mut idx = ShardIndex::new(42, long_out_str, schema);

        // Add L0_COMPACT_THRESHOLD + 1 shards (triggers compaction).
        let mut all_pks = Vec::new();
        for i in 0..5u64 {
            let pk = (i + 1) * 10;
            let path = write_test_shard(dir.path(), &format!("s{i}.db"), &[pk], &[pk as i64]);
            idx.add_shard(&path, i, i + 1).unwrap();
            all_pks.push(pk);
        }

        assert!(idx.should_compact());
        let l0_before = idx.l0.len();

        let result = idx.run_compact();
        assert!(result.is_err(), "expected Err when output path exceeds 255 bytes");

        // L0 must be unchanged — atomicity fix ensures this.
        assert_eq!(idx.l0.len(), l0_before, "L0 must not be modified on failure");

        // needs_compaction must still be true (update_flags was not called).
        assert!(idx.should_compact(), "needs_compaction must remain true after failure");

        // All original keys must still be findable via L0.
        for pk in &all_pks {
            let mut found = false;
            idx.find_pk(*pk as u128, &mut |_, _| found = true);
            assert!(found, "pk {pk} lost from L0 after failed run_compact");
        }
    }

    /// Bug 1: When L1 guard at key=100 is vertically compacted into L2 that has
    /// a guard at key=200, the routing must cover keys below 200 (the source
    /// range's lower bound). Without the fix, keys 100-199 become unfindable.
    #[test]
    fn test_compact_guard_vertical_routing_gap() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Build L1 (levels[0]) and L2 (levels[1])
        idx.ensure_level(2);

        // L1 guard at key=100: 5 shards (> GUARD_FILE_THRESHOLD=4) with keys in [100, 199]
        let src_pks: Vec<u64> = vec![100, 120, 140, 160, 180];
        for (i, &pk) in src_pks.iter().enumerate() {
            let name = format!("src_{i}.db");
            let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 10]);
            let entry = ShardEntry::open(&path, &schema, 0, 100).unwrap();
            idx.levels[0].get_or_create_guard(100).entries.push(entry);
        }

        // L1 guard at key=500: 1 shard (so worst_guard picks key=100)
        {
            let path = write_test_shard(dir.path(), "high.db", &[500], &[5000]);
            let entry = ShardEntry::open(&path, &schema, 0, 50).unwrap();
            idx.levels[0].get_or_create_guard(500).entries.push(entry);
        }

        // L2 guard at key=200: 1 shard with key=250
        {
            let path = write_test_shard(dir.path(), "dest.db", &[250], &[2500]);
            let entry = ShardEntry::open(&path, &schema, 0, 80).unwrap();
            idx.levels[1].get_or_create_guard(200).entries.push(entry);
        }

        // Compact L1 → L2 (src_level_num=1)
        idx.compact_guard_vertical(1).unwrap();

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

    #[test]
    fn test_gc_orphans_removes_stale_shard() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Write a live shard and add it to the index.
        let live_path = write_test_shard(dir.path(), "shard_42_1.db", &[10], &[100]);
        idx.add_shard(&live_path, 1, 1).unwrap();

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
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        // Files belonging to a different table must not be touched.
        let other_path = dir.path().join("shard_99_1.db");
        std::fs::write(&other_path, b"data").unwrap();
        let other_hcomp = dir.path().join("hcomp_99_L1_G0_1.db");
        std::fs::write(&other_hcomp, b"data").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 0);
        assert!(other_path.exists(), "other-table shard must not be removed");
        assert!(other_hcomp.exists(), "other-table hcomp must not be removed");
    }

    #[test]
    fn test_gc_orphans_removes_manifest_tmp() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        let tmp_path = dir.path().join("manifest.bin.tmp");
        std::fs::write(&tmp_path, b"stray").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 1);
        assert!(!tmp_path.exists(), "manifest.bin.tmp must be removed");
    }

    #[test]
    fn test_gc_orphans_removes_tmp_suffix_orphans() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        let shard_tmp = dir.path().join("shard_42_5.db.tmp");
        std::fs::write(&shard_tmp, b"half-written").unwrap();
        let hcomp_tmp = dir.path().join("hcomp_42_L1_G0_3.db.tmp");
        std::fs::write(&hcomp_tmp, b"half-written").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 2);
        assert!(!shard_tmp.exists(), "shard .tmp must be removed");
        assert!(!hcomp_tmp.exists(), "hcomp .tmp must be removed");
    }

    #[test]
    fn test_gc_orphans_empty_index_removes_stray() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        // Empty index — no load_manifest call.
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);

        let stray = dir.path().join("shard_42_7.db");
        std::fs::write(&stray, b"orphan").unwrap();

        let removed = idx.gc_orphans();
        assert_eq!(removed, 1);
        assert!(!stray.exists(), "stray shard must be removed when index is empty");
    }

    /// Single-PK regression: probe_pk range gate and L0 sort order are
    /// identical to the pre-PkBuf u128 logic (golden values).
    #[test]
    fn test_single_pk_probe_and_sort_golden() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();

        let p_lo = write_test_shard(dir.path(), "lo.db", &[10, 20], &[1, 2]);
        let p_hi = write_test_shard(dir.path(), "hi.db", &[30, 40], &[3, 4]);
        let e_lo = ShardEntry::open(&p_lo, &schema, 0, 1).unwrap();
        let e_hi = ShardEntry::open(&p_hi, &schema, 0, 1).unwrap();

        // Range gate: in-range key passes (and resolves), out-of-range
        // key is pruned. OPK for a U64 PK is the value's big-endian bytes.
        assert!(e_lo.probe_pk_bytes(&10u64.to_be_bytes()).is_some());
        assert!(e_lo.probe_pk_bytes(&20u64.to_be_bytes()).is_some());
        assert!(e_lo.probe_pk_bytes(&25u64.to_be_bytes()).is_none(), "25 outside [10,20]");
        assert!(e_hi.probe_pk_bytes(&5u64.to_be_bytes()).is_none(), "5 below [30,40]");

        // L0 sort orders by pk_min, empty entries last (golden order).
        let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);
        let p_empty = write_test_shard(dir.path(), "empty.db", &[], &[]);
        idx.add_shard(&p_hi, 0, 1).unwrap();
        idx.add_shard(&p_lo, 0, 1).unwrap();
        idx.add_shard(&p_empty, 0, 1).unwrap();
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
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();

        let single = test_schema();
        let p = write_test_shard(dir.path(), "e_single.db", &[], &[]);
        let e = ShardEntry::open(&p, &single, 0, 0).unwrap();
        assert!(e.is_empty());
        assert!(e.probe_pk_bytes(&0u64.to_be_bytes()).is_none());
        assert!(e.probe_pk_bytes(&u64::MAX.to_be_bytes()).is_none());

        let compound = compound_schema();
        let pc = write_compound_shard(dir.path(), "e_compound.db", &[], &[]);
        let ec = ShardEntry::open(&pc, &compound, 0, 0).unwrap();
        assert!(ec.is_empty());
        assert_eq!(ec.pk_min.len, compound.pk_stride());
        // Short-circuits before the stride assert / pk_in_range.
        assert!(ec.probe_pk_bytes(&opk2(1, 1)).is_none());
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
            pk_in_range(&min, &max, &inside),
            "key inside the true compound range must not be pruned",
        );
        assert!(
            !pk_in_range(&min, &max, &below),
            "key below the true compound range must be pruned",
        );
        assert!(
            !pk_in_range(&min, &max, &above),
            "key above the true compound range must be pruned",
        );

        // probe_pk_bytes's compound arm prunes an out-of-range key (exercises
        // the stride assert + pk_in_range wiring).
        let dir = tempfile::tempdir().unwrap();
        raise_fd_limit_for_tests();
        let p = write_compound_shard(
            dir.path(),
            "compound.db",
            &[(1, 5), (1, 9), (2, 3)],
            &[10, 20, 30],
        );
        let entry = ShardEntry::open(&p, &schema, 0, 1).unwrap();
        assert_eq!(entry.pk_min.pk_bytes(), &opk2(1, 5));
        assert_eq!(entry.pk_max.pk_bytes(), &opk2(2, 3));
        assert!(
            entry.probe_pk_bytes(&opk2(3, 0)).is_none(),
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
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = wide_schema();
        assert_eq!(schema.pk_stride(), 24);
        let idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema);
        assert_eq!(idx.l1_guard_keys(), vec![0]);
    }
}
