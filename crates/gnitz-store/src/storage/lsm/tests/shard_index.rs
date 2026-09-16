use super::super::batch::Batch;
use super::super::naming;
use super::super::shard_file;
use super::*;
use crate::schema::key::probe_key;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64, opk_pk, pk_payload_schema};

/// Test-only adapters: production budgets a store at construction and reads its
/// manifest in `Table::new`, where a case here does both against a live index.
impl ShardIndex {
    fn set_capacity(&mut self, capacity_bytes: Option<u64>) {
        self.budget = capacity_bytes.map_or(ShardBudget::Unbounded, ShardBudget::Dehydrate);
    }

    fn set_delta_budget(&mut self, budget: u64) {
        self.budget = ShardBudget::Drop(budget);
    }

    /// Native-`u128` oracle over [`ShardIndex::find_pk_bytes`]: it OPK-encodes
    /// the value first. Wide PKs cannot fit a u128.
    fn find_pk(&self, key: u128, visitor: &mut impl FnMut(Rc<MappedShard>, usize)) {
        let opk = crate::schema::key::opk_key(&self.schema, &key.to_le_bytes());
        let filter_key = crate::schema::key::probe_key(opk.pk_bytes());
        self.find_pk_bytes(opk.pk_bytes(), filter_key, visitor);
    }

    fn load_manifest(&mut self, path: &str) -> Result<(), StorageError> {
        if let Some((entries, header)) = super::super::manifest::read_file(path)? {
            self.install_manifest(&entries, &header)?;
        }
        Ok(())
    }
}

/// Derives the filter key the way the production sweep does, so no assertion
/// hand-spells a second version of it.
fn probe(e: &ShardEntry, key: &[u8]) -> Option<(Rc<MappedShard>, usize)> {
    e.probe_pk_bytes(key, probe_key(key))
}

/// Synthetic 2-column compound PK schema: (U64, U64) PK + I64
/// payload. 16-byte PK region, but the column-aware comparison
/// differs from a u128 numerical compare of the concatenation.
fn compound_schema() -> SchemaDescriptor {
    pk_payload_schema(&[type_code::U64, type_code::U64])
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

/// OPK (order-preserving) encoding of a (U64, U64) compound key — what the PK
/// region stores and what `probe_pk_bytes`/`pk_in_range` expect. memcmp of
/// these bytes equals the typed (col0, col1) comparison.
fn opk2(a: u64, b: u64) -> Vec<u8> {
    opk_pk(&compound_schema(), &[a as u128, b as u128])
}

/// Write a shard whose PK region is the OPK concatenation of two U64
/// columns (16 bytes/row), with one I64 payload column. Rows must be passed
/// in compound-sorted order.
fn write_compound_shard(dir: &std::path::Path, name: &str, pks: &[(u64, u64)], values: &[i64]) -> String {
    let rows: Vec<(Vec<u8>, i64, i64)> = pks.iter().zip(values).map(|(&(a, b), &v)| (opk2(a, b), 1, v)).collect();
    let path = dir.join(name);
    shard_file::write_test_shard(&path, &compound_schema(), &rows, shard_file::ShardWriteOpts::default());
    path.to_str().unwrap().to_string()
}

/// Guard key for a native u64 PK value — the OPK bytes themselves, which is
/// exactly what the read router and the compaction merge compare, so
/// multi-guard levels route data keys correctly.
fn gk(v: u64) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes())
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

/// A shard of `n` rows carrying a `width`-byte STRING payload. Ints pack, so a
/// dense integer shard shrinks below the guard target the moment it is folded;
/// a string payload comes out of the fold as fat as it went in, which is what
/// lets a guard still be over target after one fold.
fn write_fat_shard(dir: &std::path::Path, name: &str, base: u64, n: u64, width: usize) -> String {
    let schema = make_schema_pk_u64_payload_string();
    let mut b = Batch::with_capacity(&schema, n as usize);
    for pk in base..base + n {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        // Vary the body per row so the text carries no run the writer can fold.
        let body: Vec<u8> = (0..width).map(|i| b'a' + ((pk as usize + i) % 26) as u8).collect();
        b.extend_col_blob(0, &body);
        b.count += 1;
    }
    let path = dir.join(name);
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    b.write_as_shard(&cpath, shard_file::ShardWriteOpts::default()).unwrap();
    path.to_str().unwrap().to_string()
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
fn seed_guard(idx: &mut ShardIndex, level_idx: usize, key: PkBuf, path: &str, lsn: u64) {
    let schema = idx.schema;
    let entry = ShardEntry::open(path, &schema, lsn, true).unwrap();
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
    let m = idx.prepare_manifest(&cpath, 0).unwrap();
    m.commit().unwrap();
}

#[test]
fn test_add_unsynced_shard_and_find_pk() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

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
    let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();

    // Every key is findable in the reloaded index.
    assert_all_found(&idx2, (0..5u64).map(|i| i * 10 + 1));

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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

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
    assert!(!idx.levels[0].guards.is_empty());
    assert!(idx.levels[0].bytes() > 0);

    // All keys still findable
    assert_all_found(&idx, all_pks.iter().copied());
}

#[test]
fn the_rebalance_holds_every_level_at_its_targets() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    // Manually populate L1 with > GUARD_FILE_THRESHOLD entries in one guard
    let guard = idx.levels[0].get_or_create_guard(gk(0));
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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    // Two destination guards over disjoint key bands, each large enough that
    // the trailing rebalance would neither merge nor split them.
    let mut dest_pks = Vec::new();
    for (name, base, key) in [("d_lo.db", 200u64, gk(100)), ("d_hi.db", 100_100, gk(100_000))] {
        let (p, pks) = write_stable_shard(dir.path(), name, base);
        seed_guard(&mut idx, 1, key, &p, 80);
        dest_pks.extend(pks);
    }
    // One L1 guard spanning both of them.
    let src_pks: Vec<u64> = vec![100, 150, 100_500, 100_550];
    for (i, &pk) in src_pks.iter().enumerate() {
        let p = write_test_shard(dir.path(), &format!("src_{i}.db"), &[pk], &[pk as i64]);
        seed_guard(&mut idx, 0, gk(100), &p, 100);
    }

    // compact_seq 1 splits the source into its two bands, 2 folds the first
    // band down, 3 folds the second — which is the one blocked here.
    // compact_seq 3 is the second band's terminal fold; it routes into the one
    // destination guard its span overlaps, so its single output is part 0.
    let blocker = dir.path().join(naming::compact_shard_name(42, 3, 2, 0));
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
    // Regression for a routing mismatch between the write split and the read
    // router: a key inserted below L1's first guard key (100) must remain
    // findable after an L0→L1 compaction.
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    // L1 already has a guard at key 100 (keys 100, 200).
    let path = write_test_shard(dir.path(), "l1_g100.db", &[100, 200], &[1000, 2000]);
    seed_guard(&mut idx, 0, gk(100), &path, 1);

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
    let range = |l: &FLSMLevel, lo: u64, hi: u64| l.find_guards_for_range(gk(lo).pk_bytes(), gk(hi).pk_bytes());

    let mut level = FLSMLevel::new();
    // Guards at keys 0, 100, 200, 300
    for k in [0u64, 100, 200, 300] {
        level.guards.push(LevelGuard::new(gk(k)));
    }

    // Range entirely within guard 0
    assert_eq!(range(&level, 10, 50), 0..1);

    // Range spanning guards 1 and 2
    assert_eq!(range(&level, 100, 250), 1..3);

    // Range spanning all guards
    assert_eq!(range(&level, 0, 999), 0..4);

    // Point query at exact guard boundary
    assert_eq!(range(&level, 200, 200), 2..3);

    // Range below all guards still hits guard 0 (partition_point - 1)
    assert_eq!(range(&level, 0, 0), 0..1);

    // A range entirely below the first guard KEY still names guard 0, which
    // owns the tail below it. An empty run here would let a caller mint a
    // second guard down there without rewriting the rows already in it.
    let mut above_zero = FLSMLevel::new();
    for k in [100u64, 200] {
        above_zero.guards.push(LevelGuard::new(gk(k)));
    }
    assert_eq!(range(&above_zero, 10, 50), 0..1);

    // No guards at all
    let empty = FLSMLevel::new();
    assert!(range(&empty, 0, 100).is_empty());
}

#[test]
fn test_try_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

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

    // A missing file counts as deleted, so nothing stays queued.
    idx.try_cleanup();
    assert!(idx.pending_deletions.is_empty());
    assert!(!std::path::Path::new(&path1).exists());
    assert!(!std::path::Path::new(&path2).exists());
}

/// Every shard the index registers is unsynced until a barrier sweeps it:
/// spills on the way in, compaction outputs on the way out, and the inputs a
/// compaction consumed drop out (an unpublished input is unlinked).
#[test]
fn test_unsynced_tracking_register_prune_clear() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    assert!(idx.unsynced_paths().next().is_none());
    let mut spills = Vec::new();
    for i in 0..5u64 {
        let pk = (i + 1) * 10;
        let p = write_test_shard(dir.path(), &naming::spill_shard_name(42, i), &[pk], &[pk as i64]);
        idx.add_unsynced_shard(&p, i + 1).unwrap();
        spills.push(p);
    }
    assert_eq!(idx.unsynced_paths().count(), 5, "registration marks, on its own");

    assert!(idx.should_compact());
    idx.run_compact().unwrap();
    assert!(
        idx.pending_deletions.is_empty(),
        "an unpublished input does not wait for the barrier"
    );
    for p in &spills {
        assert!(!std::path::Path::new(p).exists(), "unpublished input {p} is unlinked");
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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    for i in 0..3u64 {
        let p = write_test_shard(dir.path(), &naming::spill_shard_name(42, i), &[i * 10 + 1], &[i as i64]);
        idx.add_unsynced_shard(&p, i + 1).unwrap();
    }
    let manifest_path = dir.path().join("MANIFEST");
    publish_manifest(&idx, &manifest_path);

    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
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
    idx.swap_schema(wide).unwrap();
    assert!(
        idx.all_entries().all(|e| e.shard.col_regions.len() == 2),
        "a payload widen re-maps every shard"
    );
    assert!(idx.unsynced_paths().next().is_none(), "a re-mmap moves no durability");
}

#[test]
fn test_max_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

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
    let mut idx = ShardIndex::new(
        42,
        missing_out_dir.to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
    );

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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    // L1 guard at key=100: 5 shards (> GUARD_FILE_THRESHOLD=4) with keys in [100, 199]
    let src_pks: Vec<u64> = vec![100, 120, 140, 160, 180];
    for (i, &pk) in src_pks.iter().enumerate() {
        let name = format!("src_{i}.db");
        let path = write_test_shard(dir.path(), &name, &[pk], &[pk as i64 * 10]);
        seed_guard(&mut idx, 0, gk(100), &path, 100);
    }

    // L1 guard at key=500: 1 shard
    {
        let path = write_test_shard(dir.path(), "high.db", &[500], &[5000]);
        seed_guard(&mut idx, 0, gk(500), &path, 50);
    }

    // L2 guard at key=200: 1 shard with key=250
    {
        let path = write_test_shard(dir.path(), "dest.db", &[250], &[2500]);
        seed_guard(&mut idx, 1, gk(200), &path, 80);
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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    let mut dest_pks = Vec::new();
    for (name, base, key) in [("d_lo.db", 200u64, gk(100)), ("d_hi.db", 100_100, gk(100_000))] {
        let (p, pks) = write_stable_shard(dir.path(), name, base);
        seed_guard(&mut idx, TERMINAL_LEVEL_IDX, key, &p, 80);
        dest_pks.extend(pks);
    }
    let before: Vec<(PkBuf, String)> = idx.levels[TERMINAL_LEVEL_IDX]
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
    let after: Vec<(PkBuf, String)> = idx.levels[TERMINAL_LEVEL_IDX]
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

/// Verticals into disjoint destination guards at the same `max_lsn` name their
/// outputs apart, so neither rename clobbers the other's live shard.
#[test]
fn test_vertical_disjoint_guards_no_name_collision() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    // Key 250 routes to the gk(100) bucket; 6000 to the gk(5000) bucket.
    // L1 guard gk(100): two entries (keys 100, 110).
    for (i, &k) in [100u64, 110].iter().enumerate() {
        let p = write_test_shard(dir.path(), &format!("l1a_{i}.db"), &[k], &[k as i64]);
        seed_guard(&mut idx, 0, gk(100), &p, 100);
    }
    // L1 guard gk(5000): two entries (keys 5000, 5010).
    for (i, &k) in [5000u64, 5010].iter().enumerate() {
        let p = write_test_shard(dir.path(), &format!("l1b_{i}.db"), &[k], &[k as i64]);
        seed_guard(&mut idx, 0, gk(5000), &p, 100);
    }
    // L2 pre-seed: guard gk(100) (keys 250…) and guard gk(5000) (keys 6000…)
    // at the same max_lsn. Stable-sized, so the trailing rebalance keeps them
    // two guards.
    let mut l2_pks = Vec::new();
    for (name, base, key) in [("l2a.db", 250u64, gk(100)), ("l2b.db", 6000, gk(5000))] {
        let (p, pks) = write_stable_shard(dir.path(), name, base);
        seed_guard(&mut idx, 1, key, &p, 100);
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
    let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();
    let l2_ends = l2_pks.iter().flat_map(|pks| [pks[0], *pks.last().unwrap()]);
    assert_all_found(&idx2, [100u64, 110, 5000, 5010].into_iter().chain(l2_ends));
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
    let mut idx = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);

    // L2 guard gk(100) pre-seeded with key 250.
    {
        let p = write_test_shard(dir.path(), "l2.db", &[250], &[2500]);
        seed_guard(&mut idx, 1, gk(100), &p, 100);
    }
    // L1 guard gk(100): two entries (keys 100, 110).
    for (i, &k) in [100u64, 110].iter().enumerate() {
        let p = write_test_shard(dir.path(), &format!("l1a_{i}.db"), &[k], &[k as i64]);
        seed_guard(&mut idx, 0, gk(100), &p, 100);
    }
    idx.vertical_fold(0).unwrap();

    // Re-add L1 guard gk(100) with two more entries (keys 120, 130) and
    // re-compact into the same destination guard.
    for (i, &k) in [120u64, 130].iter().enumerate() {
        let p = write_test_shard(dir.path(), &format!("l1b_{i}.db"), &[k], &[k as i64]);
        seed_guard(&mut idx, 0, gk(100), &p, 100);
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
    let mut idx2 = ShardIndex::new(42, dir.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    idx2.load_manifest(manifest_path.to_str().unwrap()).unwrap();
    assert_all_found(&idx2, [100u64, 110, 120, 130, 250]);
}

/// `gc_orphans` unlinks exactly the files that belong to this table and no
/// live entry names: stale shards of either grammar, half-written `.tmp`
/// leftovers, and the staged manifest. Another table's files, and this table's
/// live shard, are not its to touch. Asserting the surviving *set* rather than
/// a removal count catches a file wrongly kept and one wrongly deleted alike.
#[test]
fn gc_orphans_removes_exactly_the_unreferenced_files_of_its_own_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::new(
        42,
        dir.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );

    let live = naming::spill_shard_name(42, 1);
    let live_path = write_test_shard(dir.path(), &live, &[10], &[100]);
    idx.add_unsynced_shard(&live_path, 1).unwrap();

    let doomed = [
        naming::spill_shard_name(42, 99),                   // stale spill
        naming::compact_shard_name(42, 7, 1, 0),            // stale compaction output
        format!("{}.tmp", naming::spill_shard_name(42, 5)), // half-written spill
        format!("{}.tmp", naming::compact_shard_name(42, 3, 1, 0)),
        "manifest.bin.tmp".to_string(),
    ];
    let kept = [
        naming::spill_shard_name(99, 1),         // another table's spill
        naming::compact_shard_name(99, 1, 1, 0), // another table's output
    ];
    for name in doomed.iter().chain(kept.iter()) {
        std::fs::write(dir.path().join(name), b"x").unwrap();
    }

    idx.gc_orphans();

    let mut survivors: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    survivors.sort();
    let mut want: Vec<String> = kept.iter().cloned().chain([live]).collect();
    want.sort();
    assert_eq!(survivors, want);
}

/// An index that never loaded a manifest names no live shard, so every file of
/// its table is an orphan — the boot-time case, where a crash left a shard the
/// manifest was never updated to reference.
#[test]
fn gc_orphans_on_an_empty_index_removes_every_shard_of_its_table() {
    let dir = tempfile::tempdir().unwrap();
    let idx = ShardIndex::new(
        42,
        dir.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    let stray = dir.path().join(naming::spill_shard_name(42, 7));
    std::fs::write(&stray, b"orphan").unwrap();

    idx.gc_orphans();
    assert!(!stray.exists());
}

/// Golden values for the single-PK probe range gate.
#[test]
fn test_single_pk_probe_golden() {
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
}

/// Every shard writer skips an empty output, so a zero-row file can only be
/// damage, and the open refuses it rather than register an entry with no
/// bounds.
#[test]
fn a_zero_row_shard_is_refused_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_test_shard(dir.path(), "empty.db", &[], &[]);
    assert!(matches!(
        ShardEntry::open(&p, &make_schema_u64_i64(), 0, true),
        Err(StorageError::InvalidShard)
    ));
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

    // probe_pk_bytes's compound arm prunes an out-of-range key (exercises the
    // pk_in_range wiring).
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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

/// A guard holding one distinct key cannot be cut — a guard boundary IS a key,
/// so rows sharing a PK cannot be split across one, under any key
/// representation. It must therefore not report itself overfull either, or the
/// fold would rewrite it to the same size forever.
#[test]
fn a_guard_of_one_distinct_key_neither_splits_nor_refolds() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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

/// A wide PK whose keys agree on their leading sixteen bytes and differ only
/// past them: the guard cuts between them, and the byte target bounds it, exactly
/// as at a narrow stride. Under a truncated routing key these keys would be
/// indistinguishable, the guard uncuttable, and the target no bound at all —
/// which is the shape a capacity sweep dehydrates all-or-nothing.
#[test]
fn a_wide_pk_sharing_its_leading_sixteen_bytes_still_splits() {
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

    let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    seed_guard(&mut idx, 0, PkBuf::zeroed(24), path.to_str().unwrap(), 1);
    let target = idx.guard_target_bytes(0);
    let before = idx.levels[0].guards[0].bytes();
    assert!(before > target, "premise: {before} B is not over the {target} B target");

    idx.split_overfull_guards(0).unwrap();
    assert!(idx.levels[0].guards.len() > 1, "the guard cut past byte 16");
    assert!(
        idx.levels[0].guards.iter().all(|g| g.bytes() <= target),
        "the byte target bounds a wide guard too",
    );
}

/// A `pk_cols`×U64 PK plus an I64 payload — strides 8, 24 and 32, so a guard key
/// is narrow, wide, and wide-with-a-16-byte-boundary in turn.
fn stride_schema(pk_cols: usize) -> SchemaDescriptor {
    pk_payload_schema(&vec![type_code::U64; pk_cols])
}

/// Row `i`'s key over `stride_schema(pk_cols)`: ascending in the **last** PK
/// column, so at every stride but 8 the keys agree on their leading bytes and
/// differ only in the trailing ones — past byte 16 for `pk_cols >= 3`.
fn trailing_gk(pk_cols: usize, i: u64) -> PkBuf {
    let mut pk = vec![0u8; (pk_cols - 1) * 8];
    pk.extend_from_slice(&i.to_be_bytes());
    PkBuf::from_bytes(&pk)
}

/// One shard of rows `base..base + n` at [`trailing_gk`]'s keys.
fn write_trailing_key_shard(dir: &std::path::Path, name: &str, pk_cols: usize, base: u64, n: u64) -> String {
    let rows: Vec<(Vec<u8>, i64, i64)> = (base..base + n)
        .map(|i| (trailing_gk(pk_cols, i).pk_bytes().to_vec(), 1, i as i64))
        .collect();
    let path = dir.join(name);
    shard_file::write_test_shard(
        &path,
        &stride_schema(pk_cols),
        &rows,
        shard_file::ShardWriteOpts::default(),
    );
    path.to_str().unwrap().to_string()
}

/// The byte target bounds a guard at every PK stride, and a split holds. Swept
/// over 8, 24 and 32 rather than asserted at 8 alone: at 24 and 32 the keys
/// differ only past byte 16, where a truncated routing key cannot tell them
/// apart and the guard would be uncuttable.
#[test]
fn the_byte_target_bounds_a_guard_at_every_stride() {
    for pk_cols in [1usize, 3, 4] {
        let tmp = tempfile::tempdir().unwrap();
        let schema = stride_schema(pk_cols);
        assert_eq!(schema.pk_stride(), pk_cols * 8);
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
        let p = write_trailing_key_shard(tmp.path(), "big.db", pk_cols, 1, OVER_TARGET_ROWS);
        seed_guard(&mut idx, 0, trailing_gk(pk_cols, 1), &p, 1);

        let target = idx.guard_target_bytes(0);
        let before = idx.levels[0].guards[0].bytes();
        assert!(before > target, "stride {}: {before} B is not over target", pk_cols * 8);

        idx.split_overfull_guards(0).unwrap();
        assert!(idx.levels[0].guards.len() > 1, "stride {}: no split", pk_cols * 8);
        assert!(
            idx.levels[0].guards.iter().all(|g| g.bytes() <= target),
            "stride {}: a part is still over target",
            pk_cols * 8,
        );
        // Every key still routes to a shard that holds it.
        for i in (1..=OVER_TARGET_ROWS).step_by(97) {
            let key = trailing_gk(pk_cols, i);
            let mut found = false;
            idx.find_pk_bytes(key.pk_bytes(), probe_key(key.pk_bytes()), &mut |_, _| found = true);
            assert!(found, "stride {}: key {i} is unreachable", pk_cols * 8);
        }
    }
}

/// The merge pass follows the level's bytes back down at every stride too — a
/// guard partition that can only grow is a partition the sweep cannot shrink.
#[test]
fn underfull_guards_merge_at_every_stride() {
    for pk_cols in [1usize, 3, 4] {
        let tmp = tempfile::tempdir().unwrap();
        let schema = stride_schema(pk_cols);
        let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
        for i in 0..4u64 {
            let base = 1 + i * 1000;
            let p = write_trailing_key_shard(tmp.path(), &format!("g{i}.db"), pk_cols, base, 100);
            seed_guard(&mut idx, 0, trailing_gk(pk_cols, base), &p, i + 1);
        }
        assert_eq!(idx.levels[0].guards.len(), 4);
        // The merge bound is a byte bound, so the four guards must fit it at the
        // widest stride too or the run breaks for a reason this test is not about.
        let bound = idx.guard_target_bytes(0) / 2;
        let total: u64 = idx.levels[0].bytes();
        assert!(
            total <= bound,
            "stride {}: {total} B does not fit the {bound} B run bound",
            pk_cols * 8
        );

        idx.merge_underfull_guards(0).unwrap();
        assert_eq!(
            idx.levels[0].guards.len(),
            1,
            "stride {}: one run, one guard",
            pk_cols * 8
        );
        assert_eq!(
            idx.levels[0].guards[0].guard_key,
            trailing_gk(pk_cols, 1),
            "stride {}: keyed by the run's lowest",
            pk_cols * 8,
        );
    }
}

/// One fold writes at most `MAX_PARTS` shards however far over target the
/// guard is; repeated folds converge instead of one merge writing hundreds of
/// files.
#[test]
fn a_guard_far_over_target_splits_in_bounded_steps() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_pk_u64_payload_string(),
        ShardBudget::Unbounded,
        false,
    );
    let rows = 4_400u64;
    let p = write_fat_shard(tmp.path(), "huge.db", 1, rows, 1000);
    seed_guard(&mut idx, 0, gk(1), &p, 1);
    let target = idx.guard_target_bytes(0);
    assert!(
        idx.levels[0].guards[0].bytes() > MAX_PARTS * target,
        "premise: the guard must want more parts than one fold may write",
    );

    idx.split_overfull_guards(0).unwrap();
    assert_eq!(idx.levels[0].guards.len(), MAX_PARTS as usize);
    assert!(
        idx.levels[0].guards.iter().any(|g| g.bytes() > target),
        "premise: one bounded fold cannot have finished the job, or the \
         convergence below asserts nothing",
    );

    for _ in 0..4 {
        idx.split_overfull_guards(0).unwrap();
    }
    assert!(
        idx.levels[0].guards.iter().all(|g| g.bytes() <= target),
        "repeated folds converge to guards at the target",
    );
    assert_all_found(&idx, (1..=rows).step_by(97));
}

/// A fold whose source bucket comes out empty removes the guard rather than
/// leaving an entry-less slot in the router's search space.
#[test]
fn a_guard_whose_rows_all_cancel_is_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    // Five entries, so the file threshold fires: two insert/retract pairs and
    // one more insert that its own pair cancels.
    for i in 0..5u64 {
        let w = if i % 2 == 0 { 1 } else { -1 };
        let rows: Vec<(Vec<u8>, i64, i64)> = (0..4u64).map(|k| (k.to_be_bytes().to_vec(), w, k as i64)).collect();
        let path = tmp.path().join(format!("cancel_{i}.db"));
        shard_file::write_test_shard(&path, &schema, &rows, shard_file::ShardWriteOpts::default());
        seed_guard(&mut idx, 0, gk(0), path.to_str().unwrap(), i + 1);
    }
    // Weights sum to +1 per key over five entries, so one more retraction
    // takes every key to zero.
    let rows: Vec<(Vec<u8>, i64, i64)> = (0..4u64).map(|k| (k.to_be_bytes().to_vec(), -1, k as i64)).collect();
    let path = tmp.path().join("cancel_last.db");
    shard_file::write_test_shard(&path, &schema, &rows, shard_file::ShardWriteOpts::default());
    seed_guard(&mut idx, 0, gk(0), path.to_str().unwrap(), 6);

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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    let rows = 8_000u64;
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    // Eight rows per key: dehydration folds each key to one `(PK, Σweight)`
    // row, which is the order-of-magnitude shrink the merge pass exists for.
    let keys = 2_500u64;
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
        count <= (idx.levels[TERMINAL_LEVEL_IDX].bytes().div_ceil(target / 2) + 1) as usize,
        "{count} guards for {} B at a {target} B target",
        idx.levels[TERMINAL_LEVEL_IDX].bytes(),
    );
    assert_all_found(&idx, (1..=keys).step_by(101));
}

/// A read whose key bound is known before the open reaches only the guards
/// that can own it — where a whole-index gather is Θ(shards) per open.
#[test]
fn a_range_gather_visits_only_the_guards_that_can_own_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    for i in 0..4u64 {
        let base = 1 + i * 1000;
        let p = write_dense_shard(tmp.path(), &format!("g{i}.db"), base, 200);
        seed_guard(&mut idx, 0, gk(base), &p, i + 1);
    }
    let count = |lo: u64, hi: Option<u64>| {
        idx.shard_arcs_in_range(gk(lo), hi.map_or_else(|| PkBuf::max(8), gk))
            .count()
    };

    assert_eq!(idx.all_shard_arcs_iter().count(), 4);
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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

    // A guard can outgrow `R` — one of a single distinct key cannot be cut — so
    // the largest guard is not the unit a reload may recover.
    let p = write_dense_shard(tmp.path(), "outgrown.db", 10_000_000, 50_000);
    seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(10_000_000), &p, 200);
    assert!(
        idx.levels.iter().flat_map(|l| &l.guards).any(|g| g.bytes() > folded),
        "premise: a guard larger than R"
    );
    let manifest_path = tmp.path().join("MANIFEST");
    publish_manifest(&idx, &manifest_path);
    let mut reloaded = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    reloaded.load_manifest(manifest_path.to_str().unwrap()).unwrap();
    assert_eq!(reloaded.l0_run_bytes, folded, "R survives a restart as it was observed");
}

/// The terminal level of a budgeted store takes one sweep step, clamped into
/// `[MIN_GUARD_BYTES, R]` because `parse_size` admits any positive `u64`.
#[test]
fn a_budgeted_terminal_target_is_an_eighth_of_the_budget_within_the_clamp() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
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
/// through a level's registered bytes would need a terabyte of mapped shards.
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

/// An index holding `n` L0 shards of 40 distinct keys each, spill-stamped
/// with ascending LSNs so write-recency victim ordering is observable.
fn index_with_l0(dir: &std::path::Path, n: u64) -> ShardIndex {
    let mut idx = ShardIndex::new(
        1,
        dir.to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    for s in 0..n {
        let p = write_dense_shard(dir, &format!("l0_{s}.db"), s * 1000 + 1, 40);
        idx.add_unsynced_shard(&p, s + 1).unwrap();
    }
    idx
}

/// Guard indices of the terminal level, split by representation.
fn terminal_split(idx: &ShardIndex) -> (Vec<usize>, Vec<usize>) {
    let mut dehy = Vec::new();
    let mut hyd = Vec::new();
    for (gi, g) in idx.levels[TERMINAL_LEVEL_IDX].guards.iter().enumerate() {
        if g.dehydrated() {
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
    assert!(idx.levels.iter().all(|l| l.guards.is_empty()), "no guard was created");
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
// Delta-store retention (ShardBudget::Drop)
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
/// guard is removed, its file unlinked at once, and the highest key it held
/// becomes the retention floor.
#[test]
fn a_delta_budget_drops_its_victim_and_raises_the_floor() {
    let tmp = tempfile::tempdir().unwrap();
    const SHARDS: u64 = 4;
    // The highest key `index_with_l0` writes, which is the floor a full drop leaves.
    const LAST_KEY: u64 = (SHARDS - 1) * 1000 + 40;
    let mut idx = index_with_l0(tmp.path(), SHARDS);
    idx.set_delta_budget(1);
    assert_eq!(idx.dropped_max(), PkBuf::zeroed(8), "nothing dropped yet");

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
    assert_eq!(
        idx.dropped_max(),
        gk(LAST_KEY),
        "the watermark is the HIGHEST key dropped, taken from the victim's pk_max"
    );
    assert!(on_disk_shards(tmp.path()).is_empty(), "every dropped shard is unlinked");
    assert!(idx.pending_deletions.is_empty());
}

/// Only `enforce_capacity` drops: a delta store's ordinary compactions keep
/// every row and leave the floor at zero.
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

    assert_eq!(idx.dropped_max(), PkBuf::zeroed(8), "ordinary compaction drops nothing");
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

#[test]
fn a_superseded_shard_waits_for_the_barrier_only_if_a_manifest_names_it() {
    let tmp = tempfile::tempdir().unwrap();
    let files = |idx: &ShardIndex| {
        let mut f: Vec<String> = idx.all_entries().map(|e| e.filename.clone()).collect();
        f.sort();
        f
    };
    let exists = |p: &String| std::path::Path::new(p).exists();

    let dir = tmp.path().join("published");
    std::fs::create_dir_all(&dir).unwrap();
    let manifest_path = dir.join("MANIFEST");
    publish_manifest(&index_with_l0(&dir, 5), &manifest_path);
    let mut idx = ShardIndex::new(
        1,
        dir.to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    idx.load_manifest(manifest_path.to_str().unwrap()).unwrap();
    let inputs = files(&idx);
    assert_eq!(inputs.len(), 5);
    idx.run_compact().unwrap();
    let mut queued = idx.pending_deletions.clone();
    queued.sort();
    assert_eq!(queued, inputs, "every published input waits for the barrier");
    assert!(inputs.iter().all(exists), "and is still on disk until then");

    let dir = tmp.path().join("unpublished");
    std::fs::create_dir_all(&dir).unwrap();
    let mut idx = index_with_l0(&dir, 5);
    let inputs = files(&idx);
    idx.run_compact().unwrap();
    assert!(idx.pending_deletions.is_empty(), "nothing waits for the barrier");
    assert!(!inputs.iter().any(exists), "every unpublished input is gone");
    let outputs = files(&idx);
    assert!(
        !outputs.is_empty() && outputs.iter().all(exists),
        "the outputs are present"
    );
}

/// A store fed one spill's worth per round, swept every round — the shape a
/// live delta store takes. Its footprint must plateau: what a leak, or a sweep
/// that cannot keep pace, shows up as is a footprint that tracks everything
/// ever written.
#[test]
fn a_swept_delta_store_plateaus_under_a_steady_write_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    idx.set_delta_budget(1);

    let mut early = 0u64;
    for round in 0..60u64 {
        // Ascending keys, as a `_tick`-led delta store's always are.
        let p = write_dense_shard(tmp.path(), &format!("spill_{round}.db"), round * 1000 + 1, 40);
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
    assert!(idx.dropped_max() > PkBuf::zeroed(8), "the sweep dropped something");
    // Nothing left behind on disk beyond what the index still registers.
    let registered: usize = idx.all_entries().count();
    assert_eq!(on_disk_shards(tmp.path()).len(), registered, "no orphan shard files");
}

/// The contract a delta read's expiry refusal is derived from: a drop removes
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    let mut written: Vec<u64> = Vec::new();
    let add = |idx: &mut ShardIndex, written: &mut Vec<u64>, round: u64| {
        // Ascending and distinct, as a `_tick`-led delta store's keys are, so
        // nothing cancels in a fold and a row count is a faithful census.
        let base = round * 1000 + 1;
        let p = write_dense_shard(tmp.path(), &format!("spill_{round}.db"), base, 40);
        idx.add_unsynced_shard(&p, round + 1).unwrap();
        written.extend(base..base + 40);
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

    let floor = idx.dropped_max();
    let retained: usize = idx.all_entries().map(|e| e.shard.count).sum();
    assert!(
        floor > PkBuf::zeroed(8),
        "the sweep dropped nothing — nothing is being tested"
    );
    assert!(retained > 0, "the sweep emptied the store — nothing is being tested");

    let above = written.iter().filter(|&&k| gk(k) > floor).count();
    assert_eq!(
        retained, above,
        "floor {floor:?}: every one of the {above} rows above it must survive, and \
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
    let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    // Three hydrated terminal guards at distinct write recencies, each too
    // big for the rebalance to merge into its neighbour.
    for (i, base) in [1u64, 10_000, 20_000].into_iter().enumerate() {
        let (p, _) = write_stable_shard(tmp.path(), &format!("t{i}.db"), base);
        seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(base), &p, 30 - i as u64);
    }
    let (dehy, hyd) = terminal_split(&idx);
    assert!(dehy.is_empty() && hyd.len() >= 2, "several hydrated terminal guards");

    let oldest = *hyd
        .iter()
        .min_by_key(|&&gi| idx.levels[TERMINAL_LEVEL_IDX].guards[gi].newest_lsn())
        .unwrap();
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
    let mut idx = ShardIndex::new(
        1,
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
    );
    // One key band, so every later fold routes back into the same guard.
    for s in 0..2u64 {
        let p = write_dense_shard(tmp.path(), &format!("a{s}.db"), 1, 20);
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
    let p = write_dense_shard(tmp.path(), "b.db", 1, 20);
    idx.add_unsynced_shard(&p, 99).unwrap();
    idx.run_compact().unwrap();
    assert!(idx.levels[0].guards.is_empty(), "the drain emptied L1");

    let (dehy, hyd) = terminal_split(&idx);
    assert!(hyd.is_empty() && !dehy.is_empty(), "the derived rule kept it skeleton");
}

/// `vertical_fold` bounds its destination range by the source guard's true
/// key extent, not by the gap to the next L1 guard key — which is the top of
/// the key space for the last (or only) guard and would rewrite the whole
/// terminal level on every spill.
#[test]
fn vertical_fold_touches_only_the_guards_its_extent_overlaps() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::new(1, tmp.path().to_str().unwrap(), schema, ShardBudget::Unbounded, false);
    // Three well-separated terminal bands, each too big for the rebalance to
    // merge into its neighbour or to split.
    for (i, base) in [1u64, 10_000, 20_000].into_iter().enumerate() {
        let (p, _) = write_stable_shard(tmp.path(), &format!("t{i}.db"), base);
        seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(base), &p, 10);
    }
    let names: Vec<String> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| g.entries[0].filename.clone())
        .collect();

    // The only L1 guard, so a destination range derived from the gap to the
    // next L1 guard key would run to the top of the key space and rewrite all
    // three.
    let p = write_test_shard(tmp.path(), "late.db", &[2, 3], &[2, 3]);
    seed_guard(&mut idx, 0, gk(2), &p, 50);
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
