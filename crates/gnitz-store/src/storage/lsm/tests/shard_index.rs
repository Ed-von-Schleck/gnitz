use super::super::manifest;
use super::*;
use crate::schema::key::probe_key;
use crate::schema::{SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode};
use crate::storage::repr::batch::Batch;
use crate::storage::repr::shard_file;
use crate::storage::BatchBuilder;
use crate::test_support::{
    make_batch_opk, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64, opk_pk, pk_payload_schema,
};

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
        let opk = self.schema.opk_key(&key.to_le_bytes());
        let filter_key = crate::schema::key::probe_key(opk.pk_bytes());
        self.find_pk_bytes(opk.pk_bytes(), filter_key, visitor);
    }
}

/// Reopen the store at `dir` from its published manifest, as `Table::new` does.
fn reopen(dir: &str, schema: SchemaDescriptor) -> ShardIndex {
    let shards = manifest::read(dir).unwrap().map(|m| m.shards);
    ShardIndex::open(dir, schema, ShardBudget::Unbounded, false, shards.as_ref()).unwrap()
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
    pk_payload_schema(&[TypeCode::U64, TypeCode::U64])
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

/// A `(U64 PK | I64 payload)` batch at weight 1.
fn test_batch(pks: &[u64], values: &[i64]) -> Batch {
    let rows: Vec<(u64, i64, i64)> = pks.iter().zip(values).map(|(&p, &v)| (p, 1, v)).collect();
    make_batch_raw(&make_schema_u64_i64(), &rows)
}

/// [`test_batch`] written to `dir/name`, outside any index.
fn write_test_shard(dir: &std::path::Path, name: &str, pks: &[u64], values: &[i64]) -> String {
    let path = dir.join(name).to_str().unwrap().to_owned();
    test_batch(pks, values)
        .write_as_shard(&path, shard_file::ShardWriteOpts::default())
        .unwrap();
    path
}

/// A `(U64 PK | I64)` batch of `n` dense keys from `base`, payload = key.
fn dense_batch(base: u64, n: u64) -> Batch {
    let pks: Vec<u64> = (base..base + n).collect();
    let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
    test_batch(&pks, &vals)
}

/// A batch of `n` rows carrying a `width`-byte STRING payload. Ints pack, so a
/// dense integer shard shrinks below the guard target the moment it is folded;
/// a string payload comes out of the fold as fat as it went in, which is what
/// lets a guard still be over target after one fold.
fn fat_batch(base: u64, n: u64, width: usize) -> Batch {
    let schema = make_schema_pk_u64_payload_string();
    let mut b = BatchBuilder::new(schema);
    for pk in base..base + n {
        // Vary the body per row so the text carries no run the writer can fold.
        let body: Vec<u8> = (0..width).map(|i| b'a' + ((pk as usize + i) % 26) as u8).collect();
        b.begin_row(pk as u128, 1);
        b.put_blob(&body);
        b.end_row();
    }
    b.finish()
}

/// Rows in a shard the guard rebalance neither splits nor merges.
const STABLE_ROWS: u64 = 2800;

fn assert_stable(len: u64) {
    assert!(
        (MIN_GUARD_BYTES / 2..MIN_GUARD_BYTES).contains(&len),
        "a stable shard must sit between the merge and split thresholds, got {len} B",
    );
}

/// Write `batch` as a published entry of `level_idx`'s guard `key`, creating it,
/// stamped `stamp`.
fn seed_guard(idx: &mut ShardIndex, level_idx: usize, key: PkBuf, batch: &Batch, stamp: u64) {
    let entry = idx
        .write_shard(batch, shard_file::ShardWriteOpts::default(), Some(stamp))
        .unwrap();
    idx.levels[level_idx].get_or_create_guard(key).entries.push(entry);
    idx.mark_published();
}

/// [`seed_guard`] of a stable shard of keys `base..`; answers them.
fn seed_stable(idx: &mut ShardIndex, level_idx: usize, key: PkBuf, base: u64, stamp: u64) -> Vec<u64> {
    seed_guard(idx, level_idx, key, &dense_batch(base, STABLE_ROWS), stamp);
    assert_stable(
        idx.levels[level_idx]
            .get_or_create_guard(key)
            .entries
            .last()
            .unwrap()
            .shard
            .file_len(),
    );
    (base..base + STABLE_ROWS).collect()
}

/// Append a stable shard of keys `base..` to L0; answers its bytes.
fn append_stable(idx: &mut ShardIndex, base: u64) -> u64 {
    idx.append_l0_run(&dense_batch(base, STABLE_ROWS)).unwrap();
    let len = idx.l0.last().unwrap().shard.file_len();
    assert_stable(len);
    len
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

/// Rename the index's manifest into place, as the barrier does minus the
/// fsyncs, leaving the retired shards on disk.
fn rename_manifest(idx: &mut ShardIndex) {
    let bytes = manifest::encode(&manifest::Manifest {
        checkpoint_mark: 0,
        caller_record: Vec::new(),
        shards: idx.shard_set(),
    });
    manifest::prepare(&idx.output_dir, &bytes).unwrap();
    manifest::commit(&idx.output_dir).unwrap();
    idx.mark_published();
}

/// Publish the index's manifest as the barrier does, minus the fsyncs.
fn publish_manifest(idx: &mut ShardIndex) {
    rename_manifest(idx);
    idx.unlink_retired();
}

#[test]
fn test_append_l0_run_and_find_pk() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    idx.append_l0_run(&test_batch(&[10, 20, 30], &[100, 200, 300])).unwrap();
    idx.append_l0_run(&test_batch(&[25, 35, 40], &[250, 350, 400])).unwrap();

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
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // Add enough shards to trigger compaction to L1
    for i in 0..5u64 {
        let pk = i * 10 + 1;
        idx.append_l0_run(&test_batch(&[pk], &[pk as i64 * 100])).unwrap();
    }
    idx.run_compact().unwrap();

    // Publish manifest
    publish_manifest(&mut idx);

    // Load into a fresh index
    let mut idx2 = reopen(dir.path().to_str().unwrap(), schema);

    // Every key is findable in the reloaded index.
    assert_all_found(&idx2, (0..5u64).map(|i| i * 10 + 1));

    assert_eq!(idx.shard_set(), idx2.shard_set());

    // A write after the reload takes a name no live shard holds.
    idx2.append_l0_run(&test_batch(&[99], &[990])).unwrap();
    let mut names: Vec<u64> = idx2.all_entries().map(|e| e.seq).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), idx2.shard_count());
    assert_all_found(&idx2, (0..5u64).map(|i| i * 10 + 1).chain([99]));
}

#[test]
fn test_run_compact_l0_to_l1() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // Add > L0_COMPACT_THRESHOLD shards
    let mut all_pks = Vec::new();
    for i in 0..5u64 {
        let pk = (i + 1) * 10;
        idx.append_l0_run(&test_batch(&[pk], &[pk as i64])).unwrap();
        all_pks.push(pk);
    }

    assert!(idx.l0.len() > L0_COMPACT_THRESHOLD);
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
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // Manually populate L1 with > GUARD_FILE_THRESHOLD entries in one guard
    let mut all_pks = Vec::new();
    for i in 0..6u64 {
        let pk = i + 1;
        seed_guard(&mut idx, 0, gk(0), &test_batch(&[pk], &[pk as i64 * 10]), 100);
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
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // Two destination guards over disjoint key bands, each large enough that
    // the trailing rebalance would neither merge nor split them.
    let mut dest_pks = Vec::new();
    for (base, key) in [(200u64, gk(100)), (100_100, gk(100_000))] {
        dest_pks.extend(seed_stable(&mut idx, 1, key, base, 80));
    }
    // One L1 guard spanning both of them.
    let src_pks: Vec<u64> = vec![100, 150, 100_500, 100_550];
    for &pk in &src_pks {
        seed_guard(&mut idx, 0, gk(100), &test_batch(&[pk], &[pk as i64]), 100);
    }

    let (split_outputs, first_band_fold) = (2, 1);
    let second_band_fold = idx.shard_seq + split_outputs + first_band_fold + 1;
    let blocker = dir.path().join(manifest::shard_name(second_band_fold));
    std::fs::create_dir_all(&blocker).unwrap();

    let hi_dest_seq = idx.levels[1].guards[1].entries[0].seq;
    assert!(idx.vertical_fold(0).is_err(), "the second band cannot write");

    assert_eq!(idx.levels[0].guards.len(), 1, "only the failed band is left in L1");
    assert_eq!(
        idx.levels[0].guards[0].guard_key,
        gk(100_000),
        "the failed band keeps its own source guard",
    );
    assert_eq!(
        idx.levels[1].guards[1].entries[0].seq, hi_dest_seq,
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
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // L1 already has a guard at key 100 (keys 100, 200).
    seed_guard(&mut idx, 0, gk(100), &test_batch(&[100, 200], &[1000, 2000]), 1);

    // Insert 5 L0 shards (> L0_COMPACT_THRESHOLD) with keys all below 100.
    let low_keys = [50u64, 60, 70, 80, 90];
    for &k in &low_keys {
        idx.append_l0_run(&test_batch(&[k], &[k as i64 * 10])).unwrap();
    }
    assert!(idx.l0.len() > L0_COMPACT_THRESHOLD);
    idx.run_compact().unwrap();

    // Every below-first-guard key must be findable, plus the original L1 keys.
    assert_all_found(&idx, low_keys.iter().copied().chain([100u64, 200]));
}

#[test]
fn test_find_guards_for_range() {
    let range = |l: &FLSMLevel, lo: u64, hi: u64| l.find_guards_for_range(gk(lo).pk_bytes(), gk(hi).pk_bytes());

    let mut level = FLSMLevel::default();
    // Guards at keys 0, 100, 200, 300
    for k in [0u64, 100, 200, 300] {
        level.guards.push(LevelGuard { guard_key: gk(k), entries: Vec::new() });
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
    let mut above_zero = FLSMLevel::default();
    for k in [100u64, 200] {
        above_zero
            .guards
            .push(LevelGuard { guard_key: gk(k), entries: Vec::new() });
    }
    assert_eq!(range(&above_zero, 10, 50), 0..1);

    // No guards at all
    let empty = FLSMLevel::default();
    assert!(range(&empty, 0, 100).is_empty());
}

/// Spills and compaction outputs stay unsynced until published; consumed
/// unpublished inputs are unlinked and leave the set.
#[test]
fn test_unsynced_tracking_register_prune_clear() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    assert!(idx.unsynced_paths().next().is_none());
    for i in 0..5u64 {
        let pk = (i + 1) * 10;
        idx.append_l0_run(&test_batch(&[pk], &[pk as i64])).unwrap();
    }
    let spills: Vec<String> = idx
        .l0
        .iter()
        .map(|e| manifest::shard_path(&idx.output_dir, e.seq))
        .collect();
    assert_eq!(idx.unsynced_paths().count(), 5, "registration marks, on its own");

    assert!(idx.l0.len() > L0_COMPACT_THRESHOLD);
    idx.run_compact().unwrap();
    for p in &spills {
        assert!(!std::path::Path::new(p).exists(), "unpublished input {p} is unlinked");
        assert!(
            !idx.unsynced_paths().any(|q| q == *p),
            "consumed input {p} must leave the unsynced set"
        );
    }
    assert!(
        idx.unsynced_paths().next().is_some(),
        "the compaction outputs are themselves unsynced until a barrier publishes them"
    );

    idx.mark_published();
    assert!(idx.unsynced_paths().next().is_none());
}

/// A manifest reload and a widening rebind write no shard, so leave none unsynced.
#[test]
fn reload_and_widen_leave_nothing_unsynced() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    for i in 0..3u64 {
        idx.append_l0_run(&test_batch(&[i * 10 + 1], &[i as i64])).unwrap();
    }
    publish_manifest(&mut idx);

    let mut idx = reopen(dir.path().to_str().unwrap(), schema);
    assert!(idx.unsynced_paths().next().is_none(), "a manifest names durable files");

    let wide = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    idx.swap_schema(wide).unwrap();
    assert!(
        idx.all_entries()
            .all(|e| gnitz_wire::null_word_get(e.shard.get_null_word(0), 1)),
        "a payload widen rebinds every shard: the appended column reads NULL"
    );
    assert!(idx.unsynced_paths().next().is_none(), "a rebind moves no durability");
}

/// A schema change that keeps the payload arity — `DROP NOT NULL` — still
/// rebinds every shard, so what a shard slices out carries the new schema.
#[test]
fn a_nullability_change_rebinds_every_shard() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    for i in 0..3u64 {
        idx.append_l0_run(&test_batch(&[i * 10 + 1], &[i as i64])).unwrap();
    }
    let nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    assert!(nullable != schema && nullable.num_payload_cols() == schema.num_payload_cols());
    idx.swap_schema(nullable).unwrap();
    assert!(
        idx.all_entries()
            .all(|e| *e.shard.slice_to_owned_batch(0, 1).schema() == nullable),
        "every shard is bound to the published schema"
    );
}

/// A compaction that cannot write its output leaves L0 exactly as it was, so
/// the next trigger retries against intact inputs.
#[test]
fn test_run_compact_failure_leaves_l0_intact() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // Add L0_COMPACT_THRESHOLD + 1 shards (triggers compaction).
    let mut all_pks = Vec::new();
    for i in 0..5u64 {
        let pk = (i + 1) * 10;
        idx.append_l0_run(&test_batch(&[pk], &[pk as i64])).unwrap();
        all_pks.push(pk);
    }

    // A directory at the next seq's name: the output shard cannot be created.
    std::fs::create_dir_all(dir.path().join(manifest::shard_name(idx.shard_seq + 1))).unwrap();
    let l0_before = idx.l0.len();

    let result = idx.run_compact();
    assert!(result.is_err(), "expected Err when the output shard cannot be written");

    assert_eq!(idx.l0.len(), l0_before, "L0 must not be modified on failure");
    assert!(idx.levels.iter().all(|l| l.guards.is_empty()), "nothing was registered");

    // All original keys must still be findable via L0.
    assert_all_found(&idx, all_pks.iter().copied());
}

/// A compaction whose second output cannot be written unlinks the first, which
/// it had already written and opened, and registers neither.
#[test]
fn a_compaction_failing_past_its_first_output_leaves_no_output_behind() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // One L1 guard key per L0 run, so the fold writes two outputs.
    idx.append_l0_run(&test_batch(&[10, 50], &[1, 1])).unwrap();
    idx.append_l0_run(&test_batch(&[150, 250], &[1, 1])).unwrap();
    let inputs: Vec<String> = idx
        .l0
        .iter()
        .map(|e| manifest::shard_path(&idx.output_dir, e.seq))
        .collect();

    let first = dir.path().join(manifest::shard_name(idx.shard_seq + 1));
    std::fs::create_dir_all(dir.path().join(manifest::shard_name(idx.shard_seq + 2))).unwrap();

    assert!(idx.run_compact().is_err(), "the second output cannot be written");
    assert!(!first.exists(), "the first output was unlinked");
    assert_eq!(idx.l0.len(), 2, "both inputs stay registered");
    assert!(inputs.iter().all(|p| std::path::Path::new(p).exists()));
    assert!(idx.levels.iter().all(|l| l.guards.is_empty()), "nothing was registered");
}

/// An L1 guard at key=100 folded into an L2 that starts at key=200 must
/// route the keys below 200 — the source range's lower bound — or 100..199
/// become unfindable.
#[test]
fn a_vertical_does_not_lose_keys_below_the_destination_guard() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // L1 guard at key=100: 5 shards (> GUARD_FILE_THRESHOLD=4) with keys in [100, 199]
    let src_pks: Vec<u64> = vec![100, 120, 140, 160, 180];
    for &pk in &src_pks {
        seed_guard(&mut idx, 0, gk(100), &test_batch(&[pk], &[pk as i64 * 10]), 100);
    }

    // L1 guard at key=500: 1 shard
    seed_guard(&mut idx, 0, gk(500), &test_batch(&[500], &[5000]), 50);

    // L2 guard at key=200: 1 shard with key=250
    seed_guard(&mut idx, 1, gk(200), &test_batch(&[250], &[2500]), 80);

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
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // L2 guard at key 200, holding keys on both sides of it.
    let dest_pks = [50u64, 150, 250];
    let vals: Vec<i64> = dest_pks.iter().map(|&p| p as i64).collect();
    seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(200), &test_batch(&dest_pks, &vals), 80);

    // L1 guard whose whole extent is below the destination's key.
    let src_pks = [100u64, 180];
    let vals: Vec<i64> = src_pks.iter().map(|&p| p as i64).collect();
    seed_guard(&mut idx, 0, gk(100), &test_batch(&src_pks, &vals), 100);

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
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    let mut dest_pks = Vec::new();
    for (base, key) in [(200u64, gk(100)), (100_100, gk(100_000))] {
        dest_pks.extend(seed_stable(&mut idx, TERMINAL_LEVEL_IDX, key, base, 80));
    }
    let before: Vec<(PkBuf, u64)> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| (g.guard_key, g.entries[0].seq))
        .collect();

    let src_pks = [100u64, 150, 100_500, 100_550];
    let vals: Vec<i64> = src_pks.iter().map(|&p| p as i64).collect();
    seed_guard(&mut idx, 0, gk(100), &test_batch(&src_pks, &vals), 100);

    idx.vertical_fold(0).unwrap();

    assert!(idx.levels[0].guards.is_empty(), "every band went down");
    let after: Vec<(PkBuf, u64)> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| (g.guard_key, g.entries[0].seq))
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

/// Verticals into disjoint destination guards at the same stamp name their
/// outputs apart, so neither rename clobbers the other's live shard.
#[test]
fn test_vertical_disjoint_guards_no_name_collision() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // Key 250 routes to the gk(100) bucket; 6000 to the gk(5000) bucket.
    // L1 guard gk(100): two entries (keys 100, 110).
    for k in [100u64, 110] {
        seed_guard(&mut idx, 0, gk(100), &test_batch(&[k], &[k as i64]), 100);
    }
    // L1 guard gk(5000): two entries (keys 5000, 5010).
    for k in [5000u64, 5010] {
        seed_guard(&mut idx, 0, gk(5000), &test_batch(&[k], &[k as i64]), 100);
    }
    // L2 pre-seed: guard gk(100) (keys 250…) and guard gk(5000) (keys 6000…)
    // at the same stamp. Stable-sized, so the trailing rebalance keeps them
    // two guards.
    let mut l2_pks = Vec::new();
    for (base, key) in [(250u64, gk(100)), (6000, gk(5000))] {
        l2_pks.push(seed_stable(&mut idx, 1, key, base, 100));
    }

    // Call 1 folds L1 guard 5000 → L2 guard 5000; call 2 folds L1 guard 100 →
    // L2 guard 100 (disjoint destinations, both stamped 100).
    idx.vertical_fold(1).unwrap();
    idx.vertical_fold(0).unwrap();

    // Both destination guards reference distinct, existing files.
    let files: Vec<String> = idx.levels[1]
        .guards
        .iter()
        .flat_map(|g| g.entries.iter().map(|e| manifest::shard_path(&idx.output_dir, e.seq)))
        .collect();
    assert_eq!(files.len(), 2, "two L2 guards, one entry each");
    assert_ne!(files[0], files[1], "disjoint-guard outputs must not share a name");
    for f in &files {
        assert!(std::path::Path::new(f).exists(), "output shard {f} missing");
    }

    // Publish + reload into a fresh index: every key must survive.
    publish_manifest(&mut idx);
    let idx2 = reopen(dir.path().to_str().unwrap(), schema);
    let l2_ends = l2_pks.iter().flat_map(|pks| [pks[0], *pks.last().unwrap()]);
    assert_all_found(&idx2, [100u64, 110, 5000, 5010].into_iter().chain(l2_ends));
}

/// Two folds into the same destination guard write distinct files, so
/// `unlink_retired` removes the first fold's output and keeps the live one.
#[test]
fn test_vertical_same_guard_recompaction_unlink_retired_keeps_live() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();

    // L2 guard gk(100) pre-seeded with key 250.
    seed_guard(&mut idx, 1, gk(100), &test_batch(&[250], &[2500]), 100);
    // L1 guard gk(100): two entries (keys 100, 110).
    for k in [100u64, 110] {
        seed_guard(&mut idx, 0, gk(100), &test_batch(&[k], &[k as i64]), 100);
    }
    idx.vertical_fold(0).unwrap();

    // Re-add L1 guard gk(100) with two more entries (keys 120, 130) and
    // re-compact into the same destination guard.
    for k in [120u64, 130] {
        seed_guard(&mut idx, 0, gk(100), &test_batch(&[k], &[k as i64]), 100);
    }
    idx.vertical_fold(0).unwrap();

    publish_manifest(&mut idx);
    let live = manifest::shard_path(&idx.output_dir, idx.levels[1].guards[0].entries[0].seq);
    assert!(
        std::path::Path::new(&live).exists(),
        "unlink_retired deleted the live L2 shard {live}",
    );

    // Reload: every key survives.
    let idx2 = reopen(dir.path().to_str().unwrap(), schema);
    assert_all_found(&idx2, [100u64, 110, 120, 130, 250]);
}

/// `open` unlinks exactly the shard and staging files its manifest does not name.
#[test]
fn open_removes_exactly_the_unreferenced_files() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().to_str().unwrap();
    let mut idx = ShardIndex::open(d, make_schema_u64_i64(), ShardBudget::Unbounded, false, None).unwrap();
    idx.append_l0_run(&test_batch(&[10], &[100])).unwrap();
    publish_manifest(&mut idx);
    let live = manifest::shard_name(idx.shard_seq);

    for seq in [99, 7] {
        write_test_shard(dir.path(), &manifest::shard_name(seq), &[seq], &[1]);
    }
    std::fs::write(dir.path().join("manifest.bin.tmp"), b"x").unwrap();
    std::fs::write(dir.path().join("other"), b"x").unwrap();

    let idx = reopen(d, make_schema_u64_i64());
    assert_all_found(&idx, [10]);
    let mut survivors: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    survivors.sort();
    let mut want = [live, "manifest.bin".to_string(), "other".to_string()];
    want.sort();
    assert_eq!(survivors, want);
}

/// With no manifest, every shard in the directory is an orphan.
#[test]
fn open_of_no_manifest_removes_every_shard() {
    let dir = tempfile::tempdir().unwrap();
    let stray = dir.path().join(manifest::shard_name(7));
    std::fs::write(&stray, b"orphan").unwrap();
    ShardIndex::open(
        dir.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    assert!(!stray.exists());
}

/// Golden values for the single-PK probe range gate.
#[test]
fn test_single_pk_probe_golden() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();

    let d = dir.path().to_str().unwrap();
    write_test_shard(dir.path(), &manifest::shard_name(1), &[10, 20], &[1, 2]);
    write_test_shard(dir.path(), &manifest::shard_name(2), &[30, 40], &[3, 4]);
    let e_lo = ShardEntry::open(d, 1, &schema, 1).unwrap();
    let e_hi = ShardEntry::open(d, 2, &schema, 2).unwrap();

    // Range gate: in-range key passes (and resolves), out-of-range
    // key is pruned. OPK for a U64 PK is the value's big-endian bytes.
    assert!(probe(&e_lo, &10u64.to_be_bytes()).is_some());
    assert!(probe(&e_lo, &20u64.to_be_bytes()).is_some());
    assert!(probe(&e_lo, &25u64.to_be_bytes()).is_none(), "25 outside [10,20]");
    assert!(probe(&e_hi, &5u64.to_be_bytes()).is_none(), "5 below [30,40]");
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
    write_compound_shard(
        dir.path(),
        &manifest::shard_name(1),
        &[(1, 5), (1, 9), (2, 3)],
        &[10, 20, 30],
    );
    let entry = ShardEntry::open(dir.path().to_str().unwrap(), 1, &schema, 1).unwrap();
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    seed_guard(&mut idx, 0, gk(1), &dense_batch(1, OVER_TARGET_ROWS), 1);

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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    // Guard 0 keyed above most of what it holds — the shape a spill below the
    // partition's floor leaves behind.
    let key = gk(OVER_TARGET_ROWS * 4 / 5);
    seed_guard(&mut idx, 0, key, &dense_batch(1, OVER_TARGET_ROWS), 1);

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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    // 9000 rows of one PK at distinct payloads — a legal intermediate batch
    // shape, and past the target even with the PK region as compressible as
    // it gets.
    let pks = vec![7u64; 9000];
    let vals: Vec<i64> = (0..9000).collect();
    seed_guard(&mut idx, 0, gk(7), &test_batch(&pks, &vals), 1);

    let target = idx.guard_target_bytes(0);
    assert!(idx.levels[0].guards[0].bytes() > target, "premise: over target");
    assert_eq!(idx.levels[0].guards[0].fold_destinations(target), vec![gk(7)]);

    let before = idx.levels[0].guards[0].entries[0].seq;
    idx.split_overfull_guards(0).unwrap();
    idx.split_overfull_guards(0).unwrap();
    assert_eq!(idx.levels[0].guards.len(), 1);
    assert_eq!(
        idx.levels[0].guards[0].entries[0].seq, before,
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
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
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
    let rows: Vec<(&[u8], i64, i64)> = rows.iter().map(|(pk, w, v)| (pk.as_slice(), *w, *v)).collect();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    seed_guard(&mut idx, 0, PkBuf::zeroed(24), &make_batch_opk(&schema, &rows), 1);
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
    pk_payload_schema(&vec![TypeCode::U64; pk_cols])
}

/// Row `i`'s key over `stride_schema(pk_cols)`: ascending in the **last** PK
/// column, so at every stride but 8 the keys agree on their leading bytes and
/// differ only in the trailing ones — past byte 16 for `pk_cols >= 3`.
fn trailing_gk(pk_cols: usize, i: u64) -> PkBuf {
    let mut pk = vec![0u8; (pk_cols - 1) * 8];
    pk.extend_from_slice(&i.to_be_bytes());
    PkBuf::from_bytes(&pk)
}

/// One batch of rows `base..base + n` at [`trailing_gk`]'s keys.
fn trailing_key_batch(pk_cols: usize, base: u64, n: u64) -> Batch {
    let rows: Vec<_> = (base..base + n)
        .map(|i| (trailing_gk(pk_cols, i).pk_bytes().to_vec(), 1, i as i64))
        .collect();
    make_batch_opk(&stride_schema(pk_cols), &rows)
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
        let mut idx = ShardIndex::open(
            tmp.path().to_str().unwrap(),
            schema,
            ShardBudget::Unbounded,
            false,
            None,
        )
        .unwrap();
        let batch = trailing_key_batch(pk_cols, 1, OVER_TARGET_ROWS);
        seed_guard(&mut idx, 0, trailing_gk(pk_cols, 1), &batch, 1);

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
        let mut idx = ShardIndex::open(
            tmp.path().to_str().unwrap(),
            schema,
            ShardBudget::Unbounded,
            false,
            None,
        )
        .unwrap();
        for i in 0..4u64 {
            let base = 1 + i * 1000;
            let batch = trailing_key_batch(pk_cols, base, 100);
            seed_guard(&mut idx, 0, trailing_gk(pk_cols, base), &batch, i + 1);
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_pk_u64_payload_string(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    let rows = 4_400u64;
    seed_guard(&mut idx, 0, gk(1), &fat_batch(1, rows, 1000), 1);
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    // Five entries, so the file threshold fires: two insert/retract pairs and
    // one more insert that its own pair cancels.
    for i in 0..5u64 {
        let w = if i % 2 == 0 { 1 } else { -1 };
        let rows: Vec<(u64, i64, i64)> = (0..4u64).map(|k| (k, w, k as i64)).collect();
        seed_guard(&mut idx, 0, gk(0), &make_batch_raw(&schema, &rows), i + 1);
    }
    // Weights sum to +1 per key over five entries, so one more retraction
    // takes every key to zero.
    let rows: Vec<(u64, i64, i64)> = (0..4u64).map(|k| (k, -1, k as i64)).collect();
    seed_guard(&mut idx, 0, gk(0), &make_batch_raw(&schema, &rows), 6);

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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    for i in 0..4u64 {
        let base = 1 + i * 1000;
        seed_guard(&mut idx, 0, gk(base), &dense_batch(base, 200), i + 1);
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

    let after = idx.levels[0].guards[0].entries[0].seq;
    idx.split_overfull_guards(0).unwrap();
    assert_eq!(
        idx.levels[0].guards[0].entries[0].seq, after,
        "the merged guard is below the split trigger",
    );
}

/// A run breaks at a change of representation: folding a hydrated guard
/// together with a dehydrated one would evict it.
#[test]
fn a_merge_run_does_not_cross_a_representation_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    for (i, base) in [1u64, 1000].into_iter().enumerate() {
        seed_guard(
            &mut idx,
            TERMINAL_LEVEL_IDX,
            gk(base),
            &dense_batch(base, 200),
            i as u64 + 1,
        );
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    seed_guard(
        &mut idx,
        TERMINAL_LEVEL_IDX,
        gk(1),
        &dense_batch(1, OVER_TARGET_ROWS),
        1,
    );
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    let rows = 8_000u64;
    seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(1), &dense_batch(1, rows), 1);
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

    let names: Vec<u64> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| g.entries[0].seq)
        .collect();
    idx.split_overfull_guards(TERMINAL_LEVEL_IDX).unwrap();
    let after: Vec<u64> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| g.entries[0].seq)
        .collect();
    assert_eq!(names, after, "the trigger cleared");
}

/// The guard count tracks the level's bytes in both directions — splitting
/// alone would make it a high-water mark.
#[test]
fn the_guard_count_comes_back_down_after_the_bytes_do() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    // Eight rows per key: dehydration folds each key to one `(PK, Σweight)`
    // row, which is the order-of-magnitude shrink the merge pass exists for.
    let keys = 2_500u64;
    let pks: Vec<u64> = (1..=keys).flat_map(|k| std::iter::repeat_n(k, 8)).collect();
    let vals: Vec<i64> = (0..pks.len() as i64).collect();
    seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(1), &test_batch(&pks, &vals), 1);
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

/// A range open takes only the shards whose extent meets the range.
#[test]
fn a_range_gather_visits_only_the_guards_that_can_own_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    for i in 0..4u64 {
        let base = 1 + i * 1000;
        seed_guard(&mut idx, 0, gk(base), &dense_batch(base, 200), i + 1);
    }
    let count = |lo: u64, hi: Option<u64>| {
        idx.shard_arcs_in_range(gk(lo), hi.map_or_else(|| PkBuf::max(8), gk))
            .count()
    };

    assert_eq!(idx.all_shard_arcs_iter().count(), 4);
    assert_eq!(count(1100, Some(1100)), 1, "a point read routes to one guard");
    assert_eq!(count(1100, Some(2100)), 2, "a range takes the run it spans");
    assert_eq!(count(0, None), 4, "an open end takes the rest of the key space");
    assert_eq!(count(1500, Some(1500)), 0, "a guard's shard is rejected by its extent");
    assert_eq!(count(1500, Some(2100)), 1, "an edge guard is rejected by its extent");
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    assert_eq!(idx.l0_run_bytes, MIN_GUARD_BYTES, "before any fold");

    let mut folded = 0u64;
    for i in 0..5u64 {
        folded += append_stable(&mut idx, 1 + i * 10_000);
    }
    idx.run_compact().unwrap();
    assert_eq!(idx.l0_run_bytes, folded, "the fold this store actually performed");
    assert_eq!(idx.guard_target_bytes(0), folded, "every unevicted level takes R");

    for i in 0..5u64 {
        idx.append_l0_run(&dense_batch(500_000 + i * 100, 10)).unwrap();
    }
    idx.run_compact().unwrap();
    assert_eq!(
        idx.l0_run_bytes, folded,
        "a running max never shrinks under a small fold"
    );

    // A guard can outgrow `R` — one of a single distinct key cannot be cut — so
    // the largest guard is not the unit a reload may recover.
    seed_guard(
        &mut idx,
        TERMINAL_LEVEL_IDX,
        gk(10_000_000),
        &dense_batch(10_000_000, 50_000),
        200,
    );
    assert!(
        idx.levels.iter().flat_map(|l| &l.guards).any(|g| g.bytes() > folded),
        "premise: a guard larger than R"
    );
    publish_manifest(&mut idx);
    let reloaded = reopen(tmp.path().to_str().unwrap(), make_schema_u64_i64());
    assert_eq!(reloaded.l0_run_bytes, folded, "R survives a restart as it was observed");
}

/// The terminal level of a budgeted store takes one sweep step, clamped into
/// `[MIN_GUARD_BYTES, R]` because `parse_size` admits any positive `u64`.
#[test]
fn a_budgeted_terminal_target_is_an_eighth_of_the_budget_within_the_clamp() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    for i in 0..5u64 {
        append_stable(&mut idx, 1 + i * 10_000);
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

/// An index holding `n` L0 shards of 40 distinct keys each, stamped with
/// ascending seqs so write-recency victim ordering is observable.
fn index_with_l0(dir: &std::path::Path, n: u64) -> ShardIndex {
    let mut idx = ShardIndex::open(
        dir.to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    for s in 0..n {
        idx.append_l0_run(&dense_batch(s * 1000 + 1, 40)).unwrap();
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
    let live: usize = idx.all_entries().map(|e| e.shard.row_count()).sum();
    assert_eq!(live, 4 * 40, "every row survived the folds");
}

#[test]
fn a_superseded_shard_waits_for_the_barrier_only_if_a_manifest_names_it() {
    let tmp = tempfile::tempdir().unwrap();
    let files = |idx: &ShardIndex| {
        let mut f: Vec<String> = idx
            .all_entries()
            .map(|e| manifest::shard_path(&idx.output_dir, e.seq))
            .collect();
        f.sort();
        f
    };
    let exists = |p: &String| std::path::Path::new(p).exists();

    let dir = tmp.path().join("published");
    std::fs::create_dir_all(&dir).unwrap();
    publish_manifest(&mut index_with_l0(&dir, 5));
    let mut idx = reopen(dir.to_str().unwrap(), make_schema_u64_i64());
    let inputs = files(&idx);
    assert_eq!(inputs.len(), 5);
    idx.run_compact().unwrap();
    assert!(inputs.iter().all(exists), "every published input waits for the barrier");
    publish_manifest(&mut idx);
    assert!(!inputs.iter().any(exists), "and the post-publish drain removes it");

    let dir = tmp.path().join("unpublished");
    std::fs::create_dir_all(&dir).unwrap();
    let mut idx = index_with_l0(&dir, 5);
    let inputs = files(&idx);
    idx.run_compact().unwrap();
    assert!(!inputs.iter().any(exists), "every unpublished input is gone at once");
    let outputs = files(&idx);
    assert!(
        !outputs.is_empty() && outputs.iter().all(exists),
        "the outputs are present"
    );
}

/// A barrier that renames its manifest and fails before the directory fsync
/// keeps the superseded inputs until a later barrier's drain.
#[test]
fn retired_shards_outlive_a_rename_until_the_drain() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = index_with_l0(tmp.path(), 5);
    publish_manifest(&mut idx);
    let inputs: Vec<String> = idx
        .all_entries()
        .map(|e| manifest::shard_path(&idx.output_dir, e.seq))
        .collect();
    idx.run_compact().unwrap();

    rename_manifest(&mut idx);
    assert!(
        inputs.iter().all(|p| std::path::Path::new(p).exists()),
        "a rename alone unlinks no superseded input"
    );

    idx.unlink_retired();
    assert!(
        !inputs.iter().any(|p| std::path::Path::new(p).exists()),
        "the drain unlinks every superseded input"
    );
}

/// A store fed one spill's worth per round, swept every round — the shape a
/// live delta store takes. Its footprint must plateau: what a leak, or a sweep
/// that cannot keep pace, shows up as is a footprint that tracks everything
/// ever written.
#[test]
fn a_swept_delta_store_plateaus_under_a_steady_write_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    idx.set_delta_budget(1);

    let mut early = 0u64;
    for round in 0..60u64 {
        // Ascending keys, as a `_tick`-led delta store's always are.
        idx.append_l0_run(&dense_batch(round * 1000 + 1, 40)).unwrap();
        idx.maintain().unwrap();
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    let mut written: Vec<u64> = Vec::new();
    let add = |idx: &mut ShardIndex, written: &mut Vec<u64>, round: u64| {
        // Ascending and distinct, as a `_tick`-led delta store's keys are, so
        // nothing cancels in a fold and a row count is a faithful census.
        let base = round * 1000 + 1;
        idx.append_l0_run(&dense_batch(base, 40)).unwrap();
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
        idx.maintain().unwrap();
    }

    let floor = idx.dropped_max();
    let retained: usize = idx.all_entries().map(|e| e.shard.row_count()).sum();
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
/// whose newest entry carries the smallest stamp goes first. The row
/// content survives — a skeleton row keeps its key and its summed weight.
#[test]
fn dehydration_takes_the_oldest_written_terminal_guard_first() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    // Three hydrated terminal guards at distinct write recencies, each too
    // big for the rebalance to merge into its neighbour.
    for (i, base) in [1u64, 10_000, 20_000].into_iter().enumerate() {
        seed_stable(&mut idx, TERMINAL_LEVEL_IDX, gk(base), base, 30 - i as u64);
    }
    let (dehy, hyd) = terminal_split(&idx);
    assert!(dehy.is_empty() && hyd.len() >= 2, "several hydrated terminal guards");

    let oldest = *hyd
        .iter()
        .min_by_key(|&&gi| idx.levels[TERMINAL_LEVEL_IDX].guards[gi].newest())
        .unwrap();
    let oldest_key = idx.levels[TERMINAL_LEVEL_IDX].guards[oldest].guard_key;
    let live_before: Vec<(u128, i64)> = {
        let g = &idx.levels[TERMINAL_LEVEL_IDX].guards[oldest];
        let s = &g.entries[0].shard;
        (0..s.row_count()).map(|i| (s.get_pk(i), s.get_weight(i))).collect()
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
    let live_after: Vec<(u128, i64)> = (0..s.row_count()).map(|i| (s.get_pk(i), s.get_weight(i))).collect();
    assert_eq!(live_after, live_before, "keys and coarse weights survive dehydration");
}

/// Ordinary compaction never re-hydrates: a vertical folding hydrated L1 data
/// into an already-dehydrated terminal guard emits skeleton, so the sweep's
/// work is not undone.
#[test]
fn a_dehydrated_guard_stays_dehydrated_under_ordinary_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        make_schema_u64_i64(),
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    // One key band, so every later fold routes back into the same guard.
    for _ in 0..2 {
        idx.append_l0_run(&dense_batch(1, 20)).unwrap();
    }
    idx.run_compact().unwrap();
    idx.vertical_fold(0).unwrap();
    idx.set_capacity(Some(1));
    idx.enforce_capacity().unwrap();
    assert!(terminal_split(&idx).0.contains(&0), "guard 0 is dehydrated");

    // New hydrated data over the same keys, folded down by the ordinary path:
    // under a capacity this tight `run_compact` drains L1 to the terminal
    // level itself.
    idx.append_l0_run(&dense_batch(1, 20)).unwrap();
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
    let mut idx = ShardIndex::open(
        tmp.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    // Three well-separated terminal bands, each too big for the rebalance to
    // merge into its neighbour or to split.
    for base in [1u64, 10_000, 20_000] {
        seed_stable(&mut idx, TERMINAL_LEVEL_IDX, gk(base), base, 10);
    }
    let names: Vec<u64> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| g.entries[0].seq)
        .collect();

    // The only L1 guard, so a destination range derived from the gap to the
    // next L1 guard key would run to the top of the key space and rewrite all
    // three.
    seed_guard(&mut idx, 0, gk(2), &test_batch(&[2, 3], &[2, 3]), 50);
    idx.vertical_fold(0).unwrap();

    let after: Vec<u64> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| g.entries[0].seq)
        .collect();
    assert_eq!(after.len(), 3);
    assert_ne!(after[0], names[0], "the overlapped guard was rewritten");
    assert_eq!(&after[1..], &names[1..], "the untouched guards kept their files");
}

/// A spilled run carrying dead heap bytes is compacted on the way to disk: the
/// shard's heap holds only the spans its rows reference, and every row reads
/// back.
#[test]
fn a_spilled_run_leaves_its_dead_heap_behind() {
    use gnitz_expr::RowSource;
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    let mut idx = ShardIndex::open(
        dir.path().to_str().unwrap(),
        schema,
        ShardBudget::Unbounded,
        false,
        None,
    )
    .unwrap();
    let vals = [[b'a'; 20], [b'b'; 20]];
    let rows: Vec<(u64, i64, &[u8])> = vec![(1, 1, &vals[0]), (2, 1, &vals[1])];
    let mut run = crate::test_support::make_batch_bytes(&schema, &rows);
    run.blob.extend_from_slice(&[0; 10]);
    run.dead_heap = 10;
    idx.append_l0_run(&run).unwrap();

    for (pk, want) in [(1u128, &vals[0]), (2, &vals[1])] {
        let mut hits = Vec::new();
        idx.find_pk(pk, &mut |shard, row| hits.push((shard, row)));
        let (shard, row) = hits.pop().expect("the key was spilled");
        assert_eq!(shard.blob().len(), 40, "only the two referenced spans reach disk");
        assert_eq!(
            gnitz_wire::german_string_content(shard.get_col_ptr(row, 0, 16), shard.blob()),
            want
        );
    }
}
