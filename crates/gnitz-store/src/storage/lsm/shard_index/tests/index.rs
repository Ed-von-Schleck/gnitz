use super::super::manifest;
use super::super::*;
use crate::schema::key::probe_key;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::repr::batch::{Batch, REG_PAYLOAD_START};
use crate::storage::repr::layout::Encoding;
use crate::storage::repr::seek::pk_group_end;
use crate::storage::repr::shard_file::{region_dir, ShardWriteOpts};
use crate::storage::BatchBuilder;
use crate::test_support::{
    make_batch_opk, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_payload_schema,
};
use std::path::Path;

fn open(dir: &Path, schema: SchemaDescriptor, budget: ShardBudget) -> ShardIndex {
    let dir = dir.to_str().unwrap();
    let shards = manifest::read(dir).unwrap().map(|m| m.shards);
    ShardIndex::open(dir, schema, budget, false, shards.as_ref()).unwrap()
}

/// An unbounded store at `dir`, reloaded from its manifest if it has one.
fn fresh(dir: &Path, schema: SchemaDescriptor) -> ShardIndex {
    open(dir, schema, ShardBudget::Unbounded)
}

/// Every file name in `dir`, sorted.
fn dir_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// The shard files physically in `dir` — where a leak shows, since
/// `resident_bytes` counts registered entries only.
fn shard_names(dir: &Path) -> Vec<String> {
    dir_names(dir)
        .into_iter()
        .filter(|n| n.starts_with(manifest::SHARD_PREFIX))
        .collect()
}

/// Guard key for a native u64 PK value — its OPK bytes.
fn gk(v: u64) -> PkBuf {
    PkBuf::from_bytes(&v.to_be_bytes())
}

/// A `(U64 PK | I64 payload)` batch at weight 1.
fn test_batch(pks: &[u64], values: &[i64]) -> Batch {
    let rows: Vec<(u64, i64, i64)> = pks.iter().zip(values).map(|(&p, &v)| (p, 1, v)).collect();
    make_batch_raw(&make_schema_u64_i64(), &rows)
}

/// A `(U64 PK | I64)` batch of `n` dense keys from `base`, payload = key.
fn dense_batch(base: u64, n: u64) -> Batch {
    let pks: Vec<u64> = (base..base + n).collect();
    let vals: Vec<i64> = pks.iter().map(|&p| p as i64).collect();
    test_batch(&pks, &vals)
}

/// `n` rows of a `width`-byte STRING payload, which a fold cannot pack smaller.
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
    let entry = idx.write_shard(batch, ShardWriteOpts::default(), Some(stamp)).unwrap();
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

/// Every key routes to rows that sum to `weight` — the property a guard
/// partition has to keep across every fold.
fn assert_weighs(idx: &ShardIndex, weight: i64, keys: impl IntoIterator<Item = PkBuf>) {
    for k in keys {
        let key = k.pk_bytes();
        let mut sum = 0;
        idx.find_pk_bytes(key, probe_key(key), &mut |shard, start| {
            sum += (start..pk_group_end(&*shard, start))
                .map(|r| shard.get_weight(r))
                .sum::<i64>();
        });
        assert_eq!(sum, weight, "key {key:?} through the guard partition");
    }
}

/// [`assert_weighs`] at weight 1 over native u64 keys.
fn assert_all_found(idx: &ShardIndex, keys: impl IntoIterator<Item = u64>) {
    assert_weighs(idx, 1, keys.into_iter().map(gk));
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

/// `idx` published and reopened under `budget`, which binds only at open.
fn reopened_under(mut idx: ShardIndex, budget: ShardBudget) -> ShardIndex {
    publish_manifest(&mut idx);
    let (dir, schema) = (idx.output_dir.clone(), idx.schema);
    drop(idx);
    open(Path::new(&dir), schema, budget)
}

/// L0 spills stay unsynced until published; a fold unlinks its unpublished
/// inputs and leaves its outputs unsynced; a reload names durable files only,
/// serves every key, and draws fresh seqs past every name it loaded.
#[test]
fn a_fold_and_reload_keep_every_key_and_track_what_is_unsynced() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = fresh(dir.path(), schema);
    let pks: Vec<u64> = (1..=5).map(|i| i * 10).collect();
    for &pk in &pks {
        idx.append_l0_run(&test_batch(&[pk], &[pk as i64])).unwrap();
    }
    let spills: Vec<String> = idx.unsynced_paths().collect();
    assert_eq!(spills.len(), 5);

    idx.run_compact().unwrap();
    assert!(idx.l0.is_empty() && !idx.levels[0].guards.is_empty());
    assert!(
        spills.iter().all(|p| !Path::new(p).exists()),
        "unpublished inputs are unlinked"
    );
    assert!(idx.unsynced_paths().all(|p| !spills.contains(&p)));
    assert!(idx.unsynced_paths().next().is_some(), "the outputs wait for a barrier");
    assert_all_found(&idx, pks.iter().copied());
    assert_weighs(&idx, 0, [gk(99)]);

    publish_manifest(&mut idx);
    let mut idx2 = fresh(dir.path(), schema);
    assert!(idx2.shard_set() == idx.shard_set());
    assert!(idx2.unsynced_paths().next().is_none(), "a manifest names durable files");
    idx2.append_l0_run(&test_batch(&[99], &[990])).unwrap();
    let mut seqs: Vec<u64> = idx2.all_entries().map(|e| e.seq).collect();
    seqs.dedup();
    assert_eq!(
        seqs.len(),
        idx2.shard_count(),
        "a write after the reload takes a fresh name"
    );
    assert_all_found(&idx2, pks.iter().copied().chain([99]));
}

/// A guard over the file threshold folds to one file in place, and one whose
/// rows all cancel is removed rather than left as an entry-less slot.
#[test]
fn guards_over_the_file_threshold_fold_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = fresh(dir.path(), schema);
    for pk in 1..=GUARD_FILE_THRESHOLD as u64 + 1 {
        seed_guard(&mut idx, 0, gk(0), &test_batch(&[pk], &[pk as i64]), 1);
    }
    // Alternating inserts and retractions of the same rows, netting to zero.
    for i in 0..GUARD_FILE_THRESHOLD as i64 + 2 {
        let rows: Vec<(u64, i64, i64)> = (100..104).map(|k| (k, if i % 2 == 0 { 1 } else { -1 }, 0)).collect();
        seed_guard(&mut idx, 0, gk(100), &make_batch_raw(&schema, &rows), 1);
    }

    idx.split_overfull_guards(0).unwrap();
    let guards: Vec<(PkBuf, usize)> = idx.levels[0]
        .guards
        .iter()
        .map(|g| (g.guard_key, g.entries.len()))
        .collect();
    assert_eq!(guards, [(gk(0), 1)]);
    assert_all_found(&idx, 1..=GUARD_FILE_THRESHOLD as u64 + 1);
}

/// A vertical is atomic per band: a failure in band `k` leaves bands `0..k`
/// folded, the failing band untouched, and every key reachable.
#[test]
fn a_failing_vertical_band_leaves_the_bands_before_it_folded() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = fresh(dir.path(), schema);

    // Two destination guards over disjoint key bands, each large enough that
    // the trailing rebalance would neither merge nor split them.
    let mut dest_pks = Vec::new();
    for (base, key) in [(200u64, gk(100)), (100_100, gk(100_000))] {
        dest_pks.extend(seed_stable(&mut idx, 1, key, base, 80));
    }
    // One L1 guard spanning both of them.
    let src_pks: Vec<u64> = vec![100, 150, 100_050, 100_060];
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
    let mut idx = fresh(dir.path(), schema);

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

/// A schema change rebinds every shard — a widen and a `DROP NOT NULL` alike —
/// and moves no durability.
#[test]
fn a_schema_change_rebinds_every_shard() {
    let col = |nullable| SchemaColumn::new(TypeCode::I64, nullable);
    let u64_pk = SchemaColumn::new(TypeCode::U64, false);
    let widened = SchemaDescriptor::new(&[u64_pk, col(false), col(true)], &[0]);
    let nullable = SchemaDescriptor::new(&[u64_pk, col(true)], &[0]);
    for new in [widened, nullable] {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = fresh(dir.path(), make_schema_u64_i64());
        for i in 0..3u64 {
            idx.append_l0_run(&test_batch(&[i * 10 + 1], &[i as i64])).unwrap();
        }
        publish_manifest(&mut idx);
        idx.swap_schema(new).unwrap();
        for e in idx.all_entries() {
            let row = e.shard.slice_to_owned_batch(0, 1);
            assert!(*row.schema() == new);
            let nw = row.get_null_word(0);
            assert!(
                (1..new.num_payload_cols()).all(|pi| gnitz_wire::null_word_get(nw, pi)),
                "appended columns read NULL"
            );
        }
        assert!(idx.unsynced_paths().next().is_none(), "a rebind moves no durability");
    }
}

/// A compaction whose input body fails its checksum writes nothing and retires
/// nothing.
#[test]
fn a_corrupt_input_body_fails_the_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = fresh(dir.path(), make_schema_u64_i64());
    for i in 0..5u64 {
        idx.append_l0_run(&dense_batch(i * 100, 20)).unwrap();
    }
    let before = shard_names(dir.path());
    let third = idx.unsynced_paths().nth(2).unwrap();
    crate::test_support::flip_last_byte_in_place(Path::new(&third));

    assert_eq!(idx.run_compact(), Err(StorageError::Corrupt("body checksum")));
    assert_eq!(idx.l0.len(), 5, "every input stays registered");
    assert_eq!(
        shard_names(dir.path()),
        before,
        "no input retired, no output left behind"
    );
}

/// Only compaction packs integer payloads; an L0 spill stays raw.
#[test]
fn compaction_outputs_pack_and_spills_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = fresh(dir.path(), make_schema_u64_i64());
    let payload_encoding = |e: &ShardEntry| {
        let raw = std::fs::read(manifest::shard_path(dir.path().to_str().unwrap(), e.seq)).unwrap();
        region_dir(&raw, REG_PAYLOAD_START).1
    };
    for i in 0..5u64 {
        idx.append_l0_run(&dense_batch(i * 300, 300)).unwrap();
    }
    assert!(idx.l0.iter().all(|e| payload_encoding(e) == Encoding::Raw));
    idx.run_compact().unwrap();
    assert!(idx.all_entries().all(|e| payload_encoding(e) == Encoding::For));
    assert_all_found(&idx, 0..1500);
}

/// A compaction that cannot write its output leaves L0 exactly as it was, so
/// the next trigger retries against intact inputs.
#[test]
fn test_run_compact_failure_leaves_l0_intact() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = fresh(dir.path(), schema);

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
    let mut idx = fresh(dir.path(), schema);

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
    let mut idx = fresh(dir.path(), schema);

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

    assert_all_found(&idx, [250]);
}

/// A vertical whose source lies below its destination guard's key folds into
/// that guard, which owns the keys below it.
#[test]
fn a_vertical_into_a_guards_lower_tail_does_not_shadow_it() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = fresh(dir.path(), schema);

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
    let mut idx = fresh(dir.path(), schema);

    let mut dest_pks = Vec::new();
    for (base, key) in [(200u64, gk(100)), (100_100, gk(100_000))] {
        dest_pks.extend(seed_stable(&mut idx, TERMINAL_LEVEL_IDX, key, base, 80));
    }
    let before: Vec<(PkBuf, u64)> = idx.levels[TERMINAL_LEVEL_IDX]
        .guards
        .iter()
        .map(|g| (g.guard_key, g.entries[0].seq))
        .collect();

    let src_pks = [100u64, 150, 100_050, 100_060];
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

/// Two folds into the same destination guard write distinct files, so
/// `unlink_retired` removes the first fold's output and keeps the live one.
#[test]
fn test_vertical_same_guard_recompaction_unlink_retired_keeps_live() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = fresh(dir.path(), schema);

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
    assert_eq!(
        shard_names(dir.path()).len(),
        idx.shard_count(),
        "only the live shards remain"
    );
    let idx2 = fresh(dir.path(), schema);
    assert_all_found(&idx2, [100u64, 110, 120, 130, 250]);
}

/// `open` unlinks exactly the shard and staging files its manifest does not name.
#[test]
fn open_removes_exactly_the_unreferenced_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = fresh(dir.path(), make_schema_u64_i64());
    idx.append_l0_run(&test_batch(&[10], &[100])).unwrap();
    publish_manifest(&mut idx);
    let live = manifest::shard_name(idx.shard_seq);

    for seq in [99, 7] {
        std::fs::write(dir.path().join(manifest::shard_name(seq)), b"x").unwrap();
    }
    std::fs::write(dir.path().join("manifest.bin.tmp"), b"x").unwrap();
    std::fs::write(dir.path().join("other"), b"x").unwrap();

    let idx = fresh(dir.path(), make_schema_u64_i64());
    assert_all_found(&idx, [10]);
    let mut want = [live, "manifest.bin".to_string(), "other".to_string()];
    want.sort();
    assert_eq!(dir_names(dir.path()), want);
}

/// With no manifest, every shard in the directory is an orphan.
#[test]
fn open_of_no_manifest_removes_every_shard() {
    let dir = tempfile::tempdir().unwrap();
    let stray = dir.path().join(manifest::shard_name(7));
    std::fs::write(&stray, b"orphan").unwrap();
    fresh(dir.path(), make_schema_u64_i64());
    assert!(!stray.exists());
}

// -----------------------------------------------------------------------
// Byte targets: guard split and merge
// -----------------------------------------------------------------------

/// Rows enough to put one dense shard over `MIN_GUARD_BYTES` — the guard
/// target an unbounded store that has folded nothing yet carries.
const OVER_TARGET_ROWS: u64 = 5000;

/// Guard 0 owns the key line below its own key, so a quantile down there is
/// how that tail becomes addressable — the one place a split mints a key
/// below the guard it splits.
#[test]
fn splitting_guard_zero_mints_keys_below_its_own() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = fresh(tmp.path(), make_schema_u64_i64());
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

/// A guard of one distinct key cannot be cut — a boundary is a key — so it
/// neither splits nor refolds.
#[test]
fn a_guard_of_one_distinct_key_neither_splits_nor_refolds() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = fresh(tmp.path(), make_schema_u64_i64());
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

/// The byte target bounds a guard at every PK stride, including keys that
/// differ only past byte 16.
#[test]
fn the_byte_target_bounds_a_guard_at_every_stride() {
    for pk_cols in [1usize, 3, 4] {
        let tmp = tempfile::tempdir().unwrap();
        let schema = stride_schema(pk_cols);
        assert_eq!(schema.pk_stride(), pk_cols * 8);
        let mut idx = fresh(tmp.path(), schema);
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

/// Adjacent guards whose bytes fit half the target merge into the run's
/// lowest key at every stride, and the result is under the split trigger.
#[test]
fn underfull_guards_merge_at_every_stride() {
    for pk_cols in [1usize, 3, 4] {
        let tmp = tempfile::tempdir().unwrap();
        let schema = stride_schema(pk_cols);
        let mut idx = fresh(tmp.path(), schema);
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
        // Merged under half the target, so the split pass leaves it be.
        let merged = idx.levels[0].guards[0].entries[0].seq;
        idx.split_overfull_guards(0).unwrap();
        assert_eq!(idx.levels[0].guards[0].entries[0].seq, merged, "stride {}", pk_cols * 8);
    }
}

/// One fold writes at most `MAX_PARTS` shards however far over target the
/// guard is; repeated folds converge instead of one merge writing hundreds of
/// files.
#[test]
fn a_guard_far_over_target_splits_in_bounded_steps() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = fresh(tmp.path(), make_schema_pk_u64_payload_string());
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

/// A run breaks at a change of representation: folding a hydrated guard
/// together with a dehydrated one would evict it.
#[test]
fn a_merge_run_does_not_cross_a_representation_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = open(tmp.path(), make_schema_u64_i64(), ShardBudget::Dehydrate(1));
    for (i, base) in [1u64, 1000].into_iter().enumerate() {
        seed_guard(
            &mut idx,
            TERMINAL_LEVEL_IDX,
            gk(base),
            &dense_batch(base, 200),
            i as u64 + 1,
        );
    }
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
    let mut idx = open(tmp.path(), make_schema_u64_i64(), ShardBudget::Dehydrate(1));
    seed_guard(
        &mut idx,
        TERMINAL_LEVEL_IDX,
        gk(1),
        &dense_batch(1, OVER_TARGET_ROWS),
        1,
    );

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
    let mut idx = open(tmp.path(), make_schema_u64_i64(), ShardBudget::Dehydrate(1));
    let rows = 8_000u64;
    seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(1), &dense_batch(1, rows), 1);
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
    let mut idx = open(tmp.path(), make_schema_u64_i64(), ShardBudget::Dehydrate(1));
    // Eight rows per key: dehydration folds each key to one `(PK, Σweight)`
    // row, which is the order-of-magnitude shrink the merge pass exists for.
    let keys = 2_500u64;
    let pks: Vec<u64> = (1..=keys).flat_map(|k| std::iter::repeat_n(k, 8)).collect();
    let vals: Vec<i64> = (0..pks.len() as i64).collect();
    seed_guard(&mut idx, TERMINAL_LEVEL_IDX, gk(1), &test_batch(&pks, &vals), 1);

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
    assert_weighs(&idx, 8, (1..=keys).step_by(101).map(gk));
}

/// A range open takes only the shards whose extent meets the range.
#[test]
fn a_range_gather_visits_only_the_guards_that_can_own_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = fresh(tmp.path(), make_schema_u64_i64());
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
    let mut idx = fresh(tmp.path(), make_schema_u64_i64());
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
    let reloaded = fresh(tmp.path(), make_schema_u64_i64());
    assert_eq!(reloaded.l0_run_bytes, folded, "R survives a restart as it was observed");
}

/// The terminal level of a budgeted store takes one sweep step, clamped into
/// `[MIN_GUARD_BYTES, R]` because `parse_size` admits any positive `u64`.
#[test]
fn a_budgeted_terminal_target_is_an_eighth_of_the_budget_within_the_clamp() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = fresh(tmp.path(), make_schema_u64_i64());
    for i in 0..5u64 {
        append_stable(&mut idx, 1 + i * 10_000);
    }
    idx.run_compact().unwrap();
    let r = idx.l0_run_bytes;
    assert!(r > 2 * MIN_GUARD_BYTES, "premise: R leaves room inside the clamp");
    let mid = (MIN_GUARD_BYTES + r) / 2;

    for (cap, want) in [(mid * 8, mid), (1024, MIN_GUARD_BYTES), (8 * 1024 * 1024 * 1024, r)] {
        idx = reopened_under(idx, ShardBudget::Dehydrate(cap));
        assert_eq!(idx.guard_target_bytes(TERMINAL_LEVEL_IDX), want, "capacity {cap}");
        assert_eq!(idx.guard_target_bytes(0), r, "only the evicted level reads the budget");
    }
}

/// The `u128` intermediate is not optional: at the design point `|L2| × R`
/// runs past `u64::MAX`. Pure arithmetic, because reaching that product
/// through a level's registered bytes would need a terabyte of mapped shards.
#[test]
fn the_balanced_l1_target_computes_its_product_in_u128() {
    let (l2, r) = (1u64 << 40, 1u64 << 28);
    assert!(l2.checked_mul(r).is_none(), "premise: the product overflows a u64");
    assert_eq!(ShardIndex::balanced_l1_target(l2, r), 1 << 35);

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
fn index_with_l0(dir: &Path, n: u64, budget: ShardBudget) -> ShardIndex {
    let mut idx = open(dir, make_schema_u64_i64(), budget);
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
    let idx = index_with_l0(tmp.path(), 3, ShardBudget::Unbounded);
    let before = idx.resident_bytes();
    let mut idx = reopened_under(idx, ShardBudget::Dehydrate(before * 4));
    idx.enforce_capacity().unwrap();

    assert_eq!(idx.resident_bytes(), before, "no compaction ran");
    assert_eq!(idx.l0.len(), 3, "L0 was not pushed down");
    assert!(idx.levels.iter().all(|l| l.guards.is_empty()), "no guard was created");
    assert!(idx.all_entries().all(|e| !e.shard.is_skeleton()));
}

/// A capacity under the skeleton floor converges to it — everything terminal
/// and skeleton — and the floor is a fixpoint.
#[test]
fn the_sweep_converges_to_the_skeleton_floor() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = index_with_l0(tmp.path(), 4, ShardBudget::Dehydrate(1));

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

/// A delta store's sweep **drops** its victim rather than dehydrating it: the
/// guard is removed, its file unlinked at once, and the highest key it held
/// becomes the retention floor.
#[test]
fn a_delta_budget_drops_its_victim_and_raises_the_floor() {
    let tmp = tempfile::tempdir().unwrap();
    const SHARDS: u64 = 4;
    // The highest key `index_with_l0` writes, which is the floor a full drop leaves.
    const LAST_KEY: u64 = (SHARDS - 1) * 1000 + 40;
    let mut idx = index_with_l0(tmp.path(), SHARDS, ShardBudget::Drop(1));
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
    assert!(shard_names(tmp.path()).is_empty(), "every dropped shard is unlinked");
}

/// Only `enforce_capacity` drops: a delta store's ordinary compactions keep
/// every row and leave the floor at zero.
#[test]
fn ordinary_compaction_of_a_delta_store_keeps_its_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let idx = index_with_l0(tmp.path(), 4, ShardBudget::Unbounded);
    let before = idx.resident_bytes();
    // A budget the store already meets, so the sweep's own step never runs.
    let mut idx = reopened_under(idx, ShardBudget::Drop(before * 8));

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
    publish_manifest(&mut index_with_l0(&dir, 5, ShardBudget::Unbounded));
    let mut idx = fresh(&dir, make_schema_u64_i64());
    let inputs = files(&idx);
    assert_eq!(inputs.len(), 5);
    idx.run_compact().unwrap();
    assert!(inputs.iter().all(exists), "every published input waits for the barrier");
    publish_manifest(&mut idx);
    assert!(!inputs.iter().any(exists), "and the post-publish drain removes it");

    let dir = tmp.path().join("unpublished");
    std::fs::create_dir_all(&dir).unwrap();
    let mut idx = index_with_l0(&dir, 5, ShardBudget::Unbounded);
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
    let mut idx = index_with_l0(tmp.path(), 5, ShardBudget::Unbounded);
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

/// A delta store written and swept every round plateaus rather than tracking
/// everything ever written.
#[test]
fn a_swept_delta_store_plateaus_under_a_steady_write_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = open(tmp.path(), make_schema_u64_i64(), ShardBudget::Drop(1));

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
    assert_eq!(shard_names(tmp.path()).len(), registered, "no orphan shard files");
}

/// A drop removes only rows at or below the floor it raises — why a delta
/// read can serve a cursor sitting exactly at the floor.
#[test]
fn a_drop_removes_nothing_above_the_floor_it_raises() {
    let tmp = tempfile::tempdir().unwrap();
    let mut idx = fresh(tmp.path(), make_schema_u64_i64());
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
    let budget = idx.resident_bytes();
    let mut idx = reopened_under(idx, ShardBudget::Drop(budget));

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
    let mut idx = fresh(tmp.path(), schema);
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
    let cap = idx.resident_bytes() - 1;
    let mut idx = reopened_under(idx, ShardBudget::Dehydrate(cap));
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
    // Under a capacity this tight `run_compact` drains L1 to the terminal level.
    let mut idx = open(tmp.path(), make_schema_u64_i64(), ShardBudget::Dehydrate(1));
    // One key band, so every later fold routes back into the same guard.
    for _ in 0..2 {
        idx.append_l0_run(&dense_batch(1, 20)).unwrap();
    }
    idx.run_compact().unwrap();
    idx.enforce_capacity().unwrap();
    assert_eq!(terminal_split(&idx), (vec![0], vec![]), "the one guard is dehydrated");

    // New hydrated rows over the same keys sink into it by the ordinary path.
    idx.append_l0_run(&dense_batch(1, 20)).unwrap();
    idx.run_compact().unwrap();
    assert!(idx.levels[0].guards.is_empty(), "the drain emptied L1");
    assert_eq!(
        terminal_split(&idx),
        (vec![0], vec![]),
        "the derived rule kept it skeleton"
    );
    assert_weighs(&idx, 3, (1..=20).map(gk));
}

/// `vertical_fold` rewrites only the terminal guards its source's key extent
/// overlaps.
#[test]
fn vertical_fold_touches_only_the_guards_its_extent_overlaps() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut idx = fresh(tmp.path(), schema);
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
