use super::*;

use std::collections::HashMap;
use std::path::Path;

use super::super::manifest::{manifest_path, shard_seqs, staging_path};
use super::super::shard_index::L0_COMPACT_THRESHOLD;
use super::flush_barrier;

use crate::test_support::{
    arb_fold_case, fold_batch, make_batch_raw, make_schema_u64_i64, new_table, row_key, scratch_table, table_shards,
    zset_of, zset_sum, FoldRow, RowKey,
};
use gnitz_wire::PkKeys;
use proptest::prelude::*;

/// unconsolidated rows over [`make_schema_u64_i64`]; the ingest path runs the sort+fold.
fn rows(rows: &[(u64, i64, i64)]) -> Batch {
    make_batch_raw(&make_schema_u64_i64(), rows)
}

/// Every shard file in `dir`.
fn shard_files(dir: &Path) -> usize {
    shard_seqs(dir.to_str().unwrap()).unwrap().len()
}

/// A model step: ingest the next `n` generated rows, or move the tiers.
#[derive(Clone, Debug)]
enum Op {
    Ingest(usize),
    IngestPending(usize),
    Seal,
    FoldToRam,
    Flush,
    Crash,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1usize..8).prop_map(Op::Ingest),
        3 => (1usize..8).prop_map(Op::IngestPending),
        2 => Just(Op::Seal),
        2 => Just(Op::FoldToRam),
        1 => Just(Op::Flush),
        1 => Just(Op::Crash),
    ]
}

/// A row's element identity under `schema`: the one row [`fold_batch`] writes for it.
fn elem(schema: &SchemaDescriptor, r: &FoldRow) -> RowKey {
    row_key(&fold_batch(schema, std::slice::from_ref(r)), schema, 0)
}

/// Every row `g` gathers, as a Z-set summed over its chunks.
fn gathered(mut g: PkSetGather, s: &SchemaDescriptor) -> HashMap<RowKey, i64> {
    let chunks: Vec<Batch> = std::iter::from_fn(|| g.drain_chunk(3)).collect();
    zset_sum(&chunks, s)
}

/// Every read `t` serves agrees with `live`, and one at the cut with `sealed`.
fn assert_serves(t: &Table, live: &HashMap<RowKey, i64>, sealed: &HashMap<RowKey, i64>, keys: &[Vec<u8>]) {
    let s = *t.schema();
    let want = live.clone();
    assert_eq!(zset_of(&t.full_scan(), &s), want, "full_scan");

    for k in keys {
        let sum: i64 = live.iter().filter(|(e, _)| &e.0 == k).map(|(_, w)| w).sum();
        assert_eq!(t.net_weight_at(k), sum, "net_weight_at {k:?}");
    }

    for (cut, want) in [(Cut::Now, want.clone()), (Cut::Sealed, sealed.clone())] {
        let gather = t.gather(PkKeys::from_sorted(s.pk_stride(), keys.concat()), cut);
        assert_eq!(gathered(gather, &s), want, "gather at {cut:?}");

        // Every other key's leading column alone: a gather and a cursor between
        // the first and last answer every row under each, and nothing else.
        let p = s.pk_prefix_stride(1);
        if p < s.pk_stride() {
            let mut prefixes: Vec<&[u8]> = keys.iter().map(|k| &k[..p]).collect();
            prefixes.dedup();
            let prefixes: Vec<&[u8]> = prefixes.into_iter().step_by(2).collect();
            let under = |zset: &HashMap<RowKey, i64>| -> HashMap<RowKey, i64> {
                zset.iter()
                    .filter(|(k, _)| prefixes.contains(&&k.0[..p]))
                    .map(|(k, &w)| (k.clone(), w))
                    .collect()
            };
            let gather = t.gather(PkKeys::from_sorted(p, prefixes.concat()), cut);
            assert_eq!(gathered(gather, &s), under(&want), "prefix gather at {cut:?}");
            let (first, last) = (prefixes[0], prefixes[prefixes.len() - 1]);
            let between = zset_of(&t.cursor_between(first, last, cut).materialize(), &s);
            assert_eq!(under(&between), under(&want), "prefix cursor at {cut:?}");
        }

        assert_eq!(
            zset_of(&t.open_cursor(cut).materialize(), &s),
            want,
            "cursor at {cut:?}"
        );
        for w in keys.windows(2) {
            let (lo, hi) = (PkBuf::from_bytes(&w[0]), PkBuf::from_bytes(&w[1]));
            let got = zset_of(&t.cursor(Some((lo, Some(hi))), cut).materialize(), &s);
            let band: HashMap<RowKey, i64> = want
                .iter()
                .filter(|(k, _)| w[0] <= k.0 && k.0 < w[1])
                .map(|(k, &w)| (k.clone(), w))
                .collect();
            assert_eq!(got, band, "range cursor at {cut:?}");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Under any sequence of ingests, seals, folds, barriers and crashes, a table
    /// serves the Z-set its ingests sum to, a seal answers the rows ingested
    /// above the cut since the last, and a crash reopens at the last barrier.
    #[test]
    fn a_table_serves_the_zset_its_ingests_sum_to(
        (s, stream) in arb_fold_case(),
        ops in prop::collection::vec(arb_op(), 1..48),
        salreplay in any::<bool>(),
        spills in any::<bool>(),
    ) {
        let rs = match salreplay {
            true => RecoverySource::SalReplay,
            false => RecoverySource::Rederive { resume_at: Some(0) },
        };
        let ram = if spills { 100 } else { DEFAULT_RAM_TIER_BYTES };
        let dir = tempfile::tempdir().unwrap();
        let open = || new_table(dir.path(), s, rs, ram);
        let mut keys: Vec<Vec<u8>> = stream.iter().map(|r| r.0.clone()).collect();
        keys.extend([vec![0; s.pk_stride()], vec![0xff; s.pk_stride()]]);
        keys.sort();
        keys.dedup();

        let mut t = open();
        let (mut live, mut sealed, mut durable) = (HashMap::new(), HashMap::new(), HashMap::new());
        let mut stream = stream.into_iter().cycle();
        // A reopen finds the pending shards its last barrier published in L0,
        // however many they are, until the next upkeep.
        for op in ops {
            match op {
                Op::Ingest(n) | Op::IngestPending(n) => {
                    // Base-table positivity: a retraction never takes an element
                    // below zero.
                    let mut batch = Vec::new();
                    for mut r in stream.by_ref().take(n) {
                        let held = live.entry(elem(&s, &r)).or_insert(0);
                        r.1 = r.1.max(-*held);
                        *held += r.1;
                        batch.push(r);
                    }
                    live.retain(|_, w| *w != 0);
                    let batch = fold_batch(&s, &batch);
                    match op {
                        Op::IngestPending(_) => t.ingest_pending(batch),
                        // Below the cut, as the registry ingests: no row is left above it.
                        _ => {
                            t.seal().unwrap();
                            t.ingest(batch).unwrap();
                            sealed = live.clone();
                        }
                    }
                }
                Op::Seal => {
                    let mut moved = live.clone();
                    for (e, w) in &sealed {
                        *moved.entry(e.clone()).or_insert(0) -= w;
                    }
                    moved.retain(|_, w| *w != 0);
                    let delta = t.seal().unwrap();
                    let delta = delta.map(|d| zset_of(&d, &s)).unwrap_or_default();
                    prop_assert_eq!(delta, moved, "the seal's delta");
                    prop_assert!(!t.has_pending());
                    sealed = live.clone();
                }
                Op::FoldToRam => t.fold_to_ram().unwrap(),
                Op::Flush => {
                    flush_barrier([&mut t], 0).unwrap();
                    durable = live.clone();
                    prop_assert_eq!(shard_files(dir.path()), t.shard_index.shard_count());
                }
                Op::Crash => {
                    drop(t);
                    t = open();
                    live = durable.clone();
                    sealed = durable.clone();
                    prop_assert_eq!(shard_files(dir.path()), t.shard_index.shard_count());
                }
            }
            assert_serves(&t, &live, &sealed, &keys);
        }
        t.settle().unwrap();
        assert_serves(&t, &live, &sealed, &keys);
        prop_assert!(t.level_shape().0 <= L0_COMPACT_THRESHOLD, "L0 over its trigger");
    }
}

/// A cursor opened between zero-width prefixes reads what one opened over the
/// whole store does, at either cut: a row and its retraction in two runs cancel,
/// and a retraction above the cut cancels only at `Cut::Now`.
#[test]
fn a_cursor_between_empty_prefixes_reads_the_whole_store() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut t = scratch_table(dir.path(), schema);
    t.ingest(rows(&[(1, 1, 10), (2, 1, 20), (5, 1, 50)])).unwrap();
    t.fold_to_ram().unwrap();
    t.ingest(rows(&[(2, -1, 20), (3, 1, 30)])).unwrap();
    t.ingest_pending(rows(&[(1, -1, 10), (4, 1, 40)]));
    assert_eq!(
        (t.ram_tier.len(), t.memtable.len(), t.pending.len()),
        (1, 1, 1),
        "premise: one run a tier"
    );

    for (cut, want) in [
        (Cut::Now, rows(&[(3, 1, 30), (4, 1, 40), (5, 1, 50)])),
        (Cut::Sealed, rows(&[(1, 1, 10), (3, 1, 30), (5, 1, 50)])),
    ] {
        let between = t.cursor_between(&[], &[], cut).materialize();
        let whole = t.open_cursor(cut).materialize();
        assert_eq!(between.len(), whole.len(), "{cut:?}: row count");
        assert_eq!(zset_of(&between, &schema), zset_of(&whole, &schema), "{cut:?}");
        assert_eq!(zset_of(&between, &schema), zset_of(&want, &schema), "{cut:?}");
    }
}

/// The OPK of `pk` under [`make_schema_u64_i64`].
fn key(pk: u64) -> Vec<u8> {
    pk.to_be_bytes().to_vec()
}

/// A store whose keys 1..=64 are inserted and retracted in two runs, and whose
/// one live row is key 100.
fn store_cancelled_under_100(dir: &Path) -> Table {
    let mut t = scratch_table(dir, make_schema_u64_i64());
    let cancelled: Vec<(u64, i64, i64)> = (1..=64).map(|pk| (pk, 1, 0)).collect();
    t.ingest(rows(&[cancelled.as_slice(), &[(100, 1, 0)]].concat()))
        .unwrap();
    t.fold_to_ram().unwrap();
    t.ingest(rows(
        &cancelled.iter().map(|&(pk, _, v)| (pk, -1, v)).collect::<Vec<_>>(),
    ))
    .unwrap();
    t
}

fn floor(t: &Table) -> Option<Vec<u8>> {
    t.live_floor.get().at.map(|k| k.pk_bytes().to_vec())
}

fn scan(t: &Table, cut: Cut) -> HashMap<RowKey, i64> {
    zset_of(&t.open_cursor(cut).materialize(), &make_schema_u64_i64())
}

fn live(pks: &[u64]) -> HashMap<RowKey, i64> {
    zset_of(
        &rows(&pks.iter().map(|&pk| (pk, 1, 0)).collect::<Vec<_>>()),
        &make_schema_u64_i64(),
    )
}

#[test]
fn an_open_keeps_the_first_live_key_as_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let t = store_cancelled_under_100(dir.path());
    assert_eq!(floor(&t), None);
    assert_eq!(scan(&t, Cut::Now), live(&[100]));
    assert_eq!(floor(&t), Some(key(100)));
    assert_eq!(scan(&t, Cut::Sealed), live(&[100]));
    let band = |lo, hi| {
        let band = (PkBuf::from_bytes(&key(lo)), Some(PkBuf::from_bytes(&key(hi))));
        zset_of(&t.cursor(Some(band), Cut::Now).materialize(), &make_schema_u64_i64())
    };
    assert_eq!(band(0, 100), live(&[]));
    assert_eq!(band(0, 101), live(&[100]));
}

#[test]
fn a_pending_row_lowers_the_floor_both_cuts_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = store_cancelled_under_100(dir.path());
    scan(&t, Cut::Now);
    t.ingest_pending(rows(&[(50, 1, 0)]));
    assert_eq!(floor(&t), Some(key(50)));
    assert_eq!(scan(&t, Cut::Now), live(&[50, 100]));
    assert_eq!(scan(&t, Cut::Sealed), live(&[100]));
    t.seal().unwrap();
    assert_eq!(floor(&t), Some(key(50)));
    assert_eq!(scan(&t, Cut::Sealed), live(&[50, 100]));
}

/// A read at the store's newest rows, where a pending retraction cancels a
/// row, keeps the floor under that row, so a read at the cut still finds it.
#[test]
fn a_pending_retraction_keeps_its_row_over_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = store_cancelled_under_100(dir.path());
    t.ingest(rows(&[(65, 1, 0)])).unwrap();
    t.ingest_pending(rows(&[(65, -1, 0)]));
    assert_eq!(scan(&t, Cut::Now), live(&[100]));
    assert_eq!(floor(&t), Some(key(65)));
    assert_eq!(scan(&t, Cut::Sealed), live(&[65, 100]));
    t.seal().unwrap();
    assert_eq!(scan(&t, Cut::Sealed), live(&[100]));
}

#[test]
fn a_floor_with_nothing_under_it_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = store_cancelled_under_100(dir.path());
    scan(&t, Cut::Now);
    t.ingest(rows(&[(0, 1, 0)])).unwrap();
    assert_eq!(floor(&t), Some(key(0)));
    assert_eq!(scan(&t, Cut::Now), live(&[0, 100]));
    assert_eq!(floor(&t), None);
}

#[test]
fn a_fold_under_the_floor_drops_it_at_the_next_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = store_cancelled_under_100(dir.path());
    scan(&t, Cut::Now);
    t.fold_to_ram().unwrap();
    assert_eq!(t.ram_tier.row_count(), 1, "premise: the fold cancelled the pairs");
    assert_eq!(floor(&t), Some(key(100)));
    scan(&t, Cut::Now);
    assert_eq!(floor(&t), None);
}

#[test]
fn a_store_of_cancelled_rows_floors_at_the_top_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = store_cancelled_under_100(dir.path());
    t.ingest(rows(&[(100, -1, 0)])).unwrap();
    assert_eq!(scan(&t, Cut::Now), live(&[]));
    let top = PkBuf::max(make_schema_u64_i64().pk_stride());
    assert_eq!(floor(&t), Some(top.pk_bytes().to_vec()));
    t.clear();
    assert_eq!(floor(&t), None);
}

/// An ingest past the memtable's budget folds it into the RAM tier on its own.
#[test]
fn a_memtable_over_budget_folds_into_the_ram_tier() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = scratch_table(dir.path(), make_schema_u64_i64());
    let n = MEMTABLE_BYTES as u64 / 32 + 1;
    t.ingest(rows(&(0..n).map(|k| (k, 1, 0)).collect::<Vec<_>>())).unwrap();
    assert_eq!(t.memtable.len(), 0, "the ingest folded the memtable");
    assert_eq!(t.ram_tier.row_count() as u64, n);
}

/// A damaged manifest fails a `SalReplay` open, whose shards are its only copy;
/// a `Rederive` open rebuilds empty — unless reading the manifest itself failed.
#[test]
fn a_damaged_manifest_fails_a_replayed_open_and_rebuilds_a_rederived_one() {
    type Damage = fn(&Path);
    // Every decode failure is one `Corrupt` to the open, so one stands for all.
    let damages: [(&str, Damage); 2] = [
        ("checksum", |m| crate::test_support::flip_last_byte_in_place(m)),
        ("unreadable", |m| {
            std::fs::remove_file(m).unwrap();
            std::fs::create_dir(m).unwrap();
        }),
    ];
    let schema = make_schema_u64_i64();
    for (name, damage) in damages {
        for rs in [
            RecoverySource::SalReplay,
            RecoverySource::Rederive { resume_at: Some(7) },
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut t = new_table(dir.path(), schema, RecoverySource::SalReplay, DEFAULT_RAM_TIER_BYTES);
            t.ingest(rows(&[(1, 1, 100)])).unwrap();
            flush_barrier([&mut t], 7).unwrap();
            drop(t);
            damage(Path::new(&manifest_path(dir.path().to_str().unwrap())));

            let opened = Table::new(
                dir.path().to_str().unwrap(),
                schema,
                rs,
                DEFAULT_RAM_TIER_BYTES,
                None,
                false,
            );
            match (rs, name) {
                (RecoverySource::Rederive { .. }, "unreadable") => {
                    assert_eq!(opened.err(), Some(StorageError::Io(libc::EISDIR)))
                }
                (RecoverySource::SalReplay, _) => assert!(opened.is_err(), "{name}: {rs:?}"),
                _ => {
                    let t = opened.unwrap_or_else(|e| panic!("{name}: a rederived store rebuilds: {e:?}"));
                    assert_eq!(t.full_scan().len(), 0, "{name}: the rebuild opens empty");
                    assert_eq!(shard_files(dir.path()), 0, "{name}: stale shards erased");
                    continue;
                }
            }
            assert_eq!(shard_files(dir.path()), 1, "{name}: {rs:?} erases nothing");
        }
    }
}

/// `Rederive`'s open verdict: a manifest at the caller's generation loads, any
/// other generation erases the shards and unlinks the manifest, so a later
/// open cannot reload it.
#[test]
fn rederive_checkpointed_conditional_load() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let manifest = manifest_path(dir.path().to_str().unwrap());
    let pushed = rows(&[(1, 1, 100), (2, 1, 200)]);
    let reopen = |at| {
        new_table(
            dir.path(),
            schema,
            RecoverySource::Rederive { resume_at: Some(at) },
            1 << 20,
        )
    };
    {
        let mut t = new_table(dir.path(), schema, RecoverySource::SalReplay, 1 << 20);
        t.ingest(pushed.clone()).unwrap();
        flush_barrier([&mut t], 7).unwrap();
    }
    let t = reopen(7);
    assert!(t.loaded_mark().is_some());
    assert_eq!(
        zset_of(&t.full_scan(), &schema),
        zset_of(&pushed, &schema),
        "a matching generation loads"
    );
    drop(t);
    let t = reopen(8);
    assert!(t.loaded_mark().is_none());
    assert_eq!(t.full_scan().len(), 0, "a mismatched generation erases");
    assert!(!std::fs::exists(&manifest).unwrap());
}

/// A reopen reports the last published checkpoint mark; a store without a
/// manifest reports none.
#[test]
fn a_barriers_checkpoint_mark_is_what_the_reopen_reports() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut t = new_table(dir.path(), schema, RecoverySource::SalReplay, 1 << 20);
    assert_eq!(t.loaded_mark(), None, "a fresh store carries no mark");
    t.ingest(rows(&[(1, 1, 10)])).unwrap();
    flush_barrier([&mut t], 9).unwrap();
    t.ingest(rows(&[(2, 1, 10)])).unwrap();
    flush_barrier([&mut t], 7).unwrap();
    assert_eq!(t.loaded_mark(), None, "the open's mark is read-only");
    drop(t);
    let t = new_table(dir.path(), schema, RecoverySource::SalReplay, 1 << 20);
    assert_eq!(t.loaded_mark(), Some(7), "the last published mark, not the highest");
}

/// A barrier publishes exactly when the manifest would change — rows, shards or
/// caller record — and on a fresh store's first round.
#[test]
fn a_barrier_publishes_exactly_when_the_manifest_changes() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let resume = RecoverySource::Rederive { resume_at: Some(0) };
    let mut t = new_table(dir.path(), schema, resume, 100);
    let publishes = |t: &mut Table| {
        let staged = t.flush_prepare(0).unwrap().is_some();
        flush_barrier([&mut *t], 0).unwrap();
        staged
    };
    assert!(publishes(&mut t), "a first round publishes");
    assert!(!publishes(&mut t), "an idle store stages nothing");
    t.set_caller_record(b"first".to_vec());
    assert!(publishes(&mut t), "a changed record publishes");
    assert!(!publishes(&mut t));

    // One spilled shard per round, then a retraction whose spill compacts the
    // whole L0 to nothing.
    let round = |r: u64, w: i64| rows(&(0..10).map(|k| (r * 100 + k, w, 1)).collect::<Vec<_>>());
    for r in 0..L0_COMPACT_THRESHOLD as u64 {
        t.ingest(round(r, 1)).unwrap();
        assert!(publishes(&mut t), "written rows publish");
    }
    for r in 0..L0_COMPACT_THRESHOLD as u64 {
        t.ingest(round(r, -1)).unwrap();
    }
    t.settle().unwrap();
    assert_eq!(t.shard_index.shard_count(), 0, "the compaction cancelled everything");
    assert!(publishes(&mut t), "an emptied index publishes");
    drop(t);

    let t = new_table(dir.path(), schema, resume, 100);
    assert_eq!(
        t.shard_index.dir.caller_record(),
        b"first",
        "the record reloads with its manifest"
    );
    assert_eq!(t.full_scan().len(), 0, "no retracted row comes back");
}

/// A reopened store's first barrier owes the manifest it read only its
/// directory syncs: the file is rewritten when the manifest changes, and not
/// otherwise.
#[test]
fn a_reopened_stores_first_barrier_rewrites_only_a_changed_manifest() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = manifest_path(dir.path().to_str().unwrap());
    let inode = || std::fs::metadata(&path).unwrap().ino();
    let staging = Path::new(&staging_path(dir.path().to_str().unwrap())).to_path_buf();
    let resume = RecoverySource::Rederive { resume_at: Some(4) };
    let mut pushed = Vec::new();
    {
        let mut t = new_table(dir.path(), schema, resume, 100);
        // Several shards over two levels, so the reloaded order is not trivial.
        for r in 0..2 * L0_COMPACT_THRESHOLD as u64 {
            let b = rows(&[(r, 1, 10)]);
            t.ingest(b.clone()).unwrap();
            pushed.push(b);
            flush_barrier([&mut t], 4).unwrap();
        }
        t.set_caller_record(b"rec".to_vec());
        flush_barrier([&mut t], 4).unwrap();
    }
    let published = inode();

    let mut t = new_table(dir.path(), schema, resume, 100);
    assert!(t.flush_prepare(4).unwrap().is_some(), "the directory syncs are owed");
    assert!(!staging.exists(), "nothing is staged for the manifest already in place");
    flush_barrier([&mut t], 4).unwrap();
    assert_eq!(inode(), published, "the manifest this open read stays in place");
    assert!(t.flush_prepare(4).unwrap().is_none(), "and is durable from here on");
    assert_eq!(zset_of(&t.full_scan(), &schema), zset_sum(&pushed, &schema));
    drop(t);

    // Another mark is another manifest.
    let mut t = new_table(dir.path(), schema, resume, 100);
    flush_barrier([&mut t], 5).unwrap();
    assert_ne!(inode(), published, "a changed manifest is renamed into place");
    let published = inode();
    drop(t);

    // So are new rows, and the manifest read at open stops counting once one is staged.
    let resume = RecoverySource::Rederive { resume_at: Some(5) };
    let mut t = new_table(dir.path(), schema, resume, 100);
    let b = rows(&[(900, 1, 10)]);
    t.ingest(b.clone()).unwrap();
    pushed.push(b);
    assert!(t.flush_prepare(5).unwrap().is_some());
    assert!(staging.exists(), "new rows stage a manifest");
    flush_barrier([&mut t], 5).unwrap();
    assert_ne!(inode(), published);
    drop(t);
    let t = new_table(dir.path(), schema, resume, 100);
    assert_eq!(zset_of(&t.full_scan(), &schema), zset_sum(&pushed, &schema));
}

/// Only base-table paths point-probe a store by PK, and they are the only
/// readers of a shard's PK filter. Silent both ways: a missing filter still
/// answers every probe (just slower), a useless one costs only bytes.
#[test]
fn pk_filter_follows_whether_the_store_is_probed() {
    // Six spills: the fifth crosses the L0 threshold and the sixth sits above
    // the fold, so both shard writers' output is registered at once.
    for (rs, filtered) in [
        (RecoverySource::SalReplay, true),
        (RecoverySource::Rederive { resume_at: None }, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut t = new_table(dir.path(), make_schema_u64_i64(), rs, 100);
        for r in 0..6u64 {
            t.ingest(rows(&(0..10).map(|k| (r * 100 + k, 1, 1)).collect::<Vec<_>>()))
                .unwrap();
            t.fold_to_ram().unwrap();
        }
        let (l0, levels) = t.level_shape();
        assert!(
            l0 > 0 && levels.iter().sum::<usize>() > 0,
            "spills and compaction outputs"
        );
        let shards = table_shards(&t);
        assert!(shards.iter().all(|s| s.has_pk_filter() == filtered), "{rs:?}");
    }
}

/// A RAM tier past its ceiling folds, and stays in RAM when the fold leaves it
/// room; folded to within a sixteenth of the ceiling it spills, since every
/// later crossing would fold it whole again.
#[test]
fn a_tier_folded_to_just_under_its_ceiling_spills() {
    const CEILING_ROWS: u64 = 1 << 15;
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    for (held, spills) in [(CEILING_ROWS * 7 / 8, false), (CEILING_ROWS * 31 / 32, true)] {
        let mut t = new_table(
            dir.path().join(held.to_string()),
            schema,
            RecoverySource::Rederive { resume_at: None },
            CEILING_ROWS as usize * 32,
        );
        let mut pushed = vec![rows(&(0..held).map(|k| (k, 1, 0)).collect::<Vec<_>>())];
        t.ingest(pushed[0].clone()).unwrap();
        t.fold_to_ram().unwrap();
        assert_eq!(table_shards(&t).len(), 0, "{held}: under the ceiling");
        // Updates of a quarter of the ceiling's rows, in pushes too small to merge.
        for half in [0..CEILING_ROWS / 8, CEILING_ROWS / 8..CEILING_ROWS / 4] {
            let updates = half.flat_map(|k| [(k, -1, 0), (k, 1, 1)]);
            let b = rows(&updates.collect::<Vec<_>>());
            t.ingest(b.clone()).unwrap();
            pushed.push(b);
            t.fold_to_ram().unwrap();
        }
        assert_eq!(!table_shards(&t).is_empty(), spills, "{held} rows held");
        assert_eq!(
            zset_of(&t.full_scan(), &schema),
            zset_sum(&pushed, &schema),
            "{held} rows held"
        );
    }
}

/// A store held as a read replica of another process's directory never writes
/// into it, however far past its RAM-tier ceiling it grows.
#[test]
fn a_store_held_in_ram_never_spills() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut t = new_table(dir.path(), schema, RecoverySource::SalReplay, 96);
    t.hold_in_ram();
    let mut pushed = Vec::new();
    for r in 0..20u64 {
        let b = rows(&(0..10).map(|k| (r * 100 + k, 1, 1)).collect::<Vec<_>>());
        t.ingest(b.clone()).unwrap();
        pushed.push(b);
        t.fold_to_ram().unwrap();
    }
    assert_eq!(shard_files(dir.path()), 0, "no shard file was written");
    assert_eq!(
        zset_of(&t.full_scan(), &schema),
        zset_sum(&pushed, &schema),
        "every row held in RAM"
    );
}

/// A seal that moves pending shards into L0 runs the disk tier's upkeep,
/// whether or not its delta also overflows the memtable.
#[test]
fn a_seal_that_enters_shards_into_l0_compacts_it() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    for overflow in [false, true] {
        let mut t = new_table(
            dir.path().join(format!("{overflow}")),
            schema,
            RecoverySource::SalReplay,
            1 << 20,
        );
        let mut pushed = Vec::new();
        let mut push = |t: &mut Table, keys: std::ops::Range<u64>| {
            pushed.push(rows(&keys.map(|k| (k, 1, 0)).collect::<Vec<_>>()));
            t.ingest_pending(pushed.last().unwrap().clone());
        };
        for k in 0..=L0_COMPACT_THRESHOLD as u64 {
            push(&mut t, k..k + 1);
            flush_barrier([&mut t], 0).unwrap();
        }
        if overflow {
            // Past the memtable's budget, under the RAM tier's.
            push(&mut t, 100..10_100);
        }
        assert_eq!(t.level_shape().0, 0, "pending shards sit outside L0");
        let delta = t.seal().unwrap().expect("rows were pending");
        assert_eq!(t.level_shape().0, 0, "overflow={overflow}: the seal folded L0 into L1");
        assert_eq!(t.level_shape().1[0], 1, "overflow={overflow}: L1 holds the fold");
        let want = zset_sum(&pushed, &schema);
        assert_eq!(zset_of(&delta, &schema), want, "overflow={overflow}: the delta");
        assert_eq!(zset_of(&t.full_scan(), &schema), want, "overflow={overflow}: the rows");
    }
}

/// Row `i`'s sentence: words of a short list, so that a symbol table codes a
/// heap of them to a fraction of its bytes, and unique to `i`.
fn sentence(i: u64) -> String {
    const WORDS: [&str; 8] = [
        "the", "river", "carried", "every", "lantern", "toward", "the", "harbour",
    ];
    let mut s = format!("{i} ");
    for k in 0..8 + i % 7 {
        s.push_str(WORDS[((i * 3 + k * 5) % 8) as usize]);
        s.push(' ');
    }
    s
}

/// A compressing store codes the heaps of the shards it spills and compacts
/// under one table, which a reopen takes up again, and serves its rows as
/// given.
#[test]
fn a_compressing_store_codes_its_shards_under_one_table() {
    let dir = tempfile::tempdir().unwrap();
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
    let open = || {
        Table::new(
            dir.path().to_str().unwrap(),
            schema,
            RecoverySource::SalReplay,
            1 << 20,
            None,
            true,
        )
        .unwrap()
    };
    let batch = |keys: std::ops::Range<u64>| {
        let values: Vec<String> = keys.clone().map(sentence).collect();
        let rows: Vec<(u64, i64, &[u8])> = keys.zip(&values).map(|(k, v)| (k, 1, v.as_bytes())).collect();
        crate::test_support::make_string_batch(&rows)
    };
    let digests = |t: &Table| -> Vec<Option<u64>> {
        let shards = table_shards(t);
        shards.iter().map(|s| s.codebook().map(|b| b.digest())).collect()
    };
    let mut t = open();
    let mut pushed = Vec::new();
    for round in 0..=L0_COMPACT_THRESHOLD as u64 {
        pushed.push(batch(round * 400..(round + 1) * 400));
        t.ingest(pushed.last().unwrap().clone()).unwrap();
        flush_barrier([&mut t], round).unwrap();
    }
    t.settle().unwrap();
    assert_eq!(t.level_shape().0, 0, "the spills folded out of L0");
    let tables = digests(&t);
    assert!(tables.iter().all(|d| d.is_some() && *d == tables[0]), "{tables:?}");
    assert_eq!(zset_of(&t.full_scan(), &schema), zset_sum(&pushed, &schema));
    drop(t);

    let mut t = open();
    pushed.push(batch(10_000..10_400));
    t.ingest(pushed.last().unwrap().clone()).unwrap();
    flush_barrier([&mut t], 99).unwrap();
    let reopened = digests(&t);
    assert!(
        reopened.iter().all(|d| *d == tables[0]),
        "the reopen codes under the table it found"
    );
    assert_eq!(zset_of(&t.full_scan(), &schema), zset_sum(&pushed, &schema));
}

/// The keys a base table's history writes: enough for a write to make a run
/// long enough to probe through a filter of its own.
const UNIQUE_KEYS: u64 = 256;

/// A step of a base table's history: upsert or delete distinct keys, below
/// or above the cut, or move the tiers.
#[derive(Clone, Debug)]
enum UniqueOp {
    Write {
        keys: Vec<(u64, Option<i64>)>,
        pending: bool,
    },
    Seal,
    FoldToRam,
    Flush,
    Crash,
}

fn arb_unique_op() -> impl Strategy<Value = UniqueOp> {
    let write = (
        prop::collection::btree_map(0u64..UNIQUE_KEYS, prop::option::weighted(0.8, 0i64..4), 1..96),
        any::<bool>(),
    )
        .prop_map(|(keys, pending)| UniqueOp::Write {
            keys: keys.into_iter().collect(),
            pending,
        });
    prop_oneof![
        6 => write,
        2 => Just(UniqueOp::Seal),
        2 => Just(UniqueOp::FoldToRam),
        1 => Just(UniqueOp::Flush),
        1 => Just(UniqueOp::Crash),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Under any history a base table's unique-PK rule writes — each key's
    /// stored row retracted as its new one lands — interleaved with seals,
    /// folds, barriers and crashes, `unique_live_row_at` answers each key's
    /// one live row.
    #[test]
    fn unique_live_row_at_answers_each_keys_live_row(
        ops in prop::collection::vec(arb_unique_op(), 1..40),
        salreplay in any::<bool>(),
        spills in any::<bool>(),
    ) {
        let s = make_schema_u64_i64();
        let rs = match salreplay {
            true => RecoverySource::SalReplay,
            false => RecoverySource::Rederive { resume_at: Some(0) },
        };
        let ram = if spills { 100 } else { DEFAULT_RAM_TIER_BYTES };
        let dir = tempfile::tempdir().unwrap();
        let open = || new_table(dir.path(), s, rs, ram);
        let mut t = open();
        let (mut live, mut durable) = (HashMap::<u64, i64>::new(), HashMap::new());
        for op in ops {
            match op {
                UniqueOp::Write { keys, pending } => {
                    let mut delta = Vec::new();
                    for (k, new) in keys {
                        if let Some(old) = live.remove(&k) {
                            delta.push((k, -1, old));
                        }
                        if let Some(v) = new {
                            delta.push((k, 1, v));
                            live.insert(k, v);
                        }
                    }
                    if delta.is_empty() {
                        continue;
                    }
                    match pending {
                        true => t.ingest_pending(rows(&delta)),
                        false => {
                            t.seal().unwrap();
                            t.ingest(rows(&delta)).unwrap();
                        }
                    }
                }
                UniqueOp::Seal => drop(t.seal().unwrap()),
                UniqueOp::FoldToRam => t.fold_to_ram().unwrap(),
                UniqueOp::Flush => {
                    flush_barrier([&mut t], 0).unwrap();
                    durable = live.clone();
                }
                UniqueOp::Crash => {
                    drop(t);
                    t = open();
                    live = durable.clone();
                }
            }
            for k in 0..UNIQUE_KEYS {
                let got = t.unique_live_row_at(&k.to_be_bytes()).map(|r| {
                    let (src, i) = r.source();
                    row_key(src, &s, i)
                });
                let want = live.get(&k).map(|&v| row_key(&rows(&[(k, 1, v)]), &s, 0));
                prop_assert_eq!(got, want, "key {}", k);
            }
        }
    }
}
