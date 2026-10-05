use super::*;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use super::super::manifest::{manifest_path, shard_seqs};
use super::super::shard_index::L0_COMPACT_THRESHOLD;
use super::flush_barrier;

use crate::test_support::{
    arb_fold_case, fold_batch, fold_schemas, make_batch_raw, make_schema_u64_i64, new_table, row_key, zset_of,
    zset_sum, FoldRow, RowKey,
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

/// A row's element identity under `schema` — what [`fold_batch`] writes.
type Elem = (Vec<u8>, Option<u8>, Option<i64>);

fn elem(schema: &SchemaDescriptor, (pk, _, s, v): &FoldRow) -> Elem {
    match schema.num_payload_cols() {
        2 => (pk.clone(), *s, *v),
        _ => (pk.clone(), None, Some(v.unwrap_or(0))),
    }
}

/// The Z-set `model` holds, keyed as a batch of `schema` is.
fn zset(schema: &SchemaDescriptor, model: &BTreeMap<Elem, i64>) -> HashMap<RowKey, i64> {
    let rows: Vec<FoldRow> = model.iter().map(|((pk, st, v), &w)| (pk.clone(), w, *st, *v)).collect();
    zset_of(&fold_batch(schema, &rows), schema)
}

/// Every read `t` serves agrees with `live`, and one at the cut with `sealed`.
fn assert_serves(t: &Table, live: &BTreeMap<Elem, i64>, sealed: &BTreeMap<Elem, i64>, keys: &[Vec<u8>]) {
    let s = *t.schema();
    let want = zset(&s, live);
    let where_pk = |f: &dyn Fn(&[u8]) -> bool| -> HashMap<RowKey, i64> {
        want.iter()
            .filter(|(k, _)| f(&k.0))
            .map(|(k, &w)| (k.clone(), w))
            .collect()
    };
    assert_eq!(zset_of(&t.full_scan(), &s), want, "full_scan");

    for k in keys {
        let sum: i64 = live.iter().filter(|(e, _)| &e.0 == k).map(|(_, w)| w).sum();
        assert_eq!(t.has_pk_bytes(k), sum > 0, "has_pk {k:?}");
        let (w, row) = t.live_row_at(k);
        assert_eq!((w, row.is_some()), (sum, sum > 0), "live_row_at {k:?}");
        if let Some(r) = row {
            let (src, i) = r.source();
            assert!(
                want.get(&row_key(src, &s, i)).is_some_and(|&w| w > 0),
                "the found row is live"
            );
        }
    }

    for (cut, want) in [(Cut::Now, want.clone()), (Cut::Sealed, zset(&s, sealed))] {
        let mut gather = t.gather(PkKeys::from_sorted(s.pk_stride(), keys.concat()), cut);
        let mut gathered: HashMap<RowKey, i64> = HashMap::new();
        while let Some(b) = gather.drain_chunk(3) {
            gathered.extend(zset_of(&b, &s));
        }
        assert_eq!(gathered, want, "gather at {cut:?}");
    }

    for w in keys.windows(2) {
        let (lo, hi) = (PkBuf::from_bytes(&w[0]), PkBuf::from_bytes(&w[1]));
        let got = zset_of(&t.range_cursor(Some((lo, Some(hi)))).materialize(), &s);
        assert_eq!(got, where_pk(&|p| &w[0][..] <= p && p < &w[1][..]), "range cursor");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Under any sequence of ingests, seals, folds, barriers and crashes, a table
    /// serves the Z-set its ingests sum to, a seal answers the rows ingested
    /// above the cut since the last, and a crash reopens at the last barrier.
    #[test]
    fn a_table_serves_the_zset_its_ingests_sum_to(
        (si, stream) in arb_fold_case(),
        ops in prop::collection::vec(arb_op(), 1..48),
        salreplay in any::<bool>(),
        spills in any::<bool>(),
    ) {
        let s = fold_schemas()[si];
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
        let (mut live, mut sealed, mut durable) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
        let mut stream = stream.into_iter().cycle();
        // A reopen finds the pending shards its last barrier published in L0,
        // however many they are, until the next upkeep.
        let mut reopened_over = false;
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
                            t.ingest_owned_batch(batch).unwrap();
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
                    prop_assert_eq!(delta, zset(&s, &moved), "the seal's delta");
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
                    reopened_over = true;
                    prop_assert_eq!(shard_files(dir.path()), t.shard_index.shard_count());
                }
            }
            assert_serves(&t, &live, &sealed, &keys);
            reopened_over &= t.level_shape().0 > L0_COMPACT_THRESHOLD;
            prop_assert!(reopened_over || t.level_shape().0 <= L0_COMPACT_THRESHOLD, "L0 over its trigger");
        }
    }
}

/// An ingest past the memtable's budget folds it into the RAM tier on its own.
#[test]
fn a_memtable_over_budget_folds_into_the_ram_tier() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = new_table(
        dir.path(),
        make_schema_u64_i64(),
        RecoverySource::SalReplay,
        DEFAULT_RAM_TIER_BYTES,
    );
    let n = MEMTABLE_BYTES as u64 / 32 + 1;
    t.ingest_owned_batch(rows(&(0..n).map(|k| (k, 1, 0)).collect::<Vec<_>>()))
        .unwrap();
    assert_eq!(t.memtable.len(), 0, "the ingest folded the memtable");
    assert_eq!(t.ram_tier.row_count() as u64, n);
}

/// A damaged manifest fails a `SalReplay` open, whose shards are its only copy;
/// a `Rederive` open rebuilds empty — unless reading the manifest itself failed.
#[test]
fn a_damaged_manifest_fails_a_replayed_open_and_rebuilds_a_rederived_one() {
    type Damage = fn(&Path);
    let damages: [(&str, Damage); 4] = [
        ("truncated", |m| {
            std::fs::write(m, &std::fs::read(m).unwrap()[..20]).unwrap()
        }),
        ("bad magic", |m| {
            let mut b = std::fs::read(m).unwrap();
            b[0] ^= 0xff;
            std::fs::write(m, b).unwrap();
        }),
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
            t.ingest_owned_batch(rows(&[(1, 1, 100)])).unwrap();
            flush_barrier([&mut t], 7).unwrap();
            drop(t);
            damage(Path::new(&manifest_path(dir.path().to_str().unwrap())));

            let opened = Table::new(dir.path().to_str().unwrap(), schema, rs, StoreBudgets::default());
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
        t.ingest_owned_batch(rows(&[(1, 1, 100), (2, 1, 200)])).unwrap();
        flush_barrier([&mut t], 7).unwrap();
    }
    let t = reopen(7);
    assert!(t.resumed_from_checkpoint());
    assert_eq!(t.full_scan().len(), 2, "a matching generation loads");
    drop(t);
    let t = reopen(8);
    assert!(!t.resumed_from_checkpoint());
    assert_eq!(t.full_scan().len(), 0, "a mismatched generation erases");
    assert!(!std::fs::exists(&manifest).unwrap());
}

/// A reopen reports the last published checkpoint mark; no manifest reports 0.
#[test]
fn a_barriers_checkpoint_mark_is_what_the_reopen_reports() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut t = new_table(dir.path(), schema, RecoverySource::SalReplay, 1 << 20);
    assert_eq!(t.checkpoint_mark(), 0, "a fresh store carries no mark");
    t.ingest_owned_batch(rows(&[(1, 1, 10)])).unwrap();
    flush_barrier([&mut t], 9).unwrap();
    t.ingest_owned_batch(rows(&[(2, 1, 10)])).unwrap();
    flush_barrier([&mut t], 7).unwrap();
    assert_eq!(t.checkpoint_mark(), 0, "the open's mark is read-only");
    drop(t);
    let t = new_table(dir.path(), schema, RecoverySource::SalReplay, 1 << 20);
    assert_eq!(t.checkpoint_mark(), 7, "the last published mark, not the highest");
}

/// A barrier publishes exactly when the manifest would change — rows, shards or
/// caller record — and on a process's first round.
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
        t.ingest_owned_batch(round(r, 1)).unwrap();
        assert!(publishes(&mut t), "written rows publish");
    }
    for r in 0..L0_COMPACT_THRESHOLD as u64 {
        t.ingest_owned_batch(round(r, -1)).unwrap();
    }
    t.fold_to_ram().unwrap();
    assert_eq!(t.shard_index.shard_count(), 0, "the compaction cancelled everything");
    assert!(publishes(&mut t), "an emptied index publishes");
    drop(t);

    let mut t = new_table(dir.path(), schema, resume, 100);
    assert_eq!(t.caller_record, b"first", "the record reloads with its manifest");
    assert_eq!(t.full_scan().len(), 0, "no retracted row comes back");
    assert!(publishes(&mut t), "a reopened store's first round publishes");
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
            t.ingest_owned_batch(rows(&(0..10).map(|k| (r * 100 + k, 1, 1)).collect::<Vec<_>>()))
                .unwrap();
            t.fold_to_ram().unwrap();
        }
        let (l0, levels) = t.level_shape();
        assert!(
            l0 > 0 && levels.iter().sum::<usize>() > 0,
            "spills and compaction outputs"
        );
        let shards = t.all_shard_arcs();
        assert!(shards.iter().all(|s| s.has_shard_filter() == filtered), "{rs:?}");
    }
}

/// A RAM tier past its ceiling folds, and stays in RAM when the fold leaves it
/// room; folded to within a sixteenth of the ceiling it spills, since every
/// later crossing would fold it whole again.
#[test]
fn a_tier_folded_to_just_under_its_ceiling_spills() {
    const CEILING_ROWS: u64 = 1 << 15;
    let dir = tempfile::tempdir().unwrap();
    for (held, spills) in [(CEILING_ROWS * 7 / 8, false), (CEILING_ROWS * 31 / 32, true)] {
        let mut t = new_table(
            dir.path().join(held.to_string()),
            make_schema_u64_i64(),
            RecoverySource::Rederive { resume_at: None },
            CEILING_ROWS as usize * 32,
        );
        t.ingest_owned_batch(rows(&(0..held).map(|k| (k, 1, 0)).collect::<Vec<_>>()))
            .unwrap();
        t.fold_to_ram().unwrap();
        assert_eq!(t.all_shard_arcs().len(), 0, "{held}: under the ceiling");
        // Updates of a quarter of the ceiling's rows: each a retraction and an insert.
        let updates = (0..CEILING_ROWS / 4).flat_map(|k| [(k, -1, 0), (k, 1, 1)]);
        t.ingest_owned_batch(rows(&updates.collect::<Vec<_>>())).unwrap();
        t.fold_to_ram().unwrap();
        assert_eq!(!t.all_shard_arcs().is_empty(), spills, "{held} rows held");
        assert_eq!(t.full_scan().len() as u64, held);
    }
}

/// A store held as a read replica of another process's directory never writes
/// into it, however far past its RAM-tier ceiling it grows.
#[test]
fn a_store_held_in_ram_never_spills() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = new_table(dir.path(), make_schema_u64_i64(), RecoverySource::SalReplay, 96);
    t.hold_in_ram();
    for r in 0..20u64 {
        t.ingest_owned_batch(rows(&(0..10).map(|k| (r * 100 + k, 1, 1)).collect::<Vec<_>>()))
            .unwrap();
        t.fold_to_ram().unwrap();
    }
    assert_eq!(shard_files(dir.path()), 0, "no shard file was written");
    assert_eq!(t.full_scan().len(), 200, "every row held in RAM");
}

/// A seal folds its delta into the memtable unless the delta is a small
/// fraction of it; either way the delta it answers and the rows it holds are
/// the pushes'.
#[test]
fn a_seal_folds_the_memtable_only_for_a_sizable_delta() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut t = new_table(dir.path(), schema, RecoverySource::SalReplay, 1 << 20);
    let mut pushed = Vec::new();
    let mut seal = |t: &mut Table, keys: std::ops::Range<u64>| {
        let batch = rows(&keys.map(|k| (k, 1, 0)).collect::<Vec<_>>());
        t.ingest_pending(batch.clone());
        let delta = t.seal().unwrap().expect("rows were pending");
        assert_eq!(zset_of(&delta, &schema), zset_of(&batch, &schema), "the delta");
        pushed.push(batch);
        assert_eq!(zset_of(&t.full_scan(), &schema), zset_sum(&pushed, &schema));
    };
    let held = 4 * SEAL_FOLD_RATIO as u64;
    seal(&mut t, 0..held);
    seal(&mut t, held..held + 1);
    assert_eq!(t.memtable.len(), 2, "a one-row delta stays a run of its own");
    seal(&mut t, held + 1..2 * held);
    assert_eq!(t.memtable.len(), 1, "a delta as long as the memtable folds it");
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
