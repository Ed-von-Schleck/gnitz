use super::*;
use crate::test_support::{
    RowKey, arb_fold_case, certified, fold_batch, make_batch, make_batch_bytes, make_schema_pk_u64_payload_string,
    make_schema_u64_i64, opk_pk, row_key, u64_pk_schema, weighted_rows, zset_add, zset_of, zset_sum,
};
use gnitz_wire::TypeCode;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::SchemaColumn;
use proptest::prelude::*;
use std::collections::{BTreeSet, HashMap};

proptest! {
    /// A set holds the Z-set sum of its pushes since the last clear, in fewer
    /// than `FOLD_THRESHOLD` consolidated runs, its PK probe reaches exactly
    /// each pushed key's rows, and a spill empties it only once its rows are
    /// written.
    #[test]
    fn a_run_set_holds_the_zset_of_its_pushes(
        (s, rows) in arb_fold_case(),
        steps in prop::collection::vec((0usize..10, 0u8..8), 1..40),
        cancelling in any::<bool>(),
    ) {
        let mut set = if cancelling { RunSet::cancelling(1 << 20) } else { RunSet::new(1 << 20) };
        let mut pushed: Vec<Batch> = Vec::new();
        let mut rows = rows.iter().cycle();
        for (n, action) in steps {
            let run = fold_batch(&s, &rows.by_ref().take(n).cloned().collect::<Vec<_>>()).into_consolidated();
            pushed.push(run.clone());
            set.push_run(Rc::new(run.trimmed()), &s);
            match action {
                0 => set.fold(&s),
                1 => {
                    set.clear();
                    pushed.clear();
                }
                _ => {}
            }
            let want = zset_sum(&pushed, &s);
            let held: Vec<Batch> = set.batches().map(|r| (**r).clone()).collect();
            prop_assert_eq!(&zset_sum(&held, &s), &want);
            prop_assert!(set.len() < FOLD_THRESHOLD);
            prop_assert!(held.iter().all(Batch::is_consolidated));
            prop_assert_eq!(set.bytes, held.iter().map(Batch::total_bytes).sum::<usize>());

            // Every pushed key, a cancelled one included: a probe that missed
            // rows summing to zero would pass a per-key weight check.
            let keys: BTreeSet<&[u8]> = pushed.iter().flat_map(|b| (0..b.len()).map(|i| b.get_pk_bytes(i))).collect();
            for key in keys {
                let mut got: HashMap<RowKey, i64> = HashMap::new();
                for (run, rows) in set.holding(key, probe_key(key)) {
                    zset_add(&mut got, rows.map(|r| (row_key(&**run, &s, r), run.get_weight(r))));
                }
                let want_key: HashMap<RowKey, i64> =
                    want.iter().filter(|(k, _)| k.0 == key).map(|(k, w)| (k.clone(), *w)).collect();
                prop_assert_eq!(got, want_key);
            }
        }
        let held = zset_sum(&pushed, &s);
        let refused = StorageError::Io(libc::EIO);
        let failed = set.spill(&s, |_| Err(refused));
        prop_assert_eq!(failed, if held.is_empty() { Ok(()) } else { Err(refused) });
        let mut written = Default::default();
        let wrote = set.spill(&s, |run| {
            written = zset_of(run, &s);
            Ok(())
        });
        prop_assert_eq!(wrote, Ok(()));
        prop_assert_eq!(written, held, "the failed write left every row held");
        prop_assert_eq!((set.len(), set.bytes), (0, 0));
    }
}

fn keyed_run(schema: &SchemaDescriptor, keys: std::ops::Range<u64>, w: i64, v: i64) -> Batch {
    certified(make_batch(schema, &keys.map(|k| (k, w, v)).collect::<Vec<_>>()))
}

/// Whether a probe of `set` finds a row under PK `k`.
fn holds(set: &RunSet, schema: &SchemaDescriptor, k: u64) -> bool {
    let key = opk_pk(schema, &[k as u128]);
    set.holding(&key, probe_key(&key)).next().is_some()
}

/// Only a run cancelling rows of the one before it, and not much smaller than
/// it, merges.
#[test]
fn a_cancelling_set_merges_only_a_run_that_cancels_the_one_before_it() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::cancelling(usize::MAX);
    set.push_run(Rc::new(keyed_run(&schema, 0..100, 1, 0).trimmed()), &schema);
    set.push_run(Rc::new(keyed_run(&schema, 100..200, 1, 0).trimmed()), &schema);
    assert_eq!(set.len(), 2, "an insert-only run stays apart");

    let update: Vec<_> = (100..160).flat_map(|k| [(k, -1, 0), (k, 1, 1)]).collect();
    set.push_run(Rc::new(certified(make_batch(&schema, &update)).trimmed()), &schema);
    assert_eq!(set.len(), 2, "the update cancelled into the run before it");
    assert_eq!(set.row_count(), 200, "its retractions and their rows are gone");

    set.push_run(Rc::new(keyed_run(&schema, 150..160, -1, 1).trimmed()), &schema);
    assert_eq!(set.len(), 3, "a run much larger than its successor is not merged into");

    set.push_run(Rc::new(keyed_run(&schema, 0..50, -1, 0).trimmed()), &schema);
    assert_eq!(set.len(), 4, "retractions of an older run's rows stay apart");
    set.fold(&schema);
    assert_eq!(set.row_count(), 140);
}

/// A probed set's filter is sized to its rows: a fold that cancels a few rows
/// keeps it, an overfull push drops it for the next probe to rebuild larger
/// until the budget caps it, an overfull filter at the cap stays until a fold
/// cancels most of what went in, and probes answer alike throughout.
#[test]
fn a_filter_grows_with_its_rows_and_is_dropped_once_most_of_its_keys_are_gone() {
    let schema = make_schema_u64_i64();
    // Caps the filter at 64 blocks: room for 1638 keys.
    let mut set = RunSet::new(1638 * schema.row_width());
    let push = |set: &mut RunSet, keys: std::ops::Range<u64>, w: i64| {
        set.push_run(Rc::new(keyed_run(&schema, keys, w, 7).trimmed()), &schema)
    };

    push(&mut set, 0..10, 1);
    assert!(holds(&set, &schema, 3), "the first probe builds the filter");
    push(&mut set, 0..5, -1);
    set.fold(&schema);
    assert!(set.bloom.get().is_some());
    assert!(holds(&set, &schema, 7) && !holds(&set, &schema, 3));

    push(&mut set, 100..600, 1);
    assert!(
        set.bloom.get().is_none(),
        "an overfull push drops the floor-sized filter"
    );
    assert!(holds(&set, &schema, 105), "the next probe rebuilds it");
    push(&mut set, 100..600, -1);
    set.fold(&schema);
    assert!(set.bloom.get().is_some());

    push(&mut set, 1000..1400, 1);
    push(&mut set, 1000..1400, -1);
    assert!(set.bloom.get().is_some(), "overfull, but at the budget's cap");
    set.fold(&schema);
    assert!(set.bloom.get().is_none());
    assert!(holds(&set, &schema, 7) && !holds(&set, &schema, 3));
    assert!(!holds(&set, &schema, 105) && !holds(&set, &schema, 1005));
}

/// A set whose budget holds fewer rows than the filter's floor still probes.
#[test]
fn a_set_with_a_tiny_budget_probes() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(0);
    set.push_run(Rc::new(keyed_run(&schema, 0..10, 1, 7).trimmed()), &schema);
    assert!(holds(&set, &schema, 3) && !holds(&set, &schema, 10));
}

/// Widening fills the new column with NULL, recounts the set's bytes, and
/// keeps the filter, whose keys it leaves untouched.
#[test]
fn widening_fills_nulls_and_keeps_the_filter() {
    let narrow = make_schema_u64_i64();
    let wide = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let mut set = RunSet::new(1 << 20);
    set.push_run(Rc::new(keyed_run(&narrow, 0..10, 1, 7).trimmed()), &narrow);
    set.push_run(Rc::new(keyed_run(&narrow, 10..20, 1, 7).trimmed()), &narrow);
    assert!(holds(&set, &narrow, 15), "the first probe builds the filter");

    set.widen_runs(&wide);
    assert!(set.bloom.get().is_some());
    assert!(holds(&set, &wide, 15));
    assert_eq!(set.bytes, set.batches().map(|r| r.total_bytes()).sum::<usize>());
    for run in set.batches() {
        assert!(
            weighted_rows(run)
                .iter()
                .all(|((_, v), _)| v.len() == 2 && v[1].is_none())
        );
    }
}

/// Runs from either side of an `ALTER … DROP NOT NULL` fold under the schema the
/// fold is handed, not the dominant run's NOT NULL label.
#[test]
fn a_fold_merges_runs_of_different_nullability_under_its_own_schema() {
    let label = |nullable| u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, nullable)]);
    let (not_null, nullable) = (label(false), label(true));
    let run = |schema: SchemaDescriptor, rows: &[(u128, Option<i64>, i64)]| {
        let mut b = BatchBuilder::new(&schema);
        for &(pk, v, w) in rows {
            b.begin_row(pk, w);
            b.put_opt_int(v.map(|v| v as u128));
            b.end_row();
        }
        let mut b = b.finish();
        b.certify_consolidated();
        b
    };

    let mut set = RunSet::new(1 << 20);
    set.push_run(
        Rc::new(run(not_null, &[(1, Some(-3), 1), (2, Some(7), 1), (3, Some(8), 1)]).trimmed()),
        &not_null,
    );
    set.push_run(
        Rc::new(run(nullable, &[(1, None, 1), (1, Some(-3), -1)]).trimmed()),
        &nullable,
    );
    set.fold(&nullable);

    let folded = &set.runs[0].batch;
    assert!(*folded.schema() == nullable);
    let want = run(nullable, &[(1, None, 1), (2, Some(7), 1), (3, Some(8), 1)]);
    assert_eq!(weighted_rows(folded), weighted_rows(&want));
}

/// A 42-byte string payload naming `pk` at update generation `generation`;
/// zero-padded, so a later generation sorts after an earlier one.
fn churn_value(pk: u64, generation: u64) -> Vec<u8> {
    format!("{pk:08}-{generation:08}-{}", "v".repeat(24)).into_bytes()
}

/// Updates retract rows whose spans a carried heap keeps; every stored run
/// stays trimmed while carrying dead bytes.
#[test]
fn a_fold_under_churn_keeps_every_run_trimmed() {
    let schema = make_schema_pk_u64_payload_string();
    const KEYS: u64 = 1000;
    let mut set = RunSet::new(usize::MAX);
    let mut latest = vec![0u64; KEYS as usize];
    let push = |set: &mut RunSet, rows: &[(u64, i64, Vec<u8>)]| {
        set.push_run(Rc::new(certified(make_batch_bytes(&schema, rows)).trimmed()), &schema);
    };
    push(
        &mut set,
        &(0..KEYS).map(|pk| (pk, 1, churn_value(pk, 0))).collect::<Vec<_>>(),
    );

    let mut saw_dead = false;
    for generation in 1..=300u64 {
        let mut rows = Vec::new();
        for i in 0..5 {
            let pk = (i * 200 + generation * 7) % KEYS;
            rows.push((pk, -1, churn_value(pk, latest[pk as usize])));
            rows.push((pk, 1, churn_value(pk, generation)));
            latest[pk as usize] = generation;
        }
        rows.sort_by(|a, b| (a.0, &a.2).cmp(&(b.0, &b.2)));
        push(&mut set, &rows);
        for run in set.batches() {
            saw_dead |= run.dead_heap() > 0;
            let trimmed = (**run).clone().trimmed();
            assert_eq!(
                (trimmed.blob().len(), trimmed.dead_heap()),
                (run.blob().len(), run.dead_heap()),
                "a stored run is not trimmed"
            );
        }
    }
    assert!(
        saw_dead,
        "the churn must drive a fold that carries a heap with dead bytes"
    );
}
