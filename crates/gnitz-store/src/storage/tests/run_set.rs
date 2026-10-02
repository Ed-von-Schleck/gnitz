use super::*;
use crate::test_support::{arb_fold_case, fold_batch, fold_schemas, zset_of, zset_sum};
use gnitz_zset::repr::pk_group_end;
use gnitz_zset::repr::BatchBuilder;
use proptest::prelude::*;

proptest! {
    /// A set holds the Z-set sum of its pushes since the last clear, in fewer
    /// than `FOLD_THRESHOLD` runs, and its PK probe reaches each key's rows.
    #[test]
    fn a_run_set_holds_the_zset_of_its_pushes(
        (si, rows) in arb_fold_case(),
        steps in prop::collection::vec((0usize..10, 0u8..8), 1..40),
    ) {
        let s = fold_schemas()[si];
        let mut set = RunSet::new(1 << 20);
        let mut pushed: Vec<Batch> = Vec::new();
        let mut rows = rows.iter().cycle();
        for (n, action) in steps {
            let run = fold_batch(&s, &rows.by_ref().take(n).cloned().collect::<Vec<_>>()).into_consolidated();
            pushed.push(run.clone());
            set.push(TrimmedRun::new(run), &s);
            match action {
                0 => set.fold(&s),
                1 => {
                    set.clear();
                    pushed.clear();
                }
                _ => {}
            }
            let held: Vec<Batch> = set.runs.iter().map(|r| (**r).clone()).collect();
            prop_assert_eq!(zset_sum(&held, &s), zset_sum(&pushed, &s));
            prop_assert!(set.len() < FOLD_THRESHOLD);
            prop_assert_eq!(set.bytes, held.iter().map(Batch::total_bytes).sum::<usize>());

            let want = zset_sum(&pushed, &s);
            for key in want.keys().map(|k| &k.0) {
                let mut sum = 0;
                set.find_pk_bytes(key, probe_key(key), |run, start| {
                    sum += (start..pk_group_end(&**run, start)).map(|r| run.get_weight(r)).sum::<i64>();
                });
                let want_sum: i64 = want.iter().filter(|(k, _)| &k.0 == key).map(|(_, w)| w).sum();
                prop_assert_eq!(sum, want_sum);
            }
        }
        if let Some(folded) = set.fold_to_single(&s) {
            prop_assert_eq!(zset_of(&folded, &s), zset_sum(&pushed, &s));
        }
    }
}

/// A probed set keeps its filter through a fold that cancels a few rows, drops
/// it at one that cancels most of what went in, and answers its probes alike.
#[test]
fn a_fold_drops_the_filter_once_most_of_its_keys_are_gone() {
    use crate::test_support::{make_batch_raw, make_schema_u64_i64, opk_pk};
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(0);
    let push = |set: &mut RunSet, keys: std::ops::Range<u64>, w: i64| {
        let rows: Vec<_> = keys.map(|k| (k, w, 7)).collect();
        set.push(
            TrimmedRun::new(make_batch_raw(&schema, &rows).into_consolidated()),
            &schema,
        );
    };
    let holds = |set: &RunSet, k: u64| {
        let key = opk_pk(&schema, &[k as u128]);
        let mut found = false;
        set.find_pk_bytes(&key, probe_key(&key), |_, _| found = true);
        found
    };

    push(&mut set, 0..10, 1);
    assert!(holds(&set, 3), "the first probe builds the filter");
    push(&mut set, 0..5, -1);
    set.fold(&schema);
    assert!(set.bloom.get().is_some());
    assert!(holds(&set, 7) && !holds(&set, 3));

    push(&mut set, 100..1100, 1);
    push(&mut set, 100..1100, -1);
    set.fold(&schema);
    assert!(set.bloom.get().is_none());
    assert!(holds(&set, 7) && !holds(&set, 3) && !holds(&set, 105));
}

/// Runs from either side of an `ALTER … DROP NOT NULL` fold under the schema the
/// fold is handed, not the dominant run's NOT NULL label.
#[test]
fn a_fold_merges_runs_of_different_nullability_under_its_own_schema() {
    use gnitz_wire::TypeCode;
    use gnitz_zset::schema::SchemaColumn;

    let label = |nullable| {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(TypeCode::I64, nullable),
            ],
            &[0],
        )
    };
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
        TrimmedRun::new(b)
    };

    let mut set = RunSet::new(1 << 20);
    set.push(
        run(not_null, &[(1, Some(-3), 1), (2, Some(7), 1), (3, Some(8), 1)]),
        &not_null,
    );
    set.push(run(nullable, &[(1, None, 1), (1, Some(-3), -1)]), &nullable);
    set.fold(&nullable);

    let folded = &set.runs[0];
    let got: Vec<(u128, Option<i64>, i64)> = (0..folded.len())
        .map(|i| {
            let v = (folded.get_null_word(i) & 1 == 0)
                .then(|| i64::from_le_bytes(folded.get_col_ptr(i, 0, 8).try_into().unwrap()));
            (folded.get_pk(i), v, folded.get_weight(i))
        })
        .collect();
    assert_eq!(got, vec![(1, None, 1), (2, Some(7), 1), (3, Some(8), 1)]);
}

/// A 42-byte string payload naming `pk` at update generation `generation`;
/// zero-padded, so a later generation sorts after an earlier one.
fn churn_value(pk: u64, generation: u64) -> Vec<u8> {
    format!("{pk:08}-{generation:08}-{}", "v".repeat(24)).into_bytes()
}

/// A consolidated `(U64 pk, STRING)` run of `(pk, weight, bytes)` rows.
fn string_run(schema: &SchemaDescriptor, rows: &[(u64, i64, Vec<u8>)]) -> Batch {
    let rows: Vec<(u64, i64, &[u8])> = rows.iter().map(|(k, w, v)| (*k, *w, &v[..])).collect();
    crate::test_support::make_batch_bytes(schema, &rows)
}

/// Updates retract rows whose spans a carried heap keeps; every fold still
/// leaves each stored run at most a quarter dead, and the set reads back the
/// latest value of every key.
#[test]
fn a_fold_under_churn_keeps_every_run_at_most_a_quarter_dead() {
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
    const KEYS: u64 = 1000;
    let mut set = RunSet::new(usize::MAX);
    let mut latest = vec![0u64; KEYS as usize];
    let initial: Vec<_> = (0..KEYS).map(|pk| (pk, 1, churn_value(pk, 0))).collect();
    set.push(TrimmedRun::new(string_run(&schema, &initial)), &schema);

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
        set.push(TrimmedRun::new(string_run(&schema, &rows)), &schema);
        for run in &set.runs {
            saw_dead |= run.dead_heap() > 0;
            assert!(
                run.dead_heap() * 4 <= run.blob().len(),
                "a stored run is {} of {} bytes dead",
                run.dead_heap(),
                run.blob().len()
            );
        }
    }
    assert!(
        saw_dead,
        "the churn must drive a fold that carries a heap with dead bytes"
    );

    let folded = set.fold_to_single(&schema).expect("every key survives");
    let got: Vec<(u128, i64, Vec<u8>)> = (0..folded.len())
        .map(|row| {
            let s = crate::test_support::read_german_string(&folded, 0, row);
            (folded.get_pk(row), folded.get_weight(row), s)
        })
        .collect();
    let want: Vec<_> = (0..KEYS)
        .map(|pk| (pk as u128, 1, churn_value(pk, latest[pk as usize])))
        .collect();
    assert_eq!(got, want);
}

/// Fold a 200k-row dominant run with fourteen 10k-row runs of 40-byte strings;
/// only the fold is measured. `append` runs hold fresh keys only; `churn` runs
/// are half retractions of dominant rows and half fresh keys.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn run_set_fold_strings_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    use std::time::Instant;
    const DOMINANT: u64 = 200_000;
    const RUNS: u64 = 14;
    const PER_RUN: u64 = 10_000;
    let schema = crate::test_support::make_schema_pk_u64_payload_string();
    let value = |pk: u64| format!("{pk:010}-{}", "s".repeat(29)).into_bytes();
    let dominant = string_run(
        &schema,
        &(0..DOMINANT).map(|i| (2 * i, 1, value(2 * i))).collect::<Vec<_>>(),
    );
    let append: Vec<Batch> = (0..RUNS)
        .map(|k| {
            let keys = k * PER_RUN..(k + 1) * PER_RUN;
            string_run(
                &schema,
                &keys.map(|j| (2 * j + 1, 1, value(2 * j + 1))).collect::<Vec<_>>(),
            )
        })
        .collect();
    let churn: Vec<Batch> = (0..RUNS)
        .map(|k| {
            let rows: Vec<_> = (k * PER_RUN / 2..(k + 1) * PER_RUN / 2)
                .flat_map(|j| [(2 * j, -1, value(2 * j)), (2 * j + 1, 1, value(2 * j + 1))])
                .collect();
            string_run(&schema, &rows)
        })
        .collect();
    let instructions = Counter::instructions().unwrap();
    for (shape, small) in [("append", &append), ("churn", &churn)] {
        for _ in 0..5 {
            let mut set = RunSet::new(usize::MAX);
            set.push(TrimmedRun::new(dominant.clone()), &schema);
            for run in small {
                set.push(TrimmedRun::new(run.clone()), &schema);
            }
            assert_eq!(set.len(), 1 + RUNS as usize, "the fold must not have run yet");
            let t = Instant::now();
            let ((), i) = instructions.measure(|| set.fold(&schema));
            let elapsed = t.elapsed();
            black_box(&set.runs);
            println!("run_set_fold_strings {shape}: {i} instructions, {elapsed:?}");
        }
    }
}

/// Runs that ascend past one another fold to their rows in order, whichever of
/// them dominates; one run reaching back into another's keys folds by merge to
/// the same Z-set.
#[test]
fn ascending_runs_fold_to_their_rows_in_order() {
    use crate::test_support::{make_batch_raw, make_schema_u64_i64};
    let schema = make_schema_u64_i64();
    for (first_run, reach_back) in [(10u64, false), (10, true), (1000, false), (1000, true)] {
        let mut set = RunSet::new(usize::MAX);
        let mut pushed: Vec<Batch> = Vec::new();
        let mut next = 0u64;
        let mut push = |set: &mut RunSet, rows: Vec<(u64, i64, i64)>| {
            let run = make_batch_raw(&schema, &rows).into_consolidated();
            pushed.push(run.clone());
            set.push(TrimmedRun::new(run), &schema);
        };
        for n in std::iter::once(first_run).chain(std::iter::repeat_n(10, 9)) {
            push(&mut set, (next..next + n).map(|k| (k, 1, k as i64)).collect());
            next += n;
        }
        if reach_back {
            push(&mut set, vec![(3, -1, 3), (next, 1, 0)]);
        }
        let folded = set.fold_to_single(&schema).expect("rows survive");
        assert!(folded.consolidated_verified());
        assert_eq!(zset_of(&folded, &schema), zset_sum(&pushed, &schema));
        assert_eq!(folded.len(), next as usize);
    }
}
