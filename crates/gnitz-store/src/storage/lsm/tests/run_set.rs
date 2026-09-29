use super::*;
use crate::storage::BatchBuilder;
use crate::test_support::{make_batch, make_batch_raw, make_schema_u64_i64};

fn push(set: &mut RunSet, schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) {
    set.push(TrimmedRun::new(make_batch(schema, rows)), schema);
}

/// Probe by a narrow PK, deriving the filter key the way the production walk
/// does so no assertion spells a second version of it.
fn probes(set: &RunSet, pk: u64) -> bool {
    set.may_contain(probe_key(&pk.to_be_bytes()))
}

#[test]
fn fold_sums_weights_and_drops_ghosts() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    assert!(set.is_empty());

    push(&mut set, &schema, &[(10, 1, 100), (30, 1, 300)]);
    push(&mut set, &schema, &[(20, 1, 200), (30, -1, 300)]);
    assert_eq!(set.len(), 2);

    let folded = set.fold_to_single(&schema).expect("survivors remain");
    assert_eq!(folded.count, 2, "PK 30 cancels to a ghost");
    assert_eq!(folded.get_pk(0), 10);
    assert_eq!(folded.get_pk(1), 20);
}

/// A single run folds by handing back the run itself — no rewrite.
#[test]
fn fold_to_single_is_identity_for_one_run() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    push(&mut set, &schema, &[(10, 1, 100), (20, 1, 200)]);
    let original = Rc::clone(&set.runs()[0]);

    let folded = set.fold_to_single(&schema).expect("one run");
    assert!(std::ptr::eq(&*folded, &*original), "singleton fold must not rewrite");
}

#[test]
fn empty_and_fully_cancelled_sets_fold_to_none() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    assert!(set.fold_to_single(&schema).is_none(), "empty set");

    set.push(TrimmedRun::new(make_batch(&schema, &[])), &schema);
    assert!(set.is_empty(), "a 0-row push stores no run");

    push(&mut set, &schema, &[(1, 1, 10)]);
    push(&mut set, &schema, &[(1, -1, 10)]);
    assert!(set.fold_to_single(&schema).is_none(), "fully cancelled set");
    assert!(set.is_empty());
    assert_eq!(set.bytes(), 0, "byte total tracks the fold");
}

/// Pushing past the threshold folds inline, so the run count never exceeds it.
#[test]
fn push_folds_at_the_threshold() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    for i in 0..FOLD_THRESHOLD as u64 - 1 {
        push(&mut set, &schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
    }
    assert_eq!(set.len(), FOLD_THRESHOLD - 1, "below the threshold: no fold");

    push(&mut set, &schema, &[(FOLD_THRESHOLD as u64, 1, 1600)]);
    assert_eq!(set.len(), 1, "the threshold push folds");
    assert_eq!(set.row_count(), FOLD_THRESHOLD);
}

/// The bloom answers for every live PK, is maintained across pushes once
/// built, and survives a fold (rebuilt lazily from the survivors).
#[test]
fn bloom_covers_live_rows_across_push_and_fold() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    push(&mut set, &schema, &[(10, 1, 100), (20, 1, 200)]);

    assert!(probes(&set, 10), "first probe builds the filter");
    assert!(probes(&set, 20));

    // Maintained incrementally once built.
    push(&mut set, &schema, &[(30, 1, 300)]);
    assert!(probes(&set, 30));

    set.fold(&schema);
    for pk in [10u64, 20, 30] {
        assert!(probes(&set, pk), "PK {pk} after fold");
    }
}

/// A run handed out by `runs()` stays readable after the set is cleared —
/// the cursor-lifetime contract.
#[test]
fn handed_out_runs_survive_clear() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    push(&mut set, &schema, &[(10, 1, 100)]);
    let held = Rc::clone(&set.runs()[0]);

    set.clear();
    assert!(set.is_empty());
    assert_eq!(held.count, 1);
    assert_eq!(held.get_pk(0), 10);
}

/// A 2-row batch with descending PKs — not `(PK, payload)`-sorted.
fn desc_two_row_batch(schema: &SchemaDescriptor) -> Batch {
    make_batch_raw(schema, &[(20, 1, 200), (10, 1, 100)])
}

/// A run that lies about being consolidated is rejected on the way in.
/// `set_layout_unchecked` stamps the tag without inspecting the data, so the
/// descending batch is built without complaint — but `push`, which skips a
/// re-fold on the strength of that tag, verifies it and panics. In production
/// the ingress strip clears the layout, so such a run never reaches a set.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "flagged consolidated")]
fn lying_consolidated_run_is_rejected() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);

    let mut bad = desc_two_row_batch(&schema);
    bad.set_layout_unchecked(crate::storage::Layout::Consolidated);
    set.push(TrimmedRun::new(bad), &schema);
}

/// The same unsorted rows with the flags stripped (as the ingress strip
/// leaves every client batch) sort+consolidate and push without complaint.
#[test]
fn cleared_flags_unsorted_run_consolidates_ok() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    push(&mut set, &schema, &[(5, 1, 50)]);

    let clean = desc_two_row_batch(&schema);
    set.push(TrimmedRun::new(clean.into_consolidated(&schema)), &schema);

    let folded = set.fold_to_single(&schema).expect("three rows survive");
    assert_eq!(folded.count, 3);
    assert_eq!(folded.get_pk(0), 5);
    assert_eq!(folded.get_pk(1), 10);
    assert_eq!(folded.get_pk(2), 20);
}

/// Reduce-output shape: insertion + retraction across ticks, where each tick
/// retracts the previous aggregate and inserts the new one. Only the last
/// tick's aggregate may survive the fold.
#[test]
fn reduce_output_folds_to_the_latest_aggregate() {
    use crate::schema::{SchemaColumn, TypeCode};

    // U128 PK + I64 group_val + I64 agg_val.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let make = |rows: &[(u128, i64, i64, i64)]| {
        let mut b = BatchBuilder::new(schema);
        for &(pk, w, gv, av) in rows {
            b.begin_row(pk, w);
            b.put_int(gv as u128);
            b.put_int(av as u128);
            b.end_row();
        }
        let b = b.finish();
        TrimmedRun::new(b.into_consolidated(&schema))
    };

    let mut set = RunSet::new(1 << 20);
    set.push(make(&[(0, 1, 0, 5000)]), &schema);
    set.push(make(&[(0, -1, 0, 5000), (0, 1, 0, 10000)]), &schema);
    set.push(make(&[(0, -1, 0, 10000), (0, 1, 0, 15000)]), &schema);

    let folded = set.fold_to_single(&schema).expect("the latest aggregate survives");
    assert_eq!(folded.count, 1, "only the latest aggregate remains");
    assert_eq!(folded.get_pk(0), 0);
    assert_eq!(folded.get_weight(0), 1);
    let agg = i64::from_le_bytes(folded.get_col_ptr(0, 1, 8).try_into().unwrap());
    assert_eq!(agg, 15000);
}

#[test]
fn byte_total_tracks_pushes_and_folds() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    assert_eq!(set.bytes(), 0);
    push(&mut set, &schema, &[(1, 1, 10)]);
    let one = set.bytes();
    assert!(one > 0);
    push(&mut set, &schema, &[(2, 1, 20)]);
    assert_eq!(set.bytes(), 2 * one, "pushes accumulate");

    set.fold(&schema);
    assert_eq!(
        set.bytes(),
        set.runs()[0].total_bytes(),
        "fold re-derives from the merged run"
    );
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
    use crate::storage::repr::merge::heap_is_wasteful;
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
        for run in set.runs() {
            saw_dead |= run.dead_heap > 0;
            assert!(
                !heap_is_wasteful(run.dead_heap, run.blob().len()),
                "a stored run is {} of {} bytes dead",
                run.dead_heap,
                run.blob().len()
            );
        }
    }
    assert!(
        saw_dead,
        "the churn must drive a fold that carries a heap with dead bytes"
    );

    let folded = set.fold_to_single(&schema).expect("every key survives");
    let got: Vec<(u128, i64, Vec<u8>)> = (0..folded.count)
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
            black_box(set.runs());
            println!("run_set_fold_strings {shape}: {i} instructions, {elapsed:?}");
        }
    }
}
