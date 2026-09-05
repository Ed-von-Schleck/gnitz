use super::*;
use crate::test_support::{make_batch, make_batch_raw, make_schema_u64_i64};

fn push(set: &mut RunSet, schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) {
    set.push(Rc::new(make_batch(schema, rows)), schema);
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
    assert!(Rc::ptr_eq(&folded, &original), "singleton fold must not rewrite");
}

#[test]
fn empty_and_fully_cancelled_sets_fold_to_none() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    assert!(set.fold_to_single(&schema).is_none(), "empty set");

    set.push(Rc::new(make_batch(&schema, &[])), &schema);
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
    bad.set_layout_unchecked(Layout::Consolidated);
    set.push(Rc::new(bad), &schema);
}

/// The same unsorted rows with the flags stripped (as the ingress strip
/// leaves every client batch) sort+consolidate and push without complaint.
#[test]
fn cleared_flags_unsorted_run_consolidates_ok() {
    let schema = make_schema_u64_i64();
    let mut set = RunSet::new(1 << 20);
    push(&mut set, &schema, &[(5, 1, 50)]);

    let clean = desc_two_row_batch(&schema);
    set.push(Rc::new(clean.into_consolidated(&schema)), &schema);

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
    use crate::schema::{type_code, SchemaColumn};

    // U128 PK + I64 group_val + I64 agg_val.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let make = |rows: &[(u128, i64, i64, i64)]| {
        let mut b = Batch::with_capacity(schema, rows.len().max(1));
        for &(pk, w, gv, av) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &gv.to_le_bytes());
            b.extend_col(1, &av.to_le_bytes());
            b.count += 1;
        }
        Rc::new(b.into_consolidated(&schema))
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
