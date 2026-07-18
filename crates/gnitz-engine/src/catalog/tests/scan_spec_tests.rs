//! `scan_spec_family` — the worker-side parameterized bounded read. These drive
//! the bound walks (None / PkRange / PkSet) and the two-shape sink (top-k /
//! materialize / early-stop) with an **identity** spec (empty predicate and
//! projection, so `reply_schema == source schema`), isolating the cursor +
//! reduction mechanics from the expression VM (which the e2e suite covers).

use super::*;
use crate::storage::Batch;
use gnitz_wire::{Cut, OrderKey, RangeDescriptor, ReadBound, ReadSpec};

/// A `(id U64 PK | val I64)` base of `n` rows, `val = val_of(id)`, each at
/// weight 1. Returns `(engine, tid)`.
fn fixture(name: &str, n: u64, val_of: impl Fn(u64) -> i64) -> (CatalogEngine, i64) {
    weighted_fixture(name, (0..n).map(|i| (i, val_of(i), 1i64)))
}

/// A `(id U64 PK | val I64)` base built from explicit `(id, val, weight)` triples
/// (non-unique PK so weighted / duplicate rows are admitted verbatim).
fn weighted_fixture(name: &str, rows: impl Iterator<Item = (u64, i64, i64)>) -> (CatalogEngine, i64) {
    let dir = temp_dir(name);
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.t", &cols, &[0], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for (id, val, w) in rows {
        bb.begin_row(id as u128, w);
        bb.put_u64(val as u64);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    (engine, tid)
}

/// Extract `(id, val, weight)` triples from a `(id U64 PK | val I64)` reply batch.
fn triples(b: &Batch) -> Vec<(u128, i64, i64)> {
    (0..b.count)
        .map(|i| {
            let val = i64::from_le_bytes(b.get_col_ptr(i, 0, 8).try_into().unwrap());
            (b.get_pk(i), val, b.get_weight(i))
        })
        .collect()
}

/// An identity `ReadSpec` (no predicate, no projection) with the given bound,
/// order, and limit.
fn identity_spec(bound: ReadBound, order: Vec<OrderKey>, limit_k: u64) -> ReadSpec {
    ReadSpec {
        bound,
        predicate: vec![],
        projection: vec![],
        order,
        limit_k,
    }
}

fn run(engine: &mut CatalogEngine, tid: i64, spec: &ReadSpec) -> Vec<(u128, i64, i64)> {
    let reply_schema = engine.get_schema(tid).unwrap();
    let keeper = engine.scan_spec_family(tid, spec, &reply_schema).unwrap();
    triples(&keeper)
}

#[test]
fn full_scan_returns_all_rows_with_weights() {
    let (mut e, tid) = fixture("ss_full", 10, |i| (i * 10) as i64);
    let mut got = run(&mut e, tid, &identity_spec(ReadBound::None, vec![], 0));
    got.sort();
    let want: Vec<_> = (0..10u128).map(|i| (i, (i * 10) as i64, 1)).collect();
    assert_eq!(got, want);
}

#[test]
fn pk_range_ge_subsets() {
    let (mut e, tid) = fixture("ss_pkrange", 10, |i| i as i64);
    // pk >= 5
    let bound = ReadBound::PkRange(RangeDescriptor::new(&[], Cut::Before(5), Cut::After(u64::MAX as u128)));
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    let want: Vec<_> = (5..10u128).map(|i| (i, i as i64, 1)).collect();
    assert_eq!(got, want);
}

#[test]
fn pk_range_point_lookup() {
    let (mut e, tid) = fixture("ss_point", 10, |i| i as i64);
    // pk = 5  →  [Before(5), After(5)]
    let bound = ReadBound::PkRange(RangeDescriptor::new(&[], Cut::Before(5), Cut::After(5)));
    let got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    assert_eq!(got, vec![(5u128, 5i64, 1)]);
}

#[test]
fn pk_range_bounded_above_truncates_at_end() {
    let (mut e, tid) = fixture("ss_between", 20, |i| i as i64);
    // 5 <= pk < 8  →  [Before(5), Before(8))
    let bound = ReadBound::PkRange(RangeDescriptor::new(&[], Cut::Before(5), Cut::Before(8)));
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    assert_eq!(got, vec![(5u128, 5, 1), (6u128, 6, 1), (7u128, 7, 1)]);
}

#[test]
fn pk_set_gathers_named_keys() {
    let (mut e, tid) = fixture("ss_pkset", 10, |i| (i * 100) as i64);
    let bound = ReadBound::PkSet(vec![2, 5, 7]);
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    assert_eq!(got, vec![(2u128, 200, 1), (5u128, 500, 1), (7u128, 700, 1)]);
}

#[test]
fn pk_set_missing_keys_miss_silently() {
    let (mut e, tid) = fixture("ss_pkset_miss", 5, |i| i as i64);
    // 3 present (0,4), 99 absent.
    let bound = ReadBound::PkSet(vec![0, 99, 4]);
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    assert_eq!(got, vec![(0u128, 0, 1), (4u128, 4, 1)]);
}

#[test]
fn order_by_desc_limit_keeps_top_k() {
    let (mut e, tid) = fixture("ss_topk", 20, |i| i as i64); // val == id
                                                             // ORDER BY val DESC LIMIT 3  →  the 3 largest vals (ids 19, 18, 17).
    let order = vec![OrderKey {
        col: 1,
        desc: true,
        nulls_first: false,
    }];
    let mut got = run(&mut e, tid, &identity_spec(ReadBound::None, order, 3));
    got.sort();
    assert_eq!(got, vec![(17u128, 17, 1), (18u128, 18, 1), (19u128, 19, 1)]);
}

#[test]
fn order_by_asc_limit_keeps_smallest() {
    let (mut e, tid) = fixture("ss_topk_asc", 20, |i| i as i64);
    let order = vec![OrderKey {
        col: 1,
        desc: false,
        nulls_first: false,
    }];
    let mut got = run(&mut e, tid, &identity_spec(ReadBound::None, order, 3));
    got.sort();
    assert_eq!(got, vec![(0u128, 0, 1), (1u128, 1, 1), (2u128, 2, 1)]);
}

#[test]
fn top_k_keeps_boundary_row_whole() {
    // The top row by val carries weight 3; LIMIT 2 covers its weight and it must
    // be kept whole (weight 3), not clipped to 2 — the client window does the
    // exact split.
    let (mut e, tid) = weighted_fixture(
        "ss_topk_weight",
        [(1u64, 100i64, 3i64), (2, 50, 1), (3, 10, 1)].into_iter(),
    );
    let order = vec![OrderKey {
        col: 1,
        desc: true,
        nulls_first: false,
    }];
    let got = run(&mut e, tid, &identity_spec(ReadBound::None, order, 2));
    assert_eq!(got, vec![(1u128, 100, 3)], "boundary row kept whole at its true weight");
}

#[test]
fn no_order_limit_early_stops_on_summed_weight() {
    // No ORDER BY + LIMIT 5, all weight-1: the worker keeps a >=5-weight prefix in
    // cursor order — some legal subset, all weight 1, count 5.
    let (mut e, tid) = fixture("ss_earlystop", 100, |i| i as i64);
    let got = run(&mut e, tid, &identity_spec(ReadBound::None, vec![], 5));
    let total: i64 = got.iter().map(|(_, _, w)| w).sum();
    assert!(total >= 5, "early-stop must cover the window weight, got {total}");
    assert!(
        got.len() <= 6,
        "early-stop should not materialize the whole relation, got {}",
        got.len()
    );
}

#[test]
fn reply_pk_stride_mismatch_errs() {
    let (mut e, tid) = fixture("ss_stride", 4, |i| i as i64);
    // A reply schema with a narrower (U32) PK than the source's U64.
    let bad = crate::schema::SchemaDescriptor::new(
        &[
            crate::schema::SchemaColumn::new(type_code::U32, 0),
            crate::schema::SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let spec = identity_spec(ReadBound::None, vec![], 0);
    assert!(e.scan_spec_family(tid, &spec, &bad).is_err());
}
