use super::*;
use crate::query::compiler::fixtures::{compile, predicate, rejection, scan, tables_at, Build, Source, Stage};
use crate::test_support::{make_schema_u64_i64, reindexed_on_col1};
use gnitz_wire::{Circuit, JoinKind, TypeCode};
use gnitz_zset::algebra::Slot;

/// The two-term join of the reindexed `sources`, its side-B delta through `b` and
/// its output through `out`.
fn join_with(sources: [u64; 2], b: Stage, out: JoinKind) -> (Circuit, NodeId) {
    let mut c = Circuit::default();
    let [da, db] = sources.map(|s| reindexed_on_col1(&mut c, s, TypeCode::I64));
    let db = b(&mut c, db);
    c.join([da, db], [da, db], out);
    (c, da)
}

/// The linear shape seeds at its `ScanDelta`, through any number of
/// filter/map nodes; the two-term join from the integral of side A's delta — a
/// semi-join's inner term included, whose side-B delta is a `distinct`.
#[test]
fn a_linear_chain_seeds_at_its_scan_and_a_two_term_join_at_side_as_integral() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 77);
    let filtered = c.filter(source, predicate(1));
    c.map(filtered, &[0]);
    assert_eq!(seed_node(&c), Ok(SeedAt::Scan { node: source, source: 77 }));
    let cases: [(&str, Stage); 2] = [("inner", |_, n| n), ("semi", |c, n| c.distinct(n))];
    for (why, b) in cases {
        let (c, da) = join_with([100, 200], b, JoinKind::Equi);
        assert_eq!(seed_node(&c), Ok(SeedAt::Trace { delta: da }), "{why}");
    }
}

/// Every broken clause of the two-term form, and every other shape, is refused.
#[test]
fn a_shape_outside_the_equation_is_refused() {
    let refused = |c: &Circuit, why: &str| {
        assert_eq!(seed_node(c).err().as_deref(), Some(UNSUPPORTED), "{why}");
    };
    let mut c = Circuit::default();
    let [da, db] = [100, 200].map(|s| reindexed_on_col1(&mut c, s, TypeCode::I64));
    let slice = c.worker_filter(db);
    c.join([da, db], [da, slice], JoinKind::Equi);
    refused(&c, "a delta joins another integral than the other delta's");
    refused(&join_with([100, 200], |_, n| n, JoinKind::Cross).0, "a non-equi kind");

    let cases: [(&str, Build); 3] = [
        ("a union as the output", |c| {
            let (a, b) = (scan(c, 1), scan(c, 2));
            c.union(a, b);
        }),
        ("a reduce as the output", |c| {
            let a = scan(c, 1);
            c.reduce_multi_local(a, &[0], &[gnitz_wire::AggDescriptor::COUNT_STAR]);
        }),
        ("an exchange", |c| {
            let a = scan(c, 1);
            c.shard(a);
        }),
    ];
    for (why, build) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        refused(&c, why);
    }
}

/// A bounded view's replay seeds from its source table's store behind a linear
/// body, and from the integral of side A's delta behind a join: the child the
/// other term's join declared, or the table's own store where that is the
/// integral. A stream has no rows to seed from.
#[test]
fn a_bounded_view_seeds_from_the_store_holding_its_rows() {
    use gnitz_store::relation::RelationKind::{BaseTable, Stream};
    let table = Source::from(make_schema_u64_i64());
    let on_pk: Build = |c| {
        let [da, db] = [10, 11].map(|s| crate::test_support::scan_keyed(c, s, &[(0, TypeCode::U64)]));
        c.join([da, db], [da, db], JoinKind::Equi);
    };
    let on_payload: Build = |c| {
        let deltas = [10, 11].map(|s| reindexed_on_col1(c, s, TypeCode::I64));
        c.join(deltas, deltas, JoinKind::Equi);
    };
    let linear: Build = |c| {
        let a = scan(c, 10);
        c.filter(a, predicate(1));
    };
    let seed = |kind, build: Build| {
        let (registry, _dir) = tables_at(Slot::SOLO, [(10, kind, table), (11, BaseTable, table)]);
        let mut c = Circuit::default();
        build(&mut c);
        compile(&c, &registry, true).map(|out| out.hydration.expect("a bounded view").seed)
    };
    assert_eq!(seed(BaseTable, linear), Ok(Integral::Relation(10)), "a linear body");
    assert_eq!(seed(BaseTable, on_pk), Ok(Integral::Relation(10)), "a join on the PK");
    assert!(
        matches!(seed(BaseTable, on_payload), Ok(Integral::Own(_))),
        "a join on a payload column"
    );
    assert!(
        rejection(seed(Stream, linear)).ends_with("over a stream has no stored rows to recompute from"),
        "a stream"
    );
}
