use super::*;
use crate::query::compiler::fixtures::{
    compile, count_by, predicate, rejection, scan, sources, tables_at, Build, Source,
};
use crate::test_support::{make_schema_u64_i64, map_copy, reindexed_on_col1};
use gnitz_wire::{Circuit, JoinKind, TypeCode};
use gnitz_zset::algebra::Slot;

/// Every broken clause of the two-term form, and every other shape, is refused.
#[test]
fn a_shape_outside_the_equation_is_refused() {
    let registry = sources([1, 2, 100, 200].map(|id| (id, make_schema_u64_i64())));
    let refused = |c: &Circuit, why: &str| {
        assert_eq!(rejection(compile(c, &registry, true)), UNSUPPORTED, "{why}");
    };
    let cases: [(&str, Build); 4] = [
        ("a delta joins another integral than the other delta's", |c| {
            let [da, db] = [100, 200].map(|s| reindexed_on_col1(c, s, TypeCode::I64));
            let other = c.negate(db);
            c.join([da, db], [da, other], JoinKind::Equi);
        }),
        ("a non-equi kind", |c| {
            let deltas = [100, 200].map(|s| c.input_scattered(s, gnitz_wire::ReadBound::None, Some(Vec::new())));
            c.join(deltas, deltas, JoinKind::Cross);
        }),
        ("a union as the output", |c| {
            let (a, b) = (scan(c, 1), scan(c, 2));
            c.union(a, b);
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

/// A reduce as the output would read the view's own store, skeleton rows and
/// all, as its trace.
#[test]
fn a_reduce_as_the_output_is_refused() {
    let registry = sources([(1, make_schema_u64_i64())]);
    let mut c = Circuit::default();
    let a = scan(&mut c, 1);
    c.reduce_local(a, count_by(&[0]));
    assert_eq!(rejection(compile(&c, &registry, true)), READS_OWN_STORE);
}

/// A bounded view's replay seeds from its source table's store behind a linear
/// body, and from the integral of side A's delta behind a join: the child the
/// other term's join declared, or the table's own store where that is the
/// integral — a semi-join's inner term included, whose side-B delta is a
/// `distinct`. A stream has no rows to seed from.
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
    let semi: Build = |c| {
        let [da, db] = [10, 11].map(|s| reindexed_on_col1(c, s, TypeCode::I64));
        let db = c.distinct(db);
        c.join([da, db], [da, db], JoinKind::Equi);
    };
    let linear: Build = |c| {
        let a = scan(c, 10);
        let filtered = c.filter(a, predicate(1));
        map_copy(c, filtered, &[(1, TypeCode::I64)]);
    };
    let seed = |kind, build: Build| {
        let (registry, _dir) = tables_at(Slot::SOLO, [(10, kind, table), (11, BaseTable, table)]);
        let mut c = Circuit::default();
        build(&mut c);
        compile(&c, &registry, true).map(|out| out.hydration.expect("a bounded view").seed())
    };
    assert_eq!(seed(BaseTable, linear), Ok(Integral::Relation(10)), "a linear body");
    assert_eq!(seed(BaseTable, on_pk), Ok(Integral::Relation(10)), "a join on the PK");
    assert!(
        matches!(seed(BaseTable, on_payload), Ok(Integral::Own(_))),
        "a join on a payload column"
    );
    assert!(matches!(seed(BaseTable, semi), Ok(Integral::Own(_))), "a semi-join");
    assert!(
        rejection(seed(Stream, linear)).ends_with("over a stream has no stored rows to recompute from"),
        "a stream"
    );
}
