use super::*;
use crate::query::compiler::fixtures::{dummy_expr_blob, loaded, scan};
use crate::test_support::reindexed_on_col1;
use gnitz_wire::{Circuit, JoinKind, TypeCode};

/// Sides reindexed from `sources`, with `terms` building the output out of
/// `[da, db]`.
fn join_with(sources: [u64; 2], terms: impl FnOnce(&mut Circuit, [NodeId; 2]) -> NodeId) -> LoadedCircuit {
    let mut c = Circuit::default();
    let deltas = sources.map(|s| reindexed_on_col1(&mut c, s, TypeCode::I64));
    terms(&mut c, deltas);
    loaded(c)
}

/// The linear shape seeds at its `ScanDelta`, through any number of
/// filter/map nodes.
#[test]
fn a_linear_chain_seeds_at_its_scan() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 77);
    let filtered = c.filter(source, dummy_expr_blob());
    c.map(filtered, &[0]);
    assert_eq!(seed_node(&loaded(c)), Ok(SeedAt::Scan { node: source, source: 77 }));
}

/// The two-term join seeds from the integral of side A's delta — a semi-join's
/// inner term included, whose side-B delta is a `distinct`.
#[test]
fn a_two_term_join_seeds_from_side_as_integral() {
    for semi in [false, true] {
        let mut c = Circuit::default();
        let da = reindexed_on_col1(&mut c, 100, TypeCode::I64);
        let mut db = reindexed_on_col1(&mut c, 200, TypeCode::I64);
        if semi {
            db = c.distinct(db);
        }
        c.join_terms([da, db], [da, db], JoinKind::Equi);
        assert_eq!(seed_node(&loaded(c)), Ok(SeedAt::Trace { delta: da }), "semi: {semi}");
    }
}

/// Every broken clause of the two-term form, and every other shape, is refused.
#[test]
fn a_shape_outside_the_equation_is_refused() {
    let refused = |lc: LoadedCircuit, why: &str| {
        assert_eq!(seed_node(&lc).err().as_deref(), Some(UNSUPPORTED), "{why}");
    };

    refused(
        join_with([100, 200], |c, [da, db]| {
            let ab = c.join(da, db, JoinKind::Equi, false);
            let ba = c.join(db, db, JoinKind::Equi, true);
            c.union(ab, ba)
        }),
        "both terms probe the same integral",
    );
    refused(
        join_with([100, 200], |c, [da, db]| {
            let ab = c.join(da, db, JoinKind::Equi, false);
            let ba = c.join(da, da, JoinKind::Equi, true);
            c.union(ab, ba)
        }),
        "the second term's delta is the first's",
    );
    refused(
        join_with([100, 200], |c, [da, db]| {
            let ab = c.join(da, db, JoinKind::Equi, false);
            let ba = c.join(db, da, JoinKind::Equi, false);
            c.union(ab, ba)
        }),
        "both terms write one side flag",
    );
    refused(
        join_with([100, 200], |c, [da, _]| {
            let dm = c.map(da, &[0]);
            c.join_terms([da, dm], [da, dm], JoinKind::Equi)
        }),
        "the second delta is computed from the first",
    );
    refused(
        join_with([100, 100], |c, deltas| c.join_terms(deltas, deltas, JoinKind::Equi)),
        "both deltas read one relation",
    );
    refused(
        join_with([100, 200], |c, deltas| c.join_terms(deltas, deltas, JoinKind::Cross)),
        "a non-equi kind",
    );

    let mut c = Circuit::default();
    let (a, b) = (scan(&mut c, 1), scan(&mut c, 2));
    c.union(a, b);
    refused(loaded(c), "a union input that is not a join");

    let mut c = Circuit::default();
    let a = scan(&mut c, 1);
    c.reduce_multi_local(a, &[0], &[gnitz_wire::AggDescriptor::COUNT_STAR]);
    refused(loaded(c), "a reduce as the output");

    let mut c = Circuit::default();
    let a = scan(&mut c, 1);
    c.shard(a);
    refused(loaded(c), "an exchange");
}
