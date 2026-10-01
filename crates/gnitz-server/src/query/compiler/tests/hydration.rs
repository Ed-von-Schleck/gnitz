use super::*;
use crate::query::compiler::fixtures::{dummy_expr_blob, loaded, scan};
use crate::test_support::reindexed_on_col1;
use gnitz_wire::{Circuit, JoinKind, TypeCode};

/// Sides reindexed from `sources`, each integrated, with `terms` building the
/// sink's input out of `[da, db]` and `[ta, tb]`.
fn join_with(sources: [u64; 2], terms: impl FnOnce(&mut Circuit, [NodeId; 2], [NodeId; 2]) -> NodeId) -> LoadedCircuit {
    let mut c = Circuit::default();
    let deltas = sources.map(|s| reindexed_on_col1(&mut c, s, TypeCode::I64));
    let traces = deltas.map(|d| c.integrate_trace(d));
    let out = terms(&mut c, deltas, traces);
    c.sink(out);
    loaded(c)
}

/// The linear shape seeds at its `ScanDelta`, through any number of
/// filter/map nodes.
#[test]
fn a_linear_chain_seeds_at_its_scan() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 77);
    let filtered = c.filter(source, dummy_expr_blob());
    let mapped = c.map(filtered, &[0]);
    c.sink(mapped);
    assert_eq!(seed_node(&loaded(c)), Ok(SeedAt::Scan { node: source, source: 77 }));
}

/// The two-term join seeds from the trace integrating side A's delta — a
/// semi-join's inner term included, whose side-B delta is a `distinct`.
#[test]
fn a_two_term_join_seeds_from_side_as_trace() {
    for semi in [false, true] {
        let mut c = Circuit::default();
        let da = reindexed_on_col1(&mut c, 100, TypeCode::I64);
        let mut db = reindexed_on_col1(&mut c, 200, TypeCode::I64);
        if semi {
            db = c.distinct(db);
        }
        let [ta, tb] = [da, db].map(|d| c.integrate_trace(d));
        let joined = c.join_terms([da, db], [ta, tb], JoinKind::Equi);
        c.sink(joined);
        assert_eq!(
            seed_node(&loaded(c)),
            Ok(SeedAt::Trace { delta: da, trace: ta }),
            "semi: {semi}"
        );
    }
}

/// Every broken clause of the two-term form, and every other shape, is refused.
#[test]
fn a_shape_outside_the_equation_is_refused() {
    let refused = |lc: LoadedCircuit, why: &str| {
        assert_eq!(seed_node(&lc).err().as_deref(), Some(UNSUPPORTED), "{why}");
    };

    refused(
        join_with([100, 200], |c, [da, db], [_, tb]| {
            let ab = c.join(da, tb, JoinKind::Equi, false);
            let ba = c.join(db, tb, JoinKind::Equi, true);
            c.union(ab, ba)
        }),
        "both terms trace the same integral",
    );
    refused(
        join_with([100, 200], |c, [da, _], [ta, tb]| {
            let ab = c.join(da, tb, JoinKind::Equi, false);
            let ba = c.join(da, ta, JoinKind::Equi, true);
            c.union(ab, ba)
        }),
        "the second term's delta is the first's",
    );
    refused(
        join_with([100, 200], |c, [da, db], [ta, tb]| {
            let ab = c.join(da, tb, JoinKind::Equi, false);
            let ba = c.join(db, ta, JoinKind::Equi, false);
            c.union(ab, ba)
        }),
        "both terms write one side flag",
    );
    refused(
        join_with([100, 200], |c, [da, _], [ta, _]| {
            let dm = c.map(da, &[0]);
            let tm = c.integrate_trace(dm);
            c.join_terms([da, dm], [ta, tm], JoinKind::Equi)
        }),
        "the second delta is computed from the first",
    );
    refused(
        join_with([100, 100], |c, deltas, traces| {
            c.join_terms(deltas, traces, JoinKind::Equi)
        }),
        "both deltas read one relation",
    );
    refused(
        join_with([100, 200], |c, deltas, traces| {
            c.join_terms(deltas, traces, JoinKind::Cross)
        }),
        "a non-equi kind",
    );

    let mut c = Circuit::default();
    let (a, b) = (scan(&mut c, 1), scan(&mut c, 2));
    let u = c.union(a, b);
    c.sink(u);
    refused(loaded(c), "a union input that is not a join");

    let mut c = Circuit::default();
    let a = scan(&mut c, 1);
    let reduced = c.reduce_multi_local(a, &[0], &[gnitz_wire::AggDescriptor::COUNT_STAR], false);
    c.sink(reduced);
    refused(loaded(c), "a reduce under the sink");

    let mut c = Circuit::default();
    let a = scan(&mut c, 1);
    let sharded = c.shard(a, &[0]);
    c.sink(sharded);
    refused(loaded(c), "an exchange");
}
