use super::*;
use crate::test_support::{reindexed_on_col1, two_term_join_circuit};
use gnitz_wire::{Circuit, JoinKind, OpNode, ReadBound};

fn loaded(circuit: Circuit) -> LoadedCircuit {
    LoadedCircuit::new(circuit).expect("test circuit within the node limit")
}

/// Sides reindexed from `sources`, each integrated, with `terms` building the
/// sink's input out of `[da, db]` and `[ta, tb]`.
fn join_with(sources: [u64; 2], terms: impl FnOnce(&mut Circuit, [NodeId; 2], [NodeId; 2]) -> NodeId) -> LoadedCircuit {
    let mut c = Circuit::default();
    let deltas = sources.map(|s| reindexed_on_col1(&mut c, s));
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
    let scan = c.input_delta(77, ReadBound::None);
    let filtered = c.filter(scan, crate::query::compiler::fixtures::dummy_expr_blob());
    let mapped = c.map(filtered, &[0]);
    c.sink(mapped);
    assert_eq!(seed_node(&loaded(c)), Ok(SeedAt::Scan { node: scan, source: 77 }));
}

/// The two-term join seeds from the trace integrating side A's reindex.
#[test]
fn a_two_term_join_seeds_from_side_as_trace() {
    let lc = loaded(two_term_join_circuit(100, 200));
    let Ok(SeedAt::Trace { delta, trace }) = seed_node(&lc) else {
        panic!("a two-term join seeds at a trace");
    };
    assert!(matches!(lc.op(trace), OpNode::IntegrateTrace));
    assert_eq!(lc.inputs(trace).unary(), delta);
    assert!(matches!(
        lc.op(lc.inputs(delta).unary()),
        OpNode::ScanDelta { source: 100, .. }
    ));
}

/// A semi-join's inner term, whose side-B delta is a `distinct`.
#[test]
fn a_semi_join_term_is_accepted() {
    let mut c = Circuit::default();
    let da = reindexed_on_col1(&mut c, 100);
    let rb = reindexed_on_col1(&mut c, 200);
    let db = c.distinct(rb);
    let ta = c.integrate_trace(da);
    let tb = c.integrate_trace(db);
    let joined = c.join_terms([da, db], [ta, tb], JoinKind::Equi);
    c.sink(joined);
    assert_eq!(seed_node(&loaded(c)), Ok(SeedAt::Trace { delta: da, trace: ta }));
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
    let a = c.input_delta(1, ReadBound::None);
    let b = c.input_delta(2, ReadBound::None);
    let u = c.union(a, b);
    c.sink(u);
    refused(loaded(c), "a union input that is not a join");

    let mut c = Circuit::default();
    let scan = c.input_delta(1, ReadBound::None);
    let count = gnitz_wire::AggDescriptor {
        agg_op: gnitz_wire::AggFunc::Count,
        col_idx: 0,
    };
    let reduced = c.reduce_multi_local(scan, &[0], &[count], false);
    c.sink(reduced);
    refused(loaded(c), "a reduce under the sink");

    let mut c = Circuit::default();
    let scan = c.input_delta(1, ReadBound::None);
    let sharded = c.shard(scan, &[0]);
    c.sink(sharded);
    refused(loaded(c), "an exchange");
}
