use super::fixtures::*;
use super::*;
use crate::query::vm::Op;
use crate::test_support::{
    identity_circuit, make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_only_schema, pk_payload_schema,
    u64_pk_schema,
};
use gnitz_wire::{AggDescriptor, AggFunc, Circuit, GroupAggs, OrderKey, ReadBound, TypeCode};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::schema::SchemaColumn;

// ── The shapes a compile refuses ────────────────────────────────────────

/// The output must match the view schema's physical layout, not just its width.
/// Nullability is not part of it.
#[test]
fn an_output_schema_unequal_to_the_view_schema_is_rejected() {
    use TypeCode::{I64, U64};
    let compiles = |source: SchemaDescriptor, view: SchemaDescriptor| {
        let circuit = identity_circuit(10, ReadBound::None);
        compile_view(&circuit, &sources([(10, source)]), VIEW, &view, false).map(|_| ())
    };
    let pk_only = pk_only_schema(&[U64]);
    assert!(compiles(pk_only, pk_only).is_ok(), "an equal pair compiles");
    assert!(
        compiles(make_schema_u64_i64(), u64_pk_schema(&[SchemaColumn::new(I64, true)])).is_ok(),
        "a nullable view column over a non-null one"
    );
    for (source, view, why) in [
        (make_schema_u64_i64(), pk_only, "a wider output"),
        (
            make_schema_u64_i64(),
            make_schema_pk_u64_payload_string(),
            "equal widths, another column type",
        ),
        (
            pk_payload_schema(&[U64, U64]),
            u64_pk_schema(&[SchemaColumn::new(U64, false), SchemaColumn::new(I64, false)]),
            "equal region types, another PK arity",
        ),
    ] {
        assert_eq!(
            rejection(compiles(source, view)),
            "the circuit's output schema is not the view's",
            "{why}"
        );
    }
}

// ── Stage operators ────────────────────────────────────────────────────

const BY_COL1: [OrderKey; 1] = [OrderKey { col: 1, desc: false, nulls_first: false }];

/// `op` of column 1 over the whole input, with the COUNT(*) every reduce carries.
fn global(op: AggFunc) -> GroupAggs {
    GroupAggs {
        group_cols: vec![],
        aggs: vec![AggDescriptor { agg_op: op, col_idx: 1 }, AggDescriptor::COUNT_STAR],
    }
}

const SUM: Stage = |c, a| c.reduce(a, global(AggFunc::Sum));
const MIN: Stage = |c, a| c.reduce(a, global(AggFunc::Min));
const TOP_N: Stage = |c, a| c.top_n(a, &[], &BY_COL1, 10, 5);
const WINDOW: Stage = |c, a| {
    c.window(
        a,
        GroupAggs { group_cols: vec![], aggs: vec![] },
        &BY_COL1,
        (true, false),
    )
};
/// A reduce of each worker's slice.
const LOCAL_SUM: Stage = |c, a| c.reduce_local(a, global(AggFunc::Sum));

/// `build` over a scan of 10, compiled as worker `slot` into the view its
/// unsplit, one-worker plan outputs: a split plan must write that layout too.
fn compile_global(build: impl Fn(&mut Circuit, NodeId), source: Source, slot: Slot) -> Result<CompileOutput, String> {
    let mut c = Circuit::default();
    let a = scan(&mut c, 10);
    build(&mut c, a);
    let view = *compile(&c, &sources([(10, source)]), false)?.vm.out_schema();
    compile_view(&c, &sources_at(slot, [(10, source)]), VIEW, &view, false).map(|(out, ..)| out)
}

/// A side ends in its global operator's partial — a layout of its own — exactly
/// where partials combine and no worker holds the whole input. Otherwise it relays
/// its input as is.
#[test]
fn a_global_operator_splits_only_where_partials_combine_and_workers_differ() {
    let keyed = Source::from(make_schema_u64_i64());
    let replicated = keyed.placed(Placement::Replicated);
    for (why, op, source, of, split) in [
        ("a partitioned SUM", SUM, keyed, 4, true),
        ("a partitioned MIN", MIN, keyed, 4, true),
        ("a partitioned top-N", TOP_N, keyed, 4, true),
        ("one worker", SUM, keyed, 1, false),
        ("one worker", TOP_N, keyed, 1, false),
        ("a replicated source", SUM, replicated, 4, false),
        ("a replicated source", TOP_N, replicated, 4, false),
        // A row's rank reads every row before it: no worker's rows stand for the rest.
        ("a partitioned window", WINDOW, keyed, 4, false),
    ] {
        let out = compile_global(|c, a| _ = op(c, a), source, Slot::new(0, of)).expect(why);
        let stateful = out
            .vm
            .ops()
            .filter(|op| matches!(op, Op::Reduce { .. } | Op::Ordered { .. }));
        assert_eq!(stateful.count() == 2, split, "{why}");
        let on_view_store = out
            .vm
            .ops()
            .filter(|op| matches!(op, Op::Reduce { out_trace: Integral::Relation(_), .. }));
        assert!(on_view_store.count() <= 1, "{why}: a partial's trace is its own");
    }
}

/// A global aggregate's ground row is minted in an epoch of its own on one
/// worker, entered behind the reduce — so a round behind the aggregate would wait
/// on workers that never run it. A reduce that owes no ground row may sit behind
/// one.
#[test]
fn an_exchange_behind_a_global_aggregate_is_rejected() {
    let keyed = Source::from(make_schema_u64_i64());
    let above = |op: Stage| {
        compile_global(
            |c, a| {
                let r = op(c, a);
                c.shard(r);
            },
            keyed,
            Slot::new(0, 4),
        )
    };
    assert_eq!(rejection(above(SUM)), "an exchange behind a global aggregate");
    assert!(
        above(LOCAL_SUM).is_ok(),
        "a reduce of each worker's slice owes no ground row"
    );
}

/// A global reduce owes a ground row, and its plan says so at every rank: which
/// process mints it is the backfill's to say. A local one aggregates each
/// worker's slice, and owes none.
#[test]
fn the_ground_row_is_seeded_where_the_whole_input_arrives() {
    let keyed = Source::from(make_schema_u64_i64());
    let replicated = keyed.placed(Placement::Replicated);
    // Whether the circuit's own reduce, the plan's last, seeds the ground row.
    let seeds = |op: Stage, source: Source, slot: Slot| {
        let out = compile_global(|c, a| _ = op(c, a), source, slot).expect("the fixture compiles");
        let reduces = out.vm.ops().filter_map(|op| match op {
            Op::Reduce { plan, .. } => Some(plan.ground_row().is_some()),
            _ => None,
        });
        reduces.last().expect("a reduce")
    };
    for (source, slot) in [
        (keyed, Slot::SOLO),
        (keyed, Slot::new(3, 4)),
        (replicated, Slot::new(3, 4)),
    ] {
        assert!(seeds(MIN, source, slot), "an exchange, {slot:?}");
        assert!(!seeds(LOCAL_SUM, source, slot), "no exchange, {slot:?}");
    }
}

// ── Backfill bounds ─────────────────────────────────────────────────────

/// A backfill bound survives per source scanned once; a source scanned twice
/// shares one backfill cursor, so it keeps none even where one scan is bounded.
#[test]
fn a_source_scanned_once_keeps_its_backfill_bound() {
    use gnitz_wire::{KeyRange, PkColList};
    let schema = make_schema_u64_i64();
    let bound = |col: u32| ReadBound::Range(KeyRange::point(PkColList::from_slice(&[col]), &[0]));
    let bounds_of = |scans: [(u64, ReadBound); 2]| {
        let mut c = Circuit::default();
        let [a, b] = scans.clone().map(|(source, bound)| c.input_delta(source, bound));
        c.union(a, b);
        let out = compile(&c, &sources([(10, schema), (11, schema)]), false).expect("the fixture compiles");
        scans.map(|(source, _)| out.scan(source).bound.clone())
    };
    assert_eq!(
        bounds_of([(10, bound(1)), (11, ReadBound::None)]),
        [bound(1), ReadBound::None],
        "each source scanned once keeps its own"
    );
    assert_eq!(
        bounds_of([(10, bound(1)), (10, ReadBound::None)]),
        [ReadBound::None, ReadBound::None],
        "a source scanned twice has none"
    );
}
