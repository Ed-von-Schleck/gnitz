use super::fixtures::*;
use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{
    col_def, make_schema_u64_i64, pk_only_schema, scratch_dir, u64_pk_schema, write_circuit, write_identity_circuit,
};
use gnitz_expr::SchemaFacts;
use gnitz_store::schema::SchemaColumn;
use gnitz_wire::{OpNode, TypeCode};

// ── carve: the exchange shape, decided on the graph ─────────────────────

/// The guard `carve` rejected the circuit `nodes`/`edges` with.
fn carve_rejection(nodes: Vec<(NodeId, OpNode)>, edges: Vec<(NodeId, NodeId, usize)>) -> String {
    rejection(loaded_for_test(nodes, edges).carve().map(drop))
}

/// A side relays under its source, routed by the view's one shard key, so
/// two sides keyed differently would bounds-check one side by the other's key.
#[test]
fn exchange_sides_sharding_on_different_keys_are_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (3, OpNode::ExchangeShard { shard_cols: vec![1] }),
                (4, OpNode::Union),
                (5, OpNode::IntegrateSink),
            ],
            vec![
                (0, 2, SLOT_IN),
                (1, 3, SLOT_IN),
                (2, 4, SLOT_IN),
                (3, 4, SLOT_B),
                (4, 5, SLOT_IN)
            ],
        ),
        "exchange sides shard on different keys"
    );
}

/// A node in two sides would open one scratch child twice, under two
/// unsynchronized shard indexes.
#[test]
fn exchange_sides_sharing_an_ancestor_are_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (2, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (3, OpNode::Union),
                (4, OpNode::IntegrateSink),
            ],
            vec![
                (0, 1, SLOT_IN),
                (0, 2, SLOT_IN),
                (1, 3, SLOT_IN),
                (2, 3, SLOT_B),
                (3, 4, SLOT_IN)
            ],
        ),
        "exchange sides share a node"
    );
}

/// A shard upstream of another lies in both ancestor sets, so it never reaches
/// a plan's node list — where emitting it would abort a worker. No planner path
/// emits the shape; a circuit hand-built through `Circuit` can.
#[test]
fn a_chained_exchange_is_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (2, OpNode::Negate),
                (3, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (4, OpNode::IntegrateSink),
            ],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN), (3, 4, SLOT_IN)],
        ),
        "exchange sides share a node"
    );
}

/// A delta is routed to the side scanning its source, so a scan in the post
/// phase of an exchanged plan would receive nothing.
#[test]
fn a_post_phase_scan_in_an_exchanged_plan_is_rejected() {
    assert_eq!(
        carve_rejection(
            vec![
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, OpNode::ExchangeShard { shard_cols: vec![0] }),
                (3, OpNode::Union),
                (4, OpNode::IntegrateSink),
            ],
            vec![(0, 2, SLOT_IN), (2, 3, SLOT_IN), (1, 3, SLOT_B), (3, 4, SLOT_IN)],
        ),
        "an exchanged plan scans a relation outside every exchange side"
    );
}

/// Each side is its shard's ancestors without the shard; the post phase is every
/// other node, both shards excluded and the sink included.
#[test]
fn the_carve_splits_sides_from_the_post_phase() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scan_delta(11)),
            (2, OpNode::Negate),
            (3, OpNode::ExchangeShard { shard_cols: vec![0] }),
            (4, OpNode::ExchangeShard { shard_cols: vec![0] }),
            (5, OpNode::Union),
            (6, OpNode::WeightClamp(gnitz_wire::ClampKind::Distinct)),
            (7, OpNode::IntegrateSink),
        ],
        vec![
            (0, 2, SLOT_IN),
            (2, 3, SLOT_IN),
            (1, 4, SLOT_IN),
            (3, 5, SLOT_IN),
            (4, 5, SLOT_B),
            (5, 6, SLOT_IN),
            (6, 7, SLOT_IN),
        ],
    );
    let carve = loaded.carve().expect("a well-formed set-op");
    let mut sides: Vec<(NodeId, Vec<NodeId>)> = carve.sides.iter().map(|s| (s.shard, s.nodes.clone())).collect();
    sides.sort();
    assert_eq!(sides, vec![(3, vec![0, 2]), (4, vec![1])]);
    assert_eq!(carve.post, vec![5, 6, 7]);

    let unexchanged = loaded_for_test(
        [(0, scan_delta(10)), (1, OpNode::Negate), (2, OpNode::IntegrateSink)],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    let carve = unexchanged.carve().expect("an exchange-free circuit");
    assert!(carve.sides.is_empty());
    assert_eq!(carve.post, vec![0, 1, 2], "the post phase is the whole circuit");
}

// ── sink: exactly one ───────────────────────────────────────────────────

#[test]
fn a_circuit_holds_exactly_one_sink() {
    assert_eq!(
        rejection(loaded_for_test([(0, scan_delta(10))], vec![]).sink()),
        "circuit has no IntegrateSink"
    );
    assert_eq!(
        rejection(loaded_for_test([], vec![]).sink()),
        "circuit has no IntegrateSink",
        "an empty circuit"
    );
    let two = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, OpNode::IntegrateSink),
            (2, OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (0, 2, SLOT_IN)],
    );
    assert_eq!(rejection(two.sink()), "circuit has more than one IntegrateSink");
    let one = loaded_for_test([(0, scan_delta(10)), (1, OpNode::IntegrateSink)], vec![(0, 1, SLOT_IN)]);
    assert_eq!(one.sink(), Ok(1));
}

// ── compile_view: the sink contract ─────────────────────────────────────

/// The sink must match the view schema's physical layout, not just its width.
#[test]
fn a_sink_schema_unequal_to_the_view_schema_is_rejected() {
    let dir = scratch_dir("compiler", "sink_schema");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let u64_only = engine
        .create_table("public.u64_only", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let u64_i64 = engine
        .create_table(
            "public.u64_i64",
            &[col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)],
            &[0],
        )
        .unwrap();
    let u64_str = engine
        .create_table(
            "public.u64_str",
            &[col_def("id", TypeCode::U64), col_def("s", TypeCode::String)],
            &[0],
        )
        .unwrap();
    // One identity view per source, `ScanDelta(source) → IntegrateSink`.
    let view_over = |engine: &mut CatalogEngine, source: u64| {
        let vid = engine.allocate_ids(1).unwrap();
        write_identity_circuit(engine, vid, source, gnitz_wire::ReadBound::None);
        vid
    };
    let (v_u64_only, v_u64_i64, v_u64_str) = (
        view_over(&mut engine, u64_only),
        view_over(&mut engine, u64_i64),
        view_over(&mut engine, u64_str),
    );

    let view_schema = pk_only_schema(&[TypeCode::U64]);
    let string_payload = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let against = |view_schema: &SchemaDescriptor, vid: u64| {
        let loaded = load_circuit(&engine.registry, vid)?;
        compile_view(&loaded, &engine.registry, view_schema, false).map(drop)
    };
    assert!(against(&view_schema, v_u64_only).is_ok(), "an equal pair compiles");
    for vid in [v_u64_i64, v_u64_str] {
        assert_eq!(
            rejection(against(&view_schema, vid)),
            "sink schema does not match view output schema",
        );
    }
    // Equal column counts, different types: the same guard, on the sharper input.
    assert_eq!(
        rejection(against(&string_payload, v_u64_i64)),
        "sink schema does not match view output schema",
    );
    engine.close();
}

/// A shard column the relay's group key would refuse is a `CREATE VIEW`
/// rejection: workers route by it only mid-round, where a refusal is fatal.
#[test]
fn a_float_shard_column_is_rejected() {
    let dir = scratch_dir("compiler", "float_shard");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let source = engine
        .create_table(
            "public.u64_f64",
            &[col_def("id", TypeCode::U64), col_def("v", TypeCode::F64)],
            &[0],
        )
        .unwrap();
    let view_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    );
    let compile = |engine: &mut CatalogEngine, shard_col: u32| {
        let vid = engine.allocate_ids(1).unwrap();
        let mut circuit = gnitz_wire::Circuit::default();
        let scan = circuit.input_delta(source, gnitz_wire::ReadBound::None);
        let shard = circuit.shard(scan, &[shard_col]);
        circuit.sink(shard);
        write_circuit(engine, vid, circuit);
        let loaded = load_circuit(&engine.registry, vid)?;
        compile_view(&loaded, &engine.registry, &view_schema, false).map(drop)
    };
    assert!(compile(&mut engine, 0).is_ok(), "an integer shard column compiles");
    assert_eq!(
        rejection(compile(&mut engine, 1)),
        "group key: column 1 is a float, which has no order-preserving key image",
    );
    engine.close();
}

/// The node cap is what keeps every downstream `u16` id — registers, tables —
/// in range without any per-plan arithmetic, so it is enforced in the one
/// constructor every plan passes through rather than at each plan build.
#[test]
fn a_circuit_over_the_node_limit_is_rejected() {
    let chain = |n: usize| LoadedCircuit::new(crate::test_support::negate_chain(10, n));
    assert_eq!(
        rejection(chain(MAX_CIRCUIT_NODES + 1)),
        "circuit exceeds the node limit"
    );
    assert!(
        chain(MAX_CIRCUIT_NODES).is_ok(),
        "exactly MAX_CIRCUIT_NODES is accepted"
    );
}

// ── compile_view: the global split ──────────────────────────────────────

/// `ScanDelta(10) → [ExchangeShard []] → op → IntegrateSink`.
fn global_circuit(op: OpNode, exchanged: bool) -> LoadedCircuit {
    let mut nodes = vec![(0, scan_delta(10))];
    if exchanged {
        nodes.push((1, OpNode::ExchangeShard { shard_cols: vec![] }));
    }
    let n = nodes.len();
    nodes.push((n, op));
    nodes.push((n + 1, OpNode::IntegrateSink));
    let edges = (0..n + 1).map(|i| (i, i + 1, SLOT_IN)).collect();
    loaded_for_test(nodes, edges)
}

/// The schema `op`'s side relays, compiled over `source` as worker 0 of `of`, in a
/// view placed as `source` is.
fn side_output(op: OpNode, source: SchemaDescriptor, of: u32) -> SchemaDescriptor {
    let loaded = global_circuit(op.clone(), true);
    let mut registry = RelationRegistry::new("", gnitz_store::schema::Slot::new(0, of), Default::default());
    register_sources(&mut registry, [(10, source)]);
    let view_schema = match &op {
        OpNode::Reduce { agg, .. } => {
            gnitz_store::ops::ReducePlan::from_wire(&source, &[], agg, true)
                .unwrap()
                .shape
                .output_schema
        }
        OpNode::TopN { order, limit, offset, .. } => {
            gnitz_store::ops::TopNPlan::from_wire(&source, &[], order, *limit, *offset)
                .unwrap()
                .output_schema
        }
        _ => unreachable!("a global operator"),
    }
    .with_placement(source.placement());
    let (out, _) = compile_view(&loaded, &registry, &view_schema, false).expect("the fixture compiles");
    *out.sides[0].plan.vm.program.out_schema()
}

fn global_reduce(op: gnitz_wire::AggFunc) -> OpNode {
    OpNode::Reduce {
        group_cols: vec![],
        agg: vec![
            gnitz_wire::AggDescriptor { agg_op: op, col_idx: 1 },
            gnitz_wire::AggDescriptor::COUNT_STAR,
        ],
        global_ground: true,
    }
}

fn global_topn() -> OpNode {
    OpNode::TopN {
        group_cols: vec![],
        order: vec![gnitz_wire::OrderKey { col: 1, desc: false, nulls_first: false }],
        limit: 10,
        offset: 5,
    }
}

/// A partitioned global SUM or top-N relays its partials, which lead with the
/// synthetic group key.
#[test]
fn a_partitioned_global_reduce_or_topn_relays_partials() {
    let keyed = make_schema_u64_i64();
    for op in [global_reduce(gnitz_wire::AggFunc::Sum), global_topn()] {
        assert_eq!(side_output(op, keyed, 4).columns[0].type_code, TypeCode::U128);
    }
}

/// One worker, a replicated source, a float SUM and an extreme each relay the
/// input as is.
#[test]
fn a_global_reduce_splits_only_where_partials_combine_and_workers_differ() {
    use gnitz_store::schema::Placement;
    let keyed = make_schema_u64_i64();
    let unsplit = |op: OpNode, source: SchemaDescriptor, of: u32| side_output(op, source, of).same_layout(&source);
    assert!(unsplit(global_reduce(gnitz_wire::AggFunc::Sum), keyed, 1), "one worker");
    assert!(unsplit(global_topn(), keyed, 1), "one worker");
    let replicated = keyed.with_placement(Placement::Replicated);
    assert!(
        unsplit(global_reduce(gnitz_wire::AggFunc::Sum), replicated, 4),
        "a replicated source"
    );
    assert!(unsplit(global_topn(), replicated, 4), "a replicated source");
    let float = u64_pk_schema(SchemaColumn::new(TypeCode::F64, false));
    assert!(
        unsplit(global_reduce(gnitz_wire::AggFunc::Sum), float, 4),
        "a float SUM"
    );
    assert!(
        unsplit(global_reduce(gnitz_wire::AggFunc::Min), keyed, 4),
        "a global MIN"
    );
}

/// Only a worker holding the whole input may seed a ground row without an exchange.
#[test]
fn a_global_ground_reduce_with_no_exchange_is_refused_unless_self_contained() {
    use gnitz_store::schema::Placement;
    let compile = |of: u32, placement: Placement| {
        let source = make_schema_u64_i64().with_placement(placement);
        let op = global_reduce(gnitz_wire::AggFunc::Sum);
        let OpNode::Reduce { agg, .. } = &op else {
            unreachable!()
        };
        let view_schema = gnitz_store::ops::ReducePlan::from_wire(&source, &[], agg, true)
            .unwrap()
            .shape
            .output_schema
            .with_placement(placement);
        let mut registry = RelationRegistry::new("", gnitz_store::schema::Slot::new(0, of), Default::default());
        register_sources(&mut registry, [(10, source)]);
        compile_view(&global_circuit(op, false), &registry, &view_schema, false).map(drop)
    };
    assert_eq!(
        rejection(compile(4, make_schema_u64_i64().placement())),
        "reduce: a global aggregate over a partitioned input with no exchange"
    );
    assert!(compile(1, make_schema_u64_i64().placement()).is_ok(), "one worker");
    assert!(compile(4, Placement::Replicated).is_ok(), "a replicated view");
}
