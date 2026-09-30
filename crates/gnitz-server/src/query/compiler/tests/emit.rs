use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::{make_schema_u64_i64, pk_only_schema, pk_payload_schema};
use gnitz_store::relation::StateLayout;
use gnitz_store::schema::SchemaColumn;
use gnitz_wire::TypeCode;

// ── Fixtures ────────────────────────────────────────────────────────────

/// Every registry below is `Slot::SOLO`, so every plan here computes its whole
/// result locally — what `build_plan`'s `self_contained` takes.
const SELF_CONTAINED: bool = true;

/// `ScanDelta(10) → mid → IntegrateSink`, planned up to `mid`: a guard test varies
/// one field of `mid`.
struct MidCircuit {
    in_schema: SchemaDescriptor,
}

impl MidCircuit {
    fn new(in_schema: SchemaDescriptor) -> Self {
        MidCircuit { in_schema }
    }

    fn build(&self, mid: gnitz_wire::OpNode) -> Result<(SubPlan, Vec<Option<OutReg>>), String> {
        let loaded = loaded_for_test(
            [(0, scan_delta(10)), (1, mid), (2, gnitz_wire::OpNode::IntegrateSink)],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        );
        build(
            &loaded,
            &subgraph_ordered(&loaded, 1),
            &sources([(10, self.in_schema)]),
            SELF_CONTAINED,
            &[],
            1,
        )
    }

    /// The guard that rejected `mid`.
    fn rejection(&self, mid: gnitz_wire::OpNode) -> String {
        rejection(self.build(mid))
    }
}

/// [`build_plan`] into a fresh layout: one sub-plan, compiled over `registry`.
fn build(
    loaded: &LoadedCircuit,
    ordered: &[NodeId],
    registry: &RelationRegistry,
    self_contained: bool,
    seeds: &[(NodeId, SchemaDescriptor)],
    out: NodeId,
) -> Result<(SubPlan, Vec<Option<OutReg>>), String> {
    let seeds: Vec<Seed> = seeds
        .iter()
        .map(|&(shard, schema)| Seed { shard, schema, partials: false })
        .collect();
    build_plan(
        loaded,
        ordered,
        registry,
        &mut StateLayout::default(),
        self_contained,
        &seeds,
        PlanOut::Node(out),
    )
    .map(|b| (b.plan, b.regs))
}

// ── The plan's output ───────────────────────────────────────────────────

/// A plan's output node must be in its node list.
#[test]
fn a_plan_refuses_an_output_node_outside_its_node_list() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::Negate),
            (2, gnitz_wire::OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    let plan = |ordered: &[NodeId]| {
        build(
            &loaded,
            ordered,
            &sources([(10, pk_only_schema(&[TypeCode::U64]))]),
            SELF_CONTAINED,
            &[],
            1,
        )
    };

    assert!(plan(&subgraph_ordered(&loaded, 1)).is_ok());
    assert_eq!(rejection(plan(&[0])), "operand is produced outside this plan");
}

/// The driver seeds one register per source, so a plan scanning one source
/// twice is refused rather than leaving the second scan unseeded.
#[test]
fn a_plan_scanning_one_source_twice_is_rejected() {
    let loaded = loaded_for_test(
        [(0, scan_delta(10)), (1, scan_delta(10)), (2, gnitz_wire::OpNode::Union)],
        vec![(0, 2, SLOT_IN), (1, 2, SLOT_B)],
    );
    let plan = build(
        &loaded,
        &loaded.ordered_where(|_| true),
        &sources([(10, make_schema_u64_i64())]),
        SELF_CONTAINED,
        &[],
        2,
    );
    assert_eq!(rejection(plan), "scan-delta: a plan scans one source twice");
}

/// An integral routed into a delta port names a store, not a batch: every port
/// that takes a delta refuses one.
#[test]
fn an_integral_reaching_a_delta_port_is_rejected() {
    let one = make_schema_u64_i64();
    // `ScanDelta(10) → IntegrateTrace → consumer`, planned as a subgraph ending
    // at the consumer. The consumer is the only thing that varies.
    let plan = |consumer: gnitz_wire::OpNode, edges: Vec<(NodeId, NodeId, usize)>| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, gnitz_wire::OpNode::IntegrateTrace),
                (2, consumer),
            ],
            edges,
        );
        build(
            &loaded,
            &loaded.ordered_where(|_| true),
            &sources([(10, one)]),
            SELF_CONTAINED,
            &[],
            2,
        )
        .map(drop)
    };
    const GUARD: &str = "operand port takes a delta, not an integral";
    assert_eq!(
        rejection(plan(gnitz_wire::OpNode::Negate, vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)])),
        GUARD,
        "a unary delta operator over an integral",
    );
    assert_eq!(
        rejection(plan(
            gnitz_wire::OpNode::Union,
            vec![(0, 1, SLOT_IN), (0, 2, SLOT_IN), (1, 2, SLOT_B)]
        )),
        GUARD,
        "a union's right operand is a delta port, not a trace one",
    );
}

/// The sink is the one operand no emit arm resolves, so `build_plan` is where a
/// plan whose output is an integral is refused: the epoch epilogue extracts a
/// *batch*, and an integral is a store.
#[test]
fn a_plan_whose_output_is_an_integral_is_rejected() {
    let loaded = loaded_for_test(
        [(0, scan_delta(10)), (1, gnitz_wire::OpNode::IntegrateTrace)],
        vec![(0, 1, SLOT_IN)],
    );
    let result = build(
        &loaded,
        &loaded.ordered_where(|_| true),
        &sources([(10, make_schema_u64_i64())]),
        SELF_CONTAINED,
        &[],
        1,
    );
    assert_eq!(rejection(result), "operand port takes a delta, not an integral");
}

/// A global aggregate's ground row is routed by the upstream shard's key and its
/// owner elected from the empty group key, so a shard keyed on anything else
/// would place the row on one worker and elect another. Every planner path emits
/// an empty `shard_cols` here; a hand-built circuit that does not is refused.
#[test]
fn a_global_aggregate_under_a_keyed_shard_is_rejected() {
    let schema = make_schema_u64_i64();
    let plan = |shard_cols: Vec<u32>| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, gnitz_wire::OpNode::ExchangeShard { shard_cols }),
                (
                    2,
                    gnitz_wire::OpNode::Reduce {
                        group_cols: vec![],
                        agg: vec![gnitz_wire::AggDescriptor {
                            agg_op: gnitz_wire::AggFunc::Count,
                            col_idx: 1,
                        }],
                        global_ground: true,
                    },
                ),
                (3, gnitz_wire::OpNode::IntegrateSink),
            ],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN)],
        );
        // The post phase: the shard is a seed register, exactly as `compile_view`
        // hands it over.
        build(
            &loaded,
            &loaded.ordered_where(|n| n >= 2),
            &sources([(10, schema)]),
            SELF_CONTAINED,
            &[(1, schema)],
            3,
        )
        .map(drop)
    };
    assert!(plan(vec![]).is_ok(), "the empty group key every planner path emits");
    assert_eq!(
        rejection(plan(vec![1])),
        "reduce: a global aggregate under a keyed exchange shard"
    );
}

/// Under an empty-keyed shard, only the worker V₀ routes to seeds the ground row.
#[test]
fn only_the_ground_owner_seeds_the_ground_row() {
    let schema = make_schema_u64_i64();
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![] }),
            (
                2,
                gnitz_wire::OpNode::Reduce {
                    group_cols: vec![],
                    agg: vec![gnitz_wire::AggDescriptor::COUNT_STAR],
                    global_ground: true,
                },
            ),
            (3, gnitz_wire::OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN)],
    );
    for rank in 0..4u32 {
        let mut registry = RelationRegistry::new("", gnitz_store::schema::Slot::new(rank, 4), Default::default());
        register_sources(&mut registry, [(10, schema)]);
        // The post phase: the shard is a seed register.
        let (plan, _) = build(
            &loaded,
            &loaded.ordered_where(|n| n >= 2),
            &registry,
            false,
            &[(1, schema)],
            3,
        )
        .unwrap();
        assert_eq!(
            plan.vm.pending_ground_row,
            rank as usize == gnitz_store::schema::ground_owner(4),
            "rank {rank}"
        );
    }
}

// ── Crafted raw-field guards: reject at compile, never abort at run ─────
//
// Each guard is proven by construction: the ONLY difference between the two
// builds is the crafted field, so a valid build's `Ok` and the crafted build's
// named rejection are both attributable solely to that field.

/// A present expression blob that fails to decode must abort the compile, not
/// silently degrade to `WHERE TRUE` / an identity map — whether it is garbled or
/// empty (a damaged catalog cell reads back empty, and `load_circuit` hands it
/// on as present).
#[test]
fn a_corrupt_expression_blob_aborts_the_compile() {
    use gnitz_wire::{MapKind, OpNode};
    let fixture = MidCircuit::new(pk_only_schema(&[TypeCode::U64]));
    for blob in [vec![0xFFu8; 16], Vec::new()] {
        assert!(
            fixture
                .rejection(OpNode::Filter(blob.clone()))
                .starts_with("filter: invalid predicate program"),
            "a {}-byte Filter blob must abort compilation",
            blob.len()
        );
        assert!(
            fixture
                .rejection(OpNode::Map(MapKind::Compute(gnitz_wire::ComputeMap {
                    program: blob.clone(),
                    out_cols: vec![],
                })))
                .starts_with("map: invalid program"),
            "a {}-byte Map blob must abort compilation",
            blob.len()
        );
    }
}

/// The range probe's preconditions are enforced at compile time, not by a
/// `debug_assert` a release build strips. The walk slices both sides' PK
/// regions at one equality width, so a crafted circuit whose two sides reindex
/// to different strides — or whose `n_eq` leaves no range slot — must be
/// rejected rather than reach the operator.
#[test]
fn range_join_probe_preconditions_are_rejected_at_compile_time() {
    use gnitz_wire::{JoinKind, RangeRel};
    let plan = |delta_schema, trace_schema, n_eq| {
        plan_two_source_join(JoinKind::Range { n_eq, rel: RangeRel::Lt }, delta_schema, trace_schema)
    };
    // The shape the reindex packer produces: `[eq slot, range slot]` PK, then payload.
    let band = |eq_tc: TypeCode| pk_payload_schema(&[eq_tc, TypeCode::U64]);
    let wide = band(TypeCode::U64); // pk_stride 16
    let narrow = band(TypeCode::U32); // pk_stride 12
    assert!(
        plan(wide, wide, 1).is_ok(),
        "a matched pair at the common promoted type must compile"
    );
    assert_eq!(rejection(plan(narrow, wide, 1)), TYPES, "delta side narrower");
    assert_eq!(rejection(plan(wide, narrow, 1)), TYPES, "delta side wider");
    assert_eq!(
        rejection(plan(wide, wide, 2)),
        "range join: n_eq does not match trace key arity"
    );

    // A PK-last column order, which the packer never emits but a crafted circuit
    // can name: read in PK-list order, its `n_eq = 1` prefix is 4 bytes and the
    // range slot keeps the other 4.
    let pk_last = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::U32, false),
        ],
        &[1, 2],
    );
    assert!(
        plan(pk_last, pk_last, 1).is_ok(),
        "a 4-byte eq prefix leaves a range slot"
    );
}

/// The equi probe takes the same key-layout check the range probe does, including
/// a same-stride pair whose OPK images differ by the sign flip.
#[test]
fn equi_join_pk_type_mismatches_are_rejected_at_compile_time() {
    let plan =
        |delta_schema, trace_schema| plan_two_source_join(gnitz_wire::JoinKind::Equi, delta_schema, trace_schema);
    let signed = pk_payload_schema(&[TypeCode::I64]);
    let unsigned = pk_payload_schema(&[TypeCode::U64]);
    let narrow = pk_payload_schema(&[TypeCode::U32]);
    assert!(plan(signed, signed).is_ok(), "a matched pair compiles");
    assert_eq!(rejection(plan(signed, unsigned)), TYPES, "same stride, opposite sign");
    assert_eq!(rejection(plan(unsigned, narrow)), TYPES, "different strides");
}

/// The one rejection both keyed probes share.
const TYPES: &str = "join: delta and trace PK column types differ (both sides must reindex at the pair's common type)";

/// The cross probe compares no key, so the two sides' PK regions need not agree
/// on a stride — the pair the range probe rejects above compiles as a cross
/// join.
#[test]
fn a_cross_join_accepts_sides_of_different_pk_strides() {
    let narrow = pk_payload_schema(&[TypeCode::U32]);
    let wide = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let plan = plan_two_source_join(gnitz_wire::JoinKind::Cross, narrow, wide);
    assert!(plan.is_ok(), "{:?}", plan.err());
}

/// Compile the minimal two-source join circuit — a delta on `SLOT_IN`, the
/// integral of a second scan on `SLOT_B` — at the given kind and side
/// schemas. The probe preconditions are all this shape exercises, so the join
/// kind and the two schemas are the only things a caller varies.
fn plan_two_source_join(
    kind: gnitz_wire::JoinKind,
    delta_schema: SchemaDescriptor,
    trace_schema: SchemaDescriptor,
) -> Result<(), String> {
    use gnitz_wire::OpNode;
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scan_delta(11)),
            (2, OpNode::IntegrateTrace),
            (3, OpNode::Join { kind, delta_is_right: false }),
            (4, OpNode::IntegrateSink),
        ],
        vec![(0, 3, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_B), (3, 4, SLOT_IN)],
    );
    build(
        &loaded,
        &subgraph_ordered(&loaded, 3),
        &sources([(10, delta_schema), (11, trace_schema)]),
        SELF_CONTAINED,
        &[],
        3,
    )
    .map(drop)
}

/// A trace port fed by a node that is not an integral is rejected at compile
/// time: the load checks a `Join`'s port arity, not its producer's kind,
/// and such a register would reach the dispatch with no cursor. The ONLY
/// difference between the two builds is which node feeds `SLOT_B`.
#[test]
fn a_join_whose_trace_port_is_not_an_integral_is_rejected() {
    let two_col = make_schema_u64_i64();
    // Node 2 is the integral of scan 11; node 1 is that scan's own register,
    // which carries a delta and no table.
    let plan = |trace_src: NodeId| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, gnitz_wire::OpNode::IntegrateTrace),
                (
                    3,
                    gnitz_wire::OpNode::Join {
                        kind: gnitz_wire::JoinKind::Equi,
                        delta_is_right: false,
                    },
                ),
            ],
            vec![(1, 2, SLOT_IN), (0, 3, SLOT_IN), (trace_src, 3, SLOT_B)],
        );
        build(
            &loaded,
            &loaded.ordered_where(|_| true),
            &sources([(10, two_col), (11, two_col)]),
            SELF_CONTAINED,
            &[],
            3,
        )
        .map(drop)
    };
    assert!(plan(2).is_ok(), "an integral is a valid trace port");
    assert_eq!(rejection(plan(1)), "operand port takes an integral, not a delta");
}

/// A wide (24-byte, 3 × U64) PK carries through the join's byte-keyed probe.
#[test]
fn a_wide_pk_join_compiles() {
    let schema = crate::test_support::pk_only_schema(&[TypeCode::U64; 3]);
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scan_delta(20)),
            (2, gnitz_wire::OpNode::IntegrateTrace),
            (
                3,
                gnitz_wire::OpNode::Join {
                    kind: gnitz_wire::JoinKind::Equi,
                    delta_is_right: false,
                },
            ),
            (4, gnitz_wire::OpNode::IntegrateSink),
        ],
        vec![(0, 3, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_B), (3, 4, SLOT_IN)],
    );
    assert!(build(
        &loaded,
        &subgraph_ordered(&loaded, 3),
        &sources([(10, schema), (20, schema)]),
        SELF_CONTAINED,
        &[],
        3
    )
    .is_ok());
}

// ── Replica sides ───────────────────────────────────────────────────────

/// A side emits a replica exactly when every worker computes the same rows — a
/// fact about the side's sources, not its arity.
#[test]
fn a_side_emits_a_replica_only_over_replicated_sources_it_does_not_trim() {
    use gnitz_store::schema::Placement;
    let replica_of = |schema: SchemaDescriptor, mid: gnitz_wire::OpNode| {
        let loaded = loaded_for_test(
            [(0, scan_delta(10)), (1, mid), (2, gnitz_wire::OpNode::IntegrateSink)],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        );
        let registry = sources([(10, schema)]);
        let (plan, _) =
            build(&loaded, &subgraph_ordered(&loaded, 1), &registry, false, &[], 1).expect("the fixture compiles");
        emits_replica(&plan, &registry)
    };
    let replicated = make_schema_u64_i64().with_placement(Placement::Replicated);
    assert!(replica_of(replicated, gnitz_wire::OpNode::Negate));
    assert!(
        !replica_of(make_schema_u64_i64(), gnitz_wire::OpNode::Negate),
        "a keyed source gives each worker its own slice"
    );
    assert!(
        !replica_of(replicated, gnitz_wire::OpNode::WorkerFilter),
        "a trimmed side emits a slice of the replica, not the replica"
    );
}
