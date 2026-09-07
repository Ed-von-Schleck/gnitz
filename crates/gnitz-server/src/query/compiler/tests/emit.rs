use super::*;
use crate::test_support::{make_batch, make_schema_u64_i64, pk_only_schema, pk_payload_schema, sum_weights};
use gnitz_store::schema::{type_code, SchemaColumn};
use std::collections::HashMap;

// ── Fixtures ────────────────────────────────────────────────────────────

/// The `ScanDelta(10) → mid → IntegrateSink` circuit: everything a guard test
/// needs to isolate one crafted field on `mid`. Entered on `build_plan`'s
/// `PlanTarget::Subgraph` path, which suppresses the sink-schema contract —
/// what a test isolating a *mid-node* guard wants, since the mid node's output
/// schema is exactly what it is varying.
struct MidCircuit {
    in_schema: SchemaDescriptor,
    /// Homes the mid node's scratch children, and outlives every plan `build`
    /// returns — a plan holds `Table`s under this directory.
    tmp: tempfile::TempDir,
}

impl MidCircuit {
    fn new(in_schema: SchemaDescriptor) -> Self {
        MidCircuit {
            in_schema,
            tmp: tempfile::tempdir().unwrap(),
        }
    }

    fn build(&self, mid: gnitz_wire::OpNode) -> Result<(SubPlan, FxHashMap<i32, OutReg>), CompileError> {
        let loaded = loaded_for_test(
            [(0, scan_delta(10)), (1, mid), (2, gnitz_wire::OpNode::IntegrateSink)],
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        );
        build_plan(
            &loaded,
            &loaded.subgraph_ordered(1),
            &ext_tables([(10i64, self.in_schema)]),
            test_site(self.tmp.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::Subgraph { out: 1 },
        )
    }

    /// The guard that rejected `mid`.
    fn rejection(&self, mid: gnitz_wire::OpNode) -> String {
        rejection(self.build(mid))
    }
}

/// The guard that rejected a build. Naming it is what makes a guard test
/// attributable: a bare `is_err()` also passes when an unrelated guard fires.
fn rejection<T>(r: Result<T, CompileError>) -> String {
    r.map(|_| "a plan").expect_err("expected a rejection").to_string()
}

/// A view site over a throwaway directory: nothing under it was ever
/// checkpointed, so the children it opens resume nothing.
fn test_site(dir: &str, id: u64) -> ViewSite<'_> {
    ViewSite {
        dir,
        id,
        recovery: RecoverySource::Rederive { resume_at: None },
        slot: Slot::SOLO,
        budgets: StoreBudgets::default(),
    }
}

// ── Plan targets and the sink contract ──────────────────────────────────

/// A `Subgraph`'s output register is the named node's, and a node list that
/// reached the sink is rejected — production carves an exchange side out of
/// the shard input's ancestors, so the sink is never in one.
#[test]
fn a_subgraph_outputs_the_named_node_and_must_not_hold_the_sink() {
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::Negate),
            (2, gnitz_wire::OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    let ext = ext_tables([(10i64, pk_only_schema(&[type_code::U64]))]);
    let plan = |ordered: &[i32]| {
        build_plan(
            &loaded,
            ordered,
            &ext,
            test_site("", 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::Subgraph { out: 1 },
        )
    };

    let (carved, out_reg_of) = plan(&loaded.subgraph_ordered(1)).expect("the carve production performs");
    assert_eq!(
        carved.vm.program.out_reg,
        out_reg_of[&1].delta().expect("node 1 is a delta"),
        "a subgraph outputs the register of the node it names"
    );
    assert_eq!(rejection(plan(&loaded.ordered)), "subgraph contains the sink");
}

/// A sink register whose schema is not the view's output schema is rejected
/// rather than emitted: the view store would then be written through a
/// descriptor its rows do not match. A column-count match is not enough —
/// equal counts with mismatched types would let the client read a 16-byte
/// string descriptor out of 8-byte integer storage.
#[test]
fn a_sink_schema_unequal_to_the_view_schema_is_rejected() {
    let view_schema = pk_only_schema(&[type_code::U64]);
    let string_payload = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let loaded = loaded_for_test(
        [(0, scan_delta(10)), (1, gnitz_wire::OpNode::IntegrateSink)],
        vec![(0, 1, SLOT_IN)],
    );
    let against = |view_schema: &SchemaDescriptor, source: SchemaDescriptor| {
        build_plan(
            &loaded,
            &loaded.ordered,
            &ext_tables([(10i64, source)]),
            test_site("", 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::ViewOutput { out_schema: view_schema, seeds: &[] },
        )
    };
    assert!(against(&view_schema, view_schema).is_ok(), "an equal pair compiles");
    for source in [make_schema_u64_i64(), string_payload] {
        assert_eq!(
            rejection(against(&view_schema, source)),
            "sink schema does not match view output schema",
        );
    }
    // Equal column counts, different types: the same guard, on the sharper input.
    assert_eq!(
        rejection(against(&string_payload, make_schema_u64_i64())),
        "sink schema does not match view output schema",
    );
}

/// A trace register routed into a delta port reads permanent emptiness — a
/// silently empty view. `topo_sorted` settles port *arity* and nothing else, so
/// this rejection is all that stands between a hand-built circuit and that.
#[test]
fn a_trace_register_reaching_a_delta_port_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let one = make_schema_u64_i64();
    // `ScanDelta(10) → IntegrateTrace → consumer`, planned as a subgraph ending
    // at the consumer. The consumer is the only thing that varies.
    let plan = |consumer: gnitz_wire::OpNode, edges: Vec<(i32, i32, usize)>| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, gnitz_wire::OpNode::IntegrateTrace),
                (2, consumer),
            ],
            edges,
        );
        build_plan(
            &loaded,
            &loaded.ordered,
            &ext_tables([(10i64, one)]),
            test_site(dir.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::Subgraph { out: 2 },
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
            vec![(0, 1, SLOT_IN), (0, 2, SLOT_IN), (1, 2, SLOT_TRACE)]
        )),
        GUARD,
        "a union's right operand is a delta port, not a trace one",
    );
}

/// The sink is the one register no emit arm resolves, so `build_plan` is where a
/// plan whose output is an integral is refused: the epoch epilogue extracts a
/// *batch*, and a trace register's is permanently empty.
#[test]
fn a_plan_whose_output_is_an_integral_is_rejected() {
    let loaded = loaded_for_test(
        [(0, scan_delta(10)), (1, gnitz_wire::OpNode::IntegrateTrace)],
        vec![(0, 1, SLOT_IN)],
    );
    let dir = tempfile::tempdir().unwrap();
    let result = build_plan(
        &loaded,
        &loaded.ordered,
        &ext_tables([(10i64, make_schema_u64_i64())]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        &mut ScratchGuard::new(),
        PlanTarget::Subgraph { out: 1 },
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
        let dir = tempfile::tempdir().unwrap();
        // `SyntheticFold`: the `_agg_pk` U128 key, then the COUNT column.
        let out_schema = pk_payload_schema(&[type_code::U128]);
        build_plan(
            &loaded,
            &loaded.ordered_where(|n| n >= 2),
            &ext_tables([(10i64, schema)]),
            test_site(dir.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::ViewOutput {
                out_schema: &out_schema,
                seeds: &[(1, schema)],
            },
        )
        .map(drop)
    };
    assert!(plan(vec![]).is_ok(), "the empty group key every planner path emits");
    assert_eq!(
        rejection(plan(vec![1])),
        "reduce: a global aggregate under a keyed exchange shard"
    );
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
    let fixture = MidCircuit::new(pk_only_schema(&[type_code::U64]));
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
    let band = |eq_tc: u8| pk_payload_schema(&[eq_tc, type_code::U64]);
    let wide = band(type_code::U64); // pk_stride 16
    let narrow = band(type_code::U32); // pk_stride 12
    assert!(
        plan(wide, wide, 1).is_ok(),
        "a matched pair at the common promoted type must compile"
    );
    const STRIDE: &str =
        "range join: delta and trace PK strides differ (both sides must reindex at the pair's common type)";
    assert_eq!(rejection(plan(narrow, wide, 1)), STRIDE, "delta side narrower");
    assert_eq!(rejection(plan(wide, narrow, 1)), STRIDE, "delta side wider");

    // `leading_key_size` sums *schema*-order columns, so a PK-last column order —
    // which the packer never emits but a crafted circuit can name — puts the whole
    // 8-byte key inside the `n_eq = 1` prefix while the key arity is still
    // `n_eq + 1`. That leaves no range slot.
    let pk_last = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
        ],
        &[1, 2],
    );
    assert_eq!(
        rejection(plan(pk_last, pk_last, 1)),
        "range join: eq prefix covers the whole key"
    );
}

/// The cross probe compares no key, so the two sides' PK regions need not agree
/// on a stride — the pair the range probe rejects above compiles as a cross
/// join.
#[test]
fn a_cross_join_accepts_sides_of_different_pk_strides() {
    let narrow = pk_payload_schema(&[type_code::U32]);
    let wide = pk_payload_schema(&[type_code::U64, type_code::U64]);
    let plan = plan_two_source_join(gnitz_wire::JoinKind::Cross, narrow, wide);
    assert!(plan.is_ok(), "{:?}", plan.err());
}

/// Compile the minimal two-source join circuit — a delta on `SLOT_IN`, the
/// integral of a second scan on `SLOT_TRACE` — at the given kind and side
/// schemas. The probe preconditions are all this shape exercises, so the join
/// kind and the two schemas are the only things a caller varies.
fn plan_two_source_join(
    kind: gnitz_wire::JoinKind,
    delta_schema: SchemaDescriptor,
    trace_schema: SchemaDescriptor,
) -> Result<(), CompileError> {
    use gnitz_wire::OpNode;
    let dir = tempfile::tempdir().unwrap();
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scan_delta(11)),
            (2, OpNode::IntegrateTrace),
            (3, OpNode::Join(kind)),
            (4, OpNode::IntegrateSink),
        ],
        vec![(0, 3, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_TRACE), (3, 4, SLOT_IN)],
    );
    build_plan(
        &loaded,
        &loaded.subgraph_ordered(3),
        &ext_tables([(10i64, delta_schema), (11, trace_schema)]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        &mut ScratchGuard::new(),
        PlanTarget::Subgraph { out: 3 },
    )
    .map(drop)
}

/// A trace port fed by a node that is not an integral is rejected at compile
/// time: `topo_sorted` checks a `Join`'s port arity, not its producer's kind,
/// and such a register would reach the dispatch with no cursor. The ONLY
/// difference between the two builds is which node feeds `SLOT_TRACE`.
#[test]
fn a_join_whose_trace_port_is_not_an_integral_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let two_col = make_schema_u64_i64();
    // Node 2 is the integral of scan 11; node 1 is that scan's own register,
    // which carries a delta and no table.
    let plan = |trace_src: i32| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, gnitz_wire::OpNode::IntegrateTrace),
                (3, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::Equi)),
            ],
            vec![(1, 2, SLOT_IN), (0, 3, SLOT_IN), (trace_src, 3, SLOT_TRACE)],
        );
        build_plan(
            &loaded,
            &loaded.ordered,
            &ext_tables([(10i64, two_col), (11, two_col)]),
            test_site(dir.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::Subgraph { out: 3 },
        )
        .map(drop)
    };
    assert!(plan(2).is_ok(), "an integral is a valid trace port");
    assert_eq!(rejection(plan(1)), "operand port takes an integral, not a delta");
}

/// A wide (24-byte, 3 × U64) PK carries through the join's byte-keyed probe.
#[test]
fn a_wide_pk_join_compiles() {
    let schema = crate::test_support::pk_only_schema(&[type_code::U64; 3]);
    let dir = tempfile::tempdir().unwrap();
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, scan_delta(20)),
            (2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::Equi)),
            (3, gnitz_wire::OpNode::IntegrateSink),
            (4, gnitz_wire::OpNode::IntegrateTrace),
        ],
        vec![(0, 2, SLOT_IN), (1, 4, SLOT_IN), (4, 2, SLOT_TRACE), (2, 3, SLOT_IN)],
    );
    // The plan owns scratch dirs under `dir`, so it must drop first: build it
    // inside the assert rather than binding it past `dir`'s scope.
    assert!(build_plan(
        &loaded,
        &loaded.subgraph_ordered(2),
        &ext_tables([(10i64, schema), (20, schema)]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        &mut ScratchGuard::new(),
        PlanTarget::Subgraph { out: 2 }
    )
    .is_ok());
}

/// A failing node after one that already created scratch: `ScratchGuard`'s drop
/// must remove the directory, so probing unsupported queries cannot leak inodes.
/// The failing node must come *after* a scratch-creating one, otherwise there is
/// nothing to remove and the assertion holds vacuously.
#[test]
fn a_failed_compile_removes_the_scratch_dirs_it_created() {
    let dir = tempfile::tempdir().unwrap();
    let view_dir = dir.path();
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            // `Distinct`, not `IntegrateTrace`: it creates a scratch child too,
            // and its output is a delta the failing Map can take as its input.
            (1, gnitz_wire::OpNode::Distinct),
            (2, gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Projection(vec![200]))),
            (3, gnitz_wire::OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN)],
    );
    let result = build_plan(
        &loaded,
        &loaded.subgraph_ordered(2),
        &ext_tables([(10i64, make_schema_u64_i64())]),
        test_site(view_dir.to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        &mut ScratchGuard::new(),
        PlanTarget::Subgraph { out: 2 },
    );
    assert_eq!(
        rejection(result),
        "projection map: column 200 is not a payload column of a 2-column schema"
    );

    let leftover: Vec<String> = std::fs::read_dir(view_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("scratch_"))
        .collect();
    assert!(
        leftover.is_empty(),
        "scratch dirs must be removed on compile failure, found: {leftover:?}",
    );
}

/// `compile_view` filters the exchange nids out of the *post* phase's node
/// list, but a side's list is `ancestors_inclusive` of its own exchange input
/// with no such filter — so a shard upstream of another shard's input lands
/// inside that side and reaches `emit_node`. It must reject, not panic: a panic
/// there is a worker abort, and a worker crash takes the cluster down. No
/// planner path emits the shape, but a circuit hand-built through
/// `gnitz_core::CircuitBuilder` bypasses the planner entirely.
#[test]
fn a_chained_exchange_is_rejected_instead_of_panicking() {
    let dir = tempfile::tempdir().unwrap();
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] }),
            (2, gnitz_wire::OpNode::Negate),
            (3, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] }),
            (4, gnitz_wire::OpNode::IntegrateSink),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN), (3, 4, SLOT_IN)],
    );

    // The carve `compile_view` performs for the sink-nearest shard.
    let ex_in = loaded.inputs(3).unary();
    let side_ordered = loaded.subgraph_ordered(ex_in);
    assert!(
        side_ordered.contains(&1),
        "fixture must place the upstream shard inside the side's node list, got {side_ordered:?}"
    );

    let result = build_plan(
        &loaded,
        &side_ordered,
        &ext_tables([(10i64, make_schema_u64_i64())]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        &mut ScratchGuard::new(),
        PlanTarget::Subgraph { out: ex_in },
    );
    assert_eq!(rejection(result), "chained exchange nodes");
}

// ── Weight-clamp presets ────────────────────────────────────────────────

/// The two clamp operators differ in nothing but their preset, and the preset is
/// what makes them different operators: `distinct` is the set-membership clamp,
/// `positive_part` the bag clamp that drops the negative part only. The bounds
/// each resolves to are `ops::ClampPreset`'s and tested there.
#[test]
fn the_two_clamp_operators_carry_their_own_presets() {
    use gnitz_store::ops::ClampPreset;
    let fixture = MidCircuit::new(make_schema_u64_i64());
    for (op, want) in [
        (gnitz_wire::OpNode::Distinct, ClampPreset::Distinct),
        (gnitz_wire::OpNode::PositivePart, ClampPreset::PositivePart),
    ] {
        let (plan, _) = fixture.build(op.clone()).expect("both clamps compile");
        let got = plan.vm.program.instructions.iter().find_map(|i| match i {
            Instr::WeightClamp { preset, .. } => Some(*preset),
            _ => None,
        });
        assert_eq!(got, Some(want), "{op:?}");
    }
}

// ── Destructive-register liveness ───────────────────────────────────────
//
// A register may be emptied in place iff it has no later reader and is not the
// sink the epoch extracts — a property of the emitted list, decided per input
// register rather than per view.

/// Every destructive opcode's take verdict in program order, labelled by its
/// slot; a `Union` contributes both operands. Read off the same `reads` table
/// the dispatch decides through, against the `last_read` the build produced —
/// so this asserts the verdict the VM will act on, not an intermediate.
fn consume_flags(plan: &SubPlan) -> Vec<(&'static str, bool)> {
    let program = &plan.vm.program;
    program
        .instructions
        .iter()
        .enumerate()
        .flat_map(|(pc, instr)| {
            let label = match instr {
                Instr::Union { .. } => ["union.a", "union.b"],
                Instr::WeightClamp { .. } => ["clamp", ""],
                Instr::Negate { .. } => ["negate", ""],
                _ => return Vec::new(),
            };
            crate::query::vm::reads(instr)
                .into_iter()
                .enumerate()
                .filter_map(|(slot, reg)| Some((label[slot], program.last_read[reg?.0 as usize] == pc as u32)))
                .collect()
        })
        .collect()
}

/// The INTERSECT/EXCEPT fan-out shape: ScanDelta(10)'s register fans into both
/// a destructive `Distinct` and a non-destructive `Negate` co-reader (standing
/// in for integrate_trace). Kahn's ascending tie-break schedules the lower id
/// first, so the ids decide which consumer runs first. (The reader is a
/// `Negate`, not a `Filter(None)`: a predicate-less Filter is elided by register
/// aliasing and would no longer read the register at runtime.)
#[test]
fn a_destructive_op_takes_its_input_only_when_it_is_the_last_reader() {
    let dir = tempfile::tempdir().unwrap();
    let ext = ext_tables([(10i64, make_schema_u64_i64())]);
    let flags = |distinct_id: i32, reader_id: i32| {
        let loaded = loaded_for_test(
            [
                (0, scan_delta(10)),
                (distinct_id, gnitz_wire::OpNode::Distinct),
                (reader_id, gnitz_wire::OpNode::Negate),
            ],
            vec![(0, distinct_id, SLOT_IN), (0, reader_id, SLOT_IN)],
        );
        let plan = build_plan(
            &loaded,
            &loaded.ordered,
            &ext,
            test_site(dir.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::Subgraph { out: distinct_id },
        )
        .expect("both orderings compile")
        .0;
        consume_flags(&plan)
    };
    assert_eq!(
        flags(2, 1),
        vec![("negate", false), ("clamp", true)],
        "the co-reader ran first and had to clone; the clamp's take is then free"
    );
    assert_eq!(
        flags(1, 2),
        vec![("clamp", false), ("negate", true)],
        "the clamp runs first while the co-reader still has to read, so it must clone"
    );
}

/// The set-operation shape: two sources meet at one `Union`, neither operand has
/// a later reader, so both sides are taken. And the epoch-end output extraction
/// reads the sink register without being an instruction — so a `Union` whose
/// operand *is* the sink must not take it, or it would emit nothing, silently,
/// every epoch.
#[test]
fn a_union_takes_each_unread_operand_but_never_the_sink_register() {
    let two_col = make_schema_u64_i64();
    let flags = |nodes: HashMap<i32, gnitz_wire::OpNode>, edges: Vec<(i32, i32, usize)>, out: i32| {
        let loaded = loaded_for_test(nodes, edges);
        let plan = build_plan(
            &loaded,
            &loaded.ordered,
            &ext_tables([(10i64, two_col), (11, two_col)]),
            test_site("", 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::Subgraph { out },
        )
        .expect("a union plan compiles")
        .0;
        consume_flags(&plan)
    };

    assert_eq!(
        flags(
            HashMap::from([(0, scan_delta(10)), (1, scan_delta(11)), (2, gnitz_wire::OpNode::Union),]),
            vec![(0, 2, SLOT_IN), (1, 2, SLOT_TRACE)],
            2,
        ),
        vec![("union.a", true), ("union.b", true)],
        "neither operand has a later reader, so both are taken"
    );

    assert_eq!(
        flags(
            HashMap::from([
                (0, scan_delta(10)),
                (1, gnitz_wire::OpNode::Negate),
                (2, gnitz_wire::OpNode::Union),
            ]),
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (0, 2, SLOT_TRACE)],
            1,
        ),
        vec![("negate", false), ("union.a", false), ("union.b", true)],
        "operand A is the sink and must not be taken; the Negate's own input is \
         still read by operand B, which has no later reader of its own",
    );
}

// ── Scratch cleanup ─────────────────────────────────────────────────────

/// A compile that fails after reopening a checkpointed operator trace must leave
/// it intact: the boot verdict cannot see the loss, so the view would resume with
/// that operator's history emptied.
#[test]
fn a_failed_compile_keeps_a_pre_existing_scratch_child() {
    use gnitz_store::storage::{flush_barrier, ChildAddr, FlushRound};
    const G: u64 = 7;
    const VIEW_ID: u64 = 1;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let schema = make_schema_u64_i64();
    // The name `emit_node`'s `Distinct` arm derives for node 1 of view 1.
    let child_dir = ChildAddr::Scratch { child: "_hist_1_1", rank: 0 }.dir(dir);
    // `Some(G)`, not `None`: under `None` the open unlinks the manifest and
    // erases the store anyway, so the test would pass without the fix.
    let recovery = RecoverySource::Rederive { resume_at: Some(G) };

    // A checkpointed operator trace: one row, published at generation G.
    let mut committed = Table::new(&child_dir, schema, 1, recovery, StoreBudgets::default()).unwrap();
    committed.ingest_owned_batch(make_batch(&schema, &[(1, 1, 5)])).unwrap();
    flush_barrier([&mut committed], FlushRound::Ephemeral(G)).unwrap();
    drop(committed);

    // Node 1 reopens that child; node 2 then fails the compile.
    let loaded = loaded_for_test(
        [
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::Distinct),
            (2, gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Projection(vec![200]))),
        ],
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    let site = ViewSite {
        dir,
        id: VIEW_ID,
        recovery,
        slot: Slot::SOLO,
        budgets: StoreBudgets::default(),
    };
    assert_eq!(
        rejection(build_plan(
            &loaded,
            &loaded.subgraph_ordered(2),
            &ext_tables([(10i64, schema)]),
            site,
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            &mut ScratchGuard::new(),
            PlanTarget::Subgraph { out: 2 },
        )),
        "projection map: column 200 is not a payload column of a 2-column schema"
    );

    let reopened = Table::new(&child_dir, schema, 1, recovery, StoreBudgets::default()).unwrap();
    assert_eq!(
        sum_weights(reopened.open_cursor()),
        1,
        "the committed trace row must survive a failed compile"
    );
}
