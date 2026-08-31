use super::*;
use crate::schema::{type_code, SchemaColumn};

/// `union_nullability_merge` ORs the two inputs' per-column nullability, so a
/// null-carrying side reclassifies the output from the null-blind
/// `FixedIntNonnull` fast comparator to the null-aware `Generic` one.
#[test]
fn test_union_nullability_merge_classification() {
    use crate::schema::PayloadCmpKind;
    let pk = SchemaColumn::new(type_code::U128, 0);
    let nonnull = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 0)], &[0]);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 1)], &[0]);

    // Non-nullable A + nullable B → nullable output column, Generic comparator.
    let m = union_nullability_merge(&nonnull, &nullable).expect("shared layout");
    assert_eq!(m.columns[1].nullable, 1, "OR of non-nullable and nullable = nullable");
    assert_eq!(m.payload_cmp, PayloadCmpKind::Generic);

    // Both non-nullable → stays on the FixedIntNonnull fast path (byte-identical).
    let m2 = union_nullability_merge(&nonnull, &nonnull).expect("shared layout");
    assert_eq!(m2.columns[1].nullable, 0);
    assert_eq!(m2.payload_cmp, PayloadCmpKind::FixedIntNonnull);

    // Nullable A + non-nullable B → Generic too (OR is symmetric).
    let m3 = union_nullability_merge(&nullable, &nonnull).expect("shared layout");
    assert_eq!(m3.columns[1].nullable, 1);
    assert_eq!(m3.payload_cmp, PayloadCmpKind::Generic);
}

/// `union_nullability_merge` is the Union arm's whole layout contract, and in
/// release there is nothing else: a mismatched pair would adopt `a`'s schema
/// and let `op_union` read `b`'s bytes through it.
#[test]
fn test_union_mismatched_input_layout_rejected() {
    let one = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
    let two = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, scan_delta(11));
    nodes.insert(2, gnitz_wire::OpNode::Union);
    let loaded = loaded_for_test(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B)]);

    // `Subgraph` so the union guard is the only one that can fire — the sink
    // contract never runs.
    let plan = |b: SchemaDescriptor| {
        build_plan(
            &loaded,
            &subgraph_ordered(&loaded, 2),
            &HashMap::from([(10, one), (11, b)]),
            test_site("", 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 2 },
        )
    };
    assert!(plan(one).is_ok(), "a matched pair must compile");
    match plan(two) {
        Err(CompileError::Rejected(guard)) => {
            assert_eq!(guard, "union: inputs do not share a physical layout")
        }
        other => panic!("expected a rejection, got {:?}", other.map(|_| "a plan")),
    }
}

/// A `Subgraph`'s output register is the named node's, and a node list that
/// reached the sink is rejected — production carves an exchange side out of
/// the shard input's ancestors, so the sink is never in one.
#[test]
fn test_subgraph_output_is_the_named_node() {
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, gnitz_wire::OpNode::Negate);
    nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
    let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
    let ext: ExtTables = HashMap::from([(10, schema)]);
    let plan = |ordered: &[i32]| {
        build_plan(
            &loaded,
            ordered,
            &ext,
            test_site("", 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 1 },
        )
    };

    let carved = plan(&subgraph_ordered(&loaded, 1)).expect("the carve production performs");
    assert_eq!(
        carved.vm.program.out_reg,
        *carved.out_reg_of.get(&1).unwrap(),
        "a subgraph outputs the register of the node it names"
    );
    assert!(
        matches!(
            plan(&loaded.ordered),
            Err(CompileError::Rejected("subgraph contains the sink"))
        ),
        "a node list reaching the sink is a mis-carve, not a plan"
    );
}

/// A sink register whose schema is not the view's output schema is rejected
/// rather than emitted: the view store would then be written through a
/// descriptor its rows do not match.
#[test]
fn test_mismatched_sink_schema_rejected() {
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, gnitz_wire::OpNode::IntegrateSink);
    let view_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
    // The scan source carries an extra payload column, so the sink register
    // reaches `build_plan` with a schema the view's does not equal.
    let source_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);

    let result = build_plan(
        &loaded,
        &loaded.ordered,
        &HashMap::from([(10, source_schema)]),
        test_site("", 1),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::ViewOutput {
            out_schema: &view_schema,
            seeds: &[],
        },
    );
    match result {
        Err(CompileError::Rejected(guard)) => {
            assert_eq!(guard, "sink schema does not match view output schema")
        }
        other => panic!("expected a rejection, got {:?}", other.map(|_| "a plan")),
    }
}

#[test]
fn test_build_plan_register_overflow_rejected() {
    // A circuit producing > u16::MAX registers must fail the compile rather
    // than wrap the u16 cast and panic in ProgramBuilder::build.
    let n = u16::MAX as i32 + 1; // 65536 nodes → 65536 registers
    let mut nodes = HashMap::from([(0, scan_delta(10))]);
    let mut edges = Vec::new();
    for nid in 1..n {
        nodes.insert(nid, gnitz_wire::OpNode::Negate);
        edges.push((nid - 1, nid, PORT_IN));
    }
    let loaded = loaded_for_test(nodes, edges);
    assert_eq!(loaded.ordered.len(), n as usize);
    let result = build_plan(
        &loaded,
        &loaded.ordered,
        &HashMap::new(),
        test_site("", 1),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::ViewOutput {
            out_schema: &SchemaDescriptor::minimal_u64(),
            seeds: &[],
        },
    );
    assert!(
        result.is_err(),
        "build_plan must fail when register count exceeds u16::MAX"
    );
}

/// The planner ships the reduce output-key kind; the engine validates it
/// against the input schema and hard-rejects (build_plan → None) any kind the
/// schema does not warrant — the guard that turns a silent output-column
/// scramble into a compile failure. Covers all three schema shapes × all three
/// kinds: the three matching kinds compile, the six cross pairings reject.
#[test]
fn reduce_out_key_validation_rejects_mismatch() {
    use crate::schema::ReduceOutKey;
    use gnitz_wire::{AggFunc, OpNode};
    let compiles = |in_schema: SchemaDescriptor, group: Vec<u32>, out_key: ReduceOutKey| -> bool {
        compiles_mid_node(
            in_schema,
            OpNode::Reduce {
                group_cols: group,
                // A linear COUNT keeps the MIN/MAX-eligibility guard out of the
                // picture, isolating the out_key validation.
                agg: vec![(AggFunc::Count, 0)],
                global_ground: false,
                out_key,
            },
        )
    };

    // (schema, group cols, the ONE kind the schema warrants, tag).
    let eq_pk = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let single_nat = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0), // natural group col
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let synthetic = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0), // non-natural group col
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let cases = [
        (eq_pk, vec![0u32], ReduceOutKey::PkPermutation, "eqpk"),
        (single_nat, vec![1u32], ReduceOutKey::SingleNaturalCol, "single"),
        (synthetic, vec![1u32], ReduceOutKey::SyntheticFold, "synth"),
    ];
    let all_kinds = [
        ReduceOutKey::SyntheticFold,
        ReduceOutKey::PkPermutation,
        ReduceOutKey::SingleNaturalCol,
    ];
    for (schema, group, correct, tag) in cases {
        for kind in all_kinds {
            let ok = compiles(schema, group.clone(), kind);
            assert_eq!(
                ok,
                kind == correct,
                "schema {tag}: out_key {kind:?} should {} (schema warrants {correct:?})",
                if kind == correct { "compile" } else { "reject" },
            );
        }
    }
}

#[test]
fn test_build_plan_wide_pk_join_accepted() {
    // After byte-API port: wide-PK Join(DeltaTrace) must compile successfully.
    // ScanDelta(wide) --port0--> Join(DT) <--port1-- IntegrateTrace(wide)
    // Join(DT) --> IntegrateSink.
    // 3 × U64 = a 24-byte PK, wide.
    let schema = crate::test_support::pk_only_schema(&[type_code::U64; 3]);
    let dir = tempfile::tempdir().unwrap();
    let view_dir = dir.path().to_str().unwrap();

    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, scan_delta(20));
    nodes.insert(2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
    nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
    nodes.insert(4, gnitz_wire::OpNode::IntegrateTrace);
    let edges = vec![(0, 2, PORT_IN_A), (1, 4, PORT_IN), (4, 2, PORT_TRACE), (2, 3, PORT_IN)];
    let loaded = loaded_for_test(nodes, edges);
    let ext: ExtTables = HashMap::from([(10, schema), (20, schema)]);
    // The plan owns scratch dirs under `dir`, so it must drop first: build it
    // inside the assert rather than binding it past `dir`'s scope.
    assert!(
        build_plan(
            &loaded,
            &subgraph_ordered(&loaded, 2),
            &ext,
            test_site(view_dir, 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 2 }
        )
        .is_ok(),
        "wide-PK Join(DeltaTrace) must compile after byte-API port"
    );
}

// ── Item 32: sink schema type validation ────────────────────────────────

#[test]
fn test_build_plan_sink_schema_type_mismatch_rejected() {
    // ScanDelta(99) → IntegrateSink. The source schema is [U64 pk, I64];
    // the view's declared out_schema is [U64 pk, STRING]. Same column count,
    // different physical layout → must be rejected, else the client
    // reads a 16-byte string descriptor out of 8-byte integer storage.
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(99));
    nodes.insert(1, gnitz_wire::OpNode::IntegrateSink);
    let edges = vec![(0, 1, PORT_IN)];
    let loaded = loaded_for_test(nodes, edges);
    let view_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let ext: ExtTables = HashMap::from([(99, in_schema)]);
    let result = build_plan(
        &loaded,
        &loaded.ordered,
        &ext,
        test_site("", 99),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::ViewOutput {
            out_schema: &view_schema,
            seeds: &[],
        },
    );
    assert!(result.is_err(), "type-mismatched sink schema must be rejected");
}

// ── Item 35: corrupt Filter/Map blob aborts compilation ─────────────────

#[test]
fn test_build_plan_corrupt_filter_blob_aborts() {
    // ScanDelta(99) → Filter(blob) → IntegrateSink. A present blob that
    // fails to decode must abort, not silently degrade to WHERE TRUE —
    // whether it is garbled or empty (a damaged catalog cell reads back
    // empty, and `load_circuit` hands it on as present).
    let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
    // Hold the sink to the input schema so the sink-schema check passes; the
    // only thing that can fail this compile is the blob.
    let fixture = MidCircuit::new(in_schema).with_out_schema(in_schema);
    for blob in [vec![0xFFu8; 16], Vec::new()] {
        let what = if blob.is_empty() { "empty" } else { "garbled" };
        assert!(
            !fixture.compiles(gnitz_wire::OpNode::Filter(Some(blob))),
            "a {what} Filter blob must abort compilation"
        );
    }
}

#[test]
fn test_build_plan_corrupt_map_blob_aborts() {
    // ScanDelta(99) → Map(Expression{corrupt blob}) → IntegrateSink.
    let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
    let corrupt = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Compute {
        program: vec![0xFFu8; 16],
        out_cols: vec![],
    });
    assert!(
        !MidCircuit::new(in_schema).compiles(corrupt),
        "corrupt Map blob must abort compilation"
    );
}

/// A compound (len > 1) reindex Map now compiles end-to-end: the gate is
/// lifted and `emit_node` builds a 2-slot-PK node schema, so `build_plan`
/// returns `Some` (the sink's output schema matches the reindex output).
#[test]
fn test_build_plan_compound_reindex_accepted() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // The sink validates against the reindex Map's output schema (2 synthetic
    // PK slots [U64, I64] + the two input columns).
    let out_schema = crate::schema::key::ReindexPacker::new(&in_schema, &[0, 1], &[])
        .unwrap()
        .output_schema(&in_schema, &[0, 1])
        .unwrap();
    let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
        keep: vec![0, 1],
        reindex_cols: vec![0, 1],
        reindex_target_tcs: vec![],
        role: gnitz_wire::ReindexRole::ScatterKey,
    });
    assert!(
        MidCircuit::new(in_schema).with_out_schema(out_schema).compiles(map),
        "compound (len > 1) reindex must compile after the gate lift"
    );
}

/// A reindex list longer than `MAX_PK_COLUMNS` overflows the output schema's
/// fixed PK array; `emit_node` must fail the compile cleanly (`build_plan`
/// returns None) rather than panic or build a truncated key.
#[test]
fn test_build_plan_reindex_exceeds_max_pk_columns_rejected() {
    // 6-column source, reindex on all 6 → pk_n (6) > MAX_PK_COLUMNS (5).
    let n_cols = crate::schema::MAX_PK_COLUMNS + 1;
    let cols: Vec<SchemaColumn> = (0..n_cols).map(|_| SchemaColumn::new(type_code::U64, 0)).collect();
    let in_schema = SchemaDescriptor::new(&cols, &[0]);
    let reindex_cols: Vec<u32> = (0..n_cols as u32).collect();

    let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
        keep: vec![0],
        reindex_cols,
        reindex_target_tcs: vec![],
        role: gnitz_wire::ReindexRole::ScatterKey,
    });
    assert!(
        !MidCircuit::new(in_schema).compiles(map),
        "reindex list > MAX_PK_COLUMNS must fail the compile"
    );
}

/// The reindex output payload schema is derived from its kept-column list:
/// a node keeping only a subset of the input columns compiles to the pruned
/// `[key slots ‖ kept columns]` layout end-to-end (`build_plan` returns
/// `Some` against a sink schema built from the same kept list).
#[test]
fn test_build_plan_pruned_reindex_compiles() {
    // 3-column source; reindex on col0, keeping only col 2 as payload.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = crate::schema::key::ReindexPacker::new(&in_schema, &[0], &[])
        .unwrap()
        .output_schema(&in_schema, &[2])
        .unwrap();
    let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
        keep: vec![2],
        reindex_cols: vec![0],
        reindex_target_tcs: vec![],
        role: gnitz_wire::ReindexRole::ScatterKey,
    });
    assert!(
        MidCircuit::new(in_schema).with_out_schema(out_schema).compiles(map),
        "pruned reindex must compile to the derived schema"
    );
}

/// A reindex keeping an out-of-range source column is a corrupt/forged
/// catalog; `emit_node` must fail the compile cleanly (`build_plan` returns
/// None) rather than read a zeroed schema slot.
#[test]
fn test_build_plan_reindex_keep_oob_col_rejected() {
    // reindex on col0, keeping col 9 on a 2-column source.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
        keep: vec![9],
        reindex_cols: vec![0],
        reindex_target_tcs: vec![],
        role: gnitz_wire::ReindexRole::ScatterKey,
    });
    assert!(
        !MidCircuit::new(in_schema).compiles(map),
        "an out-of-range kept column must fail the compile"
    );
}

// ── Item 29: scratch dir cleanup on compile failure ─────────────────────

#[test]
fn test_build_plan_cleans_scratch_dirs_on_failure() {
    // ScanDelta → IntegrateTrace → Map → IntegrateSink, with the Map
    // projecting an out-of-bounds column so it fails the compile. The
    // IntegrateTrace before it has already created its scratch dir under
    // `view_dir`; `ScratchGuard`'s drop must remove it, so probing
    // unsupported queries can't leak inodes.
    //
    // The failing node must come *after* a node that creates scratch,
    // otherwise there is nothing for the cleanup to remove and the
    // assertion below holds vacuously.
    let dir = tempfile::tempdir().unwrap();
    let view_dir = dir.path();

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, gnitz_wire::OpNode::IntegrateTrace);
    nodes.insert(2, gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Projection(vec![200])));
    nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
    let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)];
    let loaded = loaded_for_test(nodes, edges);
    let ext: ExtTables = HashMap::from([(10, schema)]);
    let result = build_plan(
        &loaded,
        &subgraph_ordered(&loaded, 2),
        &ext,
        test_site(view_dir.to_str().unwrap(), 1),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: 2 },
    );
    assert!(result.is_err(), "out-of-bounds projection must fail the compile");

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
/// list, but a side's list is `ancestors_inclusive` of its own exchange
/// input with no such filter — so a shard upstream of another shard's input
/// lands inside that side and reaches `emit_node`. It must reject, not
/// panic: a panic there is a worker abort, and a worker crash takes the
/// cluster down. No planner path emits the shape, and the planner asserts
/// against it, but a circuit hand-built through `gnitz_core::CircuitBuilder`
/// bypasses the planner entirely.
#[test]
fn chained_exchange_rejects_instead_of_panicking() {
    let dir = tempfile::tempdir().unwrap();
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] });
    nodes.insert(2, gnitz_wire::OpNode::Filter(None));
    nodes.insert(3, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] });
    nodes.insert(4, gnitz_wire::OpNode::IntegrateSink);
    let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN), (3, 4, PORT_IN)];
    let loaded = loaded_for_test(nodes, edges);
    let ext: ExtTables = HashMap::from([(10, schema)]);

    // The carve `compile_view` performs for the sink-nearest shard.
    let ex_in = loaded.inputs(3).unary();
    let set = ancestors_inclusive(&loaded, ex_in);
    let side_ordered: Vec<i32> = loaded.ordered.iter().copied().filter(|n| set.contains(n)).collect();
    assert!(
        side_ordered.contains(&1),
        "fixture must place the upstream shard inside the side's node list, got {side_ordered:?}"
    );

    let result = build_plan(
        &loaded,
        &side_ordered,
        &ext,
        test_site(dir.path().to_str().unwrap(), 1),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: ex_in },
    );
    assert!(
        matches!(result, Err(CompileError::Rejected("chained exchange nodes"))),
        "chained exchange must be a named rejection"
    );
}

// ── helpers shared by join tests ─────────────────────────────────────

fn two_col_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    )
}

// ── Part B: crafted raw-field guards reject at compile, never abort at run ──
//
// Each guard is proven by construction: the ONLY difference between the two
// builds is the crafted field, so a valid build's `Some` and the crafted
// build's `None` are both attributable solely to that field.

/// The `ScanDelta(10) → mid → IntegrateSink` fixture: everything a guard test
/// needs to isolate one crafted field on `mid`.
struct MidCircuit {
    in_schema: SchemaDescriptor,
    out_schema: Option<SchemaDescriptor>,
    /// Where the mid node's scratch children are created. `None` = a fresh
    /// tempdir; a bad path is how a test reaches `create_child_table`'s
    /// failure arm.
    dir: Option<String>,
}

impl MidCircuit {
    fn new(in_schema: SchemaDescriptor) -> Self {
        MidCircuit {
            in_schema,
            out_schema: None,
            dir: None,
        }
    }

    /// Hold the sink to `out_schema`. Left unset, `build_plan` is entered on
    /// its `PlanTarget::Subgraph` path, which suppresses the sink-schema
    /// contract — what a test isolating a *mid-node* guard wants, since the mid
    /// node's output schema is exactly what it is varying.
    fn with_out_schema(mut self, out_schema: SchemaDescriptor) -> Self {
        self.out_schema = Some(out_schema);
        self
    }

    /// Home the scratch children at `dir` instead of a fresh tempdir.
    fn with_dir(mut self, dir: &str) -> Self {
        self.dir = Some(dir.to_owned());
        self
    }

    fn build(&self, mid: gnitz_wire::OpNode) -> Result<PlanBuildResult, CompileError> {
        let tmp = tempfile::tempdir().unwrap();
        let dir = self
            .dir
            .clone()
            .unwrap_or_else(|| tmp.path().to_str().unwrap().to_owned());
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, mid);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        let ext: ExtTables = HashMap::from([(10, self.in_schema)]);
        let (ordered, target) = match &self.out_schema {
            Some(out_schema) => (
                loaded.ordered.clone(),
                PlanTarget::ViewOutput { out_schema, seeds: &[] },
            ),
            None => (subgraph_ordered(&loaded, 1), PlanTarget::Subgraph { out: 1 }),
        };
        build_plan(
            &loaded,
            &ordered,
            &ext,
            test_site(&dir, 1),
            crate::schema::Placement::KEYED_DEFAULT,
            target,
        )
    }

    fn compiles(&self, mid: gnitz_wire::OpNode) -> bool {
        self.build(mid).is_ok()
    }
}

/// `ScanDelta(10) ⋈range IntegrateTrace(ScanDelta(11)) → IntegrateSink`,
/// compiled against the two given source schemas. Reports whether it
/// compiles.
fn range_join_plan(
    delta_schema: SchemaDescriptor,
    trace_schema: SchemaDescriptor,
    n_eq: u8,
) -> Result<PlanBuildResult, CompileError> {
    use gnitz_wire::{JoinKind, OpNode, RangeRel};
    let dir = tempfile::tempdir().unwrap();
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, scan_delta(11));
    nodes.insert(2, OpNode::IntegrateTrace);
    nodes.insert(
        3,
        OpNode::Join(JoinKind::DeltaTraceRange {
            n_eq,
            rel: RangeRel::Lt,
        }),
    );
    nodes.insert(4, OpNode::IntegrateSink);
    let loaded = loaded_for_test(
        nodes,
        vec![(0, 3, PORT_IN_A), (1, 2, PORT_IN), (2, 3, PORT_TRACE), (3, 4, PORT_IN)],
    );
    let ext: ExtTables = HashMap::from([(10, delta_schema), (11, trace_schema)]);
    build_plan(
        &loaded,
        &subgraph_ordered(&loaded, 3),
        &ext,
        test_site(dir.path().to_str().unwrap(), 1),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: 3 },
    )
}

/// The range probe's preconditions are enforced at compile time, not by a
/// `debug_assert` a release build strips. The walk slices both sides' PK
/// regions at one equality width, so a crafted circuit whose two sides
/// reindex to different strides — or whose `n_eq` leaves no range slot — must
/// be rejected rather than reach the operator.
#[test]
fn test_range_join_probe_preconditions_rejected() {
    // The shape the reindex packer produces: `[eq slot, range slot]` PK, then
    // payload.
    let band = |eq_tc: u8| {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(eq_tc, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    };
    let wide = band(type_code::U64); // pk_stride 16
    let narrow = band(type_code::U32); // pk_stride 12
    let rejection = |d, t, n_eq| match range_join_plan(d, t, n_eq) {
        Err(CompileError::Rejected(guard)) => guard,
        other => panic!("expected a rejection, got {:?}", other.map(|_| "a plan")),
    };
    assert!(
        range_join_plan(wide, wide, 1).is_ok(),
        "a matched pair at the common promoted type must compile"
    );
    const STRIDE: &str =
        "range join: delta and trace PK strides differ (both sides must reindex at the pair's common type)";
    assert_eq!(rejection(narrow, wide, 1), STRIDE, "delta side narrower than the trace");
    assert_eq!(rejection(wide, narrow, 1), STRIDE, "delta side wider than the trace");

    // `leading_key_size` sums *schema*-order columns, so a PK-last column
    // order — which the packer never emits but a crafted circuit can name —
    // puts the whole 8-byte key inside the `n_eq = 1` prefix while the key
    // arity is still `n_eq + 1`. That leaves no range slot.
    let pk_last = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
        ],
        &[1, 2],
    );
    assert_eq!(
        rejection(pk_last, pk_last, 1),
        "range join: eq prefix covers the whole key",
        "an eq prefix leaving no range slot",
    );
}

/// Build `ScanDelta(10) → mid → IntegrateSink` and report whether it compiles.
fn compiles_mid_node(in_schema: SchemaDescriptor, mid: gnitz_wire::OpNode) -> bool {
    MidCircuit::new(in_schema).compiles(mid)
}

/// The guard that rejected `ScanDelta(10) → mid → IntegrateSink`. Naming it is
/// what makes a guard test attributable: a bare `is_err()` also passes when an
/// unrelated guard fires, which is why these used to need a control build.
fn mid_node_rejection(in_schema: SchemaDescriptor, mid: gnitz_wire::OpNode) -> String {
    match MidCircuit::new(in_schema).build(mid) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected a rejection, got a plan"),
    }
}

#[test]
fn test_reduce_group_cols_out_of_bounds_rejected() {
    use crate::schema::ReduceOutKey;
    use gnitz_wire::{AggFunc, OpNode};
    let reduce = |group: Vec<u32>| OpNode::Reduce {
        group_cols: group,
        agg: vec![(AggFunc::Count, 0)],
        global_ground: false,
        out_key: ReduceOutKey::PkPermutation,
    };
    assert_eq!(
        mid_node_rejection(two_col_schema(), reduce(vec![200])),
        "reduce: group columns out of range"
    );
}

#[test]
fn test_reduce_agg_spec_col_out_of_bounds_rejected() {
    use crate::schema::ReduceOutKey;
    use gnitz_wire::{AggFunc, OpNode};
    let reduce = |col: u32| OpNode::Reduce {
        group_cols: vec![0],
        agg: vec![(AggFunc::Count, col)],
        global_ground: false,
        out_key: ReduceOutKey::PkPermutation,
    };
    assert_eq!(
        mid_node_rejection(two_col_schema(), reduce(200)),
        "reduce: aggregate column out of range"
    );
}

/// Every aggregate that decodes its column value needs a scalar register
/// image (`ScalarKind`) — the ≤8-byte int/float set. The SQL binder rejects
/// the rest upstream, so this covers the low-level `CircuitBuilder` path
/// that bypasses it.
#[test]
fn test_value_reading_aggregate_over_non_encodable_column_rejected() {
    use crate::schema::ReduceOutKey;
    use gnitz_wire::{AggFunc, OpNode};
    // col 0 = U64 PK and the whole group key (⇒ PkPermutation); col 1 = the
    // aggregate column, whose type is the only thing varying.
    let schema = |agg_tc: u8| {
        SchemaDescriptor::new(
            &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(agg_tc, 0)],
            &[0],
        )
    };
    let reduce = |func| OpNode::Reduce {
        group_cols: vec![0],
        agg: vec![(func, 1)],
        global_ground: false,
        out_key: ReduceOutKey::PkPermutation,
    };
    for func in [AggFunc::Sum, AggFunc::SumZero, AggFunc::Min, AggFunc::Max] {
        assert!(
            compiles_mid_node(schema(type_code::I64), reduce(func)),
            "{func:?} over I64"
        );
        for tc in [type_code::U128, type_code::STRING] {
            assert_eq!(
                mid_node_rejection(schema(tc), reduce(func)),
                "reduce: aggregate column type has no scalar register image",
                "{func:?} over type code {tc}",
            );
        }
    }
    // COUNT never reads the value, so no type excludes it.
    assert!(compiles_mid_node(schema(type_code::STRING), reduce(AggFunc::Count)));
}

#[test]
fn test_projection_col_out_of_bounds_rejected() {
    use gnitz_wire::{MapKind, OpNode};
    let rejection = |cols: Vec<u32>| mid_node_rejection(two_col_schema(), OpNode::Map(MapKind::Projection(cols)));
    assert_eq!(rejection(vec![200]), "projection map: columns out of range");
    // A PK source: `project_schema` drops it while `copy_cols`
    // numbers destinations densely, so the copy addresses a slot that does
    // not exist — `from_map` would index past the fixed `[_; 65]`.
    assert!(rejection(vec![0]).starts_with("map: program/schema mismatch"));
    // `oob_cols` bounds each index but not the list length, and duplicates
    // are legal, so a long list overruns `project_schema`'s array.
    // Exactly MAX_COLUMNS payload sources already overflow — the schema also
    // carries the input's PK column, which a length-only bound misses.
    assert_eq!(
        rejection(vec![1; crate::schema::MAX_COLUMNS]),
        "projection map: output exceeds MAX_COLUMNS"
    );
}

#[test]
fn test_null_extend_overflow_rejected() {
    use gnitz_wire::OpNode;
    const GUARD: &str = "null-extend: merged schema exceeds MAX_COLUMNS";
    let extend = |n: usize| OpNode::NullExtend {
        type_codes: vec![type_code::I64; n],
    };
    // A short type_codes list null-extends cleanly.
    assert!(compiles_mid_node(two_col_schema(), extend(1)));
    // MAX_COLUMNS type_codes overflow the fixed `[_; 65]` schema array.
    assert_eq!(
        mid_node_rejection(two_col_schema(), extend(crate::schema::MAX_COLUMNS)),
        GUARD
    );
    // (An undecodable type code is rejected at the wire decode boundary,
    // where the two sibling type-code lists are also validated.)
    // A near-max-width input plus a short extension overflows the *merged*
    // output width, which a bound on the list length alone cannot catch:
    // 64 + 2 > 65.
    let wide = {
        let mut cols = [SchemaColumn::new(type_code::I64, 0); 64];
        cols[0] = SchemaColumn::new(type_code::U64, 0);
        SchemaDescriptor::new(&cols, &[0])
    };
    assert_eq!(mid_node_rejection(wide, extend(2)), GUARD);
}

/// A view site over a throwaway directory: nothing under it was ever
/// checkpointed, so the children it opens resume nothing.
/// The node list of a subgraph ending at `out` — the same carve
/// `compile_view` performs for an exchange side, so a fixture cannot hand
/// `build_plan` a list production would never produce (one holding the sink).
fn subgraph_ordered(loaded: &LoadedCircuit, out: i32) -> Vec<i32> {
    let set = ancestors_inclusive(loaded, out);
    loaded.ordered.iter().copied().filter(|n| set.contains(n)).collect()
}

fn test_site(dir: &str, id: u64) -> ViewSite<'_> {
    ViewSite {
        dir,
        id,
        recovery: RecoverySource::Rederive { resume_at: None },
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
fn consume_flags(plan: &PlanBuildResult) -> Vec<(&'static str, bool)> {
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
                .filter_map(|(slot, reg)| Some((label[slot], program.last_read[reg? as usize] == pc as u32)))
                .collect()
        })
        .collect()
}

/// The INTERSECT/EXCEPT fan-out shape: ScanDelta(10)'s register fans into both
/// a destructive `Distinct` and a non-destructive `Negate` co-reader (standing
/// in for integrate_trace). Kahn's ascending tie-break schedules the lower id
/// first, so the ids decide which consumer runs first. (The reader is a
/// `Negate`, not a `Filter(None)`: a predicate-less Filter is elided by
/// register aliasing and would no longer read the register at runtime.)
fn make_dtor_fanout(distinct_id: i32, reader_id: i32) -> LoadedCircuit {
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(distinct_id, gnitz_wire::OpNode::Distinct);
    nodes.insert(reader_id, gnitz_wire::OpNode::Negate);
    let edges = vec![(0, distinct_id, PORT_IN), (0, reader_id, PORT_IN)];
    loaded_for_test(nodes, edges)
}

#[test]
fn a_destructive_op_takes_its_input_only_when_it_is_the_last_reader() {
    let dir = tempfile::tempdir().unwrap();
    let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
    let flags = |distinct_id: i32, reader_id: i32| {
        let loaded = make_dtor_fanout(distinct_id, reader_id);
        let plan = build_plan(
            &loaded,
            &loaded.ordered,
            &ext,
            test_site(dir.path().to_str().unwrap(), 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: distinct_id },
        )
        .expect("both orderings compile");
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
/// a later reader, so both sides are taken. `consume_b` carries as much as
/// `consume_a`: one operand is empty every epoch and the other is returned whole.
#[test]
fn a_union_of_two_unread_operands_takes_both_sides() {
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, scan_delta(11));
    nodes.insert(2, gnitz_wire::OpNode::Union);
    let loaded = loaded_for_test(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B)]);
    let plan = build_plan(
        &loaded,
        &loaded.ordered,
        &HashMap::from([(10, two_col_schema()), (11, two_col_schema())]),
        test_site("", 1),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: 2 },
    )
    .expect("a two-source union compiles");
    assert_eq!(consume_flags(&plan), vec![("union.a", true), ("union.b", true)]);
}

/// The epoch-end output extraction reads the sink register, and it is not an
/// instruction — so a `Union` whose operand *is* the sink would otherwise take
/// it empty and emit nothing, silently, every epoch.
#[test]
fn a_union_over_the_sink_register_does_not_take_it() {
    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(1, gnitz_wire::OpNode::Negate);
    nodes.insert(2, gnitz_wire::OpNode::Union);
    let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN_A), (0, 2, PORT_IN_B)]);
    let plan = build_plan(
        &loaded,
        &loaded.ordered,
        &HashMap::from([(10, two_col_schema())]),
        test_site("", 1),
        crate::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: 1 },
    )
    .expect("a union over the plan's own output register compiles");
    assert_eq!(
        consume_flags(&plan),
        vec![("negate", false), ("union.a", false), ("union.b", true)],
        "operand A is the sink and must not be taken; the Negate's own input is \
         still read by operand B, which has no later reader of its own",
    );
}

/// A trace port fed by a node that is not an integral is rejected at compile
/// time: `resolve_inputs` checks a `Join`'s port arity, not its producer's
/// kind, and such a register would reach the dispatch with no cursor. The ONLY
/// difference between the two builds is which node feeds `PORT_TRACE`.
#[test]
fn a_join_whose_trace_port_is_not_an_integral_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let ext: ExtTables = HashMap::from([(10, two_col_schema()), (11, two_col_schema())]);
    // Node 2 is the integral of scan 11; node 1 is that scan's own register,
    // which carries a delta and no table.
    let plan = |trace_src: i32| {
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, scan_delta(11));
        nodes.insert(2, gnitz_wire::OpNode::IntegrateTrace);
        nodes.insert(3, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        let loaded = loaded_for_test(
            nodes,
            vec![(1, 2, PORT_IN), (0, 3, PORT_IN_A), (trace_src, 3, PORT_TRACE)],
        );
        build_plan(
            &loaded,
            &loaded.ordered,
            &ext,
            test_site(dir.path().to_str().unwrap(), 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 3 },
        )
    };
    assert!(plan(2).is_ok(), "an integral is a valid trace port");
    assert!(
        matches!(
            plan(1),
            Err(CompileError::Rejected("join: trace port is not an integral"))
        ),
        "a delta register on the trace port is a mis-built circuit, not a plan",
    );
}

#[test]
fn test_destructive_fanout_skipped_distinct_not_rejected() {
    use crate::schema::ReduceOutKey;
    // `ScanDelta → Reduce → {Distinct, Negate}`: the Distinct schedules before
    // its co-reader, the destructive-first shape the guard rejects. But a
    // Reduce's output is already distinct, so the elision pass drops the
    // Distinct — it aliases the Reduce's register and emits no destructive op,
    // and the guard must not reject it.
    let dir = tempfile::tempdir().unwrap();
    let view_dir = dir.path().to_str().unwrap();

    let mut nodes = HashMap::new();
    nodes.insert(0, scan_delta(10));
    nodes.insert(
        1,
        gnitz_wire::OpNode::Reduce {
            group_cols: vec![],
            agg: vec![(gnitz_wire::AggFunc::Count, 0)],
            global_ground: false,
            out_key: ReduceOutKey::SyntheticFold,
        },
    );
    nodes.insert(2, gnitz_wire::OpNode::Distinct);
    nodes.insert(3, gnitz_wire::OpNode::Negate);
    let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (1, 3, PORT_IN)]);
    assert!(
        compute_skip_nodes(&loaded).contains(&2),
        "test precondition: the elision pass must drop the Reduce-fed Distinct"
    );

    let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
    // The plan owns scratch dirs under `dir`, so it must drop first: build it
    // inside the assert rather than binding it past `dir`'s scope.
    assert!(
        build_plan(
            &loaded,
            &loaded.ordered,
            &ext,
            test_site(view_dir, 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 2 }
        )
        .is_ok(),
        "a skipped (optimized-out) Distinct does not run destructively; \
         the guard must not reject it"
    );
}

/// Every operator that creates a scratch child must fail the compile when the
/// creation fails, never emit a plan without it: a dropped `Integrate` would
/// compile a view that never persists its differential state, leaving its
/// output permanently empty.
#[test]
fn test_build_plan_child_table_failure_rejected() {
    let one_col = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
    let fixture = MidCircuit::new(one_col).with_dir("/nonexistent_gnitz_test_path_xyz_abc");
    for mid in [gnitz_wire::OpNode::Distinct, gnitz_wire::OpNode::IntegrateTrace] {
        assert!(
            matches!(fixture.build(mid.clone()), Err(CompileError::StorageFailed(..))),
            "{mid:?} must fail the compile when its child table cannot be created",
        );
    }
}

/// The two derived map out-schemas are by construction ones `MapPlan`
/// accepts. `HashRow` is the only site that emits a *promoting* `CopyCol`,
/// so it is what makes `check_copy_types`' widening clause do work; the
/// reindex case pins that `payload_copy_srcs` and
/// `ReindexPacker::output_schema` cannot drift apart into a mixed-type copy.
#[test]
fn test_derived_map_schemas_satisfy_copy_types() {
    use crate::expr::{MapPlan, PkSource};
    use gnitz_expr::LogicalProgram;
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 1),
            SchemaColumn::new(type_code::F32, 1),
            SchemaColumn::new(type_code::U128, 1),
            SchemaColumn::new(type_code::U32, 1),
        ],
        &[0],
    );
    // Every source column copied into a dense destination slot, the shape
    // `create_universal_projection` builds.
    let cols: Vec<u32> = vec![0, 1, 2, 3, 4];
    let prog = || LogicalProgram::copy_cols(&cols);
    let payload_cols = prog().payload_copy_srcs().unwrap().to_vec();
    let reindexed = crate::schema::key::ReindexPacker::new(&in_schema, &[4], &[type_code::U64])
        .unwrap()
        .output_schema(&in_schema, &payload_cols)
        .unwrap();
    assert!(MapPlan::from_map(prog(), &in_schema, &reindexed, PkSource::Inherit).is_ok());

    // A cross-width set-op coercion: the U32 column promoted to I64, every
    // other column carried verbatim (target 0). The promotion is one
    // `payload_promotion_invalid` admits, so a real HashRow can build it.
    let tcs = vec![0, 0, 0, 0, type_code::I64];
    let wire_cols: Vec<u32> = cols.to_vec();
    assert!(!optimize::payload_promotion_invalid(&wire_cols, &tcs, &in_schema));
    let hashed = hashrow_output_schema(&in_schema, &cols, &tcs).unwrap();
    assert!(MapPlan::from_map(prog(), &in_schema, &hashed, PkSource::Inherit).is_ok());
}
