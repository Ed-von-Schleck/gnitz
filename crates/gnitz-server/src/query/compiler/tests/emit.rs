use super::*;
use crate::test_support::{
    make_schema_u128_i64, make_schema_u64_i64, pk_only_schema, pk_payload_schema, u64_pk_schema,
};
use gnitz_store::schema::{type_code, SchemaColumn};

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

    fn build(&self, mid: gnitz_wire::OpNode) -> Result<PlanBuildResult, CompileError> {
        let loaded = loaded_for_test(
            HashMap::from([(0, scan_delta(10)), (1, mid), (2, gnitz_wire::OpNode::IntegrateSink)]),
            vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
        );
        build_plan(
            &loaded,
            &loaded.subgraph_ordered(1),
            &HashMap::from([(10i64, self.in_schema)]),
            test_site(self.tmp.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 1 },
        )
    }

    fn compiles(&self, mid: gnitz_wire::OpNode) -> bool {
        self.build(mid).is_ok()
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
        ram: RamBudgets::default(),
    }
}

// ── Union layout ────────────────────────────────────────────────────────

/// `union_nullability_merge` ORs the two inputs' per-column nullability, so a
/// null-carrying side reclassifies the output from the null-blind
/// `FixedIntNonnull` fast comparator to the null-aware `Generic` one.
#[test]
fn union_merges_nullability_and_reclassifies_the_comparator() {
    use gnitz_store::schema::PayloadCmpKind;
    let nonnull = make_schema_u128_i64();
    let nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Both non-nullable stays on the fast path; either side nullable forces
    // `Generic`, and the OR is symmetric.
    for (a, b, want_nullable) in [(nonnull, nonnull, 0u8), (nonnull, nullable, 1), (nullable, nonnull, 1)] {
        let m = union_nullability_merge(&a, &b).expect("shared layout");
        assert_eq!(m.columns[1].nullable, want_nullable);
        assert_eq!(
            m.payload_cmp,
            if want_nullable == 1 {
                PayloadCmpKind::Generic
            } else {
                PayloadCmpKind::FixedIntNonnull
            }
        );
    }
}

/// `union_nullability_merge` is the Union arm's whole layout contract, and in
/// release there is nothing else: a mismatched pair would adopt `a`'s schema
/// and let `op_union` read `b`'s bytes through it.
#[test]
fn union_of_mismatched_input_layouts_is_rejected() {
    let one = pk_only_schema(&[type_code::U64]);
    let loaded = loaded_for_test(
        HashMap::from([(0, scan_delta(10)), (1, scan_delta(11)), (2, gnitz_wire::OpNode::Union)]),
        vec![(0, 2, SLOT_IN), (1, 2, SLOT_TRACE)],
    );
    // `Subgraph` so the union guard is the only one that can fire — the sink
    // contract never runs.
    let plan = |b: SchemaDescriptor| {
        build_plan(
            &loaded,
            &loaded.subgraph_ordered(2),
            &HashMap::from([(10i64, one), (11, b)]),
            test_site("", 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 2 },
        )
    };
    assert!(plan(one).is_ok(), "a matched pair must compile");
    assert_eq!(
        rejection(plan(make_schema_u64_i64())),
        "union: inputs do not share a physical layout"
    );
}

// ── Plan targets and the sink contract ──────────────────────────────────

/// A `Subgraph`'s output register is the named node's, and a node list that
/// reached the sink is rejected — production carves an exchange side out of
/// the shard input's ancestors, so the sink is never in one.
#[test]
fn a_subgraph_outputs_the_named_node_and_must_not_hold_the_sink() {
    let loaded = loaded_for_test(
        HashMap::from([
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::Negate),
            (2, gnitz_wire::OpNode::IntegrateSink),
        ]),
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN)],
    );
    let ext: ExtTables = HashMap::from([(10i64, pk_only_schema(&[type_code::U64]))]);
    let plan = |ordered: &[i32]| {
        build_plan(
            &loaded,
            ordered,
            &ext,
            test_site("", 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 1 },
        )
    };

    let carved = plan(&loaded.subgraph_ordered(1)).expect("the carve production performs");
    assert_eq!(
        carved.vm.program.out_reg,
        *carved.out_reg_of.get(&1).unwrap(),
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
        HashMap::from([(0, scan_delta(10)), (1, gnitz_wire::OpNode::IntegrateSink)]),
        vec![(0, 1, SLOT_IN)],
    );
    let against = |view_schema: &SchemaDescriptor, source: SchemaDescriptor| {
        build_plan(
            &loaded,
            &loaded.ordered,
            &HashMap::from([(10i64, source)]),
            test_site("", 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
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

/// A circuit producing more than `u16::MAX` registers must fail the compile
/// rather than wrap the cast and panic in `ProgramBuilder::build`.
#[test]
fn a_register_count_over_u16_max_is_rejected() {
    let n = u16::MAX as i32 + 1;
    let mut nodes = HashMap::from([(0, scan_delta(10))]);
    let mut edges = Vec::new();
    for nid in 1..n {
        nodes.insert(nid, gnitz_wire::OpNode::Negate);
        edges.push((nid - 1, nid, SLOT_IN));
    }
    let loaded = loaded_for_test(nodes, edges);
    assert_eq!(loaded.ordered.len(), n as usize);
    let plan = build_plan(
        &loaded,
        &loaded.ordered,
        &ExtTables::new(),
        test_site("", 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        PlanTarget::ViewOutput {
            out_schema: &pk_only_schema(&[type_code::U64]),
            seeds: &[],
        },
    );
    assert_eq!(rejection(plan), "register count exceeds u16::MAX");
}

// ── Crafted raw-field guards: reject at compile, never abort at run ─────
//
// Each guard is proven by construction: the ONLY difference between the two
// builds is the crafted field, so a valid build's `Ok` and the crafted build's
// named rejection are both attributable solely to that field.

/// The planner ships the reduce output-key kind; the engine validates it
/// against the input schema and hard-rejects any kind the schema does not
/// warrant — the guard that turns a silent output-column scramble into a
/// compile failure. Covers all three schema shapes × all three kinds: the three
/// matching kinds compile, the six cross pairings reject.
#[test]
fn a_reduce_out_key_the_schema_does_not_warrant_is_rejected() {
    use gnitz_store::schema::ReduceOutKey;
    use gnitz_wire::{AggFunc, OpNode};
    let reduce = |group: Vec<u32>, out_key: ReduceOutKey| OpNode::Reduce {
        group_cols: group,
        // A linear COUNT keeps the MIN/MAX-eligibility guard out of the picture,
        // isolating the out_key validation.
        agg: vec![gnitz_wire::AggDescriptor { agg_op: AggFunc::Count, col_idx: 0 }],
        global_ground: false,
        out_key,
    };
    let with_group_col = |tc: u8| {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(tc, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    };
    // (schema, group cols, the ONE kind the schema warrants).
    let cases = [
        (make_schema_u64_i64(), vec![0u32], ReduceOutKey::PkPermutation),
        (with_group_col(type_code::U64), vec![1], ReduceOutKey::SingleNaturalCol),
        (with_group_col(type_code::STRING), vec![1], ReduceOutKey::SyntheticFold),
    ];
    let all_kinds = [
        ReduceOutKey::SyntheticFold,
        ReduceOutKey::PkPermutation,
        ReduceOutKey::SingleNaturalCol,
    ];
    for (schema, group, correct) in cases {
        let fixture = MidCircuit::new(schema);
        for kind in all_kinds {
            let mid = reduce(group.clone(), kind);
            if kind == correct {
                assert!(fixture.compiles(mid), "{kind:?} is the kind this schema warrants");
            } else {
                assert_eq!(
                    fixture.rejection(mid),
                    "reduce: out_key does not match input schema",
                    "out_key {kind:?} against the schema warranting {correct:?}",
                );
            }
        }
    }
}

#[test]
fn reduce_column_indices_out_of_range_are_rejected() {
    use gnitz_store::schema::ReduceOutKey;
    use gnitz_wire::{AggFunc, OpNode};
    let reduce = |group: Vec<u32>, agg_col: u32| OpNode::Reduce {
        group_cols: group,
        agg: vec![gnitz_wire::AggDescriptor { agg_op: AggFunc::Count, col_idx: agg_col }],
        global_ground: false,
        out_key: ReduceOutKey::PkPermutation,
    };
    let fixture = MidCircuit::new(make_schema_u64_i64());
    assert_eq!(
        fixture.rejection(reduce(vec![200], 0)),
        "reduce: group columns out of range"
    );
    assert_eq!(
        fixture.rejection(reduce(vec![0], 200)),
        "reduce: aggregate column out of range"
    );
}

/// Every aggregate that decodes its column value needs a scalar register image
/// (`ScalarKind`) — the ≤8-byte int/float set. The SQL binder rejects the rest
/// upstream, so this covers the low-level `CircuitBuilder` path that bypasses it.
#[test]
fn a_value_reading_aggregate_over_a_non_scalar_column_is_rejected() {
    use gnitz_store::schema::ReduceOutKey;
    use gnitz_wire::{AggFunc, OpNode};
    // col 0 = U64 PK and the whole group key (⇒ PkPermutation); col 1 = the
    // aggregate column, whose type is the only thing varying.
    let reduce = |func| OpNode::Reduce {
        group_cols: vec![0],
        agg: vec![gnitz_wire::AggDescriptor { agg_op: func, col_idx: 1 }],
        global_ground: false,
        out_key: ReduceOutKey::PkPermutation,
    };
    for func in [AggFunc::Sum, AggFunc::SumZero, AggFunc::Min, AggFunc::Max] {
        assert!(
            MidCircuit::new(make_schema_u64_i64()).compiles(reduce(func)),
            "{func:?} over I64"
        );
        for tc in [type_code::U128, type_code::STRING] {
            assert_eq!(
                MidCircuit::new(u64_pk_schema(SchemaColumn::new(tc, 0))).rejection(reduce(func)),
                "reduce: aggregate column type has no scalar register image",
                "{func:?} over type code {tc}",
            );
        }
    }
    // COUNT never reads the value, so no type excludes it.
    assert!(MidCircuit::new(u64_pk_schema(SchemaColumn::new(type_code::STRING, 0))).compiles(reduce(AggFunc::Count)));
}

#[test]
fn projection_columns_out_of_range_are_rejected() {
    use gnitz_wire::{MapKind, OpNode};
    let fixture = MidCircuit::new(make_schema_u64_i64());
    let rejection = |cols: Vec<u32>| fixture.rejection(OpNode::Map(MapKind::Projection(cols)));
    assert_eq!(rejection(vec![200]), "projection map: columns out of range");
    // A PK source: `project_schema` drops it while `copy_cols` numbers
    // destinations densely, so the copy addresses a slot that does not exist.
    assert!(rejection(vec![0]).starts_with("map: program/schema mismatch"));
    // `oob_cols` bounds each index but not the list length, and duplicates are
    // legal, so a long list overruns `project_schema`'s array. Exactly
    // MAX_COLUMNS payload sources already overflow — the schema also carries the
    // input's PK column, which a length-only bound misses.
    assert_eq!(
        rejection(vec![1; gnitz_store::schema::MAX_COLUMNS]),
        "projection map: output exceeds MAX_COLUMNS"
    );
}

#[test]
fn a_null_extend_overflowing_the_merged_schema_is_rejected() {
    use gnitz_wire::OpNode;
    const GUARD: &str = "null-extend: merged schema exceeds MAX_COLUMNS";
    let extend = |n: usize| OpNode::NullExtend { type_codes: vec![type_code::I64; n] };
    let narrow = MidCircuit::new(make_schema_u64_i64());
    assert!(narrow.compiles(extend(1)), "a short type_codes list extends cleanly");
    // MAX_COLUMNS type_codes overflow the fixed schema array on their own.
    assert_eq!(narrow.rejection(extend(gnitz_store::schema::MAX_COLUMNS)), GUARD);
    // A near-max-width input plus a short extension overflows the *merged* output
    // width, which a bound on the list length alone cannot catch: 64 + 2 > 65.
    let wide = {
        let mut cols = [SchemaColumn::new(type_code::I64, 0); 64];
        cols[0] = SchemaColumn::new(type_code::U64, 0);
        SchemaDescriptor::new(&cols, &[0])
    };
    assert_eq!(MidCircuit::new(wide).rejection(extend(2)), GUARD);
    // (An undecodable type code is rejected at the wire decode boundary, where
    // the two sibling type-code lists are also validated.)
}

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
                .rejection(OpNode::Filter(Some(blob.clone())))
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

/// The reindex arm derives its output layout from the client's key and kept-column
/// lists, so every way those lists can overrun the fixed schema arrays has to be a
/// named rejection rather than a panic or a truncated key.
#[test]
fn reindex_key_and_kept_column_lists_are_bounds_checked() {
    use gnitz_wire::{MapKind, OpNode};
    let reindex = |keep: Vec<u32>, key_cols: Vec<u32>| {
        OpNode::Map(MapKind::Reindex {
            keep,
            key: key_cols.into_iter().map(|c| (c, None)).collect(),
            role: gnitz_wire::ReindexRole::ScatterKey,
        })
    };
    let three = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // A compound key and a pruned payload both compile — neither is an identity,
    // since a reindex overwrites every row's PK.
    assert!(MidCircuit::new(make_schema_u64_i64()).compiles(reindex(vec![0, 1], vec![0, 1])));
    assert!(MidCircuit::new(three).compiles(reindex(vec![2], vec![0])));

    let narrow = MidCircuit::new(make_schema_u64_i64());
    assert_eq!(
        narrow.rejection(reindex(vec![9], vec![0])),
        "map: reindex payload column out of range"
    );
    assert_eq!(
        narrow.rejection(reindex(vec![0], vec![9])),
        "map: reindex columns out of range"
    );
    // A key longer than MAX_PK_COLUMNS overflows the output schema's fixed PK array.
    let n = gnitz_store::schema::MAX_PK_COLUMNS + 1;
    let wide = SchemaDescriptor::new(&vec![SchemaColumn::new(type_code::U64, 0); n], &[0]);
    assert_eq!(
        MidCircuit::new(wide).rejection(reindex(vec![0], (0..n as u32).collect())),
        "map: invalid reindex key"
    );
}

/// The set-op full-row identity map: a synthetic U128 PK over the projected
/// payload, with each slot optionally promoted to a wider fixed-int type so a
/// cross-width pair (`I32 UNION I64`) hashes one physical layout.
#[test]
fn a_hash_row_map_promotes_within_the_copy_kernel_domain_or_is_rejected() {
    use gnitz_wire::{MapKind, OpNode};
    let hash_row = |cols: Vec<u32>, tcs: Vec<Option<gnitz_wire::TypeCode>>| {
        OpNode::Map(MapKind::HashRow {
            cols: cols.into_iter().zip(tcs).collect(),
            branch_id: 0,
        })
    };
    let fixture = MidCircuit::new(u64_pk_schema(SchemaColumn::new(type_code::U32, 0)));
    assert!(fixture.compiles(hash_row(vec![1], vec![None])), "no promotion");
    assert!(
        fixture.compiles(hash_row(vec![1], vec![Some(gnitz_wire::TypeCode::I64)])),
        "U32 → I64 is the ≤8-byte widen the copy kernel supports"
    );
    assert_eq!(
        fixture.rejection(hash_row(vec![9], vec![None])),
        "hash-row map: columns out of range"
    );
    assert_eq!(
        fixture.rejection(hash_row(vec![1], vec![Some(gnitz_wire::TypeCode::String)])),
        "hash-row map: invalid promotion target",
        "a German string is not a fixed-int widen"
    );
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
        plan_two_source_join(
            JoinKind::DeltaTraceRange { n_eq, rel: RangeRel::Lt },
            delta_schema,
            trace_schema,
        )
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
    let plan = plan_two_source_join(gnitz_wire::JoinKind::DeltaTraceCross, narrow, wide);
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
        HashMap::from([
            (0, scan_delta(10)),
            (1, scan_delta(11)),
            (2, OpNode::IntegrateTrace),
            (3, OpNode::Join(kind)),
            (4, OpNode::IntegrateSink),
        ]),
        vec![(0, 3, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_TRACE), (3, 4, SLOT_IN)],
    );
    build_plan(
        &loaded,
        &loaded.subgraph_ordered(3),
        &HashMap::from([(10i64, delta_schema), (11, trace_schema)]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
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
            HashMap::from([
                (0, scan_delta(10)),
                (1, scan_delta(11)),
                (2, gnitz_wire::OpNode::IntegrateTrace),
                (3, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace)),
            ]),
            vec![(1, 2, SLOT_IN), (0, 3, SLOT_IN), (trace_src, 3, SLOT_TRACE)],
        );
        build_plan(
            &loaded,
            &loaded.ordered,
            &HashMap::from([(10i64, two_col), (11, two_col)]),
            test_site(dir.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 3 },
        )
        .map(drop)
    };
    assert!(plan(2).is_ok(), "an integral is a valid trace port");
    assert_eq!(rejection(plan(1)), "join: trace port is not an integral");
}

/// A wide (24-byte, 3 × U64) PK carries through the join's byte-keyed probe.
#[test]
fn a_wide_pk_join_compiles() {
    let schema = crate::test_support::pk_only_schema(&[type_code::U64; 3]);
    let dir = tempfile::tempdir().unwrap();
    let loaded = loaded_for_test(
        HashMap::from([
            (0, scan_delta(10)),
            (1, scan_delta(20)),
            (2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace)),
            (3, gnitz_wire::OpNode::IntegrateSink),
            (4, gnitz_wire::OpNode::IntegrateTrace),
        ]),
        vec![(0, 2, SLOT_IN), (1, 4, SLOT_IN), (4, 2, SLOT_TRACE), (2, 3, SLOT_IN)],
    );
    // The plan owns scratch dirs under `dir`, so it must drop first: build it
    // inside the assert rather than binding it past `dir`'s scope.
    assert!(build_plan(
        &loaded,
        &loaded.subgraph_ordered(2),
        &HashMap::from([(10i64, schema), (20, schema)]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
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
        HashMap::from([
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::IntegrateTrace),
            (2, gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Projection(vec![200]))),
            (3, gnitz_wire::OpNode::IntegrateSink),
        ]),
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (2, 3, SLOT_IN)],
    );
    let result = build_plan(
        &loaded,
        &loaded.subgraph_ordered(2),
        &HashMap::from([(10i64, make_schema_u64_i64())]),
        test_site(view_dir.to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: 2 },
    );
    assert_eq!(rejection(result), "projection map: columns out of range");

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
        HashMap::from([
            (0, scan_delta(10)),
            (1, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] }),
            (2, gnitz_wire::OpNode::Filter(None)),
            (3, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] }),
            (4, gnitz_wire::OpNode::IntegrateSink),
        ]),
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
        &HashMap::from([(10i64, make_schema_u64_i64())]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: ex_in },
    );
    assert_eq!(rejection(result), "chained exchange nodes");
}

// ── Weight-clamp presets ────────────────────────────────────────────────

/// The two clamp operators differ in nothing but their preset, and the preset is
/// what makes them different operators: `distinct` is the set-membership clamp
/// `[-1, 1]`, `positive_part` the bag clamp that drops the negative part only.
#[test]
fn the_two_clamp_operators_carry_their_own_presets() {
    let fixture = MidCircuit::new(make_schema_u64_i64());
    for (op, want) in [
        (gnitz_wire::OpNode::Distinct, (-1i64, 1i64)),
        (gnitz_wire::OpNode::PositivePart, (0, i64::MAX)),
    ] {
        let plan = fixture.build(op.clone()).expect("both clamps compile");
        let got = plan.vm.program.instructions.iter().find_map(|i| match i {
            Instr::WeightClamp { lo, hi, .. } => Some((*lo, *hi)),
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
/// `Negate`, not a `Filter(None)`: a predicate-less Filter is elided by register
/// aliasing and would no longer read the register at runtime.)
#[test]
fn a_destructive_op_takes_its_input_only_when_it_is_the_last_reader() {
    let dir = tempfile::tempdir().unwrap();
    let ext: ExtTables = HashMap::from([(10i64, make_schema_u64_i64())]);
    let flags = |distinct_id: i32, reader_id: i32| {
        let loaded = loaded_for_test(
            HashMap::from([
                (0, scan_delta(10)),
                (distinct_id, gnitz_wire::OpNode::Distinct),
                (reader_id, gnitz_wire::OpNode::Negate),
            ]),
            vec![(0, distinct_id, SLOT_IN), (0, reader_id, SLOT_IN)],
        );
        let plan = build_plan(
            &loaded,
            &loaded.ordered,
            &ext,
            test_site(dir.path().to_str().unwrap(), 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
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
            &HashMap::from([(10i64, two_col), (11, two_col)]),
            test_site("", 1),
            gnitz_store::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out },
        )
        .expect("a union plan compiles");
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

/// `ScanDelta → Reduce → {Distinct, Negate}`: the Distinct schedules before its
/// co-reader, the destructive-first shape the liveness guard rejects. But a
/// Reduce's output is already distinct, so the elision pass drops the Distinct —
/// it aliases the Reduce's register and emits no destructive op at all.
#[test]
fn an_elided_distinct_does_not_trip_the_destructive_fanout_guard() {
    use gnitz_store::schema::ReduceOutKey;
    let dir = tempfile::tempdir().unwrap();
    let loaded = loaded_for_test(
        HashMap::from([
            (0, scan_delta(10)),
            (
                1,
                gnitz_wire::OpNode::Reduce {
                    group_cols: vec![],
                    agg: vec![gnitz_wire::AggDescriptor {
                        agg_op: gnitz_wire::AggFunc::Count,
                        col_idx: 0,
                    }],
                    global_ground: false,
                    out_key: ReduceOutKey::SyntheticFold,
                },
            ),
            (2, gnitz_wire::OpNode::Distinct),
            (3, gnitz_wire::OpNode::Negate),
        ]),
        vec![(0, 1, SLOT_IN), (1, 2, SLOT_IN), (1, 3, SLOT_IN)],
    );
    assert!(
        loaded.skip_nodes.contains(&2),
        "test precondition: the elision pass must drop the Reduce-fed Distinct"
    );

    // The plan owns scratch dirs under `dir`, so it must drop first: build it
    // inside the assert rather than binding it past `dir`'s scope.
    assert!(build_plan(
        &loaded,
        &loaded.ordered,
        &HashMap::from([(10i64, make_schema_u64_i64())]),
        test_site(dir.path().to_str().unwrap(), 1),
        gnitz_store::schema::Placement::KEYED_DEFAULT,
        PlanTarget::Subgraph { out: 2 }
    )
    .is_ok());
}
