use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::make_schema_u64_i64;
use gnitz_store::relation::StateLayout;
use gnitz_wire::{Circuit, JoinKind};

/// Exchange-free `build` emitted whole, as one worker's plan over sources 10 and 11.
fn plan(build: Build) -> Result<SubPlan, String> {
    let mut c = Circuit::default();
    build(&mut c);
    let loaded = loaded(c);
    let schema = make_schema_u64_i64();
    build_plan(
        &loaded,
        &loaded.ordered_where(|_| true),
        &sources([(10, schema), (11, schema)]),
        &mut StateLayout::default(),
        true,
        &[],
        PlanOut::Node(loaded.sink()?),
    )
    .map(|built| built.plan)
}

/// The driver seeds one register per source, so a second scan of one would
/// silently see nothing.
#[test]
fn a_plan_scanning_one_source_twice_is_rejected() {
    let twice = plan(|c| {
        let (a, b) = (scan(c, 10), scan(c, 10));
        let u = c.union(a, b);
        c.sink(u);
    });
    assert_eq!(rejection(twice), "scan-delta: a plan scans one source twice");
}

/// A `Filter` program that does not decode aborts the compile: passing every row
/// instead would turn the WHERE into WHERE TRUE.
#[test]
fn a_corrupt_filter_program_aborts_the_compile() {
    let garbled = plan(|c| {
        let a = scan(c, 10);
        let f = c.filter(a, vec![0xFF; 16]);
        c.sink(f);
    });
    let empty = plan(|c| {
        let a = scan(c, 10);
        let f = c.filter(a, Vec::new());
        c.sink(f);
    });
    for built in [garbled, empty] {
        assert!(rejection(built).starts_with("filter: invalid predicate program"));
    }
}

// ── An integral that is its source's own store ────────────────────────────

/// What varies of relation 10 `(a, b | v)`, keyed `(a, b)`.
#[derive(Clone, Copy)]
struct Wide {
    kind: gnitz_store::relation::RelationKind,
    placement: fn(&SchemaDescriptor) -> gnitz_zset::schema::Placement,
    a: gnitz_wire::TypeCode,
}

/// Relation 10 as `wide` has it and table 11 `(k | w)`, registered for worker
/// `slot` under a fresh directory.
fn two_tables(slot: gnitz_zset::schema::Slot, wide: Wide) -> (RelationRegistry, tempfile::TempDir) {
    use gnitz_store::relation::{RelationKind, RelationSpec, StoreConfig};
    use gnitz_wire::TypeCode;
    use gnitz_zset::schema::{Placement, SchemaColumn};
    let col = SchemaColumn::new(TypeCode::U64, false);
    let (kind_of_10, placement) = (wide.kind, wide.placement);
    let wide = SchemaDescriptor::new(&[SchemaColumn::new(wide.a, false), col, col], &[0, 1]);
    let narrow = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::new(dir.path().to_str().unwrap(), slot, StoreConfig::default());
    for (id, kind, schema, placement) in [
        (10, kind_of_10, wide, placement(&wide)),
        (11, RelationKind::BaseTable, narrow, Placement::full_pk(&narrow)),
    ] {
        registry
            .register(RelationSpec {
                id,
                kind,
                schema,
                placement,
                pk_repeats: false,
            })
            .unwrap();
    }
    (registry, dir)
}

/// The integral of the node `build` answers, as its circuit compiled on worker
/// `slot`.
fn trace_of(slot: gnitz_zset::schema::Slot, wide: Wide, build: fn(&mut Circuit) -> NodeId) -> Integral {
    let (registry, _dir) = two_tables(slot, wide);
    let mut c = Circuit::default();
    let integrand = build(&mut c);
    let loaded = loaded(c);
    let built = build_plan(
        &loaded,
        &loaded.ordered_where(|_| true),
        &registry,
        &mut StateLayout::default(),
        slot.of == 1,
        &[],
        PlanOut::Node(loaded.sink().unwrap()),
    )
    .unwrap();
    built.integrals[integrand].expect("a join probes the integrand")
}

/// `10 ⋈ 11` on `10.a = 11.k`: 10 re-keyed onto `key`, `through` in between;
/// answers 10's re-key, the node 11's delta probes the integral of.
fn rekeyed_join(
    c: &mut Circuit,
    key: &[gnitz_wire::ReindexSlot],
    scatters: bool,
    through: fn(&mut Circuit, NodeId) -> NodeId,
    kind: JoinKind,
) -> NodeId {
    use gnitz_wire::{NullKeys, ReindexRole};
    let role = |key: &[gnitz_wire::ReindexSlot]| match scatters {
        true => ReindexRole::ScatterKey { source_key: key.to_vec() },
        false => ReindexRole::Auxiliary,
    };
    let (a, b) = (scan(c, 10), scan(c, 11));
    let a = through(c, a);
    let ra = c.map_reindex(a, key, &[1, 2], role(key), NullKeys::Drop);
    // Both sides pack at the one type the pair shares.
    let b_key = [(0, key[0].1)];
    let rb = c.map_reindex(b, &b_key, &[1], role(&b_key), NullKeys::Drop);
    let j = c.join_terms([ra, rb], [ra, rb], kind);
    c.sink(j);
    ra
}

const A: [gnitz_wire::ReindexSlot; 1] = [(0, gnitz_wire::TypeCode::U64)];

/// Nothing between the scan and the re-key.
fn id(_: &mut Circuit, n: NodeId) -> NodeId {
    n
}

/// An integral is read off its source table exactly where the table's store
/// holds, on this worker and in key order, every row the integral would.
#[test]
fn an_integral_is_its_source_table_only_where_the_table_holds_what_it_would() {
    use gnitz_store::relation::RelationKind::{BaseTable, Stream};
    use gnitz_wire::TypeCode::{U128, U32, U64, UUID};
    use gnitz_zset::schema::{Placement, Slot};
    let is_source = |t: Integral| t == Integral::Source(10);
    let table = Wide {
        kind: BaseTable,
        placement: Placement::full_pk,
        a: U64,
    };
    let on_a: fn(&mut Circuit) -> NodeId = |c| rekeyed_join(c, &A, true, id, JoinKind::Equi);

    assert!(is_source(trace_of(Slot::SOLO, table, on_a)), "a PK prefix");
    assert!(
        !is_source(trace_of(Slot::SOLO, table, |c| {
            rekeyed_join(c, &[(1, U64)], true, id, JoinKind::Equi)
        })),
        "a key that is no PK prefix"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, table, |c| {
            rekeyed_join(c, &A, true, |c, n| c.negate(n), JoinKind::Equi)
        })),
        "an operator between the scan and the re-key"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, table, |c| {
            let rel = gnitz_wire::RangeRel::Lt;
            rekeyed_join(c, &A, true, id, JoinKind::Range { rel })
        })),
        "a join that walks a range of keys"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, Wide { a: U32, ..table }, on_a)),
        "a key packed wider than its column"
    );
    assert!(
        is_source(trace_of(Slot::SOLO, Wide { a: UUID, ..table }, |c| {
            rekeyed_join(c, &[(0, U128)], true, id, JoinKind::Equi)
        })),
        "a key packed at another type of the same bytes"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, Wide { kind: Stream, ..table }, on_a)),
        "a stream holds no rows"
    );

    // Beside other workers, the table's rows must also be placed by that key.
    let third = Slot::new(2, 4);
    let by_a = Wide {
        placement: |s| Placement::keyed(s, 1),
        ..table
    };
    assert!(is_source(trace_of(third, by_a, on_a)), "placed by the key");
    assert!(
        is_source(trace_of(
            third,
            Wide {
                placement: |_| Placement::Replicated,
                ..table
            },
            on_a
        )),
        "replicated"
    );
    assert!(!is_source(trace_of(third, table, on_a)), "placed by the whole PK");
    assert!(
        !is_source(trace_of(third, by_a, |c| rekeyed_join(
            c,
            &A,
            false,
            id,
            JoinKind::Equi
        ))),
        "a re-key that does not state where the delta scatters"
    );
}

/// Seeding a bounded view's replay reads the integral of side A's delta: the
/// child the other term's join declared, or the table's own store where that is
/// the integral.
#[test]
fn a_bounded_join_seeds_from_a_stored_integral_and_from_a_source_store() {
    use gnitz_store::relation::RelationKind::BaseTable;
    use gnitz_zset::schema::{Placement, Slot};
    let table = Wide {
        kind: BaseTable,
        placement: Placement::full_pk,
        a: gnitz_wire::TypeCode::U64,
    };
    let seed = |build: fn(&mut Circuit) -> NodeId| {
        let (registry, _dir) = two_tables(Slot::SOLO, table);
        let mut c = Circuit::default();
        build(&mut c);
        let loaded = loaded(c);
        let all = loaded.ordered_where(|_| true);
        let out = PlanOut::Node(loaded.sink().unwrap());
        let view = *build_plan(&loaded, &all, &registry, &mut StateLayout::default(), true, &[], out)
            .unwrap()
            .plan
            .vm
            .out_schema();
        let (out, _) = compile_view(&loaded, &registry, &view, Placement::full_pk(&view), true).unwrap();
        out.hydration.expect("a bounded view").seed
    };
    assert_eq!(
        seed(|c| rekeyed_join(c, &A, true, id, JoinKind::Equi)),
        Integral::Source(10)
    );
    assert!(matches!(
        seed(|c| rekeyed_join(c, &A, true, |c, n| c.negate(n), JoinKind::Equi)),
        Integral::Own(_)
    ));
}

/// A join's integrand must be a node of the join's own plan, also where its
/// integral would be a table's store and need no register.
#[test]
fn an_integrand_outside_the_plan_is_refused_even_as_a_source_store() {
    use gnitz_store::relation::RelationKind::BaseTable;
    use gnitz_zset::schema::{Placement, Slot};
    let table = Wide {
        kind: BaseTable,
        placement: Placement::full_pk,
        a: gnitz_wire::TypeCode::U64,
    };
    let (registry, _dir) = two_tables(Slot::SOLO, table);
    let mut c = Circuit::default();
    let ra = rekeyed_join(&mut c, &A, true, id, JoinKind::Equi);
    let loaded = loaded(c);
    // 11's side alone: its scan, its re-key, and the term probing 10's.
    let probe = loaded.readers(ra).find(|&n| loaded.inputs(n)[1] == ra).unwrap();
    let rb = loaded.inputs(probe)[0];
    let built = build_plan(
        &loaded,
        &[loaded.inputs(rb)[0], rb, probe],
        &registry,
        &mut StateLayout::default(),
        true,
        &[],
        PlanOut::Node(probe),
    );
    assert_eq!(
        rejection(built.map(|b| b.plan)),
        "operand is produced outside this plan"
    );
}

// ── One integral per integrand, one register per re-key ───────────────────

/// [`left_join_circuit`] of table 10 `(id | nn, nullable)` and table 11 `(id | k, w)`,
/// every column a U64, on column `key` of 10, compiled whole for one worker.
/// Answers 10's join re-key beside the plan.
fn left_join_plan(key: u32) -> (Built, StateLayout, NodeId) {
    use gnitz_wire::TypeCode::U64;
    use gnitz_zset::schema::SchemaColumn;
    let (nn, nullable) = (SchemaColumn::new(U64, false), SchemaColumn::new(U64, true));
    let a = SchemaDescriptor::new(&[nn, nn, nullable], &[0]);
    let b = SchemaDescriptor::new(&[nn, nn, nn], &[0]);
    let (c, ra) = crate::test_support::left_join_circuit(10, 11, key);
    let loaded = loaded(c);
    let mut layout = StateLayout::default();
    let built = build_plan(
        &loaded,
        &loaded.ordered_where(|_| true),
        &sources([(10, a), (11, b)]),
        &mut layout,
        true,
        &[],
        PlanOut::Node(loaded.sink().unwrap()),
    )
    .unwrap();
    (built, layout, ra)
}

/// A join names the node whose integral it probes, so the two joins reading the
/// preserved side's re-key — the inner term and the matched term — share one
/// store, and every child of the view has a name of its own.
#[test]
fn two_joins_probing_one_integrand_declare_one_child() {
    let (built, layout, ra) = left_join_plan(1);
    let names: Vec<&str> = layout.names().collect();
    assert_eq!(names.iter().filter(|n| **n == format!("int_{ra}")).count(), 1);
    assert_eq!(
        names.iter().filter(|n| n.starts_with("int_")).count(),
        3,
        "A's re-key, B's, and B's key set: {names:?}"
    );
    let mut distinct = names.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), names.len(), "{names:?}");
    assert_eq!(built.plan.vm.count_ops(|op| matches!(op, Op::JoinDT { .. })), 4);
}

/// The preserved side's two re-keys differ only in whether a NULL-keyed row
/// survives. Over a NOT NULL key none exists to drop, so they are one instruction;
/// over a nullable key they are two.
#[test]
fn a_rekey_that_drops_no_row_is_emitted_once() {
    // B's re-key, B's key projection, and A's.
    let maps = |key: u32| {
        let (built, ..) = left_join_plan(key);
        built.plan.vm.count_ops(|op| matches!(op, Op::Map(_)))
    };
    assert_eq!(maps(1), 3, "a NOT NULL key");
    assert_eq!(maps(2), 4, "a nullable key");
}

// ── A reduce or top-N whose output is the view's ──────────────────────────

/// The children declared by a plan over table 10 `(id | v)` that feeds `tip`'s
/// node to the sink and outputs what `out` makes of that sink.
fn children_of(tip: fn(&mut Circuit, NodeId) -> NodeId, out: fn(NodeId) -> PlanOut) -> Vec<String> {
    let mut c = Circuit::default();
    let a = scan(&mut c, 10);
    let tip = tip(&mut c, a);
    c.sink(tip);
    let loaded = loaded(c);
    let mut layout = StateLayout::default();
    build_plan(
        &loaded,
        &loaded.ordered_where(|_| true),
        &sources([(10, make_schema_u64_i64())]),
        &mut layout,
        true,
        &[],
        out(loaded.sink().unwrap()),
    )
    .unwrap();
    layout.names().map(str::to_string).collect()
}

/// A reduce or top-N the sink alone reads declares no output trace where the
/// view's store holds every row the sink is fed. A bounded view's does not, and
/// behind another operator the store holds other rows than the trace would.
#[test]
fn a_view_store_stands_in_for_the_output_trace_of_the_node_the_sink_reads() {
    use gnitz_wire::{AggDescriptor, OpNode, OrderKey};
    let reduce = |c: &mut Circuit, a| c.reduce_multi_local(a, &[1], &[AggDescriptor::COUNT_STAR]);
    // No exchange in front of it: the plan is one worker's.
    let top = |c: &mut Circuit, a| {
        let op = OpNode::TopN {
            group_cols: vec![1],
            order: vec![OrderKey { col: 0, desc: true, nulls_first: false }],
            limit: 3,
            offset: 0,
        };
        c.push(op, &[a]).unwrap()
    };
    let negated = |c: &mut Circuit, a| {
        let r = c.reduce_multi_local(a, &[1], &[AggDescriptor::COUNT_STAR]);
        c.negate(r)
    };
    assert_eq!(children_of(reduce, PlanOut::Store), [""; 0]);
    assert_eq!(children_of(reduce, PlanOut::Node), ["reduce_1"]);
    assert_eq!(children_of(top, PlanOut::Store), ["topnidx_1"]);
    assert_eq!(children_of(top, PlanOut::Node), ["topn_1", "topnidx_1"]);
    assert_eq!(children_of(negated, PlanOut::Store), ["reduce_1"]);
}
