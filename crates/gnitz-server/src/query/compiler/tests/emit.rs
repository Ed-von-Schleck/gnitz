use super::*;
use crate::query::compiler::fixtures::*;
use crate::query::vm::Vm;
use crate::test_support::make_schema_u64_i64;
use gnitz_wire::{Circuit, JoinKind};

/// Exchange-free `build` emitted whole, as one worker's plan over sources 10 and 11.
fn plan(build: Build) -> Result<Vm, String> {
    let mut c = Circuit::default();
    build(&mut c);
    let schema = make_schema_u64_i64();
    let registry = sources([(10, schema), (11, schema)]);
    let meta = routing(&c, &registry)?;
    EmitCtx::emit(&c, &registry, &meta, None).map(|ctx| ctx.prog.finish(ctx.regs[c.out()]))
}

/// A join's two terms each read the other side as it stood before the epoch, so
/// a relation feeding both deltas would lose their product in every epoch of
/// it — whether through two scans or through one delta computed from the other.
#[test]
fn a_join_whose_deltas_one_relation_feeds_is_rejected() {
    use crate::test_support::reindexed_on_col1;
    use gnitz_wire::TypeCode::I64;
    let twice = plan(|c| {
        let deltas = [10, 10].map(|s| reindexed_on_col1(c, s, I64));
        c.join(deltas, deltas, JoinKind::Equi);
    });
    let derived = plan(|c| {
        let da = reindexed_on_col1(c, 10, I64);
        let _ = reindexed_on_col1(c, 11, I64);
        let dm = c.negate(da);
        c.join([da, dm], [da, dm], JoinKind::Equi);
    });
    for built in [twice, derived] {
        assert_eq!(rejection(built), "join: one relation feeds both deltas");
    }
    assert!(plan(|c| {
        let deltas = [10, 11].map(|s| reindexed_on_col1(c, s, I64));
        c.join(deltas, deltas, JoinKind::Equi);
    })
    .is_ok());
}

/// A `Filter` program that does not decode aborts the compile: passing every row
/// instead would turn the WHERE into WHERE TRUE.
#[test]
fn a_corrupt_filter_program_aborts_the_compile() {
    let garbled = plan(|c| {
        let a = scan(c, 10);
        c.filter(a, vec![0xFF; 16]);
    });
    let empty = plan(|c| {
        let a = scan(c, 10);
        c.filter(a, Vec::new());
    });
    for built in [garbled, empty] {
        assert!(rejection(built).starts_with("filter: invalid predicate program"));
    }
}

// ── An integral that is its source's own store ────────────────────────────

/// What varies of relation 10 `(a, b | v)`, keyed `(a, b)`, and of the table it
/// is joined with.
#[derive(Clone, Copy)]
struct Wide {
    kind: gnitz_store::relation::RelationKind,
    placement: fn(&SchemaDescriptor) -> gnitz_zset::algebra::Placement,
    a: gnitz_wire::TypeCode,
    /// Table 11 is replicated, where it is otherwise placed by its PK.
    replicated_partner: bool,
}

/// Relation 10 as `wide` has it and table 11 `(k | w)`, registered for worker
/// `slot` under a fresh directory.
fn two_tables(slot: gnitz_zset::algebra::Slot, wide: Wide) -> (RelationRegistry, tempfile::TempDir) {
    use gnitz_store::relation::RelationKind;
    use gnitz_zset::algebra::Placement;
    use gnitz_zset::schema::SchemaColumn;
    let col = SchemaColumn::new(gnitz_wire::TypeCode::U64, false);
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(wide.a, false), col, col], &[0, 1]);
    let partner = Source::from(make_schema_u64_i64());
    let partner = match wide.replicated_partner {
        true => partner.placed(Placement::Replicated),
        false => partner,
    };
    tables_at(
        slot,
        [
            (10, wide.kind, Source::from(schema).placed((wide.placement)(&schema))),
            (11, RelationKind::BaseTable, partner),
        ],
    )
}

/// The integral of the node `build` answers and every child declared, as its
/// circuit compiled on worker `slot` under the routing derived from it.
fn trace_and_children(
    slot: gnitz_zset::algebra::Slot,
    wide: Wide,
    build: fn(&mut Circuit) -> NodeId,
) -> (Integral, Vec<String>) {
    let (registry, _dir) = two_tables(slot, wide);
    let mut c = Circuit::default();
    let integrand = build(&mut c);
    let meta = routing(&c, &registry).unwrap();
    let ctx = EmitCtx::emit(&c, &registry, &meta, None).unwrap();
    (
        ctx.integrals[integrand].expect("a join probes the integrand"),
        ctx.layout.names().map(str::to_string).collect(),
    )
}

fn trace_of(slot: gnitz_zset::algebra::Slot, wide: Wide, build: fn(&mut Circuit) -> NodeId) -> Integral {
    trace_and_children(slot, wide, build).0
}

/// `10 ⋈ 11` on `10.a = 11.k`: 10 re-keyed onto `key`, `through` in between,
/// each side stating its key as its route; answers 10's re-key, the node 11's
/// delta probes the integral of.
fn rekeyed_join(
    c: &mut Circuit,
    key: &[gnitz_wire::ReindexSlot],
    through: fn(&mut Circuit, NodeId) -> NodeId,
    kind: JoinKind,
) -> NodeId {
    use gnitz_wire::{NullKeys, ReindexRole};
    let role = |key: &[gnitz_wire::ReindexSlot]| ReindexRole::ScatterKey {
        source_cols: key.iter().map(|slot| slot.0).collect(),
    };
    let (a, b) = (scan(c, 10), scan(c, 11));
    let a = through(c, a);
    let ra = c.map_reindex(a, key, &[1, 2], role(key), NullKeys::Drop);
    // Both sides pack at the one type the pair shares.
    let b_key = [(0, key[0].1)];
    let rb = c.map_reindex(b, &b_key, &[1], role(&b_key), NullKeys::Drop);
    c.join([ra, rb], [ra, rb], kind);
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
    use gnitz_zset::algebra::{Placement, Slot};
    let is_source = |t: Integral| t == Integral::Relation(10, gnitz_store::relation::Cut::Sealed);
    let table = Wide {
        kind: BaseTable,
        placement: Placement::full_pk,
        a: U64,
        replicated_partner: false,
    };
    let on_a: fn(&mut Circuit) -> NodeId = |c| rekeyed_join(c, &A, id, JoinKind::Equi);

    assert!(is_source(trace_of(Slot::SOLO, table, on_a)), "a PK prefix");
    assert!(
        !is_source(trace_of(Slot::SOLO, table, |c| {
            rekeyed_join(c, &[(1, U64)], id, JoinKind::Equi)
        })),
        "a key that is no PK prefix"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, table, |c| {
            rekeyed_join(c, &A, |c, n| c.distinct(n), JoinKind::Equi)
        })),
        "an operator between the scan and the re-key"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, table, |c| {
            let rel = gnitz_wire::RangeRel::Lt;
            rekeyed_join(c, &A, id, JoinKind::Range { rel })
        })),
        "a join that walks a range of keys"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, Wide { a: U32, ..table }, on_a)),
        "a key packed wider than its column"
    );
    assert!(
        is_source(trace_of(Slot::SOLO, Wide { a: UUID, ..table }, |c| {
            rekeyed_join(c, &[(0, U128)], id, JoinKind::Equi)
        })),
        "a key packed at another type of the same bytes"
    );
    assert!(
        !is_source(trace_of(Slot::SOLO, Wide { kind: Stream, ..table }, on_a)),
        "a stream holds no rows"
    );

    // Beside other workers, the table's delta must also stay where the table
    // holds it.
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
    assert!(
        !is_source(trace_of(third, table, on_a)),
        "placed by the whole PK, relayed by the key"
    );
    // A replicated partner makes every match where the row already is, so the
    // delta takes no relay.
    let (trace, children) = trace_and_children(third, Wide { replicated_partner: true, ..table }, on_a);
    assert!(is_source(trace), "placed by the whole PK beside a replicated partner");
    assert!(!children.iter().any(|name| name.starts_with("int_")), "{children:?}");
}

/// Seeding a bounded view's replay reads the integral of side A's delta: the
/// child the other term's join declared, or the table's own store where that is
/// the integral.
#[test]
fn a_bounded_join_seeds_from_a_stored_integral_and_from_a_source_store() {
    use gnitz_store::relation::RelationKind::BaseTable;
    use gnitz_zset::algebra::{Placement, Slot};
    let table = Wide {
        kind: BaseTable,
        placement: Placement::full_pk,
        a: gnitz_wire::TypeCode::U64,
        replicated_partner: false,
    };
    let seed = |build: fn(&mut Circuit) -> NodeId| {
        let (registry, _dir) = two_tables(Slot::SOLO, table);
        let mut c = Circuit::default();
        build(&mut c);
        let meta = routing(&c, &registry).unwrap();
        let ctx = EmitCtx::emit(&c, &registry, &meta, None).unwrap();
        let view = ctx.prog.schema_of(ctx.regs[c.out()]);
        let out = compile(c, &registry, &view, true).unwrap();
        out.hydration.expect("a bounded view").seed
    };
    assert_eq!(
        seed(|c| rekeyed_join(c, &A, id, JoinKind::Equi)),
        Integral::Relation(10, gnitz_store::relation::Cut::Sealed)
    );
    assert!(matches!(
        seed(|c| rekeyed_join(c, &A, |c, n| c.distinct(n), JoinKind::Equi)),
        Integral::Own(_)
    ));
}

// ── One integral per integrand ────────────────────────────────────────────

/// [`left_join_circuit`] of table 10 `(id | nn, nullable)` and table 11 `(id | k, w)`,
/// every column a U64, on column `key` of 10, compiled whole for one worker.
/// Answers 10's join re-key beside the plan.
fn left_join_plan(key: u32) -> (Vm, Vec<String>, NodeId) {
    use gnitz_wire::TypeCode::U64;
    use gnitz_zset::schema::SchemaColumn;
    let (nn, nullable) = (SchemaColumn::new(U64, false), SchemaColumn::new(U64, true));
    let a = SchemaDescriptor::new(&[nn, nn, nullable], &[0]);
    let b = SchemaDescriptor::new(&[nn, nn, nn], &[0]);
    let (c, ra) = crate::test_support::left_join_circuit(10, 11, key);
    let registry = sources([(10, a), (11, b)]);
    let meta = routing(&c, &registry).unwrap();
    let ctx = EmitCtx::emit(&c, &registry, &meta, None).unwrap();
    let names = ctx.layout.names().map(str::to_string).collect();
    (ctx.prog.finish(ctx.regs[c.out()]), names, ra)
}

/// A join names the node whose integral it probes, so the two joins reading the
/// preserved side's re-key — the inner term and the matched term — share one
/// store, and every child of the view has a name of its own.
#[test]
fn two_joins_probing_one_integrand_declare_one_child() {
    let (vm, names, ra) = left_join_plan(1);
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
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
    assert_eq!(vm.ops().filter(|op| matches!(op, Op::JoinDT { .. })).count(), 4);
}

// ── A reduce whose output is the view's ───────────────────────────────────

/// The children declared by a plan over table 10 `(id | v)` whose last node is
/// `tip`'s, output as `out` makes of it.
fn children_of(tip: fn(&mut Circuit, NodeId) -> NodeId, store: bool) -> Vec<String> {
    let mut c = Circuit::default();
    let a = scan(&mut c, 10);
    tip(&mut c, a);
    let registry = sources([(10, make_schema_u64_i64())]);
    let meta = routing(&c, &registry).unwrap();
    let ctx = EmitCtx::emit(&c, &registry, &meta, store.then_some(VIEW)).unwrap();
    ctx.layout.names().map(str::to_string).collect()
}

/// A reduce that is the view's output declares no output trace where the view's
/// store holds every row it emits. A bounded view's does not, and behind another
/// operator the store holds other rows than the trace would.
#[test]
fn a_view_store_stands_in_for_the_output_trace_of_the_output_node() {
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
    assert_eq!(children_of(reduce, true), [""; 0]);
    assert_eq!(children_of(reduce, false), ["reduce_1"]);
    assert_eq!(children_of(top, true), ["topnidx_1"]);
    assert_eq!(children_of(top, false), ["topnidx_1"]);
    assert_eq!(children_of(negated, true), ["reduce_1"]);
}

// ── A routed source's relay ───────────────────────────────────────────────

/// The relays of a plan over sources 7 and 9, beside three other workers:
/// `(rounds, shares)`.
fn relays(sources: [Source; 2], build: Build) -> (usize, usize) {
    use gnitz_zset::algebra::Slot;
    let mut c = Circuit::default();
    build(&mut c);
    let registry = sources_at(Slot::new(0, 4), [(7, sources[0]), (9, sources[1])]);
    let meta = routing(&c, &registry).unwrap();
    let vm = EmitCtx::emit(&c, &registry, &meta, None)
        .map(|ctx| ctx.prog.finish(ctx.regs[c.out()]))
        .unwrap();
    let rounds = vm.ops().filter(|op| matches!(op, Op::Round { .. })).count();
    let shares = vm.ops().filter(|op| matches!(op, Op::Share(_))).count();
    (rounds, shares)
}

/// A routed source's delta is relayed by a round, and kept by share where the
/// source is replicated: every worker already holds that delta whole.
#[test]
fn a_routed_source_is_shared_where_replicated_and_relayed_by_a_round_otherwise() {
    use crate::test_support::{scan_keyed, self_typed_slots};
    use gnitz_zset::algebra::Placement;
    let keyed = Source::from(make_schema_u64_i64());
    let replicated = keyed.placed(Placement::Replicated);
    // `7 ⋈ 9` on a payload column, 7's re-key also read beside the join: read
    // there a replicated delta counts once per worker, so neither source skips.
    let build: Build = |c| {
        let key = self_typed_slots(&make_schema_u64_i64(), &[1]);
        let (a, b) = (scan_keyed(c, 7, &key), scan_keyed(c, 9, &key));
        c.negate(a);
        c.join([a, b], [a, b], JoinKind::Equi);
    };
    assert_eq!(relays([replicated, keyed], build), (1, 1));
    assert_eq!(relays([keyed, keyed], build), (2, 0));
}

/// The output exchange moves nothing behind a scan placed by the shard key — but
/// not behind one a join's relay has moved off its source's owner: each source's
/// relay runs, and so does the exchange.
#[test]
fn the_output_exchange_runs_behind_a_relayed_scan() {
    use crate::test_support::{scan_keyed, self_typed_slots};
    use gnitz_wire::AggDescriptor;
    let keyed = Source::from(make_schema_u64_i64());
    let build: Build = |c| {
        let key = self_typed_slots(&make_schema_u64_i64(), &[1]);
        let deltas = [7, 9].map(|source| scan_keyed(c, source, &key));
        c.join(deltas, deltas, JoinKind::Equi);
        // `scan_keyed` pushed 7's scan first.
        c.reduce_multi(0, &[0], &[AggDescriptor::COUNT_STAR]);
    };
    assert_eq!(relays([keyed, keyed], build), (3, 0));
}

// ── A clamp whose history is its sources' stores ──────────────────────────

/// One table of a set operation: its kind, and its placement from its schema.
type Side = (
    gnitz_store::relation::RelationKind,
    fn(&SchemaDescriptor) -> gnitz_zset::algebra::Placement,
);

/// The children `build`'s circuit declares, compiled on worker `slot` over
/// tables 10 and 11, each `(a, b | v)` keyed `(a, b)`, of the kind and placement
/// `sides` gives.
fn set_op_children(slot: gnitz_zset::algebra::Slot, sides: [Side; 2], build: Build) -> Vec<String> {
    use gnitz_zset::schema::SchemaColumn;
    let col = SchemaColumn::new(gnitz_wire::TypeCode::U64, false);
    let schema = SchemaDescriptor::new(&[col, col, col], &[0, 1]);
    let tables = [10, 11]
        .into_iter()
        .zip(sides)
        .map(|(id, (kind, placement))| (id, kind, Source::from(schema).placed(placement(&schema))));
    let (registry, _dir) = tables_at(slot, tables);
    let mut c = Circuit::default();
    build(&mut c);
    let meta = routing(&c, &registry).unwrap();
    let ctx = EmitCtx::emit(&c, &registry, &meta, None).unwrap();
    ctx.layout.names().map(str::to_string).collect()
}

/// `10 EXCEPT ALL 11` over leaves keyed by their own PK, each behind an exchange
/// on it.
fn keyed_except(c: &mut Circuit) {
    let [a, b] = [10, 11].map(|s| {
        let leaf = scan(c, s);
        c.shard(leaf)
    });
    c.positive_diff(a, b);
}

/// A clamp declares no history of its own exactly where each term of its input
/// is a base table's rows on the worker that stores them.
#[test]
fn a_clamp_reads_its_sources_where_they_hold_its_history() {
    use gnitz_store::relation::RelationKind::{BaseTable, Stream};
    use gnitz_zset::algebra::{Placement, Slot};
    let reads_sources = |children: &[String]| !children.iter().any(|n| n.starts_with("hist_"));
    let table: Side = (BaseTable, Placement::full_pk);
    let third = Slot::new(2, 4);

    assert!(
        reads_sources(&set_op_children(Slot::SOLO, [table; 2], keyed_except)),
        "one worker"
    );
    assert!(
        reads_sources(&set_op_children(third, [table; 2], keyed_except)),
        "an exchange that moves nothing"
    );
    let replicated: Side = (BaseTable, |_| Placement::Replicated);
    assert!(
        reads_sources(&set_op_children(third, [replicated, table], keyed_except)),
        "a replicated side's share"
    );
    assert!(
        reads_sources(&set_op_children(third, [table; 2], |c| {
            let a = scan(c, 10);
            let a = c.filter(
                a,
                crate::test_support::cmp_const(gnitz_expr::CmpOp::Lt, 2, 100).to_blob_bytes(),
            );
            let a = c.map(a, &[2]);
            let b = scan(c, 11);
            c.positive_diff(a, b);
        })),
        "a filter and a projection on the way"
    );

    let by_prefix: Side = (BaseTable, |s| Placement::keyed(s, 1));
    assert!(
        !reads_sources(&set_op_children(third, [by_prefix, table], keyed_except)),
        "an exchange that moves a side placed by its PK prefix"
    );
    let stream: Side = (Stream, Placement::full_pk);
    assert!(
        !reads_sources(&set_op_children(Slot::SOLO, [stream, table], keyed_except)),
        "a stream holds no rows"
    );
    assert!(
        !reads_sources(&set_op_children(Slot::SOLO, [table; 2], |c| {
            let [a, b] = [10, 11].map(|s| {
                let leaf = scan(c, s);
                c.map_hash_row(leaf, &[(0, gnitz_wire::TypeCode::U64), (2, gnitz_wire::TypeCode::U64)])
            });
            c.positive_diff(a, b);
        })),
        "a re-key onto a content hash"
    );
    // The outer clamp of a chain reads the inner one's output, which no store holds.
    let chain = set_op_children(Slot::SOLO, [table; 2], |c| {
        let [a, b] = [10, 11].map(|s| scan(c, s));
        let inner = c.positive_diff(a, b);
        c.distinct(inner);
    });
    assert_eq!(chain.iter().filter(|n| n.starts_with("hist_")).count(), 1, "{chain:?}");
}
