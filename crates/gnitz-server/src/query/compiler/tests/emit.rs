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

const TAKES_A_DELTA: &str = "operand port takes a delta, not an integral";

/// The load holds a node to its arity, not to its producers' kinds: an integral
/// names a store and a delta a batch, so every port refuses the other.
#[test]
fn a_port_refuses_the_other_kind_of_operand() {
    let cases: [(&str, Build, &str); 6] = [
        (
            "a unary delta operator over an integral",
            |c: &mut Circuit| {
                let t = integral(c, 10);
                let n = c.negate(t);
                c.sink(n);
            },
            TAKES_A_DELTA,
        ),
        (
            "a union's left operand",
            |c: &mut Circuit| {
                let (t, b) = (integral(c, 10), scan(c, 11));
                let u = c.union(t, b);
                c.sink(u);
            },
            TAKES_A_DELTA,
        ),
        (
            "a union's right operand is a delta port, not a trace one",
            |c: &mut Circuit| {
                let (a, t) = (scan(c, 10), integral(c, 11));
                let u = c.union(a, t);
                c.sink(u);
            },
            TAKES_A_DELTA,
        ),
        (
            "a join's delta port",
            |c: &mut Circuit| {
                let (ta, tb) = (integral(c, 10), integral(c, 11));
                let j = c.join(ta, tb, JoinKind::Equi, false);
                c.sink(j);
            },
            TAKES_A_DELTA,
        ),
        (
            "the sink",
            |c: &mut Circuit| {
                let t = integral(c, 10);
                c.sink(t);
            },
            TAKES_A_DELTA,
        ),
        (
            "a join's trace port",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 10), scan(c, 11));
                let j = c.join(a, b, JoinKind::Equi, false);
                c.sink(j);
            },
            "operand port takes an integral, not a delta",
        ),
    ];
    for (why, build, guard) in cases {
        assert_eq!(rejection(plan(build)), guard, "{why}");
    }
    plan(|c| {
        let (a, tb) = (scan(c, 10), integral(c, 11));
        let j = c.join(a, tb, JoinKind::Equi, false);
        c.sink(j);
    })
    .expect("a delta probing an integral");
}

fn integral(c: &mut Circuit, source: u64) -> NodeId {
    let s = scan(c, source);
    c.integrate_trace(s)
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

// ── A trace that is its source's own store ────────────────────────────────

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
        registry.register(RelationSpec { id, kind, schema, placement }).unwrap();
    }
    (registry, dir)
}

/// What node `trace` of `build`'s circuit compiled to on worker `slot`.
fn trace_of(slot: gnitz_zset::schema::Slot, wide: Wide, build: fn(&mut Circuit) -> NodeId) -> Integral {
    let (registry, _dir) = two_tables(slot, wide);
    let mut c = Circuit::default();
    let trace = build(&mut c);
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
    built.regs[trace].unwrap().trace().unwrap()
}

/// `10 ⋈ 11` on `10.a = 11.k`: 10 re-keyed onto `key`, `through` in between;
/// answers 10's trace node.
fn rekeyed_join(
    c: &mut Circuit,
    key: &[gnitz_wire::ReindexSlot],
    scatters: bool,
    through: fn(&mut Circuit, NodeId) -> NodeId,
    kind: JoinKind,
) -> NodeId {
    use gnitz_wire::{NullKeys, ReindexRole};
    let role = |source: u64, key: &[gnitz_wire::ReindexSlot]| match scatters {
        true => ReindexRole::ScatterKey { source, source_key: key.to_vec() },
        false => ReindexRole::Auxiliary,
    };
    let (a, b) = (scan(c, 10), scan(c, 11));
    let a = through(c, a);
    let ra = c.map_reindex(a, key, &[1, 2], role(10, key), NullKeys::Drop);
    // Both sides pack at the one type the pair shares.
    let b_key = [(0, key[0].1)];
    let rb = c.map_reindex(b, &b_key, &[1], role(11, &b_key), NullKeys::Drop);
    let (ta, tb) = (c.integrate_trace(ra), c.integrate_trace(rb));
    let j = c.join_terms([ra, rb], [ta, tb], kind);
    c.sink(j);
    ta
}

const A: [gnitz_wire::ReindexSlot; 1] = [(0, gnitz_wire::TypeCode::U64)];

/// Nothing between the scan and the re-key.
fn id(_: &mut Circuit, n: NodeId) -> NodeId {
    n
}

/// A trace is read off its source table exactly when the table's store holds,
/// on this worker and in key order, every row the trace would: the re-key is
/// onto leading PK columns at their own widths, nothing sits between the scan
/// and it, the source is a table, and every reader probes equal keys.
#[test]
fn a_trace_is_its_source_table_only_where_the_table_holds_what_it_would() {
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
            rekeyed_join(c, &A, true, id, JoinKind::Range { n_eq: 0, rel })
        })),
        "a reader that walks a range of keys"
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
