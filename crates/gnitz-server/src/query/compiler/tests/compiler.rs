use super::fixtures::*;
use super::*;
use crate::test_support::{
    identity_circuit, make_batch, make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_only_schema, u64_pk_schema,
};
use gnitz_expr::SchemaFacts;
use gnitz_wire::{AggDescriptor, AggFunc, Circuit, OpNode, ReadBound, TypeCode};
use gnitz_zset::schema::{Placement, SchemaColumn, Slot};

// ── The shapes a compile refuses ────────────────────────────────────────

/// No planner path emits any of these; a circuit hand-built through `Circuit`
/// can, so each is refused at compile by the guard named.
#[test]
fn a_circuit_no_plan_can_be_carved_from_is_rejected() {
    let cases: [(Build, &str); 8] = [
        // A side relays under its source, routed by the view's one shard key, so
        // two sides keyed differently would bounds-check one by the other's key.
        (
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 10), scan(c, 11));
                let (sa, sb) = (c.shard(a, &[0]), c.shard(b, &[1]));
                let u = c.union(sa, sb);
                c.sink(u);
            },
            "exchange sides shard on different keys",
        ),
        // A node in two sides would open one scratch child twice, under two
        // unsynchronized shard indexes.
        (
            |c: &mut Circuit| {
                let a = scan(c, 10);
                let (s1, s2) = (c.shard(a, &[0]), c.shard(a, &[0]));
                let u = c.union(s1, s2);
                c.sink(u);
            },
            "exchange sides share a node",
        ),
        // A shard upstream of another lies in both ancestor sets.
        (
            |c: &mut Circuit| {
                let a = scan(c, 10);
                let s1 = c.shard(a, &[0]);
                let n = c.negate(s1);
                let s2 = c.shard(n, &[0]);
                c.sink(s2);
            },
            "exchange sides share a node",
        ),
        // A delta is routed to the side scanning its source, so a scan in the post
        // phase would receive nothing.
        (
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 10), scan(c, 11));
                let s = c.shard(a, &[0]);
                let u = c.union(s, b);
                c.sink(u);
            },
            "an exchanged plan scans a relation outside every exchange side",
        ),
        // A post-phase node reading into a side, past its shard: the side's
        // registers are another plan's.
        (
            |c: &mut Circuit| {
                let a = scan(c, 10);
                let n = c.negate(a);
                let s = c.shard(n, &[0]);
                let u = c.union(s, n);
                c.sink(u);
            },
            "operand is produced outside this plan",
        ),
        // So is the delta a post-phase join would integrate.
        (
            |c: &mut Circuit| {
                let a = scan(c, 10);
                let n = c.negate(a);
                let s = c.shard(n, &[0]);
                let j = c.join(s, n, gnitz_wire::JoinKind::Equi, false);
                c.sink(j);
            },
            "operand is produced outside this plan",
        ),
        (
            |c: &mut Circuit| {
                scan(c, 10);
            },
            "circuit has no IntegrateSink",
        ),
        (
            |c: &mut Circuit| {
                let a = scan(c, 10);
                c.sink(a);
                c.sink(a);
            },
            "circuit has more than one IntegrateSink",
        ),
    ];
    let schema = make_schema_u64_i64();
    for (build, guard) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let compiled = compile_view(
            &loaded(c),
            &sources([(10, schema), (11, schema)]),
            &schema,
            Placement::full_pk(&schema),
            false,
        );
        assert_eq!(rejection(compiled), guard);
    }
}

/// The sink must match the view schema's physical layout, not just its width.
#[test]
fn a_sink_schema_unequal_to_the_view_schema_is_rejected() {
    let compile = |source: SchemaDescriptor, view: SchemaDescriptor| {
        let circuit = loaded(identity_circuit(10, ReadBound::None));
        compile_view(
            &circuit,
            &sources([(10, source)]),
            &view,
            Placement::full_pk(&view),
            false,
        )
    };
    let pk_only = pk_only_schema(&[TypeCode::U64]);
    assert!(compile(pk_only, pk_only).is_ok(), "an equal pair compiles");
    for (source, view, why) in [
        (make_schema_u64_i64(), pk_only, "a wider sink"),
        (
            make_schema_u64_i64(),
            make_schema_pk_u64_payload_string(),
            "equal widths, another column type",
        ),
    ] {
        assert_eq!(
            rejection(compile(source, view)),
            "sink schema does not match view output schema",
            "{why}"
        );
    }
}

/// A shard column the relay's group key would refuse is a `CREATE VIEW`
/// rejection, even on one worker, where nothing relays.
#[test]
fn a_float_shard_column_is_rejected() {
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::F64, false));
    let compile = |shard_col: u32| {
        let mut c = Circuit::default();
        let a = scan(&mut c, 10);
        let s = c.shard(a, &[shard_col]);
        c.sink(s);
        compile_view(
            &loaded(c),
            &sources([(10, schema)]),
            &schema,
            Placement::full_pk(&schema),
            false,
        )
    };
    assert!(compile(0).is_ok(), "an integer shard column compiles");
    assert_eq!(
        rejection(compile(1)),
        "group key: column 1 is a float, which has no order-preserving key image",
    );
}

// ── A side's relay ──────────────────────────────────────────────────────

/// A side's output stays where its rows already are, is kept by share where
/// every worker computed the same rows, and takes a round otherwise.
#[test]
fn a_side_relays_only_what_is_not_already_in_place() {
    let view = make_schema_u64_i64();
    let keyed = Source::from(view);
    let replicated = keyed.placed(Placement::Replicated);
    // One `ScanDelta → [mid] → ExchangeShard(cols)` side per source, unioned into
    // the sink of a partitioned view; the relays in circuit order.
    let relays = |of: u32, sides: &[(Source, Option<OpNode>, &[u32])]| {
        let mut c = Circuit::default();
        let shards: Vec<NodeId> = (10..)
            .zip(sides)
            .map(|(source, (_, mid, cols))| {
                let mut tip = scan(&mut c, source);
                if let Some(op) = mid {
                    tip = c.push(op.clone(), &[tip]).unwrap();
                }
                c.shard(tip, cols)
            })
            .collect();
        let out = shards.into_iter().reduce(|a, b| c.union(a, b)).unwrap();
        c.sink(out);
        let registry = sources_at(Slot::new(0, of), (10..).zip(sides.iter().map(|s| s.0)));
        let (out, _) =
            compile_view(&loaded(c), &registry, &view, Placement::full_pk(&view), false).expect("the fixture compiles");
        out.sides.iter().map(|s| route(s.relay.as_ref())).collect::<Vec<_>>()
    };
    let negate = || Some(OpNode::Negate);
    assert_eq!(
        relays(4, &[(keyed, None, &[0])]),
        [Route::Stays],
        "a shard on the key its scan's rows are placed by"
    );
    assert_eq!(relays(4, &[(keyed, negate(), &[1])]), [Route::Round]);
    assert_eq!(
        relays(4, &[(replicated, negate(), &[1])]),
        [Route::Share],
        "every worker computed the same rows"
    );
    assert_eq!(
        relays(4, &[(replicated, Some(OpNode::WorkerFilter), &[1])]),
        [Route::Round],
        "a trimmed side emits a slice of the replica, not the replica"
    );
    assert_eq!(
        relays(4, &[(replicated, negate(), &[1]), (keyed, negate(), &[1])]),
        [Route::Share, Route::Round],
        "each side by its own sources"
    );
    assert_eq!(relays(1, &[(keyed, negate(), &[1])]), [Route::Stays], "one worker");
}

// ── Global operators ────────────────────────────────────────────────────

fn global_reduce(op: AggFunc) -> OpNode {
    OpNode::Reduce {
        group_cols: vec![],
        agg: vec![AggDescriptor { agg_op: op, col_idx: 1 }, AggDescriptor::COUNT_STAR],
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

/// `ScanDelta(10) → [ExchangeShard(shard)] → op → IntegrateSink`.
fn global_circuit(op: &OpNode, shard: Option<&[u32]>) -> Circuit {
    let mut c = Circuit::default();
    let mut tip = scan(&mut c, 10);
    if let Some(cols) = shard {
        tip = c.shard(tip, cols);
    }
    let out = c.push(op.clone(), &[tip]).unwrap();
    c.sink(out);
    c
}

/// Compile a [`global_circuit`] over `source` as worker `slot`, into a view placed
/// as `source` is.
fn compile_global(circuit: Circuit, source: Source, slot: Slot) -> Result<CompileOutput, String> {
    let Source { schema, placement } = source;
    let view = circuit
        .nodes()
        .iter()
        .find_map(|n| match &n.op {
            OpNode::Reduce { agg, .. } => Some(
                *gnitz_zset::stream::ReducePlan::from_wire(&schema, &[], agg, true)
                    .unwrap()
                    .output_schema(),
            ),
            OpNode::TopN { order, limit, offset, .. } => Some(
                gnitz_zset::stream::TopNPlan::from_wire(&schema, &[], order, *limit, *offset)
                    .unwrap()
                    .output_schema,
            ),
            _ => None,
        })
        .expect("a global operator");
    let view_placement = match placement {
        Placement::Keyed { .. } => Placement::full_pk(&view),
        p => p,
    };
    compile_view(
        &loaded(circuit),
        &sources_at(slot, [(10, source)]),
        &view,
        view_placement,
        false,
    )
    .map(|(out, _)| out)
}

/// A side ends in its global operator's partial — a layout of its own — exactly
/// where partials combine and no worker holds the whole input. Otherwise it relays
/// its input as is.
#[test]
fn a_global_operator_splits_only_where_partials_combine_and_workers_differ() {
    let keyed = Source::from(make_schema_u64_i64());
    let replicated = keyed.placed(Placement::Replicated);
    let split = |circuit: Circuit, source: Source, of: u32| {
        let out = compile_global(circuit, source, Slot::new(0, of)).expect("the fixture compiles");
        !out.sides[0].plan.vm.out_schema().same_layout(&source.schema)
    };
    for (why, op, source, of, want) in [
        ("a partitioned SUM", global_reduce(AggFunc::Sum), keyed, 4, true),
        ("a partitioned top-N", global_topn(), keyed, 4, true),
        ("one worker", global_reduce(AggFunc::Sum), keyed, 1, false),
        ("one worker", global_topn(), keyed, 1, false),
        ("a replicated source", global_reduce(AggFunc::Sum), replicated, 4, false),
        ("a replicated source", global_topn(), replicated, 4, false),
        (
            "a MIN's partials do not combine",
            global_reduce(AggFunc::Min),
            keyed,
            4,
            false,
        ),
    ] {
        assert_eq!(split(global_circuit(&op, Some(&[])), source, of), want, "{why}");
    }

    // A second reader of the shard would be handed the partials too.
    let mut shared = global_circuit(&global_reduce(AggFunc::Sum), Some(&[]));
    let shard = shared
        .nodes()
        .iter()
        .position(|n| matches!(n.op, OpNode::ExchangeShard { .. }))
        .unwrap();
    shared.negate(shard);
    assert!(!split(shared, keyed, 4), "a shard read twice");
}

const OFF_GROUP_EXCHANGE: &str = "an exchange in front of a reduce or top-N shards on other than its group columns";

/// The view behind an exchange registers under the key its reduce or top-N stamps
/// over the group columns, so an exchange on anything else places rows where no
/// read looks for them.
#[test]
fn an_exchange_off_the_group_columns_is_rejected() {
    let source = Source::from(make_schema_u64_i64());
    let grouped = |group_cols: Vec<u32>| OpNode::Reduce {
        group_cols,
        agg: vec![AggDescriptor::COUNT_STAR],
        global_ground: false,
    };
    let top = |group_cols: Vec<u32>| OpNode::TopN {
        group_cols,
        order: vec![gnitz_wire::OrderKey { col: 1, desc: false, nulls_first: false }],
        limit: 10,
        offset: 5,
    };
    let compile = |op: OpNode, shard: &[u32]| {
        let loaded = loaded(global_circuit(&op, Some(shard)));
        let carve = loaded.carve().unwrap();
        build_plan(
            &loaded,
            &carve.post,
            &sources([(10, source)]),
            &mut StateLayout::default(),
            true,
            &[Seed {
                shard: carve.sides[0].shard,
                schema: source.schema,
                partials: false,
            }],
            PlanOut::Node(loaded.sink().unwrap()),
        )
        .map(|built| built.plan)
    };
    for (op, shard) in [
        (grouped(vec![1]), &[0][..]),
        (grouped(vec![0, 1]), &[0]),
        (grouped(vec![]), &[1]),
        (top(vec![1]), &[0]),
        (top(vec![]), &[1]),
    ] {
        assert_eq!(rejection(compile(op.clone(), shard)), OFF_GROUP_EXCHANGE, "{op:?}");
    }
    for (op, shard) in [(grouped(vec![1]), &[1][..]), (top(vec![1]), &[1])] {
        assert!(compile(op.clone(), shard).is_ok(), "{op:?}");
    }
}

/// A global aggregate's ground row is seeded once per copy of the result: on each
/// worker holding the whole input, else on the one worker the empty-keyed shard
/// sends every row to.
#[test]
fn the_ground_row_is_seeded_where_the_whole_input_arrives() {
    let keyed = Source::from(make_schema_u64_i64());
    let op = global_reduce(AggFunc::Min);
    let seeds = |shard: Option<&[u32]>, source: Source, slot: Slot| {
        compile_global(global_circuit(&op, shard), source, slot).map(|out| out.post.vm.pending_ground_row)
    };
    assert_eq!(seeds(None, keyed, Slot::SOLO), Ok(true), "one worker");
    assert_eq!(
        seeds(None, keyed.placed(Placement::Replicated), Slot::new(2, 4)),
        Ok(true),
        "a replicated view"
    );
    assert_eq!(
        rejection(seeds(None, keyed, Slot::new(0, 4))),
        "reduce: a global aggregate over a partitioned input with no exchange"
    );
    // The row's owner is elected from the empty group key, so a shard keyed on
    // anything else would route the input to one worker and elect another.
    assert_eq!(rejection(seeds(Some(&[1]), keyed, Slot::SOLO)), OFF_GROUP_EXCHANGE);

    let probe = make_batch(&keyed.schema, &[(1, 1, 1)]);
    let (seeded, receives): (Vec<bool>, Vec<bool>) = (0..4)
        .map(|rank| {
            let slot = Slot::new(rank, 4);
            let out = compile_global(global_circuit(&op, Some(&[])), keyed, slot).expect("the fixture compiles");
            let Some(Relay::Round(relay)) = &out.sides[0].relay else {
                panic!("a partitioned side takes a round");
            };
            (out.post.vm.pending_ground_row, !relay.share(&probe, slot).is_empty())
        })
        .unzip();
    assert_eq!(seeded, receives);
    assert_eq!(seeded.iter().filter(|&&s| s).count(), 1, "one owner");
}
