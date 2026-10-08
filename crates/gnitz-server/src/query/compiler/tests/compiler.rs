use super::fixtures::*;
use super::*;
use crate::test_support::{
    identity_circuit, make_batch, make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_only_schema, u64_pk_schema,
};
use gnitz_wire::{AggDescriptor, AggFunc, Circuit, OpNode, ReadBound, TypeCode};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::schema::SchemaColumn;

// ── The shapes a compile refuses ────────────────────────────────────────

/// The output must match the view schema's physical layout, not just its width.
#[test]
fn an_output_schema_unequal_to_the_view_schema_is_rejected() {
    let compile = |source: SchemaDescriptor, view: SchemaDescriptor| {
        compile(
            identity_circuit(10, ReadBound::None),
            &sources([(10, source)]),
            &view,
            false,
        )
    };
    let pk_only = pk_only_schema(&[TypeCode::U64]);
    assert!(compile(pk_only, pk_only).is_ok(), "an equal pair compiles");
    for (source, view, why) in [
        (make_schema_u64_i64(), pk_only, "a wider output"),
        (
            make_schema_u64_i64(),
            make_schema_pk_u64_payload_string(),
            "equal widths, another column type",
        ),
    ] {
        assert_eq!(
            rejection(compile(source, view)),
            "the circuit's output schema is not the view's",
            "{why}"
        );
    }
}

/// A group column the relay's group key would refuse is a `CREATE VIEW`
/// rejection, even on one worker, where nothing relays.
#[test]
fn a_float_group_column_behind_an_exchange_is_rejected() {
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::F64, false));
    let aggs = [AggDescriptor::COUNT_STAR];
    let compile = |group: u32, view: SchemaDescriptor| {
        let mut c = Circuit::default();
        let a = scan(&mut c, 10);
        let s = c.shard(a);
        let reduce = OpNode::Reduce {
            group_cols: vec![group],
            agg: aggs.to_vec(),
        };
        c.push(reduce, &[s]).unwrap();
        compile(c, &sources([(10, schema)]), &view, false)
    };
    let counted = *gnitz_zset::stream::ReducePlan::from_wire(&schema, &[0], &aggs, false)
        .unwrap()
        .output_schema();
    assert!(compile(0, counted).is_ok(), "an integer group column compiles");
    assert_eq!(
        rejection(compile(1, schema)),
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
    // One `ScanDelta → [mid] → ExchangeShard` side per source, unioned into the
    // output of a partitioned view; the relays in circuit order.
    let relays = |of: u32, sides: &[(Source, Option<OpNode>)]| {
        let mut c = Circuit::default();
        let shards: Vec<NodeId> = (10..)
            .zip(sides)
            .map(|(source, (_, mid))| {
                let mut tip = scan(&mut c, source);
                if let Some(op) = mid {
                    tip = c.push(op.clone(), &[tip]).unwrap();
                }
                c.shard(tip)
            })
            .collect();
        shards.into_iter().reduce(|a, b| c.union(a, b)).unwrap();
        let registry = sources_at(Slot::new(0, of), (10..).zip(sides.iter().map(|s| s.0)));
        let (out, _) = compile(c, &registry, &view, false).expect("the fixture compiles");
        let rounds = out.vm.ops().filter(|op| matches!(op, Op::Round { .. })).count();
        let shares = out.vm.ops().filter(|op| matches!(op, Op::Share(_))).count();
        (rounds, shares)
    };
    use crate::query::vm::Op;
    let negate = || Some(OpNode::Negate);
    assert_eq!(
        relays(4, &[(keyed, None)]),
        (0, 0),
        "a shard on the key its scan's rows are placed by"
    );
    assert_eq!(relays(4, &[(keyed, negate())]), (1, 0));
    assert_eq!(
        relays(4, &[(replicated, negate()), (keyed, negate())]),
        (1, 1),
        "every worker computed the same rows of a replicated side"
    );
    assert_eq!(
        relays(4, &[(replicated, Some(OpNode::WorkerFilter)), (keyed, negate())]),
        // The trim is a share of its own.
        (2, 1),
        "a trimmed side emits a slice of the replica, not the replica"
    );
    assert_eq!(relays(1, &[(keyed, negate())]), (0, 0), "one worker");
    assert_eq!(
        relays(4, &[(replicated, negate())]),
        (0, 0),
        "a view over replicated sources alone is computed whole on every worker"
    );
}

// ── Global operators ────────────────────────────────────────────────────

fn global_reduce(op: AggFunc) -> OpNode {
    OpNode::Reduce {
        group_cols: vec![],
        agg: vec![AggDescriptor { agg_op: op, col_idx: 1 }, AggDescriptor::COUNT_STAR],
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

/// `ScanDelta(10) → [ExchangeShard] → op`.
fn global_circuit(op: &OpNode, exchanged: bool) -> Circuit {
    let mut c = Circuit::default();
    let mut tip = scan(&mut c, 10);
    if exchanged {
        tip = c.shard(tip);
    }
    c.push(op.clone(), &[tip]).unwrap();
    c
}

/// Compile a [`global_circuit`] over `source` as worker `slot`, into the view its
/// global operator outputs.
fn compile_global(circuit: Circuit, source: Source, slot: Slot) -> Result<CompileOutput, String> {
    let schema = source.schema;
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
                *gnitz_zset::stream::TopNPlan::from_wire(&schema, &[], order, *limit, *offset)
                    .unwrap()
                    .output_schema(),
            ),
            _ => None,
        })
        .expect("a global operator");
    compile(circuit, &sources_at(slot, [(10, source)]), &view, false).map(|(out, _)| out)
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
        out.vm
            .ops()
            .filter(|op| {
                matches!(
                    op,
                    crate::query::vm::Op::Reduce { .. } | crate::query::vm::Op::TopN { .. }
                )
            })
            .count()
            == 2
    };
    for (why, op, source, of, want) in [
        ("a partitioned SUM", global_reduce(AggFunc::Sum), keyed, 4, true),
        ("a partitioned top-N", global_topn(), keyed, 4, true),
        ("one worker", global_reduce(AggFunc::Sum), keyed, 1, false),
        ("one worker", global_topn(), keyed, 1, false),
        ("a replicated source", global_reduce(AggFunc::Sum), replicated, 4, false),
        ("a replicated source", global_topn(), replicated, 4, false),
        ("a partitioned MIN", global_reduce(AggFunc::Min), keyed, 4, true),
    ] {
        assert_eq!(split(global_circuit(&op, true), source, of), want, "{why}");
    }
}

/// A global aggregate's ground row is minted by the first epoch of any source,
/// and a round runs only in the epochs of the sources that reach it — so a round
/// behind the aggregate would drop the row minted in another source's epoch. A
/// reduce that owes no ground row may sit behind one.
#[test]
fn an_exchange_behind_a_global_aggregate_is_rejected() {
    let keyed = Source::from(make_schema_u64_i64());
    let above = |exchanged: bool| {
        let mut c = global_circuit(&global_reduce(AggFunc::Sum), exchanged);
        let reduced = c.nodes().len() - 1;
        c.shard(reduced);
        compile_global(c, keyed, Slot::new(0, 4))
    };
    assert_eq!(rejection(above(true)), "an exchange behind a global aggregate");
    assert!(
        above(false).is_ok(),
        "a reduce of each worker's slice owes no ground row"
    );
}

/// An exchange places its rows by the key of the reduce or top-N reading it, so
/// a second reader would be handed rows placed by a key that is not its own.
#[test]
fn an_exchange_a_reduce_shares_with_another_reader_is_rejected() {
    let keyed = Source::from(make_schema_u64_i64());
    for op in [global_reduce(AggFunc::Sum), global_topn()] {
        let mut shared = Circuit::default();
        let a = scan(&mut shared, 10);
        let shard = shared.shard(a);
        shared.negate(shard);
        shared.push(op.clone(), &[shard]).unwrap();
        for of in [1, 4] {
            assert_eq!(
                rejection(compile_global(shared.clone(), keyed, Slot::new(0, of))),
                "an exchange in front of a reduce or top-N has another reader",
                "{op:?} of {of}"
            );
        }
    }
}

/// A global aggregate's ground row is seeded once per copy of the result: on each
/// worker holding the whole input, else on the one worker the empty-keyed shard
/// sends every row to. With no exchange the reduce aggregates each worker's
/// slice, and owes none.
#[test]
fn the_ground_row_is_seeded_where_the_whole_input_arrives() {
    let keyed = Source::from(make_schema_u64_i64());
    let replicated = keyed.placed(Placement::Replicated);
    let op = global_reduce(AggFunc::Min);
    let seeds = |exchanged: bool, source: Source, slot: Slot| {
        compile_global(global_circuit(&op, exchanged), source, slot).map(|out| out.vm.pending_ground_row)
    };
    assert_eq!(seeds(true, keyed, Slot::SOLO), Ok(true), "one worker");
    assert_eq!(seeds(true, replicated, Slot::new(2, 4)), Ok(true), "a replicated view");
    for (source, slot) in [
        (keyed, Slot::SOLO),
        (keyed, Slot::new(0, 4)),
        (replicated, Slot::new(2, 4)),
    ] {
        assert_eq!(seeds(false, source, slot), Ok(false), "no exchange, {slot:?}");
    }

    let probe = make_batch(&keyed.schema, &[(1, 1, 1)]);
    let (seeded, receives): (Vec<bool>, Vec<bool>) = (0..4)
        .map(|rank| {
            let slot = Slot::new(rank, 4);
            let out = compile_global(global_circuit(&op, true), keyed, slot).expect("the fixture compiles");
            let round = out.vm.ops().find_map(|op| match op {
                crate::query::vm::Op::Round { plan, .. } => Some(std::rc::Rc::clone(plan)),
                _ => None,
            });
            let round = round.expect("a partitioned side takes a round");
            (out.vm.pending_ground_row, !round.share(&probe, slot).is_empty())
        })
        .unzip();
    assert_eq!(seeded, receives);
    assert_eq!(seeded.iter().filter(|&&s| s).count(), 1, "one owner");
}
