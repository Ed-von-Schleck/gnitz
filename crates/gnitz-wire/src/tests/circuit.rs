use super::*;
use crate::outside;
use crate::{AggDescriptor, AggFunc};

/// One node per opcode, exhaustive with no wildcard, so a new `Opcode` variant
/// cannot reach the wire without a round-trip. Each sample carries a non-empty
/// parameter list where its layout has one, so the sweeps below have something
/// to cut.
fn sample(op: Opcode) -> OpNode {
    match op {
        Opcode::Filter => OpNode::Filter(vec![1, 2, 3, 4]),
        Opcode::Negate => OpNode::Negate,
        Opcode::Union => OpNode::Union,
        Opcode::JoinEqui => OpNode::Join(JoinKind::Equi),
        Opcode::JoinRange => OpNode::Join(JoinKind::range(RangeRel::Le)),
        Opcode::JoinCross => OpNode::Join(JoinKind::Cross),
        // Every aggregate, at a distinct source column.
        Opcode::Reduce => OpNode::Reduce {
            group: GroupAggs {
                group_cols: vec![2, 7],
                aggs: AggFunc::ALL
                    .iter()
                    .enumerate()
                    .map(|(i, &agg_op)| AggDescriptor { agg_op, col_idx: i as u32 })
                    .collect(),
            },
            kind: ReduceKind::Aggregate,
        },
        Opcode::Distinct => OpNode::WeightClamp(ClampKind::Distinct),
        // A non-ascending index column list: `PkColList`'s `PartialEq` spans the
        // whole backing array, so a reordered list fails the round-trip.
        Opcode::ScanDelta => OpNode::ScanDelta {
            source: 42,
            bound: crate::ReadBound::Range(crate::KeyRange::new(
                crate::PkColList::from_slice(&[9, 3, 5]),
                &[7, 11],
                5,
                89,
            )),
            scatter: Some(vec![(1, TypeCode::I64), (6, TypeCode::U32)]),
        },
        Opcode::ExchangeShard => OpNode::ExchangeShard,
        Opcode::NullExtend => OpNode::NullExtend {
            type_codes: vec![TypeCode::I64, TypeCode::String],
            nulls_first: true,
        },
        Opcode::MapExpr => OpNode::Map(MapKind::Compute(ComputeMap {
            program: vec![7],
            out_cols: vec![(TypeCode::I64, false), (TypeCode::String, true)],
        })),
        Opcode::MapHashRow => OpNode::Map(MapKind::HashRow {
            cols: vec![(1, TypeCode::String), (2, TypeCode::I32)],
        }),
        Opcode::PositivePart => OpNode::WeightClamp(ClampKind::PositivePart),
        Opcode::MapReindex => OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, TypeCode::I64), (5, TypeCode::U32)],
            nulls: NullKeys::Drop,
        }),
        Opcode::TopN => OpNode::TopN {
            group_cols: vec![1],
            order: vec![
                crate::OrderKey { col: 3, desc: true, nulls_first: false },
                crate::OrderKey { col: 0, desc: false, nulls_first: true },
            ],
            slots: crate::WeightSlots { skip: 5, keep: 10 },
        },
        Opcode::Window => OpNode::Window {
            group: GroupAggs {
                group_cols: vec![1, 4],
                aggs: vec![AggDescriptor { col_idx: 2, agg_op: AggFunc::Max }],
            },
            order: vec![crate::OrderKey { col: 3, desc: true, nulls_first: true }],
            rank: true,
            dense_rank: false,
        },
    }
}

fn scan(source: u64) -> OpNode {
    OpNode::ScanDelta {
        source,
        bound: crate::ReadBound::None,
        scatter: None,
    }
}

/// The bytes of one unbounded scan stating no scatter key in a cell: its tag,
/// its source, its bound tag, its scatter flag.
const SCAN_LEN: usize = 11;

/// `op` behind one unbounded scan per input slot, wired on them in descending
/// order, so a codec that swapped a binary operator's slots fails the round-trip.
fn wired(op: OpNode) -> Circuit {
    let mut c = Circuit::default();
    let mut inputs: Vec<NodeId> = (0..op.arity())
        .map(|i| c.push(scan(7 + i as u64), &[]).unwrap())
        .collect();
    inputs.reverse();
    c.push(op, &inputs).unwrap();
    c
}

/// Where `op`'s opcode byte sits in `wired(op).encode()`: behind the node count
/// and the scans.
fn tag_at(op: &OpNode) -> usize {
    2 + SCAN_LEN * op.arity()
}

/// A cell laid out as `encode` would, around `push`: what a forger can write and
/// a builder cannot.
fn cell(nodes: &[(OpNode, &[NodeId])]) -> Vec<u8> {
    let mut w = Writer::new();
    w.count(nodes.len());
    for (op, inputs) in nodes {
        op.write(&mut w);
        for &input in *inputs {
            w.u16(input.try_into().unwrap());
        }
    }
    w.into_vec()
}

/// Every `OpNode` shape survives `encode` → `decode`, the opcode space and each
/// wire enum the encoder embeds driven from its own `ALL`.
#[test]
fn every_op_node_variant_roundtrips() {
    let mut nodes: Vec<OpNode> = Opcode::ALL.iter().map(|&op| sample(op)).collect();
    // The shapes an opcode's own sample cannot also be: the other bound kinds, a
    // scan stating no scatter key and one stating a broadcast, and the empty lists
    // each counted layout allows.
    nodes.extend([
        scan(7),
        OpNode::ScanDelta {
            source: 7,
            bound: crate::ReadBound::PkSet(crate::PkKeys::from_sorted(
                8,
                [2u64.to_be_bytes(), 7u64.to_be_bytes()].concat(),
            )),
            scatter: Some(vec![]),
        },
        OpNode::Map(MapKind::Compute(ComputeMap { program: vec![9, 9], out_cols: vec![] })),
        // A hash-row target's domain is the copy kernel's, not this decode's.
        OpNode::Map(MapKind::HashRow { cols: vec![(3, TypeCode::U128)] }),
        // The global shape: no group, one key, no offset.
        OpNode::TopN {
            group_cols: vec![],
            order: vec![crate::OrderKey { col: 2, desc: false, nulls_first: false }],
            slots: crate::WeightSlots { skip: 0, keep: 1 },
        },
        // The other NULL-key rule than the sample's.
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, TypeCode::I64)],
            nulls: NullKeys::Keep,
        }),
    ]);
    // Every relation the range kind can carry — `JoinRange`'s own sample can
    // only be one of them. Same for the reduce's kinds and the null-extend's two
    // placements.
    for &kind in ReduceKind::ALL {
        nodes.push(OpNode::Reduce {
            group: GroupAggs {
                group_cols: vec![],
                aggs: vec![AggDescriptor::COUNT_STAR],
            },
            kind,
        });
    }
    for &rel in RangeRel::ALL {
        for clock in [None, Some(JoinSide::Left), Some(JoinSide::Right)] {
            nodes.push(OpNode::Join(JoinKind::Range { rel, clock }));
        }
    }
    for nulls_first in [false, true] {
        nodes.push(OpNode::NullExtend {
            type_codes: vec![TypeCode::I64],
            nulls_first,
        });
    }
    for node in nodes {
        let circuit = wired(node);
        assert_eq!(Circuit::decode(&circuit.encode()), Ok(circuit.clone()), "{circuit:?}");
    }
}

/// Each opcode's sample encodes under its own tag, and its cell is refused cut
/// anywhere, one byte long, or under a tag no operator has.
#[test]
fn each_opcode_cell_refuses_every_perturbation() {
    let unknown = outside(Opcode::from_wire);
    for &op in Opcode::ALL {
        let node = sample(op);
        let bytes = wired(node.clone()).encode();
        let at = tag_at(&node);
        assert_eq!(bytes[at], op.as_wire(), "{op:?} encodes under its own tag");
        for cut in 0..bytes.len() {
            let err = Circuit::decode(&bytes[..cut]).expect_err("a cut cell");
            assert_eq!(err.matches("circuit").count(), 1, "{op:?} cut at {cut}: {err:?}");
        }
        let mut over_long = bytes.clone();
        over_long.push(0);
        assert!(Circuit::decode(&over_long).unwrap_err().contains("trailing"), "{op:?}");
        let mut forged = bytes;
        forged[at] = unknown;
        assert!(
            Circuit::decode(&forged).unwrap_err().contains("unknown Opcode"),
            "{op:?}"
        );
    }
}

/// A cell's layout moves only with its version: the digest is every opcode's
/// sample cell, so either both halves of the pair change or neither does.
#[test]
fn the_cell_layout_is_pinned_to_its_version() {
    let cells: Vec<u8> = Opcode::ALL.iter().flat_map(|&op| wired(sample(op)).encode()).collect();
    assert_eq!((CIRCUIT_VERSION, crate::checksum(&cells)), (23, 0xbab2_506f_e812_3f97));
}

/// Every framing guard, against the forgery that trips it.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    // `off` counts from the operator's opcode byte.
    let poke = |op: &OpNode, off: usize, v: u8| {
        let mut bytes = wired(op.clone()).encode();
        bytes[tag_at(op) + off] = v;
        bytes
    };
    // tag 0 | nulls 1 | key count 2..4 | col 4..8 | type 8 | keep count 9..11
    let aux = OpNode::Map(MapKind::Reindex {
        keep: vec![],
        key: vec![(3, TypeCode::I64)],
        nulls: NullKeys::Keep,
    });
    // tag 0 | count 1..3 | col 3..7 | type 7
    let hash_row = OpNode::Map(MapKind::HashRow { cols: vec![(3, TypeCode::I64)] });
    // tag 0 | nulls_first 1 | count 2..4 | type 4
    let null_ext = OpNode::NullExtend {
        type_codes: vec![TypeCode::I64],
        nulls_first: false,
    };
    // tag 0 | kind 1 | group count 2..4 | agg count 4..6 | func 6
    let reduce = OpNode::Reduce {
        group: GroupAggs {
            group_cols: vec![],
            aggs: vec![AggDescriptor::COUNT_STAR],
        },
        kind: ReduceKind::Aggregate,
    };
    let cases = [
        (poke(&aux, 1, outside(NullKeys::from_wire)), "unknown NullKeys"),
        (poke(&aux, 8, outside(TypeCode::from_wire)), "unknown TypeCode"),
        (poke(&reduce, 1, outside(ReduceKind::from_wire)), "unknown ReduceKind"),
        (poke(&hash_row, 7, outside(TypeCode::from_wire)), "unknown TypeCode"),
        (poke(&null_ext, 4, outside(TypeCode::from_wire)), "unknown TypeCode"),
        // tag 0 | rel 1
        (
            poke(&sample(Opcode::JoinRange), 1, outside(RangeRel::from_wire)),
            "unknown RangeRel",
        ),
        (poke(&reduce, 6, outside(AggFunc::from_wire)), "unknown AggFunc"),
        // The encoder is infallible; the decode refuses by the count before
        // reading the body.
        (
            wired(OpNode::Map(MapKind::Compute(ComputeMap {
                program: vec![],
                out_cols: vec![(TypeCode::I64, false); crate::MAX_COLUMNS + 1],
            })))
            .encode(),
            "exceeds cap",
        ),
        // tag 0 | source 1..9 | bound tag 9. A malformed backfill hint is catalog
        // corruption, not a wider scan.
        (
            poke(&scan(3), 9, outside(crate::BoundTag::from_wire)),
            "unknown BoundTag",
        ),
        // A circuit's output is its last node, so one of no nodes has none.
        (Circuit::default().encode(), "a circuit has no nodes"),
        // A cell holds at least its node count.
        (vec![], "truncated"),
    ];
    for (bytes, want) in cases {
        let err = Circuit::decode(&bytes).unwrap_err();
        assert!(err.contains(want), "{err:?} does not name {want:?}");
    }
}

/// A circuit at the node cap decodes, and one node more is refused by its count
/// before a node is read.
#[test]
fn decode_accepts_a_circuit_at_the_node_cap() {
    let mut c = Circuit::default();
    let mut last = c.input_delta(7, crate::ReadBound::None);
    while c.nodes.len() < MAX_CIRCUIT_NODES {
        last = c.negate(last);
    }
    assert_eq!(Circuit::decode(&c.encode()), Ok(c.clone()));
    c.negate(last);
    assert_eq!(
        Circuit::decode(&c.encode()).unwrap_err(),
        "circuit: nodes: 16385 entries exceeds cap 16384"
    );
}

/// What holds of an operator under every schema, and an input naming a node not
/// yet pushed, is `push`'s, so a built node and a decoded one are refused alike.
/// An input count off the operator's arity is refused when built; a cell cannot
/// spell one, its operator fixing how many inputs it reads.
#[test]
fn push_refuses_a_malformed_operator_built_or_decoded() {
    let keyless = OpNode::Map(MapKind::Reindex {
        keep: vec![],
        key: vec![],
        nulls: NullKeys::Keep,
    });
    let forward = "a node's input is not an earlier node";
    let arity = "node's inputs do not match its operator's arity";
    let reduce = OpNode::Reduce {
        group: GroupAggs {
            group_cols: vec![0],
            aggs: vec![AggDescriptor::COUNT_STAR],
        },
        kind: ReduceKind::Aggregate,
    };
    let countless = OpNode::Reduce {
        group: GroupAggs {
            group_cols: vec![0],
            aggs: vec![AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum }],
        },
        kind: ReduceKind::Aggregate,
    };
    let keepless = OpNode::TopN {
        group_cols: vec![],
        order: vec![crate::OrderKey { col: 0, desc: false, nulls_first: false }],
        slots: crate::WeightSlots { skip: 3, keep: 0 },
    };
    let cases: [(OpNode, &[NodeId], &str); 8] = [
        (keyless, &[0], "a reindex names no key columns"),
        (keepless, &[0], "a top-N keeps no slot"),
        (
            OpNode::Map(MapKind::HashRow { cols: vec![] }),
            &[0],
            "a hash-row map names no columns",
        ),
        (countless, &[0], "a reduce carries no COUNT(*)"),
        (OpNode::Negate, &[1], forward),
        (OpNode::Negate, &[7], forward),
        (OpNode::Union, &[0], arity),
        // Slot 1 is filled solely by a binary operator.
        (reduce, &[0, 0], arity),
    ];
    for (op, inputs, want) in cases {
        let mut c = Circuit::default();
        assert_eq!(c.push(scan(7), &[]), Ok(0));
        assert_eq!(c.push(op.clone(), inputs).unwrap_err(), want);
        assert_eq!(c.nodes.len(), 1, "a refused push appends nothing");
        assert_eq!(c.inputs(0), [] as [NodeId; 0]);
        if want != arity {
            let decoded = Circuit::decode(&cell(&[(scan(7), &[]), (op, inputs)])).unwrap_err();
            assert_eq!(decoded, format!("circuit: {want}"));
        }
    }
    let mut c = Circuit::default();
    let input = c.push(scan(7), &[]).unwrap();
    assert_eq!(c.push(OpNode::Negate, &[input]), Ok(1));
    assert_eq!(c.inputs(1), [input]);
}

/// Each relation's two predicates, and `x OP y ⟺ y converse(OP) x`: the
/// converse keeps whether equality is admitted and flips the bounding side,
/// which determines it.
#[test]
fn range_rel_predicates_and_converse() {
    use RangeRel::*;
    for (r, bounds_below, admits_equal) in [
        (Lt, false, false),
        (Le, false, true),
        (Gt, true, false),
        (Ge, true, true),
    ] {
        assert_eq!(
            (r.bounds_below(), r.admits_equal()),
            (bounds_below, admits_equal),
            "{r:?}"
        );
        let c = r.converse();
        assert_eq!(
            (c.bounds_below(), c.admits_equal()),
            (!bounds_below, admits_equal),
            "{r:?}"
        );
        assert_eq!(c.converse(), r);
    }
}

/// A circuit's sources are its scans', each once: a source scanned again
/// answers its first node, and the client's id substitution moves each.
#[test]
fn sources_are_the_scans_sources() {
    let mut c = Circuit::default();
    let a = c.input_delta(7, crate::ReadBound::None);
    let b = c.input_delta(9, crate::ReadBound::None);
    let again = c.input_delta(7, crate::ReadBound::None);
    assert_eq!(again, a);
    c.union(a, b);
    assert_eq!(c.sources().collect::<Vec<_>>(), [7, 9]);
    for source in c.sources_mut() {
        *source += 100;
    }
    assert_eq!(c.sources().collect::<Vec<_>>(), [107, 109]);
}

/// A source scanned again keeps an equal bound and widens differing ones to a
/// full scan; whichever order the scans arrive in.
#[test]
fn a_source_scanned_again_keeps_an_equal_bound_and_widens_others() {
    let range =
        |first| crate::ReadBound::Range(crate::KeyRange::new(crate::PkColList::from_slice(&[0]), &[], first, 89));
    let cases = [
        ([range(5), range(5)], range(5)),
        ([range(5), range(6)], crate::ReadBound::None),
        ([range(5), crate::ReadBound::None], crate::ReadBound::None),
        ([crate::ReadBound::None, range(5)], crate::ReadBound::None),
    ];
    for (scans, want) in cases {
        let mut c = Circuit::default();
        let [a, b] = scans.clone().map(|bound| c.input_delta(7, bound));
        assert_eq!(a, b, "{scans:?}");
        assert_eq!(c.nodes.len(), 1);
        let OpNode::ScanDelta { bound, .. } = c.op(a) else {
            panic!("{scans:?}: not a scan")
        };
        assert_eq!(*bound, want, "{scans:?}");
    }
}

/// A source's scatter key is one: a scan stating none enters under one stated
/// before or after it, one stating the same key shares the node, and one stating
/// another key is refused and changes nothing.
#[test]
fn a_source_states_one_scatter_key() {
    let key = |col| Some(vec![(col, TypeCode::I64)]);
    let scatter_of = |c: &Circuit, nid| match c.op(nid) {
        OpNode::ScanDelta { scatter, .. } => scatter.clone(),
        op => panic!("{op:?} is not a scan"),
    };
    for first in [None, key(3)] {
        let mut c = Circuit::default();
        let a = c.input_scattered(7, crate::ReadBound::None, first).unwrap();
        assert_eq!(c.input_scattered(7, crate::ReadBound::None, key(3)), Ok(a));
        assert_eq!(c.input_scattered(7, crate::ReadBound::None, None), Ok(a));
        assert_eq!(scatter_of(&c, a), key(3));
        assert_eq!(
            c.input_scattered(7, crate::ReadBound::None, key(4)),
            Err("source 7 is scanned under several distinct scatter keys".into())
        );
        assert_eq!(scatter_of(&c, a), key(3));
        assert_eq!(c.nodes.len(), 1);
    }
}

/// A cell scanning one source in two nodes is refused, never merged: a builder
/// cannot write one.
#[test]
fn decode_refuses_a_source_scanned_twice() {
    let bytes = cell(&[(scan(7), &[]), (scan(9), &[]), (scan(7), &[]), (OpNode::Union, &[0, 2])]);
    assert_eq!(
        Circuit::decode(&bytes).unwrap_err(),
        "circuit: source 7 is scanned by more than one node"
    );
}

/// A reduce is exchanged unless it is each worker's local fold, and only an
/// aggregate over no group columns owes its ground row.
#[test]
fn the_reduce_kinds_differ_in_the_exchange_and_the_ground_row() {
    let mut c = Circuit::default();
    let input = c.input_delta(7, crate::ReadBound::None);
    let agg = |group_cols| GroupAggs {
        group_cols,
        aggs: vec![AggDescriptor::COUNT_STAR],
    };
    let global = c.reduce(input, agg(vec![]), ReduceKind::Aggregate);
    let grouped = c.reduce(input, agg(vec![1]), ReduceKind::Aggregate);
    let groups = c.reduce(input, agg(vec![]), ReduceKind::Groups);
    let local = c.reduce(input, agg(vec![]), ReduceKind::Local);
    assert_eq!(
        [global, grouped, groups, local].map(|id| c.op(id).owes_ground_row()),
        [true, false, false, false]
    );
    assert_eq!(
        [global, grouped, groups, local].map(|id| c.op(id).exchanges()),
        [true, true, true, false]
    );
    assert_eq!(c.inputs(grouped), [input]);
}
