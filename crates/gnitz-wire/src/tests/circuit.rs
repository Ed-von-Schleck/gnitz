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
        Opcode::JoinRange => OpNode::Join(JoinKind::Range { rel: RangeRel::Le }),
        Opcode::JoinCross => OpNode::Join(JoinKind::Cross),
        // Every aggregate, at a distinct source column.
        Opcode::Reduce => OpNode::Reduce(GroupAggs {
            group_cols: vec![2, 7],
            aggs: AggFunc::ALL
                .iter()
                .enumerate()
                .map(|(i, &agg_op)| AggDescriptor { agg_op, col_idx: i as u32 })
                .collect(),
        }),
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
        Opcode::WorkerFilter => OpNode::WorkerFilter,
        Opcode::PositivePart => OpNode::WeightClamp(ClampKind::PositivePart),
        Opcode::MapReindex => OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, TypeCode::I64), (5, TypeCode::U32)],
            role: ReindexRole::ScatterKey { source_cols: vec![1, 6] },
            nulls: NullKeys::Drop,
        }),
        Opcode::TopN => OpNode::TopN {
            group_cols: vec![1],
            order: vec![
                crate::OrderKey { col: 3, desc: true, nulls_first: false },
                crate::OrderKey { col: 0, desc: false, nulls_first: true },
            ],
            limit: 10,
            offset: 5,
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
    OpNode::ScanDelta { source, bound: crate::ReadBound::None }
}

/// The bytes of one unbounded scan in a cell: its tag, its source, its bound tag.
const SCAN_LEN: usize = 10;

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
    // The shapes an opcode's own sample cannot also be: the other bound kinds,
    // and the empty lists each counted layout allows.
    nodes.extend([
        scan(7),
        OpNode::ScanDelta {
            source: 7,
            bound: crate::ReadBound::PkSet(crate::PkKeys::from_keys(
                8,
                [&7u64.to_be_bytes()[..], &2u64.to_be_bytes()[..]],
            )),
        },
        OpNode::Map(MapKind::Compute(ComputeMap { program: vec![9, 9], out_cols: vec![] })),
        // A hash-row target's domain is the copy kernel's, not this decode's.
        OpNode::Map(MapKind::HashRow { cols: vec![(3, TypeCode::U128)] }),
        OpNode::Reduce(GroupAggs {
            group_cols: vec![],
            aggs: vec![AggDescriptor::COUNT_STAR],
        }),
        // The global shape: no group, one key, no offset — and a zero limit,
        // which frames fine: refusing it is the top-N kernel's.
        OpNode::TopN {
            group_cols: vec![],
            order: vec![crate::OrderKey { col: 2, desc: false, nulls_first: false }],
            limit: 0,
            offset: 0,
        },
        // The other role and NULL-key rule than the sample's.
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, TypeCode::I64)],
            role: ReindexRole::Auxiliary,
            nulls: NullKeys::Keep,
        }),
        // A stated route of no columns: a broadcast.
        OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, TypeCode::I64)],
            role: ReindexRole::ScatterKey { source_cols: vec![] },
            nulls: NullKeys::Keep,
        }),
    ]);
    // Every join kind on either side, and every relation the range kind can carry
    // — `Join`'s own sample can only be one of them. Same for the null-extend's
    // two placements.
    nodes.push(OpNode::Join(JoinKind::Cross));
    for &rel in RangeRel::ALL {
        nodes.push(OpNode::Join(JoinKind::Range { rel }));
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
    assert_eq!((CIRCUIT_VERSION, crate::checksum(&cells)), (15, 0x230c_3eef_fd93_1ff5));
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
    // tag 0 | nulls 1 | key count 2..4 | col 4..8 | type 8 | keep count 9..11 | role 11
    let aux = OpNode::Map(MapKind::Reindex {
        keep: vec![],
        key: vec![(3, TypeCode::I64)],
        role: ReindexRole::Auxiliary,
        nulls: NullKeys::Keep,
    });
    // tag 0 | count 1..3 | col 3..7 | type 7
    let hash_row = OpNode::Map(MapKind::HashRow { cols: vec![(3, TypeCode::I64)] });
    // tag 0 | nulls_first 1 | count 2..4 | type 4
    let null_ext = OpNode::NullExtend {
        type_codes: vec![TypeCode::I64],
        nulls_first: false,
    };
    // tag 0 | group count 1..3 | agg count 3..5 | func 5
    let reduce = OpNode::Reduce(GroupAggs {
        group_cols: vec![],
        aggs: vec![AggDescriptor::COUNT_STAR],
    });
    let cases = [
        (poke(&aux, 1, outside(NullKeys::from_wire)), "unknown NullKeys"),
        (poke(&aux, 8, 0), "unknown TypeCode 0"),
        (poke(&aux, 8, 200), "unknown TypeCode 200"),
        (poke(&aux, 11, 2), "boolean byte 2"),
        (poke(&hash_row, 7, 0), "unknown TypeCode 0"),
        (poke(&null_ext, 4, 200), "unknown TypeCode 200"),
        // tag 0 | rel 1
        (
            poke(&sample(Opcode::JoinRange), 1, outside(RangeRel::from_wire)),
            "unknown RangeRel",
        ),
        (poke(&reduce, 5, outside(AggFunc::from_wire)), "unknown AggFunc"),
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
    let key = vec![(3, TypeCode::I64)];
    let reindex = |role, key| {
        OpNode::Map(MapKind::Reindex {
            keep: vec![],
            key,
            role,
            nulls: NullKeys::Keep,
        })
    };
    let scatter = |source_cols| ReindexRole::ScatterKey { source_cols };
    let forward = "a node's input is not an earlier node";
    let arity = "node's inputs do not match its operator's arity";
    let reduce = OpNode::Reduce(GroupAggs {
        group_cols: vec![0],
        aggs: vec![AggDescriptor::COUNT_STAR],
    });
    let countless = OpNode::Reduce(GroupAggs {
        group_cols: vec![0],
        aggs: vec![AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum }],
    });
    let cases: [(OpNode, &[NodeId], &str); 8] = [
        (
            reindex(ReindexRole::Auxiliary, vec![]),
            &[0],
            "a reindex names no key columns",
        ),
        // A route names leading slots of the key.
        (
            reindex(scatter(vec![1, 2]), key),
            &[0],
            "a scatter key is longer than its reindex key",
        ),
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

/// A circuit's sources are its scans', one per scan: the client's id substitution
/// moves each, and a source scanned twice is named twice.
#[test]
fn sources_are_the_scans_sources() {
    let mut c = Circuit::default();
    let a = c.input_delta(7, crate::ReadBound::None);
    let b = c.input_delta(9, crate::ReadBound::None);
    let again = c.input_delta(7, crate::ReadBound::None);
    let ab = c.union(a, b);
    c.union(ab, again);
    assert_eq!(c.sources().collect::<Vec<_>>(), [7, 9, 7]);
    for source in c.sources_mut() {
        *source += 100;
    }
    assert_eq!(c.sources().collect::<Vec<_>>(), [107, 109, 107]);
}

/// A reduce is exchanged or each worker's local fold by its builder, and a
/// group-less exchanged one alone owes its ground row.
#[test]
fn the_reduce_builders_differ_in_the_exchange() {
    let mut c = Circuit::default();
    let input = c.input_delta(7, crate::ReadBound::None);
    let agg = |group_cols| GroupAggs {
        group_cols,
        aggs: vec![AggDescriptor::COUNT_STAR],
    };
    let global = c.reduce(input, agg(vec![]));
    let grouped = c.reduce(input, agg(vec![1]));
    let local = c.reduce_local(input, agg(vec![]));
    assert_eq!(
        [global, grouped, local].map(|id| c.owes_ground_row(id)),
        [true, false, false]
    );
    assert_eq!(c.op(c.inputs(grouped)[0]), &OpNode::ExchangeShard);
}
