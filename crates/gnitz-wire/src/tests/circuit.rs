use super::*;

fn agg(agg_op: AggFunc, col_idx: u32) -> AggDescriptor {
    AggDescriptor { agg_op, col_idx }
}

/// One node per opcode, exhaustive with no wildcard, so a new `Opcode` variant
/// cannot reach the wire without a round-trip. Each sample carries a non-empty
/// parameter list where its layout has one, so the sweeps below have something
/// to cut.
fn sample(op: Opcode) -> OpNode {
    match op {
        Opcode::Filter => OpNode::Filter(vec![1, 2, 3, 4]),
        Opcode::Negate => OpNode::Negate,
        Opcode::Union => OpNode::Union,
        Opcode::JoinEqui => OpNode::Join {
            kind: JoinKind::Equi,
            delta_is_right: true,
        },
        Opcode::JoinRange => OpNode::Join {
            kind: JoinKind::Range { rel: RangeRel::Le },
            delta_is_right: true,
        },
        Opcode::JoinCross => OpNode::Join {
            kind: JoinKind::Cross,
            delta_is_right: true,
        },
        Opcode::IntegrateSink => OpNode::IntegrateSink,
        // Every aggregate, at a distinct source column.
        Opcode::Reduce => OpNode::Reduce {
            group_cols: vec![2, 7],
            agg: AggFunc::ALL
                .iter()
                .enumerate()
                .map(|(i, &f)| agg(f, i as u32))
                .collect(),
            global_ground: false,
        },
        Opcode::Distinct => OpNode::WeightClamp(ClampKind::Distinct),
        // A non-ascending index column list: `PkColList`'s `PartialEq` spans the
        // whole backing array, so a reordered list fails the round-trip.
        Opcode::ScanDelta => OpNode::ScanDelta {
            source: 42,
            bound: crate::ReadBound::Range(crate::KeyRange::new(
                crate::PkColList::from_slice(&[9, 3, 5]),
                &[7, 11],
                crate::Cut::after(4),
                crate::Cut::before(90),
            )),
        },
        Opcode::ExchangeShard => OpNode::ExchangeShard { shard_cols: vec![0, 2] },
        Opcode::NullExtend => OpNode::NullExtend {
            type_codes: vec![TypeCode::I64, TypeCode::String],
            nulls_first: true,
        },
        Opcode::MapProj => OpNode::Map(MapKind::Projection(vec![4, 0, 9])),
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
            role: ReindexRole::ScatterKey {
                source_key: vec![(1, TypeCode::I64), (6, TypeCode::U32)],
            },
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
fn cell(nodes: &[(OpNode, &[u16])]) -> Vec<u8> {
    let mut w = Writer::new();
    w.count(nodes.len());
    for (op, inputs) in nodes {
        op.write(&mut w);
        for &input in *inputs {
            w.u16(input);
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
        OpNode::Map(MapKind::Projection(vec![])),
        OpNode::Map(MapKind::Compute(ComputeMap { program: vec![9, 9], out_cols: vec![] })),
        // A hash-row target's domain is the copy kernel's, not this decode's.
        OpNode::Map(MapKind::HashRow { cols: vec![(3, TypeCode::U128)] }),
        OpNode::Reduce {
            group_cols: vec![],
            agg: vec![AggDescriptor::COUNT_STAR],
            global_ground: true,
        },
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
    ]);
    // Every join kind on either side, and every relation the range kind can carry
    // — `Join`'s own sample can only be one of them. Same for the null-extend's
    // two placements.
    for delta_is_right in [false, true] {
        for kind in [JoinKind::Equi, JoinKind::Cross] {
            nodes.push(OpNode::Join { kind, delta_is_right });
        }
        for &rel in RangeRel::ALL {
            nodes.push(OpNode::Join {
                kind: JoinKind::Range { rel },
                delta_is_right,
            });
        }
        nodes.push(OpNode::NullExtend {
            type_codes: vec![TypeCode::I64],
            nulls_first: delta_is_right,
        });
    }
    for node in nodes {
        let circuit = wired(node);
        assert_eq!(Circuit::decode(&circuit.encode()), Ok(circuit.clone()), "{circuit:?}");
    }
    assert_eq!(Circuit::decode(&Circuit::default().encode()), Ok(Circuit::default()));
}

/// Each opcode's sample encodes under its own tag, and its cell is refused cut
/// anywhere, one byte long, or under a tag no operator has.
#[test]
fn each_opcode_cell_refuses_every_perturbation() {
    let unknown = (0..=u8::MAX).find(|&o| Opcode::from_wire(o).is_none()).unwrap();
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
    assert_eq!((CIRCUIT_VERSION, crate::checksum(&cells)), (9, 0x01dd_98d4_fe31_574d));
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
    // tag 0 | global_ground 1 | group count 2..4 | agg count 4..6 | func 6
    let reduce = OpNode::Reduce {
        group_cols: vec![],
        agg: vec![AggDescriptor::COUNT_STAR],
        global_ground: false,
    };
    // The first byte outside each wire enum — not a literal, which the next
    // variant added would quietly turn into a valid value.
    fn outside<T>(from_wire: impl Fn(u8) -> Option<T>) -> u8 {
        (0..=u8::MAX).find(|&b| from_wire(b).is_none()).unwrap()
    }
    let mut past_cap = Writer::new();
    past_cap.count(MAX_CIRCUIT_NODES + 1);
    let cases = [
        (poke(&aux, 1, outside(NullKeys::from_wire)), "unknown NullKeys"),
        (poke(&aux, 8, 0), "unknown TypeCode 0"),
        (poke(&aux, 8, 200), "unknown TypeCode 200"),
        (poke(&aux, 11, 2), "boolean byte 2"),
        (poke(&hash_row, 7, 0), "unknown TypeCode 0"),
        (poke(&null_ext, 4, 200), "unknown TypeCode 200"),
        // tag 0 | rel 1 | delta_is_right 2
        (
            poke(&sample(Opcode::JoinRange), 1, outside(RangeRel::from_wire)),
            "unknown RangeRel",
        ),
        (poke(&reduce, 6, outside(AggFunc::from_wire)), "unknown AggFunc"),
        // The encoder is infallible; the decode refuses by the count before
        // reading the body.
        (
            wired(OpNode::Map(MapKind::Projection(
                (0..=crate::MAX_COLUMNS as u32).collect(),
            )))
            .encode(),
            "exceeds cap",
        ),
        // tag 0 | source 1..9 | bound tag 9. A malformed backfill hint is catalog
        // corruption, not a wider scan.
        (poke(&scan(3), 9, 9), "unknown bound kind"),
        // The count is refused before a node is read.
        (past_cap.into_vec(), "nodes: 16385 entries exceeds cap 16384"),
        (
            cell(&[(scan(7), &[]), (OpNode::Negate, &[1])]),
            "a node's input is not an earlier node",
        ),
        (cell(&[(OpNode::Negate, &[0])]), "a node's input is not an earlier node"),
        // A cell holds at least its node count.
        (vec![], "truncated"),
    ];
    for (bytes, want) in cases {
        let err = Circuit::decode(&bytes).unwrap_err();
        assert!(err.contains(want), "{err:?} does not name {want:?}");
    }
}

/// The count `decode` refuses past is one a circuit may hold.
#[test]
fn decode_accepts_a_circuit_at_the_node_cap() {
    let mut c = Circuit::default();
    let mut last = c.input_delta(7, crate::ReadBound::None);
    while c.nodes().len() < MAX_CIRCUIT_NODES {
        last = c.negate(last);
    }
    assert_eq!(Circuit::decode(&c.encode()), Ok(c.clone()));
    c.negate(last);
    assert!(Circuit::decode(&c.encode()).unwrap_err().contains("exceeds cap"));
}

/// What holds of an operator under every schema is `push`'s, so a built node and
/// a decoded one are refused alike.
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
    let scatter = |source_key| ReindexRole::ScatterKey { source_key };
    let cases = [
        (
            reindex(ReindexRole::Auxiliary, vec![]),
            "a reindex names no key columns",
        ),
        // The scatter and the trace must pack one byte image.
        (
            reindex(scatter(vec![(1, TypeCode::I32)]), key.clone()),
            "a scatter key's slot types are not its reindex key's",
        ),
        // A stated route with no source columns would scatter the whole relation
        // onto one worker.
        (
            reindex(scatter(vec![]), key.clone()),
            "a scatter key's slot types are not its reindex key's",
        ),
        (
            reindex(scatter(vec![(1, TypeCode::I64), (2, TypeCode::I64)]), key),
            "a scatter key's slot types are not its reindex key's",
        ),
        (
            OpNode::Map(MapKind::HashRow { cols: vec![] }),
            "a hash-row map names no columns",
        ),
        // A ground-seeding reduce groups on nothing.
        (
            OpNode::Reduce {
                group_cols: vec![0],
                agg: vec![AggDescriptor::COUNT_STAR],
                global_ground: true,
            },
            "a global-ground reduce over a non-empty group set",
        ),
    ];
    for (op, want) in cases {
        let mut c = Circuit::default();
        let input = c.push(scan(7), &[]).unwrap();
        assert_eq!(c.push(op.clone(), &[input]).unwrap_err(), want);
        assert_eq!(c.nodes().len(), 1, "a refused push appends nothing");
        let decoded = Circuit::decode(&cell(&[(scan(7), &[]), (op, &[0])])).unwrap_err();
        assert_eq!(decoded, format!("circuit: {want}"));
    }
}

/// The reduce output key: the source PK list keys on itself, and so does a
/// single NOT NULL PK-eligible column (eligibility is `TypeCode::is_pk_eligible`'s,
/// swept there); anything else — no column, a nullable, float or string column,
/// a partial, reordered or wider set — folds into a synthetic key.
#[test]
#[allow(clippy::type_complexity)]
fn for_group_cols_picks_the_output_key() {
    use crate::TypeCode as T;
    use ReduceOutKey::*;
    // (pk, group, (type code, nullable), key)
    let rows: &[(&[u32], &[u32], (TypeCode, bool), ReduceOutKey)] = &[
        (&[0], &[], (T::U64, false), SyntheticFold),
        (&[0], &[1], (T::U64, true), SyntheticFold),
        (&[0], &[1], (T::U64, false), Natural),
        (&[0], &[1], (T::F64, false), SyntheticFold),
        (&[0], &[1], (T::String, false), SyntheticFold),
        (&[0], &[0], (T::I64, false), Natural),
        (&[0, 1], &[0, 1], (T::I64, false), Natural),
        (&[0, 1], &[1, 0], (T::I64, false), SyntheticFold),
        (&[0, 1], &[0], (T::U64, false), Natural),
        (&[0, 1], &[0, 1, 2], (T::U64, false), SyntheticFold),
        (&[0, 1], &[1, 2], (T::U64, false), SyntheticFold),
    ];
    for (pk, group, col, want) in rows {
        assert_eq!(
            ReduceOutKey::for_group_cols(pk, group, |_| *col),
            *want,
            "{pk:?} {group:?} {col:?}"
        );
    }
}

#[test]
fn output_layout_is_the_key_region_then_the_unspelled_row() {
    use ReduceOutKey::*;
    use ReduceOutSlot::{Carried, Key, SyntheticKey};
    // The fold key spells no input column, so the whole row rides behind it —
    // group columns included, since the synthetic key is not one of them.
    assert_eq!(
        SyntheticFold.output_layout(&[2], 0..4),
        vec![SyntheticKey, Carried(0), Carried(1), Carried(2), Carried(3)]
    );
    // A natural key column is in the PK region, so it is not carried again.
    assert_eq!(
        Natural.output_layout(&[2], 0..4),
        vec![Key(2), Carried(0), Carried(1), Carried(3)]
    );
    // The PK list keys on every PK column.
    assert_eq!(
        Natural.output_layout(&[0, 2], 0..4),
        vec![Key(0), Key(2), Carried(1), Carried(3)]
    );
    // A fold's row is its group set, a repeated column carried once per occurrence.
    assert_eq!(
        SyntheticFold.output_layout(&[1, 1], [1, 1]),
        vec![SyntheticKey, Carried(1), Carried(1)]
    );
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

/// `push` is the graph's only constructor: an input count off the operator's
/// arity, or an input naming a node not yet pushed, is refused.
#[test]
fn push_refuses_an_arity_mismatch_and_a_forward_input() {
    let mut c = Circuit::default();
    let scan = c.push(scan(7), &[]).unwrap();
    assert_eq!(scan, 0);
    assert_eq!(
        c.push(OpNode::Union, &[scan]).unwrap_err(),
        "node's inputs do not match its operator's arity"
    );
    assert_eq!(
        c.push(OpNode::Negate, &[1]).unwrap_err(),
        "a node's input is not an earlier node"
    );
    assert_eq!(
        c.push(OpNode::Negate, &[scan]),
        Ok(1),
        "an input naming an earlier node is accepted"
    );
    assert_eq!(c.nodes().len(), 2, "a refused push appends nothing");
    assert_eq!(c.nodes()[1].inputs(), [scan]);
    assert_eq!(c.nodes()[0].inputs(), [] as [NodeId; 0]);

    // Slot 1 is filled solely by a binary operator.
    let reduce = OpNode::Reduce {
        group_cols: vec![0],
        agg: vec![AggDescriptor::COUNT_STAR],
        global_ground: false,
    };
    assert_eq!(
        c.push(reduce, &[0, 1]).unwrap_err(),
        "node's inputs do not match its operator's arity"
    );
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

/// A reduce over no group columns owes its ground row behind an exchange and not
/// as each worker's local fold; a grouped one never does.
#[test]
fn the_reduce_builders_derive_the_ground_row() {
    let ground = |c: &Circuit, id: NodeId| match c.nodes()[id].op {
        OpNode::Reduce { global_ground, .. } => global_ground,
        ref op => panic!("{op:?}"),
    };
    let mut c = Circuit::default();
    let input = c.input_delta(7, crate::ReadBound::None);
    let aggs = [AggDescriptor::COUNT_STAR];
    let global = c.reduce_multi(input, &[], &aggs);
    let grouped = c.reduce_multi(input, &[1], &aggs);
    let local = c.reduce_multi_local(input, &[], &aggs);
    assert_eq!(
        [ground(&c, global), ground(&c, grouped), ground(&c, local)],
        [true, false, false]
    );
}

/// COUNT is always I64, and MIN/MAX select an existing row, so they keep the
/// source type. A SUM needs a scalar register, and a calendar value does not add.
#[test]
fn agg_output_type_is_count_i64_min_max_identity_and_sum_widening() {
    use TypeCode::*;
    for &tc in TypeCode::ALL {
        assert_eq!(agg_output_type(AggFunc::Count, tc), Some(I64), "{tc}");
        assert_eq!(agg_output_type(AggFunc::Min, tc), Some(tc), "{tc}");
        assert_eq!(agg_output_type(AggFunc::Max, tc), Some(tc), "{tc}");
    }
    for (src, want) in [
        (F64, Some(F64)),
        (I32, Some(I64)),
        (F32, Some(F64)),
        // The i64 accumulator's bit pattern is the correct unsigned sum, so a
        // downstream unsigned compare re-seeds right.
        (U64, Some(U64)),
        (Decimal, Some(Decimal)),
        (String, None),
        (Blob, None),
        (U128, None),
        (Date, None),
        (Timestamp, None),
    ] {
        assert_eq!(agg_output_type(AggFunc::Sum, src), want, "SUM over {src}");
    }
}

/// A partial's merge aggregate keeps the partial's type.
#[test]
fn agg_merge_preserves_output_type() {
    for &f in AggFunc::ALL {
        let merge = f.merge_op();
        for &tc in TypeCode::ALL {
            let Some(out) = agg_output_type(f, tc) else {
                continue;
            };
            assert_eq!(agg_output_type(merge, out), Some(out), "{f:?} over {tc:?}");
        }
    }
}

/// COUNT and SUM never render NULL; MIN/MAX do over a nullable source or with no group.
#[test]
fn raw_output_nullable_matches_emit_semantics() {
    for f in [AggFunc::Count, AggFunc::CountNonNull, AggFunc::Sum] {
        for src_nullable in [false, true] {
            for ungrouped in [false, true] {
                assert!(!f.raw_output_nullable(src_nullable, ungrouped), "{f:?}");
            }
        }
    }
    for f in [AggFunc::Min, AggFunc::Max] {
        assert!(!f.raw_output_nullable(false, false), "{f:?} grouped, non-nullable");
        assert!(f.raw_output_nullable(true, false), "{f:?} grouped, nullable");
        assert!(f.raw_output_nullable(false, true), "{f:?} global, non-nullable");
        assert!(f.raw_output_nullable(true, true), "{f:?} global, nullable");
    }
}
