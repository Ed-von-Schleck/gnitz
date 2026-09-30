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
            kind: JoinKind::Range { n_eq: 3, rel: RangeRel::Le },
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
        Opcode::IntegrateTrace => OpNode::IntegrateTrace,
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
            key: vec![(2, TypeCode::I64), (5, TypeCode::I64)],
            role: ReindexRole::ScatterKey {
                source: 4,
                source_key: vec![(1, TypeCode::I64), (6, TypeCode::I64)],
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

/// Re-decode a node through the row fields `encode_op_node` produces.
fn roundtrip(op: OpNode) -> Result<OpNode, String> {
    let (opcode, src_tab, params) = encode_op_node(&op);
    decode_op_node(opcode.as_wire(), src_tab, params.as_deref())
}

/// Every `OpNode` shape survives `encode_op_node` → `decode_op_node`. This is
/// the crate's largest codec and the only one whose bugs land in a persisted
/// circuit, so the variant set is swept rather than sampled: the opcode space and
/// every wire enum the encoder embeds are driven from their own `ALL`, so a
/// variant added to any of them is round-tripped without a second edit here.
#[test]
fn every_op_node_variant_roundtrips() {
    let mut nodes: Vec<OpNode> = Opcode::ALL.iter().map(|&op| sample(op)).collect();
    // The shapes an opcode's own sample cannot also be: an absent parameter cell,
    // the other bound kind, and the empty lists each counted layout allows.
    nodes.extend([
        OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None },
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
        // which frames fine: refusing it is `TopNPlan::from_wire`'s.
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
                kind: JoinKind::Range { n_eq: 3, rel },
                delta_is_right,
            });
        }
        nodes.push(OpNode::NullExtend {
            type_codes: vec![TypeCode::I64],
            nulls_first: delta_is_right,
        });
    }
    for node in nodes {
        assert_eq!(roundtrip(node.clone()).unwrap(), node, "round-trip failed for {node:?}");
    }
}

/// Each opcode's sample encodes under that opcode — so encode and decode cannot
/// agree on a permuted opcode table — and a row differing from what
/// `encode_op_node` writes is refused: the `source_table` cell's presence
/// flipped, or a params cell one byte short, one long, or present and empty. A
/// layout reading a fixed prefix and ignoring the rest would pass the round-trip
/// and fail here.
#[test]
fn each_opcode_row_refuses_every_perturbation() {
    for &op in Opcode::ALL {
        let (opcode, src_tab, params) = encode_op_node(&sample(op));
        assert_eq!(opcode, op, "{op:?} encodes under {opcode:?}");
        let decode = |src, p: Option<&[u8]>| decode_op_node(op.as_wire(), src, p);
        let flipped = if src_tab.is_some() { None } else { Some(3) };
        assert!(
            decode(flipped, params.as_deref()).is_err(),
            "{op:?}: source_table flipped"
        );
        let mut over_long = params.clone().unwrap_or_default();
        over_long.push(0);
        assert!(decode(src_tab, Some(&over_long)).is_err(), "{op:?}: trailing byte");
        assert!(decode(src_tab, Some(&[])).is_err(), "{op:?}: empty cell");
        if let Some(p) = &params {
            assert!(decode(src_tab, Some(&p[..p.len() - 1])).is_err(), "{op:?}: truncated");
        }
    }
    let unknown = (0..).find(|&o| Opcode::from_wire(o).is_none()).unwrap();
    assert!(decode_op_node(unknown, None, None)
        .unwrap_err()
        .contains("unknown opcode"));
}

/// Every params guard, against the forgery that trips it: a well-framed cell
/// whose content the decoder refuses rather than defaults. Most are shapes the
/// infallible encoder writes as given; the rest are one byte poked into an
/// encoded sample. A bound's own guards are `read_spec`'s and `range`'s.
#[test]
fn each_params_guard_rejects_its_own_forgery() {
    let row = |op: &OpNode| {
        let (c, s, p) = encode_op_node(op);
        (c, s, p.unwrap())
    };
    let poke = |op: &OpNode, off: usize, v: u8| {
        let (c, s, mut p) = row(op);
        p[off] = v;
        (c, s, p)
    };
    let key = vec![(3, TypeCode::I64)];
    let reindex = |role, key| {
        OpNode::Map(MapKind::Reindex {
            keep: vec![],
            key,
            role,
            nulls: NullKeys::Keep,
        })
    };
    // role 0 | nulls 1 | key count 2..4 | col 4..8 | type 8
    let aux = reindex(ReindexRole::Auxiliary, key.clone());
    // count 0..2 | col 2..6 | type 6
    let hash_row = OpNode::Map(MapKind::HashRow { cols: key.clone() });
    // nulls_first 0 | count 1..3 | type 3
    let null_ext = OpNode::NullExtend {
        type_codes: vec![TypeCode::I64],
        nulls_first: false,
    };
    let bad_rel = (0..=u8::MAX).find(|&b| RangeRel::from_wire(b).is_none()).unwrap();
    let scan = |cell: Vec<u8>| (Opcode::ScanDelta, Some(3), cell);
    let cases = [
        (poke(&aux, 0, 99), "unknown route-key role"),
        (poke(&aux, 1, 7), "unknown NULL-key rule"),
        (poke(&aux, 8, 0), "invalid type code 0"),
        (poke(&aux, 8, 200), "invalid type code 200"),
        (poke(&hash_row, 6, 0), "invalid type code 0"),
        (poke(&null_ext, 3, 200), "invalid type code 200"),
        (poke(&sample(Opcode::JoinRange), 2, bad_rel), "JOIN unknown rel"),
        (row(&reindex(ReindexRole::Auxiliary, vec![])), "names no key columns"),
        // A stated route with no source columns would scatter the whole
        // relation onto one worker.
        (
            row(&reindex(
                ReindexRole::ScatterKey { source: 7, source_key: vec![] },
                key.clone(),
            )),
            "scatter key names no source columns",
        ),
        (
            row(&OpNode::Map(MapKind::HashRow { cols: vec![] })),
            "MAP_HASH_ROW names no columns",
        ),
        // A ground-seeding reduce groups on nothing.
        (
            row(&OpNode::Reduce {
                group_cols: vec![0],
                agg: vec![AggDescriptor::COUNT_STAR],
                global_ground: true,
            }),
            "global-ground over a non-empty group set",
        ),
        // The encoder is infallible; the decode refuses by the count before
        // reading the body.
        (
            row(&OpNode::Map(MapKind::Projection(
                (0..=crate::MAX_COLUMNS as u32).collect(),
            ))),
            "exceeds cap",
        ),
        // "No bound" is spelled by the absent cell alone.
        (scan(vec![0]), "carries no params cell"),
        // A malformed backfill hint is catalog corruption, not a wider scan.
        (scan(vec![9]), "unknown bound kind"),
    ];
    for ((op, src, params), want) in cases {
        let err = decode_op_node(op.as_wire(), src, Some(&params)).unwrap_err();
        assert!(err.contains(want), "{op:?}: {err:?} does not name {want:?}");
    }
    assert!(decode_op_node(Opcode::ScanDelta.as_wire(), None, None)
        .unwrap_err()
        .contains("missing source_table"));
    let unbounded = OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None };
    assert_eq!(encode_op_node(&unbounded), (Opcode::ScanDelta, Some(7), None));
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
    let scan = c
        .push(
            OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None },
            NodeInputs::Source,
        )
        .unwrap();
    assert_eq!(scan, 0);
    assert_eq!(
        c.push(OpNode::Union, NodeInputs::Unary(scan)).unwrap_err(),
        "node's inputs do not match its operator's arity"
    );
    assert_eq!(
        c.push(OpNode::Negate, NodeInputs::Unary(1)).unwrap_err(),
        "a node's input is not an earlier node"
    );
    assert_eq!(
        c.push(OpNode::Negate, NodeInputs::Unary(scan)),
        Ok(1),
        "an input naming an earlier node is accepted"
    );
    assert_eq!(c.nodes().len(), 2, "a refused push appends nothing");

    // Slot 1 is filled solely by a binary join: a forged bundle filling it on a
    // reduce would hand it a delta register with no `Integrate` behind it.
    let reduce = OpNode::Reduce {
        group_cols: vec![0],
        agg: vec![AggDescriptor::COUNT_STAR],
        global_ground: false,
    };
    assert_eq!(
        c.push(reduce, NodeInputs::Binary { a: 0, b: 1 }).unwrap_err(),
        "node's inputs do not match its operator's arity"
    );
}

/// The client's id substitution moves every relation a circuit names: each
/// scan's source and each scatter key's route.
#[test]
fn sources_mut_yields_every_named_relation() {
    let mut c = Circuit::default();
    let scan = c
        .push(
            OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None },
            NodeInputs::Source,
        )
        .unwrap();
    let scatter = c.push(sample(Opcode::MapReindex), NodeInputs::Unary(scan)).unwrap();
    let aux = OpNode::Map(MapKind::Reindex {
        keep: vec![],
        key: vec![(0, TypeCode::I64)],
        role: ReindexRole::Auxiliary,
        nulls: NullKeys::Keep,
    });
    c.push(aux, NodeInputs::Unary(scatter)).unwrap();
    assert_eq!(c.sources_mut().map(|s| *s).collect::<Vec<_>>(), [7, 4]);
}

/// A row's slots round-trip through `NodeInputs`, and a slot 1 filled behind an
/// empty slot 0 is no operator's wiring.
#[test]
fn from_slots_is_to_slots_inverse_and_refuses_a_trailing_slot() {
    for inputs in [
        NodeInputs::Source,
        NodeInputs::Unary(3),
        NodeInputs::Binary { a: 1, b: 2 },
    ] {
        assert_eq!(NodeInputs::from_slots(inputs.to_slots()), Ok(inputs));
    }
    assert_eq!(
        NodeInputs::from_slots([None, Some(0)]).unwrap_err(),
        "node's inputs do not match its operator's arity"
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
