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
        Opcode::JoinEqui => OpNode::Join(JoinKind::Equi),
        Opcode::JoinRange => OpNode::Join(JoinKind::Range { n_eq: 3, rel: RangeRel::Le }),
        Opcode::JoinCross => OpNode::Join(JoinKind::Cross),
        Opcode::IntegrateSink => OpNode::IntegrateSink,
        Opcode::Reduce => OpNode::Reduce {
            group_cols: vec![2, 7],
            agg: vec![agg(AggFunc::Min, 4), agg(AggFunc::Sum, 1)],
            global_ground: false,
        },
        Opcode::Distinct => OpNode::Distinct,
        // A non-ascending index column list: `PkColList`'s `PartialEq` spans the
        // whole backing array, so a reordered list fails the round-trip.
        Opcode::ScanDelta => OpNode::ScanDelta {
            source: 42,
            bound: crate::ReadBound::IndexRange {
                bound: crate::IndexBound {
                    idx_cols: crate::PkColList::from_slice(&[9, 3, 5]),
                    desc: crate::RangeDescriptor::new(&[7, 11], crate::Cut::After(4), crate::Cut::Before(90)),
                },
                walk: crate::IndexWalk::Optional,
            },
        },
        Opcode::ExchangeShard => OpNode::ExchangeShard { shard_cols: vec![0, 2] },
        Opcode::NullExtend => OpNode::NullExtend {
            type_codes: vec![crate::type_code::I64, crate::type_code::STRING],
        },
        Opcode::IntegrateTrace => OpNode::IntegrateTrace,
        Opcode::MapProj => OpNode::Map(MapKind::Projection(vec![4, 0, 9])),
        Opcode::MapExpr => OpNode::Map(MapKind::Compute(ComputeMap {
            program: vec![7],
            out_cols: vec![(crate::type_code::I64, false), (crate::type_code::STRING, true)],
        })),
        Opcode::MapHashRow => OpNode::Map(MapKind::HashRow {
            cols: vec![(1, None), (2, Some(TypeCode::I32))],
            branch_id: 0,
        }),
        Opcode::WorkerFilter => OpNode::WorkerFilter,
        Opcode::PositivePart => OpNode::PositivePart,
        Opcode::MapReindex => OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, None), (5, Some(TypeCode::I64))],
            role: ReindexRole::ScatterKey,
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
    // and the empty lists each counted layout allows.
    nodes.extend([
        OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None },
        OpNode::Map(MapKind::Projection(vec![])),
        OpNode::Map(MapKind::Compute(ComputeMap { program: vec![9, 9], out_cols: vec![] })),
        OpNode::Map(MapKind::HashRow { cols: vec![(3, None)], branch_id: 1 }),
        OpNode::Reduce {
            group_cols: vec![],
            agg: vec![agg(AggFunc::Count, 0)],
            global_ground: true,
        },
    ]);
    // The global shape: no group, one key, no offset.
    nodes.push(OpNode::TopN {
        group_cols: vec![],
        order: vec![crate::OrderKey { col: 2, desc: false, nulls_first: false }],
        limit: 1,
        offset: 0,
    });
    for &role in ReindexRole::ALL {
        nodes.push(OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, None), (5, Some(TypeCode::I64))],
            role,
        }));
    }
    // One reduce per aggregate, at a distinct source column.
    for (i, &func) in AggFunc::ALL.iter().enumerate() {
        nodes.push(OpNode::Reduce {
            group_cols: vec![2, 7],
            agg: vec![agg(func, i as u32)],
            global_ground: false,
        });
    }
    // Every join kind, and every relation the range kind can carry — `Join`'s
    // own sample can only be one of them.
    nodes.extend([OpNode::Join(JoinKind::Equi), OpNode::Join(JoinKind::Cross)]);
    for &rel in RangeRel::ALL {
        nodes.push(OpNode::Join(JoinKind::Range { n_eq: 3, rel }));
    }
    for node in nodes {
        assert_eq!(roundtrip(node.clone()).unwrap(), node, "round-trip failed for {node:?}");
    }
}

/// The opcode an `OpNode` encodes under is the one its sample was drawn from —
/// otherwise the sweep above could round-trip every node while two opcodes
/// aliased one layout.
#[test]
fn each_sample_encodes_under_its_own_opcode() {
    for &op in Opcode::ALL {
        let (opcode, ..) = encode_op_node(&sample(op));
        assert_eq!(opcode, op, "{op:?} encodes under {opcode:?}");
    }
}

/// A `params` cell one byte short, one long, or present and empty, is a layout
/// disagreement. Every opcode is swept: a layout reading a fixed prefix and
/// ignoring the rest would pass the round-trip and fail here.
#[test]
fn a_truncated_or_over_long_params_cell_is_rejected() {
    for &op in Opcode::ALL {
        let (_, src_tab, params) = encode_op_node(&sample(op));
        let mut over_long = params.clone().unwrap_or_default();
        over_long.push(0);
        let decode = |bytes: &[u8]| decode_op_node(op.as_wire(), src_tab, Some(bytes));
        assert!(decode(&over_long).is_err(), "{op:?}: a trailing byte must be rejected");
        assert!(decode(&[]).is_err(), "{op:?}: an empty cell must be rejected");
        if let Some(p) = &params {
            assert!(
                decode(&p[..p.len() - 1]).is_err(),
                "{op:?}: a truncated cell must be rejected",
            );
        }
    }
}

/// A SCAN node with a NULL `source_table` cell is a corrupt circuit: reject it at
/// the decode trust boundary rather than coercing to the (invalid) table id 0.
#[test]
fn decode_rejects_scan_missing_source_table() {
    assert!(decode_op_node(Opcode::ScanDelta.as_wire(), None, None)
        .unwrap_err()
        .contains("source_table"));
}

/// An opcode outside the space is a corrupt circuit, not a node to skip.
#[test]
fn decode_rejects_an_unknown_opcode() {
    assert!(decode_op_node(9999, None, None).unwrap_err().contains("unknown opcode"));
}

/// Which promotion targets a key slot admits is the plan builders' rule, which
/// hold the source schema: an out-of-domain target decodes. An undecodable byte
/// is refused on both, not decoded to "no target".
#[test]
fn decode_rejects_an_out_of_domain_reindex_target() {
    let reindex = |tc: u8| {
        let mut w = Vec::new();
        w.push(ReindexRole::ScatterKey.as_wire());
        w.extend(1u16.to_le_bytes()); // one key column
        w.extend(3u32.to_le_bytes());
        w.push(tc);
        w.extend(0u16.to_le_bytes()); // no kept columns
        w
    };
    let hash_row = |tc: u8| {
        let mut w = vec![0u8]; // branch id
        w.extend(1u16.to_le_bytes());
        w.extend(3u32.to_le_bytes());
        w.push(tc);
        w
    };
    assert!(
        decode_op_node(
            Opcode::MapHashRow.as_wire(),
            None,
            Some(&hash_row(crate::type_code::U128)),
        )
        .is_ok(),
        "a hash-row target's domain is the copy kernel's, not this decode's",
    );

    // An undecodable code is a refusal on both, not a decode to "no target".
    for (op, params) in [(Opcode::MapReindex, reindex(200)), (Opcode::MapHashRow, hash_row(200))] {
        let err = decode_op_node(op.as_wire(), None, Some(&params)).unwrap_err();
        assert!(err.contains("unknown promotion type code"), "{op:?}: {err}");
    }
}

/// A NULL_EXTEND type code becomes a schema column verbatim. An undecodable
/// one is not inert — `wire_stride` reports 8 for it, so it clears any width
/// test, and the schema it lands in becomes every downstream node's input.
#[test]
fn decode_rejects_invalid_null_extend_type_code() {
    let mut params = 1u16.to_le_bytes().to_vec();
    params.push(200);
    let err = decode_op_node(Opcode::NullExtend.as_wire(), None, Some(&params)).unwrap_err();
    assert!(err.contains("invalid column type code"), "got: {err}");
}

/// A `MAP_REINDEX` whose role cannot be read decides nothing about routing, so
/// it must fail the decode rather than default. (Contrast a `ScanDelta` bound,
/// which decides only scan speed and therefore degrades instead of erroring.) A
/// node naming no key columns is refused for the same reason: it re-keys onto
/// nothing.
#[test]
fn a_map_reindex_whose_role_or_key_is_unusable_is_rejected() {
    let params = |role: u8, n_keys: u16| {
        let mut w = vec![role];
        w.extend(n_keys.to_le_bytes());
        for _ in 0..n_keys {
            w.extend(3u32.to_le_bytes());
            w.push(0);
        }
        w.extend(0u16.to_le_bytes()); // no kept columns
        w
    };
    let decode = |p: Vec<u8>| decode_op_node(Opcode::MapReindex.as_wire(), None, Some(&p));
    assert!(decode(params(99, 1)).unwrap_err().contains("unknown route-key role"));
    assert!(decode(params(ReindexRole::Auxiliary.as_wire(), 0))
        .unwrap_err()
        .contains("names no key columns"));
}

/// An unbounded `ScanDelta` writes no parameters at all: "absent" and "empty"
/// must stay distinguishable, so the common shape costs nothing.
#[test]
fn unbounded_scan_delta_encodes_identically() {
    let fields = encode_op_node(&OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None });
    assert_eq!(fields, (Opcode::ScanDelta, Some(7), None));
    assert_eq!(
        decode_op_node(Opcode::ScanDelta.as_wire(), Some(7), None).unwrap(),
        OpNode::ScanDelta { source: 7, bound: crate::ReadBound::None }
    );
}

/// Every kind of read bound survives as a backfill hint, the walk byte included.
#[test]
fn every_scan_bound_kind_roundtrips() {
    let desc = crate::RangeDescriptor::new(&[3], crate::Cut::Before(1), crate::Cut::After(9));
    let keys = crate::PkKeys::from_keys(8, [&7u64.to_be_bytes()[..], &2u64.to_be_bytes()[..]]);
    for bound in [
        crate::ReadBound::PkRange(desc),
        crate::ReadBound::PkSet(keys),
        crate::ReadBound::IndexRange {
            bound: crate::IndexBound {
                idx_cols: crate::PkColList::from_slice(&[2, 1]),
                desc,
            },
            walk: crate::IndexWalk::Required,
        },
    ] {
        let node = OpNode::ScanDelta { source: 7, bound };
        assert_eq!(roundtrip(node.clone()).unwrap(), node);
    }
}

/// A malformed backfill hint is catalog corruption, and every other opcode in
/// the same decoder rejects one — so this one does too, rather than silently
/// widening the initial scan.
#[test]
fn a_malformed_scan_bound_is_rejected() {
    let mut w = crate::codec::Writer::with_capacity(0);
    crate::range::write_range_descriptor(
        &mut w,
        &crate::RangeDescriptor::new(&[1], crate::Cut::Before(0), crate::Cut::After(9)),
    );
    let desc = w.into_vec();
    // An `IndexRange` cell: tag, column word, `tail`, then an Optional walk byte.
    let index_range = |word: u64, tail: &[u8]| {
        let mut p = vec![2u8];
        p.extend_from_slice(&word.to_le_bytes());
        p.extend_from_slice(tail);
        p.push(0);
        p
    };
    // A `PkSet` cell: tag, stride, count, then the key bytes.
    let pk_set = |stride: u8, count: u32, keys: &[u8]| {
        let mut p = vec![3u8, stride];
        p.extend_from_slice(&count.to_le_bytes());
        p.extend_from_slice(keys);
        p
    };
    let over_arity = {
        let n_eq = crate::catalog::PK_LIST_MAX_COLS;
        let mut p = vec![1u8, n_eq as u8, 0];
        p.resize(1 + 2 + 16 * (n_eq + 2), 0);
        p
    };
    let mut trailing = pk_set(1, 1, &[5]);
    trailing.push(0);
    let cap = crate::PkKeys::max_per_request(8) as u32;
    let cases: &[(&str, Vec<u8>)] = &[
        ("unknown bound tag", vec![9]),
        // A column-list word whose count is past the arity cap — `as_slice` would
        // silently truncate it and `from_slice` would panic.
        ("over-long list", index_range(crate::PK_LIST_PACKED_FLAG | 7, &desc)),
        // A word carrying no packed-list flag at all.
        ("untagged word", index_range(1, &desc)),
        // A bounded node whose descriptor never arrived.
        ("missing descriptor", {
            let mut p = vec![2u8];
            p.extend_from_slice(&crate::pack_pk_cols(&[1]).to_le_bytes());
            p
        }),
        // A descriptor that fails `RangeDescriptor::decode`'s validation.
        (
            "undecodable descriptor",
            index_range(crate::pack_pk_cols(&[1]), &[0xff, 0xff, 0xff]),
        ),
        ("zero stride", pk_set(0, 0, &[])),
        ("over-wide stride", pk_set(crate::MAX_PK_BYTES as u8 + 1, 0, &[])),
        ("count over the cap", pk_set(8, cap + 1, &[])),
        ("unsorted keys", pk_set(1, 2, &[5, 4])),
        ("pk range past the arity cap", over_arity),
        ("trailing bytes", trailing),
        // Nothing at all: a present but empty cell is damaged, not absent.
        ("empty cell", Vec::new()),
        // "No bound" is spelled by the absent cell alone.
        ("a cell spelling no bound", vec![0]),
    ];
    for (what, params) in cases {
        assert!(
            decode_op_node(Opcode::ScanDelta.as_wire(), Some(3), Some(params)).is_err(),
            "{what} must be rejected"
        );
    }
}

/// The reduce output key: a group set that is a permutation of the source PK
/// keys on it; a single NOT NULL natural-key column keys on itself; anything
/// else — a nullable or signed column, a partial or wider set — folds into a
/// synthetic key.
#[test]
#[allow(clippy::type_complexity)]
fn for_group_cols_picks_the_output_key() {
    use ReduceOutKey::*;
    let (u64, u128, uuid, i64) = (
        crate::TypeCode::U64 as u8,
        crate::TypeCode::U128 as u8,
        crate::TypeCode::UUID as u8,
        crate::TypeCode::I64 as u8,
    );
    // (pk, group, (type code, nullable), key)
    let rows: &[(&[u32], &[u32], (u8, bool), ReduceOutKey)] = &[
        (&[0], &[1], (u64, true), SyntheticFold),
        (&[0], &[1], (u64, false), SingleNaturalCol),
        (&[0], &[1], (uuid, false), SingleNaturalCol),
        (&[0], &[1], (u128, false), SingleNaturalCol),
        (&[0], &[1], (i64, false), SyntheticFold),
        (&[0], &[0], (i64, false), PkPermutation),
        (&[0, 1], &[1, 0], (i64, false), PkPermutation),
        (&[0, 1], &[0], (u64, false), SingleNaturalCol),
        (&[0, 1], &[0, 1, 2], (u64, false), SyntheticFold),
        (&[0, 1], &[1, 2], (u64, false), SyntheticFold),
    ];
    for (pk, group, col, want) in rows {
        assert_eq!(
            ReduceOutKey::for_group_cols(pk, group, |_| *col),
            *want,
            "{pk:?} {group:?} {col:?}"
        );
    }
}

/// A top-N ordering by more keys than a read may is refused at decode. A zero
/// limit frames fine: refusing it is `TopNPlan::from_wire`'s.
#[test]
fn top_n_rejects_too_many_keys() {
    let decode = |limit, n_keys| {
        let order = (0..n_keys)
            .map(|i| crate::OrderKey {
                col: i as u16,
                desc: false,
                nulls_first: false,
            })
            .collect();
        let (_, _, params) = encode_op_node(&OpNode::TopN {
            group_cols: vec![],
            order,
            limit,
            offset: 0,
        });
        decode_op_node(Opcode::TopN.as_wire(), None, params.as_deref())
    };
    assert!(decode(0, 1).is_ok());
    assert!(decode(1, crate::MAX_ORDER_KEYS + 1).unwrap_err().contains("order keys"));
    assert!(decode(1, crate::MAX_ORDER_KEYS).is_ok());
}

#[test]
fn output_layout_is_the_key_region_then_the_unspelled_row() {
    use ReduceOutKey::*;
    use ReduceOutSlot::{Carried, Key, SyntheticKey};
    // The fold key spells no input column, so the whole row rides behind it —
    // group columns included, since the synthetic key is not one of them.
    assert_eq!(
        SyntheticFold.output_layout(&[0], &[2], 0..4),
        vec![SyntheticKey, Carried(0), Carried(1), Carried(2), Carried(3)]
    );
    assert_eq!(
        SyntheticFold.output_layout(&[0], &[], 0..3),
        vec![SyntheticKey, Carried(0), Carried(1), Carried(2)]
    );
    // A natural key column is in the PK region, so it is not carried again.
    assert_eq!(
        SingleNaturalCol.output_layout(&[0], &[2], 0..4),
        vec![Key(2), Carried(0), Carried(1), Carried(3)]
    );
    // A permuted PK keys on every PK column whatever order the group set lists.
    assert_eq!(
        PkPermutation.output_layout(&[0, 2], &[2, 0], 0..4),
        vec![Key(0), Key(2), Carried(1), Carried(3)]
    );
    // A fold's row is its group set, a repeated column carried once per occurrence.
    assert_eq!(
        SyntheticFold.output_layout(&[0], &[1, 1], [1, 1]),
        vec![SyntheticKey, Carried(1), Carried(1)]
    );
}

/// `x OP y ⟺ y converse(OP) x`, and the converse is an involution.
#[test]
fn range_rel_converse_is_an_involution() {
    for r in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
        assert_ne!(r.converse(), r);
        assert_eq!(r.converse().converse(), r);
    }
    assert_eq!(RangeRel::Lt.converse(), RangeRel::Gt);
    assert_eq!(RangeRel::Le.converse(), RangeRel::Ge);
}

/// A list longer than the column cap encodes — the encoder is infallible — and
/// the decode refuses it by its count before reading the body.
#[test]
fn an_over_wide_list_encodes_and_is_refused_at_decode() {
    let wide: Vec<u32> = (0..crate::MAX_COLUMNS as u32 + 1).collect();
    let err = roundtrip(OpNode::Map(MapKind::Projection(wide))).unwrap_err();
    assert!(
        err.contains(&format!("exceeds cap {}", crate::MAX_COLUMNS)),
        "got: {err}"
    );
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
