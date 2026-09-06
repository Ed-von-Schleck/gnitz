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
        Opcode::Filter => OpNode::Filter(Some(vec![1, 2, 3, 4])),
        Opcode::Negate => OpNode::Negate,
        Opcode::Union => OpNode::Union,
        Opcode::Join => OpNode::Join(JoinKind::DeltaTraceRange { n_eq: 3, rel: RangeRel::Le }),
        Opcode::Integrate => OpNode::IntegrateSink,
        Opcode::Reduce => OpNode::Reduce {
            group_cols: vec![2, 7],
            agg: vec![agg(AggFunc::Min, 4), agg(AggFunc::Sum, 1)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        },
        Opcode::Distinct => OpNode::Distinct,
        // A non-ascending index column list: `PkColList`'s `PartialEq` spans the
        // whole backing array, so a reordered list fails the round-trip.
        Opcode::ScanDelta => OpNode::ScanDelta {
            source: 42,
            bound: Some(ScanBound {
                idx_cols: crate::PkColList::from_slice(&[9, 3, 5]),
                desc: crate::RangeDescriptor::new(&[7, 11], crate::Cut::After(4), crate::Cut::Before(90)),
            }),
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
    }
}

/// Re-decode a node through the row fields `encode_op_node` produces.
fn roundtrip(op: OpNode) -> Result<OpNode, String> {
    let (opcode, src_tab, params) = encode_op_node(op);
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
        OpNode::ScanDelta { source: 7, bound: None },
        OpNode::Filter(None),
        OpNode::Map(MapKind::Projection(vec![])),
        OpNode::Map(MapKind::Compute(ComputeMap { program: vec![9, 9], out_cols: vec![] })),
        OpNode::Map(MapKind::HashRow { cols: vec![(3, None)], branch_id: 1 }),
        OpNode::Reduce {
            group_cols: vec![],
            agg: vec![agg(AggFunc::Count, 0)],
            global_ground: true,
            out_key: ReduceOutKey::SyntheticFold,
        },
    ]);
    for &role in ReindexRole::ALL {
        nodes.push(OpNode::Map(MapKind::Reindex {
            keep: vec![0],
            key: vec![(2, None), (5, Some(TypeCode::I64))],
            role,
        }));
    }
    // One reduce per aggregate, at a distinct source column, and one per output
    // key kind — the two enums a REDUCE row carries.
    for (i, &func) in AggFunc::ALL.iter().enumerate() {
        nodes.push(OpNode::Reduce {
            group_cols: vec![2, 7],
            agg: vec![agg(func, i as u32)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        });
    }
    for &out_key in ReduceOutKey::ALL {
        nodes.push(OpNode::Reduce {
            group_cols: vec![4],
            agg: vec![agg(AggFunc::Min, 4), agg(AggFunc::Sum, 1)],
            global_ground: false,
            out_key,
        });
    }
    // Every join kind, and every relation the range kind can carry — `Join`'s
    // own sample can only be one of them.
    nodes.extend([
        OpNode::Join(JoinKind::DeltaTrace),
        OpNode::Join(JoinKind::DeltaTraceCross),
    ]);
    for &rel in RangeRel::ALL {
        nodes.push(OpNode::Join(JoinKind::DeltaTraceRange { n_eq: 3, rel }));
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
        let (opcode, ..) = encode_op_node(sample(op));
        assert_eq!(opcode, op, "{op:?} encodes under {opcode:?}");
    }
}

/// A `params` cell one byte short, or one long, is a layout disagreement. Every
/// opcode is swept: a layout reading a fixed prefix and ignoring the rest would
/// pass the round-trip and fail here. `ScanDelta` is the deliberate exception,
/// asserted the other way — a damaged access hint degrades to `bound: None`.
#[test]
fn a_truncated_or_over_long_params_cell_is_rejected() {
    for &op in Opcode::ALL {
        let (_, src_tab, params) = encode_op_node(sample(op));
        let mut over_long = params.clone().unwrap_or_default();
        over_long.push(0);
        let decode = |bytes: &[u8]| decode_op_node(op.as_wire(), src_tab, Some(bytes));
        if op == Opcode::ScanDelta {
            for bytes in [&params.as_ref().unwrap()[..1], over_long.as_slice()] {
                assert_eq!(
                    decode(bytes).unwrap(),
                    OpNode::ScanDelta { source: 42, bound: None },
                    "a damaged scan bound must degrade, not fail",
                );
            }
            continue;
        }
        assert!(decode(&over_long).is_err(), "{op:?}: a trailing byte must be rejected");
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

/// A promoted target that is not PK-eligible is rejected at the decode trust
/// boundary (here: a float code), not silently mis-strided. `MAP_HASH_ROW`
/// promotes payload rather than key bytes, so its domain is stricter still —
/// `is_pk_eligible` admits the 16-byte types its `copy_column` widen cannot hold.
#[test]
fn decode_rejects_an_out_of_domain_promotion_target() {
    let reindex = |tc: u8| {
        let mut w = Vec::new();
        w.push(ReindexRole::ScatterKey.as_wire() as u8);
        w.extend(1u16.to_le_bytes()); // one key column
        w.extend(3u32.to_le_bytes());
        w.push(tc);
        w.extend(0u16.to_le_bytes()); // no kept columns
        w
    };
    let err = decode_op_node(
        Opcode::MapReindex.as_wire(),
        None,
        Some(&reindex(crate::type_code::F64)),
    )
    .unwrap_err();
    assert!(err.contains("not PK-eligible"), "got: {err}");

    let hash_row = |tc: u8| {
        let mut w = vec![0u8]; // branch id
        w.extend(1u16.to_le_bytes());
        w.extend(3u32.to_le_bytes());
        w.push(tc);
        w
    };
    let err = decode_op_node(
        Opcode::MapHashRow.as_wire(),
        None,
        Some(&hash_row(crate::type_code::U128)),
    )
    .unwrap_err();
    assert!(err.contains("fixed-width integer"), "got: {err}");

    // An undecodable code is the same refusal, not a decode to "no target".
    let err = decode_op_node(Opcode::MapReindex.as_wire(), None, Some(&reindex(200))).unwrap_err();
    assert!(err.contains("not PK-eligible"), "got: {err}");
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

/// The ground row is written at payload index 0, so a group set would leave
/// its exemplar slots short — the cross-check belongs where the two fields
/// arrive, which is here rather than in each consumer. A spec-less REDUCE is
/// refused at the same boundary: every producer ships at least one.
#[test]
fn reduce_rejects_a_ground_group_set_and_an_empty_agg_list() {
    let reduce = |global_ground, group_cols: Vec<u32>, agg: Vec<AggDescriptor>| {
        let (_, _, params) = encode_op_node(OpNode::Reduce {
            group_cols,
            agg,
            global_ground,
            out_key: ReduceOutKey::SyntheticFold,
        });
        decode_op_node(Opcode::Reduce.as_wire(), None, params.as_deref())
    };
    let count = || vec![agg(AggFunc::Count, 0)];
    assert!(reduce(true, vec![], count()).is_ok(), "group-less ground is valid");
    assert!(reduce(false, vec![3], count()).is_ok(), "a grouped reduce is valid");
    assert!(reduce(true, vec![3], count()).unwrap_err().contains("global-ground"));
    assert!(reduce(false, vec![3], vec![])
        .unwrap_err()
        .contains("no aggregate spec"));
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
    assert!(decode(params(ReindexRole::Auxiliary.as_wire() as u8, 0))
        .unwrap_err()
        .contains("names no key columns"));
}

/// An unbounded `ScanDelta` writes no parameters at all: "absent" and "empty"
/// must stay distinguishable, so the common shape costs nothing.
#[test]
fn unbounded_scan_delta_encodes_identically() {
    let fields = encode_op_node(OpNode::ScanDelta { source: 7, bound: None });
    assert_eq!(fields, (Opcode::ScanDelta, Some(7), None));
    assert_eq!(
        decode_op_node(Opcode::ScanDelta.as_wire(), Some(7), None).unwrap(),
        OpNode::ScanDelta { source: 7, bound: None }
    );
}

/// Every malformed-hint shape degrades to `bound: None` — no panic, no `Err`.
/// An `Err` here would abort the whole view load over a physical access hint.
#[test]
fn malformed_scan_bound_degrades_to_none() {
    let desc = crate::RangeDescriptor::new(&[1], crate::Cut::Before(0), crate::Cut::After(9)).encode();
    let with_word = |word: u64, tail: &[u8]| {
        let mut p = word.to_le_bytes().to_vec();
        p.extend_from_slice(tail);
        p
    };
    let cases: &[(&str, Vec<u8>)] = &[
        // A column-list word whose count is past the arity cap — `as_slice` would
        // silently truncate it and `from_slice` would panic.
        ("over-long list", with_word(crate::PK_LIST_PACKED_FLAG | 7, &desc)),
        // A bounded node whose descriptor never arrived.
        ("missing descriptor", with_word(crate::pack_pk_cols(&[1]), &[])),
        // A descriptor that fails `RangeDescriptor::decode`'s validation.
        (
            "undecodable descriptor",
            with_word(crate::pack_pk_cols(&[1]), &[0xff, 0xff, 0xff]),
        ),
        // Nothing at all: a present but empty cell is damaged, not absent.
        ("empty cell", Vec::new()),
    ];
    for (what, params) in cases {
        assert_eq!(
            decode_op_node(Opcode::ScanDelta.as_wire(), Some(3), Some(params)).unwrap(),
            OpNode::ScanDelta { source: 3, bound: None },
            "{what} must degrade to an unbounded scan"
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
