use super::*;

fn reindex_cols_of(node: OpNode) -> Vec<u32> {
    match node {
        OpNode::Map(MapKind::Reindex { reindex_cols, .. }) => reindex_cols,
        other => panic!("expected Map(Expression), got {other:?}"),
    }
}

fn reindex_of(node: OpNode) -> (Vec<u32>, Vec<u8>) {
    match node {
        OpNode::Map(MapKind::Reindex {
            reindex_cols,
            reindex_target_tcs,
            ..
        }) => (reindex_cols, reindex_target_tcs),
        other => panic!("expected Map(Expression), got {other:?}"),
    }
}

/// MAP_EXPR reindex columns decode from NODE_COL_KIND_REINDEX rows in
/// position order (value1 = source column).
#[test]
fn decode_reindex_cols_from_kind_rows() {
    let cols = [
        CircuitNodeColumn {
            kind: NODE_COL_KIND_REINDEX,
            position: 0,
            value1: 3,
            value2: 0,
        },
        CircuitNodeColumn {
            kind: NODE_COL_KIND_REINDEX,
            position: 1,
            value1: 9,
            value2: 0,
        },
        CircuitNodeColumn {
            kind: NODE_COL_KIND_ROUTE_KEY,
            position: 0,
            value1: 1,
            value2: 0,
        },
    ];
    let node = decode_op_node(OPCODE_MAP_EXPR, None, Some(vec![1, 2, 3]), &cols).unwrap();
    assert_eq!(reindex_cols_of(node), vec![3, 9]);
}

/// A non-zero `value2` is the promoted key type code `T`, decoded parallel to
/// `reindex_cols`. A `value2 = 0` slot means "derive from source".
#[test]
fn decode_reindex_target_tcs_from_value2() {
    let cols = [
        CircuitNodeColumn {
            kind: NODE_COL_KIND_REINDEX,
            position: 0,
            value1: 3,
            value2: 0,
        }, // T = derive
        CircuitNodeColumn {
            kind: NODE_COL_KIND_REINDEX,
            position: 1,
            value1: 3,
            value2: crate::type_code::I64 as u64,
        }, // T = I64
        CircuitNodeColumn {
            kind: NODE_COL_KIND_ROUTE_KEY,
            position: 0,
            value1: 1,
            value2: 0,
        },
    ];
    let node = decode_op_node(OPCODE_MAP_EXPR, None, Some(vec![1, 2, 3]), &cols).unwrap();
    assert_eq!(reindex_of(node), (vec![3, 3], vec![0, crate::type_code::I64]));
}

/// Column order comes from `position`, not from the order the rows happen to
/// arrive in: a catalog cursor yields them in PK order, which is neither
/// kind- nor position-grouped. Shuffling the input must not change the
/// decoded list.
#[test]
fn decode_orders_lists_by_position_not_input_order() {
    let col = |kind: u64, position: u16, value1: u64| CircuitNodeColumn {
        kind,
        position,
        value1,
        value2: 0,
    };
    // Two interleaved lists, each in reverse position order.
    let shuffled = [
        col(NODE_COL_KIND_GROUP, 2, 30),
        col(NODE_COL_KIND_AGG_SPEC, 1, AGG_SUM),
        col(NODE_COL_KIND_GROUP, 0, 10),
        col(NODE_COL_KIND_AGG_SPEC, 0, AGG_COUNT),
        col(NODE_COL_KIND_GROUP, 1, 20),
    ];
    let node = decode_op_node(OPCODE_REDUCE, None, None, &shuffled).unwrap();
    match node {
        OpNode::Reduce { group_cols, agg, .. } => {
            assert_eq!(group_cols, vec![10, 20, 30]);
            assert_eq!(agg, vec![(AggFunc::Count, 0), (AggFunc::Sum, 0)]);
        }
        other => panic!("expected Reduce, got {other:?}"),
    }
}

/// A range-join node decodes its `(n_eq, rel)` from the NODE_COL_KIND_RANGE_JOIN
/// param row.
#[test]
fn decode_range_join_params() {
    let cols = [CircuitNodeColumn {
        kind: NODE_COL_KIND_RANGE_JOIN,
        position: 0,
        value1: 2,
        value2: RangeRel::Gt.as_wire(),
    }];
    let node = decode_op_node(OPCODE_JOIN_DELTA_TRACE_RANGE, None, None, &cols).unwrap();
    assert_eq!(
        node,
        OpNode::Join(JoinKind::DeltaTraceRange {
            n_eq: 2,
            rel: RangeRel::Gt
        })
    );
}

/// A range-join opcode with no param row is rejected at the decode trust
/// boundary rather than defaulting to a bogus probe shape.
#[test]
fn decode_range_join_rejects_missing_param_row() {
    let err = decode_op_node(OPCODE_JOIN_DELTA_TRACE_RANGE, None, None, &[]).unwrap_err();
    assert!(err.contains("missing range-join param row"), "got: {err}");
}

/// An out-of-range `rel` value is rejected, not silently mapped.
#[test]
fn decode_range_join_rejects_unknown_rel() {
    let cols = [CircuitNodeColumn {
        kind: NODE_COL_KIND_RANGE_JOIN,
        position: 0,
        value1: 0,
        value2: 99,
    }];
    let err = decode_op_node(OPCODE_JOIN_DELTA_TRACE_RANGE, None, None, &cols).unwrap_err();
    assert!(err.contains("unknown rel"), "got: {err}");
}

/// The partition-filter opcode decodes to the payload-free node.
#[test]
fn decode_worker_filter() {
    let node = decode_op_node(OPCODE_WORKER_FILTER, None, None, &[]).unwrap();
    assert_eq!(node, OpNode::WorkerFilter);
}

/// A non-zero `value2` that is not a PK-eligible type code is rejected at the
/// decode trust boundary (here: a float code), not silently mis-strided.
#[test]
fn decode_rejects_non_pk_eligible_target_tc() {
    let cols = [CircuitNodeColumn {
        kind: NODE_COL_KIND_REINDEX,
        position: 0,
        value1: 3,
        value2: crate::type_code::F64 as u64,
    }];
    let err = decode_op_node(OPCODE_MAP_EXPR, None, Some(vec![1, 2, 3]), &cols).unwrap_err();
    assert!(err.contains("not PK-eligible"), "got: {err}");
}

/// A NULL_EXTEND type code becomes a schema column verbatim. An undecodable
/// one is not inert — `wire_stride` reports 8 for it, so it clears any width
/// test, and the schema it lands in becomes every downstream node's input.
#[test]
fn decode_rejects_invalid_null_extend_type_code() {
    let col = |tc: u64| CircuitNodeColumn {
        kind: NODE_COL_KIND_NULL_EXT,
        position: 0,
        value1: tc,
        value2: 0,
    };
    let err = decode_op_node(OPCODE_NULL_EXTEND, None, None, &[col(200)]).unwrap_err();
    assert!(err.contains("invalid column type code"), "got: {err}");
    assert_eq!(
        decode_op_node(OPCODE_NULL_EXTEND, None, None, &[col(crate::type_code::I64 as u64)]).unwrap(),
        OpNode::NullExtend {
            type_codes: vec![crate::type_code::I64],
        }
    );
}

/// A SCAN node with a NULL `source_table` cell is a corrupt circuit: reject it at
/// the decode trust boundary rather than coercing to the (invalid) table id 0.
#[test]
fn decode_rejects_scan_missing_source_table() {
    assert!(decode_op_node(OPCODE_SCAN_DELTA, None, None, &[])
        .unwrap_err()
        .contains("source_table"));
}

/// Re-decode an encoded node through the row bundle `encode_op_node` produces.
fn roundtrip(op: OpNode) -> Result<OpNode, String> {
    let ((opcode, src_tab, blob), rows) = encode_op_node(op);
    let cols: Vec<CircuitNodeColumn> = rows
        .into_iter()
        .map(|(kind, position, value1, value2)| CircuitNodeColumn {
            kind,
            position,
            value1,
            value2,
        })
        .collect();
    decode_op_node(opcode, src_tab, blob, &cols)
}

/// Every `OpNode` shape survives `encode_op_node` → `decode_op_node`. This is
/// the crate's largest codec and the only one whose bugs land in a persisted
/// A `MAP_EXPR` whose role cannot be read decides nothing about routing, so it
/// The ground row is written at payload index 0, so a group set would leave
/// its exemplar slots short — the cross-check belongs where the two fields
/// arrive, which is here rather than in each consumer.
#[test]
fn a_global_ground_reduce_over_a_group_set_is_rejected() {
    let row = |kind, position, value1, value2| CircuitNodeColumn {
        kind,
        position,
        value1,
        value2,
    };
    let agg = row(NODE_COL_KIND_AGG_SPEC, 0, AGG_COUNT, 0);
    let ground = row(NODE_COL_KIND_GLOBAL_GROUND, 0, 1, 0);
    let group = row(NODE_COL_KIND_GROUP, 0, 3, 0);
    let decode = |cols: &[CircuitNodeColumn]| decode_op_node(OPCODE_REDUCE, None, None, cols);
    assert!(decode(&[agg, ground]).is_ok(), "group-less ground is the valid shape");
    assert!(
        decode(&[agg, group]).is_ok(),
        "a grouped reduce without the flag is fine"
    );
    assert!(decode(&[agg, ground, group]).unwrap_err().contains("global-ground"));
}

/// must fail the decode rather than default — a missing row is a circuit
/// stored before the role existed. (Contrast `SCAN_BOUND`, which decides only
/// scan speed and therefore degrades instead of erroring.) A route-key row
/// with no reindex columns cannot say anything: the columns are what
/// discriminate a re-key from a computed projection.
#[test]
fn a_map_expr_whose_role_is_unreadable_is_rejected() {
    let row = |kind, value1| {
        [CircuitNodeColumn {
            kind,
            position: 0,
            value1,
            value2: 0,
        }]
    };
    let decode = |cols: &[CircuitNodeColumn]| decode_op_node(OPCODE_MAP_EXPR, None, Some(vec![1]), cols);
    assert!(decode(&row(NODE_COL_KIND_REINDEX, 3))
        .unwrap_err()
        .contains("route-key"));
    assert!(matches!(
        decode(&row(NODE_COL_KIND_ROUTE_KEY, 1)),
        Ok(OpNode::Map(MapKind::Compute { .. }))
    ));
}

/// circuit, so the variant set is swept rather than sampled.
#[test]
fn every_op_node_variant_roundtrips() {
    let nodes = vec![
        OpNode::ScanDelta { source: 7, bound: None },
        OpNode::Filter(None),
        OpNode::Filter(Some(vec![1, 2, 3, 4])),
        OpNode::Map(MapKind::Projection(vec![])),
        OpNode::Map(MapKind::Projection(vec![4, 0, 9])),
        OpNode::Map(MapKind::Compute {
            program: vec![9, 9],
            out_cols: vec![],
        }),
        OpNode::Map(MapKind::Compute {
            program: vec![7],
            out_cols: vec![(crate::type_code::I64, false), (crate::type_code::STRING, true)],
        }),
        OpNode::Map(MapKind::Reindex {
            program: vec![1],
            reindex_cols: vec![2, 5],
            reindex_target_tcs: vec![0, crate::type_code::I64],
            role: ReindexRole::ScatterKey,
        }),
        OpNode::Map(MapKind::Reindex {
            program: vec![1],
            reindex_cols: vec![2, 5],
            reindex_target_tcs: vec![0, crate::type_code::I64],
            role: ReindexRole::Auxiliary,
        }),
        OpNode::Map(MapKind::HashRow(vec![1, 2], vec![0, crate::type_code::I32], 0)),
        OpNode::Map(MapKind::HashRow(vec![3], vec![0], 1)),
        OpNode::Negate,
        OpNode::Union,
        OpNode::Distinct,
        OpNode::PositivePart,
        OpNode::Reduce {
            group_cols: vec![],
            agg: vec![(AggFunc::Count, 0)],
            global_ground: true,
            out_key: ReduceOutKey::SyntheticFold,
        },
        OpNode::Reduce {
            group_cols: vec![2, 7],
            agg: vec![(AggFunc::Sum, 1), (AggFunc::Max, 3), (AggFunc::SumZero, 0)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        },
        OpNode::Reduce {
            group_cols: vec![4],
            agg: vec![(AggFunc::Min, 4)],
            global_ground: false,
            out_key: ReduceOutKey::SingleNaturalCol,
        },
        OpNode::Join(JoinKind::DeltaTrace),
        OpNode::Join(JoinKind::DeltaTraceRange {
            n_eq: 3,
            rel: RangeRel::Ge,
        }),
        OpNode::IntegrateSink,
        OpNode::IntegrateTrace,
        OpNode::ExchangeShard { shard_cols: vec![0, 2] },
        OpNode::NullExtend {
            type_codes: vec![crate::type_code::I64, crate::type_code::STRING],
        },
        OpNode::WorkerFilter,
    ];
    for node in nodes {
        assert_eq!(roundtrip(node.clone()).unwrap(), node, "round-trip failed for {node:?}");
    }
}

/// A bounded `ScanDelta` round-trips its descriptor and its column list — in
/// order. The list is position-keyed, so a reversed list must stay reversed.
#[test]
fn scan_bound_roundtrips_preserving_col_order() {
    let bound = ScanBound {
        idx_cols: crate::PkColList::from_slice(&[9, 3, 5]),
        desc: crate::RangeDescriptor::new(&[7, 11], crate::Cut::After(4), crate::Cut::Before(90)),
    };
    let node = OpNode::ScanDelta {
        source: 42,
        bound: Some(bound),
    };
    assert_eq!(roundtrip(node.clone()).unwrap(), node);
    // Order is significant and carried by `position`, not by chance.
    match roundtrip(node).unwrap() {
        OpNode::ScanDelta { bound: Some(b), .. } => assert_eq!(b.idx_cols.as_slice(), &[9, 3, 5]),
        other => panic!("expected a bounded ScanDelta, got {other:?}"),
    }
}

/// An unbounded `ScanDelta` emits no param rows and no blob — byte-identical to
/// the pre-bound encoding, so stored circuits round-trip unchanged.
#[test]
fn unbounded_scan_delta_encodes_identically() {
    let (fields, rows) = encode_op_node(OpNode::ScanDelta { source: 7, bound: None });
    assert_eq!(fields, (OPCODE_SCAN_DELTA, Some(7), None));
    assert!(rows.is_empty());
    assert_eq!(
        decode_op_node(OPCODE_SCAN_DELTA, Some(7), None, &[]).unwrap(),
        OpNode::ScanDelta { source: 7, bound: None }
    );
}

/// Every malformed-hint shape degrades to `bound: None` — no panic, no `Err`.
/// An `Err` here would abort the whole view load over a physical access hint.
#[test]
fn malformed_scan_bound_degrades_to_none() {
    let good_blob = crate::RangeDescriptor::new(&[1], crate::Cut::Before(0), crate::Cut::After(9)).encode();
    let col = |position: u16, value1: u64| CircuitNodeColumn {
        kind: NODE_COL_KIND_SCAN_BOUND,
        position,
        value1,
        value2: 0,
    };
    let over_long: Vec<CircuitNodeColumn> = (0..=crate::PK_LIST_MAX_COLS as u16).map(|i| col(i, i as u64)).collect();
    struct Case {
        what: &'static str,
        blob: Option<Vec<u8>>,
        cols: Vec<CircuitNodeColumn>,
    }
    let cases = [
        // A col-list longer than the arity cap — would panic `from_slice`.
        Case {
            what: "over-long list",
            blob: Some(good_blob.clone()),
            cols: over_long,
        },
        // A bounded node whose descriptor blob never arrived.
        Case {
            what: "missing blob",
            blob: None,
            cols: vec![col(0, 1)],
        },
        // A blob that fails `RangeDescriptor::decode`'s validation.
        Case {
            what: "undecodable blob",
            blob: Some(vec![0xff, 0xff, 0xff]),
            cols: vec![col(0, 1)],
        },
        // A blob with no col-list: "absent" and "length 0" are the same bytes.
        Case {
            what: "no col list",
            blob: Some(good_blob),
            cols: Vec::new(),
        },
    ];
    for Case { what, blob, cols } in cases {
        assert_eq!(
            decode_op_node(OPCODE_SCAN_DELTA, Some(3), blob, &cols).unwrap(),
            OpNode::ScanDelta { source: 3, bound: None },
            "{what} must degrade to an unbounded scan"
        );
    }
}
