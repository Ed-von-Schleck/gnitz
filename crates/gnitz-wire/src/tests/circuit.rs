use super::*;

fn reindex_of(node: OpNode) -> (Vec<u32>, Vec<u8>) {
    match node {
        OpNode::Map(MapKind::Reindex {
            reindex_cols,
            reindex_target_tcs,
            ..
        }) => (reindex_cols, reindex_target_tcs),
        other => panic!("expected Map(Reindex), got {other:?}"),
    }
}

/// MAP_EXPR reindex columns decode from NODE_COL_KIND_REINDEX rows in position
/// order (value1 = source column), and a non-zero `value2` is the promoted key
/// type code `T`, decoded parallel to them. A `value2 = 0` slot means "derive
/// from source".
#[test]
fn decode_reindex_cols_and_target_tcs() {
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

/// Each range-join decode guard, against the param row that trips it. A
/// range-join with no readable probe shape must fail rather than default to a
/// bogus one.
#[test]
fn decode_range_join_rejects_a_malformed_param_row() {
    let unknown_rel = vec![CircuitNodeColumn {
        kind: NODE_COL_KIND_RANGE_JOIN,
        position: 0,
        value1: 0,
        value2: 99,
    }];
    let cases: &[(&str, Vec<CircuitNodeColumn>, &str)] = &[
        ("missing param row", Vec::new(), "missing range-join param row"),
        ("unknown rel", unknown_rel, "unknown rel"),
    ];
    for (what, cols, want) in cases {
        let err = decode_op_node(OPCODE_JOIN_DELTA_TRACE_RANGE, None, None, cols).expect_err(what);
        assert!(err.contains(want), "{what}: got {err}");
    }
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
    let col = CircuitNodeColumn {
        kind: NODE_COL_KIND_NULL_EXT,
        position: 0,
        value1: 200,
        value2: 0,
    };
    let err = decode_op_node(OPCODE_NULL_EXTEND, None, None, &[col]).unwrap_err();
    assert!(err.contains("invalid column type code"), "got: {err}");
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

/// A `MAP_EXPR` whose role cannot be read decides nothing about routing, so it
/// must fail the decode rather than default. (Contrast `SCAN_BOUND`, which
/// decides only scan speed and therefore degrades instead of erroring.) A
/// route-key row with no reindex columns cannot say anything: the columns are
/// what discriminate a re-key from a computed projection.
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
        .contains("missing its route-key row"));
    // A value naming no role is refused too — the role decides routing, so it
    // must not silently resolve the way a scan hint does.
    let mut unknown_role = row(NODE_COL_KIND_REINDEX, 3).to_vec();
    unknown_role.push(CircuitNodeColumn {
        kind: NODE_COL_KIND_ROUTE_KEY,
        position: 0,
        value1: 99,
        value2: 0,
    });
    assert!(decode(&unknown_role).unwrap_err().contains("unknown route-key role"));
    assert!(matches!(
        decode(&row(NODE_COL_KIND_ROUTE_KEY, 1)),
        Ok(OpNode::Map(MapKind::Compute { .. }))
    ));
}

/// Every `OpNode` shape survives `encode_op_node` → `decode_op_node`. This is
/// the crate's largest codec and the only one whose bugs land in a persisted
/// circuit, so the variant set is swept rather than sampled: every wire enum the
/// encoder embeds is driven from its own `ALL`, so a variant added to any of them
/// is round-tripped without a second edit here.
#[test]
fn every_op_node_variant_roundtrips() {
    let mut nodes = vec![
        OpNode::ScanDelta { source: 7, bound: None },
        // A bounded scan, whose index column list is non-ascending: order is
        // carried by `position`, and `PkColList`'s `PartialEq` spans the whole
        // backing array, so a reordered list fails the round-trip.
        OpNode::ScanDelta {
            source: 42,
            bound: Some(ScanBound {
                idx_cols: crate::PkColList::from_slice(&[9, 3, 5]),
                desc: crate::RangeDescriptor::new(&[7, 11], crate::Cut::After(4), crate::Cut::Before(90)),
            }),
        },
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
        OpNode::Join(JoinKind::DeltaTrace),
        OpNode::IntegrateSink,
        OpNode::IntegrateTrace,
        OpNode::ExchangeShard { shard_cols: vec![0, 2] },
        OpNode::NullExtend {
            type_codes: vec![crate::type_code::I64, crate::type_code::STRING],
        },
        OpNode::WorkerFilter,
    ];
    for &role in ReindexRole::ALL {
        nodes.push(OpNode::Map(MapKind::Reindex {
            program: vec![1],
            reindex_cols: vec![2, 5],
            reindex_target_tcs: vec![0, crate::type_code::I64],
            role,
        }));
    }
    // One reduce per aggregate, at a distinct source column, and one per output
    // key kind — the two enums a REDUCE row carries.
    for (i, &func) in AggFunc::ALL.iter().enumerate() {
        nodes.push(OpNode::Reduce {
            group_cols: vec![2, 7],
            agg: vec![(func, i as u32)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        });
    }
    for &out_key in ReduceOutKey::ALL {
        nodes.push(OpNode::Reduce {
            group_cols: vec![4],
            agg: vec![(AggFunc::Min, 4), (AggFunc::Sum, 1)],
            global_ground: false,
            out_key,
        });
    }
    for &rel in RangeRel::ALL {
        nodes.push(OpNode::Join(JoinKind::DeltaTraceRange { n_eq: 3, rel }));
    }
    for node in nodes {
        assert_eq!(roundtrip(node.clone()).unwrap(), node, "round-trip failed for {node:?}");
    }
}

/// A REDUCE's two sparse-default param rows are written iff the field is
/// non-default, so an ordinary grouped reduce costs neither. Presence *is* the
/// encoding here, which the round-trip sweep above cannot observe.
#[test]
fn reduce_writes_its_sparse_param_rows_only_when_set() {
    let reduce = |global_ground, out_key| OpNode::Reduce {
        group_cols: vec![],
        agg: vec![(AggFunc::Count, 0)],
        global_ground,
        out_key,
    };
    let count = |op: OpNode, kind: u64| {
        let (_, rows) = encode_op_node(op);
        rows.iter().filter(|(k, ..)| *k == kind).count()
    };
    for ground in [false, true] {
        assert_eq!(
            count(reduce(ground, ReduceOutKey::SyntheticFold), NODE_COL_KIND_GLOBAL_GROUND),
            ground as usize,
            "the ground row is present iff global_ground",
        );
    }
    for &out_key in ReduceOutKey::ALL {
        assert_eq!(
            count(reduce(false, out_key), NODE_COL_KIND_REDUCE_OUT_KEY),
            (out_key != ReduceOutKey::SyntheticFold) as usize,
            "{out_key:?}: the out-key row is present iff non-default",
        );
    }
}

/// An unbounded `ScanDelta` emits no param rows and no blob: "absent" and
/// "empty" must stay the same bytes, so the common shape costs nothing.
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
