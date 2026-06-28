//! Circuit-layer wire definitions: operator opcodes, ports, aggregate IDs, and
//! the typed `OpNode` representation shared between gnitz-core and gnitz-engine.

// ---------------------------------------------------------------------------
// Circuit opcodes
// ---------------------------------------------------------------------------

pub const OPCODE_FILTER: u64 = 1;
pub const OPCODE_MAP: u64 = 2;
pub const OPCODE_NEGATE: u64 = 3;
pub const OPCODE_UNION: u64 = 4;
pub const OPCODE_JOIN_DELTA_TRACE: u64 = 5;
pub const OPCODE_JOIN_DELTA_DELTA: u64 = 6;
pub const OPCODE_INTEGRATE: u64 = 7;
pub const OPCODE_DELAY: u64 = 8;
pub const OPCODE_REDUCE: u64 = 9;
pub const OPCODE_DISTINCT: u64 = 10;
/// Delta input source for a base table. The table id lives in the node row's
/// `source_table` column.
pub const OPCODE_SCAN_DELTA: u64 = 11;
pub const OPCODE_SEEK_TRACE: u64 = 12;
pub const OPCODE_CLEAR_DELTAS: u64 = 15;
pub const OPCODE_ANTI_JOIN_DELTA_TRACE: u64 = 16;
pub const OPCODE_ANTI_JOIN_DELTA_DELTA: u64 = 17;
pub const OPCODE_SEMI_JOIN_DELTA_TRACE: u64 = 18;
pub const OPCODE_SEMI_JOIN_DELTA_DELTA: u64 = 19;
pub const OPCODE_EXCHANGE_SHARD: u64 = 20;
pub const OPCODE_EXCHANGE_GATHER: u64 = 21;
pub const OPCODE_JOIN_DELTA_TRACE_OUTER: u64 = 22;
pub const OPCODE_NULL_EXTEND: u64 = 23;
pub const OPCODE_GATHER_REDUCE: u64 = 24;
/// Discriminates IntegrateTrace from IntegrateSink (OPCODE_INTEGRATE=7)
/// without a nullable column.
pub const OPCODE_INTEGRATE_TRACE: u64 = 25;
/// MAP sub-variant: pure projection (column reorder/drop).
pub const OPCODE_MAP_PROJ: u64 = 26;
/// MAP sub-variant: expression program (compute) with optional PK reindex.
pub const OPCODE_MAP_EXPR: u64 = 27;
/// MAP sub-variant: drop payload, keep PK only.
pub const OPCODE_MAP_KEY_ONLY: u64 = 28;
/// MAP sub-variant: copy all columns to payload, set PK = hash of full row.
pub const OPCODE_MAP_HASH_ROW: u64 = 29;
/// Read-only trace source for join trace ports; never participates in cascade.
pub const OPCODE_SCAN_TRACE_TABLE: u64 = 31;
/// Non-equi (range) join: symmetric delta-trace join whose probe is an ordered
/// half-open range walk over the trace instead of an equal-key seek.
pub const OPCODE_JOIN_DELTA_TRACE_RANGE: u64 = 32;
/// Drop trace rows this worker does not own (**pure** range-join broadcast input;
/// a band join scatters by its eq prefix and omits this node). Worker identity is
/// a compile-time constant, so the node carries no payload.
pub const OPCODE_PARTITION_FILTER: u64 = 33;

// ---------------------------------------------------------------------------
// Circuit-layer type aliases
// ---------------------------------------------------------------------------

pub type TableId = u64;

// ---------------------------------------------------------------------------
// Port constants
// ---------------------------------------------------------------------------

pub const PORT_IN: u64 = 0;
pub const PORT_TRACE: u64 = 1;
pub const PORT_IN_A: u64 = 0;
pub const PORT_IN_B: u64 = 1;

// ---------------------------------------------------------------------------
// CircuitNodeColumns `kind` discriminator values
// ---------------------------------------------------------------------------
//
// Every "ordered list of column indices" attached to a circuit node lives
// in `CircuitNodeColumns` keyed by (view_id, node_id, kind, position). The
// `kind` discriminator selects which list is being addressed:

pub const NODE_COL_KIND_GROUP: u64 = 0; // REDUCE group-by columns
pub const NODE_COL_KIND_SHARD: u64 = 1; // EXCHANGE_SHARD shard columns
pub const NODE_COL_KIND_PROJ: u64 = 2; // MAP projection columns
pub const NODE_COL_KIND_NULL_EXT: u64 = 3; // NULL_EXTEND payload type codes
pub const NODE_COL_KIND_AGG_SPEC: u64 = 4; // REDUCE aggregate specs (value1=func_id, value2=col_idx)
pub const NODE_COL_KIND_BRANCH_ID: u64 = 5; // MAP_HASH_ROW per-side branch discriminator (value1=branch_id)
pub const NODE_COL_KIND_REINDEX: u64 = 6; // MAP_EXPR equijoin pre-index cols (value1=col_idx, position=key order)
pub const NODE_COL_KIND_RANGE_JOIN: u64 = 7; // JOIN_DELTA_TRACE_RANGE params (value1=n_eq, value2=rel)
pub const NODE_COL_KIND_GLOBAL_GROUND: u64 = 8; // REDUCE global-aggregate ground discriminator (value1=bool)

// ---------------------------------------------------------------------------
// Aggregate function IDs
// ---------------------------------------------------------------------------

pub const AGG_COUNT: u64 = 1;
pub const AGG_SUM: u64 = 2;
pub const AGG_MIN: u64 = 3;
pub const AGG_MAX: u64 = 4;
pub const AGG_COUNT_NON_NULL: u64 = 5;

// ---------------------------------------------------------------------------
// Typed circuit-node representation (shared between gnitz-core and gnitz-engine)
// ---------------------------------------------------------------------------

/// Aggregate function discriminant. Values match the `AGG_*` wire constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum AggFunc {
    Count = AGG_COUNT,
    Sum = AGG_SUM,
    Min = AGG_MIN,
    Max = AGG_MAX,
    CountNonNull = AGG_COUNT_NON_NULL,
}

impl AggFunc {
    pub fn from_wire(v: u64) -> Option<Self> {
        match v {
            AGG_COUNT => Some(AggFunc::Count),
            AGG_SUM => Some(AggFunc::Sum),
            AGG_MIN => Some(AggFunc::Min),
            AGG_MAX => Some(AggFunc::Max),
            AGG_COUNT_NON_NULL => Some(AggFunc::CountNonNull),
            _ => None,
        }
    }
    pub fn as_u64(self) -> u64 {
        self as u64
    }
}

/// REDUCE aggregation kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggKind {
    Null,
    Specs(Vec<(AggFunc, u16)>),
}

/// The relation a **trace** slot must satisfy versus the **delta** slot in a
/// range-join probe (`{ trace_slot REL delta_slot }`). Canonicalized from the ON
/// clause's `L.x OP R.y`: term AB's rel is the converse of OP, term BA's rel is
/// OP itself. Wire values are stable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum RangeRel {
    Lt = 0,
    Le = 1,
    Gt = 2,
    Ge = 3,
}

impl RangeRel {
    pub fn from_wire(v: u64) -> Option<Self> {
        match v {
            0 => Some(RangeRel::Lt),
            1 => Some(RangeRel::Le),
            2 => Some(RangeRel::Gt),
            3 => Some(RangeRel::Ge),
            _ => None,
        }
    }
    pub fn as_u64(self) -> u64 {
        self as u64
    }
}

/// Join physical strategy. `DeltaTraceRange` keeps `JoinKind: Copy` (its fields
/// are `Copy`). Only `OpNode::Join` carries this; anti-/semi-joins carry the
/// narrower [`SetJoinKind`], so their outer/range forms are not merely "rejected
/// in the planner" — the type cannot name them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    DeltaTrace,
    DeltaTraceOuter,
    DeltaDelta,
    DeltaTraceRange { n_eq: u8, rel: RangeRel },
}

/// Physical strategy for anti-/semi-join. Narrower than [`JoinKind`]: the
/// set-difference operators have no outer (null-fill) or range form, so the type
/// cannot name one. Opcodes 16–19 already encode exactly this set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetJoinKind {
    DeltaTrace,
    DeltaDelta,
}

/// MAP sub-variant discriminant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapKind {
    /// Pure projection/column-reorder. Carries payload column indices to keep.
    Projection(Vec<u16>),
    /// Expression-based map. `program` is an opaque `ExprProgram` blob;
    /// each crate decodes it with its own decoder. `reindex_cols` lists the
    /// source columns, in key order, that become the synthetic PK for equijoin
    /// pre-indexing (empty for a plain compute map).
    ///
    /// `reindex_target_tcs` is parallel to `reindex_cols` — a decoded
    /// `Expression` always has `reindex_target_tcs.len() == reindex_cols.len()`.
    /// Entry `i` is the promoted key type code `T` for slot `i` of a cross-width
    /// equijoin key, or `0` meaning "derive the slot type from the source column"
    /// (the same-type path, byte-identical to non-promoted circuits). Both are
    /// empty for a plain compute map.
    Expression {
        program: Vec<u8>,
        reindex_cols: Vec<u16>,
        reindex_target_tcs: Vec<u8>,
    },
    /// Drop all payload columns, keep only PK and weight.
    KeyOnly,
    /// Full-row-identity reindex. Like `Projection` (keep the listed columns as
    /// payload, in order), but the synthetic PK is set to a hash of the kept
    /// payload bytes. Used by EXCEPT/INTERSECT/DISTINCT so set membership is
    /// decided by the projected row content, not by the source PK.
    ///
    /// The second field is a per-side `branch_id` mixed into the hash so that
    /// identical payloads on the left vs right branch of a `UNION ALL` get
    /// distinct synthetic PKs (and therefore accumulate weight +2 rather than
    /// collapsing). Deduplicating set-ops (UNION/EXCEPT/INTERSECT) use 0 on both
    /// sides; UNION ALL uses 0 on the left and 1 on the right.
    HashRow(Vec<u16>, u8),
}

/// Typed operator-node payload. Expression blobs are stored as raw `Vec<u8>`;
/// each crate decodes them independently (gnitz-core uses `ExprProgram::decode`;
/// gnitz-engine uses its own `decode_expr_blob`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// `OPCODE_SCAN_DELTA = 11`. Delta input; carries the source `table_id`.
    ScanDelta(TableId),
    /// `OPCODE_SCAN_TRACE_TABLE = 31`. Read-only trace source for join trace ports.
    ScanTrace(TableId),
    /// `OPCODE_FILTER = 1`. Optional expression predicate blob.
    Filter(Option<Vec<u8>>),
    Map(MapKind),
    Negate,
    Union,
    Delay,
    Distinct,
    Reduce {
        group_cols: Vec<u16>,
        agg: AggKind,
        /// True only for the user's ungrouped (global) scalar aggregate — the
        /// reduce that must emit exactly one row over an empty/fully-retracted
        /// source (COUNT(*)=0, SUM/MIN/MAX/AVG=NULL). A **SQL-intent
        /// discriminator**, not a Z-set property: the LEFT range-join's threshold
        /// reduce (`reduce_multi_local`) also has empty group cols but must NOT
        /// seed a ground row, so the flag cannot be derived from
        /// `group_cols.is_empty()` and travels explicitly from the planner.
        global_ground: bool,
    },
    Join(JoinKind),
    AntiJoin(SetJoinKind),
    SemiJoin(SetJoinKind),
    /// `OPCODE_INTEGRATE = 7`. Primary INTEGRATE: writes to view storage.
    IntegrateSink,
    /// `OPCODE_INTEGRATE_TRACE = 25`. Accumulates Z-set for join trace.
    IntegrateTrace,
    ExchangeShard {
        shard_cols: Vec<u16>,
    },
    /// `OPCODE_EXCHANGE_GATHER = 21`. Register pass-through; no VM instruction emitted.
    ExchangeGather,
    NullExtend {
        type_codes: Vec<u8>,
    },
    /// `OPCODE_GATHER_REDUCE = 24`. Server-internal; carries no typed fields until
    /// the GatherReduce planning milestone adds `agg: AggKind`.
    GatherReduce,
    /// `OPCODE_SEEK_TRACE = 12`. Server-internal.
    SeekTrace,
    /// `OPCODE_CLEAR_DELTAS = 15`. Server-internal.
    ClearDeltas,
    /// `OPCODE_PARTITION_FILTER = 33`. Keep only rows whose packed-PK partition is
    /// owned by this worker (**pure** range-join broadcast input; a band join
    /// scatters by its eq prefix and omits this node). Worker identity is a
    /// compile-time constant, so no payload travels on the wire.
    PartitionFilter,
}

/// One decoded row of the `CircuitNodeColumns` system table for a single node,
/// sorted by (kind, position). `value1`/`value2` are interpreted per `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitNodeColumn {
    pub kind: u64,
    pub position: u16,
    pub value1: u64,
    pub value2: u64,
}

/// Reconstruct an `OpNode` from the three-table row bundle.
///
/// `cols` is the sorted (kind, position, value1, value2) slice for this node,
/// pre-filtered to the current `node_id`. `expr_blob` is stored as-is without
/// any attempt to decode the `ExprProgram` — callers do that on their side of
/// the crate boundary.
pub fn decode_op_node(
    opcode: u64,
    src_tab: Option<TableId>,
    expr_blob: Option<Vec<u8>>,
    cols: &[CircuitNodeColumn],
) -> Result<OpNode, String> {
    let collect_cols = |kind: u64| -> Vec<u16> {
        cols.iter()
            .filter(|c| c.kind == kind)
            .map(|c| c.value1 as u16)
            .collect()
    };
    let collect_typecodes =
        |kind: u64| -> Vec<u8> { cols.iter().filter(|c| c.kind == kind).map(|c| c.value1 as u8).collect() };
    let collect_aggs = || -> Result<Vec<(AggFunc, u16)>, String> {
        cols.iter()
            .filter(|c| c.kind == NODE_COL_KIND_AGG_SPEC)
            .map(|c| {
                AggFunc::from_wire(c.value1)
                    .ok_or_else(|| format!("unknown agg func id {}", c.value1))
                    .map(|f| (f, c.value2 as u16))
            })
            .collect()
    };
    Ok(match opcode {
        OPCODE_SCAN_DELTA => {
            let tid = src_tab.ok_or_else(|| "SCAN_DELTA missing source_table".to_string())?;
            OpNode::ScanDelta(tid)
        }
        OPCODE_SCAN_TRACE_TABLE => {
            let tid = src_tab.ok_or_else(|| "SCAN_TRACE missing source_table".to_string())?;
            OpNode::ScanTrace(tid)
        }
        OPCODE_FILTER => OpNode::Filter(expr_blob),
        OPCODE_MAP_PROJ => OpNode::Map(MapKind::Projection(collect_cols(NODE_COL_KIND_PROJ))),
        OPCODE_MAP_EXPR => {
            let program = expr_blob.ok_or_else(|| "MAP_EXPR missing expr_program blob".to_string())?;
            // Reindex columns live in CircuitNodeColumns (NODE_COL_KIND_REINDEX),
            // position-ordered so a compound key's column order is preserved.
            // `value1` is the source column, `value2` the promoted key type code
            // `T` (`0` = derive from source). This decode is the trust boundary
            // where catalog bytes become a typed node: reject a non-zero `value2`
            // that is not a PK-eligible type code here, rather than letting it
            // silently produce a wrong reindex output stride downstream (a float /
            // unknown code would otherwise survive as a bogus slot width).
            let mut reindex_cols: Vec<u16> = Vec::new();
            let mut reindex_target_tcs: Vec<u8> = Vec::new();
            for c in cols {
                if c.kind == NODE_COL_KIND_REINDEX {
                    let tc = c.value2 as u8;
                    if tc != 0 && !crate::is_pk_eligible(tc) {
                        return Err(format!("MAP_EXPR reindex target type code {tc} is not PK-eligible"));
                    }
                    reindex_cols.push(c.value1 as u16);
                    reindex_target_tcs.push(tc);
                }
            }
            OpNode::Map(MapKind::Expression {
                program,
                reindex_cols,
                reindex_target_tcs,
            })
        }
        OPCODE_MAP_KEY_ONLY => OpNode::Map(MapKind::KeyOnly),
        OPCODE_MAP_HASH_ROW => {
            let branch_id = cols
                .iter()
                .find(|c| c.kind == NODE_COL_KIND_BRANCH_ID)
                .map(|c| c.value1 as u8)
                .unwrap_or(0);
            OpNode::Map(MapKind::HashRow(collect_cols(NODE_COL_KIND_PROJ), branch_id))
        }
        OPCODE_NEGATE => OpNode::Negate,
        OPCODE_UNION => OpNode::Union,
        OPCODE_DELAY => OpNode::Delay,
        OPCODE_DISTINCT => OpNode::Distinct,
        OPCODE_REDUCE => {
            let group_cols = collect_cols(NODE_COL_KIND_GROUP);
            let specs = collect_aggs()?;
            let agg = if specs.is_empty() {
                AggKind::Null
            } else {
                AggKind::Specs(specs)
            };
            // Absent row ⇒ `false` (the ordinary grouped / range-join reduce); a
            // present row carries the global-aggregate intent in value1. One param
            // row, exactly like NODE_COL_KIND_RANGE_JOIN.
            let global_ground = cols
                .iter()
                .find(|c| c.kind == NODE_COL_KIND_GLOBAL_GROUND)
                .is_some_and(|c| c.value1 != 0);
            OpNode::Reduce {
                group_cols,
                agg,
                global_ground,
            }
        }
        OPCODE_JOIN_DELTA_TRACE => OpNode::Join(JoinKind::DeltaTrace),
        OPCODE_JOIN_DELTA_TRACE_OUTER => OpNode::Join(JoinKind::DeltaTraceOuter),
        OPCODE_JOIN_DELTA_DELTA => OpNode::Join(JoinKind::DeltaDelta),
        OPCODE_JOIN_DELTA_TRACE_RANGE => {
            // n_eq + rel ride in a single NODE_COL_KIND_RANGE_JOIN row. Reject a
            // missing row or an unknown rel at this decode trust boundary rather
            // than letting a bogus probe shape survive downstream.
            let row = cols
                .iter()
                .find(|c| c.kind == NODE_COL_KIND_RANGE_JOIN)
                .ok_or_else(|| "JOIN_DELTA_TRACE_RANGE missing range-join param row".to_string())?;
            let n_eq = row.value1 as u8;
            let rel = RangeRel::from_wire(row.value2)
                .ok_or_else(|| format!("JOIN_DELTA_TRACE_RANGE unknown rel {}", row.value2))?;
            OpNode::Join(JoinKind::DeltaTraceRange { n_eq, rel })
        }
        OPCODE_ANTI_JOIN_DELTA_TRACE => OpNode::AntiJoin(SetJoinKind::DeltaTrace),
        OPCODE_ANTI_JOIN_DELTA_DELTA => OpNode::AntiJoin(SetJoinKind::DeltaDelta),
        OPCODE_SEMI_JOIN_DELTA_TRACE => OpNode::SemiJoin(SetJoinKind::DeltaTrace),
        OPCODE_SEMI_JOIN_DELTA_DELTA => OpNode::SemiJoin(SetJoinKind::DeltaDelta),
        OPCODE_INTEGRATE => OpNode::IntegrateSink,
        OPCODE_INTEGRATE_TRACE => OpNode::IntegrateTrace,
        OPCODE_EXCHANGE_SHARD => OpNode::ExchangeShard {
            shard_cols: collect_cols(NODE_COL_KIND_SHARD),
        },
        OPCODE_EXCHANGE_GATHER => OpNode::ExchangeGather,
        OPCODE_NULL_EXTEND => OpNode::NullExtend {
            type_codes: collect_typecodes(NODE_COL_KIND_NULL_EXT),
        },
        OPCODE_GATHER_REDUCE => OpNode::GatherReduce,
        OPCODE_SEEK_TRACE => OpNode::SeekTrace,
        OPCODE_CLEAR_DELTAS => OpNode::ClearDeltas,
        OPCODE_PARTITION_FILTER => OpNode::PartitionFilter,
        _ => return Err(format!("unknown opcode {opcode}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reindex_cols_of(node: OpNode) -> Vec<u16> {
        match node {
            OpNode::Map(MapKind::Expression { reindex_cols, .. }) => reindex_cols,
            other => panic!("expected Map(Expression), got {other:?}"),
        }
    }

    fn reindex_of(node: OpNode) -> (Vec<u16>, Vec<u8>) {
        match node {
            OpNode::Map(MapKind::Expression {
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
        ];
        let node = decode_op_node(OPCODE_MAP_EXPR, None, Some(vec![1, 2, 3]), &cols).unwrap();
        assert_eq!(reindex_of(node), (vec![3, 3], vec![0, crate::type_code::I64]));
    }

    /// A range-join node decodes its `(n_eq, rel)` from the NODE_COL_KIND_RANGE_JOIN
    /// param row.
    #[test]
    fn decode_range_join_params() {
        let cols = [CircuitNodeColumn {
            kind: NODE_COL_KIND_RANGE_JOIN,
            position: 0,
            value1: 2,
            value2: RangeRel::Gt.as_u64(),
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
    fn decode_partition_filter() {
        let node = decode_op_node(OPCODE_PARTITION_FILTER, None, None, &[]).unwrap();
        assert_eq!(node, OpNode::PartitionFilter);
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

    /// A SCAN node with a NULL `source_table` cell is a corrupt circuit: reject it at
    /// the decode trust boundary rather than coercing to the (invalid) table id 0.
    #[test]
    fn decode_rejects_scan_missing_source_table() {
        assert!(decode_op_node(OPCODE_SCAN_DELTA, None, None, &[])
            .unwrap_err()
            .contains("source_table"));
        assert!(decode_op_node(OPCODE_SCAN_TRACE_TABLE, None, None, &[])
            .unwrap_err()
            .contains("source_table"));
    }
}
