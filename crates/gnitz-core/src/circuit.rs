use crate::expr::ExprProgram;

pub type NodeId  = u64;
pub type Port    = u8;
pub type TableId = u64;

/// Aggregate function discriminant. Numeric values match the wire-level
/// `AGG_*` constants — `Count = 1`, `Sum = 2`, `Min = 3`, `Max = 4`,
/// `CountNonNull = 5`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum AggFunc {
    Count        = gnitz_wire::AGG_COUNT,
    Sum          = gnitz_wire::AGG_SUM,
    Min          = gnitz_wire::AGG_MIN,
    Max          = gnitz_wire::AGG_MAX,
    CountNonNull = gnitz_wire::AGG_COUNT_NON_NULL,
}

impl AggFunc {
    /// Convert a wire-level aggregate function id to the typed variant.
    pub fn from_wire(v: u64) -> Option<Self> {
        match v {
            gnitz_wire::AGG_COUNT          => Some(AggFunc::Count),
            gnitz_wire::AGG_SUM            => Some(AggFunc::Sum),
            gnitz_wire::AGG_MIN            => Some(AggFunc::Min),
            gnitz_wire::AGG_MAX            => Some(AggFunc::Max),
            gnitz_wire::AGG_COUNT_NON_NULL => Some(AggFunc::CountNonNull),
            _ => None,
        }
    }
    pub fn as_u64(self) -> u64 { self as u64 }
}

/// REDUCE aggregation kind. `Null` is GROUP BY without aggregation
/// (equivalent to `AggOp::Null` in the engine and serialises to zero
/// AGG_SPEC rows). `Specs` covers both single- and multi-aggregate REDUCE.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggKind {
    Null,
    Specs(Vec<(AggFunc, u16)>),
}

/// Join physical strategy. `DeltaTrace` is the standard delta-vs-trace
/// shape (port A = delta, port TRACE = integrated other side).
/// `DeltaTraceOuter` is a left outer join. `DeltaDelta` joins two delta
/// inputs directly (used when both sides are pre-integrated downstream).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind { DeltaTrace, DeltaTraceOuter, DeltaDelta }

/// Discriminator for the three MAP variants. Each maps to a distinct wire
/// opcode (`OPCODE_MAP_PROJ` / `OPCODE_MAP_EXPR` / `OPCODE_MAP_KEY_ONLY`),
/// so `load_circuit()` dispatches on opcode alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapKind {
    /// Projection / column reorder, no compute. Carries the list of
    /// payload column indices to keep.
    Projection(Vec<u16>),
    /// Expression-based map. Optional `reindex_col` flips the PK column
    /// (equijoin pre-indexing).
    Expression { program: ExprProgram, reindex_col: Option<u16> },
    /// Drop all payload columns, keep only PK and weight. Used to project
    /// reindexed batches down to join-key-only for distinct tracking.
    KeyOnly,
}

/// Typed operator-node payload. Each variant maps to a distinct opcode;
/// `load_circuit()` dispatches on opcode alone — no presence-of-param check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// `OPCODE_SCAN_DELTA = 11`. Delta input; carries the source `table_id`
    /// directly. Replaces both `input_delta()` (primary trigger) and
    /// `input_delta_tagged()` (secondary join input). Compiler reads
    /// `ext_tables[source_table]` directly; no DEP_TAB walking needed.
    ScanDelta(TableId),
    /// `OPCODE_SCAN_TRACE_TABLE = 31`. Read-only trace source for join
    /// trace ports; never participates in cascade.
    ScanTrace(TableId),
    Filter(Option<ExprProgram>),
    Map(MapKind),
    Negate,
    Union,
    Delay,
    Distinct,
    Reduce { group_cols: Vec<u16>, agg: AggKind },
    Join(JoinKind),
    AntiJoin(JoinKind),
    SemiJoin(JoinKind),
    /// `OPCODE_INTEGRATE = 7`. Primary INTEGRATE: writes to view storage.
    IntegrateSink,
    /// `OPCODE_INTEGRATE_TRACE = 25`. Accumulates Z-set for join trace.
    IntegrateTrace,
    ExchangeShard { shard_cols: Vec<u16> },
    /// `OPCODE_EXCHANGE_GATHER = 21`. Compiler arm is a register-schema
    /// pass-through (no VM instruction emitted). The legacy
    /// `PARAM_GATHER_WORKER` is dead state and not stored.
    ExchangeGather,
    NullExtend { type_codes: Vec<u8> },
    /// `OPCODE_GATHER_REDUCE = 24`. Server-internal opcode reserved for
    /// future multi-worker REDUCE planning.
    GatherReduce,
    /// `OPCODE_SEEK_TRACE = 12`. Server-internal — emitted only by the
    /// compiler.
    SeekTrace,
    /// `OPCODE_CLEAR_DELTAS = 15`. Server-internal — emitted only by the
    /// compiler.
    ClearDeltas,
}

/// In-memory circuit graph: typed `OpNode` per node + (dst,port) → src edges.
#[derive(Clone, Debug)]
pub struct Circuit {
    pub view_id: u64,
    pub nodes: std::collections::BTreeMap<NodeId, OpNode>,
    pub edges: std::collections::BTreeMap<(NodeId, Port), NodeId>,
}

/// Three-table row bundle materialised from a `Circuit` for a single catalog
/// write. Each `Vec` is one logical row in the corresponding system table.
#[derive(Clone, Debug, Default)]
pub struct CircuitRows {
    /// `(node_id, opcode, source_table, reindex_col, expr_program_blob)`.
    /// `source_table` is `None` for nodes that don't carry one;
    /// `reindex_col` is `None` outside `MapKind::Expression { reindex_col: Some(_) }`;
    /// `expr_program` is `None` outside `Filter`/`MapKind::Expression`.
    pub nodes:        Vec<(NodeId, u64, Option<TableId>, Option<u16>, Option<Vec<u8>>)>,
    /// `(dst_node, dst_port, src_node)`. View id is implicit at the call site.
    pub edges:        Vec<(NodeId, Port, NodeId)>,
    /// `(node_id, kind, position, value1, value2)`.
    pub node_columns: Vec<(NodeId, u64, u16, u64, u64)>,
}

impl Circuit {
    /// Tables this view reads cascading deltas from — every `ScanDelta`
    /// node's `source_table`, deduped. `ScanTrace` table_ids are
    /// deliberately excluded (they're read-only lookups; updating them
    /// must NOT trigger view recalculation).
    pub fn dependencies(&self) -> Vec<TableId> {
        let mut deps: Vec<TableId> = Vec::new();
        for op in self.nodes.values() {
            if let OpNode::ScanDelta(tid) = op {
                if !deps.contains(tid) {
                    deps.push(*tid);
                }
            }
        }
        deps
    }

    /// Materialise the circuit into the three-table row bundle. Pure
    /// transformation — no I/O.
    pub fn into_rows(self) -> CircuitRows {
        let mut rows = CircuitRows::default();
        for (nid, op) in self.nodes {
            let (opcode, src_tab, reindex, expr_blob, kind_rows) = encode_op_node(op);
            rows.nodes.push((nid, opcode, src_tab, reindex, expr_blob));
            for (kind, pos, v1, v2) in kind_rows {
                rows.node_columns.push((nid, kind, pos, v1, v2));
            }
        }
        for ((dst, port), src) in self.edges {
            rows.edges.push((dst, port, src));
        }
        rows
    }

    /// Inverse of [`Circuit::into_rows`]. Reconstructs from the three system
    /// tables. Returns `Err(String)` if the rows describe a malformed graph
    /// (unknown opcode, contradicting node-column kind, etc.).
    pub fn from_rows(view_id: u64, rows: CircuitRows) -> Result<Self, String> {
        // Group node-column rows by node_id so each node sees the relevant slice.
        use std::collections::BTreeMap;
        let mut per_node: BTreeMap<NodeId, Vec<(u64, u16, u64, u64)>> = BTreeMap::new();
        for (nid, kind, pos, v1, v2) in rows.node_columns {
            per_node.entry(nid).or_default().push((kind, pos, v1, v2));
        }
        // Sort each group by (kind, position) so the typed payloads come out
        // in the order callers wrote them — the load path relies on this for
        // group_cols / shard_cols / proj_cols / agg_specs / null_extend.
        for v in per_node.values_mut() {
            v.sort_by_key(|&(kind, pos, _, _)| (kind, pos));
        }

        let mut nodes = BTreeMap::new();
        for (nid, opcode, src_tab, reindex, expr_blob) in rows.nodes {
            let cols = per_node.remove(&nid).unwrap_or_default();
            let op = decode_op_node(opcode, src_tab, reindex, expr_blob, &cols)?;
            nodes.insert(nid, op);
        }
        let mut edges = BTreeMap::new();
        for (dst, port, src) in rows.edges {
            edges.insert((dst, port), src);
        }
        Ok(Circuit { view_id, nodes, edges })
    }
}

// Helper: produce the row tuples for a single OpNode.
fn encode_op_node(
    op: OpNode,
) -> (u64, Option<TableId>, Option<u16>, Option<Vec<u8>>, Vec<(u64, u16, u64, u64)>) {
    use gnitz_wire::*;
    match op {
        OpNode::ScanDelta(tid) => (OPCODE_SCAN_DELTA, Some(tid), None, None, Vec::new()),
        OpNode::ScanTrace(tid) => (OPCODE_SCAN_TRACE_TABLE, Some(tid), None, None, Vec::new()),
        OpNode::Filter(expr) => (OPCODE_FILTER, None, None, expr.as_ref().map(|e| e.encode()), Vec::new()),
        OpNode::Map(MapKind::Projection(cols)) => {
            let kind_rows = cols.into_iter().enumerate()
                .map(|(i, c)| (NODE_COL_KIND_PROJ, i as u16, c as u64, 0u64))
                .collect();
            (OPCODE_MAP_PROJ, None, None, None, kind_rows)
        }
        OpNode::Map(MapKind::Expression { program, reindex_col }) => {
            (OPCODE_MAP_EXPR, None, reindex_col, Some(program.encode()), Vec::new())
        }
        OpNode::Map(MapKind::KeyOnly) => (OPCODE_MAP_KEY_ONLY, None, None, None, Vec::new()),
        OpNode::Negate         => (OPCODE_NEGATE, None, None, None, Vec::new()),
        OpNode::Union          => (OPCODE_UNION, None, None, None, Vec::new()),
        OpNode::Delay          => (OPCODE_DELAY, None, None, None, Vec::new()),
        OpNode::Distinct       => (OPCODE_DISTINCT, None, None, None, Vec::new()),
        OpNode::Reduce { group_cols, agg } => {
            let mut kind_rows = Vec::with_capacity(group_cols.len() + 4);
            for (i, c) in group_cols.iter().enumerate() {
                kind_rows.push((NODE_COL_KIND_GROUP, i as u16, *c as u64, 0));
            }
            if let AggKind::Specs(specs) = agg {
                for (i, (func, col)) in specs.into_iter().enumerate() {
                    kind_rows.push((NODE_COL_KIND_AGG_SPEC, i as u16, func.as_u64(), col as u64));
                }
            }
            (OPCODE_REDUCE, None, None, None, kind_rows)
        }
        OpNode::Join(JoinKind::DeltaTrace)         => (OPCODE_JOIN_DELTA_TRACE, None, None, None, Vec::new()),
        OpNode::Join(JoinKind::DeltaTraceOuter)    => (OPCODE_JOIN_DELTA_TRACE_OUTER, None, None, None, Vec::new()),
        OpNode::Join(JoinKind::DeltaDelta)         => (OPCODE_JOIN_DELTA_DELTA, None, None, None, Vec::new()),
        OpNode::AntiJoin(JoinKind::DeltaTrace)      => (OPCODE_ANTI_JOIN_DELTA_TRACE, None, None, None, Vec::new()),
        OpNode::AntiJoin(JoinKind::DeltaTraceOuter) => unreachable!("no wire opcode for anti-join outer; no builder creates this variant"),
        OpNode::AntiJoin(JoinKind::DeltaDelta)      => (OPCODE_ANTI_JOIN_DELTA_DELTA, None, None, None, Vec::new()),
        OpNode::SemiJoin(JoinKind::DeltaTrace)      => (OPCODE_SEMI_JOIN_DELTA_TRACE, None, None, None, Vec::new()),
        OpNode::SemiJoin(JoinKind::DeltaTraceOuter) => unreachable!("no wire opcode for semi-join outer; no builder creates this variant"),
        OpNode::SemiJoin(JoinKind::DeltaDelta)      => (OPCODE_SEMI_JOIN_DELTA_DELTA, None, None, None, Vec::new()),
        OpNode::IntegrateSink  => (OPCODE_INTEGRATE, None, None, None, Vec::new()),
        OpNode::IntegrateTrace => (OPCODE_INTEGRATE_TRACE, None, None, None, Vec::new()),
        OpNode::ExchangeShard { shard_cols } => {
            let kind_rows = shard_cols.into_iter().enumerate()
                .map(|(i, c)| (NODE_COL_KIND_SHARD, i as u16, c as u64, 0))
                .collect();
            (OPCODE_EXCHANGE_SHARD, None, None, None, kind_rows)
        }
        OpNode::ExchangeGather => (OPCODE_EXCHANGE_GATHER, None, None, None, Vec::new()),
        OpNode::NullExtend { type_codes } => {
            let kind_rows = type_codes.into_iter().enumerate()
                .map(|(i, tc)| (NODE_COL_KIND_NULL_EXT, i as u16, tc as u64, 0))
                .collect();
            (OPCODE_NULL_EXTEND, None, None, None, kind_rows)
        }
        OpNode::GatherReduce  => (OPCODE_GATHER_REDUCE, None, None, None, Vec::new()),
        OpNode::SeekTrace     => (OPCODE_SEEK_TRACE, None, None, None, Vec::new()),
        OpNode::ClearDeltas   => (OPCODE_CLEAR_DELTAS, None, None, None, Vec::new()),
    }
}

// Helper: reconstruct an OpNode from its row tuples.
fn decode_op_node(
    opcode: u64,
    src_tab: Option<TableId>,
    reindex: Option<u16>,
    expr_blob: Option<Vec<u8>>,
    cols: &[(u64, u16, u64, u64)],
) -> Result<OpNode, String> {
    use gnitz_wire::*;
    let collect_cols = |kind: u64| -> Vec<u16> {
        cols.iter()
            .filter(|(k, _, _, _)| *k == kind)
            .map(|(_, _, v1, _)| *v1 as u16)
            .collect()
    };
    let collect_typecodes = |kind: u64| -> Vec<u8> {
        cols.iter()
            .filter(|(k, _, _, _)| *k == kind)
            .map(|(_, _, v1, _)| *v1 as u8)
            .collect()
    };
    let collect_aggs = || -> Result<Vec<(AggFunc, u16)>, String> {
        cols.iter()
            .filter(|(k, _, _, _)| *k == NODE_COL_KIND_AGG_SPEC)
            .map(|(_, _, v1, v2)| {
                AggFunc::from_wire(*v1)
                    .ok_or_else(|| format!("unknown agg func id {}", v1))
                    .map(|f| (f, *v2 as u16))
            })
            .collect()
    };
    Ok(match opcode {
        x if x == OPCODE_SCAN_DELTA => OpNode::ScanDelta(src_tab.unwrap_or(0)),
        x if x == OPCODE_SCAN_TRACE_TABLE => OpNode::ScanTrace(src_tab.unwrap_or(0)),
        x if x == OPCODE_FILTER => {
            let expr = match expr_blob {
                Some(b) => Some(ExprProgram::decode(&b).map_err(|e| e.to_string())?),
                None => None,
            };
            OpNode::Filter(expr)
        }
        x if x == OPCODE_MAP_PROJ => OpNode::Map(MapKind::Projection(collect_cols(NODE_COL_KIND_PROJ))),
        x if x == OPCODE_MAP_EXPR => {
            let blob = expr_blob.ok_or_else(|| "MAP_EXPR missing expr_program blob".to_string())?;
            let program = ExprProgram::decode(&blob).map_err(|e| e.to_string())?;
            OpNode::Map(MapKind::Expression { program, reindex_col: reindex })
        }
        x if x == OPCODE_MAP_KEY_ONLY => OpNode::Map(MapKind::KeyOnly),
        x if x == OPCODE_NEGATE  => OpNode::Negate,
        x if x == OPCODE_UNION   => OpNode::Union,
        x if x == OPCODE_DELAY   => OpNode::Delay,
        x if x == OPCODE_DISTINCT => OpNode::Distinct,
        x if x == OPCODE_REDUCE => {
            let group_cols = collect_cols(NODE_COL_KIND_GROUP);
            let specs = collect_aggs()?;
            let agg = if specs.is_empty() { AggKind::Null } else { AggKind::Specs(specs) };
            OpNode::Reduce { group_cols, agg }
        }
        x if x == OPCODE_JOIN_DELTA_TRACE       => OpNode::Join(JoinKind::DeltaTrace),
        x if x == OPCODE_JOIN_DELTA_TRACE_OUTER => OpNode::Join(JoinKind::DeltaTraceOuter),
        x if x == OPCODE_JOIN_DELTA_DELTA       => OpNode::Join(JoinKind::DeltaDelta),
        x if x == OPCODE_ANTI_JOIN_DELTA_TRACE  => OpNode::AntiJoin(JoinKind::DeltaTrace),
        x if x == OPCODE_ANTI_JOIN_DELTA_DELTA  => OpNode::AntiJoin(JoinKind::DeltaDelta),
        x if x == OPCODE_SEMI_JOIN_DELTA_TRACE  => OpNode::SemiJoin(JoinKind::DeltaTrace),
        x if x == OPCODE_SEMI_JOIN_DELTA_DELTA  => OpNode::SemiJoin(JoinKind::DeltaDelta),
        x if x == OPCODE_INTEGRATE              => OpNode::IntegrateSink,
        x if x == OPCODE_INTEGRATE_TRACE        => OpNode::IntegrateTrace,
        x if x == OPCODE_EXCHANGE_SHARD         => OpNode::ExchangeShard { shard_cols: collect_cols(NODE_COL_KIND_SHARD) },
        x if x == OPCODE_EXCHANGE_GATHER        => OpNode::ExchangeGather,
        x if x == OPCODE_NULL_EXTEND            => OpNode::NullExtend { type_codes: collect_typecodes(NODE_COL_KIND_NULL_EXT) },
        x if x == OPCODE_GATHER_REDUCE          => OpNode::GatherReduce,
        x if x == OPCODE_SEEK_TRACE             => OpNode::SeekTrace,
        x if x == OPCODE_CLEAR_DELTAS           => OpNode::ClearDeltas,
        _ => return Err(format!("unknown opcode {}", opcode)),
    })
}

/// Fluent builder for DBSP circuit graphs, producing a typed [`Circuit`]
/// for `GnitzClient::create_view_with_circuit`.
///
/// Sequential `node_id`s start at 1 (mirroring the legacy slot encoding's
/// 40-bit cap). `primary_source_id` is the table_id passed to the first
/// `input_delta()` call so legacy callers don't have to thread it through
/// every method invocation.
pub struct CircuitBuilder {
    view_id:           u64,
    primary_source_id: u64,
    next_node_id:      u64,
    nodes: std::collections::BTreeMap<NodeId, OpNode>,
    edges: std::collections::BTreeMap<(NodeId, Port), NodeId>,
}

impl CircuitBuilder {
    pub fn new(view_id: u64, primary_source_id: u64) -> Self {
        CircuitBuilder {
            view_id,
            primary_source_id,
            next_node_id: 1,
            nodes: std::collections::BTreeMap::new(),
            edges: std::collections::BTreeMap::new(),
        }
    }

    fn alloc_node(&mut self, op: OpNode) -> NodeId {
        let nid = self.next_node_id;
        self.next_node_id += 1;
        self.nodes.insert(nid, op);
        nid
    }

    fn connect(&mut self, src: NodeId, dst: NodeId, port: u64) {
        let port_u8 = port as Port;
        self.edges.insert((dst, port_u8), src);
    }

    /// Primary delta input. Carries the `primary_source_id` set at builder
    /// construction. Replaces the legacy "SCAN_TRACE with source=0 +
    /// dependency lookup" trick.
    pub fn input_delta(&mut self) -> NodeId {
        self.alloc_node(OpNode::ScanDelta(self.primary_source_id))
    }

    /// Read-only trace source for a join trace port. Never participates in
    /// cascade — its table_id is excluded from `dependencies()`.
    pub fn trace_scan(&mut self, table_id: u64) -> NodeId {
        self.alloc_node(OpNode::ScanTrace(table_id))
    }

    /// Tagged secondary delta input for multi-input views (e.g. equijoin).
    /// `source_table_id` becomes a real dependency.
    pub fn input_delta_tagged(&mut self, source_table_id: u64) -> NodeId {
        self.alloc_node(OpNode::ScanDelta(source_table_id))
    }

    pub fn filter(&mut self, input: NodeId, expr: Option<ExprProgram>) -> NodeId {
        let nid = self.alloc_node(OpNode::Filter(expr));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    pub fn map_expr(&mut self, input: NodeId, program: ExprProgram) -> NodeId {
        let nid = self.alloc_node(OpNode::Map(MapKind::Expression { program, reindex_col: None }));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Map with PK reindexing (equijoin pre-indexing). The new PK column is
    /// `reindex_col` from the input schema.
    pub fn map_reindex(&mut self, input: NodeId, reindex_col: usize, program: ExprProgram) -> NodeId {
        let nid = self.alloc_node(OpNode::Map(MapKind::Expression {
            program,
            reindex_col: Some(reindex_col as u16),
        }));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Pure projection: keep only the listed payload columns, in order.
    pub fn map(&mut self, input: NodeId, projection: &[usize]) -> NodeId {
        let cols: Vec<u16> = projection.iter().map(|&c| c as u16).collect();
        let nid = self.alloc_node(OpNode::Map(MapKind::Projection(cols)));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Strip all payload columns, keep only PK and weight.
    pub fn map_key_only(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::Map(MapKind::KeyOnly));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    pub fn negate(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::Negate);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::Union);
        self.connect(a, nid, gnitz_wire::PORT_IN_A);
        self.connect(b, nid, gnitz_wire::PORT_IN_B);
        nid
    }

    pub fn delay(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::Delay);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    pub fn distinct(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::Distinct);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    fn binary_join(&mut self, op: OpNode, delta: NodeId, trace_node: NodeId) -> NodeId {
        let nid = self.alloc_node(op);
        self.connect(delta, nid, gnitz_wire::PORT_IN_A);
        self.connect(trace_node, nid, gnitz_wire::PORT_TRACE);
        nid
    }

    fn binary_join_scan(&mut self, op: OpNode, delta: NodeId, trace_table_id: u64) -> NodeId {
        let trace = self.trace_scan(trace_table_id);
        self.binary_join(op, delta, trace)
    }

    pub fn join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OpNode::Join(JoinKind::DeltaTrace), delta, trace_table_id)
    }

    pub fn anti_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OpNode::AntiJoin(JoinKind::DeltaTrace), delta, trace_table_id)
    }

    pub fn semi_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OpNode::SemiJoin(JoinKind::DeltaTrace), delta, trace_table_id)
    }

    pub fn join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::Join(JoinKind::DeltaTrace), delta, trace_node)
    }

    pub fn anti_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::AntiJoin(JoinKind::DeltaTrace), delta, trace_node)
    }

    pub fn left_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OpNode::Join(JoinKind::DeltaTraceOuter), delta, trace_table_id)
    }

    pub fn left_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::Join(JoinKind::DeltaTraceOuter), delta, trace_node)
    }

    pub fn semi_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::SemiJoin(JoinKind::DeltaTrace), delta, trace_node)
    }

    /// Reduce with automatic shard insertion (required for multi-worker correctness).
    pub fn reduce(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_func_id: u64,
        agg_col_idx: usize,
    ) -> NodeId {
        let sharded = self.shard(input, group_cols);
        let group: Vec<u16> = group_cols.iter().map(|&c| c as u16).collect();
        let func = AggFunc::from_wire(agg_func_id)
            .unwrap_or_else(|| panic!("unknown agg func id {}", agg_func_id));
        let nid = self.alloc_node(OpNode::Reduce {
            group_cols: group,
            agg: AggKind::Specs(vec![(func, agg_col_idx as u16)]),
        });
        self.connect(sharded, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Multi-aggregate reduce. `agg_specs`: list of (agg_func_id, col_idx).
    pub fn reduce_multi(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(u64, usize)],
    ) -> NodeId {
        let sharded = self.shard(input, group_cols);
        let group: Vec<u16> = group_cols.iter().map(|&c| c as u16).collect();
        let specs: Vec<(AggFunc, u16)> = agg_specs.iter()
            .map(|&(func_id, col)| (
                AggFunc::from_wire(func_id).unwrap_or_else(|| panic!("unknown agg func id {}", func_id)),
                col as u16,
            ))
            .collect();
        let nid = self.alloc_node(OpNode::Reduce {
            group_cols: group,
            agg: if specs.is_empty() { AggKind::Null } else { AggKind::Specs(specs) },
        });
        self.connect(sharded, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Exchange shard: routes rows to workers by hashing the given columns.
    pub fn shard(&mut self, input: NodeId, shard_cols: &[usize]) -> NodeId {
        let cols: Vec<u16> = shard_cols.iter().map(|&c| c as u16).collect();
        let nid = self.alloc_node(OpNode::ExchangeShard { shard_cols: cols });
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Exchange gather: collects results from all workers (no worker_id —
    /// the legacy parameter was dead state).
    pub fn gather(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::ExchangeGather);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Intermediate trace integration (equijoin accumulator).
    pub fn integrate_trace(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::IntegrateTrace);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Null-extend: appends N null payload columns to each row.
    /// `right_col_type_codes` contains the column type code (u8) for each
    /// null column to append.
    pub fn null_extend(&mut self, input: NodeId, right_col_type_codes: &[u64]) -> NodeId {
        let codes: Vec<u8> = right_col_type_codes.iter().map(|&t| t as u8).collect();
        let nid = self.alloc_node(OpNode::NullExtend { type_codes: codes });
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// **Deprecated no-op.** Const strings are now embedded directly in the
    /// `ExprProgram` blob; the compiler reads them via `ExprProgram::decode`.
    /// Retained as an empty stub so SQL planner / Python / C API call sites
    /// continue to compile during the rewrite.
    pub fn add_const_string(&mut self, _node_id: NodeId, _index: u32, _value: String) {}

    /// Sink — primary INTEGRATE that writes to view storage.
    pub fn sink(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::IntegrateSink);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Finalises the circuit.
    pub fn build(self) -> Circuit {
        Circuit { view_id: self.view_id, nodes: self.nodes, edges: self.edges }
    }
}
