use crate::expr::ExprProgram;

pub use gnitz_wire::{AggFunc, AggKind, JoinKind, MapKind, OpNode, RangeRel, SetJoinKind};

pub type NodeId = u64;
pub type Port = u8;
pub type TableId = u64;

/// In-memory circuit graph: typed `OpNode` per node + (dst,port) → src edges.
#[derive(Clone, Debug)]
pub struct Circuit {
    pub view_id: u64,
    pub nodes: std::collections::BTreeMap<NodeId, OpNode>,
    pub edges: std::collections::BTreeMap<(NodeId, Port), NodeId>,
}

/// `(opcode, source_table, expr_program_blob)` — the fields of a `nodes`
/// system-table row excluding the node id (which is added by the caller).
/// Reused by both `CircuitRows::nodes` and `encode_op_node`. The reindex column
/// list is stored in `CircuitNodeColumns` under `NODE_COL_KIND_REINDEX`.
pub type NodeFields = (u64, Option<TableId>, Option<Vec<u8>>);

/// One full row of the `nodes` system table: node id + [`NodeFields`].
pub type NodeRow = (NodeId, u64, Option<TableId>, Option<Vec<u8>>);

/// One row of the `node_columns` system table:
/// `(kind, position, value1, value2)` — the node_id is prepended by the caller.
pub type NodeColumnPayload = (u64, u16, u64, u64);

/// Three-table row bundle materialised from a `Circuit` for a single catalog
/// write. Each `Vec` is one logical row in the corresponding system table.
#[derive(Clone, Debug, Default)]
pub struct CircuitRows {
    /// `source_table` is `None` for nodes that don't carry one; the reindex column
    /// list is stored in `node_columns` under `NODE_COL_KIND_REINDEX`;
    /// `expr_program` is `None` outside `Filter`/`MapKind::Expression`.
    pub nodes: Vec<NodeRow>,
    /// `(dst_node, dst_port, src_node)`. View id is implicit at the call site.
    pub edges: Vec<(NodeId, Port, NodeId)>,
    /// `(node_id, kind, position, value1, value2)`.
    pub node_columns: Vec<(NodeId, u64, u16, u64, u64)>,
}

impl Circuit {
    /// Tables this view reads cascading deltas from — every `ScanDelta`
    /// node's `source_table`, deduped. `ScanTrace` table_ids are
    /// deliberately excluded (they're read-only lookups; updating them
    /// must NOT trigger view recalculation).
    pub fn dependencies(&self) -> Vec<TableId> {
        // A view's dependency set is 1–4 entries; a linear `Vec::contains` dedup
        // is alloc-free and beats a HashSet at this n (same small-n convention as
        // `Schema::validate_pk_cols`). Iterating `nodes` in BTreeMap key order
        // preserves the first-wins ordering of the prior HashSet-insert filter.
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
            let ((opcode, src_tab, expr_blob), kind_rows) = encode_op_node(op);
            rows.nodes.push((nid, opcode, src_tab, expr_blob));
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
        let mut per_node: BTreeMap<NodeId, Vec<gnitz_wire::CircuitNodeColumn>> = BTreeMap::new();
        for (nid, kind, pos, v1, v2) in rows.node_columns {
            per_node.entry(nid).or_default().push(gnitz_wire::CircuitNodeColumn {
                kind,
                position: pos,
                value1: v1,
                value2: v2,
            });
        }
        // Sort each group by (kind, position) so the typed payloads come out
        // in the order callers wrote them — the load path relies on this for
        // group_cols / shard_cols / proj_cols / agg_specs / null_extend / reindex_cols.
        for v in per_node.values_mut() {
            v.sort_by_key(|c| (c.kind, c.position));
        }

        let mut nodes = BTreeMap::new();
        for (nid, opcode, src_tab, expr_blob) in rows.nodes {
            let cols: Vec<gnitz_wire::CircuitNodeColumn> = per_node.remove(&nid).unwrap_or_default();
            let op = gnitz_wire::decode_op_node(opcode, src_tab, expr_blob, &cols)?;
            nodes.insert(nid, op);
        }
        let mut edges = BTreeMap::new();
        for (dst, port, src) in rows.edges {
            edges.insert((dst, port), src);
        }
        Ok(Circuit { view_id, nodes, edges })
    }
}

fn encode_col_list<I, T>(kind: u64, iter: I) -> Vec<(u64, u16, u64, u64)>
where
    I: IntoIterator<Item = T>,
    T: Into<u64>,
{
    iter.into_iter()
        .enumerate()
        .map(|(i, v)| (kind, i as u16, v.into(), 0u64))
        .collect()
}

fn encode_op_node(op: OpNode) -> (NodeFields, Vec<NodeColumnPayload>) {
    use gnitz_wire::*;
    match op {
        OpNode::ScanDelta(tid) => ((OPCODE_SCAN_DELTA, Some(tid), None), Vec::new()),
        OpNode::ScanTrace(tid) => ((OPCODE_SCAN_TRACE_TABLE, Some(tid), None), Vec::new()),
        OpNode::Filter(blob) => ((OPCODE_FILTER, None, blob), Vec::new()),
        OpNode::Map(MapKind::Projection(cols)) => {
            ((OPCODE_MAP_PROJ, None, None), encode_col_list(NODE_COL_KIND_PROJ, cols))
        }
        OpNode::Map(MapKind::Expression {
            program,
            reindex_cols,
            reindex_target_tcs,
        }) => {
            // value1 = column index, value2 = promoted target type code (0 = derive
            // from source). Hand-rolled (not encode_col_list) so the shared
            // PROJ/HashRow helper stays a pure index list; the planner writes 0 on a
            // side already at T, keeping same-type / U128-vs-UUID / string circuits
            // byte-identical.
            let kind_rows: Vec<(u64, u16, u64, u64)> = reindex_cols
                .iter()
                .enumerate()
                .map(|(i, &col)| {
                    let v2 = reindex_target_tcs.get(i).copied().unwrap_or(0) as u64;
                    (NODE_COL_KIND_REINDEX, i as u16, col as u64, v2)
                })
                .collect();
            ((OPCODE_MAP_EXPR, None, Some(program)), kind_rows)
        }
        OpNode::Map(MapKind::KeyOnly) => ((OPCODE_MAP_KEY_ONLY, None, None), Vec::new()),
        OpNode::Map(MapKind::HashRow(cols, branch_id)) => {
            let mut kind_rows = encode_col_list(NODE_COL_KIND_PROJ, cols);
            kind_rows.push((NODE_COL_KIND_BRANCH_ID, 0, branch_id as u64, 0));
            ((OPCODE_MAP_HASH_ROW, None, None), kind_rows)
        }
        OpNode::Negate => ((OPCODE_NEGATE, None, None), Vec::new()),
        OpNode::Union => ((OPCODE_UNION, None, None), Vec::new()),
        OpNode::Delay => ((OPCODE_DELAY, None, None), Vec::new()),
        OpNode::Distinct => ((OPCODE_DISTINCT, None, None), Vec::new()),
        OpNode::Reduce {
            group_cols,
            agg,
            global_ground,
        } => {
            let mut kind_rows = Vec::with_capacity(group_cols.len() + 4);
            for (i, c) in group_cols.iter().enumerate() {
                kind_rows.push((NODE_COL_KIND_GROUP, i as u16, *c as u64, 0));
            }
            if let AggKind::Specs(specs) = agg {
                for (i, (func, col)) in specs.into_iter().enumerate() {
                    kind_rows.push((NODE_COL_KIND_AGG_SPEC, i as u16, func.as_u64(), col as u64));
                }
            }
            // Only the user's global scalar aggregate carries the row; an
            // ordinary grouped / range-join reduce omits it (decodes to `false`),
            // keeping every existing reduce circuit byte-identical on the wire.
            if global_ground {
                kind_rows.push((NODE_COL_KIND_GLOBAL_GROUND, 0, 1, 0));
            }
            ((OPCODE_REDUCE, None, None), kind_rows)
        }
        OpNode::Join(JoinKind::DeltaTrace) => ((OPCODE_JOIN_DELTA_TRACE, None, None), Vec::new()),
        OpNode::Join(JoinKind::DeltaTraceOuter) => ((OPCODE_JOIN_DELTA_TRACE_OUTER, None, None), Vec::new()),
        OpNode::Join(JoinKind::DeltaDelta) => ((OPCODE_JOIN_DELTA_DELTA, None, None), Vec::new()),
        OpNode::Join(JoinKind::DeltaTraceRange { n_eq, rel }) => (
            (OPCODE_JOIN_DELTA_TRACE_RANGE, None, None),
            vec![(NODE_COL_KIND_RANGE_JOIN, 0, n_eq as u64, rel.as_u64())],
        ),
        OpNode::AntiJoin(SetJoinKind::DeltaTrace) => ((OPCODE_ANTI_JOIN_DELTA_TRACE, None, None), Vec::new()),
        OpNode::AntiJoin(SetJoinKind::DeltaDelta) => ((OPCODE_ANTI_JOIN_DELTA_DELTA, None, None), Vec::new()),
        OpNode::SemiJoin(SetJoinKind::DeltaTrace) => ((OPCODE_SEMI_JOIN_DELTA_TRACE, None, None), Vec::new()),
        OpNode::SemiJoin(SetJoinKind::DeltaDelta) => ((OPCODE_SEMI_JOIN_DELTA_DELTA, None, None), Vec::new()),
        OpNode::IntegrateSink => ((OPCODE_INTEGRATE, None, None), Vec::new()),
        OpNode::IntegrateTrace => ((OPCODE_INTEGRATE_TRACE, None, None), Vec::new()),
        OpNode::ExchangeShard { shard_cols } => (
            (OPCODE_EXCHANGE_SHARD, None, None),
            encode_col_list(NODE_COL_KIND_SHARD, shard_cols),
        ),
        OpNode::ExchangeGather => ((OPCODE_EXCHANGE_GATHER, None, None), Vec::new()),
        OpNode::NullExtend { type_codes } => (
            (OPCODE_NULL_EXTEND, None, None),
            encode_col_list(NODE_COL_KIND_NULL_EXT, type_codes),
        ),
        OpNode::SeekTrace => ((OPCODE_SEEK_TRACE, None, None), Vec::new()),
        OpNode::ClearDeltas => ((OPCODE_CLEAR_DELTAS, None, None), Vec::new()),
        OpNode::PartitionFilter => ((OPCODE_PARTITION_FILTER, None, None), Vec::new()),
    }
}

/// Fluent builder for DBSP circuit graphs, producing a typed [`Circuit`]
/// for `GnitzClient::create_view_with_circuit`.
///
/// Sequential `node_id`s start at 1 (mirroring the legacy slot encoding's
/// 40-bit cap). `primary_source_id` is the table_id passed to the first
/// `input_delta()` call so legacy callers don't have to thread it through
/// every method invocation.
pub struct CircuitBuilder {
    view_id: u64,
    primary_source_id: u64,
    next_node_id: u64,
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
        let nid = self.alloc_node(OpNode::Filter(expr.map(|e| e.encode())));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    pub fn map_expr(&mut self, input: NodeId, program: ExprProgram) -> NodeId {
        let blob = program.encode();
        let nid = self.alloc_node(OpNode::Map(MapKind::Expression {
            program: blob,
            reindex_cols: Vec::new(),
            reindex_target_tcs: Vec::new(),
        }));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Map with PK reindexing (equijoin pre-indexing). The new synthetic PK is built
    /// from `reindex_cols` of the input schema, in the given order. Pass a one-element
    /// slice for a single-column join key.
    ///
    /// `target_tcs` is parallel to `reindex_cols`: entry `i` is the promoted key
    /// type code `T` for a cross-width equijoin key slot, or `0` to derive the slot
    /// type from the source column (the same-type / legacy path). Pass an empty
    /// slice (or all-zero) for a non-promoted reindex; the result is byte-identical
    /// to the pre-promotion serialization.
    pub fn map_reindex(
        &mut self,
        input: NodeId,
        reindex_cols: &[usize],
        target_tcs: &[u8],
        program: ExprProgram,
    ) -> NodeId {
        let blob = program.encode();
        let nid = self.alloc_node(OpNode::Map(MapKind::Expression {
            program: blob,
            reindex_cols: reindex_cols.iter().map(|&c| c as u16).collect(),
            reindex_target_tcs: target_tcs.to_vec(),
        }));
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Full-row-identity reindex: keep the listed columns as payload (in order)
    /// and set the synthetic PK to a hash of those payload bytes, so set
    /// membership is decided by the projected row content, not by the source PK
    /// (EXCEPT/INTERSECT/DISTINCT).
    ///
    /// `branch_id` is mixed into the hash; pass distinct ids (0 and 1) to the two
    /// sides of a `UNION ALL` so identical rows do not collide to one PK, and 0
    /// to both sides of deduplicating set-ops.
    pub fn map_hash_row(&mut self, input: NodeId, projection: &[usize], branch_id: u8) -> NodeId {
        let cols: Vec<u16> = projection.iter().map(|&c| c as u16).collect();
        let nid = self.alloc_node(OpNode::Map(MapKind::HashRow(cols, branch_id)));
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
        self.binary_join_scan(OpNode::AntiJoin(SetJoinKind::DeltaTrace), delta, trace_table_id)
    }

    pub fn semi_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OpNode::SemiJoin(SetJoinKind::DeltaTrace), delta, trace_table_id)
    }

    pub fn join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::Join(JoinKind::DeltaTrace), delta, trace_node)
    }

    /// Non-equi (range) join term: the delta probes `trace_node` with an ordered
    /// half-open range walk per the §3 cut-point table. `n_eq` leading key slots
    /// are equality-pinned (the band-join prefix); `rel` is the relation the trace
    /// slot must satisfy versus the delta slot. Mirrors `join_with_trace_node` but
    /// for `JoinKind::DeltaTraceRange`.
    pub fn join_with_trace_range_node(&mut self, delta: NodeId, trace_node: NodeId, n_eq: u8, rel: RangeRel) -> NodeId {
        self.binary_join(OpNode::Join(JoinKind::DeltaTraceRange { n_eq, rel }), delta, trace_node)
    }

    /// Keep only rows this worker owns (by packed-PK partition) before they
    /// integrate into a **pure** range-join trace under the broadcast input relay
    /// (a band join's eq-prefix scatter omits this node — its trace is already
    /// eq-prefix-partitioned). Worker identity is baked in at compile time, so the
    /// node carries no payload; single-process compiles emit `(0, 1)` = keep-all.
    pub fn partition_filter(&mut self, input: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::PartitionFilter);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    pub fn anti_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::AntiJoin(SetJoinKind::DeltaTrace), delta, trace_node)
    }

    pub fn left_join(&mut self, delta: NodeId, trace_table_id: u64) -> NodeId {
        self.binary_join_scan(OpNode::Join(JoinKind::DeltaTraceOuter), delta, trace_table_id)
    }

    pub fn left_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::Join(JoinKind::DeltaTraceOuter), delta, trace_node)
    }

    pub fn semi_join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.binary_join(OpNode::SemiJoin(SetJoinKind::DeltaTrace), delta, trace_node)
    }

    /// Shared `Reduce`-node construction: map the group cols + agg specs, alloc
    /// the node, and wire `input` to `PORT_IN`. The caller decides the
    /// partitioning of `input` — `reduce`/`reduce_multi` shard first;
    /// `reduce_multi_local` passes a deliberately pre-replicated input straight
    /// through. Empty `agg_specs` ⇒ `AggKind::Null` (group-only distinct-reduce).
    fn reduce_node(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(u64, usize)],
        global_ground: bool,
    ) -> NodeId {
        let group: Vec<u16> = group_cols.iter().map(|&c| c as u16).collect();
        let specs: Vec<(AggFunc, u16)> = agg_specs
            .iter()
            .map(|&(func_id, col)| {
                (
                    AggFunc::from_wire(func_id).unwrap_or_else(|| panic!("unknown agg func id {func_id}")),
                    col as u16,
                )
            })
            .collect();
        let nid = self.alloc_node(OpNode::Reduce {
            group_cols: group,
            agg: if specs.is_empty() {
                AggKind::Null
            } else {
                AggKind::Specs(specs)
            },
            global_ground,
        });
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    /// Reduce with automatic shard insertion (required for multi-worker correctness).
    pub fn reduce(&mut self, input: NodeId, group_cols: &[usize], agg_func_id: u64, agg_col_idx: usize) -> NodeId {
        let sharded = self.shard(input, group_cols);
        // The low-level single-agg API is never the user's global scalar
        // aggregate (that path goes through `reduce_multi`/`reduce_multi_local`),
        // so it never seeds a ground row.
        self.reduce_node(sharded, group_cols, &[(agg_func_id, agg_col_idx)], false)
    }

    /// Multi-aggregate reduce with automatic shard insertion (required for
    /// multi-worker correctness). `agg_specs`: list of (agg_func_id, col_idx).
    /// `global_ground` is `true` only for the user's ungrouped scalar aggregate
    /// (empty `group_cols`); the grouped builder passes `group_cols.is_empty()`.
    pub fn reduce_multi(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(u64, usize)],
        global_ground: bool,
    ) -> NodeId {
        let sharded = self.shard(input, group_cols);
        self.reduce_node(sharded, group_cols, agg_specs, global_ground)
    }

    /// Shard-free multi-aggregate reduce: aggregates `input` **locally on every
    /// worker** with NO upstream `ExchangeShard`. Two valid modes, distinguished by
    /// `input`'s partitioning (the builder cannot type-enforce which):
    ///
    /// * **Replicated input** (byte-identical *contents* per worker): each worker's
    ///   local reduce computes the SAME full global aggregate. Pass
    ///   `global_ground = true` for the user's ungrouped scalar aggregate so each
    ///   worker seeds the ground over an empty source.
    /// * **Partitioned input** (the two-phase global aggregate, phase 1): each worker
    ///   folds its own shard into a per-worker *partial*, which a downstream
    ///   `reduce_multi` then exchanges (≤ N partials) and combines. Pass
    ///   `global_ground = false` — a worker with no local rows must contribute no
    ///   partial, never a spurious per-worker ground row.
    ///
    /// The LEFT range-join threshold reduce (also empty group cols) likewise passes
    /// `false` so it never seeds a spurious `(m=NULL)` row.
    pub fn reduce_multi_local(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(u64, usize)],
        global_ground: bool,
    ) -> NodeId {
        self.reduce_node(input, group_cols, agg_specs, global_ground)
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
        Circuit {
            view_id: self.view_id,
            nodes: self.nodes,
            edges: self.edges,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_wire::NODE_COL_KIND_REINDEX;

    fn empty_prog() -> ExprProgram {
        ExprProgram {
            num_regs: 0,
            result_reg: 0,
            code: Vec::new(),
            const_strings: Vec::new(),
        }
    }

    /// A compound (2-column) reindex descriptor must survive into_rows → from_rows
    /// with its column *order* preserved, stored under NODE_COL_KIND_REINDEX with
    /// position = key order.
    #[test]
    fn reindex_cols_roundtrip_ordered() {
        let (c1, c2) = (2usize, 5usize);
        let mut cb = CircuitBuilder::new(7, 100);
        let input = cb.input_delta();
        let map_nid = cb.map_reindex(input, &[c1, c2], &[], empty_prog());
        let circuit = cb.build();

        let rows = circuit.into_rows();

        // Exactly two NODE_COL_KIND_REINDEX rows, position-ordered, value1 = column.
        let mut reindex_rows: Vec<_> = rows
            .node_columns
            .iter()
            .filter(|(nid, kind, _, _, _)| *nid == map_nid && *kind == NODE_COL_KIND_REINDEX)
            .map(|&(_, _, pos, v1, v2)| (pos, v1, v2))
            .collect();
        reindex_rows.sort_by_key(|&(pos, _, _)| pos);
        assert_eq!(reindex_rows, vec![(0, c1 as u64, 0), (1, c2 as u64, 0)]);

        // Round-trip: decode preserves the ordered list.
        let decoded = Circuit::from_rows(7, rows).expect("from_rows");
        match decoded.nodes.get(&map_nid) {
            Some(OpNode::Map(MapKind::Expression { reindex_cols, .. })) => {
                assert_eq!(*reindex_cols, vec![c1 as u16, c2 as u16], "order must be preserved");
            }
            other => panic!("expected Map(Expression), got {other:?}"),
        }
    }

    /// A cross-width reindex carries a per-slot promoted target type code `T` in
    /// `value2`; it survives into_rows → from_rows parallel to the columns. A `0`
    /// slot means "derive from source".
    #[test]
    fn reindex_target_tcs_roundtrip() {
        use gnitz_wire::type_code;
        let mut cb = CircuitBuilder::new(7, 100);
        let input = cb.input_delta();
        // Overlapping key [x, x] with distinct per-slot targets: slot 0 derives,
        // slot 1 promotes to I64.
        let map_nid = cb.map_reindex(input, &[3, 3], &[0, type_code::I64], empty_prog());
        let rows = cb.build().into_rows();

        let mut reindex_rows: Vec<_> = rows
            .node_columns
            .iter()
            .filter(|(nid, kind, _, _, _)| *nid == map_nid && *kind == NODE_COL_KIND_REINDEX)
            .map(|&(_, _, pos, v1, v2)| (pos, v1, v2))
            .collect();
        reindex_rows.sort_by_key(|&(pos, _, _)| pos);
        assert_eq!(reindex_rows, vec![(0, 3, 0), (1, 3, type_code::I64 as u64)]);

        let decoded = Circuit::from_rows(7, rows).expect("from_rows");
        match decoded.nodes.get(&map_nid) {
            Some(OpNode::Map(MapKind::Expression {
                reindex_cols,
                reindex_target_tcs,
                ..
            })) => {
                assert_eq!(*reindex_cols, vec![3, 3]);
                assert_eq!(*reindex_target_tcs, vec![0, type_code::I64]);
            }
            other => panic!("expected Map(Expression), got {other:?}"),
        }
    }

    /// A range-join node round-trips its `(n_eq, rel)` through the single
    /// NODE_COL_KIND_RANGE_JOIN param row, and a partition-filter node round-trips
    /// as a bare opcode.
    #[test]
    fn range_join_and_partition_filter_roundtrip() {
        use gnitz_wire::NODE_COL_KIND_RANGE_JOIN;
        let mut cb = CircuitBuilder::new(9, 100);
        let a = cb.input_delta_tagged(100);
        let b = cb.input_delta_tagged(200);
        let reindex_b = cb.map_reindex(b, &[0], &[], empty_prog());
        let filt_b = cb.partition_filter(reindex_b);
        let trace_b = cb.integrate_trace(filt_b);
        let join = cb.join_with_trace_range_node(a, trace_b, 1, RangeRel::Le);
        cb.sink(join);
        let rows = cb.build().into_rows();

        // Exactly one range-join param row: (n_eq=1, rel=Le).
        let rj: Vec<_> = rows
            .node_columns
            .iter()
            .filter(|(_, kind, ..)| *kind == NODE_COL_KIND_RANGE_JOIN)
            .map(|&(_, _, pos, v1, v2)| (pos, v1, v2))
            .collect();
        assert_eq!(rj, vec![(0, 1, RangeRel::Le.as_u64())]);

        let decoded = Circuit::from_rows(9, rows).expect("from_rows");
        assert!(decoded.nodes.values().any(|n| matches!(n, OpNode::PartitionFilter)));
        assert!(decoded.nodes.values().any(|n| matches!(
            n,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 1,
                rel: RangeRel::Le
            })
        )));
    }

    /// Every `RangeRel` survives the wire round-trip with the right discriminant.
    #[test]
    fn range_rel_roundtrips_all_four() {
        for rel in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
            let mut cb = CircuitBuilder::new(1, 100);
            let a = cb.input_delta_tagged(100);
            let trace = cb.trace_scan(200);
            let join = cb.join_with_trace_range_node(a, trace, 0, rel);
            cb.sink(join);
            let decoded = Circuit::from_rows(1, cb.build().into_rows()).expect("from_rows");
            assert!(
                decoded
                    .nodes
                    .values()
                    .any(|n| matches!(n, OpNode::Join(JoinKind::DeltaTraceRange { n_eq: 0, rel: r }) if *r == rel)),
                "rel {rel:?} did not round-trip"
            );
        }
    }

    /// The `global_ground` discriminator rides as one param row and survives
    /// into_rows → from_rows: set for an ungrouped global aggregate, clear for an
    /// ordinary grouped reduce (so existing reduce circuits are byte-identical).
    #[test]
    fn reduce_global_ground_roundtrips() {
        use gnitz_wire::{AggKind, NODE_COL_KIND_GLOBAL_GROUND};
        for ground in [false, true] {
            let mut cb = CircuitBuilder::new(3, 100);
            let input = cb.input_delta();
            // Empty group cols + one COUNT(*) spec, the ungrouped-aggregate shape.
            let red = cb.reduce_multi(input, &[], &[(gnitz_wire::AGG_COUNT, 0)], ground);
            cb.sink(red);
            let rows = cb.build().into_rows();

            // The param row is present iff `ground`.
            let gg_rows = rows
                .node_columns
                .iter()
                .filter(|(nid, kind, ..)| *nid == red && *kind == NODE_COL_KIND_GLOBAL_GROUND)
                .count();
            assert_eq!(gg_rows, ground as usize, "global_ground row present iff set");

            let decoded = Circuit::from_rows(3, rows).expect("from_rows");
            match decoded.nodes.get(&red) {
                Some(OpNode::Reduce { global_ground, agg, .. }) => {
                    assert_eq!(*global_ground, ground);
                    assert!(matches!(agg, AggKind::Specs(_)));
                }
                other => panic!("expected Reduce, got {other:?}"),
            }
        }
    }

    /// A plain compute map (`map_expr`) carries no reindex columns: no kind rows
    /// and an empty decoded list.
    #[test]
    fn map_expr_has_no_reindex_cols() {
        let mut cb = CircuitBuilder::new(1, 100);
        let input = cb.input_delta();
        let map_nid = cb.map_expr(input, empty_prog());
        let rows = cb.build().into_rows();
        assert!(rows
            .node_columns
            .iter()
            .all(|(nid, kind, ..)| !(*nid == map_nid && *kind == NODE_COL_KIND_REINDEX)));
        let decoded = Circuit::from_rows(1, rows).expect("from_rows");
        match decoded.nodes.get(&map_nid) {
            Some(OpNode::Map(MapKind::Expression { reindex_cols, .. })) => assert!(reindex_cols.is_empty()),
            other => panic!("expected Map(Expression), got {other:?}"),
        }
    }
}
