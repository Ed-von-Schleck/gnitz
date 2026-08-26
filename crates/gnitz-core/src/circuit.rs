use std::collections::HashMap;

use gnitz_expr::ExprProgram;

use crate::error::ClientError;

pub use gnitz_wire::{
    agg_output_type, AggFunc, JoinKind, MapKind, NodeColumnPayload, NodeFields, OpNode, RangeRel, ReduceOutKey,
    ReindexRole,
};

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

/// One full row of the `nodes` system table: node id + [`NodeFields`].
pub type NodeRow = (NodeId, u64, Option<TableId>, Option<Vec<u8>>);

/// The `k`-th symbolic segment id of one view bundle. Symbolic ids start at
/// [`gnitz_wire::RELATION_ID_CEILING`], which no durable relation id reaches, so
/// a `ScanDelta.source` naming a segment is distinguishable from one naming a
/// base table.
pub fn segment_id(k: u64) -> u64 {
    gnitz_wire::RELATION_ID_CEILING + k
}

/// Whether `id` is a symbolic segment id rather than a real relation id.
pub fn is_segment_id(id: u64) -> bool {
    id >= gnitz_wire::RELATION_ID_CEILING
}

/// Rewrite one id slot through `map`, rejecting a segment id that survives it: a
/// slot whose id was minted but never registered. Both substitution sites use
/// this — [`Circuit::resolve_seg_ids`] for the circuit's own slots,
/// `create_view_chain` for a hidden segment's owner.
pub fn substitute_seg_id(slot: u64, map: &HashMap<u64, u64>) -> Result<u64, ClientError> {
    let real = map.get(&slot).copied().unwrap_or(slot);
    if is_segment_id(real) {
        return Err(ClientError::ServerError(format!(
            "view bundle carries unresolved segment id {slot}"
        )));
    }
    Ok(real)
}

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
    /// node's `source_table`, deduped.
    pub fn dependencies(&self) -> Vec<TableId> {
        // A view's dependency set is 1–4 entries; a linear `Vec::contains` dedup
        // is alloc-free and beats a HashSet at this n (same small-n convention as
        // `Schema::validate_pk_cols`). Iterating `nodes` in BTreeMap key order
        // preserves the first-wins ordering of the prior HashSet-insert filter.
        let mut deps: Vec<TableId> = Vec::new();
        for op in self.nodes.values() {
            if let OpNode::ScanDelta { source, .. } = op {
                if !deps.contains(source) {
                    deps.push(*source);
                }
            }
        }
        deps
    }

    /// Rewrite `view_id` and every `ScanDelta.source` through `map`. Nothing
    /// downstream catches a segment id that survives: the engine's id-ceiling
    /// rejection covers only TABLE_TAB and VIEW_TAB PKs, so a phantom source
    /// would commit as a durable dependency edge on a relation that does not
    /// exist.
    pub fn resolve_seg_ids(&mut self, map: &HashMap<u64, u64>) -> Result<(), ClientError> {
        self.view_id = substitute_seg_id(self.view_id, map)?;
        for op in self.nodes.values_mut() {
            if let OpNode::ScanDelta { source, .. } = op {
                *source = substitute_seg_id(*source, map)?;
            }
        }
        Ok(())
    }

    /// Materialise the circuit into the three-table row bundle. Pure
    /// transformation — no I/O.
    pub fn into_rows(self) -> CircuitRows {
        let mut rows = CircuitRows::default();
        for (nid, op) in self.nodes {
            let ((opcode, src_tab, expr_blob), kind_rows) = gnitz_wire::encode_op_node(op);
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
    ///
    /// Test-only: the engine decodes circuits through its own path; the client
    /// only ever *encodes* (`into_rows`). Kept to exercise the
    /// `encode_op_node`↔`decode_op_node` round-trip at the graph level.
    #[cfg(test)]
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

/// Fluent builder for DBSP circuit graphs, producing a typed [`Circuit`]
/// for `GnitzClient::create_view_with_circuit`.
///
/// Sequential `node_id`s start at 1. `primary_source_id` is fixed at
/// construction and becomes the source of every `input_delta()` node, so it is
/// not threaded through each call; `input_delta_tagged` names its own source.
#[derive(Clone)]
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
    /// construction.
    pub fn input_delta(&mut self) -> NodeId {
        self.input_delta_bounded(None)
    }

    /// Primary delta input whose **backfill scan** is bounded to a secondary-index
    /// range. Identical to [`Self::input_delta`] except that the initial
    /// full-source scan reads only the index range; steady-state deltas ignore the
    /// bound entirely. The caller still emits the full `Filter` downstream — the
    /// bound narrows what is read, never what the view contains.
    pub fn input_delta_bounded(&mut self, bound: Option<gnitz_wire::ScanBound>) -> NodeId {
        self.alloc_node(OpNode::ScanDelta {
            source: self.primary_source_id,
            bound,
        })
    }

    /// Tagged secondary delta input for multi-input views (e.g. equijoin).
    /// `source_table_id` becomes a real dependency.
    pub fn input_delta_tagged(&mut self, source_table_id: u64) -> NodeId {
        self.alloc_node(OpNode::ScanDelta {
            source: source_table_id,
            bound: None,
        })
    }

    pub fn filter(&mut self, input: NodeId, expr: Option<ExprProgram>) -> NodeId {
        self.alloc_unary(OpNode::Filter(expr.map(|e| e.encode())), input)
    }

    /// A computed projection. It carries no reindex columns, so it re-keys nothing
    /// — and the engine therefore types its output with the **view's own output
    /// schema**, having no dense copy list to derive one from (`SELECT a + b` is
    /// not a copy). Every caller must emit the circuit's final projection. A
    /// reindex-free map that projected something narrower would be typed with the
    /// view's width and mis-read downstream.
    pub fn map_expr(&mut self, input: NodeId, program: ExprProgram) -> NodeId {
        let blob = program.encode();
        self.alloc_unary(
            OpNode::Map(MapKind::Expression {
                program: blob,
                reindex_cols: Vec::new(),
                reindex_target_tcs: Vec::new(),
                role: ReindexRole::Auxiliary,
            }),
            input,
        )
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
    ///
    /// The engine derives the node's output payload schema from `program`'s copy
    /// list: a program that is one COPY_COL per payload column with dense outputs
    /// `0..n` places exactly its source columns behind the key slots (so copying a
    /// column subset prunes the payload). Only this arm derives it; a
    /// *reindex-free* `map_expr` is typed by the view's own output schema.
    pub fn map_reindex(
        &mut self,
        input: NodeId,
        reindex_cols: &[usize],
        target_tcs: &[u8],
        program: ExprProgram,
        role: ReindexRole,
    ) -> NodeId {
        let blob = program.encode();
        self.alloc_unary(
            OpNode::Map(MapKind::Expression {
                program: blob,
                reindex_cols: reindex_cols.iter().map(|&c| c as u16).collect(),
                reindex_target_tcs: target_tcs.to_vec(),
                role,
            }),
            input,
        )
    }

    /// Full-row-identity reindex: keep the listed columns as payload (in order)
    /// and set the synthetic PK to a hash of those payload bytes, so set
    /// membership is decided by the projected row content, not by the source PK
    /// (EXCEPT/INTERSECT/DISTINCT).
    ///
    /// `target_tcs` is parallel to `projection` (`0` = keep the source column's
    /// type); a non-zero entry promotes that payload column to the given ≤8-byte
    /// integer type so a cross-width set-op pair shares one physical layout. Pass
    /// an all-zero slice (or one shorter than `projection`) for the same-type /
    /// DISTINCT case, which compiles a byte-identical circuit.
    ///
    /// `branch_id` is mixed into the hash; pass distinct ids (0 and 1) to the two
    /// sides of a `UNION ALL` so identical rows do not collide to one PK, and 0
    /// to both sides of deduplicating set-ops.
    pub fn map_hash_row(&mut self, input: NodeId, projection: &[usize], target_tcs: &[u8], branch_id: u8) -> NodeId {
        let cols: Vec<u16> = projection.iter().map(|&c| c as u16).collect();
        self.alloc_unary(
            OpNode::Map(MapKind::HashRow(cols, target_tcs.to_vec(), branch_id)),
            input,
        )
    }

    /// Pure projection: keep only the listed payload columns, in order.
    pub fn map(&mut self, input: NodeId, projection: &[usize]) -> NodeId {
        let cols: Vec<u16> = projection.iter().map(|&c| c as u16).collect();
        self.alloc_unary(OpNode::Map(MapKind::Projection(cols)), input)
    }

    pub fn negate(&mut self, input: NodeId) -> NodeId {
        self.alloc_unary(OpNode::Negate, input)
    }

    pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId {
        let nid = self.alloc_node(OpNode::Union);
        self.connect(a, nid, gnitz_wire::PORT_IN_A);
        self.connect(b, nid, gnitz_wire::PORT_IN_B);
        nid
    }

    pub fn distinct(&mut self, input: NodeId) -> NodeId {
        self.alloc_unary(OpNode::Distinct, input)
    }

    /// The weight-exact Z-set difference `positive_part(minuend − subtrahend)` as
    /// one node triple (`negate` → `union` → `PositivePart`). Used by EXCEPT /
    /// INTERSECT set-ops and by the LEFT/RIGHT/FULL outer-join null-fills
    /// (`ν = positive_part(P − π_P(inner))`). `PositivePart` clamps each
    /// consolidated (PK, payload)'s net weight to `[0, i64::MAX]`, so output
    /// weights stay ≥ 0 without collapsing to set membership.
    ///
    /// Centralizes the operand-order rule: the `minuend` rides the
    /// non-destructive `PORT_IN_B` operand because `op_union` empties
    /// `PORT_IN_A`, and the minuend may be a shared node (e.g. a null-fill's
    /// `a_all` aliasing the join's `reindex_a`); `negate(subtrahend)` is freshly
    /// allocated, so `PORT_IN_A` is safe for it.
    pub fn positive_diff(&mut self, minuend: NodeId, subtrahend: NodeId) -> NodeId {
        let neg = self.negate(subtrahend);
        let diff = self.union(neg, minuend);
        self.alloc_unary(OpNode::PositivePart, diff)
    }

    /// Allocate a single-input node and wire `input` to its `PORT_IN`.
    fn alloc_unary(&mut self, op: OpNode, input: NodeId) -> NodeId {
        let nid = self.alloc_node(op);
        self.connect(input, nid, gnitz_wire::PORT_IN);
        nid
    }

    fn binary_join(&mut self, op: OpNode, delta: NodeId, trace_node: NodeId) -> NodeId {
        let nid = self.alloc_node(op);
        self.connect(delta, nid, gnitz_wire::PORT_IN_A);
        self.connect(trace_node, nid, gnitz_wire::PORT_TRACE);
        nid
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
    pub fn worker_filter(&mut self, input: NodeId) -> NodeId {
        self.alloc_unary(OpNode::WorkerFilter, input)
    }

    /// Shared `Reduce`-node construction: map the group cols + agg specs, alloc
    /// the node, and wire `input` to `PORT_IN`. The caller decides the
    /// partitioning of `input` — `reduce`/`reduce_multi` shard first;
    /// `reduce_multi_local` passes a deliberately pre-replicated input straight
    /// through. `agg_specs` must not be empty (the engine rejects a spec-less
    /// REDUCE at decode).
    fn reduce_node(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(u64, usize)],
        global_ground: bool,
        out_key: ReduceOutKey,
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
        self.alloc_unary(
            OpNode::Reduce {
                group_cols: group,
                agg: specs,
                global_ground,
                out_key,
            },
            input,
        )
    }

    /// Reduce with automatic shard insertion (required for multi-worker correctness).
    pub fn reduce(&mut self, input: NodeId, group_cols: &[usize], agg_func_id: u64, agg_col_idx: usize) -> NodeId {
        let sharded = self.shard(input, group_cols);
        // The low-level single-agg API is never the user's global scalar
        // aggregate (that path goes through `reduce_multi`/`reduce_multi_local`),
        // so it never seeds a ground row. It also cannot track the input schema,
        // so it always keys the output synthetically; the engine hard-rejects it
        // if the group set actually warrants a natural key (only the SQL planner
        // ships the natural-key kinds).
        self.reduce_node(
            sharded,
            group_cols,
            &[(agg_func_id, agg_col_idx)],
            false,
            ReduceOutKey::SyntheticFold,
        )
    }

    /// Multi-aggregate reduce with automatic shard insertion (required for
    /// multi-worker correctness). `agg_specs`: list of (agg_func_id, col_idx).
    /// `global_ground` is `true` only for the user's ungrouped scalar aggregate
    /// (empty `group_cols`); the grouped builder passes `group_cols.is_empty()`.
    /// `out_key` is the caller's schema-derived output-key decision; the engine
    /// validates it against the input schema.
    pub fn reduce_multi(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(u64, usize)],
        global_ground: bool,
        out_key: ReduceOutKey,
    ) -> NodeId {
        let sharded = self.shard(input, group_cols);
        self.reduce_node(sharded, group_cols, agg_specs, global_ground, out_key)
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
        out_key: ReduceOutKey,
    ) -> NodeId {
        self.reduce_node(input, group_cols, agg_specs, global_ground, out_key)
    }

    /// Exchange shard: routes rows to workers by hashing the given columns.
    pub fn shard(&mut self, input: NodeId, shard_cols: &[usize]) -> NodeId {
        let cols: Vec<u16> = shard_cols.iter().map(|&c| c as u16).collect();
        self.alloc_unary(OpNode::ExchangeShard { shard_cols: cols }, input)
    }

    /// Intermediate trace integration (equijoin accumulator).
    pub fn integrate_trace(&mut self, input: NodeId) -> NodeId {
        self.alloc_unary(OpNode::IntegrateTrace, input)
    }

    /// Null-extend: appends N null payload columns to each row.
    /// `right_col_type_codes` contains the column type code (u8) for each
    /// null column to append.
    pub fn null_extend(&mut self, input: NodeId, right_col_type_codes: &[u64]) -> NodeId {
        let codes: Vec<u8> = right_col_type_codes.iter().map(|&t| t as u8).collect();
        self.alloc_unary(OpNode::NullExtend { type_codes: codes }, input)
    }

    /// Sink — primary INTEGRATE that writes to view storage.
    pub fn sink(&mut self, input: NodeId) -> NodeId {
        self.alloc_unary(OpNode::IntegrateSink, input)
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

    /// A two-segment chain lowered with symbolic ids substitutes to the circuit
    /// inline real-id allocation would have produced (same nodes, same `view_id`,
    /// same `ScanDelta.source`), and leaves the base-table id alone.
    #[test]
    fn resolve_seg_ids_reproduces_inline_allocation() {
        // `base → seg → final`, built once with symbolic ids and once with the
        // real ones the substitution assigns.
        let (base, seg_real, final_real) = (100u64, 4096u64, 4097u64);
        let (seg_a, seg_b) = (segment_id(0), segment_id(1));
        let chain = |seg: u64, fin: u64| {
            let mut cb = CircuitBuilder::new(fin, seg);
            let up = cb.input_delta_tagged(base);
            let inp = cb.input_delta();
            cb.sink(up);
            cb.sink(inp);
            cb.build()
        };

        let mut symbolic = chain(seg_a, seg_b);
        let map = HashMap::from([(seg_a, seg_real), (seg_b, final_real)]);
        symbolic.resolve_seg_ids(&map).expect("every tag is in the map");

        let inline = chain(seg_real, final_real);
        assert_eq!(symbolic.view_id, inline.view_id);
        assert_eq!(
            format!("{:?}", symbolic.into_rows()),
            format!("{:?}", inline.into_rows()),
            "substitution must produce the inline-allocated rows"
        );
    }

    /// An identity pass over an empty map leaves a real relation id untouched:
    /// the shape a bundle of caller-preset ids takes.
    #[test]
    fn resolve_seg_ids_leaves_real_ids_alone() {
        let mut cb = CircuitBuilder::new(17, 100);
        let inp = cb.input_delta();
        cb.sink(inp);
        let mut circuit = cb.build();
        circuit.resolve_seg_ids(&HashMap::new()).expect("no tag to resolve");
        assert_eq!(circuit.view_id, 17);
        assert_eq!(circuit.dependencies(), vec![100]);
    }

    /// A tag that survives the substitution is rejected, in `view_id` and in a
    /// `ScanDelta.source` alike, whether the map is empty or merely missing that
    /// one entry.
    #[test]
    fn resolve_seg_ids_rejects_a_surviving_tag() {
        // A tagged `view_id` with nothing to resolve it.
        let mut cb = CircuitBuilder::new(segment_id(0), 100);
        let inp = cb.input_delta();
        cb.sink(inp);
        let mut circuit = cb.build();
        assert!(circuit.resolve_seg_ids(&HashMap::new()).is_err(), "tagged view_id");

        // A tagged source absent from a non-empty map: the lowering path that
        // mints a segment id and forgets to register it.
        let mut cb = CircuitBuilder::new(segment_id(0), 100);
        let up = cb.input_delta_tagged(segment_id(7));
        cb.sink(up);
        let mut circuit = cb.build();
        let map = HashMap::from([(segment_id(0), 17u64)]);
        let err = circuit.resolve_seg_ids(&map).expect_err("tagged source");
        assert!(
            format!("{err}").contains(&format!("{}", segment_id(7))),
            "the error names the unresolved id: {err}"
        );
    }

    /// A compound (2-column) reindex descriptor must survive into_rows → from_rows
    /// with its column *order* preserved, stored under NODE_COL_KIND_REINDEX with
    /// position = key order.
    #[test]
    fn reindex_cols_roundtrip_ordered() {
        let (c1, c2) = (2usize, 5usize);
        let mut cb = CircuitBuilder::new(7, 100);
        let input = cb.input_delta();
        let map_nid = cb.map_reindex(input, &[c1, c2], &[], empty_prog(), ReindexRole::ScatterKey);
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
        let map_nid = cb.map_reindex(
            input,
            &[3, 3],
            &[0, type_code::I64],
            empty_prog(),
            ReindexRole::ScatterKey,
        );
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
    /// NODE_COL_KIND_RANGE_JOIN param row, and a worker-filter node round-trips
    /// as a bare opcode.
    #[test]
    fn range_join_and_worker_filter_roundtrip() {
        use gnitz_wire::NODE_COL_KIND_RANGE_JOIN;
        let mut cb = CircuitBuilder::new(9, 100);
        let a = cb.input_delta_tagged(100);
        let b = cb.input_delta_tagged(200);
        let reindex_b = cb.map_reindex(b, &[0], &[], empty_prog(), ReindexRole::ScatterKey);
        let filt_b = cb.worker_filter(reindex_b);
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
        assert_eq!(rj, vec![(0, 1, RangeRel::Le.as_wire())]);

        let decoded = Circuit::from_rows(9, rows).expect("from_rows");
        assert!(decoded.nodes.values().any(|n| matches!(n, OpNode::WorkerFilter)));
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
            let b = cb.input_delta_tagged(200);
            let trace = cb.integrate_trace(b);
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
        use gnitz_wire::NODE_COL_KIND_GLOBAL_GROUND;
        for ground in [false, true] {
            let mut cb = CircuitBuilder::new(3, 100);
            let input = cb.input_delta();
            // Empty group cols + one COUNT(*) spec, the ungrouped-aggregate shape.
            let red = cb.reduce_multi(
                input,
                &[],
                &[(gnitz_wire::AGG_COUNT, 0)],
                ground,
                ReduceOutKey::SyntheticFold,
            );
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
                    assert!(!agg.is_empty());
                }
                other => panic!("expected Reduce, got {other:?}"),
            }
        }
    }

    /// The `out_key` discriminator rides as one sparse-default param row and
    /// survives into_rows → from_rows for every kind: `SyntheticFold` (the
    /// default) omits the row, the two natural-key kinds carry it.
    #[test]
    fn reduce_out_key_roundtrips() {
        use gnitz_wire::NODE_COL_KIND_REDUCE_OUT_KEY;
        for kind in [
            ReduceOutKey::SyntheticFold,
            ReduceOutKey::PkPermutation,
            ReduceOutKey::SingleNaturalCol,
        ] {
            let mut cb = CircuitBuilder::new(4, 100);
            let input = cb.input_delta();
            let red = cb.reduce_multi(input, &[0], &[(gnitz_wire::AGG_COUNT, 0)], false, kind);
            cb.sink(red);
            let rows = cb.build().into_rows();

            // The param row is present iff the kind is not the default.
            let key_rows = rows
                .node_columns
                .iter()
                .filter(|(nid, k, ..)| *nid == red && *k == NODE_COL_KIND_REDUCE_OUT_KEY)
                .count();
            assert_eq!(
                key_rows,
                (kind != ReduceOutKey::SyntheticFold) as usize,
                "out_key row present iff non-default",
            );

            let decoded = Circuit::from_rows(4, rows).expect("from_rows");
            match decoded.nodes.get(&red) {
                Some(OpNode::Reduce { out_key, .. }) => assert_eq!(*out_key, kind),
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
