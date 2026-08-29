use std::collections::HashMap;

use gnitz_expr::ExprProgram;

use crate::error::ClientError;

pub use gnitz_wire::{agg_output_type, AggFunc, JoinKind, MapKind, OpNode, RangeRel, ReduceOutKey, ReindexRole};

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

/// One full row of the `nodes` system table: node id + `gnitz_wire`'s `NodeFields`.
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
    /// `expr_program` is `None` outside `Filter` and the two expression `Map`s.
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
}

/// Fluent builder for DBSP circuit graphs, producing a typed [`Circuit`]
/// for `GnitzClient::create_view_chain`.
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

    /// A computed projection: `program` writes one payload slot each, and
    /// `out_cols` declares those slots as `(type_code, nullable)` in payload
    /// order. `SELECT a + b` has no copy list the engine could derive a schema
    /// from, so the declaration travels; the PK region is inherited from the input
    /// and is not listed.
    pub fn map_expr(&mut self, input: NodeId, program: ExprProgram, out_cols: &[(u8, bool)]) -> NodeId {
        self.alloc_unary(
            OpNode::Map(MapKind::Compute {
                program: program.encode(),
                out_cols: out_cols.to_vec(),
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
    /// column subset prunes the payload).
    ///
    /// Panics on an empty `reindex_cols`: a map that re-keys nothing is a
    /// [`Self::map_expr`], and the wire tells the two apart by these columns.
    pub fn map_reindex(
        &mut self,
        input: NodeId,
        reindex_cols: &[usize],
        target_tcs: &[u8],
        program: ExprProgram,
        role: ReindexRole,
    ) -> NodeId {
        assert!(!reindex_cols.is_empty(), "a reindex map must name its key columns");
        let reindex_target_tcs = Self::promotion_targets(reindex_cols, target_tcs);
        self.alloc_unary(
            OpNode::Map(MapKind::Reindex {
                program: program.encode(),
                reindex_cols: reindex_cols.iter().map(|&c| c as u32).collect(),
                reindex_target_tcs,
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
    /// `target_tcs` promotes payload columns; see [`Self::promotion_targets`].
    ///
    /// `branch_id` is mixed into the hash; pass distinct ids (0 and 1) to the two
    /// sides of a `UNION ALL` so identical rows do not collide to one PK, and 0
    /// to both sides of deduplicating set-ops.
    pub fn map_hash_row(&mut self, input: NodeId, projection: &[usize], target_tcs: &[u8], branch_id: u8) -> NodeId {
        let tcs = Self::promotion_targets(projection, target_tcs);
        let cols: Vec<u32> = projection.iter().map(|&c| c as u32).collect();
        self.alloc_unary(OpNode::Map(MapKind::HashRow(cols, tcs, branch_id)), input)
    }

    /// The per-column promotion targets to persist beside `cols`: a non-zero
    /// entry promotes that column to the named ≤8-byte integer type, so a
    /// cross-width join or set-op pair shares one physical layout.
    ///
    /// **Pass `&[]` when nothing is promoted.** The encoding needs the two lists
    /// parallel, so the all-zero vector is built here rather than spelled
    /// `vec![0; cols.len()]` at a call site that could get the length wrong.
    fn promotion_targets(cols: &[usize], target_tcs: &[u8]) -> Vec<u8> {
        if target_tcs.is_empty() {
            return vec![0; cols.len()];
        }
        assert_eq!(cols.len(), target_tcs.len(), "one promotion target per key column");
        target_tcs.to_vec()
    }

    /// Pure projection: keep only the listed payload columns, in order.
    pub fn map(&mut self, input: NodeId, projection: &[usize]) -> NodeId {
        let cols: Vec<u32> = projection.iter().map(|&c| c as u32).collect();
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
    /// The operand order is a **cost** contract, not a correctness one: the engine
    /// takes `PORT_IN_A` in place where nothing reads it later and clones it
    /// otherwise. `negate(subtrahend)` is freshly allocated and read nowhere else,
    /// so it earns the take; the `minuend` may be shared (a null-fill's `a_all`
    /// aliasing the join's `reindex_a`), where the swap would cost a clone every
    /// epoch — the same answer, just paid for.
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
        let group: Vec<u32> = group_cols.iter().map(|&c| c as u32).collect();
        let specs: Vec<(AggFunc, u32)> = agg_specs
            .iter()
            .map(|&(func_id, col)| {
                (
                    AggFunc::from_wire(func_id).unwrap_or_else(|| panic!("unknown agg func id {func_id}")),
                    col as u32,
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
        let cols: Vec<u32> = shard_cols.iter().map(|&c| c as u32).collect();
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
    use gnitz_wire::type_code;

    fn empty_prog() -> ExprProgram {
        ExprProgram {
            num_regs: 0,
            result_reg: 0,
            code: Vec::new(),
            const_strings: Vec::new(),
        }
    }

    /// The reindex target list a node carries, built through the public builder.
    fn built_target_tcs(reindex_cols: &[usize], target_tcs: &[u8]) -> Vec<u8> {
        let mut cb = CircuitBuilder::new(7, 100);
        let input = cb.input_delta();
        let nid = cb.map_reindex(input, reindex_cols, target_tcs, empty_prog(), ReindexRole::ScatterKey);
        match cb.build().nodes.remove(&nid) {
            Some(OpNode::Map(MapKind::Reindex { reindex_target_tcs, .. })) => reindex_target_tcs,
            other => panic!("expected Map(Reindex), got {other:?}"),
        }
    }

    /// `&[]` is the spelling for "nothing promoted", and the builder expands it
    /// to the parallel all-zero vector the encoding needs — so no call site
    /// carries a length it could get wrong. A stated list is carried verbatim.
    #[test]
    fn promotion_targets_are_parallel_to_the_key_columns() {
        assert_eq!(
            built_target_tcs(&[2, 5], &[]),
            vec![0, 0],
            "an absent list expands to one zero per key column"
        );
        assert_eq!(built_target_tcs(&[2, 5], &[0, type_code::I64]), vec![0, type_code::I64]);
    }

    /// A stated list of the wrong length is a caller bug, and fails at the
    /// caller's own site rather than as a padded encoding two crates away.
    #[test]
    #[should_panic(expected = "one promotion target per key column")]
    fn a_mis_sized_promotion_target_list_is_refused() {
        built_target_tcs(&[2, 5], &[0]);
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
}
