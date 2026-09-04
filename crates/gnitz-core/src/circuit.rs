use std::collections::HashMap;

use gnitz_expr::LogicalProgram;

use crate::error::ClientError;

pub use gnitz_wire::{
    agg_output_type, AggDescriptor, AggFunc, ComputeMap, JoinKind, MapKind, OpNode, RangeRel, ReduceOutKey,
    ReindexRole, TableId,
};

pub type NodeId = u64;

/// In-memory circuit graph: a typed `OpNode` per node, and per node the producer
/// feeding each of its input slots. Slot `i` is the port `i` the operator's
/// [`OpNode::arity`] declares, so a duplicate or out-of-range port cannot be
/// expressed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Circuit {
    pub nodes: std::collections::BTreeMap<NodeId, OpNode>,
    pub inputs: std::collections::BTreeMap<NodeId, [Option<NodeId>; 2]>,
}

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

impl Circuit {
    /// Tables this view reads cascading deltas from — every `ScanDelta`
    /// node's `source_table`, deduped.
    pub fn dependencies(&self) -> Vec<TableId> {
        // A view's dependency set is 1–4 entries; a linear `Vec::contains` dedup
        // is alloc-free and beats a HashSet at this n (same small-n convention as
        // `Schema::validate_parts`).
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

    /// Rewrite every `ScanDelta.source` through `map`. Nothing downstream
    /// catches a segment id that survives: the engine's id-ceiling rejection
    /// covers only TABLE_TAB and VIEW_TAB PKs, so a phantom source would commit
    /// as a durable dependency edge on a relation that does not exist.
    pub fn resolve_seg_ids(&mut self, map: &HashMap<u64, u64>) -> Result<(), ClientError> {
        for op in self.nodes.values_mut() {
            if let OpNode::ScanDelta { source, .. } = op {
                *source = substitute_seg_id(*source, map)?;
            }
        }
        Ok(())
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
    primary_source_id: u64,
    next_node_id: u64,
    nodes: std::collections::BTreeMap<NodeId, OpNode>,
    inputs: std::collections::BTreeMap<NodeId, [Option<NodeId>; 2]>,
}

impl CircuitBuilder {
    pub fn new(primary_source_id: u64) -> Self {
        CircuitBuilder {
            primary_source_id,
            next_node_id: 1,
            nodes: std::collections::BTreeMap::new(),
            inputs: std::collections::BTreeMap::new(),
        }
    }

    /// Allocate a node and wire `inputs[i]` into its slot `i` — the one wiring
    /// path, held to the operator's own [`OpNode::arity`]. A second table of
    /// which operators take which ports would surface its disagreements as
    /// view-load rejections on circuits a client had already committed.
    fn alloc_wired(&mut self, op: OpNode, inputs: &[NodeId]) -> NodeId {
        debug_assert_eq!(op.arity(), inputs.len(), "an operator is wired on exactly its arity");
        let nid = self.next_node_id;
        self.next_node_id += 1;
        self.nodes.insert(nid, op);
        let mut slots = [None; 2];
        for (slot, &src) in slots.iter_mut().zip(inputs) {
            *slot = Some(src);
        }
        self.inputs.insert(nid, slots);
        nid
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
        self.alloc_wired(
            OpNode::ScanDelta {
                source: self.primary_source_id,
                bound,
            },
            &[],
        )
    }

    /// Tagged secondary delta input for multi-input views (e.g. equijoin).
    /// `source_table_id` becomes a real dependency.
    pub fn input_delta_tagged(&mut self, source_table_id: u64) -> NodeId {
        self.alloc_wired(
            OpNode::ScanDelta {
                source: source_table_id,
                bound: None,
            },
            &[],
        )
    }

    pub fn filter(&mut self, input: NodeId, expr: Option<LogicalProgram>) -> NodeId {
        self.alloc_wired(OpNode::Filter(expr.map(|e| e.to_blob_bytes())), &[input])
    }

    /// A computed projection: `program` writes one payload slot each, and
    /// `out_cols` declares those slots as `(type_code, nullable)` in payload
    /// order. `SELECT a + b` has no copy list the engine could derive a schema
    /// from, so the declaration travels; the PK region is inherited from the input
    /// and is not listed.
    pub fn map_expr(&mut self, input: NodeId, program: LogicalProgram, out_cols: &[(u8, bool)]) -> NodeId {
        self.alloc_wired(
            OpNode::Map(MapKind::Compute(ComputeMap {
                program: program.to_blob_bytes(),
                out_cols: out_cols.to_vec(),
            })),
            &[input],
        )
    }

    /// Map with PK reindexing (equijoin pre-indexing): the new synthetic PK is
    /// [`MapKind::Reindex`]'s `key`, and `keep` lists the source columns that
    /// survive as payload behind it, in output order — so omitting a column the
    /// view never reads prunes it out of the trace too.
    ///
    /// Panics on an empty `key`: a map that re-keys nothing is a
    /// [`Self::map_expr`], and the two carry different opcodes.
    pub fn map_reindex(&mut self, input: NodeId, key: &[(u32, u8)], keep: &[u32], role: ReindexRole) -> NodeId {
        assert!(!key.is_empty(), "a reindex map must name its key columns");
        self.alloc_wired(
            OpNode::Map(MapKind::Reindex {
                keep: keep.to_vec(),
                key: key.to_vec(),
                role,
            }),
            &[input],
        )
    }

    /// Full-row-identity reindex: keep the listed columns as payload (in order)
    /// and set the synthetic PK to a hash of those payload bytes, so set
    /// membership is decided by the projected row content, not by the source PK
    /// (EXCEPT/INTERSECT/DISTINCT). Each column's `u8` promotes it to that
    /// ≤8-byte integer type, or `0` keeps the source type.
    ///
    /// `branch_id` is mixed into the hash; pass distinct ids (0 and 1) to the two
    /// sides of a `UNION ALL` so identical rows do not collide to one PK, and 0
    /// to both sides of deduplicating set-ops.
    pub fn map_hash_row(&mut self, input: NodeId, cols: &[(u32, u8)], branch_id: u8) -> NodeId {
        self.alloc_wired(
            OpNode::Map(MapKind::HashRow {
                cols: cols.to_vec(),
                branch_id,
            }),
            &[input],
        )
    }

    /// Pure projection: keep only the listed payload columns, in order.
    pub fn map(&mut self, input: NodeId, projection: &[usize]) -> NodeId {
        let cols: Vec<u32> = projection.iter().map(|&c| c as u32).collect();
        self.alloc_wired(OpNode::Map(MapKind::Projection(cols)), &[input])
    }

    pub fn negate(&mut self, input: NodeId) -> NodeId {
        self.alloc_wired(OpNode::Negate, &[input])
    }

    pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId {
        self.alloc_wired(OpNode::Union, &[a, b])
    }

    pub fn distinct(&mut self, input: NodeId) -> NodeId {
        self.alloc_wired(OpNode::Distinct, &[input])
    }

    /// The weight-exact Z-set difference `positive_part(minuend − subtrahend)` as
    /// one node triple (`negate` → `union` → `PositivePart`). Used by EXCEPT /
    /// INTERSECT set-ops and by the LEFT/RIGHT/FULL outer-join null-fills
    /// (`ν = positive_part(P − π_P(inner))`). `PositivePart` clamps each
    /// consolidated (PK, payload)'s net weight to `[0, i64::MAX]`, so output
    /// weights stay ≥ 0 without collapsing to set membership.
    ///
    /// The operand order is a **cost** contract, not a correctness one: the engine
    /// takes a union's first operand in place where nothing reads it later, and clones it
    /// otherwise. `negate(subtrahend)` is freshly allocated and read nowhere else,
    /// so it earns the take; the `minuend` may be shared (a null-fill's `a_all`
    /// aliasing the join's `reindex_a`), where the swap would cost a clone every
    /// epoch — the same answer, just paid for.
    pub fn positive_diff(&mut self, minuend: NodeId, subtrahend: NodeId) -> NodeId {
        let neg = self.negate(subtrahend);
        let diff = self.union(neg, minuend);
        self.alloc_wired(OpNode::PositivePart, &[diff])
    }

    pub fn join_with_trace_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.alloc_wired(OpNode::Join(JoinKind::DeltaTrace), &[delta, trace_node])
    }

    /// Non-equi (range) join term: the delta probes `trace_node` with an ordered
    /// half-open range walk per the §3 cut-point table. `n_eq` leading key slots
    /// are equality-pinned (the band-join prefix); `rel` is the relation the trace
    /// slot must satisfy versus the delta slot. Mirrors `join_with_trace_node` but
    /// for `JoinKind::DeltaTraceRange`.
    pub fn join_with_trace_range_node(&mut self, delta: NodeId, trace_node: NodeId, n_eq: u8, rel: RangeRel) -> NodeId {
        self.alloc_wired(
            OpNode::Join(JoinKind::DeltaTraceRange { n_eq, rel }),
            &[delta, trace_node],
        )
    }

    /// Keyless (cross) join term: every delta row pairs with every trace row.
    /// Neither side's key is compared, so the two sides need not agree on a key
    /// type or width — each keys on whatever partitions its trace.
    pub fn join_with_trace_cross_node(&mut self, delta: NodeId, trace_node: NodeId) -> NodeId {
        self.alloc_wired(OpNode::Join(JoinKind::DeltaTraceCross), &[delta, trace_node])
    }

    /// Keep only rows this worker owns (by packed-PK partition) before they
    /// integrate into the trace of a join whose input relay broadcasts. Worker
    /// identity is baked in at compile time, so the node carries no payload.
    pub fn worker_filter(&mut self, input: NodeId) -> NodeId {
        self.alloc_wired(OpNode::WorkerFilter, &[input])
    }

    /// Shared `Reduce`-node construction: map the group cols + agg specs and
    /// alloc the node over `input`. The caller decides the
    /// partitioning of `input` — `reduce`/`reduce_multi` shard first;
    /// `reduce_multi_local` passes a deliberately pre-replicated input straight
    /// through. `agg_specs` must not be empty (the engine rejects a spec-less
    /// REDUCE at decode).
    fn reduce_node(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(AggFunc, usize)],
        global_ground: bool,
        out_key: ReduceOutKey,
    ) -> NodeId {
        let group: Vec<u32> = group_cols.iter().map(|&c| c as u32).collect();
        let specs: Vec<AggDescriptor> = agg_specs
            .iter()
            .map(|&(agg_op, col)| AggDescriptor {
                agg_op,
                col_idx: col as u32,
            })
            .collect();
        self.alloc_wired(
            OpNode::Reduce {
                group_cols: group,
                agg: specs,
                global_ground,
                out_key,
            },
            &[input],
        )
    }

    /// Multi-aggregate reduce with automatic shard insertion (required for
    /// multi-worker correctness). `agg_specs`: list of (agg func, col_idx).
    /// `global_ground` is `true` only for the user's ungrouped scalar aggregate
    /// (empty `group_cols`); the grouped builder passes `group_cols.is_empty()`.
    /// `out_key` is the caller's schema-derived output-key decision; the engine
    /// validates it against the input schema.
    pub fn reduce_multi(
        &mut self,
        input: NodeId,
        group_cols: &[usize],
        agg_specs: &[(AggFunc, usize)],
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
        agg_specs: &[(AggFunc, usize)],
        global_ground: bool,
        out_key: ReduceOutKey,
    ) -> NodeId {
        self.reduce_node(input, group_cols, agg_specs, global_ground, out_key)
    }

    /// Exchange shard: routes rows to workers by hashing the given columns.
    pub fn shard(&mut self, input: NodeId, shard_cols: &[usize]) -> NodeId {
        let cols: Vec<u32> = shard_cols.iter().map(|&c| c as u32).collect();
        self.alloc_wired(OpNode::ExchangeShard { shard_cols: cols }, &[input])
    }

    /// Intermediate trace integration (equijoin accumulator).
    pub fn integrate_trace(&mut self, input: NodeId) -> NodeId {
        self.alloc_wired(OpNode::IntegrateTrace, &[input])
    }

    /// Null-extend: appends N null payload columns to each row.
    /// `right_col_type_codes` contains the column type code for each null column
    /// to append.
    pub fn null_extend(&mut self, input: NodeId, right_col_type_codes: &[u8]) -> NodeId {
        self.alloc_wired(
            OpNode::NullExtend {
                type_codes: right_col_type_codes.to_vec(),
            },
            &[input],
        )
    }

    /// Sink — primary INTEGRATE that writes to view storage.
    pub fn sink(&mut self, input: NodeId) -> NodeId {
        self.alloc_wired(OpNode::IntegrateSink, &[input])
    }

    /// Finalises the circuit.
    pub fn build(self) -> Circuit {
        Circuit {
            nodes: self.nodes,
            inputs: self.inputs,
        }
    }
}

#[cfg(test)]
#[path = "tests/circuit.rs"]
mod tests;
