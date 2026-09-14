//! The CREATE VIEW segment sink: `ViewChain` collects the hidden segments the
//! HIR lowering cuts (in dependency order) plus the user-named final view, and
//! commits them as one atomic `create_view_chain` bundle. `EmitPieces` is the
//! per-segment emit product; `debug_assert_exchange_topology` is the one
//! structural backstop every emitted circuit passes through.

use super::lower::SegInput;
use super::physical::Frame;
use crate::error::GnitzSqlError;
use gnitz_core::{segment_id, Circuit, PlannedView, Schema};
use std::sync::Arc;

/// What every view emitter returns: the circuit and its output frame, whose
/// schema is the view's and whose layout is what a cut segment exposes to its
/// parent.
pub(crate) struct EmitPieces {
    pub circuit: Circuit,
    pub out: Frame,
}

/// Structural check of every emitted circuit's exchange topology, catching a
/// planner regression at the emitter rather than as a `CREATE VIEW` rejection
/// from the engine. Wider than what the engine rejects — sink-adjacency and the
/// two-shard cap have no engine-side counterpart. Phrased positionally: if any
/// `Join` node is present every `ExchangeShard` must be sink-adjacent (the
/// range-join output shard is the only exchange a join circuit carries, on the
/// `shard→sink` tail); at most two `ExchangeShard`s (two only as parallel set-op
/// sides); none downstream of another, directly or through intervening nodes.
/// Structural only — a wrong-*columns* cut still passes, so the weight pins are
/// the real net.
///
/// One home, inside `ViewChain::push`, which every emitted circuit reaches.
pub(crate) fn debug_assert_exchange_topology(circuit: &Circuit) {
    if !cfg!(debug_assertions) {
        return;
    }
    let shards: Vec<gnitz_core::NodeId> = circuit
        .nodes
        .iter()
        .filter(|(_, op)| matches!(op, gnitz_core::OpNode::ExchangeShard { .. }))
        .map(|(id, _)| *id)
        .collect();
    debug_assert!(
        shards.len() <= 2,
        "view circuit has {} ExchangeShard nodes (max 2)",
        shards.len()
    );
    let has_join = circuit
        .nodes
        .values()
        .any(|op| matches!(op, gnitz_core::OpNode::Join(_)));
    // A node's consumers (the nodes it feeds). `inputs` maps a consumer to its
    // producer per slot, so a consumer of `src` is any node holding `src`.
    let feeds = |src: gnitz_core::NodeId| -> Vec<&gnitz_core::OpNode> {
        circuit
            .inputs
            .iter()
            .filter(|(_, slots)| slots.contains(&Some(src)))
            .filter_map(|(consumer, _)| circuit.nodes.get(consumer))
            .collect()
    };
    // Every node reachable downstream of `src`. Transitive, so an intervening
    // Filter cannot hide one shard from another.
    let reaches = |src: gnitz_core::NodeId| -> Vec<gnitz_core::NodeId> {
        let (mut seen, mut stack) = (Vec::new(), vec![src]);
        while let Some(n) = stack.pop() {
            for (consumer, slots) in &circuit.inputs {
                if slots.contains(&Some(n)) && !seen.contains(consumer) {
                    seen.push(*consumer);
                    stack.push(*consumer);
                }
            }
        }
        seen
    };
    for &s in &shards {
        // No shard anywhere downstream of another shard.
        let downstream_shard = reaches(s).iter().any(|n| shards.contains(n));
        debug_assert!(!downstream_shard, "an ExchangeShard feeds another ExchangeShard");
        if has_join {
            // In a join circuit every shard must be sink-adjacent (the range-join
            // output `shard→sink` tail).
            let sink_adjacent = feeds(s)
                .iter()
                .any(|op| matches!(op, gnitz_core::OpNode::IntegrateSink));
            debug_assert!(sink_adjacent, "a Join circuit has a non-sink-adjacent ExchangeShard");
        }
    }
}

/// Reject a node whose counted list exceeds `MAX_COLUMNS`, which `write_count`
/// asserts on rather than rejects. An intermediate node can be wider than the
/// view it feeds, so the output schema does not bound it.
pub(crate) fn reject_circuit_column_overflow(circuit: &Circuit) -> Result<(), GnitzSqlError> {
    use crate::validate::reject_column_overflow as check;
    use gnitz_core::{MapKind, OpNode};
    for op in circuit.nodes.values() {
        match op {
            OpNode::Map(MapKind::Projection(cols)) => check("a view circuit projection", cols.len())?,
            OpNode::Map(MapKind::Compute(map)) => check("a view circuit computed projection", map.out_cols.len())?,
            OpNode::Map(MapKind::Reindex { keep, key, role: _ }) => {
                check("a view circuit reindex key", key.len())?;
                check("a view circuit reindex payload", keep.len())?;
            }
            OpNode::Map(MapKind::HashRow { cols, branch_id: _ }) => {
                check("a view circuit content-hash key", cols.len())?
            }
            OpNode::Reduce { group_cols, agg, global_ground: _ } => {
                check("a view circuit group key", group_cols.len())?;
                check("a view circuit aggregate list", agg.len())?;
            }
            OpNode::ExchangeShard { shard_cols } => check("a view circuit shard key", shard_cols.len())?,
            OpNode::TopN { group_cols, order, limit: _, offset: _ } => {
                check("a view circuit top-N group key", group_cols.len())?;
                check("a view circuit top-N order key", order.len())?;
            }
            OpNode::NullExtend { type_codes } => check("a view circuit null-fill region", type_codes.len())?,
            // No counted list to check.
            OpNode::ScanDelta { .. }
            | OpNode::Filter(_)
            | OpNode::Negate
            | OpNode::Union
            | OpNode::Distinct
            | OpNode::PositivePart
            | OpNode::Join(_)
            | OpNode::IntegrateSink
            | OpNode::IntegrateTrace
            | OpNode::WorkerFilter => {}
        }
    }
    Ok(())
}

/// `Schema::validate_parts`, with a rejection naming the stage `what`.
pub(crate) fn admit(schema: &Schema, what: &str) -> Result<(), GnitzSqlError> {
    Schema::validate_parts(&schema.pk_cols, &schema.columns)
        .map_err(|e| GnitzSqlError::Unsupported(format!("{what}: {e}")))
}

/// The in-flight CREATE VIEW bundle: the hidden segments compiled so far plus
/// the id of the user-named final view, which every hidden segment names as its
/// owner (the DROP-cascade convention). Ends as one atomic `create_view_chain`
/// bundle (hiddens then final).
///
/// Ids are symbolic, minted from a chain-local counter and substituted for real
/// ones at commit. Compiling a body therefore reaches no server and repeats
/// exactly, which the resolve loop needs to re-run a pass.
pub(crate) struct ViewChain {
    next_seg: u32,
    pub(crate) segments: Vec<PlannedView>,
}

impl ViewChain {
    pub(crate) fn new() -> Self {
        ViewChain {
            // Slot 0 is the owner's, taken before any hidden segment can mint.
            next_seg: 1,
            segments: Vec::new(),
        }
    }

    /// The next free chain-local slot.
    fn mint(&mut self) -> u32 {
        let k = self.next_seg;
        self.next_seg += 1;
        k
    }

    /// Push one emitted circuit at `seg`: the one path every circuit, hidden or
    /// final, reaches — so no emitter has to remember the checks, and none can be
    /// added that escapes them.
    fn push(
        &mut self,
        seg: u32,
        circuit: Circuit,
        schema: Schema,
        capacity_bytes: Option<u64>,
        delta_bytes: Option<u64>,
        what: &str,
    ) -> Result<(), GnitzSqlError> {
        debug_assert_exchange_topology(&circuit);
        // Before the node cap, so a view too wide to register is named by its own
        // columns rather than by whichever node first exceeds the cap.
        admit(&schema, what)?;
        reject_circuit_column_overflow(&circuit)?;
        self.segments.push(PlannedView {
            seg,
            circuit,
            output_columns: schema.columns,
            pk_cols: schema.pk_cols,
            capacity_bytes,
            delta_bytes,
        });
        Ok(())
    }

    /// Mint one hidden segment: take its chain-local slot, run `emit` (the
    /// emitter may push its own upstream segments first — it gets `self` back),
    /// and push the emitted pieces. Returns the segment as a `SegInput`.
    ///
    /// The mint order is the invariant this owns: the slot is taken before the
    /// circuit is built, so a downstream circuit can reference this segment, and
    /// segments land on the chain in dependency order.
    pub(crate) fn add_segment(
        &mut self,
        emit: impl FnOnce(&mut ViewChain) -> Result<EmitPieces, GnitzSqlError>,
    ) -> Result<SegInput, GnitzSqlError> {
        let seg = self.mint();
        let EmitPieces { circuit, out } = emit(self)?;
        // Capacity and delta feeds belong to the user-named view alone.
        self.push(
            seg,
            circuit,
            Schema::clone(&out.schema),
            None,
            None,
            "view segment output",
        )?;
        Ok(SegInput {
            tid: segment_id(seg as u64),
            frame: out,
            // A chain-minted id, not a catalog one: no kind, no index bound.
            desc: None,
        })
    }

    /// Push the user-named view, always the chain's slot 0.
    pub(crate) fn push_final(
        &mut self,
        pieces: EmitPieces,
        capacity_bytes: Option<u64>,
        delta_bytes: Option<u64>,
    ) -> Result<(), GnitzSqlError> {
        let EmitPieces { circuit, out } = pieces;
        self.push(
            0,
            circuit,
            Arc::unwrap_or_clone(out.schema),
            capacity_bytes,
            delta_bytes,
            "view output",
        )
    }
}
