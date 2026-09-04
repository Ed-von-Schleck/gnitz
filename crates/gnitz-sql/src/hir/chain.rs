//! The CREATE VIEW segment sink: `ViewChain` collects the hidden segments the
//! HIR lowering cuts (in dependency order) plus the user-named final view, and
//! commits them as one atomic `create_view_chain` bundle. `EmitPieces` is the
//! per-segment emit product; `debug_assert_exchange_topology` is the one
//! structural backstop every emitted circuit passes through.

use super::lower::SegInput;
use super::ColId;
use crate::error::GnitzSqlError;
use gnitz_core::{segment_id, Circuit, ColumnDef, PlannedView, Schema};
use std::sync::Arc;

/// Circuit + output columns + pk-list — the pieces every view emitter returns
/// for a pre-allocated view id; the caller wraps them into a `PlannedView`.
/// How many leading output columns are the PK region. Every emitter puts the PK
/// at the front, so the arity is the whole pk-list — `pk_col_list` spells it out
/// at the wire boundary, and nothing in between carries the redundant vector.
pub(crate) type PkArity = usize;

/// The wire's pk-list for a [`PkArity`].
pub(crate) fn pk_col_list(pk: PkArity) -> Vec<u32> {
    (0..pk as u32).collect()
}

pub(crate) type EmitPieces = (Circuit, Vec<ColumnDef>, PkArity);

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
/// One home, on the two paths every circuit reaches: `add_segment` (hidden
/// segments) and the final view push (`create::build_query_segments`). No emitter
/// has to remember to call it, and none can be added that escapes it.
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

/// A `Schema` from emitted pieces: the output columns, of which the leading
/// [`PkArity`] are the PK region. Runs the shared admissibility rules
/// (`Schema::from_parts`) here, where `what` names the stage, instead of leaving
/// them to the DDL gateway — whose verdict is identical but names only a segment
/// index, and which the ad-hoc fold's pre-map never reaches.
pub(crate) fn schema_of(cols: &[ColumnDef], pk: PkArity, what: &str) -> Result<Arc<Schema>, GnitzSqlError> {
    Schema::from_parts(cols.to_vec(), (0..pk).collect())
        .map(Arc::new)
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

    /// Mint one hidden segment: take its chain-local slot, run `emit` (the
    /// emitter may push its own upstream segments first — it gets `self` back),
    /// and push the emitted pieces. Returns the segment's `(symbolic view id,
    /// schema)` plus whatever `emit` returned alongside its pieces.
    ///
    /// The mint order is the invariant this owns: the slot is taken before the
    /// circuit is built, so a downstream circuit can reference this segment, and
    /// segments land on the chain in dependency order.
    pub(crate) fn add_segment(
        &mut self,
        emit: impl FnOnce(&mut ViewChain) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError>,
    ) -> Result<SegInput, GnitzSqlError> {
        let seg = self.mint();
        let ((circuit, cols, pk), layout) = emit(self)?;
        debug_assert_exchange_topology(&circuit);
        let schema = schema_of(&cols, pk, "view segment output")?;
        self.push_hidden(seg, cols, pk, circuit);
        Ok(SegInput {
            tid: segment_id(seg as u64),
            schema,
            layout,
            // A chain-minted id, not a catalog one: no kind, no index bound.
            desc: None,
        })
    }

    /// Append an internal segment. It carries no name: `create_view_chain` names
    /// every non-final element of a bundle from its own allocated id, and records
    /// the user view as its owner in a column.
    fn push_hidden(&mut self, seg: u32, cols: Vec<ColumnDef>, pk: PkArity, circuit: Circuit) {
        self.segments.push(PlannedView {
            seg,
            circuit,
            output_columns: cols,
            pk_cols: pk_col_list(pk),
            // A hidden segment is never bounded — it is the unbounded
            // materialization a bounded view may not sit on — and never fed: the
            // feed belongs to the final segment, the one the client names and the
            // only one whose store it can read.
            capacity_bytes: None,
            delta_bytes: None,
        });
    }
}
