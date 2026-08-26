//! The CREATE VIEW segment sink: `ViewChain` collects the hidden segments the
//! HIR lowering cuts (in dependency order) plus the user-named final view, and
//! commits them as one atomic `create_view_chain` bundle. `EmitPieces` is the
//! per-segment emit product; `debug_assert_exchange_topology` is the one
//! structural backstop every emitted circuit passes through.

use crate::error::GnitzSqlError;
use gnitz_core::{segment_id, Circuit, ColumnDef, PlannedView, Schema, ViewName};
use std::sync::Arc;

/// Circuit + output columns + pk-list — the pieces every view emitter returns
/// for a pre-allocated view id; the caller wraps them into a `PlannedView`.
pub(crate) type EmitPieces = (Circuit, Vec<ColumnDef>, Vec<u32>);

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
    // A node's consumers (the nodes it feeds). `edges` maps (consumer, port) →
    // producer, so a consumer of `src` is any key whose value is `src`.
    let feeds = |src: gnitz_core::NodeId| -> Vec<&gnitz_core::OpNode> {
        circuit
            .edges
            .iter()
            .filter(|(_, producer)| **producer == src)
            .filter_map(|((consumer, _port), _)| circuit.nodes.get(consumer))
            .collect()
    };
    // Every node reachable downstream of `src`. Transitive, so an intervening
    // Filter cannot hide one shard from another.
    let reaches = |src: gnitz_core::NodeId| -> Vec<gnitz_core::NodeId> {
        let (mut seen, mut stack) = (Vec::new(), vec![src]);
        while let Some(n) = stack.pop() {
            for ((consumer, _), producer) in &circuit.edges {
                if *producer == n && !seen.contains(consumer) {
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

/// A `Schema` from emitted pieces: the output columns plus the pk-list (the
/// leading `k` slots, widened from the wire's `u32` indices).
fn schema_of(cols: &[ColumnDef], pk: &[u32]) -> Arc<Schema> {
    Arc::new(Schema {
        columns: cols.to_vec(),
        pk_cols: pk.iter().map(|&c| c as usize).collect(),
    })
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
    next_seg: u64,
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

    /// The next chain-local symbolic id.
    fn mint(&mut self) -> u64 {
        let v = segment_id(self.next_seg);
        self.next_seg += 1;
        v
    }

    /// The final view's symbolic id — chain-local slot 0.
    pub(crate) fn owner_vid(&self) -> u64 {
        segment_id(0)
    }

    /// Mint one hidden segment: take its view id, run `emit` with it (the emitter
    /// may push its own upstream segments first — it gets `self` back), and push
    /// the emitted pieces. Returns the segment's `(view id, schema)` plus whatever
    /// `emit` returned alongside its pieces.
    ///
    /// The mint order is the invariant this owns: the id exists before the circuit
    /// is built, so a downstream circuit can reference it, and segments land on the
    /// chain in dependency order.
    pub(crate) fn add_segment<T>(
        &mut self,
        emit: impl FnOnce(&mut ViewChain, u64) -> Result<(EmitPieces, T), GnitzSqlError>,
    ) -> Result<(u64, Arc<Schema>, T), GnitzSqlError> {
        let vid = self.mint();
        let ((circuit, cols, pk), extra) = emit(self, vid)?;
        debug_assert_exchange_topology(&circuit);
        let schema = schema_of(&cols, &pk);
        self.push_hidden(cols, pk, circuit);
        Ok((vid, schema, extra))
    }

    /// Append a hidden segment, naming it after its owner and position. The one
    /// site that names one, and it uses `ViewName::Hidden` rather than a string
    /// because the owner's real id does not exist yet.
    fn push_hidden(&mut self, cols: Vec<ColumnDef>, pk: Vec<u32>, circuit: Circuit) {
        let owner = self.owner_vid();
        let idx = self.segments.len();
        self.segments.push(PlannedView {
            name: ViewName::Hidden { owner, idx },
            sql_text: "-- hidden segment".to_string(),
            circuit,
            output_columns: cols,
            pk_cols: pk,
            // A hidden segment is never bounded — it is the unbounded
            // materialization a bounded view may not sit on — and never fed: the
            // feed belongs to the final segment, the one the client names and the
            // only one whose store it can read.
            capacity_bytes: None,
            delta_bytes: None,
        });
    }
}
