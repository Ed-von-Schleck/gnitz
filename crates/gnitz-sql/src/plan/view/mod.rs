//! CREATE VIEW circuit builders, one per view shape. `dispatch` is the front
//! door (classify + route); `predicates` and `join` form the join cluster;
//! `group_by`, `set_op`, and `simple` cover the remaining shapes. Exposed:
//! the dispatch entry points plus the shared aggregate/projection analysis the
//! ad-hoc fold path consumes; every emitter is internal to the cluster.

mod dispatch;
mod exists;
mod group_by;
// `pub(crate)` for the HIR lowering: `crate::hir` calls these modules' AST-free
// primitives cross-module (a private `mod` is visible only to its parent's
// descendants, and `hir` is not one). The primitives relocate into `hir/lower/`
// once their old orchestration callers (`scalar`, `compile_hidden_body`) are
// migrated and deleted.
pub(crate) mod join;
pub(crate) mod predicates;
mod scalar;
pub(crate) mod set_op;
pub(crate) mod simple;

pub(crate) use dispatch::{cte_passthrough, execute_alter_view, execute_create_view};
// The single-relation aggregate analysis + HAVING binding and the bare-column
// projection resolver double as the ad-hoc fold planner's front end
// (`dml::select`) — re-exported so the view emitters themselves stay private.
// `has_scalar_subquery` and `cte_passthrough` back the ad-hoc read route's
// derivation-rejection / pass-through-CTE gate.
pub(crate) use group_by::{analyze_group_by, bind_having_expr, HavingCtx};
pub(crate) use scalar::has_scalar_subquery;
pub(crate) use set_op::resolve_set_projection;

use crate::error::GnitzSqlError;
use gnitz_core::{Circuit, ColumnDef, GnitzClient, PlannedView, Schema};
use std::rc::Rc;

/// Circuit + output columns + pk-list — the pieces every view emitter returns
/// for a pre-allocated view id; the caller wraps them into a `PlannedView`.
pub(crate) type EmitPieces = (Circuit, Vec<ColumnDef>, Vec<u32>);

/// Structural check of every emitted circuit's exchange topology — the one
/// backstop, since the engine hits `unreachable!` in `build_plan` on a violation
/// rather than erroring cleanly. Phrased positionally: if any `Join` node is
/// present every `ExchangeShard` must be sink-adjacent (the range-join output
/// shard is the only exchange a join circuit carries, on the `shard→sink` tail);
/// at most two `ExchangeShard`s (two only as parallel set-op sides); none
/// downstream of another. Structural only — a wrong-*columns* cut still passes,
/// so the weight pins are the real net.
///
/// One home, on the two paths every circuit reaches: `add_segment` (hidden
/// segments) and `push_final` (the user-named view). No emitter has to remember
/// to call it, and none can be added that escapes it.
pub(crate) fn debug_assert_exchange_topology(circuit: &Circuit) {
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
    for &s in &shards {
        // No shard directly downstream of another shard.
        let downstream_shard = feeds(s)
            .iter()
            .any(|op| matches!(op, gnitz_core::OpNode::ExchangeShard { .. }));
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
fn schema_of(cols: &[ColumnDef], pk: &[u32]) -> Rc<Schema> {
    Rc::new(Schema {
        columns: cols.to_vec(),
        pk_cols: pk.iter().map(|&c| c as usize).collect(),
    })
}

/// The in-flight CREATE VIEW bundle: the hidden segments compiled so far plus
/// the lazily allocated id of the user-named final view, which every hidden
/// segment's name embeds (`__h{owner}_{idx}`, the DROP-cascade convention).
/// Ends as one atomic `create_view_chain` bundle (hiddens then final).
pub(crate) struct ViewChain {
    owner_vid: Option<u64>,
    pub segments: Vec<PlannedView>,
}

impl ViewChain {
    pub fn new() -> Self {
        ViewChain {
            owner_vid: None,
            segments: Vec::new(),
        }
    }

    /// The final view's id — a durable `alloc_table_id` server round trip,
    /// allocated on first use so a view with no hidden segments never pays the
    /// allocation before its own emit.
    pub fn owner_vid(&mut self, client: &mut GnitzClient) -> Result<u64, GnitzSqlError> {
        if let Some(v) = self.owner_vid {
            return Ok(v);
        }
        let v = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
        self.owner_vid = Some(v);
        Ok(v)
    }

    /// True iff `tid` is a segment already on this chain whose circuit contains a
    /// `Join` or `ExchangeShard` node — the client-side mirror of the engine's
    /// `view_seeds_exchange_backfill`. A linear view whose delta source is such a
    /// seeding segment must be routed through the ordered distributed backfill
    /// (`simple::emit_linear` appends its identity shard), or it silently loses
    /// all pre-existing base data: its inline hook-time backfill reads a
    /// still-empty sibling segment. A source that is a base table, a pass-through
    /// CTE alias, or a linear segment is fully populated (or inline-backfilled in
    /// dependency order) at hook time, so it keeps the unsharded emit.
    pub fn segment_seeds_backfill(&self, tid: u64) -> bool {
        self.segments.iter().any(|s| {
            s.circuit.view_id == tid
                && s.circuit.nodes.values().any(|op| {
                    matches!(
                        op,
                        gnitz_core::OpNode::Join(_) | gnitz_core::OpNode::ExchangeShard { .. }
                    )
                })
        })
    }

    /// Mint one hidden segment: allocate its view id, run `emit` with it (the
    /// emitter may push its own upstream segments first — it gets `self` back),
    /// and push the emitted pieces. Returns the segment's `(view id, schema)`.
    /// The single home for the mint sequence's invariants: the id is allocated
    /// before the circuit is built (so downstream circuits can reference it) and
    /// segments land on the chain in dependency order.
    pub fn add_segment(
        &mut self,
        client: &mut GnitzClient,
        emit: impl FnOnce(&mut GnitzClient, &mut ViewChain, u64) -> Result<EmitPieces, GnitzSqlError>,
    ) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        let vid = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
        let (circuit, cols, pk) = emit(client, self, vid)?;
        debug_assert_exchange_topology(&circuit);
        let schema = schema_of(&cols, &pk);
        self.push_hidden(client, cols, pk, circuit)?;
        Ok((vid, schema))
    }

    /// Append a hidden segment, naming it `__h{owner}_{idx}` at creation — the
    /// single site that mints hidden view names.
    fn push_hidden(
        &mut self,
        client: &mut GnitzClient,
        cols: Vec<ColumnDef>,
        pk: Vec<u32>,
        circuit: Circuit,
    ) -> Result<(), GnitzSqlError> {
        let owner = self.owner_vid(client)?;
        self.segments.push(PlannedView {
            name: gnitz_core::hidden_view_name(owner, self.segments.len()),
            sql_text: "-- hidden segment".to_string(),
            circuit,
            output_columns: cols,
            pk_cols: pk,
        });
        Ok(())
    }
}
