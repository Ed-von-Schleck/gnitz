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
mod set_op;
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
