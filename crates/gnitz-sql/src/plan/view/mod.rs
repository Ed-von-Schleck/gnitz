//! CREATE VIEW circuit builders, one per view shape. `dispatch` is the front
//! door (classify + route); `predicates` and `join` form the join cluster;
//! `group_by`, `set_op`, and `simple` cover the remaining shapes. Only
//! `execute_create_view` is exposed; everything else is internal to the cluster.

mod dispatch;
mod exists;
mod group_by;
mod join;
mod predicates;
mod scalar;
mod set_op;
mod simple;

pub(crate) use dispatch::execute_create_view;

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

    /// The final view's id, allocated on first use — a view with no hidden
    /// segments never pays the allocation before its own emit.
    pub fn owner_vid(&mut self, client: &mut GnitzClient) -> Result<u64, GnitzSqlError> {
        if let Some(v) = self.owner_vid {
            return Ok(v);
        }
        let v = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
        self.owner_vid = Some(v);
        Ok(v)
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
