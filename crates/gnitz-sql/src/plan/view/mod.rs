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

pub(crate) use dispatch::{compile_query_to_circuit, execute_create_view};

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
    /// Id source. `None` — durable CREATE VIEW: every segment id is a real
    /// `alloc_table_id` server round trip. `Some(next)` — the ad-hoc transient
    /// path: ids are minted from a **local provisional** counter, never touching
    /// the durable `SEQ_ID_TABLES` sequence (the master discards them and remaps
    /// server-side). Both modes flow through `mint_id`, so the segment/owner
    /// invariants are identical; only the id origin differs.
    next_local_id: Option<u64>,
}

impl ViewChain {
    pub fn new() -> Self {
        ViewChain {
            owner_vid: None,
            segments: Vec::new(),
            next_local_id: None,
        }
    }

    /// A chain for the ad-hoc transient path: ids are minted locally starting at
    /// `1` (the circuit's provisional `view_id`), with no durable id allocation.
    pub fn new_transient() -> Self {
        ViewChain {
            owner_vid: None,
            segments: Vec::new(),
            next_local_id: Some(1),
        }
    }

    /// Mint the next segment id: a durable `alloc_table_id` for CREATE VIEW, or
    /// the next local provisional id for a transient chain.
    fn mint_id(&mut self, client: &mut GnitzClient) -> Result<u64, GnitzSqlError> {
        match self.next_local_id.as_mut() {
            Some(next) => {
                let id = *next;
                *next += 1;
                Ok(id)
            }
            None => client.alloc_table_id().map_err(GnitzSqlError::Exec),
        }
    }

    /// The final view's id, allocated on first use — a view with no hidden
    /// segments never pays the allocation before its own emit.
    pub fn owner_vid(&mut self, client: &mut GnitzClient) -> Result<u64, GnitzSqlError> {
        if let Some(v) = self.owner_vid {
            return Ok(v);
        }
        let v = self.mint_id(client)?;
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
        let vid = self.mint_id(client)?;
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
