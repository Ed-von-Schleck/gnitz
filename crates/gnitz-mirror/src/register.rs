//! Registering a mirrored view in the local registry.
//!
//! The store holds each relation under the **server's** `table_id` — the read
//! seam is `scan_spec(table_id, …)` — and allocates no id of its own, so there
//! is nothing for a server-assigned one to collide with.
//!
//! **No copy holds a skeleton row and none owns an index**: the registration
//! `Mirror::enter` builds carries neither a capacity budget nor an index. That
//! is what keeps the DBSP layer unreachable, and what stops an `IndexRange {
//! walk: Required }` bound from ever meeting a store with no index to answer it.

use gnitz_core::{Invalidate, MirrorError, Schema};
use gnitz_store::schema::SchemaDescriptor;

use crate::handle::Mirror;
use crate::state::MirrorRecord;

impl Mirror {
    /// Reconcile the local registry against `tid`'s upstream layout: a record
    /// holding `tid` under the same schema block stands, cursor and all, and
    /// takes the upstream name; anything else at this id or this name is a
    /// relation that changed identity, whose copy is retracted. Returns the id
    /// whose registration that retracted, so the caller can drop its own
    /// binding under the same name.
    pub(crate) fn register_inner(
        &mut self,
        tid: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Option<u64>, MirrorError> {
        let block = gnitz_core::protocol::codec::encode_schema_block(schema, 0);
        // Whatever the store holds under this name at another id was renamed or
        // recreated upstream; its copy directory and state row outlive every
        // checkpoint if nothing retracts them. Outside the match because a pure
        // rename between two identically-shaped views takes the in-place arm.
        let renamed = self
            .records
            .iter()
            .find(|(&t, r)| t != tid && r.schema_name == schema_name && r.name == name)
            .map(|(&t, _)| t);
        if let Some(old) = renamed {
            self.invalidate_inner(old, Invalidate::Registration)?;
        }
        match self.records.get_mut(&tid).filter(|r| r.block == block) {
            // A rename upstream keeps the id; a stale name here would match a
            // later view created under it.
            Some(r) => {
                r.schema_name = schema_name.to_string();
                r.name = name.to_string();
            }
            None => {
                self.invalidate_inner(tid, Invalidate::Registration)?;
                self.enter(
                    tid,
                    MirrorRecord {
                        schema_name: schema_name.to_string(),
                        name: name.to_string(),
                        block,
                        cursor: None,
                    },
                )?;
            }
        }
        Ok(renamed)
    }
}

/// The engine descriptor a client `Schema` denotes, through the shared codec —
/// so neither end is a second spelling of the block's rules.
pub(crate) fn descriptor_of(schema: &Schema) -> Result<SchemaDescriptor, MirrorError> {
    let block = gnitz_core::protocol::codec::encode_schema_block(schema, 0);
    gnitz_store::schema::decode_schema_block(&block, false)
        .map_err(|e| MirrorError::Engine(format!("mirror: schema block: {e}")))
}
