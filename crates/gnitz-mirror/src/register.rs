//! Registering a mirrored view in the local registry.
//!
//! The store holds each relation under the **server's** `table_id` — the read
//! seam is `scan_spec(table_id, …)` — and allocates no id of its own, so there
//! is nothing for a server-assigned one to collide with.
//!
//! **No copy holds a skeleton row and none owns an index**, structurally:
//! [`spec_for`] carries neither a capacity budget nor an index. That is what
//! keeps the DBSP layer unreachable, and what stops an `IndexRange { exact:
//! true }` bound from ever meeting a store with no index to answer it.

use std::sync::Arc;

use gnitz_core::{Invalidate, MirrorError, Schema};
use gnitz_store::relation::{relation_dir, RelationKind, RelationSpec, ViewBudgets};
use gnitz_store::schema::{make_delta_schema, SchemaDescriptor};

use crate::handle::{Mirror, Shapes};
use crate::state::MirrorRecord;

impl Mirror {
    /// Reconcile the local registry against `tid`'s upstream layout and leave the
    /// shapes the copy is read and written under. Returns the ids retracted.
    ///
    /// Keyed by **id**, not by name: the id is what the copy is stored under and
    /// what a read names. A registration that still holds `tid` with the same
    /// layout stands, and its cursor with it — whether this session wrote it or a
    /// previous one did and the open replayed it. Anything else describes a
    /// relation that was dropped and recreated, or altered, and the local copy of
    /// a relation that changed identity is worthless.
    pub(crate) fn register_inner(
        &mut self,
        tid: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Vec<u64>, MirrorError> {
        let block = gnitz_core::protocol::codec::encode_schema_block(schema, 0);
        let want_desc = descriptor_of_block(&block)?;
        let mut retracted = Vec::new();

        if !self
            .registry
            .get_schema_desc(tid as i64)
            .is_some_and(|desc| desc == want_desc)
        {
            // Retract whatever the registry holds at this id, and — the
            // dropped-and-recreated case — whatever it holds under this name at
            // some other id. Each goes down the whole ladder, so the cursor drop
            // is inherited rather than restated. The scan is linear over a map
            // whose size is the number of views one host mirrors, once per
            // `mirror_view`.
            let renamed = self
                .records
                .iter()
                .find(|(&t, r)| t != tid && r.schema_name == schema_name && r.name == name)
                .map(|(&t, _)| t);
            for old in std::iter::once(tid).chain(renamed) {
                if !self.registry.has_id(old as i64) {
                    continue;
                }
                self.invalidate_inner(old, Invalidate::Registration)?;
                retracted.push(old);
            }
            self.enter(
                tid,
                MirrorRecord {
                    schema_name: schema_name.to_string(),
                    name: name.to_string(),
                    block,
                },
                want_desc,
            )?;
        }

        // Either the registry already held `want_desc` or `enter` just registered
        // it, so it is the stamped shape on both arms.
        let view_desc = want_desc;
        let delta_desc = make_delta_schema(&view_desc).ok_or_else(|| {
            MirrorError::Engine(format!(
                "view '{schema_name}.{name}' cannot carry a delta feed: the stamped shape \
                 overruns a schema limit"
            ))
        })?;
        self.shapes.insert(
            tid,
            Shapes {
                schema: Arc::new(schema.clone()),
                view_desc,
                delta_desc,
            },
        );
        Ok(retracted)
    }
}

/// The registration one copy is opened under.
///
/// **The kind stays `View`, and changing it breaks persistence.** That is what
/// maps to `RecoverySource::Rederive`, and a rederived store folds to RAM and
/// publishes no manifest on a `Base` round — so the ephemeral round is the only
/// one that publishes a copy, and it is the only round a mirror runs. A copy
/// relabelled as a held relation would silently stop being persisted.
pub(crate) fn spec_for(copies_root: &str, tid: u64, schema_name: &str, schema: SchemaDescriptor) -> RelationSpec {
    RelationSpec {
        id: tid as i64,
        kind: RelationKind::View,
        schema,
        directory: relation_dir(copies_root, schema_name, RelationKind::View, tid as i64),
        // A copy scans nothing, so it is at the bottom of no chain. Depth only
        // orders a backfill, which a mirror never runs.
        depth: 0,
        // No skeleton row is ever written, so nothing can ask this store to
        // hydrate; and the store maintains no feed of its own.
        budgets: ViewBudgets::default(),
    }
}

/// The engine descriptor a wire schema block denotes, through the shared codec —
/// so neither end is a second spelling of the block's rules.
pub(crate) fn descriptor_of_block(block: &[u8]) -> Result<SchemaDescriptor, MirrorError> {
    gnitz_store::schema::decode_schema_block(block, false)
        .map_err(|e| MirrorError::Engine(format!("mirror: schema block: {e}")))
}

/// [`descriptor_of_block`] over a client `Schema`, through the same encoder the
/// registration records.
pub(crate) fn descriptor_of(schema: &Schema) -> Result<SchemaDescriptor, MirrorError> {
    descriptor_of_block(&gnitz_core::protocol::codec::encode_schema_block(schema, 0))
}
