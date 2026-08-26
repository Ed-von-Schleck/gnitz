//! Registering a mirrored view in the local catalog — the worker's own
//! registration path, plus ownership of the generation counter.
//!
//! The mirror must hold the relation under the **server's** `table_id`: the SQL
//! layer resolves a name to a server-assigned id and the read seam is
//! `scan_spec(table_id, …)`. A local `CatalogEngine` allocates its own ids from
//! `FIRST_USER_TABLE_ID`, so the mirror needs a way to own an id it did not
//! allocate — and that is exactly a worker's situation. A worker never allocates
//! either: the master does, and every worker learns relations through
//! `ddl_sync`, whose `table_id` argument is the *system-table family*, so the
//! user relation's own id travels inside the rows where nothing constrains it to
//! a locally-allocated value.
//!
//! No `IDX_TAB` row is ever written: only a base table may own an index, and the
//! registration guard refuses a view owner wherever it arrives. So the mirror
//! holds no index and can never be handed the `IndexRange { exact: true }` bound
//! whose conjunct-stripping contract would make a missing index a correctness
//! fault rather than a slowdown.
//!
//! Nor are any circuit rows written, and that is what keeps the compile path
//! unreachable: the copy is fed by direct ingest of drained deltas and never by
//! evaluating a circuit, and `capacity_bytes = 0` means no store of the
//! mirror's ever holds a skeleton row, so nothing can ask it to hydrate.

use std::sync::Arc;

use gnitz_core::{delta_reply_schema, qualified_name, RelClass, Schema};
use gnitz_engine::catalog::SysFamily;
use gnitz_engine::schema::{make_delta_schema, SchemaDescriptor};
use gnitz_engine::storage::BatchBuilder;
use gnitz_wire::sys_rows::{write_col_tab_row, write_schema_tab_row, write_view_tab_row};
use gnitz_wire::sys_rows::{ColTabRow, SchemaTabRow, ViewTabRow};
use gnitz_wire::OWNER_KIND_VIEW;

use crate::apply::PollOutcome;
use crate::error::MirrorError;
use crate::handle::{Mirror, MirroredView};

impl Mirror {
    /// Mirror `schema_name.name`, and bring its copy up to date.
    ///
    /// Idempotent, and the same call whether this is a first registration or a
    /// reopen: it resolves the relation upstream, reconciles that against
    /// whatever the local catalog replayed, and then either advances the copy
    /// from its persisted cursor or reseeds it.
    ///
    /// Only a view with a delta feed can be mirrored. `delta_bytes` is a
    /// `VIEW_TAB` column and a `Delta` bound is refused against any relation
    /// whose feed is absent, at every round including zero — so a base table
    /// cannot be mirrored, and neither can a view created without one.
    ///
    /// Returns the relation's server id — which is what a read names it by — and
    /// whether the copy was reseeded rather than advanced: after a reopen this is
    /// the call that resumes-or-reseeds, so it is where a host learns which.
    pub fn mirror_view(&mut self, schema_name: &str, name: &str) -> Result<PollOutcome, MirrorError> {
        self.check_poison()?;
        // Identifiers are canonically lower-case: the rows written below must
        // carry the same spelling the server's do, or a later resolve of the
        // local catalog would miss.
        let schema_name = schema_name.to_ascii_lowercase();
        let name = name.to_ascii_lowercase();

        let tid = self.reconcile_registration(&schema_name, &name)?;
        self.advance_or_bootstrap(tid)
    }

    /// Stop mirroring `table_id`: retract its local catalog rows — which
    /// unregisters the relation and queues its directory for deletion — and drop
    /// its cursor.
    pub fn forget_view(&mut self, table_id: u64) -> Result<(), MirrorError> {
        self.check_poison()?;
        if !self.engine.has_id(table_id as i64) {
            return Ok(());
        }
        self.views.remove(&table_id);
        self.cursors.remove(&table_id);
        self.retract_relation(table_id)
    }

    /// Resolve upstream, reconcile that against the local catalog, and leave a
    /// [`MirroredView`] entry for the result. Returns the relation's server id.
    ///
    /// Reconciliation is keyed by **id**, not by name: the id is what the copy
    /// is stored under and what a read names. A registration that still holds
    /// the resolved id with the same layout stands; anything else describes a
    /// relation that was dropped and recreated, or altered, and the local copy
    /// of a relation that changed identity is worthless — the bootstrap that
    /// follows is the only correct answer anyway.
    pub(crate) fn reconcile_registration(&mut self, schema_name: &str, name: &str) -> Result<u64, MirrorError> {
        let rel = self.client.resolve_relation(schema_name, name)?;
        if !rel.class.is_view() {
            return Err(MirrorError::Engine(format!(
                "'{schema_name}.{name}' is a {}; only a view can be mirrored",
                rel.class.noun()
            )));
        }
        if rel.class == RelClass::BoundedView {
            return Err(MirrorError::Engine(format!(
                "view '{schema_name}.{name}' is capacity-bounded, and a capacity and a feed \
                 are refused together, so it carries no feed to subscribe to"
            )));
        }
        if !rel.delta {
            return Err(MirrorError::Engine(format!(
                "view '{schema_name}.{name}' keeps no delta feed; \
                 create it WITH (delta = '<size>') to mirror it"
            )));
        }

        let tid = rel.tid;
        let schema = Arc::clone(&rel.schema);
        let want_desc = descriptor_of(&schema)?;

        // A registration the local catalog already holds at this id and layout
        // stands, and its cursor with it — whether this session registered it or
        // a previous one did and the open replayed it.
        if !self
            .engine
            .get_schema_desc(tid as i64)
            .is_some_and(|desc| desc == want_desc)
        {
            // Retract whatever the local catalog holds at this id, and — the
            // dropped-and-recreated case — whatever it holds under this name at
            // some other id. Then register fresh.
            let renamed = self
                .engine
                .entity_id_by_qname(&qualified_name(schema_name, name))
                .map(|t| t as u64)
                .filter(|&t| t != tid);
            for old in std::iter::once(tid).chain(renamed) {
                if !self.engine.has_id(old as i64) {
                    continue;
                }
                self.views.remove(&old);
                self.cursors.remove(&old);
                self.retract_relation(old)?;
            }
            self.register_locally(schema_name, name, tid, &schema)?;
        }

        let view_desc = self.engine.schema_or_err(tid as i64, "mirror registration")?;
        let delta_desc = make_delta_schema(&view_desc).ok_or_else(|| {
            MirrorError::Engine(format!(
                "view '{schema_name}.{name}' cannot carry a delta feed: the stamped shape \
                 overruns a schema limit"
            ))
        })?;
        self.views.insert(
            tid,
            MirroredView {
                schema_name: schema_name.to_string(),
                name: name.to_string(),
                delta_reply_schema: Arc::new(delta_reply_schema(&schema)),
                desc: rel,
                view_desc,
                delta_desc,
            },
        );
        Ok(tid)
    }

    /// Write the catalog rows a mirrored view needs, in registration order.
    fn register_locally(
        &mut self,
        schema_name: &str,
        name: &str,
        tid: u64,
        schema: &Schema,
    ) -> Result<(), MirrorError> {
        // The schema id is the one field the resolve reply does not carry. A
        // schema the local catalog already holds answers it from the id written
        // there on the first registration, which came from the server; only a
        // first sighting pays the SCHEMA_TAB scan every DDL path takes. Using
        // the server's id rather than a locally-allocated one is not cosmetic: a
        // local counter would mint an id the server had already given to a
        // *different* schema whose view the handle mirrors next.
        //
        // One SCHEMA_TAB row per schema, not per view: the row is shared by
        // every view the handle mirrors out of it, and the local catalog runs no
        // PK-uniqueness enforcement on a system family, so a second `+1` would
        // stack a weight rather than be absorbed — and the retraction of one
        // view would then leave the others' schema row behind at the wrong
        // weight. `public` is already there from the local bootstrap.
        let mut schema_b = BatchBuilder::new(SysFamily::Schema.schema());
        let schema_id = match self.engine.schema_id(schema_name) {
            Some(id) => id as u64,
            None => {
                let id = self.client.schema_id(schema_name)?;
                write_schema_tab_row(
                    &mut schema_b,
                    &SchemaTabRow {
                        schema_id: id,
                        name: schema_name,
                    },
                    1,
                );
                id
            }
        };

        let mut col_b = BatchBuilder::new(SysFamily::Column.schema());
        for (ci, cd) in schema.columns.iter().enumerate() {
            write_col_tab_row(
                &mut col_b,
                &ColTabRow {
                    owner_id: tid,
                    owner_kind: OWNER_KIND_VIEW,
                    col_idx: ci as u64,
                    name: &cd.name,
                    type_code: cd.type_code as u64,
                    is_nullable: cd.is_nullable,
                    // A view's column carries no FK.
                    fk_table_id: 0,
                    fk_col_idx: 0,
                    is_serial: cd.is_serial,
                    // A view's physical PK is often a synthetic hidden column, and
                    // dropping the flag would leave the local schema a column
                    // narrower than the one the store is keyed by.
                    is_hidden: cd.is_hidden,
                },
                1,
            )?;
        }

        let pk_cols: Vec<u32> = schema.pk_indices().iter().map(|&i| i as u32).collect();
        let mut view_b = BatchBuilder::new(SysFamily::View.schema());
        write_view_tab_row(
            &mut view_b,
            &ViewTabRow {
                view_id: tid,
                schema_id,
                name,
                // Nothing local re-plans the view; the definition lives upstream.
                sql_definition: "",
                pk_col_idx: gnitz_wire::pack_pk_cols(&pk_cols),
                // The copy holds no skeleton rows, so nothing can ask it to
                // hydrate — which is what keeps the compile path unreachable.
                capacity_bytes: 0,
                // The mirror maintains no feed of its own.
                delta_bytes: 0,
            },
            1,
        );

        // Ascending topo priority, and the order is a correctness requirement: the
        // relation register hook reads its columns back through `sys_columns`
        // storage rather than the cache, so a VIEW_TAB row applied before the
        // COL_TAB rows registers a view whose schema build finds no columns.
        // Nothing sorts for this caller — it calls `ddl_sync` directly.
        let families = [
            (SysFamily::Schema, schema_b.finish()),
            (SysFamily::Column, col_b.finish()),
            (SysFamily::View, view_b.finish()),
        ];
        for (family, batch) in families {
            if batch.count > 0 {
                self.engine.ddl_sync(family.id(), batch)?;
            }
        }
        Ok(())
    }

    /// Unregister `relation_id` locally and remove its directory. The SCHEMA_TAB
    /// row stays: every view mirrored out of that schema shares it.
    fn retract_relation(&mut self, relation_id: u64) -> Result<(), MirrorError> {
        self.engine.retract_relation_registration(relation_id as i64)?;
        // No worker here can still be applying the create this drop races, so the
        // gate the server needs is vacuous and the directory goes now.
        self.engine.defer_pending_dir_deletions();
        self.engine.drain_checkpoint_gated_deletions();
        Ok(())
    }
}

/// The engine descriptor a client `Schema` denotes, through the shared wire
/// codec in both directions — so neither end is a second spelling of the block's
/// rules.
pub(crate) fn descriptor_of(schema: &Schema) -> Result<SchemaDescriptor, MirrorError> {
    let block = gnitz_core::protocol::codec::encode_schema_block(schema, 0);
    gnitz_engine::schema::decode_schema_block(&block, false)
        .map_err(|e| MirrorError::Engine(format!("mirror: schema block: {e}")))
}
