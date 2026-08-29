//! Registering a mirrored view in the local catalog — the worker's own
//! registration path.
//!
//! The store must hold the relation under the **server's** `table_id`: the SQL
//! layer resolves a name to a server-assigned id and the read seam is
//! `scan_spec(table_id, …)`. A local `CatalogEngine` allocates its own ids from
//! `FIRST_USER_TABLE_ID`, so the store needs a way to own an id it did not
//! allocate — and that is exactly a worker's situation. A worker never allocates
//! either: the master does, and every worker learns relations through
//! `ddl_sync`, whose `table_id` argument is the *system-table family*, so the
//! user relation's own id travels inside the rows where nothing constrains it to
//! a locally-allocated value.
//!
//! No `IDX_TAB` row is ever written: only a base table may own an index, and the
//! registration guard refuses a view owner wherever it arrives. So the store
//! holds no index and can never be handed the `IndexRange { exact: true }` bound
//! whose conjunct-stripping contract would make a missing index a correctness
//! fault rather than a slowdown.
//!
//! Nor are any circuit rows written, and that is what keeps the compile path
//! unreachable: the copy is fed by direct ingest of drained deltas and never by
//! evaluating a circuit, and `capacity_bytes = 0` means no store here ever holds
//! a skeleton row, so nothing can ask it to hydrate.

use std::sync::Arc;

use gnitz_core::{qualified_name, Invalidate, MirrorError, Schema};
use gnitz_engine::catalog::SysFamily;
use gnitz_engine::schema::{make_delta_schema, SchemaDescriptor};
use gnitz_engine::storage::BatchBuilder;
use gnitz_wire::sys_rows::{write_col_tab_row, write_schema_tab_row, write_view_tab_row};
use gnitz_wire::sys_rows::{SchemaTabRow, ViewTabRow};
use gnitz_wire::OWNER_KIND_VIEW;

use crate::handle::{Mirror, Shapes};

impl Mirror {
    /// Reconcile the local catalog against `tid`'s upstream layout and leave the
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
        schema_id: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Vec<u64>, MirrorError> {
        let want_desc = descriptor_of(schema)?;
        let mut retracted = Vec::new();

        if !self
            .engine
            .get_schema_desc(tid as i64)
            .is_some_and(|desc| desc == want_desc)
        {
            // Retract whatever the local catalog holds at this id, and — the
            // dropped-and-recreated case — whatever it holds under this name at
            // some other id. Then register fresh. Each goes down the whole
            // ladder, so the cursor drop is inherited rather than restated.
            let renamed = self
                .engine
                .entity_id_by_qname(&qualified_name(schema_name, name))
                .map(|t| t as u64)
                .filter(|&t| t != tid);
            for old in std::iter::once(tid).chain(renamed) {
                if !self.engine.has_id(old as i64) {
                    continue;
                }
                self.invalidate_inner(old, Invalidate::Registration)?;
                retracted.push(old);
            }
            self.register_locally(schema_name, name, tid, schema_id, schema)?;
        }

        let view_desc = self.engine.schema_or_err(tid as i64, "mirror registration")?;
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

    /// Write the catalog rows a mirrored view needs, in registration order.
    fn register_locally(
        &mut self,
        schema_name: &str,
        name: &str,
        tid: u64,
        schema_id: u64,
        schema: &Schema,
    ) -> Result<(), MirrorError> {
        // One SCHEMA_TAB row per schema, not per view: the row is shared by every
        // view mirrored out of it, and the local catalog runs no PK-uniqueness
        // enforcement on a system family, so a second `+1` would stack a weight
        // rather than be absorbed — and the retraction of one view would then
        // leave the others' schema row behind at the wrong weight. `public` is
        // already there from the local bootstrap.
        let mut schema_b = BatchBuilder::new(SysFamily::Schema.schema());
        if self.engine.schema_id(schema_name).is_none() {
            write_schema_tab_row(
                &mut schema_b,
                &SchemaTabRow {
                    schema_id,
                    name: schema_name,
                },
                1,
            );
        }

        // Through `ColumnDef::col_tab_row`, the same mapping the upstream client
        // registers a view's columns with — including `is_hidden`, which a view's
        // synthetic PK carries and whose loss would leave the local schema a
        // column narrower than the store it keys.
        let mut col_b = BatchBuilder::new(SysFamily::Column.schema());
        for (ci, cd) in schema.columns.iter().enumerate() {
            write_col_tab_row(&mut col_b, &cd.col_tab_row(tid, OWNER_KIND_VIEW, ci), 1)?;
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
                // The store maintains no feed of its own.
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
}

/// The engine descriptor a client `Schema` denotes, through the shared wire
/// codec in both directions — so neither end is a second spelling of the block's
/// rules.
pub(crate) fn descriptor_of(schema: &Schema) -> Result<SchemaDescriptor, MirrorError> {
    let block = gnitz_core::protocol::codec::encode_schema_block(schema, 0);
    gnitz_engine::schema::decode_schema_block(&block, false)
        .map_err(|e| MirrorError::Engine(format!("mirror: schema block: {e}")))
}
