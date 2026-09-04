//! Test-only in-process DDL: the direct `create_table` / `drop_view` /
//! `create_index` … entry points the catalog tests drive the applier with.
//!
//! **Not** a production code path. Every real DDL statement (SQL planner,
//! C-API, `gnitz-py`) is built client-side and pushed over the wire as a
//! `FLAG_DDL_TXN` bundle of system-table deltas, which the executor applies
//! through `precheck_family` + `apply_and_enqueue_family`. These wrappers exist
//! so a unit test can reach the same appliers without a server.

use super::super::*;
use gnitz_wire::sys_rows::{write_schema_tab_row, SchemaTabRow};

/// Split `schema.name`, defaulting the schema half. Only these direct entry
/// points take qualified-name strings; the wire path ships schema and entity ids
/// separately.
pub(super) fn parse_qualified_name<'a>(name: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    match name.find('.') {
        Some(dot) => (&name[..dot], &name[dot + 1..]),
        None => (default_schema, name),
    }
}

/// Production index names arrive pre-built over the wire; only `create_index`
/// below names one engine-side.
pub(super) fn make_secondary_index_name(schema_name: &str, table_name: &str, col_name: &str) -> String {
    format!("{schema_name}__{table_name}__idx_{col_name}")
}

impl CatalogEngine {
    /// Emit the single retraction delta for `pk` in `family`: seek the live row,
    /// copy it with weight −1, and submit it through the one applied-delta path.
    /// The drop cascade is the applier's reaction to that −1 (fired from
    /// `fire_hooks`), not the caller's concern. Reads through the immutable
    /// `sys_store` accessor; the `submit` move comes after.
    /// `retract_pk_list` returns an empty batch when the PK is absent
    /// or already retracted; emitters resolve the friendly "does not exist"
    /// message from the caches before calling, so the `count == 0` arm only
    /// fires on cache/storage divergence.
    /// Only these fixture drop paths retract engine-side; production
    /// retractions arrive as wire deltas.
    pub(super) fn submit_retraction(&mut self, family: SysFamily, pk: u128) -> Result<(), String> {
        let schema = family.schema();
        let batch = retract_pk_list(self.sys_store(family), &schema, vec![pk]);
        if batch.is_empty() {
            return Err("Entity does not exist in catalog".into());
        }
        self.submit(family, batch)
    }

    // -- DDL: CREATE/DROP SCHEMA -------------------------------------------

    pub(in crate::catalog) fn create_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if self.has_schema(name) {
            return Err(format!("Schema already exists: {name}"));
        }
        let sid = self.allocate_schema_id().unwrap();

        // Write schema record
        let schema = SysFamily::Schema.schema();
        let mut bb = BatchBuilder::new(schema);
        write_schema_tab_row(&mut bb, &SchemaTabRow { schema_id: sid as u64, name }, 1);
        let batch = bb.finish();

        // Submit the schemas-family delta (triggers hook).
        self.submit(SysFamily::Schema, batch)?;

        Ok(())
    }

    /// Drop every member of a schema, then the schema row — views before
    /// tables, since a view may read a member table.
    ///
    /// The engine has no `DROP SCHEMA CASCADE`; `precheck_schema_family` rejects
    /// a non-empty drop. Cascade is a client-side composition of ordinary drops
    /// (`gnitz-core`'s `Client::drop_schema`), and so is this fixture. It does
    /// not reproduce that client's retry-until-stable drain or hidden-segment
    /// filter, which resolve view-on-view and FK chains — the client's algorithm
    /// to get right, covered end-to-end, not the guard these tests are about.
    pub(in crate::catalog) fn drop_schema(&mut self, name: &str) -> Result<(), String> {
        validate_user_identifier(name)?;
        if !self.has_schema(name) {
            return Err("Schema does not exist".into());
        }
        let sid = self.schema_id(name).expect("the schema exists");

        let members: Vec<i64> = self
            .caches
            .members_by_schema
            .get(&sid)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let (views, tables): (Vec<i64>, Vec<i64>) = members
            .into_iter()
            .partition(|id| self.registry().entry(*id).is_some_and(|e| e.kind.is_view()));
        for vid in views {
            // Clears the plan caches only — the view stays registered, so the
            // drop cascade's the registry guard still resolves it.
            self.dag.invalidate(vid);
            self.submit_retraction(SysFamily::View, vid as u128)?;
        }
        for tid in tables {
            self.submit_retraction(SysFamily::Table, tid as u128)?;
        }

        // The schema is empty now, so the engine's member-count guard accepts
        // this row.
        let schema = SysFamily::Schema.schema();
        let mut bb = BatchBuilder::new(schema);
        write_schema_tab_row(&mut bb, &SchemaTabRow { schema_id: sid as u64, name }, -1);
        let batch = bb.finish();

        self.submit(SysFamily::Schema, batch)?;
        Ok(())
    }

    // -- DDL: CREATE/DROP TABLE --------------------------------------------

    /// Build a table directly in the catalog: allocate the id, write the COL_TAB
    /// records, then submit the TABLE_TAB `+1` that fires the register hook.
    pub(crate) fn create_table(
        &mut self,
        qualified_name: &str,
        col_defs: &[ColumnDef],
        pk_cols: &[u32],
    ) -> Result<i64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        let raw_pk_cols = pack_pk_cols(pk_cols);

        // Only what `submit` cannot derive for itself: the schema id, and an id
        // for the new table. Every rule this shape must satisfy is the
        // production precheck's, a few lines below — restating one here would
        // let a test asserting that rejection pass against this copy while the
        // production arm was broken.
        let sid = self
            .schema_id(schema_name)
            .ok_or_else(|| format!("Schema does not exist: {schema_name}"))?;
        let tid = self.allocate_table_id().unwrap();

        // This in-process test shortcut always builds a keyed, full-PK-distributed
        // tables (`replicated = false`, `k = 0` = default). REPLICATED and CLUSTER BY
        // routing are exercised through the catalog hook / SQL planner, not here.
        let flags = gnitz_wire::TableProps::default().pack();

        // Write columns first (table hook reads them via sys_columns)
        self.write_column_records(tid, OWNER_KIND_TABLE, col_defs)?;

        // Write table record (triggers hook)
        {
            let mut bb = BatchBuilder::new(SysFamily::Table.schema());
            push_table_tab_row(&mut bb, tid, sid, table_name, raw_pk_cols, flags, 1);
            let batch = bb.finish();
            self.submit(SysFamily::Table, batch)?;
        }

        Ok(tid)
    }

    pub(in crate::catalog) fn drop_table(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");
        validate_user_identifier(schema_name)?;
        validate_user_identifier(table_name)?;

        let qualified = format!("{schema_name}.{table_name}");
        let tid = *self
            .caches
            .entity_by_qname
            .get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {qualified}"))?;

        // Retract only the TABLE_TAB row. Its -1 fires hook_relation_register,
        // which cascades cascade_retract_indices + cascade_retract_columns. The
        // cascade lives in the hook (not inline) so WAL replay and worker sync —
        // which re-apply the -1 without calling drop_table — clean up
        // identically.
        self.submit_retraction(SysFamily::Table, tid as u128)
    }

    // -- DDL: CREATE/DROP VIEW ---------------------------------------------

    pub(in crate::catalog) fn drop_view(&mut self, qualified_name: &str) -> Result<(), String> {
        let (schema_name, view_name) = parse_qualified_name(qualified_name, "public");
        let qualified = format!("{schema_name}.{view_name}");
        let vid = *self
            .caches
            .entity_by_qname
            .get(&qualified)
            .ok_or_else(|| format!("View does not exist: {qualified}"))?;

        // Clears the plan caches only — the view stays registered, so the
        // cascade's the registry guard still resolves it.
        self.dag.invalidate(vid);

        // Retract only the VIEW_TAB row. Its -1 fires hook_relation_register, which
        // cascades cascade_retract_circuit + cascade_retract_columns and queues
        // the view directory for deferred deletion (the executor removes it
        // after the DDL zone is durable, so no delete races the WAL fdatasync).
        // Keeping the cascade in the hook makes replay and worker sync clean up
        // identically.
        self.submit_retraction(SysFamily::View, vid as u128)
    }

    // -- DDL: CREATE/DROP INDEX --------------------------------------------

    pub(in crate::catalog) fn create_index(
        &mut self,
        qualified_owner: &str,
        col_names: &[&str],
        is_unique: bool,
    ) -> Result<i64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_owner, "public");
        let qualified = format!("{schema_name}.{table_name}");
        let owner_id = *self
            .caches
            .entity_by_qname
            .get(&qualified)
            .ok_or_else(|| format!("Table does not exist: {qualified}"))?;

        // Resolve each column name to its index, in declared order.
        let col_defs = self.read_column_defs(owner_id);
        let col_indices: Vec<u32> = col_names
            .iter()
            .map(|name| {
                col_defs
                    .iter()
                    .position(|cd| cd.name == *name)
                    .map(|p| p as u32)
                    .ok_or_else(|| format!("Column not found in owner: {name}"))
            })
            .collect::<Result<_, _>>()?;

        let index_name = make_secondary_index_name(schema_name, table_name, &col_names.join("_"));
        let index_id = self.allocate_index_id().unwrap();

        // Precheck and apply as two steps, exactly as the DDL_TXN handler does:
        // a precheck rejection wrote nothing, so submitting the compensating −1
        // would leave a permanent net −1 ghost (sys_indices runs no
        // `enforce_unique_pk`). Only an apply failure needs the undo.
        let packed_cols = gnitz_wire::pack_pk_cols(&col_indices);
        let batch = idx_tab_batch(
            index_id,
            owner_id,
            packed_cols,
            &index_name,
            gnitz_wire::IndexProps { is_unique, is_internal: false },
            1,
        );
        self.precheck_family(SysFamily::Index, &batch)?;
        if let Err(e) = self.apply_and_enqueue_family(SysFamily::Index, batch) {
            // The +1 failed in hook_index_register *before* it was enqueued
            // into pending_broadcasts, so it was never broadcast to workers.
            // Route the undo through submit_local: it fires the cache-reversal
            // hooks but does NOT enqueue the −1. Broadcasting the −1 would
            // deliver a phantom retraction to workers that never saw the +1.
            let undo = idx_tab_batch(
                index_id,
                owner_id,
                packed_cols,
                &index_name,
                gnitz_wire::IndexProps { is_unique, is_internal: false },
                -1,
            );
            self.rollback_index_registration(undo, index_id).unwrap();
            // The index directory is already gone: the hook staged it before
            // `Table::new`, and `with_staged_dir` reclaims a stage whose
            // closure failed. Nothing queued it here — the `-1` retraction
            // hook only queues a directory whose circuit was registered.
            return Err(e);
        }
        Ok(index_id)
    }

    pub(in crate::catalog) fn drop_index(&mut self, index_name: &str) -> Result<(), String> {
        let idx_id = *self
            .caches
            .index_by_name
            .get(index_name)
            .ok_or_else(|| format!("Index does not exist: {index_name}"))?;

        // precheck_family enforces the FK-target uniqueness guard on the -1;
        // the cascade (circuit demotion/deletion) is the applier's reaction in
        // hook_index_register.
        self.submit_retraction(SysFamily::Index, idx_id as u128)
    }

    // -- Write helpers for system tables -----------------------------------

    pub(in crate::catalog) fn build_col_batch(
        &self,
        owner_id: i64,
        kind: i64,
        col_defs: &[ColumnDef],
        weight: i64,
    ) -> Batch {
        let mut bb = BatchBuilder::new(SysFamily::Column.schema());
        for (i, cd) in col_defs.iter().enumerate() {
            push_col_tab_row(&mut bb, owner_id, kind, i as i64, cd, weight);
        }
        bb.finish()
    }

    pub(crate) fn write_column_records(
        &mut self,
        owner_id: i64,
        kind: i64,
        col_defs: &[ColumnDef],
    ) -> Result<(), String> {
        let batch = self.build_col_batch(owner_id, kind, col_defs, 1);
        self.submit(SysFamily::Column, batch)
    }

    /// Ingest into any relation by raw id: a system family through
    /// [`CatalogEngine::submit`], a user table through the registry's DML path.
    /// Production emitters name the family and own their batch, so they call
    /// `submit` and pay no clone.
    pub(in crate::catalog) fn ingest_to_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            let family = SysFamily::from_id(table_id).ok_or_else(|| format!("Unknown system family {table_id}"))?;
            self.submit(family, batch.clone())
        } else {
            self.registry
                .ingest_returning_effective(table_id, batch.clone_batch(), false)
                .map(drop)
                .map_err(|e| format!("ingest failed for table_id={table_id}: {e}"))
        }
    }

    /// Register a base table at a caller-chosen `tid` the way a worker does: the
    /// COL_TAB and TABLE_TAB rows a DDL bundle carries, each through `ddl_sync`,
    /// so the register hooks fire and the relation store is built. `pk` names PK
    /// column indices into `cols`, in key order. The register hook raises the id
    /// counter past `tid`, so a later engine-side allocation cannot collide.
    pub(crate) fn register_table(
        &mut self,
        tid: i64,
        schema_id: i64,
        name: &str,
        cols: &[ColumnDef],
        pk: &[u32],
    ) -> Result<(), String> {
        let col_batch = self.build_col_batch(tid, OWNER_KIND_TABLE, cols, 1);
        self.ddl_sync(SysFamily::Column.id(), col_batch)?;

        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        push_table_tab_row(
            &mut bb,
            tid,
            schema_id,
            name,
            gnitz_wire::pack_pk_cols(pk),
            gnitz_wire::TableProps::default().pack(),
            1,
        );
        self.ddl_sync(SysFamily::Table.id(), bb.finish())
    }
}
