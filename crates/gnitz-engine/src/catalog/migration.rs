//! Migration helpers: cascade resolution, directory snapshotting,
//! and atomic swap-batch construction.
//!
//! All public entry points here are called from the `apply_migration`
//! orchestrator in `executor.rs`. Collect_* / build_migration_row_batch
//! run under `catalog_rwlock.read()`; the final ingest + broadcast
//! runs under `catalog_rwlock.write()`.

use std::path::PathBuf;

use crate::storage::Batch;
use super::{
    BatchBuilder, CatalogEngine,
    SchemaDescriptor,
    // re-exported items from sys_tables via catalog::mod
    copy_cursor_row_with_weight, cursor_read_u64, cursor_read_string,
    retract_single_row, retract_rows_by_pk_hi,
    ingest_batch_into,
};
use super::sys_tables::*;

/// Phase-2 record of a single staged object.  Created by `apply_phase2`
/// and consumed by `apply_phase3` via `build_cascade_from_staged`.
#[derive(Clone, Debug)]
pub(crate) struct StagedMapping {
    pub kind: StagedKind,
    /// The tid / vid / idx_id allocated in Phase 2.
    pub staged_id: i64,
    pub final_schema: String,
    pub final_name: String,
    /// For views only: the original SQL text (with the final view name)
    /// so Phase 3's rename overwrites `sql_definition` correctly.
    pub final_sql: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StagedKind { Table, View, Index }

pub(crate) const STAGED_PREFIX: &str = "__mig_";

pub(crate) fn staged_name(hash_hex: &str, name: &str) -> String {
    format!("__mig_{}_{}", hash_hex, name)
}

impl From<StagedKind> for RenameKind {
    fn from(k: StagedKind) -> Self {
        match k {
            StagedKind::Table => RenameKind::Table,
            StagedKind::View  => RenameKind::View,
            StagedKind::Index => RenameKind::Index,
        }
    }
}

/// One entry in the Phase-3 swap that takes a staged object named
/// `__mig_<hash>_<final_name>` and renames it to `<final_name>`.
/// The object's tid/vid/idx_id is preserved across the swap; only
/// the `name` column (plus `sql_definition` for views) changes.
#[derive(Clone, Debug)]
pub(crate) struct Rename {
    pub kind: RenameKind,
    pub id: i64,
    pub final_schema: String,
    pub final_name: String,
    /// For `Kind::View` only: the rewritten SQL text (with any
    /// `__mig_` prefixes stripped from dependency names). None for
    /// tables and indices.
    pub final_sql: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RenameKind { Table, View, Index }

/// Resolved universe of Phase 3 mutations.
#[derive(Default, Debug)]
pub(crate) struct Cascade {
    pub dropped_table_ids: Vec<i64>,
    pub dropped_view_ids:  Vec<i64>,
    /// Both user-specified index drops and cascade drops from dropped
    /// tables (FK + secondary).
    pub dropped_index_ids: Vec<i64>,
    pub renames: Vec<Rename>,
}

/// Plain-data representation of a migration row.
#[derive(Clone, Debug)]
pub(crate) struct MigrationRow {
    pub hash: u128,
    pub parent_hash: u128,
    pub author: String,
    pub message: String,
    pub created_lsn: i64,
    pub desired_state_sql: String,
    /// Raw bincode bytes — may contain non-UTF-8 sequences. Stored as
    /// STRING via the German-string codec (accepts raw bytes on the
    /// ingest path).
    pub desired_state_canonical: Vec<u8>,
    pub format_version: u64,
}

impl CatalogEngine {
    // -- Cascade ---------------------------------------------------------

    /// Resolve dropped-object names → ids and append FK/secondary
    /// indices owned by dropped tables to `dropped_index_ids`. Stale
    /// names (not in the registry) are silently skipped — they are
    /// either already dropped by an earlier same-migration step or
    /// an erroneous diff; Phase 3's TOCTOU re-check rejects the
    /// latter.
    pub(crate) fn collect_cascade(
        &mut self,
        dropped_tables: &[(String, String)],
        dropped_views:  &[(String, String)],
        dropped_indices: &[(String, String)],
        renames: Vec<Rename>,
    ) -> Cascade {
        let mut cascade = Cascade::default();
        cascade.renames = renames;

        for (s, n) in dropped_tables {
            let q = format!("{}.{}", s, n);
            if let Some(&id) = self.name_to_id.get(&q) {
                cascade.dropped_table_ids.push(id);
            }
        }
        for (s, n) in dropped_views {
            let q = format!("{}.{}", s, n);
            if let Some(&id) = self.name_to_id.get(&q) {
                cascade.dropped_view_ids.push(id);
            }
        }
        for (_s, n) in dropped_indices {
            // Indices are keyed by bare name (index_by_name). A single
            // index row has no owning schema column — it belongs to a
            // schema by way of its owner table.
            if let Some(&id) = self.index_by_name.get(n) {
                cascade.dropped_index_ids.push(id);
            }
        }

        // Cascade: every secondary index on a dropped table must be
        // retracted. Scan sys_indices for matching owner_id rows.
        let owner_set: std::collections::HashSet<i64> =
            cascade.dropped_table_ids.iter().copied().collect();
        if !owner_set.is_empty() {
            let mut cursor = match self.sys_indices.create_cursor() {
                Ok(c) => c,
                Err(_) => return cascade,
            };
            while cursor.cursor.valid {
                if cursor.cursor.current_weight > 0 {
                    let owner_id = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64;
                    if owner_set.contains(&owner_id) {
                        let idx_id = cursor.cursor.current_key_lo as i64;
                        if !cascade.dropped_index_ids.contains(&idx_id) {
                            cascade.dropped_index_ids.push(idx_id);
                        }
                    }
                }
                cursor.cursor.advance();
            }
        }

        cascade
    }

    /// Build the Phase-3 Cascade from the list of staged objects produced
    /// by Phase 2 plus the drop lists in `diff`.  Must be called under
    /// `catalog_rwlock.write()`.
    pub(crate) fn build_cascade_from_staged(
        &mut self,
        diff: &gnitz_wire::migration::Diff,
        staged: &[StagedMapping],
    ) -> Cascade {
        let renames: Vec<Rename> = staged.iter().map(|m| Rename {
            kind: m.kind.into(),
            id: m.staged_id,
            final_schema: m.final_schema.clone(),
            final_name:   m.final_name.clone(),
            final_sql:    m.final_sql.clone(),
        }).collect();

        let dropped_tables: Vec<(String, String)> = diff.dropped_tables.iter()
            .map(|t| (t.schema.clone(), t.name.clone())).collect();
        let dropped_views: Vec<(String, String)> = diff.dropped_views.iter()
            .map(|v| (v.schema.clone(), v.name.clone())).collect();
        let dropped_indices: Vec<(String, String)> = diff.dropped_indices.iter()
            .map(|i| (i.schema.clone(), i.name.clone())).collect();

        self.collect_cascade(&dropped_tables, &dropped_views, &dropped_indices, renames)
    }

    /// Collect all `__mig_*` objects currently live in the catalog.  Called by
    /// `mig_gc_task` under a brief read lock to snapshot the orphan list.
    pub(crate) fn collect_staged_orphans(&mut self) -> Vec<(StagedKind, String, String)> {
        let mut out: Vec<(StagedKind, String, String)> = Vec::new();

        // Collect __mig_* table / view names (they share the name_to_id map).
        let mig_entries: Vec<(String, String, i64)> = self.name_to_id.iter()
            .filter_map(|(qualified, &id)| {
                let mut it = qualified.splitn(2, '.');
                let schema = it.next()?;
                let name   = it.next()?;
                if name.starts_with(STAGED_PREFIX) {
                    Some((schema.to_string(), name.to_string(), id))
                } else {
                    None
                }
            })
            .collect();

        for (schema, name, id) in mig_entries {
            let kind = if self.is_view_id(id) { StagedKind::View } else { StagedKind::Table };
            out.push((kind, schema, name));
        }

        // Collect __mig_* index names.
        let mig_idx: Vec<String> = self.index_by_name.keys()
            .filter(|n| n.starts_with(STAGED_PREFIX))
            .cloned()
            .collect();
        for name in mig_idx {
            out.push((StagedKind::Index, String::new(), name));
        }

        out
    }

    // -- Directory snapshot ---------------------------------------------

    /// Collect on-disk directory paths for every object that Phase 3
    /// will drop. Must be called BEFORE applying the swap: once the
    /// SYS_TABLES -1 fires, `dag.unregister_table` clears the
    /// TableEntry and the directory becomes unreadable from the
    /// catalog.
    pub(crate) fn collect_dropped_directories(&self, cascade: &Cascade) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = Vec::new();
        for &tid in &cascade.dropped_table_ids {
            if let Some(entry) = self.dag.tables.get(&tid) {
                out.push(PathBuf::from(&entry.directory));
            }
        }
        for &vid in &cascade.dropped_view_ids {
            if let Some(entry) = self.dag.tables.get(&vid) {
                out.push(PathBuf::from(&entry.directory));
            }
        }
        // Indices have their own subdir `<owner_dir>/idx_<idx_id>`,
        // but the owner's directory tree (dropped above) already
        // covers them when the owner table is dropped. For
        // explicitly-dropped indices on tables that survive the
        // migration, we rely on the same startup sweep (B3) to reap
        // them — no need to enumerate here.
        out
    }

    // -- Build migration row batch ---------------------------------------

    /// Construct the `sys_migrations` insert batch for a freshly
    /// committed migration. The caller ingests this separately from
    /// the swap batches (SAL order: swap entries first, then the
    /// sys_migrations row, so workers update `current_migration_hash`
    /// only after the swap has applied).
    pub(crate) fn build_migration_row_batch(&self, row: &MigrationRow) -> Batch {
        let schema = migrations_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        let pk_lo = row.hash as u64;
        let pk_hi = (row.hash >> 64) as u64;
        bb.begin_row(pk_lo, pk_hi, 1);
        let parent_lo = row.parent_hash as u64;
        let parent_hi = (row.parent_hash >> 64) as u64;
        bb.put_u128(parent_lo, parent_hi);
        bb.put_string(&row.author);
        bb.put_string(&row.message);
        bb.put_u64(row.created_lsn as u64);
        bb.put_string(&row.desired_state_sql);
        // Store canonical bytes as a hex STRING so the column holds
        // valid UTF-8 at every layer (wire, memtable, WAL). The raw
        // bytes are what the migration hash covers, so both client
        // and server hex-decode before hashing and both recompute
        // the same hash.
        bb.put_string(&gnitz_wire::migration::to_hex(&row.desired_state_canonical));
        bb.put_u64(row.format_version);
        bb.end_row();
        bb.finish()
    }

    // -- Swap batch construction ----------------------------------------

    /// Build the Phase 3 swap as a list of `(sys_table_id, Batch)`
    /// pairs in emission order. Order is load-bearing — see
    /// `schema-migrations.md` design doc for the rationale of each
    /// step.
    ///
    /// All `-1` rows carry the exact stored payload (cursor-seek +
    /// verbatim copy) so consolidation cancels them cleanly. `+1`
    /// rename rows are emitted under the SAME PK as their `-1`
    /// partner so the hook's rename fast-path detects them.
    pub(crate) fn build_swap_batches(
        &mut self,
        cascade: &Cascade,
    ) -> Result<Vec<(i64, Batch)>, String> {
        let mut out: Vec<(i64, Batch)> = Vec::new();

        // 1. SYS_INDICES: drops + index renames
        {
            let schema = idx_tab_schema();
            let mut batch = Batch::with_schema(schema, 8);
            for &idx_id in &cascade.dropped_index_ids {
                let r = retract_single_row(&mut self.sys_indices, &schema, idx_id as u64, 0);
                batch.append_batch(&r, 0, r.count);
            }
            for rn in &cascade.renames {
                if rn.kind != RenameKind::Index { continue; }
                let id = rn.id;
                // -1: verbatim stored row
                let r = retract_single_row(&mut self.sys_indices, &schema, id as u64, 0);
                batch.append_batch(&r, 0, r.count);
                // +1: build with new name, copy every other column from storage
                self.append_index_rename_plus(&mut batch, id, &rn.final_name)?;
            }
            if batch.count > 0 { out.push((IDX_TAB_ID, batch)); }
        }

        // 2. SYS_COLUMNS: -1 for every column of every dropped table AND view
        {
            let schema = col_tab_schema();
            let mut batch = Batch::with_schema(schema, 8);
            for &tid in &cascade.dropped_table_ids {
                let r = self.build_retract_column_records(tid, OWNER_KIND_TABLE)?;
                batch.append_batch(&r, 0, r.count);
            }
            for &vid in &cascade.dropped_view_ids {
                let r = self.build_retract_column_records(vid, OWNER_KIND_VIEW)?;
                batch.append_batch(&r, 0, r.count);
            }
            if batch.count > 0 { out.push((COL_TAB_ID, batch)); }
        }

        // 3. SYS_TABLES: drops + table renames
        {
            let schema = table_tab_schema();
            let mut batch = Batch::with_schema(schema, 8);
            for &tid in &cascade.dropped_table_ids {
                let r = retract_single_row(&mut self.sys_tables, &schema, tid as u64, 0);
                batch.append_batch(&r, 0, r.count);
            }
            for rn in &cascade.renames {
                if rn.kind != RenameKind::Table { continue; }
                let id = rn.id;
                let r = retract_single_row(&mut self.sys_tables, &schema, id as u64, 0);
                batch.append_batch(&r, 0, r.count);
                self.append_table_rename_plus(&mut batch, id, &rn.final_name)?;
            }
            if batch.count > 0 { out.push((TABLE_TAB_ID, batch)); }
        }

        // 4. SYS_VIEWS: drops + view renames (rewriting name + sql_definition)
        {
            let schema = view_tab_schema();
            let mut batch = Batch::with_schema(schema, 8);
            for &vid in &cascade.dropped_view_ids {
                let r = retract_single_row(&mut self.sys_views, &schema, vid as u64, 0);
                batch.append_batch(&r, 0, r.count);
            }
            for rn in &cascade.renames {
                if rn.kind != RenameKind::View { continue; }
                let id = rn.id;
                let r = retract_single_row(&mut self.sys_views, &schema, id as u64, 0);
                batch.append_batch(&r, 0, r.count);
                let final_sql = rn.final_sql.as_deref().unwrap_or("");
                self.append_view_rename_plus(&mut batch, id, &rn.final_name, final_sql)?;
            }
            if batch.count > 0 { out.push((VIEW_TAB_ID, batch)); }
        }

        // 5. SYS_VIEW_DEPS: -1 per dropped view
        {
            let schema = dep_tab_schema();
            let mut batch = Batch::with_schema(schema, 8);
            for &vid in &cascade.dropped_view_ids {
                let r = retract_rows_by_pk_hi(&mut self.sys_view_deps, &schema, vid as u64);
                batch.append_batch(&r, 0, r.count);
            }
            if batch.count > 0 { out.push((DEP_TAB_ID, batch)); }
        }

        // 6-10. Five SYS_CIRCUIT_* tables: -1 per dropped view
        for &(tab_id, sys_tab_ptr_fn) in &[
            (CIRCUIT_NODES_TAB_ID,      sys_circuit_nodes_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_EDGES_TAB_ID,      sys_circuit_edges_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_SOURCES_TAB_ID,    sys_circuit_sources_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_PARAMS_TAB_ID,     sys_circuit_params_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_GROUP_COLS_TAB_ID, sys_circuit_group_cols_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
        ] {
            let schema = sys_tab_schema(tab_id);
            let mut batch = Batch::with_schema(schema, 8);
            let tab_ptr = sys_tab_ptr_fn(self);
            for &vid in &cascade.dropped_view_ids {
                let r = retract_rows_by_pk_hi(unsafe { &mut *tab_ptr }, &schema, vid as u64);
                batch.append_batch(&r, 0, r.count);
            }
            if batch.count > 0 { out.push((tab_id, batch)); }
        }

        Ok(out)
    }

    // -- Rename +1 row builders --------------------------------------

    fn append_table_rename_plus(
        &mut self, batch: &mut Batch, tid: i64, final_name: &str,
    ) -> Result<(), String> {
        let schema = table_tab_schema();
        let stored = self.read_stored_table_row(tid)?;
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(tid as u64, 0, 1);
        bb.put_u64(stored.schema_id as u64);   // schema_id (verbatim)
        bb.put_string(final_name);              // name (OVERWRITE)
        bb.put_string(&stored.directory);       // directory (verbatim — keeps __mig_ path)
        bb.put_u64(stored.pk_col_idx as u64);   // pk_col_idx (verbatim)
        bb.put_u64(stored.created_lsn);         // created_lsn (verbatim)
        bb.put_u64(stored.flags);               // flags (verbatim)
        bb.end_row();
        let tmp = bb.finish();
        batch.append_batch(&tmp, 0, tmp.count);
        Ok(())
    }

    fn append_view_rename_plus(
        &mut self, batch: &mut Batch, vid: i64, final_name: &str, final_sql: &str,
    ) -> Result<(), String> {
        let schema = view_tab_schema();
        let stored = self.read_stored_view_row(vid)?;
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(vid as u64, 0, 1);
        bb.put_u64(stored.schema_id as u64);      // schema_id (verbatim)
        bb.put_string(final_name);                  // name (OVERWRITE)
        bb.put_string(final_sql);                   // sql_definition (OVERWRITE)
        bb.put_string(&stored.cache_directory);     // cache_directory (verbatim)
        bb.put_u64(stored.created_lsn);             // created_lsn (verbatim)
        bb.end_row();
        let tmp = bb.finish();
        batch.append_batch(&tmp, 0, tmp.count);
        Ok(())
    }

    fn append_index_rename_plus(
        &mut self, batch: &mut Batch, idx_id: i64, final_name: &str,
    ) -> Result<(), String> {
        let schema = idx_tab_schema();
        let stored = self.read_stored_index_row(idx_id)?;
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(idx_id as u64, 0, 1);
        bb.put_u64(stored.owner_id as u64);       // owner_id (verbatim)
        bb.put_u64(stored.owner_kind as u64);     // owner_kind (verbatim)
        bb.put_u64(stored.source_col_idx as u64); // source_col_idx (verbatim)
        bb.put_string(final_name);                  // name (OVERWRITE)
        bb.put_u64(if stored.is_unique { 1 } else { 0 }); // is_unique (verbatim)
        bb.put_string(&stored.cache_directory);     // cache_directory (verbatim)
        bb.end_row();
        let tmp = bb.finish();
        batch.append_batch(&tmp, 0, tmp.count);
        Ok(())
    }

    // -- Stored-row readers -----------------------------------------------

    fn read_stored_table_row(&mut self, tid: i64) -> Result<StoredTableRow, String> {
        let mut cursor = self.sys_tables.create_cursor()
            .map_err(|e| format!("sys_tables cursor error: {}", e))?;
        cursor.cursor.seek(crate::util::make_pk(tid as u64, 0));
        if !cursor.cursor.valid || cursor.cursor.current_key_lo != tid as u64 {
            return Err(format!("sys_tables: no row for tid={}", tid));
        }
        if cursor.cursor.current_weight <= 0 {
            return Err(format!("sys_tables: tid={} is retracted", tid));
        }
        Ok(StoredTableRow {
            schema_id:   cursor_read_u64(&cursor, TABLETAB_COL_SCHEMA_ID) as i64,
            directory:   cursor_read_string(&cursor, TABLETAB_COL_DIRECTORY),
            pk_col_idx:  cursor_read_u64(&cursor, TABLETAB_COL_PK_COL_IDX) as u32,
            created_lsn: cursor_read_u64(&cursor, TABLETAB_COL_CREATED_LSN),
            flags:       cursor_read_u64(&cursor, TABLETAB_COL_FLAGS),
        })
    }

    fn read_stored_view_row(&mut self, vid: i64) -> Result<StoredViewRow, String> {
        let mut cursor = self.sys_views.create_cursor()
            .map_err(|e| format!("sys_views cursor error: {}", e))?;
        cursor.cursor.seek(crate::util::make_pk(vid as u64, 0));
        if !cursor.cursor.valid || cursor.cursor.current_key_lo != vid as u64 {
            return Err(format!("sys_views: no row for vid={}", vid));
        }
        if cursor.cursor.current_weight <= 0 {
            return Err(format!("sys_views: vid={} is retracted", vid));
        }
        Ok(StoredViewRow {
            schema_id:        cursor_read_u64(&cursor, VIEWTAB_COL_SCHEMA_ID) as i64,
            cache_directory:  cursor_read_string(&cursor, VIEWTAB_COL_CACHE_DIRECTORY),
            created_lsn:      cursor_read_u64(&cursor, VIEWTAB_COL_CREATED_LSN),
        })
    }

    fn read_stored_index_row(&mut self, idx_id: i64) -> Result<StoredIndexRow, String> {
        let mut cursor = self.sys_indices.create_cursor()
            .map_err(|e| format!("sys_indices cursor error: {}", e))?;
        cursor.cursor.seek(crate::util::make_pk(idx_id as u64, 0));
        if !cursor.cursor.valid || cursor.cursor.current_key_lo != idx_id as u64 {
            return Err(format!("sys_indices: no row for idx_id={}", idx_id));
        }
        if cursor.cursor.current_weight <= 0 {
            return Err(format!("sys_indices: idx_id={} is retracted", idx_id));
        }
        Ok(StoredIndexRow {
            owner_id:        cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64,
            owner_kind:      cursor_read_u64(&cursor, IDXTAB_COL_OWNER_KIND) as i64,
            source_col_idx:  cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COL_IDX) as u32,
            is_unique:       cursor_read_u64(&cursor, IDXTAB_COL_IS_UNIQUE) != 0,
            cache_directory: cursor_read_string(&cursor, IDXTAB_COL_CACHE_DIRECTORY),
        })
    }
}

struct StoredTableRow {
    schema_id: i64,
    directory: String,
    pk_col_idx: u32,
    created_lsn: u64,
    flags: u64,
}

struct StoredViewRow {
    schema_id: i64,
    cache_directory: String,
    created_lsn: u64,
}

struct StoredIndexRow {
    owner_id: i64,
    owner_kind: i64,
    source_col_idx: u32,
    is_unique: bool,
    cache_directory: String,
}

// --- Table pointer helpers for SYS_CIRCUIT_* loops ------------------------

fn sys_circuit_nodes_ptr(c: &mut CatalogEngine) -> *mut crate::storage::Table {
    &mut *c.sys_circuit_nodes as *mut _
}
fn sys_circuit_edges_ptr(c: &mut CatalogEngine) -> *mut crate::storage::Table {
    &mut *c.sys_circuit_edges as *mut _
}
fn sys_circuit_sources_ptr(c: &mut CatalogEngine) -> *mut crate::storage::Table {
    &mut *c.sys_circuit_sources as *mut _
}
fn sys_circuit_params_ptr(c: &mut CatalogEngine) -> *mut crate::storage::Table {
    &mut *c.sys_circuit_params as *mut _
}
fn sys_circuit_group_cols_ptr(c: &mut CatalogEngine) -> *mut crate::storage::Table {
    &mut *c.sys_circuit_group_cols as *mut _
}

// ---------------------------------------------------------------------------
// Direct create/drop path (no __mig_ staging)
// ---------------------------------------------------------------------------
//
// Currently applies creates/drops directly without an intermediate
// staging phase. Correct under migration_in_flight because no concurrent
// DDL can interleave — the full create+drop batch set is broadcast
// atomically under one sal_writer_excl + fsync. The staging pattern
// (with __mig_<hash>_ names and rename fast-path, above) only becomes
// necessary once backfill lands, which needs isolation for transformed
// data before cutover.
//
// These helpers construct the sys_* batches that apply_migration
// ingests locally (master WAL) and broadcasts via SAL to workers.

use gnitz_wire::migration::{
    ColumnDef as AstColumnDef, IndexDef as AstIndexDef, TableDef as AstTableDef,
    ViewDef as AstViewDef,
};
use super::{pack_column_id, pack_dep_pk, pack_node_pk, pack_edge_pk, pack_param_pk, pack_gcol_pk};

impl CatalogEngine {
    /// Allocate tid and build the sys_columns +1 batch (one row per
    /// column) followed by the sys_tables +1 batch.
    ///
    /// Pairs must emit in the returned order: columns first, then
    /// table row — `on_table_delta` reads freshly-ingested sys_columns
    /// to build the schema.
    pub(crate) fn build_create_table_batches(
        &mut self, t: &AstTableDef,
    ) -> Result<(i64, Vec<(i64, Batch)>), String> {
        let tid = self.allocate_table_id();
        let sid = self.get_schema_id(&t.schema);
        if sid < 0 {
            return Err(format!("schema does not exist: {}", t.schema));
        }
        let directory = format!("{}/{}/{}_{}", self.base_dir, t.schema, t.name, tid);

        // 1. sys_columns batch
        let cols_schema = col_tab_schema();
        let mut cbb = BatchBuilder::new(cols_schema);
        for (i, c) in t.columns.iter().enumerate() {
            let pk = pack_column_id(tid, i as i64);
            cbb.begin_row(pk, 0, 1);
            cbb.put_u64(tid as u64);
            cbb.put_u64(OWNER_KIND_TABLE as u64);
            cbb.put_u64(i as u64);
            cbb.put_string(&c.name);
            cbb.put_u64(c.type_code as u64);
            cbb.put_u64(if c.is_nullable { 1 } else { 0 });
            // FK columns: look up fk_table_id by name
            let fk_tid = if c.fk_table.is_empty() {
                0
            } else {
                let q = format!("{}.{}", c.fk_schema, c.fk_table);
                self.name_to_id.get(&q).copied().unwrap_or(0)
            };
            cbb.put_u64(fk_tid as u64);
            cbb.put_u64(c.fk_col_idx as u64);
            cbb.end_row();
        }
        let cols_batch = cbb.finish();

        // 2. sys_tables batch
        let tab_schema = table_tab_schema();
        let mut tbb = BatchBuilder::new(tab_schema);
        tbb.begin_row(tid as u64, 0, 1);
        tbb.put_u64(sid as u64);
        tbb.put_string(&t.name);
        tbb.put_string(&directory);
        tbb.put_u64(t.pk_col_idx as u64);
        tbb.put_u64(0);                       // created_lsn placeholder
        let flags = if t.unique_pk { TABLETAB_FLAG_UNIQUE_PK } else { 0 };
        tbb.put_u64(flags);
        tbb.end_row();
        let table_batch = tbb.finish();

        Ok((tid, vec![
            (COL_TAB_ID, cols_batch),
            (TABLE_TAB_ID, table_batch),
        ]))
    }

    /// Allocate idx_id and build the sys_indices +1 batch. Worker's
    /// `on_index_delta` runs `backfill_index` against the owner table
    /// when the broadcast arrives, which is cheap when the owner was
    /// created in the same migration (empty table).
    pub(crate) fn build_create_index_batches(
        &mut self, idx: &AstIndexDef,
    ) -> Result<(i64, Vec<(i64, Batch)>), String> {
        let owner_qualified = format!("{}.{}", idx.owner_schema, idx.owner_name);
        let owner_id = *self.name_to_id.get(&owner_qualified)
            .ok_or_else(|| format!("index owner table not found: {}", owner_qualified))?;

        // Look up source_col_idx from owner's column set.
        let col_defs = self.read_column_defs(owner_id)?;
        let source_col_idx = col_defs.iter().position(|c| c.name == idx.source_col)
            .ok_or_else(|| format!(
                "index column {} not found in table {}", idx.source_col, owner_qualified,
            ))? as u32;

        let idx_id = self.allocate_index_id();
        let schema = idx_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(idx_id as u64, 0, 1);
        bb.put_u64(owner_id as u64);
        bb.put_u64(OWNER_KIND_TABLE as u64);
        bb.put_u64(source_col_idx as u64);
        bb.put_string(&idx.name);
        bb.put_u64(if idx.is_unique { 1 } else { 0 });
        bb.put_string("");                    // cache_directory
        bb.end_row();
        let batch = bb.finish();
        Ok((idx_id, vec![(IDX_TAB_ID, batch)]))
    }

    /// For a DROP TABLE: retract its sys_indices rows (cascade),
    /// sys_columns rows, then sys_tables row. Returns `(tid,
    /// batches, rmtree_dir)` — the tid lets callers invalidate
    /// any per-tid caches they hold.
    pub(crate) fn build_drop_table_batches(
        &mut self, schema_name: &str, table_name: &str,
    ) -> Result<(i64, Vec<(i64, Batch)>, Option<PathBuf>), String> {
        let q = format!("{}.{}", schema_name, table_name);
        let tid = *self.name_to_id.get(&q)
            .ok_or_else(|| format!("drop: table not found: {}", q))?;

        let mut out: Vec<(i64, Batch)> = Vec::new();

        // Cascade indices first.
        let idx_schema = idx_tab_schema();
        let mut idx_batch = Batch::with_schema(idx_schema, 4);
        let index_ids: Vec<i64> = {
            // Find every sys_indices row whose owner_id == tid.
            let mut ids: Vec<i64> = Vec::new();
            if let Ok(mut cursor) = self.sys_indices.create_cursor() {
                while cursor.cursor.valid {
                    if cursor.cursor.current_weight > 0 {
                        let oid = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64;
                        if oid == tid {
                            ids.push(cursor.cursor.current_key_lo as i64);
                        }
                    }
                    cursor.cursor.advance();
                }
            }
            ids
        };
        for idx_id in index_ids {
            let r = retract_single_row(&mut self.sys_indices, &idx_schema, idx_id as u64, 0);
            idx_batch.append_batch(&r, 0, r.count);
        }
        if idx_batch.count > 0 { out.push((IDX_TAB_ID, idx_batch)); }

        // Retract columns.
        let cols_retract = self.build_retract_column_records(tid, OWNER_KIND_TABLE)?;
        if cols_retract.count > 0 { out.push((COL_TAB_ID, cols_retract)); }

        // Retract the sys_tables row.
        let tab_schema = table_tab_schema();
        let tab_batch = retract_single_row(&mut self.sys_tables, &tab_schema, tid as u64, 0);
        if tab_batch.count == 0 {
            return Err(format!("drop: sys_tables has no row for tid={}", tid));
        }
        let dir = self.dag.tables.get(&tid).map(|e| PathBuf::from(&e.directory));
        out.push((TABLE_TAB_ID, tab_batch));

        Ok((tid, out, dir))
    }

    /// Allocate vid and build the full set of +1 batches for a
    /// CREATE VIEW: columns → view_deps → circuit (5 tables) → view
    /// row. sys_views is emitted LAST so `on_view_delta` fires only
    /// after all dependency rows are in place.
    pub(crate) fn build_create_view_batches(
        &mut self, v: &AstViewDef,
    ) -> Result<(i64, Vec<(i64, Batch)>), String> {
        let sid = self.get_schema_id(&v.schema);
        if sid < 0 {
            return Err(format!("schema does not exist: {}", v.schema));
        }

        let vid = self.allocate_table_id();
        let directory = format!("{}/{}/view_{}_{}", self.base_dir, v.schema, v.name, vid);

        let mut out: Vec<(i64, Batch)> = Vec::new();

        // 1. sys_columns +1 (one row per output column)
        {
            let schema = col_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            for (i, (name, tc)) in v.circuit.output_col_defs.iter().enumerate() {
                let pk = pack_column_id(vid, i as i64);
                bb.begin_row(pk, 0, 1);
                bb.put_u64(vid as u64);
                bb.put_u64(OWNER_KIND_VIEW as u64);
                bb.put_u64(i as u64);
                bb.put_string(name);
                bb.put_u64(*tc as u64);
                bb.put_u64(0); // is_nullable
                bb.put_u64(0); // fk_table_id
                bb.put_u64(0); // fk_col_idx
                bb.end_row();
            }
            let batch = bb.finish();
            if batch.count > 0 { out.push((COL_TAB_ID, batch)); }
        }

        // 2. sys_view_deps +1
        {
            let schema = dep_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            for &dep_tid in &v.circuit.dependencies {
                let (pk_lo, pk_hi) = pack_dep_pk(vid, dep_tid as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(vid as u64);
                bb.put_u64(0); // dep_view_id
                bb.put_u64(dep_tid);
                bb.end_row();
            }
            let batch = bb.finish();
            if batch.count > 0 { out.push((DEP_TAB_ID, batch)); }
        }

        // 3. sys_circuit_nodes +1
        {
            let schema = circuit_nodes_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, opcode) in &v.circuit.nodes {
                let (pk_lo, pk_hi) = pack_node_pk(vid, node_id as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(opcode);
                bb.end_row();
            }
            let batch = bb.finish();
            if batch.count > 0 { out.push((CIRCUIT_NODES_TAB_ID, batch)); }
        }

        // 4. sys_circuit_edges +1
        {
            let schema = circuit_edges_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(edge_id, src, dst, port) in &v.circuit.edges {
                let (pk_lo, pk_hi) = pack_edge_pk(vid, edge_id as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(src);
                bb.put_u64(dst);
                bb.put_u64(port);
                bb.end_row();
            }
            let batch = bb.finish();
            if batch.count > 0 { out.push((CIRCUIT_EDGES_TAB_ID, batch)); }
        }

        // 5. sys_circuit_sources +1
        {
            let schema = circuit_sources_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, table_id) in &v.circuit.sources {
                let (pk_lo, pk_hi) = pack_node_pk(vid, node_id as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(table_id);
                bb.end_row();
            }
            let batch = bb.finish();
            if batch.count > 0 { out.push((CIRCUIT_SOURCES_TAB_ID, batch)); }
        }

        // 6. sys_circuit_params +1
        {
            let schema = circuit_params_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, slot, value) in &v.circuit.params {
                let (pk_lo, pk_hi) = pack_param_pk(vid, node_id as i64, slot as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(value);
                bb.put_null(); // str_value
                bb.end_row();
            }
            let batch = bb.finish();
            if batch.count > 0 { out.push((CIRCUIT_PARAMS_TAB_ID, batch)); }
        }

        // 7. sys_circuit_group_cols +1
        {
            let schema = circuit_group_cols_schema();
            let mut bb = BatchBuilder::new(schema);
            for &(node_id, col_idx) in &v.circuit.group_cols {
                let (pk_lo, pk_hi) = pack_gcol_pk(vid, node_id as i64, col_idx as i64);
                bb.begin_row(pk_lo, pk_hi, 1);
                bb.put_u64(col_idx);
                bb.end_row();
            }
            let batch = bb.finish();
            if batch.count > 0 { out.push((CIRCUIT_GROUP_COLS_TAB_ID, batch)); }
        }

        // 8. sys_views +1 (LAST — triggers on_view_delta → register + ensure_compiled)
        {
            let schema = view_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(vid as u64, 0, 1);
            bb.put_u64(sid as u64);
            bb.put_string(&v.name);
            bb.put_string(&v.sql);
            bb.put_string(&directory);
            bb.put_u64(0); // created_lsn placeholder
            bb.end_row();
            let batch = bb.finish();
            out.push((VIEW_TAB_ID, batch));
        }

        Ok((vid, out))
    }

    /// Retract every sys_* row that `build_create_view_batches`
    /// emitted: circuit (5 tables) → view_deps → columns → view row.
    /// Cursor-seek payloads for byte-exact consolidation.
    pub(crate) fn build_drop_view_batches(
        &mut self, schema_name: &str, view_name: &str,
    ) -> Result<(i64, Vec<(i64, Batch)>, Option<PathBuf>), String> {
        let q = format!("{}.{}", schema_name, view_name);
        let vid = *self.name_to_id.get(&q)
            .ok_or_else(|| format!("drop: view not found: {}", q))?;

        let mut out: Vec<(i64, Batch)> = Vec::new();

        // 1. sys_circuit_* -1 per dropped view.
        for &(tab_id, ptr_fn) in &[
            (CIRCUIT_NODES_TAB_ID,      sys_circuit_nodes_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_EDGES_TAB_ID,      sys_circuit_edges_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_SOURCES_TAB_ID,    sys_circuit_sources_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_PARAMS_TAB_ID,     sys_circuit_params_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
            (CIRCUIT_GROUP_COLS_TAB_ID, sys_circuit_group_cols_ptr as fn(&mut CatalogEngine) -> *mut crate::storage::Table),
        ] {
            let schema = sys_tab_schema(tab_id);
            let tab_ptr = ptr_fn(self);
            let batch = retract_rows_by_pk_hi(unsafe { &mut *tab_ptr }, &schema, vid as u64);
            if batch.count > 0 { out.push((tab_id, batch)); }
        }

        // 2. sys_view_deps -1
        {
            let schema = dep_tab_schema();
            let batch = retract_rows_by_pk_hi(&mut self.sys_view_deps, &schema, vid as u64);
            if batch.count > 0 { out.push((DEP_TAB_ID, batch)); }
        }

        // 3. sys_columns -1
        let cols_retract = self.build_retract_column_records(vid, OWNER_KIND_VIEW)?;
        if cols_retract.count > 0 { out.push((COL_TAB_ID, cols_retract)); }

        // 4. sys_views -1 (last)
        let tab_batch = retract_single_row(&mut self.sys_views, &view_tab_schema(), vid as u64, 0);
        if tab_batch.count == 0 {
            return Err(format!("drop: sys_views has no row for vid={}", vid));
        }
        let dir = self.dag.tables.get(&vid).map(|e| PathBuf::from(&e.directory));
        out.push((VIEW_TAB_ID, tab_batch));

        Ok((vid, out, dir))
    }

    /// For a DROP INDEX: retract its sys_indices row.
    pub(crate) fn build_drop_index_batches(
        &mut self, index_name: &str,
    ) -> Result<Vec<(i64, Batch)>, String> {
        let idx_id = *self.index_by_name.get(index_name)
            .ok_or_else(|| format!("drop: index not found: {}", index_name))?;
        let schema = idx_tab_schema();
        let batch = retract_single_row(&mut self.sys_indices, &schema, idx_id as u64, 0);
        if batch.count == 0 {
            return Err(format!("drop: sys_indices has no row for idx_id={}", idx_id));
        }
        Ok(vec![(IDX_TAB_ID, batch)])
    }

    /// After applying any creates, emit a sys_sequences update so
    /// the durable HWM reflects master's `next_table_id` / `next_index_id`.
    pub(crate) fn build_sequence_hwm_batch(&mut self) -> Result<Batch, String> {
        let schema = seq_tab_schema();
        let mut batch = Batch::with_schema(schema, 4);
        for seq_id in [SEQ_ID_TABLES, SEQ_ID_INDICES] {
            let r = retract_single_row(&mut self.sys_sequences, &schema, seq_id as u64, 0);
            batch.append_batch(&r, 0, r.count);
            let new_val = match seq_id {
                SEQ_ID_TABLES => self.next_table_id - 1,
                SEQ_ID_INDICES => self.next_index_id - 1,
                _ => 0,
            };
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(seq_id as u64, 0, 1);
            bb.put_u64(new_val as u64);
            bb.end_row();
            let plus = bb.finish();
            batch.append_batch(&plus, 0, plus.count);
        }
        Ok(batch)
    }

    /// Phase-1 transitive drop-closure check.  For every live record in
    /// sys_view_deps, if the depended-upon object (table or view) is in
    /// `diff.dropped_*`, the dependent view must also be in `diff.dropped_views`.
    ///
    /// Transitivity is handled naturally: each intermediate dep record
    /// (V→W) is checked individually — if W is also being dropped then
    /// that record is OK, and the record (W→T) is checked separately.
    pub(crate) fn validate_drop_closure_diff(
        &mut self,
        diff: &gnitz_wire::migration::Diff,
    ) -> Result<(), String> {
        use std::collections::HashSet;

        if diff.dropped_tables.is_empty() && diff.dropped_views.is_empty() {
            return Ok(());
        }

        let mut dropped_ids: HashSet<i64> = HashSet::new();
        let mut dropped_view_ids: HashSet<i64> = HashSet::new();
        for t in &diff.dropped_tables {
            let q = format!("{}.{}", t.schema, t.name);
            if let Some(&id) = self.name_to_id.get(&q) {
                dropped_ids.insert(id);
            }
        }
        for v in &diff.dropped_views {
            let q = format!("{}.{}", v.schema, v.name);
            if let Some(&id) = self.name_to_id.get(&q) {
                dropped_ids.insert(id);
                dropped_view_ids.insert(id);
            }
        }

        if dropped_ids.is_empty() {
            return Ok(());
        }

        let Ok(mut cursor) = self.sys_view_deps.create_cursor() else {
            return Ok(());
        };

        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                let view_id = cursor_read_u64(&cursor, DEPTAB_COL_VIEW_ID) as i64;
                let dep_id  = cursor_read_u64(&cursor, DEPTAB_COL_DEP_TABLE_ID) as i64;
                if dropped_ids.contains(&dep_id) && !dropped_view_ids.contains(&view_id) {
                    let dep_name = self.id_to_qualified.get(&dep_id)
                        .map(|(s, n)| format!("{}.{}", s, n))
                        .unwrap_or_else(|| format!("id={}", dep_id));
                    let view_name = self.id_to_qualified.get(&view_id)
                        .map(|(s, n)| format!("{}.{}", s, n))
                        .unwrap_or_else(|| format!("id={}", view_id));
                    return Err(format!(
                        "cannot drop {}: still referenced by view {}",
                        dep_name, view_name,
                    ));
                }
            }
            cursor.cursor.advance();
        }

        Ok(())
    }

    /// Convenience used by apply_migration to surface the engine's
    /// current migration head to a caller that only has a `&CatalogEngine`.
    pub(crate) fn migration_head(&self) -> u128 { self.current_migration_hash }
}

// --- Unused-import silencer ------------------------------------------------

#[allow(dead_code)]
fn _unused_schema_descriptor(_: SchemaDescriptor) {}
#[allow(dead_code)]
fn _unused_copy_cursor_row_with_weight() {
    let _: fn(&_, &_, &mut _, _) = copy_cursor_row_with_weight;
}
#[allow(dead_code)]
fn _unused_ingest_batch_into(t: &mut crate::storage::Table, b: &Batch) {
    ingest_batch_into(t, b);
}
#[allow(dead_code)]
fn _unused_unused_ast_column_def(_: AstColumnDef) {}
