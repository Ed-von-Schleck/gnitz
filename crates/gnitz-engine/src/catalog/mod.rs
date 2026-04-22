#![allow(dead_code, unused_variables)]
//! Catalog engine: DDL operations, system table management, hook processing,
//! and entity registry.
//!
//! The CatalogEngine wraps DagEngine and adds:
//! - EntityRegistry (name → ID mapping, FK constraints, index tracking)
//! - System table definitions and bootstrap
//! - DDL intent (CREATE/DROP SCHEMA/TABLE/VIEW/INDEX)
//! - Hook processing (schema, table, view, index, dep effects)
//! - Catalog persistence and recovery
//!
//! # Hook model
//!
//! System-table writes flow through [`fire_hooks`](CatalogEngine::fire_hooks),
//! which dispatches two categories of handler per sys_table_id:
//!
//! * `apply_*` — pure cache-delta appliers. Each row's weight drives a
//!   HashMap/HashSet insert (weight > 0) or remove (weight < 0). Naturally
//!   handles retract+insert pairs because HashMap ops commute with the sign
//!   of the weight.
//! * `hook_*` — side-effectful, edge-triggered handlers that create
//!   directories, allocate store partitions, register DAG entries, or
//!   backfill derived state. These are NOT symmetric under retract+insert
//!   on the same row; the current DDL surface (CREATE is +1, DROP is -1)
//!   never produces that pattern, so edge-triggering is safe.
//!
//! See `hooks.rs` for the cross-sys-table ordering contract and where it's
//! enforced.

mod sys_tables;
mod types;
mod utils;
mod cache;
mod bootstrap;
mod hooks;
mod ddl;
mod validation;
mod store;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::rc::Rc;
use std::sync::Arc;

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::dag::{DagEngine, StoreHandle};
use crate::storage::{Batch, PartitionedTable, partition_arena_size, CursorHandle, Table};

// Re-export items used by other crate modules.
pub(crate) use sys_tables::{FIRST_USER_TABLE_ID, SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES};
pub(crate) use sys_tables::{SYSTEM_SCHEMA_ID, PUBLIC_SCHEMA_ID};

// Import everything from sys_tables for internal use.
use sys_tables::*;

// Re-export types needed by other modules.
pub(crate) use types::{ColumnDef, CircuitGraph, FkConstraint};
pub(crate) use utils::{BatchBuilder, validate_user_identifier, parse_qualified_name,
                       make_fk_index_name, make_secondary_index_name, ingest_batch_into,
                       flush_sys_table,
                       ensure_dir, fsync_dir,
                       get_index_key_type, make_index_schema,
                       cursor_read_u64, cursor_read_string,
                       copy_cursor_row_with_weight,
                       retract_single_row,
                       retract_rows_by_pk_hi,
                       retract_rows_by_pk_lo_range};
pub(crate) use cache::CatalogCacheSet;

// ---------------------------------------------------------------------------
// CatalogEngine
// ---------------------------------------------------------------------------

/// The catalog engine wraps DagEngine and manages the entity registry,
/// system tables, DDL operations, and hook processing.
pub struct CatalogEngine {
    pub(crate) dag: DagEngine,
    pub(crate) base_dir: String,

    // --- Unified cache set (replaces 11 ad-hoc HashMaps) ---
    pub(crate) caches: CatalogCacheSet,

    // --- Sequence counters ---
    pub(crate) next_schema_id: i64,
    pub(crate) next_table_id: i64,
    pub(crate) next_index_id: i64,
    pub(crate) active_part_start: u32,
    pub(crate) active_part_end: u32,

    // --- System tables (owned, single-partition, durable) ---
    pub(crate) sys_schemas: Box<Table>,
    pub(crate) sys_tables: Box<Table>,
    pub(crate) sys_views: Box<Table>,
    pub(crate) sys_columns: Box<Table>,
    pub(crate) sys_indices: Box<Table>,
    pub(crate) sys_view_deps: Box<Table>,
    pub(crate) sys_sequences: Box<Table>,
    pub(crate) sys_circuit_nodes: Box<Table>,
    pub(crate) sys_circuit_edges: Box<Table>,
    pub(crate) sys_circuit_sources: Box<Table>,
    pub(crate) sys_circuit_params: Box<Table>,
    pub(crate) sys_circuit_group_cols: Box<Table>,

    // --- Pending broadcasts (ordered innermost → outermost) ---
    //
    // System-table DDL goes through `ingest_to_family` → `fire_hooks`. Hooks
    // may recursively call `ingest_to_family` to cascade retractions (indices,
    // columns, circuit graph, view deps). Each nested call appends its
    // (table_id, batch) AFTER its own hooks fire, so this queue ends up in
    // dependency-safe order: children before parents. The executor drains it
    // once per top-level DDL and relays each entry to workers. Workers never
    // drain this (no broadcast channel), so it stays empty there.
    pub(crate) pending_broadcasts: Vec<(i64, Batch)>,
}

// SAFETY: CatalogEngine is only accessed from a single thread.
unsafe impl Send for CatalogEngine {}
