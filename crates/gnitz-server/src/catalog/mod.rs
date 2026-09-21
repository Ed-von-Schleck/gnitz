//! Catalog engine: the entity registry, the system tables and their bootstrap,
//! DDL application, hook processing, and catalog recovery — over the two
//! siblings `CatalogEngine` holds, `RelationRegistry` and `DagEngine`.
//!
//! # Hook model
//!
//! Every system-table write flows through
//! [`fire_hooks`](CatalogEngine::fire_hooks), which dispatches a static
//! per-family sequence of two kinds of handler over the batch:
//!
//! * `apply_*` — pure cache-delta appliers.
//! * `hook_*` — side effects: directories, stores, DAG registrations, derived
//!   state. They write no system rows.
//!
//! See `hooks.rs` for the cross-family ordering contract and where it is
//! enforced.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! Tests no single module owns live in `suites/`, a declared `mod suites;` child
//! of this module: they reach this subsystem's surface, not any one module's
//! private items.

mod bootstrap;
mod cache;
mod constraints;
mod hooks;
mod precheck;
mod registry;
mod schema_block;
mod sys_tables;
mod types;
mod utils;
mod view_state;
mod write_path;

#[cfg(test)]
mod suites;

use std::fs;
use std::rc::Rc;

use crate::query::DagEngine;
use gnitz_store::relation::{Relation, RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_store::schema::{Placement, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, ReadCursor, StoreError, StoredRow};
use gnitz_wire::ViewProps;

// ── Crate-wide facade — items with genuine out-of-catalog consumers ──────────
// The DDL_TXN driver's bundle decoders: it resolves each family once, carries
// the value, and reads back what the bundle created or dropped.
pub(crate) use bootstrap::UnreplayedCatalog;
pub(crate) use constraints::RowConstraints;
#[cfg(test)]
pub(crate) use sys_tables::write_col_tab_rows;
#[cfg(test)]
pub(crate) use sys_tables::PUBLIC_SCHEMA_ID;
pub(crate) use sys_tables::{family_pk_partition, idx_tab_partition, PkPartition};
pub(crate) use sys_tables::{SysFamily, FIRST_USER_TABLE_ID};
pub(crate) use types::{ColumnDef, FkEdge};
// The anonymous schema-wire-block encoder, for a schema no catalog entry describes.
pub(crate) use schema_block::encode_schema_block;

// Import everything from sys_tables for internal use.
use precheck::build_schema_from_col_defs;
use sys_tables::*;

// ── Catalog-internal re-exports — no out-of-catalog consumer (W8). These reach
//    the submodules through their `use super::*` glob, so they stay re-exported
//    but scoped to the catalog subtree rather than the crate-wide surface. ─────
pub(in crate::catalog) use cache::CatalogCacheSet;
pub(in crate::catalog) use gnitz_wire::validate_user_identifier;
// The child-directory grammar and the directory primitives are storage's; the
// catalog only consumes them.
#[cfg(test)]
pub(in crate::catalog) use gnitz_store::storage::ChildAddr;
// The relation rung's directory primitives; the catalog only consumes them.
#[cfg(test)]
pub(in crate::catalog) use gnitz_store::relation::relations_dir;
pub(in crate::catalog) use gnitz_store::relation::{lock_data_dir, relation_dir, DIR_LOCK_RETRY_FOR};
// `BatchBuilder` holds no catalog state and lives in `storage`; re-export it
// for the catalog's row builders.
pub(in crate::catalog) use gnitz_store::storage::BatchBuilder;
// The generic payload-cell readers every system-row decoder in this subsystem
// reads a cell through, whatever the row's source.
pub(in crate::catalog) use gnitz_expr::{payload_string, payload_u64};

// ---------------------------------------------------------------------------
// CatalogEngine
// ---------------------------------------------------------------------------

/// The catalog engine wraps the relation registry and the DAG engine and
/// manages the system tables, DDL operations, and hook processing.
pub(crate) struct CatalogEngine {
    /// Which relations exist, and the stores behind them. A **sibling** of
    /// [`DagEngine`], not a field of it: a mirror drives one with no compiler
    /// and no VM at all, which is what the split exists for.
    pub(crate) registry: RelationRegistry,
    pub(crate) dag: DagEngine,
    pub(in crate::catalog) base_dir: String,

    /// The `flock` keeping a second writer off `base_dir`. Declared after
    /// `registry`, so it is released only once every store is closed.
    _dir_lock: fs::File,

    /// Every derived lookup the catalog maintains from system-table deltas and
    /// relation registrations.
    pub(in crate::catalog) caches: CatalogCacheSet,

    /// The next catalog object id (schema, relation or index) `allocate_ids`
    /// hands out. Every applied id-bearing row raises it past its own id.
    pub(in crate::catalog) next_id: i64,
    /// The master's applied families in apply order, each queued before its ingest:
    /// the zone's broadcast and the undo log `compensate_stage_a` replays.
    /// `ddl_sync` never enqueues.
    pub(in crate::catalog) pending_broadcasts: Vec<(SysFamily, Batch)>,
}
