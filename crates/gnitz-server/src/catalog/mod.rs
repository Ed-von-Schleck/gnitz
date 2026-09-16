//! Catalog engine: the entity registry, the system tables and their bootstrap,
//! DDL application, hook processing, and catalog recovery — over the two
//! siblings `CatalogEngine` holds, `RelationRegistry` and `DagEngine`.
//!
//! # Hook model
//!
//! Every system-table write flows through
//! [`fire_hooks`](CatalogEngine::fire_hooks), which dispatches a static
//! per-family sequence of two kinds of handler over a sign-partitioned view of
//! the batch (all retractions before all insertions, so a rename's `-1,+1` pair
//! on one PK applies in that order):
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
mod hooks;
mod index_backfill;
mod metadata;
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
use gnitz_store::relation::{
    OnRegister, Relation, RelationKind, RelationRegistry, RelationSpec, SecondaryIndex, StoreConfig, ViewBudgets,
};
use gnitz_store::schema::{Placement, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, ReadCursor, Slot, StoreError};

// ── Crate-wide facade — items with genuine out-of-catalog consumers ──────────
// The DDL_TXN driver's bundle decoders: it resolves each family once, carries
// the value, and reads back what the bundle created or dropped.
pub(crate) use sys_tables::{family_pk_partition, idx_tab_partition, PkPartition};
pub(crate) use sys_tables::{SysFamily, FIRST_USER_TABLE_ID, PUBLIC_SCHEMA_ID};
pub(crate) use types::{ColumnDef, FkEdge};
// The reply path's schema-wire-block encoders. The `SchemaWireEntry` they fill
// is named only inside the catalog — the reply path takes one by value from
// `schema_wire_entry` and reads its fields — so it is re-exported below.
pub(crate) use schema_block::encode_schema_block;

// Import everything from sys_tables for internal use.
use precheck::{check_col_defs, validate_pk_against_cols};
use registry::build_schema_from_col_defs;
use sys_tables::*;

// ── Catalog-internal re-exports — no out-of-catalog consumer (W8). These reach
//    the submodules through their `use super::*` glob, so they stay re-exported
//    but scoped to the catalog subtree rather than the crate-wide surface. ─────
pub(in crate::catalog) use cache::{CatalogCacheSet, SchemaWireEntry};
pub(in crate::catalog) use gnitz_wire::validate_user_identifier;
pub(in crate::catalog) use registry::raise_id_counter;
// The child-directory grammar and the directory primitives are storage's; the
// catalog only consumes them.
#[cfg(test)]
pub(in crate::catalog) use gnitz_store::storage::ChildAddr;
pub(in crate::catalog) use utils::{pair_opk, preflight_dir, sys_opk};
// The relation rung's directory primitives; the catalog only consumes them.
pub(in crate::catalog) use gnitz_store::relation::{
    lock_data_dir, relation_dir, relations_dir, staged_dir, DIR_LOCK_RETRY_FOR,
};
// `BatchBuilder` holds no catalog state and lives in `storage`; re-export it
// for the catalog's row builders.
pub(in crate::catalog) use gnitz_store::storage::BatchBuilder;
// The generic payload-cell readers every system-row decoder in this subsystem
// reads a cell through, whatever the row's source.
pub(in crate::catalog) use gnitz_store::storage::{payload_string, payload_u64};

// ---------------------------------------------------------------------------
// CatalogEngine
// ---------------------------------------------------------------------------

/// The catalog engine wraps the relation registry and the DAG engine and
/// manages the system tables, DDL operations, and hook processing.
pub(crate) struct CatalogEngine {
    /// Which relations exist, and the stores behind them. A **sibling** of
    /// [`DagEngine`], not a field of it: a mirror drives one with no compiler
    /// and no VM at all, which is what the split exists for.
    pub(in crate::catalog) registry: RelationRegistry,
    pub(in crate::catalog) dag: DagEngine,
    pub(in crate::catalog) base_dir: String,

    /// The `flock`ed handle on `base_dir`'s lock file, dropped with the engine —
    /// at process teardown, or by `close` in the crash-semantics tests. Dropping
    /// it releases the lock, so it is held until then: a second writer would
    /// reseed `current_lsn` from `max_lsn + 1` and mint shard names the first
    /// writer is already using.
    _dir_lock: fs::File,

    /// Every derived lookup the catalog maintains from system-table deltas.
    pub(in crate::catalog) caches: CatalogCacheSet,

    /// True in the master process, whose index copies stay permanently empty:
    /// its index hook skips the backfill the workers run slice-local. Set by
    /// `open_master`, cleared by `become_worker`.
    pub(in crate::catalog) is_master: bool,

    // --- Sequence counters ---
    pub(in crate::catalog) next_schema_id: i64,
    pub(in crate::catalog) next_table_id: i64,
    pub(in crate::catalog) next_index_id: i64,
    /// User-table SERIAL sequences: `seq_id` (== table_id) → high-water (last id
    /// handed out). Next id = high_water + 1. Populated at recovery from the
    /// flushed `sys_sequences` shard and newer SAL advances, and durably advanced
    /// per range reservation. Distinct from the scalar catalog counters above
    /// because a user sequence's values live in worker-owned rows the master
    /// cannot re-derive.
    pub(in crate::catalog) user_sequences: std::collections::HashMap<i64, i64>,

    /// The checkpoint generation durably recorded in `SEQ_ID_CHECKPOINT_GEN` —
    /// what the next `advance_sequence` must retract, and the floor the next
    /// bump raises. Recovered at boot (0 on a fresh DB).
    pub(in crate::catalog) durable_generation: u64,
    /// The topology word durably recorded in `SEQ_ID_TOPOLOGY`, `0` on a fresh DB
    /// — which no real word can equal. Half of every resume verdict.
    pub(in crate::catalog) recorded_topology: u64,
    /// View ids whose checkpointed state — output stores and operator traces
    /// alike — was rejected at boot (generation mismatch, topology change, or a
    /// transitively-invalid source view) and must
    /// be reset-and-rebuilt rather than resumed. Computed pre-fork by
    /// `compute_invalid_views`, COW-inherited by every worker, and consumed by
    /// the master's boot rebuild sweep and the per-worker output reset. Empty on
    /// a clean same-topology restart (every view resumes).
    pub(in crate::catalog) invalid_views: rustc_hash::FxHashSet<i64>,

    /// The master's applied families in apply order, each queued before its ingest:
    /// the zone's broadcast and the undo log `compensate_stage_a` replays.
    /// `ddl_sync` never enqueues.
    pub(in crate::catalog) pending_broadcasts: Vec<(SysFamily, Batch)>,
}
