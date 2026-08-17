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
//! * `apply_*` — pure cache-delta appliers, run over a sign-partitioned view of
//!   the batch (all retractions before all insertions) so a rewrite pair
//!   (a rename's -1,+1 on one PK) applies its retraction before its insertion.
//! * `hook_*` — side-effectful handlers that create directories, allocate store
//!   stores, register DAG entries, or backfill derived state. Storage is
//!   applied before hooks fire, so the register/cascade hooks reconcile against
//!   the row's *net* live state (`advance_to_exact_live`) rather than its own sign — a
//!   rename pair (net-live before and after) fires neither teardown nor
//!   re-registration, in any row order, on every application path.
//!
//! See `hooks.rs` for the cross-sys-table ordering contract and where it's
//! enforced.

mod apply_context;
mod bootstrap;
mod cache;
mod ddl;
mod hooks;
mod metadata;
mod registry;
mod scan_spec;
mod store_io;
mod store_lsn;
mod sys_tables;
mod types;
mod utils;
mod validation;
mod write_path;

#[cfg(test)]
mod tests;

use std::fs;
use std::rc::Rc;

use crate::query::{DagEngine, RelationKind, StoreHandle};
use crate::schema::{Placement, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, ReadCursor, RecoverySource, Table};

// ── Crate-wide facade — items with genuine out-of-catalog consumers ──────────
// The DDL_TXN driver's bundle decoders: it resolves each family once, carries
// the value, and reads back what the bundle created or dropped.
pub(crate) use sys_tables::{family_pks_by_sign, idx_tab_drops, idx_tab_unique_creates};
pub(crate) use sys_tables::{SysFamily, FIRST_USER_TABLE_ID, SEQ_TAB_ID};
pub(crate) use types::{ColumnDef, FkEdge};
// The reply path's cached schema wire block.
pub(crate) use cache::SchemaWireEntry;
// The master's ScanSpec confinement test.
pub(crate) use scan_spec::scan_spec_worker;
// The fixed system-table schema for a family tid, for callers holding a raw
// id. Anything holding an untrusted id resolves it through `SysFamily::from_id`
// and reads `SysFamily::schema` instead — this panics on a non-family id.
pub(crate) use sys_tables::sys_tab_schema;

// Import everything from sys_tables for internal use.
use registry::build_schema_from_col_defs;
use sys_tables::*;

// ── Catalog-internal re-exports — no out-of-catalog consumer (W8). These reach
//    the submodules through their `use super::*` glob, so they stay re-exported
//    but scoped to the catalog subtree rather than the crate-wide surface. ─────
pub(in crate::catalog) use apply_context::ApplyContext;
pub(in crate::catalog) use cache::CatalogCacheSet;
#[cfg(test)]
pub(in crate::catalog) use gnitz_wire::validate_user_identifier;
pub(in crate::catalog) use gnitz_wire::FK_INDEX_INFIX;
pub(in crate::catalog) use registry::raise_id_counter;
pub(in crate::catalog) use sys_tables::{PUBLIC_SCHEMA_ID, SYSTEM_SCHEMA_ID};
// The child-directory grammar is owned by storage; the catalog only consumes it.
pub(in crate::catalog) use crate::storage::{subdir_names, ChildAddr};
pub(in crate::catalog) use utils::{
    cursor_read_string, cursor_read_u64, ensure_dir, fsync_dir, index_dir, is_index_dir_name, is_table_dir_name,
    make_fk_index_name, preflight_dir, reclaim_retired_children, relation_dir, retract_key_range, retract_single_row,
    schema_dir, sys_catalog_dir, sys_family_dir, sys_opk,
};
#[cfg(test)]
pub(in crate::catalog) use utils::{make_secondary_index_name, parse_qualified_name};
pub(in crate::catalog) use write_path::CatalogDeltaSink;
// `BatchBuilder` holds no catalog state and lives in `storage`; re-export it
// for catalog's ddl/bootstrap/store callers.
pub(crate) use crate::storage::BatchBuilder;

// ---------------------------------------------------------------------------
// CatalogEngine
// ---------------------------------------------------------------------------

/// Rows per `drain_chunk` call on a DDL scan (index/view backfill, unique
/// pre-flight). Bounds peak backfill memory at O(chunk × row_width).
pub(crate) const DDL_SCAN_CHUNK_ROWS: usize = 65_536;

/// The catalog engine wraps DagEngine and manages the entity registry,
/// system tables, DDL operations, and hook processing.
pub struct CatalogEngine {
    pub(crate) dag: DagEngine,
    pub(crate) base_dir: String,

    /// Every derived lookup the catalog maintains from system-table deltas.
    pub(crate) caches: CatalogCacheSet,

    // --- Sequence counters ---
    pub(crate) next_schema_id: i64,
    pub(crate) next_table_id: i64,
    pub(crate) next_index_id: i64,
    /// User-table SERIAL sequences: `seq_id` (== table_id) → high-water (last id
    /// handed out). Next id = high_water + 1. Populated at recovery from the
    /// flushed `sys_sequences` shard and newer SAL advances, and durably advanced
    /// per range reservation. Distinct from the scalar catalog counters above
    /// because a user sequence's values live in worker-owned rows the master
    /// cannot re-derive.
    pub(crate) user_sequences: std::collections::HashMap<i64, i64>,

    /// The launched worker count. Threaded in from `run_server` rather than read
    /// off `worker_ctx`, which is 1 in the master process (`set_worker_rank` runs
    /// only post-fork): a store built from the ambient value would be named
    /// `w0of1` while the boot repartition wrote `w0of{W}..`, the child-dir sweep
    /// would delete it as unowned, and worker 0 would inherit a deleted directory.
    pub(crate) num_workers: u32,
    /// True while this process owns its relations' stores. The post-fork master
    /// detaches every user relation and stays inert, so anything reading local
    /// base data must check this first.
    pub(crate) owns_stores: bool,

    /// The checkpoint generation durably recorded in `SEQ_ID_CHECKPOINT_GEN` —
    /// what the next `advance_sequence` must retract, and the floor the next
    /// bump raises. Recovered at boot (0 on a fresh DB).
    pub(crate) durable_generation: u64,
    /// The generation a manifest must carry to be resumed from. Equal to
    /// `durable_generation` except across `recovery_start_generation_bump`,
    /// which pushes that one to `G+1` while what a boot may resume from stays
    /// the recovered `G`. Written only by `set_resume_generation`, which mirrors
    /// it into `worker_ctx` for the `Table::new` callers holding no catalog.
    pub(crate) resume_generation: u64,
    /// The topology row last recorded: `(worker_count as u64) << 32 | STATE_FORMAT`.
    /// Recovered from `SEQ_ID_TOPOLOGY` at boot (0 on a fresh DB); commit 3
    /// compares it against the launched worker count + `STATE_FORMAT` to decide
    /// whether persisted view state is reloadable.
    pub(crate) recorded_topology: u64,
    /// View ids whose checkpointed output state was rejected at boot (generation
    /// mismatch, topology change, or a transitively-invalid source view) and must
    /// be reset-and-rebuilt rather than resumed. Computed pre-fork by
    /// `compute_invalid_views`, COW-inherited by every worker, and consumed by
    /// the master's boot rebuild sweep and the per-worker output reset. Empty on
    /// a clean same-topology restart (every view resumes).
    pub(crate) invalid_views: rustc_hash::FxHashSet<i64>,

    // --- System tables (owned, one `Table` each, durable) ---
    //
    // One store per family, indexed by `SysFamily` discriminant (parallel to
    // `SYS_FAMILIES`). The `Box` keeps each table's heap address stable, so the
    // `Borrowed(*mut Table)` DAG registrations survive engine moves.
    pub(crate) sys_stores: [Box<Table>; SysFamily::COUNT],

    // --- Pending broadcasts (ordered innermost → outermost) ---
    //
    // System-table DDL goes through `ingest_to_family` → `fire_hooks`. Hooks
    // may recursively call `ingest_to_family` to cascade retractions (indices,
    // columns, circuit graph, view deps). Each nested call appends its
    // (family, batch) AFTER its own hooks fire, so this queue ends up in
    // dependency-safe order: children before parents. The executor drains it
    // once per top-level DDL and relays each entry to workers. Workers never
    // drain this (no broadcast channel), so it stays empty there.
    pub(crate) pending_broadcasts: Vec<(SysFamily, Batch)>,

    // --- Deferred physical directory deletions ---
    //
    // A table/view/index drop hook queues the entity's on-disk directory here
    // instead of deleting it synchronously. `fire_hooks` runs during DAG
    // evaluation, which overlaps the WAL fdatasync; a synchronous delete would
    // open a window where the directory is gone but the DROP is not yet durably
    // committed (a crash there leaves the catalog showing the entity as still
    // existing while its files are permanently absent). The executor drains
    // this only after the DDL zone's fdatasync confirms durability — the
    // catalog analog of `ShardIndex::pending_deletions`.
    pub(crate) pending_dir_deletions: Vec<String>,

    /// Directories that are durably dropped but must NOT be physically removed
    /// yet: worker processes share this on-disk tree and may still be applying
    /// the CREATE of the same entity (FLAG_DDL_SYNC is fire-and-forget and
    /// applied in-order, slower than the master's own removal). Removal is
    /// deferred to the next checkpoint, whose per-worker ACK barrier proves
    /// every worker has consumed past this DROP — hence finished the CREATE.
    pub(crate) checkpoint_gated_deletions: Vec<String>,

    // The applier's current execution context: replay/live phase, the two
    // transient sub-operation flags (rollback / cascade-drop), and the
    // DDL-zone LSN. See `ApplyContext`.
    pub(crate) ctx: ApplyContext,

    /// Rows per `drain_chunk` call in DDL backfills and the unique pre-flight
    /// scan. Defaults to [`DDL_SCAN_CHUNK_ROWS`]; lives on the engine rather
    /// than being a parameter because the backfills are invoked from hooks,
    /// which tests can only reach through `submit` — they shrink this field
    /// instead to exercise chunk boundaries.
    pub(crate) ddl_scan_chunk_rows: usize,

    /// Per-worker distinct-group cap for the ad-hoc aggregate fold
    /// (`GNITZ_ADHOC_GROUP_CAP` override, default `ADHOC_GROUP_CAP`). Beyond it
    /// the fold aborts the request with a CREATE-VIEW suggestion — a stated
    /// resource-exhaustion posture, never silent degradation. Per-worker, so it
    /// never fires when the global group count ≤ cap.
    pub(crate) adhoc_group_cap: usize,
}

/// Default value of [`CatalogEngine::adhoc_group_cap`].
pub(crate) const ADHOC_GROUP_CAP: usize = 65_536;
