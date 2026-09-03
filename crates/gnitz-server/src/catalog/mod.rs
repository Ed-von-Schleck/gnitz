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
//!   state. Storage is applied before hooks fire, so these reconcile against the
//!   row's *net* live state rather than its own sign; a rename pair is net-live
//!   before and after, so it fires neither teardown nor re-registration.
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

mod apply_context;
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
    IndexCircuitEntry, RelationKind, RelationRegistry, RelationSpec, StoreConfig, ViewBudgets,
};
use gnitz_store::schema::{Placement, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, RamBudgets, ReadCursor, RecoverySource, Slot, StorageError, StoreError, Table};

// ── Crate-wide facade — items with genuine out-of-catalog consumers ──────────
// The DDL_TXN driver's bundle decoders: it resolves each family once, carries
// the value, and reads back what the bundle created or dropped.
pub(crate) use sys_tables::{family_pks_by_sign, idx_tab_drops, idx_tab_unique_creates};
pub(crate) use sys_tables::{SysFamily, FIRST_USER_TABLE_ID, PUBLIC_SCHEMA_ID, SEQ_TAB_ID};
pub(crate) use types::{ColumnDef, FkEdge};
// The reply path's schema-wire-block encoders. The `SchemaWireEntry` they fill
// is named only inside the catalog — the reply path takes one by value from
// `schema_wire_entry` and reads its fields — so it is re-exported below.
pub(crate) use schema_block::{encode_schema_block, encode_schema_block_ipc};

// Import everything from sys_tables for internal use.
use registry::build_schema_from_col_defs;
use sys_tables::*;

// ── Catalog-internal re-exports — no out-of-catalog consumer (W8). These reach
//    the submodules through their `use super::*` glob, so they stay re-exported
//    but scoped to the catalog subtree rather than the crate-wide surface. ─────
pub(in crate::catalog) use apply_context::{ApplyContext, ApplyMode};
pub(in crate::catalog) use cache::{CatalogCacheSet, SchemaWireEntry};
pub(in crate::catalog) use gnitz_wire::validate_user_identifier;
pub(in crate::catalog) use index_backfill::IndexPass;
pub(in crate::catalog) use registry::raise_id_counter;
// The child-directory grammar and the directory primitives are storage's; the
// catalog only consumes them.
#[cfg(test)]
pub(in crate::catalog) use gnitz_store::storage::subdir_names;
pub(in crate::catalog) use gnitz_store::storage::{children_at_generation, fsync_dir, ChildAddr};
#[cfg(test)]
pub(in crate::catalog) use utils::cursor_read_string;
pub(in crate::catalog) use utils::{
    cursor_read_u64, index_dir, make_fk_index_name, preflight_dir, retract_key_range, retract_pk_list, schema_dir,
    sys_catalog_dir, sys_family_dir, sys_opk,
};
// The relation rung's directory primitives; the catalog only consumes them.
pub(in crate::catalog) use gnitz_store::relation::{
    ensure_dir, lock_data_dir, relation_dir, staged_dir, DIR_LOCK_RETRY_FOR,
};
// `BatchBuilder` holds no catalog state and lives in `storage`; re-export it
// for the catalog's row builders.
pub(in crate::catalog) use gnitz_store::storage::BatchBuilder;

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
    /// View ids whose checkpointed state — output stores and operator traces
    /// alike — was rejected at boot (generation mismatch, topology change, or a
    /// transitively-invalid source view) and must
    /// be reset-and-rebuilt rather than resumed. Computed pre-fork by
    /// `compute_invalid_views`, COW-inherited by every worker, and consumed by
    /// the master's boot rebuild sweep and the per-worker output reset. Empty on
    /// a clean same-topology restart (every view resumes).
    pub(in crate::catalog) invalid_views: rustc_hash::FxHashSet<i64>,

    // --- Pending broadcasts (ordered innermost → outermost) ---
    //
    // Every family the master applies is queued here for relay to the workers.
    // A hook may recursively `submit` a cascade retraction (indices, columns,
    // circuit graph); each nested call appends its (family, batch) AFTER its own
    // hooks fire, so the queue ends up in dependency-safe order — children
    // before parents. The executor drains it once per top-level DDL.
    //
    // A worker reaches the same enqueue through `ddl_sync` → cascade → `submit`,
    // and never drains. It stays empty there only because the master broadcasts
    // children before parents: by the time a worker applies the parent `-1`, its
    // own cascade finds nothing live and produces an empty batch, which
    // `apply_and_enqueue_family` drops.
    pub(in crate::catalog) pending_broadcasts: Vec<(SysFamily, Batch)>,

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
    pub(in crate::catalog) pending_dir_deletions: Vec<String>,

    /// Directories that are durably dropped but must NOT be physically removed
    /// yet: worker processes share this on-disk tree and may still be applying
    /// the CREATE of the same entity (a `DdlSync` group is fire-and-forget and
    /// applied in-order, slower than the master's own removal). Removal is
    /// deferred to the next checkpoint, whose per-worker ACK barrier proves
    /// every worker has consumed past this DROP — hence finished the CREATE.
    pub(in crate::catalog) checkpoint_gated_deletions: Vec<String>,

    // The applier's current execution context: the apply mode and the DDL-zone
    // LSN. See `ApplyContext`.
    pub(in crate::catalog) ctx: ApplyContext,
}
