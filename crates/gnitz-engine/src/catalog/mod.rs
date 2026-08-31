//! Catalog engine: the entity registry, the system tables and their bootstrap,
//! DDL application, hook processing, and catalog recovery — all wrapped around
//! `DagEngine`.
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
mod scan_spec;
mod schema_block;
mod store_io;
mod store_lsn;
mod sys_tables;
mod types;
mod utils;
mod write_path;

#[cfg(test)]
mod suites;

use std::fs;
use std::rc::Rc;

use crate::query::{DagEngine, DeltaFeed, RelationKind, RelationStores, StoreHandle};
use crate::schema::{Placement, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, ReadCursor, RecoverySource, StorageError, Table};

// ── Crate-wide facade — items with genuine out-of-catalog consumers ──────────
// The DDL_TXN driver's bundle decoders: it resolves each family once, carries
// the value, and reads back what the bundle created or dropped.
pub use sys_tables::{family_pks_by_sign, idx_tab_drops, idx_tab_unique_creates};
pub use sys_tables::{SysFamily, FIRST_USER_TABLE_ID, PUBLIC_SCHEMA_ID, SEQ_TAB_ID};
pub use types::{ColumnDef, FkEdge};
// The reply path's cached schema wire block, and the encoders that fill it.
pub use cache::SchemaWireEntry;
pub use schema_block::{encode_named_schema_block, encode_schema_block, encode_schema_block_ipc};
pub use store_io::IngestError;
// The master's ScanSpec confinement test.
pub use scan_spec::scan_spec_worker;

// Import everything from sys_tables for internal use.
use registry::build_schema_from_col_defs;
use sys_tables::*;

// ── Catalog-internal re-exports — no out-of-catalog consumer (W8). These reach
//    the submodules through their `use super::*` glob, so they stay re-exported
//    but scoped to the catalog subtree rather than the crate-wide surface. ─────
pub(in crate::catalog) use apply_context::{ApplyContext, ApplyMode};
pub(in crate::catalog) use cache::CatalogCacheSet;
pub(in crate::catalog) use gnitz_wire::validate_user_identifier;
pub(in crate::catalog) use gnitz_wire::FK_INDEX_INFIX;
pub(in crate::catalog) use registry::raise_id_counter;
// The child-directory grammar and the directory primitives are storage's; the
// catalog only consumes them.
pub(in crate::catalog) use crate::storage::{
    fsync_dir, peek_header, reclaim_retired_children, remove_child, state_child_manifests, subdir_names, ChildAddr,
};
#[cfg(test)]
pub(in crate::catalog) use utils::cursor_read_string;
pub(in crate::catalog) use utils::{
    cursor_read_u64, ensure_dir, index_dir, is_table_dir_name, lock_data_dir, make_fk_index_name, preflight_dir,
    relation_dir, retract_key_range, retract_pk_list, schema_dir, sys_catalog_dir, sys_family_dir, sys_opk,
};
// `BatchBuilder` holds no catalog state and lives in `storage`; re-export it
// for the catalog's row builders.
pub(crate) use crate::storage::BatchBuilder;

// ---------------------------------------------------------------------------
// CatalogEngine
// ---------------------------------------------------------------------------

/// Default rows per `drain_chunk` call on a chunked scan. Bounds peak scan
/// memory at O(chunk × row_width).
pub(crate) const DDL_SCAN_CHUNK_ROWS: usize = 65_536;

/// The catalog engine wraps DagEngine and manages the entity registry,
/// system tables, DDL operations, and hook processing.
pub struct CatalogEngine {
    pub(crate) dag: DagEngine,
    pub(crate) base_dir: String,

    /// The `flock`ed handle on `base_dir`'s lock file, dropped by
    /// [`CatalogEngine::close`]. Closing it releases the lock, so it is held
    /// until then: a second writer would reseed `current_lsn` from
    /// `max_lsn + 1` and mint shard names the first writer is already using.
    dir_lock: Option<fs::File>,

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
    /// off `worker_ctx`, which is 1 in the master process (`set_worker_identity`
    /// runs only post-fork): a store built from the ambient value would be named
    /// `w0of1` while the boot repartition wrote `w0of{W}…`, and the child-dir
    /// sweep would delete it as unowned.
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
    /// the recovered `G`. Written only by `set_resume_generation`, and reaches a
    /// `Table::new` only through `rederive_source`, so every rederived relation
    /// opens against one value.
    pub(crate) resume_generation: u64,
    /// The topology row last recorded: `(worker_count as u64) << 32 | STATE_FORMAT`.
    /// Recovered from `SEQ_ID_TOPOLOGY` at boot (0 on a fresh DB). `topology_matches`
    /// compares it against the launched worker count + `STATE_FORMAT` to decide
    /// whether persisted view state is reloadable.
    pub(crate) recorded_topology: u64,
    /// View ids whose checkpointed state — output stores and operator traces
    /// alike — was rejected at boot (generation mismatch, topology change, or a
    /// transitively-invalid source view) and must
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

    // The applier's current execution context: the apply mode and the DDL-zone
    // LSN. See `ApplyContext`.
    pub(crate) ctx: ApplyContext,

    /// Rows per `drain_chunk` call on every chunked scan the engine drives:
    /// index and view backfill, the bounded-view hydration merge, the ad-hoc
    /// `ReadSpec` scan. Defaults to [`DDL_SCAN_CHUNK_ROWS`], overridden by
    /// `GNITZ_DDL_SCAN_CHUNK_ROWS`. A field rather than a parameter because the
    /// backfills are invoked from hooks, which tests can only reach through
    /// `submit` — they shrink this instead to exercise chunk boundaries.
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
