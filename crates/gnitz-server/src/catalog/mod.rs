//! Catalog engine: the entity registry, the system tables and their bootstrap,
//! DDL application, hook processing, and catalog recovery — over the two
//! siblings `CatalogEngine` holds, `RelationRegistry` and `DagEngine`.
//!
//! # Hook model
//!
//! Every system-table write flows through
//! [`fire_hooks`](CatalogEngine::fire_hooks), which runs, per family, that family's
//! register hook. A registration reads the relation's COL_TAB rows, and a view's
//! its CIRCUIT_TAB row, from the store, so those families are applied first:
//! [`SysFamily::ALL`] is the apply order, which `apply_bundle` walks for a live
//! bundle and `replay_catalog` at boot, and `with_owned_retractions` in reverse
//! for a drop's owned rows.
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
mod sequences;
mod sys_reads;
mod sys_tables;
mod view_state;
mod write_path;

#[cfg(test)]
mod suites;

use gnitz_store::relation::RelationRegistry;

use crate::query::DagEngine;
use cache::CatalogCacheSet;

// ── Crate-wide facade — items with genuine out-of-catalog consumers ──────────
pub(crate) use bootstrap::UnreplayedCatalog;
pub(crate) use constraints::{FkEdge, RowConstraints};
#[cfg(test)]
pub(crate) use sys_tables::PUBLIC_SCHEMA_ID;
pub(crate) use sys_tables::{DdlBundle, SysFamily};
// The DDL_TXN driver's unique-index pre-flight input.
pub(crate) use sys_tables::unique_index_creates;
pub(crate) use write_path::{ZoneError, ZoneSink};

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

    /// Every derived lookup the catalog maintains from system-table deltas and
    /// relation registrations.
    pub(in crate::catalog) caches: CatalogCacheSet,

    /// The next catalog object id (schema, relation or index) `allocate_ids`
    /// hands out. Boot raises it past every stored id; the precheck admits no id
    /// at or above it.
    pub(in crate::catalog) next_id: u64,
    /// The newest SAL zone applied to the system families; every system flush
    /// records it as its replay floor.
    pub(in crate::catalog) system_zone: u64,
    /// The checkpoint generation this boot recovered, which a manifest must
    /// carry to be resumed from.
    pub(crate) resume_generation: u64,
}
