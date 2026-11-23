//! Catalog engine: the entity registry, the system tables and their bootstrap,
//! DDL application, hook processing, and catalog recovery — over the two
//! siblings `CatalogEngine` holds, `RelationRegistry` and `DagEngine`.
//!
//! # Hook model
//!
//! Every DDL group — a bundle's on the master, a DdlSync's on a worker — is
//! ingested and then run through [`fire_hooks`](CatalogEngine::fire_hooks),
//! which runs, per family, that family's register hook; boot ingests the
//! recovered groups hook-free and then fires each family's hook once over its
//! whole store. `sequences` has no hook, and its writes fire none. A
//! registration reads the relation's COL_TAB rows, and a view's
//! its CIRCUIT_TAB row, from the store, so those families are applied first:
//! [`SysFamily::ALL`] is the apply order — of a live bundle, of the replay
//! at boot — reversed for a bundle that only retracts, so a dependent is retired
//! before what it depends on, and reversed around a drop's own group by
//! `with_owned_retractions`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! Tests no single module owns live in `suites/`, a declared `mod suites;` child
//! of this module: they reach this subsystem's surface, not any one module's
//! private items.

mod bootstrap;
mod hooks;
mod precheck;
mod relations;
mod sequences;
mod sys_reads;
mod sys_tables;
mod view_state;
mod write_path;

#[cfg(test)]
mod suites;

use gnitz_store::relation::RelationRegistry;
use rustc_hash::FxHashSet;

use crate::query::DagEngine;
use relations::CatalogCacheSet;

// ── Crate-wide facade — items with genuine out-of-catalog consumers ──────────
pub(crate) use relations::{FkEdge, LockSet, RowConstraints};
#[cfg(test)]
pub(crate) use sys_tables::PUBLIC_SCHEMA_ID;
pub(crate) use sys_tables::{DdlBundle, SysFamily, SysFamilyExt};
pub(crate) use write_path::{AppliedDdl, ZoneError, ZoneSink};

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

    /// [`next_catalog_id`](Self::next_catalog_id) as this boot's catalog
    /// replay left it: every id below it was handed out by an earlier boot,
    /// whose connections are gone.
    pub(in crate::catalog) id_floor: u64,
    /// The ids a bundle this process committed created. With `id_floor` they
    /// make a catalog object id single-use: the precheck admits a create under
    /// neither.
    pub(in crate::catalog) used_ids: FxHashSet<u64>,
    /// The newest SAL zone applied to the system families; every system flush
    /// records it as its replay floor.
    pub(in crate::catalog) applied_zone: u64,
    /// What this boot resumes: decided once, before any store opens.
    pub(crate) boot: BootVerdict,
}

/// The boot verdict: the committed checkpoint generation kept state resumes
/// from, and the views rebuilt instead. Read at store open; the master's
/// recovery takes the rebuild set to rebuild them.
#[derive(Default)]
pub(crate) struct BootVerdict {
    pub(crate) resume_at: u64,
    pub(crate) rebuild: FxHashSet<u64>,
}
