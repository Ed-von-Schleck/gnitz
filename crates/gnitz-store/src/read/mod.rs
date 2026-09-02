//! The read rung — the `ReadSpec` executor and the store read verbs, as
//! `impl RelationRegistry` blocks over `relation` and `ops`.
//!
//! It is a rung of its own because it names the operator layer — `AdhocFold` in
//! `scan_spec`, `op_union` in `store_io` — which `relation` may not. Two
//! symbols, but they are what put `read` above `ops`.
//!
//! Nothing here reaches a `CatalogEngine` or a `DagEngine`: those are in
//! `gnitz-server`, which depends on this crate, so the direction is the crate
//! graph's. Recomputing a capacity-bounded view's skeleton row does need the
//! view's compiled program, and that one edge is injected as
//! [`SkeletonHydrator`] rather than reached — a host that maintains no circuit
//! passes `None`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover.

mod scan_spec;
mod store_io;

pub use store_io::IndexWalk;

use crate::relation::RelationRegistry;
use crate::storage::{Batch, StoreError};

/// Recompute the payload of `keys` from the view's own maintained state — the
/// source store for a linear body, the two `integrate_trace` tables for an
/// inner equi-join. Implemented once, in the DBSP layer.
///
/// The registry is a parameter rather than something the implementor holds: the
/// one implementor is `DagEngine`, which is the registry's *sibling* and holds no
/// reference to it.
pub trait SkeletonHydrator {
    fn hydrate_keys(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        keys: Vec<u8>,
        coarse: &[i64],
    ) -> Result<Batch, StoreError>;
}
