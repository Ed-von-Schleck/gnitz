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
pub use store_io::SourceCursor;

use crate::relation::RelationRegistry;
use crate::storage::{Batch, StoreError};

/// Recomputes a capacity-bounded view's rows at its skeleton keys, from the view's
/// own maintained state. A host that maintains no circuit has none.
pub trait SkeletonHydrator {
    /// Every row of `view_id` at `keys` — flat OPK images at the view's
    /// `pk_stride`, strictly ascending, each held by the view's store as a skeleton
    /// row — consolidated, each key's weights summing to its skeleton row's weight.
    fn hydrate_keys(&mut self, registry: &RelationRegistry, view_id: u64, keys: Vec<u8>) -> Result<Batch, StoreError>;
}
