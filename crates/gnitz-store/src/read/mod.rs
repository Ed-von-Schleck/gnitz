//! The read rung — the `ReadSpec` executor and the store read verbs, as
//! `impl RelationRegistry` blocks over `relation`.
//!
//! What is decided here is which source serves a bound — the store, or an index
//! over it — and how a skeleton row is hydrated. The filter `scan_spec` drives is
//! `gnitz-expr`'s, the map and the sink are `gnitz_zset::algebra`'s — the same
//! kernels a circuit's linear operators dispatch to — so a read here and a view
//! there compute a row the same way.
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

#[cfg(test)]
mod bench_scan_spec;

use crate::relation::RelationRegistry;
use gnitz_wire::PkKeys;
use gnitz_zset::repr::Batch;

/// Recomputes a capacity-bounded view's rows at its skeleton keys, from the view's
/// own maintained state. A host that maintains no circuit has none.
pub trait SkeletonHydrator {
    /// Every row of `view_id` at `keys` — at the view's `pk_stride`, each held by
    /// the view's store as a skeleton row — consolidated, each key's weights summing
    /// to its skeleton row's weight.
    fn hydrate_keys(&mut self, registry: &RelationRegistry, view_id: u64, keys: PkKeys) -> Result<Batch, String>;
}
