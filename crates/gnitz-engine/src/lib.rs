//! GnitzDB's DBSP layer: the circuit compiler, the bytecode VM, epoch execution
//! and the system-table catalog.
//!
//! The Z-set store beneath it — the columnar batch representation, the LSM, the
//! operators, the relation registry and the `ReadSpec` executor — is
//! `gnitz-store`, a separate crate this one depends on. That split is what makes
//! "a client links no compiler, no VM and no catalog" a fact of the crate graph
//! rather than a convention: a host holding a mirrored view links `gnitz-store`
//! alone.
//!
//! The two public module roots below are the API, in the order of the layer
//! ladder they form: `catalog` over `query` over everything `gnitz-store`
//! publishes. The submodules under each root are private; what a root
//! re-exports is what it publishes.
//!
//! There is no crate-root re-export façade: a type's rung is part of what its
//! path says. Nothing this crate exports ends the calling process: every
//! fallible path returns its error, and the decision to fail-stop belongs to the
//! process that owns a restart contract.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

// FIRST, and before any module: `#[macro_use]` reaches only code that follows
// it, and it is what puts `gnitz_warn!` and its siblings — defined in
// `gnitz-store` — into textual scope for the rest of this crate.
#[macro_use]
extern crate gnitz_store;

pub mod catalog;
pub mod query;

// `test_support::shared` is compiled both here and as half of
// `gnitz-engine-testkit` (which is how `gnitz-server`'s tests reach it), so it
// spells every path `gnitz_engine::`. This alias is what makes those paths
// resolve in this crate.
#[cfg(test)]
extern crate self as gnitz_engine;
#[cfg(test)]
pub mod test_support;
