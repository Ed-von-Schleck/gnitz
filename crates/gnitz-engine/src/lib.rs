//! GnitzDB's single-node database: the Z-set store, the DBSP operators, the
//! circuit compiler and the catalog.
//!
//! The six public module roots below are the API, in the order of the layer
//! ladder they form — each depends only on those beneath it, and all of them on
//! `foundation`. The submodules under each root are private; what a root
//! re-exports is what it publishes, plus the eight submodules named as modules
//! (`foundation::{posix_io, log, worker_ctx, env, fault, xxh}`, `schema::key`
//! and `storage::batch_pool`).
//!
//! There is no crate-root re-export façade: a type's rung is part of what its
//! path says. The only names at the root are the `gnitz_*!` macros, which have
//! no other export site. Four of them log; the fifth, `gnitz_fatal_abort!`,
//! ends the calling process — see its doc before linking this crate into one
//! that cannot afford to be ended.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

// FIRST, and `#[macro_use]`: the attribute reaches only code that follows this
// item, and it — not `#[macro_export]` — is what puts `gnitz_warn!` and its
// siblings into textual scope for the rest of the crate.
#[macro_use]
pub mod foundation;

pub mod catalog;
pub mod ops;
pub mod query;
pub mod schema;
pub mod storage;

// Private: nothing outside the crate names it, and a `pub mod` would both widen
// the surface for no caller and turn off dead-code analysis for its contents.
// A consumer that needs expression types names `gnitz-expr`, the crate.
mod expr;

#[cfg(test)]
mod test_rng;

// `test_support::shared` is compiled both here and as the whole of
// `gnitz-engine-testkit` (which is how `gnitz-server`'s tests reach it), so it
// spells every path `gnitz_engine::`. This alias is what makes those paths
// resolve in this crate.
#[cfg(test)]
extern crate self as gnitz_engine;
#[cfg(test)]
pub mod test_support;
