//! GnitzDB's Z-set store: the columnar batch representation, the LSM, the DBSP
//! operators, the relation registry and the `ReadSpec` executor.
//!
//! This is the half a **client** links. A host holding a mirrored view drives
//! `relation` and `read` directly and links neither the circuit compiler, the
//! DBSP VM, epoch execution nor the system-table catalog — all of which live in
//! `gnitz-server`, the binary that depends on this crate. The seam is the crate
//! graph, not a comment: nothing here can name anything there, and nothing links
//! what is there.
//!
//! The seven public module roots below are the API, in the order of the layer
//! ladder they form — each depends only on those beneath it, and all of them on
//! `foundation`. The submodules under each root are private; what a root
//! re-exports is what it publishes, plus the eight submodules named as modules
//! (`foundation::{posix_io, log, worker_ctx, env, fault, xxh}`, `schema::key`
//! and `storage::batch_pool`). An item is `pub` because another crate names it;
//! everything else is `pub(crate)`.
//!
//! There is no crate-root re-export façade: a type's rung is part of what its
//! path says. The only names at the root are the four `gnitz_*!` logging
//! macros, which have no other export site. Nothing this crate exports ends the
//! calling process: every fallible path returns its error, and the decision to
//! fail-stop belongs to the process that owns a restart contract.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

// FIRST, and `#[macro_use]`: the attribute reaches only code that follows this
// item, and it — not `#[macro_export]` — is what puts `gnitz_warn!` and its
// siblings into textual scope for the rest of the crate.
#[macro_use]
pub mod foundation;

pub mod ops;
pub mod read;
pub mod relation;
pub mod schema;
pub mod storage;

// Public only because the seam needs it: `gnitz-server`'s compiler and VM name
// `MapPlan` and its siblings, and `read` is on this side of the crate boundary
// while they are not. A consumer that needs the expression *language* names
// `gnitz-expr`, the crate.
pub mod expr;

#[cfg(test)]
mod test_rng;

/// Tests no single module owns: the rung guard over `relation/` and `read/`.
#[cfg(test)]
#[path = "tests/rungs.rs"]
mod rung_tests;

/// Tests no single module owns: the guard that no other crate writes a row count.
#[cfg(test)]
#[path = "tests/row_count.rs"]
mod row_count_tests;

// `test_support::shared` is compiled here, as the whole of `gnitz-store-testkit`,
// and again inside `gnitz-server` — all from one source, which spells every path
// `gnitz_store::`. This alias is what makes those paths resolve in this crate.
#[cfg(test)]
extern crate self as gnitz_store;
#[cfg(test)]
pub mod test_support;
