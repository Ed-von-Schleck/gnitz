//! GnitzDB's Z-set store: the LSM that keeps a relation on disk, the relation
//! registry and the `ReadSpec` executor.
//!
//! The Z-sets it stores — the schema, the columnar batch, the shard image, the
//! cursor and every operator over them — are `gnitz-zset`, the kernel crate
//! beneath this one. This crate owns their lifecycle: which runs a store holds,
//! when they flush, compact and publish a manifest, which relations a process
//! registered, and how one is read.
//!
//! This and the kernel are what a **client** links. A host holding a mirrored
//! view drives `relation` and `read` directly and links neither the circuit
//! compiler, the DBSP VM, epoch execution nor the system-table catalog — all of
//! which live in `gnitz-server`, the binary that depends on this crate. The seam
//! is the crate graph, not a comment: nothing here can name anything there, and
//! nothing links what is there.
//!
//! The module roots below form a layer ladder, each naming only those beneath it
//! — `tests/rungs.rs` states that table and enforces it. `relation` and `read`
//! are the API; `storage`, the LSM under them, is reached only through a
//! relation's stores. The submodules under each root are private; what a root
//! re-exports is what it publishes. An item is `pub` because another crate
//! names it; everything else is `pub(crate)`.
//!
//! There is no crate-root re-export façade: a type's rung is part of what its
//! path says, and the root itself holds no name.
//!
//! A public function panics only on a violation of its own contract by the
//! caller: an out-of-range column index, a malformed slot, a lifecycle verb
//! called from the wrong [`Residency`](relation::Residency). Every *runtime*
//! failure returns its error, because the decision to fail-stop belongs to the
//! process that owns a restart contract.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

// Crate-wide scope for `gnitz_warn!` and its siblings; a plain `use` would
// reach this module only.
#[macro_use]
extern crate gnitz_foundation;

pub mod read;
pub mod relation;
mod storage;

/// Tests no single module owns: the rung guard over the module roots above.
#[cfg(test)]
#[path = "tests/rungs.rs"]
mod rung_tests;

#[cfg(test)]
pub mod test_support;
