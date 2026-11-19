//! GnitzDB's traces: the sorted runs a Z-set's history is kept as, the shard
//! file a run is written to, the cursor that merges runs, and the operators
//! that read a trace.
//!
//! This is the half of the row kernels that reads history; the functions of
//! one Z-set are `gnitz-zset`'s. `gnitz-store` keeps runs in an LSM and
//! `gnitz-server` runs the operators, each calling in once per run, per batch
//! or per probed key.
//!
//! The public module roots below are the API. They form a layer ladder over
//! `gnitz-zset`, each naming only those beneath it — `tests/rungs.rs` states
//! that table and enforces it:
//!
//! ```text
//! stream   → runs, gnitz-zset         the operators that read a trace
//! runs     → gnitz-zset               sorted runs · the shard file · the cursor
//! ```
//!
//! The submodules under each root are private; what a root re-exports is what
//! it publishes. An item is `pub` because another crate names it; everything
//! else is `pub(crate)`.
//!
//! A public function panics only on a violation of its own contract by the
//! caller. Every *runtime* failure returns its error, because the decision to
//! fail-stop belongs to the process that owns a restart contract.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

// This crate publishes an API to the store and the server above it, so an
// over-wide `pub` inside a private module is an error (`warnings = "deny"`).
#![warn(unreachable_pub)]

pub mod runs;
pub mod stream;

/// Tests no single module owns: the rung guard over the module roots above.
#[cfg(test)]
#[path = "tests/rungs.rs"]
mod rung_tests;

#[cfg(test)]
pub(crate) mod test_support;
