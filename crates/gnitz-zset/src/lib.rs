//! GnitzDB's Z-set kernel: the schema, the columnar batch representation and its
//! cursor, and every operator over them.
//!
//! This is the row-level half of the engine. Every row kernel lives here —
//! operator, merge, sort, encoder, cursor; the crates above — `gnitz-store`,
//! which keeps Z-sets in an LSM, and `gnitz-server`, which maintains circuits —
//! call in once per batch, per run or per probed key. Nothing here opens a
//! store, publishes a manifest or knows a relation exists.
//!
//! The public module roots below are the API. They form a layer ladder, each
//! naming only those beneath it — `tests/rungs.rs` states that table and
//! enforces it:
//!
//! ```text
//! stream   → algebra, repr, schema    the operators that read a trace
//! algebra  → repr, schema             functions of one Z-set · key encoders
//! repr     → schema                   a Z-set as bytes · the cursor over runs
//! schema                              the bottom rung
//! ```
//!
//! The submodules under each root are private; what a root re-exports is what
//! it publishes, plus `schema::key`, named as a module. An item is `pub`
//! because another crate names it; everything else is `pub(crate)`.
//!
//! There is no crate-root re-export façade: a type's rung is part of what its
//! path says, and the root itself holds no name.
//!
//! A public function panics only on a violation of its own contract by the
//! caller: an out-of-range column index, a malformed slot. Every *runtime*
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

pub mod algebra;
pub mod repr;
pub mod schema;
pub mod stream;

/// Tests no single module owns: the rung guard over the module roots above.
#[cfg(test)]
#[path = "tests/rungs.rs"]
mod rung_tests;

// `test_support::shared` is compiled here and again as `gnitz-zset-testkit` —
// from one source, which spells every path `gnitz_zset::`. This alias is what
// makes those paths resolve in this crate.
#[cfg(test)]
extern crate self as gnitz_zset;
#[cfg(test)]
pub mod test_support;
