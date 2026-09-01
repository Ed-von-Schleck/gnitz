//! Test helpers: this crate's own, plus two of `gnitz-store`'s compiled in.
//!
//! [`internal`] is this crate's own — the helpers that name `CatalogEngine` and
//! `ColumnDef`, and reach crate-internals as `crate::`. [`store`] is
//! `gnitz-store`'s `shared` file, compiled here a second time from that one
//! source so a fixture is not two independent Z-set batch builders free to
//! drift; [`ladder`] is its rung-guard walk, so the two crates' guards cannot
//! drift on what counts as a reach.
//!
//! There is no `shared` half here: that division marked what crossed a crate
//! boundary, and nothing this crate builds crosses one — nothing links it.
//!
//! All three are re-exported here, so every call site names
//! `crate::test_support::X` and moving a helper between them touches no test.

pub mod internal;

// One source compiled in several crates, and no consumer calls every helper in
// it — the lint here would be a claim about this crate's tests, not the file.
#[allow(dead_code)]
#[path = "../../../gnitz-store/src/test_support/shared.rs"]
pub mod store;

#[path = "../../../gnitz-store/src/test_support/ladder.rs"]
pub mod ladder;

pub use internal::*;
pub use ladder::*;
pub use store::*;
