//! Test helpers, split by how far they may reach and by which rung they name.
//!
//! [`shared`] and [`internal`] are the catalog-level halves — the same
//! `shared`/`internal` division `gnitz-store` makes, applied to the helpers that
//! name `CatalogEngine` and `ColumnDef`. [`store`] is `gnitz-store`'s own
//! `shared` file, compiled here a second time from that one source so a fixture
//! is not two independent Z-set batch builders free to drift.
//!
//! All three are re-exported here, so every call site names
//! `crate::test_support::X` and moving a helper between them touches no test.

pub mod internal;
pub mod shared;

#[path = "../../../gnitz-store/src/test_support/shared.rs"]
pub mod store;

pub use internal::*;
pub use shared::*;
pub use store::*;
