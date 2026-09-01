//! Test helpers, split by how far they may reach.
//!
//! [`shared`] is the half `gnitz-server`'s tests and `gnitz-store-testkit` also
//! use; it is compiled from this one source in each of those places too.
//! [`ladder`] is the rung-guard walk, likewise compiled into `gnitz-server`.
//! [`internal`] is this crate's own, and may name crate-internals.
//!
//! **The split is what keeps a test helper from widening the library.** A
//! helper in `shared` sees only `gnitz-store`'s public API, so one that reaches
//! an internal fails to build in the other compilations — and the tempting way
//! out is to publish the internal, which makes it permanent API for a helper no
//! other crate calls. Add a helper to `internal` unless another crate needs it;
//! when one in `shared` does need an internal, build it out of a published item
//! that already earns its place rather than publishing a second.
//!
//! All three are re-exported here, so every call site names
//! `crate::test_support::X` and moving a helper between them touches no test.

pub mod internal;
pub mod ladder;
pub mod shared;

pub use internal::*;
pub use ladder::*;
pub use shared::*;
