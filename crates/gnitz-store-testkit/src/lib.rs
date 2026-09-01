//! `gnitz-store`'s test helpers, compiled as a library so other crates' tests
//! can reach them.
//!
//! The helpers themselves live in `gnitz-store/src/test_support/shared.rs` and
//! are compiled from that one source in every place that needs them; see its
//! header for why a copy would be the wrong shape. Only the *shared* half
//! crosses: the crate's own helpers sit in its sibling `internal` module, so
//! nothing here forces an internal to be published. This crate cannot be
//! replaced by a dev-dependency pointing back the other way: a library's
//! `--test` unit is a distinct crate instance from the rlib a dependency links,
//! so its `Batch` and the helper's `Batch` are different types.
//!
//! A crate rather than a `testing` cargo feature on the library: features are
//! additive across a whole build graph, so one consumer listing
//! `features = ["testing"]` as a normal dependency would compile a test-only
//! layout spoofer into a release build. A dev-only crate is never in one.

#[path = "../../gnitz-store/src/test_support/shared.rs"]
mod store;

pub use store::*;
