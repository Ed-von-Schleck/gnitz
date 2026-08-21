//! `gnitz-engine`'s test helpers, compiled as a library so `gnitz-server`'s
//! tests can reach them.
//!
//! The helpers themselves live in `gnitz-engine/src/test_support/shared.rs` and
//! are compiled from that one source in both places — see its header for why a
//! copy would be the wrong shape. Only the *shared* half crosses: the engine's
//! own helpers sit in the sibling `internal` module, so nothing here forces an
//! engine internal to be published. This crate cannot be replaced by a dev-dependency
//! on `gnitz-engine` pointing back the other way: a library's `--test` unit is a
//! distinct crate instance from the rlib a dependency links, so its `Batch` and
//! the helper's `Batch` are different types.
//!
//! A crate rather than a `testing` cargo feature on the library: features are
//! additive across a whole build graph, so one consumer listing
//! `features = ["testing"]` as a normal dependency would compile a test-only
//! layout spoofer into a release build. A dev-only crate is never in one.

#[path = "../../gnitz-engine/src/test_support/shared.rs"]
mod shared;

pub use shared::*;
