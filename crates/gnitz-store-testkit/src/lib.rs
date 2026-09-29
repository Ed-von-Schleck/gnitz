//! `gnitz-store`'s shared test helpers and its rung-guard walk, compiled from
//! `gnitz-store/src/test_support` as a library other crates' tests link.

#[path = "../../gnitz-store/src/test_support/shared.rs"]
mod store;

#[path = "../../gnitz-store/src/test_support/ladder.rs"]
mod ladder;

pub use ladder::*;
pub use store::*;
