//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the rungs are one crate, so nothing
//! else stops a `runs` file from naming `crate::stream` tomorrow. The walk is
//! `gnitz-zset-testkit`'s; what is stated here is only this crate's ladder.

use crate::test_support::assert_ladder;

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The tiers, bottom first — the table in `lib.rs`.
const LADDER: &[&[&str]] = &[&["runs"], &["stream"]];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(SRC, LADDER);
}
