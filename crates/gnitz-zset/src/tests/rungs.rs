//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the rungs are one crate, so nothing
//! else stops a `repr` file from naming `crate::algebra` tomorrow. The walk
//! itself is `test_support::ladder`, shared with the guards of the crates above;
//! what is stated here is only this crate's ladder.

use std::path::Path;

use crate::test_support::assert_ladder;

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The tiers, bottom first — the ladder `CLAUDE.md` and the `mod.rs` headers
/// state in prose.
const LADDER: &[&[&str]] = &[&["schema"], &["repr"], &["algebra"]];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(SRC), LADDER);
}

/// The walk sees an edge the ladder withholds: `algebra` does name `repr`.
#[test]
#[should_panic(expected = "algebra/ names crate::repr")]
fn a_withheld_edge_fails_the_guard() {
    let mut ladder = LADDER.to_vec();
    ladder.swap(1, 2);
    assert_ladder(Path::new(SRC), &ladder);
}
