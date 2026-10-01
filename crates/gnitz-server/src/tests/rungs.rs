//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the three rungs are one crate. The
//! walk is `gnitz-zset-testkit`'s, shared with the guards of the crates beneath; what
//! is stated here is only this crate's ladder.

use std::path::Path;

use crate::test_support::assert_ladder;

/// The tiers, bottom first — the ladder `CLAUDE.md` and the crate root state
/// in prose.
const LADDER: &[&[&str]] = &[&["query"], &["catalog"], &["runtime"]];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")), LADDER);
}
