//! The ladder guard: every rung names only the rungs beneath it, and of the
//! kernel beneath them only the rungs below `stream`.
//!
//! The crate graph cannot enforce either — the rungs are one crate, and the
//! kernel's `stream` is public because `gnitz-server` runs it. The walk is
//! `gnitz-zset-testkit`'s, shared with the kernel's and the server's own
//! guards; what is stated here is only this crate's ladder.

use std::path::Path;

use crate::test_support::{assert_ladder, assert_rungs_name_only};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The tiers, bottom first — the ladder `CLAUDE.md` and the `mod.rs` headers
/// state in prose.
const LADDER: &[&[&str]] = &[&["storage"], &["relation"], &["read"]];

/// The kernel rungs the store may name. Never `stream`: its operators read a
/// circuit's traces, and only `gnitz-server` holds a circuit.
const KERNEL: &[&str] = &["schema", "repr", "algebra"];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(SRC), LADDER);
}

#[test]
fn every_rung_names_only_the_kernel_below_stream() {
    assert_rungs_name_only(Path::new(SRC), &LADDER.concat(), "gnitz_zset", KERNEL);
}

/// The walk sees the kernel paths the store does name: `algebra` withheld, the
/// guard refuses them.
#[test]
#[should_panic(expected = "names gnitz_zset::algebra")]
fn a_withheld_kernel_rung_fails_the_guard() {
    assert_rungs_name_only(Path::new(SRC), &LADDER.concat(), "gnitz_zset", &["schema", "repr"]);
}
