//! The ladder guard: every rung names only the rungs beneath it, and of the
//! traces beneath them only the runs, never `stream`.
//!
//! The crate graph cannot enforce either — the rungs are one crate, and
//! `gnitz-trace`'s `stream` is public because `gnitz-server` runs it. The walk
//! is `gnitz-zset-testkit`'s, shared with the kernel's and the server's own
//! guards; what is stated here is only this crate's ladder.

use std::path::Path;

use crate::test_support::{assert_ladder, assert_rungs_name_only};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The tiers, bottom first — the ladder `CLAUDE.md` and the `mod.rs` headers
/// state in prose.
const LADDER: &[&[&str]] = &[&["storage"], &["relation"], &["read"]];

/// The trace rungs the store may name. Never `stream`: its operators read a
/// circuit's traces, and only `gnitz-server` holds a circuit.
const TRACE: &[&str] = &["runs"];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(SRC), LADDER);
}

#[test]
fn every_rung_names_only_the_runs_of_the_traces() {
    assert_rungs_name_only(Path::new(SRC), &LADDER.concat(), "gnitz_trace", TRACE);
}

/// The walk sees the trace paths the store does name: `runs` withheld, the
/// guard refuses them.
#[test]
#[should_panic(expected = "names gnitz_trace::runs")]
fn a_withheld_trace_rung_fails_the_guard() {
    assert_rungs_name_only(Path::new(SRC), &LADDER.concat(), "gnitz_trace", &[]);
}
