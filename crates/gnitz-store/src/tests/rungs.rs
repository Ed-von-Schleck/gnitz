//! The ladder guard: every rung names only the rungs beneath it, and nothing in
//! the crate names a trace rung but the runs, never `stream`.
//!
//! The crate graph cannot enforce either — the rungs are one crate, and
//! `gnitz-trace`'s `stream` is public because `gnitz-server` runs it. The walk
//! is `gnitz-zset-testkit`'s; what is stated here is only this crate's ladder.

use crate::test_support::{assert_ladder, assert_names_only};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The rungs, bottom first.
const LADDER: &[&str] = &["storage", "relation", "read"];

/// The trace rungs the store may name. Never `stream`: its operators read a
/// circuit's traces, and only `gnitz-server` holds a circuit.
const TRACE: &[&str] = &["runs"];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(SRC, LADDER);
}

#[test]
fn the_crate_names_only_the_runs_of_the_traces() {
    assert_names_only(SRC, "gnitz_trace", TRACE);
}
