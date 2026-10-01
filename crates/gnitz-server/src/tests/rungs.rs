//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the three rungs are one crate. The
//! walk is `gnitz-store-testkit`'s, shared with `gnitz-store`'s own guard; what
//! is stated here is only this crate's table.

use std::path::Path;

use crate::test_support::assert_ladder;

/// Each rung and the rungs it may name — the ladder `CLAUDE.md` and the crate
/// root state in prose. A total order here, unlike `gnitz-store`'s table: the
/// catalog names the DAG, and the runtime names both.
const LADDER: &[(&str, &[&str])] = &[
    ("query", &[]),
    ("catalog", &["query"]),
    ("runtime", &["catalog", "query"]),
];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")), LADDER);
}
