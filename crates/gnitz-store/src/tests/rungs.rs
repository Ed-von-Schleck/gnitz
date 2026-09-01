//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the seven rungs are one crate, so
//! nothing else stops a `relation` file from naming `crate::ops` tomorrow. The
//! walk itself is `test_support::ladder`, shared with `gnitz-server`'s own
//! guard; what is stated here is only this crate's table.

use std::path::Path;

use crate::test_support::assert_ladder;

/// Each rung and the rungs it may name — the table `CLAUDE.md` and the `mod.rs`
/// headers state in prose.
///
/// `relation` and `ops` are incomparable: neither names the other, and `read` is
/// the rung that may name both. That is why this is a table and not an ordering.
const LADDER: &[(&str, &[&str])] = &[
    ("foundation", &[]),
    ("schema", &["foundation"]),
    ("storage", &["foundation", "schema"]),
    ("expr", &["foundation", "schema", "storage"]),
    ("ops", &["foundation", "schema", "storage", "expr"]),
    ("relation", &["foundation", "schema", "storage"]),
    ("read", &["foundation", "schema", "storage", "expr", "ops", "relation"]),
];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")), LADDER);
}
