//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the rungs are one crate, so
//! nothing else stops a `relation` file from naming `crate::ops` tomorrow. The
//! walk itself is `test_support::ladder`, shared with `gnitz-server`'s own
//! guard; what is stated here is only this crate's table.

use std::fs;
use std::path::Path;

use crate::test_support::{assert_ladder, rung_files};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// Each rung and the rungs it may name — the table `CLAUDE.md` and the `mod.rs`
/// headers state in prose.
///
/// `relation` and `ops` are incomparable: neither names the other, and `read` is
/// the rung that may name both. That is why this is a table and not an ordering.
const LADDER: &[(&str, &[&str])] = &[
    ("schema", &[]),
    ("storage", &["schema"]),
    ("ops", &["schema", "storage"]),
    ("relation", &["schema", "storage"]),
    ("read", &["schema", "storage", "ops", "relation"]),
];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(SRC), LADDER);
}

/// The walk sees an edge the table withholds: `storage` does name `schema`.
#[test]
#[should_panic(expected = "storage/ is not above schema")]
fn a_withheld_edge_fails_the_guard() {
    let mut ladder = LADDER.to_vec();
    ladder[1].1 = &[];
    assert_ladder(Path::new(SRC), &ladder);
}

/// Inside `storage`, the representation layer never names the LSM above it —
/// by its module path, or through the facade that re-exports LSM types.
#[test]
fn storage_repr_never_names_the_lsm() {
    for f in rung_files(Path::new(SRC), "storage/repr") {
        let text = fs::read_to_string(&f).unwrap();
        let through_facade = text
            .split("crate::storage::")
            .skip(1)
            .any(|rest| !rest.starts_with("repr::") && !rest.starts_with("error::"));
        assert!(
            !text.contains("lsm::") && !text.contains("super::super::") && !through_facade,
            "{} names storage above repr",
            f.display(),
        );
    }
}
