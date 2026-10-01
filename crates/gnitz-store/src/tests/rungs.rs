//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the rungs are one crate, so
//! nothing else stops a `relation` file from naming `crate::ops` tomorrow. The
//! walk itself is `test_support::ladder`, shared with `gnitz-server`'s own
//! guard; what is stated here is only this crate's ladder.

use std::fs;
use std::path::Path;

use crate::test_support::{assert_ladder, rung_files};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The tiers, bottom first — the ladder `CLAUDE.md` and the `mod.rs` headers
/// state in prose. `ops` and `relation` share a tier: neither names the other,
/// and `read` may name both.
const LADDER: &[&[&str]] = &[&["schema"], &["storage"], &["ops", "relation"], &["read"]];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(SRC), LADDER);
}

/// The walk sees an edge the ladder withholds: `storage` does name `schema`.
#[test]
#[should_panic(expected = "storage/ names crate::schema")]
fn a_withheld_edge_fails_the_guard() {
    let mut ladder = LADDER.to_vec();
    ladder.swap(0, 1);
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
