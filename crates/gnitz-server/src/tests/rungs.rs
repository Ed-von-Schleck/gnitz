//! The ladder guard: every rung names only the rungs beneath it, and neither of
//! the two lower rungs can end the process.
//!
//! The crate graph cannot enforce either — the three rungs are one crate. The
//! walk is `test_support::ladder`, shared with `gnitz-store`'s own guard; what
//! is stated here is only this crate's table.

use std::fs;
use std::path::Path;

use crate::test_support::{assert_ladder, rung_files};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

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
    assert_ladder(Path::new(SRC), LADDER);
}

/// What keeps `gnitz_fatal_abort!` out of the two lower rungs is the declaration
/// order in the crate root — `#[macro_use] mod fatal;` sits after both, and the
/// attribute reaches only code that follows it. This asserts the outcome, so a
/// reorder there cannot silently un-make it.
#[test]
fn the_rungs_below_runtime_never_end_the_process() {
    for rung in ["catalog", "query"] {
        for f in rung_files(Path::new(SRC), rung) {
            let src = fs::read_to_string(&f).expect("a source file this crate compiles");
            assert!(
                !src.contains("gnitz_fatal_abort!"),
                "{}: {rung}/ returns its errors; only runtime/ may end the process",
                f.display(),
            );
        }
    }
}
