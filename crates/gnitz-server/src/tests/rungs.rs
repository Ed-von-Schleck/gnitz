//! The ladder guard: every rung names only the rungs beneath it, and neither
//! rung below `runtime` ends the process.
//!
//! The crate graph cannot enforce this — the three rungs are one crate. The
//! walk is `gnitz-zset-testkit`'s, shared with the guards of the crates beneath; what
//! is stated here is only this crate's ladder.

use std::fs;
use std::path::Path;

use crate::test_support::{assert_ladder, rung_files};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The tiers, bottom first — the ladder `CLAUDE.md` and the crate root state
/// in prose.
const LADDER: &[&[&str]] = &[&["query"], &["catalog"], &["runtime"]];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(SRC), LADDER);
}

/// `catalog` and `query` return their errors, and the `runtime` call site that
/// owns the recovery decides. `gnitz_fatal_abort!` is out of their scope and
/// `clippy::exit` is denied on both; this covers the two spellings left.
#[test]
fn the_rungs_below_runtime_do_not_end_the_process() {
    for rung in ["query", "catalog"] {
        for f in rung_files(Path::new(SRC), rung) {
            let text = fs::read_to_string(&f).unwrap();
            for ender in ["libc::_exit", "process::abort"] {
                assert!(!text.contains(ender), "{}: {rung}/ calls {ender}", f.display());
            }
        }
    }
}
