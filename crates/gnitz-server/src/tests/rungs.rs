//! The ladder guard: every rung names only the rungs beneath it, and neither
//! rung below `runtime` ends the process.
//!
//! The crate graph cannot enforce this — the three rungs are one crate. The
//! walk is `gnitz-zset-testkit`'s; what is stated here is only this crate's
//! ladder.

use std::fs;

use crate::test_support::{assert_ladder, rs_files};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The rungs, bottom first — the ladder `CLAUDE.md` and the crate root state
/// in prose.
const LADDER: &[&str] = &["query", "catalog", "runtime"];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(SRC, LADDER);
}

/// `catalog` and `query` return their errors, and the `runtime` call site that
/// owns the recovery decides. `clippy::exit` is denied on both, and
/// `gnitz_fatal_abort!` is out of scope there; this covers the direct spellings
/// of an abort.
#[test]
fn the_rungs_below_runtime_do_not_end_the_process() {
    for rung in ["query", "catalog"] {
        for f in rs_files(format!("{SRC}/{rung}")) {
            let text = fs::read_to_string(&f).unwrap();
            for ender in ["libc::_exit", "libc::abort", "process::abort"] {
                assert!(!text.contains(ender), "{}: {rung}/ calls {ender}", f.display());
            }
        }
    }
}
