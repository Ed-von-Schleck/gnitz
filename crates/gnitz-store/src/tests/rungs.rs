//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the rungs are one crate, so nothing
//! else stops a `relation` file from naming `crate::read` tomorrow. The walk
//! itself is `gnitz-zset-testkit`'s, shared with the kernel's and the server's
//! own guards; what is stated here is only this crate's ladder. Everything
//! beneath it — the schema, the batch and its cursor, the operators — is
//! `gnitz-zset`, which the crate graph keeps below.

use std::fs;
use std::path::Path;

use crate::test_support::{assert_ladder, rung_files};

const SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

/// The tiers, bottom first — the ladder `CLAUDE.md` and the `mod.rs` headers
/// state in prose.
const LADDER: &[&[&str]] = &[&["storage"], &["relation"], &["read"]];

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    assert_ladder(Path::new(SRC), LADDER);
}

/// The store runs the kernel's `algebra` and never its `stream` operators: those
/// read a circuit's traces, and only `gnitz-server` holds a circuit.
#[test]
fn no_rung_names_a_stream_operator() {
    for rung in LADDER.concat() {
        for f in rung_files(Path::new(SRC), rung) {
            let text = fs::read_to_string(&f).unwrap();
            assert!(!text.contains("stream::"), "{} names gnitz_zset::stream", f.display());
        }
    }
}
