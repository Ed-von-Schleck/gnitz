//! The ladder guard: every rung names only the rungs beneath it.
//!
//! The crate graph cannot enforce this — the seven rungs are one crate, so
//! nothing else stops a `relation` file from naming `crate::ops` tomorrow.
//! Pinned as source text because no runtime assertion can observe the absence of
//! a call, and as a directory walk rather than an `include_str!` because the
//! rungs will gain files.
//!
//! `tests/` directories are skipped: a unit test asserting about a rung may name
//! a higher one to build its fixture, which is not the dependency the ladder is
//! about.

use std::fs;
use std::path::{Path, PathBuf};

/// Each rung and the rungs it may name — the table `CLAUDE.md` and the `mod.rs`
/// headers state in prose. Anything in the crate that is not in a rung's row and
/// is not the rung itself sits above or beside it, and is forbidden.
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

/// Every `.rs` file of a rung — the rung's directory walked recursively, or the
/// single file when the rung is one (`expr`). `tests/` is left out.
fn rung_files(rung: &str) -> Vec<PathBuf> {
    let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
    let mut out = Vec::new();
    let dir = root.join(rung);
    if dir.is_dir() {
        walk(&dir, &mut out);
    } else {
        out.push(root.join(format!("{rung}.rs")));
    }
    assert!(!out.is_empty(), "the rung guard found no files for {rung}");
    out
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        panic!("the rung guard must be able to read {}", dir.display());
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            if p.file_name().is_some_and(|n| n == "tests") {
                continue;
            }
            walk(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn every_rung_names_only_the_rungs_beneath_it() {
    for &(rung, allowed) in LADDER {
        let forbidden: Vec<&str> = LADDER
            .iter()
            .map(|&(name, _)| name)
            .filter(|name| *name != rung && !allowed.contains(name))
            .collect();
        for f in rung_files(rung) {
            let src = fs::read_to_string(&f).expect("a source file this crate compiles");
            for up in &forbidden {
                assert!(
                    !src.contains(&format!("crate::{up}")),
                    "{}: {rung}/ is not above {up} and must not name it",
                    f.display(),
                );
            }
        }
    }
}
