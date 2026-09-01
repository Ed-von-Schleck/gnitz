//! The rung-guard walk: one crate's layer ladder, asserted as source text.
//!
//! **This file is compiled twice, from one source** — as a `cfg(test)` module
//! here and through a `#[path]` in `gnitz-server`'s own `test_support`. Both
//! crates are laid out as a ladder of rungs whose ordering the crate graph
//! cannot enforce, and one walk keeps the two guards from drifting on what
//! counts as a reach.
//!
//! It is deliberately not part of [`super::shared`]: that file is store-level
//! *fixtures*, and is compiled into `gnitz-store-testkit` as library code. A
//! ladder guard is neither, and keeping it out leaves the testkit untouched.

use std::fs;
use std::path::{Path, PathBuf};

/// Assert that every rung under `src_root` names only the rungs beneath it.
///
/// `ladder` pairs each rung with the rungs it may name; anything in the table
/// that is not in a rung's row and is not the rung itself sits above or beside
/// it, and is forbidden. A table rather than an ordering because rungs can be
/// incomparable — neither naming the other, with a third above both.
///
/// It matches `crate::{up}` and does not see `super::`-relative reaches, so it
/// is a regression guard and not a proof. Pinned as source text because no
/// runtime assertion can observe the absence of a call, and as a directory walk
/// rather than an `include_str!` because the rungs will gain files.
pub fn assert_ladder(src_root: &Path, ladder: &[(&str, &[&str])]) {
    for &(rung, allowed) in ladder {
        let forbidden: Vec<&str> = ladder
            .iter()
            .map(|&(name, _)| name)
            .filter(|name| *name != rung && !allowed.contains(name))
            .collect();
        for f in rung_files(src_root, rung) {
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

/// Every `.rs` file of a rung — the rung's directory walked recursively, or the
/// single file when the rung is one.
///
/// `tests/` directories are skipped: a unit test asserting about a rung may name
/// a higher one to build its fixture, which is not the dependency the ladder is
/// about.
pub fn rung_files(src_root: &Path, rung: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let dir = src_root.join(rung);
    if dir.is_dir() {
        walk(&dir, &mut out);
    } else {
        out.push(src_root.join(format!("{rung}.rs")));
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
