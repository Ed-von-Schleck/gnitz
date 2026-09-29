//! The rung-guard walk: a crate's layer ladder, asserted as source text, and the
//! source walk every source-text guard reads through. Compiled here and as part
//! of `gnitz-store-testkit`.

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
    let dir = src_root.join(rung);
    let out = match dir.is_dir() {
        true => rs_files_under(&dir, |p| p.file_name().is_none_or(|n| n != "tests")),
        false => vec![src_root.join(format!("{rung}.rs"))],
    };
    assert!(!out.is_empty(), "the rung guard found no files for {rung}");
    out
}

/// Every `.rs` file under `dir`, recursively, entering the sub-directories
/// `enter` accepts. An unreadable directory panics.
pub fn rs_files_under(dir: &Path, enter: impl Fn(&Path) -> bool + Copy) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        panic!("a source walk must be able to read {}", dir.display());
    };
    let mut out = Vec::new();
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            if enter(&p) {
                out.extend(rs_files_under(&p, enter));
            }
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out
}
