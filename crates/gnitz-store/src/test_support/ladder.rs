//! The rung-guard walk: a crate's layer ladder, asserted as source text.
//! Compiled here and as part of `gnitz-store-testkit`.

use std::fs;
use std::path::{Path, PathBuf};

/// Assert that every rung under `src_root` names only the rungs beneath it.
///
/// `ladder` pairs each rung with the rungs it may name; anything in the table
/// that is not in a rung's row and is not the rung itself sits above or beside
/// it, and is forbidden. A table rather than an ordering because rungs can be
/// incomparable — neither naming the other, with a third above both. Every
/// module directory under `src_root` must be in the table.
///
/// It matches `crate::<up>` — which is why a grouped `crate::{…}` import is
/// refused outright — and does not see `super::`-relative reaches, so it is a
/// regression guard and not a proof.
pub fn assert_ladder(src_root: &Path, ladder: &[(&str, &[&str])]) {
    let rungs: Vec<&str> = ladder.iter().map(|&(name, _)| name).collect();
    for dir in entries(src_root).into_iter().filter(|p| p.is_dir()) {
        let name = dir.file_name().unwrap().to_str().unwrap();
        assert!(
            rungs.contains(&name) || name == "tests" || name == "test_support",
            "{name}/ is a module root the ladder does not place",
        );
    }
    for &(rung, allowed) in ladder {
        for f in rung_files(src_root, rung) {
            let src = fs::read_to_string(&f).unwrap();
            assert!(
                !src.contains("crate::{"),
                "{}: a grouped crate import hides its rungs from the ladder guard",
                f.display(),
            );
            for up in rungs.iter().filter(|up| **up != rung && !allowed.contains(up)) {
                assert!(
                    !src.contains(&format!("crate::{up}")),
                    "{}: {rung}/ is not above {up} and must not name it",
                    f.display(),
                );
            }
        }
    }
}

/// Every `.rs` file of a rung, its directory walked recursively.
///
/// `tests/` directories are skipped: a unit test asserting about a rung may name
/// a higher one to build its fixture, which is not the dependency the ladder is
/// about.
pub fn rung_files(src_root: &Path, rung: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut dirs = vec![src_root.join(rung)];
    while let Some(dir) = dirs.pop() {
        for p in entries(&dir) {
            if p.is_dir() {
                if !p.ends_with("tests") {
                    dirs.push(p);
                }
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out
}

fn entries(dir: &Path) -> Vec<PathBuf> {
    let read = fs::read_dir(dir).unwrap_or_else(|e| panic!("a source walk must read {}: {e}", dir.display()));
    read.map(|e| e.unwrap().path()).collect()
}
