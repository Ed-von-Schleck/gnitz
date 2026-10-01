//! The rung-guard walk: a crate's layer ladder, asserted as source text.
//! Compiled here and as part of `gnitz-store-testkit`.

use std::fs;
use std::path::{Path, PathBuf};

/// The crate-root names any file may use: the roots themselves and the test
/// scaffolding beside the rungs.
const UNLADDERED: [&str; 5] = ["lib", "main", "tests", "test_support", "test_rng"];

/// Assert that every rung under `src_root` names only the rungs beneath it.
///
/// `ladder` lists the tiers bottom first: a rung may name itself and the rungs
/// of every earlier tier, and rungs sharing a tier are incomparable. Every
/// entry of `src_root` must be a rung or one of [`UNLADDERED`].
///
/// It reads the name each `crate::` path leads with, so a crate-root item and a
/// grouped `crate::{…}` import are refused along with a higher rung. It does
/// not see `super::`-relative reaches, so it is a regression guard and not a
/// proof.
pub fn assert_ladder(src_root: &Path, ladder: &[&[&str]]) {
    for p in entries(src_root) {
        let name = p.file_stem().unwrap().to_str().unwrap();
        assert!(
            ladder.concat().contains(&name) || UNLADDERED.contains(&name),
            "{name} is a module root the ladder does not place",
        );
    }
    for (tier, rungs) in ladder.iter().enumerate() {
        let beneath = ladder[..tier].concat();
        for rung in *rungs {
            for f in rung_files(src_root, rung) {
                let src = fs::read_to_string(&f).unwrap();
                for path in src.split("crate::").skip(1) {
                    let named = path.split(|c: char| c != '_' && !c.is_alphanumeric()).next().unwrap();
                    assert!(
                        named == *rung || beneath.contains(&named) || UNLADDERED.contains(&named),
                        "{}: {rung}/ names crate::{named}, which is not beneath it",
                        f.display(),
                    );
                }
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
