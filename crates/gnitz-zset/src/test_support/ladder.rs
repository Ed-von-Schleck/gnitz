//! The rung-guard walk: a crate's layer ladder, asserted as source text.
//! Compiled here and as part of `gnitz-zset-testkit`.

use std::fs;
use std::path::{Path, PathBuf};

/// The crate-root names any file may use: the roots themselves and the test
/// scaffolding beside the rungs. A `#[macro_export]` macro is a crate-root name
/// too, wherever it is defined, so the crate's own may be named as well.
const UNLADDERED: [&str; 4] = ["lib", "main", "tests", "test_support"];

/// Assert that every rung under `src_root` names only the rungs beneath it.
///
/// `ladder` lists the tiers bottom first: a rung may name itself and the rungs
/// of every earlier tier, and rungs sharing a tier are incomparable. Every
/// entry of `src_root` must be a rung or one of `UNLADDERED`.
///
/// It reads the name each `crate::` path leads with, so a crate-root item other
/// than an exported macro and a grouped `crate::{…}` import are refused along
/// with a higher rung. It does
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
    let macros = exported_macros(src_root);
    let macros: Vec<&str> = macros.iter().map(String::as_str).collect();
    for (tier, rungs) in ladder.iter().enumerate() {
        for rung in *rungs {
            let allowed = [&[*rung], &ladder[..tier].concat()[..], &UNLADDERED[..], &macros[..]].concat();
            assert_rungs_name_only(src_root, &[rung], "crate", &allowed);
        }
    }
}

/// Assert that every path into crate `krate` from a file of `rungs` leads with
/// one of `allowed`: the modules of a crate beneath that these rungs may reach.
///
/// It reads paths as [`assert_ladder`] does: a grouped `krate::{…}` import is
/// refused and a module renamed on import is checked under its own name, but a
/// reach through an alias of `krate` itself goes unseen.
pub fn assert_rungs_name_only(src_root: &Path, rungs: &[&str], krate: &str, allowed: &[&str]) {
    for rung in rungs {
        for f in rung_files(src_root, rung) {
            let src = fs::read_to_string(&f).unwrap();
            for named in heads(&src, krate) {
                assert!(
                    allowed.contains(&named),
                    "{}: {rung}/ names {krate}::{named}, which is not beneath it",
                    f.display(),
                );
            }
        }
    }
}

/// Every `.rs` file of a rung, its directory walked recursively.
///
/// `tests/` and `benches/` directories are skipped: a unit test or benchmark of a
/// rung may name a higher one to build its fixture, which is not the dependency
/// the ladder is about.
pub fn rung_files(src_root: &Path, rung: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut dirs = vec![src_root.join(rung)];
    while let Some(dir) = dirs.pop() {
        for p in entries(&dir) {
            if p.is_dir() {
                if !p.ends_with("tests") && !p.ends_with("benches") {
                    dirs.push(p);
                }
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out
}

/// The names of the `#[macro_export]` macros defined anywhere under `src_root`.
fn exported_macros(src_root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut dirs = vec![src_root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for p in entries(&dir) {
            if p.is_dir() {
                dirs.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                let src = fs::read_to_string(&p).unwrap();
                for (at, _) in src.match_indices("#[macro_export]") {
                    let rest = &src[at..];
                    let Some(def) = rest.find("macro_rules! ") else {
                        continue;
                    };
                    let name = &rest[def + "macro_rules! ".len()..];
                    let name = &name[..name
                        .find(|c: char| c != '_' && !c.is_alphanumeric())
                        .unwrap_or(name.len())];
                    out.push(name.to_string());
                }
            }
        }
    }
    out
}

/// The name each `root::` path in `src` leads with; `""` for a grouped
/// `root::{…}`. A `root` that ends a longer identifier (`my_crate::`) is not
/// this one.
fn heads<'a>(src: &'a str, root: &'a str) -> impl Iterator<Item = &'a str> {
    let ident = |c: char| c == '_' || c.is_alphanumeric();
    src.match_indices(root).filter_map(move |(at, _)| {
        let rest = src[at + root.len()..].strip_prefix("::")?;
        (!src[..at].ends_with(ident)).then(|| &rest[..rest.find(|c: char| !ident(c)).unwrap_or(rest.len())])
    })
}

fn entries(dir: &Path) -> Vec<PathBuf> {
    let read = fs::read_dir(dir).unwrap_or_else(|e| panic!("a source walk must read {}: {e}", dir.display()));
    read.map(|e| e.unwrap().path()).collect()
}
