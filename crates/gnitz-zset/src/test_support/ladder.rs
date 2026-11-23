//! The rung-guard walk: a crate's layer ladder, asserted as source text.
//! Compiled here and as part of `gnitz-zset-testkit`.

use std::fs;
use std::path::{Path, PathBuf};

/// The entries of `src/` that are not rungs: the crate roots and the test
/// scaffolding. A file may name the scaffolding; a `#[macro_export]` macro is a
/// crate-root name wherever it is defined, so a rung's own may be named too.
const UNLADDERED: [&str; 4] = ["lib", "main", "tests", "test_support"];

/// Assert that every rung under `src_root` names only the rungs beneath it.
///
/// `ladder` lists the tiers bottom first: a rung may name itself and the rungs
/// of every earlier tier, and rungs sharing a tier are incomparable. Every
/// entry of `src_root` must be a rung or one of `UNLADDERED`.
///
/// It reads the name each `crate::` path leads with, so a crate-root item other
/// than an exported macro and a grouped `crate::{…}` import are refused along
/// with a higher rung. It does not see `super::`-relative reaches, so it is a
/// regression guard and not a proof.
pub fn assert_ladder(src_root: impl AsRef<Path>, ladder: &[&[&str]]) {
    let src_root = src_root.as_ref();
    let rungs = ladder.concat();
    for p in entries(src_root) {
        let name = p.file_stem().unwrap().to_str().unwrap();
        assert!(
            rungs.contains(&name) || UNLADDERED.contains(&name),
            "{name} is a module root the ladder does not place",
        );
    }
    let macros: Vec<String> = rungs
        .iter()
        .flat_map(|rung| rs_files(src_root.join(rung)))
        .flat_map(exported_macros)
        .collect();
    let macros: Vec<&str> = macros.iter().map(String::as_str).collect();
    for (tier, tier_rungs) in ladder.iter().enumerate() {
        for rung in *tier_rungs {
            let allowed = [&[*rung], &ladder[..tier].concat()[..], &UNLADDERED[..], &macros[..]].concat();
            assert_names_only(src_root.join(rung), "crate", &allowed);
        }
    }
}

/// Assert that every path into crate `krate` from a file under `dir` leads with
/// one of `allowed`: the modules of a crate beneath that `dir` may reach.
///
/// It reads paths as [`assert_ladder`] does: a grouped `krate::{…}` import is
/// refused and a module renamed on import is checked under its own name, but a
/// reach through an alias of `krate` itself goes unseen.
pub fn assert_names_only(dir: impl AsRef<Path>, krate: &str, allowed: &[&str]) {
    for f in rs_files(dir) {
        let src = fs::read_to_string(&f).unwrap();
        for named in heads(&src, krate) {
            assert!(
                allowed.contains(&named),
                "{}: names {krate}::{named}, which is not beneath it",
                f.display(),
            );
        }
    }
}

/// Every `.rs` file under `dir`, walked recursively.
///
/// `tests/`, `benches/` and `fuzz/` directories are skipped: a unit test,
/// benchmark or fuzz target may name a higher rung to build its fixture, which
/// is not the dependency the ladder is about.
pub fn rs_files(dir: impl AsRef<Path>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut dirs = vec![dir.as_ref().to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for p in entries(&dir) {
            if p.is_dir() {
                if !["tests", "benches", "fuzz"].iter().any(|d| p.ends_with(d)) {
                    dirs.push(p);
                }
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out
}

/// The names of the `#[macro_export]` macros defined in the file at `path`.
fn exported_macros(path: PathBuf) -> Vec<String> {
    const RULES: &str = "macro_rules! ";
    let src = fs::read_to_string(path).unwrap();
    src.match_indices("#[macro_export]")
        .filter_map(|(at, _)| {
            let rest = &src[at..];
            let name = &rest[rest.find(RULES)? + RULES.len()..];
            let name = &name[..name
                .find(|c: char| c != '_' && !c.is_alphanumeric())
                .unwrap_or(name.len())];
            (!name.is_empty()).then(|| name.to_string())
        })
        .collect()
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
