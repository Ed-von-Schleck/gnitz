//! The row-count guard: no crate outside this one writes a batch's row count.
//!
//! `Batch::count` and `MemBatch::count` are `pub(crate)`, so the compiler
//! already refuses this — the guard is what makes re-publishing either field a
//! test failure and not just a diff. A counted row whose regions were not all
//! written reads uninitialised arena bytes, silently.

use std::fs;
use std::path::{Path, PathBuf};

use crate::test_support::rs_files_under;

/// Every `.rs` file of every *other* workspace crate. A crate need not have both
/// `src/` and `tests/`, so each is walked only where it exists.
fn sibling_crate_files() -> Vec<PathBuf> {
    let this = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = this.parent().expect("a crate directory has a parent");
    let mut out = Vec::new();
    for e in fs::read_dir(workspace)
        .expect("the workspace directory is readable")
        .flatten()
    {
        let p = e.path();
        if !p.is_dir() || p == this || !p.join("Cargo.toml").is_file() {
            continue;
        }
        for d in [p.join("src"), p.join("tests")] {
            if d.is_dir() {
                out.extend(rs_files_under(&d, |_| true));
            }
        }
    }
    assert!(!out.is_empty(), "the row-count guard found no sibling crate sources");
    out
}

/// The assignment operator `line` applies to the `.count` at `at`, if any.
/// `.count(` (the `Iterator` method) and a bare read yield `None`.
fn assignment_after(line: &str, at: usize) -> Option<&'static str> {
    let rest = line[at + ".count".len()..].trim_start();
    if rest.starts_with("+=") {
        Some("+=")
    } else if rest.starts_with("-=") {
        Some("-=")
    } else if rest.starts_with('=') && !rest.starts_with("==") {
        Some("=")
    } else {
        None
    }
}

/// True when the receiver of the `.count` at `at` is `self` — a struct writing
/// its own field, which is not a batch write.
fn receiver_is_self(line: &str, at: usize) -> bool {
    let before = &line[..at];
    let Some(head) = before.strip_suffix("self") else {
        return false;
    };
    !head.ends_with(|c: char| c.is_alphanumeric() || c == '_')
}

#[test]
fn no_other_crate_writes_a_batch_row_count() {
    for f in sibling_crate_files() {
        let src = fs::read_to_string(&f).expect("a source file the workspace compiles");
        for (n, line) in src.lines().enumerate() {
            for (at, _) in line.match_indices(".count") {
                let Some(op) = assignment_after(line, at) else {
                    continue;
                };
                assert!(
                    receiver_is_self(line, at),
                    "{}:{}: `.count {op}` writes a row count from outside gnitz-store — \
                     append rows through push_key_row / push_zero_filled_row / BatchBuilder, \
                     each of which advances the count and every region together\n  {}",
                    f.display(),
                    n + 1,
                    line.trim(),
                );
            }
        }
    }
}
