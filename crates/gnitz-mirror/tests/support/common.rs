//! What both test binaries share: where a mirrored copy lives on disk, through
//! the engine's own path grammar.

use gnitz_store::relation::relation_dir;
use gnitz_store::relation::{ChildAddr, ChildKind};
use gnitz_zset::schema::Slot;

const ROWS: ChildAddr = ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO };

/// Whether `tid`'s copy has a directory under `base_dir`.
pub fn has_copy(base_dir: &str, tid: u64) -> bool {
    std::path::Path::new(&relation_dir(base_dir, tid)).exists()
}

/// The directory `tid`'s rows live in.
fn rows_dir(base_dir: &str, tid: u64) -> String {
    ROWS.dir(&relation_dir(base_dir, tid))
}

pub fn manifest_path(base_dir: &str, tid: u64) -> String {
    ROWS.manifest(&relation_dir(base_dir, tid))
}

pub fn has_manifest(base_dir: &str, tid: u64) -> bool {
    std::path::Path::new(&manifest_path(base_dir, tid)).exists()
}

/// Put a regular file where `tid`'s rows directory belongs, moving the
/// directory aside: an open of the copy fails, and so does anything that writes
/// into it. [`unblock_copy`] undoes it.
pub fn block_copy(base_dir: &str, tid: u64) {
    let child = rows_dir(base_dir, tid);
    std::fs::rename(&child, aside(base_dir, tid)).expect("the copy's rows directory");
    std::fs::write(&child, b"not a directory").expect("block the rows path");
}

pub fn unblock_copy(base_dir: &str, tid: u64) {
    let child = rows_dir(base_dir, tid);
    std::fs::remove_file(&child).unwrap();
    std::fs::rename(aside(base_dir, tid), &child).unwrap();
}

fn aside(base_dir: &str, tid: u64) -> String {
    format!("{base_dir}/aside_{tid}")
}
