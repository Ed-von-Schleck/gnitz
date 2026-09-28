//! Where a mirrored copy lives on disk, through the engine's own path grammar.

use gnitz_store::relation::relation_dir;
use gnitz_store::relation::{ChildAddr, ChildKind};
use gnitz_store::schema::Slot;

/// The directory one mirrored copy lives in.
pub fn copy_dir(base_dir: &str, tid: u64) -> String {
    relation_dir(base_dir, tid as i64)
}

pub fn manifest_path(base_dir: &str, tid: u64) -> String {
    ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.manifest(&copy_dir(base_dir, tid))
}

pub fn has_manifest(base_dir: &str, tid: u64) -> bool {
    std::path::Path::new(&manifest_path(base_dir, tid)).exists()
}
