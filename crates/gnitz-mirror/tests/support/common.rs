//! What both test binaries share: where a mirrored copy lives on disk, through
//! the engine's own path grammar.

use gnitz_store::relation::relation_dir;
use gnitz_store::relation::{ChildAddr, ChildKind};
use gnitz_store::schema::Slot;

/// Whether `tid`'s copy has a directory under `base_dir`.
pub fn has_copy(base_dir: &str, tid: u64) -> bool {
    std::path::Path::new(&relation_dir(base_dir, tid)).exists()
}

pub fn manifest_path(base_dir: &str, tid: u64) -> String {
    ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.manifest(&relation_dir(base_dir, tid))
}

pub fn has_manifest(base_dir: &str, tid: u64) -> bool {
    std::path::Path::new(&manifest_path(base_dir, tid)).exists()
}
