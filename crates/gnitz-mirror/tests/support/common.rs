//! What both test binaries share: the one test lock, and where a mirrored copy
//! lives on disk, through the engine's own path grammar.

use std::sync::{Mutex, MutexGuard};

use gnitz_store::relation::relation_dir;
use gnitz_store::relation::{ChildAddr, ChildKind};
use gnitz_store::schema::Slot;

/// Every test in a binary takes this lock.
///
/// `cargo test` runs a target's tests as threads of one process, so the state a
/// mirror open touches process-wide is shared between them: the one-shot fault
/// seams, the `io_uring` verdict latched once per process, and the environment
/// variables an open re-reads. A parallel test would be racing all three.
static SERIAL: Mutex<()> = Mutex::new(());

pub fn serial() -> MutexGuard<'static, ()> {
    // A test that panics while holding it must not fail every later test with a
    // poisoned lock; the state it protects is re-established by the next open.
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// The directory one mirrored copy lives in.
pub fn copy_dir(base_dir: &str, tid: u64) -> String {
    relation_dir(base_dir, tid)
}

pub fn manifest_path(base_dir: &str, tid: u64) -> String {
    ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.manifest(&copy_dir(base_dir, tid))
}

pub fn has_manifest(base_dir: &str, tid: u64) -> bool {
    std::path::Path::new(&manifest_path(base_dir, tid)).exists()
}
