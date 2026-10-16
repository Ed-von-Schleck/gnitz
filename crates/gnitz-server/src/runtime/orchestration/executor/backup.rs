//! BACKUP: the data directory copied, at a checkpoint, into an entry of the
//! backup directory the server was started with.
//!
//! The copy is a data directory a server boots on, at any worker count: every
//! store published at the checkpoint, hard-linked where the backup directory
//! shares the data directory's filesystem and copied where it does not, and no
//! log — the checkpoint left nothing in it a restart would need. It holds every
//! write acknowledged before the request, and is one cut: the committer runs the
//! checkpoint and the copy back to back, so no write and no other checkpoint
//! lands between them, and the catalog guard keeps every DDL out.

use std::fs;
use std::path::Path;

use super::Shared;
use crate::runtime::committer::{BackupJob, CommitRequest};
use crate::runtime::reactor::oneshot;
use gnitz_wire::WireFault;

/// The longest entry name a backup takes.
const MAX_NAME_LEN: usize = 128;

/// BACKUP into the entry `name` of the backup directory.
pub(super) async fn backup(shared: &Shared, name: &[u8]) -> Result<(), WireFault> {
    let root = shared
        .backup_dir
        .as_deref()
        .ok_or_else(|| "the server was started without --backup-dir".to_string())?;
    let name = entry_name(name)?;
    let dest = Path::new(root).join(name);
    let dest = dest
        .to_str()
        .ok_or_else(|| "a backup path that is not UTF-8".to_string())?
        .to_string();
    if shared.draining.get() {
        return Err("server shutting down".to_string().into());
    }
    let catalog = shared.catalog_rwlock.read().await;
    match fs::create_dir(&dest) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(format!("backup '{name}' already exists").into());
        }
        Err(e) => return Err(format!("create backup '{dest}': {e}").into()),
    }
    let (done, rx) = oneshot::channel();
    shared
        .committer_tx
        .send(CommitRequest::Backup(BackupJob { dir: dest.clone(), done }));
    let result = rx.await;
    drop(catalog);
    if result.is_err() {
        // Only this request made the directory, and it is no backup.
        if let Err(e) = fs::remove_dir_all(&dest) {
            gnitz_warn!("backup: failed to remove the incomplete '{}': {}", dest, e);
        }
    }
    result
}

/// `name` as an entry of the backup directory: one path component of the
/// characters a file name takes anywhere, not hidden.
fn entry_name(name: &[u8]) -> Result<&str, WireFault> {
    let refused = || {
        WireFault::from(format!(
            "a backup name is 1 to {MAX_NAME_LEN} of the characters A-Z a-z 0-9 . _ -, not starting with '.'"
        ))
    };
    let name = std::str::from_utf8(name).map_err(|_| refused())?;
    let ok = (1..=MAX_NAME_LEN).contains(&name.len())
        && !name.starts_with('.')
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    ok.then_some(name).ok_or_else(refused)
}

#[cfg(test)]
#[path = "tests/backup.rs"]
mod tests;
