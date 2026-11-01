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
use gnitz_foundation::posix_io;
use gnitz_wire::WireFault;

/// The longest entry name a backup takes.
const MAX_NAME_LEN: usize = 128;

/// BACKUP into the entry `name` of the backup directory. The copy is made under
/// a name no entry takes and renamed onto `name` once complete, so an entry of
/// the backup directory is always a whole backup: an interrupted one leaves only
/// its partial copy behind, which a later BACKUP of that name refuses to reuse.
pub(super) async fn backup(shared: &Shared, name: &[u8]) -> Result<(), WireFault> {
    let root = shared
        .backup_dir
        .as_deref()
        .ok_or_else(|| "the server was started without --backup-dir".to_string())?;
    let name = entry_name(name)?;
    let (dest, partial) = (format!("{root}/{name}"), format!("{root}/.{name}.partial"));
    let catalog = shared.catalog_rwlock.read().await;
    if Path::new(&dest).exists() {
        return Err(format!("backup '{name}' already exists").into());
    }
    match fs::create_dir(&partial) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(
                format!("backup '{name}' is being taken, or an interrupted one left '{partial}' behind").into(),
            );
        }
        Err(e) => return Err(format!("create backup '{partial}': {e}").into()),
    }
    let mut copy_at = partial.as_str();
    let result = shared.committer.backup(partial.clone()).await.and_then(|()| {
        let synced = |dir: &str| posix_io::fsync_dir(dir).map_err(|e| format!("fsync '{dir}': {e}"));
        // The copy's own entries, durable before the name makes it a backup.
        synced(&partial)?;
        posix_io::rename_noreplace(&partial, &dest).map_err(|e| format!("name backup '{dest}': {e}"))?;
        copy_at = &dest;
        synced(root).map_err(WireFault::from)
    });
    drop(catalog);
    match &result {
        Ok(()) => gnitz_info!("backup '{}' complete", dest),
        // Only this request made the copy, and it is no backup: a name that
        // could not be made durable is taken back with it.
        Err(_) => {
            if let Err(e) = fs::remove_dir_all(copy_at) {
                gnitz_warn!("backup: failed to remove the incomplete '{}': {}", copy_at, e);
            }
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
