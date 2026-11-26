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

use gnitz_foundation::{gnitz_info, gnitz_warn};
use std::fs;
use std::path::Path;

use super::Shared;
use crate::runtime::data_format::DATA_FORMAT;
use crate::runtime::sal::Read;
use gnitz_foundation::fault::Switch;
use gnitz_foundation::posix_io;
use gnitz_wire::WireFault;

/// The longest entry name a backup takes.
const MAX_NAME_LEN: usize = 128;

/// `GNITZ_INJECT_BACKUP_ABORT`: abort the master between the workers' copies and
/// its own, as a crash part-way through a backup would.
static BACKUP_ABORT: Switch = Switch::new("GNITZ_INJECT_BACKUP_ABORT");

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
    // Stamped as a boot stamps a data directory of this build's format, ahead
    // of the committer's copy, which holds up every write while it runs.
    let copied = match gnitz_store::relation::write_stamp(&partial, DATA_FORMAT) {
        Ok(()) => shared.committer.backup(partial.clone()).await,
        Err(e) => Err(e.into()),
    };
    let result = copied.and_then(|()| {
        let synced = |dir: &str| posix_io::fsync_dir(dir).map_err(|e| format!("fsync '{dir}': {e}"));
        // The copy's own entries, durable before the name makes it a backup.
        synced(&partial)?;
        rename_noreplace(&partial, &dest).map_err(|e| format!("name backup '{dest}': {e}"))?;
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

/// A backup's copy, run by the committer right behind a checkpoint: every
/// worker puts the stores it published into the data directory `dir`, then the
/// master puts its system families', each store with the entries that reach it
/// durable. `dir` exists and is stamped; every store under it is made here.
pub(super) async fn copy_published(shared: &Shared, dir: &str) -> Result<(), WireFault> {
    shared.dispatcher.command(Read::Backup { dir }).await?;
    if BACKUP_ABORT.armed() {
        gnitz_fatal_abort!("injected backup abort");
    }
    shared.cat().registry.link_published_into(dir)?;
    gnitz_info!("backup copied into {}", dir);
    Ok(())
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

/// Rename `from` to `to`, refused with `AlreadyExists` where `to` exists:
/// `std::fs::rename` replaces a file, or an empty directory, in its place.
fn rename_noreplace(from: &str, to: &str) -> std::io::Result<()> {
    let (from, to) = (std::ffi::CString::new(from)?, std::ffi::CString::new(to)?);
    // SAFETY: both paths are NUL-terminated and outlive the call.
    let r = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if r != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/backup.rs"]
mod tests;
