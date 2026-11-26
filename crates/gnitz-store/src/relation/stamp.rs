//! The data directory's format stamp: the file naming the format a data
//! directory's files are in and the release that last opened it, read before
//! any other file in the directory is.
//!
//! The stamp's own layout never changes, so every build reads every stamp,
//! one written by a later release included.

use std::fs;
use std::io;

use gnitz_foundation::posix_io::write_file_durable;
use gnitz_trace::runs::SHARD_VERSION;
use gnitz_wire::{RELEASE, format_digest};

use super::dirs::{relations_dir, subdir_names};
use crate::storage::MANIFEST_VERSION;

/// `<base_dir>/FORMAT`.
const STAMP_FILENAME: &str = "FORMAT";

/// The stamp's first line.
const STAMP_HEADER: &str = "gnitz data directory";

/// Bumped by hand for any change to the names of the files and directories a
/// store keeps under a data directory, which no other format word covers.
const DIR_LAYOUT: u64 = 1;

/// The format of what `gnitz-store` keeps under a data directory: the shards,
/// the store manifests and the tree they sit in. A host stamps its directory
/// with a [`format_digest`] of this and the words of what it keeps beside them.
pub const STORE_FORMAT: u64 = format_digest(&[SHARD_VERSION, MANIFEST_VERSION, DIR_LAYOUT]);

/// Who wrote a data directory of another format.
#[derive(Debug, PartialEq, Eq)]
pub enum Foreign {
    /// A build of a release from before data directories were stamped.
    Unstamped,
    /// A build of `release`.
    Stamped { release: String },
}

struct Stamp {
    format: u64,
    release: String,
}

fn stamp_path(base_dir: &str) -> String {
    format!("{base_dir}/{STAMP_FILENAME}")
}

fn encode(format: u64, release: &str) -> String {
    format!("{STAMP_HEADER}\nformat {format:016x}\nrelease {release}\n")
}

/// The stamp `text` holds; `None` unless its first line is [`STAMP_HEADER`]
/// and it names a format and a release. Lines it does not know are skipped.
fn decode(text: &str) -> Option<Stamp> {
    let mut lines = text.lines();
    if lines.next()? != STAMP_HEADER {
        return None;
    }
    let (mut format, mut release) = (None, None);
    for line in lines {
        if let Some(hex) = line.strip_prefix("format ") {
            format = u64::from_str_radix(hex, 16).ok();
        } else if let Some(r) = line.strip_prefix("release ") {
            release = Some(r.to_string());
        }
    }
    Some(Stamp { format: format?, release: release? })
}

fn read_stamp(base_dir: &str) -> Result<Option<Stamp>, String> {
    let path = stamp_path(base_dir);
    match fs::read_to_string(&path) {
        Ok(text) => decode(&text)
            .map(Some)
            .ok_or_else(|| format!("the format stamp '{path}' is damaged")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("read the format stamp '{path}': {e}")),
    }
}

/// Whether `base_dir` holds a relation directory.
fn holds_relations(base_dir: &str) -> Result<bool, String> {
    Ok(!subdir_names(&relations_dir(base_dir))?.is_empty())
}

/// What `base_dir` holds against the format its host reads.
pub(super) enum Assessed {
    /// Stamped with the format and this release.
    Current,
    /// Nothing yet, or stamped with the format by another release: owed a stamp.
    Owed,
    /// Files in a format the host does not read.
    Foreign(Foreign),
}

pub(super) fn assess(base_dir: &str, format: u64) -> Result<Assessed, String> {
    Ok(match read_stamp(base_dir)? {
        Some(s) if s.format != format => Assessed::Foreign(Foreign::Stamped { release: s.release }),
        Some(s) if s.release == RELEASE => Assessed::Current,
        Some(_) => Assessed::Owed,
        // A stamping host stamps before it creates a relation directory.
        None if holds_relations(base_dir)? => Assessed::Foreign(Foreign::Unstamped),
        None => Assessed::Owed,
    })
}

/// Stamp `base_dir` with `format` and this release, durably: the write syncs
/// `base_dir`, so every entry made in it before the stamp is durable with it.
/// A host writing a data directory of its own format, as a backup, stamps it
/// as a boot does.
pub fn write_stamp(base_dir: &str, format: u64) -> Result<(), String> {
    let path = stamp_path(base_dir);
    write_file_durable(&path, encode(format, RELEASE).as_bytes(), 0o644).map_err(|e| format!("write '{path}': {e}"))
}

#[cfg(test)]
#[path = "tests/stamp.rs"]
mod tests;
