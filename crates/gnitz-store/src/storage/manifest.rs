//! One store directory: the manifest serde, the directory's file names and the
//! primitives over them. A store owns its directory, so every shard and staging
//! file in it is the store's.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;

use gnitz_foundation::posix_io::{create_dir, fsync_dir};
use gnitz_wire::MAX_PK_BYTES;
use gnitz_wire::{decode_all, Reader, Writer};
use gnitz_zset::repr::StorageError;
use gnitz_zset::schema::key::PkBuf;

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION: u64 = 17;

const MANIFEST_FILE: &str = "manifest.bin";
/// The one file a manifest is staged under before its rename.
const STAGING_FILE: &str = "manifest.bin.tmp";

/// Every shard basename's prefix.
pub(super) const SHARD_PREFIX: &str = "shard_";

/// What a shard index publishes and reopens from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ShardSet {
    /// The shard index's `R`. Stored, not derived: a guard of one distinct key
    /// outgrows `R` and cannot be split, so no shard size recovers it.
    pub run_bytes: u64,
    pub entries: Vec<ManifestEntry>,
}

/// One store's published shard set and the counters that must survive a
/// restart.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Manifest {
    /// The owner's word for this publish, read back at the next open.
    pub checkpoint_mark: u64,
    /// Bytes the store's owner publishes with its rows; opaque here.
    pub caller_record: Vec<u8>,
    pub shards: ShardSet,
}

/// One live shard. Its PK bounds are re-derived at open, so not stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManifestEntry {
    /// The seq that names the shard.
    pub seq: u64,
    /// The highest seq whose rows the shard holds.
    pub newest: u64,
    /// The shard's level in the index, 0 at the top.
    pub level: u64,
    /// The guard the shard sits under.
    pub guard_key: PkBuf,
}

/// Encode `m` as the file [`read`] decodes.
pub(crate) fn encode(m: &Manifest) -> Vec<u8> {
    let mut w = Writer::new();
    w.u64(MAGIC)
        .u64(VERSION)
        .u64(m.checkpoint_mark)
        .u64(m.shards.run_bytes)
        .bytes32(&m.caller_record);
    for e in &m.shards.entries {
        w.u64(e.seq).u64(e.newest).u64(e.level).bytes32(e.guard_key.pk_bytes());
    }
    let mut buf = w.into_vec();
    let digest = gnitz_wire::checksum(&buf);
    buf.extend_from_slice(&digest.to_le_bytes());
    buf
}

fn decode(buf: &[u8]) -> Result<Manifest, StorageError> {
    const TRUNCATED: StorageError = StorageError::Corrupt("manifest truncated");
    let truncated = |_| TRUNCATED;
    let (covered, digest) = buf.split_last_chunk::<8>().ok_or(TRUNCATED)?;
    let mut r = Reader::new(covered);
    if r.u64().map_err(truncated)? != MAGIC {
        return Err(StorageError::Corrupt("manifest magic"));
    }
    if r.u64().map_err(truncated)? != VERSION {
        return Err(StorageError::Corrupt("manifest version"));
    }
    if gnitz_wire::checksum(covered) != u64::from_le_bytes(*digest) {
        return Err(StorageError::Corrupt("manifest checksum"));
    }
    decode_all(&covered[r.pos()..], "manifest body", decode_body).map_err(|_| StorageError::Corrupt("manifest body"))
}

fn decode_body(r: &mut Reader) -> Result<Manifest, String> {
    let checkpoint_mark = r.u64()?;
    let run_bytes = r.u64()?;
    let caller_record = r.bytes32()?.to_vec();
    let mut m = Manifest {
        checkpoint_mark,
        caller_record,
        shards: ShardSet { run_bytes, entries: Vec::new() },
    };
    while r.remaining() > 0 {
        let (seq, newest, level) = (r.u64()?, r.u64()?, r.u64()?);
        let key = r.bytes32()?;
        if key.len() > MAX_PK_BYTES {
            return Err("guard key too wide".into());
        }
        m.shards.entries.push(ManifestEntry {
            seq,
            newest,
            level,
            guard_key: PkBuf::from_bytes(key),
        });
    }
    Ok(m)
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

/// The manifest path of the store at `store_dir`.
pub(crate) fn manifest_path(store_dir: &str) -> String {
    format!("{store_dir}/{MANIFEST_FILE}")
}

/// The path a manifest is staged under in the store at `dir`.
pub(super) fn staging_path(dir: &str) -> String {
    format!("{dir}/{STAGING_FILE}")
}

/// The basename of the shard drawn at `seq`: `shard_{seq}.db`.
pub(super) fn shard_name(seq: u64) -> String {
    format!("{SHARD_PREFIX}{seq}.db")
}

/// The path of the shard drawn at `seq` in the store at `dir`.
pub(crate) fn shard_path(dir: &str, seq: u64) -> String {
    format!("{dir}/{}", shard_name(seq))
}

/// `r`, with `NotFound` read as success.
fn absent_ok(r: io::Result<()>) -> io::Result<()> {
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}

/// Remove the staging file and every shard not in `keep` from `dir`.
pub(super) fn remove_stale_files(dir: &str, keep: &HashSet<String>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name == STAGING_FILE || (name.starts_with(SHARD_PREFIX) && !keep.contains(name)) {
            absent_ok(fs::remove_file(entry.path()))?;
        }
    }
    Ok(())
}

/// Every file in `dir` named as a shard, with the level `dir`'s intact manifest
/// places it at — `None` for one the manifest does not name.
pub(crate) fn shard_files(dir: &str) -> Result<Vec<(String, Option<u64>)>, StorageError> {
    let entries = read_intact(dir)?.map_or(Vec::new(), |m| m.shards.entries);
    let levels: HashMap<String, u64> = entries.iter().map(|e| (shard_name(e.seq), e.level)).collect();
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let Some(name) = name.to_str().filter(|n| n.starts_with(SHARD_PREFIX)) else {
            continue;
        };
        files.push((format!("{dir}/{name}"), levels.get(name).copied()));
    }
    Ok(files)
}

/// Read and decode `dir`'s manifest. `Ok(None)` when it does not exist yet;
/// `Err` on damage or a failed read.
pub(crate) fn read(dir: &str) -> Result<Option<Manifest>, StorageError> {
    match fs::read(manifest_path(dir)) {
        Ok(buf) => decode(&buf).map(Some),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// [`read`], with damage read as absence: only a failed read is an error.
pub(crate) fn read_intact(dir: &str) -> Result<Option<Manifest>, StorageError> {
    match read(dir) {
        Err(StorageError::Corrupt(_)) => Ok(None),
        r => r,
    }
}

/// The intact manifest in `dir` iff it carries `generation`.
pub(crate) fn read_at(dir: &str, generation: u64) -> Result<Option<Manifest>, StorageError> {
    Ok(read_intact(dir)?.filter(|m| m.checkpoint_mark == generation))
}

/// Stage `bytes` (an [`encode`]d manifest) beside `dir`'s manifest. Does NOT
/// fdatasync or rename.
pub(crate) fn prepare(dir: &str, bytes: &[u8]) -> Result<(), StorageError> {
    fs::write(staging_path(dir), bytes)?;
    Ok(())
}

/// Rename the manifest [`prepare`] staged onto `dir`'s manifest path.
pub(crate) fn commit(dir: &str) -> Result<(), StorageError> {
    fs::rename(staging_path(dir), manifest_path(dir))?;
    Ok(())
}

/// Durably unlink `dir`'s manifest; an absent manifest or directory is already
/// unlinked.
pub(crate) fn unlink(dir: &str) -> Result<(), StorageError> {
    absent_ok(fs::remove_file(manifest_path(dir)))?;
    Ok(absent_ok(fsync_dir(dir))?)
}

/// Retire the store at `store_dir`: once this returns `Ok`, no crash brings its
/// manifest back. Removing the directory itself is best-effort.
pub(crate) fn retire_store(store_dir: &str) -> Result<(), StorageError> {
    unlink(store_dir)?;
    // Without its manifest the directory holds no reachable rows.
    if let Err(e) = absent_ok(fs::remove_dir_all(store_dir)) {
        gnitz_warn!("storage: failed to remove retired store dir {}: {}", store_dir, e);
    }
    Ok(())
}

/// Make `dst_dir` a durable hard-linked copy of the published store at `src_dir`.
pub(crate) fn link_store(src_dir: &str, dst_dir: &str) -> Result<(), StorageError> {
    let m = read(src_dir)?.ok_or(StorageError::Io(libc::ENOENT))?;
    create_dir(dst_dir)?;
    for e in &m.shards.entries {
        fs::hard_link(shard_path(src_dir, e.seq), shard_path(dst_dir, e.seq))?;
    }
    // The shards are durable before a manifest names them.
    fsync_dir(dst_dir)?;
    fs::hard_link(manifest_path(src_dir), manifest_path(dst_dir))?;
    Ok(fsync_dir(dst_dir)?)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
