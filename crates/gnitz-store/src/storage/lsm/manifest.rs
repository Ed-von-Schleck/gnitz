use std::fs;

use super::naming::shard_path;
use crate::schema::key::PkBuf;
use crate::schema::MAX_PK_BYTES;
use crate::storage::error::StorageError;
use gnitz_foundation::posix_io::{create_dir, fsync_dir};
use gnitz_wire::{decode_all, Reader, Writer};

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION: u64 = 16;

const MANIFEST_FILE: &str = "manifest.bin";

/// The suffix a manifest is staged under before its rename — also how startup
/// GC names a stray one.
pub(super) const STAGING_SUFFIX: &str = ".tmp";

/// What a shard index publishes and reopens from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShardSet {
    /// The shard index's `R`. Stored, not derived: a guard of one distinct key
    /// outgrows `R` and cannot be split, so no shard size recovers it.
    pub run_bytes: u64,
    pub entries: Vec<ManifestEntry>,
}

/// One store's published shard set and the counters that must survive a
/// restart.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// 0 = L0.
    pub level: u64,
    /// The guard the shard sits under; width 0 at L0.
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

fn staging_path(dir: &str) -> String {
    format!("{}{STAGING_SUFFIX}", manifest_path(dir))
}

/// Read and decode `dir`'s manifest. `Ok(None)` when it does not exist yet;
/// `Err` on damage or a failed read.
pub(crate) fn read(dir: &str) -> Result<Option<Manifest>, StorageError> {
    match fs::read(manifest_path(dir)) {
        Ok(buf) => decode(&buf).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
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

/// What the store at `store_dir` last published for its owner.
pub(crate) struct Published {
    pub checkpoint_mark: u64,
    pub caller_record: Vec<u8>,
}

/// [`read_intact`], narrowed to what the store's owner published.
pub(crate) fn published(store_dir: &str) -> Result<Option<Published>, StorageError> {
    read_intact(store_dir).map(|o| {
        o.map(|m| Published {
            checkpoint_mark: m.checkpoint_mark,
            caller_record: m.caller_record,
        })
    })
}

/// Stage `bytes` (an [`encode`]d manifest) beside `dir`'s manifest, returning
/// the staged file's path. Does NOT fdatasync or rename.
pub(crate) fn prepare(dir: &str, bytes: &[u8]) -> Result<String, StorageError> {
    let tmp = staging_path(dir);
    fs::write(&tmp, bytes)?;
    Ok(tmp)
}

/// Rename the manifest [`prepare`] staged onto `dir`'s manifest path.
pub(crate) fn commit(dir: &str) -> Result<(), StorageError> {
    fs::rename(staging_path(dir), manifest_path(dir))?;
    Ok(())
}

/// Durably unlink `dir`'s manifest; an absent manifest or directory is already
/// unlinked.
pub(crate) fn unlink(dir: &str) -> Result<(), StorageError> {
    let absent_ok = |r: std::io::Result<()>| match r {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        r => r,
    };
    absent_ok(fs::remove_file(manifest_path(dir)))?;
    Ok(absent_ok(fsync_dir(dir))?)
}

/// Retire the store at `store_dir`: once this returns `Ok`, no crash brings its
/// manifest back. Removing the directory itself is best-effort.
pub(crate) fn retire_store(store_dir: &str) -> Result<(), StorageError> {
    unlink(store_dir)?;
    // Without its manifest the directory holds no reachable rows.
    match fs::remove_dir_all(store_dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            gnitz_warn!("storage: failed to remove retired store dir {}: {}", store_dir, e)
        }
        _ => {}
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
