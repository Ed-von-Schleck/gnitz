use std::os::unix::fs::FileExt;

use super::error::StorageError;
use super::StagedFile;
use crate::schema::key::PkBuf;
use crate::schema::MAX_PK_BYTES;
use gnitz_wire::{Reader, Writer};

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION: u64 = 15;

const MANIFEST_FILE: &str = "manifest.bin";

/// The words a publish writes beside the shard set, for its caller to read back
/// at the next open.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) struct ManifestStamp {
    /// The ephemeral round's resume generation.
    pub checkpoint_gen: u64,
    /// The system round's SAL replay floor.
    pub replay_floor: u64,
}

/// One store's published shard set and the counters that must survive a
/// restart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub stamp: ManifestStamp,
    /// The shard index's `R`. Stored, not derived: a guard of one distinct key
    /// outgrows `R` and cannot be split, so no shard size recovers it.
    pub run_bytes: u64,
    /// Bytes the store's owner publishes with its rows; opaque here.
    pub caller_record: Vec<u8>,
    pub entries: Vec<ManifestEntry>,
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
    let mut w = Writer::with_capacity(64 * (1 + m.entries.len()));
    w.u64(MAGIC)
        .u64(VERSION)
        .u64(m.stamp.checkpoint_gen)
        .u64(m.stamp.replay_floor)
        .u64(m.run_bytes)
        .bytes32(&m.caller_record);
    for e in &m.entries {
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
    let mut r = Reader::new(covered, "manifest");
    if r.u64().map_err(truncated)? != MAGIC {
        return Err(StorageError::Corrupt("manifest magic"));
    }
    if r.u64().map_err(truncated)? != VERSION {
        return Err(StorageError::Corrupt("manifest version"));
    }
    if gnitz_wire::checksum(covered) != u64::from_le_bytes(*digest) {
        return Err(StorageError::Corrupt("manifest checksum"));
    }
    decode_body(&mut r).map_err(|_| StorageError::Corrupt("manifest body"))
}

fn decode_body(r: &mut Reader) -> Result<Manifest, String> {
    let mut m = Manifest {
        stamp: ManifestStamp {
            checkpoint_gen: r.u64()?,
            replay_floor: r.u64()?,
        },
        run_bytes: r.u64()?,
        caller_record: r.bytes32()?.to_vec(),
        entries: Vec::new(),
    };
    while r.remaining() > 0 {
        let (seq, newest, level) = (r.u64()?, r.u64()?, r.u64()?);
        let key = r.bytes32()?;
        if key.len() > MAX_PK_BYTES {
            return Err("guard key too wide".into());
        }
        m.entries.push(ManifestEntry {
            seq,
            newest,
            level,
            guard_key: PkBuf::from_bytes(key),
        });
    }
    Ok(m)
}

// ---------------------------------------------------------------------------
// File I/O (read + atomic write)
// ---------------------------------------------------------------------------

/// `dir`'s manifest path.
pub(crate) fn path(dir: &str) -> String {
    format!("{dir}/{MANIFEST_FILE}")
}

/// Read and decode `dir`'s manifest. `Ok(None)` when it does not exist yet;
/// `Err` on damage or a failed read.
pub(crate) fn read(dir: &str) -> Result<Option<Manifest>, StorageError> {
    match std::fs::read(path(dir)) {
        Ok(buf) => decode(&buf).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Stage `bytes` (an [`encode`]d manifest) as `dir`'s manifest. Does NOT
/// fdatasync or rename.
pub(crate) fn prepare(dir: &str, bytes: &[u8]) -> Result<StagedFile, StorageError> {
    let (staged, file) = StagedFile::create(&path(dir))?;
    file.write_all_at(bytes, 0)?;
    Ok(staged)
}

/// Durably unlink `dir`'s manifest; an absent manifest or directory is already
/// unlinked.
pub(crate) fn unlink(dir: &str) -> Result<(), StorageError> {
    let absent_ok = |r: Result<(), StorageError>| match r {
        Err(StorageError::Io(libc::ENOENT)) => Ok(()),
        r => r,
    };
    absent_ok(std::fs::remove_file(path(dir)).map_err(StorageError::from))?;
    absent_ok(crate::storage::fsync_dir(dir))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
