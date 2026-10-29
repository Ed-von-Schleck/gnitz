//! One store directory: the manifest serde, the directory's file names and the
//! primitives over them. A store owns its directory, so every shard and staging
//! file in it is the store's.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use io_uring::types::FsyncFlags;

use super::batch_fsync::Syncer;
use gnitz_wire::PkBuf;
use gnitz_wire::{Reader, Writer};
use gnitz_zset::repr::StorageError;

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION: u64 = 17;

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
fn encode(m: &Manifest) -> Vec<u8> {
    let mut w = Writer::new();
    w.u64(MAGIC)
        .u64(VERSION)
        .u64(m.checkpoint_mark)
        .u64(m.shards.run_bytes)
        .bytes32(&m.caller_record);
    for e in &m.shards.entries {
        w.u64(e.seq).u64(e.newest).u64(e.level).put(&e.guard_key);
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
    decode_body(&mut r).map_err(|_| StorageError::Corrupt("manifest body"))
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
        let guard_key = r.get::<PkBuf>()?;
        m.shards.entries.push(ManifestEntry { seq, newest, level, guard_key });
    }
    Ok(m)
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

/// The manifest path of the store at `store_dir`.
pub(crate) fn manifest_path(store_dir: &str) -> String {
    format!("{store_dir}/manifest.bin")
}

/// The one path a manifest is staged under before its rename, in the store at
/// `dir`.
pub(super) fn staging_path(dir: &str) -> String {
    format!("{dir}/manifest.bin.tmp")
}

/// The basename of the shard drawn at `seq`: `shard_{seq}.db`.
pub(super) fn shard_name(seq: u64) -> String {
    format!("shard_{seq}.db")
}

/// The seq `name` denotes, or `None` unless [`shard_name`] gives that seq
/// exactly this name.
fn shard_seq(name: &str) -> Option<u64> {
    let seq = name.strip_prefix("shard_")?.strip_suffix(".db")?.parse().ok()?;
    (shard_name(seq) == name).then_some(seq)
}

/// The seq of every shard file in `dir`.
pub(super) fn shard_seqs(dir: &str) -> io::Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in fs::read_dir(dir)? {
        seqs.extend(entry?.file_name().to_str().and_then(shard_seq));
    }
    Ok(seqs)
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

/// Remove the staging file and every shard `live` does not hold from `dir`.
pub(super) fn remove_stale_files(dir: &str, live: impl Fn(u64) -> bool) -> io::Result<()> {
    absent_ok(fs::remove_file(staging_path(dir)))?;
    for seq in shard_seqs(dir)? {
        if !live(seq) {
            absent_ok(fs::remove_file(shard_path(dir, seq)))?;
        }
    }
    Ok(())
}

/// Every file in `dir` named as a shard, with the level `dir`'s intact manifest
/// places it at — `None` for one the manifest does not name.
pub(crate) fn shard_files(dir: &str) -> Result<Vec<(String, Option<u64>)>, StorageError> {
    let entries = read_intact(dir)?.map_or(Vec::new(), |m| m.shards.entries);
    let levels: HashMap<u64, u64> = entries.iter().map(|e| (e.seq, e.level)).collect();
    let seqs = shard_seqs(dir)?.into_iter();
    Ok(seqs
        .map(|seq| (shard_path(dir, seq), levels.get(&seq).copied()))
        .collect())
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

/// Write `m` to `dir`'s [`staging_path`], for [`commit`] to publish.
pub(crate) fn stage(dir: &str, m: &Manifest) -> Result<(), StorageError> {
    fs::write(staging_path(dir), encode(m))?;
    Ok(())
}

/// Rename the manifest staged at [`staging_path`] onto `dir`'s manifest path.
pub(crate) fn commit(dir: &str) -> Result<(), StorageError> {
    fs::rename(staging_path(dir), manifest_path(dir))?;
    Ok(())
}

/// The directories a sync of the store directory `dir` makes durable: its own,
/// for the files in it, and on its `first` sync its parent's and grandparent's
/// too, for the entries that reach it.
pub(super) fn owed_dirs(dir: &str, first: bool) -> impl Iterator<Item = PathBuf> + '_ {
    Path::new(dir)
        .ancestors()
        .take(if first { 3 } else { 1 })
        .filter(|d| !d.as_os_str().is_empty())
        .map(Path::to_path_buf)
}

/// Durably unlink the manifest of every store in `dirs`, in one sync batch; an
/// absent manifest or directory is already unlinked.
pub(crate) fn unlink<S: AsRef<str>>(dirs: &[S]) -> Result<(), StorageError> {
    let mut present = Vec::with_capacity(dirs.len());
    for dir in dirs.iter().map(AsRef::as_ref) {
        absent_ok(fs::remove_file(manifest_path(dir)))?;
        // Synced even with no manifest to unlink: a process that died between
        // its own unlink and the sync left the unlink in the page cache alone.
        if Path::new(dir).is_dir() {
            present.push(dir);
        }
    }
    Syncer::default().sync_paths(present, FsyncFlags::empty())
}

/// Retire every store in `dirs`: once this returns `Ok`, no crash brings any of
/// their manifests back. Removing the directories themselves is best-effort, and
/// starts only once every unlink is durable: a removal that reached the disk
/// ahead of its store's unlink would leave a manifest naming shards that are gone.
pub(crate) fn retire_stores(dirs: &[String]) -> Result<(), StorageError> {
    unlink(dirs)?;
    // Without its manifest a directory holds no reachable rows.
    for dir in dirs {
        if let Err(e) = absent_ok(fs::remove_dir_all(dir)) {
            gnitz_warn!("storage: failed to remove retired store dir {}: {}", dir, e);
        }
    }
    Ok(())
}

/// Make each pair's `dst` a durable copy of the store its `src` published — the
/// manifest and the shards it names — published as [`flush_barrier`] publishes
/// a store's first manifest. A file is hard-linked where `dst` shares `src`'s
/// filesystem, and copied where it does not; the file names are kept, since a
/// shard's descriptor digest covers its own. A `src` that published no manifest
/// puts nothing.
///
/// [`flush_barrier`]: super::flush_barrier
pub(crate) fn link_stores(pairs: &[(String, String)]) -> Result<(), StorageError> {
    // `Ok(true)` for a copy, whose bytes are owed a sync of their own.
    let put = |src: &str, dst: &str| match fs::hard_link(src, dst) {
        Err(e) if e.raw_os_error() == Some(libc::EXDEV) => fs::copy(src, dst).map(|_| true),
        r => r.map(|()| false),
    };
    let mut stores = Vec::with_capacity(pairs.len());
    let mut copies = Vec::new();
    let mut dirs = BTreeSet::new();
    for (src, dst) in pairs {
        let Some(m) = read(src)? else { continue };
        fs::create_dir_all(dst)?;
        let shards = m
            .shards
            .entries
            .iter()
            .map(|e| (shard_path(src, e.seq), shard_path(dst, e.seq)));
        for (from, to) in shards.chain([(manifest_path(src), staging_path(dst))]) {
            if put(&from, &to)? {
                copies.push(to);
            }
        }
        dirs.extend(owed_dirs(dst, true));
        stores.push(dst);
    }
    let mut sync = Syncer::default();
    // Every copied shard and staged manifest, before any rename.
    sync.sync_paths(copies, FsyncFlags::DATASYNC)?;
    for dst in stores {
        commit(dst)?;
    }
    sync.sync_paths(&dirs, FsyncFlags::empty())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
