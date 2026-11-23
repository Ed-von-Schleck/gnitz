//! One store directory: the manifest serde, the directory's file names and the
//! primitives over them, and the [`StoreDir`] a store draws its shards in and
//! publishes them through. A store owns its directory, so every shard and
//! staging file in it is the store's.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::Path;

use io_uring::types::FsyncFlags;

use super::batch_fsync::Syncer;
use gnitz_foundation::gnitz_warn;
use gnitz_trace::runs::StorageError;
use gnitz_wire::PkBuf;
use gnitz_wire::{Reader, Writer};

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
fn shard_path(dir: &str, seq: u64) -> String {
    format!("{dir}/{}", shard_name(seq))
}

/// `r`, with `NotFound` read as success.
fn absent_ok(r: io::Result<()>) -> io::Result<()> {
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
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
fn read(dir: &str) -> Result<Option<Manifest>, StorageError> {
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

/// The directories a sync of the store directory `dir` makes durable: its own,
/// for the files in it, and on its `first` sync its parent's and grandparent's
/// too, for the entries that reach it. A store directory sits at most two below
/// the data directory's `_relations` (`relation_dir[/child]`), whose own entry
/// `lock_data_dir` syncs.
fn owed_dirs(dir: &str, first: bool) -> impl Iterator<Item = &Path> {
    Path::new(dir)
        .ancestors()
        .take(if first { 3 } else { 1 })
        .filter(|d| !d.as_os_str().is_empty())
}

/// Durably unlink the manifest of every store in `dirs`, in one sync batch; an
/// absent manifest or directory is already unlinked.
fn unlink<S: AsRef<str>>(dirs: &[S]) -> Result<(), StorageError> {
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
        // The shards' directory entries are synced only after the rename, as
        // in `StoreDir::publish`, under the same metadata-ordering reliance.
        fs::rename(staging_path(dst), manifest_path(dst))?;
    }
    sync.sync_paths(&dirs, FsyncFlags::empty())
}

// ---------------------------------------------------------------------------
// The store directory
// ---------------------------------------------------------------------------

/// How a relation's tail is recovered across a restart.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RecoverySource {
    /// The tail is recovered by replaying the fsynced SAL over the shards loaded
    /// from the manifest at open.
    SalReplay,
    /// Derived state, persisted under a generation-stamped manifest by each
    /// ephemeral round that picks its relation.
    Rederive {
        /// The generation a manifest must carry for the open to resume from it
        /// instead of erasing it; `None` erases it.
        resume_at: Option<u64>,
    },
}

/// What this process knows of the manifest in place in a store's directory.
#[derive(Debug)]
enum InPlace {
    /// None, or one staged whose barrier has not completed: a failed publish
    /// may have renamed it.
    Unknown,
    /// Renamed into place, its directory entries possibly not durable: the one
    /// an open read, whose rename may predate a kill that came before the
    /// directory syncs.
    Renamed(Manifest),
    /// Renamed, and its directories synced by a barrier of this process.
    Durable(Manifest),
}

/// One store's directory: the seqs its shards are drawn at, which of them the
/// manifest in place names, the shards dropped since, and that manifest.
pub(super) struct StoreDir {
    path: String,
    /// The last seq drawn; a shard is named by its seq.
    seq: u64,
    /// The last seq drawn when the most recently renamed manifest was built:
    /// a live shard drawn at or below it is one that manifest names.
    published_through: u64,
    /// Shards dropped that a published manifest names, unlinked once one that
    /// does not is durable.
    retired: Vec<u64>,
    in_place: InPlace,
    /// The checkpoint mark of the manifest the open loaded.
    loaded_mark: Option<u64>,
    /// What the next publish writes as the manifest's caller record.
    caller_record: Vec<u8>,
}

/// A publish [`StoreDir::stage`] staged and [`StoreDir::publish`] completes. It
/// holds its store until then, so nothing draws or drops a shard in between.
pub(super) struct FlushWork<'a> {
    dir: &'a mut StoreDir,
    /// The manifest the barrier leaves durable: the one staged, or the one
    /// already in place.
    manifest: Manifest,
    /// Whether `manifest` was staged and is owed its rename; when false it is
    /// the manifest in place, owed only the directory syncs.
    rename: bool,
    /// Whether no publish of this store is known durable, so the entries that
    /// reach its directory are owed a sync beside the directory itself.
    first_sync: bool,
}

impl StoreDir {
    /// Open the store directory at `path`, creating it if it is missing.
    /// `recovery` decides which manifest, if any, the open loads; every shard
    /// that manifest does not name, and the staging file, are unlinked.
    pub(super) fn open(path: &str, recovery: RecoverySource) -> Result<Self, StorageError> {
        // First, so an unusable directory fails the open rather than the first
        // flush. `created` is `mkdir`'s own verdict, never a `stat`'s: a store
        // read as freshly created skips its manifest, and so unlinks every
        // shard in the directory.
        let created = match fs::create_dir(path) {
            Ok(()) => true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(path)?;
                true
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(e.into()),
        };
        let loaded = match recovery {
            _ if created => None,
            // Its shards are its only copy, so damage stops the open.
            RecoverySource::SalReplay => read(path)?,
            RecoverySource::Rederive { resume_at } => {
                let m = match resume_at {
                    // Erased at open, so it reads nothing: an I/O error on the
                    // file it is about to unlink must not abort it.
                    None => None,
                    // Rebuilt from its sources, so damage is erased rather than
                    // fatal.
                    Some(want) => read_at(path, want)?,
                };
                if m.is_none() {
                    // So a later re-open cannot reload what this one rejected.
                    unlink(&[path])?;
                }
                m
            }
        };
        // Unlink the staging file and every shard the loaded manifest does not name.
        let live: HashSet<u64> = loaded.iter().flat_map(|m| &m.shards.entries).map(|e| e.seq).collect();
        absent_ok(fs::remove_file(staging_path(path)))?;
        for seq in shard_seqs(path)?.into_iter().filter(|seq| !live.contains(seq)) {
            absent_ok(fs::remove_file(shard_path(path, seq)))?;
        }
        let seq = live.into_iter().max().unwrap_or(0);
        Ok(StoreDir {
            path: path.to_string(),
            seq,
            published_through: seq,
            retired: Vec::new(),
            loaded_mark: loaded.as_ref().map(|m| m.checkpoint_mark),
            caller_record: loaded.as_ref().map(|m| m.caller_record.clone()).unwrap_or_default(),
            in_place: loaded.map_or(InPlace::Unknown, InPlace::Renamed),
        })
    }

    /// The shard set of the manifest in place; at open, the one it loaded.
    pub(super) fn in_place(&self) -> Option<&ShardSet> {
        match &self.in_place {
            InPlace::Unknown => None,
            InPlace::Renamed(m) | InPlace::Durable(m) => Some(&m.shards),
        }
    }

    /// The checkpoint mark of the manifest the open loaded; `None` without one.
    pub(super) fn loaded_mark(&self) -> Option<u64> {
        self.loaded_mark
    }

    /// The caller record of the manifest in place, or of the next publish
    /// once set.
    #[cfg(test)]
    pub(super) fn caller_record(&self) -> &[u8] {
        &self.caller_record
    }

    /// What the next publish writes as the manifest's caller record.
    pub(super) fn set_caller_record(&mut self, record: Vec<u8>) {
        self.caller_record = record;
    }

    #[cfg(test)]
    pub(super) fn path(&self) -> &str {
        &self.path
    }

    #[cfg(test)]
    pub(super) fn last_seq(&self) -> u64 {
        self.seq
    }

    /// Treat every shard drawn so far as published.
    #[cfg(test)]
    pub(super) fn mark_published(&mut self) {
        self.published_through = self.seq;
    }

    /// The path of the shard drawn at `seq`.
    pub(super) fn shard_path(&self, seq: u64) -> String {
        shard_path(&self.path, seq)
    }

    /// A fresh seq, and the path of the shard it names.
    pub(super) fn draw(&mut self) -> (u64, String) {
        self.seq += 1;
        (self.seq, self.shard_path(self.seq))
    }

    /// Whether the manifest last renamed names the live shard drawn at `seq`.
    pub(super) fn published(&self, seq: u64) -> bool {
        seq <= self.published_through
    }

    /// Drop the shard drawn at `seq`: unlinked now if no published manifest
    /// names it, else once a manifest that does not is durable.
    pub(super) fn retire(&mut self, seq: u64) {
        if self.published(seq) {
            self.retired.push(seq);
        } else {
            // Best-effort: the next open unlinks a shard no manifest names.
            let _ = fs::remove_file(self.shard_path(seq));
        }
    }

    /// Stage the manifest of `shards` carrying `checkpoint_mark` and the
    /// caller record for [`Self::publish`], unless it is the manifest in place;
    /// `None` when that one is also durable.
    pub(super) fn stage(
        &mut self,
        checkpoint_mark: u64,
        shards: ShardSet,
    ) -> Result<Option<FlushWork<'_>>, StorageError> {
        let m = Manifest {
            checkpoint_mark,
            caller_record: self.caller_record.clone(),
            shards,
        };
        let first_sync = !matches!(self.in_place, InPlace::Durable(_));
        let rename = match &self.in_place {
            InPlace::Durable(p) if *p == m => return Ok(None),
            InPlace::Renamed(p) if *p == m => {
                debug_assert!(
                    m.shards.entries.iter().all(|e| self.published(e.seq)),
                    "the manifest in place names an unsynced shard"
                );
                false
            }
            _ => {
                // Until the barrier records otherwise: a failed publish may have
                // renamed it.
                self.in_place = InPlace::Unknown;
                fs::write(staging_path(&self.path), encode(&m))?;
                true
            }
        };
        Ok(Some(FlushWork {
            dir: self,
            manifest: m,
            rename,
            first_sync,
        }))
    }

    /// Durably publish every store in `work` as [`Self::stage`] staged it, then
    /// unlink the shards its new manifest no longer names.
    pub(super) fn publish(mut work: Vec<FlushWork<'_>>) -> Result<(), StorageError> {
        let mut sync = Syncer::default();
        // Every staged manifest, and every shard it names that no published one
        // did, before any rename.
        sync.sync_paths(
            work.iter().filter(|w| w.rename).flat_map(|w| {
                let unsynced = w.manifest.shards.entries.iter().filter(|e| !w.dir.published(e.seq));
                (unsynced.map(|e| w.dir.shard_path(e.seq))).chain([staging_path(&w.dir.path)])
            }),
            FsyncFlags::DATASYNC,
        )?;
        for w in &mut work {
            if w.rename {
                // The rename publishes every shard the manifest names. Their
                // directory entries are synced only after it, so a crash in
                // between relies on the filesystem ordering its metadata (ext4,
                // xfs), which POSIX does not promise.
                fs::rename(staging_path(&w.dir.path), manifest_path(&w.dir.path))?;
                w.dir.published_through = w.dir.seq;
            }
        }
        // After every rename. A rename is metadata: a full fsync, not fdatasync.
        let dirs: BTreeSet<&Path> = work.iter().flat_map(|w| owed_dirs(&w.dir.path, w.first_sync)).collect();
        sync.sync_paths(&dirs, FsyncFlags::empty())?;
        // Each store's manifest, staged or re-synced, is now durable, and no
        // durable manifest names a superseded shard any more.
        for FlushWork { dir, manifest, .. } in work {
            dir.in_place = InPlace::Durable(manifest);
            for seq in std::mem::take(&mut dir.retired) {
                let _ = fs::remove_file(dir.shard_path(seq));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
