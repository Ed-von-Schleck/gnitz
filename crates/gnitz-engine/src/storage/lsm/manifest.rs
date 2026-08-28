use std::fs::File;
use std::io::Write;
use std::os::fd::AsRawFd;

use super::error::StorageError;
use crate::foundation::posix_io::{self, open_owned};
use crate::foundation::xxh;
use crate::schema::key::PkBuf;
use crate::schema::MAX_PK_BYTES;
use gnitz_wire::{read_u64_le, write_u64_le};

// ---------------------------------------------------------------------------
// Manifest file format
// ---------------------------------------------------------------------------
//
// Header (56 bytes):
//   [0,8)   Magic   0x4D414E49464E5447
//   [8,16)  Version u64 (10)
//   [16,24) Count   u64
//   [24,32) Compaction sequence u64
//   [32,40) Checkpoint generation u64
//   [40,48) Layout sequence u64
//   [48,56) Checksum — XXH3-64 over the whole file, these eight bytes excluded
//
// Entry, laid out by the `W_*` widths below (224 bytes at MAX_PK_BYTES = 80):
//   max_lsn    u64
//   filename   W_FILENAME bytes (null-terminated)
//   level      u64
//   guard_key  W_GUARD_KEY OPK bytes
//
// The manifest records only what `load_manifest` consumes: which shard files
// are live, their tier placement (level, guard) and LSN watermark, plus the
// two header counters that must survive a restart. PK bounds are re-derived
// from each shard's mmap at open (`ShardEntry::open`), so they are not
// serialized. `guard_key` is a whole OPK key — the same key the compaction
// writer routes by and the read router probes with. It carries no length:
// every stored guard key is exactly the schema's `pk_stride` wide, and
// `load_manifest` holds the schema, exactly as `ShardEntry::open` does.

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION: u64 = 10;
const HEADER_SIZE: usize = 56;

/// Operator-state format version. Bump on any change to an operator-state
/// schema; a mismatch (recorded in `_sequences` via `SEQ_ID_TOPOLOGY`) marks
/// every Rederive view invalid at boot, so its state is rebuilt. Shard and
/// manifest layout changes are carried by their own version words.
pub(crate) const STATE_FORMAT: u32 = 7;

/// The durable topology word recorded in `_sequences` (`SEQ_ID_TOPOLOGY`):
/// `(worker_count << 32) | STATE_FORMAT`. The single packer shared by the
/// boot-time recorder and the resume-verdict validator, so the two can never
/// drift on the encoding.
pub(crate) fn topology_word(worker_count: u32) -> u64 {
    ((worker_count as u64) << 32) | STATE_FORMAT as u64
}

// Header offsets.
const OFF_ENTRY_COUNT: usize = 16;
const OFF_COMPACT_SEQ: usize = 24;
const OFF_CHECKPOINT_GEN: usize = 32;
const OFF_LAYOUT_SEQ: usize = 40;
const OFF_CHECKSUM: usize = 48;

// One entry's field widths, in layout order. Each offset is the running sum of
// the widths before it and `ENTRY_SIZE` is the total, so a width edit moves the
// fields after it instead of desyncing an offset written down a second time.
const W_MAX_LSN: usize = 8;
/// Bound on a shard basename — the binding limit on the naming grammar, which
/// checks against this rather than a second copy of the number.
pub(super) const W_FILENAME: usize = 128;
const W_LEVEL: usize = 8;
const W_GUARD_KEY: usize = MAX_PK_BYTES;

const OFF_MAX_LSN: usize = 0;
const OFF_FILENAME: usize = OFF_MAX_LSN + W_MAX_LSN;
const OFF_LEVEL: usize = OFF_FILENAME + W_FILENAME;
const OFF_GUARD_KEY: usize = OFF_LEVEL + W_LEVEL;
const ENTRY_SIZE: usize = OFF_GUARD_KEY + W_GUARD_KEY;

/// On-disk manifest entry.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ManifestEntryRaw {
    pub max_lsn: u64,
    pub filename: [u8; W_FILENAME],
    pub level: u64,
    /// The guard key zero-padded to the full array. Meaningful only for
    /// `level > 0`; an L0 entry stores zeros, which nothing reads back.
    pub guard_key: [u8; W_GUARD_KEY],
}

/// The NUL-terminated (or buffer-length) prefix of `buf` as UTF-8, `""` if
/// the bytes are not valid UTF-8.
fn cstr_from_buf(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    std::str::from_utf8(&buf[..end]).unwrap_or("")
}

impl ManifestEntryRaw {
    /// One entry naming a shard already written at `basename` inside the table's
    /// own directory. The **basename** is stored, never a full path: a path could
    /// exceed the field, and a truncated name is an unopenable shard at
    /// reload. Every basename the naming grammar produces is bounded well under
    /// the field width, so an overflow is a naming-scheme bug — fail loudly
    /// rather than truncate.
    pub(crate) fn new(basename: &str, max_lsn: u64, level: u64, guard_key: PkBuf) -> Self {
        let bytes = basename.as_bytes();
        assert!(
            bytes.len() < W_FILENAME,
            "shard basename overflows the manifest filename field: {basename}",
        );
        let mut filename = [0u8; W_FILENAME];
        filename[..bytes.len()].copy_from_slice(bytes);
        // A `PkBuf`'s tail past its width is always zero, so the stored array is
        // the key zero-padded — which is what `load_manifest` slices back to
        // `pk_stride`.
        let mut key = [0u8; W_GUARD_KEY];
        key.copy_from_slice(guard_key.padded(W_GUARD_KEY));
        ManifestEntryRaw {
            max_lsn,
            filename,
            level,
            guard_key: key,
        }
    }

    pub(crate) fn filename_str(&self) -> &str {
        cstr_from_buf(&self.filename)
    }
}

/// Header metadata serialized alongside the entries.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(crate) struct ManifestHeader {
    pub compact_seq: u64,
    /// The checkpoint generation the ephemeral round published at. Read by the
    /// conditional reload (`Rederive`) and the boot resume verdict.
    /// A base publish stamps 0; nothing reads it back from a base table.
    pub checkpoint_gen: u64,
    /// The layout sequence of the `w{k}of{n}` child set this manifest belongs
    /// to. `Table::new` loads it and every publish re-stamps it, so it survives
    /// checkpoints; the boot relayout stamps a target set one above its
    /// source's, which is what decides the live set when two complete sets
    /// survive a crash.
    pub layout_seq: u64,
}

/// Serialize manifest entries into `out_buf`.
/// Returns bytes written, or `BufferTooSmall` if `out_buf` cannot fit.
fn serialize(out_buf: &mut [u8], entries: &[ManifestEntryRaw], header: ManifestHeader) -> Result<usize, StorageError> {
    let count = entries.len();
    let total = HEADER_SIZE + count * ENTRY_SIZE;
    if out_buf.len() < total {
        return Err(StorageError::BufferTooSmall);
    }

    // Zero header
    out_buf[..HEADER_SIZE].fill(0);

    // Write header
    write_u64_le(out_buf, 0, MAGIC);
    write_u64_le(out_buf, 8, VERSION);
    write_u64_le(out_buf, OFF_ENTRY_COUNT, count as u64);
    write_u64_le(out_buf, OFF_COMPACT_SEQ, header.compact_seq);
    write_u64_le(out_buf, OFF_CHECKPOINT_GEN, header.checkpoint_gen);
    write_u64_le(out_buf, OFF_LAYOUT_SEQ, header.layout_seq);

    // Write entries field-by-field (symmetric with parse; immune to padding changes).
    for (i, e) in entries.iter().enumerate() {
        let off = HEADER_SIZE + i * ENTRY_SIZE;
        write_u64_le(out_buf, off + OFF_MAX_LSN, e.max_lsn);
        out_buf[off + OFF_FILENAME..off + OFF_FILENAME + W_FILENAME].copy_from_slice(&e.filename);
        write_u64_le(out_buf, off + OFF_LEVEL, e.level);
        out_buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + W_GUARD_KEY].copy_from_slice(&e.guard_key);
    }

    // Spans the whole serialized prefix, which is exactly what `prepare_file`
    // writes and therefore exactly what `verify` hashes back.
    write_u64_le(
        out_buf,
        OFF_CHECKSUM,
        xxh::digest_with_hole(&[], &out_buf[..total], OFF_CHECKSUM),
    );

    Ok(total)
}

/// The one gate a manifest buffer passes before any field is believed. Returns
/// the header and the entry count.
///
/// Current version only. There is no on-disk data to migrate in dev, so any
/// other version is a hard `InvalidVersion` — no per-version field gating, no
/// shims.
///
/// Both `parse` and the header-only [`peek_header`] come through here, so
/// neither can accept a manifest the other rejects.
fn verify(buf: &[u8]) -> Result<(ManifestHeader, usize), StorageError> {
    // Ordered: each check makes the next one's reads meaningful. Magic and
    // version come before the body-length check so a wrong-format file names
    // its actual defect, and both lengths come before the digest so a truncated
    // file is not reported as a hash mismatch.
    if buf.len() < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }
    if read_u64_le(buf, 0) != MAGIC {
        return Err(StorageError::InvalidMagic);
    }
    if read_u64_le(buf, 8) != VERSION {
        return Err(StorageError::InvalidVersion);
    }
    let count = read_u64_le(buf, OFF_ENTRY_COUNT) as usize;
    let body = count.checked_mul(ENTRY_SIZE).ok_or(StorageError::Truncated)?;
    if buf.len() < HEADER_SIZE.checked_add(body).ok_or(StorageError::Truncated)? {
        return Err(StorageError::Truncated);
    }
    // Over the whole buffer rather than the expected length: guard keys, LSNs
    // and the header counters have no other check, and hashing past the entries
    // also rejects bytes appended to an otherwise honest manifest.
    if xxh::digest_with_hole(&[], buf, OFF_CHECKSUM) != read_u64_le(buf, OFF_CHECKSUM) {
        return Err(StorageError::ChecksumMismatch);
    }
    Ok((
        ManifestHeader {
            compact_seq: read_u64_le(buf, OFF_COMPACT_SEQ),
            checkpoint_gen: read_u64_le(buf, OFF_CHECKPOINT_GEN),
            layout_seq: read_u64_le(buf, OFF_LAYOUT_SEQ),
        },
        count,
    ))
}

/// Decode a verified manifest buffer into an exact-count entry `Vec`.
fn parse(buf: &[u8]) -> Result<(Vec<ManifestEntryRaw>, ManifestHeader), StorageError> {
    let (header, count) = verify(buf)?;
    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let off = HEADER_SIZE + i * ENTRY_SIZE;
        let mut filename = [0u8; W_FILENAME];
        filename.copy_from_slice(&buf[off + OFF_FILENAME..off + OFF_FILENAME + W_FILENAME]);
        let mut guard_key = [0u8; W_GUARD_KEY];
        guard_key.copy_from_slice(&buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + W_GUARD_KEY]);
        entries.push(ManifestEntryRaw {
            max_lsn: read_u64_le(buf, off + OFF_MAX_LSN),
            filename,
            level: read_u64_le(buf, off + OFF_LEVEL),
            guard_key,
        });
    }

    Ok((entries, header))
}

/// Returns the buffer size needed to serialize `count` entries.
pub(crate) const fn serialized_size(count: usize) -> usize {
    HEADER_SIZE + count * ENTRY_SIZE
}

// ---------------------------------------------------------------------------
// File I/O (read + atomic write)
// ---------------------------------------------------------------------------

/// Read a manifest file into memory. `Ok(None)` when it does not exist yet
/// (first-time table boot ⇒ empty manifest); any other read failure is
/// `Err(Io)`.
fn read_bytes(path: &std::ffi::CStr) -> Result<Option<Vec<u8>>, StorageError> {
    use std::os::unix::ffi::OsStrExt;
    match std::fs::read(std::path::Path::new(std::ffi::OsStr::from_bytes(path.to_bytes()))) {
        Ok(buf) => Ok(Some(buf)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Read and parse a manifest file in one open. `Ok(None)` when the file does
/// not exist yet; a damaged one is an error, since its shards are already open
/// for business and dropping them silently would lose data.
pub(crate) fn read_file(
    path: &std::ffi::CStr,
) -> Result<Option<(Vec<ManifestEntryRaw>, ManifestHeader)>, StorageError> {
    read_bytes(path)?.map(|buf| parse(&buf)).transpose()
}

/// Open .tmp manifest at "<path>.tmp", serialize entries into it, and return
/// the open fd plus owned path buffers. Does NOT fdatasync, close, or rename.
/// On any internal write error closes the fd and unlinks the .tmp.
pub(crate) struct PreparedManifest {
    file: File,
    tmp_path: std::ffi::CString,
    final_path: std::ffi::CString,
    /// Set true once the `.tmp` has been renamed into place (`commit`). Until
    /// then, Drop unlinks the `.tmp` so a panic or early return between
    /// `prepare_file` and the rename never leaks the temporary file. The
    /// descriptor closes with the `File`.
    committed: bool,
}

impl PreparedManifest {
    /// The staged `.tmp`'s raw fd, open from `prepare_file` until the value
    /// drops (after `commit`'s rename, or on the abandon path). Raw because the
    /// flush barrier hands whole chunks of these to `IORING_OP_FSYNC` at once;
    /// a blocking `sync_data` per file would undo that batching.
    pub(crate) fn fd(&self) -> libc::c_int {
        self.file.as_raw_fd()
    }

    /// Make the staged bytes durable. The one-directory-at-a-time counterpart
    /// of the barrier's batched fsync.
    pub(crate) fn sync(&self) -> std::io::Result<()> {
        self.file.sync_data()
    }

    /// Rename the staged `.tmp` into place, consuming the staging value. The
    /// sole owner of the rename + Drop-suppression transition: a failure leaves
    /// `committed` unset, so Drop unlinks the `.tmp`.
    pub(crate) fn commit(mut self) -> Result<(), StorageError> {
        // `self` drops on the error path: Drop unlinks the .tmp.
        posix_io::renameat(libc::AT_FDCWD, &self.tmp_path, libc::AT_FDCWD, &self.final_path)?;
        self.committed = true; // renamed; suppress the .tmp unlink in Drop
        Ok(())
    }
}

impl Drop for PreparedManifest {
    fn drop(&mut self) {
        if !self.committed {
            unsafe {
                libc::unlink(self.tmp_path.as_ptr());
            }
        }
    }
}

pub(crate) fn prepare_file(
    path: &std::ffi::CStr,
    entries: &[ManifestEntryRaw],
    header: ManifestHeader,
) -> Result<PreparedManifest, StorageError> {
    let count = entries.len();
    let total = serialized_size(count);

    let mut buf = vec![0u8; total];
    let written = serialize(&mut buf, entries, header)?;

    let tmp_path = super::cstr_with_tmp_suffix(path)?;
    let final_path = super::cstr(path.to_bytes())?;

    let mut file = File::from(open_owned(&tmp_path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC)?);

    if let Err(e) = file.write_all(&buf[..written]) {
        unsafe {
            libc::unlink(tmp_path.as_ptr());
        }
        return Err(e.into());
    }

    Ok(PreparedManifest {
        file,
        tmp_path,
        final_path,
        committed: false,
    })
}

/// Basename of a table's manifest inside its own directory. The one spelling —
/// every producer, peeker and unlinker goes through `path`/`tmp_path`.
pub(crate) const MANIFEST_FILE: &str = "manifest.bin";

/// A table directory's manifest path.
pub(crate) fn path(dir: &str) -> String {
    format!("{dir}/{MANIFEST_FILE}")
}

/// The staging name `prepare_file` writes before renaming into `path(dir)`.
pub(crate) fn tmp_path(dir: &str) -> String {
    format!("{}.tmp", path(dir))
}

/// A manifest's header without decoding its entries. `Ok(None)` when there is no
/// usable manifest — absent, or damaged: a damaged manifest names no generation,
/// and every caller answers that by rebuilding. Only a failed read is `Err(Io)`;
/// that is not evidence of staleness, so it must not erase.
///
/// The digest spans the entries, so no header-only peek can skip reading them,
/// but decoding them is another matter — this stops at [`verify`].
pub(crate) fn peek_header(path: &std::ffi::CStr) -> Result<Option<ManifestHeader>, StorageError> {
    Ok(read_bytes(path)?.and_then(|buf| Some(verify(&buf).ok()?.0)))
}

/// Stage `entries` as `dir`'s manifest and rename it into place, durable at
/// every step: the `.tmp` is fdatasync'd and the directory fsync'd before the
/// rename, and fsync'd again after it. A crash therefore leaves a manifest-less
/// directory the next boot redoes, never a manifest whose shards are missing.
///
/// The blocking counterpart of the barrier's io_uring publish, for the boot
/// paths that publish one directory at a time.
pub(super) fn publish_sync(
    dir: &str,
    entries: &[ManifestEntryRaw],
    header: ManifestHeader,
) -> Result<(), StorageError> {
    let staged = prepare_file(&super::cstr(path(dir))?, entries, header)?;
    staged.sync()?;
    super::child_dir::fsync_dir(dir)?;
    staged.commit()?;
    super::child_dir::fsync_dir(dir)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/manifest.rs"]
mod tests;
