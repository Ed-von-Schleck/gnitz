use std::fs::File;
use std::io::Write;
use std::os::fd::AsRawFd;

use super::error::StorageError;
use crate::foundation::posix_io::{self, open_owned};
use crate::foundation::xxh;
use gnitz_wire::{read_u64_le, write_u64_le};

// ---------------------------------------------------------------------------
// Manifest file format
// ---------------------------------------------------------------------------
//
// Header (56 bytes):
//   [0,8)   Magic   0x4D414E49464E5447
//   [8,16)  Version u64 (9)
//   [16,24) Count   u64
//   [24,32) Compaction sequence u64
//   [32,40) Checkpoint generation u64
//   [40,48) Layout sequence u64
//   [48,56) Checksum — XXH3-64 over the whole file, these eight bytes excluded
//
// Entry (160 bytes each):
//   [0,8)     max_lsn    u64
//   [8,136)   filename   128 bytes (null-terminated)
//   [136,144) level      u64
//   [144,160) guard_key  u128 LE
//
// The manifest records only what `load_manifest` consumes: which shard files
// are live, their tier placement (level, guard) and LSN watermark, plus the
// two header counters that must survive a restart. PK bounds are re-derived
// from each shard's mmap at open (`ShardEntry::open`), so they are not
// serialized. `guard_key` lives in the order-preserving `pack_pk_be` prefix
// space — the whole key for narrow PKs, a lossy-but-order-preserving 16-byte
// prefix for wide (compound) PKs, exactly the routing key the compaction
// writer and read router share.

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION: u64 = 9;
const HEADER_SIZE: usize = 56;
const ENTRY_SIZE: usize = 160;

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

// Field offsets within an entry, kept in sync with the doc-comment above.
const OFF_MAX_LSN: usize = 0;
const OFF_FILENAME: usize = 8;
const OFF_LEVEL: usize = 136;
const OFF_GUARD_KEY: usize = 144;

// Build break if the documented field widths stop summing to the entry
// size — an offset/size edit that desyncs serialize/parse fails the
// build rather than corrupting the manifest.
const _: () = assert!(
    ENTRY_SIZE == 8 + 128 + 8 + 16,
    "entry field widths do not sum to ENTRY_SIZE",
);

/// On-disk manifest entry.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ManifestEntryRaw {
    pub max_lsn: u64,
    pub filename: [u8; 128],
    pub level: u64,
    pub guard_key: u128,
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
    /// exceed the 128-byte field, and a truncated name is an unopenable shard at
    /// reload. Every basename the naming grammar produces is bounded well under
    /// the field width, so an overflow is a naming-scheme bug — fail loudly
    /// rather than truncate.
    pub(crate) fn new(basename: &str, max_lsn: u64, level: u64, guard_key: u128) -> Self {
        let bytes = basename.as_bytes();
        assert!(
            bytes.len() < 128,
            "shard basename overflows the manifest filename field: {basename}",
        );
        let mut filename = [0u8; 128];
        filename[..bytes.len()].copy_from_slice(bytes);
        ManifestEntryRaw {
            max_lsn,
            filename,
            level,
            guard_key,
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
        out_buf[off + OFF_FILENAME..off + OFF_FILENAME + 128].copy_from_slice(&e.filename);
        write_u64_le(out_buf, off + OFF_LEVEL, e.level);
        out_buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + 16].copy_from_slice(&e.guard_key.to_le_bytes());
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
        let mut filename = [0u8; 128];
        filename.copy_from_slice(&buf[off + OFF_FILENAME..off + OFF_FILENAME + 128]);
        entries.push(ManifestEntryRaw {
            max_lsn: read_u64_le(buf, off + OFF_MAX_LSN),
            filename,
            level: read_u64_le(buf, off + OFF_LEVEL),
            guard_key: u128::from_le_bytes(buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + 16].try_into().unwrap()),
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
mod tests {
    use super::*;

    fn hdr(compact_seq: u64, checkpoint_gen: u64) -> ManifestHeader {
        ManifestHeader {
            compact_seq,
            checkpoint_gen,
            layout_seq: 0,
        }
    }

    #[test]
    fn parse_rejects_count_overflow() {
        // A corrupt header whose count * ENTRY_SIZE overflows usize must be
        // rejected, not wrap past the length check.
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, MAGIC);
        write_u64_le(&mut buf, 8, VERSION);
        write_u64_le(&mut buf, 16, u64::MAX); // count
        let r = parse(&buf);
        assert!(matches!(r, Err(StorageError::Truncated)));
    }

    fn make_entry(max_lsn: u64, name: &str) -> ManifestEntryRaw {
        ManifestEntryRaw::new(name, max_lsn, 1, 42)
    }

    #[test]
    fn old_version_rejected() {
        // Any non-current version must be rejected outright — no legacy reader.
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, MAGIC);
        write_u64_le(&mut buf, 8, VERSION - 1);
        write_u64_le(&mut buf, 16, 0);

        assert_eq!(parse(&buf).unwrap_err(), StorageError::InvalidVersion);
    }

    #[test]
    fn bad_magic() {
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, 0xDEADBEEF);

        assert_eq!(parse(&buf).unwrap_err(), StorageError::InvalidMagic);
    }

    #[test]
    fn truncated() {
        assert_eq!(parse(&[0u8; 10]).unwrap_err(), StorageError::Truncated);
    }

    #[test]
    fn buffer_too_small() {
        let entries = vec![make_entry(1, "test.db")];
        let mut buf = vec![0u8; 32]; // too small for header + entry
        assert_eq!(
            serialize(&mut buf, &entries, hdr(0, 0)),
            Err(StorageError::BufferTooSmall)
        );
    }

    #[test]
    fn empty_manifest() {
        let mut buf = vec![0u8; HEADER_SIZE];
        let written = serialize(&mut buf, &[], hdr(42, 0)).unwrap();
        assert_eq!(written, HEADER_SIZE);

        let (out, header) = parse(&buf).unwrap();
        assert!(out.is_empty());
        assert_eq!(header.compact_seq, 42);
    }

    #[test]
    fn filename_null_terminated() {
        let e = make_entry(1, "hello.db");
        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &[e], hdr(0, 0)).unwrap();

        let (out, _) = parse(&buf).unwrap();

        // Extract filename
        let end = out[0].filename.iter().position(|&b| b == 0).unwrap_or(128);
        let name = std::str::from_utf8(&out[0].filename[..end]).unwrap();
        assert_eq!(name, "hello.db");
    }

    // --- File I/O tests ---

    /// Publish `entries` at `path` for round-trip tests: stage the `.tmp` via
    /// the production `prepare_file`, then rename it into place (the barrier's
    /// `flush_commit` step, minus the fsyncs the round-trip doesn't observe).
    fn write_manifest(path: &std::ffi::CStr, entries: &[ManifestEntryRaw], header: ManifestHeader) {
        let m = prepare_file(path, entries, header).unwrap();
        m.commit().unwrap();
    }

    #[test]
    fn write_read_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let entries = vec![
            make_entry(1, "shard_1.db"),
            make_entry(2, "shard_2.db"),
            make_entry(3, "shard_3.db"),
        ];

        write_manifest(&cpath, &entries, hdr(5, 2));
        assert!(path.exists());

        let (out, header) = read_file(&cpath).unwrap().unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(
            header,
            hdr(5, 2),
            "header must round-trip through prepare_file/read_file"
        );
        assert_eq!(out[0].max_lsn, 1);
        assert_eq!(out[1].max_lsn, 2);
        assert_eq!(out[2].max_lsn, 3);
        assert_eq!(out[0].level, 1);
        assert_eq!(out[0].guard_key, 42);
    }

    #[test]
    fn read_file_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("DOES_NOT_EXIST");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        assert!(
            read_file(&cpath).unwrap().is_none(),
            "missing manifest file reads as the empty manifest"
        );
    }

    #[test]
    fn write_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST_EMPTY");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        write_manifest(&cpath, &[], hdr(42, 0));

        let (out, header) = read_file(&cpath).unwrap().unwrap();
        assert!(out.is_empty());
        assert_eq!(header.compact_seq, 42);
    }

    /// A serialized manifest for `count` entries, ready to forge against.
    fn serialized(count: usize) -> Vec<u8> {
        let entries: Vec<ManifestEntryRaw> = (0..count)
            .map(|i| make_entry(100 + i as u64, &format!("shard_7_{i}.db")))
            .collect();
        let mut buf = vec![0u8; serialized_size(count)];
        serialize(&mut buf, &entries, hdr(11, 5)).unwrap();
        buf
    }

    #[test]
    fn roundtrips_at_zero_one_and_many_entries() {
        for count in [0usize, 1, 5] {
            let buf = serialized(count);
            assert_eq!(buf.len(), serialized_size(count));
            let (out, header) = parse(&buf).unwrap();
            assert_eq!(header, hdr(11, 5), "count={count}");
            assert_eq!(out.len(), count);
            for (i, e) in out.iter().enumerate() {
                assert_eq!(e.max_lsn, 100 + i as u64);
                assert_eq!(e.filename_str(), format!("shard_7_{i}.db"));
                assert_eq!((e.level, e.guard_key), (1, 42));
            }
        }
    }

    #[test]
    fn forged_entry_count_up_reports_truncated() {
        // The length check runs ahead of the digest, which is what pins `parse`'s
        // order: a genuinely short file must not report a hash mismatch.
        let mut buf = serialized(5);
        write_u64_le(&mut buf, OFF_ENTRY_COUNT, 6);
        assert_eq!(parse(&buf).unwrap_err(), StorageError::Truncated);
    }

    #[test]
    fn forged_entry_count_down_reports_checksum_mismatch() {
        // The count field is inside the digest. Were it not, a shrunk count
        // would drop shards from the live set, and `gc_orphans` unlinks any
        // shard the loaded manifest does not name.
        let mut buf = serialized(5);
        write_u64_le(&mut buf, OFF_ENTRY_COUNT, 4);
        assert_eq!(parse(&buf).unwrap_err(), StorageError::ChecksumMismatch);
    }

    #[test]
    fn trailing_bytes_report_checksum_mismatch() {
        // The neighbouring case the count field cannot close: bytes appended to
        // an otherwise honest manifest. The digest spans the whole buffer.
        let mut buf = serialized(2);
        buf.extend_from_slice(&[0u8; 16]);
        assert_eq!(parse(&buf).unwrap_err(), StorageError::ChecksumMismatch);
    }

    #[test]
    fn every_byte_past_the_count_field_is_inside_the_digest() {
        // Filenames, levels, guard keys, LSNs and the header counters have no
        // other check, so the sweep is over every byte rather than a chosen few.
        // It starts past the count field, whose forgeries split between
        // `Truncated` and `ChecksumMismatch` and have their own tests above.
        let base = serialized(3);
        for off in OFF_COMPACT_SEQ..base.len() {
            let mut buf = base.clone();
            buf[off] ^= 0x01;
            assert_eq!(parse(&buf).unwrap_err(), StorageError::ChecksumMismatch, "byte {off}");
        }
    }

    #[test]
    fn peek_header_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST_GEN");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        // Absent file ⇒ Ok(None).
        assert_eq!(peek_header(&cpath).unwrap(), None);

        // The two sequence fields are independent and both round-trip.
        let full = ManifestHeader {
            compact_seq: 3,
            checkpoint_gen: 42,
            layout_seq: 7,
        };
        write_manifest(&cpath, &[make_entry(1, "shard_1.db")], full);
        assert_eq!(peek_header(&cpath).unwrap(), Some(full));

        // Republish at generation 0 (the base-round stamp).
        write_manifest(&cpath, &[make_entry(1, "shard_1.db")], hdr(3, 0));
        assert_eq!(peek_header(&cpath).unwrap().unwrap().checkpoint_gen, 0);
    }
}
