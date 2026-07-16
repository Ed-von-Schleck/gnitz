use std::os::fd::{AsRawFd, OwnedFd};

use super::error::StorageError;
use crate::foundation::codec::{read_u64_le, write_u64_le};
use crate::foundation::posix_io::open_owned;

// ---------------------------------------------------------------------------
// Manifest file format
// ---------------------------------------------------------------------------
//
// Header (48 bytes):
//   [0,8)   Magic   0x4D414E49464E5447
//   [8,16)  Version u64 (7)
//   [16,24) Count   u64
//   [24,32) Compaction sequence u64
//   [32,40) Checkpoint generation u64
//   [40,48) Reserved
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
// from each shard's mmap at open (`ShardEntry::from_mapped`), so they are not
// serialized. `guard_key` lives in the order-preserving `pack_pk_be` prefix
// space — the whole key for narrow PKs, a lossy-but-order-preserving 16-byte
// prefix for wide (compound) PKs, exactly the routing key the compaction
// writer and read router share.

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION: u64 = 7;
const HEADER_SIZE: usize = 48;
const ENTRY_SIZE: usize = 160;

/// Operator-state / on-disk-layout format version. Bump on any change to an
/// operator-state schema or to the shard/manifest layout; a mismatch (recorded
/// in `_sequences` via `SEQ_ID_TOPOLOGY`) wipes all Rederive view state at boot,
/// always correct because it re-derives.
pub const STATE_FORMAT: u32 = 2;

/// The durable topology word recorded in `_sequences` (`SEQ_ID_TOPOLOGY`):
/// `(worker_count << 32) | STATE_FORMAT`. The single packer shared by the
/// boot-time recorder and the resume-verdict validator, so the two can never
/// drift on the encoding.
pub fn topology_word(worker_count: u32) -> u64 {
    ((worker_count as u64) << 32) | STATE_FORMAT as u64
}

// Header offsets.
const OFF_ENTRY_COUNT: usize = 16;
const OFF_COMPACT_SEQ: usize = 24;
const OFF_GENERATION: usize = 32;

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
pub struct ManifestEntryRaw {
    pub max_lsn: u64,
    pub filename: [u8; 128],
    pub level: u64,
    pub guard_key: u128,
}

impl Default for ManifestEntryRaw {
    fn default() -> Self {
        Self {
            max_lsn: 0,
            filename: [0; 128],
            level: 0,
            guard_key: 0,
        }
    }
}

/// The NUL-terminated (or buffer-length) prefix of `buf` as UTF-8, `""` if
/// the bytes are not valid UTF-8.
fn cstr_from_buf(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    std::str::from_utf8(&buf[..end]).unwrap_or("")
}

impl ManifestEntryRaw {
    pub fn filename_str(&self) -> &str {
        cstr_from_buf(&self.filename)
    }
}

/// Header metadata serialized alongside the entries.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ManifestHeader {
    pub compact_seq: u64,
    /// Checkpoint generation this manifest was published at. Stamped by the
    /// publish path (the ephemeral checkpoint round passes the committed
    /// generation; base `SalReplay` publishes stamp 0 and never read it back);
    /// `RederiveCheckpointed` view tables gate their conditional reload on it.
    pub generation: u64,
}

/// Serialize manifest entries into `out_buf`.
/// Returns bytes written, or `BufferTooSmall` if `out_buf` cannot fit.
pub fn serialize(
    out_buf: &mut [u8],
    entries: &[ManifestEntryRaw],
    header: ManifestHeader,
) -> Result<usize, StorageError> {
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
    write_u64_le(out_buf, OFF_GENERATION, header.generation);

    // Write entries field-by-field (symmetric with parse; immune to padding changes).
    for (i, e) in entries.iter().enumerate().take(count) {
        let off = HEADER_SIZE + i * ENTRY_SIZE;
        write_u64_le(out_buf, off + OFF_MAX_LSN, e.max_lsn);
        out_buf[off + OFF_FILENAME..off + OFF_FILENAME + 128].copy_from_slice(&e.filename);
        write_u64_le(out_buf, off + OFF_LEVEL, e.level);
        out_buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + 16].copy_from_slice(&e.guard_key.to_le_bytes());
    }

    Ok(total)
}

/// Parse a manifest buffer into an exact-count entry `Vec` plus the header.
///
/// Current version only. There is no on-disk data to migrate in dev and the
/// test suite is green at session start, so any other version is a hard
/// `InvalidVersion` — no per-version field gating, no shims.
pub fn parse(buf: &[u8]) -> Result<(Vec<ManifestEntryRaw>, ManifestHeader), StorageError> {
    if buf.len() < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }

    let magic = read_u64_le(buf, 0);
    if magic != MAGIC {
        return Err(StorageError::InvalidMagic);
    }

    let version = read_u64_le(buf, 8);
    let count = read_u64_le(buf, OFF_ENTRY_COUNT) as usize;
    let header = ManifestHeader {
        compact_seq: read_u64_le(buf, OFF_COMPACT_SEQ),
        generation: read_u64_le(buf, OFF_GENERATION),
    };

    if version != VERSION {
        return Err(StorageError::InvalidVersion);
    }

    let body = count.checked_mul(ENTRY_SIZE).ok_or(StorageError::Truncated)?;
    let expected_data = HEADER_SIZE.checked_add(body).ok_or(StorageError::Truncated)?;
    if buf.len() < expected_data {
        return Err(StorageError::Truncated);
    }

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
pub const fn serialized_size(count: usize) -> usize {
    HEADER_SIZE + count * ENTRY_SIZE
}

// ---------------------------------------------------------------------------
// File I/O (read + atomic write)
// ---------------------------------------------------------------------------

/// Read and parse a manifest file in one open. Returns `Ok(None)` when the
/// file does not exist yet (first-time table boot ⇒ empty manifest); any other
/// read failure is `Err(Io)`.
pub fn read_file(path: &std::ffi::CStr) -> Result<Option<(Vec<ManifestEntryRaw>, ManifestHeader)>, StorageError> {
    use std::os::unix::ffi::OsStrExt;
    let buf = match std::fs::read(std::path::Path::new(std::ffi::OsStr::from_bytes(path.to_bytes()))) {
        Ok(buf) => buf,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(StorageError::Io),
    };
    parse(&buf).map(Some)
}

/// Open .tmp manifest at "<path>.tmp", serialize entries into it, and return
/// the open fd plus owned path buffers. Does NOT fdatasync, close, or rename.
/// On any internal write error closes the fd and unlinks the .tmp.
pub struct PreparedManifest {
    fd: OwnedFd,
    tmp_path: std::ffi::CString,
    final_path: std::ffi::CString,
    /// Set true once the `.tmp` has been renamed into place (`commit`). Until
    /// then, Drop unlinks the `.tmp` so a panic or early return between
    /// `prepare_file` and the rename never leaks the temporary file. The fd
    /// closes via `OwnedFd`.
    committed: bool,
}

impl PreparedManifest {
    /// The staged `.tmp`'s fd, open from `prepare_file` until the value drops
    /// (after `commit`'s rename, or on the abandon path).
    pub fn fd(&self) -> libc::c_int {
        self.fd.as_raw_fd()
    }

    /// Rename the staged `.tmp` into place, consuming the staging value. The
    /// sole owner of the rename + Drop-suppression transition: a failure leaves
    /// `committed` unset, so Drop unlinks the `.tmp`.
    pub fn commit(mut self) -> Result<(), StorageError> {
        let rc = unsafe { libc::rename(self.tmp_path.as_ptr(), self.final_path.as_ptr()) };
        if rc < 0 {
            // `self` drops here: Drop unlinks the .tmp.
            return Err(StorageError::Io);
        }
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

pub fn prepare_file(
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

    let fd = open_owned(&tmp_path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC).ok_or(StorageError::Io)?;

    if crate::foundation::posix_io::write_all_fd(fd.as_raw_fd(), &buf[..written]).is_err() {
        unsafe {
            libc::unlink(tmp_path.as_ptr());
        }
        return Err(StorageError::Io);
    }

    Ok(PreparedManifest {
        fd,
        tmp_path,
        final_path,
        committed: false,
    })
}

/// Read one `u64` field at `off` from a manifest file's validated header.
///
/// Returns `Ok(None)` when the file cannot be opened (does not exist yet),
/// `Ok(Some(v))` when the header parses, or `Err(_)` on any other failure.
fn peek_header_u64(path: &std::ffi::CStr, off: usize) -> Result<Option<u64>, StorageError> {
    let Some(fd) = open_owned(path, libc::O_RDONLY) else {
        return Ok(None);
    };
    let mut hdr = [0u8; HEADER_SIZE];
    let n = crate::foundation::posix_io::read_all_fd(fd.as_raw_fd(), &mut hdr).unwrap_or(0);
    if n < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }
    let magic = read_u64_le(&hdr, 0);
    if magic != MAGIC {
        return Err(StorageError::InvalidMagic);
    }
    Ok(Some(read_u64_le(&hdr, off)))
}

/// Read just the checkpoint generation from a manifest file header. `Ok(None)`
/// when the file does not exist yet. Used by the conditional view-state reload:
/// a `RederiveCheckpointed` table loads its shards only when its manifest
/// generation equals the committed checkpoint generation, and erases them
/// otherwise.
/// Path of one partition's manifest inside a partitioned store's directory —
/// the single spelling of the `part_{p}/manifest.bin` layout the boot resume
/// verdict peeks and the rebuild reset unlinks.
pub fn partition_manifest_path(dir: &str, p: u32) -> String {
    format!("{dir}/part_{p}/manifest.bin")
}

pub fn peek_generation(path: &std::ffi::CStr) -> Result<Option<u64>, StorageError> {
    peek_header_u64(path, OFF_GENERATION)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(compact_seq: u64, generation: u64) -> ManifestHeader {
        ManifestHeader {
            compact_seq,
            generation,
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
        let bytes = name.as_bytes();
        let len = bytes.len().min(127);
        let mut filename = [0u8; 128];
        filename[..len].copy_from_slice(&bytes[..len]);
        ManifestEntryRaw {
            max_lsn,
            filename,
            level: 1,
            guard_key: 42,
        }
    }

    #[test]
    fn roundtrip() {
        let entries: Vec<ManifestEntryRaw> = (0..4)
            .map(|i| make_entry(100 + i as u64, &format!("shard_{i}.db")))
            .collect();
        let n = entries.len();
        let mut buf = vec![0u8; serialized_size(n)];

        let written = serialize(&mut buf, &entries, hdr(7, 3)).unwrap();
        assert_eq!(written, buf.len());

        let (out, header) = parse(&buf).unwrap();
        assert_eq!(out.len(), n);
        assert_eq!(header, hdr(7, 3), "header must round-trip through serialize/parse");

        for i in 0..n {
            assert_eq!(out[i].max_lsn, entries[i].max_lsn);
            assert_eq!(out[i].level, entries[i].level);
            assert_eq!(out[i].guard_key, entries[i].guard_key);
            assert_eq!(out[i].filename, entries[i].filename);
        }
    }

    #[test]
    fn generation_roundtrip() {
        // The generation stamp must survive serialize/parse.
        let entries = vec![make_entry(1, "shard_1.db")];
        let header = ManifestHeader {
            compact_seq: 9,
            generation: 42,
        };
        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &entries, header).unwrap();

        let (out, got) = parse(&buf).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(got, header, "generation must round-trip through serialize/parse");
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

    #[test]
    fn peek_generation_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST_GEN");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        // Absent file ⇒ Ok(None).
        assert_eq!(peek_generation(&cpath).unwrap(), None);

        // Non-zero generation round-trips through the header.
        write_manifest(&cpath, &[make_entry(1, "shard_1.db")], hdr(3, 42));
        assert_eq!(peek_generation(&cpath).unwrap(), Some(42));

        // Republish at generation 0 (the base-round stamp).
        write_manifest(&cpath, &[make_entry(1, "shard_1.db")], hdr(3, 0));
        assert_eq!(peek_generation(&cpath).unwrap(), Some(0));
    }
}
