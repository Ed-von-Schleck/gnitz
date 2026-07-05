use std::os::fd::{AsRawFd, OwnedFd};

use super::error::StorageError;
use crate::foundation::codec::{read_u64_le, write_u64_le};
use crate::foundation::posix_io::open_owned;
use crate::schema::MAX_PK_BYTES;

// ---------------------------------------------------------------------------
// Manifest file format (V4)
// ---------------------------------------------------------------------------
//
// Header (64 bytes):
//   [0,8)   Magic   0x4D414E49464E5447
//   [8,16)  Version u64 (5)
//   [16,24) Count   u64
//   [24,32) Global max LSN u64
//   [32,40) Compaction sequence u64
//   [40,64) Reserved
//
// Entry (338 bytes each):
//   [0,8)     table_id   u64
//   [8,89)    pk_min     PkBuf  (1 len byte + 80 raw bytes)
//   [89,170)  pk_max     PkBuf  (1 len byte + 80 raw bytes)
//   [170,178) min_lsn    u64
//   [178,186) max_lsn    u64
//   [186,314) filename   128 bytes (null-terminated)
//   [314,322) level      u64
//   [322,338) guard_key  u128 LE
//
// `pk_min`/`pk_max` carry a width-tagged byte buffer so a shard whose
// PK region is wider than 16 bytes (a compound PK) records its true
// min/max key. `guard_key` stays u128 — the compaction-routing path is
// single-PK only and out of scope here.

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION_V5: u64 = 5;
const HEADER_SIZE: usize = 64;
const ENTRY_SIZE_V4: usize = 338;

// Header offset of the per-table compaction sequence.
const OFF_COMPACT_SEQ: usize = 32;

// Field offsets within an entry, kept in sync with the doc-comment above.
const OFF_TABLE_ID: usize = 0;
const OFF_PK_MIN: usize = 8;
const OFF_PK_MAX: usize = 89;
const OFF_MIN_LSN: usize = 170;
const OFF_MAX_LSN: usize = 178;
const OFF_FILENAME: usize = 186;
const OFF_LEVEL: usize = 314;
const OFF_GUARD_KEY: usize = 322;

// Build break if the documented field widths stop summing to the entry
// size — an offset/size edit that desyncs serialize/parse fails the
// build rather than corrupting the manifest. Literal widths only: the
// in-memory struct is not #[repr(C)], so size_of is unrelated to the
// 338-byte on-disk image.
const _: () = assert!(
    ENTRY_SIZE_V4 == 8 + (1 + 80) + (1 + 80) + 8 + 8 + 128 + 8 + 16,
    "V4 entry field widths do not sum to ENTRY_SIZE_V4",
);
// The 81-byte PkBuf field windows are fixed. A wider MAX_PK_BYTES would
// silently grow PkBuf::bytes while the windows stay fixed, so serialize's
// copy_from_slice of the full bytes array would panic at runtime. Force
// a deliberate build break (→ V5 format decision) instead.
const _: () = assert!(MAX_PK_BYTES == 80, "V4 manifest layout assumes an 80-byte PK capacity",);

// `PkBuf` — the width-tagged PK byte buffer — now lives in `schema::key`
// (its `Ord` delegates to `compare_pk_bytes`, which moved there too).
// Re-exported so `manifest::PkBuf` paths and this module's own uses are
// unchanged.
pub(crate) use crate::schema::key::PkBuf;

/// On-disk manifest entry. Matches the V4 binary format exactly.
#[derive(Clone, Copy)]
pub struct ManifestEntryRaw {
    pub table_id: u64,
    pub pk_min: PkBuf,
    pub pk_max: PkBuf,
    pub min_lsn: u64,
    pub max_lsn: u64,
    pub filename: [u8; 128],
    pub level: u64,
    pub guard_key: u128,
}

impl Default for ManifestEntryRaw {
    fn default() -> Self {
        Self {
            table_id: 0,
            pk_min: PkBuf::empty(0),
            pk_max: PkBuf::empty(0),
            min_lsn: 0,
            max_lsn: 0,
            filename: [0; 128],
            level: 0,
            guard_key: 0,
        }
    }
}

impl ManifestEntryRaw {
    pub fn zeroed() -> Self {
        Self::default()
    }

    pub fn filename_str(&self) -> &str {
        crate::foundation::codec::cstr_from_buf(&self.filename)
    }
}

/// Header metadata serialized alongside the entries.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ManifestHeader {
    pub global_max_lsn: u64,
    pub compact_seq: u64,
}

/// Serialize manifest entries into `out_buf`.
/// Returns bytes written, or `BufferTooSmall` if `out_buf` cannot fit.
pub fn serialize(
    out_buf: &mut [u8],
    entries: &[ManifestEntryRaw],
    header: ManifestHeader,
) -> Result<usize, StorageError> {
    let count = entries.len();
    let total = HEADER_SIZE + count * ENTRY_SIZE_V4;
    if out_buf.len() < total {
        return Err(StorageError::BufferTooSmall);
    }

    // Zero header
    out_buf[..HEADER_SIZE].fill(0);

    // Write header
    write_u64_le(out_buf, 0, MAGIC);
    write_u64_le(out_buf, 8, VERSION_V5);
    write_u64_le(out_buf, 16, count as u64);
    write_u64_le(out_buf, 24, header.global_max_lsn);
    write_u64_le(out_buf, OFF_COMPACT_SEQ, header.compact_seq);

    // Write entries field-by-field (symmetric with parse; immune to padding changes).
    // Each PkBuf is written as `len: u8` then the full 80-byte `bytes`
    // array verbatim (not bytes[..len]) — the tail is always zero by
    // construction, so the on-disk image is deterministic regardless of
    // the zero-init of the output buffer.
    for (i, e) in entries.iter().enumerate().take(count) {
        let off = HEADER_SIZE + i * ENTRY_SIZE_V4;
        write_u64_le(out_buf, off + OFF_TABLE_ID, e.table_id);
        out_buf[off + OFF_PK_MIN] = e.pk_min.len;
        out_buf[off + OFF_PK_MIN + 1..off + OFF_PK_MIN + 1 + MAX_PK_BYTES].copy_from_slice(&e.pk_min.bytes);
        out_buf[off + OFF_PK_MAX] = e.pk_max.len;
        out_buf[off + OFF_PK_MAX + 1..off + OFF_PK_MAX + 1 + MAX_PK_BYTES].copy_from_slice(&e.pk_max.bytes);
        write_u64_le(out_buf, off + OFF_MIN_LSN, e.min_lsn);
        write_u64_le(out_buf, off + OFF_MAX_LSN, e.max_lsn);
        out_buf[off + OFF_FILENAME..off + OFF_FILENAME + 128].copy_from_slice(&e.filename);
        write_u64_le(out_buf, off + OFF_LEVEL, e.level);
        out_buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + 16].copy_from_slice(&e.guard_key.to_le_bytes());
    }

    Ok(total)
}

/// Parse a manifest buffer. Returns the entry count and header on success.
///
/// V4 only. There is no on-disk data to migrate in dev and the test
/// suite is green at session start, so a non-V4 version is a hard
/// `InvalidVersion` — no per-version field gating, no shims.
pub fn parse(
    buf: &[u8],
    out_entries: &mut [ManifestEntryRaw],
    max_entries: u32,
) -> Result<(usize, ManifestHeader), StorageError> {
    if buf.len() < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }

    let magic = read_u64_le(buf, 0);
    if magic != MAGIC {
        return Err(StorageError::InvalidMagic);
    }

    let version = read_u64_le(buf, 8);
    let count = read_u64_le(buf, 16) as usize;
    let header = ManifestHeader {
        global_max_lsn: read_u64_le(buf, 24),
        compact_seq: read_u64_le(buf, OFF_COMPACT_SEQ),
    };

    if version != VERSION_V5 {
        return Err(StorageError::InvalidVersion);
    }

    let body = count.checked_mul(ENTRY_SIZE_V4).ok_or(StorageError::Truncated)?;
    let expected_data = HEADER_SIZE.checked_add(body).ok_or(StorageError::Truncated)?;
    if buf.len() < expected_data {
        return Err(StorageError::Truncated);
    }

    let n = count.min(max_entries as usize);
    for (i, out_entry) in out_entries.iter_mut().enumerate().take(n) {
        let off = HEADER_SIZE + i * ENTRY_SIZE_V4;
        let mut entry = ManifestEntryRaw::zeroed();

        entry.table_id = read_u64_le(buf, off + OFF_TABLE_ID);
        entry.pk_min = parse_pkbuf(buf, off + OFF_PK_MIN)?;
        entry.pk_max = parse_pkbuf(buf, off + OFF_PK_MAX)?;
        entry.min_lsn = read_u64_le(buf, off + OFF_MIN_LSN);
        entry.max_lsn = read_u64_le(buf, off + OFF_MAX_LSN);
        entry
            .filename
            .copy_from_slice(&buf[off + OFF_FILENAME..off + OFF_FILENAME + 128]);
        entry.level = read_u64_le(buf, off + OFF_LEVEL);
        entry.guard_key = u128::from_le_bytes(buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + 16].try_into().unwrap());

        *out_entry = entry;
    }

    Ok((n, header))
}

/// Read a `[len u8][80 bytes]` PkBuf field at `field_off`.
///
/// `PkBuf::from_bytes` only `debug_assert!`s `len <= MAX_PK_BYTES`; in a
/// release build a corrupt manifest with `len > 80` would index past
/// the 80-byte field window. Reject it as a soft parse error, matching
/// the existing `Truncated`/`InvalidMagic` posture — a corrupt manifest
/// is not a panic.
#[inline]
fn parse_pkbuf(buf: &[u8], field_off: usize) -> Result<PkBuf, StorageError> {
    let len = buf[field_off] as usize;
    if len > MAX_PK_BYTES {
        return Err(StorageError::Truncated);
    }
    Ok(PkBuf::from_bytes(&buf[field_off + 1..field_off + 1 + len]))
}

/// Returns the buffer size needed to serialize `count` entries.
pub const fn serialized_size(count: usize) -> usize {
    HEADER_SIZE + count * ENTRY_SIZE_V4
}

// ---------------------------------------------------------------------------
// File I/O (read + atomic write)
// ---------------------------------------------------------------------------

/// Read a manifest file from disk, parse it, write entries into `out_entries`.
///
/// Returns the entry count and header on success.
pub fn read_file(
    path: &std::ffi::CStr,
    out_entries: &mut [ManifestEntryRaw],
    max_entries: u32,
) -> Result<(usize, ManifestHeader), StorageError> {
    let fd = open_owned(path, libc::O_RDONLY).ok_or(StorageError::Io)?;

    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } < 0 {
        return Err(StorageError::Io);
    }
    let file_size = st.st_size as usize;

    if file_size < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }

    let mut buf = vec![0u8; file_size];
    // SAFETY: `buf` is a valid writable buffer of `file_size` bytes.
    let bytes_read = unsafe { crate::foundation::posix_io::read_all_fd(fd.as_raw_fd(), &mut buf) };

    if bytes_read < 0 || (bytes_read as usize) < file_size {
        return Err(StorageError::Io);
    }

    parse(&buf, out_entries, max_entries)
}

/// Open .tmp manifest at "<path>.tmp", serialize entries into it, and return
/// the open fd plus owned path buffers. Does NOT fdatasync, close, or rename.
/// On any internal write error closes the fd and unlinks the .tmp.
pub struct PreparedManifest {
    pub fd: OwnedFd,
    pub tmp_path: std::ffi::CString,
    pub final_path: std::ffi::CString,
    /// Set true once the `.tmp` has been renamed into place. Until then, Drop
    /// unlinks the `.tmp` so a panic or early return between `prepare_file` and
    /// the rename never leaks the temporary file. The fd closes via `OwnedFd`.
    pub committed: bool,
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
    let final_path = std::ffi::CString::new(path.to_bytes()).map_err(|_| StorageError::InvalidPath)?;

    let fd = open_owned(&tmp_path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC).ok_or(StorageError::Io)?;

    // SAFETY: `buf[..written]` is a valid initialized byte range.
    let rc = unsafe { crate::foundation::posix_io::write_all_fd(fd.as_raw_fd(), &buf[..written]) };
    if rc < 0 {
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

/// Read just the entry count from a manifest file header.
///
/// Returns `Ok(None)` when the file cannot be opened (does not exist yet —
/// load_manifest treats this as the empty manifest), `Ok(Some(n))` when the
/// header parses, or `Err(_)` on any other failure.
pub fn entry_count(path: &std::ffi::CStr) -> Result<Option<usize>, StorageError> {
    let Some(fd) = open_owned(path, libc::O_RDONLY) else {
        return Ok(None);
    };
    let mut hdr = [0u8; HEADER_SIZE];
    // SAFETY: `hdr` is a valid writable buffer of `HEADER_SIZE` bytes.
    let n = unsafe { crate::foundation::posix_io::read_all_fd(fd.as_raw_fd(), &mut hdr) };
    if n < HEADER_SIZE as i64 {
        return Err(StorageError::Truncated);
    }
    let magic = read_u64_le(&hdr, 0);
    if magic != MAGIC {
        return Err(StorageError::InvalidMagic);
    }
    Ok(Some(read_u64_le(&hdr, 16) as usize))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pkbuf(slice: &[u8]) -> PkBuf {
        PkBuf::from_bytes(slice)
    }

    fn hdr(global_max_lsn: u64, compact_seq: u64) -> ManifestHeader {
        ManifestHeader {
            global_max_lsn,
            compact_seq,
        }
    }

    fn assert_pkbuf_eq(got: &PkBuf, want: &PkBuf) {
        assert_eq!(got.len, want.len, "PkBuf len mismatch");
        assert_eq!(got.pk_bytes(), want.pk_bytes(), "PkBuf bytes mismatch");
        assert!(
            got.bytes[got.len as usize..].iter().all(|&b| b == 0),
            "PkBuf tail must be zero",
        );
    }

    #[test]
    fn pkbuf_eq_hash_compare_only_len_window() {
        use std::collections::HashSet;
        // Same meaningful bytes, different tail → equal and same hash.
        let mut a = PkBuf::from_bytes(&7u64.to_le_bytes());
        let b = PkBuf::from_bytes(&7u64.to_le_bytes());
        a.bytes[8] = 0xAB; // tail garbage past len; eq/hash must ignore it
        assert!(a == b);
        let mut set: HashSet<PkBuf> = HashSet::new();
        set.insert(b);
        assert!(set.contains(&a), "tail bytes must not affect membership");
    }

    #[test]
    fn pkbuf_borrow_heterogeneous_lookup() {
        use std::collections::HashSet;
        let mut set: HashSet<PkBuf> = HashSet::new();
        set.insert(PkBuf::from_bytes(&123u64.to_le_bytes()));
        // Raw &[u8] lookup via Borrow<[u8]> — no PkBuf construction.
        assert!(set.contains(&123u64.to_le_bytes()[..]));
        assert!(!set.contains(&124u64.to_le_bytes()[..]));
    }

    #[test]
    fn pkbuf_wide_differs_past_byte_16() {
        // Two 24-byte keys identical in the first 16 bytes but differing in the
        // last 8 must be distinct — the failure mode of any u128-truncating key.
        let mut x = Vec::new();
        x.extend_from_slice(&1u64.to_le_bytes());
        x.extend_from_slice(&2u64.to_le_bytes());
        x.extend_from_slice(&3u64.to_le_bytes());
        let mut y = x.clone();
        y[16..24].copy_from_slice(&999u64.to_le_bytes());
        let px = PkBuf::from_bytes(&x);
        let py = PkBuf::from_bytes(&y);
        assert!(px != py);
        let mut set = std::collections::HashSet::new();
        set.insert(px);
        assert!(!set.contains(&py));
    }

    #[test]
    fn parse_rejects_count_overflow() {
        // A corrupt header whose count * ENTRY_SIZE_V4 overflows usize must be
        // rejected, not wrap past the length check.
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, MAGIC);
        write_u64_le(&mut buf, 8, VERSION_V5);
        write_u64_le(&mut buf, 16, u64::MAX); // count
        let mut out = [ManifestEntryRaw::zeroed(); 1];
        let r = parse(&buf, &mut out, 1);
        assert!(matches!(r, Err(StorageError::Truncated)));
    }

    fn make_entry(id: u64, name: &str) -> ManifestEntryRaw {
        let mut e = ManifestEntryRaw::zeroed();
        e.table_id = id;
        e.pk_min = pkbuf(&(id * 10).to_le_bytes());
        e.pk_max = pkbuf(&(id * 10 + 99).to_le_bytes());
        e.min_lsn = 1;
        e.max_lsn = 100;
        e.level = 1;
        e.guard_key = 42;
        let bytes = name.as_bytes();
        let len = bytes.len().min(127);
        e.filename[..len].copy_from_slice(&bytes[..len]);
        e
    }

    #[test]
    fn roundtrip() {
        // Cover PkBuf widths 8, 16, 24, and 80.
        let widths = [8usize, 16, 24, 80];
        let entries: Vec<ManifestEntryRaw> = widths
            .iter()
            .enumerate()
            .map(|(i, &w)| {
                let mut e = make_entry(i as u64 + 1, &format!("shard_{i}.db"));
                let mut min_b = vec![0u8; w];
                let mut max_b = vec![0u8; w];
                for (j, b) in min_b.iter_mut().enumerate() {
                    *b = (j as u8).wrapping_add(1);
                }
                for (j, b) in max_b.iter_mut().enumerate() {
                    *b = (j as u8).wrapping_add(0x80);
                }
                e.pk_min = pkbuf(&min_b);
                e.pk_max = pkbuf(&max_b);
                e
            })
            .collect();
        let n = entries.len();
        let mut buf = vec![0u8; serialized_size(n)];

        let written = serialize(&mut buf, &entries, hdr(999, 7)).unwrap();
        assert_eq!(written, buf.len());

        let mut out = vec![ManifestEntryRaw::zeroed(); n];
        let (count, header) = parse(&buf, &mut out, n as u32).unwrap();
        assert_eq!(count, n);
        assert_eq!(header, hdr(999, 7), "header must round-trip through serialize/parse");

        for i in 0..n {
            assert_eq!(out[i].table_id, entries[i].table_id);
            assert_pkbuf_eq(&out[i].pk_min, &entries[i].pk_min);
            assert_pkbuf_eq(&out[i].pk_max, &entries[i].pk_max);
            assert_eq!(out[i].pk_min.len as usize, widths[i]);
            assert_eq!(out[i].min_lsn, entries[i].min_lsn);
            assert_eq!(out[i].max_lsn, entries[i].max_lsn);
            assert_eq!(out[i].level, entries[i].level);
            assert_eq!(out[i].guard_key, entries[i].guard_key);
            assert_eq!(out[i].filename, entries[i].filename);
        }
    }

    #[test]
    fn v3_rejected() {
        // A V3 header must be rejected outright — no legacy reader.
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, MAGIC);
        write_u64_le(&mut buf, 8, 3); // V3
        write_u64_le(&mut buf, 16, 0);
        write_u64_le(&mut buf, 24, 0);

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        assert_eq!(parse(&buf, &mut out, 1), Err(StorageError::InvalidVersion));
    }

    #[test]
    fn corrupt_pkbuf_len_rejected() {
        let entries = vec![make_entry(1, "shard_1.db")];
        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &entries, hdr(0, 0)).unwrap();

        // Overwrite the pk_min len byte with 255 (> MAX_PK_BYTES).
        buf[HEADER_SIZE + OFF_PK_MIN] = 255;

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        assert_eq!(parse(&buf, &mut out, 1), Err(StorageError::Truncated));
    }

    #[test]
    fn pkbuf_width_independent_of_schema_stride() {
        // A PkBuf with len=16 must round-trip as len=16 even though some
        // table's pk_stride is 8 — PkBuf.len, not the live schema,
        // defines the stored key width (no silent truncation).
        let mut e = make_entry(1, "wide.db");
        e.pk_min = pkbuf(&[0xAB; 16]);
        e.pk_max = pkbuf(&[0xCD; 16]);

        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &[e], hdr(0, 0)).unwrap();

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        parse(&buf, &mut out, 1).unwrap();

        assert_eq!(out[0].pk_min.len, 16);
        assert_eq!(out[0].pk_min.pk_bytes(), &[0xAB; 16]);
        assert_eq!(out[0].pk_max.len, 16);
        assert_eq!(out[0].pk_max.pk_bytes(), &[0xCD; 16]);
    }

    #[test]
    fn bad_magic() {
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, 0xDEADBEEF);

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        assert_eq!(parse(&buf, &mut out, 1), Err(StorageError::InvalidMagic));
    }

    #[test]
    fn truncated() {
        assert_eq!(parse(&[0u8; 10], &mut [], 0), Err(StorageError::Truncated));
    }

    #[test]
    fn buffer_too_small() {
        let entries = vec![make_entry(1, "test.db")];
        let mut buf = vec![0u8; 64]; // too small for header + entry
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

        let (count, header) = parse(&buf, &mut [], 0).unwrap();
        assert_eq!(count, 0);
        assert_eq!(header.global_max_lsn, 42);
    }

    #[test]
    fn filename_null_terminated() {
        let e = make_entry(1, "hello.db");
        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &[e], hdr(0, 0)).unwrap();

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        parse(&buf, &mut out, 1).unwrap();

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
        let mut m = prepare_file(path, entries, header).unwrap();
        assert_eq!(unsafe { libc::rename(m.tmp_path.as_ptr(), m.final_path.as_ptr()) }, 0);
        m.committed = true;
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

        write_manifest(&cpath, &entries, hdr(99, 5));
        assert!(path.exists());

        let mut out = vec![ManifestEntryRaw::zeroed(); 8];
        let (count, header) = read_file(&cpath, &mut out, 8).unwrap();
        assert_eq!(count, 3);
        assert_eq!(
            header,
            hdr(99, 5),
            "header must round-trip through prepare_file/read_file"
        );
        assert_eq!(out[0].table_id, 1);
        assert_eq!(out[1].table_id, 2);
        assert_eq!(out[2].table_id, 3);
        assert_eq!(out[0].level, 1);
        assert_eq!(out[0].guard_key, 42);
    }

    #[test]
    fn read_file_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("DOES_NOT_EXIST");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let rc = read_file(&cpath, &mut out, 1);
        assert_eq!(rc, Err(StorageError::Io));
    }

    #[test]
    fn write_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST_EMPTY");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        write_manifest(&cpath, &[], hdr(42, 0));

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let (count, header) = read_file(&cpath, &mut out, 1).unwrap();
        assert_eq!(count, 0);
        assert_eq!(header.global_max_lsn, 42);
    }
}
